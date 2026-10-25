//! The recorder in the class call: it joins listen-only, the way the web app's call client does
//! (`lms-web/src/lib/features/calls/call-client.ts`), receives every voice, camera and shared
//! screen the SFU forwards, and hands each one to the recording. It never sends media and never
//! names the cameras it wants, so rtcd forwards all of them.
//!
//! Which cameras a recording keeps is a product choice, and today it is every one. Keeping fewer
//! (the teacher's only, say) would cost no change to rtcd or the server: the recorder would send
//! `video_subscribe` with the chosen call sessions once connected and again when they change, as
//! the web client's `setWantedVideo` does, and rtcd would stop forwarding the others to it; every
//! voice and the shared screen still arrive.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rtc::peer_connection::sdp::RTCSdpType;
use rtc::peer_connection::state::RTCPeerConnectionState;
use rtc::peer_connection::transport::RTCIceCandidateInit;
use serde::Deserialize;
use serde_json::value::RawValue;
use tracing::{error, warn};

use crate::media::Recording;
use crate::peer::{Cmd, Peer, PeerEvent, Users};
use crate::socket::Sender;

/// How long an attempt to join may take before the recorder tries again (the call may have ended
/// or restarted).
pub const JOIN_ACK_TIMEOUT: Duration = Duration::from_secs(15);
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
pub const REJOIN_AFTER: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Not in the call (none running, or between attempts).
    Idle,
    /// Join sent, waiting for the ack.
    Joining,
    /// Peer connection up or negotiating.
    Joined,
}

#[derive(Debug)]
pub struct Call {
    channel_id: String,
    sock: Sender,
    rec: Arc<Recording>,
    pub phase: Phase,
    pub phase_at: Instant,
    /// Ours, from the join ack.
    pub session_id: String,
    peer: Option<Peer>,
    pub connected: bool,
    pub failed: bool,
    users: Users,
    /// The people in the call besides the recorder.
    others: HashSet<String>,
}

/// What the SFU says over the websocket's `signal` event.
#[derive(Debug, PartialEq, Eq)]
pub enum Signal {
    Description { kind: RTCSdpType, sdp: String },
    Candidate(RTCIceCandidateInit),
}

impl Call {
    pub fn new(channel_id: &str, sock: Sender, rec: Arc<Recording>, now: Instant) -> Self {
        Self {
            channel_id: channel_id.to_owned(),
            sock,
            rec,
            phase: Phase::Idle,
            phase_at: now,
            session_id: String::new(),
            peer: None,
            connected: false,
            failed: false,
            users: Users::default(),
            others: HashSet::new(),
        }
    }

    pub fn users(&self) -> HashMap<String, String> {
        self.users.lock().map(|u| u.clone()).unwrap_or_default()
    }

    pub fn has_peer(&self) -> bool {
        self.peer.is_some()
    }

    pub fn set_phase(&mut self, p: Phase, now: Instant) {
        self.phase = p;
        self.phase_at = now;
    }

    /// Asks to join the channel's call.
    pub fn join(&mut self, now: Instant) {
        if let Err(e) = self.sock.send(
            "custom_calls_join",
            serde_json::json!({ "channelID": self.channel_id }),
        ) {
            warn!(error = %e, "cannot join the call yet");
            self.set_phase(Phase::Idle, now);
            return;
        }
        self.set_phase(Phase::Joining, now);
    }

    /// Sends an action whose loss costs nothing worse than a log line: the recorder acts on what
    /// the server says next, not on this send.
    fn notify(&self, action: &str, data: impl serde::Serialize) {
        if let Err(e) = self.sock.send(action, data) {
            warn!(action, error = %e, "action not sent");
        }
    }

    fn send_signal(&self, action: &str, json: String) {
        if let Err(e) = self.sock.send(
            &format!("custom_calls_{action}"),
            serde_json::json!({ "data": json }),
        ) {
            warn!(action, error = %e, "signaling not sent");
        }
    }

    /// Builds the peer connection once the server accepted the join.
    pub async fn joined(&mut self, data: &RawValue, now: Instant) {
        if self.phase != Phase::Joining {
            return;
        }
        #[derive(Deserialize)]
        struct Ack {
            #[serde(rename = "connID", default)]
            conn_id: String,
        }
        let Ok(ack) = serde_json::from_str::<Ack>(data.get()) else {
            return;
        };
        if ack.conn_id.is_empty() {
            return;
        }
        // The media is on the SFU's own addresses; its candidates reach the recorder through the
        // signaling, so the ICE servers of the ack are not needed to connect.
        let peer = match Peer::start(self.rec.clone(), self.users.clone()).await {
            Ok(p) => p,
            Err(e) => {
                error!(error = %e, "cannot create the peer connection");
                self.leave(now).await;
                return;
            }
        };
        self.session_id = ack.conn_id;
        self.connected = false;
        self.failed = false;
        self.set_phase(Phase::Joined, now);
        self.send_signal("sdp", peer.offer.clone());
        self.peer = Some(peer);
        // Listen-only: the server marks us muted, like the web app's recorder.
        self.notify("custom_calls_mute", serde_json::json!({}));
    }

    /// The next thing the peer connection has to say; pending while there is none.
    pub async fn peer_event(&mut self) -> PeerEvent {
        match self.peer.as_mut() {
            Some(p) => match p.events.recv().await {
                Some(ev) => ev,
                None => std::future::pending().await,
            },
            None => std::future::pending().await,
        }
    }

    pub fn on_peer_event(&mut self, ev: PeerEvent) {
        match ev {
            PeerEvent::Signal { action, json } => self.send_signal(action, json),
            PeerEvent::State(RTCPeerConnectionState::Connected) => self.connected = true,
            PeerEvent::State(RTCPeerConnectionState::Failed) => self.failed = true,
            PeerEvent::State(_) => {}
        }
    }

    /// Applies the SFU's SDP or ICE from the websocket (the `signal` event).
    pub fn signal(&mut self, data: &RawValue) {
        let Some(peer) = &self.peer else { return };
        match parse_signal(data.get()) {
            Some(Signal::Description { kind, sdp }) => peer.send(Cmd::Description { kind, sdp }),
            Some(Signal::Candidate(c)) => peer.send(Cmd::Candidate(c)),
            None => {}
        }
    }

    /// Reads the call's participants (the `call_state` event: the call as a JSON string).
    pub fn state(&mut self, data: &RawValue) {
        #[derive(Deserialize)]
        struct Wrapper {
            #[serde(default)]
            call: String,
        }
        #[derive(Deserialize)]
        struct CallState {
            #[serde(default)]
            sessions: Vec<Member>,
        }
        #[derive(Deserialize)]
        struct Member {
            #[serde(default)]
            id: String,
            #[serde(default)]
            user_id: String,
        }
        let Ok(w) = serde_json::from_str::<Wrapper>(data.get()) else {
            return;
        };
        if w.call.is_empty() {
            return;
        }
        let Ok(cs) = serde_json::from_str::<CallState>(&w.call) else {
            return;
        };
        self.others.clear();
        let mut users = self.users.lock().unwrap_or_else(|p| p.into_inner());
        for s in cs.sessions {
            if s.id != self.session_id {
                self.others.insert(s.id.clone());
            }
            users.insert(s.id, s.user_id);
        }
    }

    /// Follows people joining and leaving (`user_joined`, `user_left`).
    pub fn presence(&mut self, data: &RawValue, joined: bool) {
        #[derive(Deserialize)]
        struct P {
            #[serde(default)]
            user_id: String,
            #[serde(default)]
            session_id: String,
        }
        let Ok(p) = serde_json::from_str::<P>(data.get()) else {
            return;
        };
        if p.session_id.is_empty() || p.session_id == self.session_id {
            return;
        }
        if joined {
            self.users
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(p.session_id.clone(), p.user_id);
            self.others.insert(p.session_id);
        } else {
            self.others.remove(&p.session_id);
        }
    }

    /// Whether nobody but the recorder is in the call.
    pub fn alone(&self) -> bool {
        self.others.is_empty()
    }

    pub fn other_count(&self) -> usize {
        self.others.len()
    }

    /// Points our call session at a new websocket connection (the server could not resume the old
    /// one).
    pub fn reconnected(&self) {
        if self.phase != Phase::Joined {
            return;
        }
        self.notify(
            "custom_calls_reconnect",
            serde_json::json!({ "channelID": self.channel_id, "originalConnID": self.session_id, "prevConnID": self.session_id }),
        );
        self.notify(
            "custom_calls_call_state",
            serde_json::json!({ "channelID": self.channel_id }),
        );
    }

    /// Ends our participation; the streams being written end with it.
    pub async fn leave(&mut self, now: Instant) {
        if self.phase != Phase::Idle {
            self.notify(
                "custom_calls_leave",
                serde_json::json!({ "channelID": self.channel_id }),
            );
        }
        if let Some(peer) = self.peer.take() {
            peer.close().await;
        }
        self.session_id.clear();
        self.connected = false;
        self.failed = false;
        self.set_phase(Phase::Idle, now);
    }

    /// Reports a join that did not get through in time.
    pub fn stalled(&self, now: Instant) -> bool {
        let waited = now.saturating_duration_since(self.phase_at);
        match self.phase {
            Phase::Joining => waited > JOIN_ACK_TIMEOUT,
            Phase::Joined => {
                if self.failed {
                    true
                } else if self.connected {
                    false
                } else {
                    waited > CONNECT_TIMEOUT
                }
            }
            Phase::Idle => false,
        }
    }

    #[cfg(test)]
    pub fn force(&mut self, phase: Phase, session_id: &str, now: Instant) {
        self.phase = phase;
        self.phase_at = now;
        self.session_id = session_id.to_owned();
    }
}

/// Reads a `signal` event's payload: `{"data": "<json>"}` where the JSON is a description
/// (`{"type":"offer"|"answer","sdp":...}`) or a candidate (nested under `candidate`, or flat).
pub fn parse_signal(payload: &str) -> Option<Signal> {
    #[derive(Deserialize)]
    struct Wrapper {
        #[serde(default)]
        data: String,
    }
    #[derive(Deserialize)]
    struct Msg<'a> {
        #[serde(rename = "type", default)]
        kind: String,
        #[serde(default)]
        sdp: String,
        #[serde(borrow, default)]
        candidate: Option<&'a RawValue>,
    }
    let wrapper: Wrapper = serde_json::from_str(payload).ok()?;
    if wrapper.data.is_empty() {
        return None;
    }
    let msg: Msg<'_> = serde_json::from_str(&wrapper.data).ok()?;
    match msg.kind.as_str() {
        "offer" => {
            return Some(Signal::Description {
                kind: RTCSdpType::Offer,
                sdp: msg.sdp,
            });
        }
        "answer" => {
            return Some(Signal::Description {
                kind: RTCSdpType::Answer,
                sdp: msg.sdp,
            });
        }
        _ => {}
    }
    let raw = match msg.candidate {
        Some(c) if c.get().starts_with('{') => c.get(),
        _ => wrapper.data.as_str(),
    };
    match serde_json::from_str::<RTCIceCandidateInit>(raw) {
        Ok(c) if c.candidate.is_empty() => None,
        Ok(c) => Some(Signal::Candidate(c)),
        Err(e) => {
            warn!(error = %e, "unreadable candidate");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wrap(data: &str) -> String {
        serde_json::json!({ "data": data }).to_string()
    }

    #[test]
    fn descriptions_are_read() {
        let s = parse_signal(&wrap(r#"{"type":"offer","sdp":"v=0"}"#));
        assert_eq!(
            s,
            Some(Signal::Description {
                kind: RTCSdpType::Offer,
                sdp: "v=0".into()
            })
        );
        let s = parse_signal(&wrap(r#"{"type":"answer","sdp":"v=1"}"#));
        assert_eq!(
            s,
            Some(Signal::Description {
                kind: RTCSdpType::Answer,
                sdp: "v=1".into()
            })
        );
    }

    #[test]
    fn candidates_come_nested_or_flat_and_empty_ones_are_dropped() {
        let nested = parse_signal(&wrap(
            r#"{"candidate":{"candidate":"candidate:1 1 udp 2113937151 192.0.2.1 5000 typ host","sdpMid":"0","sdpMLineIndex":0}}"#,
        ));
        let Some(Signal::Candidate(c)) = nested else {
            panic!("{nested:?}")
        };
        assert!(c.candidate.contains("192.0.2.1"));
        assert_eq!(
            (c.sdp_mid.as_deref(), c.sdp_mline_index),
            (Some("0"), Some(0))
        );

        let flat = parse_signal(&wrap(
            r#"{"candidate":"candidate:2 1 udp 2113937151 192.0.2.2 5000 typ host"}"#,
        ));
        let Some(Signal::Candidate(c)) = flat else {
            panic!("{flat:?}")
        };
        assert!(c.candidate.contains("192.0.2.2"));

        assert_eq!(
            parse_signal(&wrap(r#"{"type":"ice"}"#)),
            None,
            "the end of candidates"
        );
    }

    #[test]
    fn garbage_is_ignored() {
        assert_eq!(parse_signal("nope"), None);
        assert_eq!(parse_signal(&wrap("")), None);
        assert_eq!(parse_signal(&wrap("nope")), None);
        assert_eq!(
            parse_signal(&wrap(r#"{"candidate":{"candidate":5}}"#)),
            None
        );
        assert_eq!(
            parse_signal(&wrap(
                r#"{"candidate":"candidate:1 1 udp 1 192.0.2.1 5000 typ host","sdpMLineIndex":"x"}"#
            )),
            None
        );
    }

    fn call() -> Call {
        let now = Instant::now();
        let (tx, _rx) = tokio::sync::mpsc::channel(8);
        let sock = crate::socket::Sender::detached(tx);
        let rec = Arc::new(Recording::new(&std::env::temp_dir(), now, u64::MAX));
        Call::new("chan", sock, rec, now)
    }

    fn raw(s: &str) -> Box<RawValue> {
        RawValue::from_string(s.to_owned()).unwrap()
    }

    #[test]
    fn people_in_the_call() {
        let mut c = call();
        c.session_id = "me".into();
        assert!(c.alone(), "nobody else");

        c.state(&raw(r#"{"call":"{\"sessions\":[{\"id\":\"me\",\"user_id\":\"bot\"},{\"id\":\"s1\",\"user_id\":\"u1\"}]}"}"#));
        assert!(!c.alone());
        assert_eq!(c.other_count(), 1);
        assert_eq!(c.users()["s1"], "u1");
        assert_eq!(c.users()["me"], "bot");

        // A state that does not parse changes nothing.
        c.state(&raw(r#"{"call":"not json"}"#));
        c.state(&raw(r#"{"call":""}"#));
        c.state(&raw("\"nope\""));
        assert_eq!(c.other_count(), 1);

        // A new state replaces who is there.
        c.state(&raw(
            r#"{"call":"{\"sessions\":[{\"id\":\"me\",\"user_id\":\"bot\"}]}"}"#,
        ));
        assert!(c.alone(), "everyone left in the new state");

        c.presence(&raw(r#"{"user_id":"u2","session_id":"s2"}"#), true);
        assert!(!c.alone());
        assert_eq!(c.users()["s2"], "u2");
        c.presence(&raw(r#"{"user_id":"bot","session_id":"me"}"#), true);
        c.presence(&raw(r#"{"user_id":"u3"}"#), true);
        c.presence(&raw("\"nope\""), true);
        assert_eq!(
            c.other_count(),
            1,
            "the recorder itself and a nameless session are not people"
        );
        c.presence(&raw(r#"{"user_id":"u2","session_id":"s2"}"#), false);
        assert!(c.alone(), "the user left");
    }

    #[test]
    fn a_join_that_stalls_is_noticed() {
        let mut c = call();
        let t0 = Instant::now();
        assert!(
            !c.stalled(t0 + Duration::from_secs(3600)),
            "idle never stalls"
        );

        c.set_phase(Phase::Joining, t0);
        assert!(!c.stalled(t0 + JOIN_ACK_TIMEOUT));
        assert!(
            c.stalled(t0 + JOIN_ACK_TIMEOUT + Duration::from_millis(1)),
            "a join waits for its ack"
        );

        c.set_phase(Phase::Joined, t0);
        assert!(!c.stalled(t0 + CONNECT_TIMEOUT));
        assert!(
            c.stalled(t0 + CONNECT_TIMEOUT + Duration::from_millis(1)),
            "a connection waits for connect"
        );
        c.on_peer_event(PeerEvent::State(RTCPeerConnectionState::Connected));
        assert!(
            !c.stalled(t0 + Duration::from_secs(3600)),
            "a connected call never stalls"
        );

        c.on_peer_event(PeerEvent::State(RTCPeerConnectionState::Failed));
        assert!(c.stalled(t0), "a failed connection is stalled at once");
    }
}
