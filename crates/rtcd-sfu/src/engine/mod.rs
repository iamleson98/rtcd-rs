//! The sans-I/O core of the SFU.
//!
//! An [`Engine`] owns every session of one shard and all their peer connections. It is driven from
//! outside by four kinds of input: [`Command`]s from the service, datagrams from the network, the
//! passage of time, and nothing else. It answers with datagrams to send ([`Engine::poll_transmit`])
//! and [`Event`]s for the service ([`Engine::poll_event`]). With no clock, socket or thread of its
//! own it runs unchanged in production, on virtual time in tests, and in benchmarks.
//!
//! One engine is one thread of work: a call lives wholly inside one engine, so forwarding a packet
//! from the participant who sent it to everyone who receives it touches no lock.

mod datachannel;
mod join;
mod media;
mod negotiate;
mod quality;
mod route;
mod session;
mod signaling;
mod types;
mod video;

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::Instant;

use bytes::BytesMut;
use rtc::sansio::Protocol;
use rtcd_proto::{Message, MessageType};
use tracing::{debug, error, info, warn};

pub(crate) use route::FastMap;
pub use route::RouteUpdate;
pub(crate) use route::stun_local_ufrag;
pub use signaling::Channel;
pub use types::{CloseReason, Command, Event, Proto, Transmit};

use self::route::Routes;
use self::session::{SessionKey, Sessions, SourceKey};
use crate::config::EngineConfig;
use crate::ids::Rng;
use crate::loudest::Selector;
use crate::metrics::Metrics;

/// How much a shard is asked for compared with what its thread can do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Load {
    /// Room to spare.
    #[default]
    Ok,
    /// No room left: small video layers and fewer voices.
    Shedding,
    /// At the limit: the same, harder, and the shard takes no new sessions.
    Saturated,
}

/// A call: the sessions that hear each other.
#[derive(Debug)]
pub(crate) struct Call {
    pub members: Vec<SessionKey>,
    /// The member sharing a screen, if any; one share per call.
    pub screen: Option<SessionKey>,
    /// Which open microphones are forwarded.
    pub voices: Selector<SessionKey>,
}

impl Call {
    fn new(audio_loudest: usize) -> Self {
        Self {
            members: Vec::new(),
            screen: None,
            voices: Selector::new(audio_loudest),
        }
    }
}

/// Names a call: its group (the service client that owns it) and its id.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CallId {
    pub group: String,
    pub call: String,
}

/// The core of an SFU shard.
pub struct Engine {
    /// Counts packet writes, to time one in a few.
    write_clock: u32,
    /// How much the shard this engine runs on is asked for compared with what it can do (see
    /// `runtime::scaler::Pressure`).
    load: Load,
    cfg: Arc<EngineConfig>,
    metrics: Arc<dyn Metrics>,
    rng: Rng,
    sessions: Sessions,
    by_id: HashMap<String, SessionKey>,
    calls: HashMap<CallId, Call>,
    routes: Routes,
    /// Who receives each stream a participant sends.
    fanout: HashMap<SourceKey, media::Fan>,
    /// Sessions with work to do.
    dirty: Vec<SessionKey>,
    timers: BTreeMap<(Instant, u64), SessionKey>,
    timer_seq: u64,
    out: VecDeque<Transmit>,
    events: VecDeque<Event>,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("sessions", &self.sessions.len())
            .field("calls", &self.calls.len())
            .finish()
    }
}

impl Engine {
    /// Tells the engine how loaded its shard is: from `Shedding` on, its receivers are held to the
    /// small video layers and its calls forward fewer voices (one, when `Saturated`).
    pub fn set_load(&mut self, load: Load) {
        self.load = load;
    }

    pub fn new(cfg: Arc<EngineConfig>, metrics: Arc<dyn Metrics>, rng: Rng) -> Self {
        Self {
            write_clock: 0,
            load: Load::Ok,
            cfg,
            metrics,
            rng,
            sessions: Sessions::default(),
            by_id: HashMap::new(),
            calls: HashMap::new(),
            routes: Routes::default(),
            fanout: HashMap::new(),
            dirty: Vec::new(),
            timers: BTreeMap::new(),
            timer_seq: 0,
            out: VecDeque::new(),
            events: VecDeque::new(),
        }
    }

    /// The number of live sessions.
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Applies a command from the service.
    pub fn handle_command(&mut self, now: Instant, cmd: Command) {
        match cmd {
            Command::Join(cfg) => self.join(now, cfg),
            Command::Leave { session_id } => {
                if let Some(&key) = self.by_id.get(&session_id) {
                    self.close_session(key, CloseReason::Left, now);
                }
            }
            Command::Signal(msg) => self.signal(now, msg),
        }
        self.drive(now);
    }

    /// Delivers a datagram received on `local` from `peer`. Returns whether a session took it: a
    /// datagram from an address and with an ICE username no session knows is not anyone's.
    pub fn handle_datagram(
        &mut self,
        now: Instant,
        proto: Proto,
        local: std::net::SocketAddr,
        peer: std::net::SocketAddr,
        data: BytesMut,
    ) -> bool {
        let routed = self.route(proto, local, peer, &data);
        if let Some(key) = routed {
            if let Some(s) = self.sessions.get_mut(key) {
                s.last_rx = now;
                use rtc::shared::{TaggedBytesMut, TransportContext};
                let result = s.pc.handle_read(TaggedBytesMut {
                    now,
                    transport: TransportContext {
                        local_addr: local,
                        peer_addr: peer,
                        ecn: None,
                        transport_protocol: proto.to_transport(),
                    },
                    message: data,
                });
                if let Err(err) = result {
                    // Garbage on the wire is routine; it is not worth more than a debug line.
                    debug!(session = %s.cfg.session_id, %err, "datagram rejected");
                }
            }
            self.mark_dirty(key);
        }
        self.drive(now);
        routed.is_some()
    }

    /// Advances time: wakes every session whose deadline has passed.
    pub fn handle_timeout(&mut self, now: Instant) {
        while let Some((&(at, seq), &key)) = self.timers.iter().next() {
            if at > now {
                break;
            }
            self.timers.remove(&(at, seq));
            if let Some(s) = self.sessions.get_mut(key) {
                s.timer = None;
                if let Err(err) = s.pc.handle_timeout(now) {
                    debug!(session = %s.cfg.session_id, %err, "timeout handling failed");
                }
            }
            self.check_deadlines(key, now);
            self.mark_dirty(key);
        }
        self.drive(now);
    }

    /// When the engine next needs [`handle_timeout`](Self::handle_timeout).
    pub fn poll_timeout(&self) -> Option<Instant> {
        self.timers.keys().next().map(|&(at, _)| at)
    }

    /// The next datagram to send.
    pub fn poll_transmit(&mut self) -> Option<Transmit> {
        self.out.pop_front()
    }

    /// The next report for the service.
    pub fn poll_event(&mut self) -> Option<Event> {
        self.events.pop_front()
    }

    /// Ends every session; the engine is empty afterwards.
    pub fn shutdown(&mut self, now: Instant) {
        for key in self.sessions.keys().collect::<Vec<_>>() {
            self.close_session(key, CloseReason::Shutdown, now);
        }
        self.drive(now);
    }

    fn mark_dirty(&mut self, key: SessionKey) {
        if let Some(s) = self.sessions.get_mut(key)
            && !s.dirty
        {
            s.dirty = true;
            self.dirty.push(key);
        }
    }

    /// Works through every session that has something to do, until none has.
    fn drive(&mut self, now: Instant) {
        while let Some(key) = self.dirty.pop() {
            if let Some(s) = self.sessions.get_mut(key) {
                s.dirty = false;
            } else {
                continue;
            }
            // One participant's bug must not take the whole shard's calls down with it.
            let outcome = catch_unwind(AssertUnwindSafe(|| self.pump(key, now)));
            if outcome.is_err() {
                let id = self
                    .sessions
                    .get(key)
                    .map(|s| s.cfg.session_id.clone())
                    .unwrap_or_default();
                error!(session = %id, "session panicked; closing it");
                self.metrics.error(
                    self.sessions
                        .get(key)
                        .map_or("", |s| s.cfg.group_id.as_str()),
                    "panic",
                );
                self.close_session(key, CloseReason::SignalingFailed, now);
            }
        }
    }

    /// Everything one session has pending: what its connection reported, what it received, what
    /// negotiation needs next, what it wants to send.
    fn pump(&mut self, key: SessionKey, now: Instant) {
        self.process_events(key, now);
        let Some(rtp) = self.process_reads(key, now) else {
            return;
        };
        if self.participant_closed(key) {
            info!(session = %self.session_id(key), "the participant closed the connection");
            self.close_session(key, CloseReason::ConnectionLost, now);
            return;
        }
        self.advance_negotiation(key, now);
        self.forward(key, rtp, now);
        self.flush(key);
        self.reschedule(key, now);
    }

    /// Moves what the connection wants to send into the output queue.
    fn flush(&mut self, key: SessionKey) {
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        while let Some(t) = s.pc.poll_write() {
            let proto = Proto::from_transport(t.transport.transport_protocol);
            tracing::trace!(session = %s.cfg.session_id, bytes = t.message.len(), peer = %t.transport.peer_addr, "transmit");
            self.out.push_back(Transmit {
                proto,
                local: t.transport.local_addr,
                peer: t.transport.peer_addr,
                data: t.message,
            });
        }
    }

    /// Registers the session's next wake-up in the timer queue.
    fn reschedule(&mut self, key: SessionKey, now: Instant) {
        let needs_tick = self.needs_tick(key);
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        // Video wants looking at once a second, as long as there is any.
        s.tick = match (needs_tick, s.tick) {
            (false, _) => None,
            (true, None) => Some(now + Self::tick_interval()),
            (true, kept) => kept,
        };
        let mut next = s.pc.poll_timeout();
        let mut consider = |t: Option<Instant>| {
            if let Some(t) = t {
                next = Some(next.map_or(t, |n| n.min(t)));
            }
        };
        consider(s.lock.deadline());
        consider(s.tick);
        consider(match s.phase {
            signaling::Phase::AwaitingFirstOffer { deadline }
            | signaling::Phase::Offering { deadline, .. } => Some(deadline),
            signaling::Phase::Idle => None,
        });
        match s.lifecycle {
            session::Lifecycle::Connecting => {
                consider(Some(s.created_at + self.cfg.connect_timeout))
            }
            session::Lifecycle::Connected => consider(Some(s.last_rx + self.cfg.idle_timeout)),
        }
        // An offer that had to wait (the participant held the lock) is retried when the hold ends.
        // Never ask to be woken at or before now: that would spin.
        let next = next.map(|t| t.max(now + std::time::Duration::from_millis(1)));
        if s.timer.map(|(t, _)| t) == next {
            return;
        }
        if let Some(old) = s.timer.take() {
            self.timers.remove(&old);
        }
        if let Some(at) = next {
            self.timer_seq += 1;
            let entry = (at, self.timer_seq);
            s.timer = Some(entry);
            self.timers.insert(entry, key);
        }
    }

    /// Acts on a session's expired deadlines.
    fn check_deadlines(&mut self, key: SessionKey, now: Instant) {
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        if let Some(lost) = s.lock.expire(now) {
            warn!(session = %s.cfg.session_id, ?lost, "signaling lock expired");
            self.metrics
                .error(&s.cfg.group_id, "signaling_lock_timeout");
        }
        let (phase, lifecycle, created_at) = (s.phase, s.lifecycle, s.created_at);
        if s.tick.is_some_and(|t| now >= t) {
            s.tick = Some(now + Self::tick_interval());
            self.housekeeping(key, now);
        }
        let Some(silent_since) = self.sessions.get(key).map(|s| s.last_rx) else {
            return;
        };
        match phase {
            signaling::Phase::AwaitingFirstOffer { deadline } if now >= deadline => {
                warn!(session = %self.session_id(key), "timed out waiting for the first offer");
                self.metrics.error(&self.group_id(key), "signaling");
                self.close_session(key, CloseReason::SignalingFailed, now);
                return;
            }
            signaling::Phase::Offering { deadline, .. } if now >= deadline => {
                self.offer_timed_out(key, now);
                return;
            }
            _ => {}
        }
        if lifecycle == session::Lifecycle::Connecting
            && now >= created_at + self.cfg.connect_timeout
        {
            warn!(session = %self.session_id(key), "never connected");
            self.close_session(key, CloseReason::ConnectTimeout, now);
        } else if lifecycle == session::Lifecycle::Connected
            && now.saturating_duration_since(silent_since) >= self.cfg.idle_timeout
        {
            warn!(session = %self.session_id(key), "no packet from the participant, dropping the session");
            self.metrics.connection_state("failed");
            self.close_session(key, CloseReason::ConnectionLost, now);
        }
    }

    /// Whether the participant told us it is leaving: closing a connection sends a DTLS close alert,
    /// which leaves the connection state as it was but closes the DTLS transport.
    fn participant_closed(&self, key: SessionKey) -> bool {
        self.sessions.get(key).is_some_and(|s| {
            s.pc.sctp().map(|sctp| sctp.transport().state())
                == Some(rtc::peer_connection::transport::RTCDtlsTransportState::Closed)
        })
    }

    fn session_id(&self, key: SessionKey) -> String {
        self.sessions
            .get(key)
            .map(|s| s.cfg.session_id.clone())
            .unwrap_or_default()
    }

    fn group_id(&self, key: SessionKey) -> String {
        self.sessions
            .get(key)
            .map(|s| s.cfg.group_id.clone())
            .unwrap_or_default()
    }

    /// Queues a message for the service about a session.
    fn emit(&mut self, key: SessionKey, kind: MessageType, data: Vec<u8>) {
        let Some(s) = self.sessions.get(key) else {
            return;
        };
        self.events.push_back(Event::Message(Message {
            group_id: s.cfg.group_id.clone(),
            user_id: s.cfg.user_id.clone(),
            session_id: s.cfg.session_id.clone(),
            call_id: s.cfg.call_id.clone(),
            kind,
            data,
        }));
    }

    /// Dispatches a message from a participant's client.
    fn signal(&mut self, now: Instant, msg: Message) {
        let Some(&key) = self.by_id.get(&msg.session_id) else {
            warn!(session = %msg.session_id, kind = ?msg.kind, "message for an unknown session");
            return;
        };
        match msg.kind {
            MessageType::Ice => self.remote_candidate(key, &msg.data),
            MessageType::Sdp => self.remote_description(key, &msg.data, Channel::Ws, now),
            MessageType::Mute => self.set_muted(key, true, now),
            MessageType::Unmute => self.set_muted(key, false, now),
            MessageType::ScreenOn => self.screen_on(key, &msg.data, now),
            MessageType::ScreenOff => self.screen_off(key, now),
            MessageType::VideoOn => self.video_on(key, &msg.data),
            MessageType::VideoOff => {}
            MessageType::VideoSubscribe => self.video_subscribe(key, &msg.data, now),
            MessageType::VoiceOn | MessageType::VoiceOff => {
                warn!(session = %msg.session_id, "client sent a message only the SFU sends");
            }
        }
        self.mark_dirty(key);
    }
}
