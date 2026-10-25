//! Starting and ending sessions.

use std::collections::VecDeque;
use std::time::Instant;

use rtc::peer_connection::transport::{
    CandidateConfig, CandidateHostConfig, RTCIceCandidate, RTCIceCandidateInit,
};
use rtc::sansio::Protocol;
use rtcd_proto::SessionConfig;
use tracing::{debug, error, info, warn};

use super::session::{DownPhase, Downlink, Lifecycle, Session, SessionKey, SourceKey, Uplink};
use super::signaling::{Phase, SignalingLock};
use super::types::{CloseReason, Event};
use super::{CallId, Engine};
use crate::metrics::Direction;
use crate::peer::new_peer;

impl Engine {
    /// Starts a session for `cfg` and waits for its client's first offer.
    pub(super) fn join(&mut self, now: Instant, cfg: SessionConfig) {
        if let Err(why) = cfg.validate() {
            warn!(session = %cfg.session_id, why, "join refused: invalid session config");
            self.metrics.error(&cfg.group_id, "join");
            return;
        }
        if self.by_id.contains_key(&cfg.session_id) {
            // The session in place is working; closing it for a duplicate request would cut a
            // participant off.
            warn!(session = %cfg.session_id, "join refused: the session already exists");
            self.metrics.error(&cfg.group_id, "join");
            return;
        }
        if self.sessions.len() >= self.cfg.max_sessions {
            warn!(session = %cfg.session_id, limit = self.cfg.max_sessions, "join refused: shard is full");
            self.metrics.error(&cfg.group_id, "capacity");
            self.reject(cfg);
            return;
        }
        let (pc, estimate) = match new_peer(&self.cfg, now) {
            Ok(parts) => parts,
            Err(err) => {
                error!(session = %cfg.session_id, %err, "failed to create the peer connection");
                self.metrics.error(&cfg.group_id, "join");
                self.reject(cfg);
                return;
            }
        };
        let ufrag = match pc.local_ice_parameters() {
            Ok(p) => p.username_fragment,
            Err(err) => {
                error!(session = %cfg.session_id, %err, "no local ICE parameters");
                self.reject(cfg);
                return;
            }
        };

        let call_id = CallId {
            group: cfg.group_id.clone(),
            call: cfg.call_id.clone(),
        };
        info!(session = %cfg.session_id, call = %cfg.call_id, user = %cfg.user_id, "session joined");
        self.metrics.session_started(&cfg.group_id);
        let session = Session {
            cfg,
            pc,
            estimate,
            created_at: now,
            last_rx: now,
            lifecycle: Lifecycle::Connecting,
            dirty: false,
            timer: None,
            ufrags: vec![ufrag.clone()],
            addrs: Vec::new(),
            candidates_added: false,
            early_candidates: Vec::new(),
            tick: None,
            phase: Phase::AwaitingFirstOffer {
                deadline: now + self.cfg.signaling_timeout,
            },
            lock: SignalingLock::new(self.cfg.lock_max_hold),
            wants_offer_since: None,
            offer_failures: 0,
            dc: None,
            dc_queue: VecDeque::new(),
            up: Uplink::default(),
            down: Downlink::default(),
            quality: Default::default(),
        };
        let session_id = session.cfg.session_id.clone();
        let key = self.sessions.insert(session);
        self.by_id.insert(session_id, key);
        self.routes.add_ufrag(ufrag, key);
        self.calls.entry(call_id).or_default().members.push(key);
        self.mark_dirty(key);
    }

    /// Tells the service a join that never became a session is over, so it does not wait for one.
    fn reject(&mut self, cfg: SessionConfig) {
        self.events.push_back(Event::SessionClosed {
            group_id: cfg.group_id,
            session_id: cfg.session_id,
            reason: CloseReason::Rejected,
        });
    }

    /// Adds the SFU's own addresses to the connection as host candidates.
    pub(super) fn add_local_candidates(&mut self, key: SessionKey) {
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        if s.candidates_added {
            return;
        }
        s.candidates_added = true;
        for a in self.cfg.addresses.iter().filter(|a| a.advertise) {
            let udp = a.advertised_udp();
            let mut candidates = vec![("udp", udp, rtc::ice::tcp_type::TcpType::Unspecified)];
            if let Some(tcp) = a.advertised_tcp() {
                candidates.push(("tcp", tcp, rtc::ice::tcp_type::TcpType::Passive));
            }
            for (network, addr, tcp_type) in candidates {
                let built = CandidateHostConfig {
                    base_config: CandidateConfig {
                        network: network.to_owned(),
                        address: addr.ip().to_string(),
                        port: addr.port(),
                        component: 1,
                        ..Default::default()
                    },
                    tcp_type,
                }
                .new_candidate_host()
                .map_err(|e| e.to_string())
                .and_then(|c| {
                    RTCIceCandidate::from(&c)
                        .to_json()
                        .map_err(|e| e.to_string())
                });
                match built {
                    Ok(init) => {
                        if let Err(err) = s.pc.add_local_candidate(init) {
                            warn!(session = %s.cfg.session_id, %err, %addr, "candidate refused");
                        }
                    }
                    Err(err) => {
                        warn!(session = %s.cfg.session_id, %err, %addr, "candidate not built")
                    }
                }
            }
        }
        // End of candidates.
        let _ = s.pc.add_local_candidate(RTCIceCandidateInit::default());
    }

    /// Ends a session and everything that hangs off it, and tells the service.
    pub(super) fn close_session(&mut self, key: SessionKey, reason: CloseReason, now: Instant) {
        let Some(mut s) = self.sessions.remove(key) else {
            return;
        };
        info!(session = %s.cfg.session_id, ?reason, "session closed");
        self.by_id.remove(&s.cfg.session_id);
        self.routes.remove_session(&s.ufrags, &s.addrs);
        if let Some(t) = s.timer.take() {
            self.timers.remove(&t);
        }
        self.metrics.session_ended(&s.cfg.group_id);
        self.metrics.connection_state("closed");

        let call_id = CallId {
            group: s.cfg.group_id.clone(),
            call: s.cfg.call_id.clone(),
        };
        if let Some(call) = self.calls.get_mut(&call_id) {
            call.members.retain(|&m| m != key);
            if call.screen == Some(key) {
                call.screen = None;
            }
            if call.members.is_empty() {
                self.calls.remove(&call_id);
            }
        }

        // Receivers of what this session sent: those streams are over.
        for src in &s.up.sources {
            self.metrics
                .track_removed(&s.cfg.group_id, Direction::In, &src.codec.mime_type);
            if let Some(fan) = self.fanout.remove(&SourceKey {
                session: key,
                source: src.id,
            }) {
                for sub in fan.serving.into_iter().chain(fan.waiting) {
                    if let Some(r) = self.sessions.get_mut(sub.session) {
                        r.down.drop_stream(sub.stream);
                    }
                    self.mark_dirty(sub.session);
                }
            }
        }
        // Senders to this session: no longer fed.
        for (id, ds) in s.down.iter() {
            for source in [ds.source, ds.target].into_iter().flatten() {
                if let Some(fan) = self.fanout.get_mut(&SourceKey {
                    session: ds.publisher,
                    source,
                }) {
                    fan.serving
                        .retain(|sub| !(sub.session == key && sub.stream == id));
                    fan.waiting
                        .retain(|sub| !(sub.session == key && sub.stream == id));
                }
            }
            if ds.phase == DownPhase::Active {
                self.metrics
                    .track_removed(&s.cfg.group_id, Direction::Out, &ds.codec.mime_type);
            }
        }

        // Let the peer see the connection close (a DTLS alert) rather than time out.
        let _ = s.pc.close();
        while let Some(t) = rtc::sansio::Protocol::poll_write(&mut s.pc) {
            let proto = super::types::Proto::from_transport(t.transport.transport_protocol);
            self.out.push_back(super::types::Transmit {
                proto,
                local: t.transport.local_addr,
                peer: t.transport.peer_addr,
                data: t.message,
            });
        }
        debug!(session = %s.cfg.session_id, "peer connection closed");

        self.events.push_back(Event::SessionClosed {
            group_id: s.cfg.group_id.clone(),
            session_id: s.cfg.session_id.clone(),
            reason,
        });
        let _ = now;
    }

    /// The members of the call a session belongs to, in join order.
    pub(super) fn call_members(&self, key: SessionKey) -> Vec<SessionKey> {
        let Some(s) = self.sessions.get(key) else {
            return Vec::new();
        };
        let id = CallId {
            group: s.cfg.group_id.clone(),
            call: s.cfg.call_id.clone(),
        };
        self.calls
            .get(&id)
            .map(|c| c.members.clone())
            .unwrap_or_default()
    }

    /// The configuration of a session, for the service's lookups.
    pub fn session_config(&self, session_id: &str) -> Option<SessionConfig> {
        self.sessions
            .get(*self.by_id.get(session_id)?)
            .map(|s| s.cfg.clone())
    }
}
