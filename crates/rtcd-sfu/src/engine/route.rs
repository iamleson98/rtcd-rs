//! Finding the session a datagram belongs to.
//!
//! Every session shares the SFU's ports, so the sender's address is the only identity a media
//! packet carries. The address becomes known from the first ICE check, which names the session by
//! the ICE username fragment the SFU gave it in its answer.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// A map hashed with a fast, randomly seeded function: it is read for every datagram, and the
/// standard SipHash costs more than the lookup is worth.
pub(crate) type FastMap<K, V> = HashMap<K, V, foldhash::fast::RandomState>;
use std::net::SocketAddr;

use bytes::BytesMut;

use super::Engine;
use super::session::SessionKey;
use super::types::Proto;

/// Addresses remembered per session; a participant moving between networks adds one now and then.
const MAX_ADDRS_PER_SESSION: usize = 8;

/// How long an address may go unheard before another user's session can take it over: a browser
/// sends an ICE consent check every few seconds, and consent lapses after this long without one
/// (RFC 7675), so a NAT may since have handed the port to someone else.
pub(crate) const ADDRESS_SILENCE: Duration = Duration::from_secs(30);

/// How many addresses a session may gain in one [`NEW_ADDR_WINDOW`]. A participant changes network
/// now and then; a peer signing checks for its own session from many addresses is churning the
/// routes (and the shards' sockets), and its checks beyond this are dropped.
const NEW_ADDRS_PER_WINDOW: usize = 4;
const NEW_ADDR_WINDOW: Duration = Duration::from_secs(10);

/// How often an address that is still being heard is published again, which is how the other
/// shards tell it is alive.
const REPUBLISH_EVERY: Duration = Duration::from_secs(10);

/// A change to what this engine answers for, published so other shards can send it the datagrams
/// that reached them instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteUpdate {
    AddUfrag(String),
    RemoveUfrag(String),
    /// An address learned for a session of `user`: where it was heard (`local`, as advertised) and
    /// from whom, and when it was last heard. Another shard's session takes the address only if
    /// its user is the same (and not empty) or the address has been silent for
    /// [`ADDRESS_SILENCE`]. A `refresh` only says the address is still heard: it never moves it.
    AddAddr {
        proto: Proto,
        local: SocketAddr,
        peer: SocketAddr,
        user: String,
        heard: Instant,
        refresh: bool,
    },
    RemoveAddr(Proto, SocketAddr),
}

/// How many new addresses a session may still gain in the current window.
#[derive(Debug)]
pub(crate) struct AddrBudget {
    began: Instant,
    used: usize,
}

impl AddrBudget {
    pub fn new(now: Instant) -> Self {
        Self {
            began: now,
            used: 0,
        }
    }

    /// Spends one new address, if the window has any left.
    fn take(&mut self, now: Instant) -> bool {
        if now.saturating_duration_since(self.began) >= NEW_ADDR_WINDOW {
            *self = Self::new(now);
        }
        if self.used >= NEW_ADDRS_PER_WINDOW {
            return false;
        }
        self.used += 1;
        true
    }
}

/// The session an address was learned for.
#[derive(Debug)]
struct Holder {
    key: SessionKey,
    /// When a datagram last came from the address.
    heard: Instant,
    /// When the address was last published.
    published: Instant,
}

#[derive(Debug, Default)]
pub(crate) struct Routes {
    by_addr: FastMap<(Proto, SocketAddr), Holder>,
    by_ufrag: HashMap<String, SessionKey>,
    /// Whether changes are recorded in `updates` (only when other shards need to know).
    publish: bool,
    updates: Vec<RouteUpdate>,
}

impl Routes {
    pub fn add_ufrag(&mut self, ufrag: String, key: SessionKey) {
        if self.publish {
            self.updates.push(RouteUpdate::AddUfrag(ufrag.clone()));
        }
        self.by_ufrag.insert(ufrag, key);
    }

    pub fn remove_session(&mut self, ufrags: &[String], addrs: &[(Proto, SocketAddr)]) {
        for u in ufrags {
            self.by_ufrag.remove(u);
            if self.publish {
                self.updates.push(RouteUpdate::RemoveUfrag(u.clone()));
            }
        }
        for a in addrs {
            self.by_addr.remove(a);
            if self.publish {
                self.updates.push(RouteUpdate::RemoveAddr(a.0, a.1));
            }
        }
    }

    pub fn set_publishing(&mut self, on: bool) {
        self.publish = on;
        if !on {
            self.updates.clear();
        }
    }

    pub fn take_updates(&mut self) -> Vec<RouteUpdate> {
        std::mem::take(&mut self.updates)
    }
}

impl Engine {
    /// The session `data` from `peer` is for, learning the address from a STUN check.
    ///
    /// An ICE check names its session by the username fragment, which is what routes it; every
    /// other datagram goes by the address its sender was last heard from. A check that would give
    /// a session an address it does not have is believed only if its MESSAGE-INTEGRITY holds for
    /// the session it names, and takes an address that another session holds only from a session
    /// of the same user (a rejoin from the same port) or when the holder has gone quiet there.
    /// Whoever knows their own session's password can sign a check from any address they like, so
    /// that is as far as a check is trusted.
    pub(super) fn route(
        &mut self,
        now: Instant,
        proto: Proto,
        local: SocketAddr,
        peer: SocketAddr,
        data: &BytesMut,
    ) -> Option<SessionKey> {
        let named = stun_local_ufrag(data).and_then(|u| self.routes.by_ufrag.get(u).copied());
        let Some(holder) = self.routes.by_addr.get_mut(&(proto, peer)) else {
            return self.adopt(now, proto, local, peer, named?, data);
        };
        let owner = holder.key;
        if let Some(key) = named
            && key != owner
        {
            return self.adopt(now, proto, local, peer, key, data);
        }
        holder.heard = now;
        let republish = self.routes.publish && now - holder.published >= REPUBLISH_EVERY;
        if republish {
            holder.published = now;
            self.publish_addr(owner, proto, local, peer, now, true);
        }
        Some(owner)
    }

    /// Gives session `key`, which a check from `peer` names, that address, if the check may.
    /// Returns where the datagram goes.
    fn adopt(
        &mut self,
        now: Instant,
        proto: Proto,
        local: SocketAddr,
        peer: SocketAddr,
        key: SessionKey,
        data: &[u8],
    ) -> Option<SessionKey> {
        let held = self
            .routes
            .by_addr
            .get(&(proto, peer))
            .map(|h| (h.key, h.heard));
        if !self.check_integrity(key, data) {
            return held.map(|(owner, _)| owner);
        }
        if let Some((owner, heard)) = held {
            let user = &self.sessions.get(owner)?.cfg.user_id;
            let same_user = !user.is_empty() && *user == self.sessions.get(key)?.cfg.user_id;
            if !same_user && now.saturating_duration_since(heard) <= ADDRESS_SILENCE {
                return Some(owner);
            }
        }
        if !self.sessions.get_mut(key)?.new_addrs.take(now) {
            return held.map(|(owner, _)| owner);
        }
        if let Some((owner, _)) = held {
            self.sessions
                .get_mut(owner)?
                .addrs
                .retain(|&a| a != (proto, peer));
        }
        let s = self.sessions.get_mut(key)?;
        if s.addrs.len() >= MAX_ADDRS_PER_SESSION {
            // Forget the oldest address; the session is alive and recently seen ones win.
            let old = s.addrs.remove(0);
            self.routes.by_addr.remove(&old);
            if self.routes.publish {
                self.routes
                    .updates
                    .push(RouteUpdate::RemoveAddr(old.0, old.1));
            }
        }
        s.addrs.push((proto, peer));
        self.routes.by_addr.insert(
            (proto, peer),
            Holder {
                key,
                heard: now,
                published: now,
            },
        );
        self.publish_addr(key, proto, local, peer, now, false);
        Some(key)
    }

    fn publish_addr(
        &mut self,
        key: SessionKey,
        proto: Proto,
        local: SocketAddr,
        peer: SocketAddr,
        now: Instant,
        refresh: bool,
    ) {
        if !self.routes.publish {
            return;
        }
        if let Some(s) = self.sessions.get(key) {
            self.routes.updates.push(RouteUpdate::AddAddr {
                proto,
                local,
                peer,
                user: s.cfg.user_id.clone(),
                heard: now,
                refresh,
            });
        }
    }

    /// Forgets an address that another shard's session holds now: the directory refused it to a
    /// session of this engine, or gave it away from one.
    pub fn forget_address(&mut self, proto: Proto, peer: SocketAddr) {
        if let Some(h) = self.routes.by_addr.remove(&(proto, peer))
            && let Some(s) = self.sessions.get_mut(h.key)
        {
            s.addrs.retain(|&a| a != (proto, peer));
        }
    }

    /// Whether the STUN check `data` is signed with the ICE password of session `key`.
    fn check_integrity(&self, key: SessionKey, data: &[u8]) -> bool {
        let Some(s) = self.sessions.get(key) else {
            return false;
        };
        let (Ok(params), Ok(provider)) =
            (s.pc.local_ice_parameters(), rtc::crypto::default_provider())
        else {
            return false;
        };
        let mut message = rtc::stun::message::Message::new();
        message.unmarshal_binary(data).is_ok()
            && rtc::stun::integrity::MessageIntegrity::check(
                &mut message,
                params.password.as_bytes(),
                provider.crypto(),
            )
            .is_ok()
    }

    /// Whether this engine would take `data` from `peer`: an ICE check is its own if it names one
    /// of its sessions, whoever the address was learned for; anything else, if the address is
    /// known.
    pub fn would_route(&self, proto: Proto, peer: SocketAddr, data: &[u8]) -> bool {
        match stun_local_ufrag(data) {
            Some(u) => self.routes.by_ufrag.contains_key(u),
            None => self.routes.by_addr.contains_key(&(proto, peer)),
        }
    }

    /// Starts or stops recording route changes for [`Engine::take_route_updates`].
    pub fn publish_routes(&mut self, on: bool) {
        self.routes.set_publishing(on);
    }

    /// The route changes since the last call.
    pub fn take_route_updates(&mut self) -> Vec<RouteUpdate> {
        self.routes.take_updates()
    }
}

/// The local ICE username fragment of a STUN binding request: its USERNAME attribute is
/// `<ufrag of the receiver>:<ufrag of the sender>`.
pub(crate) fn stun_local_ufrag(data: &[u8]) -> Option<&str> {
    const MAGIC: [u8; 4] = [0x21, 0x12, 0xa4, 0x42];
    const USERNAME: u16 = 0x0006;
    // A STUN message starts with two zero bits, which no DTLS (20..=63) or RTP (128..=191) packet does.
    if data.len() < 20 || data[0] > 1 || data[4..8] != MAGIC {
        return None;
    }
    let mut at = 20;
    while at + 4 <= data.len() {
        let kind = u16::from_be_bytes([data[at], data[at + 1]]);
        let len = u16::from_be_bytes([data[at + 2], data[at + 3]]) as usize;
        let value = data.get(at + 4..at + 4 + len)?;
        if kind == USERNAME {
            return std::str::from_utf8(value).ok()?.split(':').next();
        }
        at += 4 + len.div_ceil(4) * 4;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding_request(username: &str) -> Vec<u8> {
        let mut attrs = Vec::new();
        attrs.extend_from_slice(&0x0006u16.to_be_bytes());
        attrs.extend_from_slice(&(username.len() as u16).to_be_bytes());
        attrs.extend_from_slice(username.as_bytes());
        attrs.resize(attrs.len().next_multiple_of(4), 0);
        let mut msg = vec![0x00, 0x01];
        msg.extend_from_slice(&(attrs.len() as u16).to_be_bytes());
        msg.extend_from_slice(&[0x21, 0x12, 0xa4, 0x42]);
        msg.extend_from_slice(&[7; 12]);
        msg.extend_from_slice(&attrs);
        msg
    }

    #[test]
    fn the_local_part_of_the_username_names_the_session() {
        assert_eq!(
            stun_local_ufrag(&binding_request("abcd:wxyz")),
            Some("abcd")
        );
        assert_eq!(
            stun_local_ufrag(&binding_request("abcdefg")),
            Some("abcdefg")
        );
    }

    #[test]
    fn other_traffic_is_not_stun() {
        assert_eq!(stun_local_ufrag(&[0x16; 40]), None, "DTLS");
        assert_eq!(stun_local_ufrag(&[0x80; 100]), None, "RTP");
        assert_eq!(stun_local_ufrag(&[]), None);
        let mut truncated = binding_request("abcd:wxyz");
        truncated.truncate(24);
        assert_eq!(stun_local_ufrag(&truncated), None);
    }

    #[test]
    fn a_length_that_overruns_the_packet_is_rejected() {
        let mut m = binding_request("abcd:wxyz");
        m[22] = 0xff;
        assert_eq!(stun_local_ufrag(&m), None);
    }

    #[test]
    fn a_session_gains_only_a_few_addresses_per_window() {
        let t0 = Instant::now();
        let mut budget = AddrBudget::new(t0);
        for n in 0..NEW_ADDRS_PER_WINDOW {
            assert!(budget.take(t0 + Duration::from_millis(n as u64)), "{n}");
        }
        assert!(!budget.take(t0 + Duration::from_secs(1)), "one too many");
        assert!(!budget.take(t0 + NEW_ADDR_WINDOW - Duration::from_millis(1)));
        assert!(budget.take(t0 + NEW_ADDR_WINDOW), "the next window");
    }
}
