//! Finding the session a datagram belongs to.
//!
//! Every session shares the SFU's ports, so the sender's address is the only identity a media
//! packet carries. The address becomes known from the first ICE check, which names the session by
//! the ICE username fragment the SFU gave it in its answer.

use std::collections::HashMap;

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

/// A change to what this engine answers for, published so other shards can send it the datagrams
/// that reached them instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteUpdate {
    AddUfrag(String),
    RemoveUfrag(String),
    /// An address learned for a session: where it was heard (`local`, as advertised) and from whom.
    AddAddr(Proto, SocketAddr, SocketAddr),
    RemoveAddr(Proto, SocketAddr),
}

#[derive(Debug, Default)]
pub(crate) struct Routes {
    by_addr: FastMap<(Proto, SocketAddr), SessionKey>,
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
    pub(super) fn route(
        &mut self,
        proto: Proto,
        local: SocketAddr,
        peer: SocketAddr,
        data: &BytesMut,
    ) -> Option<SessionKey> {
        if let Some(&key) = self.routes.by_addr.get(&(proto, peer)) {
            return Some(key);
        }
        let ufrag = stun_local_ufrag(data)?;
        let key = *self.routes.by_ufrag.get(ufrag)?;
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
        self.routes.by_addr.insert((proto, peer), key);
        if self.routes.publish {
            self.routes
                .updates
                .push(RouteUpdate::AddAddr(proto, local, peer));
        }
        Some(key)
    }

    /// Whether this engine would take `data` from `peer`: the address is known, or the datagram
    /// is an ICE check naming one of its sessions.
    pub fn would_route(&self, proto: Proto, peer: SocketAddr, data: &[u8]) -> bool {
        self.routes.by_addr.contains_key(&(proto, peer))
            || stun_local_ufrag(data).is_some_and(|u| self.routes.by_ufrag.contains_key(u))
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
}
