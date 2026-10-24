//! Which shard serves what, for the datagrams the kernel delivers to the wrong one.
//!
//! The sockets of all shards share a port, so the kernel chooses by the sender's address which
//! shard reads a datagram, and the session it belongs to may live on another. Each shard publishes
//! the ICE username fragments of its sessions and the addresses it has learned for them; a shard
//! that is handed a datagram it cannot place looks here and passes it on.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::RwLock;

use crate::engine::stun_local_ufrag;
use crate::engine::{FastMap, Proto, RouteUpdate};

#[derive(Debug, Default)]
pub(crate) struct Directory {
    ufrags: RwLock<HashMap<String, usize>>,
    addrs: RwLock<FastMap<(Proto, SocketAddr), usize>>,
}

impl Directory {
    /// Applies the route changes `shard` made.
    pub fn apply(&self, shard: usize, updates: Vec<RouteUpdate>) {
        if updates.is_empty() {
            return;
        }
        let mut ufrags = self.ufrags.write().unwrap_or_else(|e| e.into_inner());
        let mut addrs = self.addrs.write().unwrap_or_else(|e| e.into_inner());
        for update in updates {
            match update {
                RouteUpdate::AddUfrag(u) => {
                    ufrags.insert(u, shard);
                }
                RouteUpdate::RemoveUfrag(u) => {
                    if ufrags.get(&u) == Some(&shard) {
                        ufrags.remove(&u);
                    }
                }
                RouteUpdate::AddAddr(p, _, a) => {
                    addrs.insert((p, a), shard);
                }
                RouteUpdate::RemoveAddr(p, a) => {
                    if addrs.get(&(p, a)) == Some(&shard) {
                        addrs.remove(&(p, a));
                    }
                }
            }
        }
    }

    /// The shard that serves `data` from `peer`: the one that knows the address, else the one whose
    /// session the ICE check names. An address found by the check is remembered at once, so what
    /// follows the check is not lost while the owner publishes it.
    pub fn owner(&self, proto: Proto, peer: SocketAddr, data: &[u8]) -> Option<usize> {
        if let Some(&s) = self
            .addrs
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(proto, peer))
        {
            return Some(s);
        }
        let ufrag = stun_local_ufrag(data)?;
        let shard = *self
            .ufrags
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(ufrag)?;
        self.addrs
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert((proto, peer), shard);
        Some(shard)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(p: u16) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, 1], p))
    }

    fn check(username: &str) -> Vec<u8> {
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
    fn a_check_finds_its_shard_and_what_follows_goes_the_same_way() {
        let d = Directory::default();
        d.apply(2, vec![RouteUpdate::AddUfrag("abcd".into())]);
        assert_eq!(d.owner(Proto::Udp, addr(1), &check("abcd:wxyz")), Some(2));
        assert_eq!(
            d.owner(Proto::Udp, addr(1), &[0x80; 40]),
            Some(2),
            "RTP from the same address"
        );
        assert_eq!(
            d.owner(Proto::Udp, addr(9), &[0x80; 40]),
            None,
            "a stranger"
        );
        assert_eq!(d.owner(Proto::Udp, addr(9), &check("nope:wxyz")), None);
    }

    #[test]
    fn a_session_ending_takes_its_entries_but_not_another_shards() {
        let d = Directory::default();
        d.apply(
            1,
            vec![
                RouteUpdate::AddUfrag("u".into()),
                RouteUpdate::AddAddr(Proto::Udp, addr(1), addr(5)),
            ],
        );
        d.apply(
            3,
            vec![
                RouteUpdate::RemoveAddr(Proto::Udp, addr(5)),
                RouteUpdate::RemoveUfrag("u".into()),
            ],
        );
        assert_eq!(
            d.owner(Proto::Udp, addr(5), &[0x80; 40]),
            Some(1),
            "shard 3 does not own it"
        );
        d.apply(
            1,
            vec![
                RouteUpdate::RemoveAddr(Proto::Udp, addr(5)),
                RouteUpdate::RemoveUfrag("u".into()),
            ],
        );
        assert_eq!(d.owner(Proto::Udp, addr(5), &check("u:x")), None);
    }
}
