//! Which shard serves what, for the datagrams the kernel delivers to the wrong one.
//!
//! The sockets of all shards share a port, so the kernel chooses by the sender's address which
//! shard reads a datagram, and the session it belongs to may live on another. Each shard publishes
//! the ICE username fragments of its sessions and the addresses it has learned for them; a shard
//! that is handed a datagram it cannot place looks here and passes it on.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::Instant;

use crate::engine::stun_local_ufrag;
use crate::engine::{ADDRESS_SILENCE, FastMap, Proto, RouteUpdate};

/// An address a shard has published, and whose it is.
#[derive(Debug)]
struct Holder {
    shard: usize,
    user: String,
    /// When the address was last heard, as of its last publication.
    heard: Instant,
}

#[derive(Debug, Default)]
pub(crate) struct Directory {
    ufrags: RwLock<HashMap<String, usize>>,
    addrs: RwLock<FastMap<(Proto, SocketAddr), Holder>>,
    /// Per shard, the addresses that were taken from it, for it to forget.
    lost: Mutex<HashMap<usize, Vec<(Proto, SocketAddr)>>>,
    /// How many addresses `lost` holds, so a shard with nothing to forget takes no lock.
    lost_count: AtomicUsize,
}

impl Directory {
    /// Applies the route changes `shard` made. An address that another shard holds is not taken
    /// from it unless the new session is its user's or the address has gone quiet; those the
    /// directory refused are returned, for the shard to forget. An address that did change hands
    /// is queued for the shard that lost it (see [`take_lost`](Self::take_lost)).
    pub fn apply(&self, shard: usize, updates: &[RouteUpdate]) -> Vec<(Proto, SocketAddr)> {
        let mut refused = Vec::new();
        if updates.is_empty() {
            return refused;
        }
        let mut ufrags = self.ufrags.write().unwrap_or_else(|e| e.into_inner());
        let mut addrs = self.addrs.write().unwrap_or_else(|e| e.into_inner());
        for update in updates {
            match update {
                RouteUpdate::AddUfrag(u) => {
                    ufrags.insert(u.clone(), shard);
                }
                RouteUpdate::RemoveUfrag(u) => {
                    if ufrags.get(u) == Some(&shard) {
                        ufrags.remove(u);
                    }
                }
                RouteUpdate::AddAddr {
                    proto,
                    peer,
                    user,
                    heard,
                    refresh,
                    ..
                } => {
                    let key = (*proto, *peer);
                    if let Some(h) = addrs.get(&key)
                        && h.shard != shard
                    {
                        let same_user = !h.user.is_empty() && h.user == *user;
                        if *refresh
                            || (!same_user
                                && heard.saturating_duration_since(h.heard) <= ADDRESS_SILENCE)
                        {
                            refused.push(key);
                            continue;
                        }
                        self.lost
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .entry(h.shard)
                            .or_default()
                            .push(key);
                        self.lost_count.fetch_add(1, Ordering::Relaxed);
                    }
                    addrs.insert(
                        key,
                        Holder {
                            shard,
                            user: user.clone(),
                            heard: *heard,
                        },
                    );
                }
                RouteUpdate::RemoveAddr(p, a) => {
                    if addrs.get(&(*p, *a)).is_some_and(|h| h.shard == shard) {
                        addrs.remove(&(*p, *a));
                    }
                }
            }
        }
        refused
    }

    /// The addresses another shard has taken from `shard` since it last asked.
    pub fn take_lost(&self, shard: usize) -> Vec<(Proto, SocketAddr)> {
        if self.lost_count.load(Ordering::Relaxed) == 0 {
            return Vec::new();
        }
        let taken = self
            .lost
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&shard)
            .unwrap_or_default();
        self.lost_count.fetch_sub(taken.len(), Ordering::Relaxed);
        taken
    }

    /// The shard that serves `data` from `peer`: the one whose session an ICE check names, else the
    /// one that has published the address. A check names its session itself, so it is never sent by
    /// an address that was learned for another one (a participant that rejoins from the port its
    /// last session used); and an address counts only once its shard has published it, so one
    /// that no shard adopted cannot send later sessions the wrong way.
    pub fn owner(&self, proto: Proto, peer: SocketAddr, data: &[u8]) -> Option<usize> {
        if let Some(ufrag) = stun_local_ufrag(data)
            && let Some(&s) = self
                .ufrags
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .get(ufrag)
        {
            return Some(s);
        }
        self.addrs
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(proto, peer))
            .map(|h| h.shard)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn addr(p: u16) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, 1], p))
    }

    /// `user`'s session heard from `peer` at `heard`.
    fn learned(user: &str, peer: SocketAddr, heard: Instant) -> RouteUpdate {
        RouteUpdate::AddAddr {
            proto: Proto::Udp,
            local: addr(1),
            peer,
            user: user.into(),
            heard,
            refresh: false,
        }
    }

    /// The shard that holds the address says `user` was heard from it again.
    fn heard_again(user: &str, peer: SocketAddr, heard: Instant) -> RouteUpdate {
        match learned(user, peer, heard) {
            RouteUpdate::AddAddr {
                proto,
                local,
                peer,
                user,
                heard,
                ..
            } => RouteUpdate::AddAddr {
                proto,
                local,
                peer,
                user,
                heard,
                refresh: true,
            },
            other => other,
        }
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
    fn a_check_finds_its_shard_and_what_follows_goes_the_same_way_once_the_shard_publishes_it() {
        let d = Directory::default();
        d.apply(2, &[RouteUpdate::AddUfrag("abcd".into())]);
        assert_eq!(d.owner(Proto::Udp, addr(1), &check("abcd:wxyz")), Some(2));
        d.apply(2, &[learned("u", addr(1), Instant::now())]);
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
            &[
                RouteUpdate::AddUfrag("u".into()),
                learned("u", addr(5), Instant::now()),
            ],
        );
        d.apply(
            3,
            &[
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
            &[
                RouteUpdate::RemoveAddr(Proto::Udp, addr(5)),
                RouteUpdate::RemoveUfrag("u".into()),
            ],
        );
        assert_eq!(d.owner(Proto::Udp, addr(5), &check("u:x")), None);
    }

    #[test]
    fn a_check_names_its_own_session_whatever_the_address_was_heard_for() {
        let d = Directory::default();
        d.apply(
            1,
            &[
                RouteUpdate::AddUfrag("old".into()),
                learned("u", addr(5), Instant::now()),
            ],
        );
        d.apply(2, &[RouteUpdate::AddUfrag("new".into())]);
        assert_eq!(
            d.owner(Proto::Udp, addr(5), &check("new:x")),
            Some(2),
            "a rejoin from the port the last session used"
        );
        assert_eq!(
            d.owner(Proto::Udp, addr(5), &[0x80; 40]),
            Some(1),
            "what is not a check keeps going where the address was learned"
        );
    }

    #[test]
    fn an_address_is_taken_from_the_shard_that_publishes_it_not_from_a_check() {
        let d = Directory::default();
        d.apply(1, &[RouteUpdate::AddUfrag("gone".into())]);
        assert_eq!(d.owner(Proto::Udp, addr(5), &check("gone:x")), Some(1));
        // The session ended before its shard learned the address: nothing was published for it.
        d.apply(1, &[RouteUpdate::RemoveUfrag("gone".into())]);
        d.apply(2, &[RouteUpdate::AddUfrag("next".into())]);
        assert_eq!(
            d.owner(Proto::Udp, addr(5), &check("next:x")),
            Some(2),
            "the next session checks from the same port"
        );
        assert_eq!(
            d.owner(Proto::Udp, addr(5), &[0x80; 40]),
            None,
            "an address nobody has adopted is nobody's"
        );
    }

    #[test]
    fn an_address_changes_hands_between_shards_only_for_the_same_user_or_once_quiet() {
        let d = Directory::default();
        let t0 = Instant::now();
        d.apply(1, &[learned("alice", addr(5), t0)]);

        let refused = d.apply(
            2,
            &[learned("mallory", addr(5), t0 + Duration::from_secs(5))],
        );
        assert_eq!(refused, vec![(Proto::Udp, addr(5))]);
        assert_eq!(
            d.owner(Proto::Udp, addr(5), &[0x80; 40]),
            Some(1),
            "another user's session on another shard cannot take it"
        );

        // The holder is heard again, which its shard publishes.
        d.apply(
            1,
            &[learned("alice", addr(5), t0 + Duration::from_secs(40))],
        );
        let refused = d.apply(
            2,
            &[learned("mallory", addr(5), t0 + Duration::from_secs(45))],
        );
        assert_eq!(refused.len(), 1, "alice was heard five seconds ago");

        let refused = d.apply(
            2,
            &[learned("alice", addr(5), t0 + Duration::from_secs(46))],
        );
        assert!(refused.is_empty());
        assert_eq!(
            d.owner(Proto::Udp, addr(5), &[0x80; 40]),
            Some(2),
            "a rejoin of the same user, on another shard"
        );

        let refused = d.apply(3, &[learned("bob", addr(5), t0 + Duration::from_secs(90))]);
        assert!(refused.is_empty(), "nobody has been heard from it since");
        assert_eq!(d.owner(Proto::Udp, addr(5), &[0x80; 40]), Some(3));
    }

    #[test]
    fn the_shard_that_loses_an_address_is_told_and_cannot_win_it_back_by_republishing() {
        let d = Directory::default();
        let t0 = Instant::now();
        d.apply(1, &[learned("alice", addr(5), t0)]);
        assert!(d.take_lost(1).is_empty());

        let refused = d.apply(2, &[learned("alice", addr(5), t0 + Duration::from_secs(1))]);
        assert!(refused.is_empty(), "a rejoin of the same user");
        assert_eq!(d.take_lost(1), vec![(Proto::Udp, addr(5))]);
        assert!(d.take_lost(1).is_empty(), "told once");
        assert!(d.take_lost(2).is_empty(), "the winner loses nothing");

        // Shard 1 republished the address before it was told: that moves nothing.
        let refused = d.apply(
            1,
            &[heard_again("alice", addr(5), t0 + Duration::from_secs(10))],
        );
        assert_eq!(refused, vec![(Proto::Udp, addr(5))]);
        assert_eq!(d.owner(Proto::Udp, addr(5), &[0x80; 40]), Some(2));
        assert!(d.take_lost(2).is_empty());
    }

    #[test]
    fn a_user_without_an_id_is_nobodys_same_user() {
        let d = Directory::default();
        let t0 = Instant::now();
        d.apply(1, &[learned("", addr(5), t0)]);
        let refused = d.apply(2, &[learned("", addr(5), t0 + Duration::from_secs(1))]);
        assert_eq!(refused.len(), 1);
        assert!(d.take_lost(1).is_empty());
        assert_eq!(d.owner(Proto::Udp, addr(5), &[0x80; 40]), Some(1));
    }
}
