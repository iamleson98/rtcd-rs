//! Which voices of a call are forwarded when more people have a microphone open than anyone can
//! listen to.
//!
//! Every open microphone is a stream to every receiver, so a call with `m` open microphones and `n`
//! receivers costs `m × n` voice streams while a person follows two or three speakers at most. The
//! large products forward only the loudest few (Meet maps the three loudest onto three fixed
//! streams, Jitsi's `route-loudest-only` forwards the three loudest sources); this is the same
//! rule as a pure policy over the audio levels (RFC 6464) the SFU already reads, so it can be
//! tested without a network.
//!
//! A call with no more open microphones than the limit is not touched: every voice is forwarded as
//! before. Beyond it, each source's loudness is a smoothed level that rises fast (a speaker is
//! heard from the first syllables) and falls slowly (a pause does not hand the slot over), and the
//! `limit` loudest sources are forwarded. A source already forwarded keeps its slot until another
//! is clearly louder, so two people of equal volume do not take turns on every packet.

use std::collections::HashMap;
use std::hash::Hash;
use std::time::{Duration, Instant};

/// Time constants of the smoothed loudness, in seconds: rising (a source gets louder) and falling.
const ATTACK: f32 = 0.1;
const RELEASE: f32 = 0.5;
/// How much louder, in loudness units (about a decibel each), a source must be than one that is
/// forwarded to take its slot.
const HYSTERESIS: f32 = 10.0;
/// A source that sent a packet this recently competes for a slot. A browser using discontinuous
/// transmission sends one packet every 400 ms while silent.
const ACTIVE_FOR: Duration = Duration::from_millis(1500);
/// A source silent for this long is forgotten (muted, or gone).
const FORGET_AFTER: Duration = Duration::from_secs(5);
/// The ranking is recomputed at most this often, unless a source that is not forwarded gets louder
/// than the quietest one that is.
const RERANK_EVERY: Duration = Duration::from_millis(100);

#[derive(Debug)]
struct Entry {
    /// Smoothed loudness, 0 (silence) to 127.
    score: f32,
    at: Instant,
    forwarded: bool,
}

/// Decides, packet by packet, whether a voice of one call is forwarded.
#[derive(Debug)]
pub struct Selector<K> {
    /// How many voices are forwarded at most; 0 forwards all.
    limit: usize,
    entries: HashMap<K, Entry>,
    /// The quietest forwarded source's score (with its hysteresis bonus) when the call has more
    /// active sources than the limit, else negative infinity.
    cutoff: f32,
    ranked_at: Option<Instant>,
}

impl<K: Copy + Eq + Hash> Selector<K> {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            entries: HashMap::new(),
            cutoff: f32::NEG_INFINITY,
            ranked_at: None,
        }
    }

    /// Records one voice packet of `key` with its audio level (-dBov, 127 is silence) and returns
    /// whether to forward it.
    pub fn hear(&mut self, key: K, level: u8, now: Instant) -> bool {
        if self.limit == 0 {
            return true;
        }
        let loudness = f32::from(127 - level.min(127));
        let free_slot = self.entries.len() < self.limit;
        let entry = self.entries.entry(key).or_insert(Entry {
            score: loudness,
            at: now,
            forwarded: free_slot,
        });
        let elapsed = now.saturating_duration_since(entry.at).as_secs_f32();
        let tau = if loudness > entry.score {
            ATTACK
        } else {
            RELEASE
        };
        entry.score += (loudness - entry.score) * (1.0 - (-elapsed / tau).exp());
        entry.at = now;

        let due = self
            .ranked_at
            .is_none_or(|t| now.saturating_duration_since(t) >= RERANK_EVERY);
        if due || (!entry.forwarded && entry.score > self.cutoff) {
            self.rank(now);
        }
        self.entries.get(&key).is_some_and(|e| e.forwarded)
    }

    /// Changes how many voices are forwarded at most (0 forwards all), taking effect from the next
    /// packet: a node under pressure asks fewer of its calls.
    pub fn set_limit(&mut self, limit: usize) {
        if self.limit == limit {
            return;
        }
        self.limit = limit;
        // Rank again at once; with no limit nobody is left out.
        self.ranked_at = None;
        if limit == 0 {
            for e in self.entries.values_mut() {
                e.forwarded = true;
            }
            self.cutoff = f32::NEG_INFINITY;
        }
    }

    /// A source left the call or was muted: its slot is free.
    pub fn remove(&mut self, key: K) {
        if self.entries.remove(&key).is_some() {
            self.ranked_at = None;
        }
    }

    fn rank(&mut self, now: Instant) {
        self.ranked_at = Some(now);
        self.entries
            .retain(|_, e| now.saturating_duration_since(e.at) < FORGET_AFTER);
        let mut active: Vec<(K, f32)> = self
            .entries
            .iter()
            .filter(|(_, e)| now.saturating_duration_since(e.at) < ACTIVE_FOR)
            .map(|(&k, e)| (k, e.score + if e.forwarded { HYSTERESIS } else { 0.0 }))
            .collect();
        if active.len() <= self.limit {
            // Nobody is left out: everything is forwarded, including a source that was silent and
            // starts again, so no first syllable is lost to a ranking not yet made.
            for e in self.entries.values_mut() {
                e.forwarded = true;
            }
            self.cutoff = f32::NEG_INFINITY;
            return;
        }
        active.sort_by(|a, b| b.1.total_cmp(&a.1));
        self.cutoff = active[self.limit - 1].1;
        let kept: Vec<K> = active[..self.limit].iter().map(|&(k, _)| k).collect();
        for (k, e) in self.entries.iter_mut() {
            e.forwarded = kept.contains(k);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STEP: Duration = Duration::from_millis(20);

    /// Feeds each `(source, level)` of `levels` one packet per 20 ms for `ms` milliseconds from
    /// `from`, returning the time reached and, for each source, how many packets were forwarded
    /// out of how many sent in the last second.
    fn feed(
        sel: &mut Selector<u32>,
        from: Instant,
        ms: u64,
        levels: &[(u32, u8)],
    ) -> (Instant, HashMap<u32, (u32, u32)>) {
        let mut now = from;
        let mut last_second: HashMap<u32, (u32, u32)> = HashMap::new();
        let steps = ms / 20;
        for i in 0..steps {
            now += STEP;
            for &(k, level) in levels {
                let forwarded = sel.hear(k, level, now);
                if i + 50 >= steps {
                    let c = last_second.entry(k).or_default();
                    c.1 += 1;
                    c.0 += u32::from(forwarded);
                }
            }
        }
        (now, last_second)
    }

    #[test]
    fn a_limit_of_zero_forwards_everything() {
        let mut sel = Selector::new(0);
        let t = Instant::now();
        assert!(sel.hear(1, 127, t));
        assert!(sel.hear(2, 10, t));
    }

    #[test]
    fn a_call_within_the_limit_is_not_touched() {
        let mut sel = Selector::new(3);
        let (_, counts) = feed(
            &mut sel,
            Instant::now(),
            3000,
            &[(1, 30), (2, 100), (3, 127)],
        );
        for k in 1..=3 {
            assert_eq!(counts[&k].0, counts[&k].1, "source {k} lost packets");
        }
    }

    #[test]
    fn the_loudest_are_forwarded_and_the_rest_are_not() {
        let mut sel = Selector::new(3);
        let loud = [(1, 25), (2, 30), (3, 35)];
        let quiet = [(4, 95), (5, 110)];
        let all: Vec<_> = loud.iter().chain(quiet.iter()).copied().collect();
        let (_, counts) = feed(&mut sel, Instant::now(), 4000, &all);
        for (k, _) in loud {
            assert_eq!(counts[&k].0, counts[&k].1, "loud source {k} was cut");
        }
        for (k, _) in quiet {
            assert_eq!(counts[&k].0, 0, "quiet source {k} was forwarded");
        }
    }

    #[test]
    fn a_new_speaker_takes_a_slot_within_a_fraction_of_a_second() {
        let mut sel = Selector::new(3);
        let start = Instant::now();
        let (t, _) = feed(
            &mut sel,
            start,
            3000,
            &[(1, 25), (2, 30), (3, 60), (4, 110)],
        );
        // Source 4 starts talking, louder than source 3.
        let mut now = t;
        let mut heard_after = None;
        for i in 0..50u64 {
            now += STEP;
            for (k, level) in [(1, 25), (2, 30), (3, 60), (4, 20)] {
                let forwarded = sel.hear(k, level, now);
                if k == 4 && forwarded && heard_after.is_none() {
                    heard_after = Some(i * 20);
                }
            }
        }
        let ms = heard_after.expect("the new speaker was never forwarded");
        assert!(ms <= 200, "the new speaker waited {ms} ms");
        let (_, counts) = feed(&mut sel, now, 2000, &[(1, 25), (2, 30), (3, 60), (4, 20)]);
        assert_eq!(counts[&4].0, counts[&4].1);
        assert_eq!(
            counts[&3].0, 0,
            "the quietest forwarded source kept its slot"
        );
    }

    #[test]
    fn equal_voices_do_not_take_turns_on_every_packet() {
        let mut sel = Selector::new(1);
        let mut now = Instant::now();
        let mut changes = 0;
        let mut last = None;
        for i in 0..500u32 {
            now += STEP;
            // Two sources a decibel or two apart, swapping which is louder.
            let (l1, l2) = if i % 7 < 3 { (40, 42) } else { (42, 40) };
            let a = sel.hear(1, l1, now);
            let b = sel.hear(2, l2, now);
            let holder = if a {
                1
            } else if b {
                2
            } else {
                0
            };
            if last.is_some() && last != Some(holder) {
                changes += 1;
            }
            last = Some(holder);
        }
        assert!(changes <= 2, "the slot changed hands {changes} times");
    }

    #[test]
    fn a_source_that_leaves_frees_its_slot() {
        let mut sel = Selector::new(1);
        let (t, counts) = feed(&mut sel, Instant::now(), 2000, &[(1, 20), (2, 60)]);
        assert_eq!(counts[&2].0, 0);
        sel.remove(1);
        let (_, counts) = feed(&mut sel, t, 1000, &[(2, 60)]);
        assert_eq!(counts[&2].0, counts[&2].1);
    }

    #[test]
    fn lowering_the_limit_drops_the_quietest_voices_at_once() {
        let mut sel = Selector::new(3);
        let t = Instant::now();
        for step in 0..30 {
            let now = t + Duration::from_millis(20 * step);
            for (key, level) in [(1u32, 20u8), (2, 30), (3, 40), (4, 80)] {
                sel.hear(key, level, now);
            }
        }
        let now = t + Duration::from_millis(700);
        assert!(
            sel.hear(3, 40, now),
            "the third loudest is forwarded under a limit of three"
        );
        sel.set_limit(1);
        let now = now + Duration::from_millis(20);
        assert!(sel.hear(1, 20, now));
        assert!(!sel.hear(2, 30, now), "only the loudest voice remains");
        assert!(!sel.hear(3, 40, now));
        sel.set_limit(0);
        assert!(
            sel.hear(4, 80, now + Duration::from_millis(20)),
            "no limit forwards everything"
        );
    }
}
