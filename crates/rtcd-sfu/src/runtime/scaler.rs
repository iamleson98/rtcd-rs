//! When to start another shard and when to stop one.
//!
//! A shard is one thread, and a call lives on one shard, so the number of shards bounds how many
//! cores the media plane can use. One shard is the cheapest way to serve a small load (its sockets
//! batch best, and nothing is passed between threads), and it carries a few hundred participants;
//! past that it saturates and drops packets, where a second shard would have served the same
//! load. Waiting for an operator to notice is waiting for a class to hear dropouts, so the SFU
//! adds a shard itself when every shard it has is busy, and gives one back when it has been empty
//! for a while.
//!
//! This is only the policy: it is given what the shards report once a tick and says what to do.

use std::time::{Duration, Instant};

/// How often the policy is consulted.
pub(crate) const TICK: Duration = Duration::from_secs(1);

/// When shards are added and stopped. The defaults are what the SFU runs with; the fields are
/// public so that a test can use milliseconds instead of minutes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScalingPolicy {
    /// How often the shards are looked at.
    pub tick: Duration,
    /// A shard this busy (the fraction of its time spent working rather than waiting) counts as
    /// hot. A single shard measured at 74 % of a core was already dropping 3 % of the packets at
    /// 600 participants, so the margin is wide.
    pub hot: f32,
    /// How many ticks in a row every shard must be hot before another is added: a burst of joins
    /// (handshakes) is not load that stays.
    pub hot_ticks: u32,
    /// How long a shard without sessions is kept before it is stopped. A class that ends is often
    /// followed by another, and a shard that is there costs nothing while it waits.
    pub empty_for: Duration,
    /// How many sessions the emptiest shard may already hold before a new call gets a shard of its
    /// own (while there is room for one). A call cannot move once it has started, and classes
    /// start together when a bell rings, so waiting to see a shard busy is waiting until every
    /// call has been placed on it: the count is what can be known when the call arrives. Measured,
    /// one shard held 300 participants at a third of a core and 600 at over two thirds, with the
    /// first dropped packets. So a shard is filled to the load it was measured to carry comfortably
    /// before another is started: spreading a burst over every shard there is room for, tried at
    /// 150, delivered no more at 900 participants and spent a fifth more CPU.
    pub sessions_per_shard: usize,
    /// How many sessions a call counts for, at least, while the shards are compared: a class
    /// starts with its first participant and fills over the next seconds, and when a bell starts
    /// thirty classes at once each shard has admitted a handful of sessions by the time the last
    /// call is placed. Counting what a call will bring spreads them. A call with more sessions
    /// counts for what it has.
    pub expected_call_size: usize,
}

impl Default for ScalingPolicy {
    fn default() -> Self {
        Self {
            tick: TICK,
            hot: HOT,
            hot_ticks: HOT_TICKS,
            empty_for: EMPTY_FOR,
            sessions_per_shard: SESSIONS_PER_SHARD,
            expected_call_size: EXPECTED_CALL_SIZE,
        }
    }
}

const HOT: f32 = 0.5;
const HOT_TICKS: u32 = 3;
const EMPTY_FOR: Duration = Duration::from_secs(120);
const SESSIONS_PER_SHARD: usize = 300;
const EXPECTED_CALL_SIZE: usize = 20;

/// What one shard reports.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ShardLoad {
    pub slot: usize,
    /// Busy fraction over the last second, 0 to 1.
    pub busy: f32,
    pub sessions: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Step {
    Hold,
    Add,
    Retire(usize),
}

#[derive(Debug)]
pub(crate) struct Scaler {
    policy: ScalingPolicy,
    /// Ticks in a row in which every shard was hot.
    hot: u32,
    /// Since when each slot has had no sessions.
    empty_since: Vec<Option<Instant>>,
}

impl Scaler {
    pub fn new(slots: usize, policy: ScalingPolicy) -> Self {
        Self {
            policy,
            hot: 0,
            empty_since: vec![None; slots],
        }
    }

    /// What to do given the running shards, at most `max` of them.
    ///
    /// A shard is added only when all of them are hot: one that has room takes the next call, and
    /// a hot shard whose calls are long cannot hand them over anyway, so more shards than the
    /// busy ones need would only be threads that pass datagrams to each other.
    pub fn step(&mut self, now: Instant, shards: &[ShardLoad], max: usize) -> Step {
        for (slot, since) in self.empty_since.iter_mut().enumerate() {
            let empty = shards.iter().any(|s| s.slot == slot && s.sessions == 0);
            if empty {
                since.get_or_insert(now);
            } else {
                *since = None;
            }
        }

        let all_hot = !shards.is_empty() && shards.iter().all(|s| s.busy >= self.policy.hot);
        self.hot = if all_hot { self.hot + 1 } else { 0 };
        if self.hot >= self.policy.hot_ticks && shards.len() < max {
            self.hot = 0;
            return Step::Add;
        }

        // Give one back when it has been empty long enough and the rest are not hot: stopping
        // one just before the load needs it again would only be a restart.
        if shards.len() > 1 {
            let hot = self.policy.hot;
            let rest_cool = |slot: usize| {
                shards
                    .iter()
                    .filter(|s| s.slot != slot)
                    .all(|s| s.busy < hot)
            };
            let idle = shards
                .iter()
                .filter(|s| s.sessions == 0)
                .filter(|s| {
                    self.empty_since
                        .get(s.slot)
                        .copied()
                        .flatten()
                        .is_some_and(|t| now.saturating_duration_since(t) >= self.policy.empty_for)
                })
                .map(|s| s.slot)
                .filter(|&slot| rest_cool(slot))
                .max();
            if let Some(slot) = idle {
                if let Some(since) = self.empty_since.get_mut(slot) {
                    *since = None;
                }
                return Step::Retire(slot);
            }
        }
        Step::Hold
    }
}

/// A shard at this fraction of its time working for [`PRESSURE_FOR`] has no room left.
const PRESSURE_AT: f32 = 0.9;
const PRESSURE_FOR: u32 = 2;
/// And has room again below this fraction for [`RELEASE_FOR`]: long, because what it shed is what
/// brought it down, and giving it back at once would bring the shard straight back up.
const PRESSURE_RELEASE: f32 = 0.7;
const RELEASE_FOR: u32 = 30;
/// A shard this close to all of its time working for [`SATURATED_FOR`] is at the edge of a cliff:
/// measured, a thread at 97 % lost 0.2 % of its packets and at 100 % a third, because losses are
/// asked for again and the thread has no time left for them. The sessions it has are all it
/// should serve, and it must ease off before it reaches 100 %.
const SATURATED_AT: f32 = 0.95;
const SATURATED_FOR: u32 = 3;
/// It leaves that state after this many seconds under [`PRESSURE_AT`].
const SATURATED_RELEASE_FOR: u32 = 10;

/// How much a shard is asked for compared with what its thread can do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Level {
    /// Room to spare.
    #[default]
    Ok,
    /// No room left: its receivers are held to the small video layers and its calls forward two
    /// voices at most.
    Shedding,
    /// Shedding is not enough: the thread is at its limit and the shard refuses new sessions, so
    /// the ones it has keep their quality instead of everyone losing packets.
    Saturated,
}

/// The [`Level`] of one shard, from the busy fraction it reports once a second. A call lives on
/// one shard and cannot move, so a shard past its limit drops packets for every call on it at
/// once while another shard may be idle: it has to ask less of what it carries, and past that
/// take no more.
#[derive(Debug, Default)]
pub(crate) struct Pressure {
    /// Readings in a row at or over [`PRESSURE_AT`], [`SATURATED_AT`], under [`PRESSURE_AT`] and
    /// under [`PRESSURE_RELEASE`].
    hot: u32,
    full: u32,
    below: u32,
    calm: u32,
    level: Level,
}

impl Pressure {
    /// Takes one more reading and returns the shard's level.
    pub fn update(&mut self, busy: f32) -> Level {
        let run = |count: u32, holds: bool| if holds { count + 1 } else { 0 };
        self.hot = run(self.hot, busy >= PRESSURE_AT);
        self.full = run(self.full, busy >= SATURATED_AT);
        self.below = run(self.below, busy < PRESSURE_AT);
        self.calm = run(self.calm, busy < PRESSURE_RELEASE);
        self.level = if self.full >= SATURATED_FOR {
            Level::Saturated
        } else {
            match self.level {
                Level::Ok if self.hot >= PRESSURE_FOR => Level::Shedding,
                // Out of the harder level once clearly under the shedding mark for a while; it
                // keeps shedding, since what it sheds is what holds it down.
                Level::Saturated if self.below >= SATURATED_RELEASE_FOR => Level::Shedding,
                Level::Shedding if self.calm >= RELEASE_FOR => Level::Ok,
                level => level,
            }
        };
        self.level
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load(slot: usize, busy: f32, sessions: usize) -> ShardLoad {
        ShardLoad {
            slot,
            busy,
            sessions,
        }
    }

    fn ticks(
        scaler: &mut Scaler,
        start: Instant,
        from: u32,
        n: u32,
        shards: &[ShardLoad],
        max: usize,
    ) -> Vec<Step> {
        (from..from + n)
            .map(|i| scaler.step(start + TICK * i, shards, max))
            .collect()
    }

    #[test]
    fn a_shard_is_added_when_every_shard_has_been_hot_for_a_few_ticks() {
        let t = Instant::now();
        let mut s = Scaler::new(4, ScalingPolicy::default());
        let hot = [load(0, 0.8, 300)];
        assert_eq!(
            ticks(&mut s, t, 0, 3, &hot, 3),
            [Step::Hold, Step::Hold, Step::Add]
        );
    }

    #[test]
    fn a_burst_is_not_load() {
        let t = Instant::now();
        let mut s = Scaler::new(4, ScalingPolicy::default());
        let hot = [load(0, 0.9, 300)];
        let cool = [load(0, 0.2, 300)];
        for round in 0..5 {
            let at = round * 3;
            assert_eq!(ticks(&mut s, t, at, 2, &hot, 3), [Step::Hold, Step::Hold]);
            assert_eq!(ticks(&mut s, t, at + 2, 1, &cool, 3), [Step::Hold]);
        }
    }

    #[test]
    fn a_shard_with_room_means_no_new_shard() {
        let t = Instant::now();
        let mut s = Scaler::new(4, ScalingPolicy::default());
        // The first is hot with long calls; the second has only just started.
        let shards = [load(0, 0.95, 300), load(1, 0.2, 30)];
        assert!(
            ticks(&mut s, t, 0, 20, &shards, 3)
                .iter()
                .all(|s| *s == Step::Hold)
        );
    }

    #[test]
    fn it_never_goes_past_the_most_it_may_have() {
        let t = Instant::now();
        let mut s = Scaler::new(4, ScalingPolicy::default());
        let shards = [load(0, 0.9, 300), load(1, 0.9, 300)];
        assert!(
            ticks(&mut s, t, 0, 20, &shards, 2)
                .iter()
                .all(|s| *s == Step::Hold)
        );
    }

    #[test]
    fn after_adding_it_waits_for_the_new_shard_to_show_whether_it_is_needed() {
        let t = Instant::now();
        let mut s = Scaler::new(4, ScalingPolicy::default());
        let one = [load(0, 0.8, 300)];
        assert_eq!(ticks(&mut s, t, 0, 3, &one, 3)[2], Step::Add);
        // The next ticks see the new one idle: nothing more is added.
        let two = [load(0, 0.8, 300), load(1, 0.0, 0)];
        assert!(
            ticks(&mut s, t, 3, 10, &two, 3)
                .iter()
                .all(|s| *s == Step::Hold)
        );
    }

    #[test]
    fn a_shard_empty_for_long_enough_is_given_back_and_the_last_never_is() {
        let t = Instant::now();
        let mut s = Scaler::new(4, ScalingPolicy::default());
        let shards = [load(0, 0.1, 30), load(1, 0.0, 0)];
        assert_eq!(s.step(t, &shards, 3), Step::Hold);
        assert_eq!(s.step(t + EMPTY_FOR / 2, &shards, 3), Step::Hold);
        assert_eq!(s.step(t + EMPTY_FOR, &shards, 3), Step::Retire(1));

        let alone = [load(0, 0.0, 0)];
        assert_eq!(s.step(t + EMPTY_FOR * 4, &alone, 3), Step::Hold);
    }

    #[test]
    fn a_session_joining_restarts_the_wait() {
        let t = Instant::now();
        let mut s = Scaler::new(4, ScalingPolicy::default());
        let empty = [load(0, 0.1, 30), load(1, 0.0, 0)];
        let used = [load(0, 0.1, 30), load(1, 0.1, 5)];
        s.step(t, &empty, 3);
        s.step(t + EMPTY_FOR / 2, &used, 3);
        assert_eq!(s.step(t + EMPTY_FOR, &empty, 3), Step::Hold);
        assert_eq!(
            s.step(t + EMPTY_FOR + EMPTY_FOR, &empty, 3),
            Step::Retire(1)
        );
    }

    #[test]
    fn an_empty_shard_stays_while_the_others_are_hot() {
        let t = Instant::now();
        let mut s = Scaler::new(4, ScalingPolicy::default());
        let shards = [load(0, 0.9, 300), load(1, 0.0, 0)];
        s.step(t, &shards, 3);
        assert_eq!(s.step(t + EMPTY_FOR * 2, &shards, 3), Step::Hold);
    }

    #[test]
    fn a_shard_with_no_room_for_a_few_seconds_is_shedding_and_stays_so_for_a_while() {
        let mut p = Pressure::default();
        assert_eq!(p.update(0.95), Level::Ok, "one hot second is a burst");
        assert_eq!(p.update(0.95), Level::Shedding, "two are not");
        for _ in 0..29 {
            assert_eq!(
                p.update(0.3),
                Level::Shedding,
                "what it shed is what brought it down"
            );
        }
        assert_eq!(p.update(0.3), Level::Ok, "thirty calm seconds give it back");
    }

    #[test]
    fn a_shard_hovering_between_the_marks_neither_enters_nor_leaves() {
        let mut p = Pressure::default();
        for _ in 0..60 {
            assert_eq!(p.update(0.8), Level::Ok);
        }
        p.update(0.95);
        assert_eq!(p.update(0.95), Level::Shedding);
        for _ in 0..60 {
            assert_eq!(p.update(0.8), Level::Shedding);
        }
    }

    #[test]
    fn a_thread_at_its_limit_refuses_new_sessions_until_it_has_eased_for_a_while() {
        let mut p = Pressure::default();
        for _ in 0..2 {
            p.update(1.0);
        }
        assert_eq!(
            p.update(1.0),
            Level::Saturated,
            "three seconds at the limit"
        );
        // Eases a little, still hot: not released.
        for _ in 0..30 {
            assert_eq!(p.update(0.93), Level::Saturated);
        }
        for _ in 0..9 {
            assert_eq!(p.update(0.8), Level::Saturated);
        }
        assert_eq!(
            p.update(0.8),
            Level::Shedding,
            "ten seconds under the shedding mark"
        );
        assert_eq!(
            p.update(0.99),
            Level::Shedding,
            "a single reading at the limit is a burst"
        );
    }
}
