//! The node's own limits on taking more people.
//!
//! A shard at its limit refuses new sessions by itself (see `scaler::Level`). The node as a whole
//! has limits no shard sees: it can run out of memory with every thread idle, and its card can
//! refuse datagrams at a rate no single thread caused. Past them nothing slows down first. The
//! kernel kills the process, and every call on the node ends at once. A node that is nearly there
//! takes nobody new, and sheds what it carries, so that the people it has keep their calls.
//!
//! [`Admission`] is the decision (so it can be tested without a machine); the thread that feeds it
//! readings once every [`TICK`] is in `runtime/mod.rs`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use crate::hostmem::Memory;

/// How often the node looks at itself. Memory can double in seconds when a thread falls behind.
pub(crate) const TICK: std::time::Duration = std::time::Duration::from_millis(250);

/// What a node is asked to stay under.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Limits {
    /// The share of the memory the process may hold at which newcomers are refused and the node
    /// sheds (0: not at all). It is released ten points lower.
    pub memory_soft: f64,
    /// The share at which nobody is accepted, not even a class's host, and calls forward their
    /// loudest voice only (0: not at all). It is released ten points lower.
    pub memory_hard: f64,
    /// Whether newcomers are refused while the kernel refuses datagrams.
    pub on_refusals: bool,
    /// The share of the card's ceiling (bits or packets a second, whichever is nearer; see
    /// `capacity`) at which the node refuses newcomers, and the share at which it sheds (small
    /// video layers, fewer voices). 0 turns each off. A call cannot move and keeps growing as
    /// people rejoin or arrive late, so the node that is near its card must stop taking them
    /// before the kernel starts refusing. It sheds only when what its people ask for is past what
    /// the card takes (a ceiling learned is the rate the kernel last took whole, so 110 % of it is past the wall): a
    /// demand a little over loses a little to the kernel, which a receiver recovers, and cutting
    /// everyone's video to its small layer for that would cost far more. While it sheds the rate
    /// is lower than the demand, so the node stays closed to newcomers for as long.
    pub egress_refuse: f64,
    pub egress_shed: f64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            memory_soft: 0.70,
            memory_hard: 0.85,
            on_refusals: true,
            egress_refuse: 0.94,
            egress_shed: 1.10,
        }
    }
}

/// Where the node reads its memory from: the system's, or a source of a test's own (nothing can
/// make a machine use 90 % of its memory on demand).
#[derive(Clone)]
pub struct MemorySource(Arc<dyn Fn() -> Option<Memory> + Send + Sync>);

impl MemorySource {
    pub fn new(read: impl Fn() -> Option<Memory> + Send + Sync + 'static) -> Self {
        Self(Arc::new(read))
    }

    pub(crate) fn read(&self) -> Memory {
        (self.0)().unwrap_or_default()
    }
}

impl Default for MemorySource {
    fn default() -> Self {
        Self::new(crate::hostmem::read)
    }
}

impl std::fmt::Debug for MemorySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MemorySource")
    }
}

/// How far a node's limits are reached, in bits (what a [`Verdict`] is stored as).
const MEMORY: u8 = 1;
const MEMORY_HARD: u8 = 2;
const REFUSALS: u8 = 4;
const EGRESS: u8 = 8;
const EGRESS_SHED: u8 = 16;

/// What the node's limits say about taking more people, and about what it carries.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Verdict(u8);

impl Verdict {
    /// A newcomer to a call, who is not its host and not coming back, is refused.
    pub fn refuses_newcomers(self) -> bool {
        self.0 != 0
    }

    /// Nobody is accepted: not a host, not a participant coming back.
    pub fn refuses_everyone(self) -> bool {
        self.0 & MEMORY_HARD != 0
    }

    /// The node holds receivers to the small video layers and calls to few voices.
    pub fn sheds(self) -> bool {
        self.0 & (MEMORY | MEMORY_HARD | EGRESS_SHED) != 0
    }

    /// The node holds calls to their loudest voice.
    pub fn chokes(self) -> bool {
        self.0 & MEMORY_HARD != 0
    }

    /// What closes the node, for logs and metrics ("" when nothing does).
    pub fn reason(self) -> &'static str {
        match self.0 {
            0 => "",
            v if v & MEMORY_HARD != 0 => "memory_hard",
            v if v & MEMORY != 0 => "memory",
            v if v & (EGRESS | EGRESS_SHED) != 0 => "egress",
            _ => "refusals",
        }
    }
}

/// The latest verdict, shared with the threads that read it.
#[derive(Debug, Default)]
pub(crate) struct Shared(AtomicU8);

impl Shared {
    pub fn get(&self) -> Verdict {
        Verdict(self.0.load(Ordering::Relaxed))
    }

    pub fn set(&self, v: Verdict) {
        self.0.store(v.0, Ordering::Relaxed);
    }
}

/// What the node reads of itself.
#[derive(Debug, Clone, Copy, Default)]
pub struct Reading {
    /// What the process holds in RAM, and what it may hold (0: not known).
    pub rss: u64,
    pub memory: u64,
    /// The kernel is refusing datagrams.
    pub refusing: bool,
    /// The send rate in bits and packets per second, and the ceilings of the card for them (0:
    /// not known).
    pub bps: u64,
    pub pps: u64,
    pub ceiling_bps: u64,
    pub ceiling_pps: u64,
    /// Which of those ceilings are facts (the operator's, or a wall the card showed) and not a
    /// probe: the node sheds only against a fact.
    pub proven_bps: bool,
    pub proven_pps: bool,
}

/// The decision, with the memory it keeps so that a reading hovering at a limit does not open and
/// close the node every quarter second.
#[derive(Debug, Default)]
pub struct Admission {
    limits: Limits,
    verdict: Verdict,
    /// Updates so far, to time the hold of the shed.
    ticks: u64,
    shed: Shed,
}

/// The shed of quality near the card's ceiling, which is held (see [`Admission::update`]).
#[derive(Debug, Default)]
struct Shed {
    on: bool,
    until: u64,
    hold: u64,
    ended: u64,
    /// Newcomers stay refused until then: the rate was low because of the shed, not because the
    /// people had gone.
    closed_until: u64,
}

/// The share of a memory limit below which a limit that was reached is released.
const RELEASE: f64 = 0.10;
/// The same for the refusal of newcomers near the card's ceiling: the node sends close to it by
/// design, so a release ten points lower would leave a tenth of the card unused.
const EGRESS_RELEASE: f64 = 0.05;
/// The shed is released only when the rate is this far under its mark, because it is the shed that
/// lowered the rate.
const SHED_RELEASE: f64 = 0.15;
/// How long a shed lasts at least, twice as long when it comes back within [`SHED_AGAIN`] of its
/// end, up to [`SHED_MAX`]: a demand just over the ceiling does not switch every layer every few
/// seconds. In updates ([`TICK`]).
const SHED_HOLD: u64 = 20 * 4;
const SHED_AGAIN: u64 = 120 * 4;
const SHED_MAX: u64 = 300 * 4;
/// How long the node stays closed to newcomers after a shed, in updates.
const SHED_COOL: u64 = 30 * 4;

impl Admission {
    pub fn new(limits: Limits) -> Self {
        Self {
            limits,
            verdict: Verdict::default(),
            ticks: 0,
            shed: Shed::default(),
        }
    }

    pub fn verdict(&self) -> Verdict {
        self.verdict
    }

    pub fn update(&mut self, r: Reading) -> Verdict {
        let held = self.verdict.0;
        let mut v = 0;
        let share = if r.memory > 0 {
            r.rss as f64 / r.memory as f64
        } else {
            0.0
        };
        let level = |limit: f64, bit: u8| {
            if limit <= 0.0 {
                0
            } else if share >= limit || (held & bit != 0 && share >= limit - RELEASE) {
                bit
            } else {
                0
            }
        };
        v |= level(self.limits.memory_soft, MEMORY);
        v |= level(self.limits.memory_hard, MEMORY_HARD);
        self.ticks += 1;
        let share = |rate: u64, ceiling: u64| {
            if ceiling == 0 {
                0.0
            } else {
                rate as f64 / ceiling as f64
            }
        };
        let sent = share(r.bps, r.ceiling_bps).max(share(r.pps, r.ceiling_pps));
        let refuse = self.limits.egress_refuse;
        if refuse > 0.0
            && (sent >= refuse || (held & EGRESS != 0 && sent >= refuse - EGRESS_RELEASE))
        {
            v |= EGRESS;
        }
        let known = share(if r.proven_bps { r.bps } else { 0 }, r.ceiling_bps)
            .max(share(if r.proven_pps { r.pps } else { 0 }, r.ceiling_pps));
        if self.limits.egress_shed > 0.0 && self.shed(known, r.refusing) {
            v |= EGRESS_SHED | EGRESS;
        }
        if self.ticks < self.shed.closed_until {
            v |= EGRESS;
        }
        if self.limits.on_refusals && r.refusing {
            v |= REFUSALS;
        }
        self.verdict = Verdict(v);
        self.verdict
    }

    /// Whether the node sheds at this share of its card's ceiling: from `egress_shed` of it while
    /// the kernel refuses, for at least [`SHED_HOLD`] and until the rate is [`SHED_RELEASE`] under
    /// the mark. A node over a ceiling the kernel does not refuse at has a ceiling that is too low,
    /// which the estimator finds out.
    fn shed(&mut self, sent: f64, refusing: bool) -> bool {
        let mark = self.limits.egress_shed;
        let t = self.ticks;
        let sh = &mut self.shed;
        if sent >= mark && (sh.on || refusing) {
            if !sh.on {
                let soon = sh.ended > 0 && t.saturating_sub(sh.ended) < SHED_AGAIN;
                sh.hold = if soon {
                    (sh.hold * 2).min(SHED_MAX)
                } else {
                    SHED_HOLD
                };
                sh.on = true;
            }
            sh.until = t + sh.hold;
        } else if sh.on && t >= sh.until && sent < mark - SHED_RELEASE {
            sh.on = false;
            sh.ended = t;
            sh.closed_until = t + SHED_COOL;
        }
        sh.on
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1 << 30;

    fn memory(rss_gib: f64) -> Reading {
        Reading {
            rss: (rss_gib * GIB as f64) as u64,
            memory: 3 * GIB,
            ..Default::default()
        }
    }

    #[test]
    fn a_node_with_room_takes_everyone() {
        let mut a = Admission::default();
        assert!(!a.update(memory(0.5)).refuses_newcomers());
        assert!(!a.update(Reading::default()).refuses_newcomers());
    }

    #[test]
    fn newcomers_are_refused_from_seventy_percent_of_the_memory_and_hosts_from_eighty_five() {
        let mut a = Admission::new(Limits::default());
        let v = a.update(memory(2.0));
        assert!(!v.refuses_newcomers(), "2.0 of 3 GiB is 67 %");
        let v = a.update(memory(2.2));
        assert!(v.refuses_newcomers() && !v.refuses_everyone() && v.sheds() && !v.chokes());
        assert_eq!(v.reason(), "memory");
        let v = a.update(memory(2.6));
        assert!(v.refuses_everyone() && v.chokes());
        assert_eq!(v.reason(), "memory_hard");
    }

    #[test]
    fn a_limit_is_released_ten_points_lower_not_at_the_mark() {
        let mut a = Admission::default();
        a.update(memory(2.2));
        assert!(
            a.update(memory(1.9)).refuses_newcomers(),
            "63 %: above 60, still held"
        );
        assert!(!a.update(memory(1.7)).refuses_newcomers(), "57 %: released");
        a.update(memory(2.6));
        assert!(
            a.update(memory(2.4)).refuses_everyone(),
            "80 %: above 75, hosts still refused"
        );
        let v = a.update(memory(2.2));
        assert!(
            !v.refuses_everyone() && v.refuses_newcomers(),
            "73 %: hosts again, newcomers not yet"
        );
    }

    #[test]
    fn a_limit_of_zero_is_not_enforced_and_an_unknown_memory_never_closes_the_node() {
        let mut a = Admission::new(Limits {
            memory_soft: 0.0,
            memory_hard: 0.0,
            on_refusals: true,
            ..Limits::default()
        });
        assert!(!a.update(memory(2.99)).refuses_newcomers());
        let mut a = Admission::default();
        assert!(
            !a.update(Reading {
                rss: 99 * GIB,
                memory: 0,
                ..Default::default()
            })
            .refuses_newcomers()
        );
    }

    #[test]
    fn the_kernels_refusals_close_the_node_to_newcomers_but_not_to_hosts() {
        let mut a = Admission::default();
        let v = a.update(Reading {
            refusing: true,
            ..Default::default()
        });
        assert!(v.refuses_newcomers() && !v.refuses_everyone() && !v.sheds());
        assert_eq!(v.reason(), "refusals");
        assert!(!a.update(Reading::default()).refuses_newcomers());
        let mut off = Admission::new(Limits {
            on_refusals: false,
            ..Limits::default()
        });
        assert!(
            !off.update(Reading {
                refusing: true,
                ..Default::default()
            })
            .refuses_newcomers()
        );
    }

    #[test]
    fn the_verdict_is_shared_between_threads() {
        let s = Shared::default();
        assert!(!s.get().refuses_newcomers());
        let mut a = Admission::default();
        s.set(a.update(memory(2.7)));
        assert!(s.get().refuses_everyone());
    }

    fn card(bps: u64, pps: u64) -> Reading {
        Reading {
            bps,
            pps,
            ceiling_bps: 850_000_000,
            ceiling_pps: 117_000,
            proven_bps: true,
            proven_pps: true,
            ..Default::default()
        }
    }

    fn mbit(m: u64, pps: u64) -> Reading {
        Reading {
            refusing: m > 850,
            ..card(m * 1_000_000, pps)
        }
    }

    #[test]
    fn a_node_takes_newcomers_almost_to_its_ceiling_and_not_beyond() {
        let mut a = Admission::default();
        assert!(!a.update(mbit(700, 50_000)).refuses_newcomers(), "82 %");
        assert!(!a.update(mbit(790, 50_000)).refuses_newcomers(), "93 %");
        let v = a.update(mbit(810, 50_000));
        assert!(
            v.refuses_newcomers() && !v.refuses_everyone() && !v.sheds(),
            "95 %"
        );
        assert_eq!(v.reason(), "egress");
        assert!(
            a.update(mbit(770, 50_000)).refuses_newcomers(),
            "91 %: above 89, held"
        );
        assert!(!a.update(mbit(740, 50_000)).refuses_newcomers(), "87 %");
    }

    #[test]
    fn the_nearer_of_the_two_ceilings_decides() {
        let mut a = Admission::default();
        // Far from the bits, at the packets: voice.
        let v = a.update(mbit(90, 112_000));
        assert!(v.refuses_newcomers(), "96 % of the packets");
        assert!(!a.update(mbit(90, 80_000)).refuses_newcomers(), "68 %");
    }

    #[test]
    fn a_node_sheds_only_at_the_ceiling_and_keeps_shedding_for_a_while() {
        let mut a = Admission::default();
        assert!(
            !a.update(mbit(850, 50_000)).sheds(),
            "100 %: the wall itself"
        );
        assert!(!a.update(mbit(920, 50_000)).sheds(), "108 %");
        let v = a.update(mbit(950, 50_000));
        assert!(v.sheds() && !v.chokes() && v.refuses_newcomers(), "112 %");
        // The shed took the rate down; the node holds it for twenty seconds all the same, and
        // stays closed to newcomers: the demand is still there.
        for _ in 0..79 {
            let v = a.update(mbit(500, 50_000));
            assert!(v.sheds() && v.refuses_newcomers());
        }
        let v = a.update(mbit(500, 50_000));
        assert!(
            !v.sheds() && v.refuses_newcomers(),
            "after twenty seconds, still closed"
        );
        for _ in 0..119 {
            assert!(a.update(mbit(500, 50_000)).refuses_newcomers());
        }
        assert!(
            !a.update(mbit(500, 50_000)).refuses_newcomers(),
            "after thirty more"
        );
        // Not released at 98 %: the shed is what lowered it. It is at 95 %.
        let mut a = Admission::default();
        a.update(mbit(950, 50_000));
        for _ in 0..100 {
            assert!(
                a.update(mbit(830, 50_000)).sheds(),
                "98 %: over the release mark"
            );
        }
        assert!(!a.update(mbit(790, 50_000)).sheds(), "93 %");
    }

    #[test]
    fn a_shed_that_comes_back_is_held_longer() {
        let mut a = Admission::default();
        let hold = |a: &mut Admission| {
            a.update(mbit(950, 50_000));
            let mut n = 0;
            while a.update(mbit(300, 50_000)).sheds() {
                n += 1;
            }
            n
        };
        let first = hold(&mut a);
        let second = hold(&mut a);
        assert!(second >= 2 * first - 2, "{first} then {second} updates");
    }

    #[test]
    fn a_node_with_no_ceiling_known_has_no_card_to_be_near() {
        let mut a = Admission::default();
        let v = a.update(Reading {
            bps: 5_000_000_000,
            pps: 900_000,
            ..Default::default()
        });
        assert!(!v.refuses_newcomers() && !v.sheds());
        let mut off = Admission::new(Limits {
            egress_refuse: 0.0,
            egress_shed: 0.0,
            on_refusals: false,
            ..Limits::default()
        });
        assert!(!off.update(mbit(900, 200_000)).refuses_newcomers());
    }
}
