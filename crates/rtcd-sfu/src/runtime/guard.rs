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
    /// Whether newcomers are refused while the kernel refuses datagrams or the node sends at the
    /// ceiling it learned.
    pub on_refusals: bool,
    /// What the node's card is planned to carry, in bits per second (0: not said). With it, the
    /// node refuses newcomers once it sends `egress_refuse` of it, and sheds (small video layers,
    /// fewer voices) from `egress_shed`; each is released ten points lower, and 0 turns it off. A
    /// call cannot move and keeps growing as people rejoin or arrive late, so the node that is
    /// near its card must stop taking them before the kernel starts refusing.
    pub egress_capacity_bps: u64,
    pub egress_refuse: f64,
    pub egress_shed: f64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            memory_soft: 0.70,
            memory_hard: 0.85,
            on_refusals: true,
            egress_capacity_bps: 0,
            egress_refuse: 0.70,
            egress_shed: 0.90,
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
    /// The send rate in bits and packets per second, and the ceilings the node learned for them
    /// (0: none yet).
    pub bps: u64,
    pub pps: u64,
    pub ceiling_bps: u64,
    pub ceiling_pps: u64,
}

/// The decision, with the memory it keeps so that a reading hovering at a limit does not open and
/// close the node every quarter second.
#[derive(Debug, Default)]
pub struct Admission {
    limits: Limits,
    verdict: Verdict,
}

/// The share of the limit below which a limit that was reached is released.
const RELEASE: f64 = 0.10;
/// The share of the learned ceiling at which the node counts as sending at it, and below which it
/// does not any more.
const AT_CEILING: f64 = 1.0;
const OFF_CEILING: f64 = 0.9;

impl Admission {
    pub fn new(limits: Limits) -> Self {
        Self {
            limits,
            verdict: Verdict::default(),
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
        if self.limits.egress_capacity_bps > 0 {
            let capacity = self.limits.egress_capacity_bps as f64;
            let sent = r.bps as f64 / capacity;
            let level = |limit: f64, bit: u8| {
                if limit <= 0.0 {
                    0
                } else if sent >= limit || (held & bit != 0 && sent >= limit - RELEASE) {
                    bit
                } else {
                    0
                }
            };
            v |= level(self.limits.egress_refuse, EGRESS);
            v |= level(self.limits.egress_shed, EGRESS_SHED);
        }
        if self.limits.on_refusals {
            let at = |rate: u64, ceiling: u64| {
                if ceiling == 0 {
                    return false;
                }
                let f = rate as f64 / ceiling as f64;
                f >= AT_CEILING || (held & REFUSALS != 0 && f >= OFF_CEILING)
            };
            if r.refusing || at(r.bps, r.ceiling_bps) || at(r.pps, r.ceiling_pps) {
                v |= REFUSALS;
            }
        }
        self.verdict = Verdict(v);
        self.verdict
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
    fn sending_at_the_learned_ceiling_closes_the_node_until_it_is_ten_percent_under() {
        let mut a = Admission::default();
        let at = |bps| Reading {
            bps,
            ceiling_bps: 800_000_000,
            ..Default::default()
        };
        assert!(!a.update(at(700_000_000)).refuses_newcomers());
        assert!(a.update(at(800_000_000)).refuses_newcomers());
        assert!(a.update(at(740_000_000)).refuses_newcomers(), "92 %: held");
        assert!(!a.update(at(700_000_000)).refuses_newcomers());
        let none = Reading {
            bps: 900_000_000,
            ..Default::default()
        };
        assert!(
            !a.update(none).refuses_newcomers(),
            "no ceiling learned, nothing to be at"
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

    #[test]
    fn a_node_near_its_card_takes_no_newcomer_and_sheds_nearer_still() {
        let mut a = Admission::new(Limits {
            egress_capacity_bps: 1_000_000_000,
            ..Limits::default()
        });
        let at = |mbps: u64| Reading {
            bps: mbps * 1_000_000,
            ..Default::default()
        };
        assert!(!a.update(at(650)).refuses_newcomers(), "65 %");
        let v = a.update(at(720));
        assert!(v.refuses_newcomers() && !v.refuses_everyone() && !v.sheds());
        assert_eq!(v.reason(), "egress");
        assert!(
            a.update(at(650)).refuses_newcomers(),
            "65 %: above 60, held"
        );
        let v = a.update(at(920));
        assert!(v.sheds() && !v.chokes(), "92 %");
        assert!(!a.update(at(550)).refuses_newcomers(), "55 %: released");
        let mut unknown = Admission::default();
        assert!(
            !unknown.update(at(5_000)).refuses_newcomers(),
            "no card said, nothing to be near"
        );
    }
}
