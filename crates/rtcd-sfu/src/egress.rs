//! The node's send rate, and what to do when it nears the ceiling.
//!
//! A node that is asked for more than its network can carry does not slow down evenly: its send
//! queues overflow and every receiver loses packets at once (the production load test showed 50 to
//! 75 % loss for everyone past about 320 Mbit/s). What the large products do instead is share a
//! budget out: when the total nears it, each receiver is moved to smaller video. This is that
//! rule for a node: the shards add what they send to one [`Gauge`], and when the smoothed rate
//! reaches a share of the configured budget every receiver's cameras and screen shares fall back
//! to their small layer, until the rate has stayed well under the budget for a while.
//!
//! The rate is also what a placement decision wants to know, so it is readable whether or not a
//! budget is set.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Video falls back to the small layers at this share of the budget.
const SQUEEZE_AT: f64 = 0.85;
/// And is released only below this share, held for [`RELEASE_AFTER`], so the rate falling because
/// of the squeeze does not release it at once.
const RELEASE_BELOW: f64 = 0.65;
const RELEASE_AFTER: Duration = Duration::from_secs(20);
/// The rate is measured over windows of at least this long and smoothed over two of them.
const WINDOW: Duration = Duration::from_secs(1);

#[derive(Debug, Default)]
struct State {
    at: Option<Instant>,
    bytes: u64,
    rate_bps: f64,
    squeezed: bool,
    calm_since: Option<Instant>,
}

/// What the shards of one node have sent, shared by all of them.
#[derive(Debug, Default)]
pub struct Gauge {
    bytes: AtomicU64,
    state: Mutex<State>,
    /// The smoothed rate in bits per second, for readers that do not take the lock.
    rate: AtomicU64,
    squeezed: AtomicBool,
}

impl Gauge {
    /// Counts bytes put on the wire (headers included).
    pub fn add(&self, bytes: u64) {
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// The smoothed send rate in bits per second.
    pub fn rate_bps(&self) -> u64 {
        self.rate.load(Ordering::Relaxed)
    }

    /// Whether receivers are being held to the small layers.
    pub fn squeezed(&self) -> bool {
        self.squeezed.load(Ordering::Relaxed)
    }

    /// Recomputes the rate and the decision; cheap to call often, it works at most once a window.
    /// A `budget_bps` of 0 only measures.
    pub fn update(&self, now: Instant, budget_bps: u64) {
        let Ok(mut st) = self.state.try_lock() else {
            return;
        };
        let total = self.bytes.load(Ordering::Relaxed);
        let Some(at) = st.at else {
            st.at = Some(now);
            st.bytes = total;
            return;
        };
        let elapsed = now.saturating_duration_since(at);
        if elapsed < WINDOW {
            return;
        }
        let instant = total.saturating_sub(st.bytes) as f64 * 8.0 / elapsed.as_secs_f64();
        st.rate_bps = if st.rate_bps == 0.0 {
            instant
        } else {
            (st.rate_bps + instant) / 2.0
        };
        st.at = Some(now);
        st.bytes = total;
        self.rate.store(st.rate_bps as u64, Ordering::Relaxed);

        let squeezed = decide(&mut st, now, budget_bps);
        self.squeezed.store(squeezed, Ordering::Relaxed);
    }
}

/// The policy on its own: squeezed from the budget share it reaches, released after a calm spell.
fn decide(st: &mut State, now: Instant, budget_bps: u64) -> bool {
    if budget_bps == 0 {
        st.squeezed = false;
        st.calm_since = None;
        return false;
    }
    let share = st.rate_bps / budget_bps as f64;
    if share >= SQUEEZE_AT {
        st.squeezed = true;
        st.calm_since = None;
    } else if st.squeezed {
        if share < RELEASE_BELOW {
            let since = *st.calm_since.get_or_insert(now);
            if now.saturating_duration_since(since) >= RELEASE_AFTER {
                st.squeezed = false;
                st.calm_since = None;
            }
        } else {
            st.calm_since = None;
        }
    }
    st.squeezed
}

#[cfg(test)]
mod tests {
    use super::*;

    const MBIT: u64 = 125_000;

    /// Runs the gauge for `secs` seconds at `mbps`, one update per second.
    fn run(g: &Gauge, now: &mut Instant, secs: u64, mbps: u64, budget_mbps: u64) {
        for _ in 0..secs {
            g.add(mbps * MBIT);
            *now += Duration::from_secs(1);
            g.update(*now, budget_mbps * 1_000_000);
        }
    }

    #[test]
    fn the_rate_is_measured_whether_or_not_there_is_a_budget() {
        let g = Gauge::default();
        let mut now = Instant::now();
        g.update(now, 0);
        run(&g, &mut now, 5, 100, 0);
        let rate = g.rate_bps() / 1_000_000;
        assert!((95..=105).contains(&rate), "rate {rate} Mbit/s");
        assert!(!g.squeezed());
    }

    #[test]
    fn video_is_squeezed_near_the_budget_and_released_after_a_calm_spell() {
        let g = Gauge::default();
        let mut now = Instant::now();
        g.update(now, 300_000_000);
        run(&g, &mut now, 5, 100, 300);
        assert!(!g.squeezed(), "a third of the budget");
        run(&g, &mut now, 5, 280, 300);
        assert!(g.squeezed(), "93 % of the budget");
        run(&g, &mut now, 10, 150, 300);
        assert!(g.squeezed(), "the rate fell but the calm spell is not over");
        run(&g, &mut now, 30, 150, 300);
        assert!(!g.squeezed(), "half the budget for long enough");
    }

    #[test]
    fn a_rate_hovering_between_the_marks_does_not_release() {
        let g = Gauge::default();
        let mut now = Instant::now();
        g.update(now, 300_000_000);
        run(&g, &mut now, 6, 290, 300);
        assert!(g.squeezed());
        // 75 % is under the squeeze mark but over the release mark.
        run(&g, &mut now, 60, 225, 300);
        assert!(g.squeezed());
    }

    #[test]
    fn no_budget_never_squeezes() {
        let g = Gauge::default();
        let mut now = Instant::now();
        g.update(now, 0);
        run(&g, &mut now, 10, 900, 0);
        assert!(!g.squeezed());
    }
}
