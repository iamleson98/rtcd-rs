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
//! There are two budgets because there are two ceilings. A virtual machine's network card takes a
//! number of bits per second and also a number of packets per second, and the second one is the
//! one voice meets first: an Opus packet is 100 bytes, so a call with many open microphones asks
//! for far more packets than bits (measured on the OVH video node: 150 to 200 thousand small
//! packets a second were sent without loss, and at 270 thousand the card's transmit ring stayed
//! full and the kernel dropped what it could not queue, at 400 Mbit/s, half what the same card
//! sends in large packets). Either budget squeezes.
//!
//! The rate is also what a placement decision wants to know, so it is readable whether or not a
//! budget is set. The kernel's own count of datagrams it refused to queue is kept too: UDP
//! reports those to nobody, so without it a node that drops half its packets sees none of them.

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
/// The gauge is updated by the sessions' upkeep, so while no session exists it is not. A gap this
/// long between two updates was a quiet node: what it held before says nothing about now.
const IDLE_GAP: Duration = Duration::from_secs(10);

#[derive(Debug, Default)]
struct State {
    at: Option<Instant>,
    bytes: u64,
    packets: u64,
    rate_bps: f64,
    rate_pps: f64,
    squeezed: bool,
    calm_since: Option<Instant>,
}

/// What the shards of one node have sent, shared by all of them.
#[derive(Debug, Default)]
pub struct Gauge {
    bytes: AtomicU64,
    packets: AtomicU64,
    state: Mutex<State>,
    /// The smoothed rates, for readers that do not take the lock.
    rate: AtomicU64,
    rate_packets: AtomicU64,
    squeezed: AtomicBool,
    /// Datagrams the kernel refused to queue since the node started (UDP `SndbufErrors`).
    kernel_drops: AtomicU64,
}

impl Gauge {
    /// Counts what was put on the wire: `bytes` (headers included) in `packets` datagrams.
    pub fn add(&self, bytes: u64, packets: u64) {
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        self.packets.fetch_add(packets, Ordering::Relaxed);
    }

    /// The smoothed send rate in bits per second.
    pub fn rate_bps(&self) -> u64 {
        self.rate.load(Ordering::Relaxed)
    }

    /// The smoothed send rate in packets per second.
    pub fn rate_pps(&self) -> u64 {
        self.rate_packets.load(Ordering::Relaxed)
    }

    /// How many datagrams the kernel has refused to queue since the node started: what was handed
    /// to the socket and never left (0 where the system does not say).
    pub fn kernel_send_drops(&self) -> u64 {
        self.kernel_drops.load(Ordering::Relaxed)
    }

    /// Whether receivers are being held to the small layers.
    pub fn squeezed(&self) -> bool {
        self.squeezed.load(Ordering::Relaxed)
    }

    /// Recomputes the rates and the decision; cheap to call often, it works at most once a window.
    /// A budget of 0 is not enforced: with both at 0 the gauge only measures.
    pub fn update(&self, now: Instant, budget_bps: u64, budget_pps: u64) {
        let Ok(mut st) = self.state.try_lock() else {
            return;
        };
        let total = self.bytes.load(Ordering::Relaxed);
        let packets = self.packets.load(Ordering::Relaxed);
        let Some(at) = st.at else {
            st.at = Some(now);
            st.bytes = total;
            st.packets = packets;
            return;
        };
        let elapsed = now.saturating_duration_since(at);
        if elapsed < WINDOW {
            return;
        }
        let secs = elapsed.as_secs_f64();
        let idle = elapsed >= IDLE_GAP;
        let smooth = |old: f64, instant: f64| {
            if old == 0.0 || idle {
                instant
            } else {
                (old + instant) / 2.0
            }
        };
        st.rate_bps = smooth(
            st.rate_bps,
            total.saturating_sub(st.bytes) as f64 * 8.0 / secs,
        );
        st.rate_pps = smooth(
            st.rate_pps,
            packets.saturating_sub(st.packets) as f64 / secs,
        );
        st.at = Some(now);
        st.bytes = total;
        st.packets = packets;
        self.rate.store(st.rate_bps as u64, Ordering::Relaxed);
        self.rate_packets
            .store(st.rate_pps as u64, Ordering::Relaxed);
        if let Some(drops) = udp_send_errors() {
            self.kernel_drops.store(drops, Ordering::Relaxed);
        }

        if idle {
            // The quiet was the calm spell: a class that starts now does not begin held to the
            // small layers because the last one ended squeezed.
            st.calm_since = Some(at);
        }
        let squeezed = decide(&mut st, now, budget_bps, budget_pps);
        self.squeezed.store(squeezed, Ordering::Relaxed);
    }
}

/// The kernel's count of UDP datagrams it could not queue for sending (`SndbufErrors` in
/// `/proc/net/snmp`), where the system has one.
fn udp_send_errors() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let snmp = std::fs::read_to_string("/proc/net/snmp").ok()?;
        let mut lines = snmp.lines().filter(|l| l.starts_with("Udp:"));
        let names = lines.next()?;
        let values = lines.next()?;
        let at = names.split_whitespace().position(|n| n == "SndbufErrors")?;
        values.split_whitespace().nth(at)?.parse().ok()
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// The policy on its own: squeezed from the share of a budget it reaches, released after a calm
/// spell. The share is that of whichever budget is nearer its limit.
fn decide(st: &mut State, now: Instant, budget_bps: u64, budget_pps: u64) -> bool {
    if budget_bps == 0 && budget_pps == 0 {
        st.squeezed = false;
        st.calm_since = None;
        return false;
    }
    let share_of = |rate: f64, budget: u64| {
        if budget == 0 {
            0.0
        } else {
            rate / budget as f64
        }
    };
    let share = share_of(st.rate_bps, budget_bps).max(share_of(st.rate_pps, budget_pps));
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

    /// Runs the gauge for `secs` seconds at `mbps`, one update per second, with 1,200-byte packets.
    fn run(g: &Gauge, now: &mut Instant, secs: u64, mbps: u64, budget_mbps: u64) {
        run_packets(
            g,
            now,
            secs,
            mbps * MBIT,
            (mbps * MBIT).div_ceil(1_200),
            budget_mbps,
            0,
        );
    }

    /// Runs the gauge for `secs` seconds, sending `bytes` in `packets` each second.
    fn run_packets(
        g: &Gauge,
        now: &mut Instant,
        secs: u64,
        bytes: u64,
        packets: u64,
        budget_mbps: u64,
        budget_pps: u64,
    ) {
        for _ in 0..secs {
            g.add(bytes, packets);
            *now += Duration::from_secs(1);
            g.update(*now, budget_mbps * 1_000_000, budget_pps);
        }
    }

    #[test]
    fn the_rate_is_measured_whether_or_not_there_is_a_budget() {
        let g = Gauge::default();
        let mut now = Instant::now();
        g.update(now, 0, 0);
        run(&g, &mut now, 5, 100, 0);
        let rate = g.rate_bps() / 1_000_000;
        assert!((95..=105).contains(&rate), "rate {rate} Mbit/s");
        assert!(!g.squeezed());
    }

    #[test]
    fn video_is_squeezed_near_the_budget_and_released_after_a_calm_spell() {
        let g = Gauge::default();
        let mut now = Instant::now();
        g.update(now, 300_000_000, 0);
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
        g.update(now, 300_000_000, 0);
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
        g.update(now, 0, 0);
        run(&g, &mut now, 10, 900, 0);
        assert!(!g.squeezed());
    }

    #[test]
    fn the_packet_rate_squeezes_on_its_own() {
        // Voice: 100,000 packets of 100 bytes a second is 80 Mbit/s, far under the byte budget,
        // and the whole of a 110,000-packet budget.
        let g = Gauge::default();
        let mut now = Instant::now();
        g.update(now, 600_000_000, 110_000);
        run_packets(&g, &mut now, 5, 100_000 * 100, 100_000, 600, 110_000);
        assert!(
            g.squeezed(),
            "91 % of the packet budget, 13 % of the byte budget"
        );
        assert_eq!(g.rate_pps(), 100_000);
        run_packets(&g, &mut now, 30, 30_000 * 100, 30_000, 600, 110_000);
        assert!(!g.squeezed(), "a quarter of either budget for long enough");
    }

    #[test]
    fn a_budget_of_zero_is_not_enforced() {
        let g = Gauge::default();
        let mut now = Instant::now();
        g.update(now, 0, 0);
        // Only the byte budget is set: a packet rate far beyond any sensible budget does not matter.
        run_packets(&g, &mut now, 10, 10 * MBIT, 900_000, 600, 0);
        assert!(!g.squeezed());
        assert_eq!(g.rate_pps(), 900_000);
    }

    #[test]
    fn a_node_that_was_squeezed_and_then_idle_starts_the_next_call_released() {
        let g = Gauge::default();
        let mut now = Instant::now();
        g.update(now, 300_000_000, 0);
        run(&g, &mut now, 6, 290, 300);
        assert!(g.squeezed());
        // The last session leaves: nothing updates the gauge for ten minutes.
        now += Duration::from_secs(600);
        g.add(5 * MBIT, 100);
        g.update(now, 300_000_000, 0);
        assert!(!g.squeezed(), "the idle spell released it");
        assert!(
            g.rate_bps() < 1_000_000,
            "the rate is the idle one, not the old {}",
            g.rate_bps()
        );
    }
}
