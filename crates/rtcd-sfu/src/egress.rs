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
//! The budgets are optional. With none set the node learns its own ceiling from the one signal
//! that cannot be wrong: the kernel refusing datagrams. The first time it does, the rate it was
//! sending becomes the ceiling (a little under it, so the node stays clear of the edge), the
//! squeeze starts, and the ceiling is lifted a notch every few quiet minutes so that a node whose
//! network got better, or whose neighbour went quiet, is not held to its worst moment. A budget
//! that is set still applies, whichever of the two is lower.
//!
//! The rate is also what a placement decision wants to know, so it is readable whether or not a
//! budget is set. The kernel's own count of datagrams it refused to queue is kept too: UDP
//! reports those to nobody, so without it a node that drops half its packets sees none of them.
//!
//! What is counted is what the shards hand to their sockets, one datagram at a time, headers up
//! to the link layer included: media after encryption, receiver reports, retransmissions, ICE
//! and DTLS, all of it, as the network card carries it. The send queue does not tell a voice
//! packet from a report, and a count of the media alone reads a third to a half low.
//!
//! Squeezing video takes the bits down, and voice, which is most of the packets, stays. So a
//! refusal that goes on after the squeeze is answered a second time: past [`CHOKE_AFTER`] the
//! node is *choked*, and calls forward the loudest voice only until the kernel has stopped
//! refusing. Work that can wait (uploading recordings) stands aside for as long as the kernel
//! refuses ([`Gauge::refusing`]).

use std::net::SocketAddr;
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
/// A window in which the kernel refused at least this many datagrams a second taught the node
/// where its ceiling is (one stray refusal is not a ceiling).
const DROPS_PER_SEC: f64 = 50.0;
/// ...in this many windows in a row: a burst (a keyframe fanned out to a whole class) overflows the
/// queue for a moment without the rate being too high, and a ceiling learned from it would hold
/// calls back for the hour it takes to lift.
const LEARN_AFTER: u32 = 2;
/// The learned ceiling is this share of the rate the node was sending when the kernel refused.
const LEARNED_SHARE: f64 = 0.9;
/// A refusal at a rate this close to the learned ceiling, or above it, teaches a lower one; below
/// it the ceiling was not what was refused, and the squeeze alone answers.
const RELEARN_NEAR: f64 = 0.95;
/// A learned ceiling is lifted by [`GROW_BY`] after this long without a refusal.
const GROW_AFTER: Duration = Duration::from_secs(300);
const GROW_BY: f64 = 1.1;
/// The kernel refuses datagrams still, this long after the squeeze began (and it did not help):
/// the node is choked.
const CHOKE_AFTER: Duration = Duration::from_secs(5);
/// The kernel is taken to be refusing until a window this long has gone by without a refusal.
const REFUSING_FOR: Duration = Duration::from_secs(10);
/// The Ethernet header, which the network card's counters include; the IP header (20 bytes, 40
/// for IPv6) and the UDP (8) or TCP (20) one are added per datagram.
const LINK_OVERHEAD: u64 = 14;
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
    /// What the kernel's refusals taught: the ceilings in bits and packets per second (0: none yet).
    learned_bps: f64,
    learned_pps: f64,
    /// The kernel's count at the last window, to tell what it refused during this one.
    last_drops: Option<u64>,
    /// When the ceiling was last taught or lifted.
    ceiling_at: Option<Instant>,
    /// The kernel's count when the node first read it: refusals before that were not ours.
    drops_base: Option<u64>,
    /// Windows in a row in which the kernel refused enough datagrams to matter.
    refusal_windows: u32,
    /// The last window in which the kernel refused enough datagrams to matter, and the first of
    /// the run of them it belongs to.
    refusing_at: Option<Instant>,
    refusing_since: Option<Instant>,
}

/// Where the kernel's count of refused datagrams comes from.
struct DropsSource(Box<dyn Fn() -> Option<u64> + Send + Sync>);

impl Default for DropsSource {
    fn default() -> Self {
        Self(Box::new(udp_send_errors))
    }
}

impl std::fmt::Debug for DropsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DropsSource")
    }
}

/// What the shards of one node have sent, shared by all of them.
#[derive(Debug, Default)]
pub struct Gauge {
    drops_source: DropsSource,
    bytes: AtomicU64,
    packets: AtomicU64,
    state: Mutex<State>,
    /// The smoothed rates, for readers that do not take the lock.
    rate: AtomicU64,
    rate_packets: AtomicU64,
    squeezed: AtomicBool,
    /// Datagrams the kernel refused to queue since the node started (UDP `SndbufErrors`).
    kernel_drops: AtomicU64,
    /// The kernel is refusing datagrams now, and has been, without a calm window, for how long
    /// the two flags say.
    refusing: AtomicBool,
    choked: AtomicBool,
    /// The ceilings the node learned (0 until the kernel has refused something).
    ceiling: AtomicU64,
    ceiling_packets: AtomicU64,
}

impl Gauge {
    /// A gauge that reads the count of refused datagrams from `source` instead of the system's
    /// (`None`: it does not say), for a platform that keeps the count elsewhere and for tests,
    /// which cannot make a kernel refuse.
    pub fn with_kernel_drops(source: impl Fn() -> Option<u64> + Send + Sync + 'static) -> Self {
        Self {
            drops_source: DropsSource(Box::new(source)),
            ..Self::default()
        }
    }

    /// Counts a datagram of `len` bytes handed to a socket for `peer`, with the headers it will
    /// carry on the wire. `tcp`: a segment of a TCP stream, not a UDP datagram.
    pub fn add_datagram(&self, len: usize, peer: SocketAddr, tcp: bool) {
        let ip = if peer.is_ipv6() { 40 } else { 20 };
        let transport = if tcp { 20 } else { 8 };
        self.add(len as u64 + LINK_OVERHEAD + ip + transport, 1);
    }

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

    /// How many datagrams the kernel has refused to queue since this process read the count first:
    /// what was handed to the socket and never left (0 where the system does not say).
    pub fn kernel_send_drops(&self) -> u64 {
        self.kernel_drops.load(Ordering::Relaxed)
    }

    /// The send rate in bits per second at which the kernel started refusing datagrams, as the node
    /// learned it (0 until it has).
    pub fn ceiling_bps(&self) -> u64 {
        self.ceiling.load(Ordering::Relaxed)
    }

    /// The same in packets per second.
    pub fn ceiling_pps(&self) -> u64 {
        self.ceiling_packets.load(Ordering::Relaxed)
    }

    /// Whether receivers are being held to the small layers.
    pub fn squeezed(&self) -> bool {
        self.squeezed.load(Ordering::Relaxed)
    }

    /// Whether the kernel is refusing datagrams now (it was within [`REFUSING_FOR`]).
    pub fn refusing(&self) -> bool {
        self.refusing.load(Ordering::Relaxed)
    }

    /// Whether it has been refusing for [`CHOKE_AFTER`] or more, squeeze or not: the bits fell
    /// and the packets did not.
    pub fn choked(&self) -> bool {
        self.choked.load(Ordering::Relaxed)
    }

    /// Recomputes the rates and the decision; cheap to call often, it works at most once a window.
    /// A budget of 0 is not set: the node's learned ceiling stands in for it.
    pub fn update(&self, now: Instant, budget_bps: u64, budget_pps: u64) {
        // Every session calls this once a second, and the kernel's count is a file to read and
        // parse: only the call that finds a window over pays for it.
        if !self.due(now) {
            return;
        }
        self.update_with(now, budget_bps, budget_pps, (self.drops_source.0)());
    }

    /// Whether a window has gone by since the last update (and nobody is updating just now).
    fn due(&self, now: Instant) -> bool {
        self.state.try_lock().is_ok_and(|st| {
            st.at
                .is_none_or(|at| now.saturating_duration_since(at) >= WINDOW)
        })
    }

    /// [`Gauge::update`] with the kernel's count of refused datagrams given (`None` where the
    /// system does not say).
    fn update_with(&self, now: Instant, budget_bps: u64, budget_pps: u64, drops: Option<u64>) {
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
        let refused = drops.map_or(0, |d| {
            let base = *st.drops_base.get_or_insert(d);
            self.kernel_drops
                .store(d.saturating_sub(base), Ordering::Relaxed);
            let before = st.last_drops.replace(d).unwrap_or(d);
            d.saturating_sub(before)
        });

        if idle {
            // The quiet was the calm spell: a class that starts now does not begin held to the
            // small layers because the last one ended squeezed.
            st.calm_since = Some(at);
        }
        learn(&mut st, now, refused as f64 / secs);
        let (refusing, choked) = track_refusals(&mut st, now, refused as f64 / secs);
        self.refusing.store(refusing, Ordering::Relaxed);
        self.choked.store(choked, Ordering::Relaxed);
        self.ceiling.store(st.learned_bps as u64, Ordering::Relaxed);
        self.ceiling_packets
            .store(st.learned_pps as u64, Ordering::Relaxed);
        let (limit_bps, limit_pps) = (
            lower_of(budget_bps, st.learned_bps),
            lower_of(budget_pps, st.learned_pps),
        );
        let squeezed = decide(&mut st, now, limit_bps, limit_pps);
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

/// The lower of a configured budget (0: none) and a learned ceiling (0: none yet); 0 if neither.
fn lower_of(budget: u64, learned: f64) -> u64 {
    match (budget, learned as u64) {
        (0, l) => l,
        (b, 0) => b,
        (b, l) => b.min(l),
    }
}

/// What the kernel's refusals teach, given how many it made a second during the window just
/// measured: after [`LEARN_AFTER`] such windows in a row a ceiling where there was none or a lower
/// one, with the squeeze at once, and a notch back up after a long quiet.
fn learn(st: &mut State, now: Instant, refused_per_sec: f64) {
    if refused_per_sec >= DROPS_PER_SEC {
        st.refusal_windows += 1;
        if st.refusal_windows < LEARN_AFTER {
            return;
        }
        let near = |rate: f64, learned: f64| learned == 0.0 || rate >= learned * RELEARN_NEAR;
        if near(st.rate_bps, st.learned_bps) || near(st.rate_pps, st.learned_pps) {
            st.learned_bps = LEARNED_SHARE * st.rate_bps;
            st.learned_pps = LEARNED_SHARE * st.rate_pps;
        }
        st.squeezed = true;
        st.calm_since = None;
        st.ceiling_at = Some(now);
    } else {
        st.refusal_windows = 0;
        if st.learned_bps > 0.0 || st.learned_pps > 0.0 {
            let since = *st.ceiling_at.get_or_insert(now);
            if now.saturating_duration_since(since) >= GROW_AFTER {
                st.learned_bps *= GROW_BY;
                st.learned_pps *= GROW_BY;
                st.ceiling_at = Some(now);
            }
        }
    }
}

/// Whether the kernel is refusing datagrams and has been for long enough that the squeeze did not
/// answer it, given how many it refused a second in the window just measured.
fn track_refusals(st: &mut State, now: Instant, refused_per_sec: f64) -> (bool, bool) {
    if refused_per_sec >= DROPS_PER_SEC {
        st.refusing_at = Some(now);
        st.refusing_since.get_or_insert(now);
    }
    let refusing = st
        .refusing_at
        .is_some_and(|t| now.saturating_duration_since(t) < REFUSING_FOR);
    if !refusing {
        st.refusing_since = None;
    }
    let choked = refusing
        && st
            .refusing_since
            .is_some_and(|t| now.saturating_duration_since(t) >= CHOKE_AFTER);
    (refusing, choked)
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
            g.update_with(*now, budget_mbps * 1_000_000, budget_pps, None);
        }
    }

    #[test]
    fn the_rate_is_measured_whether_or_not_there_is_a_budget() {
        let g = Gauge::default();
        let mut now = Instant::now();
        g.update_with(now, 0, 0, None);
        run(&g, &mut now, 5, 100, 0);
        let rate = g.rate_bps() / 1_000_000;
        assert!((95..=105).contains(&rate), "rate {rate} Mbit/s");
        assert!(!g.squeezed());
    }

    #[test]
    fn video_is_squeezed_near_the_budget_and_released_after_a_calm_spell() {
        let g = Gauge::default();
        let mut now = Instant::now();
        g.update_with(now, 300_000_000, 0, None);
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
        g.update_with(now, 300_000_000, 0, None);
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
        g.update_with(now, 0, 0, None);
        run(&g, &mut now, 10, 900, 0);
        assert!(!g.squeezed());
    }

    #[test]
    fn the_packet_rate_squeezes_on_its_own() {
        // Voice: 100,000 packets of 100 bytes a second is 80 Mbit/s, far under the byte budget,
        // and the whole of a 110,000-packet budget.
        let g = Gauge::default();
        let mut now = Instant::now();
        g.update_with(now, 600_000_000, 110_000, None);
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
        g.update_with(now, 0, 0, None);
        // Only the byte budget is set: a packet rate far beyond any sensible budget does not matter.
        run_packets(&g, &mut now, 10, 10 * MBIT, 900_000, 600, 0);
        assert!(!g.squeezed());
        assert_eq!(g.rate_pps(), 900_000);
    }

    #[test]
    fn a_node_that_was_squeezed_and_then_idle_starts_the_next_call_released() {
        let g = Gauge::default();
        let mut now = Instant::now();
        g.update_with(now, 300_000_000, 0, None);
        run(&g, &mut now, 6, 290, 300);
        assert!(g.squeezed());
        // The last session leaves: nothing updates the gauge for ten minutes.
        now += Duration::from_secs(600);
        g.add(5 * MBIT, 100);
        g.update_with(now, 300_000_000, 0, None);
        assert!(!g.squeezed(), "the idle spell released it");
        assert!(
            g.rate_bps() < 1_000_000,
            "the rate is the idle one, not the old {}",
            g.rate_bps()
        );
    }

    /// Runs the gauge for `secs` seconds at `mbps`, the kernel refusing `refused` datagrams a second.
    fn run_refusing(
        g: &Gauge,
        now: &mut Instant,
        secs: u64,
        mbps: u64,
        refused: &mut u64,
        per_sec: u64,
    ) {
        for _ in 0..secs {
            g.add(mbps * MBIT, (mbps * MBIT).div_ceil(1_200));
            *now += Duration::from_secs(1);
            *refused += per_sec;
            g.update_with(*now, 0, 0, Some(*refused));
        }
    }

    #[test]
    fn a_node_with_no_budget_learns_its_ceiling_from_the_kernel_refusing_datagrams() {
        let g = Gauge::default();
        let mut now = Instant::now();
        let mut refused = 1_000_000;
        g.update_with(now, 0, 0, Some(refused));
        run_refusing(&g, &mut now, 20, 700, &mut refused, 0);
        assert!(
            !g.squeezed(),
            "700 Mbit/s and nothing refused: no reason to hold back"
        );
        assert_eq!(g.ceiling_bps(), 0);

        run_refusing(&g, &mut now, 3, 700, &mut refused, 5_000);
        assert!(g.squeezed(), "the kernel refused 5,000 datagrams a second");
        let ceiling = g.ceiling_bps() / 1_000_000;
        assert!(
            (600..=640).contains(&ceiling),
            "a little under the 700 Mbit/s it sent: {ceiling}"
        );
        assert!(g.ceiling_pps() > 0);
        assert_eq!(
            g.kernel_send_drops(),
            3 * 5_000,
            "what was refused since the node first looked, not since the machine booted"
        );
    }

    #[test]
    fn a_few_stray_refusals_are_not_a_ceiling() {
        let g = Gauge::default();
        let mut now = Instant::now();
        let mut refused = 0;
        g.update_with(now, 0, 0, Some(refused));
        run_refusing(&g, &mut now, 10, 700, &mut refused, 10);
        assert!(!g.squeezed());
        assert_eq!(g.ceiling_bps(), 0);
    }

    #[test]
    fn one_burst_of_refusals_is_not_a_ceiling() {
        let g = Gauge::default();
        let mut now = Instant::now();
        let mut refused = 0;
        g.update_with(now, 0, 0, Some(refused));
        run_refusing(&g, &mut now, 10, 300, &mut refused, 0);
        // A keyframe fanned out to a class: one window of refusals at a modest rate.
        run_refusing(&g, &mut now, 1, 300, &mut refused, 20_000);
        run_refusing(&g, &mut now, 10, 300, &mut refused, 0);
        assert_eq!(g.ceiling_bps(), 0);
        assert!(!g.squeezed());
        // Two in a row are a ceiling, even with calm between the pairs.
        run_refusing(&g, &mut now, 1, 600, &mut refused, 20_000);
        run_refusing(&g, &mut now, 3, 600, &mut refused, 0);
        run_refusing(&g, &mut now, 1, 600, &mut refused, 20_000);
        assert_eq!(g.ceiling_bps(), 0, "the windows were not in a row");
        run_refusing(&g, &mut now, 1, 600, &mut refused, 20_000);
        assert!(g.ceiling_bps() > 0 && g.squeezed());
    }

    #[test]
    fn the_squeeze_that_follows_a_refusal_is_released_when_the_rate_falls() {
        let g = Gauge::default();
        let mut now = Instant::now();
        let mut refused = 0;
        g.update_with(now, 0, 0, Some(refused));
        run_refusing(&g, &mut now, 4, 700, &mut refused, 5_000);
        assert!(g.squeezed());
        run_refusing(&g, &mut now, 10, 300, &mut refused, 0);
        assert!(g.squeezed(), "the calm spell is not over");
        run_refusing(&g, &mut now, 30, 300, &mut refused, 0);
        assert!(
            !g.squeezed(),
            "under two thirds of the ceiling for long enough"
        );
    }

    #[test]
    fn refusals_while_squeezed_below_the_ceiling_do_not_lower_it() {
        let g = Gauge::default();
        let mut now = Instant::now();
        let mut refused = 0;
        g.update_with(now, 0, 0, Some(refused));
        run_refusing(&g, &mut now, 4, 700, &mut refused, 5_000);
        let learned = g.ceiling_bps();
        // The squeeze takes the rate down, with the last queued datagrams still being refused.
        run_refusing(&g, &mut now, 10, 300, &mut refused, 5_000);
        assert_eq!(g.ceiling_bps(), learned);
        assert!(g.squeezed());
    }

    #[test]
    fn a_refusal_at_the_ceiling_teaches_a_lower_one() {
        let g = Gauge::default();
        let mut now = Instant::now();
        let mut refused = 0;
        g.update_with(now, 0, 0, Some(refused));
        run_refusing(&g, &mut now, 4, 700, &mut refused, 5_000);
        let first = g.ceiling_bps();
        run_refusing(&g, &mut now, 40, 300, &mut refused, 0);
        assert!(!g.squeezed());
        // It climbs back to the ceiling and is refused there again: the first was too high.
        run_refusing(&g, &mut now, 8, 650, &mut refused, 5_000);
        assert!(g.ceiling_bps() < first, "{} < {first}", g.ceiling_bps());
    }

    #[test]
    fn a_learned_ceiling_is_lifted_after_a_long_quiet() {
        let g = Gauge::default();
        let mut now = Instant::now();
        let mut refused = 0;
        g.update_with(now, 0, 0, Some(refused));
        run_refusing(&g, &mut now, 4, 700, &mut refused, 5_000);
        let learned = g.ceiling_bps();
        run_refusing(&g, &mut now, 290, 100, &mut refused, 0);
        assert_eq!(g.ceiling_bps(), learned, "not yet");
        run_refusing(&g, &mut now, 20, 100, &mut refused, 0);
        assert!(g.ceiling_bps() > learned, "{} > {learned}", g.ceiling_bps());
    }

    #[test]
    fn a_configured_budget_below_the_learned_ceiling_wins() {
        let g = Gauge::default();
        let mut now = Instant::now();
        let mut refused = 0;
        g.update_with(now, 300_000_000, 0, Some(refused));
        for _ in 0..4 {
            g.add(700 * MBIT, 700 * MBIT / 1_200);
            now += Duration::from_secs(1);
            refused += 5_000;
            g.update_with(now, 300_000_000, 0, Some(refused));
        }
        // 300 Mbit/s is under the 630 learned: the node is squeezed at 85 % of the budget.
        run_packets_refused(&g, &mut now, &mut refused, 270);
        assert!(g.squeezed());
        assert_eq!(lower_of(300_000_000, g.ceiling_bps() as f64), 300_000_000);
    }

    fn run_packets_refused(g: &Gauge, now: &mut Instant, refused: &mut u64, mbps: u64) {
        for _ in 0..30 {
            g.add(mbps * MBIT, mbps * MBIT / 1_200);
            *now += Duration::from_secs(1);
            g.update_with(*now, 300_000_000, 0, Some(*refused));
        }
    }

    #[test]
    fn the_kernels_count_is_read_once_a_window_not_once_a_call() {
        let reads = std::sync::Arc::new(AtomicU64::new(0));
        let counter = reads.clone();
        let g = Gauge::with_kernel_drops(move || {
            counter.fetch_add(1, Ordering::Relaxed);
            Some(0)
        });
        let mut now = Instant::now();
        // 1,500 sessions calling in the same second, ten seconds in a row.
        for _ in 0..10 {
            for _ in 0..1_500 {
                g.update(now, 0, 0);
            }
            now += Duration::from_secs(1);
        }
        let reads = reads.load(Ordering::Relaxed);
        assert!((10..=11).contains(&reads), "{reads} reads");
    }

    #[test]
    fn datagrams_are_counted_with_the_headers_the_card_carries() {
        let g = Gauge::default();
        let v4: SocketAddr = "10.0.0.1:5000".parse().unwrap();
        let v6: SocketAddr = "[2001:db8::1]:5000".parse().unwrap();
        g.add_datagram(100, v4, false);
        g.add_datagram(100, v6, false);
        g.add_datagram(100, v4, true);
        assert_eq!(g.bytes.load(Ordering::Relaxed), 142 + 162 + 154);
        assert_eq!(g.packets.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn refusals_that_go_on_after_the_squeeze_choke_the_node() {
        let g = Gauge::default();
        let mut now = Instant::now();
        let mut refused = 0;
        g.update_with(now, 0, 0, Some(refused));
        run_refusing(&g, &mut now, 3, 700, &mut refused, 5_000);
        assert!(g.squeezed() && g.refusing());
        assert!(!g.choked(), "the squeeze has had three seconds");
        // The bits fell by half and the kernel refuses as many datagrams as before (voice).
        run_refusing(&g, &mut now, 4, 350, &mut refused, 5_000);
        assert!(g.choked(), "seven seconds of refusals");
        // It stops refusing: the node stays choked until a calm spell has passed.
        run_refusing(&g, &mut now, 5, 350, &mut refused, 0);
        assert!(g.choked() && g.refusing());
        run_refusing(&g, &mut now, 6, 350, &mut refused, 0);
        assert!(
            !g.choked() && !g.refusing(),
            "ten seconds without a refusal"
        );
    }

    #[test]
    fn a_short_burst_of_refusals_squeezes_without_choking() {
        let g = Gauge::default();
        let mut now = Instant::now();
        let mut refused = 0;
        g.update_with(now, 0, 0, Some(refused));
        // (The first window only takes the kernel's count as the base.)
        run_refusing(&g, &mut now, 3, 700, &mut refused, 5_000);
        run_refusing(&g, &mut now, 20, 300, &mut refused, 0);
        assert!(g.squeezed());
        assert!(!g.choked());
        assert!(!g.refusing());
        // A second burst later is a new run, not a continuation of the first.
        run_refusing(&g, &mut now, 3, 700, &mut refused, 5_000);
        assert!(!g.choked());
    }
}
