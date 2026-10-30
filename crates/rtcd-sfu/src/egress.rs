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
//! refusal that goes on after the squeeze is answered a second time: when the kernel has refused
//! in [`CHOKE_AFTER`] windows in a row *while the node was squeezed*, the node is *choked*, and
//! calls forward the loudest voice only until the kernel has accepted everything for
//! [`CHOKE_HOLD`]. Under a steady overload that cycle would repeat (lift, overflow, wait, choke
//! again), so a choke that returns within [`CHOKE_MEMORY`] of the last one begins at the first
//! refusing window and is held twice as long, up to [`CHOKE_HOLD_MAX`]. Work that can wait
//! (uploading recordings) stands aside for as long as the kernel refuses ([`Gauge::refusing`]).
//!
//! The ceiling is only ever *taught* by the kernel refusing in [`LEARN_AFTER`] windows in a row
//! at a rate that matters against the traffic ([`REFUSAL_SHARE`] of the packets sent, with a
//! floor): a stall of the machine lasting a fraction of a second straddles at most two windows,
//! and a keyframe fanned out to a class is one. What it taught is forgotten gradually: it is
//! lifted by a tenth for every [`GROW_AFTER`] without a refusal, however long the node sat idle.

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
/// A window counts as refusing when the kernel refused at least this many datagrams a second...
const DROPS_PER_SEC: f64 = 50.0;
/// ...and at least this share of the datagrams sent in it (a node sending 300 thousand a second
/// that loses fifty is not at its ceiling). The kernel counts calls, not segments: with offload
/// one refused call is up to 64 datagrams and counts as one, so the share is read low, which is
/// the safe way.
const REFUSAL_SHARE: f64 = 0.005;
/// A ceiling is taught by this many refusing windows in a row: a burst (a keyframe fanned out to
/// a whole class) or a stall of the machine overflows the queue for a moment, across at most two
/// windows, without the rate being too high, and a ceiling learned from it would hold calls back
/// for as long as it takes to lift.
const LEARN_AFTER: u32 = 3;
/// The learned ceiling is this share of the rate the node was sending when the kernel refused.
const LEARNED_SHARE: f64 = 0.9;
/// A refusal at a rate this close to the learned ceiling, or above it, teaches a lower one; below
/// it the ceiling was not what was refused, and the squeeze alone answers.
const RELEARN_NEAR: f64 = 0.95;
/// A learned ceiling is lifted by [`GROW_BY`] after this long without a refusal.
const GROW_AFTER: Duration = Duration::from_secs(300);
const GROW_BY: f64 = 1.1;
/// At most this many notches at once: far beyond any ceiling, and finite.
const MAX_GROW_STEPS: f64 = 100.0;
/// The kernel refused in this many windows in a row while the node was squeezed (and the squeeze
/// did not help): the node is choked.
const CHOKE_AFTER: u32 = 5;
/// A choke is lifted when the kernel has accepted everything for this long...
const CHOKE_HOLD: Duration = Duration::from_secs(10);
/// ...twice as long each time it returns within [`CHOKE_MEMORY`], up to this.
const CHOKE_HOLD_MAX: Duration = Duration::from_secs(120);
/// A choke returning within this long of the last one is the same overload.
const CHOKE_MEMORY: Duration = Duration::from_secs(60);
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
    /// Windows in a row in which the kernel refused enough datagrams to matter, and of those how
    /// many were while the node was squeezed.
    run: u32,
    squeezed_run: u32,
    /// The last such window.
    refusing_at: Option<Instant>,
    /// Whether the node is choked, for how long a calm spell lifts it, when it last was lifted,
    /// and how many times in a row it has come back soon after.
    choked: bool,
    choke_hold: Duration,
    choke_lifted: Option<Instant>,
    rechokes: u32,
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

/// The moment times are counted from in the gauge's atomics.
#[derive(Debug)]
struct Epoch(Instant);

impl Default for Epoch {
    fn default() -> Self {
        Self(Instant::now())
    }
}

/// What the shards of one node have sent, shared by all of them.
#[derive(Debug, Default)]
pub struct Gauge {
    epoch: Epoch,
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
    /// Until when the kernel counts as refusing and the node as choked, in nanoseconds after
    /// `epoch` (0: not at all). Readers compare with the time they ask, so the answer ages by
    /// itself when no session is left to update the gauge.
    refusing_until: AtomicU64,
    choked_until: AtomicU64,
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

    /// What a datagram of `len` bytes handed to a socket for `peer` takes on the wire, with the
    /// headers it will carry. `tcp`: a segment of a TCP stream, not a UDP datagram.
    pub fn wire_len(len: usize, peer: SocketAddr, tcp: bool) -> u64 {
        let ip = if peer.is_ipv6() { 40 } else { 20 };
        let transport = if tcp { 20 } else { 8 };
        len as u64 + LINK_OVERHEAD + ip + transport
    }

    /// Counts what was put on the wire: `bytes` (headers included) in `packets` datagrams. Every
    /// shard adds to the same two counters, so a shard adds once for the whole of a batch it sent
    /// (the sum of [`Gauge::wire_len`]), not once per datagram.
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

    /// Whether the kernel is refusing datagrams at `now` (it was within [`REFUSING_FOR`]).
    pub fn refusing(&self, now: Instant) -> bool {
        self.refusing_until.load(Ordering::Relaxed) > self.nanos(now)
    }

    /// Whether the node is choked at `now`: the kernel went on refusing after the squeeze, the
    /// bits fell and the packets did not.
    pub fn choked(&self, now: Instant) -> bool {
        self.choked_until.load(Ordering::Relaxed) > self.nanos(now)
    }

    fn nanos(&self, t: Instant) -> u64 {
        t.saturating_duration_since(self.epoch.0).as_nanos() as u64 + 1
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
        let sent_pps = packets.saturating_sub(st.packets) as f64 / secs;
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
            // small layers because the last one ended squeezed, or choked, or in the middle of a
            // run of refusals.
            st.calm_since = Some(at);
            st.run = 0;
            st.squeezed_run = 0;
            st.choked = false;
            st.choke_lifted = None;
        }
        let refusing = refused as f64 / secs >= (REFUSAL_SHARE * sent_pps).max(DROPS_PER_SEC);
        if refusing {
            st.run += 1;
            st.refusing_at = Some(now);
        } else {
            st.run = 0;
        }
        learn(&mut st, now, refusing);
        self.ceiling.store(st.learned_bps as u64, Ordering::Relaxed);
        self.ceiling_packets
            .store(st.learned_pps as u64, Ordering::Relaxed);
        let (limit_bps, limit_pps) = (
            lower_of(budget_bps, st.learned_bps),
            lower_of(budget_pps, st.learned_pps),
        );
        let squeezed = decide(&mut st, now, limit_bps, limit_pps);
        self.squeezed.store(squeezed, Ordering::Relaxed);
        choke(&mut st, now, refusing, squeezed);
        let until = |t: Option<Instant>, hold: Duration| t.map_or(0, |t| self.nanos(t + hold));
        self.refusing_until
            .store(until(st.refusing_at, REFUSING_FOR), Ordering::Relaxed);
        let choked_until = if st.choked {
            until(st.refusing_at, st.choke_hold)
        } else {
            0
        };
        self.choked_until.store(choked_until, Ordering::Relaxed);
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

/// What the kernel's refusals teach, given whether the window just measured was a refusing one
/// (`st.run` counts the refusing windows in a row up to it): after [`LEARN_AFTER`] of them a
/// ceiling where there was none or a lower one, with the squeeze at once; and a notch back up for
/// every [`GROW_AFTER`] spent without refusals, however many went by since it was last looked at
/// (a node idle all night has its ceiling back by the morning).
fn learn(st: &mut State, now: Instant, refusing: bool) {
    if refusing {
        if st.run < LEARN_AFTER {
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
    } else if st.learned_bps > 0.0 || st.learned_pps > 0.0 {
        let since = *st.ceiling_at.get_or_insert(now);
        let steps = (now.saturating_duration_since(since).as_secs_f64() / GROW_AFTER.as_secs_f64())
            .floor()
            .min(MAX_GROW_STEPS);
        if steps >= 1.0 {
            let by = GROW_BY.powf(steps);
            st.learned_bps *= by;
            st.learned_pps *= by;
            st.ceiling_at = Some(since + GROW_AFTER.mul_f64(steps));
        }
    }
}

/// Choked: the kernel refused in [`CHOKE_AFTER`] windows in a row while the node was squeezed,
/// or in the first one when a choke was lifted only a short while ago ([`CHOKE_MEMORY`]: the
/// overload it ended is still there). Lifted after [`st.choke_hold`](State) without a refusal.
fn choke(st: &mut State, now: Instant, refusing: bool, squeezed: bool) {
    st.squeezed_run = if refusing && squeezed {
        st.squeezed_run + 1
    } else {
        0
    };
    if refusing && squeezed && !st.choked {
        let soon = st
            .choke_lifted
            .is_some_and(|t| now.saturating_duration_since(t) < CHOKE_MEMORY);
        if soon || st.squeezed_run >= CHOKE_AFTER {
            st.rechokes = if soon { st.rechokes + 1 } else { 0 };
            st.choke_hold = (CHOKE_HOLD * 2u32.saturating_pow(st.rechokes)).min(CHOKE_HOLD_MAX);
            st.choked = true;
        }
    }
    if st.choked
        && !refusing
        && st
            .refusing_at
            .is_none_or(|t| now.saturating_duration_since(t) >= st.choke_hold)
    {
        st.choked = false;
        st.choke_lifted = Some(now);
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
        let (g, mut now, mut refused) = based_gauge();
        run_refusing(&g, &mut now, 10, 300, &mut refused, 0);
        // A keyframe fanned out to a class: one window of refusals at a modest rate.
        run_refusing(&g, &mut now, 1, 300, &mut refused, 20_000);
        run_refusing(&g, &mut now, 10, 300, &mut refused, 0);
        assert_eq!(g.ceiling_bps(), 0);
        assert!(!g.squeezed());
        // Windows that are not in a row do not add up, however many there are.
        for _ in 0..4 {
            run_refusing(&g, &mut now, 2, 600, &mut refused, 20_000);
            run_refusing(&g, &mut now, 1, 600, &mut refused, 0);
        }
        assert_eq!(g.ceiling_bps(), 0, "the windows were not in a row");
        // Three in a row are a ceiling.
        run_refusing(&g, &mut now, 3, 600, &mut refused, 20_000);
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
        assert_eq!(Gauge::wire_len(100, v4, false), 142);
        assert_eq!(Gauge::wire_len(100, v6, false), 162);
        assert_eq!(Gauge::wire_len(100, v4, true), 154);
        g.add(142 + 162 + 154, 3);
        assert_eq!(g.bytes.load(Ordering::Relaxed), 458);
        assert_eq!(g.packets.load(Ordering::Relaxed), 3);
    }

    /// A gauge that has taken the kernel's count as its base (the first window only does that).
    fn based_gauge() -> (Gauge, Instant, u64) {
        let g = Gauge::default();
        let now = Instant::now();
        g.update_with(now, 0, 0, Some(0));
        (g, now, 0)
    }

    #[test]
    fn refusals_that_go_on_after_the_squeeze_choke_the_node() {
        let (g, mut now, mut refused) = based_gauge();
        run_refusing(&g, &mut now, 1, 700, &mut refused, 0);
        run_refusing(&g, &mut now, 3, 700, &mut refused, 5_000);
        assert!(g.squeezed() && g.refusing(now));
        assert!(!g.choked(now), "the squeeze has just begun");
        // The bits fell by half and the kernel refuses as many datagrams as before (voice).
        run_refusing(&g, &mut now, 4, 350, &mut refused, 5_000);
        assert!(g.choked(now), "five windows of refusals while squeezed");
        // It stops refusing: the node stays choked until a calm spell has passed.
        run_refusing(&g, &mut now, 8, 350, &mut refused, 0);
        assert!(g.choked(now) && g.refusing(now));
        run_refusing(&g, &mut now, 3, 350, &mut refused, 0);
        assert!(
            !g.choked(now) && !g.refusing(now),
            "ten seconds without a refusal"
        );
    }

    #[test]
    fn two_bursts_a_few_seconds_apart_neither_squeeze_nor_choke() {
        // A keyframe fanned out to a class overflows the queue for one window; joins and loss make
        // them recur. None of it is a ceiling and none of it is a reason to silence voices.
        for gap in [3, 5, 9] {
            let (g, mut now, mut refused) = based_gauge();
            run_refusing(&g, &mut now, 10, 300, &mut refused, 0);
            for _ in 0..6 {
                run_refusing(&g, &mut now, 1, 300, &mut refused, 20_000);
                assert!(!g.choked(now));
                run_refusing(&g, &mut now, gap, 300, &mut refused, 0);
                assert!(!g.choked(now), "gap {gap}");
            }
            assert!(!g.squeezed(), "gap {gap}");
            assert_eq!(g.ceiling_bps(), 0);
        }
    }

    #[test]
    fn a_stall_across_a_window_boundary_is_not_a_ceiling() {
        // A fraction of a second of the machine not sending (steal, a full ring) refuses a great
        // many datagrams, split by the window boundary into two windows.
        let (g, mut now, mut refused) = based_gauge();
        run_refusing(&g, &mut now, 10, 72, &mut refused, 0);
        run_refusing(&g, &mut now, 2, 72, &mut refused, 30_000);
        run_refusing(&g, &mut now, 10, 72, &mut refused, 0);
        assert_eq!(g.ceiling_bps(), 0, "72 Mbit/s taught as a ceiling");
        assert!(!g.squeezed());
    }

    #[test]
    fn refusals_are_measured_against_what_is_sent() {
        let (g, mut now, mut refused) = based_gauge();
        // 300 thousand datagrams a second; the kernel refuses 600 of them (0.2 %).
        for _ in 0..12 {
            g.add(300_000 * 200, 300_000);
            now += Duration::from_secs(1);
            refused += 600;
            g.update_with(now, 0, 0, Some(refused));
        }
        assert!(!g.refusing(now) && !g.squeezed());
        // 3,000 a second is 1 %.
        for _ in 0..4 {
            g.add(300_000 * 200, 300_000);
            now += Duration::from_secs(1);
            refused += 3_000;
            g.update_with(now, 0, 0, Some(refused));
        }
        assert!(g.refusing(now) && g.squeezed());
    }

    #[test]
    fn a_steady_overload_does_not_cycle_between_choked_and_not() {
        let (g, mut now, mut refused) = based_gauge();
        run_refusing(&g, &mut now, 1, 700, &mut refused, 0);
        run_refusing(&g, &mut now, 8, 700, &mut refused, 5_000);
        assert!(g.choked(now));
        // The choke lifts after ten calm seconds; the two voices that come back overflow again.
        run_refusing(&g, &mut now, 11, 300, &mut refused, 0);
        assert!(!g.choked(now));
        run_refusing(&g, &mut now, 1, 400, &mut refused, 5_000);
        assert!(
            g.choked(now),
            "back at the first refusing window, not five later"
        );
        // And is held twice as long this time.
        run_refusing(&g, &mut now, 11, 300, &mut refused, 0);
        assert!(g.choked(now), "held for twenty seconds the second time");
        run_refusing(&g, &mut now, 10, 300, &mut refused, 0);
        assert!(!g.choked(now));
        // A long calm spell forgets it.
        run_refusing(&g, &mut now, 70, 300, &mut refused, 0);
        run_refusing(&g, &mut now, 1, 700, &mut refused, 5_000);
        assert!(!g.choked(now), "a new overload starts from the beginning");
    }

    #[test]
    fn what_the_kernel_refused_ages_without_anyone_updating_the_gauge() {
        // The last session leaves while the kernel is refusing: nothing updates the gauge again,
        // and whoever asks (recording uploads, /system) must not read a stale yes.
        let (g, mut now, mut refused) = based_gauge();
        run_refusing(&g, &mut now, 1, 700, &mut refused, 0);
        run_refusing(&g, &mut now, 8, 700, &mut refused, 5_000);
        assert!(g.refusing(now) && g.choked(now));
        assert!(g.refusing(now + Duration::from_secs(9)));
        assert!(!g.refusing(now + Duration::from_secs(11)));
        assert!(!g.choked(now + Duration::from_secs(11)));
    }

    #[test]
    fn a_ceiling_learned_before_a_long_idle_spell_is_gone_by_the_morning() {
        let (g, mut now, mut refused) = based_gauge();
        run_refusing(&g, &mut now, 1, 70, &mut refused, 0);
        run_refusing(&g, &mut now, 4, 70, &mut refused, 5_000);
        let learned = g.ceiling_bps();
        assert!(learned > 0 && g.squeezed());
        // Twelve hours with nobody in a call, then the first class.
        now += Duration::from_secs(12 * 3600);
        g.add(70 * MBIT, 70 * MBIT / 1_200);
        g.update_with(now, 0, 0, Some(refused));
        assert!(
            g.ceiling_bps() > 100 * learned,
            "{} Mbit/s after a night, from {}",
            g.ceiling_bps() / 1_000_000,
            learned / 1_000_000
        );
        run_refusing(&g, &mut now, 30, 400, &mut refused, 0);
        assert!(
            !g.squeezed(),
            "400 Mbit/s the next morning is not held back"
        );
    }

    #[test]
    fn a_short_run_of_refusals_squeezes_without_choking() {
        let (g, mut now, mut refused) = based_gauge();
        run_refusing(&g, &mut now, 1, 700, &mut refused, 0);
        run_refusing(&g, &mut now, 4, 700, &mut refused, 5_000);
        run_refusing(&g, &mut now, 20, 300, &mut refused, 0);
        assert!(g.squeezed());
        assert!(!g.choked(now));
        assert!(!g.refusing(now));
        // A second run later is a new one, not a continuation of the first.
        run_refusing(&g, &mut now, 4, 700, &mut refused, 5_000);
        assert!(!g.choked(now));
    }
}
