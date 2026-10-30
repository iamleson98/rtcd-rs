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
//! What is counted is what the shards hand to their sockets, one datagram at a time, headers up
//! to the link layer included: media after encryption, receiver reports, retransmissions, ICE
//! and DTLS, all of it, as the network card carries it. The service ticks the gauge every second
//! whether or not a session is left, so an empty node reports no rate and no squeeze.
//!
//! # Adapting to the kernel's refusals
//!
//! Counting and reporting is always on. What *acts* on the refusals (the learned ceiling, the
//! squeeze against a stall, the choke, the slower uploads) is on only for a gauge made with
//! [`Gauge::adaptive`]; otherwise the squeeze follows a configured budget and nothing else. When
//! on, in the order a refusing run grows:
//!
//! - A window *refuses* when the kernel refused at least [`DROPS_PER_SEC`] datagrams a second and
//!   [`REFUSAL_SHARE`] of what was sent. Two in a row slow the recording uploads for as long as the
//!   refusals last and [`REFUSING_FOR`] after ([`Gauge::refusing`]).
//! - Three in a row squeeze video and voices to the small layers and two for as long as they last
//!   and [`REFUSING_FOR`] after: a stall of the machine or the card (a hypervisor pause, a full
//!   ring) lasts seconds, is no sign of a rate that is too high, and teaches nothing. A squeeze that
//!   comes back within [`SQUEEZE_AGAIN`] is held twice as long, so a demand just over the card's
//!   limit does not switch layers and voices every few seconds.
//! - [`OVERLOAD_AFTER`] in a row is longer than a stall plausibly lasts: an overload. If the node
//!   sends at least the *learn floor* in bits or in packets (below it refusals are never taken for
//!   an overload: a node that small is not at the card's limit), the ceiling becomes 90 % of the
//!   most it sent in the run, before the squeeze took it down, and the squeeze is held by it from
//!   then on: while the rate is near the ceiling, and until it has been well under it for a while.
//! - A squeeze the node decided on (a budget, a ceiling) that the kernel goes on refusing through
//!   for [`CHOKE_AFTER`] windows in a row *chokes* the node: calls forward the loudest voice only
//!   until the kernel has accepted everything for [`CHOKE_HOLD`], which doubles for a choke that
//!   comes back within [`CHOKE_MEMORY`], up to [`CHOKE_HOLD_MAX`]. A clean window between the
//!   refusals starts the count again, so an overload that stutters never chokes: the squeeze and
//!   the ceiling are what hold it then. And a stall that happens to fall in a squeeze from a real
//!   ceiling and lasts [`CHOKE_AFTER`] windows costs one voice per call for ten seconds.
//! - The ceiling is lifted a tenth for every [`GROW_AFTER`] without a refusal, forgotten once it is
//!   [`FORGET_AT`] times what was taught, and taught afresh by an overload that finds the node not
//!   squeezed (the ceiling it had did not protect it).
//!
//! Everything here is a heuristic about a signal, the kernel's refusals, that is the machine's and
//! not only the node's; it is off until a run on the real card has shown it right.

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
/// A window refuses when the kernel refused at least this many datagrams a second...
const DROPS_PER_SEC: f64 = 50.0;
/// ...and at least this share of the datagrams sent in it (a node sending 300 thousand a second
/// that loses fifty is not at its ceiling). The kernel counts calls, not segments: with offload
/// one refused call is up to 64 datagrams and counts as one, so the share is read low, which is
/// the safe way.
const REFUSAL_SHARE: f64 = 0.005;
/// Refusing windows in a row that slow the uploads, that squeeze, and that are an overload.
const YIELD_AFTER: u32 = 2;
const STALL_AFTER: u32 = 3;
const OVERLOAD_AFTER: u32 = 15;
/// The learned ceiling is this share of the rate the node carried when the kernel refused.
const LEARNED_SHARE: f64 = 0.9;
/// A refusal at a rate this close to the learned ceiling, or above it, teaches a lower one when
/// the node was squeezed already; below it the ceiling was not what was refused.
const RELEARN_NEAR: f64 = 0.95;
/// A learned ceiling is lifted by [`GROW_BY`] after this long without a refusal.
const GROW_AFTER: Duration = Duration::from_secs(300);
const GROW_BY: f64 = 1.1;
/// At most this many notches at once: far beyond any ceiling, and finite.
const MAX_GROW_STEPS: f64 = 100.0;
/// A lifted ceiling is forgotten once it is this many times what was taught: it has stopped
/// meaning anything, and an overload then teaches a new one from scratch.
const FORGET_AT: f64 = 2.0;
/// The kernel refused in this many windows in a row while the node was squeezed by a decision (and
/// the squeeze did not help): the node is choked.
const CHOKE_AFTER: u32 = 5;
/// A choke is lifted when the kernel has accepted everything for this long...
const CHOKE_HOLD: Duration = Duration::from_secs(10);
/// ...twice as long each time it returns within [`CHOKE_MEMORY`], up to this.
const CHOKE_HOLD_MAX: Duration = Duration::from_secs(120);
/// A choke returning within this long of the last one is the same overload.
const CHOKE_MEMORY: Duration = Duration::from_secs(60);
/// The kernel is taken to be refusing, and a stall squeeze lasts, this long after the last
/// refusing window.
const REFUSING_FOR: Duration = Duration::from_secs(10);
/// A squeeze that comes back within this long of its release is the same overload, and the next
/// release waits twice as long (up to [`RELEASE_MAX`]).
const SQUEEZE_AGAIN: Duration = Duration::from_secs(120);
const RELEASE_MAX: Duration = Duration::from_secs(300);
/// The Ethernet header, which the network card's counters include; the IP header (20 bytes, 40
/// for IPv6) and the UDP (8) or TCP (20) one are added per datagram.
const LINK_OVERHEAD: u64 = 14;

#[derive(Debug, Default)]
struct State {
    at: Option<Instant>,
    bytes: u64,
    packets: u64,
    rate_bps: f64,
    rate_pps: f64,
    /// Squeezed by a budget or the learned ceiling (see `decide`).
    squeezed: bool,
    calm_since: Option<Instant>,
    /// What the kernel's refusals taught: the ceilings in bits and packets per second (0: none
    /// yet), what was taught (to tell when a lifted ceiling is no longer that), and when the
    /// ceiling was last taught or lifted.
    learned_bps: f64,
    learned_pps: f64,
    taught_bps: f64,
    taught_pps: f64,
    ceiling_at: Option<Instant>,
    /// The kernel's count at the last window, to tell what it refused during this one; its value
    /// when the node first read it (refusals before that were not ours); which counters make it up
    /// (see [`Drops`]); and what had been reported when that last changed.
    last_drops: Option<u64>,
    drops_base: Option<u64>,
    drop_sources: u8,
    drops_offset: u64,
    /// Windows in a row in which the kernel refused enough to matter, and of those how many were
    /// while the squeeze was a decided one.
    run: u32,
    squeezed_run: u32,
    /// Whether the node was squeezed when the run began (if not, the ceiling it had did not
    /// protect it), and the most it sent during the run.
    run_began_squeezed: bool,
    run_peak: (f64, f64),
    /// The last window that refused, and the last in which the kernel was refusing (two in a row,
    /// for the uploads).
    refused_at: Option<Instant>,
    refusing_at: Option<Instant>,
    /// Squeezed until then against a stall, and for how long after the last refusal.
    stall_until: Option<Instant>,
    stall_hold: Duration,
    /// How long a calm spell releases a decided squeeze, when it last was released, and whether
    /// the last window was squeezed by a decision.
    release_hold: Duration,
    released_at: Option<Instant>,
    decided: bool,
    /// Whether the node is choked, for how long a calm spell lifts it, when it last was lifted,
    /// and how many times in a row it has come back soon after.
    choked: bool,
    choke_hold: Duration,
    choke_lifted: Option<Instant>,
    rechokes: u32,
}

/// The kernel's count of refused datagrams and what it is the sum of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Drops {
    count: u64,
    /// A bit for each counter in the sum (IPv4, IPv6). A counter that appears or goes away moves
    /// the total without a single datagram being refused, so a change starts the reading afresh.
    sources: u8,
}

/// Where the kernel's count of refused datagrams comes from.
struct DropsSource(Box<dyn Fn() -> Option<Drops> + Send + Sync>);

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
    /// The learn floor in bits and packets per second, if the gauge acts on the kernel's refusals.
    adapt: Option<(f64, f64)>,
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
    /// itself when nothing updates the gauge.
    refusing_until: AtomicU64,
    choked_until: AtomicU64,
    /// The ceilings the node learned (0 until it has).
    ceiling: AtomicU64,
    ceiling_packets: AtomicU64,
}

impl Gauge {
    /// A gauge that acts on the kernel's refusals (see the module documentation), below a *learn
    /// floor*: refusals while the node sends under `learn_min_bps` bits and under `learn_min_pps`
    /// packets a second are never taken for an overload.
    pub fn adaptive(learn_min_bps: u64, learn_min_pps: u64) -> Self {
        Self::default().adapting(learn_min_bps, learn_min_pps)
    }

    /// This gauge, acting on the kernel's refusals (see [`Gauge::adaptive`]).
    pub fn adapting(mut self, learn_min_bps: u64, learn_min_pps: u64) -> Self {
        self.adapt = Some((learn_min_bps as f64, learn_min_pps as f64));
        self
    }

    /// Whether the gauge acts on the kernel's refusals, or only counts and reports them.
    pub fn adapts(&self) -> bool {
        self.adapt.is_some()
    }

    /// A gauge that reads the count of refused datagrams from `source` instead of the system's
    /// (`None`: it does not say), for a platform that keeps the count elsewhere and for tests,
    /// which cannot make a kernel refuse.
    pub fn with_kernel_drops(source: impl Fn() -> Option<u64> + Send + Sync + 'static) -> Self {
        Self {
            drops_source: DropsSource(Box::new(move || {
                source().map(|count| Drops { count, sources: 1 })
            })),
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
    /// learned it (0 until it has, and always for a gauge that does not adapt).
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

    /// Whether the kernel is refusing datagrams at `now`: two windows in a row refused, and the
    /// last was within [`REFUSING_FOR`]. Reported whether or not the gauge adapts.
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
    /// A budget of 0 is not set: the node's learned ceiling, if it adapts, stands in for it.
    pub fn update(&self, now: Instant, budget_bps: u64, budget_pps: u64) {
        // Every session calls this once a second, and the kernel's count is a file to read and
        // parse: only the call that finds a window over pays for it.
        if !self.due(now) {
            return;
        }
        self.update_counted(now, budget_bps, budget_pps, (self.drops_source.0)());
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
    #[cfg(test)]
    fn update_with(&self, now: Instant, budget_bps: u64, budget_pps: u64, drops: Option<u64>) {
        let drops = drops.map(|count| Drops { count, sources: 1 });
        self.update_counted(now, budget_bps, budget_pps, drops);
    }

    fn update_counted(&self, now: Instant, budget_bps: u64, budget_pps: u64, drops: Option<Drops>) {
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
        let sent = packets.saturating_sub(st.packets);
        let smooth = |old: f64, instant: f64| {
            if old == 0.0 {
                instant
            } else {
                (old + instant) / 2.0
            }
        };
        st.rate_bps = smooth(
            st.rate_bps,
            total.saturating_sub(st.bytes) as f64 * 8.0 / secs,
        );
        st.rate_pps = smooth(st.rate_pps, sent as f64 / secs);
        st.at = Some(now);
        st.bytes = total;
        st.packets = packets;
        self.rate.store(st.rate_bps as u64, Ordering::Relaxed);
        self.rate_packets
            .store(st.rate_pps as u64, Ordering::Relaxed);
        let refused = drops.map_or(0, |d| {
            if d.sources != st.drop_sources {
                // A counter appeared or went away (IPv6 turned on): its whole total is not
                // refusals of this window. Start again from here, keeping what was reported.
                st.drops_offset = self.kernel_drops.load(Ordering::Relaxed);
                st.drop_sources = d.sources;
                st.drops_base = None;
                st.last_drops = None;
            }
            let d = d.count;
            let base = *st.drops_base.get_or_insert(d);
            self.kernel_drops
                .store(st.drops_offset + d.saturating_sub(base), Ordering::Relaxed);
            let before = st.last_drops.replace(d).unwrap_or(d);
            d.saturating_sub(before)
        });

        let refusing =
            refused as f64 / secs >= (REFUSAL_SHARE * sent as f64 / secs).max(DROPS_PER_SEC);
        if sent == 0 {
            // Nothing was sent: no session is left, and an empty node is neither squeezed nor
            // choked nor in the middle of anything (the ceiling it learned ages on its own).
            quiet(&mut st);
        } else if refusing {
            if st.run == 0 {
                st.run_began_squeezed = st.squeezed;
                st.run_peak = (0.0, 0.0);
            }
            st.run += 1;
            st.refused_at = Some(now);
            st.run_peak = (
                st.run_peak.0.max(st.rate_bps),
                st.run_peak.1.max(st.rate_pps),
            );
            if st.run >= YIELD_AFTER {
                st.refusing_at = Some(now);
            }
        } else {
            st.run = 0;
        }

        if let Some(floor) = self.adapt {
            learn(&mut st, now, refusing, floor);
            stall(&mut st, now);
        }
        self.ceiling.store(st.learned_bps as u64, Ordering::Relaxed);
        self.ceiling_packets
            .store(st.learned_pps as u64, Ordering::Relaxed);
        let (limit_bps, limit_pps) = (
            lower_of(budget_bps, st.learned_bps),
            lower_of(budget_pps, st.learned_pps),
        );
        let decided = decide(&mut st, now, limit_bps, limit_pps);
        let mut squeezed = decided;
        if self.adapt.is_some() {
            note_squeeze(&mut st, now, decided);
            squeezed |= st.stall_until.is_some_and(|t| now < t);
            // Only a squeeze the node decided on (a budget, a ceiling) is one that may fail to be
            // enough; one against a stall lasts as long as the stall.
            choke(&mut st, now, refusing && sent > 0, decided);
        }
        self.squeezed.store(squeezed, Ordering::Relaxed);
        let until = |t: Option<Instant>, hold: Duration| t.map_or(0, |t| self.nanos(t + hold));
        self.refusing_until
            .store(until(st.refusing_at, REFUSING_FOR), Ordering::Relaxed);
        let choked_until = if st.choked {
            until(st.refused_at, st.choke_hold)
        } else {
            0
        };
        self.choked_until.store(choked_until, Ordering::Relaxed);
    }
}

/// The kernel's count of UDP datagrams it could not queue for sending, IPv4 and IPv6 together
/// (`SndbufErrors` in `/proc/net/snmp`, `Udp6SndbufErrors` in `/proc/net/snmp6`), where the
/// system has one.
///
/// It is the network namespace's, so with `hostNetwork` it counts every process on the machine
/// that sends UDP, and the node's reaction (small video, fewer voices, slower uploads) answers a
/// refusal that may not be its own. It counts calls, not datagrams: a segmented send of up to 64
/// datagrams that fails adds one, so the rate it gives is low, the safe way.
fn udp_send_errors() -> Option<Drops> {
    #[cfg(target_os = "linux")]
    {
        let read = |path| std::fs::read_to_string(path).ok();
        let v4 = read("/proc/net/snmp").and_then(|t| parse_snmp(&t));
        let v6 = read("/proc/net/snmp6").and_then(|t| parse_snmp6(&t));
        (v4.is_some() || v6.is_some()).then(|| Drops {
            count: v4.unwrap_or(0) + v6.unwrap_or(0),
            sources: u8::from(v4.is_some()) | u8::from(v6.is_some()) << 1,
        })
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// `SndbufErrors` of the `Udp:` lines of `/proc/net/snmp` (a line of names, then one of values).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_snmp(snmp: &str) -> Option<u64> {
    let mut lines = snmp.lines().filter(|l| l.starts_with("Udp:"));
    let names = lines.next()?;
    let values = lines.next()?;
    let at = names.split_whitespace().position(|n| n == "SndbufErrors")?;
    values.split_whitespace().nth(at)?.parse().ok()
}

/// `Udp6SndbufErrors` of `/proc/net/snmp6` (a name and a value on each line).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_snmp6(snmp6: &str) -> Option<u64> {
    snmp6.lines().find_map(|l| {
        let mut f = l.split_whitespace();
        (f.next()? == "Udp6SndbufErrors").then(|| f.next()?.parse().ok())?
    })
}

/// The lower of a configured budget (0: none) and a learned ceiling (0: none yet); 0 if neither.
fn lower_of(budget: u64, learned: f64) -> u64 {
    match (budget, learned as u64) {
        (0, l) => l,
        (b, 0) => b,
        (b, l) => b.min(l),
    }
}

/// Nothing was sent in the window: no session is left.
fn quiet(st: &mut State) {
    st.run = 0;
    st.squeezed_run = 0;
    st.squeezed = false;
    st.calm_since = None;
    st.stall_until = None;
    st.refusing_at = None;
    st.refused_at = None;
    st.release_hold = RELEASE_AFTER;
    st.choked = false;
    st.choke_lifted = None;
}

/// What an overload teaches, given whether the window just measured refused (`st.run` counts the
/// refusing windows in a row up to it) and the learn floor: after [`OVERLOAD_AFTER`] of them at a
/// rate over the floor, a ceiling at 90 % of the most the node sent in the run (before the squeeze
/// took it down), with the squeeze at once. A node that was
/// not squeezed when the run began was not protected by the ceiling it had, whatever it was, and
/// learns afresh; one that was squeezed learns only a lower rate (a refusal near or above the
/// ceiling), the squeeze being what answers the rest. A ceiling is lifted a notch for every
/// [`GROW_AFTER`] spent without refusals, however many went by since it was last looked at, and
/// forgotten once it is [`FORGET_AT`] times what was taught.
fn learn(st: &mut State, now: Instant, refusing: bool, floor: (f64, f64)) {
    if refusing {
        if st.run < OVERLOAD_AFTER || (st.rate_bps < floor.0 && st.rate_pps < floor.1) {
            return;
        }
        let near = |rate: f64, learned: f64| learned == 0.0 || rate >= learned * RELEARN_NEAR;
        let none = st.learned_bps == 0.0 && st.learned_pps == 0.0;
        let fresh = none || !st.run_began_squeezed;
        if fresh || near(st.rate_bps, st.learned_bps) || near(st.rate_pps, st.learned_pps) {
            // What the node sent when the card began to refuse it, before the squeeze took it
            // down; a node already squeezed that is refused still has only the rate it has now.
            let base = if fresh {
                st.run_peak
            } else {
                (st.rate_bps, st.rate_pps)
            };
            st.learned_bps = LEARNED_SHARE * base.0;
            st.learned_pps = LEARNED_SHARE * base.1;
            st.taught_bps = st.learned_bps;
            st.taught_pps = st.learned_pps;
        }
        st.squeezed = true;
        // From here on the node is squeezed, the rest of the run included.
        st.run_began_squeezed = true;
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
            if st.learned_bps > FORGET_AT * st.taught_bps
                || st.learned_pps > FORGET_AT * st.taught_pps
            {
                st.learned_bps = 0.0;
                st.learned_pps = 0.0;
                st.ceiling_at = None;
            }
        }
    }
}

/// The squeeze against a stall: from [`STALL_AFTER`] refusing windows in a row until
/// [`REFUSING_FOR`] after the last, held twice as long (up to [`RELEASE_MAX`]) when it comes back
/// within [`SQUEEZE_AGAIN`] of its end.
fn stall(st: &mut State, now: Instant) {
    if st.run < STALL_AFTER {
        return;
    }
    if !st.stall_until.is_some_and(|t| now < t) {
        let soon = st
            .stall_until
            .is_some_and(|t| now.saturating_duration_since(t) < SQUEEZE_AGAIN);
        st.stall_hold = if soon {
            (st.stall_hold * 2).min(RELEASE_MAX)
        } else {
            REFUSING_FOR
        };
    }
    st.stall_until = Some(now + st.stall_hold);
}

/// Keeps the dwell before a release: a squeeze that returns soon after being released is the same
/// overload and is held twice as long the next time.
fn note_squeeze(st: &mut State, now: Instant, decided: bool) {
    if decided && !st.decided {
        let soon = st
            .released_at
            .is_some_and(|t| now.saturating_duration_since(t) < SQUEEZE_AGAIN);
        st.release_hold = if soon {
            (st.release_hold * 2).min(RELEASE_MAX)
        } else {
            RELEASE_AFTER
        };
    } else if !decided && st.decided {
        st.released_at = Some(now);
    }
    st.decided = decided;
}

/// Choked: the kernel refused in [`CHOKE_AFTER`] windows in a row while the node was squeezed by a
/// decision, or in the first one when a choke was lifted only a short while ago
/// ([`CHOKE_MEMORY`]: the overload it ended is still there). Lifted after `choke_hold` without a
/// refusal.
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
            .refused_at
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
            if now.saturating_duration_since(since) >= st.release_hold.max(RELEASE_AFTER) {
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

    /// A gauge, its clock and the kernel's count, one window a call.
    struct Rig {
        g: Gauge,
        now: Instant,
        refused: u64,
    }

    impl Rig {
        /// The gauge's first update only starts its clock and the next takes the kernel's count as
        /// the base, so a rig begins with both done (an empty window).
        fn new(g: Gauge) -> Rig {
            let mut rig = Rig {
                g,
                now: Instant::now(),
                refused: 1_000,
            };
            rig.g.update_with(rig.now, 0, 0, Some(rig.refused));
            rig.now += Duration::from_secs(1);
            rig.g.update_with(rig.now, 0, 0, Some(rig.refused));
            rig
        }

        fn adaptive() -> Rig {
            Rig::new(Gauge::default().adapting(0, 0))
        }

        /// One window sending `bytes` in `packets` (budgets given), the kernel refusing `refused`.
        fn window_with(&mut self, bytes: u64, packets: u64, refused: u64, budget: (u64, u64)) {
            self.g.add(bytes, packets);
            self.now += Duration::from_secs(1);
            self.refused += refused;
            self.g
                .update_with(self.now, budget.0, budget.1, Some(self.refused));
        }

        /// `secs` windows at `mbps` of 1,200-byte datagrams, the kernel refusing `per_sec` a second.
        fn run(&mut self, secs: u64, mbps: u64, per_sec: u64) {
            for _ in 0..secs {
                self.window_with(mbps * MBIT, (mbps * MBIT).div_ceil(1_200), per_sec, (0, 0));
            }
        }

        /// `secs` windows at `pps` datagrams of 200 bytes.
        fn run_pps(&mut self, secs: u64, pps: u64, per_sec: u64) {
            for _ in 0..secs {
                self.window_with(pps * 200, pps, per_sec, (0, 0));
            }
        }

        fn refusing(&self) -> bool {
            self.g.refusing(self.now)
        }

        fn choked(&self) -> bool {
            self.g.choked(self.now)
        }
    }

    #[test]
    fn the_rate_is_measured_whether_or_not_there_is_a_budget() {
        let mut rig = Rig::new(Gauge::default());
        rig.run(5, 100, 0);
        let rate = rig.g.rate_bps() / 1_000_000;
        assert!((95..=105).contains(&rate), "rate {rate} Mbit/s");
        assert!(!rig.g.squeezed());
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

    // ------------------------------------------------------------------------------- budgets

    /// Runs `secs` windows at `mbps` under a byte budget of `budget_mbps`.
    fn run_budget(rig: &mut Rig, secs: u64, mbps: u64, budget_mbps: u64) {
        for _ in 0..secs {
            rig.window_with(
                mbps * MBIT,
                (mbps * MBIT).div_ceil(1_200),
                0,
                (budget_mbps * 1_000_000, 0),
            );
        }
    }

    #[test]
    fn video_is_squeezed_near_the_budget_and_released_after_a_calm_spell() {
        let mut rig = Rig::new(Gauge::default());
        run_budget(&mut rig, 5, 100, 300);
        assert!(!rig.g.squeezed(), "a third of the budget");
        run_budget(&mut rig, 5, 280, 300);
        assert!(rig.g.squeezed(), "93 % of the budget");
        run_budget(&mut rig, 10, 150, 300);
        assert!(
            rig.g.squeezed(),
            "the rate fell but the calm spell is not over"
        );
        run_budget(&mut rig, 30, 150, 300);
        assert!(!rig.g.squeezed(), "half the budget for long enough");
    }

    #[test]
    fn a_rate_hovering_between_the_marks_does_not_release() {
        let mut rig = Rig::new(Gauge::default());
        run_budget(&mut rig, 6, 290, 300);
        assert!(rig.g.squeezed());
        // 75 % is under the squeeze mark but over the release mark.
        run_budget(&mut rig, 60, 225, 300);
        assert!(rig.g.squeezed());
    }

    #[test]
    fn no_budget_never_squeezes() {
        let mut rig = Rig::new(Gauge::default());
        rig.run(10, 900, 0);
        assert!(!rig.g.squeezed());
    }

    #[test]
    fn the_packet_rate_squeezes_on_its_own() {
        // Voice: 100,000 packets of 100 bytes a second is 80 Mbit/s, far under the byte budget,
        // and the whole of a 110,000-packet budget.
        let mut rig = Rig::new(Gauge::default());
        for _ in 0..5 {
            rig.window_with(100_000 * 100, 100_000, 0, (600_000_000, 110_000));
        }
        assert!(
            rig.g.squeezed(),
            "91 % of the packet budget, 13 % of the byte budget"
        );
        assert_eq!(rig.g.rate_pps(), 100_000);
        for _ in 0..30 {
            rig.window_with(30_000 * 100, 30_000, 0, (600_000_000, 110_000));
        }
        assert!(
            !rig.g.squeezed(),
            "a quarter of either budget for long enough"
        );
    }

    #[test]
    fn a_budget_of_zero_is_not_enforced() {
        let mut rig = Rig::new(Gauge::default());
        // Only the byte budget is set: a packet rate far beyond any sensible budget does not matter.
        for _ in 0..10 {
            rig.window_with(10 * MBIT, 900_000, 0, (600_000_000, 0));
        }
        assert!(!rig.g.squeezed());
        assert_eq!(rig.g.rate_pps(), 900_000);
    }

    #[test]
    fn a_node_that_was_squeezed_and_then_goes_quiet_is_released_at_once() {
        let mut rig = Rig::new(Gauge::default());
        run_budget(&mut rig, 6, 290, 300);
        assert!(rig.g.squeezed());
        // The last session leaves; the service goes on ticking an empty gauge.
        run_budget(&mut rig, 1, 0, 300);
        assert!(!rig.g.squeezed(), "squeezed with nothing sent");
        run_budget(&mut rig, 40, 0, 300);
        assert_eq!(rig.g.rate_bps(), 0);
        assert_eq!(rig.g.rate_pps(), 0);
    }

    // ------------------------------------------------------------- the kernel's count

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
    fn the_refusal_counts_of_both_families_are_read() {
        // As Linux 6 prints them (shortened).
        let snmp = "Ip: Forwarding DefaultTTL\nIp: 1 64\n\
Udp: InDatagrams NoPorts InErrors OutDatagrams RcvbufErrors SndbufErrors InCsumErrors IgnoredMulti MemErrors\n\
Udp: 10 2 0 9 1 777 0 0 0\n";
        assert_eq!(parse_snmp(snmp), Some(777));
        let snmp6 = "Ip6InReceives                   \t5\n\
Udp6InDatagrams                 \t3\n\
Udp6RcvbufErrors                \t0\n\
Udp6SndbufErrors                \t41\n\
Udp6InCsumErrors                \t0\n";
        assert_eq!(parse_snmp6(snmp6), Some(41));
        assert_eq!(parse_snmp("Udp: InDatagrams\nUdp: 1\n"), None);
        assert_eq!(parse_snmp6("Udp6InDatagrams 3\n"), None);
    }

    #[test]
    fn a_counter_that_appears_is_not_a_window_of_refusals() {
        fn window(g: &Gauge, now: &mut Instant, count: u64, sources: u8) {
            g.add(300 * MBIT, 300 * MBIT / 1_200);
            *now += Duration::from_secs(1);
            g.update_counted(*now, 0, 0, Some(Drops { count, sources }));
        }
        let g = Gauge::default().adapting(0, 0);
        let mut now = Instant::now();
        g.update_counted(
            now,
            0,
            0,
            Some(Drops {
                count: 1_000,
                sources: 1,
            }),
        );
        for _ in 0..5 {
            window(&g, &mut now, 1_000, 1);
        }
        // IPv6 turns on with a total of 90,000 already counted.
        for _ in 0..5 {
            window(&g, &mut now, 91_000, 3);
        }
        assert!(
            !g.refusing(now),
            "the counter's first value taken for refusals"
        );
        window(&g, &mut now, 91_500, 3);
        assert_eq!(g.kernel_send_drops(), 500, "what grew, in a single series");
    }

    // ------------------------------------------------- a gauge that only counts and reports

    #[test]
    fn a_gauge_that_does_not_adapt_reports_the_refusals_and_acts_on_none() {
        let mut rig = Rig::new(Gauge::default());
        rig.run(20, 300, 0);
        // A refusal storm for five minutes at a rate the node had never carried.
        rig.run(300, 700, 20_000);
        assert!(!rig.g.adapts());
        assert!(rig.refusing(), "the refusals are reported");
        assert_eq!(rig.g.kernel_send_drops(), 300 * 20_000);
        assert!(!rig.g.squeezed(), "nothing but a budget squeezes");
        assert!(!rig.choked());
        assert_eq!(rig.g.ceiling_bps(), 0);
        assert_eq!(rig.g.ceiling_pps(), 0);
        // And a budget still does, as before.
        for _ in 0..5 {
            rig.window_with(700 * MBIT, 700 * MBIT / 1_200, 0, (600_000_000, 0));
        }
        assert!(rig.g.squeezed());
    }

    // ------------------------------------------------------------- bursts and stalls

    #[test]
    fn the_uploads_wait_for_two_refusing_windows() {
        let mut rig = Rig::adaptive();
        rig.run(10, 300, 0);
        rig.run(1, 300, 20_000);
        assert!(!rig.refusing(), "one window is a burst");
        rig.run(1, 300, 0);
        rig.run(1, 300, 20_000);
        rig.run(1, 300, 20_000);
        assert!(rig.refusing());
        assert!(
            !rig.g.refusing(rig.now + Duration::from_secs(11)),
            "and ends by itself"
        );
    }

    #[test]
    fn bursts_a_few_seconds_apart_neither_squeeze_nor_choke() {
        // A keyframe fanned out to a class overflows the queue for one window; joins and loss make
        // them recur.
        for gap in [3, 5, 9] {
            let mut rig = Rig::adaptive();
            rig.run(10, 300, 0);
            for _ in 0..6 {
                rig.run(1, 300, 20_000);
                rig.run(gap, 300, 0);
                assert!(!rig.choked(), "gap {gap}");
            }
            assert!(!rig.g.squeezed(), "gap {gap}");
            assert_eq!(rig.g.ceiling_bps(), 0);
        }
    }

    #[test]
    fn a_stall_squeezes_while_it_lasts_and_teaches_nothing() {
        // The card stops taking packets for four seconds, at a rate the node carried for a while
        // and at one it had just reached: neither is a ceiling.
        for (before, during) in [(60_000, 60_000), (20_000, 45_000)] {
            let mut rig = Rig::adaptive();
            rig.run_pps(300, before, 0);
            rig.run_pps(4, during, during);
            assert!(rig.g.squeezed(), "video is small while the card is stalled");
            assert_eq!(rig.g.ceiling_pps(), 0, "a ceiling taught from the stall");
            rig.run_pps(15, during, 0);
            assert!(!rig.g.squeezed(), "squeezed after the stall was over");
            assert_eq!(rig.g.ceiling_pps(), 0);
        }
    }

    #[test]
    fn a_stall_while_the_rate_is_rising_from_nothing_teaches_nothing() {
        // A restarted node, or the first class of the morning: the clean history is empty or tiny.
        let mut rig = Rig::adaptive();
        for pps in (1..=40).map(|i| i * 1_000) {
            rig.run_pps(1, pps, 0);
        }
        rig.run_pps(4, 40_000, 40_000);
        rig.run_pps(30, 40_000, 0);
        assert_eq!(rig.g.ceiling_pps(), 0);
        assert!(!rig.g.squeezed());
    }

    #[test]
    fn a_stall_at_night_time_rates_teaches_nothing() {
        let mut rig = Rig::adaptive();
        rig.run_pps(300, 250, 0);
        rig.run_pps(5, 250, 250);
        rig.run_pps(30, 250, 0);
        assert_eq!(rig.g.ceiling_pps(), 0);
        assert!(!rig.g.squeezed());
    }

    #[test]
    fn a_long_stall_never_chokes_and_a_stall_that_returns_is_held_longer() {
        let mut rig = Rig::adaptive();
        rig.run_pps(300, 60_000, 0);
        let mut ever_choked = false;
        for _ in 0..14 {
            rig.run_pps(1, 60_000, 60_000);
            ever_choked |= rig.choked();
        }
        assert!(rig.g.squeezed() && !ever_choked);
        assert_eq!(rig.g.ceiling_pps(), 0);
        rig.run_pps(40, 60_000, 0);
        // Stalls of four seconds every thirty: the hold doubles, so the squeezes are fewer than
        // the stalls are long apart... and none of them teaches anything.
        let (mut onsets, mut was) = (0, false);
        for _ in 0..20 {
            rig.run_pps(4, 60_000, 60_000);
            for _ in 0..26 {
                rig.run_pps(1, 60_000, 0);
                if rig.g.squeezed() && !was {
                    onsets += 1;
                }
                was = rig.g.squeezed();
            }
            assert_eq!(rig.g.ceiling_pps(), 0);
            assert!(!rig.choked());
        }
        assert!(rig.g.squeezed() || onsets > 0);
        assert!(onsets <= 6, "{onsets} squeezes");
    }

    #[test]
    fn a_node_below_the_learn_floor_is_never_taught_however_long_it_is_refused() {
        let mut rig = Rig::new(Gauge::adaptive(400_000_000, 80_000));
        rig.run(60, 30, 0);
        rig.run(120, 30, 3_000);
        assert_eq!(rig.g.ceiling_bps(), 0);
        assert_eq!(rig.g.ceiling_pps(), 0);
        assert!(!rig.choked(), "only a decided squeeze can choke");
        assert!(rig.g.squeezed(), "but video is small while it lasts");
        rig.run(30, 30, 0);
        assert!(!rig.g.squeezed());
    }

    // ------------------------------------------------------------------ overloads

    #[test]
    fn an_overload_that_goes_on_teaches_the_ceiling_and_then_chokes() {
        // Voice: 100 thousand packets a second leave a card that takes 90 thousand, whatever the
        // squeeze does to the video.
        let mut rig = Rig::new(Gauge::adaptive(400_000_000, 80_000));
        rig.run_pps(300, 85_000, 0);
        let mut chokes = 0;
        for second in 0..60 {
            rig.run_pps(1, 100_000, 10_000);
            if second < 14 {
                assert_eq!(rig.g.ceiling_pps(), 0, "second {second}");
            }
            chokes += u32::from(rig.choked());
        }
        let ceiling = rig.g.ceiling_pps();
        assert!(
            (89_000..=91_000).contains(&ceiling),
            "0.9 of the 100 thousand it sent before the squeeze: {ceiling}"
        );
        assert!(rig.g.squeezed());
        assert!(rig.choked(), "the squeeze did not help");
        assert!(chokes > 30);
    }

    /// A card that takes `limit` Mbit/s, a demand that grows `per_cent` a second to `peak` times
    /// the limit, and `squeeze` times the demand once video is small.
    fn bandwidth_ramp(rig: &mut Rig, limit: f64, peak: f64, squeeze: f64, secs: u64) -> (u32, u32) {
        let (mut demand, mut onsets, mut was, mut chokes) = (limit / 5.0, 0, false, 0);
        for _ in 0..secs {
            let sent = if rig.g.squeezed() {
                demand * squeeze
            } else {
                demand
            };
            let over = (sent - limit).max(0.0);
            let packets = sent * 125_000.0 / 1_200.0;
            let refused = (over / sent * packets) as u64;
            rig.window_with((sent * 125_000.0) as u64, packets as u64, refused, (0, 0));
            if rig.g.squeezed() && !was {
                onsets += 1;
            }
            was = rig.g.squeezed();
            chokes += u32::from(rig.choked());
            demand = (demand * 1.01).min(peak * limit);
        }
        (onsets, chokes)
    }

    #[test]
    fn a_bandwidth_overload_that_the_squeeze_cures_does_not_cycle_every_few_seconds() {
        let mut rig = Rig::new(Gauge::adaptive(400_000_000, 80_000));
        let (onsets, chokes) = bandwidth_ramp(&mut rig, 500.0, 1.05, 0.6, 900);
        assert!(onsets <= 8, "{onsets} squeezes in 15 minutes");
        assert!(onsets >= 1);
        assert_eq!(chokes, 0, "nothing was refused through a squeeze");
    }

    #[test]
    fn a_bandwidth_overload_that_the_squeeze_does_not_cure_chokes() {
        let mut rig = Rig::new(Gauge::adaptive(400_000_000, 80_000));
        let (onsets, chokes) = bandwidth_ramp(&mut rig, 500.0, 1.05, 1.0, 400);
        assert!(chokes > 0, "refused for four minutes and never choked");
        assert!(onsets <= 3, "{onsets} squeezes");
        assert!(rig.g.ceiling_bps() > 400_000_000, "{}", rig.g.ceiling_bps());
    }

    #[test]
    fn a_ceiling_lifted_by_quiet_is_taught_again_by_the_overload_it_did_not_prevent() {
        let mut rig = Rig::new(Gauge::adaptive(0, 0));
        rig.run(300, 300, 0);
        rig.run(20, 700, 5_000);
        let first = rig.g.ceiling_bps();
        assert!(first > 0 && rig.g.squeezed());
        // Twenty quiet minutes lift it four notches; then the real ceiling refuses again.
        rig.run(1_200, 300, 0);
        let lifted = rig.g.ceiling_bps();
        assert!(lifted > first * 14 / 10, "{lifted} from {first}");
        assert!(!rig.g.squeezed());
        rig.run(20, 700, 5_000);
        let again = rig.g.ceiling_bps() as f64;
        assert!(
            (again - first as f64).abs() < 0.05 * first as f64,
            "{again} against {first}"
        );
        assert!(rig.g.squeezed());
    }

    #[test]
    fn a_ceiling_is_forgotten_by_the_morning_and_nothing_grows_without_bound() {
        let mut rig = Rig::new(Gauge::adaptive(0, 0));
        rig.run(300, 300, 0);
        rig.run(20, 700, 5_000);
        assert!(rig.g.ceiling_bps() > 0);
        // Twelve hours with nobody in a call, the service ticking an empty gauge every second.
        rig.run(12 * 3600, 0, 0);
        assert_eq!(rig.g.ceiling_bps(), 0, "forgotten overnight");
        assert!(!rig.g.squeezed());
        rig.run(300, 300, 0);
        rig.run(20, 700, 5_000);
        assert!(rig.g.ceiling_bps() > 0);
        for _ in 0..7 {
            rig.run(24 * 3600, 0, 0);
        }
        assert_eq!(rig.g.ceiling_bps(), 0);
    }

    #[test]
    fn a_node_that_empties_while_squeezed_and_choked_reports_neither() {
        let mut rig = Rig::new(Gauge::adaptive(0, 0));
        rig.run(300, 300, 0);
        rig.run(40, 700, 5_000);
        assert!(rig.g.squeezed() && rig.choked());
        rig.run(1, 0, 0);
        assert!(!rig.g.squeezed() && !rig.choked() && !rig.refusing());
    }

    // ---------------------------------------------------------------------- the choke

    #[test]
    fn a_calm_window_between_two_runs_starts_the_count_again() {
        let mut rig = Rig::adaptive();
        rig.run(10, 300, 0);
        rig.run(16, 700, 5_000);
        assert!(rig.g.squeezed() && !rig.choked());
        rig.run(1, 400, 0);
        for _ in 0..4 {
            rig.run(4, 700, 5_000);
            assert!(!rig.choked());
            rig.run(1, 400, 0);
            assert!(!rig.choked());
        }
    }

    #[test]
    fn a_choke_that_comes_back_is_not_a_cycle() {
        let mut rig = Rig::adaptive();
        rig.run(10, 300, 0);
        rig.run(40, 700, 5_000);
        assert!(rig.choked());
        // The choke lifts after ten calm seconds; the two voices that come back overflow again.
        rig.run(11, 300, 0);
        assert!(!rig.choked());
        rig.run(1, 400, 5_000);
        assert!(
            rig.choked(),
            "back at the first refusing window, not five later"
        );
        // And is held twice as long this time.
        rig.run(11, 300, 0);
        assert!(rig.choked(), "held for twenty seconds the second time");
        rig.run(10, 300, 0);
        assert!(!rig.choked());
        // A long calm spell forgets it.
        rig.run(70, 300, 0);
        rig.run(1, 700, 5_000);
        assert!(!rig.choked(), "a new overload starts from the beginning");
    }

    #[test]
    fn what_the_kernel_refused_ages_without_anyone_updating_the_gauge() {
        let mut rig = Rig::adaptive();
        rig.run(10, 300, 0);
        rig.run(40, 700, 5_000);
        assert!(rig.refusing() && rig.choked());
        assert!(rig.g.refusing(rig.now + Duration::from_secs(9)));
        assert!(!rig.g.refusing(rig.now + Duration::from_secs(11)));
        assert!(!rig.g.choked(rig.now + Duration::from_secs(11)));
    }

    #[test]
    fn a_decided_squeeze_that_keeps_coming_back_is_released_less_and_less_often() {
        // Demand is 1.2 times the ceiling, and half of that once squeezed: released after twenty
        // calm seconds it returns at once.
        let mut rig = Rig::new(Gauge::adaptive(0, 0));
        rig.run(10, 300, 0);
        rig.run(20, 700, 5_000);
        let ceiling = rig.g.ceiling_bps() / 1_000_000;
        let (mut onsets, mut was) = (0, rig.g.squeezed());
        for _ in 0..1_500 {
            let demand = if rig.g.squeezed() {
                ceiling / 2
            } else {
                ceiling * 12 / 10
            };
            rig.run(1, demand, 0);
            if rig.g.squeezed() && !was {
                onsets += 1;
            }
            was = rig.g.squeezed();
        }
        assert!(onsets <= 8, "{onsets} squeezes in 25 minutes");
        assert!(onsets >= 2, "it does release: {onsets}");
    }
}
