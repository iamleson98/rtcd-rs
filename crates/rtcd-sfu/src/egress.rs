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
//! # The card's ceilings
//!
//! The gauge feeds an [`Estimator`](crate::capacity::Estimator) (see [`Gauge::estimating`]) with
//! each window's rate and with what the card took of it, and the estimator keeps a ceiling in
//! bits and one in packets a second: a conservative start that rises while the node is pressed
//! against it and the kernel accepts everything, and drops to just under the rate at which the
//! kernel began to refuse. The admission (`runtime::guard`) closes the node to newcomers and sheds
//! quality near those ceilings. They are the node's whole view of its card; the squeeze below
//! follows a configured budget only.
//!
//! A window *refuses* when the kernel refused at least [`DROPS_PER_SEC`] datagrams a second and
//! [`REFUSAL_SHARE`] of what was sent.
//!
//! # Adapting to the kernel's refusals
//!
//! Counting and reporting is always on. What *acts* on the refusals (the squeeze against a stall,
//! the choke, the slower uploads) is on only for a gauge made [`Gauge::adapting`]; otherwise the
//! squeeze follows a configured budget and the admission's hold, and nothing else. When on, in the
//! order a refusing run grows:
//!
//! - Two refusing windows in a row slow the recording uploads for as long as the refusals last
//!   and [`REFUSING_FOR`] after ([`Gauge::refusing`]).
//! - Three in a row squeeze video and voices to the small layers and two for as long as they last
//!   and [`REFUSING_FOR`] after: a stall of the machine or the card (a hypervisor pause, a full
//!   ring) lasts seconds and is no sign of a rate that is too high. A squeeze that comes back
//!   within [`SQUEEZE_AGAIN`] is held twice as long, so a demand just over the card's limit does
//!   not switch layers and voices every few seconds.
//! - A squeeze the node decided on (a budget, or the admission's hold near the ceiling) that the
//!   kernel goes on refusing through for [`CHOKE_AFTER`] windows in a row *chokes* the node: calls
//!   forward the loudest voice only until the kernel has accepted everything for [`CHOKE_HOLD`],
//!   which doubles for a choke that comes back within [`CHOKE_MEMORY`], up to [`CHOKE_HOLD_MAX`].
//!   A clean window between the refusals starts the count again, so an overload that stutters
//!   never chokes.
//!
//! Everything here is a heuristic about a signal, the kernel's refusals, that is the machine's and
//! not only the node's.

use std::net::SocketAddr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::capacity::{self, Estimator};

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
/// Refusing windows in a row that slow the uploads, and that squeeze.
const YIELD_AFTER: u32 = 2;
const STALL_AFTER: u32 = 3;
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
/// The sustained rate is the median of this many windows: a burst (a keyframe fanned out to a class
/// is a second or two of several times the usual rate) is out of the figure the node decides on,
/// and a class joining is followed three windows late.
const SUSTAIN: usize = 7;
/// The Ethernet header, which the network card's counters include; the IP header (20 bytes, 40
/// for IPv6) and the UDP (8) or TCP (20) one are added per datagram.
const LINK_OVERHEAD: u64 = 14;
/// The share of the packets the node sends that retransmissions may be, and the least it answers
/// (packets a second) however little it sends: a lossy receiver on a quiet node is answered in
/// full. While the kernel refuses datagrams only the small floor is answered.
const RTX_SHARE: f64 = 0.10;
const RTX_FLOOR_PPS: f64 = 500.0;
const RTX_FLOOR_REFUSING_PPS: f64 = 50.0;

#[derive(Debug, Default)]
struct State {
    at: Option<Instant>,
    bytes: u64,
    packets: u64,
    rate_bps: f64,
    rate_pps: f64,
    /// Squeezed by a budget (see `decide`).
    squeezed: bool,
    calm_since: Option<Instant>,
    /// What the node has learned about its card (see [`Gauge::estimating`]).
    estimator: Option<Estimator>,
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
    /// The rates of the last [`SUSTAIN`] windows in turn (bits and packets per second), how many
    /// of them are filled, and their mean.
    recent: [(f64, f64); SUSTAIN],
    recent_at: usize,
    filled: usize,
    sustained: (f64, f64),
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

/// The median of the rates, in bits and in packets apart.
fn median(rates: &[(f64, f64)]) -> (f64, f64) {
    let middle = |pick: fn(&(f64, f64)) -> f64| {
        let mut v: Vec<f64> = rates.iter().map(pick).collect();
        v.sort_by(f64::total_cmp);
        v[v.len() / 2]
    };
    (middle(|r| r.0), middle(|r| r.1))
}

impl State {
    /// The slot of the next window in `recent`.
    fn at_recent(&mut self) -> usize {
        let at = self.recent_at;
        self.recent_at = (at + 1) % SUSTAIN;
        at
    }
}

/// A token bucket of packets: how many retransmissions are left to answer, and as of when.
#[derive(Debug, Default)]
struct RtxBucket {
    level: f64,
    at: Option<Instant>,
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
    /// Whether the gauge acts on the kernel's refusals (see the module documentation).
    adapt: bool,
    bytes: AtomicU64,
    packets: AtomicU64,
    state: Mutex<State>,
    /// The smoothed rates, for readers that do not take the lock, and the sustained ones.
    rate: AtomicU64,
    rate_packets: AtomicU64,
    sustained: AtomicU64,
    sustained_packets: AtomicU64,
    squeezed: AtomicBool,
    /// What the node holds regardless of the kernel (see [`Gauge::hold`]): 0 nothing, 1 squeeze,
    /// 2 squeeze and choke.
    held: AtomicU8,
    /// The retransmissions the node still answers now (see [`Gauge::allow_retransmits`]), and the
    /// ones it has left unanswered since it started.
    rtx: Mutex<RtxBucket>,
    rtx_limited: AtomicU64,
    /// Datagrams the kernel refused to queue since the node started (UDP `SndbufErrors`).
    kernel_drops: AtomicU64,
    /// Until when the kernel counts as refusing and the node as choked, in nanoseconds after
    /// `epoch` (0: not at all). Readers compare with the time they ask, so the answer ages by
    /// itself when nothing updates the gauge.
    refusing_until: AtomicU64,
    choked_until: AtomicU64,
    /// The ceilings of the card in bits and packets a second (0 while no estimator runs), and
    /// where the one in bits comes from (see [`Gauge::capacity_source`]).
    ceiling: AtomicU64,
    ceiling_packets: AtomicU64,
    source: AtomicU8,
    /// Which of the two ceilings are facts, not probes (bit 0: bits, bit 1: packets).
    proven: AtomicU8,
}

impl Gauge {
    /// A gauge that acts on the kernel's refusals (see the module documentation).
    pub fn adapting(mut self) -> Self {
        self.adapt = true;
        self
    }

    /// Whether the gauge acts on the kernel's refusals, or only counts and reports them.
    pub fn adapts(&self) -> bool {
        self.adapt
    }

    /// A gauge that learns the ceilings of the card it sends on (see [`capacity`]).
    pub fn estimating(mut self, cfg: capacity::Config) -> Self {
        let est = Estimator::new(cfg);
        self.publish(&est);
        self.state
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .estimator = Some(est);
        self
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

    /// The send rate in bits per second, the median of the last seven windows: what the node
    /// decides on (the smoothed one follows a burst, and the windows that refuse are the bursts).
    pub fn sustained_bps(&self) -> u64 {
        self.sustained.load(Ordering::Relaxed)
    }

    /// The same in packets per second.
    pub fn sustained_pps(&self) -> u64 {
        self.sustained_packets.load(Ordering::Relaxed)
    }

    /// How many datagrams the kernel has refused to queue since this process read the count first:
    /// what was handed to the socket and never left (0 where the system does not say).
    pub fn kernel_send_drops(&self) -> u64 {
        self.kernel_drops.load(Ordering::Relaxed)
    }

    /// What the card is taken to carry in bits per second: the operator's figure, or what the node
    /// has learned of it (0 for a gauge that does not estimate).
    pub fn ceiling_bps(&self) -> u64 {
        self.ceiling.load(Ordering::Relaxed)
    }

    /// The same in packets per second.
    pub fn ceiling_pps(&self) -> u64 {
        self.ceiling_packets.load(Ordering::Relaxed)
    }

    /// Where [`Gauge::ceiling_bps`] comes from: `fixed` (the operator said), `learned` (the card
    /// refused at it), `probing` (the start, or raised from it, the card has not yet refused) or
    /// `` (nothing is estimated).
    pub fn capacity_source(&self) -> &'static str {
        match self.source.load(Ordering::Relaxed) {
            1 => "probing",
            2 => "learned",
            3 => "fixed",
            _ => "",
        }
    }

    /// Whether the ceiling in bits (`bits`) or in packets is a fact, the operator's or a wall the
    /// card showed, and not a probe on its way up: only a fact is worth cutting anyone's quality
    /// for.
    pub fn ceiling_proven(&self, bits: bool) -> bool {
        self.proven.load(Ordering::Relaxed) & if bits { 1 } else { 2 } != 0
    }

    fn publish(&self, est: &Estimator) {
        self.proven.store(
            u8::from(est.proven(capacity::BITS)) | u8::from(est.proven(capacity::PACKETS)) << 1,
            Ordering::Relaxed,
        );
        self.ceiling
            .store(est.ceiling(capacity::BITS) as u64, Ordering::Relaxed);
        self.ceiling_packets
            .store(est.ceiling(capacity::PACKETS) as u64, Ordering::Relaxed);
        self.source.store(
            match est.source(capacity::BITS) {
                "probing" => 1,
                "learned" => 2,
                "fixed" => 3,
                _ => 0,
            },
            Ordering::Relaxed,
        );
    }

    /// Whether receivers are being held to the small layers.
    pub fn squeezed(&self) -> bool {
        self.squeezed.load(Ordering::Relaxed) || self.held.load(Ordering::Relaxed) > 0
    }

    /// Whether the node answers a request to retransmit `packets` packets at `now`.
    ///
    /// Retransmissions are what a node under loss sends more of: every receiver that loses asks
    /// again, for the same packets again when the answer is lost too. At 95 % loss the lab's node
    /// offered 20 times what its receivers had asked it to carry, 1.7 Gbit/s from 36 of them, and
    /// what it offered was refused, which made the loss. So they are limited to a tenth of what the
    /// node sends (never below 500 packets a second, so that a lossy receiver on a quiet node is
    /// answered), to almost nothing while the kernel refuses datagrams, and a request is answered
    /// whole or not at all. The receiver asks again, or its video freezes until the next keyframe,
    /// which is what loss this deep costs anyway.
    pub fn allow_retransmits(&self, packets: u64, now: Instant) -> bool {
        let rate = if self.refusing(now) {
            RTX_FLOOR_REFUSING_PPS
        } else {
            (self.rate_pps() as f64 * RTX_SHARE).max(RTX_FLOOR_PPS)
        };
        let mut b = self.rtx.lock().unwrap_or_else(|e| e.into_inner());
        // A second's worth at most: a spell of quiet does not store up a flood.
        let elapsed =
            b.at.map_or(1.0, |t| now.saturating_duration_since(t).as_secs_f64());
        b.at = Some(now);
        b.level = (b.level + elapsed * rate).min(rate);
        if b.level <= 0.0 {
            self.rtx_limited.fetch_add(packets, Ordering::Relaxed);
            return false;
        }
        b.level -= packets as f64;
        true
    }

    /// Retransmissions asked for and not answered since the node started (see
    /// [`Gauge::allow_retransmits`]).
    pub fn retransmits_limited(&self) -> u64 {
        self.rtx_limited.load(Ordering::Relaxed)
    }

    /// Holds the node squeezed (`1`) or squeezed and choked (`2`), or lets go (`0`), whatever the
    /// kernel says: for a limit that is not the card's (the node's memory).
    pub fn hold(&self, level: u8) {
        self.held.store(level.min(2), Ordering::Relaxed);
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
            || self.held.load(Ordering::Relaxed) > 1
    }

    fn nanos(&self, t: Instant) -> u64 {
        t.saturating_duration_since(self.epoch.0).as_nanos() as u64 + 1
    }

    /// Recomputes the rates and the decision; cheap to call often, it works at most once a window.
    /// A budget of 0 is not set.
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
        let instant = (
            total.saturating_sub(st.bytes) as f64 * 8.0 / secs,
            sent as f64 / secs,
        );
        st.rate_bps = smooth(st.rate_bps, instant.0);
        st.rate_pps = smooth(st.rate_pps, instant.1);
        let at = st.at_recent();
        st.recent[at] = instant;
        st.filled = (st.filled + 1).min(SUSTAIN);
        st.sustained = median(&st.recent[..st.filled]);
        if sent == 0 {
            // Nothing was handed to a socket: an empty node reports no rate, not the average of
            // its last seconds.
            st.rate_bps = 0.0;
            st.rate_pps = 0.0;
            st.filled = 0;
            st.sustained = (0.0, 0.0);
        }
        st.at = Some(now);
        st.bytes = total;
        st.packets = packets;
        self.rate.store(st.rate_bps as u64, Ordering::Relaxed);
        self.rate_packets
            .store(st.rate_pps as u64, Ordering::Relaxed);
        self.sustained
            .store(st.sustained.0 as u64, Ordering::Relaxed);
        self.sustained_packets
            .store(st.sustained.1 as u64, Ordering::Relaxed);
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
            // choked nor in the middle of anything.
            quiet(&mut st);
        } else if refusing {
            st.run += 1;
            st.refused_at = Some(now);
            if st.run >= YIELD_AFTER {
                st.refusing_at = Some(now);
            }
        } else {
            st.run = 0;
        }

        if sent > 0 {
            let window = capacity::Window {
                rate: [st.sustained.0, st.sustained.1],
                refusing,
                run: st.run,
                squeezed: st.squeezed || self.held.load(Ordering::Relaxed) > 0,
            };
            if let Some(est) = st.estimator.as_mut() {
                est.observe(&window);
                self.publish(est);
            }
        }
        if self.adapt {
            stall(&mut st, now);
        }
        let decided = decide(&mut st, now, budget_bps, budget_pps);
        let mut squeezed = decided;
        if self.adapt {
            note_squeeze(&mut st, now, decided);
            squeezed |= st.stall_until.is_some_and(|t| now < t);
            // Only a squeeze the node decided on (a budget, the admission's hold near its
            // ceiling) is one that may fail to be enough; one against a stall lasts as long as
            // the stall.
            let held = self.held.load(Ordering::Relaxed) > 0;
            choke(&mut st, now, refusing && sent > 0, decided || held);
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
            Rig::new(Gauge::default().adapting())
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
        let g = Gauge::default().adapting();
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
    fn a_stall_of_fourteen_seconds_never_chokes_and_a_stall_that_returns_is_held_longer() {
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
    fn a_node_below_its_start_is_never_taught_however_long_it_is_refused() {
        let mut rig = Rig::new(
            Gauge::default()
                .adapting()
                .estimating(capacity::Config::default()),
        );
        rig.run(60, 30, 0);
        rig.run(120, 30, 3_000);
        assert_eq!(rig.g.ceiling_bps(), 400_000_000, "the start stands");
        assert_eq!(rig.g.capacity_source(), "probing");
        assert!(!rig.choked(), "only a decided squeeze can choke");
        assert!(rig.g.squeezed(), "but video is small while it lasts");
        rig.run(30, 30, 0);
        assert!(!rig.g.squeezed());
    }

    // ------------------------------------------------------------------ the ceilings

    /// A gauge that learns its card, from the defaults (400 Mbit/s, 80 thousand packets).
    fn estimating() -> Rig {
        Rig::new(Gauge::default().estimating(capacity::Config::default()))
    }

    #[test]
    fn a_gauge_that_does_not_estimate_has_no_ceilings() {
        let mut rig = Rig::new(Gauge::default());
        rig.run(60, 300, 0);
        assert_eq!((rig.g.ceiling_bps(), rig.g.ceiling_pps()), (0, 0));
        assert_eq!(rig.g.capacity_source(), "");
    }

    #[test]
    fn the_gauge_starts_at_the_start_and_raises_it_while_the_node_is_pressed() {
        let mut rig = estimating();
        assert_eq!(
            rig.g.ceiling_bps(),
            400_000_000,
            "known before the first window"
        );
        assert_eq!(rig.g.ceiling_pps(), 80_000);
        rig.run(30, 100, 0);
        assert_eq!(rig.g.ceiling_bps(), 400_000_000);
        rig.run(60, 380, 0);
        assert!(rig.g.ceiling_bps() > 440_000_000, "{}", rig.g.ceiling_bps());
        assert_eq!(rig.g.capacity_source(), "probing");
    }

    #[test]
    fn what_the_card_took_is_the_wall_and_the_ceiling_is_just_under_it() {
        let mut rig = estimating();
        rig.run(30, 500, 0);
        // 600 Mbit/s asked of a card that takes 550: a thirteenth of the datagrams refused. The
        // wall is what the node carried whole before, and the burst that failed is not in it.
        let sent = 600 * MBIT / 1_200;
        for _ in 0..4 {
            rig.window_with(600 * MBIT, sent, sent / 13, (0, 0));
        }
        assert_eq!(rig.g.capacity_source(), "learned");
        let ceiling = rig.g.ceiling_bps() as f64 / 1e6;
        assert!((490.0..=520.0).contains(&ceiling), "{ceiling} Mbit/s");
    }

    #[test]
    fn a_packet_bound_overload_teaches_the_packet_ceiling_and_leaves_the_bit_one() {
        let mut rig = estimating();
        rig.run_pps(60, 85_000, 0);
        // Voice: 100 thousand packets of 100 bytes asked of a card that takes 90 thousand.
        for _ in 0..4 {
            rig.window_with(100_000 * 100, 100_000, 10_000, (0, 0));
        }
        let packets = rig.g.ceiling_pps() as f64;
        assert!((80_000.0..=90_000.0).contains(&packets), "{packets}");
        assert_eq!(rig.g.ceiling_bps(), 400_000_000);
    }

    #[test]
    fn a_stall_teaches_nothing() {
        let mut rig = estimating();
        rig.run_pps(120, 60_000, 0);
        let before = rig.g.ceiling_pps();
        // The card stops for two seconds: under three windows.
        rig.run_pps(2, 60_000, 60_000);
        rig.run_pps(60, 60_000, 0);
        assert_eq!(rig.g.capacity_source(), "probing");
        assert!(rig.g.ceiling_pps() >= before, "a stall lowered the ceiling");
    }

    #[test]
    fn an_operators_ceiling_stands_and_the_other_limit_is_learned() {
        let mut rig = Rig::new(Gauge::default().estimating(capacity::Config {
            fixed: [850e6, 0.0],
            ..capacity::Config::default()
        }));
        rig.run(60, 840, 0);
        assert_eq!(rig.g.ceiling_bps(), 850_000_000);
        assert_eq!(rig.g.capacity_source(), "fixed");
        assert!(
            rig.g.ceiling_pps() >= 80_000,
            "the packets are still learned"
        );
    }

    // ---------------------------------------------- a node and its admission against a card

    /// What a run of [`push_a_card`] came to.
    #[derive(Debug, Default)]
    struct Pushed {
        /// The most the card took in a second, Mbit/s.
        taken_peak: f64,
        /// Seconds in which the kernel refused enough to matter, in which the node shed, and in
        /// which a join was turned away.
        refusing: u32,
        shed: u32,
        turned_away: u32,
        /// Seconds from the start to the first one that sent 90 % of the card's wall.
        at_ninety: Option<u32>,
        /// The send rate over the last minute (Mbit/s) and the ceiling at the end.
        last_minute: f64,
        ceiling: f64,
        /// The share of the datagrams handed to the card that it refused, over the whole run.
        lost: f64,
    }

    /// A node with its admission (`runtime::guard`) against a card that takes `wall` Mbit/s and
    /// refuses the rest, asked by people who join `per_sec` Mbit/s of demand a second until they
    /// want `demand_cap` Mbit/s in all. A join is refused while the admission is closed; a shed
    /// takes the video down to 60 % of the demand.
    fn push_a_card(rig: &mut Rig, wall: f64, demand_cap: f64, per_sec: f64, secs: u32) -> Pushed {
        push_a_card_with_surge(rig, wall, demand_cap, per_sec, secs, (u32::MAX, 1.0))
    }

    /// As [`push_a_card`], and at the second `surge.0` what the people already in ask for grows by
    /// `surge.1` (everyone turns a camera on).
    fn push_a_card_with_surge(
        rig: &mut Rig,
        wall: f64,
        demand_cap: f64,
        per_sec: f64,
        secs: u32,
        surge: (u32, f64),
    ) -> Pushed {
        use crate::runtime::{Admission, Limits, Reading};
        let mut adm = Admission::new(Limits::default());
        let (mut demand, mut out, mut tail) = (30.0_f64, Pushed::default(), Vec::new());
        for second in 0..secs {
            if second == surge.0 {
                demand *= surge.1;
            }
            let mut verdict = adm.verdict();
            for _ in 0..4 {
                verdict = adm.update(Reading {
                    bps: rig.g.sustained_bps(),
                    pps: rig.g.sustained_pps(),
                    ceiling_bps: rig.g.ceiling_bps(),
                    ceiling_pps: rig.g.ceiling_pps(),
                    proven_bps: rig.g.ceiling_proven(true),
                    proven_pps: rig.g.ceiling_proven(false),
                    refusing: rig.g.refusing(rig.now),
                    ..Default::default()
                });
            }
            if demand < demand_cap {
                if verdict.refuses_newcomers() {
                    out.turned_away += 1;
                } else {
                    demand = (demand + per_sec).min(demand_cap);
                }
            }
            rig.g.hold(u8::from(verdict.sheds()));
            out.shed += u32::from(verdict.sheds());
            let sent = if verdict.sheds() {
                0.6 * demand
            } else {
                demand
            };
            let packets = sent * 125_000.0 / 1_200.0;
            let refused = if sent > wall {
                packets * (sent - wall) / sent
            } else {
                0.0
            };
            rig.window_with(
                (sent * 125_000.0) as u64,
                packets as u64,
                refused as u64,
                (0, 0),
            );
            let taken = sent.min(wall);
            out.taken_peak = out.taken_peak.max(taken);
            out.lost += if sent > 0.0 {
                (sent - taken) / sent
            } else {
                0.0
            };
            out.refusing += u32::from(refused > 50.0 && refused > 0.005 * packets);
            if out.at_ninety.is_none() && taken >= 0.9 * wall {
                out.at_ninety = Some(second);
            }
            tail.push(taken);
        }
        out.lost /= f64::from(secs);
        out.last_minute = tail.iter().rev().take(60).sum::<f64>() / 60.0;
        out.ceiling = rig.g.ceiling_bps() as f64 / 1e6;
        out
    }

    #[test]
    fn a_healthy_node_far_from_its_card_is_never_touched() {
        let mut rig = estimating();
        let out = push_a_card(&mut rig, 850.0, 250.0, 2.0, 1_800);
        assert_eq!(
            (out.refusing, out.shed, out.turned_away),
            (0, 0, 0),
            "{out:?}"
        );
        assert_eq!(
            rig.g.ceiling_bps(),
            400_000_000,
            "nothing pressed on the start"
        );
    }

    #[test]
    fn a_node_asked_for_more_than_its_card_climbs_to_it_and_stops_taking_people_there() {
        // The OVH video node: 850 Mbit/s taken of a 1 Gbit/s card, people joining at 6 Mbit/s a
        // second, demand for 1,200.
        let mut rig = estimating();
        let out = push_a_card(&mut rig, 850.0, 1_200.0, 6.0, 1_800);
        assert!(
            out.last_minute >= 0.97 * 850.0,
            "the last minute averaged {:.0} Mbit/s of 850: {out:?}",
            out.last_minute
        );
        assert!(out.at_ninety.is_some_and(|s| s < 600), "{out:?}");
        assert_eq!(out.shed, 0, "no one's video was cut: {out:?}");
        // What the node asked of the card past the wall in finding it, and in the demand that a
        // join admitted just before it still had.
        assert!(out.lost < 0.03, "{out:?}");
        assert!(
            (0.9 * 850.0..=1.02 * 850.0).contains(&out.ceiling),
            "the ceiling is the wall: {out:?}"
        );
    }

    #[test]
    fn a_node_whose_people_suddenly_ask_for_far_more_sheds_and_does_not_run_away() {
        // Everyone in a full node turns a camera on: half as much again.
        let mut rig = estimating();
        let out = push_a_card_with_surge(&mut rig, 850.0, 1_000.0, 6.0, 1_800, (600, 1.5));
        assert!(out.shed > 0, "{out:?}");
        assert!(out.taken_peak <= 850.0);
        assert!(out.last_minute >= 0.85 * 850.0, "{out:?}");
        assert!(out.lost < 0.1, "{out:?}");
        assert!(out.ceiling <= 1.02 * 850.0, "{out:?}");
    }

    #[test]
    fn a_node_asked_for_twice_its_card_by_a_class_a_second_takes_people_only_to_the_ceiling() {
        let mut rig = estimating();
        let out = push_a_card(&mut rig, 850.0, 1_700.0, 20.0, 1_800);
        assert!(out.last_minute >= 0.9 * 850.0, "{out:?}");
        // The joins that were in flight when the mark was reached land a few percent past it.
        assert!(out.lost < 0.05, "{out:?}");
        assert!(out.ceiling <= 1.05 * 850.0, "{out:?}");
    }

    // ---------------------------------------------------------------------- the choke

    #[test]
    fn an_overload_the_shed_does_not_cure_chokes_the_node() {
        // The admission holds a shed near the card's ceiling (`hold`); the card goes on refusing.
        let mut rig = Rig::adaptive();
        rig.run(10, 300, 0);
        rig.g.hold(1);
        let mut chokes = 0;
        for _ in 0..40 {
            rig.run(1, 700, 5_000);
            chokes += u32::from(rig.choked());
        }
        assert!(rig.choked() && chokes > 30);
        assert!(rig.g.squeezed());
    }

    #[test]
    fn a_stall_alone_never_chokes() {
        let mut rig = Rig::adaptive();
        rig.run(10, 300, 0);
        for _ in 0..40 {
            rig.run(1, 700, 5_000);
            assert!(!rig.choked(), "no squeeze was decided");
        }
        assert!(rig.g.squeezed(), "but the stall squeeze holds");
    }

    #[test]
    fn a_node_that_empties_while_squeezed_and_choked_reports_neither() {
        let mut rig = Rig::adaptive();
        rig.run(300, 300, 0);
        rig.g.hold(1);
        rig.run(40, 700, 5_000);
        assert!(rig.choked());
        rig.run(1, 0, 0);
        assert!(!rig.choked() && !rig.refusing());
        rig.g.hold(0);
        assert!(!rig.g.squeezed(), "the stall squeeze went with the traffic");
    }

    #[test]
    fn a_calm_window_between_two_runs_starts_the_count_again() {
        let mut rig = Rig::adaptive();
        rig.run(10, 300, 0);
        rig.g.hold(1);
        rig.run(16, 700, 5_000);
        assert!(rig.g.squeezed() && rig.choked());
        let mut rig = Rig::adaptive();
        rig.run(10, 300, 0);
        rig.g.hold(1);
        rig.run(4, 700, 5_000);
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
        rig.g.hold(1);
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
        rig.g.hold(1);
        rig.run(40, 700, 5_000);
        assert!(rig.refusing() && rig.choked());
        assert!(rig.g.refusing(rig.now + Duration::from_secs(9)));
        assert!(!rig.g.refusing(rig.now + Duration::from_secs(11)));
        assert!(!rig.g.choked(rig.now + Duration::from_secs(11)));
    }

    #[test]
    fn a_decided_squeeze_that_keeps_coming_back_is_released_less_and_less_often() {
        // Demand is 1.2 times the budget, and half of that once squeezed: released after twenty
        // calm seconds it returns at once.
        let mut rig = Rig::new(Gauge::default().adapting());
        let (mut onsets, mut was) = (0, false);
        for _ in 0..1_500 {
            let demand = if rig.g.squeezed() { 250 } else { 600 };
            rig.window_with(demand * MBIT, demand * MBIT / 1_200, 0, (500_000_000, 0));
            if rig.g.squeezed() && !was {
                onsets += 1;
            }
            was = rig.g.squeezed();
        }
        assert!(onsets <= 8, "{onsets} squeezes in 25 minutes");
        assert!(onsets >= 2, "it does release: {onsets}");
    }

    #[test]
    fn an_emptied_node_reports_no_rate_and_does_not_flash_a_squeeze() {
        let mut rig = Rig::adaptive();
        rig.run(300, 300, 0);
        rig.run(20, 700, 5_000);
        assert!(rig.g.squeezed() && rig.g.rate_bps() > 0);
        let mut ever = false;
        rig.run(1, 0, 0);
        ever |= rig.g.squeezed();
        assert_eq!(rig.g.rate_bps(), 0, "an average of the last seconds");
        assert_eq!(rig.g.rate_pps(), 0);
        assert!(!ever, "squeezed in the first empty window");
        // The same for a budget alone.
        let mut rig = Rig::new(Gauge::default());
        run_budget(&mut rig, 6, 290, 300);
        assert!(rig.g.squeezed());
        run_budget(&mut rig, 1, 0, 300);
        assert_eq!((rig.g.rate_bps(), rig.g.rate_pps()), (0, 0));
        assert!(!rig.g.squeezed());
    }

    #[test]
    fn a_lossy_receiver_on_a_quiet_node_is_answered_in_full() {
        let g = Gauge::default();
        let now = Instant::now();
        for i in 0..400 {
            assert!(
                g.allow_retransmits(1, now + Duration::from_millis(i * 2)),
                "request {i}"
            );
        }
        assert_eq!(g.retransmits_limited(), 0);
    }

    #[test]
    fn retransmissions_are_limited_to_a_tenth_of_what_the_node_sends() {
        let g = Gauge::default();
        let t0 = Instant::now();
        // A node sending 200,000 packets a second answers 20,000 of them: a thousand requests of
        // 100 packets, a second, then no more until the bucket refills.
        for s in 1..=3 {
            g.add(1, 200_000);
            g.update(t0 + Duration::from_secs(s), 0, 0);
        }
        assert!(g.rate_pps() > 100_000, "{}", g.rate_pps());
        let now = t0 + Duration::from_secs(3);
        let answered = (0..2_000).filter(|_| g.allow_retransmits(100, now)).count();
        assert!(
            (150..=260).contains(&answered),
            "{answered} requests of 100 answered"
        );
        assert!(g.retransmits_limited() > 100 * 1_000);
        // A second later the bucket is full again.
        assert!(g.allow_retransmits(100, now + Duration::from_secs(1)));
    }

    #[test]
    fn while_the_kernel_refuses_only_a_trickle_is_answered() {
        let refused = std::sync::Arc::new(AtomicU64::new(0));
        let counter = refused.clone();
        let g = Gauge::with_kernel_drops(move || Some(counter.load(Ordering::Relaxed)));
        let t0 = Instant::now();
        for s in 1..=6 {
            g.add(1, 200_000);
            refused.fetch_add(5_000, Ordering::Relaxed);
            g.update(t0 + Duration::from_secs(s), 0, 0);
        }
        let now = t0 + Duration::from_secs(6);
        assert!(g.refusing(now));
        let answered: u64 = (0..1_000).filter(|_| g.allow_retransmits(10, now)).count() as u64;
        assert!(answered <= 6, "{answered} answered at 50 a second");
    }
}
