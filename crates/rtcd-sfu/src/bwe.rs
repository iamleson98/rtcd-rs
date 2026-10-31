//! The send-side bandwidth estimate of one participant, readable from outside the interceptor chain,
//! and what the participant's feedback says of its path besides: how much of what was sent arrived,
//! how long what is sent now waits in a queue on the way, how fast the path delivers, and the round
//! trip.
//!
//! The queue is read from the one-way delay of each packet (when the receiver says it arrived, on
//! its clock, less when it left, on ours): the clocks differ by a constant, so the lowest delay of
//! the last half minute is the path with nothing queued, and how far above it the first packet of
//! each burst arrives is the queue that stands on the path. A frame leaves in one burst, and on a
//! slow link its last packets wait for its first ones; the first packet of the next waits only if
//! the link has not caught up. A path that is sent more than it carries holds the excess in that
//! queue (a home router or a mobile network buffers seconds of it before dropping anything), and
//! everything sharing the link waits behind it, voice included. The delay-based half of the
//! bandwidth estimate reads only the queue's growth, and a queue that is full does not grow.
//!
//! The same bursts say what the narrowest link of the path carries: they leave together and arrive
//! spread out by it, so the bytes of a burst over the time it took to arrive is that link's rate
//! (a packet train), whatever is being sent. That tells a receiver that cannot take more from one
//! that can, before anything more is sent to find out.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rtc::interceptor::{BandwidthEstimator, EstimatorStats, Gcc, PacketReport};

/// How far back the feedback is looked at for what arrived.
const DELIVERY_WINDOW: Duration = Duration::from_secs(3);
/// Packets remembered by stream and sequence number, to tell a packet sent again from the first.
const SENT_REMEMBERED: usize = 4096;
/// Less than this sent in the window is too little to say what share of it arrived.
const DELIVERY_MIN_BYTES: u64 = 30_000;
/// How far back the lowest one-way delay is looked for: long enough to have seen the path with
/// nothing queued, short enough to follow a route that changed.
const BASE_WINDOW: Duration = Duration::from_secs(30);
/// The lowest delay is kept per slice of this length, so the window costs a few entries.
const BASE_SLICE: Duration = Duration::from_secs(1);
/// What arrived over this much of the receiver's clock is the rate the path delivered.
const ACKED_WINDOW: Duration = Duration::from_secs(1);
/// A queue reading follows each feedback by this share (feedback comes every 50 to 100 ms).
const QUEUE_SMOOTHING: f64 = 0.5;
/// The queue's trend is how it moved over about this long.
const TREND_SPAN: Duration = Duration::from_millis(500);
/// A packet that left this long after the one before begins a burst. A sender paces a frame out at
/// a few megabits a second, a kilobyte every millisecond or two, and frames are tens of
/// milliseconds apart.
const BURST_GAP: Duration = Duration::from_millis(5);
/// A burst of fewer packets than this, or that arrived over less than [`TRAIN_MIN_SPREAD`] (the
/// feedback's clock counts quarter milliseconds), says nothing of the link; the median of the last
/// [`TRAINS`] that do is its rate.
const TRAIN_MIN_PACKETS: usize = 5;
const TRAIN_MIN_SPREAD: Duration = Duration::from_millis(2);
const TRAINS: usize = 15;

/// What the estimator last said. Floats are stored as bits so reads are lock-free.
#[derive(Debug, Default)]
pub struct Estimate {
    target: AtomicU64,
    delay_based: AtomicU64,
    loss_based: AtomicU64,
    loss: AtomicU64,
    /// The share of the bytes sent the first time in the window that the receiver reported, as
    /// bits of an `f64`; NaN while too little was sent to say.
    delivery: AtomicU64,
    /// Seconds what is sent waits in a queue on the path, bits per second the path delivered, and
    /// the round trip in seconds; NaN while unknown.
    queue: AtomicU64,
    queue_trend: AtomicU64,
    acked: AtomicU64,
    capacity: AtomicU64,
    rtt: AtomicU64,
}

/// A snapshot of an [`Estimate`], in bits per second and a loss fraction.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EstimateSnapshot {
    pub target: f64,
    pub delay_based: f64,
    pub loss_based: f64,
    pub average_loss: f64,
    /// The share (0..1) of the video bytes sent to the receiver the first time over the last
    /// seconds that its feedback says arrived (what the path loses), `None` when too little was
    /// sent to say.
    pub delivery: Option<f64>,
    /// Seconds of what is sent to the receiver that stand queued on its path (see the module).
    pub queue: Option<f64>,
    /// How many seconds the queue grew (negative: shrank) over the last half second. A queue
    /// that a burst left (a keyframe) shrinks as soon as the burst has gone in; one that is fed
    /// more than the path carries keeps growing.
    pub queue_trend: Option<f64>,
    /// Bits per second of video the receiver's path delivered over the last second. While a queue
    /// stands on the path, it is what the path can carry.
    pub acked: Option<f64>,
    /// Bits per second the narrowest link of the path carries, from how bursts arrive spread out.
    pub capacity: Option<f64>,
    /// The round trip, seconds: from a packet leaving to the feedback that reports it.
    pub rtt: Option<f64>,
}

impl Estimate {
    fn store(&self, target: f64, stats: EstimatorStats) {
        self.target.store(target.to_bits(), Ordering::Relaxed);
        self.delay_based.store(
            stats.delay_based_bitrate.unwrap_or(target).to_bits(),
            Ordering::Relaxed,
        );
        self.loss_based.store(
            stats.loss_based_bitrate.unwrap_or(target).to_bits(),
            Ordering::Relaxed,
        );
        self.loss.store(
            stats.packet_loss.unwrap_or(0.0).to_bits(),
            Ordering::Relaxed,
        );
    }

    pub fn snapshot(&self) -> EstimateSnapshot {
        let load = |a: &AtomicU64| f64::from_bits(a.load(Ordering::Relaxed));
        let known = |a: &AtomicU64| Some(load(a)).filter(|d| !d.is_nan());
        EstimateSnapshot {
            target: load(&self.target),
            delay_based: load(&self.delay_based),
            loss_based: load(&self.loss_based),
            average_loss: load(&self.loss),
            delivery: known(&self.delivery),
            queue: known(&self.queue),
            queue_trend: known(&self.queue_trend),
            acked: known(&self.acked),
            capacity: known(&self.capacity),
            rtt: known(&self.rtt),
        }
    }
}

fn store_known(a: &AtomicU64, v: Option<f64>) {
    a.store(v.unwrap_or(f64::NAN).to_bits(), Ordering::Relaxed);
}

/// Google congestion control that publishes each result to an [`Estimate`].
pub struct SharedGcc {
    inner: Gcc,
    out: Arc<Estimate>,
    /// What the feedback of the last [`DELIVERY_WINDOW`] said of each packet: when it was
    /// reported, how big it was, whether it arrived.
    window: VecDeque<(Instant, usize, bool)>,
    /// The packets reported on lately, by stream and sequence number.
    reported: std::collections::HashSet<u64>,
    reported_order: VecDeque<u64>,
    path: PathMeter,
}

/// The queue, delivered rate and round trip of a path, from the packets its feedback reports.
#[derive(Debug, Default)]
struct PathMeter {
    /// Departures are counted from the first one seen.
    epoch: Option<Instant>,
    /// When the last packet reported on left.
    last_departure: Option<Instant>,
    /// The lowest one-way delay (seconds, plus the clocks' unknown difference) of each slice of
    /// the last [`BASE_WINDOW`], by when the slice began.
    lowest: VecDeque<(Instant, f64)>,
    queue: Option<f64>,
    /// Recent queue readings, by when the feedback came, for the trend.
    readings: VecDeque<(Instant, f64)>,
    /// What arrived over the last [`ACKED_WINDOW`]: when (on the receiver's clock) and how big.
    arrivals: VecDeque<(Duration, usize)>,
    arrived_bytes: usize,
    /// The burst being reported on, and the rates the last ones arrived at.
    train: Train,
    trains: VecDeque<f64>,
}

/// One burst, as its packets are reported.
#[derive(Debug, Default)]
struct Train {
    packets: usize,
    /// Bytes after the first packet's, and the first and last arrival.
    bytes: usize,
    first: Option<Duration>,
    last: Option<Duration>,
    /// A packet of it was lost: its spread no longer says anything.
    broken: bool,
}

impl Train {
    /// The rate the burst arrived at, if it says anything.
    fn rate(&self) -> Option<f64> {
        let spread = self.last?.saturating_sub(self.first?);
        (!self.broken && self.packets >= TRAIN_MIN_PACKETS && spread >= TRAIN_MIN_SPREAD)
            .then(|| self.bytes as f64 * 8.0 / spread.as_secs_f64())
    }

    fn add(&mut self, arrival: Option<Duration>, size: usize) {
        let Some(at) = arrival else {
            self.broken = true;
            return;
        };
        if self.packets > 0 {
            self.bytes += size;
        }
        self.packets += 1;
        self.first = Some(self.first.map_or(at, |f| f.min(at)));
        self.last = Some(self.last.map_or(at, |l| l.max(at)));
    }
}

impl PathMeter {
    fn on_reports(&mut self, now: Instant, reports: &[PacketReport]) {
        let epoch = *self
            .epoch
            .get_or_insert_with(|| reports.first().map_or(now, |r| r.departure));
        let mut least = f64::INFINITY;
        let mut heads = f64::INFINITY;
        for r in reports {
            let head = self
                .last_departure
                .is_none_or(|d| r.departure.saturating_duration_since(d) >= BURST_GAP);
            self.last_departure = Some(r.departure);
            // A burst begins: the one before is complete.
            if head && let Some(rate) = std::mem::take(&mut self.train).rate() {
                self.trains.push_back(rate);
                if self.trains.len() > TRAINS {
                    self.trains.pop_front();
                }
            }
            self.train.add(r.arrival.filter(|_| r.arrived), r.size);
            let (true, Some(at)) = (r.arrived, r.arrival) else {
                continue;
            };
            let left = r.departure.saturating_duration_since(epoch).as_secs_f64();
            let delay = at.as_secs_f64() - left;
            least = least.min(delay);
            if head {
                heads = heads.min(delay);
            }
            self.arrivals.push_back((at, r.size));
            self.arrived_bytes += r.size;
        }
        if least.is_finite() {
            match self.lowest.back_mut() {
                Some((since, low)) if now.saturating_duration_since(*since) < BASE_SLICE => {
                    *low = low.min(least)
                }
                _ => self.lowest.push_back((now, least)),
            }
            while self
                .lowest
                .front()
                .is_some_and(|(since, _)| now.saturating_duration_since(*since) > BASE_WINDOW)
            {
                self.lowest.pop_front();
            }
        }
        if heads.is_finite() {
            let base = self
                .lowest
                .iter()
                .map(|&(_, l)| l)
                .fold(f64::INFINITY, f64::min);
            let standing = (heads - base).max(0.0);
            let q = match self.queue {
                Some(q) => q + QUEUE_SMOOTHING * (standing - q),
                None => standing,
            };
            self.queue = Some(q);
            self.readings.push_back((now, q));
            while self
                .readings
                .get(1)
                .is_some_and(|(at, _)| now.saturating_duration_since(*at) >= TREND_SPAN)
            {
                self.readings.pop_front();
            }
        }
        let newest = self.arrivals.iter().map(|&(at, _)| at).max();
        if let Some(newest) = newest {
            while self
                .arrivals
                .front()
                .is_some_and(|&(at, _)| newest.saturating_sub(at) > ACKED_WINDOW)
            {
                let (_, size) = self.arrivals.pop_front().expect("checked");
                self.arrived_bytes -= size;
            }
        }
    }

    /// The rate of the narrowest link: the median of what the last bursts arrived at.
    fn capacity(&self) -> Option<f64> {
        let mut rates: Vec<f64> = self.trains.iter().copied().collect();
        if rates.len() < TRAINS / 3 {
            return None;
        }
        rates.sort_by(f64::total_cmp);
        Some(rates[rates.len() / 2])
    }

    /// How the queue moved over the last [`TREND_SPAN`], once that much has been seen.
    fn trend(&self) -> Option<f64> {
        let &(first, then) = self.readings.front()?;
        let &(last, now) = self.readings.back()?;
        (last.saturating_duration_since(first) >= TREND_SPAN / 2).then_some(now - then)
    }

    /// Bits per second the path delivered: what arrived over the window, once the window has
    /// filled (the first arrivals of a stream would read too high or too low).
    fn acked(&self) -> Option<f64> {
        let first = self.arrivals.front()?.0;
        let last = self.arrivals.iter().map(|&(at, _)| at).max()?;
        let span = last.saturating_sub(first);
        (span >= ACKED_WINDOW / 2).then(|| self.arrived_bytes as f64 * 8.0 / span.as_secs_f64())
    }
}

impl std::fmt::Debug for SharedGcc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedGcc")
            .field("target", &self.inner.target_bitrate())
            .finish()
    }
}

impl SharedGcc {
    /// An estimator starting at `initial` bits per second and staying within `[min, max]`, and the
    /// handle to read it by.
    pub fn new(initial: f64, min: f64, max: f64) -> (Self, Arc<Estimate>) {
        let out = Arc::new(Estimate::default());
        let inner = Gcc::new(initial, min, max);
        out.store(inner.target_bitrate(), inner.stats());
        for unknown in [
            &out.delivery,
            &out.queue,
            &out.queue_trend,
            &out.acked,
            &out.capacity,
            &out.rtt,
        ] {
            store_known(unknown, None);
        }
        (
            Self {
                inner,
                out: out.clone(),
                window: VecDeque::new(),
                reported: Default::default(),
                reported_order: VecDeque::new(),
                path: PathMeter::default(),
            },
            out,
        )
    }

    fn publish(&self) {
        self.out
            .store(self.inner.target_bitrate(), self.inner.stats());
    }
}

impl BandwidthEstimator for SharedGcc {
    fn on_reports(&mut self, now: Instant, reports: &[PacketReport]) {
        self.inner.on_reports(now, reports);
        // What arrives says what the path loses only of packets sent the first time: a packet sent
        // again (the SFU sends one again each time the receiver asks) that finds the first copy
        // there is thrown away by the receiver before its feedback counts it, and reads as lost.
        for r in reports {
            let key = u64::from(r.ssrc) << 16 | u64::from(r.rtp_sequence_number);
            if !self.reported.insert(key) {
                continue;
            }
            self.reported_order.push_back(key);
            if self.reported_order.len() > SENT_REMEMBERED {
                let old = self.reported_order.pop_front().expect("over the limit");
                self.reported.remove(&old);
            }
            self.window.push_back((now, r.size, r.arrived));
        }
        while self
            .window
            .front()
            .is_some_and(|(at, ..)| now.saturating_duration_since(*at) > DELIVERY_WINDOW)
        {
            self.window.pop_front();
        }
        let (sent, arrived) =
            self.window
                .iter()
                .fold((0u64, 0u64), |(sent, arrived), &(_, size, ok)| {
                    (
                        sent + size as u64,
                        arrived + if ok { size as u64 } else { 0 },
                    )
                });
        store_known(
            &self.out.delivery,
            (sent >= DELIVERY_MIN_BYTES).then(|| arrived as f64 / sent as f64),
        );
        self.path.on_reports(now, reports);
        store_known(&self.out.queue, self.path.queue);
        store_known(&self.out.queue_trend, self.path.trend());
        store_known(&self.out.acked, self.path.acked());
        store_known(&self.out.capacity, self.path.capacity());
        // The newest packet a feedback reports arrived just before the feedback was sent: from its
        // departure to now is the round trip, give or take the time the receiver held it.
        if let Some(left) = reports
            .iter()
            .filter(|r| r.arrived)
            .map(|r| r.departure)
            .max()
        {
            store_known(
                &self.out.rtt,
                Some(now.saturating_duration_since(left).as_secs_f64()),
            );
        }
        self.publish();
    }

    fn target_bitrate(&self) -> f64 {
        self.inner.target_bitrate()
    }

    fn handle_timeout(&mut self, now: Instant) {
        self.inner.handle_timeout(now);
        self.publish();
    }

    fn poll_timeout(&self) -> Option<Instant> {
        self.inner.poll_timeout()
    }

    fn stats(&self) -> EstimatorStats {
        self.inner.stats()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A report on a packet of its own (each one a new sequence number).
    fn report(size: usize, arrived: bool, at: Instant) -> PacketReport {
        static NEXT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);
        PacketReport {
            ssrc: 1,
            id: 0,
            rtp_sequence_number: NEXT.fetch_add(1, Ordering::Relaxed),
            is_twcc: true,
            twcc_sequence_number: 0,
            size,
            arrived,
            departure: at,
            arrival: None,
            ecn: rtc::rtcp::transport_feedbacks::cc_feedback_report::Ecn::NotEct,
        }
    }

    /// `packets` of 1000 bytes, one in every `every` lost.
    fn batch(packets: usize, every: usize, at: Instant) -> Vec<PacketReport> {
        (0..packets)
            .map(|i| report(1000, every == 0 || i % every != 0, at))
            .collect()
    }

    #[test]
    fn delivery_is_the_share_of_the_bytes_sent_that_arrived() {
        let (mut gcc, estimate) = SharedGcc::new(300_000.0, 100_000.0, 4_000_000.0);
        assert_eq!(estimate.snapshot().delivery, None, "nothing was sent yet");
        let t = Instant::now();
        gcc.on_reports(t, &batch(10, 0, t));
        assert_eq!(estimate.snapshot().delivery, None, "too little to say");
        gcc.on_reports(t, &batch(90, 20, t));
        let share = estimate.snapshot().delivery.expect("enough was sent");
        assert!((share - 0.95).abs() < 1e-9, "{share}");
    }

    #[test]
    fn a_packet_sent_again_does_not_count_for_what_the_path_loses() {
        let (mut gcc, estimate) = SharedGcc::new(300_000.0, 100_000.0, 4_000_000.0);
        let t = Instant::now();
        let numbered = |seq: u16, arrived: bool| PacketReport {
            rtp_sequence_number: seq,
            ..report(1000, arrived, t)
        };
        // 100 packets, 5 lost; each lost one sent again three times, and the copies that find the
        // packet already there are dropped by the receiver.
        let mut reports: Vec<_> = (0..100).map(|s| numbered(s, s % 20 != 0)).collect();
        reports.extend(
            (0..100)
                .step_by(20)
                .flat_map(|s| [numbered(s, true), numbered(s, false), numbered(s, false)]),
        );
        gcc.on_reports(t, &reports);
        let share = estimate.snapshot().delivery.unwrap();
        assert!((share - 0.95).abs() < 1e-9, "{share}");
    }

    #[test]
    fn delivery_forgets_what_is_older_than_the_window() {
        let (mut gcc, estimate) = SharedGcc::new(300_000.0, 100_000.0, 4_000_000.0);
        let t = Instant::now();
        gcc.on_reports(t, &batch(100, 2, t));
        assert_eq!(estimate.snapshot().delivery, Some(0.5));
        let later = t + DELIVERY_WINDOW + Duration::from_millis(1);
        gcc.on_reports(later, &batch(100, 0, later));
        assert_eq!(
            estimate.snapshot().delivery,
            Some(1.0),
            "only what was reported within the window counts"
        );
    }

    /// A 1000-byte packet sent at `sent` ms that arrived `owd` ms later on the receiver's clock
    /// (which runs 5 s ahead of ours).
    fn timed(t0: Instant, sent: u64, owd: u64) -> PacketReport {
        PacketReport {
            arrival: Some(Duration::from_millis(5_000 + sent + owd)),
            ..report(1000, true, t0 + Duration::from_millis(sent))
        }
    }

    /// Feeds what a path of `owd(ms)` one-way delay reports of the packets sent from `from_ms` to
    /// `to_ms`, 100 a second, in a feedback every 100 ms.
    fn feed(gcc: &mut SharedGcc, t0: Instant, from_ms: u64, to_ms: u64, owd: impl Fn(u64) -> u64) {
        for batch in (from_ms..to_ms).step_by(100) {
            let reports: Vec<_> = (batch..batch + 100)
                .step_by(10)
                .map(|ms| timed(t0, ms, owd(ms)))
                .collect();
            let last = batch + 90;
            gcc.on_reports(t0 + Duration::from_millis(last + owd(last) + 10), &reports);
        }
    }

    #[test]
    fn a_path_with_nothing_queued_reads_no_queue_and_its_round_trip() {
        let (mut gcc, estimate) = SharedGcc::new(300_000.0, 100_000.0, 4_000_000.0);
        let t0 = Instant::now();
        feed(&mut gcc, t0, 0, 5_000, |_| 115);
        let s = estimate.snapshot();
        assert!(s.queue.unwrap() < 0.001, "{:?}", s.queue);
        let rtt = s.rtt.unwrap();
        assert!((0.120..0.130).contains(&rtt), "{rtt}");
        let acked = s.acked.unwrap();
        assert!((780_000.0..830_000.0).contains(&acked), "{acked}");
    }

    #[test]
    fn a_queue_that_builds_on_the_path_reads_as_its_depth() {
        let (mut gcc, estimate) = SharedGcc::new(300_000.0, 100_000.0, 4_000_000.0);
        let t0 = Instant::now();
        feed(&mut gcc, t0, 0, 5_000, |_| 115);
        // From 5 s on, everything waits a further 600 ms.
        feed(&mut gcc, t0, 5_000, 6_000, |_| 715);
        let q = estimate.snapshot().queue.unwrap();
        assert!((0.55..=0.6).contains(&q), "{q}");
        assert!(estimate.snapshot().queue_trend.unwrap() > 0.0, "it grew");
        // It drains.
        feed(&mut gcc, t0, 6_000, 7_000, |_| 120);
        let q = estimate.snapshot().queue.unwrap();
        assert!(q < 0.01, "{q}");
    }

    #[test]
    fn a_burst_that_a_slow_link_spreads_out_is_not_a_standing_queue() {
        let (mut gcc, estimate) = SharedGcc::new(300_000.0, 100_000.0, 4_000_000.0);
        let t0 = Instant::now();
        // A frame of 20 packets every 100 ms, all leaving at once, over a link that takes 5 ms a
        // packet: the last of each waits 95 ms behind the first, and the link is idle before the
        // next frame.
        for frame in 0..50u64 {
            let at = frame * 100;
            let reports: Vec<_> = (0..20).map(|n| timed(t0, at, 115 + n * 5)).collect();
            gcc.on_reports(t0 + Duration::from_millis(at + 115 + 100), &reports);
        }
        let s = estimate.snapshot();
        assert!(s.queue.unwrap() < 0.001, "{:?}", s.queue);
        // The link falls behind: each frame starts 30 ms later than the one before.
        for frame in 50..60u64 {
            let at = frame * 100;
            let late = (frame - 49) * 30;
            let reports: Vec<_> = (0..20).map(|n| timed(t0, at, 115 + late + n * 5)).collect();
            gcc.on_reports(t0 + Duration::from_millis(at + 115 + late + 100), &reports);
        }
        let q = estimate.snapshot().queue.unwrap();
        assert!((0.25..=0.3).contains(&q), "{q}");
    }

    #[test]
    fn bursts_that_arrive_spread_out_give_the_rate_of_the_narrowest_link() {
        let (mut gcc, estimate) = SharedGcc::new(300_000.0, 100_000.0, 4_000_000.0);
        let t0 = Instant::now();
        assert_eq!(estimate.snapshot().capacity, None);
        // Frames of 10 packets of 1000 bytes every 100 ms over a link that takes 5 ms a packet:
        // half of the link's 1.6 Mbit/s is used, and the bursts still arrive at its rate.
        for frame in 0..20u64 {
            let at = frame * 100;
            let reports: Vec<_> = (0..10).map(|n| timed(t0, at, 115 + n * 5)).collect();
            gcc.on_reports(t0 + Duration::from_millis(at + 115 + 100), &reports);
        }
        let c = estimate.snapshot().capacity.unwrap();
        assert!((1_550_000.0..1_650_000.0).contains(&c), "{c}");
    }

    #[test]
    fn the_delay_with_nothing_queued_follows_a_route_that_changed() {
        let (mut gcc, estimate) = SharedGcc::new(300_000.0, 100_000.0, 4_000_000.0);
        let t0 = Instant::now();
        feed(&mut gcc, t0, 0, 5_000, |_| 50);
        // The route is 200 ms longer from now on: it reads as a queue until the window forgets
        // the shorter one.
        feed(&mut gcc, t0, 5_000, 10_000, |_| 250);
        assert!(estimate.snapshot().queue.unwrap() > 0.15);
        feed(
            &mut gcc,
            t0,
            10_000,
            10_000 + BASE_WINDOW.as_millis() as u64 + 2_000,
            |_| 250,
        );
        assert!(estimate.snapshot().queue.unwrap() < 0.01);
    }

    #[test]
    fn the_handle_sees_the_initial_estimate_and_nothing_measured() {
        let (_gcc, estimate) = SharedGcc::new(300_000.0, 100_000.0, 4_000_000.0);
        let s = estimate.snapshot();
        assert_eq!(s.target, 300_000.0);
        assert_eq!(
            (
                s.delivery,
                s.queue,
                s.queue_trend,
                s.acked,
                s.capacity,
                s.rtt
            ),
            (None, None, None, None, None, None)
        );
    }
}
