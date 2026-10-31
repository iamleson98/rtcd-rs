//! The send-side bandwidth estimate of one participant, readable from outside the interceptor chain.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rtc::interceptor::{BandwidthEstimator, EstimatorStats, Gcc, PacketReport};

/// How far back the feedback is looked at for what arrived.
const DELIVERY_WINDOW: Duration = Duration::from_secs(3);
/// Less than this sent in the window is too little to say what share of it arrived.
const DELIVERY_MIN_BYTES: u64 = 30_000;

/// What the estimator last said. Floats are stored as bits so reads are lock-free.
#[derive(Debug, Default)]
pub struct Estimate {
    target: AtomicU64,
    delay_based: AtomicU64,
    loss_based: AtomicU64,
    loss: AtomicU64,
    /// The share of the bytes sent in the window that the receiver reported, as bits of an `f64`;
    /// NaN while too little was sent to say.
    delivery: AtomicU64,
}

/// A snapshot of an [`Estimate`], in bits per second and a loss fraction.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EstimateSnapshot {
    pub target: f64,
    pub delay_based: f64,
    pub loss_based: f64,
    pub average_loss: f64,
    /// The share (0..1) of the video bytes sent to the receiver over the last seconds that its
    /// feedback says arrived, `None` when too little was sent to say.
    pub delivery: Option<f64>,
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

    fn store_delivery(&self, delivery: Option<f64>) {
        self.delivery
            .store(delivery.unwrap_or(f64::NAN).to_bits(), Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> EstimateSnapshot {
        let load = |a: &AtomicU64| f64::from_bits(a.load(Ordering::Relaxed));
        EstimateSnapshot {
            target: load(&self.target),
            delay_based: load(&self.delay_based),
            loss_based: load(&self.loss_based),
            average_loss: load(&self.loss),
            delivery: Some(load(&self.delivery)).filter(|d| !d.is_nan()),
        }
    }
}

/// Google congestion control that publishes each result to an [`Estimate`].
pub struct SharedGcc {
    inner: Gcc,
    out: Arc<Estimate>,
    /// What the feedback of the last [`DELIVERY_WINDOW`] said of each packet: when it was
    /// reported, how big it was, whether it arrived.
    window: VecDeque<(Instant, usize, bool)>,
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
        out.store_delivery(None);
        (
            Self {
                inner,
                out: out.clone(),
                window: VecDeque::new(),
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
        self.window
            .extend(reports.iter().map(|r| (now, r.size, r.arrived)));
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
        self.out
            .store_delivery((sent >= DELIVERY_MIN_BYTES).then(|| arrived as f64 / sent as f64));
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

    fn report(size: usize, arrived: bool, at: Instant) -> PacketReport {
        PacketReport {
            ssrc: 1,
            id: 0,
            rtp_sequence_number: 0,
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

    #[test]
    fn the_handle_sees_the_initial_estimate() {
        let (_gcc, estimate) = SharedGcc::new(300_000.0, 100_000.0, 4_000_000.0);
        assert_eq!(estimate.snapshot().target, 300_000.0);
    }
}
