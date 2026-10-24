//! The send-side bandwidth estimate of one participant, readable from outside the interceptor chain.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use rtc::interceptor::{BandwidthEstimator, EstimatorStats, Gcc, PacketReport};

/// What the estimator last said. Floats are stored as bits so reads are lock-free.
#[derive(Debug, Default)]
pub struct Estimate {
    target: AtomicU64,
    delay_based: AtomicU64,
    loss_based: AtomicU64,
    loss: AtomicU64,
}

/// A snapshot of an [`Estimate`], in bits per second and a loss fraction.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EstimateSnapshot {
    pub target: f64,
    pub delay_based: f64,
    pub loss_based: f64,
    pub average_loss: f64,
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
        EstimateSnapshot {
            target: load(&self.target),
            delay_based: load(&self.delay_based),
            loss_based: load(&self.loss_based),
            average_loss: load(&self.loss),
        }
    }
}

/// Google congestion control that publishes each result to an [`Estimate`].
pub struct SharedGcc {
    inner: Gcc,
    out: Arc<Estimate>,
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
        (
            Self {
                inner,
                out: out.clone(),
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

    #[test]
    fn the_handle_sees_the_initial_estimate() {
        let (_gcc, estimate) = SharedGcc::new(300_000.0, 100_000.0, 4_000_000.0);
        assert_eq!(estimate.snapshot().target, 300_000.0);
    }
}
