//! The bitrate of a packet stream over a sliding window.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Measures the rate of a stream from the sizes and arrival times of its packets, over the last
/// `window` of them.
#[derive(Debug)]
pub struct RateMonitor {
    window: Duration,
    samples: VecDeque<(Instant, usize)>,
    /// Bytes in `samples`, kept so a rate costs no pass over them.
    total: usize,
}

impl RateMonitor {
    pub fn new(window: Duration) -> Self {
        assert!(!window.is_zero(), "the window must be positive");
        Self {
            window,
            samples: VecDeque::new(),
            total: 0,
        }
    }

    /// Records a packet of `size` bytes seen at `now`.
    pub fn push(&mut self, now: Instant, size: usize) {
        self.samples.push_back((now, size));
        self.total += size;
        // Keep a little more than the window so the rate over exactly the window is always known.
        while let Some(&(at, bytes)) = self.samples.front() {
            if now.saturating_duration_since(at) > self.window * 2 {
                self.samples.pop_front();
                self.total -= bytes;
            } else {
                break;
            }
        }
    }

    /// Bits per second over the window ending `now`, or `None` until the stream has been seen for
    /// the whole window (the first packets of a stream arrive in a burst and would read too high).
    pub fn rate(&self, now: Instant) -> Option<u64> {
        let first = self.samples.front()?.0;
        if now.saturating_duration_since(first) < self.window {
            return None;
        }
        let mut bytes = 0usize;
        let mut span = Duration::ZERO;
        for &(at, size) in self.samples.iter().rev() {
            span = now.saturating_duration_since(at);
            bytes += size;
            if span >= self.window {
                break;
            }
        }
        if span.is_zero() {
            return None;
        }
        Some((bytes as f64 * 8.0 / span.as_secs_f64()).round() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_steady_stream_reads_its_rate() {
        let mut m = RateMonitor::new(Duration::from_secs(2));
        let t = Instant::now();
        // 1000-byte packets every 10 ms: 800 kbit/s.
        for i in 0..400 {
            m.push(t + Duration::from_millis(i * 10), 1000);
        }
        let rate = m.rate(t + Duration::from_millis(4000)).unwrap();
        assert!((780_000..=820_000).contains(&rate), "{rate}");
    }

    #[test]
    fn no_rate_before_a_full_window() {
        let mut m = RateMonitor::new(Duration::from_secs(5));
        let t = Instant::now();
        m.push(t, 1200);
        assert_eq!(m.rate(t + Duration::from_secs(1)), None);
        assert!(m.rate(t + Duration::from_secs(6)).is_some());
    }

    #[test]
    fn old_samples_are_forgotten() {
        let mut m = RateMonitor::new(Duration::from_secs(1));
        let t = Instant::now();
        for i in 0..100 {
            m.push(t + Duration::from_millis(i * 10), 1000);
        }
        m.push(t + Duration::from_secs(60), 1000);
        assert_eq!(m.samples.len(), 1);
        assert_eq!(m.total, 1000);
    }

    #[test]
    fn a_stream_that_stopped_reads_lower() {
        let mut m = RateMonitor::new(Duration::from_secs(1));
        let t = Instant::now();
        for i in 0..200 {
            m.push(t + Duration::from_millis(i * 10), 1000);
        }
        let busy = m.rate(t + Duration::from_millis(2000)).unwrap();
        let idle = m.rate(t + Duration::from_millis(3500)).unwrap();
        assert!(idle < busy / 2, "{idle} vs {busy}");
    }
}
