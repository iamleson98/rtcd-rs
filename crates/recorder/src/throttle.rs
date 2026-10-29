//! A token bucket for the bytes a process uploads, shared by every upload in it.
//!
//! Recording must not compete with the media the node sends, so uploads are held to a rate. The
//! bucket is paid before the bytes go: a caller reserves its bytes, which may leave the bucket in
//! debt, and waits until the debt is repaid. A chunk larger than the bucket therefore never
//! blocks for good, and callers that arrive together are served in the order they reserved.

use std::sync::Mutex;
use std::time::{Duration, Instant};

/// How much unused rate is kept for a burst: a quarter of a second's worth.
const BURST: f64 = 0.25;

#[derive(Debug)]
struct State {
    /// Bytes that may be sent at once; negative while a reservation is being waited out.
    tokens: f64,
    at: Instant,
}

/// Holds the bytes sent through it to a rate.
#[derive(Debug)]
pub struct Throttle {
    /// Bytes per second.
    rate: f64,
    state: Mutex<State>,
}

impl Throttle {
    /// A bucket that lets `bytes_per_second` through (at least one byte a second).
    pub fn new(bytes_per_second: u64) -> Self {
        let rate = bytes_per_second.max(1) as f64;
        Self {
            rate,
            state: Mutex::new(State {
                tokens: rate * BURST,
                at: Instant::now(),
            }),
        }
    }

    /// Reserves `bytes` at `now` and says how long to wait before sending them.
    pub fn reserve(&self, bytes: u64, now: Instant) -> Duration {
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let elapsed = now.saturating_duration_since(s.at).as_secs_f64();
        s.at = s.at.max(now);
        s.tokens = (s.tokens + elapsed * self.rate).min(self.rate * BURST) - bytes as f64;
        if s.tokens >= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(-s.tokens / self.rate)
        }
    }

    /// Waits until `bytes` may be sent.
    pub async fn take(&self, bytes: u64) {
        let wait = self.reserve(bytes, Instant::now());
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_burst_passes_and_the_rest_is_paced() {
        let t = Throttle::new(1000);
        let now = Instant::now();
        assert_eq!(
            t.reserve(250, now),
            Duration::ZERO,
            "a quarter second's worth"
        );
        // Past the burst, each byte costs a millisecond.
        let wait = t.reserve(500, now);
        assert!((wait.as_secs_f64() - 0.5).abs() < 1e-6, "{wait:?}");
        // The next caller waits behind it.
        let wait = t.reserve(500, now);
        assert!((wait.as_secs_f64() - 1.0).abs() < 1e-6, "{wait:?}");
    }

    #[test]
    fn time_pays_the_debt_back_but_saves_no_more_than_a_burst() {
        let t = Throttle::new(1000);
        let now = Instant::now();
        t.reserve(2250, now); // two seconds in debt
        let later = now + Duration::from_secs(2);
        assert_eq!(t.reserve(0, later), Duration::ZERO);
        let much_later = later + Duration::from_secs(3600);
        assert_eq!(t.reserve(250, much_later), Duration::ZERO);
        let wait = t.reserve(250, much_later);
        assert!(
            (wait.as_secs_f64() - 0.25).abs() < 1e-6,
            "an hour saved no more than a burst: {wait:?}"
        );
    }

    #[tokio::test]
    async fn a_chunk_larger_than_the_bucket_still_goes() {
        let t = Throttle::new(1_000_000);
        let started = Instant::now();
        t.take(400_000).await;
        let took = started.elapsed();
        assert!(took >= Duration::from_millis(100), "{took:?}");
        assert!(took < Duration::from_secs(2), "{took:?}");
    }
}
