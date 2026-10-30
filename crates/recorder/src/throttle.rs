//! A token bucket for the bytes a process uploads, shared by every upload in it.
//!
//! Recording must not compete with the media the node sends, so uploads are held to a rate. The
//! bucket is paid before the bytes go: a caller reserves its bytes, which may leave the bucket in
//! debt, and waits until the debt is repaid. A chunk larger than the bucket therefore never
//! blocks for good, and callers that arrive together are served in the order they reserved.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant};

/// How much unused rate is kept for a burst: a quarter of a second's worth.
const BURST: f64 = 0.25;
/// The whole rate, in thousandths.
const FULL: u32 = 1000;

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
    /// How much of `rate` is let through, in thousandths (see [`Throttle::set_share`]).
    share: AtomicU32,
    state: Mutex<State>,
    /// The rate no longer applies (see [`Throttle::lift`]).
    lifted: AtomicBool,
}

impl Throttle {
    /// A bucket that lets `bytes_per_second` through (at least one byte a second).
    pub fn new(bytes_per_second: u64) -> Self {
        let rate = bytes_per_second.max(1) as f64;
        Self {
            rate,
            share: AtomicU32::new(FULL),
            state: Mutex::new(State {
                tokens: rate * BURST,
                at: Instant::now(),
            }),
            lifted: AtomicBool::new(false),
        }
    }

    /// Lets only `permille` thousandths of the rate through from now on (1,000: all of it; at
    /// least one). Uploads stand aside like this while the network is short of room for the media
    /// the same node sends: a file that waits an extra minute costs nothing, and a call that
    /// loses packets costs a class.
    pub fn set_share(&self, permille: u32) {
        self.share.store(permille.clamp(1, FULL), Ordering::Relaxed);
    }

    /// How much of the rate is let through now, in thousandths.
    pub fn share(&self) -> u32 {
        self.share.load(Ordering::Relaxed)
    }

    /// Stops holding anything back, for good: a process that is about to exit and has files to
    /// get out before it does, when the media it sends no longer matters as much.
    pub fn lift(&self) {
        self.lifted.store(true, Ordering::Release);
    }

    /// Reserves `bytes` at `now` and says how long to wait before sending them.
    pub fn reserve(&self, bytes: u64, now: Instant) -> Duration {
        if self.lifted.load(Ordering::Acquire) {
            return Duration::ZERO;
        }
        let rate = self.rate * f64::from(self.share.load(Ordering::Relaxed)) / f64::from(FULL);
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let elapsed = now.saturating_duration_since(s.at).as_secs_f64();
        s.at = s.at.max(now);
        s.tokens = (s.tokens + elapsed * rate).min(rate * BURST) - bytes as f64;
        if s.tokens >= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(-s.tokens / rate)
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

    #[test]
    fn a_smaller_share_paces_the_bytes_at_that_share_of_the_rate() {
        let t = Throttle::new(1000);
        let now = Instant::now();
        t.set_share(250);
        // A burst is a quarter second of the reduced rate (62 bytes); past it a byte costs 4 ms.
        assert_eq!(t.reserve(62, now), Duration::ZERO);
        let wait = t.reserve(250, now);
        assert!((wait.as_secs_f64() - 1.0).abs() < 0.02, "{wait:?}");
        // A share of nothing is a thousandth: slow, but the bytes still go in the end.
        let t = Throttle::new(1000);
        t.set_share(0);
        let wait = t.reserve(1000, now);
        assert!((wait.as_secs_f64() - 1000.0).abs() < 1.0, "{wait:?}");
    }

    #[test]
    fn a_lifted_throttle_holds_nothing_back() {
        let t = Throttle::new(1000);
        let now = Instant::now();
        assert!(t.reserve(10_000, now) > Duration::from_secs(5));
        t.lift();
        assert_eq!(t.reserve(1_000_000, now), Duration::ZERO);
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
