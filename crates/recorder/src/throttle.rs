//! A token bucket for the bytes a process uploads, shared by every upload in it.
//!
//! Recording must not compete with the media the node sends, so uploads are held to a rate. The
//! bucket is paid before the bytes go: a caller reserves its bytes, which may leave the bucket in
//! debt, and waits until the debt is repaid. A chunk larger than the bucket therefore never
//! blocks for good, and callers that arrive together are served in the order they reserved.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant};

/// How much unused rate is kept for a burst: a quarter of a second's worth.
const BURST: f64 = 0.25;
/// The whole rate, in thousandths.
const FULL: u32 = 1000;

/// Longest sleep before a waiter looks again at the rate, which may have changed.
const SLICE: Duration = Duration::from_millis(500);

#[derive(Debug)]
struct State {
    /// Bytes the rate has allowed so far, and bytes reserved so far, both counted from the start;
    /// what may be sent at once is the difference (negative while reservations are waited out).
    /// A reservation's place in the queue is the total reserved up to it, so a waiter can ask
    /// again later, at whatever the rate has become, how long is left.
    allowed: f64,
    reserved: f64,
    at: Instant,
}

/// Holds the bytes sent through it to a rate.
#[derive(Debug)]
pub struct Throttle {
    /// Bytes per second.
    rate: f64,
    /// How much of `rate` is let through, in thousandths (see [`Throttle::set_share`]), and the
    /// least it may be set to (see [`Throttle::may_yield_to`]).
    share: AtomicU32,
    floor: AtomicU32,
    state: Mutex<State>,
    /// The rate no longer applies (see [`Throttle::lift`]).
    lifted: AtomicBool,
    /// How many requests are being sent through it (see [`Throttle::enter`]).
    in_flight: AtomicU32,
}

/// A request being sent through a [`Throttle`], counted while it lives.
#[derive(Debug)]
pub struct Flight(Arc<Throttle>);

impl Drop for Flight {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Throttle {
    /// A bucket that lets `bytes_per_second` through (at least one byte a second).
    pub fn new(bytes_per_second: u64) -> Self {
        let rate = bytes_per_second.max(1) as f64;
        Self {
            rate,
            share: AtomicU32::new(FULL),
            floor: AtomicU32::new(FULL),
            state: Mutex::new(State {
                allowed: rate * BURST,
                reserved: 0.0,
                at: Instant::now(),
            }),
            lifted: AtomicBool::new(false),
            in_flight: AtomicU32::new(0),
        }
    }

    /// Counts a request as being sent through the throttle until the returned guard is dropped.
    pub fn enter(self: &Arc<Self>) -> Flight {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        Flight(self.clone())
    }

    /// The rate, in bytes a second, each request in flight gets when the throttle is held to its
    /// share `permille` thousandths of the whole (and at least this one request is in flight).
    pub fn fair_rate(&self, permille: u32) -> f64 {
        let in_flight = self.in_flight.load(Ordering::Relaxed).max(1);
        self.rate * f64::from(permille.clamp(1, FULL)) / f64::from(FULL) / f64::from(in_flight)
    }

    /// Lets only `permille` thousandths of the rate through from now on (1,000: all of it; at
    /// least one). Uploads stand aside like this while the network is short of room for the media
    /// the same node sends: a file that waits an extra minute costs nothing, and a call that
    /// loses packets costs a class.
    pub fn set_share(&self, permille: u32) {
        self.share.store(permille.clamp(1, FULL), Ordering::Relaxed);
    }

    /// Says that the share may be set as low as `permille` thousandths later (by whoever makes
    /// uploads stand aside), so that [`Throttle::slowest_share`] allows for it.
    pub fn may_yield_to(&self, permille: u32) {
        self.floor.store(permille.clamp(1, FULL), Ordering::Relaxed);
    }

    /// The slowest share a request sent now may find itself held to: the current one, or the
    /// lowest it may be set to if that is lower.
    pub fn slowest_share(&self) -> u32 {
        self.share().min(self.floor.load(Ordering::Relaxed))
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

    fn rate_now(&self) -> f64 {
        self.rate * f64::from(self.share.load(Ordering::Relaxed)) / f64::from(FULL)
    }

    /// Lets the time since the last call pay for bytes at the rate in force, up to a burst.
    fn advance(&self, s: &mut State, now: Instant, rate: f64) {
        let elapsed = now.saturating_duration_since(s.at).as_secs_f64();
        s.at = s.at.max(now);
        s.allowed = (s.allowed + elapsed * rate).min(s.reserved + rate * BURST);
    }

    /// Reserves `bytes` at `now`: the place in the queue, and how long to wait for it at the rate
    /// in force now.
    fn ticket(&self, bytes: u64, now: Instant) -> (f64, Duration) {
        if self.lifted.load(Ordering::Acquire) {
            return (0.0, Duration::ZERO);
        }
        let rate = self.rate_now();
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        self.advance(&mut s, now, rate);
        s.reserved += bytes as f64;
        let ticket = s.reserved;
        (ticket, wait(&s, ticket, rate))
    }

    /// How long is left to wait for a place in the queue, at the rate in force now.
    fn remaining(&self, ticket: f64, now: Instant) -> Duration {
        if self.lifted.load(Ordering::Acquire) {
            return Duration::ZERO;
        }
        let rate = self.rate_now();
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        self.advance(&mut s, now, rate);
        wait(&s, ticket, rate)
    }

    /// Reserves `bytes` at `now` and says how long to wait before sending them.
    pub fn reserve(&self, bytes: u64, now: Instant) -> Duration {
        self.ticket(bytes, now).1
    }

    /// Waits until `bytes` may be sent. The wait follows the rate: bytes reserved while uploads
    /// stood aside go as soon as the rate is back, not when the slow rate would have let them.
    pub async fn take(&self, bytes: u64) {
        let (ticket, mut left) = self.ticket(bytes, Instant::now());
        while !left.is_zero() {
            tokio::time::sleep(left.min(SLICE)).await;
            left = self.remaining(ticket, Instant::now());
        }
    }
}

fn wait(s: &State, ticket: f64, rate: f64) -> Duration {
    if s.allowed >= ticket {
        Duration::ZERO
    } else {
        Duration::from_secs_f64((ticket - s.allowed) / rate)
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
    fn a_waiter_follows_the_rate_when_it_changes() {
        let t = Throttle::new(1000);
        let now = Instant::now();
        t.set_share(100);
        // 100 bytes a second after a burst of 25: a reservation of 1250 waits 12.25 seconds.
        let (ticket, wait) = t.ticket(1250, now);
        assert!((wait.as_secs_f64() - 12.25).abs() < 0.01, "{wait:?}");
        // A second later the rate is half the full one: 25 + 500 bytes have been allowed and 725
        // are left, 1.45 seconds at the new rate, where the old rate would have taken 7 more.
        t.set_share(500);
        let left = t.remaining(ticket, now + Duration::from_secs(1));
        assert!((left.as_secs_f64() - 1.45).abs() < 0.01, "{left:?}");
    }

    #[tokio::test]
    async fn a_sleeper_wakes_early_when_the_rate_returns() {
        // 1,000 bytes a second after the burst: 5,000 bytes would take five seconds.
        let t = std::sync::Arc::new(Throttle::new(100_000));
        t.set_share(10);
        let started = Instant::now();
        let waiting = {
            let t = t.clone();
            tokio::spawn(async move { t.take(5_000).await })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        t.set_share(1000);
        waiting.await.unwrap();
        assert!(
            started.elapsed() < Duration::from_millis(1500),
            "{:?}",
            started.elapsed()
        );
    }

    #[test]
    fn waiters_are_served_in_the_order_they_reserved_whatever_the_rate_does() {
        let t = Throttle::new(1000);
        let now = Instant::now();
        // The burst is 250 bytes: the first is ready after 0.25 s, the second after 0.75 s.
        let (first, _) = t.ticket(500, now);
        let (second, _) = t.ticket(500, now);
        let at = now + Duration::from_millis(400);
        assert!(t.remaining(first, at) < t.remaining(second, at));
        let ready = now + Duration::from_millis(550);
        assert!(t.remaining(first, ready).is_zero());
        let left = t.remaining(second, ready).as_secs_f64();
        assert!((left - 0.2).abs() < 0.01, "{left}");
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
