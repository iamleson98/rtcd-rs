//! Finds where a shard's loop is held up: which passes took long and how much of that went on
//! offers and answers.
//!
//! A speaker change makes every session of a big class renegotiate, and the media already flowing
//! queues behind that work. The shard's busy fraction averages such stalls away, so this keeps the
//! worst pass of each second and speaks up only when it was slow.

use std::time::{Duration, Instant};

use tracing::info;

/// A pass of the loop longer than this is worth a line in the log.
const SLOW_TURN: Duration = Duration::from_millis(10);

/// How long a window of passes lasts, which is also the longest the log is silent between lines.
const WINDOW: Duration = Duration::from_secs(1);

/// What the passes of one window did.
#[derive(Debug, Default, PartialEq, Eq)]
struct Window {
    longest: Duration,
    offers: u32,
    offer_time: Duration,
    answers: u32,
    answer_time: Duration,
}

/// The measurements of one engine, kept per window.
#[derive(Debug)]
pub(super) struct TurnTrace {
    window_began: Instant,
    window: Window,
}

impl TurnTrace {
    pub(super) fn new() -> Self {
        Self {
            window_began: Instant::now(),
            window: Window::default(),
        }
    }

    /// Counts an offer that took `took` to make.
    pub(super) fn offer(&mut self, took: Duration) {
        self.window.offers += 1;
        self.window.offer_time += took;
    }

    /// Counts an answer that took `took` to handle.
    pub(super) fn answer(&mut self, took: Duration) {
        self.window.answers += 1;
        self.window.answer_time += took;
    }

    /// Ends a pass that took `took` at `now`; once a window is over, logs it if its longest pass
    /// was slow, and starts the next.
    pub(super) fn end_turn(&mut self, shard: usize, sessions: usize, took: Duration, now: Instant) {
        self.window.longest = self.window.longest.max(took);
        if now.saturating_duration_since(self.window_began) < WINDOW {
            return;
        }
        let w = std::mem::take(&mut self.window);
        self.window_began = now;
        if w.longest > SLOW_TURN {
            info!(
                shard,
                sessions,
                longest_turn_ms = w.longest.as_millis() as u64,
                offers = w.offers,
                offer_ms = w.offer_time.as_millis() as u64,
                answers = w.answers,
                answer_ms = w.answer_time.as_millis() as u64,
                "slow loop pass"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_window_is_reset_when_it_ends_and_keeps_the_longest_pass() {
        let t0 = Instant::now();
        let mut trace = TurnTrace::new();
        trace.window_began = t0;
        trace.offer(Duration::from_millis(2));
        trace.answer(Duration::from_millis(3));
        trace.end_turn(
            0,
            5,
            Duration::from_millis(30),
            t0 + Duration::from_millis(500),
        );
        trace.end_turn(
            0,
            5,
            Duration::from_millis(4),
            t0 + Duration::from_millis(900),
        );
        assert_eq!(trace.window.longest, Duration::from_millis(30));
        assert_eq!(trace.window.offers, 1);
        trace.end_turn(0, 5, Duration::from_millis(1), t0 + Duration::from_secs(1));
        assert_eq!(trace.window, Window::default());
        assert_eq!(trace.window_began, t0 + Duration::from_secs(1));
    }
}
