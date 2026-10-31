//! Which layer of a shared screen a viewer is sent over paths that lose, delay and limit packets,
//! with what the node reads of each path from its feedback. The sharer sends a screen at the rates
//! of the web client (500 kbit/s at 5 frames a second small, 2.5 Mbit/s at 15 large), here without
//! temporal layers (as a browser that sends none does; `screen_congestion` has them); the viewer is
//! a long way off (a round trip of 230 ms).
//!
//! A viewer starts on the small layer and is trusted with more as its path stays clear, so the
//! large layer comes a few tens of seconds in; each run lasts 90 s and the end of it is judged.

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use rtcd_sfu::metrics::Metrics;
use support::{Path, World, engine_config};

const LOW_BPS: u64 = 500_000;
const HIGH_BPS: u64 = 2_500_000;
const RUN: f64 = 90.0;

/// Per second of the share, whether the engine sent the viewer the large layer.
#[derive(Default)]
struct Tape(Mutex<Vec<bool>>);

impl Metrics for Tape {
    fn screen_layer_time(&self, large: bool, _took: Duration) {
        self.0.lock().unwrap().push(large);
    }
}

struct Run {
    large: Vec<bool>,
    /// Screen packets the viewer received.
    received: usize,
    /// Packets of the screen the viewer never got, though the sequence numbers around them came.
    missing: usize,
}

impl Run {
    /// The share of the last `secs` seconds that was sent in the large layer.
    fn large_share(&self, secs: usize) -> f64 {
        let tail = &self.large[self.large.len().saturating_sub(secs)..];
        tail.iter().filter(|&&l| l).count() as f64 / tail.len().max(1) as f64
    }
}

/// Alice shares her screen for 90 s to Bob, whose network is `path`.
fn share(path: Path) -> Run {
    let tape = Arc::new(Tape::default());
    let mut w = World::with_metrics(engine_config(), tape.clone());
    let alice = w.join("alice", "call1");
    let bob = w.join("bob", "call1");
    w.set_path(bob, path);
    w.share_screen(alice);
    w.run_for(Duration::from_secs(1));
    w.stream_screen(alice, RUN, LOW_BPS, HIGH_BPS);
    let received = w.browsers[bob]
        .rtp
        .iter()
        .filter(|(t, _)| t.starts_with("screen_"))
        .count();
    let large = tape.0.lock().unwrap().clone();
    let mut seqs: Vec<u16> = w.browsers[bob]
        .rtp
        .iter()
        .filter(|(t, _)| t.starts_with("screen_"))
        .map(|(_, p)| p.header.sequence_number)
        .collect();
    seqs.sort_unstable();
    seqs.dedup();
    let missing = seqs
        .windows(2)
        .map(|w| usize::from(w[1].wrapping_sub(w[0])) - 1)
        .sum();
    Run {
        large,
        received,
        missing,
    }
}

fn far(loss: f64, rate_bps: Option<u64>) -> Path {
    Path {
        loss,
        delay: Duration::from_millis(115),
        rate_bps,
        queue: Duration::from_millis(200),
    }
}

#[test]
fn a_viewer_a_long_way_off_gets_the_large_layer_once_its_path_has_shown_it_is_clear() {
    let run = share(far(0.0, None));
    assert_eq!(run.large_share(40), 1.0);
    assert!(
        run.large_share(RUN as usize) < 1.0,
        "a viewer starts on the small layer"
    );
}

#[test]
fn a_viewer_whose_path_loses_a_few_per_cent_at_random_gets_it_too() {
    // The loss half of the bandwidth estimate never leaves its floor on such a path: when it
    // decided, 3% or more kept the viewer on the small layer for good.
    for loss in [0.01, 0.03, 0.05, 0.08] {
        let run = share(far(loss, None));
        assert!(
            run.large_share(40) >= 0.95,
            "{:.0}% loss: the large layer was sent {:.0}% of the last 40 s",
            loss * 100.0,
            100.0 * run.large_share(40)
        );
    }
}

#[test]
fn a_viewer_whose_path_loses_a_sixth_of_the_packets_stays_on_the_small_layer() {
    let run = share(far(0.15, None));
    assert_eq!(run.large_share(RUN as usize), 0.0);
    assert!(run.received > 4_000, "the small layer keeps arriving");
}

#[test]
fn a_viewer_behind_a_link_that_cannot_carry_the_large_layer_is_kept_on_the_small_one() {
    // The large layer is not tried where the narrowest link cannot carry it, and is taken away
    // when it does not fit.
    for rate in [1_000_000, 1_800_000] {
        let run = share(far(0.0, Some(rate)));
        assert!(
            run.large_share(RUN as usize) <= 0.15,
            "a {} kbit/s link spent {:.0}% on the large layer",
            rate / 1000,
            100.0 * run.large_share(RUN as usize)
        );
        assert!(
            run.large_share(20) == 0.0,
            "and is not on it at the end of the run"
        );
        assert!(run.received > 4_000, "the small layer keeps arriving");
    }
}

#[test]
fn a_viewer_behind_a_link_that_can_carry_it_is_not_held_back() {
    let run = share(far(0.0, Some(3_000_000)));
    assert!(run.large_share(40) >= 0.95);
}

#[test]
fn what_a_lossy_path_drops_is_sent_again_until_next_to_nothing_is_missing() {
    // A packet that was lost and never came back would be a break in the picture.
    for loss in [0.03, 0.05, 0.08, 0.15] {
        let run = share(far(loss, None));
        let share = run.missing as f64 / (run.received + run.missing) as f64;
        assert!(
            share <= 0.001,
            "{:.0}% loss left {} packets missing ({:.2}%)",
            loss * 100.0,
            run.missing,
            100.0 * share
        );
    }
}
