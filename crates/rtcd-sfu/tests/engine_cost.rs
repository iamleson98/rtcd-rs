//! What the engine costs per packet it forwards. Run it by hand, on a quiet machine:
//!
//! ```text
//! cargo test --release -p rtcd-sfu --test engine_cost -- --ignored --nocapture
//! ```
//!
//! It times only the engine's own calls (the emulated browsers' work is left out) and reports the
//! best of several rounds, which is steady where a CPU percentage under load is not.

mod support;

use std::time::Duration;

use support::World;

const LISTENERS: usize = 29;
const SPEAKERS: usize = 3;

/// `ENGINE_COST_FRAMES` and `ENGINE_COST_ROUNDS` change the length of the measurement, for
/// profiling it with the set-up a small part of the run.
fn setting(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// One round: a call of `SPEAKERS + LISTENERS` participants, `SPEAKERS` of them talking for
/// `frames` frames. Returns engine time per datagram the engine received, and how many voice
/// packets reached the listeners.
fn round(frames: usize) -> (Duration, usize, usize) {
    let mut w = World::new();
    let all: Vec<usize> = (0..SPEAKERS + LISTENERS)
        .map(|n| w.join(&format!("p{n}"), "call1"))
        .collect();
    // Everyone is up and the speakers' tracks are announced before the clock starts.
    for &s in &all[..SPEAKERS] {
        w.speak(s, 5, |_| 40);
    }
    w.run_for(Duration::from_secs(1));
    let before: usize = w.browsers.iter().map(|b| b.rtp.len()).sum();
    w.engine_spent = Duration::ZERO;
    for f in 0..frames {
        for &s in &all[..SPEAKERS] {
            w.send_audio(s, if f % 2 == 0 { 20 } else { 70 });
        }
        w.run_for(Duration::from_millis(20));
    }
    let after: usize = w.browsers.iter().map(|b| b.rtp.len()).sum();
    (w.engine_spent, frames * SPEAKERS, after - before)
}

#[test]
#[ignore = "a measurement, not a check"]
fn engine_cost_per_forwarded_packet() {
    let mut best: Option<(Duration, usize, usize)> = None;
    let frames = setting("ENGINE_COST_FRAMES", 150);
    for _ in 0..setting("ENGINE_COST_ROUNDS", 5) {
        let r = round(frames);
        if best.is_none_or(|b| r.0 < b.0) {
            best = Some(r);
        }
    }
    let (spent, uplink, delivered) = best.unwrap();
    println!(
        "engine: {:.2} ms for {} uplink packets fanned out to {} deliveries: {:.0} ns per uplink packet, {:.0} ns per delivery",
        spent.as_secs_f64() * 1e3,
        uplink,
        delivered,
        spent.as_nanos() as f64 / uplink as f64,
        spent.as_nanos() as f64 / delivered.max(1) as f64,
    );
    assert!(
        delivered >= uplink * (LISTENERS + SPEAKERS - 1) * 9 / 10,
        "only {delivered} deliveries"
    );
}
