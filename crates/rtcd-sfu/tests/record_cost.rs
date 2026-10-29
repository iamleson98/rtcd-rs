//! What recording adds to the engine's cost per packet. Run it by hand, on a quiet machine:
//!
//! ```text
//! cargo test --release -p rtcd-sfu --test record_cost -- --ignored --nocapture
//! ```
//!
//! Many rooms, a few speakers each, with and without every room recorded. It times only the
//! engine's own calls, as `engine_cost` does; the queues are emptied between frames, standing in
//! for the writers, which run on threads of their own.

mod support;

use std::time::Duration;

use rtcd_sfu::record::{RecordReceiver, record_channel};
use support::World;

const ROOMS: usize = 40;
const PEOPLE: usize = 5;
const SPEAKERS: usize = 2;

/// Engine time per uplink packet over `frames` frames of a world of `ROOMS` rooms, and the packets
/// the recordings were handed.
fn round(frames: usize, recorded: bool) -> (Duration, usize, usize) {
    let mut w = World::new();
    let mut speakers = Vec::new();
    for r in 0..ROOMS {
        for p in 0..PEOPLE {
            let i = w.join(&format!("r{r}p{p}"), &format!("room{r}"));
            if p < SPEAKERS {
                speakers.push(i);
            }
        }
    }
    let mut queues: Vec<RecordReceiver> = Vec::new();
    if recorded {
        for r in 0..ROOMS {
            let (sink, rx) = record_channel(1 << 14);
            w.engine
                .record_start(w.now, "group", &format!("room{r}"), "rec", w.now, sink);
            queues.push(rx);
        }
    }
    for &s in &speakers {
        w.speak(s, 5, |_| 40);
    }
    w.run_for(Duration::from_secs(1));
    for rx in &mut queues {
        while rx.try_recv().is_ok() {}
    }
    w.engine_spent = Duration::ZERO;
    let mut handed = 0;
    for f in 0..frames {
        for &s in &speakers {
            w.send_audio(s, if f % 2 == 0 { 20 } else { 70 });
        }
        w.run_for(Duration::from_millis(20));
        for rx in &mut queues {
            while rx.try_recv().is_ok() {
                handed += 1;
            }
        }
    }
    (w.engine_spent, frames * speakers.len(), handed)
}

#[test]
#[ignore = "a measurement, not a check"]
fn recording_every_room_adds_little_to_the_engine_cost_per_packet() {
    let frames = 150;
    let best = |recorded| {
        (0..5)
            .map(|_| round(frames, recorded))
            .min_by_key(|r| r.0)
            .unwrap()
    };
    let (plain, uplink, _) = best(false);
    let (recorded, _, handed) = best(true);
    let per = |d: Duration| d.as_nanos() as f64 / uplink as f64;
    println!(
        "{ROOMS} rooms of {PEOPLE}, {SPEAKERS} speaking: {uplink} uplink packets\n\
         not recorded: {:.2} ms, {:.0} ns per uplink packet\n\
         recorded:     {:.2} ms, {:.0} ns per uplink packet ({handed} packets handed to the queues)\n\
         difference:   {:.0} ns per uplink packet",
        plain.as_secs_f64() * 1e3,
        per(plain),
        recorded.as_secs_f64() * 1e3,
        per(recorded),
        per(recorded) - per(plain),
    );
    assert!(handed >= uplink * 9 / 10, "only {handed} packets recorded");
}
