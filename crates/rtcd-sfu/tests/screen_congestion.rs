//! A shared screen sent to viewers behind a bottleneck with a deep queue: the path of a viewer in a
//! lesson a long way from the media server (a round trip of 230 ms), on a link that carries less
//! than the large layer of the screen and buffers seconds of it before dropping anything (what a
//! home router or a mobile network does). The sharer talks all along, so what the queue does to
//! the voice that shares the link shows too.
//!
//! Each run shares the screen for two minutes, as a browser sends it (two simulcast layers, three
//! temporal layers each); the second minute is judged: how long the bottleneck's queue made packets
//! wait, how much of the voice arrived, and how the picture played (`support::watch`), shown as
//! soon as it is decoded and with the viewer's playout delay of a second (lms-web asks the browser
//! for it on the screen share).
//!
//! `cargo test --release --test screen_congestion -- --nocapture` prints the figures of every run.

mod support;

use std::time::{Duration, Instant};

use support::watch::{Playback, quantile};
use support::{Path, ScreenPlan, World, engine_config};

const LOW_BPS: u64 = 500_000;
const HIGH_BPS: u64 = 2_500_000;
const RUN: f64 = 120.0;
/// The last this many seconds of a run are judged.
const JUDGED: Duration = Duration::from_secs(60);
const ONE_WAY: Duration = Duration::from_millis(115);
/// The playout delay lms-web asks the browser for on a screen share.
const PLAYOUT: Duration = Duration::from_secs(1);

/// What one viewer got over the judged part of a run.
#[derive(Debug)]
struct Seen {
    /// Seconds packets waited in the bottleneck's queue: median, 95th percentile, worst.
    queue: (f64, f64, f64),
    /// Datagrams the full queue dropped.
    queue_drops: usize,
    /// The share of the voice frames that never arrived, and how late the rest were (95th
    /// percentile, seconds).
    voice_lost: f64,
    voice_p95: f64,
    /// The picture shown as soon as it is decoded, and with the playout delay.
    asap: Playback,
    buffered: Playback,
    /// Keyframes the viewer's browser asked for, over the whole run.
    keyframe_requests: usize,
    /// Frames shown, by layer (1 small, 2 large) and temporal layer.
    layers: Vec<((u8, Option<u8>), usize)>,
}

fn seen(w: &World, viewer: usize, voice_sent: &[Instant], from: Instant, to: Instant) -> Seen {
    let b = &w.browsers[viewer];
    let mut waits: Vec<f64> = b
        .queue_waits
        .iter()
        .filter(|(t, _)| *t >= from && *t < to)
        .map(|(_, d)| d.as_secs_f64())
        .collect();
    let queue = (
        quantile(&mut waits, 0.5),
        quantile(&mut waits, 0.95),
        quantile(&mut waits, 1.0),
    );
    let mut lost = 0usize;
    let mut sent = 0usize;
    let mut delays = Vec::new();
    for (n, &at) in voice_sent.iter().enumerate() {
        if at < from || at >= to {
            continue;
        }
        sent += 1;
        match b.voice_heard.get(&(n as u32)) {
            Some(heard) => delays.push(heard.saturating_duration_since(at).as_secs_f64()),
            None => lost += 1,
        }
    }
    Seen {
        queue,
        queue_drops: b
            .queue_drops
            .iter()
            .filter(|t| **t >= from && **t < to)
            .count(),
        voice_lost: lost as f64 / sent.max(1) as f64,
        voice_p95: quantile(&mut delays, 0.95),
        asap: b.screen_watch.playback(from, to, ONE_WAY, Duration::ZERO),
        buffered: b.screen_watch.playback(from, to, ONE_WAY, PLAYOUT),
        keyframe_requests: b.screen_watch.keyframe_requests,
        layers: b
            .screen_watch
            .shown_by_layer(from, to)
            .into_iter()
            .collect(),
    }
}

impl Seen {
    fn print(&self, name: &str) {
        let p = |x: &Playback| {
            format!(
                "{} frames, frozen {:.1} s, delay p50 {:.2} p95 {:.2} max {:.2} s, {} broken, {} incomplete",
                x.frames, x.frozen, x.delay_p50, x.delay_p95, x.delay_max, x.broken, x.incomplete
            )
        };
        eprintln!(
            "{name}:\n  queue p50 {:.3} p95 {:.3} max {:.3} s, {} dropped by the queue\n  voice lost {:.1}%, p95 {:.3} s\n  shown at once: {}\n  shown after {} s: {}\n  keyframes asked {}, layers shown {:?}",
            self.queue.0,
            self.queue.1,
            self.queue.2,
            self.queue_drops,
            100.0 * self.voice_lost,
            self.voice_p95,
            p(&self.asap),
            PLAYOUT.as_secs_f64(),
            p(&self.buffered),
            self.keyframe_requests,
            self.layers
        );
    }

    /// Frames shown of the large layer.
    fn large_frames(&self) -> usize {
        self.layers
            .iter()
            .filter(|((l, _), _)| *l == 2)
            .map(|(_, n)| n)
            .sum()
    }
}

fn bottleneck(rate_bps: u64, loss: f64) -> Path {
    Path {
        loss,
        delay: ONE_WAY,
        rate_bps: Some(rate_bps),
        queue: Duration::from_secs(2),
    }
}

/// Alice shares her screen and talks for `RUN` seconds; each viewer is behind its path, which
/// `change` may alter as the run goes (it is given the time shared so far).
fn lesson(
    paths: &[Path],
    mut change: impl FnMut(&mut World, &[usize], Duration),
) -> (World, Vec<usize>, Vec<Instant>, Instant) {
    let mut w = World::with_config(engine_config());
    // A browser asks again once a round trip, ten times.
    w.nack_retry = Some((Duration::from_millis(250), 10));
    let alice = w.join("alice", "call1");
    let viewers: Vec<usize> = (0..paths.len())
        .map(|n| w.join(&format!("viewer{n}"), "call1"))
        .collect();
    for (&v, &p) in viewers.iter().zip(paths) {
        w.set_path(v, p);
    }
    w.share_screen(alice);
    w.run_for(Duration::from_secs(1));
    let start = w.now;
    let plan = ScreenPlan {
        seconds: RUN,
        low_bps: LOW_BPS,
        high_bps: HIGH_BPS,
        temporal: true,
        voice: true,
        viewers: viewers.clone(),
    };
    let vs = viewers.clone();
    let voice = w.stream_screen_live(alice, &plan, |w, t| change(w, &vs, t));
    (w, viewers, voice, start)
}

/// The judged part of a run: the last minute but two seconds, so that what is still on its way
/// when the run ends is not counted as missing.
fn judged(start: Instant) -> (Instant, Instant) {
    let end = start + Duration::from_secs_f64(RUN) - Duration::from_secs(2);
    (end - JUDGED, end)
}

/// The figures every viewer behind a slow path must reach: packets wait a short while in the
/// queue, the voice arrives, and the picture plays without freezing once the playout delay is
/// there to let lost packets be sent again.
fn assert_served(s: &Seen, name: &str) {
    assert!(
        s.queue.1 <= 0.4,
        "{name}: the queue's 95th percentile is {:.2} s",
        s.queue.1
    );
    assert!(
        s.queue.2 <= 1.0,
        "{name}: a packet waited {:.2} s",
        s.queue.2
    );
    assert!(
        s.voice_lost <= 0.05,
        "{name}: {:.1}% of the voice lost",
        100.0 * s.voice_lost
    );
    assert!(
        s.voice_p95 <= 0.5,
        "{name}: the voice's 95th percentile is {:.2} s",
        s.voice_p95
    );
    assert!(
        s.buffered.frozen <= 1.0,
        "{name}: the picture froze {:.1} s of the last minute",
        s.buffered.frozen
    );
    assert_eq!(
        s.buffered.broken, 0,
        "{name}: frames that could not be decoded"
    );
    assert!(
        s.buffered.delay_p95 <= 2.0,
        "{name}: the picture was {:.2} s late",
        s.buffered.delay_p95
    );
}

#[test]
fn a_viewer_behind_a_slow_bloated_link_gets_a_picture_that_never_freezes() {
    for (rate, loss) in [(1_500_000, 0.02), (2_000_000, 0.01), (1_000_000, 0.02)] {
        let (w, viewers, voice, start) = lesson(&[bottleneck(rate, loss)], |_, _, _| {});
        let (from, to) = judged(start);
        let s = seen(&w, viewers[0], &voice, from, to);
        let name = format!("{} kbit/s, {:.0}% loss", rate / 1000, loss * 100.0);
        s.print(&name);
        assert_served(&s, &name);
        assert!(
            s.buffered.frames >= 120,
            "{name}: only {} frames shown in a minute",
            s.buffered.frames
        );
    }
}

#[test]
fn a_link_that_carries_part_of_the_large_layer_gets_it_at_a_lower_frame_rate() {
    // Two thirds of the large layer's rate: its base temporal layers fit, its top one does not.
    let (w, viewers, voice, start) = lesson(&[bottleneck(2_000_000, 0.01)], |_, _, _| {});
    let (from, to) = judged(start);
    let s = seen(&w, viewers[0], &voice, from, to);
    s.print("2 Mbit/s");
    assert_served(&s, "2 Mbit/s");
    assert!(
        s.buffered.frames >= 15 * 60 / 2 * 9 / 10,
        "half the frame rate of the large layer: {} frames shown in a minute",
        s.buffered.frames
    );
    assert!(
        s.large_frames() as f64 >= 0.9 * s.buffered.frames as f64,
        "the large layer, sharp text, is what is shown: {:?}",
        s.layers
    );
}

#[test]
fn a_link_that_carries_everything_gets_everything() {
    let (w, viewers, voice, start) = lesson(&[bottleneck(4_000_000, 0.01)], |_, _, _| {});
    let (from, to) = judged(start);
    let s = seen(&w, viewers[0], &voice, from, to);
    s.print("4 Mbit/s");
    assert_served(&s, "4 Mbit/s");
    assert!(
        s.large_frames() >= 15 * 60 * 95 / 100,
        "15 frames a second of the large layer: {:?}",
        s.layers
    );
}

#[test]
fn a_slow_viewer_costs_the_others_nothing() {
    let fast = Path {
        loss: 0.0,
        delay: Duration::from_millis(20),
        rate_bps: None,
        queue: Duration::ZERO,
    };
    let (w, viewers, voice, start) = lesson(&[bottleneck(1_500_000, 0.02), fast], |_, _, _| {});
    let (from, to) = judged(start);
    let slow = seen(&w, viewers[0], &voice, from, to);
    let quick = seen(&w, viewers[1], &voice, from, to);
    slow.print("slow viewer");
    quick.print("fast viewer");
    assert_served(&slow, "slow viewer");
    assert!(
        quick.large_frames() >= 15 * 60 * 95 / 100,
        "the fast viewer gets the whole large layer: {:?}",
        quick.layers
    );
    assert_eq!(quick.asap.frozen, 0.0);
}

#[test]
fn a_link_that_slows_down_mid_lesson_is_followed_within_seconds() {
    // 4 Mbit/s for the first minute, then 1.2.
    let slows = Duration::from_secs(60);
    let (w, viewers, voice, start) = lesson(&[bottleneck(4_000_000, 0.01)], |w, v, t| {
        if t == slows {
            w.set_path(v[0], bottleneck(1_200_000, 0.01));
        }
    });
    let changed = start + slows;
    // The queue the large layer leaves behind in the moments before the change is seen, and how
    // long it takes to drain.
    let waits = &w.browsers[viewers[0]].queue_waits;
    let worst = waits
        .iter()
        .filter(|(t, _)| *t >= changed)
        .map(|(_, d)| d.as_secs_f64())
        .fold(0.0, f64::max);
    let drained = waits
        .iter()
        .filter(|(t, d)| *t >= changed && d.as_secs_f64() >= 0.2)
        .map(|(t, _)| t.duration_since(changed))
        .max()
        .unwrap_or_default();
    eprintln!("4 then 1.2 Mbit/s: worst wait {worst:.2} s, under 0.2 s after {drained:?}");
    assert!(worst <= 1.5, "a packet waited {worst:.2} s");
    assert!(
        drained <= Duration::from_secs(8),
        "the queue took {drained:?} to drain"
    );
    let after = changed + Duration::from_secs(10);
    let s = seen(&w, viewers[0], &voice, after, judged(start).1);
    s.print("4 then 1.2 Mbit/s, from 10 s after the change");
    assert_served(&s, "4 then 1.2 Mbit/s");
}
