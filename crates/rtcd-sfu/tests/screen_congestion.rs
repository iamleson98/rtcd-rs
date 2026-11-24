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
//! A run depends on what the bottleneck loses and on the order things happen in, so each scenario
//! is run over 16 seeds and judged on the spread of the figures: their median and their worst
//! decile, not the run a seed happens to give. `cargo test --release --test screen_congestion --
//! --nocapture` prints the spread of every scenario, and the figures of every run with
//! `SCREEN_CONGESTION_RUNS=1`.

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
        jitter: Duration::ZERO,
        delay: ONE_WAY,
        rate_bps: Some(rate_bps),
        queue: Duration::from_secs(2),
    }
}

/// Alice shares her screen and talks for `RUN` seconds in the world of `seed`; each viewer is
/// behind its path, which `change` may alter as the run goes (it is given the time shared so far).
fn lesson(
    seed: u64,
    paths: &[Path],
    mut change: impl FnMut(&mut World, &[usize], Duration),
) -> (World, Vec<usize>, Vec<Instant>, Instant) {
    let mut w = World::with_seed(engine_config(), seed);
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

/// The seeds every scenario is run over: `SCREEN_CONGESTION_SEEDS` of them, 16 unless set.
fn seeds() -> std::ops::Range<u64> {
    let n = std::env::var("SCREEN_CONGESTION_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16);
    0..n
}

/// The median and the worst decile (the 90th percentile) of a figure over the seeds, for a figure
/// that is the worse the greater it is.
fn spread(xs: impl Iterator<Item = f64>) -> (f64, f64) {
    let mut xs: Vec<f64> = xs.collect();
    (quantile(&mut xs, 0.5), quantile(&mut xs, 0.9))
}

/// The 10th percentile of a figure over the seeds, for a figure that is the worse the smaller it
/// is.
fn lowest_decile(xs: impl Iterator<Item = f64>) -> f64 {
    let mut xs: Vec<f64> = xs.collect();
    quantile(&mut xs, 0.1)
}

/// What a viewer behind a slow path must reach, as figures over the seeds: each is the most the
/// worst decile of the runs may have, and for the time the picture froze the median too.
struct Limits {
    /// Seconds packets waited in the queue: the 95th percentile of a run, and its worst.
    queue_p95: f64,
    queue_max: f64,
    /// The share of the voice lost, and how late the rest was (95th percentile, seconds).
    voice_lost: f64,
    voice_p95: f64,
    /// Seconds of the judged minute the picture froze: the median over the seeds, and the worst
    /// decile.
    frozen_median: f64,
    frozen: f64,
    /// Frames that could not be decoded.
    broken: f64,
    /// How late the picture was (95th percentile, seconds).
    delay_p95: f64,
}

fn runs_printed() -> bool {
    std::env::var("SCREEN_CONGESTION_RUNS").is_ok_and(|v| v == "1")
}

/// Judges what the viewer got in each of the runs of a scenario against `l`, and prints the
/// spread of the figures.
fn assert_served(runs: &[Seen], name: &str, l: &Limits) {
    if runs_printed() {
        for (seed, s) in runs.iter().enumerate() {
            s.print(&format!("{name}, seed {seed}"));
        }
    }
    let figure = |f: fn(&Seen) -> f64| spread(runs.iter().map(f));
    let queue_p95 = figure(|s| s.queue.1);
    let queue_max = figure(|s| s.queue.2);
    let voice_lost = figure(|s| 100.0 * s.voice_lost);
    let voice_p95 = figure(|s| s.voice_p95);
    let frozen = figure(|s| s.buffered.frozen);
    let broken = figure(|s| s.buffered.broken as f64);
    let delay_p95 = figure(|s| s.buffered.delay_p95);
    let frames = spread(runs.iter().map(|s| -(s.buffered.frames as f64)));
    eprintln!(
        "{name}, {} seeds, median / worst decile:\n  queue p95 {:.3} / {:.3} s, queue max {:.2} / {:.2} s\n  voice lost {:.1} / {:.1} %, p95 {:.3} / {:.3} s\n  frozen {:.1} / {:.1} s, broken {:.0} / {:.0}, delay p95 {:.2} / {:.2} s, frames {:.0} / {:.0}",
        runs.len(),
        queue_p95.0,
        queue_p95.1,
        queue_max.0,
        queue_max.1,
        voice_lost.0,
        voice_lost.1,
        voice_p95.0,
        voice_p95.1,
        frozen.0,
        frozen.1,
        broken.0,
        broken.1,
        delay_p95.0,
        delay_p95.1,
        -frames.0,
        -frames.1,
    );
    assert!(
        queue_p95.1 <= l.queue_p95,
        "{name}: the queue's 95th percentile is {:.2} s in the worst decile",
        queue_p95.1
    );
    assert!(
        queue_max.1 <= l.queue_max,
        "{name}: a packet waited {:.2} s in the worst decile",
        queue_max.1
    );
    assert!(
        voice_lost.1 <= 100.0 * l.voice_lost,
        "{name}: {:.1}% of the voice lost in the worst decile",
        voice_lost.1
    );
    assert!(
        voice_p95.1 <= l.voice_p95,
        "{name}: the voice's 95th percentile is {:.2} s in the worst decile",
        voice_p95.1
    );
    assert!(
        frozen.0 <= l.frozen_median,
        "{name}: the picture froze a median {:.1} s of the last minute",
        frozen.0
    );
    assert!(
        frozen.1 <= l.frozen,
        "{name}: the picture froze {:.1} s of the last minute in the worst decile",
        frozen.1
    );
    assert!(
        broken.1 <= l.broken,
        "{name}: {:.0} frames could not be decoded in the worst decile",
        broken.1
    );
    assert!(
        delay_p95.1 <= l.delay_p95,
        "{name}: the picture was {:.2} s late in the worst decile",
        delay_p95.1
    );
}

/// What a link that carries what is sent over it must reach: the packets wait a short while, the
/// voice arrives and the picture never freezes.
const SERVED: Limits = Limits {
    queue_p95: 0.4,
    queue_max: 1.0,
    voice_lost: 0.05,
    voice_p95: 0.5,
    frozen_median: 1.0,
    frozen: 1.0,
    broken: 0.0,
    delay_p95: 2.0,
};

// The links that carry less than they are sent (and lose packets on top) freeze the picture for
// seconds of the minute under any pacer, on some seeds more than on others. Their limits are the
// figures of the pacer before it released a packet in the write that brought it (rtc 44bf9277),
// over these 16 seeds and over 64, with a margin of half again on the worst decile and double on
// the median (a median over 16 seeds of a figure that is zero in half the runs is not steady).

/// 1.5 Mbit/s with 2 % loss: the old pacer froze a median 4.0 s over 64 seeds, 12.0 s in the worst
/// decile, with 48 frames broken and a queue that waited 0.58 s at its 95th percentile.
const BLOATED: Limits = Limits {
    queue_p95: 0.9,
    queue_max: 1.6,
    voice_lost: 0.04,
    voice_p95: 1.0,
    frozen_median: 8.0,
    frozen: 20.0,
    broken: 80.0,
    delay_p95: 1.8,
};

/// 1 Mbit/s with 2 % loss: the worst decile froze 3.8 s and broke 17 frames.
const NARROW: Limits = Limits {
    frozen: 6.0,
    broken: 26.0,
    ..SERVED
};

/// The slow viewer of `a_slow_viewer_costs_the_others_nothing`, on 1.5 Mbit/s with 2 % loss: a
/// worst decile of 6.9 s frozen and 32 frames broken.
const SLOW_VIEWER: Limits = Limits {
    queue_p95: 0.8,
    queue_max: 1.7,
    voice_lost: 0.04,
    voice_p95: 0.9,
    frozen_median: 9.0,
    frozen: 10.5,
    broken: 48.0,
    delay_p95: 1.7,
};

/// A viewer's figures over the judged minute of a lesson of `seed`.
fn watched(seed: u64, path: Path) -> Seen {
    let (w, viewers, voice, start) = lesson(seed, &[path], |_, _, _| {});
    let (from, to) = judged(start);
    seen(&w, viewers[0], &voice, from, to)
}

#[test]
fn a_viewer_behind_a_slow_bloated_link_freezes_a_few_seconds_a_minute() {
    for (rate, loss, limits) in [
        (1_500_000, 0.02, &BLOATED),
        (2_000_000, 0.01, &SERVED),
        (1_000_000, 0.02, &NARROW),
    ] {
        let runs: Vec<Seen> = seeds()
            .map(|seed| watched(seed, bottleneck(rate, loss)))
            .collect();
        let name = format!("{} kbit/s, {:.0}% loss", rate / 1000, loss * 100.0);
        assert_served(&runs, &name, limits);
        // Frames shown in the minute: what the worst decile of the runs shows.
        let frames = lowest_decile(runs.iter().map(|s| s.buffered.frames as f64));
        assert!(
            frames >= 120.0,
            "{name}: the worst decile showed {frames} frames in a minute"
        );
    }
}

#[test]
fn a_link_that_carries_part_of_the_large_layer_gets_it_at_a_lower_frame_rate() {
    // Two thirds of the large layer's rate: its base temporal layers fit, its top one does not.
    let runs: Vec<Seen> = seeds()
        .map(|seed| watched(seed, bottleneck(2_000_000, 0.01)))
        .collect();
    assert_served(&runs, "2 Mbit/s", &SERVED);
    let frames = lowest_decile(runs.iter().map(|s| s.buffered.frames as f64));
    assert!(
        frames >= (15 * 60 / 2 * 9 / 10) as f64,
        "half the frame rate of the large layer: the worst decile showed {frames} frames in a minute"
    );
    let large = lowest_decile(
        runs.iter()
            .map(|s| s.large_frames() as f64 / s.buffered.frames.max(1) as f64),
    );
    assert!(
        large >= 0.9,
        "the large layer, sharp text, is what is shown: {large:.2} of the frames in the worst decile"
    );
}

#[test]
fn a_link_that_carries_everything_gets_everything() {
    let runs: Vec<Seen> = seeds()
        .map(|seed| watched(seed, bottleneck(4_000_000, 0.01)))
        .collect();
    assert_served(&runs, "4 Mbit/s", &SERVED);
    let large = lowest_decile(runs.iter().map(|s| s.large_frames() as f64));
    assert!(
        large >= (15 * 60 * 95 / 100) as f64,
        "15 frames a second of the large layer: the worst decile showed {large}"
    );
}

#[test]
fn a_slow_viewer_costs_the_others_nothing() {
    let fast = Path {
        loss: 0.0,
        jitter: Duration::ZERO,
        delay: Duration::from_millis(20),
        rate_bps: None,
        queue: Duration::ZERO,
    };
    let mut slow = Vec::new();
    for seed in seeds() {
        let (w, viewers, voice, start) =
            lesson(seed, &[bottleneck(1_500_000, 0.02), fast], |_, _, _| {});
        let (from, to) = judged(start);
        slow.push(seen(&w, viewers[0], &voice, from, to));
        // The fast viewer's path loses nothing: whatever the seed, it gets everything.
        let quick = seen(&w, viewers[1], &voice, from, to);
        assert!(
            quick.large_frames() >= 15 * 60 * 95 / 100,
            "seed {seed}: the fast viewer gets the whole large layer: {:?}",
            quick.layers
        );
        assert_eq!(quick.asap.frozen, 0.0, "seed {seed}");
    }
    assert_served(&slow, "slow viewer", &SLOW_VIEWER);
}

#[test]
fn a_link_that_slows_down_mid_lesson_is_followed_within_seconds() {
    // 4 Mbit/s for the first minute, then 1.2.
    let slows = Duration::from_secs(60);
    let mut worsts = Vec::new();
    let mut drains = Vec::new();
    let mut after_runs = Vec::new();
    for seed in seeds() {
        let (w, viewers, voice, start) = lesson(seed, &[bottleneck(4_000_000, 0.01)], |w, v, t| {
            if t == slows {
                w.set_path(v[0], bottleneck(1_200_000, 0.01));
            }
        });
        let changed = start + slows;
        // The queue the large layer leaves behind in the moments before the change is seen, and
        // how long it takes to drain.
        let waits = &w.browsers[viewers[0]].queue_waits;
        worsts.push(
            waits
                .iter()
                .filter(|(t, _)| *t >= changed)
                .map(|(_, d)| d.as_secs_f64())
                .fold(0.0, f64::max),
        );
        drains.push(
            waits
                .iter()
                .filter(|(t, d)| *t >= changed && d.as_secs_f64() >= 0.2)
                .map(|(t, _)| t.duration_since(changed))
                .max()
                .unwrap_or_default()
                .as_secs_f64(),
        );
        let after = changed + Duration::from_secs(10);
        after_runs.push(seen(&w, viewers[0], &voice, after, judged(start).1));
    }
    let worst = spread(worsts.iter().copied());
    let drained = spread(drains.iter().copied());
    eprintln!(
        "4 then 1.2 Mbit/s, median / worst decile: worst wait {:.2} / {:.2} s, under 0.2 s after {:.1} / {:.1} s",
        worst.0, worst.1, drained.0, drained.1
    );
    assert!(
        worst.1 <= 1.5,
        "a packet waited {:.2} s in the worst decile",
        worst.1
    );
    assert!(
        drained.1 <= 8.0,
        "the queue took {:.1} s to drain in the worst decile",
        drained.1
    );
    assert_served(
        &after_runs,
        "4 then 1.2 Mbit/s, from 10 s after the change",
        &SERVED,
    );
}
