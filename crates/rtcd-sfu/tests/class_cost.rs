//! What the engine costs per packet it forwards when it carries what a video node carries: several
//! classes at once, with cameras, voices and the feedback real browsers send. Run it by hand, on a
//! quiet machine:
//!
//! ```text
//! cargo test --release -p rtcd-sfu --test class_cost -- --ignored --nocapture
//! ```
//!
//! `engine_cost` times a call of voices only; this is the mix of the profiled A1 node (589
//! sessions): one engine with `CLASSES` classes of a teacher and 30 students, the teacher's camera
//! and four students' with two layers each (a small and a large, with temporal layers), every
//! student watching the teacher and `WATCH` of the cameras, three open microphones a class with
//! 40 ms packets, the node's production settings (pacing on, redundant audio, the loudest three
//! voices), and browsers whose interceptors answer as a browser's do: transport-wide feedback,
//! receiver reports and NACKs for the packets a 0.5 % loss takes. Time moves in steps of 1 ms
//! with one `now` each, as a shard that wakes at each timer sees it.
//!
//! It times only the engine's own calls, split into datagram reads and timer wake-ups, and reports
//! the best of several rounds, which is steady where a CPU percentage under load is not.

mod support;

use std::time::{Duration, Instant};

use rtcd_proto::MessageType;
use rtcd_sfu::config::EngineConfig;
use rtcd_sfu::ids::{TrackType, parse_track_id};
use support::{Layer, Path, World, describe, engine_config};

const STUDENTS: usize = 30;
/// Students that send a camera, per class.
const CAMERAS: usize = 4;
/// Students that have a microphone open, per class.
const MICS: usize = 3;
const STEP: Duration = Duration::from_millis(1);
const VOICE_EVERY: Duration = Duration::from_millis(40);
/// The loss on every downlink, which makes browsers ask for packets again.
const LOSS: f64 = 0.005;

/// `CLASS_COST_CLASSES`, `_WATCH` (cameras besides the teacher's each student watches), `_WARMUP`
/// and `_SECONDS` (of virtual time) and `_ROUNDS` change the set-up and the length of the
/// measurement; `_CHUNK` is how many datagrams the engine takes in before it settles them (the
/// shard's is 32; 1 settles after each).
fn setting(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// The settings of a production video node (`rtcd`'s defaults, with 40 ms voice).
fn node() -> EngineConfig {
    EngineConfig {
        pacing: true,
        red: true,
        audio_loudest: 3,
        audio_ptime_ms: 40,
        ..engine_config()
    }
}

/// How a layer is sent: frames a second and bits a second (what lms-web's encodings and the load
/// simulator's clips give).
#[derive(Clone, Copy)]
struct Encoding {
    fps: u32,
    bps: u64,
}

const SMALL: Encoding = Encoding {
    fps: 15,
    bps: 150_000,
};
const TEACHER_LARGE: Encoding = Encoding {
    fps: 24,
    bps: 600_000,
};
const STUDENT_LARGE: Encoding = Encoding {
    fps: 20,
    bps: 400_000,
};
/// Seconds between the keyframes an encoder sends on its own.
const OVERHANG: f64 = 0.5;
const KEYFRAME_EVERY: u32 = 2;

/// How many packets of what payload a frame of an encoding is, so that the frames (and a keyframe,
/// four times one) make up its rate.
fn frame_shape(e: Encoding, key: bool, n: u32) -> (usize, usize) {
    let bytes = (e.bps / 8 / u64::from(e.fps)) as usize;
    let per = bytes as f64 / 1150.0 + if e.bps > SMALL.bps { OVERHANG } else { 0.0 };
    let packets = (((n + 1) as f64 * per).floor() - (n as f64 * per).floor()).max(1.0) as usize;
    let pad = (bytes / packets).saturating_sub(80).max(50);
    (if key { 4 * packets } else { packets }, pad)
}

/// A camera that sends a small and a large layer.
struct Camera {
    browser: usize,
    encodings: [Encoding; 2],
    next: [Instant; 2],
    sent: [u32; 2],
}

impl Camera {
    /// Sends the frames of each layer that are due at `w.now`, and a keyframe where the SFU asked
    /// for one or the encoder's own time has come.
    fn send_due(&mut self, w: &mut World) {
        let ssrcs = w.browsers[self.browser]
            .camera
            .as_ref()
            .expect("a camera")
            .ssrcs;
        for (l, layer) in [Layer::Low, Layer::High].into_iter().enumerate() {
            while self.next[l] <= w.now {
                let e = self.encodings[l];
                let asked = w.browsers[self.browser]
                    .keyframe_requests
                    .contains(&ssrcs[l]);
                if asked {
                    w.browsers[self.browser]
                        .keyframe_requests
                        .retain(|&r| r != ssrcs[l]);
                }
                let key = asked || self.sent[l].is_multiple_of(KEYFRAME_EVERY * e.fps);
                let (packets, pad) = frame_shape(e, key, self.sent[l]);
                w.frame_pad = pad;
                w.send_camera_frame(self.browser, layer, key, packets);
                self.sent[l] += 1;
                self.next[l] += Duration::from_secs(1) / e.fps;
            }
        }
    }
}

/// A microphone that sends a voice frame every 40 ms.
struct Mic {
    browser: usize,
    next: Instant,
    n: u32,
}

/// What the browsers received while the clock ran.
#[derive(Default, Clone, Copy)]
struct Heard {
    voice: usize,
    video: usize,
    /// Video packets of the large layers.
    large: usize,
    /// Video packets that reached a browser in the step they were sent in.
    video_same_step: usize,
}

/// What the engine did in a round.
struct Round {
    spent: Duration,
    reads: Duration,
    timers: Duration,
    datagrams: usize,
    timeouts: usize,
    transmits: usize,
    heard: Heard,
}

fn build(classes: usize, watch: usize, small: bool) -> (World, Vec<Camera>, Vec<Mic>) {
    let mut w = World::with_config(node());
    w.receive_chunk = setting("CLASS_COST_CHUNK", 32);
    // The shard's `FLUSH_AT`.
    w.flush_at = 2048;
    let mut cameras = Vec::new();
    let mut mics = Vec::new();
    for c in 0..classes {
        let call = format!("call{c}");
        let teacher = w.join_host_with_camera(&format!("c{c}t"), &call);
        cameras.push((teacher, [SMALL, TEACHER_LARGE]));
        for s in 0..STUDENTS {
            let name = format!("c{c}s{s}");
            let i = if s < CAMERAS {
                let i = w.join_with_camera(&name, &call);
                cameras.push((i, [SMALL, STUDENT_LARGE]));
                i
            } else {
                w.join(&name, &call)
            };
            if s < MICS {
                mics.push(i);
            }
            // The teacher and the next `watch` cameras in the ring, never one's own.
            let mut ids = vec![format!("session-c{c}t")];
            for k in 0..watch {
                let other = (s + 1 + k) % CAMERAS;
                if other != s {
                    ids.push(format!("session-c{c}s{other}"));
                }
            }
            let low: &[String] = if small { &ids } else { &[] };
            let data =
                serde_json::to_vec(&serde_json::json!({ "session_ids": ids, "low": low })).unwrap();
            w.signal(i, MessageType::VideoSubscribe, data);
        }
    }
    let path = Path {
        loss: LOSS,
        ..Default::default()
    };
    for i in 0..w.browsers.len() {
        w.set_path(i, path);
    }
    // Encoders and microphones are not in step with one another.
    let cameras = cameras
        .into_iter()
        .enumerate()
        .map(|(k, (browser, encodings))| Camera {
            browser,
            encodings,
            next: [0u64, 1]
                .map(|l| w.now + Duration::from_micros((k as u64 * 7_919 + l * 13_007) % 40_000)),
            sent: [0, 0],
        })
        .collect();
    let mics = mics
        .into_iter()
        .enumerate()
        .map(|(k, browser)| Mic {
            browser,
            next: w.now + Duration::from_micros(k as u64 * 3_001 % 40_000),
            n: 0,
        })
        .collect();
    (w, cameras, mics)
}

/// Runs `span` of virtual time in steps, with every camera and microphone sending as it would,
/// and counts what the browsers received.
fn run(w: &mut World, cameras: &mut [Camera], mics: &mut [Mic], span: Duration) -> Heard {
    let mut heard = Heard::default();
    let end = w.now + span;
    while w.now < end {
        w.step(STEP, |w| {
            for c in cameras.iter_mut() {
                c.send_due(w);
            }
            for m in mics.iter_mut() {
                while m.next <= w.now {
                    w.send_voice_at(m.browser, m.n, 20);
                    m.n += 1;
                    m.next += VOICE_EVERY;
                }
            }
        });
        let now = (w.now - w.epoch).as_micros() as u64;
        for b in &mut w.browsers {
            for (track, p) in b.rtp.drain(..) {
                match parse_track_id(&track) {
                    Some((TrackType::Voice, _)) => heard.voice += 1,
                    Some((TrackType::Video, _)) => {
                        heard.video += 1;
                        heard.large += usize::from(describe(&p).0 == Layer::High);
                        // The send time sits twelve bytes from the end (see `send_camera_frame`).
                        let at = p.payload.len() - 12;
                        let sent = u64::from_le_bytes(p.payload[at..at + 8].try_into().unwrap());
                        heard.video_same_step += usize::from(sent == now);
                    }
                    _ => {}
                }
            }
        }
    }
    heard
}

fn round(classes: usize, watch: usize, small: bool, warmup: Duration, span: Duration) -> Round {
    let (mut w, mut cameras, mut mics) = build(classes, watch, small);
    // The layers settle and the browsers' estimators and the pacer's history fill up.
    run(&mut w, &mut cameras, &mut mics, warmup);
    w.engine_spent = Duration::ZERO;
    w.reads_spent = Duration::ZERO;
    w.timers_spent = Duration::ZERO;
    w.datagrams_read = 0;
    w.timeouts_called = 0;
    w.transmits = 0;
    let heard = run(&mut w, &mut cameras, &mut mics, span);
    Round {
        spent: w.engine_spent,
        reads: w.reads_spent,
        timers: w.timers_spent,
        datagrams: w.datagrams_read,
        timeouts: w.timeouts_called,
        transmits: w.transmits,
        heard,
    }
}

#[test]
#[ignore = "a measurement, not a check"]
fn engine_cost_of_a_node_of_classes() {
    let classes = setting("CLASS_COST_CLASSES", 5);
    let watch = setting("CLASS_COST_WATCH", 1);
    let small = setting("CLASS_COST_SMALL", 1) == 1;
    let warmup = Duration::from_secs(setting("CLASS_COST_WARMUP", 4) as u64);
    let span = Duration::from_secs(setting("CLASS_COST_SECONDS", 3) as u64);
    let mut best: Option<Round> = None;
    for _ in 0..setting("CLASS_COST_ROUNDS", 3) {
        let r = round(classes, watch, small, warmup, span);
        if best.as_ref().is_none_or(|b| r.spent < b.spent) {
            best = Some(r);
        }
    }
    let r = best.unwrap();
    let delivered = r.heard.voice + r.heard.video;
    let ns = |d: Duration, n: usize| d.as_nanos() as f64 / n.max(1) as f64;
    println!(
        "{} sessions, {} s: engine {:.1} ms = reads {:.1} ms ({:.0} %) + timers {:.1} ms ({:.0} %)",
        classes * (STUDENTS + 1),
        span.as_secs(),
        r.spent.as_secs_f64() * 1e3,
        r.reads.as_secs_f64() * 1e3,
        100.0 * r.reads.as_secs_f64() / r.spent.as_secs_f64(),
        r.timers.as_secs_f64() * 1e3,
        100.0 * r.timers.as_secs_f64() / r.spent.as_secs_f64(),
    );
    println!(
        "{} datagrams read ({:.0} ns each, {:.0} ns per read call), {} timeouts ({:.0} ns each), {} datagrams sent",
        r.datagrams,
        ns(r.spent, r.datagrams),
        ns(r.reads, r.datagrams),
        r.timeouts,
        ns(r.timers, r.timeouts),
        r.transmits,
    );
    println!(
        "{} deliveries ({} voice, {} video of which {} large, {} of them in the step they were sent): {:.0} ns per delivery",
        delivered,
        r.heard.voice,
        r.heard.video,
        r.heard.large,
        r.heard.video_same_step,
        ns(r.spent, delivered),
    );
    println!(
        "out/in {:.2}, reads:timers {:.0}:{:.0}",
        r.transmits as f64 / r.datagrams.max(1) as f64,
        100.0 * r.reads.as_secs_f64() / r.spent.as_secs_f64(),
        100.0 * r.timers.as_secs_f64() / r.spent.as_secs_f64(),
    );
    assert!(delivered > 0, "nothing reached a browser");
}
