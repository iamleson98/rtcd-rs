//! What a viewer is sent when its plan caps what it receives: a teacher who sends a camera of two
//! layers (1.2 Mb/s large, 300 kb/s small) and shares a screen (1.5 Mb/s large, 500 kb/s small,
//! each in temporal layers), three students whose cameras are thumbnails (150 kb/s large, 50 kb/s
//! small), and three viewers behind a path that carries everything: one on a paid plan (2.0 Mb/s),
//! one on Free (1.5 Mb/s) and one with no budget.
//!
//! Left alone, the node sends the third about 3.2 Mb/s. The other two are held to their budgets,
//! less 100 kb/s kept for the voices: the cameras give up their large layers first, the screen
//! share is cut only after that, and the voices are never touched.
//!
//! `cargo test --release --test receiver_budget -- --nocapture` prints the figures of the run.

mod support;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rtcd_proto::MessageType;
use rtcd_sfu::config::EngineConfig;
use rtcd_sfu::engine::QUIET_OFFER_DELAY;
use rtcd_sfu::ids::{TrackType, parse_track_id};
use rtcd_sfu::metrics::Metrics;
use support::{Layer, World, describe, engine_config};

const FPS: u32 = 30;
const KEYFRAME_EVERY: u32 = 2 * FPS;
/// The rates of each camera layer, bits per second: the teacher's, then a student's.
const TEACHER: [u64; 2] = [300_000, 1_200_000];
const STUDENT: [u64; 2] = [50_000, 150_000];
/// The screen share's layers: the small one at five frames a second, the large at fifteen.
const SCREEN: [u64; 2] = [500_000, 1_500_000];
const VOICE_RESERVE: f64 = 100_000.0;
/// The first seconds settle: every sender's keyframe comes at once and reads as more than the
/// cameras cost, and what the budget cut waits ten seconds before it is taken up again.
const WARM_UP: u32 = 40;
const WINDOW: u32 = 10;

/// What the node counted of the budget cutting something, and in which second of the lesson.
#[derive(Default)]
struct Cuts {
    second: AtomicU32,
    log: Mutex<Vec<(u32, &'static str)>>,
}

impl Cuts {
    fn steps(&self) -> Vec<&'static str> {
        self.log.lock().unwrap().iter().map(|&(_, s)| s).collect()
    }

    /// The second of the lesson the last `step` was counted in.
    fn last(&self, step: &str) -> Option<u32> {
        let log = self.log.lock().unwrap();
        log.iter().rev().find(|(_, s)| *s == step).map(|&(t, _)| t)
    }
}

impl Metrics for Cuts {
    fn receiver_budget_limited(&self, step: &'static str) {
        let second = self.second.load(Ordering::Relaxed);
        self.log.lock().unwrap().push((second, step));
    }
}

/// What a viewer was sent over a window.
#[derive(Debug, Default)]
struct Seen {
    /// Bytes of cameras and screen share.
    video_bytes: usize,
    voices: usize,
    /// Packets of each camera (by sender), and how many of them on the large layer.
    cameras: Vec<(String, usize, usize)>,
    /// Packets of the screen share, and how many of them on the large layer.
    screen: usize,
    screen_large: usize,
    screen_bytes: usize,
}

impl Seen {
    fn video_rate(&self, seconds: u32) -> f64 {
        self.video_bytes as f64 * 8.0 / f64::from(seconds)
    }
}

struct Rig {
    w: World,
    /// The teacher first.
    senders: Vec<usize>,
    viewers: Vec<usize>,
    /// The teacher's camera layers, bits per second (small, large): changed during a lesson.
    teacher: [u64; 2],
    cuts: Arc<Cuts>,
    tick: u32,
    voices: u32,
    keyframe_requests: usize,
}

/// How many packets of what payload a frame of `bps` at `fps` is, so that the frames (and the
/// keyframe, four times one) make up its rate: packets of about a kilobyte.
fn frame_shape(bps: u64, fps: u32, key: bool) -> (usize, usize) {
    let bytes = (bps / 8 / u64::from(fps)) as usize;
    let packets = bytes.div_ceil(1000).max(1);
    let pad = (bytes / packets).saturating_sub(80).max(50);
    (if key { 4 * packets } else { packets }, pad)
}

impl Rig {
    /// A teacher, three students, and one viewer for each of `budgets` (kbit/s, 0: none).
    fn new(cuts: Arc<Cuts>, budgets: &[u32]) -> Rig {
        let cfg = EngineConfig {
            camera_budget: true,
            ..engine_config()
        };
        let mut w = World::with_metrics(cfg, cuts.clone());
        let mut senders = vec![w.join_host_with_camera("teacher", "call1")];
        for n in 0..3 {
            senders.push(w.join_with_camera(&format!("student{n}"), "call1"));
        }
        w.share_screen(senders[0]);
        let viewers: Vec<usize> = budgets
            .iter()
            .enumerate()
            .map(|(n, &kbps)| w.join_with_budget(&format!("viewer{n}"), "call1", kbps))
            .collect();
        // The voice streams the joins reserved are offered once their wait is over (see
        // `QUIET_OFFER_DELAY`), before anything is measured.
        w.run_for(QUIET_OFFER_DELAY + Duration::from_secs(2));
        // The senders watch nobody: only the viewers' downlinks are under test.
        for &s in &senders {
            let none = serde_json::json!({ "session_ids": [], "low": [] });
            w.signal(
                s,
                MessageType::VideoSubscribe,
                serde_json::to_vec(&none).unwrap(),
            );
        }
        Rig {
            w,
            senders,
            viewers,
            teacher: TEACHER,
            cuts,
            tick: 0,
            voices: 0,
            keyframe_requests: 0,
        }
    }

    /// Whether the sender `s` was asked for a keyframe of the stream sent with `ssrc` (the request
    /// is taken).
    fn asked(&mut self, s: usize, ssrc: u32) -> bool {
        let b = &mut self.w.browsers[s];
        let asked = b.keyframe_requests.contains(&ssrc);
        b.keyframe_requests.retain(|&r| r != ssrc);
        self.keyframe_requests += usize::from(asked);
        asked
    }

    /// Runs `seconds` of the lesson: 30 frames a second of every camera, the screen share, and the
    /// teacher's voice.
    fn run(&mut self, seconds: u32) {
        for _ in 0..seconds * FPS {
            for (k, &s) in self.senders.clone().iter().enumerate() {
                let ssrcs = self.w.browsers[s].camera.as_ref().unwrap().ssrcs;
                let rates = if k == 0 { self.teacher } else { STUDENT };
                let periodic = (self.tick + k as u32 * 7).is_multiple_of(KEYFRAME_EVERY);
                for layer in [Layer::Low, Layer::High] {
                    let key = self.asked(s, ssrcs[layer as usize]) || periodic;
                    let (packets, pad) = frame_shape(rates[layer as usize], FPS, key);
                    self.w.frame_pad = pad;
                    self.w.send_camera_frame(s, layer, key, packets);
                }
            }
            let teacher = self.senders[0];
            let ssrcs = self.w.browsers[teacher].screen.as_ref().unwrap().ssrcs;
            for (layer, every, fps) in [(Layer::Low, 6, 5), (Layer::High, 2, 15)] {
                if !self.tick.is_multiple_of(every) {
                    continue;
                }
                let key = self.asked(teacher, ssrcs[layer as usize])
                    || self.tick.is_multiple_of(10 * FPS);
                let (packets, _) = frame_shape(SCREEN[layer as usize], fps, key);
                self.w.frame_pad = 1000 - 80;
                self.w.send_screen_frame(teacher, layer, key, packets, true);
            }
            self.w.send_voice(teacher, self.voices);
            self.voices += 1;
            self.tick += 1;
            self.cuts.second.store(self.tick / FPS, Ordering::Relaxed);
            self.w.run_for(Duration::from_secs(1) / FPS);
        }
    }

    /// What each viewer is sent over the next `seconds`.
    fn measure(&mut self, seconds: u32) -> Vec<Seen> {
        let from: Vec<usize> = self
            .viewers
            .iter()
            .map(|&v| self.w.browsers[v].rtp.len())
            .collect();
        self.run(seconds);
        let mut out: Vec<Seen> = self.viewers.iter().map(|_| Seen::default()).collect();
        for (n, &v) in self.viewers.iter().enumerate() {
            let seen = &mut out[n];
            for (track, packet) in &self.w.browsers[v].rtp[from[n]..] {
                let Some((kind, session)) = parse_track_id(track) else {
                    continue;
                };
                let large = describe(packet).0 == Layer::High;
                match kind {
                    TrackType::Voice => seen.voices += 1,
                    TrackType::ScreenAudio => {}
                    TrackType::Screen => {
                        seen.screen += 1;
                        seen.screen_large += usize::from(large);
                        seen.screen_bytes +=
                            rtc::shared::marshal::MarshalSize::marshal_size(packet);
                        seen.video_bytes += rtc::shared::marshal::MarshalSize::marshal_size(packet);
                    }
                    TrackType::Video => {
                        match seen.cameras.iter_mut().find(|(s, ..)| s == session) {
                            Some((_, n, big)) => {
                                *n += 1;
                                *big += usize::from(large);
                            }
                            None => seen
                                .cameras
                                .push((session.to_owned(), 1, usize::from(large))),
                        }
                        seen.video_bytes += rtc::shared::marshal::MarshalSize::marshal_size(packet);
                    }
                }
            }
        }
        out
    }
}

impl Seen {
    /// Packets of the teacher's camera, and how many of them on the large layer.
    fn teacher(&self) -> (usize, usize) {
        self.cameras
            .iter()
            .find(|(s, ..)| s == "session-teacher")
            .map_or((0, 0), |&(_, n, large)| (n, large))
    }

    /// The cameras that flow whole: one sent less than nine tenths of its frames was paused for
    /// some of the window.
    fn flowing(&self) -> usize {
        let frames = (WINDOW * FPS) as usize;
        self.cameras
            .iter()
            .filter(|&&(_, n, _)| n * 10 >= frames * 9)
            .count()
    }
}

#[test]
fn a_receivers_plan_budget_holds_what_it_is_sent_and_never_the_voices() {
    let cuts = Arc::new(Cuts::default());
    let mut r = Rig::new(cuts.clone(), &[2000, 1500, 0]);
    // Even while it settles, what is sent stays near the budget: the first seconds, when every
    // keyframe comes at once, are the worst.
    for from in (0..WARM_UP).step_by(5) {
        let seen = r.measure(5);
        let (paid, free) = (&seen[0], &seen[1]);
        for (name, seen, budget) in [("paid", paid, 2.0e6), ("free", free, 1.5e6)] {
            assert!(
                seen.video_rate(5) <= 1.25 * budget,
                "{name} {:.2} Mb/s in the seconds from {from}",
                seen.video_rate(5) / 1e6
            );
        }
    }
    let requests = r.keyframe_requests;
    let [paid, free, open]: [Seen; 3] = r.measure(WINDOW).try_into().unwrap();
    let asked = r.keyframe_requests - requests;
    for (name, seen) in [("paid", &paid), ("free", &free), ("open", &open)] {
        eprintln!(
            "{name}: {:.2} Mb/s, {} voices, cameras {:?}, screen {} ({} large)",
            seen.video_rate(WINDOW) / 1e6,
            seen.voices,
            seen.cameras,
            seen.screen,
            seen.screen_large
        );
    }
    eprintln!("{asked} keyframes asked, cuts {:?}", cuts.steps());

    // What is sent fits the budget, less what is kept for the voices...
    assert!(
        paid.video_rate(WINDOW) <= 2_000_000.0 - VOICE_RESERVE,
        "paid {:.2} Mb/s",
        paid.video_rate(WINDOW) / 1e6
    );
    assert!(
        free.video_rate(WINDOW) <= 1_500_000.0 - VOICE_RESERVE,
        "free {:.2} Mb/s",
        free.video_rate(WINDOW) / 1e6
    );
    // ...with the budget the cause: what has none is sent far more, and a bigger budget is sent
    // more than a smaller one.
    assert!(open.video_rate(WINDOW) > 2_800_000.0);
    assert!(paid.video_rate(WINDOW) > free.video_rate(WINDOW));

    // The voices are all there, as for the viewer with no budget.
    assert!(open.voices > 0);
    assert_eq!(paid.voices, open.voices);
    assert_eq!(free.voices, open.voices);

    // The order: the cameras gave up their large layers first (the speaker's, which costs most),
    // and nothing else was cut for it: every camera still flows, and so does the screen share, on
    // its large layer.
    let (_, open_large) = open.teacher();
    assert!(open_large > 0, "no budget, no cut");
    for (name, seen) in [("paid", &paid), ("free", &free)] {
        assert_eq!(
            seen.teacher().1,
            0,
            "{name}: the speaker is on its small layer"
        );
        assert_eq!(seen.flowing(), 4, "{name}: no camera is paused");
        assert!(
            seen.screen_large > 0,
            "{name}: the screen share is still sent large"
        );
    }

    // The cuts were counted, and nothing flaps: keyframes are asked for only when a stream is
    // moved.
    assert!(cuts.steps().contains(&"camera_small"));
    assert!(asked <= 6, "{asked} keyframes asked");
}

/// The teacher's camera layers are changed to `teacher` (bits per second), and the lesson
/// goes on for `seconds`, a second at a time: what the viewer was sent of the screen share each
/// second, and whether every camera flowed.
fn lesson(r: &mut Rig, teacher: [u64; 2], seconds: u32) -> Vec<(usize, bool)> {
    r.teacher = teacher;
    (0..seconds)
        .map(|_| {
            let tight = r.measure(1).remove(0);
            (tight.screen_bytes * 8 / 1000, tight.cameras.len() == 4)
        })
        .collect()
}

/// `lesson`, until the budget has counted a cut of `step`.
fn lesson_until(r: &mut Rig, teacher: [u64; 2], cuts: &Cuts, step: &str) -> Vec<(usize, bool)> {
    let mut out = Vec::new();
    while cuts.last(step).is_none() {
        assert!(out.len() < 120, "no {step} in two minutes");
        out.extend(lesson(r, teacher, 1));
    }
    out
}

#[test]
fn a_tight_budget_cuts_the_screen_share_in_order_and_takes_it_up_again_after_a_hold() {
    let cuts = Arc::new(Cuts::default());
    let mut r = Rig::new(cuts.clone(), &[2400]);
    r.run(WARM_UP);
    let steady = lesson(&mut r, TEACHER, 20);
    eprintln!("steady {steady:?}");
    // The teacher's small layer grows: the cameras at their small layers leave the screen share
    // less, and it gives up temporal layers...
    let squeezed = lesson(&mut r, [550_000, 1_200_000], 40);
    eprintln!("squeezed {squeezed:?} {:?}", cuts.log.lock().unwrap());
    // ...and then its large layer.
    let tighter = lesson_until(&mut r, [1_800_000, 2_300_000], &cuts, "screen_small");
    // The cameras play all along, never paused for the screen share's sake.
    assert!(squeezed.iter().chain(&tighter).all(|&(_, flowing)| flowing));
    let steps = cuts.steps();
    let temporal = steps.iter().position(|&s| s == "screen_temporal");
    let small = steps.iter().position(|&s| s == "screen_small");
    assert!(temporal.is_some() && small.is_some(), "{steps:?}");
    assert!(temporal < small, "{steps:?}");

    // The teacher's camera is as before: the room comes back at once, and the screen share is
    // taken up again, a rung at a time, but not before the budget's hold has passed.
    let cut_at = cuts.log.lock().unwrap().last().unwrap().0;
    let relief_at = r.tick / FPS;
    let relief = lesson(&mut r, TEACHER, 60);
    let back = relief
        .iter()
        .position(|&(kbps, _)| kbps >= 1400)
        .map(|n| relief_at + n as u32 - cut_at)
        .expect("the screen share is taken up again");
    eprintln!("relief {relief:?} cut at {cut_at}, relief at {relief_at}, back after {back} s");
    // Held ten seconds, and then five rungs of the ladder (the base of the large layer, its
    // second layer, the whole) at one a second at the most.
    assert!(back >= 10 + 3, "back {back} s after the last cut");
    assert!(back < 60);
    let early = &relief[..(10 + cut_at).saturating_sub(relief_at) as usize];
    assert!(early.iter().all(|&(kbps, _)| kbps < 1000), "{early:?}");
}
