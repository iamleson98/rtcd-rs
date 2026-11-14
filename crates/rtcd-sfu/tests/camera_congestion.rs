//! A lesson's cameras sent to a viewer whose downlink carries less than all of them: a teacher and
//! seven students, each a single-layer camera of about 420 kb/s (what the load tests run), to a
//! viewer behind a link of 2.5 Mb/s that drops what a short queue cannot hold (what a limited
//! home connection does).
//!
//! Left alone, the SFU sends all eight, about 3.4 Mb/s, and the link drops a quarter of the
//! packets at random: every camera's picture breaks. The SFU holds the cameras to what the link
//! delivers instead: the teacher's first, the rest as many as fit, the others paused until there is
//! room for them again, each started again at a keyframe.
//!
//! `cargo test --release --test camera_congestion -- --nocapture` prints the figures of every run.

mod support;

use std::collections::BTreeMap;
use std::time::Duration;

use rtcd_proto::MessageType;
use rtcd_sfu::camera_budget::MAX_PAUSE;
use rtcd_sfu::config::EngineConfig;
use rtcd_sfu::ids::{TrackType, parse_track_id};
use rtcd_sfu::vp8::Descriptor;
use support::{Layer, Path, ScreenPlan, World, describe, engine_config};

const CAMERA_PAD: usize = 800;
const STUDENTS: usize = 7;
const FPS: u32 = 30;
/// Packets of a frame, and of a keyframe, which comes every two seconds.
const FRAME_PACKETS: usize = 2;
const KEYFRAME_PACKETS: usize = 6;
const KEYFRAME_EVERY: u32 = 2 * FPS;
/// The large layer of a camera that sends two: three times the packets of the small one, so that
/// a frame says which layer it is of by its size as well as by what it carries.
const LARGE_PACKETS: usize = 3 * FRAME_PACKETS;
const LARGE_KEYFRAME_PACKETS: usize = 3 * KEYFRAME_PACKETS;
const LINK: u64 = 2_500_000;
/// The gap before a packet that begins a burst in the estimator (`bwe::BURST_GAP`).
const BURST_GAP: Duration = Duration::from_millis(5);
/// The longest wait after a step that failed (`layers::MAX_BACKOFF`), and how long a step is on
/// trial (`layers::TRIAL`).
const LONGEST_WAIT: Duration = Duration::from_secs(20);
const TRIAL: Duration = Duration::from_secs(3);

/// A viewer, the teacher and the students, and what the viewer was sent.
struct Class {
    w: World,
    viewer: usize,
    /// The teacher first.
    senders: Vec<usize>,
    /// The cameras send three temporal layers, as a browser's encoder does.
    temporal: bool,
    /// How many of the cameras, the teacher's first, send a large layer too (simulcast), whole at
    /// every frame, as well as the small one.
    simulcast: usize,
    /// Packets of a keyframe, and frames between the ones the encoder sends on its own.
    keyframe_packets: usize,
    keyframe_every: u32,
    frame: u32,
    /// Packets of the viewer's `rtp` already counted by `window`.
    counted: usize,
    /// Bits per second of the small and the large layer of each camera, the teacher's first: its
    /// frames are sized to make them up (see `frame_shape`). Empty: every frame is the same size.
    rates: Vec<[u64; 2]>,
    /// The teacher shares a screen, a small layer of about a hundred kilobits a second.
    screen: bool,
}

/// How many packets of what payload a frame of a layer of `bps` is, so that the frames (and the
/// keyframe, four times one) make up its rate: packets of about a kilobyte, and the least of them.
fn frame_shape(bps: u64, key: bool) -> (usize, usize) {
    let bytes = (bps / 8 / u64::from(FPS)) as usize;
    let packets = bytes.div_ceil(1000).max(1);
    let pad = (bytes / packets).saturating_sub(80).max(50);
    (if key { 4 * packets } else { packets }, pad)
}

/// What the viewer was sent over a window, by camera (the session that sends it).
#[derive(Debug, Default)]
struct Window {
    packets: BTreeMap<String, usize>,
    /// Whether the first packet of each camera in the window was a keyframe.
    began_with_keyframe: BTreeMap<String, bool>,
    /// The layer the first packet of each camera in the window was of.
    began_on: BTreeMap<String, Layer>,
    drops: usize,
    bytes: usize,
    keyframe_requests: usize,
    /// Frames of the window that arrived with some of their packets missing (the first and the
    /// last of each camera, which the window may cut, are not counted).
    incomplete_frames: usize,
}

/// The engine with the camera budget on (it is off by default).
fn budget_on() -> EngineConfig {
    EngineConfig {
        camera_budget: true,
        ..engine_config()
    }
}

impl Class {
    fn new(path: Path) -> Class {
        Class::build(path, true)
    }

    /// The viewer comes in `first`, or after the cameras, which then are all asked for a
    /// keyframe at once.
    fn build(path: Path, first: bool) -> Class {
        Class::in_world(World::with_config(budget_on()), path, first)
    }

    fn in_world(w: World, path: Path, first: bool) -> Class {
        Class::of(w, path, first, STUDENTS)
    }

    /// A class of the teacher and `students`.
    fn of(mut w: World, path: Path, first: bool, students: usize) -> Class {
        let early = first.then(|| w.join("viewer", "call1"));
        let mut senders = vec![w.join_host_with_camera("teacher", "call1")];
        for n in 0..students {
            senders.push(w.join_with_camera(&format!("student{n}"), "call1"));
        }
        let viewer = early.unwrap_or_else(|| w.join("viewer", "call1"));
        // The senders watch nobody: only the viewer's downlink is under test.
        for &s in &senders {
            let none = serde_json::json!({ "session_ids": [], "low": [] });
            w.signal(
                s,
                MessageType::VideoSubscribe,
                serde_json::to_vec(&none).unwrap(),
            );
        }
        w.set_path(viewer, path);
        w.payload_pad = CAMERA_PAD;
        Class {
            w,
            viewer,
            senders,
            temporal: false,
            simulcast: 0,
            keyframe_packets: KEYFRAME_PACKETS,
            keyframe_every: KEYFRAME_EVERY,
            frame: 0,
            counted: 0,
            rates: Vec::new(),
            screen: false,
        }
    }

    /// Every camera sends a frame at 30 per second (a keyframe every two seconds, one when the SFU
    /// asks for it), spread over the frame time as unsynchronised encoders are. With so many
    /// cameras that an even spread would put every frame closer to the one before than the gap that
    /// begins a burst (see `bwe::BURST_GAP`), which no real set of encoders does, they are spread
    /// unevenly, so that the path's queue can be read from the frames that follow a gap. A camera that
    /// sends two layers sends a frame of each, and a keyframe of each layer is asked for on its
    /// own.
    fn run(&mut self, seconds: u32) {
        let slot = Duration::from_secs(1) / FPS / self.senders.len() as u32;
        let uneven = slot < BURST_GAP;
        for _ in 0..seconds * FPS {
            for (k, &s) in self.senders.iter().enumerate() {
                let layers: &[Layer] = if k < self.simulcast {
                    &[Layer::Low, Layer::High]
                } else {
                    &[Layer::Low]
                };
                let b = &mut self.w.browsers[s];
                let ssrcs = b.camera.as_ref().expect("a camera").ssrcs;
                let asked: Vec<Layer> = layers
                    .iter()
                    .copied()
                    .filter(|&l| b.keyframe_requests.contains(&ssrcs[l as usize]))
                    .collect();
                b.keyframe_requests.clear();
                b.keyframe_log
                    .extend(asked.iter().map(|&l| ssrcs[l as usize]));
                let periodic = (self.frame + k as u32 * 7).is_multiple_of(self.keyframe_every);
                for &layer in layers {
                    let key = periodic || asked.contains(&layer);
                    let mut packets = match (layer, key) {
                        (Layer::Low, true) => self.keyframe_packets,
                        (Layer::Low, false) => FRAME_PACKETS,
                        (Layer::High, true) => LARGE_KEYFRAME_PACKETS,
                        (Layer::High, false) => LARGE_PACKETS,
                    };
                    if let Some(bps) = self.rates.get(k).map(|r| r[layer as usize]) {
                        let (n, pad) = frame_shape(bps, key);
                        packets = n;
                        self.w.payload_pad = pad;
                        self.w.frame_pad = pad;
                    }
                    if self.temporal {
                        self.w.send_camera_frame(s, layer, key, packets);
                    } else {
                        self.w.send_video(s, false, layer, key, packets);
                    }
                }
                self.w.run_for(match (uneven, k % 2) {
                    (false, _) => slot,
                    (true, 0) => slot / 2,
                    (true, _) => slot * 3 / 2,
                });
            }
            if self.screen && self.frame.is_multiple_of(6) {
                // Five frames a second of three packets, and a keyframe of twelve every ten
                // seconds (and when the SFU asks).
                let teacher = self.senders[0];
                let ssrc = self.w.browsers[teacher]
                    .screen
                    .as_ref()
                    .expect("a screen")
                    .ssrcs[0];
                let asked = self.w.browsers[teacher].keyframe_requests.contains(&ssrc);
                self.w.browsers[teacher]
                    .keyframe_requests
                    .retain(|&r| r != ssrc);
                let key = asked || self.frame.is_multiple_of(300);
                let pad = std::mem::replace(&mut self.w.payload_pad, 1000);
                self.w
                    .send_video(teacher, true, Layer::Low, key, if key { 12 } else { 3 });
                self.w.payload_pad = pad;
            }
            self.frame += 1;
        }
    }

    fn keyframe_requests(&self) -> usize {
        self.senders
            .iter()
            .map(|&s| self.w.browsers[s].keyframe_log.len())
            .sum()
    }

    /// What the viewer was sent since the last window ended, by `run`s before it.
    fn window(&mut self, drops_before: usize, requests_before: usize) -> Window {
        let b = &self.w.browsers[self.viewer];
        let mut out = Window {
            drops: b.queue_drops.len() - drops_before,
            keyframe_requests: self.keyframe_requests() - requests_before,
            ..Default::default()
        };
        let mut frames: BTreeMap<(String, u32), usize> = BTreeMap::new();
        for (track, packet) in &b.rtp[self.counted..] {
            *frames
                .entry((track.clone(), packet.header.timestamp))
                .or_default() += 1;
            let Some((TrackType::Video, session)) = parse_track_id(track) else {
                continue;
            };
            *out.packets.entry(session.to_owned()).or_default() += 1;
            let (layer, key) = describe(packet);
            out.began_with_keyframe
                .entry(session.to_owned())
                .or_insert(key);
            out.began_on.entry(session.to_owned()).or_insert(layer);
            out.bytes += rtc::shared::marshal::MarshalSize::marshal_size(packet);
        }
        self.counted = b.rtp.len();
        let mut ends: BTreeMap<&str, (u32, u32)> = BTreeMap::new();
        for (track, ts) in frames.keys() {
            let e = ends.entry(track).or_insert((*ts, *ts));
            e.0 = e.0.min(*ts);
            e.1 = e.1.max(*ts);
        }
        out.incomplete_frames = frames
            .iter()
            .filter(|&((track, ts), &n)| {
                let (first, last) = ends[track.as_str()];
                let sizes = [FRAME_PACKETS, self.keyframe_packets];
                let large = [LARGE_PACKETS, LARGE_KEYFRAME_PACKETS];
                *ts != first
                    && *ts != last
                    && !sizes.contains(&n)
                    && !(self.simulcast > 0 && large.contains(&n))
            })
            .count();
        out
    }

    /// Runs `seconds` and reports what the viewer was sent in them.
    fn measure(&mut self, seconds: u32) -> Window {
        let drops = self.w.browsers[self.viewer].queue_drops.len();
        let requests = self.keyframe_requests();
        self.counted = self.w.browsers[self.viewer].rtp.len();
        self.run(seconds);
        self.window(drops, requests)
    }

    /// The layer and whether it begins a keyframe, of each packet of the teacher's camera the
    /// viewer was sent from its `rtp` number `from`.
    fn teacher_packets(&self, from: usize) -> Vec<(Layer, bool)> {
        self.w.browsers[self.viewer].rtp[from..]
            .iter()
            .filter(|(track, _)| {
                parse_track_id(track) == Some((TrackType::Video, "session-teacher"))
            })
            .map(|(_, packet)| describe(packet))
            .collect()
    }
}

/// How many times the teacher's camera changed layer in `packets`.
fn layer_changes(packets: &[(Layer, bool)]) -> usize {
    packets.windows(2).filter(|w| w[0].0 != w[1].0).count()
}

impl Window {
    /// Bits per second over `seconds`.
    fn rate(&self, seconds: u32) -> f64 {
        self.bytes as f64 * 8.0 / f64::from(seconds)
    }

    /// The cameras that were sent whole over the window: a camera sent less than nine tenths of
    /// its frames was paused for some of it.
    fn flowing(&self, seconds: u32) -> Vec<&str> {
        let whole = (seconds * FPS) as usize * FRAME_PACKETS;
        self.packets
            .iter()
            .filter(|&(_, &n)| n * 10 >= whole * 9)
            .map(|(s, _)| s.as_str())
            .collect()
    }

    fn print(&self, name: &str, seconds: u32) {
        eprintln!(
            "{name}: {:.2} Mb/s sent, {} dropped by the link, {} keyframes asked, flowing {:?}, packets {:?}",
            self.rate(seconds) / 1e6,
            self.drops,
            self.keyframe_requests,
            self.flowing(seconds),
            self.packets
        );
    }
}

fn limited(rate_bps: u64) -> Path {
    Path {
        loss: 0.0,
        jitter: Duration::ZERO,
        delay: Duration::from_millis(40),
        rate_bps: Some(rate_bps),
        queue: Duration::from_millis(300),
    }
}

#[test]
fn cameras_beyond_what_the_link_carries_are_paused_not_dropped_at_random() {
    let mut c = Class::new(limited(LINK));
    // The first seconds find out what the link carries.
    c.run(20);
    let seen = c.measure(30);
    seen.print("held", 30);

    // What is sent fits in the link with room to spare, so the link drops next to nothing...
    assert!(
        seen.rate(30) <= LINK as f64,
        "{:.2} Mb/s sent",
        seen.rate(30) / 1e6
    );
    let sent: usize = seen.packets.values().sum();
    assert!(
        seen.drops * 100 <= sent,
        "{} of {} packets dropped",
        seen.drops,
        sent
    );
    // ...the teacher's camera plays all along, with as many of the others as fit...
    let flowing = seen.flowing(30);
    assert!(flowing.contains(&"session-teacher"), "{flowing:?}");
    assert!(
        (3..=6).contains(&flowing.len()),
        "{} of 8 cameras flow: {flowing:?}",
        flowing.len()
    );
    // ...and no frame is sent in part: a camera paused or started again goes between frames.
    assert_eq!(seen.incomplete_frames, 0, "{seen:?}");
    // Nothing flaps: once the cameras are settled, a keyframe is asked for only when one is taken up.
    assert!(
        seen.keyframe_requests <= 2,
        "{} keyframes asked",
        seen.keyframe_requests
    );
}

#[test]
fn paused_cameras_start_again_at_a_keyframe_when_the_link_has_room() {
    let mut c = Class::new(limited(LINK));
    c.run(40);
    let held = c.measure(10);
    let paused: Vec<String> = c
        .senders
        .iter()
        .map(|&s| format!("session-{}", c.w.browsers[s].name))
        .filter(|s| !held.flowing(10).contains(&s.as_str()))
        .collect();
    assert!(!paused.is_empty(), "nothing was paused: {held:?}");
    // The viewer was told of each camera still paused (the budget may have moved others on the way).
    let notices = |c: &Class| -> Vec<(String, bool)> {
        c.w.browsers[c.viewer]
            .dc_frames
            .iter()
            .filter_map(|f| match f {
                rtcd_proto::dc::DcMessage::CameraPaused { sender_id, paused } => {
                    Some((sender_id.clone(), *paused))
                }
                _ => None,
            })
            .collect()
    };
    let told = notices(&c);
    for camera in &paused {
        assert_eq!(
            told.iter().rev().find(|(id, _)| id == camera),
            Some(&(camera.clone(), true)),
            "{camera}: {told:?}"
        );
    }

    // The link now carries everything: the paused cameras start again, one at a time.
    c.w.set_path(c.viewer, limited(20_000_000));
    let back = c.measure(60);
    let told = notices(&c);
    for camera in &paused {
        assert_eq!(
            told.iter().rev().find(|(id, _)| id == camera),
            Some(&(camera.clone(), false)),
            "{camera} was not told to resume: {told:?}"
        );
    }
    back.print("recovered", 60);
    let sent: usize = back.packets.values().sum();
    assert!(back.drops * 100 <= sent, "{} of {sent} dropped", back.drops);
    let last = c.measure(10);
    assert_eq!(last.flowing(10).len(), STUDENTS + 1, "{last:?}");
    for camera in &paused {
        // Each begins with a keyframe, which its sender was asked for.
        assert_eq!(
            back.began_with_keyframe.get(camera),
            Some(&true),
            "{camera}"
        );
    }
    assert!(
        back.keyframe_requests >= paused.len(),
        "{} keyframes asked for {} cameras",
        back.keyframe_requests,
        paused.len()
    );
}

#[test]
fn a_link_that_swings_a_tenth_either_way_moves_no_camera_back_and_forth() {
    let mut c = Class::new(limited(LINK));
    c.run(30);
    let drops = c.w.browsers[c.viewer].queue_drops.len();
    let requests = c.keyframe_requests();
    c.counted = c.w.browsers[c.viewer].rtp.len();
    for second in 0..60 {
        let rate = if second % 2 == 0 {
            LINK * 9 / 10
        } else {
            LINK * 11 / 10
        };
        c.w.set_path(c.viewer, limited(rate));
        c.run(1);
    }
    let seen = c.window(drops, requests);
    seen.print("swinging", 60);
    let sent: usize = seen.packets.values().sum();
    assert!(
        seen.drops * 100 <= sent * 3,
        "{} of {sent} dropped",
        seen.drops
    );
    // A camera is tried again only where it fits the link's rate with a tenth to spare, which the
    // narrow half of the swing carries: one that does not fit is not tried at all.
    assert!(
        seen.keyframe_requests <= 3,
        "{} keyframes asked",
        seen.keyframe_requests
    );
    assert!(seen.flowing(30).contains(&"session-teacher"));
}

#[test]
fn a_camera_with_temporal_layers_gives_up_frames_before_it_is_paused() {
    // A little more room than four whole cameras take, which the fifth's first two temporal
    // layers fit in.
    let link = LINK + 400_000;
    let mut c = Class::new(limited(link));
    c.temporal = true;
    c.run(30);
    let from = c.w.browsers[c.viewer].rtp.len();
    let seen = c.measure(30);
    seen.print("layered", 30);
    let sent: usize = seen.packets.values().sum();
    assert!(
        seen.rate(30) <= link as f64,
        "{:.2} Mb/s",
        seen.rate(30) / 1e6
    );
    assert!(seen.drops * 100 <= sent, "{} of {sent} dropped", seen.drops);
    assert!(seen.flowing(30).contains(&"session-teacher"));
    // A camera that does not fit whole is sent some of its frames, not none.
    let whole = (30 * FPS) as usize * FRAME_PACKETS;
    let thinned: Vec<_> = seen
        .packets
        .iter()
        .filter(|&(_, &n)| n * 10 < whole * 9 && n * 10 > whole)
        .collect();
    assert!(!thinned.is_empty(), "no camera was thinned: {seen:?}");

    // The frames that are sent follow one another for the receiver, as if the others were never
    // there: picture ids that count up by one.
    let rx = &c.w.browsers[c.viewer].rtp[from..];
    for camera in thinned.iter().map(|(s, _)| s.as_str()) {
        let ids: Vec<u16> = rx
            .iter()
            .filter(|(t, _)| parse_track_id(t).is_some_and(|(_, s)| s == camera))
            .filter_map(|(_, p)| Descriptor::parse(&p.payload))
            .filter(|d| d.start)
            .filter_map(|d| d.picture_id.map(|(id, _)| id))
            .collect();
        assert!(
            ids.windows(2).all(|w| w[1] == (w[0] + 1) & 0x7fff),
            "{camera}'s picture ids skip"
        );
    }
}

#[test]
fn the_keyframes_of_a_late_viewers_cameras_are_not_taken_for_congestion() {
    // A link that carries all eight cameras with room to spare, but not the eight keyframes that
    // answer a viewer's arrival, which come all at once.
    let mut path = limited(5_000_000);
    path.queue = Duration::from_secs(2);
    let mut c = Class::build(path, false);
    c.keyframe_packets = 110;
    c.keyframe_every = 20 * FPS;
    c.run(8);
    let seen = c.measure(8);
    seen.print("late viewer", 8);
    assert_eq!(seen.flowing(8).len(), STUDENTS + 1, "{seen:?}");
}

#[test]
fn a_clear_link_with_random_loss_pauses_nothing() {
    let mut path = limited(50_000_000);
    path.loss = 0.02;
    let mut c = Class::new(path);
    c.run(20);
    let seen = c.measure(30);
    seen.print("lossy", 30);
    // Every camera plays whole (a packet in fifty is lost, and sent again when asked for).
    assert_eq!(seen.flowing(30).len(), STUDENTS + 1, "{seen:?}");
}

/// Runs the class for a minute (twenty seconds of it before anything is looked at) and checks that
/// every camera was sent in every second of the rest, and at least `kept` tenths of what a
/// camera on a clear link is sent over it: a link that loses or delays packets does not take
/// cameras away from the viewer.
fn assert_nothing_paused(c: &mut Class, kept: usize) {
    c.run(20);
    let line = c.timeline(40);
    let paused: Vec<usize> = (0..c.senders.len())
        .map(|k| c.longest_pause(&line, k))
        .collect();
    eprintln!("longest pauses {paused:?} s");
    assert!(paused.iter().all(|&n| n == 0), "cameras paused: {paused:?}");
    for k in 0..c.senders.len() {
        let name = c.name(k);
        let got: usize = line
            .iter()
            .map(|w| w.packets.get(&name).copied().unwrap_or(0))
            .sum();
        assert!(
            got * 10 >= c.whole_packets(k) * 40 * kept,
            "{name}: {got} packets of {}",
            c.whole_packets(k) * 40
        );
    }
}

#[test]
fn a_wide_link_that_loses_a_fifth_of_the_packets_pauses_no_camera() {
    // Random loss does not fall as the rate does: cutting the cameras for it only pauses them.
    let mut path = limited(50_000_000);
    path.loss = 0.2;
    assert_nothing_paused(&mut Class::new(path), 6);
}

#[test]
fn a_wide_link_that_delays_each_packet_differently_pauses_no_camera() {
    // Packets overtake one another over some hundred milliseconds, and a tenth are lost.
    let mut path = limited(50_000_000);
    path.jitter = Duration::from_millis(200);
    path.loss = 0.1;
    assert_nothing_paused(&mut Class::new(path), 7);
}

#[test]
fn a_wide_link_that_reorders_packets_pauses_no_camera() {
    let mut path = limited(50_000_000);
    path.jitter = Duration::from_millis(100);
    assert_nothing_paused(&mut Class::new(path), 9);
}

#[test]
fn a_paused_camera_whose_sender_leaves_or_who_is_unsubscribed_is_let_go() {
    let mut c = Class::new(limited(LINK));
    c.run(40);
    let held = c.measure(10);
    let paused: Vec<usize> = c
        .senders
        .iter()
        .copied()
        .filter(|&s| {
            !held
                .flowing(10)
                .contains(&format!("session-{}", c.w.browsers[s].name).as_str())
        })
        .collect();
    assert!(paused.len() >= 2, "{held:?}");
    let (leaver, dropped) = (paused[0], paused[1]);
    let name = |c: &Class, i: usize| format!("session-{}", c.w.browsers[i].name);

    // One leaves the call; the viewer stops asking for the other's camera.
    c.w.leave(leaver);
    c.senders.retain(|&s| s != leaver);
    let wanted: Vec<String> = c
        .senders
        .iter()
        .filter(|&&s| s != dropped)
        .map(|&s| name(&c, s))
        .collect();
    let subscription = serde_json::json!({ "session_ids": wanted, "low": [] });
    c.w.signal(
        c.viewer,
        MessageType::VideoSubscribe,
        serde_json::to_vec(&subscription).unwrap(),
    );
    let requests = c.keyframe_requests();
    c.run(20);
    let ended = &c.w.browsers[c.viewer].ended_tracks;
    for gone in [leaver, dropped] {
        let who = name(&c, gone);
        assert!(
            ended
                .iter()
                .any(|t| parse_track_id(t).is_some_and(|(_, s)| s == who)),
            "{who}'s camera was not ended"
        );
    }
    // Nobody is asked for a keyframe for them, and the rest carry on.
    let seen = c.measure(10);
    assert!(c.keyframe_requests() - requests <= 2, "keyframes asked");
    assert!(seen.flowing(10).contains(&"session-teacher"));
    assert!(!seen.packets.contains_key(&name(&c, dropped)));

    // Asking for it again brings it back once the link has room.
    let wanted: Vec<String> = c.senders.iter().map(|&s| name(&c, s)).collect();
    let subscription = serde_json::json!({ "session_ids": wanted, "low": [] });
    c.w.signal(
        c.viewer,
        MessageType::VideoSubscribe,
        serde_json::to_vec(&subscription).unwrap(),
    );
    c.w.set_path(c.viewer, limited(20_000_000));
    c.run(60);
    let back = c.measure(10);
    assert_eq!(back.flowing(10).len(), c.senders.len(), "{back:?}");
}

#[test]
fn a_screen_share_keeps_its_large_layer_when_it_and_the_hosts_camera_fit_the_link() {
    // The teacher shares her screen (a large layer of 2.5 Mb/s) and has a camera, as do seven
    // students; the link carries the screen and the teacher's camera and two more.
    let mut w = World::with_config(budget_on());
    let viewer = w.join("viewer", "call1");
    let teacher = w.join_host_with_camera("teacher", "call1");
    let mut senders = vec![teacher];
    for n in 0..STUDENTS {
        senders.push(w.join_with_camera(&format!("student{n}"), "call1"));
    }
    for &s in &senders {
        let none = serde_json::json!({ "session_ids": [], "low": [] });
        w.signal(
            s,
            MessageType::VideoSubscribe,
            serde_json::to_vec(&none).unwrap(),
        );
    }
    w.set_path(viewer, limited(4_500_000));
    w.share_screen(teacher);
    // The viewer asks for the sharp picture, as the web app does for a screen on a big window.
    w.dc_send(viewer, rtcd_proto::dc::DcMessage::SharpScreen(true));
    w.run_for(Duration::from_secs(1));
    let start = w.now;
    let plan = ScreenPlan {
        seconds: 90.0,
        low_bps: 500_000,
        high_bps: 2_500_000,
        temporal: true,
        voice: true,
        viewers: vec![viewer],
    };
    let mut frame = 0u32;
    let mut next = Duration::ZERO;
    w.stream_screen_live(teacher, &plan, |w, t| {
        while t >= next {
            for (k, &s) in senders.iter().enumerate() {
                let key = (frame + k as u32 * 7).is_multiple_of(KEYFRAME_EVERY);
                let packets = if key { KEYFRAME_PACKETS } else { FRAME_PACKETS };
                w.payload_pad = CAMERA_PAD;
                w.send_video(s, false, Layer::Low, key, packets);
            }
            frame += 1;
            next += Duration::from_secs(1) / FPS;
        }
    });
    let (from, to) = (
        start + Duration::from_secs(30),
        start + Duration::from_secs(88),
    );
    let shown = w.browsers[viewer].screen_watch.shown_by_layer(from, to);
    let (large, all): (usize, usize) = shown.iter().fold((0, 0), |(l, a), (&(layer, _), &n)| {
        (l + usize::from(layer == 2) * n, a + n)
    });
    eprintln!("screen frames shown {all}, of the large layer {large}");
    // About fifteen frames a second were shown, nearly all of the large layer.
    assert!(all >= 700 && large * 10 >= all * 9, "{shown:?}");
}

#[test]
fn with_the_camera_budget_off_every_camera_is_sent_whole_whatever_the_link_drops() {
    let cfg = EngineConfig {
        camera_budget: false,
        ..engine_config()
    };
    let mut c = Class::in_world(World::with_config(cfg), limited(LINK), true);
    c.run(20);
    let seen = c.measure(30);
    seen.print("budget off", 30);
    assert_eq!(seen.packets.len(), STUDENTS + 1);
    assert!(seen.drops > 5_000, "{} dropped", seen.drops);
}

/// A link that carries the host's small layer and a student or two, but not its large layer
/// (about 1.3 Mb/s with its keyframes).
const NARROW: u64 = 1_200_000;

fn simulcast_class(path: Path, simulcast: usize) -> Class {
    let mut c = Class::new(path);
    c.simulcast = simulcast;
    c
}

#[test]
fn a_host_whose_link_carries_only_its_small_layer_is_sent_that_whole() {
    let mut c = simulcast_class(limited(NARROW), 1);
    c.run(30);
    let from = c.w.browsers[c.viewer].rtp.len();
    let seen = c.measure(30);
    seen.print("simulcast host, narrow link", 30);
    let teacher = c.teacher_packets(from);
    let large = teacher.iter().filter(|&&(l, _)| l == Layer::High).count();
    eprintln!("teacher packets {}, large {large}", teacher.len());
    let whole = (30 * FPS) as usize * FRAME_PACKETS;
    // Every frame of the small layer arrived, none of the large one, and none in part.
    assert_eq!(large, 0, "the large layer was sent");
    assert!(
        teacher.len() * 10 >= whole * 9,
        "{} of {whole} packets",
        teacher.len()
    );
    assert_eq!(seen.incomplete_frames, 0, "{seen:?}");
    let sent: usize = seen.packets.values().sum();
    assert!(seen.drops * 100 <= sent, "{} of {sent} dropped", seen.drops);
}

#[test]
fn a_host_goes_back_to_its_large_layer_at_a_keyframe_when_the_link_clears() {
    let mut c = simulcast_class(limited(NARROW), 1);
    c.run(30);
    c.w.set_path(c.viewer, limited(20_000_000));
    let from = c.w.browsers[c.viewer].rtp.len();
    let mut seconds = 0;
    // Held for a while after the cut, clear for a while, and a trial: about eight seconds, and
    // some more for the allowance to grow.
    while seconds < 20
        && !c
            .teacher_packets(from)
            .iter()
            .any(|&(layer, _)| layer == Layer::High)
    {
        c.run(1);
        seconds += 1;
    }
    eprintln!("the large layer came back after {seconds} s");
    assert!(seconds < 20, "the host stayed on its small layer");
    let teacher = c.teacher_packets(from);
    let first = teacher
        .iter()
        .position(|&(layer, _)| layer == Layer::High)
        .unwrap();
    assert!(teacher[first].1, "the large layer began with a delta frame");
    // One move, and the host stays there.
    c.run(20);
    let teacher = c.teacher_packets(from);
    assert_eq!(layer_changes(&teacher), 1);
    assert_eq!(teacher.last().map(|p| p.0), Some(Layer::High));
    let seen = c.measure(10);
    assert_eq!(seen.incomplete_frames, 0, "{seen:?}");
}

#[test]
fn a_link_that_swings_a_tenth_either_way_moves_the_host_between_layers_at_most_once() {
    let mut c = simulcast_class(limited(NARROW), 1);
    c.run(30);
    let from = c.w.browsers[c.viewer].rtp.len();
    for second in 0..90 {
        let rate = if second % 2 == 0 {
            NARROW * 9 / 10
        } else {
            NARROW * 11 / 10
        };
        c.w.set_path(c.viewer, limited(rate));
        c.run(1);
    }
    let teacher = c.teacher_packets(from);
    let changes = layer_changes(&teacher);
    eprintln!(
        "swinging: {changes} layer changes, {} packets",
        teacher.len()
    );
    assert!(changes <= 1, "{changes} layer changes");
    assert!(teacher.len() * 10 >= (90 * FPS) as usize * FRAME_PACKETS * 9);
}

#[test]
fn a_camera_paused_and_started_again_begins_at_a_keyframe_whichever_layer_fits() {
    // Every camera sends two layers; the link carries the small layers of a few of them.
    let mut c = simulcast_class(limited(NARROW), STUDENTS + 1);
    c.run(40);
    let held = c.measure(10);
    let paused: Vec<String> = c
        .senders
        .iter()
        .map(|&s| format!("session-{}", c.w.browsers[s].name))
        .filter(|s| !held.packets.contains_key(s))
        .collect();
    assert!(!paused.is_empty(), "nothing was paused: {held:?}");

    // Room for more of the small layers: the paused cameras start again.
    c.w.set_path(c.viewer, limited(NARROW * 13 / 10));
    let back = c.measure(40);
    back.print("room for more small layers", 40);
    let started: Vec<_> = paused
        .iter()
        .filter(|s| back.packets.contains_key(*s))
        .collect();
    assert!(!started.is_empty(), "no camera started again: {back:?}");
    // A camera begins at a keyframe, on the layer the budget finds room for (the small one, or
    // the large where the limit a fresh cut set leaves room for it). A link that drops some of
    // what it is sent may take the keyframe's first packet: the camera that is seen to begin on
    // one is enough.
    assert!(
        started
            .iter()
            .any(|c| back.began_with_keyframe.get(*c) == Some(&true)),
        "no camera began with a keyframe: {back:?}"
    );
}

impl Class {
    /// The teacher and seven students as a lesson sends them: every camera two layers with
    /// temporal layers, the teacher's large one 600 kb/s and the students' 400 kb/s, every small
    /// one 150 kb/s.
    fn production(path: Path) -> Class {
        let mut c = Class::build(path, true);
        c.temporal = true;
        c.simulcast = c.senders.len();
        c.rates = (0..c.senders.len())
            .map(|k| [150_000, if k == 0 { 600_000 } else { 400_000 }])
            .collect();
        c
    }

    /// What the viewer was sent in each of `seconds` seconds.
    fn timeline(&mut self, seconds: u32) -> Vec<Window> {
        (0..seconds).map(|_| self.measure(1)).collect()
    }

    /// Packets a second of the camera of sender `k` sent whole (its top layer, every frame).
    fn whole_packets(&self, k: usize) -> usize {
        let layer = usize::from(k < self.simulcast);
        let packets = match self.rates.get(k) {
            Some(r) => frame_shape(r[layer], false).0,
            None if layer == 1 => LARGE_PACKETS,
            None => FRAME_PACKETS,
        };
        packets * FPS as usize
    }

    fn name(&self, k: usize) -> String {
        format!("session-{}", self.w.browsers[self.senders[k]].name)
    }

    /// Seconds of the longest stretch of `line` in which sender `k`'s camera was sent nothing.
    fn longest_pause(&self, line: &[Window], k: usize) -> usize {
        let name = self.name(k);
        let mut longest = 0;
        let mut now = 0;
        for w in line {
            now = if w.packets.get(&name).is_none_or(|&n| n == 0) {
                now + 1
            } else {
                0
            };
            longest = longest.max(now);
        }
        longest
    }

    /// Whether every camera was sent whole (nine tenths of its frames) in `window`.
    fn all_whole(&self, window: &Window) -> bool {
        (0..self.senders.len()).all(|k| {
            window.packets.get(&self.name(k)).copied().unwrap_or(0) * 10
                >= self.whole_packets(k) * 9
        })
    }

    /// How many seconds the viewer's link held what was sent for, at the 95th percentile, over
    /// the packets of its `queue_waits` from `from` on.
    fn queue_p95(&self, from: usize) -> f64 {
        let mut waits: Vec<f64> = self.w.browsers[self.viewer].queue_waits[from..]
            .iter()
            .map(|(_, wait)| wait.as_secs_f64())
            .collect();
        waits.sort_by(f64::total_cmp);
        waits.get(waits.len() * 95 / 100).copied().unwrap_or(0.0)
    }
}

/// What the viewer was sent in each of `seconds` seconds of a link that swings between `high` and
/// `low` bits a second every three seconds.
fn flap(c: &mut Class, high: u64, low: u64, seconds: u32) -> Vec<Window> {
    (0..seconds)
        .map(|second| {
            let rate = if (second / 3) % 2 == 0 { high } else { low };
            c.w.set_path(c.viewer, limited(rate));
            c.measure(1)
        })
        .collect()
}

#[test]
fn an_outage_is_not_taken_for_congestion() {
    // Four cameras a link of 2.5 Mb/s carries with room to spare. The path then drops everything
    // both ways for five seconds: the first feedback after it reports every packet sent meanwhile
    // as lost, which says nothing of what the link carries.
    let mut c = Class::of(World::with_config(budget_on()), limited(LINK), true, 3);
    c.run(25);
    let before = c.measure(5);
    assert!(c.all_whole(&before), "{before:?}");
    let requests = c.keyframe_requests();

    c.w.browsers[c.viewer].unreachable = true;
    c.run(5);
    c.w.browsers[c.viewer].unreachable = false;
    let line = c.timeline(20);
    for (second, w) in line.iter().take(3).enumerate() {
        w.print(&format!("after the outage, second {second}"), 1);
    }
    // Within two seconds of the feedback coming back every camera is sent whole, and stays so:
    // none was paused or thinned, and no camera was started again at a keyframe.
    for w in &line[2..] {
        assert!(c.all_whole(w), "{w:?}");
    }
    assert!(
        c.keyframe_requests() - requests <= 4,
        "{} keyframes asked",
        c.keyframe_requests() - requests
    );
}

#[test]
fn a_link_that_flaps_never_pauses_the_host_nor_holds_a_camera_back_for_long() {
    // A link of 3 Mb/s and of 300 kb/s every three seconds for a minute, which a lesson of the
    // teacher and seven students (two layers each, see `production`) does not fit in the narrow
    // halves: only the teacher's small layer does.
    let mut c = Class::production(limited(20_000_000));
    c.run(20);
    let during = flap(&mut c, 3_000_000, 300_000, 60);
    c.w.set_path(c.viewer, limited(20_000_000));
    let after = c.timeline(100);
    let longest: Vec<usize> = (0..c.senders.len())
        .map(|k| c.longest_pause(&during, k))
        .collect();
    // From which second on every camera flows, and is whole, to the end.
    let flowing = (0..=after.len()).find(|&i| {
        after[i..].iter().all(|w| {
            (0..c.senders.len()).all(|k| w.packets.get(&c.name(k)).is_some_and(|&n| n > 0))
        })
    });
    let whole = (0..=after.len()).find(|&i| after[i..].iter().all(|w| c.all_whole(w)));
    eprintln!(
        "flap: longest pauses {longest:?} s; after the flap every camera flows from second \
         {flowing:?}, is whole from {whole:?}"
    );
    assert_eq!(longest[0], 0, "the host's camera was paused");
    // A camera paused for MAX_PAUSE is tried at its lowest step the next time the path has
    // nothing queued, and the trial that fails (the narrow half comes) is waited out for at most
    // the longest wait after a step that failed: the longest a camera is paused is those two and
    // two trials' worth of seconds.
    let most = (MAX_PAUSE + LONGEST_WAIT + 2 * TRIAL).as_secs() as usize;
    assert!(
        longest.iter().all(|&n| n <= most),
        "pauses over {most} seconds: {longest:?}"
    );
    // The trial that failed last (in the last narrow half) is waited out, at the longest.
    let back = (LONGEST_WAIT + TRIAL).as_secs() as usize;
    assert!(
        flowing.is_some_and(|s| s <= back),
        "every camera flows from second {flowing:?} after the flap"
    );
    // Cameras are taken up one at a time (each needs the path to stay clear for a few seconds,
    // and a step or two each), so being whole again takes a while longer.
    assert!(
        whole.is_some_and(|s| s <= 90),
        "every camera is whole from second {whole:?} after the flap"
    );
}

#[test]
fn a_viewer_that_is_sent_nothing_has_its_paused_cameras_tried_again() {
    // The host's camera is not sent to the viewer (it is off), the viewer's link carries none of
    // the three students' cameras, and nobody speaks: every camera is paused, nothing is sent, and
    // so no feedback comes. That says nothing of the link, and when it opens, the cameras return.
    let mut c = Class::of(World::with_config(budget_on()), limited(300_000), true, 3);
    let students: Vec<String> = (0..3).map(|n| format!("session-student{n}")).collect();
    let subscription = serde_json::json!({ "session_ids": students, "low": [] });
    c.w.signal(
        c.viewer,
        MessageType::VideoSubscribe,
        serde_json::to_vec(&subscription).unwrap(),
    );
    // Every camera is paused after a while; one is tried now and then (and paused again).
    c.run(30);
    let mut quiet = false;
    for _ in 0..60 {
        quiet = c.measure(3).packets.is_empty();
        if quiet {
            break;
        }
    }
    assert!(quiet, "every camera was never paused");
    c.w.set_path(c.viewer, limited(20_000_000));
    c.run(40);
    let back = c.measure(10);
    back.print("a silent viewer, link open", 10);
    assert_eq!(back.flowing(10).len(), 3, "{back:?}");
}

/// What a lesson of the teacher and four students, each camera a single layer of 528 kb/s, and
/// the teacher's screen share, sends to a viewer behind `link`.
fn five_cameras_and_a_screen(link: u64) -> Class {
    let mut c = Class::of(World::with_config(budget_on()), limited(link), true, 4);
    c.rates = vec![[528_000, 0]; 5];
    c.w.share_screen(c.senders[0]);
    c.screen = true;
    c
}

#[test]
fn single_layer_cameras_and_a_screen_on_a_narrow_link_leave_at_most_two_cameras_paused() {
    let mut c = five_cameras_and_a_screen(LINK);
    c.run(90);
    let from = c.w.browsers[c.viewer].queue_waits.len();
    let line = c.timeline(60);
    let paused: Vec<usize> = line
        .iter()
        .map(|w| {
            (0..c.senders.len())
                .filter(|&k| w.packets.get(&c.name(k)).is_none_or(|&n| n == 0))
                .count()
        })
        .collect();
    let longest: Vec<usize> = (0..c.senders.len())
        .map(|k| c.longest_pause(&line, k))
        .collect();
    let sent: usize = line.iter().flat_map(|w| w.packets.values()).sum();
    let dropped: usize = line.iter().map(|w| w.drops).sum();
    let delivery = sent as f64 / (sent + dropped) as f64;
    let queue = c.queue_p95(from);
    eprintln!(
        "five cameras and a screen: most paused at once {:?}, longest pauses {longest:?} s, \
         delivery {delivery:.3}, queue p95 {queue:.3} s",
        paused.iter().max()
    );
    // The fourth camera fits with the screen only just: it is paused and tried again now and then.
    assert!(paused.iter().all(|&n| n <= 2), "paused at once: {paused:?}");
    // Five cameras of 528 kb/s and the screen do not fit in 2.5 Mb/s: the one that does not
    // stays paused, and no other is paused for long.
    assert!(
        longest.iter().filter(|&&n| n > 12).count() <= 1,
        "pauses: {longest:?}"
    );
    assert!(delivery >= 0.95, "delivery {delivery:.3}");
    assert!(queue < 0.3, "queue p95 {queue:.3} s");
}

/// How many cameras were paused at once at most in `line`, and for how many camera-seconds.
fn paused_in(c: &Class, line: &[Window]) -> (usize, usize) {
    let each: Vec<usize> = line
        .iter()
        .map(|w| {
            (0..c.senders.len())
                .filter(|&k| w.packets.get(&c.name(k)).is_none_or(|&n| n == 0))
                .count()
        })
        .collect();
    (each.iter().copied().max().unwrap_or(0), each.iter().sum())
}

#[test]
fn a_lesson_with_two_layers_pauses_nobody_where_the_small_layers_fit() {
    let mut c = Class::production(limited(2_500_000));
    c.run(90);
    let line = c.timeline(30);
    let (at_once, seconds) = paused_in(&c, &line);
    eprintln!("production lesson at 2.5 Mb/s: {at_once} paused at once, {seconds} camera-seconds");
    assert_eq!(seconds, 0, "a camera was paused");
}

#[test]
fn a_lesson_with_two_layers_pauses_nobody_on_a_link_for_all_its_small_layers_and_a_little_more() {
    // 1.5 Mb/s carries the small layers of all eight cameras (1.2 Mb/s) with room to spare, with
    // their frames thinned: the cameras are thinned or on their small layers, none is paused.
    let mut c = Class::production(limited(1_500_000));
    c.run(90);
    let line = c.timeline(30);
    let (at_once, seconds) = paused_in(&c, &line);
    eprintln!("production lesson at 1.5 Mb/s: {at_once} paused at once, {seconds} camera-seconds");
    assert_eq!(seconds, 0, "a camera was paused");
}

/// The longest pauses of the students of a lesson of two layers on `link`, and whether the host was
/// ever paused or sent its large layer.
fn narrow_lesson(link: u64) -> (Vec<usize>, usize, usize) {
    let mut c = Class::production(limited(link));
    c.run(40);
    let from = c.w.browsers[c.viewer].rtp.len();
    let line = c.timeline(30);
    let students: Vec<usize> = (1..c.senders.len())
        .map(|k| c.longest_pause(&line, k))
        .collect();
    let teacher = c.teacher_packets(from);
    let large = teacher.iter().filter(|&&(l, _)| l == Layer::High).count();
    (students, c.longest_pause(&line, 0), large)
}

#[test]
fn a_lesson_with_two_layers_on_half_a_megabit_holds_the_host_on_its_small_layer() {
    // The lowest steps of all eight cameras (a base layer of each small layer) fit, or all but one:
    // the host, whose large layer would not leave the others room, is on its small one.
    let (students, host_paused, large) = narrow_lesson(500_000);
    eprintln!("production lesson at 0.5 Mb/s: students' longest pauses {students:?} s");
    assert_eq!(host_paused, 0, "the host's camera was paused");
    assert!(
        students.iter().filter(|&&n| n > 0).count() <= 1,
        "{students:?}"
    );
    assert_eq!(large, 0, "the host was sent its large layer");
}

#[test]
fn a_lesson_with_two_layers_pauses_only_students_where_not_even_all_the_lowest_steps_fit() {
    let (students, host_paused, large) = narrow_lesson(300_000);
    eprintln!("production lesson at 0.3 Mb/s: students' longest pauses {students:?} s");
    assert_eq!(host_paused, 0, "the host's camera was paused");
    assert!(students.iter().any(|&n| n > 0), "no student was paused");
    assert_eq!(large, 0, "the host was sent its large layer");
}
