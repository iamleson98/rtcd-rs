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
const LINK: u64 = 2_500_000;

/// A viewer, the teacher and the students, and what the viewer was sent.
struct Class {
    w: World,
    viewer: usize,
    /// The teacher first.
    senders: Vec<usize>,
    /// The cameras send three temporal layers, as a browser's encoder does.
    temporal: bool,
    /// Packets of a keyframe, and frames between the ones the encoder sends on its own.
    keyframe_packets: usize,
    keyframe_every: u32,
    frame: u32,
    /// Packets of the viewer's `rtp` already counted by `window`.
    counted: usize,
}

/// What the viewer was sent over a window, by camera (the session that sends it).
#[derive(Debug, Default)]
struct Window {
    packets: BTreeMap<String, usize>,
    /// Whether the first packet of each camera in the window was a keyframe.
    began_with_keyframe: BTreeMap<String, bool>,
    drops: usize,
    bytes: usize,
    keyframe_requests: usize,
    /// Frames of the window that arrived with some of their packets missing (the first and the
    /// last of each camera, which the window may cut, are not counted).
    incomplete_frames: usize,
}

impl Class {
    fn new(path: Path) -> Class {
        Class::build(path, true)
    }

    /// The viewer comes in `first`, or after the cameras, which then are all asked for a
    /// keyframe at once.
    fn build(path: Path, first: bool) -> Class {
        Class::in_world(World::new(), path, first)
    }

    fn in_world(mut w: World, path: Path, first: bool) -> Class {
        let early = first.then(|| w.join("viewer", "call1"));
        let mut senders = vec![w.join_host_with_camera("teacher", "call1")];
        for n in 0..STUDENTS {
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
            keyframe_packets: KEYFRAME_PACKETS,
            keyframe_every: KEYFRAME_EVERY,
            frame: 0,
            counted: 0,
        }
    }

    /// Every camera sends a frame at 30 per second (a keyframe every two seconds, one when the SFU
    /// asks for it), spread over the frame time as unsynchronised encoders are.
    fn run(&mut self, seconds: u32) {
        let slot = Duration::from_secs(1) / FPS / self.senders.len() as u32;
        for _ in 0..seconds * FPS {
            for (k, &s) in self.senders.iter().enumerate() {
                let b = &mut self.w.browsers[s];
                let ssrc = b.camera.as_ref().expect("a camera").ssrcs[Layer::Low as usize];
                let asked = b.keyframe_requests.contains(&ssrc);
                b.keyframe_requests.clear();
                if asked {
                    b.keyframe_log.push(ssrc);
                }
                let key = asked || (self.frame + k as u32 * 7).is_multiple_of(self.keyframe_every);
                let packets = if key {
                    self.keyframe_packets
                } else {
                    FRAME_PACKETS
                };
                if self.temporal {
                    self.w.send_camera_frame(s, Layer::Low, key, packets);
                } else {
                    self.w.send_video(s, false, Layer::Low, key, packets);
                }
                self.w.run_for(slot);
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
            out.began_with_keyframe
                .entry(session.to_owned())
                .or_insert_with(|| describe(packet).1);
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
                *ts != first && *ts != last && n != FRAME_PACKETS && n != self.keyframe_packets
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
    let mut w = World::new();
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
