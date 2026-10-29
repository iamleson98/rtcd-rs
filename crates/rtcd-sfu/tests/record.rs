//! Built-in recording in the engine: which streams of a call are tapped, what the tap queues, and
//! that events stay ahead of the packets after them even when the writer cannot keep up.

mod support;

use std::sync::atomic::Ordering;
use std::time::Duration;

use rtcd_sfu::ids::TrackType;
use rtcd_sfu::record::{RecordItem, RecordReceiver, record_channel};
use support::{Layer, World};

const BOTH: [Layer; 2] = [Layer::Low, Layer::High];

/// Starts recording `call` with a queue of `capacity` items.
fn start(w: &mut World, call: &str, id: &str, capacity: usize) -> RecordReceiver {
    let (sink, rx) = record_channel(capacity);
    w.engine.record_start(w.now, "group", call, id, w.now, sink);
    rx
}

fn drain(rx: &mut RecordReceiver) -> Vec<RecordItem> {
    std::iter::from_fn(|| rx.try_recv().ok()).collect()
}

/// A short name for an item, with what identifies it.
fn name(item: &RecordItem) -> String {
    match item {
        RecordItem::Packet(p) => format!("packet {} {}", p.stream, p.seq),
        RecordItem::Begin(b) => format!("begin {}", b.recording_id),
        RecordItem::Open(o) => format!("open {} {} {}", o.stream, o.kind, o.session_id),
        RecordItem::Close { stream, .. } => format!("close {stream}"),
        RecordItem::Off { stream, .. } => format!("off {stream}"),
        RecordItem::End { reason, .. } => format!("end {reason:?}"),
    }
}

fn names(items: &[RecordItem]) -> Vec<String> {
    items.iter().map(name).collect()
}

fn packets(items: &[RecordItem]) -> Vec<(u32, u16)> {
    items
        .iter()
        .filter_map(|i| match i {
            RecordItem::Packet(p) => Some((p.stream, p.seq)),
            _ => None,
        })
        .collect()
}

fn opens(items: &[RecordItem]) -> Vec<(u32, TrackType, String, String)> {
    items
        .iter()
        .filter_map(|i| match i {
            RecordItem::Open(o) => {
                Some((o.stream, o.kind, o.session_id.clone(), o.user_id.clone()))
            }
            _ => None,
        })
        .collect()
}

#[test]
fn a_call_is_recorded_from_its_speakers_and_ends_when_stopped() {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    let b = w.join("bob", "call1");
    w.speak(a, 5, |_| 40);
    w.speak(b, 5, |_| 40);
    let t0 = w.now;
    let (sink, mut rx) = record_channel(1024);
    w.engine
        .record_start(w.now, "group", "call1", "rec1", t0, sink);

    w.speak(a, 50, |_| 40);
    w.speak(b, 25, |_| 40);
    w.engine.record_stop(w.now, "group", "call1", "rec1");
    // What comes after the stop is not recorded.
    w.speak(a, 10, |_| 40);

    let items = drain(&mut rx);
    let RecordItem::Begin(begin) = &items[0] else {
        panic!("the first item is the beginning: {:?}", names(&items));
    };
    assert_eq!((begin.recording_id.as_str(), begin.t0), ("rec1", t0));
    let mut streams = opens(&items);
    streams.sort_by_key(|s| s.0);
    assert_eq!(
        streams,
        [
            (
                0,
                TrackType::Voice,
                "session-alice".into(),
                "user-alice".into()
            ),
            (1, TrackType::Voice, "session-bob".into(), "user-bob".into()),
        ]
    );
    let all = packets(&items);
    assert_eq!(all.iter().filter(|p| p.0 == 0).count(), 50);
    assert_eq!(all.iter().filter(|p| p.0 == 1).count(), 25);
    for stream in [0, 1] {
        let seqs: Vec<u16> = all.iter().filter(|p| p.0 == stream).map(|p| p.1).collect();
        assert!(
            seqs.windows(2).all(|w| w[1] == w[0].wrapping_add(1)),
            "stream {stream} in order, none missing: {seqs:?}"
        );
    }
    // Every open comes before its first packet, the closes before the end, and nothing follows it.
    let n = names(&items);
    for stream in [0, 1] {
        let open = n
            .iter()
            .position(|s| s.starts_with(&format!("open {stream} ")))
            .unwrap();
        let first = n
            .iter()
            .position(|s| s.starts_with(&format!("packet {stream} ")))
            .unwrap();
        let last = n
            .iter()
            .rposition(|s| s.starts_with(&format!("packet {stream} ")))
            .unwrap();
        let close = n
            .iter()
            .position(|s| *s == format!("close {stream}"))
            .unwrap();
        assert!(open < first && last < close, "{n:?}");
    }
    assert_eq!(n.last().unwrap(), "end Stopped");
    assert_eq!(rx.shared().dropped(), 0);
}

#[test]
fn a_stream_that_starts_after_the_recording_joins_it() {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    let mut rx = start(&mut w, "call1", "rec1", 1024);
    assert_eq!(names(&drain(&mut rx)), ["begin rec1"], "no stream yet");

    w.speak(a, 10, |_| 40);
    let items = drain(&mut rx);
    assert_eq!(
        names(&items)[..2],
        ["open 0 voice session-alice", "packet 0 0"],
        "{:?}",
        names(&items)
    );
    // A participant who joins later is taken too.
    let b = w.join("bob", "call1");
    w.speak(b, 10, |_| 40);
    assert_eq!(opens(&drain(&mut rx)).len(), 1);
}

#[test]
fn a_stream_nobody_watches_is_still_recorded() {
    let mut w = World::new();
    let a = w.join("alice", "alone");
    let mut rx = start(&mut w, "alone", "rec1", 1024);
    w.speak(a, 30, |_| 40);
    assert_eq!(
        packets(&drain(&mut rx)).len(),
        30,
        "alice is the only one in the call: no one receives her voice, and it is recorded"
    );
}

#[test]
fn the_large_layer_of_a_camera_and_of_a_screen_is_recorded() {
    let mut w = World::new();
    let a = w.join_with_camera("alice", "call1");
    let _b = w.join("bob", "call1");
    w.share_screen(a);
    let mut rx = start(&mut w, "call1", "rec1", 4096);
    for screen in [false, true] {
        w.stream_video(a, screen, 1.0, 30, &BOTH);
    }
    let items = drain(&mut rx);
    let streams = opens(&items);
    let kinds: Vec<TrackType> = streams.iter().map(|s| s.1).collect();
    assert!(
        kinds.contains(&TrackType::Video) && kinds.contains(&TrackType::Screen),
        "{streams:?}"
    );
    assert_eq!(
        kinds.iter().filter(|k| k.is_video()).count(),
        2,
        "one stream for each, not one for each layer: {streams:?}"
    );
    let video_streams: Vec<u32> = streams
        .iter()
        .filter(|s| s.1.is_video())
        .map(|s| s.0)
        .collect();
    let mut n = 0;
    for item in &items {
        if let RecordItem::Packet(p) = item
            && video_streams.contains(&p.stream)
        {
            n += 1;
            // The test's packets name their layer in the payload, four bytes from the end.
            assert_eq!(
                p.payload[p.payload.len() - 4],
                Layer::High as u8 + 1,
                "only the large layer"
            );
        }
    }
    // 30 frames of two packets on each of the two senders.
    assert_eq!(n, 2 * 30 * 2, "{:?}", names(&items));
    assert_eq!(rx.shared().dropped(), 0);
}

/// The layer each packet of the recorded video streams came from, in order, with the stream.
fn layers_recorded(items: &[RecordItem]) -> Vec<(u32, u8)> {
    items
        .iter()
        .filter_map(|i| match i {
            RecordItem::Packet(p) => Some((p.stream, p.payload[p.payload.len() - 4])),
            _ => None,
        })
        .collect()
}

#[test]
fn a_camera_with_only_the_small_layer_is_recorded_after_a_moment() {
    let mut w = World::new();
    let a = w.join_with_camera("alice", "call1");
    let mut rx = start(&mut w, "call1", "rec1", 4096);
    w.stream_video(a, false, 5.0, 30, &[Layer::Low]);
    let items = drain(&mut rx);
    assert_eq!(
        opens(&items).len(),
        1,
        "one stream, opened once the small layer had been alone for a while: {:?}",
        names(&items)
    );
    let got = packets(&items);
    assert!(
        got.first().is_some_and(|p| p.1 >= 100 + 2 * 15) && got.len() > 100,
        "the first moments of the five seconds are left out, the rest recorded: {} of 300 packets from {:?}",
        got.len(),
        got.first()
    );
}

#[test]
fn a_camera_whose_large_layer_is_paused_is_recorded_at_the_small_one() {
    let mut w = World::new();
    let a = w.join_with_camera("alice", "call1");
    let mut rx = start(&mut w, "call1", "rec1", 8192);
    w.stream_video(a, false, 2.0, 30, &BOTH);
    w.stream_video(a, false, 5.0, 30, &[Layer::Low]);
    w.stream_video(a, false, 3.0, 30, &BOTH);
    let items = drain(&mut rx);
    let layers = layers_recorded(&items);
    let (small, large) = (Layer::Low as u8 + 1, Layer::High as u8 + 1);
    // The large layer, then the small one while the large is paused, then the large again, each
    // in a stream of its own (the picture changes size, so it is a new file).
    let mut runs: Vec<(u32, u8)> = Vec::new();
    for l in layers {
        if runs.last() != Some(&l) {
            runs.push(l);
        }
    }
    assert_eq!(
        runs.iter().map(|r| r.1).collect::<Vec<_>>(),
        [large, small, large],
        "{runs:?}"
    );
    assert_eq!(
        runs.iter()
            .map(|r| r.0)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        3,
        "a stream for each: {runs:?}"
    );
}

#[test]
fn a_camera_that_is_turned_off_finishes_its_file() {
    let mut w = World::new();
    let a = w.join_with_camera("alice", "call1");
    let mut rx = start(&mut w, "call1", "rec1", 4096);
    w.stream_video(a, false, 0.5, 30, &BOTH);
    w.signal(a, rtcd_proto::MessageType::VideoOff, Vec::new());
    let n = names(&drain(&mut rx));
    assert!(n.contains(&"off 0".to_owned()), "{n:?}");
}

#[test]
fn a_screen_share_that_ends_closes_its_stream() {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    let _b = w.join("bob", "call1");
    w.share_screen(a);
    let mut rx = start(&mut w, "call1", "rec1", 4096);
    w.stream_video(a, true, 0.5, 30, &BOTH);
    w.signal(a, rtcd_proto::MessageType::ScreenOff, Vec::new());
    w.run_for(Duration::from_millis(200));
    let n = names(&drain(&mut rx));
    let open = n.iter().position(|s| s.contains(" screen ")).unwrap();
    let close = n.iter().position(|s| s.starts_with("close")).unwrap();
    assert!(open < close, "{n:?}");
    assert!(
        n.iter().skip(close + 1).all(|s| !s.starts_with("packet")),
        "nothing after the close"
    );
}

#[test]
fn the_writer_can_ask_for_a_keyframe_and_the_sender_is_asked() {
    let mut w = World::new();
    let a = w.join_with_camera("alice", "call1");
    let mut rx = start(&mut w, "call1", "rec1", 4096);
    w.stream_video(a, false, 0.5, 30, &BOTH);
    let items = drain(&mut rx);
    let Some(RecordItem::Open(open)) = items.iter().find(|i| matches!(i, RecordItem::Open(_)))
    else {
        panic!("{:?}", names(&items));
    };
    let high = w.browsers[a].camera.as_ref().unwrap().ssrcs[Layer::High as usize];
    // Let the keyframe request made when the stream joined, and the rate limit, pass.
    w.run_for(Duration::from_secs(4));
    let before = w.browsers[a]
        .keyframe_requests
        .iter()
        .filter(|&&s| s == high)
        .count();

    open.want_keyframe.store(true, Ordering::Relaxed);
    w.run_for(Duration::from_secs(3));
    let after = w.browsers[a]
        .keyframe_requests
        .iter()
        .filter(|&&s| s == high)
        .count();
    assert_eq!(
        after,
        before + 1,
        "one request for the large layer, at the next upkeep"
    );
    assert!(
        !open.want_keyframe.load(Ordering::Relaxed),
        "the flag is taken, so one wish is one request"
    );
}

#[test]
fn a_full_queue_drops_packets_without_stopping_the_engine_and_keeps_events_in_order() {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    let b = w.join("bob", "call1");
    w.speak(a, 3, |_| 40);
    w.speak(b, 3, |_| 40);
    // Room for the beginning, two opens and a handful of packets.
    let mut rx = start(&mut w, "call1", "rec1", 8);
    w.speak(a, 30, |_| 40);
    w.speak(b, 3, |_| 40);
    let dropped = rx.shared().dropped();
    assert!(
        dropped >= 20,
        "the queue held 8 items; {dropped} packets were left out and the engine went on"
    );
    // Alice leaves while the queue is full: her stream's end cannot be queued yet.
    w.leave(a);
    w.run_for(Duration::from_millis(100));
    // Bob keeps talking; his packets must not overtake alice's end.
    w.speak(b, 5, |_| 40);
    let after_leave = rx.shared().dropped();
    assert!(
        after_leave > dropped,
        "packets are left out while an event waits for room"
    );

    // The writer catches up. Everything it gets is in the order it happened, and what was waiting
    // arrives once there is room.
    let mut seen = drain(&mut rx);
    for _ in 0..20 {
        w.run_for(Duration::from_millis(100));
        seen.extend(drain(&mut rx));
        if seen
            .iter()
            .any(|i| matches!(i, RecordItem::Close { stream: 0, .. }))
        {
            break;
        }
    }
    let n = names(&seen);
    let close = n.iter().position(|s| s == "close 0").unwrap_or_else(|| {
        panic!("alice's end arrives once there is room: {n:?}");
    });
    let last_alice = n.iter().rposition(|s| s.starts_with("packet 0 "));
    assert!(
        last_alice.is_none_or(|p| p < close),
        "no packet of a stream after its close: {n:?}"
    );
    for stream in [0u32, 1] {
        let seqs: Vec<u16> = packets(&seen)
            .iter()
            .filter(|p| p.0 == stream)
            .map(|p| p.1)
            .collect();
        assert!(seqs.windows(2).all(|w| w[1] > w[0]), "increasing: {seqs:?}");
    }

    // Bob's packets flow again afterwards.
    w.speak(b, 5, |_| 40);
    let later = drain(&mut rx);
    assert_eq!(
        packets(&later).iter().filter(|p| p.0 == 1).count(),
        5,
        "{:?}",
        names(&later)
    );
}

#[test]
fn the_last_one_out_ends_the_recording() {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    let b = w.join("bob", "call1");
    w.speak(a, 5, |_| 40);
    let mut rx = start(&mut w, "call1", "rec1", 1024);
    w.speak(a, 5, |_| 40);
    w.leave(a);
    w.run_for(Duration::from_millis(100));
    let n = names(&drain(&mut rx));
    assert!(n.contains(&"close 0".to_owned()), "{n:?}");
    assert!(
        !n.iter().any(|s| s.starts_with("end")),
        "bob is still in the call: {n:?}"
    );
    w.leave(b);
    w.run_for(Duration::from_millis(100));
    assert_eq!(names(&drain(&mut rx)), ["end CallEnded"]);
    assert!(
        rx.try_recv().is_err(),
        "nothing follows the end: the engine let go of the recording"
    );
}

#[test]
fn a_call_that_is_not_there_or_is_recorded_already_is_refused() {
    let mut w = World::new();
    let _a = w.join("alice", "call1");
    let mut none = start(&mut w, "nothing", "rec0", 8);
    assert_eq!(names(&drain(&mut none)), ["begin rec0", "end Refused"]);

    let mut first = start(&mut w, "call1", "rec1", 8);
    let mut second = start(&mut w, "call1", "rec2", 8);
    assert_eq!(names(&drain(&mut second)), ["begin rec2", "end Refused"]);
    assert_eq!(names(&drain(&mut first)), ["begin rec1"]);

    // Stopping what is not recorded under that id changes nothing.
    w.engine.record_stop(w.now, "group", "call1", "rec2");
    assert!(drain(&mut first).is_empty());
    w.engine.record_stop(w.now, "group", "call1", "rec1");
    assert_eq!(names(&drain(&mut first)), ["end Stopped"]);
    // And the call can be recorded again.
    let mut again = start(&mut w, "call1", "rec3", 8);
    assert_eq!(names(&drain(&mut again)), ["begin rec3"]);
}

#[test]
fn a_writer_that_quits_is_let_go_of() {
    for how in ["cancelled", "dropped"] {
        let mut w = World::new();
        let a = w.join("alice", "call1");
        let mut rx = start(&mut w, "call1", "rec1", 64);
        w.speak(a, 5, |_| 40);
        assert!(!drain(&mut rx).is_empty());
        if how == "cancelled" {
            rx.shared().cancel();
        } else {
            drop(rx);
        }
        // The engine notices at its next once-a-second upkeep, and sends nothing more.
        w.speak(a, 100, |_| 40);
        let mut next = start(&mut w, "call1", "rec2", 64);
        w.speak(a, 5, |_| 40);
        let n = names(&drain(&mut next));
        assert_eq!(n[0], "begin rec2", "{how}: the call can be recorded again");
        assert!(
            n.iter().any(|s| s.starts_with("open")),
            "{how}: and the new recording gets the speaker: {n:?}"
        );
    }
}
