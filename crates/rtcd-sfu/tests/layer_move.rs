//! A simulcast layer that moves to a new SSRC (an encoder restart) stays the same layer of the
//! same camera: its viewers keep their stream, a recording keeps its file, and the engine keeps
//! no trace of the old SSRC.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use rtcd_proto::MessageType;
use rtcd_sfu::ids::TrackType;
use rtcd_sfu::metrics::{Direction, Metrics};
use rtcd_sfu::record::{RecordItem, record_channel};
use rtcd_sfu::vp8;
use support::{Layer, World, describe, engine_config};

const BOTH: [Layer; 2] = [Layer::Low, Layer::High];

/// The tracks received from senders, added less removed.
#[derive(Default)]
struct Tracks(AtomicI64);

impl Metrics for Tracks {
    fn track_added(&self, _group: &str, direction: Direction, _mime: &str) {
        if direction == Direction::In {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    fn track_removed(&self, _group: &str, direction: Direction, _mime: &str) {
        if direction == Direction::In {
            self.0.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

/// Alice sends a camera; Bob watches its large layer.
fn watching(tracks: Arc<Tracks>) -> (World, usize, usize) {
    let mut w = World::with_metrics(engine_config(), tracks);
    let a = w.join_with_camera("alice", "call1");
    let b = w.join("bob", "call1");
    w.stream_video(a, false, 1.0, 30, &BOTH);
    let data =
        serde_json::to_vec(&serde_json::json!({ "session_ids": ["session-alice"], "low": [] }))
            .unwrap();
    w.signal(b, MessageType::VideoSubscribe, data);
    w.run_for(Duration::from_millis(500));
    w.stream_video(a, false, 1.0, 30, &BOTH);
    (w, a, b)
}

#[test]
fn a_layer_that_moves_to_a_new_ssrc_keeps_its_viewers() {
    let (mut w, a, b) = watching(Arc::default());
    let before = w.browsers[b].rtp.len();
    assert!(before > 30, "bob sees the camera before the move: {before}");

    w.move_layer(a, false, Layer::High, 7777);
    w.stream_video(a, false, 2.0, 30, &BOTH);

    let after: Vec<_> = w.browsers[b].rtp[before..]
        .iter()
        .filter(|(t, _)| t.starts_with("video_"))
        .map(|(_, p)| p)
        .collect();
    assert!(
        after.len() > 100,
        "bob keeps getting video after the move: {} packets",
        after.len()
    );
    assert!(
        after.iter().all(|p| describe(p).0 == Layer::High),
        "from the layer that moved"
    );
    assert!(
        describe(after[0]).1,
        "he is joined at a keyframe, which the sender was asked for"
    );
    assert!(
        w.browsers[a].keyframe_log.contains(&7777),
        "the keyframe was asked for on the new SSRC: {:?}",
        w.browsers[a].keyframe_log
    );
    // His stream is one stream to him: the move shows neither as a new SSRC nor as a jump.
    let first = w.browsers[b].rtp[before - 1].1.header.clone();
    assert_eq!(after[0].header.ssrc, first.ssrc);
    for pair in after.windows(2) {
        let (x, y) = (&pair[0].header, &pair[1].header);
        assert_eq!(y.sequence_number, x.sequence_number.wrapping_add(1));
        assert!(y.timestamp.wrapping_sub(x.timestamp) < 10_000);
    }
    assert!(
        after[0]
            .header
            .sequence_number
            .wrapping_sub(first.sequence_number)
            < 100,
        "the numbers carry on from before the move"
    );
}

#[test]
fn moving_a_layer_does_not_grow_the_engine() {
    let tracks = Arc::new(Tracks::default());
    let (mut w, a, b) = watching(tracks.clone());
    let sizes = w.engine.uplink_sizes();
    let counted = tracks.0.load(Ordering::Relaxed);
    let mut seen = w.browsers[b].rtp.len();

    // The layer may move 4 times a minute; these are 20 s apart.
    for n in 0..6u32 {
        w.move_layer(a, false, Layer::High, 8000 + n);
        w.stream_video(a, false, 20.0, 30, &BOTH);
        assert_eq!(w.engine.uplink_sizes(), sizes, "after move {n}");
        assert_eq!(tracks.0.load(Ordering::Relaxed), counted, "after move {n}");
        let now = w.browsers[b].rtp.len();
        assert!(
            now > seen + 300,
            "bob is fed after move {n}: {}",
            now - seen
        );
        seen = now;
    }
}

#[test]
fn a_recording_keeps_one_stream_for_a_layer_that_moves() {
    let mut w = World::new();
    let a = w.join_with_camera("alice", "call1");
    let _b = w.join("bob", "call1");
    let (sink, mut rx) = record_channel(4096);
    w.engine
        .record_start(w.now, "group", "call1", "rec1", w.now, sink);
    w.stream_video(a, false, 1.0, 30, &BOTH);
    let before: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    let video: Vec<u32> = before
        .iter()
        .filter_map(|i| match i {
            RecordItem::Open(o) if o.kind == TrackType::Video => Some(o.stream),
            _ => None,
        })
        .collect();
    assert_eq!(video.len(), 1, "one file for the camera");

    let last = before
        .iter()
        .rev()
        .find_map(|i| match i {
            RecordItem::Packet(p) if p.stream == video[0] => Some((p.seq, p.ts, p.arrived)),
            _ => None,
        })
        .expect("packets before the move");

    w.move_layer(a, false, Layer::High, 7777);
    w.stream_video(a, false, 2.0, 30, &BOTH);
    let after: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    let packets: Vec<_> = after
        .iter()
        .map(|i| match i {
            RecordItem::Packet(p) if p.stream == video[0] => p,
            other => panic!("the file is neither closed nor another opened: {other:?}"),
        })
        .collect();
    assert!(
        packets.len() > 100,
        "and it goes on: {} packets",
        packets.len()
    );

    // The writer is given one stream: the numbers carry on, with one left out so that it waits
    // for a keyframe, and the clock runs on in real time.
    assert_eq!(packets[0].seq, last.0.wrapping_add(2));
    for pair in packets.windows(2) {
        assert_eq!(pair[1].seq, pair[0].seq.wrapping_add(1));
        let ticks = pair[1].ts.wrapping_sub(pair[0].ts);
        assert!(ticks < 10_000, "the clock never jumps: {ticks}");
    }
    let key = packets
        .iter()
        .find(|p| vp8::is_keyframe(&p.payload))
        .expect("the new stream's keyframe is recorded");
    let moved_ms = i64::from(key.ts.wrapping_sub(last.1) as i32) / 90;
    let real_ms = key.arrived.duration_since(last.2).as_millis() as i64;
    assert!(
        (0..=real_ms + 33).contains(&moved_ms),
        "the file's clock moves {moved_ms} ms across the move, real time {real_ms} ms"
    );
}
