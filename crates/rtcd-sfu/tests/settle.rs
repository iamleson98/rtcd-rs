//! The engine takes datagrams in with `receive` and handles a batch of them with one `settle`, as
//! a shard does: sessions with input in the order it came, then each session that has something
//! to send flushed once. These tests run the world with a batch of datagrams per settle and check
//! that the packets, their order and their timing are those of settling after each one.

mod support;

use std::collections::BTreeMap;
use std::time::Duration;

use rtcd_proto::MessageType;
use rtcd_sfu::config::EngineConfig;
use support::{Layer, Sent, World, engine_config, voice_number};

const MS: Duration = Duration::from_millis(1);
const BOTH: [Layer; 2] = [Layer::Low, Layer::High];

/// The destination of each datagram of one batch the engine queued, as runs: `[1, 1, 2]` is two
/// datagrams to browser 1 and then one to browser 2.
fn runs(batch: &[Sent]) -> Vec<usize> {
    let mut runs: Vec<usize> = Vec::new();
    for &(i, _) in batch {
        if runs.last() != Some(&i) {
            runs.push(i);
        }
    }
    runs
}

/// Lets `speakers` talk for a second, so that the voices are being forwarded.
fn talk(w: &mut World, speakers: &[usize]) {
    for n in 0..50 {
        for &i in speakers {
            w.send_voice(i, n);
        }
        w.run_for(Duration::from_millis(20));
    }
    for b in &mut w.browsers {
        b.rtp.clear();
    }
}

/// Three participants in a call, alice and bob speaking, the world taking in every datagram of a
/// step before it settles.
fn trio() -> (World, [usize; 3]) {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    let b = w.join("bob", "call1");
    let c = w.join("carol", "call1");
    talk(&mut w, &[a, b]);
    w.receive_chunk = usize::MAX;
    (w, [a, b, c])
}

#[test]
fn a_receiver_is_flushed_once_per_settle() {
    let (mut w, [a, b, c]) = trio();
    w.transmit_log = Some(Vec::new());
    w.step(MS, |w| {
        w.send_voice(a, 500);
        w.send_voice(b, 500);
    });
    let log = w.transmit_log.take().unwrap();
    let first = log.first().expect("the engine sent the voices on");
    assert_eq!(
        first.iter().filter(|&&(i, _)| i == c).count(),
        2,
        "carol is sent the voice of each of them: {first:?}"
    );
    let runs = runs(first);
    let distinct = {
        let mut d = runs.clone();
        d.sort_unstable();
        d.dedup();
        d.len()
    };
    assert_eq!(
        runs.len(),
        distinct,
        "everything for a receiver is queued together, not once for each sender: {first:?}"
    );
}

#[test]
fn packets_reach_each_receiver_in_stream_order() {
    let (mut w, [a, b, c]) = trio();
    w.step(MS, |w| {
        for n in 0..5 {
            w.send_voice(a, n);
            w.send_voice(b, 100 + n);
        }
    });
    w.run_for(Duration::from_millis(100));
    // Each sender's frames arrive in the order they were sent, however the two streams of the
    // batch interleave.
    for (first, name) in [(0, "alice"), (100, "bob")] {
        let numbers: Vec<u32> = w.browsers[c]
            .rtp
            .iter()
            .filter_map(|(_, p)| voice_number(&p.payload))
            .filter(|n| (first..first + 5).contains(n))
            .collect();
        assert_eq!(
            numbers,
            (first..first + 5).collect::<Vec<_>>(),
            "carol hears {name}'s frames in order"
        );
    }
    // And each stream carol receives is numbered without a gap.
    let mut by_track: BTreeMap<&str, Vec<u16>> = BTreeMap::new();
    for (track, p) in &w.browsers[c].rtp {
        by_track
            .entry(track)
            .or_default()
            .push(p.header.sequence_number);
    }
    for (track, seqs) in by_track {
        assert!(
            seqs.windows(2).all(|p| p[1] == p[0].wrapping_add(1)),
            "{track} has its sequence numbers in order: {seqs:?}"
        );
    }
}

#[test]
fn a_keyframe_request_reaches_the_publisher_in_the_same_settle() {
    let mut w = World::new();
    let a = w.join_with_camera("alice", "call1");
    let b = w.join("bob", "call1");
    w.stream_video(a, false, 1.0, 30, &BOTH);
    let ssrc = w.browsers[b]
        .tracks
        .iter()
        .find(|t| t.track_id.starts_with("video_"))
        .unwrap()
        .ssrc;
    // Past the limit of one request a second.
    w.run_for(Duration::from_secs(2));
    w.receive_chunk = usize::MAX;
    w.transmit_log = Some(Vec::new());
    w.step(MS, |w| w.write_keyframe_request(b, ssrc));
    let log = w.transmit_log.take().unwrap();
    assert!(
        log.first()
            .is_some_and(|batch| batch.iter().any(|&(i, _)| i == a)),
        "the request is on its way to alice with what the settle queued: {log:?}"
    );
}

/// A packet as a browser has it: its sequence number, timestamp and payload.
type Heard = (u16, u32, Vec<u8>);

/// The browsers' packets for each (browser, track), in the order they arrived.
fn heard(w: &World) -> BTreeMap<(String, String), Vec<Heard>> {
    let mut heard: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for b in &w.browsers {
        for (track, p) in &b.rtp {
            heard
                .entry((b.name.clone(), track.clone()))
                .or_default()
                .push((
                    p.header.sequence_number,
                    p.header.timestamp,
                    p.payload.to_vec(),
                ));
        }
    }
    heard
}

/// Has `viewer` watch the cameras of `who`'s sessions, on the large layer.
fn watch(w: &mut World, viewer: usize, who: &[&str]) {
    let ids: Vec<String> = who.iter().map(|n| format!("session-{n}")).collect();
    let data = serde_json::to_vec(&serde_json::json!({ "session_ids": ids, "low": [] })).unwrap();
    w.signal(viewer, MessageType::VideoSubscribe, data);
}

/// One step of `by` ms of a camera that sends a frame of each layer every 33 ms, a keyframe where
/// the SFU asked for one and at the start.
fn camera_step(w: &mut World, camera: usize, t: usize) {
    w.step(MS, |w| {
        if !t.is_multiple_of(33) {
            return;
        }
        let ssrcs = w.browsers[camera].camera.as_ref().unwrap().ssrcs;
        for layer in [Layer::Low, Layer::High] {
            let ssrc = ssrcs[layer as usize];
            let asked = w.browsers[camera].keyframe_requests.contains(&ssrc);
            w.browsers[camera].keyframe_requests.retain(|&r| r != ssrc);
            let key = asked || t == 0;
            w.send_camera_frame(camera, layer, key, if key { 6 } else { 3 });
        }
    });
}

/// Three participants, two of them speaking and one with a camera, for 600 ms in steps of 1 ms.
fn a_busy_call(chunk: usize) -> World {
    let mut w = World::new();
    let a = w.join_with_camera("alice", "call1");
    let b = w.join("bob", "call1");
    let c = w.join("carol", "call1");
    watch(&mut w, b, &["alice"]);
    watch(&mut w, c, &["alice"]);
    for t in 0..1500 {
        camera_step(&mut w, a, t);
    }
    w.receive_chunk = chunk;
    for browser in &mut w.browsers {
        browser.rtp.clear();
    }
    for t in 1500..2100 {
        if t % 20 == 0 {
            w.send_voice(b, (t / 20) as u32);
            w.send_voice(c, 1000 + (t / 20) as u32);
        }
        camera_step(&mut w, a, t);
    }
    w.run_for(Duration::from_millis(100));
    w
}

#[test]
fn a_batch_gives_the_same_packets_as_one_by_one() {
    let one_by_one = heard(&a_busy_call(1));
    let batched = heard(&a_busy_call(usize::MAX));
    let chunked = heard(&a_busy_call(32));
    let carried = |prefix: &str| {
        one_by_one
            .iter()
            .filter(|((_, track), _)| track.starts_with(prefix))
            .map(|(_, packets)| packets.len())
            .sum::<usize>()
    };
    assert!(carried("video_") > 100, "the call carried video");
    assert!(carried("voice_") > 50, "and voices");
    assert_eq!(batched, one_by_one, "everything in a batch, settled once");
    assert_eq!(chunked, one_by_one, "32 datagrams to a settle");
}

#[test]
fn a_nack_is_answered_in_the_turn_it_arrived() {
    let mut w = World::new();
    let a = w.join_with_camera("alice", "call1");
    let b = w.join("bob", "call1");
    watch(&mut w, b, &["alice"]);
    for t in 0..1500 {
        camera_step(&mut w, a, t);
    }
    w.receive_chunk = usize::MAX;
    // The frame at 1,980 ms is lost on its way to bob: his link is down until the engine has sent
    // it, however the pacer spreads it out. His browser asks for its packets again once the next
    // frame shows the gap.
    let mut lost: Vec<u16> = Vec::new();
    for t in 1500..3000usize {
        let down = (1980..1990).contains(&t);
        w.browsers[b].unreachable = down;
        w.transmit_log = Some(Vec::new());
        camera_step(&mut w, a, t);
        let log = w.transmit_log.take().unwrap();
        let to_bob = |batch: &[Sent]| -> Vec<u16> {
            batch
                .iter()
                .filter(|&&(i, _)| i == b)
                .filter_map(|&(_, seq)| seq)
                .collect()
        };
        if down {
            lost.extend(log.iter().flat_map(|batch| to_bob(batch)));
            continue;
        }
        if lost.is_empty() {
            continue;
        }
        let again: Vec<&Vec<Sent>> = log
            .iter()
            .filter(|batch| to_bob(batch).iter().any(|seq| lost.contains(seq)))
            .collect();
        if !again.is_empty() {
            assert!(
                to_bob(&log[0]).iter().any(|seq| lost.contains(seq)),
                "the packets are sent again with what the settle of the request queued, not after \
                 the next timer: {log:?}"
            );
            return;
        }
    }
    panic!("bob was never sent the lost packets again ({lost:?})");
}

/// The most datagrams one settle queued in a call of four where three speak, `frames` frames of
/// each in one step, the engine settling every `chunk` datagrams.
fn burst(chunk: usize, frames: u32) -> usize {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    let b = w.join("bob", "call1");
    let c = w.join("carol", "call1");
    w.join("dave", "call1");
    talk(&mut w, &[a, b, c]);
    w.receive_chunk = chunk;
    w.max_burst = 0;
    w.step(MS, |w| {
        for n in 0..frames {
            for i in [a, b, c] {
                w.send_voice(i, 500 + n);
            }
        }
    });
    w.max_burst
}

#[test]
fn the_output_queue_is_bounded_by_the_chunk() {
    // A datagram of a voice is sent to the three others at most.
    let chunk = 4;
    let bounded = burst(chunk, 8);
    assert!(
        bounded > 0 && bounded <= chunk * 3,
        "a settle of {chunk} datagrams queues at most {}: {bounded}",
        chunk * 3
    );
    let unbounded = burst(usize::MAX, 8);
    assert!(
        unbounded > 4 * bounded,
        "everything in one settle queues it all at once: {unbounded} against {bounded}"
    );
}

/// A call of an alice with a camera and `viewers` who watch it, the world taking in a chunk of
/// datagrams before it settles, and the engine settling too when the datagrams taken in may queue
/// `flush_at` sends. Alice sends a frame of 32 packets on the large layer in one step. Returns
/// the most datagrams one settle queued.
fn camera_burst(viewers: usize, flush_at: usize) -> usize {
    // Without the pacer, which would spread the frame over the viewers' send budgets.
    let mut w = World::with_config(EngineConfig {
        pacing: false,
        ..engine_config()
    });
    let a = w.join_with_camera("alice", "call1");
    for n in 0..viewers {
        let v = w.join(&format!("viewer{n}"), "call1");
        watch(&mut w, v, &["alice"]);
    }
    for t in 0..1500 {
        camera_step(&mut w, a, t);
    }
    w.receive_chunk = usize::MAX;
    w.flush_at = flush_at;
    w.max_burst = 0;
    w.step(MS, |w| w.send_camera_frame(a, Layer::High, false, 32));
    w.max_burst
}

#[test]
fn a_chunk_ends_when_what_it_may_queue_is_a_lot_whatever_the_fan() {
    const VIEWERS: usize = 40;
    const FLUSH_AT: usize = 100;
    let bounded = camera_burst(VIEWERS, FLUSH_AT);
    assert!(
        bounded > 0 && bounded <= FLUSH_AT + VIEWERS,
        "a settle queues what it was asked to and the last packet's copies: {bounded}"
    );
    let unbounded = camera_burst(VIEWERS, usize::MAX);
    assert!(
        unbounded > 4 * bounded,
        "without the rule the 32 packets are all queued before the first send: {unbounded} \
         against {bounded}"
    );
}

/// A call of alice with a camera, carol and bob watching it; bob is the first to join when
/// `bob_first`, so that the world takes his datagrams in before alice's. In one step alice sends a
/// frame and bob closes his connection, the engine taking both in before it settles. Returns the
/// world, alice's, bob's and carol's indexes, and the frame's packets.
fn bob_leaves_as_a_frame_is_sent(bob_first: bool) -> (World, [usize; 3]) {
    let mut w = World::new();
    let (a, b);
    if bob_first {
        b = w.join("bob", "call1");
        a = w.join_with_camera("alice", "call1");
    } else {
        a = w.join_with_camera("alice", "call1");
        b = w.join("bob", "call1");
    }
    let c = w.join("carol", "call1");
    for viewer in [b, c] {
        watch(&mut w, viewer, &["alice"]);
    }
    for t in 0..1500 {
        camera_step(&mut w, a, t);
    }
    w.receive_chunk = usize::MAX;
    w.browsers[c].rtp.clear();
    w.step(MS, |w| {
        w.send_camera_frame(a, Layer::High, false, 20);
        w.close_connection(b);
    });
    w.run_for(Duration::from_millis(100));
    (w, [a, b, c])
}

#[test]
fn a_session_that_leaves_mid_chunk_is_closed_once_and_the_others_lose_nothing() {
    for bob_first in [true, false] {
        let (mut w, [a, b, c]) = bob_leaves_as_a_frame_is_sent(bob_first);
        let bob = w.browsers[b].session_id.clone();
        assert_eq!(
            w.closed.iter().filter(|(id, _)| *id == bob).count(),
            1,
            "bob is closed once (bob first: {bob_first}): {:?}",
            w.closed
        );
        let seqs: Vec<u16> = w.browsers[c]
            .rtp
            .iter()
            .filter(|(track, _)| track.starts_with("video_"))
            .map(|(_, p)| p.header.sequence_number)
            .collect();
        assert_eq!(seqs.len(), 20, "carol has the whole frame: {seqs:?}");
        assert!(
            seqs.windows(2).all(|p| p[1] == p[0].wrapping_add(1)),
            "in order: {seqs:?}"
        );
        // And what comes next is not queued for bob.
        w.transmit_log = Some(Vec::new());
        for t in (33..=330).step_by(33) {
            camera_step(&mut w, a, t);
        }
        let log = w.transmit_log.take().unwrap();
        assert!(!log.is_empty(), "the call goes on");
        assert!(
            log.iter().flatten().all(|&(i, _)| i != b),
            "nothing is sent to bob after he left"
        );
    }
}

/// Alice sends a camera to bob, whose uplink is lost from the 1,500th millisecond on while the
/// node closes a session it has not heard from in two seconds. Returns the millisecond bob was
/// closed at and everything he was sent, the world settling after every `chunk` datagrams.
fn bob_falls_silent(chunk: usize) -> (Option<usize>, Vec<Heard>) {
    let mut w = World::with_config(EngineConfig {
        idle_timeout: Duration::from_secs(2),
        ..engine_config()
    });
    let a = w.join_with_camera("alice", "call1");
    let b = w.join("bob", "call1");
    watch(&mut w, b, &["alice"]);
    for t in 0..1500 {
        camera_step(&mut w, a, t);
    }
    w.receive_chunk = chunk;
    w.browsers[b].rtp.clear();
    w.browsers[b].uplink_dropped = true;
    let bob = w.browsers[b].session_id.clone();
    let mut closed_at = None;
    let mut heard = Vec::new();
    for t in 1500..4500 {
        camera_step(&mut w, a, t);
        for (_, p) in w.browsers[b].rtp.drain(..) {
            heard.push((
                p.header.sequence_number,
                p.header.timestamp,
                p.payload.to_vec(),
            ));
        }
        if closed_at.is_none() && w.closed.iter().any(|(id, _)| *id == bob) {
            closed_at = Some(t);
        }
    }
    (closed_at, heard)
}

#[test]
fn a_viewer_that_falls_silent_is_sent_the_same_and_closed_at_the_same_time() {
    let (closed, heard) = bob_falls_silent(1);
    let (closed_batched, heard_batched) = bob_falls_silent(usize::MAX);
    assert!(
        closed.is_some_and(|t| t >= 3400),
        "closed after two seconds of silence, the last packet having come before 1,500: {closed:?}"
    );
    assert_eq!(closed_batched, closed, "at the same millisecond");
    assert!(heard.len() > 100, "he was sent the camera until then");
    assert_eq!(heard_batched, heard, "the same datagrams");
}
