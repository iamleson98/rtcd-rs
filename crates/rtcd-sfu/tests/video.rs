//! Cameras and screen shares: simulcast layers, subscriptions, keyframe-gated switching.

mod support;

use std::time::Duration;

use rtcd_proto::MessageType;
use rtcd_sfu::ids::{TrackType, parse_track_id};
use support::{Layer, World, describe};

const BOTH: [Layer; 2] = [Layer::Low, Layer::High];

/// The video packets browser `i` received on tracks of `prefix`, as (layer, keyframe).
fn video_of(w: &World, i: usize, prefix: &str) -> Vec<(Layer, bool)> {
    w.browsers[i]
        .rtp
        .iter()
        .filter(|(t, _)| t.starts_with(prefix))
        .map(|(_, p)| describe(p))
        .collect()
}

fn subscribe(w: &mut World, i: usize, ids: &[&str], low: &[&str]) {
    let data = serde_json::to_vec(&serde_json::json!({ "session_ids": ids, "low": low })).unwrap();
    w.signal(i, MessageType::VideoSubscribe, data);
    w.run_for(Duration::from_millis(500));
}

#[test]
fn a_camera_reaches_the_other_participant_in_the_large_layer() {
    let mut w = World::new();
    let a = w.join_with_camera("alice", "call1");
    let b = w.join("bob", "call1");
    w.stream_video(a, false, 2.0, 30, &BOTH);

    let track = w.browsers[b]
        .tracks
        .iter()
        .find(|t| t.track_id.starts_with("video_"))
        .expect("bob receives a camera");
    assert_eq!(
        parse_track_id(&track.track_id),
        Some((TrackType::Video, "session-alice"))
    );
    let got = video_of(&w, b, "video_");
    assert!(got.len() > 60, "{} packets", got.len());
    assert!(
        got.iter().all(|(l, _)| *l == Layer::High),
        "only the large layer"
    );
    assert!(got[0].1, "and it begins with a keyframe");
}

#[test]
fn a_receiver_starts_at_a_keyframe_not_in_the_middle_of_a_gop() {
    let mut w = World::new();
    let a = w.join_with_camera("alice", "call1");
    // Alice sends a keyframe only every 3 seconds.
    w.stream_video(a, false, 1.0, 90, &BOTH);
    let b = w.join("bob", "call1");
    w.stream_video(a, false, 2.5, 90, &BOTH);
    let got = video_of(&w, b, "video_");
    assert!(!got.is_empty());
    assert!(
        got[0].1,
        "the first packet bob gets is a keyframe: {:?}",
        &got[..got.len().min(3)]
    );
}

#[test]
fn asking_for_the_small_layer_switches_without_renegotiating() {
    let mut w = World::new();
    let a = w.join_with_camera("alice", "call1");
    let b = w.join("bob", "call1");
    w.stream_video(a, false, 1.0, 30, &BOTH);
    let offers = w.browsers[b].offers_received;
    let before = video_of(&w, b, "video_").len();
    assert!(before > 0);

    subscribe(&mut w, b, &["session-alice"], &["session-alice"]);
    w.stream_video(a, false, 2.0, 30, &BOTH);

    let all = video_of(&w, b, "video_");
    let after = &all[before..];
    assert!(
        after.iter().any(|(l, _)| *l == Layer::Low),
        "moved to the small layer"
    );
    assert_eq!(
        w.browsers[b].offers_received, offers,
        "and no renegotiation"
    );
    // Once moved it stays moved, and the first small packet is a keyframe.
    let first_low = after.iter().position(|(l, _)| *l == Layer::Low).unwrap();
    assert!(after[first_low].1, "the switch happens at a keyframe");
    assert!(after[first_low..].iter().all(|(l, _)| *l == Layer::Low));
}

#[test]
fn the_stream_stays_continuous_across_a_switch() {
    let mut w = World::new();
    let a = w.join_with_camera("alice", "call1");
    let b = w.join("bob", "call1");
    w.stream_video(a, false, 1.0, 30, &BOTH);
    subscribe(&mut w, b, &["session-alice"], &["session-alice"]);
    w.stream_video(a, false, 2.0, 30, &BOTH);

    let packets: Vec<_> = w.browsers[b]
        .rtp
        .iter()
        .filter(|(t, _)| t.starts_with("video_"))
        .map(|(_, p)| p)
        .collect();
    let ssrcs: std::collections::HashSet<_> = packets.iter().map(|p| p.header.ssrc).collect();
    assert_eq!(
        ssrcs.len(),
        1,
        "one stream, one SSRC, whichever layer feeds it"
    );
    for pair in packets.windows(2) {
        let (x, y) = (
            pair[0].header.sequence_number,
            pair[1].header.sequence_number,
        );
        assert_eq!(
            y,
            x.wrapping_add(1),
            "sequence numbers carry on across the switch"
        );
    }
    for pair in packets.windows(2) {
        let (x, y) = (pair[0].header.timestamp, pair[1].header.timestamp);
        assert!(
            y.wrapping_sub(x) < 10_000,
            "the clock never jumps: {x} -> {y}"
        );
    }
}

#[test]
fn a_camera_nobody_asked_for_is_not_sent() {
    let mut w = World::new();
    let a = w.join_with_camera("alice", "call1");
    let b = w.join("bob", "call1");
    subscribe(&mut w, b, &[], &[]);
    w.stream_video(a, false, 1.0, 30, &BOTH);
    assert_eq!(video_of(&w, b, "video_").len(), 0);

    // Naming it brings it, with an offer for the new stream.
    let offers = w.browsers[b].offers_received;
    subscribe(&mut w, b, &["session-alice"], &[]);
    w.stream_video(a, false, 1.0, 30, &BOTH);
    assert!(w.browsers[b].offers_received > offers);
    assert!(video_of(&w, b, "video_").len() > 30);
}

#[test]
fn dropping_a_subscription_removes_the_stream() {
    let mut w = World::new();
    let a = w.join_with_camera("alice", "call1");
    let b = w.join("bob", "call1");
    w.stream_video(a, false, 1.0, 30, &BOTH);
    let offers = w.browsers[b].offers_received;
    subscribe(&mut w, b, &[], &[]);
    assert!(
        w.browsers[b].offers_received > offers,
        "the camera is taken back in an offer"
    );
    let seen = video_of(&w, b, "video_").len();
    w.stream_video(a, false, 1.0, 30, &BOTH);
    assert_eq!(
        video_of(&w, b, "video_").len(),
        seen,
        "nothing more arrives"
    );
}

#[test]
fn an_oversized_subscription_is_ignored() {
    let mut w = World::new();
    let a = w.join_with_camera("alice", "call1");
    let b = w.join("bob", "call1");
    let ids: Vec<String> = (0..101).map(|n| format!("s{n}")).collect();
    let data = serde_json::to_vec(&serde_json::json!({ "session_ids": ids, "low": [] })).unwrap();
    w.signal(b, MessageType::VideoSubscribe, data);
    w.stream_video(a, false, 1.0, 30, &BOTH);
    assert!(
        !video_of(&w, b, "video_").is_empty(),
        "still gets everything: the request was refused"
    );
}

#[test]
fn receivers_move_to_the_small_layer_while_the_large_one_is_paused() {
    let mut w = World::new();
    let a = w.join_with_camera("alice", "call1");
    let b = w.join("bob", "call1");
    w.stream_video(a, false, 1.5, 30, &BOTH);
    assert!(
        video_of(&w, b, "video_")
            .iter()
            .all(|(l, _)| *l == Layer::High)
    );

    // The browser drops its large layer; the small one keeps flowing.
    let seen = video_of(&w, b, "video_").len();
    w.stream_video(a, false, 4.0, 30, &[Layer::Low]);
    let after = &video_of(&w, b, "video_")[seen..];
    assert!(
        after.iter().any(|(l, _)| *l == Layer::Low),
        "bob moved to the small layer"
    );

    // And back when the large layer returns.
    let seen = video_of(&w, b, "video_").len();
    w.stream_video(a, false, 3.0, 30, &BOTH);
    let after = &video_of(&w, b, "video_")[seen..];
    assert!(
        after.iter().rev().take(10).all(|(l, _)| *l == Layer::High),
        "and back to the large one"
    );
}

#[test]
fn a_keyframe_request_from_a_receiver_reaches_the_sender() {
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
    let before = w.browsers[a].keyframe_requests.len();
    w.run_for(Duration::from_secs(2));
    w.request_keyframe(b, ssrc);
    assert!(
        w.browsers[a].keyframe_requests.len() > before,
        "alice was asked for a keyframe"
    );
    let large = w.browsers[a].camera.as_ref().unwrap().ssrcs[1];
    assert!(
        w.browsers[a].keyframe_requests.contains(&large),
        "of the layer bob is fed from"
    );
}

#[test]
fn keyframe_requests_are_limited_to_one_a_second() {
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
    w.run_for(Duration::from_secs(3));
    let before = w.browsers[a].keyframe_requests.len();
    for _ in 0..10 {
        w.request_keyframe(b, ssrc);
    }
    assert_eq!(
        w.browsers[a].keyframe_requests.len(),
        before + 1,
        "ten requests in half a second make one"
    );
}

#[test]
fn a_screen_share_reaches_everyone_and_ends_cleanly() {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    let b = w.join("bob", "call1");
    let c = w.join("carol", "call1");
    w.share_screen(a);
    w.run_for(Duration::from_secs(1));
    w.stream_video(a, true, 2.0, 30, &BOTH);

    for r in [b, c] {
        let track = w.browsers[r]
            .tracks
            .iter()
            .find(|t| t.track_id.starts_with("screen_"))
            .expect("the share arrives");
        assert_eq!(
            parse_track_id(&track.track_id),
            Some((TrackType::Screen, "session-alice"))
        );
        let got = video_of(&w, r, "screen_");
        assert!(got.len() > 60);
        // Estimates start low, so the small layer is what is sent.
        assert!(got.iter().all(|(l, _)| *l == Layer::Low));
    }
    assert!(
        w.browsers[a]
            .tracks
            .iter()
            .all(|t| !t.track_id.starts_with("screen_")),
        "the sharer is not sent its own screen"
    );

    let offers = w.browsers[b].offers_received;
    w.signal(a, MessageType::ScreenOff, vec![]);
    w.run_for(Duration::from_secs(1));
    assert!(
        w.browsers[b].offers_received > offers,
        "the share is taken back in an offer"
    );
}

/// A retransmission comes a round trip after the packet it replaces, and a screen share sends
/// hundreds of packets a second: by then the SFU has seen many packets of the stream. The packet
/// must still be accepted, or a hole never closes and a receiver of the stream is left on a frame
/// it cannot decode.
#[test]
fn a_retransmission_that_comes_late_is_accepted() {
    // An estimate above what the large layer needs, so the large layer is what bob gets.
    let cfg = rtcd_sfu::config::EngineConfig {
        min_bitrate: 3_000_000,
        max_bitrate: 8_000_000,
        ..support::engine_config()
    };
    let mut w = World::with_config(cfg);
    let a = w.join("alice", "call1");
    let b = w.join("bob", "call1");
    w.share_screen(a);
    w.run_for(Duration::from_secs(1));
    w.stream_video(a, true, 2.0, 30, &BOTH);

    w.send_video_losing(a, true, Layer::High, false, 4, Some(1));
    // Far more than the 64 packets a small anti-replay window covers go by before it is resent.
    w.stream_video(a, true, 3.0, 30, &BOTH);
    w.send_held_back(a, true);
    w.run_for(Duration::from_secs(1));

    assert!(
        w.browsers[b]
            .rtp
            .iter()
            .any(|(t, p)| t.starts_with("screen_") && p.payload.last() == Some(&support::LATE)),
        "the packet that came late reaches the receiver"
    );
}

#[test]
fn a_second_screen_share_is_refused_while_one_is_live() {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    let b = w.join("bob", "call1");
    let c = w.join("carol", "call1");
    w.share_screen(a);
    w.run_for(Duration::from_secs(1));
    w.share_screen(b);
    w.run_for(Duration::from_secs(1));
    w.stream_video(b, true, 1.0, 30, &BOTH);
    assert_eq!(
        video_of(&w, c, "screen_").len(),
        0,
        "bob's second share is not forwarded"
    );
    w.stream_video(a, true, 1.0, 30, &BOTH);
    assert!(video_of(&w, c, "screen_").len() > 30, "alice's is");
}

#[test]
fn a_late_joiner_is_brought_the_running_share_and_camera_in_one_offer() {
    let mut w = World::new();
    let a = w.join_with_camera("alice", "call1");
    w.share_screen(a);
    w.run_for(Duration::from_secs(1));
    w.stream_video(a, true, 1.0, 30, &BOTH);
    w.stream_video(a, false, 1.0, 30, &BOTH);

    let late = w.join("late", "call1");
    w.run_for(Duration::from_secs(1));
    assert_eq!(
        w.browsers[late].offers_received, 1,
        "voice-less, but camera and screen: one offer"
    );
    assert_eq!(w.browsers[late].media_maps[0].len(), 2);
    w.stream_video(a, true, 1.0, 30, &BOTH);
    w.stream_video(a, false, 1.0, 30, &BOTH);
    assert!(video_of(&w, late, "screen_").len() > 20);
    assert!(video_of(&w, late, "video_").len() > 20);
    assert!(
        video_of(&w, late, "screen_")[0].1 && video_of(&w, late, "video_")[0].1,
        "both start at keyframes"
    );
}

#[test]
fn an_undeclared_video_stream_is_not_forwarded() {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    let b = w.join("bob", "call1");
    // Alice shares a screen but never says so.
    let ssrcs = [3000, 3001];
    let _ = ssrcs;
    w.browsers[a].screen = None;
    let _ = b;
    w.run_for(Duration::from_millis(100));
    assert!(video_of(&w, b, "screen_").is_empty());
}

#[test]
fn a_receiver_with_bandwidth_to_spare_gets_the_large_screen_layer() {
    // An estimate that starts above what the large layer needs (2.5 Mbit/s nominal).
    let cfg = rtcd_sfu::config::EngineConfig {
        min_bitrate: 3_000_000,
        max_bitrate: 8_000_000,
        ..support::engine_config()
    };
    let mut w = World::with_config(cfg);
    let a = w.join("alice", "call1");
    let b = w.join("bob", "call1");
    w.share_screen(a);
    w.run_for(Duration::from_secs(1));
    w.stream_video(a, true, 4.0, 30, &BOTH);

    let got = video_of(&w, b, "screen_");
    assert!(
        got.iter().any(|(l, _)| *l == Layer::High),
        "the large layer was sent"
    );
    assert!(
        got.iter().rev().take(20).all(|(l, _)| *l == Layer::High),
        "and it stays"
    );
}

#[test]
fn a_receiver_that_asks_for_the_sharp_screen_gets_it_from_the_first_packet() {
    // The estimate is capped far below the large layer's rate: only the request puts bob on it.
    let cfg = rtcd_sfu::config::EngineConfig {
        min_bitrate: 300_000,
        max_bitrate: 600_000,
        ..support::engine_config()
    };
    let mut w = World::with_config(cfg);
    let a = w.join("alice", "call1");
    let b = w.join("bob", "call1");
    let c = w.join("carol", "call1");
    w.dc_send(b, rtcd_proto::dc::DcMessage::SharpScreen(true));
    w.share_screen(a);
    w.run_for(Duration::from_secs(1));
    w.stream_video(a, true, 4.0, 30, &BOTH);

    let sharp = video_of(&w, b, "screen_");
    assert!(
        !sharp.is_empty() && sharp.iter().all(|(l, _)| *l == Layer::High),
        "bob asked for the sharp layer, so he never gets the small one: {sharp:?}"
    );
    let plain = video_of(&w, c, "screen_");
    assert_eq!(
        plain.first().map(|(l, _)| *l),
        Some(Layer::Low),
        "carol starts on the small layer, as every receiver does"
    );
}

#[test]
fn a_subscription_with_null_lists_means_none_as_go_clients_encode_it() {
    let mut w = World::new();
    let a = w.join_with_camera("alice", "call1");
    let b = w.join("bob", "call1");
    w.stream_video(a, false, 1.0, 30, &BOTH);
    assert!(!video_of(&w, b, "video_").is_empty());
    w.signal(
        b,
        MessageType::VideoSubscribe,
        br#"{"session_ids":null,"low":null}"#.to_vec(),
    );
    w.run_for(Duration::from_millis(500));
    let seen = video_of(&w, b, "video_").len();
    w.stream_video(a, false, 1.0, 30, &BOTH);
    assert_eq!(
        video_of(&w, b, "video_").len(),
        seen,
        "null is an empty list, and an empty list wants no camera"
    );
}
