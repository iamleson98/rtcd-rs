//! Audio calls: joining, hearing each other, muting, voice activity and leaving.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use rtcd_proto::MessageType;
use rtcd_proto::dc::DcMessage;
use rtcd_sfu::config::EngineConfig;
use rtcd_sfu::engine::{CloseReason, Load};
use rtcd_sfu::ids::{TrackType, parse_track_id};
use rtcd_sfu::metrics::{Metrics, ReceiverQuality};
use support::{World, config_with_refusals, engine_config};

/// Waits until browser `i` has received `n` tracks.
fn wait_tracks(w: &mut World, i: usize, n: usize) {
    assert!(
        w.run_until(Duration::from_secs(10), |w| w.browsers[i].tracks.len() >= n),
        "{} has {} tracks",
        w.browsers[i].name,
        w.browsers[i].tracks.len()
    );
}

/// Browser `i` speaks for a second and everyone else lets it arrive.
fn talk(w: &mut World, i: usize) {
    w.speak(i, 50, |f| if f % 2 == 0 { 20 } else { 70 });
}

#[test]
fn a_participant_joins_and_connects() {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    assert!(w.browsers[a].connected);
    assert_eq!(w.engine.session_count(), 1);
}

#[test]
fn two_participants_hear_each_other() {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    let b = w.join("bob", "call1");
    // Alice already spoke before Bob came: Bob is brought up to date, and Alice is told of Bob.
    wait_tracks(&mut w, a, 0);
    talk(&mut w, a);
    talk(&mut w, b);
    wait_tracks(&mut w, a, 1);
    wait_tracks(&mut w, b, 1);

    let heard_by_bob = &w.browsers[b];
    let track = &heard_by_bob.tracks[0];
    let (ty, from) = parse_track_id(&track.track_id).expect("a track id of ours");
    assert_eq!(ty, TrackType::Voice);
    assert_eq!(from, "session-alice", "the track id names its sender");
    assert_eq!(
        track.stream_ids,
        vec!["session-alice"],
        "and so does its stream"
    );
    assert!(
        heard_by_bob.received_on("voice_session-alice") > 40,
        "bob heard {} packets",
        heard_by_bob.received_on("voice_")
    );

    let heard_by_alice = &w.browsers[a];
    assert!(heard_by_alice.received_on("voice_session-bob") > 40);
    // Nobody hears themselves.
    assert_eq!(heard_by_alice.received_on("voice_session-alice"), 0);
    assert_eq!(heard_by_bob.received_on("voice_session-bob"), 0);
}

#[test]
fn a_newcomer_gets_everyone_in_one_offer() {
    let mut w = World::new();
    for name in ["a", "b", "c", "d"] {
        let i = w.join(name, "call1");
        talk(&mut w, i);
    }
    w.run_for(Duration::from_secs(1));
    let before: Vec<_> = (0..4).map(|i| w.browsers[i].offers_received).collect();

    let late = w.join("late", "call1");
    w.run_for(Duration::from_secs(1));
    let late_browser = &w.browsers[late];
    assert_eq!(
        late_browser.offers_received, 1,
        "four tracks, one round trip"
    );
    assert_eq!(late_browser.media_maps.len(), 1, "with one media map");
    assert_eq!(late_browser.media_maps[0].len(), 4);
    for info in late_browser.media_maps[0].values() {
        assert_eq!(info.kind, "voice");
    }
    // The others each get one more offer, for the newcomer's microphone, once it sends.
    talk(&mut w, late);
    for i in 0..4 {
        talk(&mut w, i);
    }
    wait_tracks(&mut w, late, 4);
    for (i, was) in before.iter().enumerate() {
        assert_eq!(
            w.browsers[i].offers_received,
            was + 1,
            "{} was offered the newcomer once",
            w.browsers[i].name
        );
    }
}

#[test]
fn muting_stops_the_voice_and_unmuting_resumes_it() {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    let b = w.join("bob", "call1");
    talk(&mut w, a);
    wait_tracks(&mut w, b, 1);
    let heard = w.browsers[b].received_on("voice_");
    assert!(heard > 0);

    w.signal(a, MessageType::Mute, vec![]);
    talk(&mut w, a);
    assert_eq!(
        w.browsers[b].received_on("voice_"),
        heard,
        "a muted microphone is not forwarded"
    );

    w.signal(a, MessageType::Unmute, vec![]);
    talk(&mut w, a);
    assert!(w.browsers[b].received_on("voice_") > heard + 40);
}

#[test]
fn a_mute_sent_before_the_track_opens_still_applies() {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    let b = w.join("bob", "call1");
    w.signal(a, MessageType::Mute, vec![]);
    talk(&mut w, a);
    assert_eq!(w.browsers[b].received_on("voice_"), 0);
}

#[test]
fn voice_activity_is_reported_once_per_change() {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    w.join("bob", "call1");
    // A steady level is not speech.
    w.speak(a, 100, |_| 45);
    assert!(w.voice_events.is_empty(), "{:?}", w.voice_events);
    // Alternating levels are.
    w.speak(a, 100, |f| if f % 2 == 0 { 20 } else { 70 });
    assert_eq!(w.voice_events, vec![("session-alice".to_owned(), true)]);
    // Silence ends it, and muting while speaking does too.
    w.speak(a, 200, |_| 127);
    assert_eq!(
        w.voice_events.last(),
        Some(&("session-alice".to_owned(), false))
    );
}

#[test]
fn muting_while_speaking_ends_voice_activity() {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    w.join("bob", "call1");
    w.speak(a, 150, |f| if f % 2 == 0 { 20 } else { 70 });
    assert_eq!(
        w.voice_events.last(),
        Some(&("session-alice".to_owned(), true))
    );
    w.signal(a, MessageType::Mute, vec![]);
    w.run_for(Duration::from_millis(50));
    assert_eq!(
        w.voice_events.last(),
        Some(&("session-alice".to_owned(), false))
    );
}

#[test]
fn leaving_removes_the_track_from_the_others() {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    let b = w.join("bob", "call1");
    talk(&mut w, a);
    wait_tracks(&mut w, b, 1);
    let offers = w.browsers[b].offers_received;

    w.leave(a);
    w.run_for(Duration::from_secs(2));
    assert!(
        w.closed
            .contains(&("session-alice".to_owned(), CloseReason::Left))
    );
    assert_eq!(w.engine.session_count(), 1);
    assert!(
        w.browsers[b].offers_received > offers,
        "bob was offered the removal"
    );
    assert!(w.browsers[b].pc.get_transceivers().count() >= 1);
}

#[test]
fn a_call_ends_when_its_last_participant_leaves_and_the_id_can_be_reused() {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    w.leave(a);
    w.run_for(Duration::from_secs(1));
    assert_eq!(w.engine.session_count(), 0);
    // The same session id again, after the first ended.
    w.browsers.clear();
    let again = w.join("alice", "call1");
    assert!(w.browsers[again].connected);
}

#[test]
fn calls_are_isolated() {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    let b = w.join("bob", "call2");
    talk(&mut w, a);
    talk(&mut w, b);
    assert_eq!(w.browsers[a].tracks.len(), 0);
    assert_eq!(w.browsers[b].tracks.len(), 0);
}

#[test]
fn the_signaling_lock_is_given_to_one_side_at_a_time() {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    assert!(w.request_lock(a), "free lock is granted");
    assert!(!w.request_lock(a), "a held lock is refused");
    w.unlock(a);
    assert!(w.request_lock(a), "and available again after the unlock");
}

#[test]
fn the_sfu_waits_for_a_participant_holding_the_lock_before_offering() {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    let b = w.join("bob", "call1");
    talk(&mut w, a);
    wait_tracks(&mut w, b, 1);

    // Bob takes the lock; Alice's new microphone track would normally trigger an offer to Bob.
    assert!(w.request_lock(b));
    let offers = w.browsers[b].offers_received;
    let c = w.join("carol", "call1");
    talk(&mut w, c);
    w.run_for(Duration::from_secs(2));
    assert_eq!(
        w.browsers[b].offers_received, offers,
        "no offer while Bob holds the lock"
    );

    w.unlock(b);
    w.run_for(Duration::from_millis(500));
    assert_eq!(
        w.browsers[b].offers_received,
        offers + 1,
        "the offer goes out once the lock is free"
    );
    talk(&mut w, c);
    wait_tracks(&mut w, b, 2);
}

#[test]
fn a_participant_that_never_unlocks_does_not_block_the_sfu_forever() {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    let b = w.join("bob", "call1");
    let offers = w.browsers[b].offers_received;
    assert!(w.request_lock(b));
    talk(&mut w, a);
    // Bob vanishes with the lock; the hold runs out and the offer goes out.
    w.run_for(Duration::from_secs(7));
    assert_eq!(w.browsers[b].offers_received, offers, "still held");
    w.run_for(Duration::from_secs(4));
    assert_eq!(
        w.browsers[b].offers_received,
        offers + 1,
        "the SFU took the lock back"
    );
}

#[test]
fn ping_pongs_on_the_data_channel() {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    w.dc_send(a, DcMessage::Ping);
    w.run_for(Duration::from_millis(200));
    assert!(w.browsers[a].dc_frames.contains(&DcMessage::Pong));
}

#[test]
fn a_session_that_never_offers_is_dropped() {
    let mut w = World::new();
    let cfg = rtcd_proto::SessionConfig {
        group_id: "group".into(),
        call_id: "c".into(),
        user_id: "u".into(),
        session_id: "idle".into(),
        ..Default::default()
    };
    w.engine
        .handle_command(w.now, rtcd_sfu::engine::Command::Join(cfg));
    w.run_for(Duration::from_secs(11));
    assert_eq!(
        w.closed,
        vec![("idle".to_owned(), CloseReason::SignalingFailed)]
    );
    assert_eq!(w.engine.session_count(), 0);
}

#[test]
fn a_duplicate_join_leaves_the_session_alone() {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    let cfg = w.engine.session_config("session-alice").unwrap();
    w.engine
        .handle_command(w.now, rtcd_sfu::engine::Command::Join(cfg));
    w.run_for(Duration::from_secs(1));
    assert!(w.closed.is_empty());
    assert!(w.browsers[a].connected);
    assert_eq!(w.engine.session_count(), 1);
}

#[test]
fn a_join_without_ids_is_refused() {
    let mut w = World::new();
    w.engine.handle_command(
        w.now,
        rtcd_sfu::engine::Command::Join(rtcd_proto::SessionConfig::default()),
    );
    assert_eq!(w.engine.session_count(), 0);
}

#[test]
fn messages_for_unknown_sessions_are_dropped() {
    let mut w = World::new();
    let msg = rtcd_proto::Message {
        group_id: "g".into(),
        user_id: "u".into(),
        session_id: "ghost".into(),
        call_id: "c".into(),
        kind: MessageType::Mute,
        data: vec![],
    };
    w.engine
        .handle_command(w.now, rtcd_sfu::engine::Command::Signal(msg));
    assert_eq!(w.engine.session_count(), 0);
}

#[test]
fn shutdown_ends_every_session() {
    let mut w = World::new();
    w.join("alice", "call1");
    w.join("bob", "call1");
    w.engine.shutdown(w.now);
    w.run_for(Duration::from_millis(100));
    assert_eq!(w.engine.session_count(), 0);
    assert_eq!(w.closed.len(), 2);
    assert!(w.closed.iter().all(|(_, r)| *r == CloseReason::Shutdown));
}

#[test]
fn a_participant_that_goes_silent_is_dropped() {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    w.join("bob", "call1");
    w.run_for(Duration::from_secs(2));
    // The network path to Alice dies without a word.
    w.blackhole_browser(a);
    w.run_for(Duration::from_secs(29));
    assert_eq!(w.engine.session_count(), 2, "not yet");
    w.run_for(Duration::from_secs(4));
    assert!(
        w.closed
            .contains(&("session-alice".to_owned(), CloseReason::ConnectionLost)),
        "{:?}",
        w.closed
    );
    assert_eq!(w.engine.session_count(), 1);
}

#[test]
fn a_session_that_never_connects_is_dropped() {
    let mut w = World::new();
    w.blackhole = true;
    w.start("alice", "call1");
    w.run_for(Duration::from_secs(31));
    assert_eq!(
        w.closed,
        vec![("session-alice".to_owned(), CloseReason::ConnectTimeout)]
    );
}

#[test]
fn a_participant_that_closes_its_connection_is_dropped_at_once() {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    w.join("bob", "call1");
    let _ = rtc::sansio::Protocol::close(&mut w.browsers[a].pc);
    w.run_for(Duration::from_millis(500));
    assert!(
        w.closed
            .contains(&("session-alice".to_owned(), CloseReason::ConnectionLost)),
        "{:?}",
        w.closed
    );
    assert_eq!(w.engine.session_count(), 1);
}

#[test]
fn browsers_are_asked_to_send_voice_with_the_configured_packet_time() {
    let mut w = World::with_config(EngineConfig {
        audio_ptime_ms: 40,
        ..engine_config()
    });
    let a = w.join("alice", "call1");
    let b = w.join("bob", "call1");
    for i in [a, b] {
        let answer = &w.browsers[i].descriptions[0];
        assert!(
            answer.contains("\r\na=ptime:40\r\na=maxptime:40\r\n"),
            "{} was told no packet time:\n{answer}",
            w.browsers[i].name
        );
    }
    // Every later description repeats it: a browser that is given an offer without a packet time
    // goes back to its own.
    talk(&mut w, a);
    wait_tracks(&mut w, b, 1);
    assert!(
        w.browsers[b].descriptions.len() > 1,
        "an offer for alice's microphone"
    );
    for sdp in &w.browsers[b].descriptions {
        assert_eq!(
            sdp.matches("m=audio").count(),
            sdp.matches("a=ptime:40").count(),
            "{sdp}"
        );
    }
}

#[test]
fn the_browser_chooses_its_packet_time_unless_one_is_configured() {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    assert!(!w.browsers[a].descriptions[0].contains("a=ptime"));
}

/// A call where `levels` speak at once (browser `i` at audio level `levels[i]`, -dBov) for three
/// seconds, plus one listener that never speaks. Returns how many packets the listener received
/// from each speaker.
fn crowd(audio_loudest: usize, levels: &[u8]) -> Vec<usize> {
    let mut w = World::with_config(EngineConfig {
        audio_loudest,
        ..engine_config()
    });
    let speakers: Vec<usize> = (0..levels.len())
        .map(|i| w.join(&format!("s{i}"), "call1"))
        .collect();
    let listener = w.join("listener", "call1");
    wait_tracks(&mut w, listener, 0);
    for _ in 0..150 {
        for (&i, &level) in speakers.iter().zip(levels) {
            w.send_audio(i, level);
        }
        w.run_for(Duration::from_millis(20));
    }
    w.run_for(Duration::from_millis(200));
    (0..levels.len())
        .map(|i| w.browsers[listener].received_on(&format!("voice_session-s{i}")))
        .collect()
}

#[test]
fn only_the_loudest_voices_are_forwarded_beyond_the_limit() {
    let heard = crowd(2, &[25, 30, 90, 100]);
    assert!(heard[0] > 100, "the loudest was heard {} times", heard[0]);
    assert!(
        heard[1] > 100,
        "the second loudest was heard {} times",
        heard[1]
    );
    assert!(heard[2] < 20, "a quiet voice was heard {} times", heard[2]);
    assert!(heard[3] < 20, "a quiet voice was heard {} times", heard[3]);
}

#[test]
fn a_muted_voice_gives_up_its_slot_at_once() {
    let mut w = World::with_config(EngineConfig {
        audio_loudest: 1,
        ..engine_config()
    });
    let teacher = w.join("teacher", "call1");
    let student = w.join("student", "call1");
    let listener = w.join("listener", "call1");
    wait_tracks(&mut w, listener, 0);
    // The teacher is loud and holds the only slot.
    for _ in 0..100 {
        w.send_audio(teacher, 25);
        w.send_audio(student, 90);
        w.run_for(Duration::from_millis(20));
    }
    let student_before = w.browsers[listener].received_on("voice_session-student");
    assert!(
        student_before < 20,
        "the student was heard {student_before} times"
    );

    // The teacher mutes, and the student answers straight away: not half a second of the answer
    // is lost to a slot the muted voice still holds.
    w.signal(teacher, MessageType::Mute, vec![]);
    w.run_for(Duration::from_millis(20));
    for _ in 0..25 {
        w.send_audio(student, 90);
        w.run_for(Duration::from_millis(20));
    }
    let heard = w.browsers[listener].received_on("voice_session-student") - student_before;
    assert!(
        heard >= 22,
        "the student's first half second reached {heard} of 25 packets"
    );
}

/// Counts the receiver quality reports, each of which costs a full statistics snapshot.
#[derive(Default)]
struct QualityReports(AtomicUsize);

impl Metrics for QualityReports {
    fn receiver_quality(&self, _: ReceiverQuality) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn a_voice_only_call_pays_for_no_video_statistics() {
    let reports = Arc::new(QualityReports::default());
    let mut w = World::with_metrics(engine_config(), reports.clone());
    let a = w.join("alice", "call1");
    w.join("bob", "call1");
    talk(&mut w, a);
    // The upkeep that applies shedding runs every second for voice, and so would a read of the
    // statistics every five seconds if it were not left to calls with video.
    w.run_for(Duration::from_secs(12));
    assert_eq!(reports.0.load(Ordering::Relaxed), 0);
}

#[test]
fn a_call_within_the_voice_limit_forwards_everyone() {
    let heard = crowd(3, &[25, 90, 100]);
    for (i, n) in heard.iter().enumerate() {
        assert!(*n > 100, "speaker {i} was heard {n} times");
    }
}

#[test]
fn no_voice_limit_forwards_everyone() {
    let heard = crowd(0, &[25, 30, 90, 100]);
    for (i, n) in heard.iter().enumerate() {
        assert!(*n > 100, "speaker {i} was heard {n} times");
    }
}

/// The sequence numbers a listener saw on the voice of speaker `name`, in order.
fn sequence_heard(w: &World, listener: usize, name: &str) -> Vec<u16> {
    let prefix = format!("voice_session-{name}");
    w.browsers[listener]
        .rtp
        .iter()
        .filter(|(t, _)| t.starts_with(&prefix))
        .map(|(_, p)| p.header.sequence_number)
        .collect()
}

#[test]
fn a_voice_that_returns_after_being_left_out_carries_on_without_a_gap() {
    // One voice is forwarded at a time. The talkers swap twice, so each voice is left out and then
    // comes back: a receiver that saw a jump in sequence numbers would count it as lost packets and
    // may restart its jitter buffer, at the first syllable of every turn.
    let mut w = World::with_config(EngineConfig {
        audio_loudest: 1,
        ..engine_config()
    });
    let a = w.join("a", "call1");
    let b = w.join("b", "call1");
    let listener = w.join("listener", "call1");
    wait_tracks(&mut w, listener, 0);
    for (a_level, b_level) in [(20, 100), (100, 20), (20, 100)] {
        for _ in 0..150 {
            w.send_audio(a, a_level);
            w.send_audio(b, b_level);
            w.run_for(Duration::from_millis(20));
        }
    }
    w.run_for(Duration::from_millis(200));
    for name in ["a", "b"] {
        let seqs = sequence_heard(&w, listener, name);
        assert!(seqs.len() > 100, "{name} was heard {} times", seqs.len());
        let gaps: Vec<(u16, u16)> = seqs
            .windows(2)
            .filter(|p| p[1].wrapping_sub(p[0]) != 1)
            .map(|p| (p[0], p[1]))
            .collect();
        assert!(gaps.is_empty(), "{name}'s sequence jumped: {gaps:?}");
    }
}

#[test]
fn a_shard_with_no_room_left_forwards_two_voices_at_most_and_gives_them_back() {
    let mut w = World::with_config(EngineConfig {
        audio_loudest: 0,
        ..engine_config()
    });
    let speakers: Vec<usize> = (0..4).map(|i| w.join(&format!("s{i}"), "call1")).collect();
    let listener = w.join("listener", "call1");
    wait_tracks(&mut w, listener, 0);
    let levels = [25u8, 30, 90, 100];
    let talk_for = |w: &mut World, frames: usize| {
        for _ in 0..frames {
            for (&i, &level) in speakers.iter().zip(&levels) {
                w.send_audio(i, level);
            }
            w.run_for(Duration::from_millis(20));
        }
    };
    let heard =
        |w: &World, i: usize| w.browsers[listener].received_on(&format!("voice_session-s{i}"));

    // With room to spare nobody is left out.
    talk_for(&mut w, 100);
    assert!((0..4).all(|i| heard(&w, i) > 60), "everyone is heard");

    // Once the shard is under pressure (the upkeep runs once a second) the quiet voices stop.
    w.engine.set_load(Load::Shedding);
    w.run_for(Duration::from_secs(2));
    let before: Vec<usize> = (0..4).map(|i| heard(&w, i)).collect();
    talk_for(&mut w, 150);
    let gained: Vec<usize> = (0..4).map(|i| heard(&w, i) - before[i]).collect();
    assert!(
        gained[0] > 100 && gained[1] > 100,
        "the two loudest go on: {gained:?}"
    );
    assert!(
        gained[2] < 30 && gained[3] < 30,
        "the others are left out: {gained:?}"
    );

    // At its limit: the loudest voice only.
    w.engine.set_load(Load::Saturated);
    w.run_for(Duration::from_secs(2));
    let before: Vec<usize> = (0..4).map(|i| heard(&w, i)).collect();
    talk_for(&mut w, 150);
    let gained: Vec<usize> = (0..4).map(|i| heard(&w, i) - before[i]).collect();
    assert!(gained[0] > 100, "the loudest goes on: {gained:?}");
    assert!(
        gained[1] < 30 && gained[2] < 30 && gained[3] < 30,
        "the others are left out: {gained:?}"
    );

    // And come back when it has room again.
    w.engine.set_load(Load::Ok);
    w.run_for(Duration::from_secs(2));
    let before: Vec<usize> = (0..4).map(|i| heard(&w, i)).collect();
    talk_for(&mut w, 100);
    assert!(
        (0..4).all(|i| heard(&w, i) - before[i] > 60),
        "everyone is heard again"
    );
}

#[test]
fn a_node_whose_kernel_keeps_refusing_after_the_squeeze_forwards_the_loudest_voice_only() {
    let refused = Arc::new(AtomicU64::new(0));
    let mut w = World::with_config(EngineConfig {
        audio_loudest: 0,
        ..config_with_refusals(&refused)
    });
    let speakers: Vec<usize> = (0..4).map(|i| w.join(&format!("s{i}"), "call1")).collect();
    let listener = w.join("listener", "call1");
    wait_tracks(&mut w, listener, 0);
    // The level is minus dBov: the smaller, the louder.
    let levels = [25u8, 30, 90, 100];
    // One second of everyone talking while the kernel has refused `per_second` more datagrams.
    let second = |w: &mut World, per_second: u64| {
        refused.fetch_add(per_second, Ordering::Relaxed);
        for _ in 0..50 {
            for (&i, &level) in speakers.iter().zip(&levels) {
                w.send_audio(i, level);
            }
            w.run_for(Duration::from_millis(20));
        }
    };
    let heard =
        |w: &World, i: usize| w.browsers[listener].received_on(&format!("voice_session-s{i}"));
    let gained = |w: &mut World, seconds: usize, per_second: u64| {
        let before: Vec<usize> = (0..4).map(|i| heard(w, i)).collect();
        for _ in 0..seconds {
            second(w, per_second);
        }
        (0..4).map(|i| heard(w, i) - before[i]).collect::<Vec<_>>()
    };

    let calm = gained(&mut w, 3, 0);
    assert!(calm.iter().all(|&n| n > 100), "everyone is heard: {calm:?}");

    // The first seconds of refusals: video is squeezed and two voices are forwarded.
    gained(&mut w, 2, 5_000);
    let squeezed = gained(&mut w, 2, 5_000);
    assert!(
        squeezed[0] > 50 && squeezed[1] > 50 && squeezed[2] < 30 && squeezed[3] < 30,
        "the two loudest go on, the others are left out: {squeezed:?}"
    );

    // Refusals that go on past the squeeze: the loudest voice only.
    gained(&mut w, 3, 5_000);
    let choked = gained(&mut w, 3, 5_000);
    assert!(choked[0] > 100, "the loudest goes on: {choked:?}");
    assert!(
        choked[1] < 30 && choked[2] < 30 && choked[3] < 30,
        "the others are left out: {choked:?}"
    );

    // Once the kernel has accepted everything for a while the choke is over. The node does not
    // go back to what it sent when the kernel first refused (that is its ceiling now, lifted a
    // notch every few quiet minutes), so two voices are still forwarded.
    gained(&mut w, 30, 0);
    let held = gained(&mut w, 3, 0);
    assert!(
        held[0] > 100 && held[1] > 100 && held[2] < 30 && held[3] < 30,
        "the two loudest are back: {held:?}"
    );
}
