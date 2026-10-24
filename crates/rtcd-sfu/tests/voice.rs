//! Audio calls: joining, hearing each other, muting, voice activity and leaving.

mod support;

use std::time::Duration;

use rtcd_proto::MessageType;
use rtcd_proto::dc::DcMessage;
use rtcd_sfu::engine::CloseReason;
use rtcd_sfu::ids::{TrackType, parse_track_id};
use support::World;

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
