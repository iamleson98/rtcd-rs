//! A participant that answers the SFU's offer late is not given up on: the SFU keeps the offer and
//! its hold on the signaling lock while the connection lives, applies the answer whenever it comes,
//! and yields to an offer of the participant's that crosses its own.

mod support;

use std::time::Duration;

use rtcd_proto::dc::DcMessage;
use rtcd_sfu::engine::CloseReason;
use support::{Layer, World};

/// Alice is connected and ignores offers; Bob joins with a camera and streams, which makes the SFU
/// offer his tracks to Alice. Returns Alice's and Bob's indexes, once the offer has reached Alice.
fn alice_ignores_the_offer(w: &mut World) -> (usize, usize) {
    let a = w.join("alice", "call1");
    w.browsers[a].hold_answers = true;
    let b = w.join_with_camera("bob", "call1");
    w.stream_video(b, false, 0.5, 30, &[Layer::Low, Layer::High]);
    assert!(
        w.run_until(Duration::from_secs(5), |w| w.browsers[a].offers_received
            >= 1),
        "no offer reached Alice"
    );
    (a, b)
}

/// What closed once Bob had left: him, and nobody else.
fn only_bob_left() -> Vec<(String, CloseReason)> {
    vec![("session-bob".to_owned(), CloseReason::Left)]
}

/// The media sections of a description.
fn sections(sdp: &str) -> Vec<&str> {
    sdp.split("\r\nm=").skip(1).collect()
}

/// The SSRCs and `a=msid` values a media section names.
fn names(section: &str) -> Vec<String> {
    section
        .split("\r\n")
        .filter_map(|l| {
            l.strip_prefix("a=ssrc:")
                .map(|r| format!("ssrc {}", r.split(' ').next().unwrap()))
                .or_else(|| l.strip_prefix("a=msid:").map(|r| format!("msid {r}")))
        })
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// A withdrawn offer must come back as it was: the same sections, and no stream in two of them.
fn assert_offered_again_as_before(first: &str, again: &str) {
    assert_eq!(sections(first).len(), sections(again).len(), "{again}");
    let mut seen = std::collections::HashMap::new();
    for (i, section) in sections(again).into_iter().enumerate() {
        for name in names(section) {
            assert_eq!(
                seen.insert(name.clone(), i),
                None,
                "{name} is in two sections:\n{again}"
            );
        }
    }
}

/// Bob leaves; the offer that takes his tracks away has no section still sending them.
fn assert_no_ghost_after_leaving(w: &mut World, a: usize, b: usize, first: &str) {
    let before = w.browsers[a].offers.len();
    w.leave(b);
    assert!(
        w.run_until(Duration::from_secs(5), |w| w.browsers[a].offers.len()
            > before),
        "no offer took Bob's tracks away"
    );
    let last = &w.browsers[a].offers.last().unwrap().1;
    let his: Vec<String> = sections(first)
        .into_iter()
        .filter(|s| s.contains("a=ssrc:"))
        .flat_map(names)
        .collect();
    assert!(!his.is_empty());
    for section in sections(last) {
        assert!(
            names(section).iter().all(|n| !his.contains(n)),
            "a ghost of Bob's is still offered:\n{last}"
        );
    }
}

#[test]
fn an_offer_nobody_answers_is_waited_for_and_the_session_stays() {
    let mut w = World::new();
    let (a, b) = alice_ignores_the_offer(&mut w);

    // Past the first wait, and well into the longest ones: the SFU still holds the lock, so the
    // participant is not given it, and the reliable channel's offer is not sent a second time.
    w.run_for(Duration::from_secs(12));
    assert!(!w.request_lock(a), "the SFU holds the lock after a timeout");
    w.run_for(Duration::from_secs(120));
    assert!(!w.request_lock(a), "and still does");
    assert!(w.closed.is_empty(), "{:?}", w.closed);
    assert_eq!(w.engine.session_count(), 2);
    assert_eq!(w.browsers[a].offers.len(), 1, "one offer, not sent again");
    assert!(w.browsers[b].keyframe_requests.is_empty(), "not live yet");

    // The answer comes after all: it is applied, and Bob's video starts at a keyframe.
    w.answer_late(a);
    assert!(
        w.run_until(Duration::from_secs(2), |w| !w.browsers[b]
            .keyframe_requests
            .is_empty()),
        "Bob was not asked for a keyframe"
    );
    w.speak(b, 50, |_| 40);
    assert!(
        !w.browsers[a].rtp.is_empty(),
        "Bob's microphone reaches Alice"
    );
    assert!(w.request_lock(a), "the answer released the lock");
    assert!(w.closed.is_empty(), "{:?}", w.closed);
}

#[test]
fn a_second_late_answer_is_ignored() {
    let mut w = World::new();
    let (a, b) = alice_ignores_the_offer(&mut w);
    w.run_for(Duration::from_secs(11));

    w.answer_late(a);
    let keyframes = w.browsers[b].keyframe_requests.len();
    assert!(keyframes > 0);
    let answer = w.browsers[a].pc.local_description().unwrap();
    let json = serde_json::to_vec(&answer).unwrap();
    w.dc_send(a, DcMessage::Sdp(json));
    w.run_for(Duration::from_secs(1));

    assert_eq!(w.browsers[b].keyframe_requests.len(), keyframes);
    assert!(w.closed.is_empty(), "{:?}", w.closed);
    w.run_for(Duration::from_secs(200));
    assert_eq!(w.browsers[a].offers.len(), 1, "nothing is offered again");
    assert!(w.closed.is_empty(), "{:?}", w.closed);
}

#[test]
fn changes_made_while_the_offer_waits_go_into_the_next_one() {
    let mut w = World::new();
    let (a, b) = alice_ignores_the_offer(&mut w);
    let c = w.join("carol", "call1");
    w.speak(c, 50, |_| 40);
    w.run_for(Duration::from_secs(15));
    let before = w.browsers[a].offers.len();

    // From here Alice answers every offer: the late one, then the one with Carol in it.
    w.answer_late(a);
    assert!(
        w.run_until(Duration::from_secs(5), |w| w.browsers[a].offers.len()
            > before),
        "no offer followed the answer"
    );
    w.speak(c, 50, |_| 40);
    w.speak(b, 50, |_| 40);
    let tracks = w.browsers[a].track_ids();
    for who in ["session-bob", "session-carol"] {
        assert!(
            tracks.iter().any(|t| t.contains(who)),
            "no voice of {who} reached Alice: {tracks:?}"
        );
    }
    w.run_for(Duration::from_secs(200));
    assert_eq!(w.browsers[a].signaling_conflicts, 0);
    assert!(w.closed.is_empty(), "{:?}", w.closed);
}

#[test]
fn an_offer_of_the_participants_crossing_the_sfus_is_answered() {
    let mut w = World::new();
    let (a, b) = alice_ignores_the_offer(&mut w);
    w.run_for(Duration::from_secs(12));

    // Alice gave up on the lock and offers anyway, and answers what the SFU offers afterwards.
    w.browsers[a].hold_answers = false;
    let before = w.browsers[a].offers.len();
    w.offer_without_lock(a);
    assert!(
        w.run_until(Duration::from_secs(5), |w| w.browsers[a].offers.len()
            > before),
        "the SFU never offered what its withdrawn offer carried"
    );
    w.speak(b, 50, |_| 40);
    assert!(
        w.browsers[a]
            .track_ids()
            .iter()
            .any(|t| t.contains("session-bob")),
        "Bob's voice reaches Alice by the second offer"
    );
    assert_eq!(w.browsers[a].signaling_conflicts, 0);
    let (first, again) = (
        w.browsers[a].offers[0].1.clone(),
        w.browsers[a].offers[before].1.clone(),
    );
    assert_offered_again_as_before(&first, &again);
    assert_no_ghost_after_leaving(&mut w, a, b, &first);
    w.run_for(Duration::from_secs(200));
    assert_eq!(w.closed, only_bob_left());
}

#[test]
fn an_offer_is_sent_by_the_websocket_when_its_channel_is_gone() {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    w.close_data_channel(a);
    let b = w.join_with_camera("bob", "call1");
    w.stream_video(b, false, 0.5, 30, &[Layer::Low, Layer::High]);
    assert_eq!(w.browsers[a].offers.len(), 0, "the data channel is shut");

    assert!(
        w.run_until(Duration::from_secs(12), |w| w.browsers[a].offers.len() == 1),
        "the offer never came by the websocket"
    );
    w.run_for(Duration::from_secs(60));
    assert_eq!(w.browsers[a].offers.len(), 1, "once, not twice");
    assert!(
        !w.browsers[b].keyframe_requests.is_empty(),
        "the answer was applied"
    );
    assert!(w.closed.is_empty(), "{:?}", w.closed);
}

#[test]
fn an_offer_unanswered_for_good_is_given_up_on_and_the_session_rejoins_by_dropping() {
    let mut w = World::new();
    let (a, _) = alice_ignores_the_offer(&mut w);

    // 10 + 20 + 40 + 60 + 60 seconds, then the offer is withdrawn and made again.
    w.run_for(Duration::from_secs(185));
    assert_eq!(w.browsers[a].offers.len(), 1);
    w.run_for(Duration::from_secs(10));
    assert_eq!(
        w.browsers[a].offers.len(),
        2,
        "offered again after giving up"
    );
    assert!(w.closed.is_empty(), "{:?}", w.closed);
    let (first, again) = (
        w.browsers[a].offers[0].1.clone(),
        w.browsers[a].offers[1].1.clone(),
    );
    assert_offered_again_as_before(&first, &again);

    // Signaling that never recovers ends the session after three of these.
    w.run_for(Duration::from_secs(400));
    assert_eq!(
        w.closed,
        vec![("session-alice".to_owned(), CloseReason::SignalingFailed)]
    );
}

#[test]
fn an_offer_given_up_on_leaves_no_ghost_when_the_stream_ends() {
    let mut w = World::new();
    let (a, b) = alice_ignores_the_offer(&mut w);
    w.run_for(Duration::from_secs(195));
    assert_eq!(w.browsers[a].offers.len(), 2);

    w.answer_late(a);
    w.run_for(Duration::from_secs(1));
    let first = w.browsers[a].offers[0].1.clone();
    assert_no_ghost_after_leaving(&mut w, a, b, &first);
    assert_eq!(w.closed, only_bob_left());
}

#[test]
fn a_participant_that_is_gone_is_dropped_by_its_connection_not_the_offer() {
    let mut w = World::new();
    let (a, _) = alice_ignores_the_offer(&mut w);
    w.blackhole_browser(a);
    w.run_for(Duration::from_secs(40));
    assert_eq!(
        w.closed,
        vec![("session-alice".to_owned(), CloseReason::ConnectionLost)]
    );
}
