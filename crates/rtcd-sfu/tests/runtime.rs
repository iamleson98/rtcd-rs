//! The SFU on real sockets: browsers (real `rtc` peers) connect over loopback UDP to the running
//! `Sfu` and its shard thread.

mod live;

use std::time::Duration;

use live::Live;
use rtcd_proto::MessageType;
use rtcd_sfu::engine::{CloseReason, Event};
use rtcd_sfu::runtime::ScalingPolicy;

#[test]
fn two_browsers_connect_and_hear_each_other_over_real_sockets() {
    let mut live = Live::start(1);
    let a = live.join("alice", "call1");
    let b = live.join("bob", "call1");
    assert_eq!(live.sfu.session_count(), 2);

    live.speak(a, 60);
    live.speak(b, 60);
    live.run(Duration::from_millis(500));

    assert!(
        live.browsers[b].received("voice_session-alice") > 40,
        "bob heard {}",
        live.browsers[b].received("voice_")
    );
    assert!(
        live.browsers[a].received("voice_session-bob") > 40,
        "alice heard {}",
        live.browsers[a].received("voice_")
    );
}

/// Calls live on different shards, but the kernel hands each datagram to whichever shard owns the
/// sender's socket hash: the shards must pass on what is not theirs.
#[test]
fn calls_on_several_shards_connect_and_hear_only_their_own_call() {
    let mut live = Live::start(4);
    let mut pairs = Vec::new();
    for c in 0..8 {
        let a = live.join(&format!("a{c}"), &format!("call{c}"));
        let b = live.join(&format!("b{c}"), &format!("call{c}"));
        pairs.push((a, b));
    }
    assert_eq!(live.sfu.session_count(), 16);

    for &(a, b) in &pairs {
        live.speak(a, 30);
        live.speak(b, 30);
    }
    live.run(Duration::from_millis(500));

    for (c, &(a, b)) in pairs.iter().enumerate() {
        assert!(
            live.browsers[b].received(&format!("voice_session-a{c}")) > 15,
            "b{c} heard {} from a{c}",
            live.browsers[b].received("voice_")
        );
        assert!(
            live.browsers[a].received(&format!("voice_session-b{c}")) > 15,
            "a{c} heard {} from b{c}",
            live.browsers[a].received("voice_")
        );
        // Nobody hears another call.
        assert_eq!(
            live.browsers[a].received("voice_"),
            live.browsers[a].received(&format!("voice_session-b{c}")),
            "a{c} heard someone outside the call"
        );
    }
}

/// Once a shard knows a participant's address it opens a socket connected to it, and the kernel
/// delivers that participant's datagrams there: the detour through another shard is for the first
/// few packets only.
#[test]
fn datagrams_stop_taking_a_detour_once_the_participant_has_a_socket_of_its_own() {
    use std::sync::atomic::Ordering::Relaxed;
    let mut live = Live::start(4);
    let mut pairs = Vec::new();
    for c in 0..8 {
        let a = live.join(&format!("a{c}"), &format!("call{c}"));
        let b = live.join(&format!("b{c}"), &format!("call{c}"));
        pairs.push((a, b));
    }
    live.run(Duration::from_millis(300));
    let before = (
        live.traffic.packets_in.load(Relaxed),
        live.traffic.forwarded.load(Relaxed),
    );
    for &(a, b) in &pairs {
        live.speak(a, 40);
        live.speak(b, 40);
    }
    live.run(Duration::from_millis(300));
    let packets = live.traffic.packets_in.load(Relaxed) - before.0;
    let forwarded = live.traffic.forwarded.load(Relaxed) - before.1;
    assert!(packets > 400, "only {packets} datagrams arrived");
    assert!(
        forwarded * 20 < packets,
        "{forwarded} of {packets} datagrams still went through another shard"
    );
}

#[test]
fn a_session_can_leave_and_a_new_one_take_its_place_on_several_shards() {
    let mut live = Live::start(3);
    let a = live.join("alice", "call1");
    let b = live.join("bob", "call1");
    live.speak(a, 20);
    live.run(Duration::from_millis(200));
    assert!(live.browsers[b].received("voice_session-alice") > 5);

    live.sfu.leave("session-bob");
    assert!(live.run_until(Duration::from_secs(5), |l| l.sfu.session_count() == 1));
    let c = live.join("carol", "call1");
    live.speak(c, 30);
    live.speak(a, 30);
    live.run(Duration::from_millis(400));
    assert!(live.browsers[a].received("voice_session-carol") > 15);
    assert!(live.browsers[c].received("voice_session-alice") > 15);
}

#[test]
fn the_http_lookups_see_sessions_by_group_call_and_id() {
    let mut live = Live::start(1);
    live.join("alice", "call1");
    live.join("bob", "call1");
    live.join("carol", "call2");
    let cfg = live
        .sfu
        .session("group", "call1", "session-alice")
        .expect("alice is in call1");
    assert_eq!(cfg.user_id, "user-alice");
    assert!(
        live.sfu
            .session("group", "call2", "session-alice")
            .is_none(),
        "wrong call"
    );
    assert!(
        live.sfu
            .session("other", "call1", "session-alice")
            .is_none(),
        "wrong group"
    );
    assert_eq!(live.sfu.call_sessions("group", "call1").len(), 2);
    assert_eq!(live.sfu.call_sessions("group", "call2").len(), 1);
    assert!(live.sfu.call_sessions("group", "nothing").is_empty());
}

#[test]
fn leaving_ends_the_session_and_reports_it() {
    let mut live = Live::start(1);
    live.join("alice", "call1");
    live.sfu.leave("session-alice");
    live.run_until(Duration::from_secs(3), |l| {
        l.closed.iter().any(|(id, _)| id == "session-alice")
    });
    assert!(
        live.closed
            .contains(&("session-alice".to_owned(), CloseReason::Left))
    );
    assert_eq!(live.sfu.session_count(), 0);
    assert!(
        live.sfu
            .session("group", "call1", "session-alice")
            .is_none()
    );
}

#[test]
fn messages_for_a_session_that_does_not_exist_are_refused() {
    let live = Live::start(1);
    let msg = rtcd_proto::Message {
        group_id: "g".into(),
        user_id: "u".into(),
        session_id: "ghost".into(),
        call_id: "c".into(),
        kind: MessageType::Mute,
        data: vec![],
    };
    assert!(live.sfu.signal(msg).is_err());
}

#[test]
fn a_duplicate_session_id_is_refused_and_the_first_is_untouched() {
    let mut live = Live::start(1);
    live.join("alice", "call1");
    let cfg = live.sfu.session("group", "call1", "session-alice").unwrap();
    assert!(live.sfu.join(cfg).is_err());
    live.run(Duration::from_millis(200));
    assert_eq!(live.sfu.session_count(), 1);
    assert!(live.closed.is_empty());
}

#[test]
fn shutdown_waits_for_nobody_when_the_drain_is_zero_and_ends_everyone() {
    let mut live = Live::start(1);
    live.join("alice", "call1");
    live.join("bob", "call1");
    live.sfu.shutdown(Duration::ZERO);
    live.drain_events();
    assert_eq!(live.sfu.session_count(), 0);
    assert_eq!(
        live.closed
            .iter()
            .filter(|(_, r)| *r == CloseReason::Shutdown)
            .count(),
        2
    );
    // And nothing new is admitted afterwards.
    let cfg = rtcd_proto::SessionConfig {
        group_id: "g".into(),
        call_id: "c".into(),
        user_id: "u".into(),
        session_id: "late".into(),
        ..Default::default()
    };
    assert!(live.sfu.join(cfg).is_err());
}

#[test]
fn shutdown_gives_running_sessions_time_to_end_on_their_own() {
    let mut live = Live::start(1);
    live.join("alice", "call1");
    let sfu = live.sfu.clone();
    let handle = std::thread::spawn(move || {
        let start = std::time::Instant::now();
        sfu.shutdown(Duration::from_secs(10));
        start.elapsed()
    });
    std::thread::sleep(Duration::from_millis(300));
    live.sfu.leave("session-alice");
    let took = handle.join().unwrap();
    assert!(
        took < Duration::from_secs(5),
        "the drain ended when the last session did: {took:?}"
    );
    live.drain_events();
    assert!(
        live.closed
            .contains(&("session-alice".to_owned(), CloseReason::Left))
    );
}

#[allow(dead_code)]
fn _unused(_: Event) {}

/// The number of shards can change while calls run: the calls already there are not disturbed (the
/// kernel hands their datagrams to other sockets once the new one joins the port, and the shards
/// pass them on), and the new shard takes the next calls.
#[test]
fn a_shard_added_while_calls_run_takes_the_next_calls_and_leaves_the_running_ones_alone() {
    let mut live = Live::start_up_to(1, 3, ScalingPolicy::default());
    assert_eq!(live.sfu.shard_count(), 1);
    let a = live.join("alice", "call1");
    let b = live.join("bob", "call1");
    live.speak(a, 20);
    live.run(Duration::from_millis(200));
    let heard = live.browsers[b].received("voice_session-alice");
    assert!(heard > 5, "bob heard {heard}");

    assert_eq!(live.sfu.add_shard().expect("room for another"), 1);
    assert_eq!(live.sfu.shard_count(), 2);

    live.speak(a, 40);
    live.run(Duration::from_millis(300));
    assert!(
        live.browsers[b].received("voice_session-alice") > heard + 20,
        "the running call stopped when the shard was added"
    );

    // The empty shard takes the next call.
    let c = live.join("carol", "call2");
    let d = live.join("dave", "call2");
    live.speak(c, 30);
    live.speak(d, 30);
    live.run(Duration::from_millis(300));
    assert!(live.browsers[d].received("voice_session-carol") > 15);
    assert!(live.browsers[c].received("voice_session-dave") > 15);
    assert_eq!(
        live.browsers[d].received("voice_"),
        live.browsers[d].received("voice_session-carol"),
        "dave heard someone outside his call"
    );

    // It has sessions, so it stays; once they are gone it can be stopped, the other never.
    assert!(!live.sfu.retire_shard(1));
    live.sfu.leave("session-carol");
    live.sfu.leave("session-dave");
    assert!(live.run_until(Duration::from_secs(5), |l| l.sfu.session_count() == 2));
    assert!(live.sfu.retire_shard(1));
    assert_eq!(live.sfu.shard_count(), 1);
    assert!(!live.sfu.retire_shard(0), "the last shard stays");

    // The calls go on, new ones are served, and a shard can be added again.
    let e = live.join("erin", "call3");
    let f = live.join("frank", "call3");
    live.speak(e, 30);
    live.speak(f, 30);
    live.speak(a, 30);
    live.run(Duration::from_millis(300));
    assert!(live.browsers[f].received("voice_session-erin") > 15);
    assert!(live.browsers[b].received("voice_session-alice") > heard + 40);
    assert_eq!(live.sfu.add_shard().expect("room again"), 1);
}

#[test]
fn a_fixed_number_of_shards_does_not_grow() {
    let live = Live::start(2);
    assert!(live.sfu.add_shard().is_err());
    assert_eq!(live.sfu.shard_count(), 2);

    let capped = Live::start_up_to(1, 2, ScalingPolicy::default());
    assert!(capped.sfu.add_shard().is_ok());
    assert!(capped.sfu.add_shard().is_err(), "two is the most");
}

/// With every shard counted as hot the SFU adds shards by itself up to its limit, one after the
/// other, and the calls it has keep working through it.
#[test]
fn shards_are_added_on_their_own_when_all_are_busy() {
    let scaling = ScalingPolicy {
        tick: Duration::from_millis(40),
        hot: 0.0,
        hot_ticks: 2,
        empty_for: Duration::from_secs(3600),
        sessions_per_shard: 0,
        expected_call_size: 0,
        min_shards: 1,
    };
    let mut live = Live::start_up_to(1, 3, scaling);
    let a = live.join("alice", "call1");
    let b = live.join("bob", "call1");
    assert!(
        live.run_until(Duration::from_secs(5), |l| l.sfu.shard_count() == 3),
        "{} shards",
        live.sfu.shard_count()
    );
    live.speak(a, 30);
    live.run(Duration::from_millis(300));
    assert!(live.browsers[b].received("voice_session-alice") > 15);
    live.run(Duration::from_millis(400));
    assert_eq!(live.sfu.shard_count(), 3, "it went past its limit");
}

/// A shard that has had nothing to do for long enough is stopped by the SFU itself, and calls
/// that start afterwards are served.
#[test]
fn an_empty_shard_is_stopped_on_its_own_after_a_while() {
    let scaling = ScalingPolicy {
        tick: Duration::from_millis(40),
        hot: 0.5,
        hot_ticks: 3,
        empty_for: Duration::from_millis(300),
        sessions_per_shard: 0,
        expected_call_size: 0,
        min_shards: 1,
    };
    let mut live = Live::start_up_to(1, 3, scaling);
    let a = live.join("alice", "call1");
    let b = live.join("bob", "call1");
    live.sfu.add_shard().unwrap();
    live.sfu.add_shard().unwrap();
    assert_eq!(live.sfu.shard_count(), 3);
    assert!(
        live.run_until(Duration::from_secs(5), |l| l.sfu.shard_count() == 1),
        "{} shards",
        live.sfu.shard_count()
    );
    live.speak(a, 30);
    live.run(Duration::from_millis(300));
    assert!(live.browsers[b].received("voice_session-alice") > 15);
    let c = live.join("carol", "call2");
    let d = live.join("dave", "call2");
    live.speak(c, 30);
    live.run(Duration::from_millis(300));
    assert!(live.browsers[d].received("voice_session-carol") > 15);
}

/// Classes that start together cannot wait for a shard to be seen busy: every call is placed
/// before the load shows. A new call that finds the shards holding their share gets a shard of its
/// own, and the calls that are there stay where they are.
#[test]
fn a_call_that_finds_the_shards_full_gets_a_new_shard() {
    let scaling = ScalingPolicy {
        tick: Duration::from_secs(3600),
        sessions_per_shard: 4,
        expected_call_size: 0,
        ..ScalingPolicy::default()
    };
    let mut live = Live::start_up_to(1, 3, scaling);
    let a = live.join("alice", "call1");
    let b = live.join("bob", "call1");
    live.join("carol", "call2");
    live.join("dave", "call2");
    assert_eq!(
        live.sfu.shard_count(),
        1,
        "four sessions are the shard's share"
    );

    // The fifth session starts a call, and the shard is full: it gets its own.
    let e = live.join("erin", "call3");
    let f = live.join("frank", "call3");
    assert_eq!(live.sfu.shard_count(), 2);
    // A session of a call already placed does not start another.
    live.join("gina", "call3");
    live.join("hugo", "call3");
    live.join("iris", "call3");
    assert_eq!(live.sfu.shard_count(), 2, "call3 is on the second shard");

    // The next call goes where there is room, the one after has none and takes the third; the
    // limit is three.
    live.join("jon", "call4");
    live.join("kim", "call4");
    live.join("lee", "call5");
    live.join("max", "call6");
    live.join("ned", "call7");
    assert_eq!(live.sfu.shard_count(), 3, "the most it may have");

    for (x, y) in [(a, b), (e, f)] {
        live.speak(x, 30);
        live.speak(y, 30);
    }
    live.run(Duration::from_millis(300));
    assert!(live.browsers[b].received("voice_session-alice") > 15);
    assert!(live.browsers[f].received("voice_session-erin") > 15);
}

/// A bell starts many classes at once, each with its first participant: the shards are chosen by
/// what the calls will bring, so they are spread (and shards started) before the participants have
/// arrived.
#[test]
fn calls_that_start_together_are_spread_before_their_participants_arrive() {
    let scaling = ScalingPolicy {
        tick: Duration::from_secs(3600),
        sessions_per_shard: 40,
        expected_call_size: 20,
        ..ScalingPolicy::default()
    };
    let mut live = Live::start_up_to(1, 3, scaling);
    live.join("a1", "call1");
    live.join("b1", "call2");
    assert_eq!(
        live.sfu.shard_count(),
        1,
        "two calls of twenty fill the share"
    );
    live.join("c1", "call3");
    assert_eq!(live.sfu.shard_count(), 2, "the third call does not fit");
    live.join("d1", "call4");
    live.join("e1", "call5");
    assert_eq!(live.sfu.shard_count(), 3, "and the fifth finds both full");
}
