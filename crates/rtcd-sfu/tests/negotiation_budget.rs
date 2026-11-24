//! A newcomer's microphone makes every other session of its call need an offer. The engine makes
//! them a few per turn, so the media already flowing is not held up behind all of them. The
//! harness calls the engine once per datagram, as the shard does, and a turn is everything it
//! takes in between two of the harness's timeouts.

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rtcd_sfu::engine::{MAX_OFFERS_PER_TURN, QUIET_OFFER_DELAY};
use rtcd_sfu::metrics::Metrics;
use support::{World, engine_config};

const LISTENERS: usize = 20;
/// Frames of 20 ms the teacher speaks for while the newcomer joins.
const FRAMES: u32 = 100;
/// Numbers the newcomer's frames apart from the teacher's (see `World::send_voice`).
const NEWCOMER: u32 = 1_000_000;

/// The tests share the counters below, so they take turns.
static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());
/// Offers made so far, counted where the engine takes the lock for one.
static OFFERS: AtomicUsize = AtomicUsize::new(0);
static OFFERS_AT_TURN: AtomicUsize = AtomicUsize::new(0);
/// The most offers one turn made.
static MOST_IN_A_TURN: AtomicUsize = AtomicUsize::new(0);

struct Offers;

impl Metrics for Offers {
    fn signaling_lock_grab_time(&self, _group: &str, _took: Duration) {
        OFFERS.fetch_add(1, Ordering::Relaxed);
    }
}

fn around(call: &'static str, _before: bool) {
    let offers = OFFERS.load(Ordering::Relaxed);
    MOST_IN_A_TURN.fetch_max(
        offers - OFFERS_AT_TURN.load(Ordering::Relaxed),
        Ordering::Relaxed,
    );
    if call == "turn" {
        OFFERS_AT_TURN.store(offers, Ordering::Relaxed);
    }
}

#[test]
fn a_newcomer_is_offered_to_a_class_a_few_sessions_per_turn() {
    let _alone = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let mut w = World::with_metrics(engine_config(), Arc::new(Offers));
    w.around_engine = around;
    let teacher = w.join("teacher", "call1");
    let listeners: Vec<usize> = (0..LISTENERS)
        .map(|i| w.join(&format!("listener{i}"), "call1"))
        .collect();
    w.speak(teacher, 50, |_| 20);
    for &l in &listeners {
        assert!(
            w.run_until(Duration::from_secs(10), |w| w.browsers[l].tracks.len() == 1),
            "{} never got the teacher's voice",
            w.browsers[l].name
        );
    }
    w.run_for(QUIET_OFFER_DELAY + Duration::from_secs(2));

    // The newcomer's silent stream waits for the window, but its first words are offered at once to
    // every listener, whose streams for it are not in any offer yet.
    let newcomer = w.start("newcomer", "call1");
    assert!(
        w.run_until(Duration::from_secs(20), |w| w.browsers[newcomer].connected
            && w.browsers[newcomer].dc.is_some()),
        "the newcomer never connected"
    );
    MOST_IN_A_TURN.store(0, Ordering::Relaxed);
    let mut sent: Vec<(u32, Instant)> = Vec::new();
    for n in 0..FRAMES {
        sent.push((n, w.now));
        w.send_voice(teacher, n);
        w.send_voice(newcomer, NEWCOMER + n);
        w.run_for(Duration::from_millis(20));
    }
    let most = MOST_IN_A_TURN.load(Ordering::Relaxed);
    assert!(
        (1..=MAX_OFFERS_PER_TURN).contains(&most),
        "one turn made {most} offers"
    );
    for &l in &listeners {
        let b = &w.browsers[l];
        for &(n, at) in &sent {
            assert_eq!(
                b.voice_heard.get(&n),
                Some(&at),
                "{} did not hear the teacher's frame {n} when it was sent",
                b.name
            );
        }
        assert!(
            b.received_on("voice_session-newcomer") > 0,
            "{} never heard the newcomer",
            b.name
        );
    }
}

#[test]
fn newcomers_in_many_calls_in_one_turn_are_offered_a_few_at_a_time() {
    let _alone = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    const CALLS: usize = 6;
    let mut w = World::with_metrics(engine_config(), Arc::new(Offers));
    w.around_engine = around;
    let mut listeners = Vec::new();
    for c in 0..CALLS {
        let call = format!("call{c}");
        listeners.push(w.join(&format!("first{c}"), &call));
        listeners.push(w.join(&format!("second{c}"), &call));
    }
    w.run_for(QUIET_OFFER_DELAY + Duration::from_secs(2));
    MOST_IN_A_TURN.store(0, Ordering::Relaxed);

    // Every call's newcomer reaches the engine in the same turn: 12 offers are wanted, each call's
    // two listeners needing theirs for its microphone.
    let newcomers: Vec<usize> = (0..CALLS)
        .map(|c| w.start(&format!("speaker{c}"), &format!("call{c}")))
        .collect();
    assert!(
        w.run_until(Duration::from_secs(20), |w| newcomers
            .iter()
            .all(|&n| w.browsers[n].connected && w.browsers[n].dc.is_some())),
        "the newcomers never connected"
    );
    for _ in 0..100 {
        for &n in &newcomers {
            w.send_audio(n, 20);
        }
        w.run_for(Duration::from_millis(20));
    }
    for &l in &listeners {
        assert_eq!(
            w.browsers[l].tracks.len(),
            1,
            "{} was not offered its speaker",
            w.browsers[l].name
        );
    }
    let most = MOST_IN_A_TURN.load(Ordering::Relaxed);
    assert!(
        (1..=MAX_OFFERS_PER_TURN).contains(&most),
        "one turn made {most} offers"
    );
}
