//! A participant whose NAT gives it another port in the middle of a call.

mod support;

use std::time::Duration;

use support::World;

fn talk(w: &mut World, i: usize) {
    w.speak(i, 50, |f| if f % 2 == 0 { 20 } else { 70 });
}

#[test]
fn media_follows_a_participant_whose_nat_mapping_changes() {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    let b = w.join("bob", "call1");
    talk(&mut w, a);
    talk(&mut w, b);
    assert!(w.browsers[b].received_on("voice_") > 40);

    w.rebind(a, 6000);
    // Long enough for the old address to look dead if the SFU kept waiting on it.
    for _ in 0..4 {
        talk(&mut w, a);
        talk(&mut w, b);
        w.run_for(Duration::from_secs(3));
    }
    let (heard_by_bob, heard_by_alice) = (
        w.browsers[b].received_on("voice_"),
        w.browsers[a].received_on("voice_"),
    );
    talk(&mut w, a);
    talk(&mut w, b);

    assert!(w.browsers[b].received_on("voice_") > heard_by_bob + 40);
    assert!(w.browsers[a].received_on("voice_") > heard_by_alice + 40);
    assert_eq!(w.engine.session_count(), 2);
    assert!(w.closed.is_empty(), "{:?}", w.closed);
}
