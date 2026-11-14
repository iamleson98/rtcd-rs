//! A session whose ICE check comes from an address another session holds.

mod support;

use std::net::SocketAddr;
use std::time::Duration;

use rtcd_sfu::engine::Proto;
use support::World;

fn talk(w: &mut World, i: usize) {
    w.speak(i, 50, |f| if f % 2 == 0 { 20 } else { 70 });
}

#[test]
fn a_session_of_the_same_user_takes_over_the_address_of_its_old_one() {
    let mut w = World::new();
    let held: SocketAddr = "10.1.0.2:5000".parse().unwrap();
    let alice = w.join("alice", "call1");
    // The SFU learns the address the next browser will come from (as alice's), and then alice
    // moves on from it without the SFU being told.
    w.browsers[alice].mapped = held;
    w.run_for(Duration::from_secs(10));
    w.browsers[alice].mapped = "10.1.0.1:6000".parse().unwrap();
    w.run_for(Duration::from_secs(10));

    // Her rejoin's first check comes from the address the SFU still has for the old session.
    let rejoin = w.start_as("alice-again", "user-alice", "call1", false);
    assert_eq!(w.browsers[rejoin].mapped, held);
    assert!(
        w.run_until(Duration::from_secs(10), |w| w.browsers[rejoin].connected
            && w.browsers[rejoin].dc.is_some()),
        "the rejoin never connected"
    );

    let (heard_by_alice, heard_by_rejoin) = (
        w.browsers[alice].received_on("voice_"),
        w.browsers[rejoin].received_on("voice_"),
    );
    talk(&mut w, alice);
    talk(&mut w, rejoin);
    assert!(w.browsers[rejoin].received_on("voice_") > heard_by_rejoin + 40);
    assert!(w.browsers[alice].received_on("voice_") > heard_by_alice + 40);
    assert_eq!(w.engine.session_count(), 2);
    assert!(w.closed.is_empty(), "{:?}", w.closed);
}

#[test]
fn another_user_cannot_take_an_address_by_checking_from_it() {
    let mut w = World::new();
    let alice = w.join("alice", "call1");
    let carol = w.join("carol", "call1");
    talk(&mut w, alice);
    talk(&mut w, carol);
    let before = w.browsers[carol].received_on("voice_session-alice");

    // Mallory knows the password of her own session, so she can sign a check naming it that
    // comes, as far as the network shows, from alice's address.
    let mallory = w.start("mallory", "call1");
    w.browsers[mallory].mapped = w.browsers[alice].mapped;
    // Alice speaks all the while, and every packet of hers must get through.
    const FRAMES: usize = 250;
    w.speak(alice, FRAMES, |_| 20);
    w.run_for(Duration::from_secs(1));

    assert!(
        !w.browsers[mallory].connected,
        "mallory took alice's address"
    );
    let heard = w.browsers[carol].received_on("voice_session-alice") - before;
    assert!(
        heard >= FRAMES,
        "{heard} of alice's {FRAMES} packets reached the SFU"
    );
    assert!(w.browsers[alice].received_on("voice_session-carol") > 40);
    assert!(
        w.closed.iter().all(|(id, _)| id != "session-alice"),
        "{:?}",
        w.closed
    );
}

#[test]
fn an_address_the_directory_took_away_is_no_longer_routed() {
    let mut w = World::new();
    let alice = w.join("alice", "call1");
    let peer = w.browsers[alice].mapped;
    let media = [0x80; 40];
    assert!(w.engine.would_route(Proto::Udp, peer, &media));

    w.engine.forget_address(Proto::Udp, peer);
    assert!(!w.engine.would_route(Proto::Udp, peer, &media));
}
