//! Which datagrams a session takes as its own.

mod support;

use std::net::SocketAddr;
use std::time::Duration;

use bytes::BytesMut;
use rtcd_sfu::engine::Proto;
use support::World;

/// A STUN binding request whose username names `ufrag` and that carries no MESSAGE-INTEGRITY,
/// as anyone who saw a session's username fragment can send.
fn forged_check(ufrag: &str) -> BytesMut {
    let username = format!("{ufrag}:attacker");
    let mut attrs = Vec::new();
    attrs.extend_from_slice(&0x0006u16.to_be_bytes());
    attrs.extend_from_slice(&(username.len() as u16).to_be_bytes());
    attrs.extend_from_slice(username.as_bytes());
    attrs.resize(attrs.len().next_multiple_of(4), 0);
    let mut msg = vec![0x00, 0x01];
    msg.extend_from_slice(&(attrs.len() as u16).to_be_bytes());
    msg.extend_from_slice(&[0x21, 0x12, 0xa4, 0x42]);
    msg.extend_from_slice(&[7; 12]);
    msg.extend_from_slice(&attrs);
    BytesMut::from(&msg[..])
}

fn ufrag_of(answer: &str) -> String {
    answer
        .lines()
        .find_map(|l| l.strip_prefix("a=ice-ufrag:"))
        .expect("an ice-ufrag in the answer")
        .trim()
        .to_owned()
}

#[test]
fn a_check_that_only_knows_the_username_fragment_does_not_take_the_address_over() {
    let mut w = World::new();
    let a = w.join("alice", "call1");
    let b = w.join("bob", "call1");
    let ufrag = ufrag_of(&w.browsers[a].pc.remote_description().unwrap().sdp);
    let alice: SocketAddr = "10.1.0.1:5000".parse().unwrap();
    assert!(w.engine.would_route(Proto::Udp, alice, &[]));

    // More forged sources than a session remembers addresses for.
    for port in 0..20u16 {
        let attacker: SocketAddr = format!("10.66.6.6:{}", 4000 + port).parse().unwrap();
        let taken =
            w.engine
                .handle_datagram(w.now, Proto::Udp, w.server, attacker, forged_check(&ufrag));
        assert!(!taken, "a check without proof is nobody's");
        assert!(!w.engine.would_route(Proto::Udp, attacker, &[]));
    }

    assert!(
        w.engine.would_route(Proto::Udp, alice, &[]),
        "the real address was not pushed out"
    );
    w.speak(a, 50, |f| if f % 2 == 0 { 20 } else { 70 });
    w.run_for(Duration::from_millis(200));
    assert!(w.browsers[b].received_on("voice_") > 40);
}
