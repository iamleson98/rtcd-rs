//! A deployment behind NAT: the SFU listens on a private address and offers a public one.

mod support;

use std::time::Duration;

use rtcd_sfu::config::{EngineConfig, LocalAddress};
use support::World;

fn behind_nat() -> EngineConfig {
    EngineConfig {
        addresses: vec![LocalAddress {
            bind: "10.0.0.1".parse().unwrap(),
            public: Some("203.0.113.9".parse().unwrap()),
            udp_port: 8443,
            advertised_udp_port: Some(30443),
            tcp_port: None,
            advertised_tcp_port: None,
            advertise: true,
        }],
        ..Default::default()
    }
}

#[test]
fn browsers_are_offered_the_public_address_and_connect_through_it() {
    let mut w = World::with_config(behind_nat());
    let a = w.join("alice", "call1");
    let answer = w.browsers[a].pc.remote_description().unwrap().sdp.clone();
    assert!(answer.contains("203.0.113.9 30443 typ host"), "{answer}");
    assert!(
        !answer.contains("10.0.0.1"),
        "the private address is not offered: {answer}"
    );
    assert!(w.browsers[a].connected);
}

#[test]
fn media_flows_through_the_public_address() {
    let mut w = World::with_config(behind_nat());
    let a = w.join("alice", "call1");
    let b = w.join("bob", "call1");
    w.speak(a, 50, |f| if f % 2 == 0 { 20 } else { 70 });
    w.run_for(Duration::from_millis(200));
    assert!(w.browsers[b].received_on("voice_") > 40);
}

#[test]
fn an_address_that_is_listened_on_but_not_offered_is_not_a_candidate() {
    let mut cfg = behind_nat();
    let mut hidden = cfg.addresses[0].clone();
    hidden.bind = "10.0.0.2".parse().unwrap();
    hidden.advertise = false;
    cfg.addresses.push(hidden);
    let mut w = World::with_config(cfg);
    let a = w.join("alice", "call1");
    let answer = w.browsers[a].pc.remote_description().unwrap().sdp.clone();
    assert_eq!(
        answer.matches("typ host").count(),
        2,
        "one candidate (and its rtcp twin): {answer}"
    );
}
