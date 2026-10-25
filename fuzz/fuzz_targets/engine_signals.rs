//! The SFU engine given a session and then whatever a participant's client can send through the
//! server: SDP and ICE of any shape, stream announcements, subscriptions, mutes, in any order.
#![no_main]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use rtcd_proto::{Message, MessageType, SessionConfig};
use rtcd_sfu::config::{EngineConfig, LocalAddress};
use rtcd_sfu::engine::{Command, Engine};
use rtcd_sfu::ids::Rng;
use rtcd_sfu::metrics::Metrics;

struct Nop;
impl Metrics for Nop {}

#[derive(Arbitrary, Debug)]
struct Input {
    steps: Vec<(u8, u8, Vec<u8>)>,
}

fn kind(n: u8) -> MessageType {
    [
        MessageType::Ice,
        MessageType::Sdp,
        MessageType::Mute,
        MessageType::Unmute,
        MessageType::ScreenOn,
        MessageType::ScreenOff,
        MessageType::VideoOn,
        MessageType::VideoOff,
        MessageType::VideoSubscribe,
    ][n as usize % 9]
}

fuzz_target!(|input: Input| {
    let cfg = EngineConfig {
        addresses: vec![LocalAddress {
            bind: "10.0.0.1".parse().unwrap(),
            public: None,
            udp_port: 8443,
            advertised_udp_port: None,
            tcp_port: None,
            advertised_tcp_port: None,
            advertise: true,
        }],
        ..Default::default()
    };
    let mut engine = Engine::new(Arc::new(cfg), Arc::new(Nop), Rng::seeded(1));
    let mut now = Instant::now();
    for who in ["a", "b"] {
        let mut c = SessionConfig::default();
        c.group_id = "g".into();
        c.call_id = "call".into();
        c.user_id = format!("user-{who}");
        c.session_id = format!("session-{who}");
        engine.handle_command(now, Command::Join(c));
    }
    for (who, k, data) in input.steps.into_iter().take(64) {
        let session = if who % 2 == 0 { "session-a" } else { "session-b" };
        engine.handle_command(
            now,
            Command::Signal(Message {
                group_id: "g".into(),
                user_id: String::new(),
                session_id: session.into(),
                call_id: "call".into(),
                kind: kind(k),
                data,
            }),
        );
        now += Duration::from_millis(50);
        engine.handle_timeout(now);
        while engine.poll_transmit().is_some() {}
        while engine.poll_event().is_some() {}
    }
    engine.shutdown(now);
});
