//! A real-time harness: emulated browsers on loopback UDP sockets against a running [`Sfu`],
//! signaling through the SFU's API directly.
#![allow(dead_code)]

use std::sync::Arc;
use std::sync::mpsc::{Receiver, channel};
use std::time::{Duration, Instant};

use rtcd_proto::{Message, MessageType, SessionConfig};
use rtcd_sfu::config::{EngineConfig, LocalAddress};
use rtcd_sfu::engine::{CloseReason, Event};
use rtcd_sfu::metrics::{Metrics, NetTraffic};
use rtcd_sfu::runtime::{ScalingPolicy, Sfu, SfuConfig};
use rtcd_testkit::{Browser, Signal};

/// Counts the socket traffic the shards report.
#[derive(Debug, Default)]
pub struct Traffic {
    pub packets_in: std::sync::atomic::AtomicU64,
    pub forwarded: std::sync::atomic::AtomicU64,
}

impl Metrics for Traffic {
    fn net_traffic(&self, t: NetTraffic) {
        use std::sync::atomic::Ordering::Relaxed;
        self.packets_in.fetch_add(t.udp_in_packets, Relaxed);
        self.forwarded.fetch_add(t.udp_forwarded_packets, Relaxed);
    }
}

pub struct Live {
    pub traffic: Arc<Traffic>,
    pub sfu: Arc<Sfu>,
    events: Receiver<Event>,
    pub browsers: Vec<Browser>,
    pub closed: Vec<(String, CloseReason)>,
    pub voice: Vec<(String, bool)>,
}

impl Live {
    pub fn start(shards: usize) -> Live {
        Self::start_up_to(shards, 0, ScalingPolicy::default())
    }

    /// Starts with `shards` and lets the SFU grow to `max_shards` by `scaling`.
    pub fn start_up_to(shards: usize, max_shards: usize, scaling: ScalingPolicy) -> Live {
        let engine = EngineConfig {
            addresses: vec![LocalAddress {
                bind: "127.0.0.1".parse().unwrap(),
                public: None,
                udp_port: 0,
                advertised_udp_port: None,
                tcp_port: None,
                advertised_tcp_port: None,
                advertise: true,
            }],
            ..Default::default()
        };
        let cfg = SfuConfig {
            engine,
            shards,
            max_shards,
            scaling,
            udp_recv_buffer: 1 << 20,
            udp_send_buffer: 1 << 20,
            ..Default::default()
        };
        let traffic = Arc::new(Traffic::default());
        let (tx, rx) = channel();
        let tx = std::sync::Mutex::new(tx);
        let sfu = Sfu::start(
            cfg,
            traffic.clone(),
            Arc::new(move |e: Event| {
                let _ = tx.lock().unwrap().send(e);
            }),
        )
        .expect("the SFU starts");
        Live {
            traffic,
            sfu: Arc::new(sfu),
            events: rx,
            browsers: vec![],
            closed: vec![],
            voice: vec![],
        }
    }

    pub fn join(&mut self, name: &str, call: &str) -> usize {
        let i = self.start_browser(name, call);
        assert!(
            self.run_until(Duration::from_secs(15), |l| l.browsers[i].connected()
                && l.browsers[i].data_channel_open()),
            "{name} did not connect"
        );
        i
    }

    fn start_browser(&mut self, name: &str, call: &str) -> usize {
        let session_id = format!("session-{name}");
        let props = SessionConfig::from_join(
            "",
            &rtcd_proto::JoinRequest {
                dc_signaling: true,
                ..Default::default()
            },
        )
        .props;
        self.sfu
            .join(SessionConfig {
                group_id: "group".into(),
                call_id: call.into(),
                user_id: format!("user-{name}"),
                session_id: session_id.clone(),
                props,
            })
            .expect("join");
        let server = self.sfu.addresses()[0].advertised_udp();
        let (browser, first) =
            Browser::new(name, &session_id, server, 5000 + self.browsers.len() as u32);
        self.browsers.push(browser);
        let i = self.browsers.len() - 1;
        self.send(i, first);
        i
    }

    fn send(&self, i: usize, signals: Vec<Signal>) {
        let b = &self.browsers[i];
        for s in signals {
            let _ = self.sfu.signal(Message {
                group_id: "group".into(),
                user_id: format!("user-{}", b.name),
                session_id: b.session_id.clone(),
                call_id: String::new(),
                kind: s.kind,
                data: s.data,
            });
        }
    }

    pub fn speak(&mut self, i: usize, frames: usize) {
        for f in 0..frames {
            self.browsers[i].send_voice(if f % 2 == 0 { 20 } else { 70 });
            self.run(Duration::from_millis(20));
        }
    }

    /// One pass over everything: the SFU's reports and each browser's sockets and timers.
    pub fn pump(&mut self) {
        self.drain_events();
        for i in 0..self.browsers.len() {
            let out = self.browsers[i].poll();
            self.send(i, out);
        }
    }

    pub fn drain_events(&mut self) {
        while let Ok(e) = self.events.try_recv() {
            match e {
                Event::SessionClosed {
                    session_id, reason, ..
                } => self.closed.push((session_id, reason)),
                Event::Message(m) => {
                    match m.kind {
                        MessageType::VoiceOn => self.voice.push((m.session_id.clone(), true)),
                        MessageType::VoiceOff => self.voice.push((m.session_id.clone(), false)),
                        _ => {}
                    }
                    if let Some(i) = self
                        .browsers
                        .iter()
                        .position(|b| b.session_id == m.session_id)
                    {
                        let out = self.browsers[i].handle_signal(m.kind, &m.data);
                        self.send(i, out);
                    }
                }
            }
        }
    }

    pub fn run(&mut self, span: Duration) {
        let end = Instant::now() + span;
        while Instant::now() < end {
            self.pump();
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    pub fn run_until(&mut self, limit: Duration, mut done: impl FnMut(&mut Live) -> bool) -> bool {
        let end = Instant::now() + limit;
        while Instant::now() < end {
            self.pump();
            if done(self) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        false
    }
}
