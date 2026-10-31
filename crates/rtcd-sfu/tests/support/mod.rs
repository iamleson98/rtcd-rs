//! A virtual-time world for the engine: a network, the engine, and browsers that behave like the
//! lms-web call client (`call-client.ts`): a real `rtc` peer connection that offers a data channel
//! and a microphone, answers the SFU's offers, and talks on the signaling data channel.
//!
//! Nothing here sleeps or opens a socket; time is a number the world advances to the next deadline,
//! so a whole call runs in milliseconds and every run is the same.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use rtc::interceptor::Registry;
use rtc::media_stream::MediaStreamTrack;
use rtc::peer_connection::configuration::RTCConfigurationBuilder;
use rtc::peer_connection::configuration::interceptor_registry::register_default_interceptors;
use rtc::peer_connection::configuration::media_engine::MediaEngine;
use rtc::peer_connection::configuration::setting_engine::SettingEngineBuilder;
use rtc::peer_connection::event::{RTCDataChannelEvent, RTCPeerConnectionEvent, RTCTrackEvent};
use rtc::peer_connection::message::RTCMessage;
use rtc::peer_connection::sdp::{RTCSdpType, RTCSessionDescription};
use rtc::peer_connection::state::{RTCPeerConnectionState, RTCSignalingState};
use rtc::peer_connection::transport::{
    CandidateConfig, CandidateHostConfig, RTCIceCandidate, RTCIceCandidateInit,
};
use rtc::peer_connection::{RTCPeerConnection, RTCPeerConnectionBuilder};
use rtc::rtp_transceiver::rtp_sender::{
    RTCRtpCodec, RTCRtpCodingParameters, RTCRtpEncodingParameters, RTCRtpHeaderExtensionCapability,
    RtpCodecKind,
};
use rtc::rtp_transceiver::{RTCRtpSenderId, RTCRtpTransceiverDirection, RTCRtpTransceiverInit};
use rtc::sansio::Protocol;
use rtc::shared::{TaggedBytesMut, TransportContext, TransportProtocol};
use rtcd_proto::dc::{DcMessage, TrackInfo};
use rtcd_proto::{Message, MessageType, SessionConfig};
use rtcd_sfu::config::{EngineConfig, LocalAddress};
use rtcd_sfu::engine::{CloseReason, Command, Engine, Event, Proto};
use rtcd_sfu::ids::Rng;
use rtcd_sfu::metrics::{Metrics, NoMetrics};

pub const AUDIO_LEVEL_URI: &str = "urn:ietf:params:rtp-hdrext:ssrc-audio-level";
pub const SERVER: &str = "10.0.0.1:8443";
/// The last payload byte of a packet `send_video_losing` keeps back, to find it again.
pub const LATE: u8 = 0xdd;

/// A track a browser receives.
#[derive(Debug, Clone)]
pub struct RemoteTrack {
    pub track_id: String,
    pub stream_ids: Vec<String>,
    pub ssrc: u32,
    pub rid: Option<String>,
}

/// A simulcast video sender of a browser: a camera or a screen share.
pub struct VideoSender {
    pub sender: RTCRtpSenderId,
    pub stream_id: String,
    /// SSRC of the small, the large and the record layer (the last is sent only if the sender has
    /// it: see [`World::record_layer`]).
    pub ssrcs: [u32; 3],
    seq: [u16; 3],
    ts: [u32; 3],
    picture_id: [u8; 3],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layer {
    Low = 0,
    High = 1,
    /// The layer made for the recording (rid `r`), present when [`World::record_layer`] is set.
    Record = 2,
}

/// What the network between the SFU and one browser does: packets from the SFU can be lost, take
/// time, and queue behind a link of limited rate; what the browser sends (its feedback among it)
/// takes the same time.
#[derive(Debug, Clone, Copy, Default)]
pub struct Path {
    /// Fraction of the SFU's packets to the browser that are lost.
    pub loss: f64,
    /// Time a packet takes each way.
    pub delay: Duration,
    /// The downlink's rate, bits per second; unlimited when `None`.
    pub rate_bps: Option<u64>,
    /// A packet that would wait longer than this for the link is dropped.
    pub queue: Duration,
}

/// A datagram on its way, with the time it arrives.
struct InFlight {
    due: Instant,
    /// The browser it goes to or comes from.
    browser: usize,
    /// For the browser, the address it was sent from.
    local: SocketAddr,
    data: BytesMut,
}

/// An emulated browser.
pub struct Browser {
    pub name: String,
    pub session_id: String,
    pub pc: RTCPeerConnection,
    pub addr: SocketAddr,
    pub dc: Option<usize>,
    /// The verdict of the last lock request.
    pub lock_answer: Option<bool>,
    pub tracks: Vec<RemoteTrack>,
    pub ended_tracks: Vec<String>,
    /// Every RTP packet received: (track id, packet).
    pub rtp: Vec<(String, rtc::rtp::Packet)>,
    pub media_maps: Vec<BTreeMap<String, TrackInfo>>,
    pub dc_frames: Vec<DcMessage>,
    pub offers_received: usize,
    pub answers_sent: usize,
    /// The text of every description the SFU sent this browser, in order.
    pub descriptions: Vec<String>,
    pub mic: RTCRtpSenderId,
    pub mic_ssrc: u32,
    pub mic_seq: u16,
    pub camera: Option<VideoSender>,
    pub screen: Option<VideoSender>,
    /// Keyframe requests the SFU sent this browser: the SSRCs asked about.
    pub keyframe_requests: Vec<u32>,
    /// Every keyframe request seen, kept after the encoder has answered it.
    pub keyframe_log: Vec<u32>,
    pub rtcp_seen: Vec<String>,
    /// Every packet the SFU asked this browser to send again: (SSRC, sequence number).
    pub nacks: Vec<(u32, u16)>,
    /// Packets `send_video_losing` kept back instead of sending.
    pub held_back: Vec<rtc::rtp::Packet>,
    /// Errors the connection returned for datagrams from the SFU.
    pub read_errors: Vec<String>,
    pub datagrams_in: usize,
    /// Datagrams to and from this browser are lost.
    pub unreachable: bool,
    /// What the network does to this browser's traffic, if anything.
    pub path: Option<Path>,
    /// When the downlink has sent everything queued on it.
    link_free: Instant,
    pub connected: bool,
    pub signaling_conflicts: usize,
    /// Descriptions the SFU sent that the browser has not applied yet (it was mid-offer).
    pub remote_candidates_seen: usize,
}

impl Browser {
    pub fn stream_of(&self, track_id: &str) -> Option<&RemoteTrack> {
        self.tracks.iter().find(|t| t.track_id == track_id)
    }

    /// Packets received on the tracks whose id starts with `prefix`.
    pub fn received_on(&self, prefix: &str) -> usize {
        self.rtp
            .iter()
            .filter(|(t, _)| t.starts_with(prefix))
            .count()
    }

    pub fn track_ids(&self) -> Vec<String> {
        self.tracks.iter().map(|t| t.track_id.clone()).collect()
    }
}

pub struct World {
    pub now: Instant,
    pub engine: Engine,
    /// The node's send gauge, which the shards would feed from their sockets: the world is the
    /// engine's only socket.
    egress: Arc<rtcd_sfu::egress::Gauge>,
    pub server: SocketAddr,
    pub browsers: Vec<Browser>,
    pub closed: Vec<(String, CloseReason)>,
    pub voice_events: Vec<(String, bool)>,
    pub ice_messages: usize,
    next_browser: u16,
    pub group: String,
    /// Sends from the engine to the browsers at this address are lost while set.
    pub blackhole: bool,
    /// Datagrams on their way to browsers, and to the engine, over a [`Path`].
    to_browsers: Vec<InFlight>,
    to_engine: Vec<InFlight>,
    rng: u64,
    /// Bytes added to the payload of every video packet, so that frames have the size of real ones.
    pub payload_pad: usize,
    /// New cameras and screens are offered with a third simulcast layer, the record layer.
    pub record_layer: bool,
    /// Time spent inside the engine (datagrams in, timeouts), for the cost tests.
    pub engine_spent: Duration,
    /// Called just before the world calls into the engine, with `"datagram"` or `"timeout"` and
    /// `true`, and just after with `false`: for tests that measure the engine alone.
    pub around_engine: fn(&'static str, bool),
}

/// A node with no send budget whose kernel refuses the number of datagrams `refused` says it has
/// refused so far: the tests raise it as time goes by.
pub fn config_with_refusals(refused: &Arc<AtomicU64>, adapt: bool) -> EngineConfig {
    let refused = refused.clone();
    let gauge =
        rtcd_sfu::egress::Gauge::with_kernel_drops(move || Some(refused.load(Ordering::Relaxed)));
    EngineConfig {
        egress: Arc::new(if adapt { gauge.adapting(0, 0) } else { gauge }),
        ..engine_config()
    }
}

pub fn engine_config() -> EngineConfig {
    EngineConfig {
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
    }
}

fn host_candidate(addr: SocketAddr) -> RTCIceCandidateInit {
    let c = CandidateHostConfig {
        base_config: CandidateConfig {
            network: "udp".into(),
            address: addr.ip().to_string(),
            port: addr.port(),
            component: 1,
            ..Default::default()
        },
        ..Default::default()
    }
    .new_candidate_host()
    .unwrap();
    RTCIceCandidate::from(&c).to_json().unwrap()
}

impl World {
    pub fn new() -> World {
        World::with_config(engine_config())
    }

    pub fn with_config(cfg: EngineConfig) -> World {
        World::with_metrics(cfg, Arc::new(NoMetrics))
    }

    pub fn with_metrics(cfg: EngineConfig, metrics: Arc<dyn Metrics>) -> World {
        // `RUST_LOG=rtcd_sfu=debug cargo test -- --nocapture` shows what the engine decides.
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
        cfg.validate().unwrap();
        let cfg_server = cfg.addresses[0].advertised_udp();
        let egress = cfg.egress.clone();
        World {
            now: Instant::now(),
            egress,
            engine: Engine::new(Arc::new(cfg), metrics, Rng::seeded(7)),
            server: cfg_server,
            browsers: Vec::new(),
            closed: Vec::new(),
            voice_events: Vec::new(),
            ice_messages: 0,
            next_browser: 1,
            group: "group".into(),
            blackhole: false,
            to_browsers: Vec::new(),
            to_engine: Vec::new(),
            rng: 0x9e37_79b9_7f4a_7c15,
            payload_pad: 0,
            record_layer: false,
            engine_spent: Duration::ZERO,
            around_engine: |_, _| {},
        }
    }

    // ----------------------------------------------------------------------------- browsers

    /// Starts a browser in `call` and waits for it to connect and open its data channel.
    pub fn join(&mut self, name: &str, call: &str) -> usize {
        let i = self.start(name, call);
        let ok = self.run_until(Duration::from_secs(20), |w| {
            w.browsers[i].connected && w.browsers[i].dc.is_some()
        });
        assert!(ok, "{name} did not connect");
        i
    }

    /// Starts a browser: asks the engine for a session and sends the first offer, without waiting.
    pub fn start(&mut self, name: &str, call: &str) -> usize {
        self.start_with(name, call, false)
    }

    /// Like [`join`](Self::join), with a camera in the first offer, announced as the client does.
    pub fn join_with_camera(&mut self, name: &str, call: &str) -> usize {
        let i = self.start_with(name, call, true);
        let ok = self.run_until(Duration::from_secs(20), |w| {
            w.browsers[i].connected && w.browsers[i].dc.is_some()
        });
        assert!(ok, "{name} did not connect");
        i
    }

    pub fn start_with(&mut self, name: &str, call: &str, camera: bool) -> usize {
        let session_id = format!("session-{name}");
        let cfg = SessionConfig {
            group_id: self.group.clone(),
            call_id: call.to_owned(),
            user_id: format!("user-{name}"),
            session_id: session_id.clone(),
            props: SessionConfig::from_join(
                "",
                &rtcd_proto::JoinRequest {
                    call_id: call.into(),
                    user_id: "u".into(),
                    session_id: "s".into(),
                    channel_id: "ch".into(),
                    av1_support: false,
                    dc_signaling: true,
                    host: false,
                    observer: false,
                },
            )
            .props,
        };
        self.engine.handle_command(self.now, Command::Join(cfg));

        let addr: SocketAddr = format!("10.1.0.{}:5000", self.next_browser)
            .parse()
            .unwrap();
        self.next_browser += 1;
        let mut me = MediaEngine::default();
        me.register_default_codecs().unwrap();
        me.register_header_extension(
            RTCRtpHeaderExtensionCapability {
                uri: AUDIO_LEVEL_URI.into(),
            },
            RtpCodecKind::Audio,
            None,
        )
        .unwrap();
        let registry = rtc::peer_connection::configuration::interceptor_registry::configure_keyframe_request_forwarding(register_default_interceptors(Registry::new(), &mut me).unwrap());
        let mut pc = RTCPeerConnectionBuilder::new()
            .with_configuration(RTCConfigurationBuilder::new().build())
            .with_media_engine(me)
            .with_interceptor_registry(registry)
            // A browser remembers 1024 packets, not the library's 64: a packet repaired late is
            // still taken.
            .with_setting_engine(
                SettingEngineBuilder::new()
                    .with_srtp_replay_protection_window(1024)
                    .build(),
            )
            .build(self.now)
            .unwrap();
        pc.create_data_channel("calls-dc", None).unwrap();
        let mic_ssrc = 1000 + self.next_browser as u32;
        let mic = pc
            .add_track(MediaStreamTrack::new(
                format!("stream-{name}"),
                format!("mic-{name}"),
                format!("mic-{name}"),
                RtpCodecKind::Audio,
                vec![RTCRtpEncodingParameters {
                    rtp_coding_parameters: RTCRtpCodingParameters {
                        ssrc: Some(mic_ssrc),
                        ..Default::default()
                    },
                    codec: RTCRtpCodec {
                        mime_type: "audio/opus".into(),
                        clock_rate: 48000,
                        channels: 2,
                        sdp_fmtp_line: "minptime=10;useinbandfec=1".into(),
                        rtcp_feedback: vec![],
                    },
                    ..Default::default()
                }],
            ))
            .unwrap();
        let camera = camera.then(|| {
            let ssrcs = [
                2000 + self.next_browser as u32 * 10,
                2001 + self.next_browser as u32 * 10,
                2002 + self.next_browser as u32 * 10,
            ];
            video_sender(
                &mut pc,
                &format!("camera-stream-{name}"),
                &format!("camera-{name}"),
                ssrcs,
                self.record_layer,
            )
        });
        pc.add_local_candidate(host_candidate(addr)).unwrap();
        let offer = pc.create_offer(None).unwrap();
        pc.set_local_description(self.now, offer.clone()).unwrap();

        self.browsers.push(Browser {
            name: name.to_owned(),
            session_id: session_id.clone(),
            pc,
            addr,
            dc: None,
            lock_answer: None,
            tracks: vec![],
            ended_tracks: vec![],
            rtp: vec![],
            media_maps: vec![],
            dc_frames: vec![],
            offers_received: 0,
            answers_sent: 0,
            descriptions: Vec::new(),
            mic,
            mic_ssrc,
            mic_seq: 0,
            camera,
            screen: None,
            keyframe_requests: vec![],
            keyframe_log: vec![],
            rtcp_seen: vec![],
            nacks: vec![],
            held_back: vec![],
            read_errors: vec![],
            datagrams_in: 0,
            unreachable: false,
            path: None,
            link_free: self.now,
            connected: false,
            signaling_conflicts: 0,
            remote_candidates_seen: 0,
        });
        let i = self.browsers.len() - 1;
        if let Some(c) = self.browsers[i]
            .camera
            .as_ref()
            .map(|c| c.stream_id.clone())
        {
            // The client announces the stream before its first packets.
            self.signal(
                i,
                MessageType::VideoOn,
                serde_json::to_vec(&serde_json::json!({ "videoStreamID": c })).unwrap(),
            );
        }
        let json = serde_json::to_vec(&offer).unwrap();
        self.signal(i, MessageType::Sdp, json);
        i
    }

    /// Delivers a message from browser `i` to the engine, as the service would relay it.
    pub fn signal(&mut self, i: usize, kind: MessageType, data: Vec<u8>) {
        let b = &self.browsers[i];
        let msg = Message {
            group_id: self.group.clone(),
            user_id: format!("user-{}", b.name),
            session_id: b.session_id.clone(),
            call_id: String::new(),
            kind,
            data,
        };
        self.engine.handle_command(self.now, Command::Signal(msg));
    }

    pub fn leave(&mut self, i: usize) {
        let id = self.browsers[i].session_id.clone();
        self.engine
            .handle_command(self.now, Command::Leave { session_id: id });
    }

    pub fn dc_send(&mut self, i: usize, msg: DcMessage) {
        let b = &mut self.browsers[i];
        let id = b.dc.expect("data channel is open");
        b.pc.data_channel(id)
            .unwrap()
            .send(self.now, BytesMut::from(&msg.encode()[..]))
            .unwrap();
    }

    /// The browser asks the SFU for the signaling lock and returns its answer.
    pub fn request_lock(&mut self, i: usize) -> bool {
        self.browsers[i].lock_answer = None;
        self.dc_send(i, DcMessage::Lock(None));
        assert!(
            self.run_until(Duration::from_secs(2), |w| w.browsers[i]
                .lock_answer
                .is_some()),
            "no lock answer"
        );
        self.browsers[i].lock_answer.unwrap()
    }

    pub fn unlock(&mut self, i: usize) {
        self.dc_send(i, DcMessage::Unlock);
        self.run_for(Duration::from_millis(100));
    }

    /// Sends `frames` 20 ms Opus-like packets from browser `i`'s microphone, `level` giving each
    /// frame's audio level, and lets them travel.
    pub fn speak(&mut self, i: usize, frames: usize, level: impl Fn(usize) -> u8) {
        for f in 0..frames {
            self.send_audio(i, level(f));
            self.run_for(Duration::from_millis(20));
        }
    }

    pub fn send_audio(&mut self, i: usize, level: u8) {
        let now = self.now;
        let b = &mut self.browsers[i];
        let (ext_id, pt) = {
            let mut s = b.pc.rtp_sender(b.mic).expect("mic sender");
            let p = s.get_parameters();
            (
                p.rtp_parameters
                    .header_extensions
                    .iter()
                    .find(|e| e.uri == AUDIO_LEVEL_URI)
                    .map(|e| e.id),
                p.rtp_parameters
                    .codecs
                    .first()
                    .map_or(111, |c| c.payload_type),
            )
        };
        let mut header = rtc::rtp::header::Header {
            version: 2,
            payload_type: pt,
            sequence_number: b.mic_seq,
            timestamp: u32::from(b.mic_seq) * 960,
            ssrc: b.mic_ssrc,
            ..Default::default()
        };
        if let Some(id) = ext_id {
            header
                .set_extension(id as u8, Bytes::from(vec![level]))
                .unwrap();
        }
        b.mic_seq = b.mic_seq.wrapping_add(1);
        b.pc.rtp_sender(b.mic)
            .unwrap()
            .write_rtp(
                now,
                rtc::rtp::packet::Packet {
                    header,
                    payload: Bytes::from_static(&[0xfc, 1, 2, 3, 4, 5, 6, 7]),
                },
            )
            .expect("write_rtp");
    }

    // --------------------------------------------------------------------------- the network

    /// Moves everything that can move right now.
    fn settle(&mut self) {
        for _ in 0..100_000 {
            let mut moved = false;
            // What has come over a path by now.
            moved |= self.deliver_due();
            // The engine's output to the browsers.
            while let Some(t) = self.engine.poll_transmit() {
                moved = true;
                self.egress.add(
                    rtcd_sfu::egress::Gauge::wire_len(t.data.len(), t.peer, false),
                    1,
                );
                if self.blackhole {
                    continue;
                }
                let Some(i) = self
                    .browsers
                    .iter()
                    .position(|b| b.addr == t.peer && !b.unreachable)
                else {
                    continue;
                };
                match self.browsers[i].path {
                    None => self.read_by_browser(i, t.local, t.data),
                    Some(path) => self.send_down(i, path, t.local, t.data),
                }
            }
            // The engine's reports to the service, which relays them to the right browser.
            while let Some(e) = self.engine.poll_event() {
                moved = true;
                self.on_engine_event(e);
            }
            // The browsers.
            for i in 0..self.browsers.len() {
                moved |= self.pump_browser(i);
            }
            if !moved {
                return;
            }
        }
        panic!("the world never went quiet");
    }

    /// Browser `i` reads a datagram the SFU sent from `from`.
    fn read_by_browser(&mut self, i: usize, from: SocketAddr, data: BytesMut) {
        let now = self.now;
        let b = &mut self.browsers[i];
        b.datagrams_in += 1;
        if let Err(e) = b.pc.handle_read(TaggedBytesMut {
            now,
            transport: TransportContext {
                local_addr: b.addr,
                peer_addr: from,
                ecn: None,
                transport_protocol: TransportProtocol::UDP,
            },
            message: data,
        }) {
            b.read_errors.push(e.to_string());
        }
    }

    /// The engine reads a datagram that browser `i` sent.
    fn engine_reads(&mut self, i: usize, data: BytesMut) {
        let from = self.browsers[i].addr;
        let started = Instant::now();
        (self.around_engine)("datagram", true);
        self.engine
            .handle_datagram(self.now, Proto::Udp, self.server, from, data);
        (self.around_engine)("datagram", false);
        self.engine_spent += started.elapsed();
    }

    /// A datagram from the SFU enters the downlink of browser `i`: it may be lost, waits for the
    /// link's rate (and is dropped when it would wait too long), and arrives a delay later.
    fn send_down(&mut self, i: usize, path: Path, from: SocketAddr, data: BytesMut) {
        if self.chance(path.loss) {
            return;
        }
        let now = self.now;
        let b = &mut self.browsers[i];
        let mut leaves = now;
        if let Some(bps) = path.rate_bps {
            let start = b.link_free.max(now);
            if start.saturating_duration_since(now) > path.queue {
                return;
            }
            b.link_free = start + Duration::from_secs_f64(data.len() as f64 * 8.0 / bps as f64);
            leaves = b.link_free;
        }
        self.to_browsers.push(InFlight {
            due: leaves + path.delay,
            browser: i,
            local: from,
            data,
        });
    }

    /// Delivers the datagrams whose time has come, in both directions.
    fn deliver_due(&mut self) -> bool {
        let now = self.now;
        let mut moved = false;
        let (ready, later): (Vec<_>, Vec<_>) = std::mem::take(&mut self.to_browsers)
            .into_iter()
            .partition(|f| f.due <= now);
        self.to_browsers = later;
        for f in ready {
            moved = true;
            self.read_by_browser(f.browser, f.local, f.data);
        }
        let (ready, later): (Vec<_>, Vec<_>) = std::mem::take(&mut self.to_engine)
            .into_iter()
            .partition(|f| f.due <= now);
        self.to_engine = later;
        for f in ready {
            moved = true;
            self.engine_reads(f.browser, f.data);
        }
        moved
    }

    /// True with probability `p`, the same way on every run.
    fn chance(&mut self, p: f64) -> bool {
        if p <= 0.0 {
            return false;
        }
        let mut x = self.rng;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng = x;
        let r = x.wrapping_mul(0x2545_f491_4f6c_dd1d) >> 11;
        (r as f64) / ((1u64 << 53) as f64) < p
    }

    /// Puts browser `i` behind a network that does what `path` says.
    pub fn set_path(&mut self, i: usize, path: Path) {
        self.browsers[i].path = Some(path);
    }

    fn on_engine_event(&mut self, e: Event) {
        match e {
            Event::SessionClosed {
                session_id, reason, ..
            } => self.closed.push((session_id, reason)),
            Event::Message(m) => match m.kind {
                MessageType::VoiceOn => self.voice_events.push((m.session_id, true)),
                MessageType::VoiceOff => self.voice_events.push((m.session_id, false)),
                MessageType::Ice => {
                    self.ice_messages += 1;
                    if let Some(i) = self
                        .browsers
                        .iter()
                        .position(|b| b.session_id == m.session_id)
                    {
                        let v: serde_json::Value = serde_json::from_slice(&m.data).unwrap();
                        assert_eq!(v["type"], "candidate");
                        let init: RTCIceCandidateInit =
                            serde_json::from_value(v["candidate"].clone()).unwrap();
                        if !init.candidate.is_empty() {
                            self.browsers[i].remote_candidates_seen += 1;
                            let _ = self.browsers[i].pc.add_remote_candidate(init);
                        }
                    }
                }
                MessageType::Sdp => {
                    if let Some(i) = self
                        .browsers
                        .iter()
                        .position(|b| b.session_id == m.session_id)
                    {
                        self.browser_receives_description(i, &m.data, false);
                    }
                }
                other => panic!("unexpected message to a browser: {other:?}"),
            },
        }
    }

    /// A browser applies a description from the SFU and, for an offer, answers it.
    fn browser_receives_description(&mut self, i: usize, json: &[u8], over_dc: bool) {
        let desc: RTCSessionDescription = serde_json::from_slice(json).unwrap();
        let now = self.now;
        let b = &mut self.browsers[i];
        b.descriptions.push(desc.sdp.clone());
        match desc.sdp_type {
            RTCSdpType::Answer => {
                b.pc.set_remote_description(now, desc)
                    .expect("the SFU's answer applies");
            }
            RTCSdpType::Offer => {
                b.offers_received += 1;
                if b.pc.signaling_state() != RTCSignalingState::Stable {
                    b.signaling_conflicts += 1;
                    return;
                }
                b.pc.set_remote_description(now, desc)
                    .expect("the SFU's offer applies");
                let answer = b.pc.create_answer(None).unwrap();
                b.pc.set_local_description(now, answer.clone()).unwrap();
                b.answers_sent += 1;
                let json = serde_json::to_vec(&answer).unwrap();
                if let (true, Some(id)) = (over_dc, b.dc) {
                    let frame = DcMessage::Sdp(json).encode();
                    b.pc.data_channel(id)
                        .unwrap()
                        .send(now, BytesMut::from(&frame[..]))
                        .unwrap();
                } else {
                    self.signal(i, MessageType::Sdp, json);
                }
            }
            other => panic!("unexpected description {other:?}"),
        }
    }

    fn pump_browser(&mut self, i: usize) -> bool {
        let mut moved = false;
        let now = self.now;
        // What the browser sends the SFU.
        loop {
            let Some(w) = self.browsers[i].pc.poll_write() else {
                break;
            };
            moved = true;
            let b = &self.browsers[i];
            if w.transport.peer_addr == self.server && !self.blackhole && !b.unreachable {
                match b.path {
                    None => self.engine_reads(i, w.message),
                    Some(path) => self.to_engine.push(InFlight {
                        due: now + path.delay,
                        browser: i,
                        local: b.addr,
                        data: w.message,
                    }),
                }
            }
        }
        loop {
            let b = &mut self.browsers[i];
            let Some(ev) = b.pc.poll_event() else { break };
            moved = true;
            match ev {
                RTCPeerConnectionEvent::OnConnectionStateChangeEvent(s) => {
                    b.connected = s == RTCPeerConnectionState::Connected
                }
                RTCPeerConnectionEvent::OnIceCandidateEvent(e) => {
                    // The browser trickles its candidates to the SFU through the service.
                    let init = e.candidate.to_json().unwrap();
                    let data = serde_json::to_vec(&init).unwrap();
                    self.signal(i, MessageType::Ice, data);
                }
                RTCPeerConnectionEvent::OnDataChannel(RTCDataChannelEvent::OnOpen(id)) => {
                    b.dc = Some(id)
                }
                RTCPeerConnectionEvent::OnTrack(RTCTrackEvent::OnOpen(init)) => {
                    b.tracks.push(RemoteTrack {
                        track_id: init.track_id.to_string(),
                        stream_ids: init.stream_ids.iter().map(|s| s.to_string()).collect(),
                        ssrc: init.ssrc,
                        rid: init.rid.map(|r| r.to_string()),
                    })
                }
                RTCPeerConnectionEvent::OnTrack(RTCTrackEvent::OnClose(id)) => {
                    b.ended_tracks.push(id.to_string())
                }
                _ => {}
            }
        }
        loop {
            let b = &mut self.browsers[i];
            let Some(m) = b.pc.poll_read() else { break };
            moved = true;
            match m.message {
                RTCMessage::RtpPacket(track, packet) => b.rtp.push((track.to_string(), packet)),
                RTCMessage::RtcpPacket(_, packets) => {
                    for p in &packets {
                        b.rtcp_seen
                            .push(format!("{:?}", p).chars().take(60).collect());
                        if let Some(nack) = p.as_any().downcast_ref::<rtc::rtcp::transport_feedbacks::transport_layer_nack::TransportLayerNack>() {
                            for pair in &nack.nacks {
                                b.nacks.extend(pair.packet_list().into_iter().map(|seq| (nack.media_ssrc, seq)));
                            }
                        }
                        if let Some(pli) = p.as_any().downcast_ref::<rtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication>() {
                            b.keyframe_requests.push(pli.media_ssrc);
                        }
                    }
                }
                RTCMessage::DataChannelMessage(_, m) => {
                    let frame = DcMessage::decode(&m.data).expect("a frame the SFU wrote decodes");
                    b.dc_frames.push(frame.clone());
                    match frame {
                        DcMessage::Lock(Some(granted)) => b.lock_answer = Some(granted),
                        DcMessage::MediaMap(map) => b.media_maps.push(map),
                        DcMessage::Sdp(json) => self.browser_receives_description(i, &json, true),
                        _ => {}
                    }
                }
                _ => {}
            }
        }
        moved
    }

    /// Runs for `span` of virtual time, waking each side at its deadlines.
    pub fn run_for(&mut self, span: Duration) {
        let end = self.now + span;
        loop {
            self.settle();
            let next = self.next_wake().filter(|&t| t <= end).unwrap_or(end);
            if next > self.now {
                self.now = next;
            }
            self.tick_engine();
            for b in &mut self.browsers {
                let _ = b.pc.handle_timeout(self.now);
            }
            if self.now >= end {
                self.settle();
                return;
            }
        }
    }

    /// Runs until `done` holds or `limit` of virtual time passes; returns whether it held.
    pub fn run_until(&mut self, limit: Duration, mut done: impl FnMut(&mut World) -> bool) -> bool {
        let end = self.now + limit;
        loop {
            self.settle();
            if done(self) {
                return true;
            }
            if self.now >= end {
                return false;
            }
            let next = self.next_wake().filter(|&t| t <= end).unwrap_or(end);
            self.now = next.max(self.now);
            self.tick_engine();
            for b in &mut self.browsers {
                let _ = b.pc.handle_timeout(self.now);
            }
        }
    }

    fn tick_engine(&mut self) {
        let started = Instant::now();
        (self.around_engine)("timeout", true);
        self.engine.handle_timeout(self.now);
        (self.around_engine)("timeout", false);
        self.engine_spent += started.elapsed();
    }

    fn next_wake(&mut self) -> Option<Instant> {
        let mut next = self.engine.poll_timeout();
        for f in self.to_browsers.iter().chain(&self.to_engine) {
            next = Some(next.map_or(f.due, |n| n.min(f.due)));
        }
        for b in &mut self.browsers {
            if let Some(t) = b.pc.poll_timeout() {
                next = Some(next.map_or(t, |n| n.min(t)));
            }
        }
        next
    }

    /// Browser `i` shares its screen: announces the stream, adds a simulcast sender and offers.
    pub fn share_screen(&mut self, i: usize) {
        let ssrcs = [
            3000 + i as u32 * 10,
            3001 + i as u32 * 10,
            3002 + i as u32 * 10,
        ];
        let stream_id = format!("screen-stream-{}", self.browsers[i].name);
        self.signal(
            i,
            MessageType::ScreenOn,
            serde_json::to_vec(&serde_json::json!({ "screenStreamID": stream_id })).unwrap(),
        );
        let name = self.browsers[i].name.clone();
        let sender = video_sender(
            &mut self.browsers[i].pc,
            &stream_id,
            &format!("screen-{name}"),
            ssrcs,
            self.record_layer,
        );
        self.browsers[i].screen = Some(sender);
        self.renegotiate(i);
    }

    /// Browser `i` offers, as the client does: it takes the signaling lock, sends the offer over the
    /// data channel, applies the answer and releases the lock.
    pub fn renegotiate(&mut self, i: usize) {
        assert!(self.request_lock(i), "the lock is free");
        let now = self.now;
        let offer = self.browsers[i].pc.create_offer(None).unwrap();
        self.browsers[i]
            .pc
            .set_local_description(now, offer.clone())
            .unwrap();
        self.dc_send(i, DcMessage::Sdp(serde_json::to_vec(&offer).unwrap()));
        let ok = self.run_until(Duration::from_secs(5), |w| {
            w.browsers[i].pc.signaling_state() == RTCSignalingState::Stable
        });
        assert!(ok, "the SFU never answered");
        self.unlock(i);
    }

    /// Sends one frame of `packets` RTP packets on a layer of a video sender. A keyframe begins with
    /// the VP8 keyframe tag. Each packet carries the layer in its payload, so a receiver can tell
    /// where it came from.
    pub fn send_video(&mut self, i: usize, screen: bool, layer: Layer, key: bool, packets: usize) {
        self.send_video_losing(i, screen, layer, key, packets, None);
    }

    /// `send_video`, except that packet number `lost` of the frame, if any, never leaves the
    /// browser: it is kept in `held_back`, for `send_held_back` to deliver late, as a
    /// retransmission is.
    pub fn send_video_losing(
        &mut self,
        i: usize,
        screen: bool,
        layer: Layer,
        key: bool,
        packets: usize,
        lost: Option<usize>,
    ) {
        let now = self.now;
        let pad = self.payload_pad;
        let b = &mut self.browsers[i];
        let v = if screen {
            b.screen.as_mut()
        } else {
            b.camera.as_mut()
        }
        .expect("a video sender");
        let l = layer as usize;
        let (pt, mid_ext, rid_ext) = {
            let mut s = b.pc.rtp_sender(v.sender).expect("sender");
            let p = s.get_parameters();
            let ext = |uri: &str| {
                p.rtp_parameters
                    .header_extensions
                    .iter()
                    .find(|e| e.uri == uri)
                    .map(|e| e.id as u8)
            };
            (
                p.rtp_parameters
                    .codecs
                    .iter()
                    .find(|c| c.rtp_codec.mime_type.eq_ignore_ascii_case("video/vp8"))
                    .map_or(96, |c| c.payload_type),
                ext("urn:ietf:params:rtp-hdrext:sdes:mid"),
                ext("urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id"),
            )
        };
        let mid =
            b.pc.rtp_transceiver(usize::from(v.sender))
                .and_then(|t| t.mid().clone())
                .expect("the sender has a mid");
        v.picture_id[l] = v.picture_id[l].wrapping_add(1) & 0x7f;
        v.ts[l] = v.ts[l].wrapping_add(3000);
        for n in 0..packets {
            let first = n == 0;
            let mut payload = vec![0x80 | if first { 0x10 } else { 0 }, 0x80, v.picture_id[l]];
            if first {
                payload.push(if key { 0x00 } else { 0x01 });
            }
            payload.resize(payload.len() + pad, 0xcc);
            payload.extend_from_slice(&[
                layer as u8 + 1,
                0xaa,
                0xbb,
                if lost == Some(n) { LATE } else { 0xcc },
            ]);
            let mut header = rtc::rtp::header::Header {
                version: 2,
                marker: n == packets - 1,
                payload_type: pt,
                sequence_number: v.seq[l],
                timestamp: v.ts[l],
                ssrc: v.ssrcs[l],
                ..Default::default()
            };
            // A browser puts the stream's mid and rid on its packets; the SFU tells layers apart by them.
            if let Some(id) = mid_ext {
                header
                    .set_extension(id, Bytes::from(mid.clone().into_bytes()))
                    .unwrap();
            }
            if let Some(id) = rid_ext {
                header
                    .set_extension(
                        id,
                        Bytes::from(match layer {
                            Layer::Low => "l",
                            Layer::High => "h",
                            Layer::Record => "r",
                        }),
                    )
                    .unwrap();
            }
            let packet = rtc::rtp::packet::Packet {
                header,
                payload: Bytes::from(payload),
            };
            v.seq[l] = v.seq[l].wrapping_add(1);
            if lost == Some(n) {
                b.held_back.push(packet);
                continue;
            }
            b.pc.rtp_sender(v.sender)
                .unwrap()
                .write_rtp(now, packet)
                .expect("write_rtp");
        }
    }

    /// Sends the packets `send_video_losing` kept back, now, on the sender they belong to.
    pub fn send_held_back(&mut self, i: usize, screen: bool) {
        let now = self.now;
        let b = &mut self.browsers[i];
        let sender = if screen {
            b.screen.as_ref()
        } else {
            b.camera.as_ref()
        }
        .expect("a video sender")
        .sender;
        for packet in std::mem::take(&mut b.held_back) {
            b.pc.rtp_sender(sender)
                .unwrap()
                .write_rtp(now, packet)
                .expect("write_rtp");
        }
    }

    /// Sends frames at 30 per second for `seconds` on both layers of a video sender. The first
    /// frame of each layer, and one every `key_every` frames, is a keyframe.
    pub fn stream_video(
        &mut self,
        i: usize,
        screen: bool,
        seconds: f64,
        key_every: usize,
        layers: &[Layer],
    ) {
        let frames = (seconds * 30.0) as usize;
        for f in 0..frames {
            for &layer in layers {
                // An encoder answers a keyframe request with a keyframe on that layer.
                let v = if screen {
                    self.browsers[i].screen.as_ref()
                } else {
                    self.browsers[i].camera.as_ref()
                }
                .expect("a video sender");
                let ssrc = v.ssrcs[layer as usize];
                let asked = self.browsers[i]
                    .keyframe_requests
                    .iter()
                    .filter(|&&r| r == ssrc)
                    .count();
                self.browsers[i].keyframe_requests.retain(|&r| r != ssrc);
                self.browsers[i]
                    .keyframe_log
                    .extend(std::iter::repeat_n(ssrc, asked));
                self.send_video(i, screen, layer, f % key_every == 0 || asked > 0, 2);
            }
            self.run_for(Duration::from_millis(33));
        }
    }

    /// Streams the screen share of browser `i` for `seconds` as a browser sends one: the small
    /// layer at 5 frames a second and the large at 15, each frame of the size that makes up the
    /// layer's rate (`low_bps`, `high_bps`) in packets of about a kilobyte, with a keyframe four
    /// times that size at the start, every ten seconds, and whenever the SFU asks for one.
    pub fn stream_screen(&mut self, i: usize, seconds: f64, low_bps: u64, high_bps: u64) {
        let pad = std::mem::replace(&mut self.payload_pad, 1000);
        let packets = |bps: u64, fps: u64| (bps / 8 / fps / 1050).max(1) as usize;
        // The layer, a frame every this many ticks, and the packets of a frame.
        let layers = [
            (Layer::Low, 6, packets(low_bps, 5)),
            (Layer::High, 2, packets(high_bps, 15)),
        ];
        let mut last_key = [None::<usize>; 2];
        for tick in 0..(seconds * 30.0) as usize {
            for (n, &(layer, every, size)) in layers.iter().enumerate() {
                if tick % every != 0 {
                    continue;
                }
                let ssrc =
                    self.browsers[i].screen.as_ref().expect("a screen").ssrcs[layer as usize];
                let asked = self.browsers[i].keyframe_requests.contains(&ssrc);
                self.browsers[i].keyframe_requests.retain(|&r| r != ssrc);
                let key = asked || last_key[n].is_none_or(|t| tick - t >= 300);
                if key {
                    last_key[n] = Some(tick);
                }
                self.send_video(i, true, layer, key, if key { size * 4 } else { size });
            }
            self.run_for(Duration::from_millis(33));
        }
        self.payload_pad = pad;
    }

    /// Browser `i` asks for a keyframe of the stream received with `ssrc`.
    pub fn request_keyframe(&mut self, i: usize, ssrc: u32) {
        let now = self.now;
        let b = &mut self.browsers[i];
        let receiver = b.pc.get_receivers().next().expect("a receiver");
        b.pc.rtp_receiver(receiver)
            .unwrap()
            .write_rtcp(
                now,
                vec![Box::new(
                    rtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication {
                        sender_ssrc: 0,
                        media_ssrc: ssrc,
                    },
                )],
            )
            .unwrap();
        self.run_for(Duration::from_millis(50));
    }

    /// Cuts the network path to browser `i`, in both directions.
    pub fn blackhole_browser(&mut self, i: usize) {
        self.browsers[i].unreachable = true;
    }

    pub fn browser(&self, name: &str) -> usize {
        self.browsers
            .iter()
            .position(|b| b.name == name)
            .expect("no such browser")
    }
}

fn video_sender(
    pc: &mut RTCPeerConnection,
    stream_id: &str,
    track_id: &str,
    ssrcs: [u32; 3],
    record: bool,
) -> VideoSender {
    let coding = |rid: &str, ssrc: u32| RTCRtpEncodingParameters {
        rtp_coding_parameters: RTCRtpCodingParameters {
            rid: rid.to_owned(),
            ssrc: Some(ssrc),
            ..Default::default()
        },
        codec: RTCRtpCodec {
            mime_type: "video/VP8".into(),
            clock_rate: 90000,
            channels: 0,
            sdp_fmtp_line: String::new(),
            rtcp_feedback: vec![],
        },
        ..Default::default()
    };
    let track = MediaStreamTrack::new(
        stream_id.to_owned(),
        track_id.to_owned(),
        track_id.to_owned(),
        RtpCodecKind::Video,
        [coding("l", ssrcs[0]), coding("h", ssrcs[1])]
            .into_iter()
            .chain(record.then(|| coding("r", ssrcs[2])))
            .collect(),
    );
    let id = pc
        .add_transceiver_from_track(
            track,
            Some(RTCRtpTransceiverInit {
                direction: RTCRtpTransceiverDirection::Sendonly,
                streams: vec![],
                send_encodings: vec![],
            }),
        )
        .expect("a video transceiver");
    VideoSender {
        sender: RTCRtpSenderId::from(id),
        stream_id: stream_id.to_owned(),
        ssrcs,
        seq: [100, 5000, 20_000],
        ts: [1000, 900_000, 1_800_000],
        picture_id: [10, 90, 50],
    }
}

/// The layer a received video packet came from (see [`World::send_video`]; the marker sits four
/// bytes from the end) and whether it begins a keyframe.
pub fn describe(packet: &rtc::rtp::Packet) -> (Layer, bool) {
    let layer = match packet.payload[packet.payload.len() - 4] {
        1 => Layer::Low,
        3 => Layer::Record,
        _ => Layer::High,
    };
    (layer, rtcd_sfu::vp8::is_keyframe(&packet.payload))
}
