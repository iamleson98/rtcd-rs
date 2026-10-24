//! An emulated browser for tests that talk to rtcd over real sockets.
//!
//! [`Browser`] is a real `rtc` peer connection that behaves as the lms-web call client does: it
//! offers a data channel and a microphone, answers the SFU's offers (over the data channel once it
//! is open), trickles ICE, and sends audio with the audio level extension. It does no signaling
//! transport of its own: it hands back the messages the service would carry for it, and takes the
//! ones the service delivers to it, so a test can use the SFU's API directly or a websocket.

use std::net::{SocketAddr, UdpSocket};
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use rtc::interceptor::Registry;
use rtc::media_stream::MediaStreamTrack;
use rtc::peer_connection::configuration::RTCConfigurationBuilder;
use rtc::peer_connection::configuration::interceptor_registry::{
    configure_keyframe_request_forwarding, register_default_interceptors,
};
use rtc::peer_connection::configuration::media_engine::MediaEngine;
use rtc::peer_connection::event::{RTCDataChannelEvent, RTCPeerConnectionEvent};
use rtc::peer_connection::message::RTCMessage;
use rtc::peer_connection::sdp::{RTCSdpType, RTCSessionDescription};
use rtc::peer_connection::state::{RTCPeerConnectionState, RTCSignalingState};
use rtc::peer_connection::transport::{
    CandidateConfig, CandidateHostConfig, RTCIceCandidate, RTCIceCandidateInit,
};
use rtc::peer_connection::{RTCPeerConnection, RTCPeerConnectionBuilder};
use rtc::rtp_transceiver::RTCRtpSenderId;
use rtc::rtp_transceiver::rtp_sender::{
    RTCRtpCodec, RTCRtpCodingParameters, RTCRtpEncodingParameters, RTCRtpHeaderExtensionCapability,
    RtpCodecKind,
};
use rtc::sansio::Protocol;
use rtc::shared::{TaggedBytesMut, TransportContext, TransportProtocol};
use rtcd_proto::MessageType;
use rtcd_proto::dc::DcMessage;

const AUDIO_LEVEL_URI: &str = "urn:ietf:params:rtp-hdrext:ssrc-audio-level";

/// A message the browser sends to the SFU through the service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signal {
    pub kind: MessageType,
    pub data: Vec<u8>,
}

/// An emulated browser.
pub struct Browser {
    pub name: String,
    pub session_id: String,
    pc: RTCPeerConnection,
    socket: UdpSocket,
    server: SocketAddr,
    mic: RTCRtpSenderId,
    mic_ssrc: u32,
    mic_seq: u16,
    dc: Option<usize>,
    connected: bool,
    /// The track id of every RTP packet received.
    rtp: Vec<String>,
    /// Frames the SFU sent on the data channel.
    pub dc_frames: Vec<DcMessage>,
}

impl std::fmt::Debug for Browser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Browser")
            .field("name", &self.name)
            .field("connected", &self.connected)
            .finish_non_exhaustive()
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
    .expect("a host candidate");
    RTCIceCandidate::from(&c).to_json().expect("candidate json")
}

impl Browser {
    /// A browser that will connect to the SFU at `server`, and its first offer to send.
    pub fn new(
        name: &str,
        session_id: &str,
        server: SocketAddr,
        mic_ssrc: u32,
    ) -> (Self, Vec<Signal>) {
        let socket = UdpSocket::bind("127.0.0.1:0").expect("a UDP socket");
        socket.set_nonblocking(true).expect("non-blocking");
        let addr = socket.local_addr().expect("local address");
        let mut me = MediaEngine::default();
        me.register_default_codecs().expect("codecs");
        me.register_header_extension(
            RTCRtpHeaderExtensionCapability {
                uri: AUDIO_LEVEL_URI.into(),
            },
            RtpCodecKind::Audio,
            None,
        )
        .expect("extension");
        let registry = configure_keyframe_request_forwarding(
            register_default_interceptors(Registry::new(), &mut me).expect("interceptors"),
        );
        let mut pc = RTCPeerConnectionBuilder::new()
            .with_configuration(RTCConfigurationBuilder::new().build())
            .with_media_engine(me)
            .with_interceptor_registry(registry)
            .build(Instant::now())
            .expect("a peer connection");
        pc.create_data_channel("calls-dc", None)
            .expect("data channel");
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
            .expect("a microphone");
        pc.add_local_candidate(host_candidate(addr))
            .expect("local candidate");
        let offer = pc.create_offer(None).expect("an offer");
        pc.set_local_description(Instant::now(), offer.clone())
            .expect("local description");
        let browser = Self {
            name: name.into(),
            session_id: session_id.into(),
            pc,
            socket,
            server,
            mic,
            mic_ssrc,
            mic_seq: 0,
            dc: None,
            connected: false,
            rtp: Vec::new(),
            dc_frames: Vec::new(),
        };
        let first = Signal {
            kind: MessageType::Sdp,
            data: serde_json::to_vec(&offer).expect("offer json"),
        };
        (browser, vec![first])
    }

    /// Whether the media connection is up.
    pub fn connected(&self) -> bool {
        self.connected
    }

    /// Whether the signaling data channel is open.
    pub fn data_channel_open(&self) -> bool {
        self.dc.is_some()
    }

    /// RTP packets received on tracks whose id starts with `prefix`.
    pub fn received(&self, prefix: &str) -> usize {
        self.rtp.iter().filter(|t| t.starts_with(prefix)).count()
    }

    /// A message from the SFU, relayed by the service; returns what the browser answers with.
    pub fn handle_signal(&mut self, kind: MessageType, data: &[u8]) -> Vec<Signal> {
        match kind {
            MessageType::Ice => {
                if let Ok(v) = serde_json::from_slice::<serde_json::Value>(data)
                    && let Ok(init) =
                        serde_json::from_value::<RTCIceCandidateInit>(v["candidate"].clone())
                    && !init.candidate.is_empty()
                {
                    let _ = self.pc.add_remote_candidate(init);
                }
                Vec::new()
            }
            MessageType::Sdp => self.on_description(data, false),
            _ => Vec::new(),
        }
    }

    fn on_description(&mut self, json: &[u8], over_dc: bool) -> Vec<Signal> {
        let Ok(desc) = serde_json::from_slice::<RTCSessionDescription>(json) else {
            return Vec::new();
        };
        let now = Instant::now();
        match desc.sdp_type {
            RTCSdpType::Answer => {
                let _ = self.pc.set_remote_description(now, desc);
                Vec::new()
            }
            RTCSdpType::Offer if self.pc.signaling_state() == RTCSignalingState::Stable => {
                if self.pc.set_remote_description(now, desc).is_err() {
                    return Vec::new();
                }
                let Ok(answer) = self.pc.create_answer(None) else {
                    return Vec::new();
                };
                if self.pc.set_local_description(now, answer.clone()).is_err() {
                    return Vec::new();
                }
                let data = serde_json::to_vec(&answer).expect("answer json");
                match (over_dc, self.dc) {
                    (true, Some(id)) => {
                        let frame = DcMessage::Sdp(data).encode();
                        let _ = self
                            .pc
                            .data_channel(id)
                            .map(|mut dc| dc.send(now, BytesMut::from(&frame[..])));
                        Vec::new()
                    }
                    _ => vec![Signal {
                        kind: MessageType::Sdp,
                        data,
                    }],
                }
            }
            _ => Vec::new(),
        }
    }

    /// Does what is due: reads the socket, runs timers, sends what the connection wants sent.
    /// Returns messages for the SFU to carry.
    pub fn poll(&mut self) -> Vec<Signal> {
        let now = Instant::now();
        let mut out = Vec::new();
        let local = self.socket.local_addr().expect("local address");
        let mut buf = [0u8; 2048];
        while let Ok((n, from)) = self.socket.recv_from(&mut buf) {
            let _ = self.pc.handle_read(TaggedBytesMut {
                now,
                transport: TransportContext {
                    local_addr: local,
                    peer_addr: from,
                    ecn: None,
                    transport_protocol: TransportProtocol::UDP,
                },
                message: BytesMut::from(&buf[..n]),
            });
        }
        let _ = self.pc.handle_timeout(now);
        for _ in 0..4 {
            let mut moved = false;
            while let Some(w) = self.pc.poll_write() {
                let _ = self.socket.send_to(&w.message, self.server);
                moved = true;
            }
            while let Some(ev) = self.pc.poll_event() {
                moved = true;
                match ev {
                    RTCPeerConnectionEvent::OnConnectionStateChangeEvent(s) => {
                        self.connected = s == RTCPeerConnectionState::Connected
                    }
                    RTCPeerConnectionEvent::OnIceCandidateEvent(e) => {
                        if let Ok(init) = e.candidate.to_json() {
                            out.push(Signal {
                                kind: MessageType::Ice,
                                data: serde_json::to_vec(&init).expect("candidate json"),
                            });
                        }
                    }
                    RTCPeerConnectionEvent::OnDataChannel(RTCDataChannelEvent::OnOpen(id)) => {
                        self.dc = Some(id)
                    }
                    _ => {}
                }
            }
            while let Some(m) = self.pc.poll_read() {
                moved = true;
                match m.message {
                    RTCMessage::RtpPacket(track, _) => self.rtp.push(track.to_string()),
                    RTCMessage::DataChannelMessage(_, m) => {
                        if let Ok(frame) = DcMessage::decode(&m.data) {
                            if let DcMessage::Sdp(json) = &frame {
                                out.extend(self.on_description(json, true));
                            }
                            self.dc_frames.push(frame);
                        }
                    }
                    _ => {}
                }
            }
            if !moved {
                break;
            }
        }
        out
    }

    /// Sends one 20 ms audio frame of the given level (alternating levels read as speech).
    pub fn send_voice(&mut self, level: u8) {
        let (ext, pt) = {
            let Some(mut s) = self.pc.rtp_sender(self.mic) else {
                return;
            };
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
            sequence_number: self.mic_seq,
            timestamp: u32::from(self.mic_seq) * 960,
            ssrc: self.mic_ssrc,
            ..Default::default()
        };
        if let Some(id) = ext {
            let _ = header.set_extension(id as u8, Bytes::from(vec![level]));
        }
        self.mic_seq = self.mic_seq.wrapping_add(1);
        if let Some(mut s) = self.pc.rtp_sender(self.mic) {
            let _ = s.write_rtp(
                Instant::now(),
                rtc::rtp::packet::Packet {
                    header,
                    payload: Bytes::from_static(&[0xfc, 1, 2, 3, 4]),
                },
            );
        }
    }
}
