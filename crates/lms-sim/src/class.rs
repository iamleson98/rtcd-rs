//! Synthetic participants: real `rtc` peer connections that join a call through rtcd's websocket
//! and send what a browser would: a microphone (Opus packets with the audio level extension), a
//! camera and a screen share (VP8 frames, in two layers for a screen), at the real frame rates.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

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
use rtc::rtcp::payload_feedbacks::full_intra_request::FullIntraRequest;
use rtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication;
use rtc::rtp_transceiver::rtp_sender::{
    RTCRtpCodec, RTCRtpCodingParameters, RTCRtpEncodingParameters, RTCRtpHeaderExtensionCapability,
    RtpCodecKind,
};
use rtc::rtp_transceiver::{RTCRtpSenderId, RTCRtpTransceiverDirection, RTCRtpTransceiverInit};
use rtc::sansio::Protocol;
use rtc::shared::{TaggedBytesMut, TransportContext, TransportProtocol};
use rtcd_proto::dc::DcMessage;
use rtcd_proto::{ClientMessage, JoinRequest, Message, MessageType, SessionRef};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::assets::{Assets, Clip};
use crate::host::Rtcd;

const AUDIO_LEVEL_URI: &str = "urn:ietf:params:rtp-hdrext:ssrc-audio-level";
const MID_URI: &str = "urn:ietf:params:rtp-hdrext:sdes:mid";
const RID_URI: &str = "urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id";
/// The largest payload of a VP8 packet.
const MTU_PAYLOAD: usize = 1100;

/// What a participant sends.
#[derive(Debug, Clone, Default)]
pub struct Spec {
    pub name: String,
    pub voice: bool,
    pub camera: bool,
    pub screen: bool,
}

/// What a participant can be told to do while it is in the call.
#[derive(Debug, Clone, Copy)]
pub enum Ctl {
    /// Start or stop sending voice (a muted microphone sends nothing).
    Voice(bool),
    /// Start or stop sending the camera.
    Camera(bool),
    Screen(bool),
}

/// A participant in the call.
#[derive(Debug)]
pub struct Participant {
    pub session_id: String,
    pub user_id: String,
    stop: Option<oneshot::Sender<()>>,
    ctl: mpsc::UnboundedSender<Ctl>,
    task: JoinHandle<()>,
}

impl Participant {
    pub fn control(&self, c: Ctl) {
        let _ = self.ctl.send(c);
    }

    pub async fn leave(mut self) {
        if let Some(s) = self.stop.take() {
            let _ = s.send(());
        }
        let _ = tokio::time::timeout(Duration::from_secs(5), self.task).await;
    }
}

struct Video {
    sender: RTCRtpSenderId,
    ssrcs: Vec<u32>,
    seq: Vec<u16>,
    ts: Vec<u32>,
    picture_id: u8,
    pos: usize,
    clip: Clip,
    mid: Option<String>,
    next: Instant,
    step: Duration,
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

fn video_track(
    pc: &mut RTCPeerConnection,
    stream_id: &str,
    track_id: &str,
    ssrcs: &[u32],
) -> RTCRtpSenderId {
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
    let encodings = if ssrcs.len() == 1 {
        vec![coding("", ssrcs[0])]
    } else {
        vec![coding("l", ssrcs[0]), coding("h", ssrcs[1])]
    };
    let track = MediaStreamTrack::new(
        stream_id.to_owned(),
        track_id.to_owned(),
        track_id.to_owned(),
        RtpCodecKind::Video,
        encodings,
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
    RTCRtpSenderId::from(id)
}

fn message(session: &str, call: &str, kind: MessageType, data: Vec<u8>) -> ClientMessage {
    ClientMessage::Rtc(Message {
        group_id: "default".into(),
        user_id: String::new(),
        session_id: session.into(),
        call_id: call.into(),
        kind,
        data,
    })
}

/// Joins `call` and keeps sending until it is told to leave.
pub async fn join(
    rtcd: &Rtcd,
    assets: Arc<Assets>,
    call: &str,
    spec: Spec,
    index: u32,
) -> Participant {
    let session_id = format!("session-{}", spec.name);
    let user_id = format!("user-{}", spec.name);
    let mut conn = rtcd.connect().await;
    // The hello comes first.
    let _ = tokio::time::timeout(Duration::from_secs(10), conn.rx.recv()).await;
    conn.send(ClientMessage::Join(JoinRequest {
        call_id: call.into(),
        user_id: user_id.clone(),
        session_id: session_id.clone(),
        channel_id: format!("channel-{call}"),
        av1_support: false,
        dc_signaling: true,
    }));

    let socket = UdpSocket::bind("127.0.0.1:0").await.expect("a UDP socket");
    let addr = socket.local_addr().unwrap();
    rtcd.exempt(addr);
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
    let base = 10_000 + index * 100;
    let mic_ssrc = base;
    let mic = pc
        .add_track(MediaStreamTrack::new(
            format!("stream-{}", spec.name),
            format!("mic-{}", spec.name),
            format!("mic-{}", spec.name),
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
    let mut announce = Vec::new();
    let now = Instant::now();
    let mut camera = spec.camera.then(|| {
        let stream = format!("camera-stream-{}", spec.name);
        let ssrcs = vec![base + 1];
        let sender = video_track(&mut pc, &stream, &format!("camera-{}", spec.name), &ssrcs);
        announce.push(message(
            &session_id,
            call,
            MessageType::VideoOn,
            serde_json::to_vec(&serde_json::json!({ "videoStreamID": stream })).unwrap(),
        ));
        Video {
            sender,
            seq: vec![100],
            ts: vec![1000],
            ssrcs,
            picture_id: 10,
            pos: 0,
            clip: assets.camera.clone(),
            mid: None,
            next: now,
            step: Duration::from_micros(1_000_000 / u64::from(assets.camera.fps)),
        }
    });
    let mut screen = spec.screen.then(|| {
        let stream = format!("screen-stream-{}", spec.name);
        let ssrcs = vec![base + 2, base + 3];
        let sender = video_track(&mut pc, &stream, &format!("screen-{}", spec.name), &ssrcs);
        announce.push(message(
            &session_id,
            call,
            MessageType::ScreenOn,
            serde_json::to_vec(&serde_json::json!({ "screenStreamID": stream })).unwrap(),
        ));
        Video {
            sender,
            seq: vec![100, 5000],
            ts: vec![1000, 900_000],
            ssrcs,
            picture_id: 90,
            pos: 0,
            clip: assets.screen.clone(),
            mid: None,
            next: now,
            step: Duration::from_micros(1_000_000 / u64::from(assets.screen.fps)),
        }
    });
    pc.add_local_candidate(host_candidate(addr))
        .expect("local candidate");
    let offer = pc.create_offer(None).expect("an offer");
    pc.set_local_description(now, offer.clone())
        .expect("local description");
    for m in announce {
        conn.send(m);
    }
    conn.send(message(
        &session_id,
        call,
        MessageType::Sdp,
        serde_json::to_vec(&offer).unwrap(),
    ));

    let (stop, mut stop_rx) = oneshot::channel();
    let (ctl, mut ctl_rx) = mpsc::unbounded_channel::<Ctl>();
    let server = rtcd.media;
    let sid = session_id.clone();
    let call = call.to_owned();
    let mut voice = spec.voice;
    let (mut camera_on, mut screen_on) = (true, true);
    let task = tokio::spawn(async move {
        let mut buf = vec![0u8; 2048];
        let mut dc: Option<usize> = None;
        let mut connected = false;
        let mut mic_seq = 0u16;
        let mut mic_ts = 0u32;
        let mut mic_pos = 0usize;
        let mut next_audio = Instant::now();
        loop {
            // Everything the connection wants sent, and its events.
            let mut moved = true;
            while moved {
                moved = false;
                while let Some(w) = pc.poll_write() {
                    moved = true;
                    let _ = socket.send_to(&w.message, server).await;
                }
                while let Some(ev) = pc.poll_event() {
                    moved = true;
                    match ev {
                        RTCPeerConnectionEvent::OnConnectionStateChangeEvent(s) => {
                            connected = s == RTCPeerConnectionState::Connected
                        }
                        RTCPeerConnectionEvent::OnIceCandidateEvent(e) => {
                            if let Ok(init) = e.candidate.to_json() {
                                conn.send(message(
                                    &sid,
                                    &call,
                                    MessageType::Ice,
                                    serde_json::to_vec(&init).unwrap(),
                                ));
                            }
                        }
                        RTCPeerConnectionEvent::OnDataChannel(RTCDataChannelEvent::OnOpen(id)) => {
                            dc = Some(id)
                        }
                        _ => {}
                    }
                }
                while let Some(m) = pc.poll_read() {
                    moved = true;
                    match m.message {
                        RTCMessage::RtcpPacket(_, packets) => {
                            for p in &packets {
                                let asked: Vec<u32> = if let Some(pli) =
                                    p.as_any().downcast_ref::<PictureLossIndication>()
                                {
                                    vec![pli.media_ssrc]
                                } else if let Some(fir) =
                                    p.as_any().downcast_ref::<FullIntraRequest>()
                                {
                                    fir.fir.iter().map(|e| e.ssrc).collect()
                                } else {
                                    continue;
                                };
                                // An encoder answers a keyframe request with a keyframe: the clip starts over.
                                for v in [camera.as_mut(), screen.as_mut()].into_iter().flatten() {
                                    if v.ssrcs.iter().any(|s| asked.contains(s)) {
                                        v.pos = 0;
                                    }
                                }
                            }
                        }
                        RTCMessage::DataChannelMessage(_, m) => {
                            if let Ok(DcMessage::Sdp(json)) = DcMessage::decode(&m.data) {
                                describe(&mut pc, &json, dc, &conn, &sid, &call);
                            }
                        }
                        _ => {}
                    }
                }
            }

            let now = Instant::now();
            if connected && dc.is_some() {
                if voice {
                    while next_audio <= now {
                        send_voice(
                            &mut pc,
                            mic,
                            mic_ssrc,
                            &mut mic_seq,
                            mic_ts,
                            &assets.voice.packets[mic_pos % assets.voice.packets.len()],
                        );
                        mic_pos += 1;
                        mic_ts = mic_ts.wrapping_add(960);
                        next_audio += Duration::from_millis(20);
                    }
                } else {
                    // A muted microphone sends nothing, and time goes on.
                    while next_audio <= now {
                        mic_ts = mic_ts.wrapping_add(960);
                        next_audio += Duration::from_millis(20);
                    }
                }
                for (v, on) in [(camera.as_mut(), camera_on), (screen.as_mut(), screen_on)] {
                    let Some(v) = v else { continue };
                    while v.next <= now {
                        if on {
                            send_frame(&mut pc, v);
                        } else {
                            // The camera is off, and time goes on.
                            let tick = 90_000 / v.clip.fps;
                            for t in &mut v.ts {
                                *t = t.wrapping_add(tick);
                            }
                        }
                        v.next += v.step;
                    }
                }
            } else {
                next_audio = now;
                for v in [camera.as_mut(), screen.as_mut()].into_iter().flatten() {
                    v.next = now;
                }
            }

            let mut wake = now + Duration::from_millis(20);
            if let Some(t) = pc.poll_timeout() {
                wake = wake.min(t);
            }
            if connected && dc.is_some() {
                if voice {
                    wake = wake.min(next_audio);
                }
                for v in [camera.as_ref(), screen.as_ref()].into_iter().flatten() {
                    wake = wake.min(v.next);
                }
            }
            let sleep = tokio::time::sleep(wake.saturating_duration_since(Instant::now()));
            tokio::pin!(sleep);
            tokio::select! {
                _ = &mut stop_rx => {
                    conn.send(ClientMessage::Leave(SessionRef { session_id: sid.clone() }));
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    return;
                }
                read = socket.recv_from(&mut buf) => {
                    if let Ok((n, from)) = read {
                        let _ = pc.handle_read(TaggedBytesMut {
                            now: Instant::now(),
                            transport: TransportContext { local_addr: addr, peer_addr: from, ecn: None, transport_protocol: TransportProtocol::UDP },
                            message: BytesMut::from(&buf[..n]),
                        });
                    }
                }
                c = ctl_rx.recv() => match c {
                    Some(Ctl::Voice(on)) => voice = on,
                    Some(Ctl::Camera(on)) => camera_on = on,
                    Some(Ctl::Screen(on)) => screen_on = on,
                    None => {}
                },
                m = conn.rx.recv() => match m {
                    Some(ClientMessage::Rtc(m)) if m.session_id == sid => match m.kind {
                        MessageType::Sdp => describe(&mut pc, &m.data, None, &conn, &sid, &call),
                        MessageType::Ice => {
                            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&m.data)
                                && let Ok(init) = serde_json::from_value::<RTCIceCandidateInit>(v["candidate"].clone())
                                && !init.candidate.is_empty()
                            {
                                let _ = pc.add_remote_candidate(init);
                            }
                        }
                        _ => {}
                    },
                    Some(ClientMessage::Close(r)) if r.session_id == sid => return,
                    Some(_) => {}
                    None => return,
                },
                () = &mut sleep => {}
            }
            let _ = pc.handle_timeout(Instant::now());
        }
    });
    Participant {
        session_id,
        user_id,
        stop: Some(stop),
        ctl,
        task,
    }
}

/// Applies a description from the SFU and answers an offer (over the data channel when it came
/// that way).
fn describe(
    pc: &mut RTCPeerConnection,
    json: &[u8],
    dc: Option<usize>,
    conn: &crate::host::RtcdConn,
    sid: &str,
    call: &str,
) {
    let Ok(desc) = serde_json::from_slice::<RTCSessionDescription>(json) else {
        return;
    };
    let now = Instant::now();
    match desc.sdp_type {
        RTCSdpType::Answer => {
            let _ = pc.set_remote_description(now, desc);
        }
        RTCSdpType::Offer if pc.signaling_state() == RTCSignalingState::Stable => {
            if pc.set_remote_description(now, desc).is_err() {
                return;
            }
            let Ok(answer) = pc.create_answer(None) else {
                return;
            };
            if pc.set_local_description(now, answer.clone()).is_err() {
                return;
            }
            let data = serde_json::to_vec(&answer).unwrap();
            match dc {
                Some(id) => {
                    let frame = DcMessage::Sdp(data).encode();
                    let _ = pc
                        .data_channel(id)
                        .map(|mut d| d.send(now, BytesMut::from(&frame[..])));
                }
                None => conn.send(message(sid, call, MessageType::Sdp, data)),
            }
        }
        _ => {}
    }
}

fn send_voice(
    pc: &mut RTCPeerConnection,
    mic: RTCRtpSenderId,
    ssrc: u32,
    seq: &mut u16,
    ts: u32,
    payload: &[u8],
) {
    let (ext, pt) = {
        let Some(mut s) = pc.rtp_sender(mic) else {
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
        sequence_number: *seq,
        timestamp: ts,
        ssrc,
        ..Default::default()
    };
    if let Some(id) = ext {
        let _ = header.set_extension(id as u8, Bytes::from(vec![25u8]));
    }
    *seq = seq.wrapping_add(1);
    if let Some(mut s) = pc.rtp_sender(mic) {
        let _ = s.write_rtp(
            Instant::now(),
            rtc::rtp::packet::Packet {
                header,
                payload: Bytes::copy_from_slice(payload),
            },
        );
    }
}

/// Sends the clip's next frame on every layer, as a browser's encoder does.
fn send_frame(pc: &mut RTCPeerConnection, v: &mut Video) {
    let now = Instant::now();
    let (pt, mid_ext, rid_ext) = {
        let Some(mut s) = pc.rtp_sender(v.sender) else {
            return;
        };
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
            ext(MID_URI),
            ext(RID_URI),
        )
    };
    if v.mid.is_none() {
        v.mid = pc
            .rtp_transceiver(usize::from(v.sender))
            .and_then(|t| t.mid().clone());
    }
    let Some(mid) = v.mid.clone() else { return };
    let (frame, _) = v.clip.frames[v.pos % v.clip.frames.len()].clone();
    v.pos = (v.pos + 1) % v.clip.frames.len();
    v.picture_id = v.picture_id.wrapping_add(1) & 0x7f;
    let chunks: Vec<&[u8]> = frame.chunks(MTU_PAYLOAD).collect();
    let tick = 90_000 / v.clip.fps;
    for l in 0..v.ssrcs.len() {
        v.ts[l] = v.ts[l].wrapping_add(tick);
        for (n, chunk) in chunks.iter().enumerate() {
            let first = n == 0;
            let mut payload = vec![0x80 | if first { 0x10 } else { 0 }, 0x80, v.picture_id];
            payload.extend_from_slice(chunk);
            let mut header = rtc::rtp::header::Header {
                version: 2,
                marker: n == chunks.len() - 1,
                payload_type: pt,
                sequence_number: v.seq[l],
                timestamp: v.ts[l],
                ssrc: v.ssrcs[l],
                ..Default::default()
            };
            if let Some(id) = mid_ext {
                let _ = header.set_extension(id, Bytes::from(mid.clone().into_bytes()));
            }
            if let (Some(id), true) = (rid_ext, v.ssrcs.len() > 1) {
                let _ = header.set_extension(id, Bytes::from(if l == 0 { "l" } else { "h" }));
            }
            v.seq[l] = v.seq[l].wrapping_add(1);
            if let Some(mut s) = pc.rtp_sender(v.sender) {
                let _ = s.write_rtp(
                    now,
                    rtc::rtp::packet::Packet {
                        header,
                        payload: Bytes::from(payload),
                    },
                );
            }
        }
    }
}
