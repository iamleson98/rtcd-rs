//! Synthetic participants: real `rtc` peer connections that join a call through rtcd's websocket
//! and send what a browser would: a microphone (Opus packets with the audio level extension), a
//! camera and a screen share (VP8 frames, in two layers for a screen), at the real frame rates.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use rtc::interceptor::Registry;
use rtc::media_stream::MediaStreamTrack;
use rtc::peer_connection::configuration::RTCConfigurationBuilder;
use rtc::peer_connection::configuration::interceptor_registry::{
    configure_keyframe_request_forwarding, register_default_interceptors,
};
use rtc::peer_connection::configuration::media_engine::MediaEngine;
use rtc::peer_connection::event::{RTCDataChannelEvent, RTCPeerConnectionEvent, RTCTrackEvent};
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
use rtc::rtp_transceiver::{
    RTCRtpReceiverId, RTCRtpSenderId, RTCRtpTransceiverDirection, RTCRtpTransceiverInit,
};
use rtc::sansio::Protocol;
use rtc::shared::{TaggedBytesMut, TransportContext, TransportProtocol};
use rtc::statistics::StatsSelector;
use rtcd_proto::dc::DcMessage;
use rtcd_proto::{ClientMessage, JoinRequest, Message, MessageType, SessionRef};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::assets::{Assets, CameraLayers, Clip};
use crate::host::{Rtcd, RtcdConn};
use crate::hostile::{self, Hostile, Hot, Rng};
use crate::stats::{Probe, Receiver};

const AUDIO_LEVEL_URI: &str = "urn:ietf:params:rtp-hdrext:ssrc-audio-level";
const MID_URI: &str = "urn:ietf:params:rtp-hdrext:sdes:mid";
const RID_URI: &str = "urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id";
/// The largest payload of a VP8 packet.
const MTU_PAYLOAD: usize = 1100;
/// Where a sender that starts near the wrap begins: its sequence numbers wrap after 300 packets,
/// its timestamps after 20 s, so a run crosses every wrap.
const WRAP_SEQ: u16 = 0u16.wrapping_sub(300);
const WRAP_VIDEO_TS: u32 = 0u32.wrapping_sub(20 * 90_000);
const WRAP_AUDIO_TS: u32 = 0u32.wrapping_sub(20 * 48_000);

/// The port of a try in a participant's block of four: tries alternate over the first two, so that
/// a rejoin never reuses the port the SFU still knows of the try before it.
fn rejoin_port(base: u16, epoch: u32) -> u16 {
    base + (epoch % 2) as u16
}

/// The port of a NAT rebind in the block: rebinds alternate over the last two, which no try uses,
/// so that a rebind never lands on the port of a rejoin.
fn rebind_port(base: u16, slot: u32) -> u16 {
    base + 2 + (slot % 2) as u16
}

/// What a participant sends.
#[derive(Debug, Clone, Default)]
pub struct Spec {
    pub name: String,
    pub voice: bool,
    pub camera: bool,
    pub screen: bool,
    /// Names of the participants whose cameras this one watches (a `video_subscribe` once it is
    /// connected); empty watches none, as a browser with its cameras off-screen.
    pub watch: Vec<String>,
    /// Starts every stream's sequence numbers and timestamps just short of their wrap.
    pub wrap: bool,
    /// Measures what it receives and reports what it sends and sees (see [`crate::stats`]).
    pub probe: Option<Probe>,
    /// The first of a block of four UDP ports (a multiple of four) the participant's sockets bind
    /// to, one per try, so that a shaped network can pick its packets out by port; any port when
    /// `None`.
    pub udp_base: Option<u16>,
    /// Sends datagrams of MTU size and over at intervals (see [`Ctl::Mtu`]) from the start.
    pub mtu: bool,
    /// Seeds what the participant does at random (the hostile behaviours, the sizes of the MTU
    /// datagrams).
    pub seed: u64,
    /// Counts what a hostile or MTU behaviour sent, for whoever drives it.
    pub tally: Option<Arc<AtomicU64>>,
}

/// How a participant answers the SFU's offers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Quirk {
    /// At once, once.
    #[default]
    None,
    /// Not at all: it sends an offer of its own while the SFU's waits (glare).
    Glare,
    /// After the pause, as a slow client does.
    Late(Duration),
    /// Twice, at once (a duplicated answer).
    Twice,
}

/// What a participant can be told to do while it is in the call.
#[derive(Debug, Clone, Copy)]
pub enum Ctl {
    /// Start or stop sending voice (a muted microphone sends nothing).
    Voice(bool),
    /// Start or stop sending the camera.
    Camera(bool),
    Screen(bool),
    /// Start or stop the frames of one layer (0 or 1) of a screen share.
    Layer(usize, bool),
    /// Tells the SFU what a browser tells it (`mute`, `video_off`, `screen_on`, ...).
    Announce(MessageType),
    /// Subscribes to the cameras of these participants: bit 0 the teacher, bit `k` (1 to 25)
    /// student `s{k-1}`. The SFU answers with an offer when the set changes.
    Watch(u32),
    /// A burst of this many client offers, the signaling a renegotiation storm is made of.
    Renego(u32),
    /// How to answer the SFU's offers from now on.
    Quirk(Quirk),
    /// Datagrams of 1400 bytes and over, up to 3000 (they fragment), now and then, or none.
    Mtu(bool),
    /// The participant's UDP source port changes, as after a NAT rebinding: a new socket (in its
    /// port block) carries the same peer connection from now on.
    Rebind,
    /// Misbehaves for `secs` seconds at `level` (1 to 5), see [`Hostile`].
    Hostile {
        kind: Hostile,
        secs: u32,
        level: u8,
    },
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

pub(crate) struct Video {
    pub(crate) sender: RTCRtpSenderId,
    /// `camera` or `screen`, for the events.
    pub(crate) label: &'static str,
    pub(crate) ssrcs: Vec<u32>,
    pub(crate) seq: Vec<u16>,
    pub(crate) ts: Vec<u32>,
    /// Per layer: the clip it replays (layers differ in size and rate), the next frame of it, the
    /// VP8 picture id, TL0PICIDX, and when its next frame is due and how far apart they are.
    pub(crate) clips: Vec<Clip>,
    pub(crate) pos: Vec<usize>,
    pub(crate) picture_id: Vec<u16>,
    pub(crate) tl0: Vec<u8>,
    pub(crate) next: Vec<Instant>,
    pub(crate) step: Vec<Duration>,
    /// Whether the payload descriptor carries a 15-bit picture id, TL0PICIDX and the temporal layer
    /// (frame `n` of a clip in layer 0, 2, 1, 2 for `n` mod 4 = 0, 1, 2, 3, as its encoder made it).
    pub(crate) temporal: bool,
    pub(crate) mid: Option<String>,
}

impl Video {
    /// A sender of one layer per clip (an SSRC each), starting now.
    fn new(
        sender: RTCRtpSenderId,
        label: &'static str,
        ssrcs: Vec<u32>,
        clips: Vec<Clip>,
        (temporal, wrap): (bool, bool),
        now: Instant,
    ) -> Self {
        let n = ssrcs.len();
        let (seq, ts) = if wrap {
            (vec![WRAP_SEQ; n], vec![WRAP_VIDEO_TS; n])
        } else {
            (vec![100, 5000], vec![1000, 900_000])
        };
        Self {
            sender,
            label,
            seq: seq[..n].to_vec(),
            ts: ts[..n].to_vec(),
            ssrcs,
            picture_id: vec![10; n],
            tl0: vec![0; n],
            pos: vec![0; n],
            next: vec![now; n],
            step: clips
                .iter()
                .map(|c| Duration::from_micros(1_000_000 / u64::from(c.fps)))
                .collect(),
            clips,
            temporal,
            mid: None,
        }
    }

    /// The encoder starts over with a keyframe on every layer.
    fn restart(&mut self) {
        self.pos.fill(0);
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

/// A message for the SFU, the way the LMS server passes a client's on.
pub fn message(session: &str, call: &str, kind: MessageType, data: Vec<u8>) -> ClientMessage {
    ClientMessage::Rtc(Message {
        group_id: "default".into(),
        user_id: String::new(),
        session_id: session.into(),
        call_id: call.into(),
        kind,
        data,
    })
}

/// Opens a websocket to rtcd and joins `call` as `name`; returns the connection and the session
/// Opens a websocket to rtcd and joins `call` as `name`; returns the connection and the session
/// and user ids.
pub async fn open_session(rtcd: &Rtcd, call: &str, name: &str) -> (RtcdConn, String, String) {
    try_open_session(rtcd, call, name)
        .await
        .expect("the websocket opens")
}

/// Like [`open_session`]; `None` when the websocket does not open.
pub async fn try_open_session(
    rtcd: &Rtcd,
    call: &str,
    name: &str,
) -> Option<(RtcdConn, String, String)> {
    let session_id = format!("session-{name}");
    let user_id = format!("user-{name}");
    let mut conn = rtcd.try_connect().await?;
    // The hello comes first.
    let _ = tokio::time::timeout(Duration::from_secs(10), conn.rx.recv()).await;
    conn.send(ClientMessage::Join(JoinRequest {
        call_id: call.into(),
        user_id: user_id.clone(),
        session_id: session_id.clone(),
        channel_id: format!("channel-{call}"),
        av1_support: false,
        dc_signaling: true,
        host: false,
        observer: false,
    }));
    Some((conn, session_id, user_id))
}

/// What a participant's tries to be in the call share.
struct Env {
    rtcd: Arc<Rtcd>,
    assets: Arc<Assets>,
    call: String,
    sid: String,
    me: String,
    wrap: bool,
    probe: Option<Probe>,
    udp_base: Option<u16>,
    seed: u64,
    tally: Option<Arc<AtomicU64>>,
}

/// What a participant has been told to do, which a new try picks up where the last left off.
struct Persist {
    voice: bool,
    camera_on: bool,
    screen_on: bool,
    layers: [bool; 2],
    quirk: Quirk,
    mtu: bool,
    /// What it misbehaves with, until when and how hard.
    hostile: Option<(Hostile, Instant, u8)>,
}

impl Persist {
    /// Takes in what is a state; what is a one-off act (an offer, a rebind) is the try's to do.
    fn apply(&mut self, c: Ctl) {
        match c {
            Ctl::Voice(on) => self.voice = on,
            Ctl::Camera(on) => self.camera_on = on,
            Ctl::Screen(on) => self.screen_on = on,
            Ctl::Layer(l, on) => self.layers[l % 2] = on,
            Ctl::Quirk(q) => self.quirk = q,
            Ctl::Mtu(on) => self.mtu = on,
            Ctl::Hostile { kind, secs, level } => {
                self.hostile = Some((
                    kind,
                    Instant::now() + Duration::from_secs(u64::from(secs)),
                    level,
                ));
            }
            Ctl::Announce(_) | Ctl::Watch(_) | Ctl::Renego(_) | Ctl::Rebind => {}
        }
    }
}

/// One try at the seat: its websocket, its peer connection and what it sends.
struct Session {
    conn: RtcdConn,
    pc: RTCPeerConnection,
    socket: UdpSocket,
    addr: SocketAddr,
    mic: RTCRtpSenderId,
    mic_ssrc: u32,
    camera: Option<Video>,
    screen: Option<Video>,
    watch: Option<Vec<u8>>,
}

/// How a try ended.
enum End {
    /// The participant was told to leave.
    Stopped,
    /// The SFU closed the session, the signaling connection dropped or the media never came
    /// (`joined`: it had been in the call).
    Lost { joined: bool },
}

/// The pauses before each rejoin, in order (the last repeats), and how long after a drop a new
/// try may still start: lms-web's (`features/calls/rejoin.ts`).
const REJOIN_BACKOFF: [Duration; 5] = [
    Duration::from_secs(1),
    Duration::from_secs(3),
    Duration::from_secs(8),
    Duration::from_secs(15),
    Duration::from_secs(30),
];
const REJOIN_PATIENCE: Duration = Duration::from_secs(180);
/// How long a try gets to connect, and how long a media path that failed gets to come back before
/// the seat is given up (lms-web's `CONNECT_TIMEOUT_MS` and `ICE_RESTART_GRACE_MS`).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const ICE_FAIL_GRACE: Duration = Duration::from_secs(25);

/// Joins `call` and keeps sending until it is told to leave. When the SFU closes the session or
/// the signaling connection drops, the participant takes its seat back the way lms-web does (a
/// few tries with growing pauses, each logged as an `event` of kind `rejoin` with the downtime);
/// it does not restart ICE, so a media path that fails and does not come back within 25 s is
/// given up and rejoined.
pub async fn join(
    rtcd: &Arc<Rtcd>,
    assets: Arc<Assets>,
    call: &str,
    spec: Spec,
    index: u32,
) -> Participant {
    let session_id = format!("session-{}", spec.name);
    let user_id = format!("user-{}", spec.name);
    let first = build(rtcd, &assets, call, &spec, index, 0).await;
    let (stop, mut stop_rx) = oneshot::channel();
    let (ctl, mut ctl_rx) = mpsc::unbounded_channel::<Ctl>();
    let env = Env {
        rtcd: rtcd.clone(),
        assets,
        call: call.to_owned(),
        sid: session_id.clone(),
        me: spec.name.clone(),
        wrap: spec.wrap,
        probe: spec.probe.clone(),
        udp_base: spec.udp_base,
        seed: spec.seed,
        tally: spec.tally.clone(),
    };
    let mut persist = Persist {
        voice: spec.voice,
        camera_on: true,
        screen_on: true,
        layers: [true; 2],
        quirk: Quirk::None,
        mtu: spec.mtu,
        hostile: None,
    };
    let task = tokio::spawn(async move {
        let mut epoch = 0u32;
        let mut session = first;
        // When the seat was lost, and how many tries it has taken since.
        let mut lost = session.is_none().then(Instant::now);
        let mut tries = 0u32;
        loop {
            if let Some(s) = session.take() {
                let back = lost.map(|at| (at, tries));
                match drive(
                    s,
                    &env,
                    &mut persist,
                    &mut stop_rx,
                    &mut ctl_rx,
                    epoch,
                    back,
                )
                .await
                {
                    End::Stopped => return,
                    End::Lost { joined } => {
                        if joined || lost.is_none() {
                            lost = Some(Instant::now());
                            tries = 0;
                        }
                    }
                }
            }
            let at = *lost.get_or_insert_with(Instant::now);
            let pause = REJOIN_BACKOFF[(tries as usize).min(REJOIN_BACKOFF.len() - 1)];
            if at.elapsed() + pause > REJOIN_PATIENCE {
                if let Some(p) = &env.probe {
                    p.event(&env.me, "error", "gave up rejoining");
                }
                return;
            }
            let wake = tokio::time::sleep(pause);
            tokio::pin!(wake);
            loop {
                tokio::select! {
                    _ = &mut stop_rx => return,
                    c = ctl_rx.recv() => if let Some(c) = c { persist.apply(c) },
                    () = &mut wake => break,
                }
            }
            tries += 1;
            epoch += 1;
            session = build(&env.rtcd, &env.assets, &env.call, &spec, index, epoch).await;
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

/// Opens the websocket and the peer connection of one try, and sends the offer; `None` when the
/// websocket does not open or the port is taken.
async fn build(
    rtcd: &Rtcd,
    assets: &Arc<Assets>,
    call: &str,
    spec: &Spec,
    index: u32,
    epoch: u32,
) -> Option<Session> {
    let (conn, session_id, _) = try_open_session(rtcd, call, &spec.name).await?;
    // On loopback, unless the participants are on another machine than rtcd: then the address
    // they can be reached at.
    let local = std::env::var("LMS_SIM_LOCAL_IP").unwrap_or_else(|_| "127.0.0.1".into());
    // A shaped network picks the participant's packets out by its port: a block of four, see
    // `rejoin_port`.
    let port = spec.udp_base.map_or(0, |b| rejoin_port(b, epoch));
    let socket = UdpSocket::bind(format!("{local}:{port}")).await.ok()?;
    let addr = socket.local_addr().ok()?;
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
    let camera = spec.camera.then(|| {
        let stream = format!("camera-stream-{}", spec.name);
        // Cameras are sent as lms-web sends them: a small layer and a large one, the teacher's
        // larger and faster than a student's.
        let simulcast = assets.camera_layers == CameraLayers::Simulcast;
        let (ssrcs, clips) = if simulcast {
            let high = if spec.name == "teacher" {
                &assets.camera_teacher
            } else {
                &assets.camera_student
            };
            (
                vec![base + 1, base + 4],
                vec![assets.camera_low.clone(), high.clone()],
            )
        } else {
            (vec![base + 1], vec![assets.camera.clone()])
        };
        let sender = video_track(&mut pc, &stream, &format!("camera-{}", spec.name), &ssrcs);
        announce.push(message(
            &session_id,
            call,
            MessageType::VideoOn,
            serde_json::to_vec(&serde_json::json!({ "videoStreamID": stream })).unwrap(),
        ));
        Video::new(sender, "camera", ssrcs, clips, (simulcast, spec.wrap), now)
    });
    let screen = spec.screen.then(|| {
        let stream = format!("screen-stream-{}", spec.name);
        let ssrcs = vec![base + 2, base + 3];
        let sender = video_track(&mut pc, &stream, &format!("screen-{}", spec.name), &ssrcs);
        announce.push(message(
            &session_id,
            call,
            MessageType::ScreenOn,
            serde_json::to_vec(&serde_json::json!({ "screenStreamID": stream })).unwrap(),
        ));
        let clips = vec![assets.screen.clone(); 2];
        Video::new(sender, "screen", ssrcs, clips, (false, spec.wrap), now)
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
    let watch = (!spec.watch.is_empty()).then(|| {
        let ids: Vec<String> = spec.watch.iter().map(|n| format!("session-{n}")).collect();
        serde_json::to_vec(&serde_json::json!({ "session_ids": ids })).unwrap()
    });
    Some(Session {
        conn,
        pc,
        socket,
        addr,
        mic,
        mic_ssrc,
        camera,
        screen,
        watch,
    })
}

/// Runs one try until it ends.
async fn drive(
    s: Session,
    env: &Env,
    persist: &mut Persist,
    stop_rx: &mut oneshot::Receiver<()>,
    ctl_rx: &mut mpsc::UnboundedReceiver<Ctl>,
    epoch: u32,
    back: Option<(Instant, u32)>,
) -> End {
    let Session {
        mut conn,
        mut pc,
        mut socket,
        addr,
        mic,
        mic_ssrc,
        mut camera,
        mut screen,
        mut watch,
    } = s;
    let (sid, call, me, wrap, probe) = (
        env.sid.as_str(),
        env.call.as_str(),
        env.me.as_str(),
        env.wrap,
        &env.probe,
    );
    let assets = &env.assets;
    let server = env.rtcd.media;
    // The receiver lines of a later try are a series of their own (the counters start again).
    let rid = if epoch == 0 {
        me.to_owned()
    } else {
        format!("{me}#{epoch}")
    };
    let started = Instant::now();
    let mut failed_since: Option<Instant> = None;

    let mut buf = vec![0u8; 2048];
    let mut dc: Option<usize> = None;
    let mut connected = false;
    let mut mic_seq = if wrap { WRAP_SEQ } else { 0u16 };
    let mut mic_ts = if wrap { WRAP_AUDIO_TS } else { 0u32 };
    let mut mic_pos = 0usize;
    let mut next_audio = Instant::now();
    // What this participant measures of what it receives, and where to ask for a keyframe.
    let mut rx = probe
        .as_ref()
        .filter(|p| p.measure)
        .map(|_| Receiver::default());
    let mut receivers: HashMap<String, (RTCRtpReceiverId, u32)> = HashMap::new();
    let mut joined = false;
    let mut next_probe = Instant::now() + Duration::from_secs(1);
    let mut probe_ticks = 0u32;
    // The quirks: answers held back, the last offer this try sent, which port of its block it is
    // on, when the next hostile step and the next MTU datagrams are due.
    let mut held: Vec<(Instant, Vec<u8>)> = Vec::new();
    let mut last_offer: Option<RTCSessionDescription> = None;
    let mut slot = epoch;
    let mut rng = Rng::new(env.seed ^ u64::from(epoch));
    let mut next_hostile = Instant::now();
    let mut next_mtu = Instant::now() + Duration::from_secs(1);
    let tally = |n: u64| {
        if let Some(t) = &env.tally {
            t.fetch_add(n, Ordering::Relaxed);
        }
    };
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
                        connected = s == RTCPeerConnectionState::Connected;
                        failed_since = match s {
                            RTCPeerConnectionState::Failed => failed_since.or(Some(Instant::now())),
                            RTCPeerConnectionState::Connected => None,
                            _ => failed_since,
                        };
                        if let Some(p) = probe {
                            p.event(me, "ice_state", &format!("connection:{s}"));
                        }
                    }
                    RTCPeerConnectionEvent::OnIceConnectionStateChangeEvent(s) => {
                        if let Some(p) = probe {
                            p.event(me, "ice_state", &format!("ice:{s}"));
                        }
                    }
                    RTCPeerConnectionEvent::OnTrack(RTCTrackEvent::OnOpen(init)) => {
                        receivers.insert(init.track_id, (init.receiver_id, init.ssrc));
                    }
                    RTCPeerConnectionEvent::OnIceCandidateEvent(e) => {
                        if let Ok(init) = e.candidate.to_json() {
                            conn.send(message(
                                sid,
                                call,
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
                let arrived = m.now;
                match m.message {
                    RTCMessage::RtpPacket(track, packet) => {
                        if let (Some(rx), Some(p)) = (rx.as_mut(), probe) {
                            let size = packet.payload.len() + 12;
                            if let Some((what, w)) = rx.on_rtp(
                                p,
                                arrived,
                                &track,
                                packet.header.sequence_number,
                                packet.header.timestamp,
                                packet.header.marker,
                            ) && p.leader
                            {
                                for (on, name) in [(w.seq, "seq"), (w.ts, "timestamp")] {
                                    if on {
                                        p.event(me, "wrap", &format!("rx {name} {what}"));
                                    }
                                }
                            }
                            rx.count_bytes(&track, size);
                        }
                    }
                    RTCMessage::RtcpPacket(_, packets) => {
                        for p in &packets {
                            let asked: Vec<u32> = if let Some(pli) =
                                p.as_any().downcast_ref::<PictureLossIndication>()
                            {
                                vec![pli.media_ssrc]
                            } else if let Some(fir) = p.as_any().downcast_ref::<FullIntraRequest>()
                            {
                                fir.fir.iter().map(|e| e.ssrc).collect()
                            } else {
                                continue;
                            };
                            // An encoder answers a keyframe request with a keyframe: the clip starts over.
                            for v in [camera.as_mut(), screen.as_mut()].into_iter().flatten() {
                                if v.ssrcs.iter().any(|s| asked.contains(s)) {
                                    v.restart();
                                }
                            }
                        }
                    }
                    RTCMessage::DataChannelMessage(_, m) => {
                        if let Ok(DcMessage::Sdp(json)) = DcMessage::decode(&m.data) {
                            describe(
                                &mut pc,
                                &json,
                                dc,
                                &conn,
                                (sid, call),
                                (persist.quirk, &mut held),
                            );
                        }
                    }
                    _ => {}
                }
            }
        }

        let now = Instant::now();
        if connected && dc.is_some() {
            if !joined {
                joined = true;
                if let Some(p) = probe {
                    match back {
                        Some((at, tries)) => {
                            p.rejoined(me, at.elapsed().as_millis() as u64, tries);
                        }
                        None => p.event(me, "join", "connected"),
                    }
                }
            }
            if let Some(data) = watch.take() {
                conn.send(message(sid, call, MessageType::VideoSubscribe, data));
            }
            if persist.voice {
                while next_audio <= now {
                    let seq = mic_seq;
                    send_voice(
                        &mut pc,
                        mic,
                        mic_ssrc,
                        &mut mic_seq,
                        mic_ts,
                        &assets.voice.packets[mic_pos % assets.voice.packets.len()],
                    );
                    if mic_seq < seq
                        && let Some(p) = &probe
                    {
                        p.event(me, "wrap", "tx seq voice");
                    }
                    mic_pos += 1;
                    mic_ts = mic_ts.wrapping_add(960);
                    if mic_ts < 960
                        && let Some(p) = &probe
                    {
                        p.event(me, "wrap", "tx timestamp voice");
                    }
                    next_audio += Duration::from_millis(20);
                }
            } else {
                // A muted microphone sends nothing, and time goes on.
                while next_audio <= now {
                    mic_ts = mic_ts.wrapping_add(960);
                    if mic_ts < 960
                        && let Some(p) = &probe
                    {
                        p.event(me, "wrap", "tx timestamp voice");
                    }
                    next_audio += Duration::from_millis(20);
                }
            }
            for (v, on) in [
                (camera.as_mut(), persist.camera_on),
                (screen.as_mut(), persist.screen_on),
            ] {
                let Some(v) = v else { continue };
                let mask = if v.label == "screen" {
                    persist.layers
                } else {
                    [true; 2]
                };
                for l in 0..v.ssrcs.len() {
                    while v.next[l] <= now {
                        if on {
                            let (seq, ts) = (v.seq[l], v.ts[l]);
                            send_frame(&mut pc, v, l, mask[l % 2]);
                            if let (0, Some(p)) = (l, probe) {
                                for (wrapped, name) in
                                    [(v.seq[0] < seq, "seq"), (v.ts[0] < ts, "timestamp")]
                                {
                                    if wrapped {
                                        p.event(me, "wrap", &format!("tx {name} {}", v.label));
                                    }
                                }
                            }
                        } else {
                            // The camera is off, and time goes on.
                            v.ts[l] = v.ts[l].wrapping_add(90_000 / v.clips[l].fps);
                        }
                        v.next[l] += v.step[l];
                    }
                }
            }
            // Answers a slow client held back are due.
            while let Some(i) = held.iter().position(|(at, _)| *at <= now) {
                let (_, data) = held.remove(i);
                send_sdp(&mut pc, dc, &conn, (sid, call), data);
            }
            if persist.mtu && now >= next_mtu {
                next_mtu = now + Duration::from_millis(500 + rng.below(2000));
                // Datagrams that fill a path's MTU or outgrow it (and fragment), which are not
                // media (rtcd has to tell), and one valid audio packet as large.
                for size in [1400 + rng.below(73) as usize, 3000] {
                    let mut d = rng.bytes(size);
                    d[0] = 0x80;
                    let _ = socket.send_to(&d, server).await;
                }
                let big = rng.bytes(1380);
                send_voice(&mut pc, mic, mic_ssrc, &mut mic_seq, mic_ts, &big);
                tally(3);
            }
            if let Some((kind, until, level)) = persist.hostile {
                if now >= until {
                    persist.hostile = None;
                } else if now >= next_hostile {
                    next_hostile = now + Duration::from_millis(20);
                    let mut hot = Hot {
                        pc: &mut pc,
                        dc,
                        mic,
                        camera: camera.as_mut(),
                        screen: screen.as_mut(),
                        receivers: receivers.values().copied().collect(),
                        now,
                    };
                    tally(hostile::step(kind, &mut hot, &mut rng, level));
                }
            }
        } else {
            next_audio = now;
            for v in [camera.as_mut(), screen.as_mut()].into_iter().flatten() {
                v.next.fill(now);
            }
        }

        let reason = if !joined && started.elapsed() > CONNECT_TIMEOUT {
            Some("the call did not connect in time")
        } else if failed_since.is_some_and(|f| f.elapsed() > ICE_FAIL_GRACE) {
            Some("the media path stayed down")
        } else {
            None
        };
        if let Some(reason) = reason {
            if let Some(p) = probe {
                p.event(me, "error", reason);
            }
            conn.send(ClientMessage::Leave(SessionRef {
                session_id: sid.to_owned(),
            }));
            tokio::time::sleep(Duration::from_millis(100)).await;
            return End::Lost { joined };
        }

        let mut wake = now + Duration::from_millis(20);
        if let Some(t) = pc.poll_timeout() {
            wake = wake.min(t);
        }
        if rx.is_some() {
            wake = wake.min(next_probe);
        }
        if connected && dc.is_some() {
            if persist.voice {
                wake = wake.min(next_audio);
            }
            for v in [camera.as_ref(), screen.as_ref()].into_iter().flatten() {
                for n in &v.next {
                    wake = wake.min(*n);
                }
            }
        }
        let sleep = tokio::time::sleep(wake.saturating_duration_since(Instant::now()));
        tokio::pin!(sleep);
        tokio::select! {
            _ = &mut *stop_rx => {
                if let Some(p) = probe {
                    p.event(me, "leave", "stop");
                }
                conn.send(ClientMessage::Leave(SessionRef { session_id: sid.to_owned() }));
                tokio::time::sleep(Duration::from_millis(100)).await;
                return End::Stopped;
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
            c = ctl_rx.recv() => if let Some(c) = c {
                persist.apply(c);
                match c {
                    Ctl::Announce(kind) => announce(&conn, (sid, call), me, kind),
                    Ctl::Watch(mask) => {
                        let ids: Vec<String> = (0..26u32)
                            .filter(|k| mask >> k & 1 == 1)
                            .map(|k| {
                                if k == 0 { "session-teacher".to_owned() } else { format!("session-s{:02}", k - 1) }
                            })
                            .collect();
                        let data = serde_json::to_vec(&serde_json::json!({ "session_ids": ids })).unwrap();
                        conn.send(message(sid, call, MessageType::VideoSubscribe, data));
                    }
                    Ctl::Renego(n) => {
                        for k in 0..n {
                            let now = Instant::now();
                            if pc.signaling_state() == RTCSignalingState::Stable
                                && let Ok(o) = pc.create_offer(None)
                                && pc.set_local_description(now, o.clone()).is_ok()
                            {
                                last_offer = Some(o);
                            }
                            // The same offer again while its answer is out, alternately on the
                            // data channel and the websocket.
                            if let Some(o) = &last_offer {
                                let via = if k % 2 == 0 { dc } else { None };
                                send_sdp(&mut pc, via, &conn, (sid, call), serde_json::to_vec(o).unwrap());
                            }
                        }
                        tally(u64::from(n));
                    }
                    Ctl::Rebind => {
                        slot += 1;
                        let port = env.udp_base.map_or(0, |b| rebind_port(b, slot));
                        let local = std::env::var("LMS_SIM_LOCAL_IP").unwrap_or_else(|_| "127.0.0.1".into());
                        if let Ok(s) = UdpSocket::bind(format!("{local}:{port}")).await
                            && let Ok(a) = s.local_addr()
                        {
                            // Only the source the server sees changes: the peer connection keeps
                            // the local address it was given, as behind a real NAT.
                            env.rtcd.exempt(a);
                            socket = s;
                            tally(1);
                        }
                    }
                    _ => {}
                }
            },
            m = conn.rx.recv() => match m {
                Some(ClientMessage::Rtc(m)) if m.session_id == sid => match m.kind {
                    MessageType::Sdp => describe(
                        &mut pc,
                        &m.data,
                        None,
                        &conn,
                        (sid, call),
                        (persist.quirk, &mut held),
                    ),
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
                Some(ClientMessage::Close(r)) if r.session_id == sid => {
                    if let Some(p) = probe {
                        p.event(me, "error", "the SFU closed the session");
                    }
                    return End::Lost { joined };
                }
                Some(_) => {}
                None => {
                    if let Some(p) = probe {
                        p.event(me, "error", "the signaling connection closed");
                    }
                    return End::Lost { joined };
                }
            },
            () = &mut sleep => {}
        }
        let _ = pc.handle_timeout(Instant::now());
        if let (Some(rx), Some(p)) = (rx.as_mut(), probe)
            && Instant::now() >= next_probe
        {
            let now = Instant::now();
            next_probe = now + Duration::from_secs(1);
            probe_ticks += 1;
            // A decoder that has no frame for a second asks for a keyframe.
            for id in rx.keyframes_due(now) {
                if let Some((receiver, ssrc)) = receivers.get(&id)
                    && let Some(mut r) = pc.rtp_receiver(*receiver)
                {
                    let _ = r.write_rtcp(
                        now,
                        vec![Box::new(PictureLossIndication {
                            sender_ssrc: 0,
                            media_ssrc: *ssrc,
                        })],
                    );
                }
            }
            if probe_ticks.is_multiple_of(5) {
                let rtt = pc
                    .get_stats(now, StatsSelector::None)
                    .candidate_pairs()
                    .map(|c| c.current_round_trip_time)
                    .fold(0.0, f64::max);
                rx.write(p, &rid, now, (rtt > 0.0).then_some(rtt * 1000.0));
            }
        }
    }
}

/// Sends a description to the SFU: over the data channel when it is open, else the websocket.
fn send_sdp(
    pc: &mut RTCPeerConnection,
    dc: Option<usize>,
    conn: &RtcdConn,
    (sid, call): (&str, &str),
    data: Vec<u8>,
) {
    match dc {
        Some(id) => {
            let frame = DcMessage::Sdp(data).encode();
            let _ = pc
                .data_channel(id)
                .map(|mut d| d.send(Instant::now(), BytesMut::from(&frame[..])));
        }
        None => conn.send(message(sid, call, MessageType::Sdp, data)),
    }
}

/// Tells the SFU what a browser does when it mutes, turns the camera off and so on.
fn announce(conn: &RtcdConn, (sid, call): (&str, &str), me: &str, kind: MessageType) {
    let data = match kind {
        MessageType::VideoOn => serde_json::to_vec(
            &serde_json::json!({ "videoStreamID": format!("camera-stream-{me}") }),
        ),
        MessageType::ScreenOn => serde_json::to_vec(
            &serde_json::json!({ "screenStreamID": format!("screen-stream-{me}") }),
        ),
        _ => Ok(Vec::new()),
    };
    conn.send(message(sid, call, kind, data.unwrap_or_default()));
}

/// Applies a description from the SFU and answers an offer (over the data channel when it came
/// that way), the way `quirk` says: held answers are sent when they are due, see `drive`.
fn describe(
    pc: &mut RTCPeerConnection,
    json: &[u8],
    dc: Option<usize>,
    conn: &RtcdConn,
    (sid, call): (&str, &str),
    (quirk, held): (Quirk, &mut Vec<(Instant, Vec<u8>)>),
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
            if quirk == Quirk::Glare {
                // An offer of its own while the SFU's waits for an answer.
                if let Ok(offer) = pc.create_offer(None)
                    && pc.set_local_description(now, offer.clone()).is_ok()
                {
                    send_sdp(
                        pc,
                        dc,
                        conn,
                        (sid, call),
                        serde_json::to_vec(&offer).unwrap(),
                    );
                }
                return;
            }
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
            match quirk {
                Quirk::Late(pause) => held.push((Instant::now() + pause, data)),
                Quirk::Twice => {
                    send_sdp(pc, dc, conn, (sid, call), data.clone());
                    send_sdp(pc, dc, conn, (sid, call), data);
                }
                _ => send_sdp(pc, dc, conn, (sid, call), data),
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

/// The temporal layer of frames 0, 1, 2, 3 of a clip, over and over (L1T3, as libwebrtc's VP8).
const TEMPORAL_PATTERN: [u8; 4] = [0, 2, 1, 2];

/// Sends the next frame of layer `l` (skipped on the wire when `on` is false, the clip goes on).
fn send_frame(pc: &mut RTCPeerConnection, v: &mut Video, l: usize, on: bool) {
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
    let clip = &v.clips[l];
    let step = v.pos[l] % 4;
    let (frame, key) = clip.frames[v.pos[l] % clip.frames.len()].clone();
    v.pos[l] = (v.pos[l] + 1) % clip.frames.len();
    v.ts[l] = v.ts[l].wrapping_add(90_000 / clip.fps);
    // A browser's picture id is 15 bits with temporal layers; the plain stream's is 7.
    v.picture_id[l] = v.picture_id[l].wrapping_add(1) & if v.temporal { 0x7fff } else { 0x7f };
    let tid = TEMPORAL_PATTERN[step];
    if v.temporal && tid == 0 {
        v.tl0[l] = v.tl0[l].wrapping_add(1);
    }
    if !on {
        return;
    }
    let sync = !key && (tid == 1 || step == 1);
    let pid = v.picture_id[l];
    let chunks: Vec<&[u8]> = frame.chunks(MTU_PAYLOAD).collect();
    for (n, chunk) in chunks.iter().enumerate() {
        let first = n == 0;
        let mut payload = vec![0x80 | if first { 0x10 } else { 0 }];
        if v.temporal {
            // I, L and T set, as libwebrtc does (no key index).
            payload.extend_from_slice(&[0xe0, 0x80 | (pid >> 8) as u8, pid as u8, v.tl0[l]]);
            payload.push(tid << 6 | u8::from(sync) << 5);
        } else {
            payload.extend_from_slice(&[0x80, pid as u8]);
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_simulcast_camera_has_two_layers_and_a_temporal_pattern() {
        let Some(assets) = Assets::load(&std::env::temp_dir().join("lms-sim-assets")).ok() else {
            return;
        };
        let clips = [
            &assets.camera_low,
            &assets.camera_teacher,
            &assets.camera_student,
        ];
        let v = Video::new(
            RTCRtpSenderId::from(0usize),
            "camera",
            vec![1, 2],
            vec![clips[0].clone(), clips[1].clone()],
            (true, false),
            Instant::now(),
        );
        assert_eq!((v.ssrcs.len(), v.step.len(), v.clips.len()), (2, 2, 2));
        assert!(v.step[0] > v.step[1], "the small layer is the slower");
        assert_eq!(TEMPORAL_PATTERN, [0, 2, 1, 2]);
    }

    #[test]
    fn rejoins_and_rebinds_never_share_a_port_and_stay_in_the_block() {
        let base = 40_008;
        let rejoins: Vec<u16> = (0..16).map(|e| rejoin_port(base, e)).collect();
        let rebinds: Vec<u16> = (0..16).map(|s| rebind_port(base, s)).collect();
        assert!(rejoins.iter().all(|p| (base..base + 2).contains(p)));
        assert!(rebinds.iter().all(|p| (base + 2..base + 4).contains(p)));
        assert!(
            rejoins.windows(2).all(|w| w[0] != w[1]),
            "a try is on a new port"
        );
        assert!(
            rebinds.windows(2).all(|w| w[0] != w[1]),
            "a rebind is on a new port"
        );
    }
}
