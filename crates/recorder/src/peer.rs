//! The peer connection to the SFU and the media that arrives on it.
//!
//! One task owns the connection, its UDP socket and the streams being written; it never waits on
//! anything slow. It talks to the call through two channels: commands in (the SFU's descriptions
//! and candidates from the websocket, close) and events out (signals to carry over the websocket,
//! connection state).

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use rtc::interceptor::Registry;
use rtc::peer_connection::configuration::RTCConfigurationBuilder;
use rtc::peer_connection::configuration::interceptor_registry::register_default_interceptors;
use rtc::peer_connection::configuration::media_engine::MediaEngine;
use rtc::peer_connection::event::{RTCDataChannelEvent, RTCPeerConnectionEvent, RTCTrackEvent};
use rtc::peer_connection::message::RTCMessage;
use rtc::peer_connection::sdp::{RTCSdpType, RTCSessionDescription};
use rtc::peer_connection::state::{RTCPeerConnectionState, RTCSignalingState};
use rtc::peer_connection::transport::{
    CandidateConfig, CandidateHostConfig, RTCIceCandidate, RTCIceCandidateInit,
};
use rtc::peer_connection::{RTCPeerConnection, RTCPeerConnectionBuilder};
use rtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication;
use rtc::rtp_transceiver::RTCRtpReceiverId;
use rtc::rtp_transceiver::rtp_sender::RtpCodecKind;
use rtc::sansio::Protocol;
use rtc::shared::{TaggedBytesMut, TransportContext, TransportProtocol};
use rtcd_proto::dc::DcMessage;
use tokio::io::ReadBuf;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

pub use crate::media::Users;
use crate::media::{Recording, StreamInfo, StreamRecorder, track_kind};
use crate::tap::TapPacket;

/// Whether the environment asks for the screen at whatever size rtcd's estimate allows
/// (`SCREEN_QUALITY=auto`) instead of the sharp layer.
fn screen_quality_auto() -> bool {
    std::env::var("SCREEN_QUALITY").is_ok_and(|v| v.trim().eq_ignore_ascii_case("auto"))
}

/// Upkeep of the streams (gaps given up, idle files closed, keyframes asked for).
const HOUSEKEEPING: Duration = Duration::from_millis(250);

/// The UDP sockets of the connection, one per local address: the ICE agent needs to know which of
/// its candidates a datagram reached, and a socket bound to every address cannot say.
#[derive(Debug)]
struct Sockets(Vec<(SocketAddr, UdpSocket)>);

impl Sockets {
    async fn bind() -> std::io::Result<Self> {
        let mut ips = rtcd_net::local_addresses(false).unwrap_or_default();
        ips.retain(|ip| !ip.is_unspecified());
        if ips.is_empty() {
            ips.push(IpAddr::from([127, 0, 0, 1]));
        }
        let mut sockets = Vec::new();
        for ip in ips {
            let socket = UdpSocket::bind(SocketAddr::new(ip, 0)).await?;
            sockets.push((socket.local_addr()?, socket));
        }
        Ok(Self(sockets))
    }

    fn addrs(&self) -> impl Iterator<Item = SocketAddr> + '_ {
        self.0.iter().map(|(a, _)| *a)
    }

    /// Sends from the socket whose address the connection named.
    async fn send(&self, local: SocketAddr, to: SocketAddr, data: &[u8]) -> std::io::Result<usize> {
        let socket = self
            .0
            .iter()
            .find(|(a, _)| *a == local)
            .or_else(|| {
                self.0.iter().find(|(a, _)| {
                    a.is_ipv4() == to.is_ipv4() && a.ip().is_loopback() == to.ip().is_loopback()
                })
            })
            .or_else(|| self.0.first())
            .map(|(_, s)| s)
            .ok_or_else(|| std::io::Error::other("no socket"))?;
        socket.send_to(data, to).await
    }

    /// The next datagram, from whichever socket has one: its length, the local address it reached
    /// and who sent it.
    async fn recv(&self, buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr, SocketAddr)> {
        std::future::poll_fn(|cx| {
            for (local, s) in &self.0 {
                let mut rb = ReadBuf::new(buf);
                match s.poll_recv_from(cx, &mut rb) {
                    Poll::Ready(Ok(from)) => {
                        return Poll::Ready(Ok((rb.filled().len(), *local, from)));
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => {}
                }
            }
            Poll::Pending
        })
        .await
    }

    /// A datagram that is already waiting, if there is one.
    fn try_recv(&self, buf: &mut [u8]) -> Option<(usize, SocketAddr, SocketAddr)> {
        self.0
            .iter()
            .find_map(|(local, s)| s.try_recv_from(buf).ok().map(|(n, from)| (n, *local, from)))
    }
}

#[derive(Debug)]
pub enum Cmd {
    /// The SFU's offer or answer, from the websocket.
    Description {
        kind: RTCSdpType,
        sdp: String,
    },
    Candidate(RTCIceCandidateInit),
    /// A person turned their camera or screen share off (the call's `user_video_off` and
    /// `user_screen_off`): the file ends now, not when the silence has gone on long enough.
    Stopped {
        kind: &'static str,
        session_id: String,
    },
    /// End the connection and finish the files.
    Close,
}

#[derive(Debug)]
pub enum PeerEvent {
    /// A message for the SFU to carry over the websocket (`sdp` or `ice`, with its JSON).
    Signal {
        action: &'static str,
        json: String,
    },
    State(RTCPeerConnectionState),
}

#[derive(Debug)]
pub struct Peer {
    cmd: mpsc::UnboundedSender<Cmd>,
    pub events: mpsc::UnboundedReceiver<PeerEvent>,
    task: JoinHandle<()>,
    /// Our first offer (JSON), to send over the websocket.
    pub offer: String,
}

#[derive(Debug, thiserror::Error)]
pub enum PeerError {
    #[error("{0}")]
    Rtc(String),
    #[error("socket: {0}")]
    Socket(#[from] std::io::Error),
}

fn rtc_err(e: impl std::fmt::Display) -> PeerError {
    PeerError::Rtc(e.to_string())
}

fn host_candidate(addr: SocketAddr) -> Result<RTCIceCandidateInit, PeerError> {
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
    .map_err(rtc_err)?;
    RTCIceCandidate::from(&c).to_json().map_err(rtc_err)
}

impl Peer {
    /// Builds the connection with a data channel (rtcd forwards the others' media once it is
    /// open, and signals over it from then on) and its offer. The recorder never sends media.
    pub async fn start(rec: Arc<Recording>, users: Users) -> Result<Self, PeerError> {
        let sockets = Sockets::bind().await?;

        let mut me = MediaEngine::default();
        me.register_default_codecs().map_err(rtc_err)?;
        // The browser's: NACK, RTCP reports and TWCC feedback. The feedback matters: rtcd raises a
        // shared screen to its sharp layer only for receivers whose bandwidth estimate allows it.
        let registry = register_default_interceptors(Registry::new(), &mut me).map_err(rtc_err)?;
        let mut pc = RTCPeerConnectionBuilder::new()
            .with_configuration(RTCConfigurationBuilder::new().build())
            .with_media_engine(me)
            .with_interceptor_registry(registry)
            .build(Instant::now())
            .map_err(rtc_err)?;
        pc.create_data_channel("calls-dc", None).map_err(rtc_err)?;
        for addr in sockets.addrs() {
            pc.add_local_candidate(host_candidate(addr)?)
                .map_err(rtc_err)?;
        }
        let offer = pc.create_offer(None).map_err(rtc_err)?;
        pc.set_local_description(Instant::now(), offer.clone())
            .map_err(rtc_err)?;
        let offer_json = serde_json::to_string(&offer).map_err(rtc_err)?;

        let (cmd, cmd_rx) = mpsc::unbounded_channel();
        let (ev_tx, events) = mpsc::unbounded_channel();
        let task = Task {
            pc,
            sockets,
            rec,
            users,
            events: ev_tx,
            streams: HashMap::new(),
            ignored: Vec::new(),
            receivers: HashMap::new(),
            dc: None,
            candidates: Vec::new(),
            connected: false,
        };
        let task = tokio::spawn(task.run(cmd_rx));
        Ok(Self {
            cmd,
            events,
            task,
            offer: offer_json,
        })
    }

    pub fn send(&self, cmd: Cmd) {
        let _ = self.cmd.send(cmd);
    }

    /// Ends the connection and waits for the files to be finished.
    pub async fn close(self) {
        let _ = self.cmd.send(Cmd::Close);
        if tokio::time::timeout(Duration::from_secs(30), self.task)
            .await
            .is_err()
        {
            warn!("the peer connection did not finish in time");
        }
    }
}

struct Stream {
    rec: StreamRecorder,
    receiver: RTCRtpReceiverId,
    ssrc: u32,
}

struct Task {
    pc: RTCPeerConnection,
    sockets: Sockets,
    rec: Arc<Recording>,
    users: Users,
    events: mpsc::UnboundedSender<PeerEvent>,
    streams: HashMap<String, Stream>,
    /// Tracks that are not recorded (their codec is not one the player plays).
    ignored: Vec<String>,
    receivers: HashMap<String, RTCRtpReceiverId>,
    dc: Option<usize>,
    /// Candidates that came before the SFU's description.
    candidates: Vec<RTCIceCandidateInit>,
    connected: bool,
}

impl Task {
    async fn run(mut self, mut cmds: mpsc::UnboundedReceiver<Cmd>) {
        let mut buf = vec![0u8; 2048];
        let mut next_housekeeping = Instant::now() + HOUSEKEEPING;
        loop {
            self.pump(Instant::now()).await;
            let now = Instant::now();
            let mut wake = next_housekeeping;
            if let Some(t) = self.pc.poll_timeout() {
                wake = wake.min(t);
            }
            for s in self.streams.values() {
                if let Some(d) = s.rec.deadline() {
                    wake = wake.min(d);
                }
            }
            let sleep = tokio::time::sleep(wake.saturating_duration_since(now));
            tokio::pin!(sleep);
            tokio::select! {
                biased;
                cmd = cmds.recv() => match cmd {
                    Some(Cmd::Close) | None => break,
                    Some(cmd) => self.command(cmd),
                },
                read = self.sockets.recv(&mut buf) => {
                    let mut got = Some(read);
                    let mut batch = 0;
                    while let Some(read) = got.take() {
                        match read {
                            Ok((n, local, from)) => self.read(local, from, &buf[..n]),
                            Err(e) => debug!(error = %e, "socket read failed"),
                        }
                        batch += 1;
                        if batch < 64 {
                            got = self.sockets.try_recv(&mut buf).map(Ok);
                        }
                    }
                }
                () = &mut sleep => {}
            }
            let now = Instant::now();
            let _ = self.pc.handle_timeout(now);
            if now >= next_housekeeping {
                self.housekeeping(now);
                next_housekeeping = now + HOUSEKEEPING;
            } else {
                // A gap may have been waited for long enough.
                self.housekeeping_gaps(now);
            }
        }
        self.finish();
    }

    fn read(&mut self, local: SocketAddr, from: SocketAddr, data: &[u8]) {
        let msg = TaggedBytesMut {
            now: Instant::now(),
            transport: TransportContext {
                local_addr: local,
                peer_addr: from,
                ecn: None,
                transport_protocol: TransportProtocol::UDP,
            },
            message: BytesMut::from(data),
        };
        if let Err(e) = self.pc.handle_read(msg) {
            debug!(error = %e, "packet refused");
        }
    }

    /// Sends what the connection wants sent, then handles its events and the media it delivers,
    /// until it has nothing more.
    async fn pump(&mut self, now: Instant) {
        for _ in 0..8 {
            let mut moved = false;
            while let Some(w) = self.pc.poll_write() {
                moved = true;
                if let Err(e) = self
                    .sockets
                    .send(w.transport.local_addr, w.transport.peer_addr, &w.message)
                    .await
                {
                    debug!(error = %e, to = %w.transport.peer_addr, "socket write failed");
                }
            }
            while let Some(ev) = self.pc.poll_event() {
                moved = true;
                self.event(ev);
            }
            while let Some(m) = self.pc.poll_read() {
                moved = true;
                match m.message {
                    RTCMessage::RtpPacket(track, packet) => self.rtp(now, &track, packet),
                    RTCMessage::DataChannelMessage(_, m) => self.dc_message(&m.data),
                    _ => {}
                }
            }
            if !moved {
                break;
            }
        }
    }

    fn event(&mut self, ev: RTCPeerConnectionEvent) {
        match ev {
            RTCPeerConnectionEvent::OnConnectionStateChangeEvent(s) => {
                self.connected = s == RTCPeerConnectionState::Connected;
                let _ = self.events.send(PeerEvent::State(s));
            }
            RTCPeerConnectionEvent::OnIceCandidateEvent(e) => {
                if let Ok(init) = e.candidate.to_json()
                    && let Ok(json) = serde_json::to_string(&init)
                {
                    let _ = self.events.send(PeerEvent::Signal {
                        action: "ice",
                        json,
                    });
                }
            }
            RTCPeerConnectionEvent::OnDataChannel(RTCDataChannelEvent::OnOpen(id)) => {
                self.dc = Some(id);
                // A recording keeps what it gets: the shared screen at its sharpest from the first
                // frame, not at the size a bandwidth estimate (which a receiver of the small layer
                // cannot raise) allows. rtcd honours it; the Go service ignores the frame. The
                // sharp layer is two to three times the bytes of the small one, in storage and in
                // every replay: SCREEN_QUALITY=auto leaves the choice to rtcd.
                if !screen_quality_auto() {
                    self.dc_send(&DcMessage::SharpScreen(true));
                }
            }
            RTCPeerConnectionEvent::OnDataChannel(RTCDataChannelEvent::OnClose(_)) => {
                self.dc = None
            }
            RTCPeerConnectionEvent::OnTrack(RTCTrackEvent::OnOpen(init)) => {
                self.track(
                    init.track_id,
                    init.stream_ids.first().cloned().unwrap_or_default(),
                    init.receiver_id,
                    init.ssrc,
                );
            }
            RTCPeerConnectionEvent::OnTrack(RTCTrackEvent::OnClose(id)) => self.track_closed(&id),
            _ => {}
        }
    }

    /// The SFU removed a track (a camera or a share ended): its file ends now, with the picture
    /// that was on screen held until this moment.
    fn track_closed(&mut self, track_id: &str) {
        self.ignored.retain(|i| i != track_id);
        self.receivers.remove(track_id);
        if let Some(mut s) = self.streams.remove(track_id) {
            s.rec.finish(Instant::now(), "the track was removed");
            if s.rec.lost > 0 {
                info!(
                    kind = s.rec.info().kind,
                    session_id = s.rec.info().session_id,
                    lost = s.rec.lost,
                    "packets were lost"
                );
            }
        }
    }

    /// A track opened: it is recorded if it is one of ours and in a codec the player plays.
    fn track(
        &mut self,
        track_id: String,
        session_id: String,
        receiver: RTCRtpReceiverId,
        ssrc: u32,
    ) {
        let Some(kind) = track_kind(&track_id) else {
            warn!(id = track_id, "unknown track");
            self.ignored.push(track_id);
            return;
        };
        let video = self
            .pc
            .rtp_receiver(receiver)
            .map(|r| r.track().kind() == RtpCodecKind::Video)
            .unwrap_or(false);
        let mime = self
            .pc
            .rtp_receiver(receiver)
            .and_then(|r| {
                r.track()
                    .codec(ssrc)
                    .map(|c| c.mime_type.to_ascii_lowercase())
            })
            .unwrap_or_default();
        let wanted = if video { "video/vp8" } else { "audio/opus" };
        if mime != wanted {
            warn!(id = track_id, codec = mime, "track codec not recorded");
            self.ignored.push(track_id);
            return;
        }
        info!(kind, session_id, "recording a stream");
        let rec = StreamRecorder::new(
            StreamInfo {
                kind,
                session_id,
                video,
            },
            self.rec.clone(),
            self.users.clone(),
        );
        self.streams.insert(
            track_id.clone(),
            Stream {
                rec,
                receiver,
                ssrc,
            },
        );
        self.receivers.insert(track_id, receiver);
    }

    fn rtp(&mut self, now: Instant, track: &str, packet: rtc::rtp::packet::Packet) {
        let Some(s) = self.streams.get_mut(track) else {
            return;
        };
        let tap = TapPacket {
            seq: packet.header.sequence_number,
            ts: packet.header.timestamp,
            marker: packet.header.marker,
            payload: packet.payload,
            arrived: now,
        };
        if s.rec.packet(tap) {
            Self::keyframe(&mut self.pc, s, now);
        }
    }

    /// Asks the sender of a stream for a keyframe.
    fn keyframe(pc: &mut RTCPeerConnection, s: &Stream, now: Instant) {
        if let Some(mut rx) = pc.rtp_receiver(s.receiver)
            && let Err(e) = rx.write_rtcp(
                now,
                vec![Box::new(PictureLossIndication {
                    sender_ssrc: 0,
                    media_ssrc: s.ssrc,
                })],
            )
        {
            debug!(error = %e, "keyframe request not sent");
        }
    }

    fn housekeeping(&mut self, now: Instant) {
        for s in self.streams.values_mut() {
            if s.rec.tick(now) {
                Self::keyframe(&mut self.pc, s, now);
            }
        }
    }

    fn housekeeping_gaps(&mut self, now: Instant) {
        for s in self.streams.values_mut() {
            if s.rec.deadline().is_some_and(|d| d <= now) && s.rec.tick(now) {
                Self::keyframe(&mut self.pc, s, now);
            }
        }
    }

    fn command(&mut self, cmd: Cmd) {
        match cmd {
            Cmd::Description { kind, sdp } => {
                let desc = match kind {
                    RTCSdpType::Offer => RTCSessionDescription::offer(sdp),
                    RTCSdpType::Answer => RTCSessionDescription::answer(sdp),
                    _ => return,
                };
                match desc {
                    Ok(desc) => self.describe(desc, false),
                    Err(e) => warn!(error = %e, "unreadable description"),
                }
            }
            Cmd::Candidate(c) => {
                if self.pc.remote_description().is_none() {
                    self.candidates.push(c);
                } else if let Err(e) = self.pc.add_remote_candidate(c) {
                    warn!(error = %e, "candidate refused");
                }
            }
            Cmd::Stopped { kind, session_id } => {
                let now = Instant::now();
                for s in self.streams.values_mut() {
                    let i = s.rec.info();
                    if i.kind == kind && i.session_id == session_id {
                        s.rec.stop(now, "the sender turned it off");
                    }
                }
            }
            Cmd::Close => {}
        }
    }

    fn dc_message(&mut self, data: &[u8]) {
        match DcMessage::decode(data) {
            Ok(DcMessage::Sdp(json)) => {
                match serde_json::from_slice::<RTCSessionDescription>(&json) {
                    Ok(desc) => self.describe(desc, true),
                    Err(e) => warn!(error = %e, "unreadable SDP on the data channel"),
                }
            }
            Ok(DcMessage::Ping) => {
                self.dc_send(&DcMessage::Pong);
            }
            Ok(_) => {}
            Err(e) => debug!(error = %e, "unreadable data channel message"),
        }
    }

    fn dc_send(&mut self, msg: &DcMessage) -> bool {
        let Some(id) = self.dc else { return false };
        let frame = msg.encode();
        self.pc
            .data_channel(id)
            .is_some_and(|mut dc| dc.send(Instant::now(), BytesMut::from(&frame[..])).is_ok())
    }

    /// Applies the SFU's offer or answer, answering an offer.
    fn describe(&mut self, desc: RTCSessionDescription, over_dc: bool) {
        let now = Instant::now();
        let is_offer = desc.sdp_type == RTCSdpType::Offer;
        if is_offer && self.pc.signaling_state() != RTCSignalingState::Stable {
            warn!("remote description refused: not stable");
            return;
        }
        if let Err(e) = self.pc.set_remote_description(now, desc) {
            warn!(error = %e, "remote description refused");
            return;
        }
        for c in std::mem::take(&mut self.candidates) {
            if let Err(e) = self.pc.add_remote_candidate(c) {
                warn!(error = %e, "candidate refused");
            }
        }
        if !is_offer {
            return;
        }
        let answer = match self.pc.create_answer(None) {
            Ok(a) => a,
            Err(e) => {
                warn!(error = %e, "cannot answer");
                return;
            }
        };
        if let Err(e) = self.pc.set_local_description(now, answer.clone()) {
            warn!(error = %e, "cannot answer");
            return;
        }
        let Ok(json) = serde_json::to_vec(&answer) else {
            return;
        };
        // Over the data channel while the connection is up, else (the first offer) over the
        // websocket.
        if (over_dc || self.connected) && self.dc_send(&DcMessage::Sdp(json.clone())) {
            return;
        }
        let _ = self.events.send(PeerEvent::Signal {
            action: "sdp",
            json: String::from_utf8_lossy(&json).into_owned(),
        });
    }

    /// The call ended: every stream is finished and its file closed.
    fn finish(&mut self) {
        let now = Instant::now();
        for (_, mut s) in self.streams.drain() {
            s.rec.finish(now, "the call ended");
            if s.rec.lost > 0 {
                info!(
                    kind = s.rec.info().kind,
                    session_id = s.rec.info().session_id,
                    lost = s.rec.lost,
                    "packets were lost"
                );
            }
        }
        let _ = Protocol::close(&mut self.pc);
    }
}
