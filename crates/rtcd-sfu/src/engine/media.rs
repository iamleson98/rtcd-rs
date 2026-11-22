//! Tracks and packets: what a participant sends, who receives it, and the forwarding between.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use rtc::peer_connection::event::{
    RTCDataChannelEvent, RTCPeerConnectionEvent, RTCTrackEvent, RTCTrackEventInit,
};
use rtc::peer_connection::message::RTCMessage;
use rtc::peer_connection::state::RTCPeerConnectionState;
use rtc::rtp_transceiver::rtp_sender::RtpCodecKind;
use rtc::sansio::Protocol;
use rtc::shared::marshal::MarshalSize;
use rtcd_proto::MessageType;
use serde::Deserialize;
use tracing::{debug, info, warn};

use super::Engine;
use super::record::Tap;
use super::session::SessionKey;
use super::session::{DownPhase, DownStream, Lifecycle, Session, Source, SourceKey, SubRef};
use super::types::CloseReason;
use super::video::Want;
use crate::ids::{TrackType, track_id};
use crate::layers::Layer;
use crate::metrics::Direction;
use crate::peer::{AUDIO_LEVEL_URI, OPUS_PAYLOAD_TYPE, opus_codec};
use crate::rate::RateMonitor;
use crate::red::{self, MIME_TYPE_RED};
use crate::rewrite::Rewriter;
use crate::vad::{Monitor, VadConfig};
use crate::vp8;

/// Who receives a stream: those it feeds, and those about to be switched onto it.
#[derive(Debug)]
pub(crate) struct Fan {
    pub serving: Vec<SubRef>,
    /// Receivers that take this stream over from another at its next keyframe, or start with it.
    pub waiting: Vec<SubRef>,
    pub vp8: bool,
    /// What the stream carries, for the uplink counter.
    pub ty: TrackType,
    /// The next packet follows a stretch that was left out: receivers carry on their sequence
    /// numbers from where they were.
    pub resume: bool,
    /// The newest sequence number seen on the stream: a receiver waiting for a keyframe joins at
    /// the first packet of one only if it is the newest, else the packets after it have gone by.
    pub newest: Option<u16>,
    /// Where a copy of every packet goes, if the stream is being recorded.
    pub tap: Option<Tap>,
    /// The packets before the current one of a voice, for the redundant audio receivers are sent.
    pub red: red::Encoder,
    /// The payload type the sender's voice is wrapped in as redundant audio, if it negotiated it.
    pub uplink_red: Option<u8>,
}

#[derive(Deserialize)]
struct ScreenAnnouncement {
    #[serde(rename = "screenStreamID")]
    screen_stream_id: String,
}

#[derive(Deserialize)]
struct VideoAnnouncement {
    #[serde(rename = "videoStreamID")]
    video_stream_id: String,
}

impl Engine {
    /// Handles what a connection reported since last looked at.
    pub(super) fn process_events(&mut self, key: SessionKey, now: Instant) {
        loop {
            let Some(s) = self.sessions.get_mut(key) else {
                return;
            };
            let Some(event) = s.pc.poll_event() else {
                return;
            };
            match event {
                RTCPeerConnectionEvent::OnIceCandidateEvent(e) => {
                    if let Ok(init) = e.candidate.to_json() {
                        let data = serde_json::json!({ "type": "candidate", "candidate": init })
                            .to_string()
                            .into_bytes();
                        self.emit(key, MessageType::Ice, data);
                    }
                }
                RTCPeerConnectionEvent::OnConnectionStateChangeEvent(state) => {
                    self.connection_state(key, state, now)
                }
                RTCPeerConnectionEvent::OnTrack(RTCTrackEvent::OnOpen(init)) => {
                    self.track_opened(key, init, now)
                }
                RTCPeerConnectionEvent::OnTrack(RTCTrackEvent::OnClose(id)) => {
                    self.track_closed(key, &id.to_string(), now)
                }
                RTCPeerConnectionEvent::OnDataChannel(RTCDataChannelEvent::OnOpen(id)) => {
                    self.on_dc_open(key, id, now)
                }
                RTCPeerConnectionEvent::OnDataChannel(RTCDataChannelEvent::OnClose(id)) => {
                    self.on_dc_closed(key, id)
                }
                _ => {}
            }
        }
    }

    fn connection_state(&mut self, key: SessionKey, state: RTCPeerConnectionState, now: Instant) {
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        match state {
            RTCPeerConnectionState::Connected => {
                if s.lifecycle != Lifecycle::Connected {
                    s.lifecycle = Lifecycle::Connected;
                    self.metrics.connection_time(
                        &s.cfg.group_id,
                        now.saturating_duration_since(s.created_at),
                    );
                }
                self.metrics.connection_state("connected");
                info!(session = %s.cfg.session_id, "connected");
            }
            RTCPeerConnectionState::Disconnected => {
                self.metrics.connection_state("disconnected");
                debug!(session = %s.cfg.session_id, "disconnected");
            }
            RTCPeerConnectionState::Failed => {
                self.metrics.connection_state("failed");
                self.close_session(key, CloseReason::ConnectionLost, now);
            }
            RTCPeerConnectionState::Closed => {
                self.close_session(key, CloseReason::ConnectionLost, now)
            }
            _ => {}
        }
    }

    /// Drains what a connection received: media to forward, data channel messages and feedback to
    /// act on. Returns the packets to forward, or `None` if the session ended meanwhile.
    pub(super) fn process_reads(
        &mut self,
        key: SessionKey,
        now: Instant,
    ) -> Option<Vec<(u32, rtc::rtp::Packet)>> {
        let mut rtp = Vec::new();
        loop {
            let s = self.sessions.get_mut(key)?;
            let Some(msg) = s.pc.poll_read() else { break };
            match msg.message {
                RTCMessage::RtpPacket(_, packet) => {
                    let Some(&source) = s.up.by_ssrc.get(&packet.header.ssrc) else {
                        continue;
                    };
                    if self.uplink_accepts(key, source, &packet, now) {
                        rtp.push((source, packet));
                    }
                }
                RTCMessage::RtcpPacket(_, packets) => self.receiver_feedback(key, &packets, now),
                RTCMessage::DataChannelMessage(id, m) => {
                    self.on_dc_message(key, id, m.is_string, m.data, now);
                    self.sessions.get(key)?;
                }
                _ => {}
            }
        }
        Some(rtp)
    }

    /// Bookkeeping for a packet arriving from a participant; false if it must not be forwarded.
    fn uplink_accepts(
        &mut self,
        key: SessionKey,
        source: u32,
        packet: &rtc::rtp::Packet,
        now: Instant,
    ) -> bool {
        let Some(s) = self.sessions.get_mut(key) else {
            return false;
        };
        let ext_id = s.up.audio_level_ext;
        let Some(src) = s.up.sources.iter_mut().find(|x| x.id == source) else {
            return false;
        };
        src.last_packet = Some(now);
        match src.ty {
            TrackType::Voice => {
                if s.up.muted {
                    src.gap = true;
                    return false;
                }
                // The level is the low seven bits; the top bit says whether the frame is voice.
                let level = ext_id.and_then(|id| {
                    packet
                        .header
                        .get_extension_payload(id)
                        .and_then(|e| e.first())
                        .map(|b| b & 0x7f)
                });
                if let (Some(level), Some(vad)) = (level, s.up.vad.as_mut())
                    && let Some(on) = vad.push_packet(level, packet.header.timestamp, now)
                {
                    self.emit(
                        key,
                        if on {
                            MessageType::VoiceOn
                        } else {
                            MessageType::VoiceOff
                        },
                        Vec::new(),
                    );
                }
                // A sender that reports no level cannot be ranked: it is always forwarded.
                let forward = match (level, self.sessions.get(key)) {
                    (Some(level), Some(s)) => self
                        .calls
                        .get_mut(&s.call)
                        .is_none_or(|c| c.voices.hear(key, level, now)),
                    _ => true,
                };
                self.note_voice_gap(key, source, forward);
                forward
            }
            TrackType::ScreenAudio => true,
            TrackType::Screen | TrackType::Video => {
                let small_video = src.layer == Layer::Low;
                let ty = src.ty;
                let size = packet.marshal_size();
                src.rate.push(now, size);
                if let Some(t) = src.temporal_rates.as_mut() {
                    t.count_keyframe(
                        packet.header.timestamp,
                        size,
                        vp8::is_keyframe(&packet.payload),
                        packet.header.marker,
                    );
                    if let Some(layer) =
                        vp8::Descriptor::parse(&packet.payload).and_then(|d| d.temporal)
                    {
                        t.seen = true;
                        match layer.tid {
                            0 => {
                                t.base.push(now, size);
                                t.count_base_frame(packet.header.timestamp, now);
                            }
                            1 => t.second.push(now, size),
                            _ => {}
                        }
                    }
                }
                if small_video {
                    self.watch_large_layer(key, ty, now);
                }
                true
            }
        }
    }

    /// Remembers that a voice was left out, and when it comes back tells its receivers to carry on
    /// their sequence numbers (see `Rewriter::resume_after_gap`): otherwise a listener sees the
    /// packets left out as lost, at the first syllable of every turn.
    fn note_voice_gap(&mut self, key: SessionKey, source: u32, forwarded: bool) {
        let Some(src) = self
            .sessions
            .get_mut(key)
            .and_then(|s| s.up.sources.iter_mut().find(|x| x.id == source))
        else {
            return;
        };
        if !forwarded {
            src.gap = true;
        } else if src.gap {
            src.gap = false;
            if let Some(fan) = self.fanout.get_mut(&SourceKey {
                session: key,
                source,
            }) {
                fan.resume = true;
            }
        }
    }

    /// A participant's track started to arrive.
    fn track_opened(&mut self, key: SessionKey, init: RTCTrackEventInit, now: Instant) {
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        // A renegotiation can announce a stream that is already being received (Chrome's does):
        // it stays the source it was, or the receivers waiting on it would wait on a stale id.
        if let Some(known) = s.up.sources.iter().find(|x| x.ssrc == init.ssrc) {
            if known.receiver == init.receiver_id {
                return;
            }
            let stale = known.id;
            self.remove_source(key, stale, now);
        }
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        let Some(mut receiver) = s.pc.rtp_receiver(init.receiver_id) else {
            return;
        };
        let track = receiver.track();
        let kind = track.kind();
        let Some(codec) = track.codec(init.ssrc).cloned() else {
            warn!(session = %s.cfg.session_id, ssrc = init.ssrc, "track without a codec");
            return;
        };
        // A voice a sender wraps in redundant audio is Opus to everyone downstream: `forward`
        // takes the packet out of it.
        let codec = if codec.mime_type.eq_ignore_ascii_case(MIME_TYPE_RED) {
            opus_codec().rtp_codec
        } else {
            codec
        };
        let uplink_red = receiver
            .get_parameters()
            .rtp_parameters
            .codecs
            .iter()
            .find(|c| c.rtp_codec.mime_type.eq_ignore_ascii_case(MIME_TYPE_RED))
            .map(|c| c.payload_type);
        let audio_level = receiver
            .get_parameters()
            .rtp_parameters
            .header_extensions
            .iter()
            .find(|e| e.uri == AUDIO_LEVEL_URI)
            .map(|e| e.id as u8);
        let stream_id = init.stream_ids.first().cloned().unwrap_or_default();
        let layer = Layer::from_rid(init.rid.as_deref());

        let ty = match kind {
            RtpCodecKind::Audio
                if !s.up.screen_stream_id.is_empty() && stream_id == s.up.screen_stream_id =>
            {
                TrackType::ScreenAudio
            }
            RtpCodecKind::Audio => TrackType::Voice,
            RtpCodecKind::Video
                if !s.up.screen_stream_id.is_empty() && stream_id == s.up.screen_stream_id =>
            {
                TrackType::Screen
            }
            RtpCodecKind::Video
                if !s.up.video_stream_id.is_empty() && stream_id == s.up.video_stream_id =>
            {
                TrackType::Video
            }
            _ => {
                warn!(session = %s.cfg.session_id, stream = %stream_id, "video track of an unannounced stream");
                self.metrics.error(&s.cfg.group_id, "track");
                return;
            }
        };
        let entropy = self.rng.next_u64();
        let out_track_id =
            s.up.out_ids
                .entry(ty)
                .or_insert_with(|| track_id(ty, &s.cfg.session_id, entropy))
                .clone();
        if ty == TrackType::Voice {
            if audio_level.is_some() && s.up.vad.is_none() {
                s.up.vad = Some(Monitor::new(VadConfig::default()));
            }
            s.up.audio_level_ext = audio_level;
        }
        let id = s.up.next_source;
        s.up.next_source += 1;
        s.up.by_ssrc.insert(init.ssrc, id);
        let window = Duration::from_secs(if layer == Layer::High { 2 } else { 5 });
        let is_vp8 = codec.mime_type.eq_ignore_ascii_case("video/vp8");
        // The base temporal layer has a frame in four, a keyframe among them now and then: it is
        // measured over longer, so one keyframe does not read as a rate.
        let temporal_rates = (is_vp8
            && (ty == TrackType::Screen || ty == TrackType::Video && self.cfg.camera_budget))
            .then(|| super::session::TemporalRates::new(window * 2));
        s.up.sources.push(Source {
            id,
            ssrc: init.ssrc,
            ty,
            layer,
            codec: codec.clone(),
            track_id: init.track_id.clone(),
            since: now,
            receiver: init.receiver_id,
            out_track_id,
            rate: RateMonitor::new(window),
            temporal_rates,
            last_packet: Some(now),
            last_pli: None,
            bypassed: false,
            gap: false,
        });
        info!(session = %s.cfg.session_id, %ty, ?layer, ssrc = init.ssrc, "track received");
        self.metrics
            .track_added(&s.cfg.group_id, Direction::In, &codec.mime_type);
        self.fanout.insert(
            SourceKey {
                session: key,
                source: id,
            },
            Fan {
                serving: Vec::new(),
                waiting: Vec::new(),
                vp8: is_vp8,
                ty,
                resume: false,
                newest: None,
                tap: None,
                red: red::Encoder::default(),
                uplink_red,
            },
        );

        self.record_sync(key, ty, now);
        self.sync_call_stream(key, ty, now);
    }

    /// A participant's track ended.
    fn track_closed(&mut self, key: SessionKey, track: &str, now: Instant) {
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        let ended: Vec<u32> =
            s.up.sources
                .iter()
                .filter(|x| x.track_id == track)
                .map(|x| x.id)
                .collect();
        let mut types = HashSet::new();
        for id in ended {
            if let Some(ty) = self.remove_source(key, id, now) {
                types.insert(ty);
            }
        }
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        for ty in &types {
            // A voice still offered keeps its id: its receivers hold a reserved stream under it.
            let reserved = *ty == TrackType::Voice && s.up.voice_offered;
            if !reserved && !s.up.sources.iter().any(|x| x.ty == *ty) {
                s.up.out_ids.remove(ty);
            }
        }
        // Receivers may have another source for what ended (the other layer of a camera).
        for ty in types {
            self.sync_call_stream(key, ty, now);
        }
    }

    /// Forgets a source; returns its type. Receivers it fed are left to the caller to re-sync.
    pub(super) fn remove_source(
        &mut self,
        key: SessionKey,
        source: u32,
        now: Instant,
    ) -> Option<TrackType> {
        let s = self.sessions.get_mut(key)?;
        let pos = s.up.sources.iter().position(|x| x.id == source)?;
        let src = s.up.sources.remove(pos);
        s.up.by_ssrc.remove(&src.ssrc);
        self.metrics
            .track_removed(&s.cfg.group_id, Direction::In, &src.codec.mime_type);
        if let Some(fan) = self.fanout.remove(&SourceKey {
            session: key,
            source,
        }) {
            for sub in fan.serving.into_iter().chain(fan.waiting) {
                self.source_gone(sub, source);
            }
            // Another layer of it takes its place in the recording at the next upkeep.
            if let Some(tap) = fan.tap {
                self.record_closed(tap, now);
            }
        }
        Some(src.ty)
    }

    /// A source a stream was fed from or waiting for has ended.
    fn source_gone(&mut self, sub: SubRef, source: u32) {
        let Some(r) = self.sessions.get_mut(sub.session) else {
            return;
        };
        let Some(ds) = r.down.get_mut(sub.stream) else {
            return;
        };
        if ds.source == Some(source) {
            ds.source = None;
        }
        if ds.target == Some(source) {
            ds.target = None;
        }
        // Another source may take over, which `sync_call_stream` arranges; until then a stream with
        // nothing to send stays in the connection, silent.
        if ds.source.is_none() && ds.target.is_none() {
            let publisher = ds.publisher;
            let ty = ds.ty;
            r.down.drop_stream(sub.stream);
            let _ = (publisher, ty);
        }
        self.mark_dirty(sub.session);
    }

    /// Re-evaluates, for every ready member of the call, what it gets of `publisher`'s `ty` stream.
    pub(super) fn sync_call_stream(&mut self, publisher: SessionKey, ty: TrackType, now: Instant) {
        for member in self.call_members(publisher) {
            if member != publisher && self.sessions.get(member).is_some_and(|m| m.down.ready) {
                self.sync_stream(member, publisher, ty, now);
            }
        }
    }

    /// Gives a participant who just became ready every stream already in the call.
    pub(super) fn attach_existing_tracks(&mut self, key: SessionKey, now: Instant) {
        for member in self.call_members(key) {
            if member == key {
                continue;
            }
            for ty in TrackType::ALL {
                self.sync_stream(key, member, ty, now);
            }
        }
    }

    /// Makes what `receiver` gets of `publisher`'s `ty` stream match what it should get.
    pub(super) fn sync_stream(
        &mut self,
        receiver: SessionKey,
        publisher: SessionKey,
        ty: TrackType,
        now: Instant,
    ) {
        let want = self.desired_source(receiver, publisher, ty);
        let Some(r) = self.sessions.get(receiver) else {
            return;
        };
        let have = r
            .down
            .iter()
            .find(|(_, d)| {
                d.publisher == publisher
                    && d.ty == ty
                    && !matches!(d.phase, DownPhase::PendingRemove | DownPhase::Removing)
            })
            .map(|(id, d)| {
                (
                    id,
                    d.source,
                    d.target,
                    d.codec.mime_type.clone(),
                    d.reserved,
                )
            });
        match (have, want) {
            (None, None) => {}
            (Some((id, ..)), None) => self.detach_stream(receiver, id),
            (None, Some(Want::Source(src))) => {
                self.attach_stream(receiver, publisher, ty, src, now)
            }
            (None, Some(Want::Reserved)) => self.attach_reserved(receiver, publisher),
            // The reservation is kept until the first packet gives it a source.
            (Some((_, _, _, _, true)), Some(Want::Reserved)) => {}
            (Some((id, _, _, mime, true)), Some(Want::Source(src))) => {
                self.fill_reserved(receiver, publisher, id, src, &mime, now)
            }
            // A stream that had a source it no longer has: replaced by a reservation.
            (Some((id, ..)), Some(Want::Reserved)) => {
                self.detach_stream(receiver, id);
                self.attach_reserved(receiver, publisher);
            }
            (Some((id, source, target, mime, false)), Some(Want::Source(src))) => {
                if target.or(source) == Some(src) {
                    return;
                }
                let same_codec = self
                    .sessions
                    .get(publisher)
                    .and_then(|p| p.up.sources.iter().find(|x| x.id == src))
                    .is_some_and(|x| x.codec.mime_type.eq_ignore_ascii_case(&mime));
                if same_codec && ty.is_video() {
                    self.begin_switch(receiver, id, publisher, src, now);
                } else {
                    // A different codec cannot continue the stream: replace it.
                    self.detach_stream(receiver, id);
                    self.attach_stream(receiver, publisher, ty, src, now);
                }
            }
        }
    }

    /// Adds a voice stream that has no source yet: the publisher offered audio to send its voice
    /// in, so the stream goes into the connection now and the first packet needs no offer.
    fn attach_reserved(&mut self, receiver: SessionKey, publisher: SessionKey) {
        let ssrc = self.rng.ssrc();
        let Some(p) = self.sessions.get(publisher) else {
            return;
        };
        let Some(out_track_id) = p.up.out_ids.get(&TrackType::Voice).cloned() else {
            return;
        };
        let stream = DownStream {
            publisher,
            publisher_session_id: p.cfg.session_id.clone(),
            ty: TrackType::Voice,
            out_track_id,
            ssrc,
            codec: opus_codec().rtp_codec,
            phase: DownPhase::PendingAdd,
            sender: None,
            payload_type_known: false,
            red_payload_type: None,
            red_hold: red::MAX_REDUNDANT,
            reserved: true,
            source: None,
            target: None,
            rewrite: Rewriter::new(ssrc, 0, false),
            temporal: Default::default(),
            paused: false,
            paused_at: None,
            pausing: false,
            budget_small: false,
        };
        let Some(r) = self.sessions.get_mut(receiver) else {
            return;
        };
        r.down.insert(stream);
        self.mark_dirty(receiver);
    }

    /// The first packet of a voice arrived: its reserved stream is fed from it. Nothing is
    /// offered: the stream is, or is about to be, in the connection as it was reserved.
    fn fill_reserved(
        &mut self,
        receiver: SessionKey,
        publisher: SessionKey,
        stream: u32,
        source: u32,
        mime: &str,
        now: Instant,
    ) {
        let Some(codec) = self
            .sessions
            .get(publisher)
            .and_then(|p| p.up.sources.iter().find(|x| x.id == source))
            .map(|x| x.codec.mime_type.clone())
        else {
            return;
        };
        if !codec.eq_ignore_ascii_case(mime) {
            // Not the codec the stream was reserved for: replace it.
            self.detach_stream(receiver, stream);
            self.attach_stream(receiver, publisher, TrackType::Voice, source, now);
            return;
        }
        let Some(ds) = self
            .sessions
            .get_mut(receiver)
            .and_then(|r| r.down.get_mut(stream))
        else {
            return;
        };
        ds.reserved = false;
        ds.source = Some(source);
        let Some(fan) = self.fanout.get_mut(&SourceKey {
            session: publisher,
            source,
        }) else {
            return;
        };
        fan.serving.push(SubRef {
            session: receiver,
            stream,
        });
    }

    fn attach_stream(
        &mut self,
        receiver: SessionKey,
        publisher: SessionKey,
        ty: TrackType,
        source: u32,
        now: Instant,
    ) {
        let ssrc = self.rng.ssrc();
        let Some(p) = self.sessions.get(publisher) else {
            return;
        };
        let Some(src) = p.up.sources.iter().find(|x| x.id == source) else {
            return;
        };
        let is_vp8 = src.codec.mime_type.eq_ignore_ascii_case("video/vp8");
        let video = ty.is_video();
        let stream = DownStream {
            publisher,
            publisher_session_id: p.cfg.session_id.clone(),
            ty,
            out_track_id: src.out_track_id.clone(),
            ssrc,
            codec: src.codec.clone(),
            phase: DownPhase::PendingAdd,
            sender: None,
            payload_type_known: false,
            red_payload_type: None,
            // The packets before the first it is sent are not the receiver's.
            red_hold: red::MAX_REDUNDANT,
            // Video starts at a keyframe; audio at once.
            source: (!video).then_some(source),
            target: video.then_some(source),
            rewrite: Rewriter::new(ssrc, 0, is_vp8),
            temporal: Default::default(),
            paused: false,
            paused_at: None,
            pausing: false,
            budget_small: false,
            reserved: false,
        };
        let Some(r) = self.sessions.get_mut(receiver) else {
            return;
        };
        let id = r.down.insert(stream);
        let fan = self
            .fanout
            .get_mut(&SourceKey {
                session: publisher,
                source,
            })
            .expect("a live source has a fan");
        let sub = SubRef {
            session: receiver,
            stream: id,
        };
        if video {
            fan.waiting.push(sub);
        } else {
            fan.serving.push(sub);
        }
        self.mark_dirty(receiver);
        let _ = now;
    }

    /// Stops sending a receiver one of its streams.
    pub(super) fn detach_stream(&mut self, receiver: SessionKey, stream: u32) {
        let Some(r) = self.sessions.get_mut(receiver) else {
            return;
        };
        let Some(ds) = r.down.get(stream) else { return };
        let (publisher, source, target) = (ds.publisher, ds.source, ds.target);
        for src in [source, target].into_iter().flatten() {
            if let Some(fan) = self.fanout.get_mut(&SourceKey {
                session: publisher,
                source: src,
            }) {
                fan.serving
                    .retain(|s| !(s.session == receiver && s.stream == stream));
                fan.waiting
                    .retain(|s| !(s.session == receiver && s.stream == stream));
            }
        }
        r.down.drop_stream(stream);
        self.mark_dirty(receiver);
    }

    /// Writes packets a participant sent to everyone who receives them.
    pub(super) fn forward(
        &mut self,
        key: SessionKey,
        packets: Vec<(u32, rtc::rtp::Packet)>,
        now: Instant,
    ) {
        for (source, mut packet) in packets {
            let sk = SourceKey {
                session: key,
                source,
            };
            let Some(fan) = self.fanout.get_mut(&sk) else {
                continue;
            };
            let resume = std::mem::take(&mut fan.resume);
            // Whatever the sender wrapped its voice in, the recording and the receivers that do not
            // take redundant audio get the Opus packet itself.
            if fan.uplink_red == Some(packet.header.payload_type) {
                let Some(opus) = red::primary(&packet.payload, OPUS_PAYLOAD_TYPE) else {
                    continue;
                };
                packet.payload = opus;
                packet.header.payload_type = OPUS_PAYLOAD_TYPE;
            }
            let seq = packet.header.sequence_number;
            let newest = is_newer(fan.newest, seq);
            if newest {
                fan.newest = Some(seq);
            }
            // The redundant payload is built once for every receiver that takes it, those waiting
            // for the voice (it enters the loudest ones) as well as those it is sent to.
            let listened = !fan.serving.is_empty() || !fan.waiting.is_empty();
            let red = (self.cfg.red && fan.ty == TrackType::Voice && listened).then(|| {
                fan.red.encode(
                    OPUS_PAYLOAD_TYPE,
                    seq,
                    packet.header.timestamp,
                    &packet.payload,
                    newest,
                )
            });
            let fan = &*fan;
            // The uplink counts whether or not anyone receives the stream.
            let size = packet.marshal_size() as u64;
            self.metrics.rtp_bytes_received(fan.ty, size);
            // A recorded stream is copied before anything decides whether anyone watches it.
            if let Some(tap) = fan.tap {
                self.recordings.packet(tap, &packet, now);
            }
            if fan.serving.is_empty() && fan.waiting.is_empty() {
                continue;
            }
            // The receivers negotiated their own extension ids; none of the sender's apply.
            packet.header.extension = false;
            packet.header.extensions.clear();
            let mut sent = [0u64; 4];
            let mut switched = Vec::new();
            let mut pausing = Vec::new();
            // The frames of a VP8 screen share, or camera when they are held to the receivers'
            // paths, go to each receiver as far as its temporal layers allow.
            let layered =
                fan.ty == TrackType::Screen || fan.ty == TrackType::Video && self.cfg.camera_budget;
            let frame = (fan.vp8 && layered).then(|| Frame {
                descriptor: vp8::Descriptor::parse(&packet.payload),
                key: vp8::is_keyframe(&packet.payload),
                newest,
            });

            // Receivers waiting for this stream join it at a keyframe (any packet, for codecs whose
            // keyframes are not recognised). The start of one that is not the newest packet is a
            // retransmission that came after the rest of its frame went by unsent: joining there
            // would hand the receiver a keyframe without its other packets, so it asks again.
            let key_start = frame
                .as_ref()
                .map_or_else(|| vp8::is_keyframe(&packet.payload), |f| f.key);
            let keyframe_start = !fan.waiting.is_empty() && (key_start || !fan.vp8);
            let stale_keyframe =
                !fan.waiting.is_empty() && is_stale_keyframe(fan.vp8, key_start, newest);
            if keyframe_start && newest {
                for &sub in &fan.waiting {
                    let Some(r) = self.sessions.get_mut(sub.session) else {
                        continue;
                    };
                    let limit = r.down.screen_level.temporal_limit();
                    let Some(ds) = r.down.get_mut(sub.stream) else {
                        continue;
                    };
                    if ds.phase != DownPhase::Active || ds.paused {
                        continue;
                    }
                    ds.rewrite.switch_to(&packet);
                    // The new source starts at a keyframe, in the temporal layers of the receiver's
                    // rung.
                    ds.temporal.restart();
                    if ds.ty == TrackType::Screen {
                        ds.temporal.set_limit(limit);
                    }
                    if let Some(written) = write_down(
                        r,
                        sub,
                        &packet,
                        red.as_ref(),
                        frame.as_ref(),
                        now,
                        &mut self.dirty,
                    ) {
                        sent[type_slot(
                            r.down.get(sub.stream).map_or(TrackType::Video, |d| d.ty),
                        )] += written;
                    }
                    switched.push(sub);
                }
            }
            // One write in 64 is timed, for the write-time metric.
            self.write_clock = self.write_clock.wrapping_add(1);
            let timed = self.write_clock.is_multiple_of(64);
            for (n, &sub) in fan.serving.iter().enumerate() {
                let Some(r) = self.sessions.get_mut(sub.session) else {
                    continue;
                };
                let started = (timed && n == 0).then(Instant::now);
                if resume && let Some(ds) = r.down.get_mut(sub.stream) {
                    ds.rewrite.resume_after_gap(&packet);
                    ds.red_hold = red::MAX_REDUNDANT;
                }
                let wrote = write_down(
                    r,
                    sub,
                    &packet,
                    red.as_ref(),
                    frame.as_ref(),
                    now,
                    &mut self.dirty,
                );
                let ty = r.down.get(sub.stream).map_or(TrackType::Voice, |d| d.ty);
                if let Some(written) = wrote {
                    sent[type_slot(ty)] += written;
                }
                // A camera being paused goes when the frame it is in is complete.
                if packet.header.marker && r.down.get(sub.stream).is_some_and(|d| d.pausing) {
                    pausing.push(sub);
                }
                if let Some(started) = started {
                    self.metrics
                        .rtp_write_time(&r.cfg.group_id, ty, started.elapsed());
                }
            }
            for ty in TrackType::ALL {
                if sent[type_slot(ty)] > 0 {
                    self.metrics.rtp_bytes_sent(ty, sent[type_slot(ty)]);
                }
            }
            for sub in switched {
                self.complete_switch(sk, sub);
            }
            if stale_keyframe && self.waiter_ready(sk) {
                self.request_keyframe(key, source, now, true);
            }
            for sub in pausing {
                self.finish_pause(sub.session, sub.stream, now);
            }
        }
    }

    /// Whether a receiver waiting on the stream could be sent a keyframe now: one still being
    /// added to the connection, or paused, is not asking for it.
    fn waiter_ready(&self, sk: SourceKey) -> bool {
        self.fanout.get(&sk).is_some_and(|fan| {
            fan.waiting.iter().any(|sub| {
                self.sessions
                    .get(sub.session)
                    .and_then(|r| r.down.get(sub.stream))
                    .is_some_and(|d| d.phase == DownPhase::Active && !d.paused)
            })
        })
    }

    /// A receiver waiting on `to` got its keyframe: it is fed from `to` now.
    fn complete_switch(&mut self, to: SourceKey, sub: SubRef) {
        let Some(r) = self.sessions.get_mut(sub.session) else {
            return;
        };
        let Some(ds) = r.down.get_mut(sub.stream) else {
            return;
        };
        debug!(receiver = %sub.session.idx, stream = sub.stream, source = to.source, "stream switched");
        let previous = ds.source.replace(to.source);
        ds.target = None;
        let publisher = ds.publisher;
        if let Some(fan) = self.fanout.get_mut(&to) {
            fan.waiting.retain(|s| *s != sub);
            fan.serving.push(sub);
        }
        if let Some(old) = previous.filter(|&o| o != to.source)
            && let Some(fan) = self.fanout.get_mut(&SourceKey {
                session: publisher,
                source: old,
            })
        {
            fan.serving.retain(|s| *s != sub);
        }
    }

    pub(super) fn set_muted(&mut self, key: SessionKey, muted: bool, _now: Instant) {
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        s.up.muted = muted;
        let voice_ended = muted && s.up.vad.as_mut().is_some_and(Monitor::reset);
        if muted
            && let Some(id) = self.call_id_of(key)
            && let Some(call) = self.calls.get_mut(&id)
        {
            // Its slot among the loudest voices is free for whoever answers, not held until the
            // last loudness it had has aged out.
            call.voices.remove(key);
        }
        if voice_ended {
            self.emit(key, MessageType::VoiceOff, Vec::new());
        }
        debug!(session = %self.session_id(key), muted, "microphone state");
    }

    /// The participant starts a screen share on the stream `data` names.
    pub(super) fn screen_on(&mut self, key: SessionKey, data: &[u8], _now: Instant) {
        let Ok(ann) = serde_json::from_slice::<ScreenAnnouncement>(data) else {
            warn!(session = %self.session_id(key), "unreadable screen announcement");
            return;
        };
        let Some(id) = self.call_id_of(key) else {
            return;
        };
        let Some(call) = self.calls.get_mut(&id) else {
            return;
        };
        if call.screen.is_some_and(|k| k != key) {
            warn!(session = %self.session_id(key), "someone else is already sharing a screen");
            return;
        }
        call.screen = Some(key);
        if let Some(s) = self.sessions.get_mut(key) {
            s.up.screen_stream_id = ann.screen_stream_id;
        }
    }

    /// The participant ends its screen share.
    pub(super) fn screen_off(&mut self, key: SessionKey, now: Instant) {
        let Some(id) = self.call_id_of(key) else {
            return;
        };
        if let Some(call) = self.calls.get_mut(&id)
            && call.screen == Some(key)
        {
            call.screen = None;
        }
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        s.up.screen_stream_id.clear();
        let ended: Vec<u32> =
            s.up.sources
                .iter()
                .filter(|x| matches!(x.ty, TrackType::Screen | TrackType::ScreenAudio))
                .map(|x| x.id)
                .collect();
        for id in ended {
            self.remove_source(key, id, now);
        }
        if let Some(s) = self.sessions.get_mut(key) {
            s.up.out_ids.remove(&TrackType::Screen);
            s.up.out_ids.remove(&TrackType::ScreenAudio);
        }
        for ty in [TrackType::Screen, TrackType::ScreenAudio] {
            self.sync_call_stream(key, ty, now);
        }
    }

    /// The participant announces the stream its camera uses.
    pub(super) fn video_on(&mut self, key: SessionKey, data: &[u8]) {
        match serde_json::from_slice::<VideoAnnouncement>(data) {
            Ok(a) => {
                if let Some(s) = self.sessions.get_mut(key) {
                    s.up.video_stream_id = a.video_stream_id;
                }
            }
            Err(err) => {
                warn!(session = %self.session_id(key), %err, "unreadable video announcement")
            }
        }
    }

    pub(super) fn call_id_of(&self, key: SessionKey) -> Option<super::CallId> {
        let s = self.sessions.get(key)?;
        Some(super::CallId {
            group: s.cfg.group_id.clone(),
            call: s.cfg.call_id.clone(),
        })
    }
}

fn type_slot(ty: TrackType) -> usize {
    match ty {
        TrackType::Voice => 0,
        TrackType::Screen => 1,
        TrackType::ScreenAudio => 2,
        TrackType::Video => 3,
    }
}

/// How far back a stream's sequence numbers may go and still be reordering (or a packet sent
/// again): further than this, the sender started its numbers over.
const REORDER_WINDOW: u16 = 512;

/// Whether sequence number `seq` is newer than `newest`, the newest of the stream so far: one
/// that comes after it, or that goes back by more than [`REORDER_WINDOW`] (a restart).
fn is_newer(newest: Option<u16>, seq: u16) -> bool {
    newest.is_none_or(|n| {
        let ahead = seq.wrapping_sub(n);
        (ahead != 0 && ahead < 0x8000) || ahead.wrapping_neg() > REORDER_WINDOW
    })
}

/// Whether a packet is the start of a keyframe that came late (see `forward`). Only a codec whose
/// keyframes are recognised has one: of any other every packet is taken for a start.
fn is_stale_keyframe(vp8: bool, key_start: bool, newest: bool) -> bool {
    vp8 && key_start && !newest
}

/// What the forwarder reads once of a packet of a video, for every receiver's temporal layers.
struct Frame {
    descriptor: Option<vp8::Descriptor>,
    key: bool,
    /// The packet is the newest of its stream: one that is not came late (a retransmission).
    newest: bool,
}

/// Writes `packet` to the receiver's copy of the stream, rewritten for it, unless it is of a
/// temporal layer the receiver is not sent (`frame`, for a video). A receiver that negotiated
/// redundant audio is sent `red`, the payload built for the packet, in its place (or the packet
/// alone in a RED payload, while what came before it is not what the receiver was sent). Returns
/// the bytes accepted for sending, `None` when it was not.
fn write_down(
    r: &mut Session,
    sub: SubRef,
    packet: &rtc::rtp::Packet,
    red: Option<&bytes::Bytes>,
    frame: Option<&Frame>,
    now: Instant,
    dirty: &mut Vec<SessionKey>,
) -> Option<u64> {
    let ds = r.down.get_mut(sub.stream)?;
    if ds.phase != DownPhase::Active {
        return None;
    }
    if let Some(f) = frame {
        let temporal = f.descriptor.and_then(|d| d.temporal);
        if !ds
            .temporal
            .admit(packet.header.timestamp, temporal, f.key, f.newest)
        {
            ds.rewrite.leave_out(packet, f.descriptor.as_ref());
            return None;
        }
    }
    let sender_id = ds.sender?;
    let mut sender = r.pc.rtp_sender(sender_id)?;
    if !ds.payload_type_known {
        // Redundant audio is what the receiver's answer accepted (see `Engine::on_answer`), else
        // the payload type of the codec.
        let mime = &ds.codec.mime_type;
        let plain = sender
            .get_parameters()
            .rtp_parameters
            .codecs
            .iter()
            .find(|c| c.rtp_codec.mime_type.eq_ignore_ascii_case(mime))
            .map(|c| c.payload_type);
        if let Some(pt) = ds.red_payload_type.or(plain) {
            ds.rewrite.set_payload_type(pt);
        }
        ds.payload_type_known = true;
    }
    let mut out = packet.clone();
    if ds.red_payload_type.is_some() {
        // Never the bare Opus packet under the RED payload type. After a source change or a gap the
        // packets before this one are not the receiver's: it is sent alone until they are.
        out.payload = match red {
            Some(red) if ds.red_hold == 0 => red.clone(),
            _ => red::wrap(OPUS_PAYLOAD_TYPE, &packet.payload),
        };
        ds.red_hold = ds.red_hold.saturating_sub(1);
    }
    if !ds.rewrite.rewrite(&mut out) {
        return None;
    }
    let size = out.marshal_size() as u64;
    let ok = sender.write_rtp(now, out).is_ok();
    if ok && !r.dirty {
        r.dirty = true;
        dirty.push(sub.session);
    }
    ok.then_some(size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_packet_is_newer_when_it_follows_the_newest_even_across_the_wrap() {
        assert!(is_newer(None, 7));
        assert!(is_newer(Some(7), 8));
        assert!(is_newer(Some(65_535), 2));
        assert!(!is_newer(Some(8), 8), "the same packet again");
        assert!(
            !is_newer(Some(8), 3),
            "sent again after the ones that followed it"
        );
        assert!(!is_newer(Some(2), 65_535));
        assert!(!is_newer(Some(1_000), 1_000 - REORDER_WINDOW));
    }

    #[test]
    fn only_a_recognised_keyframe_start_can_be_stale() {
        assert!(is_stale_keyframe(true, true, false));
        assert!(!is_stale_keyframe(true, true, true));
        assert!(!is_stale_keyframe(true, false, false), "not a keyframe");
        assert!(
            !is_stale_keyframe(false, true, false),
            "a codec whose keyframes are not recognised takes every packet for one"
        );
    }

    #[test]
    fn numbers_that_start_over_are_a_restart_not_a_late_packet() {
        assert!(is_newer(Some(40_000), 100), "going back by 39,900");
        assert!(is_newer(Some(5_000), 5_000 - REORDER_WINDOW - 1));
    }
}
