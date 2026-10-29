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
use crate::ids::{TrackType, track_id};
use crate::layers::Layer;
use crate::metrics::Direction;
use crate::peer::AUDIO_LEVEL_URI;
use crate::rate::RateMonitor;
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
    /// Where a copy of every packet goes, if the stream is being recorded.
    pub tap: Option<Tap>,
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
                src.rate.push(now, packet.marshal_size());
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
            last_packet: Some(now),
            last_pli: None,
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
                tap: None,
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
            if !s.up.sources.iter().any(|x| x.ty == *ty) {
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
            .map(|(id, d)| (id, d.source, d.target, d.codec.mime_type.clone()));
        match (have, want) {
            (None, None) => {}
            (Some((id, ..)), None) => self.detach_stream(receiver, id),
            (None, Some(src)) => self.attach_stream(receiver, publisher, ty, src, now),
            (Some((id, source, target, mime)), Some(src)) => {
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
            // Video starts at a keyframe; audio at once.
            source: (!video).then_some(source),
            target: video.then_some(source),
            rewrite: Rewriter::new(ssrc, 0, is_vp8),
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
            let size = packet.marshal_size() as u64;
            let mut sent = [0u64; 4];
            let mut writes = 0u64;
            let mut switched = Vec::new();

            // Receivers waiting for this stream join it at a keyframe (any packet, for codecs whose
            // keyframes are not recognised).
            if !fan.waiting.is_empty() && (!fan.vp8 || vp8::is_keyframe(&packet.payload)) {
                for &sub in &fan.waiting {
                    let Some(r) = self.sessions.get_mut(sub.session) else {
                        continue;
                    };
                    let Some(ds) = r.down.get_mut(sub.stream) else {
                        continue;
                    };
                    if ds.phase != DownPhase::Active {
                        continue;
                    }
                    ds.rewrite.switch_to(&packet);
                    if write_down(r, sub, &packet, now, &mut self.dirty) {
                        sent[type_slot(
                            r.down.get(sub.stream).map_or(TrackType::Video, |d| d.ty),
                        )] += size;
                        writes += 1;
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
                }
                let wrote = write_down(r, sub, &packet, now, &mut self.dirty);
                let ty = r.down.get(sub.stream).map_or(TrackType::Voice, |d| d.ty);
                if wrote {
                    sent[type_slot(ty)] += size;
                    writes += 1;
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
            // What the node's budget counts is on the wire: the IP and UDP headers add 28 bytes.
            self.cfg
                .egress
                .add(sent.iter().sum::<u64>() + writes * 28, writes);
            for sub in switched {
                self.complete_switch(sk, sub);
            }
        }
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

/// Writes `packet` to the receiver's copy of the stream, rewritten for it. Returns whether it was
/// accepted for sending.
fn write_down(
    r: &mut Session,
    sub: SubRef,
    packet: &rtc::rtp::Packet,
    now: Instant,
    dirty: &mut Vec<SessionKey>,
) -> bool {
    let Some(ds) = r.down.get_mut(sub.stream) else {
        return false;
    };
    if ds.phase != DownPhase::Active {
        return false;
    }
    let Some(sender_id) = ds.sender else {
        return false;
    };
    let Some(mut sender) = r.pc.rtp_sender(sender_id) else {
        return false;
    };
    if !ds.payload_type_known {
        let mime = ds.codec.mime_type.clone();
        if let Some(c) = sender
            .get_parameters()
            .rtp_parameters
            .codecs
            .iter()
            .find(|c| c.rtp_codec.mime_type.eq_ignore_ascii_case(&mime))
        {
            ds.rewrite.set_payload_type(c.payload_type);
        }
        ds.payload_type_known = true;
    }
    let mut out = packet.clone();
    ds.rewrite.rewrite(&mut out);
    let ok = sender.write_rtp(now, out).is_ok();
    if ok && !r.dirty {
        r.dirty = true;
        dirty.push(sub.session);
    }
    ok
}
