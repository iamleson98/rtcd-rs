//! Offers and answers.
//!
//! The participant offers first, carrying its microphone and the data channel; the SFU answers.
//! From then on the SFU offers whenever the set of streams it sends the participant changes, and
//! the participant offers when it adds or removes something of its own. All of the SFU's pending
//! changes go into one offer, so a participant joining a class of thirty is brought up to date in
//! one round trip, not thirty.

use std::time::Instant;

use rtc::peer_connection::sdp::{RTCSdpType, RTCSessionDescription};
use rtc::peer_connection::state::RTCSignalingState;
use rtc::peer_connection::transport::RTCIceCandidateInit;
use rtcd_proto::MessageType;
use rtcd_proto::dc::{DcMessage, TrackInfo};
use serde_json::Value;
use tracing::{debug, error, info, warn};

use super::Engine;
use super::session::{DownPhase, SessionKey};
use super::signaling::{Channel, Phase};
use super::types::CloseReason;
use crate::ids::parse_track_id;
use crate::metrics::Direction;

/// Offers that went unanswered before the session is given up on.
const MAX_OFFER_FAILURES: u8 = 3;

impl Engine {
    /// A trickled candidate from the participant. It is either the candidate init itself or the
    /// `{"type":"candidate","candidate":{...}}` wrapper the SFU sends its own in.
    pub(super) fn remote_candidate(&mut self, key: SessionKey, data: &[u8]) {
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        let parsed = serde_json::from_slice::<Value>(data).and_then(|v| {
            let inner = match v.get("candidate") {
                Some(inner) if inner.is_object() => inner.clone(),
                _ => v,
            };
            serde_json::from_value::<RTCIceCandidateInit>(inner)
        });
        let init = match parsed {
            Ok(init) => init,
            Err(err) => {
                warn!(session = %s.cfg.session_id, %err, "unreadable ICE candidate");
                self.metrics.error(&s.cfg.group_id, "ice");
                return;
            }
        };
        if init.candidate.is_empty() {
            return;
        }
        if s.pc.remote_description().is_none() {
            // Candidates can outrun the offer when the two travel by different routes.
            if s.early_candidates.len() < 64 {
                s.early_candidates.push(init);
            }
            return;
        }
        if let Err(err) = s.pc.add_remote_candidate(init) {
            debug!(session = %s.cfg.session_id, %err, "remote candidate refused");
            self.metrics.error(&s.cfg.group_id, "ice");
        }
    }

    /// A session description from the participant, arriving by `via`.
    pub(super) fn remote_description(
        &mut self,
        key: SessionKey,
        data: &[u8],
        via: Channel,
        now: Instant,
    ) {
        let desc: RTCSessionDescription = match serde_json::from_slice(data) {
            Ok(d) => d,
            Err(err) => {
                warn!(session = %self.session_id(key), %err, "unreadable session description");
                self.metrics.error(&self.group_id(key), "signaling");
                return;
            }
        };
        match desc.sdp_type {
            RTCSdpType::Offer => self.on_offer(key, desc, via, now),
            RTCSdpType::Answer => self.on_answer(key, desc, now),
            other => {
                warn!(session = %self.session_id(key), ?other, "unexpected session description type")
            }
        }
    }

    fn on_offer(
        &mut self,
        key: SessionKey,
        offer: RTCSessionDescription,
        via: Channel,
        now: Instant,
    ) {
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        let first = matches!(s.phase, Phase::AwaitingFirstOffer { .. });
        // Both sides offering at once: the SFU's offer stands, the participant's is dropped (a
        // polite client retries once it has our answer).
        if matches!(s.phase, Phase::Offering { .. })
            || s.pc.signaling_state() != RTCSignalingState::Stable
        {
            debug!(session = %s.cfg.session_id, "signaling conflict, ignoring offer");
            return;
        }
        let result = (|| -> rtc::shared::error::Result<()> {
            s.pc.set_remote_description(now, offer)?;
            Ok(())
        })();
        if let Err(err) = result {
            self.negotiation_failed(key, first, "set remote description", &err.to_string(), now);
            return;
        }
        self.add_local_candidates(key);
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        for c in std::mem::take(&mut s.early_candidates) {
            let _ = s.pc.add_remote_candidate(c);
        }
        let answered =
            s.pc.create_answer(None)
                .and_then(|answer| s.pc.set_local_description(now, answer));
        if let Err(err) = answered {
            self.negotiation_failed(key, first, "answer", &err.to_string(), now);
            return;
        }
        if first {
            s.phase = Phase::Idle;
        }
        self.register_ufrag(key);
        self.send_local_description(key, via, now);
        self.mark_dirty(key);
    }

    fn on_answer(&mut self, key: SessionKey, answer: RTCSessionDescription, now: Instant) {
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        if !matches!(s.phase, Phase::Offering { .. }) {
            debug!(session = %s.cfg.session_id, "answer without an offer, ignoring");
            return;
        }
        if let Err(err) = s.pc.set_remote_description(now, answer) {
            self.negotiation_failed(
                key,
                false,
                "set remote description of the answer",
                &err.to_string(),
                now,
            );
            return;
        }
        debug!(session = %s.cfg.session_id, "answer applied");
        let group = s.cfg.group_id.clone();
        s.phase = Phase::Idle;
        s.offer_failures = 0;
        // Video that just went live starts at a keyframe, which its sender is asked for.
        let mut keyframes = Vec::new();
        for slot in s.down.streams.iter_mut() {
            let Some(ds) = slot else { continue };
            match ds.phase {
                DownPhase::Adding => {
                    ds.phase = DownPhase::Active;
                    self.metrics
                        .track_added(&group, Direction::Out, &ds.codec.mime_type);
                    if let Some(target) = ds.target {
                        keyframes.push((ds.publisher, target));
                    }
                }
                DownPhase::Removing => {
                    self.metrics
                        .track_removed(&group, Direction::Out, &ds.codec.mime_type);
                    *slot = None;
                }
                _ => {}
            }
        }
        if let Some(held) = s.lock.server_unlock(now) {
            self.metrics.signaling_lock_held_time(&group, held);
        }
        for (publisher, source) in keyframes {
            self.request_keyframe(publisher, source, now);
        }
        self.mark_dirty(key);
    }

    fn negotiation_failed(
        &mut self,
        key: SessionKey,
        first: bool,
        what: &str,
        err: &str,
        now: Instant,
    ) {
        let Some(s) = self.sessions.get(key) else {
            return;
        };
        error!(session = %s.cfg.session_id, what, err, first, "negotiation failed");
        self.metrics.error(&s.cfg.group_id, "signaling");
        if first {
            // Without a first answer there is nothing to recover to.
            self.close_session(key, CloseReason::SignalingFailed, now);
            return;
        }
        // Leave the connection as it was, so the next offer starts clean.
        let s = self.sessions.get_mut(key).expect("checked above");
        if s.pc.signaling_state() != RTCSignalingState::Stable
            && let Ok(rollback) = RTCSessionDescription::rollback(None)
        {
            let _ = s.pc.set_local_description(now, rollback);
        }
        if matches!(s.phase, Phase::Offering { .. }) {
            self.offer_unanswered(key, now);
        }
    }

    /// The SFU's offer got no answer in time.
    pub(super) fn offer_timed_out(&mut self, key: SessionKey, now: Instant) {
        warn!(session = %self.session_id(key), "timed out waiting for an answer");
        self.metrics.error(&self.group_id(key), "signaling");
        if let Some(s) = self.sessions.get_mut(key)
            && s.pc.signaling_state() != RTCSignalingState::Stable
            && let Ok(rollback) = RTCSessionDescription::rollback(None)
        {
            let _ = s.pc.set_local_description(now, rollback);
        }
        self.offer_unanswered(key, now);
    }

    /// Gives up on the offer in flight: its changes go back to pending, and after repeated
    /// failures the session ends.
    fn offer_unanswered(&mut self, key: SessionKey, now: Instant) {
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        s.offer_failures += 1;
        s.phase = Phase::Idle;
        s.lock.server_unlock(now);
        for ds in s.down.streams.iter_mut().flatten() {
            ds.phase = match ds.phase {
                DownPhase::Adding => DownPhase::PendingAdd,
                DownPhase::Removing => DownPhase::PendingRemove,
                other => other,
            };
        }
        if s.offer_failures >= MAX_OFFER_FAILURES {
            self.close_session(key, CloseReason::SignalingFailed, now);
        } else {
            self.mark_dirty(key);
        }
    }

    /// Learns the ICE username fragment the connection is listening with now (it changes on an ICE
    /// restart) so checks for it find the session.
    pub(super) fn register_ufrag(&mut self, key: SessionKey) {
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        if let Ok(p) = s.pc.local_ice_parameters()
            && !s.ufrags.contains(&p.username_fragment)
        {
            s.ufrags.push(p.username_fragment.clone());
            self.routes.add_ufrag(p.username_fragment, key);
        }
    }

    /// Sends the connection's local description to the participant.
    fn send_local_description(&mut self, key: SessionKey, via: Channel, now: Instant) {
        let Some(s) = self.sessions.get(key) else {
            return;
        };
        let Some(desc) = s.pc.local_description() else {
            return;
        };
        let sdp = with_audio_ptime(&with_opus_dtx(&desc.sdp), self.cfg.audio_ptime_ms);
        let json = serde_json::json!({ "type": desc.sdp_type.to_string(), "sdp": sdp });
        let data = json.to_string().into_bytes();
        match via {
            Channel::Ws => self.emit(key, MessageType::Sdp, data),
            Channel::Dc => self.dc_send(key, DcMessage::Sdp(data), now),
        }
    }

    /// Makes an offer of everything pending, if the session is ready for one.
    pub(super) fn advance_negotiation(&mut self, key: SessionKey, now: Instant) {
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        if s.phase != Phase::Idle || !s.down.ready {
            return;
        }
        if !s.down.has_pending() {
            s.wants_offer_since = None;
            return;
        }
        if s.pc.signaling_state() != RTCSignalingState::Stable {
            return;
        }
        let waiting_since = *s.wants_offer_since.get_or_insert(now);
        if !s.lock.server_lock(now) {
            // The participant holds the lock; its unlock (or its expiry) wakes us.
            return;
        }
        s.wants_offer_since = None;
        self.metrics.signaling_lock_grab_time(
            &s.cfg.group_id,
            now.saturating_duration_since(waiting_since),
        );

        if let Err(err) = self.apply_downlink(key) {
            self.negotiation_failed(key, false, "apply tracks", &err, now);
            return;
        }
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        let offered =
            s.pc.create_offer(None)
                .and_then(|offer| s.pc.set_local_description(now, offer));
        if let Err(err) = offered {
            // The streams were applied but cannot be offered: put the connection back and retry.
            s.phase = Phase::Offering {
                deadline: now,
                via: Channel::Ws,
            };
            self.negotiation_failed(key, false, "offer", &err.to_string(), now);
            return;
        }
        let via = if s.cfg.props.dc_signaling() {
            Channel::Dc
        } else {
            Channel::Ws
        };
        s.phase = Phase::Offering {
            deadline: now + self.cfg.answer_timeout,
            via,
        };
        info!(session = %s.cfg.session_id, "offering");
        self.register_ufrag(key);
        self.send_media_map(key, now);
        self.send_local_description(key, via, now);
    }

    /// Puts the pending additions and removals into the connection.
    fn apply_downlink(&mut self, key: SessionKey) -> Result<(), String> {
        use rtc::media_stream::MediaStreamTrack;
        use rtc::rtp_transceiver::rtp_sender::{
            RTCRtpCodingParameters, RTCRtpEncodingParameters, RtpCodecKind,
        };

        let Some(s) = self.sessions.get_mut(key) else {
            return Ok(());
        };
        for slot in s.down.streams.iter_mut().flatten() {
            match slot.phase {
                DownPhase::PendingAdd => {
                    let kind = if slot.codec.mime_type.starts_with("audio/") {
                        RtpCodecKind::Audio
                    } else {
                        RtpCodecKind::Video
                    };
                    let track = MediaStreamTrack::new(
                        crate::ids::stream_id(slot.ty, &slot.publisher_session_id),
                        slot.out_track_id.clone(),
                        slot.out_track_id.clone(),
                        kind,
                        vec![RTCRtpEncodingParameters {
                            rtp_coding_parameters: RTCRtpCodingParameters {
                                ssrc: Some(slot.ssrc),
                                ..Default::default()
                            },
                            codec: slot.codec.clone(),
                            ..Default::default()
                        }],
                    );
                    match s.pc.add_track(track) {
                        Ok(id) => {
                            slot.sender = Some(id);
                            slot.phase = DownPhase::Adding;
                        }
                        Err(err) => return Err(format!("add track {}: {err}", slot.out_track_id)),
                    }
                }
                DownPhase::PendingRemove => {
                    if let Some(sender) = slot.sender.take() {
                        if let Err(err) = s.pc.remove_track(sender) {
                            return Err(format!("remove track {}: {err}", slot.out_track_id));
                        }
                        slot.phase = DownPhase::Removing;
                    } else {
                        // Never reached the connection.
                        slot.phase = DownPhase::Removing;
                    }
                }
                _ => {}
            }
        }
        s.down.changed = false;
        // Streams removed before they were ever offered need no answer.
        Ok(())
    }

    /// Tells the participant which mid carries which track, ahead of the offer that introduces them.
    fn send_media_map(&mut self, key: SessionKey, now: Instant) {
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        let mut map = std::collections::BTreeMap::new();
        for id in s.pc.get_transceivers().collect::<Vec<_>>() {
            let Some(trx) = s.pc.rtp_transceiver(id) else {
                continue;
            };
            let (mid, sender) = (trx.mid().clone(), trx.sender());
            let (Some(mid), Some(sender)) = (mid, sender) else {
                continue;
            };
            let Some(sender) = s.pc.rtp_sender(sender) else {
                continue;
            };
            let track_id = sender.track().track_id().to_string();
            if let Some((ty, from)) = parse_track_id(&track_id) {
                map.insert(
                    mid,
                    TrackInfo {
                        kind: ty.as_str().to_owned(),
                        sender_id: from.to_owned(),
                    },
                );
            }
        }
        if !map.is_empty() {
            self.dc_send(key, DcMessage::MediaMap(map), now);
        }
    }
}

/// Adds `usedtx=1` to every Opus format of a session description, keeping an explicit setting.
///
/// A sender uses discontinuous transmission when its peer's description asks for it, and the
/// answer to a browser's offer repeats the parameters of the browser's offer, which leave it out.
/// The connection itself refuses a description changed after the fact, so only the copy sent to
/// the participant carries it.
pub(crate) fn with_opus_dtx(sdp: &str) -> String {
    let lines: Vec<&str> = sdp.split("\r\n").collect();
    let opus: Vec<&str> = lines
        .iter()
        .filter_map(|l| l.strip_prefix("a=rtpmap:"))
        .filter_map(|m| {
            let (pt, codec) = m.split_once(' ')?;
            codec
                .to_ascii_lowercase()
                .starts_with("opus/")
                .then_some(pt)
        })
        .collect();
    lines
        .iter()
        .map(
            |line| match line.strip_prefix("a=fmtp:").and_then(|f| f.split_once(' ')) {
                Some((pt, params)) if opus.contains(&pt) && !params.contains("usedtx=") => {
                    format!("{line};usedtx=1")
                }
                _ => (*line).to_owned(),
            },
        )
        .collect::<Vec<_>>()
        .join("\r\n")
}

/// Asks the participant to send its voice in packets of `ms` milliseconds, by writing `a=ptime`
/// and `a=maxptime` into every audio section the SFU receives on (0 changes nothing).
///
/// Browsers apply the packet time of the description they are given to what they send, so this is
/// how a node with many open microphones halves the packets they cost (40 ms), at 20 ms more delay.
/// Sections the SFU only sends on are left alone: what it forwards keeps the packet time it came
/// with. Like [`with_opus_dtx`], only the copy sent to the participant carries it.
pub(crate) fn with_audio_ptime(sdp: &str, ms: u32) -> String {
    if ms == 0 {
        return sdp.to_owned();
    }
    // Split at "\r\nm=": every section but the last has lost its closing line break.
    let sections: Vec<&str> = sdp.split("\r\nm=").collect();
    let last = sections.len() - 1;
    sections
        .iter()
        .enumerate()
        .map(|(i, section)| {
            let receives = section.contains("\r\na=recvonly") || section.contains("\r\na=sendrecv");
            if i == 0
                || !section.starts_with("audio ")
                || !receives
                || section.contains("\r\na=ptime:")
            {
                return (*section).to_owned();
            }
            let body = section.strip_suffix("\r\n").unwrap_or(section);
            let end = if i == last { "\r\n" } else { "" };
            format!("{body}\r\na=ptime:{ms}\r\na=maxptime:{ms}{end}")
        })
        .collect::<Vec<_>>()
        .join("\r\nm=")
}

#[cfg(test)]
mod tests {
    use super::{with_audio_ptime, with_opus_dtx};

    #[test]
    fn dtx_is_added_to_opus_only() {
        let sdp = "m=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=rtpmap:111 opus/48000/2\r\na=fmtp:111 minptime=10;useinbandfec=1\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=rtpmap:96 VP8/90000\r\na=fmtp:96 x=1\r\n";
        let out = with_opus_dtx(sdp);
        assert!(
            out.contains("a=fmtp:111 minptime=10;useinbandfec=1;usedtx=1\r\n"),
            "{out}"
        );
        assert!(out.contains("a=fmtp:96 x=1\r\n"), "{out}");
    }

    #[test]
    fn an_explicit_dtx_setting_is_kept() {
        let sdp = "a=rtpmap:111 opus/48000/2\r\na=fmtp:111 usedtx=0\r\n";
        assert_eq!(with_opus_dtx(sdp), sdp);
    }

    #[test]
    fn a_description_without_opus_is_unchanged() {
        let sdp = "a=rtpmap:96 VP8/90000\r\na=fmtp:96 a=b\r\n";
        assert_eq!(with_opus_dtx(sdp), sdp);
    }

    const BROWSER_ANSWER: &str = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=mid:0\r\na=recvonly\r\na=rtpmap:111 opus/48000/2\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:1\r\na=recvonly\r\na=rtpmap:96 VP8/90000\r\n";

    #[test]
    fn the_packet_time_is_written_into_audio_the_sfu_receives() {
        let out = with_audio_ptime(BROWSER_ANSWER, 40);
        assert!(
            out.contains("a=rtpmap:111 opus/48000/2\r\na=ptime:40\r\na=maxptime:40\r\nm=video"),
            "{out}"
        );
        assert_eq!(
            out.matches("a=ptime").count(),
            1,
            "the video section has none"
        );
        assert!(out.ends_with("a=rtpmap:96 VP8/90000\r\n"), "{out}");
    }

    #[test]
    fn the_packet_time_is_written_into_the_last_section_too() {
        let sdp = "v=0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=sendonly\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=sendrecv\r\n";
        assert!(
            with_audio_ptime(sdp, 60).ends_with("a=sendrecv\r\na=ptime:60\r\na=maxptime:60\r\n")
        );
    }

    #[test]
    fn audio_the_sfu_only_sends_keeps_its_packet_time() {
        let sdp = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=sendonly\r\n";
        assert_eq!(with_audio_ptime(sdp, 40), sdp);
    }

    #[test]
    fn a_packet_time_already_there_is_kept_and_zero_changes_nothing() {
        let sdp = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=recvonly\r\na=ptime:20\r\n";
        assert_eq!(with_audio_ptime(sdp, 40), sdp);
        assert_eq!(with_audio_ptime(BROWSER_ANSWER, 0), BROWSER_ANSWER);
    }
}
