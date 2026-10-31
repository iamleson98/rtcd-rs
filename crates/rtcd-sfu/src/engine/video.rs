//! Video policy: which camera and screen layers each receiver gets, how a receiver is moved from
//! one layer to another without renegotiating, and the keyframe requests that make it possible.
//!
//! The Go rtcd moves a receiver between layers by removing one track and adding the other, a full
//! SDP round trip each time. Here every receiver keeps one stream per sender for good; only the
//! source feeding it changes, at a keyframe of the new layer, with the numbering rewritten so the
//! receiver sees one unbroken stream.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use rtc::rtcp::payload_feedbacks::full_intra_request::FullIntraRequest;
use rtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication;
use serde::Deserialize;
use tracing::{debug, warn};

use super::session::{DownPhase, SessionKey, SourceKey, SubRef};
use super::{Engine, Load};
use crate::ids::TrackType;
use crate::layers::{Layer, ScreenInputs, WATCH_EVERY};

/// The most cameras one receiver may name.
pub const MAX_VIDEO_SUBSCRIPTIONS: usize = 100;
/// At most one keyframe request per stream per this long: a sender asked too often spends its
/// bandwidth on keyframes.
const KEYFRAME_INTERVAL: Duration = Duration::from_secs(1);
/// How many voices of a call are forwarded while the node is shedding load, whatever the
/// configured limit is (a smaller configured limit stays).
const SHEDDING_VOICES: usize = 2;
/// And on a shard at its limit: the loudest voice only.
const SATURATED_VOICES: usize = 1;

#[derive(Deserialize)]
struct Subscription {
    #[serde(default, deserialize_with = "null_as_empty")]
    session_ids: Vec<String>,
    #[serde(default, deserialize_with = "null_as_empty")]
    low: Vec<String>,
}

/// A JSON `null` where a list belongs reads as the empty list, which is how Go encodes a nil slice.
fn null_as_empty<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
    Ok(Option::<Vec<String>>::deserialize(d)?.unwrap_or_default())
}

impl Engine {
    /// The source `receiver` should be fed of `publisher`'s streams of type `ty`, if any.
    pub(super) fn desired_source(
        &self,
        receiver: SessionKey,
        publisher: SessionKey,
        ty: TrackType,
    ) -> Option<u32> {
        let p = self.sessions.get(publisher)?;
        let r = self.sessions.get(receiver)?;
        let mut candidates = p.up.sources.iter().filter(|x| x.ty == ty);
        match ty {
            TrackType::Voice | TrackType::ScreenAudio => candidates.next().map(|x| x.id),
            TrackType::Video => {
                if r.down
                    .wanted_video
                    .as_ref()
                    .is_some_and(|w| !w.contains(&p.cfg.session_id))
                {
                    return None;
                }
                // The small layer while the receiver asked for it or the sender has paused the large.
                let small = r.down.squeezed
                    || r.down.low_video.contains(&p.cfg.session_id)
                    || p.up.camera_stall.stalled();
                let preferred = if small { Layer::Low } else { Layer::High };
                self.pick_layer(&mut candidates, preferred)
            }
            TrackType::Screen => {
                let preferred = if r.down.squeezed {
                    Layer::Low
                } else {
                    r.down.screen_level.level()
                };
                self.pick_layer(&mut candidates, preferred)
            }
        }
    }

    /// The source of the preferred layer among `candidates`, else the other layer.
    fn pick_layer<'a>(
        &self,
        candidates: &mut impl Iterator<Item = &'a super::session::Source>,
        preferred: Layer,
    ) -> Option<u32> {
        let all: Vec<_> = candidates.collect();
        all.iter()
            .find(|x| x.layer == preferred)
            .or_else(|| all.first())
            .map(|x| x.id)
    }

    /// Moves a receiver's stream onto `to`, from its next keyframe.
    pub(super) fn begin_switch(
        &mut self,
        receiver: SessionKey,
        stream: u32,
        publisher: SessionKey,
        to: u32,
        now: Instant,
    ) {
        let Some(r) = self.sessions.get_mut(receiver) else {
            return;
        };
        let Some(ds) = r.down.get_mut(stream) else {
            return;
        };
        let sub = SubRef {
            session: receiver,
            stream,
        };
        let previous_target = ds.target.take();
        if let Some(old) = previous_target
            && let Some(fan) = self.fanout.get_mut(&SourceKey {
                session: publisher,
                source: old,
            })
        {
            fan.waiting.retain(|s| *s != sub);
        }
        if ds.source == Some(to) {
            // Back to what it already gets: the pending move is cancelled.
            return;
        }
        ds.target = Some(to);
        let active = ds.phase == DownPhase::Active;
        if let Some(fan) = self.fanout.get_mut(&SourceKey {
            session: publisher,
            source: to,
        }) {
            fan.waiting.push(sub);
        }
        if active {
            self.request_keyframe(publisher, to, now);
        }
    }

    /// Asks the sender for a keyframe of one of its streams, unless it was asked a moment ago.
    pub(super) fn request_keyframe(&mut self, publisher: SessionKey, source: u32, now: Instant) {
        let Some(p) = self.sessions.get_mut(publisher) else {
            return;
        };
        let Some(src) = p.up.sources.iter_mut().find(|x| x.id == source) else {
            return;
        };
        if src
            .last_pli
            .is_some_and(|t| now.saturating_duration_since(t) < KEYFRAME_INTERVAL)
        {
            self.metrics.keyframe_request(false);
            return;
        }
        self.metrics.keyframe_request(true);
        src.last_pli = Some(now);
        debug!(session = %p.cfg.session_id, ssrc = src.ssrc, "keyframe requested");
        let (receiver, ssrc) = (src.receiver, src.ssrc);
        if let Some(mut rx) = p.pc.rtp_receiver(receiver)
            && let Err(err) = rx.write_rtcp(
                now,
                vec![Box::new(PictureLossIndication {
                    sender_ssrc: 0,
                    media_ssrc: ssrc,
                })],
            )
        {
            debug!(session = %p.cfg.session_id, %err, "keyframe request failed");
        }
        self.mark_dirty(publisher);
    }

    /// Feedback a receiver sent about the streams it gets: a request for a keyframe is passed on
    /// to the sender of the stream asked about.
    pub(super) fn receiver_feedback(
        &mut self,
        key: SessionKey,
        packets: &[Box<dyn rtc::rtcp::packet::Packet>],
        now: Instant,
    ) {
        let mut asked = Vec::new();
        for p in packets {
            if let Some(pli) = p.as_any().downcast_ref::<PictureLossIndication>() {
                asked.push(pli.media_ssrc);
            } else if let Some(fir) = p.as_any().downcast_ref::<FullIntraRequest>() {
                asked.extend(fir.fir.iter().map(|e| e.ssrc));
            }
        }
        if asked.is_empty() {
            return;
        }
        let Some(s) = self.sessions.get(key) else {
            return;
        };
        let wanted: Vec<_> = s
            .down
            .iter()
            .filter(|(_, d)| asked.contains(&d.ssrc))
            .flat_map(|(_, d)| {
                [d.source, d.target]
                    .into_iter()
                    .flatten()
                    .map(|src| (d.publisher, src))
            })
            .collect();
        for (publisher, source) in wanted {
            self.request_keyframe(publisher, source, now);
        }
    }

    /// The receiver names the cameras it wants (JSON: `session_ids`, and `low` for those it wants
    /// at the small size).
    pub(super) fn video_subscribe(&mut self, key: SessionKey, data: &[u8], now: Instant) {
        let sub: Subscription = match serde_json::from_slice(data) {
            Ok(s) => s,
            Err(err) => {
                warn!(session = %self.session_id(key), %err, "unreadable video subscription");
                return;
            }
        };
        if sub.session_ids.len() > MAX_VIDEO_SUBSCRIPTIONS {
            warn!(session = %self.session_id(key), count = sub.session_ids.len(), "video subscription too large");
            self.metrics.error(&self.group_id(key), "video_subscribe");
            return;
        }
        let wanted: HashSet<String> = sub.session_ids.into_iter().collect();
        let low: HashSet<String> = sub
            .low
            .into_iter()
            .filter(|id| wanted.contains(id))
            .collect();
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        s.down.wanted_video = Some(wanted);
        s.down.low_video = low;
        // A receiver still connecting gets its cameras when it becomes ready.
        if !s.down.ready {
            return;
        }
        for member in self.call_members(key) {
            if member != key {
                self.sync_stream(key, member, TrackType::Video, now);
            }
        }
    }

    /// Once-a-second upkeep of a session's video: the sender's large layer pausing and resuming,
    /// the receiver's keyframe requests that went unanswered, and its screen share layer.
    pub(super) fn housekeeping(&mut self, key: SessionKey, now: Instant) {
        self.cfg
            .egress
            .update(now, self.cfg.egress_budget_bps, self.cfg.egress_budget_pps);
        self.follow_budget(key, now);
        self.follow_voice_limit(key, now);
        if !self.recordings.is_empty() {
            self.record_upkeep(key, now);
        }
        // What is left looks at video: a class without a camera has nothing in it to look at, and
        // reading a session's statistics is not free on the thread that forwards the voices.
        if self.has_video(key) {
            self.watch_camera(key, now);
            self.retry_keyframes(key, now);
            self.review_screen_level(key, now);
            self.report_quality(key, now);
        }
    }

    /// Whether the session sends or receives any video.
    fn has_video(&self, key: SessionKey) -> bool {
        self.sessions.get(key).is_some_and(|s| {
            s.up.sources.iter().any(|x| x.ty.is_video())
                || s.down.iter().any(|(_, d)| d.ty.is_video())
        })
    }

    /// Whether the node is asked for more than it can send, or the shard for more than it can do:
    /// a send budget (bits or packets) is nearly used, the kernel refuses datagrams (the node's own
    /// ceiling, learned), or the shard's thread has no room left.
    fn shedding(&self) -> bool {
        self.cfg.egress.squeezed() || self.load != Load::Ok
    }

    /// Lets a call forward fewer voices while the node is shedding: a call with many open
    /// microphones asks for a packet for every listener from each of them, and the listener hears
    /// two or three.
    fn follow_voice_limit(&mut self, key: SessionKey, now: Instant) {
        let configured = self.cfg.audio_loudest;
        let limit = if self.load == Load::Saturated || self.cfg.egress.choked(now) {
            SATURATED_VOICES
        } else if self.shedding() {
            match configured {
                0 => SHEDDING_VOICES,
                n => n.min(SHEDDING_VOICES),
            }
        } else {
            configured
        };
        let Some(s) = self.sessions.get(key) else {
            return;
        };
        if let Some(call) = self.calls.get_mut(&s.call) {
            call.voices.set_limit(limit);
        }
    }

    /// Holds a receiver to the small layers while the node is shedding (see `shedding`), and lets
    /// it back up when it is not.
    fn follow_budget(&mut self, key: SessionKey, now: Instant) {
        let squeeze = self.shedding();
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        if s.down.squeezed == squeeze {
            return;
        }
        s.down.squeezed = squeeze;
        debug!(session = %s.cfg.session_id, squeeze, "send budget changed the layers");
        if !s.down.ready {
            return;
        }
        for member in self.call_members(key) {
            if member != key {
                self.sync_stream(key, member, TrackType::Video, now);
                self.sync_stream(key, member, TrackType::Screen, now);
            }
        }
    }

    /// Moves the receivers of a camera between its layers as the large one pauses and resumes.
    fn watch_camera(&mut self, key: SessionKey, now: Instant) {
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        let last = |layer| {
            s.up.sources
                .iter()
                .filter(|x| x.ty == TrackType::Video && x.layer == layer)
                .filter_map(|x| x.last_packet)
                .max()
        };
        let (high, low) = (last(Layer::High), last(Layer::Low));
        if s.up.camera_stall.update(now, high, low) {
            debug!(session = %s.cfg.session_id, stalled = s.up.camera_stall.stalled(), "camera large layer changed");
            self.record_sync(key, TrackType::Video, now);
            self.sync_call_stream(key, TrackType::Video, now);
        }
    }

    /// Asks again for keyframes the senders have not delivered.
    fn retry_keyframes(&mut self, key: SessionKey, now: Instant) {
        let Some(s) = self.sessions.get(key) else {
            return;
        };
        let waiting: Vec<_> = s
            .down
            .iter()
            .filter(|(_, d)| d.phase == DownPhase::Active)
            .filter_map(|(_, d)| d.target.map(|t| (d.publisher, t)))
            .collect();
        for (publisher, source) in waiting {
            self.request_keyframe(publisher, source, now);
        }
    }

    /// Decides, from the receiver's bandwidth estimate, which layer of a screen share it gets.
    pub(super) fn review_screen_level(&mut self, key: SessionKey, now: Instant) {
        let Some(s) = self.sessions.get(key) else {
            return;
        };
        let Some(sharer) = s
            .down
            .iter()
            .find(|(_, d)| d.ty == TrackType::Screen)
            .map(|(_, d)| d.publisher)
        else {
            return;
        };
        let Some(p) = self.sessions.get(sharer) else {
            return;
        };
        let high_rate =
            p.up.sources
                .iter()
                .find(|x| x.ty == TrackType::Screen && x.layer == Layer::High)
                .and_then(|x| x.rate.rate(now));
        let estimate = s.estimate.snapshot();
        let before = s.down.screen_level.level();
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        let after = s.down.screen_level.decide(
            now,
            ScreenInputs {
                capacity: estimate.delay_based,
                delivery: estimate.delivery,
                high_rate,
            },
        );
        self.metrics
            .screen_layer_time(after == Layer::High, Self::tick_interval());
        if after != before {
            debug!(session = %s.cfg.session_id, ?after, target = estimate.target, "screen share layer changed");
            self.sync_stream(key, sharer, TrackType::Screen, now);
        }
    }

    /// Whether a session has media to look after once a second: video (its layers, keyframes,
    /// quality) or voice (how many voices its call forwards while the node sheds load).
    pub(super) fn needs_tick(&self, key: SessionKey) -> bool {
        self.sessions
            .get(key)
            .is_some_and(|s| !s.up.sources.is_empty() || s.down.iter().next().is_some())
    }

    /// The interval of the upkeep.
    pub(super) fn tick_interval() -> Duration {
        WATCH_EVERY
    }
}
