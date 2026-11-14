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
use rtcd_proto::dc::DcMessage;
use serde::Deserialize;
use tracing::{debug, warn};

use super::session::{DownPhase, SessionKey, Source, SourceKey, SubRef};
use super::{Engine, Load};
use crate::camera_budget::{
    self, Camera, CameraSteps, RANK_HOST, RANK_LOUD, RANK_SMALL, RANK_WATCHED,
};
use crate::ids::TrackType;
use crate::layers::{Ladder, Layer, LayerRates, Rung, STALL_AFTER, ScreenInputs, WATCH_EVERY};

/// The most cameras one receiver may name.
pub const MAX_VIDEO_SUBSCRIPTIONS: usize = 100;
/// At most one keyframe request per stream per this long: a sender asked too often spends its
/// bandwidth on keyframes.
const KEYFRAME_INTERVAL: Duration = Duration::from_secs(1);
/// Between the once-a-second reviews, a receiver's screen share is looked at again as soon as its
/// feedback says its path is congested, at most this often: a queue that builds for a second
/// before anything is done holds the voice back with it.
const SCREEN_REVIEW_MIN: Duration = Duration::from_millis(250);
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
                // The small layer while the receiver asked for it, the sender has paused the large,
                // or the receiver's camera budget holds the camera there.
                let small = r.down.squeezed
                    || r.down.low_video.contains(&p.cfg.session_id)
                    || p.up.camera_stall.stalled()
                    || r.down.iter().any(|(_, d)| {
                        d.publisher == publisher
                            && d.ty == ty
                            && !matches!(d.phase, DownPhase::PendingRemove | DownPhase::Removing)
                            && d.budget_small
                    });
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
        let active = ds.phase == DownPhase::Active && !ds.paused;
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
        // The keyframe that answers goes to everyone the stream feeds, in a burst on each path
        // that is not congestion (only a request that was sent brings one).
        if let Some(fan) = self.fanout.get(&SourceKey {
            session: publisher,
            source,
        }) {
            for sub in fan.serving.iter().chain(&fan.waiting) {
                if let Some(s) = self.sessions.get_mut(sub.session) {
                    s.down.cameras.expect_keyframes(now);
                }
            }
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
            .filter(|(_, d)| asked.contains(&d.ssrc) && !d.paused)
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
            self.measure_cameras(key, now);
            self.retry_keyframes(key, now);
            // The cameras are held first, so that the screen share, which comes before them, is
            // not cut for the sake of cameras that are about to be.
            self.review_cameras(key, now);
            self.review_screen_level(key, now, true);
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
            .filter(|(_, d)| d.phase == DownPhase::Active && !d.paused)
            .filter_map(|(_, d)| d.target.map(|t| (d.publisher, t)))
            .collect();
        for (publisher, source) in waiting {
            self.request_keyframe(publisher, source, now);
        }
    }

    /// Looks at a receiver's screen share and cameras again when its feedback says the path is
    /// congested, without waiting for the next once-a-second review (see [`SCREEN_REVIEW_MIN`]).
    pub(super) fn watch_path(&mut self, key: SessionKey, now: Instant) {
        let Some(s) = self.sessions.get(key) else {
            return;
        };
        let due = |at: Option<Instant>| {
            at.is_none_or(|t| now.saturating_duration_since(t) >= SCREEN_REVIEW_MIN)
        };
        let (screen, cameras) = (due(s.down.screen_review), due(s.down.camera_review));
        if !screen && !cameras {
            return;
        }
        let e = s.estimate.snapshot();
        let path = ScreenInputs {
            queue: e.queue,
            queue_trend: e.queue_trend,
            acked: e.acked,
            capacity: e.capacity,
            delivery: e.delivery,
            other: 0.0,
        };
        if !path.congested() {
            return;
        }
        if cameras {
            self.review_cameras(key, now);
        }
        if screen {
            self.review_screen_level(key, now, false);
        }
    }

    /// Decides, from what the receiver's feedback says of its path, which rung of a screen share
    /// it gets: the simulcast layer (moved at the next keyframe of the other one) and how many of
    /// its temporal layers (from the next frame). `tick` is the once-a-second review, which the
    /// layer time is counted on.
    pub(super) fn review_screen_level(&mut self, key: SessionKey, now: Instant, tick: bool) {
        let Some(s) = self.sessions.get(key) else {
            return;
        };
        let Some((stream, sharer, source)) = s
            .down
            .iter()
            .find(|(_, d)| d.ty == TrackType::Screen)
            .map(|(id, d)| (id, d.publisher, d.source))
        else {
            return;
        };
        let Some(p) = self.sessions.get(sharer) else {
            return;
        };
        let ladder = Self::screen_ladder(p, now);
        let serving = source
            .and_then(|id| p.up.sources.iter().find(|x| x.id == id))
            .map(|x| x.layer);
        let estimate = s.estimate.snapshot();
        let inputs = ScreenInputs {
            queue: estimate.queue,
            queue_trend: estimate.queue_trend,
            acked: estimate.acked,
            capacity: estimate.capacity,
            delivery: estimate.delivery,
            other: self.other_video_rate(key, now),
        };
        let before = s.down.screen_level.level();
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        s.down.screen_review = Some(now);
        let rung = s.down.screen_level.decide(now, inputs, &ladder);
        // While the receiver waits for the keyframe of a smaller layer, it gets only the base of
        // the larger: what it was sent is what congested its path.
        let limit = match serving {
            Some(layer) if layer != rung.layer && rung.layer == Layer::Low => Some(0),
            Some(layer) if layer != rung.layer => {
                s.down.get(stream).and_then(|d| d.temporal.limit())
            }
            _ => rung.tid,
        };
        if let Some(ds) = s.down.get_mut(stream) {
            ds.temporal.set_limit(limit);
        }
        if tick {
            self.metrics
                .screen_layer_time(rung.layer == Layer::High, Self::tick_interval());
        }
        if rung.layer != before {
            debug!(session = %s.cfg.session_id, ?rung, queue = ?inputs.queue, acked = ?inputs.acked, capacity = ?inputs.capacity, "screen share layer changed");
            self.sync_stream(key, sharer, TrackType::Screen, now);
        }
    }

    /// What the sharer `p` offers of its screen share, rung by rung.
    fn screen_ladder(p: &super::session::Session, now: Instant) -> Ladder {
        let rates = |layer: Layer| {
            let src =
                p.up.sources
                    .iter()
                    .find(|x| x.ty == TrackType::Screen && x.layer == layer)?;
            let temporal = src.temporal_rates.as_ref();
            Some(LayerRates {
                total: src.rate.rate(now),
                temporal: temporal
                    .filter(|t| t.seen)
                    .map(|t| [t.base.rate(now), t.second.rate(now)]),
                keyframe_bits: temporal
                    .and_then(|t| t.keyframe_bytes)
                    .map(|b| b as f64 * 8.0),
            })
        };
        Ladder::new(rates(Layer::Low), rates(Layer::High))
    }

    /// Bits per second of the screen share a receiver is sent now: it comes first on the path.
    fn screen_rate(&self, key: SessionKey, now: Instant) -> f64 {
        let Some(s) = self.sessions.get(key) else {
            return 0.0;
        };
        s.down
            .iter()
            .find(|(_, d)| d.ty == TrackType::Screen)
            .and_then(|(_, d)| self.sessions.get(d.publisher))
            .zip(s.down.screen_level.rung())
            .and_then(|(p, rung)| Self::screen_ladder(p, now).rate_of(rung))
            .unwrap_or(0.0)
    }

    /// Measures what a session's camera costs a receiver at each of its steps, once for all its
    /// receivers (`review_cameras` reads it).
    fn measure_cameras(&mut self, key: SessionKey, now: Instant) {
        if !self.cfg.camera_budget {
            return;
        }
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        s.up.camera_steps = camera_steps(&s.up.sources, now);
    }

    /// Holds the cameras a receiver is sent to what its path carries (see `camera_budget`): each
    /// is sent whole, or as much of its temporal layers as fits, or paused until a keyframe finds
    /// it room again. Looked at once a second, and sooner while the path says it is congested.
    pub(super) fn review_cameras(&mut self, key: SessionKey, now: Instant) {
        if !self.cfg.camera_budget {
            return;
        }
        if let Some(s) = self.sessions.get_mut(key) {
            s.down.camera_review = Some(now);
        }
        let Some(s) = self.sessions.get(key) else {
            return;
        };
        let e = s.estimate.snapshot();
        // A path that is down, or has just come back, is not read.
        if camera_budget::path_unreadable(now, e.fed_at, e.resumed_at) {
            return;
        }
        let path = ScreenInputs {
            queue: e.queue,
            queue_trend: e.queue_trend,
            acked: e.acked,
            capacity: e.capacity,
            delivery: e.delivery,
            other: 0.0,
        };
        if s.down.cameras.limit().is_none() && !path.congested() {
            return;
        }
        let voices = self.calls.get(&s.call).map(|c| &c.voices);
        let mut streams = Vec::new();
        let mut cameras = Vec::new();
        for (id, d) in s
            .down
            .iter()
            .filter(|(_, d)| d.ty == TrackType::Video && d.phase == DownPhase::Active)
        {
            let Some(p) = self.sessions.get(d.publisher) else {
                continue;
            };
            let Some(layer) = d
                .target
                .or(d.source)
                .and_then(|id| p.up.sources.iter().find(|x| x.id == id))
                .map(|x| x.layer)
            else {
                continue;
            };
            let rank = if p.cfg.props.host() {
                RANK_HOST
            } else if voices.is_some_and(|v| v.is_loud(d.publisher, now)) {
                RANK_LOUD
            } else if s.down.low_video.contains(&p.cfg.session_id) {
                RANK_SMALL
            } else {
                RANK_WATCHED
            };
            // A receiver held to the small layer anyway has no step onto the large one.
            let steps = if s.down.squeezed
                || s.down.low_video.contains(&p.cfg.session_id)
                || p.up.camera_stall.stalled()
            {
                p.up.camera_steps.small_only()
            } else {
                p.up.camera_steps
            };
            streams.push(id);
            cameras.push(Camera::new(
                rank,
                steps,
                (d.paused || d.pausing).then(|| {
                    d.paused_at
                        .map_or(Duration::ZERO, |t| now.saturating_duration_since(t))
                }),
                Rung {
                    layer,
                    tid: d.temporal.limit(),
                },
            ));
        }
        if cameras.is_empty() {
            return;
        }
        let path = ScreenInputs {
            other: self.screen_rate(key, now),
            ..path
        };
        let before: Vec<usize> = cameras.iter().map(|c| c.level).collect();
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        s.down
            .cameras
            .review(now, &path, camera_budget::Load::of(&cameras));
        let raise = s.down.cameras.raise_ceiling(now);
        let limit = s.down.cameras.limit();
        let trial = s.down.cameras.pause_trial(now);
        camera_budget::allocate(limit, raise, trial, &mut cameras);
        let (mut raised, mut changed) = (false, false);
        let (mut paused, mut thinned) = (0, 0);
        for ((id, camera), was) in streams.into_iter().zip(&cameras).zip(before) {
            raised |= camera.level > was;
            changed |= camera.level != was;
            match camera.level {
                0 => paused += 1,
                n if n < camera.steps.count() => thinned += 1,
                _ => {}
            }
            if camera.level != was {
                self.apply_camera_level(key, id, camera, now);
            }
        }
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        if raised {
            s.down.cameras.raised(now);
        }
        if changed {
            debug!(session = %s.cfg.session_id, ?limit, paused, thinned, cameras = cameras.len(), "cameras held to the path");
        }
        if limit.is_none() {
            self.release_small_cameras(key, now);
        }
    }

    /// Lets the cameras the budget held on their small layer go back to the layer they would be
    /// sent otherwise, now that the budget lets go of them.
    fn release_small_cameras(&mut self, key: SessionKey, now: Instant) {
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        let held: Vec<SessionKey> = s
            .down
            .streams
            .iter_mut()
            .flatten()
            .filter_map(|d| std::mem::take(&mut d.budget_small).then_some(d.publisher))
            .collect();
        for publisher in held {
            self.sync_stream(key, publisher, TrackType::Video, now);
        }
    }
    /// Sends a receiver the camera `id` at the level `camera` was given: paused, or on the layer
    /// and up to the temporal layer its step says.
    fn apply_camera_level(&mut self, key: SessionKey, id: u32, camera: &Camera, now: Instant) {
        let Some(ds) = self.sessions.get_mut(key).and_then(|s| s.down.get_mut(id)) else {
            return;
        };
        if camera.level == 0 {
            self.pause_camera(key, id, now);
            return;
        }
        ds.temporal.set_limit(camera.rung(camera.level).tid);
        ds.pausing = false;
        // Before the layer is chosen: `desired_source` reads it.
        ds.budget_small = camera.held_small(camera.level);
        let (publisher, paused) = (ds.publisher, ds.paused);
        if paused {
            self.resume_camera(key, id, now);
        } else {
            self.sync_stream(key, publisher, TrackType::Video, now);
        }
    }

    /// Stops sending a receiver a camera: at the end of the frame it is in, or at once when it is
    /// still waiting for a keyframe and nothing is sent.
    fn pause_camera(&mut self, receiver: SessionKey, stream: u32, now: Instant) {
        let Some(ds) = self
            .sessions
            .get_mut(receiver)
            .and_then(|r| r.down.get_mut(stream))
        else {
            return;
        };
        if ds.paused || ds.pausing {
            return;
        }
        if ds.source.is_some() {
            ds.pausing = true;
        } else {
            self.finish_pause(receiver, stream, now);
        }
    }

    /// Takes a camera out of the stream, to wait where a stream waits for its keyframe, but is not
    /// started by one.
    pub(super) fn finish_pause(&mut self, receiver: SessionKey, stream: u32, now: Instant) {
        let Some(ds) = self
            .sessions
            .get_mut(receiver)
            .and_then(|r| r.down.get_mut(stream))
        else {
            return;
        };
        let sub = SubRef {
            session: receiver,
            stream,
        };
        let publisher = ds.publisher;
        let (feeding, waiting) = (ds.source.take(), ds.target);
        ds.pausing = false;
        let Some(held) = waiting.or(feeding) else {
            return;
        };
        ds.target = Some(held);
        ds.paused = true;
        let sender_id = ds.publisher_session_id.clone();
        ds.paused_at = Some(now);
        if let Some(fan) = feeding.and_then(|src| {
            self.fanout.get_mut(&SourceKey {
                session: publisher,
                source: src,
            })
        }) {
            fan.serving.retain(|s| *s != sub);
        }
        if let Some(fan) = self.fanout.get_mut(&SourceKey {
            session: publisher,
            source: held,
        }) && !fan.waiting.contains(&sub)
        {
            fan.waiting.push(sub);
        }
        self.dc_send(
            receiver,
            DcMessage::CameraPaused {
                sender_id,
                paused: true,
            },
            now,
        );
    }

    /// Lets a paused camera start again at the next keyframe of its sender, which is asked for.
    fn resume_camera(&mut self, receiver: SessionKey, stream: u32, now: Instant) {
        let Some(ds) = self
            .sessions
            .get_mut(receiver)
            .and_then(|r| r.down.get_mut(stream))
        else {
            return;
        };
        ds.paused = false;
        ds.paused_at = None;
        let publisher = ds.publisher;
        let sender_id = ds.publisher_session_id.clone();
        self.dc_send(
            receiver,
            DcMessage::CameraPaused {
                sender_id,
                paused: false,
            },
            now,
        );
        // The camera may have another layer to be sent by now.
        self.sync_stream(receiver, publisher, TrackType::Video, now);
        let target = self
            .sessions
            .get(receiver)
            .and_then(|r| r.down.get(stream))
            .and_then(|d| d.target);
        if let Some(source) = target {
            self.request_keyframe(publisher, source, now);
        }
    }

    /// Bits per second of the cameras a receiver is sent now: they share its path with the
    /// screen. While the cameras are held to the path (see `review_cameras`) the screen comes
    /// first: only the host's camera at its lowest step is taken off the path for it, not the
    /// rate of cameras that are about to be cut.
    fn other_video_rate(&self, key: SessionKey, now: Instant) -> f64 {
        let Some(s) = self.sessions.get(key) else {
            return 0.0;
        };
        let held = s.down.cameras.limit().is_some();
        s.down
            .iter()
            .filter(|(_, d)| d.ty == TrackType::Video)
            .filter_map(|(_, d)| {
                let p = self.sessions.get(d.publisher)?;
                let src = p.up.sources.iter().find(|x| Some(x.id) == d.source)?;
                if held {
                    p.cfg.props.host().then(|| p.up.camera_steps.lowest())
                } else {
                    src.rate.rate(now).map(|r| r as f64)
                }
            })
            .sum()
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

/// What a camera costs at each of its steps, as measured. A layer counts only while it flows and
/// has been measured when the camera sends both: a camera whose small layer is not known to
/// flow is not sent there, and one whose large layer is not is not costed as if it were.
fn camera_steps(sources: &[Source], now: Instant) -> CameraSteps {
    let layer = |layer| {
        sources
            .iter()
            .find(|x| x.ty == TrackType::Video && x.layer == layer)
    };
    let live = |src: &Source| {
        src.rate.rate(now).is_some()
            && src
                .last_packet
                .is_some_and(|t| now.saturating_duration_since(t) < STALL_AFTER)
    };
    let rates = |src: &Source| {
        let temporal = src.temporal_rates.as_ref();
        LayerRates {
            total: src.rate.rate(now),
            temporal: temporal
                .filter(|t| t.seen)
                .map(|t| [t.base.rate(now), t.second.rate(now)]),
            keyframe_bits: temporal
                .and_then(|t| t.keyframe_bytes)
                .map(|b| b as f64 * 8.0),
        }
    };
    let (low, high) = match (layer(Layer::Low), layer(Layer::High)) {
        (Some(l), Some(h)) => match (live(l), live(h)) {
            (true, true) => (Some(l), Some(h)),
            (true, false) => (Some(l), None),
            _ => (None, Some(h)),
        },
        single => single,
    };
    CameraSteps::of(low.map(rates), high.map(rates))
}
