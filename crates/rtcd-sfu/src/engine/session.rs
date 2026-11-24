//! The state of one participant's connection.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rtc::media_stream::MediaStreamTrackId;
use rtc::peer_connection::RTCPeerConnection;
use rtc::peer_connection::transport::RTCIceCandidateInit;
use rtc::rtp_transceiver::rtp_sender::RTCRtpCodec;
use rtc::rtp_transceiver::{RTCRtpReceiverId, RTCRtpSenderId};
use rtcd_proto::SessionConfig;
use rtcd_proto::dc::DcMessage;

use super::route::AddrBudget;
use super::signaling::{Phase, SignalingLock};
use super::types::Proto;
use super::{CallId, QUIET_OFFER_DELAY};
use crate::bwe::Estimate;
use crate::camera_budget::{CameraBudget, CameraSteps};
use crate::ids::TrackType;
use crate::layers::{BudgetHold, Layer, RecentPeak, ScreenLevel, StallWatch};
use crate::rate::RateMonitor;
use crate::rewrite::Rewriter;
use crate::temporal::TemporalFilter;
use crate::vad::Monitor;

/// Names a session in the engine: an index that is reused after the session ends, and a
/// generation that tells a stale reference from the session now living at that index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct SessionKey {
    pub idx: u32,
    pub generation: u32,
}

/// Names a media stream a participant sends, within the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SourceKey {
    pub session: SessionKey,
    pub source: u32,
}

/// Where in a receiver's downlink a stream lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SubRef {
    pub session: SessionKey,
    pub stream: u32,
}

/// One media stream a participant sends to the SFU.
#[derive(Debug)]
pub(crate) struct Source {
    pub id: u32,
    pub ssrc: u32,
    pub ty: TrackType,
    pub layer: Layer,
    pub codec: RTCRtpCodec,
    pub track_id: MediaStreamTrackId,
    /// When the stream was first received.
    pub since: Instant,
    pub receiver: RTCRtpReceiverId,
    /// The id receivers see this stream under; layers of one camera share it.
    pub out_track_id: String,
    pub rate: RateMonitor,
    /// For a VP8 video: what its base and second temporal layers bring, for the rungs a receiver
    /// is offered (see `layers::Ladder`, `camera_budget`).
    pub temporal_rates: Option<TemporalRates>,
    /// When a packet of this stream last arrived.
    pub last_packet: Option<Instant>,
    /// When a keyframe was last asked of the sender for this stream.
    pub last_pli: Option<Instant>,
    /// The request that skips the limit on how often a keyframe is asked for was used since the
    /// last one that did not (see `Engine::request_keyframe`).
    pub bypassed: bool,
    /// Whether packets of this voice were left out (muted, or not among the loudest) since one
    /// was last forwarded: the receivers' sequence numbers must then carry on from where they
    /// were (see `Rewriter::resume_after_gap`).
    pub gap: bool,
}

/// The rates of the lower temporal layers of a video's layer, measured as for the whole layer, and
/// how big its keyframes are: a receiver moved onto the layer is sent one first.
#[derive(Debug)]
pub(crate) struct TemporalRates {
    pub base: RateMonitor,
    pub second: RateMonitor,
    /// Whether the stream's frames say which temporal layer they are in.
    pub seen: bool,
    /// Bytes of the last keyframe, and of the one arriving (by its RTP timestamp).
    pub keyframe_bytes: Option<usize>,
    keyframe: Option<(u32, usize)>,
    /// The RTP timestamp and arrival of the last base layer frame, and how long it came after the
    /// one before: the pace a receiver sent only the base layer is sent frames at.
    base_frame: Option<(u32, Instant)>,
    base_gap: Option<Duration>,
}

impl TemporalRates {
    pub fn new(window: std::time::Duration) -> Self {
        Self {
            base: RateMonitor::new(window),
            second: RateMonitor::new(window),
            seen: false,
            keyframe_bytes: None,
            keyframe: None,
            base_frame: None,
            base_gap: None,
        }
    }

    /// Counts a packet of a base layer frame (by its RTP timestamp, which its packets share).
    pub fn count_base_frame(&mut self, ts: u32, now: Instant) {
        if self.base_frame.is_some_and(|(at, _)| at == ts) {
            return;
        }
        if let Some((_, before)) = self.base_frame {
            self.base_gap = Some(now.saturating_duration_since(before));
        }
        self.base_frame = Some((ts, now));
    }

    /// How long a receiver sent only the base layer waits between frames: the last gap between
    /// two of them, or how long it has been since the last one, if that is longer.
    pub fn base_gap(&self, now: Instant) -> Option<Duration> {
        let (_, last) = self.base_frame?;
        let since = now.saturating_duration_since(last);
        Some(self.base_gap.map_or(since, |g| g.max(since)))
    }

    /// Counts a packet of the stream: `key` for the first packet of a keyframe, `last` for the
    /// last packet of a frame.
    pub fn count_keyframe(&mut self, ts: u32, size: usize, key: bool, last: bool) {
        if key {
            self.keyframe = Some((ts, 0));
        }
        if let Some((at, bytes)) = self.keyframe.as_mut()
            && *at == ts
        {
            *bytes += size;
            if last {
                self.keyframe_bytes = Some(*bytes);
                self.keyframe = None;
            }
        }
    }
}

/// What a participant sends.
#[derive(Debug, Default)]
pub(crate) struct Uplink {
    /// The stream id the participant announced for its screen share.
    pub screen_stream_id: String,
    /// The stream id the participant announced for its camera.
    pub video_stream_id: String,
    pub sources: Vec<Source>,
    pub by_ssrc: HashMap<u32, u32>,
    pub next_source: u32,
    /// The id receivers see the participant's voice, screen audio, screen and camera under.
    pub out_ids: HashMap<TrackType, String>,
    /// The participant is muted: its voice is dropped.
    pub muted: bool,
    /// An offer of the participant has an audio section it sends its voice in, whether or not a
    /// packet of it has arrived: receivers are given a reserved stream for it at once, so the
    /// first word of the class renegotiates nobody (see `DownStream::reserved`).
    pub voice_offered: bool,
    /// The stream ids of the audio sections the participant's last offer sends in (an empty id for
    /// a section that names none): which of them is the microphone depends on the screen share's
    /// stream, announced separately, so it is worked out again when that changes.
    pub audio_streams: Vec<String>,
    pub vad: Option<Monitor>,
    pub audio_level_ext: Option<u8>,
    /// Whether the large layer of the camera has stopped while the small one flows.
    pub camera_stall: StallWatch,
    /// The same for the screen share.
    pub screen_stall: StallWatch,
    /// What the camera costs a receiver at each step, as measured at the last upkeep (once a
    /// second per sender, not once per receiver).
    pub camera_steps: CameraSteps,
}

/// Where a stream stands in the receiver's connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DownPhase {
    /// To be added to the connection at the next offer.
    PendingAdd,
    /// Added, in an offer that is not answered yet.
    Adding,
    /// In the connection.
    Active,
    /// To be removed at the next offer.
    PendingRemove,
    /// Removed, in an offer that is not answered yet.
    Removing,
}

/// One stream the SFU sends a participant.
#[derive(Debug)]
pub(crate) struct DownStream {
    pub publisher: SessionKey,
    pub publisher_session_id: String,
    pub ty: TrackType,
    pub out_track_id: String,
    pub ssrc: u32,
    pub codec: RTCRtpCodec,
    pub phase: DownPhase,
    pub sender: Option<RTCRtpSenderId>,
    /// The payload type was read from the sender (it exists once the offer is answered).
    pub payload_type_known: bool,
    /// The payload type the receiver negotiated for redundant audio (see [`crate::red`]): the
    /// stream is sent in it instead of as plain Opus.
    pub red_payload_type: Option<u8>,
    /// How many packets are still to be sent alone, because the ones before them are not what the
    /// receiver was sent (the stream switched source or went on after a gap).
    pub red_hold: usize,
    /// A voice stream that is in the connection ahead of its sender's first packet: it has no
    /// source and is in no fan yet. The first packet only gives it a source (`Engine::sync_stream`),
    /// no offer is needed.
    pub reserved: bool,
    /// The source currently feeding the stream.
    pub source: Option<u32>,
    /// The source the stream is waiting to be moved onto, at its next keyframe.
    pub target: Option<u32>,
    pub rewrite: Rewriter,
    /// Which temporal layers of the stream the receiver is sent.
    pub temporal: TemporalFilter,
    /// A camera the receiver's path cannot carry along with the others: it waits, for the one
    /// keyframe that starts it again, on `target` (see `Engine::review_cameras`).
    pub paused: bool,
    /// Since when the camera has been paused: one paused too long is tried again at its lowest
    /// step (see `camera_budget::MAX_PAUSE`).
    pub paused_at: Option<Instant>,
    /// The camera is being paused: it leaves the stream at the end of the frame it is in, so no
    /// receiver is sent half a frame.
    pub pausing: bool,
    /// The receiver's camera budget holds the camera on its small layer for want of room, though
    /// the large one is flowing (see `Engine::apply_camera_level`): `Engine::desired_source` keeps
    /// the stream there until the budget takes it up.
    pub budget_small: bool,
}

/// What the SFU sends a participant.
#[derive(Debug, Default)]
pub(crate) struct Downlink {
    pub streams: Vec<Option<DownStream>>,
    /// Its cameras and screen share are held to the small layers because the node is near its
    /// send budget (see `egress`).
    pub squeezed: bool,
    /// The data channel is open, so tracks may be pushed.
    pub ready: bool,
    /// Whose cameras the participant wants (all, until it says otherwise).
    pub wanted_video: Option<HashSet<String>>,
    /// Of those, whose it wants small.
    pub low_video: HashSet<String>,
    /// It asked for no screen shares (its page is hidden).
    pub no_screen: bool,
    /// Streams were added or removed since the last offer.
    pub changed: bool,
    /// Since when a silent voice stream has been waiting to be added or removed (see
    /// `QUIET_OFFER_DELAY`); cleared by an offer.
    pub quiet_since: Option<Instant>,
    /// The SFU has made an offer to this session: before the first, everything pending is due.
    pub offered: bool,
    /// Which layer of a screen share this receiver is sent.
    pub screen_level: ScreenLevel,
    /// When the screen share's layer was last decided.
    pub screen_review: Option<Instant>,
    /// What the receiver's path carries of its cameras, and when that was last looked at.
    pub cameras: CameraBudget,
    pub camera_review: Option<Instant>,
    /// When the receiver's plan budget last cut something (see `layers::BudgetHold`).
    pub budget_hold: BudgetHold,
    /// The highest rate the screen share was sent at lately: what the cameras' room is split by.
    pub screen_peak: RecentPeak,
}

impl DownStream {
    /// The source `source` ended. Returns whether the stream is over with it: nothing is left to
    /// feed it, and it is not a voice whose publisher still offers its microphone. That one goes
    /// back to being a reservation, in the connection as it is, so the next source (or the same
    /// microphone's next packet) takes it over without an offer.
    pub fn source_ended(&mut self, source: u32, voice_offered: bool) -> bool {
        if self.source == Some(source) {
            self.source = None;
        }
        if self.target == Some(source) {
            self.target = None;
        }
        if self.source.is_some() || self.target.is_some() {
            return false;
        }
        if self.ty == TrackType::Voice && voice_offered {
            self.reserved = true;
            return false;
        }
        true
    }
}

impl Downlink {
    pub fn insert(&mut self, stream: DownStream) -> u32 {
        self.changed = true;
        if let Some(i) = self.streams.iter().position(Option::is_none) {
            self.streams[i] = Some(stream);
            i as u32
        } else {
            self.streams.push(Some(stream));
            (self.streams.len() - 1) as u32
        }
    }

    pub fn get(&self, id: u32) -> Option<&DownStream> {
        self.streams.get(id as usize)?.as_ref()
    }

    pub fn get_mut(&mut self, id: u32) -> Option<&mut DownStream> {
        self.streams.get_mut(id as usize)?.as_mut()
    }

    pub fn iter(&self) -> impl Iterator<Item = (u32, &DownStream)> {
        self.streams
            .iter()
            .enumerate()
            .filter_map(|(i, s)| Some((i as u32, s.as_ref()?)))
    }

    /// Ends a stream: one that never reached the connection just disappears, one in the
    /// connection is removed at the next offer (a voice's, when its quiet time is over).
    pub fn drop_stream(&mut self, id: u32, now: Instant) {
        let Some(ds) = self.get_mut(id) else { return };
        ds.source = None;
        ds.target = None;
        let voice = ds.ty == TrackType::Voice;
        match ds.phase {
            DownPhase::PendingAdd => self.streams[id as usize] = None,
            DownPhase::Adding | DownPhase::Active => {
                ds.phase = DownPhase::PendingRemove;
                self.changed = true;
                if voice {
                    self.quiet_since.get_or_insert(now);
                }
            }
            DownPhase::PendingRemove | DownPhase::Removing => {}
        }
    }

    /// Whether something waits for an offer that must not wait: a stream to add or remove that is
    /// not a silent voice (a reservation, or the removal of a voice).
    pub fn has_urgent(&self) -> bool {
        self.streams.iter().flatten().any(|s| match s.phase {
            DownPhase::PendingAdd => !s.reserved,
            DownPhase::PendingRemove => s.ty != TrackType::Voice,
            _ => false,
        })
    }

    /// Whether the only thing waiting is a silent voice stream to add (reserved) or to remove.
    fn has_quiet(&self) -> bool {
        self.streams.iter().flatten().any(|s| match s.phase {
            DownPhase::PendingAdd => s.reserved,
            DownPhase::PendingRemove => s.ty == TrackType::Voice,
            _ => false,
        })
    }

    /// Whether the silent voice changes waiting are due for an offer: before the first offer to
    /// the session (a latecomer is brought up to date in one), or once they have waited
    /// [`QUIET_OFFER_DELAY`] and a share of a second that depends on the session `key`.
    pub fn quiet_due(&self, key: SessionKey, now: Instant) -> bool {
        let delay = QUIET_OFFER_DELAY + Duration::from_millis(100 * u64::from(key.idx % 10));
        self.has_quiet()
            && (!self.offered
                || self
                    .quiet_since
                    .is_none_or(|t| now.saturating_duration_since(t) >= delay))
    }
}

/// The data channel the participant opened.
#[derive(Debug)]
pub(crate) struct DcState {
    pub id: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Lifecycle {
    /// Created, waiting for the media connection.
    Connecting,
    Connected,
}

/// A session's place in the line for an offer (see `Engine::begin_turn`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OfferTurn {
    /// Not waiting.
    Free,
    /// In the engine's queue, behind the sessions that came first.
    Queued,
    /// Taken off the queue, with an offer set aside for it in this turn.
    Due,
}

/// A participant's connection and everything the engine tracks about it.
pub(crate) struct Session {
    pub cfg: SessionConfig,
    /// The call it belongs to, for the lookups the packet path makes.
    pub call: CallId,
    pub pc: RTCPeerConnection,
    pub estimate: Arc<Estimate>,
    pub created_at: Instant,
    /// When a packet from the participant last arrived.
    pub last_rx: Instant,
    pub lifecycle: Lifecycle,
    /// On the engine's dirty list.
    pub dirty: bool,
    /// The wake-up time registered in the engine's timer queue.
    pub timer: Option<(Instant, u64)>,

    /// The ICE username fragments STUN checks for this session carry as their local part.
    pub ufrags: Vec<String>,
    /// Remote transport addresses seen for this session.
    pub addrs: Vec<(Proto, SocketAddr)>,
    /// Our host candidates were added to the connection.
    pub candidates_added: bool,
    /// Remote candidates that arrived before the offer they belong to.
    pub early_candidates: Vec<RTCIceCandidateInit>,

    /// When the once-a-second upkeep of video streams next runs.
    pub tick: Option<Instant>,

    pub phase: Phase,
    pub lock: SignalingLock,
    /// Why the SFU has not offered yet, if it wants to: when the first change was noted.
    pub wants_offer_since: Option<Instant>,
    /// How many addresses the session may still gain just now.
    pub new_addrs: AddrBudget,
    /// Where the session is in the engine's queue for an offer.
    pub offer_turn: OfferTurn,
    /// Consecutive answers that never came.
    pub offer_failures: u8,
    /// When an unreadable session description was last logged for this session.
    pub last_unreadable_warn: Option<Instant>,
    pub dc: Option<DcState>,
    /// Data channel frames waiting for the channel to open.
    pub dc_queue: VecDeque<DcMessage>,

    pub up: Uplink,
    pub down: Downlink,
    pub quality: QualityMark,
}

/// Where the counters of a connection's statistics stood when they were last reported.
#[derive(Debug, Default)]
pub(crate) struct QualityMark {
    pub at: Option<Instant>,
    pub nacks: u64,
    pub plis: u64,
    pub firs: u64,
    pub retransmitted_packets: u64,
    pub retransmitted_bytes: u64,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("cfg", &self.cfg)
            .field("lifecycle", &self.lifecycle)
            .finish_non_exhaustive()
    }
}

/// The sessions of an engine, addressed by [`SessionKey`].
#[derive(Debug, Default)]
pub(crate) struct Sessions {
    slots: Vec<Slot>,
    free: Vec<u32>,
    live: usize,
}

#[derive(Debug, Default)]
struct Slot {
    generation: u32,
    value: Option<Session>,
}

impl Sessions {
    pub fn len(&self) -> usize {
        self.live
    }

    pub fn insert(&mut self, session: Session) -> SessionKey {
        self.live += 1;
        if let Some(idx) = self.free.pop() {
            let slot = &mut self.slots[idx as usize];
            slot.value = Some(session);
            SessionKey {
                idx,
                generation: slot.generation,
            }
        } else {
            self.slots.push(Slot {
                generation: 0,
                value: Some(session),
            });
            SessionKey {
                idx: (self.slots.len() - 1) as u32,
                generation: 0,
            }
        }
    }

    pub fn get(&self, key: SessionKey) -> Option<&Session> {
        let slot = self.slots.get(key.idx as usize)?;
        if slot.generation == key.generation {
            slot.value.as_ref()
        } else {
            None
        }
    }

    pub fn get_mut(&mut self, key: SessionKey) -> Option<&mut Session> {
        let slot = self.slots.get_mut(key.idx as usize)?;
        if slot.generation == key.generation {
            slot.value.as_mut()
        } else {
            None
        }
    }

    pub fn remove(&mut self, key: SessionKey) -> Option<Session> {
        let slot = self.slots.get_mut(key.idx as usize)?;
        if slot.generation != key.generation {
            return None;
        }
        let value = slot.value.take()?;
        slot.generation = slot.generation.wrapping_add(1);
        self.free.push(key.idx);
        self.live -= 1;
        Some(value)
    }

    pub fn keys(&self) -> impl Iterator<Item = SessionKey> + '_ {
        self.slots
            .iter()
            .enumerate()
            .filter(|(_, s)| s.value.is_some())
            .map(|(i, s)| SessionKey {
                idx: i as u32,
                generation: s.generation,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::opus_codec;

    const KEY: SessionKey = SessionKey {
        idx: 3,
        generation: 0,
    };

    fn voice_stream(source: u32) -> DownStream {
        DownStream {
            publisher: SessionKey {
                idx: 0,
                generation: 0,
            },
            publisher_session_id: "alice".to_owned(),
            ty: TrackType::Voice,
            out_track_id: "voice_alice_abc".to_owned(),
            ssrc: 7,
            codec: opus_codec().rtp_codec,
            phase: DownPhase::Active,
            sender: None,
            payload_type_known: false,
            red_payload_type: None,
            red_hold: 0,
            reserved: false,
            source: Some(source),
            target: None,
            rewrite: Rewriter::new(7, 0, false),
            temporal: Default::default(),
            paused: false,
            paused_at: None,
            pausing: false,
            budget_small: false,
        }
    }

    #[test]
    fn a_voice_whose_source_ends_while_still_offered_becomes_a_reservation_in_place() {
        let mut down = Downlink::default();
        let id = down.insert(voice_stream(3));
        down.changed = false;
        let ds = down.get_mut(id).unwrap();
        assert!(!ds.source_ended(3, true), "the stream stays");
        assert!(ds.reserved && ds.source.is_none() && ds.target.is_none());
        assert_eq!(ds.out_track_id, "voice_alice_abc", "under the same id");
        assert_eq!(ds.ssrc, 7);
        assert_eq!(ds.phase, DownPhase::Active, "still in the connection");
        assert!(
            !down.has_urgent() && !down.quiet_due(KEY, Instant::now()),
            "no removal, no addition, no offer"
        );
        assert!(!down.changed);
    }

    #[test]
    fn a_silent_voice_waits_for_company_and_anything_else_does_not() {
        let t0 = Instant::now();
        let mut down = Downlink::default();
        let mut reserved = voice_stream(1);
        reserved.phase = DownPhase::PendingAdd;
        reserved.reserved = true;
        reserved.source = None;
        down.insert(reserved);
        down.quiet_since = Some(t0);
        assert!(!down.has_urgent());
        assert!(down.quiet_due(KEY, t0), "before the first offer all is due");

        down.offered = true;
        assert!(!down.quiet_due(KEY, t0 + Duration::from_secs(2)));
        let due = QUIET_OFFER_DELAY + Duration::from_millis(300);
        assert!(!down.quiet_due(KEY, t0 + due - Duration::from_millis(1)));
        assert!(down.quiet_due(KEY, t0 + due), "three seconds and its share");

        // A voice that has a source to send is urgent, so is a camera's removal.
        down.get_mut(0).unwrap().reserved = false;
        assert!(down.has_urgent());
        let mut camera = voice_stream(2);
        camera.ty = TrackType::Video;
        camera.phase = DownPhase::PendingRemove;
        let mut quiet = Downlink::default();
        quiet.insert(camera);
        assert!(quiet.has_urgent());
    }

    #[test]
    fn dropping_an_active_voice_queues_a_quiet_removal() {
        let t0 = Instant::now();
        let mut down = Downlink::default();
        let id = down.insert(voice_stream(3));
        down.offered = true;
        down.drop_stream(id, t0);
        assert_eq!(down.quiet_since, Some(t0));
        assert!(!down.has_urgent());
        assert!(down.quiet_due(KEY, t0 + Duration::from_secs(4)));
    }

    #[test]
    fn a_voice_whose_publisher_no_longer_offers_it_is_over_with_its_source() {
        let mut ds = voice_stream(3);
        assert!(ds.source_ended(3, false));
    }

    #[test]
    fn only_the_source_feeding_a_stream_ends_it() {
        let mut ds = voice_stream(3);
        assert!(!ds.source_ended(4, false), "another source ended");
        assert_eq!(ds.source, Some(3));
        assert!(!ds.reserved);
        let mut camera = voice_stream(3);
        camera.ty = TrackType::Video;
        assert!(camera.source_ended(3, true), "only a voice is held");
    }

    #[test]
    fn the_base_layer_pace_is_the_gap_between_its_frames_or_how_long_it_has_been() {
        let mut t = TemporalRates::new(Duration::from_secs(2));
        let t0 = Instant::now();
        assert_eq!(t.base_gap(t0), None, "nothing seen");
        t.count_base_frame(1000, t0);
        t.count_base_frame(1000, t0 + Duration::from_millis(5));
        assert_eq!(
            t.base_gap(t0 + Duration::from_millis(50)),
            Some(Duration::from_millis(50)),
            "the packets of a frame are not frames"
        );
        t.count_base_frame(1000 + 12_000, t0 + Duration::from_millis(133));
        assert_eq!(
            t.base_gap(t0 + Duration::from_millis(150)),
            Some(Duration::from_millis(133))
        );
        // The picture goes still: the wait grows with the silence.
        assert_eq!(
            t.base_gap(t0 + Duration::from_secs(5)),
            Some(Duration::from_millis(4_867))
        );
    }
}
