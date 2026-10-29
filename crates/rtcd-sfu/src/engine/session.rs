//! The state of one participant's connection.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use rtc::media_stream::MediaStreamTrackId;
use rtc::peer_connection::RTCPeerConnection;
use rtc::peer_connection::transport::RTCIceCandidateInit;
use rtc::rtp_transceiver::rtp_sender::RTCRtpCodec;
use rtc::rtp_transceiver::{RTCRtpReceiverId, RTCRtpSenderId};
use rtcd_proto::SessionConfig;
use rtcd_proto::dc::DcMessage;

use super::CallId;
use super::signaling::{Phase, SignalingLock};
use super::types::Proto;
use crate::bwe::Estimate;
use crate::ids::TrackType;
use crate::layers::{Layer, ScreenLevel, StallWatch};
use crate::rate::RateMonitor;
use crate::rewrite::Rewriter;
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
    /// When a packet of this stream last arrived.
    pub last_packet: Option<Instant>,
    /// When a keyframe was last asked of the sender for this stream.
    pub last_pli: Option<Instant>,
    /// Whether packets of this voice were left out (muted, or not among the loudest) since one
    /// was last forwarded: the receivers' sequence numbers must then carry on from where they
    /// were (see `Rewriter::resume_after_gap`).
    pub gap: bool,
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
    pub vad: Option<Monitor>,
    pub audio_level_ext: Option<u8>,
    /// Whether the large layer of the camera has stopped while the small one flows.
    pub camera_stall: StallWatch,
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
    /// The source currently feeding the stream.
    pub source: Option<u32>,
    /// The source the stream is waiting to be moved onto, at its next keyframe.
    pub target: Option<u32>,
    pub rewrite: Rewriter,
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
    /// Streams were added or removed since the last offer.
    pub changed: bool,
    /// Which layer of a screen share this receiver is sent.
    pub screen_level: ScreenLevel,
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
    /// connection is removed at the next offer.
    pub fn drop_stream(&mut self, id: u32) {
        let Some(ds) = self.get_mut(id) else { return };
        ds.source = None;
        ds.target = None;
        match ds.phase {
            DownPhase::PendingAdd => self.streams[id as usize] = None,
            DownPhase::Adding | DownPhase::Active => {
                ds.phase = DownPhase::PendingRemove;
                self.changed = true;
            }
            DownPhase::PendingRemove | DownPhase::Removing => {}
        }
    }

    /// Whether anything waits for an offer.
    pub fn has_pending(&self) -> bool {
        self.streams
            .iter()
            .flatten()
            .any(|s| matches!(s.phase, DownPhase::PendingAdd | DownPhase::PendingRemove))
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
    /// Consecutive answers that never came.
    pub offer_failures: u8,
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
