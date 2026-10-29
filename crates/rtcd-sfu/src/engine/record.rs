//! The engine's side of built-in recording: which streams of a call are tapped, what the tap
//! queues, and how events stay ahead of the packets that follow them.
//!
//! The packet path does one thing for a recorded stream (`Recordings::packet`): it builds an item
//! out of the header's numbers and a reference count on the payload, and tries to queue it. It
//! never waits, allocates, locks or logs. Everything else here runs when a stream appears or goes,
//! at the once-a-second upkeep, or when the service asks, and may allocate.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::TrySendError;
use std::time::{Duration, Instant};

use tracing::{debug, info};

use super::session::{SessionKey, Source, SourceKey};
use super::{CallId, Engine};
use crate::ids::TrackType;
use crate::layers::Layer;
use crate::metrics::Metrics;
use crate::record::{RecordBegin, RecordEnd, RecordItem, RecordPacket, RecordSink, StreamOpen};

/// How soon an event that did not fit the queue is tried again when nothing else wakes the engine.
const RETRY: Duration = Duration::from_millis(100);
/// A small layer is recorded only after it has been the only one this long: a sender's layers
/// arrive within moments of each other, and the large one is the one to record.
const LOW_GRACE: Duration = Duration::from_secs(2);
/// How often a recording's queue is looked at for the metrics, and its writer for having quit.
const UPKEEP: Duration = Duration::from_secs(1);

/// Where a tapped stream's packets go: set on the stream's fan-out entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Tap {
    rec: u32,
    stream: u32,
}

/// A stream a recording takes, as the upkeep needs to find it.
#[derive(Debug)]
struct Tapped {
    stream: u32,
    key: SourceKey,
    want_keyframe: Arc<AtomicBool>,
}

/// One recording of a call.
#[derive(Debug)]
pub(crate) struct Active {
    id: String,
    call: CallId,
    sink: RecordSink,
    /// Events the queue had no room for, in order. While there are any, packets are dropped:
    /// nothing may overtake them.
    pending: VecDeque<RecordItem>,
    retry_at: Option<Instant>,
    /// Items the queue took, for its depth.
    queued: u64,
    /// Packets already counted in the metrics.
    dropped_reported: u64,
    next_stream: u32,
    taps: Vec<Tapped>,
    next_upkeep: Instant,
    /// Ended: kept only until its last events are queued.
    closing: bool,
    /// The writer is gone.
    gone: bool,
}

impl Active {
    fn new(id: &str, call: CallId, sink: RecordSink, now: Instant) -> Self {
        Self {
            id: id.to_owned(),
            call,
            sink,
            pending: VecDeque::new(),
            retry_at: None,
            queued: 0,
            dropped_reported: 0,
            next_stream: 0,
            taps: Vec::new(),
            next_upkeep: now + UPKEEP,
            closing: false,
            gone: false,
        }
    }

    /// Queues an event, or keeps it to try again if the queue is full.
    fn send_control(&mut self, item: RecordItem, now: Instant) {
        if self.gone {
            return;
        }
        if !self.pending.is_empty() {
            self.pending.push_back(item);
            return;
        }
        match self.sink.try_send(item) {
            Ok(()) => self.queued += 1,
            Err(TrySendError::Full(item)) => {
                self.pending.push_back(item);
                self.retry_at = Some(now + RETRY);
            }
            Err(TrySendError::Disconnected(_)) => self.gone = true,
        }
    }

    /// Queues the events that waited, as far as there is room. True when none is left.
    fn flush(&mut self, now: Instant) -> bool {
        while let Some(item) = self.pending.pop_front() {
            match self.sink.try_send(item) {
                Ok(()) => self.queued += 1,
                Err(TrySendError::Full(item)) => {
                    self.pending.push_front(item);
                    self.retry_at = Some(now + RETRY);
                    return false;
                }
                Err(TrySendError::Disconnected(_)) => {
                    self.gone = true;
                    self.pending.clear();
                }
            }
        }
        self.retry_at = None;
        true
    }

    /// The packet path: queue one packet of a tapped stream, or count it as dropped.
    fn packet(&mut self, stream: u32, packet: &rtc::rtp::Packet, now: Instant) {
        if !self.pending.is_empty() && !self.flush(now) {
            self.sink.shared().drop_one();
            return;
        }
        let item = RecordItem::Packet(RecordPacket {
            stream,
            seq: packet.header.sequence_number,
            ts: packet.header.timestamp,
            marker: packet.header.marker,
            payload: packet.payload.clone(),
            arrived: now,
        });
        match self.sink.try_send(item) {
            Ok(()) => self.queued += 1,
            Err(TrySendError::Full(_)) => self.sink.shared().drop_one(),
            Err(TrySendError::Disconnected(_)) => {
                self.gone = true;
                self.sink.shared().drop_one();
            }
        }
    }

    /// Once a second: what the metrics report, and whether the writer has had enough. True when
    /// the recording is over.
    fn upkeep(&mut self, metrics: &dyn Metrics, now: Instant) -> bool {
        self.flush(now);
        let shared = self.sink.shared();
        let dropped = shared.dropped();
        if dropped > self.dropped_reported {
            metrics.record_dropped(dropped - self.dropped_reported);
            self.dropped_reported = dropped;
        }
        let depth = self.queued.saturating_sub(shared.taken());
        metrics.record_queue_depth(depth);
        self.gone || shared.is_cancelled()
    }
}

/// The recordings of an engine, addressed by index; each call has at most one.
#[derive(Debug, Default)]
pub(crate) struct Recordings {
    slots: Vec<Option<Active>>,
    live: usize,
}

impl Recordings {
    pub fn is_empty(&self) -> bool {
        self.live == 0
    }

    fn insert(&mut self, a: Active) -> u32 {
        self.live += 1;
        if let Some(i) = self.slots.iter().position(Option::is_none) {
            self.slots[i] = Some(a);
            i as u32
        } else {
            self.slots.push(Some(a));
            (self.slots.len() - 1) as u32
        }
    }

    fn get(&self, idx: u32) -> Option<&Active> {
        self.slots.get(idx as usize)?.as_ref()
    }

    fn get_mut(&mut self, idx: u32) -> Option<&mut Active> {
        self.slots.get_mut(idx as usize)?.as_mut()
    }

    fn remove(&mut self, idx: u32) -> Option<Active> {
        let a = self.slots.get_mut(idx as usize)?.take();
        if a.is_some() {
            self.live -= 1;
        }
        a
    }

    /// The packet path. One bounds-checked index and `Active::packet`.
    pub fn packet(&mut self, tap: Tap, packet: &rtc::rtp::Packet, now: Instant) {
        if let Some(Some(active)) = self.slots.get_mut(tap.rec as usize) {
            active.packet(tap.stream, packet, now);
        }
    }

    /// When an event that waited should be tried again.
    pub fn retry_at(&self) -> Option<Instant> {
        self.slots.iter().flatten().filter_map(|a| a.retry_at).min()
    }

    /// Queues the events that waited; drops the recordings that have ended and have nothing left.
    pub fn retry(&mut self, now: Instant) {
        for i in 0..self.slots.len() {
            let Some(a) = self.slots[i].as_mut() else {
                continue;
            };
            if a.retry_at.is_some_and(|t| t <= now) {
                a.flush(now);
            }
            if a.closing && (a.pending.is_empty() || a.gone) {
                self.slots[i] = None;
                self.live -= 1;
            }
        }
    }
}

/// Whether the recording can take a stream: the formats its files hold.
fn recordable(src: &Source) -> bool {
    let mime = &src.codec.mime_type;
    if src.ty.is_video() {
        mime.eq_ignore_ascii_case("video/vp8")
    } else {
        mime.eq_ignore_ascii_case("audio/opus")
    }
}

impl Engine {
    /// Starts recording a call: every stream it has now, and every one that comes. The recording
    /// reports on `sink`, starting with [`RecordItem::Begin`]; a call that does not exist, or has a
    /// recording already, gets an [`RecordEnd::Refused`] straight after it.
    pub fn record_start(
        &mut self,
        now: Instant,
        group: &str,
        call: &str,
        recording_id: &str,
        t0: Instant,
        sink: RecordSink,
    ) {
        let id = CallId {
            group: group.to_owned(),
            call: call.to_owned(),
        };
        let begin = RecordItem::Begin(Box::new(RecordBegin {
            recording_id: recording_id.to_owned(),
            t0,
        }));
        if !self.calls.get(&id).is_some_and(|c| c.record.is_none()) {
            let _ = sink.try_send(begin);
            let _ = sink.try_send(RecordItem::End {
                reason: RecordEnd::Refused,
                at: now,
            });
            return;
        }
        let mut active = Active::new(recording_id, id.clone(), sink, now);
        active.send_control(begin, now);
        let rec = self.recordings.insert(active);
        let Some(call) = self.calls.get_mut(&id) else {
            return;
        };
        call.record = Some(rec);
        let members = call.members.clone();
        info!(recording = recording_id, call = %id.call, "recording started");
        for member in members {
            for ty in TrackType::ALL {
                self.record_sync(member, ty, now);
            }
        }
    }

    /// Stops the recording of a call: its files are finished and the recording ends.
    pub fn record_stop(&mut self, now: Instant, group: &str, call: &str, recording_id: &str) {
        let id = CallId {
            group: group.to_owned(),
            call: call.to_owned(),
        };
        let Some(rec) = self.calls.get(&id).and_then(|c| c.record) else {
            return;
        };
        if self
            .recordings
            .get(rec)
            .is_some_and(|a| a.id == recording_id)
        {
            self.finish_recording(rec, RecordEnd::Stopped, now);
        }
    }

    /// Ends a recording by telling its writer, after it has been told the streams are over.
    pub(super) fn finish_recording(&mut self, rec: u32, reason: RecordEnd, now: Instant) {
        let Some(active) = self.recordings.get_mut(rec) else {
            return;
        };
        for t in std::mem::take(&mut active.taps) {
            if let Some(fan) = self.fanout.get_mut(&t.key) {
                fan.tap = None;
            }
            active.send_control(
                RecordItem::Close {
                    stream: t.stream,
                    at: now,
                },
                now,
            );
        }
        active.send_control(RecordItem::End { reason, at: now }, now);
        active.closing = true;
        info!(recording = %active.id, ?reason, "recording ended");
        let call = active.call.clone();
        if let Some(c) = self.calls.get_mut(&call)
            && c.record == Some(rec)
        {
            c.record = None;
        }
        self.recordings.retry(now);
    }

    /// Lets go of a recording whose writer has quit: nothing is queued for it any more.
    fn abandon_recording(&mut self, rec: u32) {
        let Some(active) = self.recordings.remove(rec) else {
            return;
        };
        info!(recording = %active.id, "recording abandoned by its writer");
        for t in &active.taps {
            if let Some(fan) = self.fanout.get_mut(&t.key) {
                fan.tap = None;
            }
        }
        if let Some(c) = self.calls.get_mut(&active.call)
            && c.record == Some(rec)
        {
            c.record = None;
        }
    }

    /// Makes the stream of `publisher`'s `ty` that is recorded the one that should be: the large
    /// layer of a camera or a screen when there is one (a camera whose large layer is paused is
    /// recorded at the small one, as every receiver sees it), else the only one, once it has
    /// lasted a moment. Called when a stream appears and at the session's upkeep.
    pub(super) fn record_sync(&mut self, publisher: SessionKey, ty: TrackType, now: Instant) {
        if self.recordings.is_empty() {
            return;
        }
        let Some(p) = self.sessions.get(publisher) else {
            return;
        };
        let Some(rec) = self.calls.get(&p.call).and_then(|c| c.record) else {
            return;
        };
        let of_type = |layer| {
            p.up.sources
                .iter()
                .find(|x| x.ty == ty && x.layer == layer && recordable(x))
        };
        let paused = ty == TrackType::Video && p.up.camera_stall.stalled();
        let chosen = if paused {
            of_type(Layer::Low).or_else(|| of_type(Layer::High))
        } else {
            of_type(Layer::High).or_else(|| of_type(Layer::Low))
        };
        let young = |x: &Source| {
            ty.is_video()
                && !paused
                && x.layer == Layer::Low
                && now.saturating_duration_since(x.since) < LOW_GRACE
        };
        let want = chosen.filter(|x| !young(x)).map(|x| x.id);
        let have = p.up.sources.iter().filter(|x| x.ty == ty).find_map(|x| {
            self.fanout
                .get(&SourceKey {
                    session: publisher,
                    source: x.id,
                })
                .and_then(|f| f.tap)
                .map(|tap| (x.id, tap))
        });
        if have.map(|(id, _)| id) == want {
            return;
        }
        if let Some((source, tap)) = have {
            if let Some(fan) = self.fanout.get_mut(&SourceKey {
                session: publisher,
                source,
            }) {
                fan.tap = None;
            }
            self.record_closed(tap, now);
        }
        if let Some(source) = want {
            self.record_attach(rec, publisher, source, ty, now);
        }
    }

    /// Adds a stream to the recording: its writer is told what it is, and its packets follow.
    fn record_attach(
        &mut self,
        rec: u32,
        publisher: SessionKey,
        source: u32,
        ty: TrackType,
        now: Instant,
    ) {
        let (Some(p), Some(active)) = (self.sessions.get(publisher), self.recordings.get_mut(rec))
        else {
            return;
        };
        let key = SourceKey {
            session: publisher,
            source,
        };
        let Some(fan) = self.fanout.get_mut(&key) else {
            return;
        };
        let stream = active.next_stream;
        active.next_stream += 1;
        let want_keyframe = Arc::new(AtomicBool::new(false));
        debug!(session = %p.cfg.session_id, %ty, stream, "stream recorded");
        active.send_control(
            RecordItem::Open(Box::new(StreamOpen {
                stream,
                kind: ty,
                session_id: p.cfg.session_id.clone(),
                user_id: p.cfg.user_id.clone(),
                want_keyframe: want_keyframe.clone(),
            })),
            now,
        );
        active.taps.push(Tapped {
            stream,
            key,
            want_keyframe,
        });
        fan.tap = Some(Tap { rec, stream });
        // The file opens on a keyframe: ask for one now rather than at the next upkeep.
        if ty.is_video() {
            self.request_keyframe(publisher, source, now);
        }
    }

    /// A tapped stream ended: tell the writer to finish its file.
    pub(super) fn record_closed(&mut self, tap: Tap, now: Instant) {
        if let Some(active) = self.recordings.get_mut(tap.rec) {
            active.taps.retain(|t| t.stream != tap.stream);
            active.send_control(
                RecordItem::Close {
                    stream: tap.stream,
                    at: now,
                },
                now,
            );
        }
    }

    /// The participant turned a stream off (a camera): the writer finishes its file and leaves out
    /// the frames still on their way. The stream stays tapped for when it is turned on again.
    pub(super) fn record_off(&mut self, publisher: SessionKey, ty: TrackType, now: Instant) {
        if self.recordings.is_empty() {
            return;
        }
        let Some(p) = self.sessions.get(publisher) else {
            return;
        };
        for src in p.up.sources.iter().filter(|x| x.ty == ty) {
            let key = SourceKey {
                session: publisher,
                source: src.id,
            };
            if let Some(tap) = self.fanout.get(&key).and_then(|f| f.tap)
                && let Some(active) = self.recordings.get_mut(tap.rec)
            {
                active.send_control(
                    RecordItem::Off {
                        stream: tap.stream,
                        at: now,
                    },
                    now,
                );
            }
        }
    }

    /// The once-a-second upkeep of a session, for the recording of its call: keyframes the writer
    /// wants of this session's streams, and the recording's own upkeep.
    pub(super) fn record_upkeep(&mut self, key: SessionKey, now: Instant) {
        let Some(s) = self.sessions.get(key) else {
            return;
        };
        let Some(rec) = self.calls.get(&s.call).and_then(|c| c.record) else {
            return;
        };
        for ty in [TrackType::Video, TrackType::Screen] {
            self.record_sync(key, ty, now);
        }
        let Some(active) = self.recordings.get_mut(rec) else {
            return;
        };
        let wanted: Vec<u32> = active
            .taps
            .iter()
            .filter(|t| t.key.session == key && t.want_keyframe.swap(false, Ordering::Relaxed))
            .map(|t| t.key.source)
            .collect();
        let over = if now >= active.next_upkeep {
            active.next_upkeep = now + UPKEEP;
            active.upkeep(&*self.metrics, now)
        } else {
            false
        };
        if over {
            self.abandon_recording(rec);
        }
        for source in wanted {
            self.request_keyframe(key, source, now);
        }
    }

    /// Queues events that waited for room, once their time has come.
    pub(super) fn retry_recordings(&mut self, now: Instant) {
        if self.recordings.retry_at().is_some_and(|t| t <= now) {
            self.recordings.retry(now);
        }
    }
}
