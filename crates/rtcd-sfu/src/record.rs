//! What the engine hands a recording: the media of a call as it is forwarded, and the events that
//! order it.
//!
//! A recording is one bounded channel from the shard that owns the call to a writer thread. The
//! shard puts a small item per packet on it with `try_send` (the queue is allocated once, here, and
//! the payload is a reference count on the buffer the packet arrived in), and never waits: a full
//! queue drops the packet and counts it. The writer sees the gap in the RTP sequence numbers and
//! treats it as the loss it is. The events of a stream (it opened, it ended) travel on the same
//! channel so that they stay in order with its packets.
//!
//! The writer must not park on the channel while a recording runs: a receiver that waits there
//! makes every send take a lock and wake it, which is what the shard's packet path must not do.
//! It polls with [`RecordReceiver::try_recv`] and sleeps on its own.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, TrySendError, sync_channel};
use std::time::Instant;

use bytes::Bytes;

use crate::ids::TrackType;

/// One RTP packet of a recorded stream: what writing the file needs and nothing else.
#[derive(Debug)]
pub struct RecordPacket {
    /// Which stream of the recording it belongs to (see [`StreamOpen`]).
    pub stream: u32,
    pub seq: u16,
    /// The RTP timestamp.
    pub ts: u32,
    /// Ends a video frame.
    pub marker: bool,
    pub payload: Bytes,
    /// When the packet reached the SFU.
    pub arrived: Instant,
}

/// The first item of every recording.
#[derive(Debug)]
pub struct RecordBegin {
    pub recording_id: String,
    /// The recording's start: offsets into the recording are measured from it.
    pub t0: Instant,
}

/// A stream joins the recording. Its packets follow, until [`RecordItem::Close`].
#[derive(Debug)]
pub struct StreamOpen {
    pub stream: u32,
    /// What the stream carries.
    pub kind: TrackType,
    /// The call session that sends it, and the user it belongs to.
    pub session_id: String,
    pub user_id: String,
    /// Set by the writer when a video stream needs a keyframe (its file waits for one, or a loss
    /// broke the picture). The engine reads it at its once-a-second upkeep and asks the sender,
    /// at most once a second; nothing is added to the packet path for it.
    pub want_keyframe: Arc<AtomicBool>,
}

/// Why a recording ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordEnd {
    /// It was stopped.
    Stopped,
    /// The last participant left the call.
    CallEnded,
    /// It could not start: the call does not exist (any more) or is being recorded already.
    Refused,
}

/// Something a recording's writer is told, in the order it happened.
#[derive(Debug)]
pub enum RecordItem {
    Packet(RecordPacket),
    /// Always first.
    Begin(Box<RecordBegin>),
    Open(Box<StreamOpen>),
    /// The stream ended (its track was removed, or its sender left): finish its file.
    Close {
        stream: u32,
        at: Instant,
    },
    /// The sender turned the stream off, which it may turn on again: finish its file, and leave
    /// out the frames still on their way.
    Off {
        stream: u32,
        at: Instant,
    },
    /// Always last, and followed by nothing: the streams still open are closed first.
    End {
        reason: RecordEnd,
        at: Instant,
    },
}

/// State the shard and the writer share.
#[derive(Debug, Default)]
pub struct RecordShared {
    /// Packets that did not fit the queue, or were left out to keep an event ahead of them.
    dropped: AtomicU64,
    /// Items the writer has taken out, for the queue's depth.
    taken: AtomicU64,
    /// The writer has ended the recording (its disk is full): the shard lets go of the call.
    cancelled: AtomicBool,
}

impl RecordShared {
    /// Packets dropped so far.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Items the writer has taken so far.
    pub(crate) fn taken(&self) -> u64 {
        self.taken.load(Ordering::Relaxed)
    }

    /// Tells the shard that nothing more is wanted: it stops queueing and lets the call go.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub(crate) fn drop_one(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

/// The shard's end of a recording's channel.
#[derive(Debug)]
pub struct RecordSink {
    tx: SyncSender<RecordItem>,
    shared: Arc<RecordShared>,
}

impl RecordSink {
    /// Queues an item without waiting.
    pub(crate) fn try_send(&self, item: RecordItem) -> Result<(), TrySendError<RecordItem>> {
        self.tx.try_send(item)
    }

    pub(crate) fn shared(&self) -> &RecordShared {
        &self.shared
    }
}

/// The writer's end of a recording's channel.
#[derive(Debug)]
pub struct RecordReceiver {
    rx: Receiver<RecordItem>,
    shared: Arc<RecordShared>,
    taken: u64,
}

impl RecordReceiver {
    /// The next item, or `Empty` when the writer has caught up, or `Disconnected` when the shard
    /// has let go of the recording (after its `End`, or without one if the shard died).
    pub fn try_recv(&mut self) -> Result<RecordItem, TryRecvError> {
        let item = self.rx.try_recv()?;
        self.taken += 1;
        self.shared.taken.store(self.taken, Ordering::Relaxed);
        Ok(item)
    }

    pub fn shared(&self) -> &Arc<RecordShared> {
        &self.shared
    }
}

/// A recording's channel, with room for `capacity` items. The room is allocated here and never
/// again: a burst larger than that is dropped, not queued.
pub fn record_channel(capacity: usize) -> (RecordSink, RecordReceiver) {
    let (tx, rx) = sync_channel(capacity.max(1));
    let shared = Arc::new(RecordShared::default());
    (
        RecordSink {
            tx,
            shared: shared.clone(),
        },
        RecordReceiver {
            rx,
            shared,
            taken: 0,
        },
    )
}

/// Ends a recording that never recorded anything: its writer gets a `Begin` and the end.
pub(crate) fn end_unbegun(
    sink: &RecordSink,
    recording_id: &str,
    t0: Instant,
    reason: RecordEnd,
    now: Instant,
) {
    let _ = sink.try_send(RecordItem::Begin(Box::new(RecordBegin {
        recording_id: recording_id.to_owned(),
        t0,
    })));
    let _ = sink.try_send(RecordItem::End { reason, at: now });
}

/// Why a recording could not be started or stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RecordError {
    #[error("no such call")]
    UnknownCall,
    #[error("the SFU is overloaded")]
    Overloaded,
}
