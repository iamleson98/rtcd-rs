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
    /// It could not start: the call has no session (after a wait), or has another recording.
    Refused,
    /// A recording with this id exists (or waits for its call) already. This sink is a second
    /// request for it, was not attached to anything, and gets this straight after its `Begin`; the
    /// first recording goes on undisturbed.
    AlreadyRecording,
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

/// The shard's end of a recording's channel. It is handed to [`Sfu::record_start`], which passes it
/// to the shard that owns the call; clones share the queue.
///
/// [`Sfu::record_start`]: crate::runtime::Sfu::record_start
#[derive(Debug, Clone)]
pub struct RecordSink {
    tx: SyncSender<RecordItem>,
    shared: Arc<RecordShared>,
}

impl RecordSink {
    /// Queues an item without waiting, or gives it back if the queue is full or the writer is
    /// gone. This is what a shard does for every packet; other producers (tests) may use it too.
    pub fn try_send(&self, item: RecordItem) -> Result<(), TrySendError<RecordItem>> {
        self.tx.try_send(item)
    }

    /// The counters shared with the writer.
    pub fn shared(&self) -> &RecordShared {
        &self.shared
    }
}

/// The writer's end of a recording's channel.
///
/// **Contract: drain it with [`try_recv`](Self::try_recv) and sleep on your own when it is empty
/// (a couple of milliseconds); never call `recv` or `recv_timeout` on the queue, and do not wrap
/// it in anything that does.** A receiver parked on the queue is registered as a waiter, and
/// then every `try_send` of the shard thread takes a lock and wakes it with a futex call: a
/// system call per packet on the thread that forwards the media. A polling writer is never
/// parked, so a send costs a few atomic operations.
///
/// The writer reads the items in the order they were queued, `Begin` first and `End` last. Use
/// the `arrived` instant of the last item it took as the time of the media while it is behind,
/// and the clock only once `try_recv` says `Empty`: a late packet may still be queued behind the
/// moment that gives up on a gap.
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

/// What one queued item can hold in memory: the payload keeps the datagram it arrived in (up to a
/// 1,500 byte buffer, of which the RTP payload is about 1.3 KB in the biggest packets), and the
/// slot in the queue is the size of an item (a little over 70 bytes).
pub const ITEM_BYTES: usize = 1500 + std::mem::size_of::<RecordItem>();

/// The memory a recording's queue may hold by default: 12 MiB, which is 8,000 items, about two
/// seconds of a call of two hundred people with thirty open streams (a few thousand packets a
/// second).
pub const DEFAULT_QUEUE_BYTES: usize = 12 << 20;

/// How many items fit in a memory `budget`, at [`ITEM_BYTES`] each: the most the queue can pin if
/// the writer stalls, since every item keeps its datagram alive.
pub const fn capacity_for(budget: usize) -> usize {
    let n = budget / ITEM_BYTES;
    if n == 0 { 1 } else { n }
}

/// A recording's channel sized for a memory budget (see [`capacity_for`]).
pub fn record_channel_for(budget: usize) -> (RecordSink, RecordReceiver) {
    record_channel(capacity_for(budget))
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
