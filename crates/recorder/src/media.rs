//! The media of a recording.
//!
//! Every stream the SFU forwards to the recorder is written as it arrives, without decoding or
//! re-encoding, into WebM files the browser plays directly (VP8 video, Opus audio). A file is one
//! stretch of continuous media from one person: a camera from when it is turned on until it is
//! turned off, a voice while it is unmuted. Each starts at its own offset into the recording, so
//! the player lines them up again.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use tracing::{error, info, warn};

use crate::disk::Disk;
use crate::reorder::{Item, Reorder};
use crate::tap::TapPacket;
use crate::vp8;
use crate::webm::{Track, WebmWriter};

/// Why a file could not be opened: the recording's limit of files is reached.
#[derive(Debug)]
struct FileLimit;

impl std::fmt::Display for FileLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the recording's limit of files is reached")
    }
}

impl std::error::Error for FileLimit {}

/// A voice that stops this long (a muted microphone, a dropped connection) closes its file; the
/// next frame opens a new one. Shorter gaps are bridged with silence, so a file plays as one
/// continuous stream.
pub const SEGMENT_GAP: Duration = Duration::from_secs(2);
/// A shared screen that sends nothing is still there: a still screen sends a frame a second at
/// best and nothing at all for seconds at a time, and the player shows a file only while it
/// covers the moment, so closing the file would blank the screen. The last picture is held (the
/// file stays open) until the share ends, which the SFU and the server say; this is the longest
/// silence waited for if they never do.
pub const VIDEO_HOLD: Duration = Duration::from_secs(30);
/// A camera sends all the time, so a camera that stops for this long is off or gone. Turning it
/// off is announced too (`user_video_off`), and ends its file at once.
pub const CAMERA_HOLD: Duration = Duration::from_secs(5);
/// How long after a person turns a camera or share off the frames still arriving are dropped.
const TURN_OFF_TAIL: Duration = Duration::from_millis(1500);
/// Each recorded camera and screen is asked for a keyframe this often: a seek decodes from the
/// keyframe before the target, and browsers otherwise send one only every few minutes.
pub const KEYFRAME_EVERY: Duration = Duration::from_secs(20);
/// While a file waits for its first keyframe, or after a loss, the sender is asked this often.
const KEYFRAME_RETRY: Duration = Duration::from_secs(1);
/// A keyframe of another size than the open file's is a new picture size only if the sender
/// stays at it: encoders step the resolution down and back within a fraction of a second, and a
/// file for each step would make the player load a new file for one frame.
const SIZE_SETTLE: Duration = Duration::from_secs(1);
/// How long a missing video packet is waited for (the sender's answer to a NACK), and how many
/// packets are held behind the gap: a keyframe is dozens of packets, a voice frame one.
const VIDEO_WAIT: Duration = Duration::from_millis(300);
const VIDEO_HELD: usize = 256;
const AUDIO_WAIT: Duration = Duration::from_millis(120);
const AUDIO_HELD: usize = 32;

/// Kinds of recorded files (the manifest's vocabulary, shared with the server and the player).
pub const VOICE: &str = "voice";
pub const CAMERA: &str = "camera";
pub const SCREEN: &str = "screen";
pub const SCREEN_AUDIO: &str = "screen_audio";

/// Maps the SFU's track ids (`<type>_<session>_<random>`) to a kind.
pub fn track_kind(track_id: &str) -> Option<&'static str> {
    match track_id.split_once('_')?.0 {
        "voice" => Some(VOICE),
        "video" => Some(CAMERA),
        "screen" => Some(SCREEN),
        "screen-audio" => Some(SCREEN_AUDIO),
        _ => None,
    }
}

fn skip_zero(v: &u32) -> bool {
    *v == 0
}

/// One file of the recording, as the manifest lists it.
#[derive(Debug, Clone, Serialize)]
pub struct Entry {
    pub kind: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub session_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub user_id: String,
    /// Place the file on the recording's timeline.
    pub start_ms: i64,
    pub end_ms: i64,
    #[serde(skip_serializing_if = "skip_zero")]
    pub width: u32,
    #[serde(skip_serializing_if = "skip_zero")]
    pub height: u32,
    pub size: u64,
    #[serde(skip)]
    pub path: PathBuf,
}

/// A recording's files while it runs.
#[derive(Debug)]
pub struct Recording {
    dir: PathBuf,
    start: Instant,
    pub disk: Disk,
    files: Mutex<Vec<Entry>>,
    next: AtomicU32,
    /// The most files that may be opened; zero is no limit.
    max_files: u32,
}

impl Recording {
    pub fn new(dir: &Path, start: Instant, limit: u64) -> Self {
        Self {
            dir: dir.to_owned(),
            start,
            disk: Disk::new(limit),
            files: Mutex::new(Vec::new()),
            next: AtomicU32::new(0),
            max_files: 0,
        }
    }

    /// Allows at most `n` files to be opened (zero, no limit): once `n` have been, a stream that
    /// would open another cannot, and what it receives is left out. A bound on the numbers the
    /// files will need, not a cut of the ones being written.
    pub fn with_file_limit(mut self, n: u32) -> Self {
        self.max_files = n;
        self
    }

    /// Whether every file the limit allows has been opened.
    pub fn file_limit_reached(&self) -> bool {
        self.max_files > 0 && self.next.load(Ordering::Relaxed) >= self.max_files
    }

    /// A moment's offset into the recording, in milliseconds.
    pub fn at(&self, t: Instant) -> i64 {
        t.saturating_duration_since(self.start).as_millis() as i64
    }

    pub fn start(&self) -> Instant {
        self.start
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The path of the next file, `None` once the limit of files is reached.
    pub fn new_path(&self, kind: &str) -> Option<PathBuf> {
        let max = self.max_files;
        let n = self
            .next
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                (max == 0 || n < max).then_some(n + 1)
            })
            .ok()?
            + 1;
        Some(self.dir.join(format!("{n:05}-{kind}.webm")))
    }

    /// A file that was given a path and could not be created does not count against the limit.
    fn give_back_path(&self) {
        self.next.fetch_sub(1, Ordering::Relaxed);
    }

    fn add(&self, e: Entry) {
        self.files.lock().unwrap_or_else(|p| p.into_inner()).push(e);
    }

    /// The files finished so far.
    pub fn files(&self) -> Vec<Entry> {
        self.files.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    /// The files finished since this was last called: for a recording whose files are taken away
    /// (uploaded) while it runs.
    pub fn take_files(&self) -> Vec<Entry> {
        std::mem::take(&mut *self.files.lock().unwrap_or_else(|p| p.into_inner()))
    }
}

/// Maps call sessions to their users (`call_state`, `user_joined`). A file looks its user up when
/// it is opened, not when its track appears: the server may say who a session is a moment later.
pub type Users = Arc<Mutex<HashMap<String, String>>>;

/// What a stream is.
#[derive(Debug, Clone)]
pub struct StreamInfo {
    pub kind: &'static str,
    pub session_id: String,
    pub video: bool,
}

struct Segment {
    entry: Entry,
    /// When the file was opened (the arrival of its first frame).
    opened: Instant,
    /// The picture size the file was opened with (the first keyframe's).
    size: (u32, u32),
    writer: WebmWriter,
    /// The last frame's RTP timestamp, and its time unwrapped, relative to the first.
    prev_ts: u32,
    ext: i64,
    /// The last frame's time (ms into the file), how long a video frame lasts (the gap between
    /// the last two) and the end of the file's span.
    last_ms: i64,
    frame_ms: i64,
    end_ms: i64,
}

/// A stream being written.
pub struct StreamRecorder {
    info: StreamInfo,
    rec: Arc<Recording>,
    users: Users,
    reorder: Reorder,
    assembler: vp8::Assembler,
    seg: Option<Segment>,
    /// When a packet last arrived, and when the stream last delivered a frame (written or not).
    last_packet: Option<Instant>,
    last_media: Option<Instant>,
    /// A video frame went missing: nothing is written until the next keyframe.
    need_key: bool,
    /// Since when the keyframes seen are of another size than the open file's.
    size_since: Option<Instant>,
    last_ask: Option<Instant>,
    /// Frames before this are the tail of a stream the sender has just turned off.
    ignore_until: Option<Instant>,
    /// A file is closed and the next one opened once it has lasted this long, so that files can
    /// be taken away while the recording runs.
    rotate_every: Option<Duration>,
    /// Frames and packets lost, for the log at the end.
    pub lost: u64,
}

impl std::fmt::Debug for StreamRecorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamRecorder")
            .field("info", &self.info)
            .finish_non_exhaustive()
    }
}

const OPUS_SILENCE: [u8; 3] = [0xf8, 0xff, 0xfe];
const OPUS_FRAME_MS: i64 = 20;

/// How much audio an Opus packet holds, in milliseconds, from its table of contents (RFC 6716
/// section 3.1): the frame size its config names times the number of frames. A packet that cannot be
/// read counts as one 20 ms frame.
fn opus_packet_ms(data: &[u8]) -> i64 {
    let Some(&toc) = data.first() else {
        return OPUS_FRAME_MS;
    };
    let config = usize::from(toc >> 3);
    // Samples of a frame at 48 kHz: SILK 10, 20, 40 or 60 ms; hybrid 10 or 20 ms; CELT 2.5, 5, 10
    // or 20 ms.
    let frame_samples: i64 = match config {
        0..=11 => [480, 960, 1920, 2880][config % 4],
        12..=15 => [480, 960][config % 2],
        _ => [120, 240, 480, 960][config % 4],
    };
    let frames = match toc & 3 {
        0 => 1,
        1 | 2 => 2,
        _ => match data.get(1).map(|b| i64::from(b & 0x3f)) {
            Some(n) if n > 0 => n,
            _ => return OPUS_FRAME_MS,
        },
    };
    (frame_samples * frames / 48).clamp(1, 120)
}

impl StreamRecorder {
    pub fn new(info: StreamInfo, rec: Arc<Recording>, users: Users) -> Self {
        let (wait, held) = if info.video {
            (VIDEO_WAIT, VIDEO_HELD)
        } else {
            (AUDIO_WAIT, AUDIO_HELD)
        };
        Self {
            info,
            rec,
            users,
            reorder: Reorder::new(wait, held),
            assembler: vp8::Assembler::new(),
            seg: None,
            last_packet: None,
            last_media: None,
            need_key: false,
            size_since: None,
            last_ask: None,
            ignore_until: None,
            rotate_every: None,
            lost: 0,
        }
    }

    /// Ends each file once it has lasted `every`: a voice at its next packet, a video at its next
    /// keyframe (so every file opens on one), with the old picture held up to the new.
    pub fn rotating(mut self, every: Duration) -> Self {
        self.rotate_every = Some(every);
        self
    }

    /// Whether the open file has lasted as long as files may.
    fn rotation_due(&self, at: Instant) -> bool {
        match (&self.seg, self.rotate_every) {
            (Some(seg), Some(every)) => at.saturating_duration_since(seg.opened) >= every,
            _ => false,
        }
    }

    /// Whether a file of this stream is open.
    pub fn is_writing(&self) -> bool {
        self.seg.is_some()
    }

    pub fn info(&self) -> &StreamInfo {
        &self.info
    }

    /// Takes a packet, which arrived when it says. Returns whether the sender should be asked for
    /// a keyframe.
    pub fn packet(&mut self, p: TapPacket) -> bool {
        let now = p.arrived;
        self.last_packet = Some(now);
        self.reorder.push(p);
        self.drain(now, now);
        self.want_keyframe(now)
    }

    /// Upkeep: gives up gaps that were waited for, closes idle files, and says whether a keyframe
    /// should be asked for.
    pub fn tick(&mut self, now: Instant) -> bool {
        self.drain(now, now);
        if self.seg.is_some()
            && self
                .last_media
                .is_some_and(|m| now.saturating_duration_since(m) > self.silence_limit())
        {
            self.close_segment("silent", None);
        }
        self.want_keyframe(now)
    }

    /// How long a stream may send nothing before its file is closed.
    fn silence_limit(&self) -> Duration {
        if !self.info.video {
            SEGMENT_GAP
        } else if self.info.kind == CAMERA {
            CAMERA_HOLD
        } else {
            VIDEO_HOLD
        }
    }

    /// Whether to ask a video sender for a keyframe now: often while a file waits for its first
    /// one or a loss broke the picture, and regularly after that (seeking decodes from the
    /// keyframe before the target). A sender that sends nothing is not asked.
    fn want_keyframe(&mut self, now: Instant) -> bool {
        if !self.info.video {
            return false;
        }
        let active = self
            .last_packet
            .is_some_and(|t| now.saturating_duration_since(t) < SEGMENT_GAP);
        let wait = if self.seg.is_none() || self.need_key {
            KEYFRAME_RETRY
        } else {
            KEYFRAME_EVERY
        };
        let due = self
            .last_ask
            .is_none_or(|t| now.saturating_duration_since(t) >= wait);
        if active && due {
            self.last_ask = Some(now);
            return true;
        }
        false
    }

    /// When [`Self::tick`] next has something to do because of a gap.
    pub fn deadline(&self) -> Option<Instant> {
        self.reorder.deadline()
    }

    /// Writes what is ready at `now`; gaps are given up as if it were `give_up_at` (the end of the
    /// stream gives them all up at once, but nothing arrived later than `now`).
    fn drain(&mut self, now: Instant, give_up_at: Instant) {
        while let Some(item) = self.reorder.pop(give_up_at) {
            match item {
                Item::Packet(p) => {
                    if self.info.video {
                        self.assembler.push(now, &p);
                        while let Some((f, data)) = self.assembler.frame() {
                            self.video_frame(f.arrived, f.timestamp, f.key, &data);
                            self.assembler.recycle(data);
                        }
                    } else {
                        self.audio_frame(now, p.ts, &p.payload);
                    }
                }
                Item::Lost(n) => {
                    self.lost += u64::from(n);
                    if self.info.video {
                        self.assembler.lost();
                        if !self.need_key {
                            // The picture is broken until the next keyframe: ask at once.
                            self.need_key = true;
                            self.last_ask = None;
                        }
                    }
                }
            }
        }
    }

    fn video_frame(&mut self, arrived: Instant, ts: u32, key: bool, data: &[u8]) {
        if self.ignore_until.is_some_and(|t| arrived < t) {
            return;
        }
        if self.seg.is_some()
            && self
                .last_media
                .is_some_and(|m| arrived.saturating_duration_since(m) > self.silence_limit())
        {
            self.close_segment("silent", None);
        }
        self.last_media = Some(arrived);
        if key && self.rotation_due(arrived) {
            self.close_segment("rotated", Some(arrived));
        }
        // A keyframe of another size (the SFU moved the share to its other layer, or the sharer
        // picked another window) starts a file of its own, so each file's header tells the truth.
        // The old picture is held up to the new one. Until the new size has lasted, the frames of
        // the other size are left out (they do not decode in this file), and a keyframe of the
        // file's own size takes up where it left off.
        if key
            && let Some(seg) = &self.seg
            && let Some(size) = vp8::size(data)
        {
            if seg.size == size {
                self.size_since = None;
            } else {
                let since = *self.size_since.get_or_insert(arrived);
                if arrived.saturating_duration_since(since) >= SIZE_SETTLE {
                    self.close_segment("the picture changed size", Some(arrived));
                } else {
                    self.need_key = true;
                    self.last_ask = None;
                    return;
                }
            }
        }
        if key {
            self.need_key = false;
        }
        // Nothing before a keyframe decodes, and nothing after a loss until the next one.
        if self.need_key || (self.seg.is_none() && !key) {
            return;
        }
        if self.seg.is_none() {
            let (w, h) = vp8::size(data).unwrap_or((0, 0));
            if let Err(e) = self.open(
                arrived,
                ts,
                Track::Vp8 {
                    width: w,
                    height: h,
                },
                (w, h),
            ) {
                self.open_failed(&e);
                return;
            }
        }
        self.write(ts, key, data, true);
    }

    fn audio_frame(&mut self, now: Instant, ts: u32, data: &[u8]) {
        if data.is_empty() || self.ignore_until.is_some_and(|t| now < t) {
            return;
        }
        if self.seg.is_some()
            && self
                .last_media
                .is_some_and(|m| now.saturating_duration_since(m) > SEGMENT_GAP)
        {
            self.close_segment("silent", None);
        }
        self.last_media = Some(now);
        if self.rotation_due(now) {
            self.close_segment("rotated", None);
        }
        if self.seg.is_none()
            && let Err(e) = self.open(now, ts, Track::Opus, (0, 0))
        {
            self.open_failed(&e);
            return;
        }
        self.write(ts, true, data, false);
    }

    /// A file could not be opened. Reaching the limit of files is not an error of the stream, and
    /// whoever set the limit knows (every packet that follows would say it again).
    fn open_failed(&self, e: &io::Error) {
        if !e.get_ref().is_some_and(|inner| inner.is::<FileLimit>()) {
            error!(kind = self.info.kind, error = %e, "cannot open a segment");
        }
    }

    fn open(
        &mut self,
        arrived: Instant,
        ts: u32,
        track: Track,
        size: (u32, u32),
    ) -> io::Result<()> {
        let Some(path) = self.rec.new_path(self.info.kind) else {
            return Err(io::Error::other(FileLimit));
        };
        let writer = match WebmWriter::create(&path, &track, self.info.kind, self.rec.disk.clone())
        {
            Ok(w) => w,
            Err(e) => {
                self.rec.give_back_path();
                return Err(e);
            }
        };
        let user_id = self
            .users
            .lock()
            .map(|u| u.get(&self.info.session_id).cloned().unwrap_or_default())
            .unwrap_or_default();
        let entry = Entry {
            kind: self.info.kind.to_owned(),
            session_id: self.info.session_id.clone(),
            user_id,
            start_ms: self.rec.at(arrived).max(0),
            end_ms: 0,
            width: size.0,
            height: size.1,
            size: 0,
            path,
        };
        info!(
            kind = self.info.kind,
            session_id = self.info.session_id,
            file = %entry.path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default(),
            start_ms = entry.start_ms,
            width = size.0,
            height = size.1,
            "file opened"
        );
        self.seg = Some(Segment {
            entry,
            opened: arrived,
            size,
            writer,
            prev_ts: ts,
            ext: 0,
            last_ms: 0,
            frame_ms: 33,
            end_ms: 0,
        });
        Ok(())
    }

    fn write(&mut self, ts: u32, key: bool, data: &[u8], video: bool) {
        let Some(seg) = self.seg.as_mut() else { return };
        let clock: i64 = if video { 90_000 } else { 48_000 };
        let next = seg.ext + i64::from(ts.wrapping_sub(seg.prev_ts) as i32);
        if next < seg.ext {
            return; // late duplicate
        }
        let ms = next * 1000 / clock;
        if !video {
            // Short silences (DTX, a lost burst) are bridged: a continuous track keeps players
            // from stalling on the hole.
            while seg.end_ms + OPUS_FRAME_MS <= ms {
                seg.writer.write_block(false, seg.end_ms, &OPUS_SILENCE);
                seg.end_ms += OPUS_FRAME_MS;
            }
        } else if seg.end_ms > 0 {
            seg.frame_ms = (ms - seg.last_ms).clamp(1, 1000);
        }
        seg.ext = next;
        seg.prev_ts = ts;
        seg.writer.write_block(key, ms, data);
        seg.last_ms = ms;
        let span = if video {
            seg.frame_ms
        } else {
            opus_packet_ms(data)
        };
        seg.end_ms = seg.end_ms.max(ms + span);
    }

    /// Finishes the open file. `hold_until` is when a picture that is still on screen stops being
    /// shown (the camera or share ended, or its next picture came), so the file covers that long;
    /// `None` ends it with its last frame.
    fn close_segment(&mut self, why: &'static str, hold_until: Option<Instant>) {
        let Some(mut seg) = self.seg.take() else {
            return;
        };
        self.size_since = None;
        if self.info.video
            && let Some(t) = hold_until
        {
            let held = self.rec.at(t) - seg.entry.start_ms;
            seg.end_ms = seg.end_ms.max(held);
        }
        let path = seg.entry.path.clone();
        match seg.writer.finish(seg.end_ms) {
            Ok(done) => {
                seg.entry.size = done.size;
                seg.entry.end_ms = seg.entry.start_ms + seg.end_ms;
                info!(
                    kind = self.info.kind,
                    session_id = self.info.session_id,
                    start_ms = seg.entry.start_ms,
                    end_ms = seg.entry.end_ms,
                    bytes = done.size,
                    why,
                    "file closed"
                );
                self.rec.add(seg.entry);
            }
            Err(e) => {
                error!(kind = self.info.kind, error = %e, "cannot finish a segment");
                if let Err(e) = std::fs::remove_file(&path)
                    && e.kind() != io::ErrorKind::NotFound
                {
                    warn!(error = %e, "cannot remove an unfinished segment");
                }
            }
        }
    }

    /// The stream ended at `now` (its track was removed, or the call ended): what is held is
    /// released and the file is finished. A picture that was on screen stays until `now`.
    pub fn finish(&mut self, now: Instant, why: &'static str) {
        // Packets behind a gap are released as they are.
        self.drain(now, now + Duration::from_secs(3600));
        // A stream that had been silent beyond the limit ended before `now`; its last picture is
        // not held up to it.
        let held = self
            .last_media
            .is_some_and(|m| now.saturating_duration_since(m) <= self.silence_limit())
            .then_some(now);
        self.close_segment(why, held);
    }

    /// The sender turned this stream off (the call said so); the stream stays, a later frame
    /// opens a new file.
    pub fn stop(&mut self, now: Instant, why: &'static str) {
        self.finish(now, why);
        self.last_media = None;
        // Packets already on their way would open a file for a stream that is over.
        self.ignore_until = Some(now + TURN_OFF_TAIL);
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_recording_with_a_file_limit_opens_no_more_files_than_it() {
        let dir = std::env::temp_dir().join(format!("recorder-limit-{}", std::process::id()));
        let rec = Recording::new(&dir, Instant::now(), u64::MAX).with_file_limit(2);
        assert!(!rec.file_limit_reached());
        assert!(rec.new_path("voice").is_some());
        rec.give_back_path(); // a file that could not be created does not count
        assert!(rec.new_path("voice").is_some());
        assert!(rec.new_path("camera").is_some());
        assert!(rec.file_limit_reached());
        assert!(rec.new_path("voice").is_none(), "the third has no number");
        let free = Recording::new(&dir, Instant::now(), u64::MAX);
        assert!((0..1000).all(|_| free.new_path("voice").is_some()));
        assert!(!free.file_limit_reached(), "zero is no limit");
    }

    use super::*;
    use crate::webm::parse::*;

    const CLUSTER: u32 = 0x1F43_B675;
    const SIMPLE_BLOCK: u32 = 0xA3;
    const TIMECODE: u32 = 0xE7;
    const SEGMENT: u32 = 0x1853_8067;
    const TRACKS: u32 = 0x1654_AE6B;
    const TRACK_ENTRY: u32 = 0xAE;
    const CODEC_ID: u32 = 0x86;
    const INFO: u32 = 0x1549_A966;
    const DURATION: u32 = 0x4489;
    const CUES: u32 = 0x1C53_BB6B;

    fn tmp(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("media-test-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn setup(tag: &str, limit: u64) -> (Arc<Recording>, Instant) {
        let start = Instant::now();
        (Arc::new(Recording::new(&tmp(tag), start, limit)), start)
    }

    fn opus(arrived: Instant, seq: u16, ts: u32, payload: &[u8]) -> TapPacket {
        TapPacket {
            seq,
            ts,
            marker: false,
            payload: payload.to_vec().into(),
            arrived,
        }
    }

    fn voice(rec: &Arc<Recording>) -> StreamRecorder {
        let users: Users = Users::default();
        users.lock().unwrap().insert("sess".into(), "user".into());
        StreamRecorder::new(
            StreamInfo {
                kind: VOICE,
                session_id: "sess".into(),
                video: false,
            },
            rec.clone(),
            users,
        )
    }

    fn camera(rec: &Arc<Recording>) -> StreamRecorder {
        StreamRecorder::new(
            StreamInfo {
                kind: CAMERA,
                session_id: "s".into(),
                video: true,
            },
            rec.clone(),
            Users::default(),
        )
    }

    /// Feeds `count` 20 ms Opus packets, the first arriving at `at`.
    fn speak(s: &mut StreamRecorder, at: Instant, seq: u16, ts: u32, count: usize) {
        for i in 0..count {
            s.packet(opus(
                at + Duration::from_millis(20 * i as u64),
                seq + i as u16,
                ts + 960 * i as u32,
                &[0xfc, 1, 2],
            ));
        }
    }

    /// The block times (ms) of a file.
    fn block_times(path: &Path) -> Vec<i64> {
        let data = std::fs::read(path).unwrap();
        let top = elements(&data, 0, data.len());
        let seg = top.iter().find(|e| e.id == SEGMENT).unwrap();
        let mut out = Vec::new();
        for c in find(&data, *seg, CLUSTER) {
            let kids = elements(&data, c.start, c.start + c.len);
            let base = uint(&data, *kids.iter().find(|e| e.id == TIMECODE).unwrap()) as i64;
            for b in kids.iter().filter(|e| e.id == SIMPLE_BLOCK) {
                out.push(
                    base + i64::from(i16::from_be_bytes([data[b.start + 1], data[b.start + 2]])),
                );
            }
        }
        out
    }

    fn codec(path: &Path) -> String {
        let data = std::fs::read(path).unwrap();
        let top = elements(&data, 0, data.len());
        let seg = top.iter().find(|e| e.id == SEGMENT).unwrap();
        let tracks = find(&data, *seg, TRACKS)[0];
        let entry = find(&data, tracks, TRACK_ENTRY)[0];
        let id = find(&data, entry, CODEC_ID)[0];
        String::from_utf8(data[id.start..id.start + id.len].to_vec()).unwrap()
    }

    fn has_cues_and_duration(path: &Path) -> bool {
        let data = std::fs::read(path).unwrap();
        let top = elements(&data, 0, data.len());
        let seg = top.iter().find(|e| e.id == SEGMENT).unwrap();
        let info = find(&data, *seg, INFO)[0];
        let d = float(&data, find(&data, info, DURATION)[0]);
        !find(&data, *seg, CUES).is_empty() && d > 0.0
    }

    #[test]
    fn voice_is_split_where_the_microphone_was_muted() {
        let (rec, start) = setup("voice", u64::MAX);
        let mut s = voice(&rec);
        // One second of speech, a 200 ms hole (bridged), more speech, then 5 s muted.
        speak(&mut s, start + Duration::from_secs(1), 100, 0, 50);
        speak(
            &mut s,
            start + Duration::from_millis(2200),
            150,
            50 * 960 + 10 * 960,
            50,
        );
        speak(&mut s, start + Duration::from_secs(8), 200, 400_000, 25);
        s.finish(start + Duration::from_secs(9), "test");

        let files = rec.files();
        assert_eq!(files.len(), 2, "{files:?}");
        let (first, second) = (&files[0], &files[1]);
        assert_eq!(
            (
                first.start_ms,
                first.kind.as_str(),
                first.session_id.as_str(),
                first.user_id.as_str()
            ),
            (1000, VOICE, "sess", "user")
        );
        assert_eq!(second.start_ms, 8000);
        // 1 s + 200 ms bridged + 1 s, ending on the last frame's end.
        assert_eq!(first.end_ms - first.start_ms, 2200);
        assert_eq!(codec(&first.path), "A_OPUS");
        assert!(
            has_cues_and_duration(&first.path),
            "the file would not seek"
        );
        let times = block_times(&first.path);
        assert_eq!(times.len(), 110, "100 frames and 10 of silence");
        assert!(
            times.windows(2).all(|w| w[1] - w[0] == 20),
            "no hole in the track: {times:?}"
        );
        assert_eq!(first.size, std::fs::metadata(&first.path).unwrap().len());
    }

    #[test]
    fn the_end_of_a_stream_releases_what_it_held_into_the_same_file() {
        let (rec, start) = setup("release", u64::MAX);
        let mut s = voice(&rec);
        // Packet 2 never came; 3 waits behind the gap when the stream ends.
        for (i, seq) in [0u16, 1, 3].iter().enumerate() {
            s.packet(opus(
                start + Duration::from_millis(20 * i as u64),
                *seq,
                u32::from(*seq) * 960,
                &[0xfc, 1, 2],
            ));
        }
        s.finish(start + Duration::from_millis(100), "test");
        let files = rec.files();
        assert_eq!(
            files.len(),
            1,
            "a held packet must not open a file of its own: {files:?}"
        );
        assert!(files[0].start_ms < 100, "{files:?}");
        assert_eq!(
            block_times(&files[0].path),
            [0, 20, 40, 60],
            "the hole is bridged with silence"
        );
    }

    #[test]
    fn packets_of_forty_milliseconds_are_recorded_without_a_hole_after_each() {
        let (rec, start) = setup("ptime40", u64::MAX);
        let mut s = voice(&rec);
        // 0xfd: two 20 ms frames in a packet, as a browser asked for 40 ms packets sends them.
        for i in 0..100u32 {
            s.packet(opus(
                start + Duration::from_millis(40 * u64::from(i)),
                i as u16,
                1920 * i,
                &[0xfd, 1, 2],
            ));
        }
        s.finish(start + Duration::from_secs(4), "test");
        let files = rec.files();
        assert_eq!(files.len(), 1, "{files:?}");
        let times = block_times(&files[0].path);
        assert_eq!(
            times.len(),
            100,
            "no silent block was added between packets"
        );
        assert!(times.windows(2).all(|w| w[1] - w[0] == 40), "{times:?}");
    }

    #[test]
    fn the_length_of_an_opus_packet_comes_from_its_table_of_contents() {
        assert_eq!(opus_packet_ms(&[0xfc, 1]), 20, "one 20 ms frame");
        assert_eq!(
            opus_packet_ms(&[0xfd, 1]),
            40,
            "two frames of the same size"
        );
        assert_eq!(
            opus_packet_ms(&[0xfe, 1]),
            40,
            "two frames of different sizes"
        );
        assert_eq!(opus_packet_ms(&[0x10, 1]), 40, "one 40 ms SILK frame");
        assert_eq!(opus_packet_ms(&[0x18, 1]), 60, "one 60 ms SILK frame");
        assert_eq!(
            opus_packet_ms(&[0xfb, 3, 0]),
            60,
            "three 20 ms frames, arbitrary count"
        );
        assert_eq!(opus_packet_ms(&[0xf8]), 20, "the silence frame");
        assert_eq!(
            opus_packet_ms(&[0xfb]),
            20,
            "a count code with no count byte is read as 20"
        );
        assert_eq!(opus_packet_ms(&[]), 20, "nothing to read");
    }

    #[test]
    fn dtx_silence_keeps_one_continuous_segment() {
        let (rec, start) = setup("dtx", u64::MAX);
        let mut s = voice(&rec);
        speak(&mut s, start, 100, 0, 50);
        // Five seconds of DTX: one packet every 400 ms.
        for i in 0..13u32 {
            s.packet(opus(
                start + Duration::from_millis(1000 + 400 * u64::from(i)),
                150 + i as u16,
                50 * 960 + i * 20 * 960,
                &[0xf8],
            ));
            s.tick(start + Duration::from_millis(1000 + 400 * u64::from(i)));
        }
        speak(&mut s, start + Duration::from_secs(6), 163, 6 * 48_000, 50);
        s.finish(start + Duration::from_secs(8), "test");

        let files = rec.files();
        assert_eq!(files.len(), 1, "{files:?}");
        assert_eq!(files[0].end_ms - files[0].start_ms, 7000);
        let times = block_times(&files[0].path);
        assert!(
            times.windows(2).all(|w| w[1] - w[0] == 20),
            "a hole in the track"
        );
    }

    #[test]
    fn an_idle_stream_is_finished_on_the_tick() {
        let (rec, start) = setup("idle", u64::MAX);
        let mut s = voice(&rec);
        speak(&mut s, start, 1, 0, 25);
        assert!(rec.files().is_empty(), "still open");
        s.tick(start + Duration::from_millis(500 + 1500));
        assert!(rec.files().is_empty(), "within the gap");
        s.tick(start + Duration::from_millis(500 + 2100));
        assert_eq!(
            rec.files().len(),
            1,
            "closed once the stream has been silent for the gap"
        );
    }

    #[test]
    fn reordered_audio_is_written_in_order() {
        let (rec, start) = setup("reorder", u64::MAX);
        let mut s = voice(&rec);
        let order = [0u16, 2, 1, 3, 5, 4, 6];
        for (n, i) in order.iter().enumerate() {
            s.packet(opus(
                start + Duration::from_millis(20 * n as u64),
                10 + i,
                u32::from(*i) * 960,
                &[0xfc, 1],
            ));
        }
        s.finish(start + Duration::from_secs(1), "test");
        let times = block_times(&rec.files()[0].path);
        assert_eq!(times, [0, 20, 40, 60, 80, 100, 120]);
    }

    /// A one-packet VP8 frame; keyframes carry their size (640x360 here).
    fn vp8_frame(arrived: Instant, seq: u16, ts: u32, key: bool) -> TapPacket {
        vp8_sized(arrived, seq, ts, key, 640, 360)
    }

    fn vp8_sized(arrived: Instant, seq: u16, ts: u32, key: bool, w: u16, h: u16) -> TapPacket {
        let frame: &[u8] = if key {
            &[
                0x10,
                0x00,
                0x00,
                0x00,
                0x9d,
                0x01,
                0x2a,
                w as u8,
                (w >> 8) as u8,
                h as u8,
                (h >> 8) as u8,
                0xaa,
            ]
        } else {
            &[0x10, 0x01, 0x00, 0x00, 0x00]
        };
        TapPacket {
            seq,
            ts,
            marker: true,
            payload: frame.to_vec().into(),
            arrived,
        }
    }

    #[test]
    fn a_camera_file_opens_on_a_keyframe() {
        let (rec, start) = setup("camera", u64::MAX);
        let mut s = camera(&rec);
        let mut asked = 0;
        for i in 0..40u32 {
            let key = i == 10 || i == 30;
            let now = start + Duration::from_millis(66 * u64::from(i));
            asked += usize::from(s.packet(vp8_frame(now, i as u16, i * 6000, key)));
        }
        s.finish(start + Duration::from_secs(4), "test");
        assert!(asked >= 1, "no keyframe was asked for while waiting");
        let files = rec.files();
        assert_eq!(files.len(), 1);
        let cam = &files[0];
        assert_eq!((cam.width, cam.height), (640, 360));
        assert_eq!(cam.start_ms, 660, "starts at the first keyframe");
        assert_eq!(codec(&cam.path), "V_VP8");
        let data = std::fs::read(&cam.path).unwrap();
        let top = elements(&data, 0, data.len());
        let seg = top.iter().find(|e| e.id == SEGMENT).unwrap();
        let clusters = find(&data, *seg, CLUSTER);
        assert_eq!(
            clusters.len(),
            2,
            "the second keyframe opens a seekable cluster"
        );
        let first_block = find(&data, clusters[0], SIMPLE_BLOCK)[0];
        assert_eq!(data[first_block.start + 3], 0x80, "opens on a keyframe");
    }

    #[test]
    fn a_lost_video_packet_freezes_the_picture_until_the_next_keyframe() {
        let (rec, start) = setup("loss", u64::MAX);
        let mut s = camera(&rec);
        let mut asked = Vec::new();
        for i in 0..60u32 {
            // Frame 20 never arrives. Keyframes at 0 and 40.
            if i == 20 {
                continue;
            }
            let now = start + Duration::from_millis(33 * u64::from(i));
            if s.packet(vp8_frame(now, i as u16, i * 3000, i % 40 == 0)) {
                asked.push(i);
            }
            s.tick(now);
        }
        s.finish(start + Duration::from_secs(5), "test");
        let files = rec.files();
        assert_eq!(
            files.len(),
            1,
            "{files:?}: the loss does not split the file"
        );
        let times = block_times(&files[0].path);
        // Frames 0..19 are written; 21..39 are not (they depend on the lost one); 40.. are.
        let ms = |frame: i64| frame * 3000 / 90;
        assert_eq!(times.iter().filter(|t| **t < ms(20)).count(), 20);
        assert!(
            times.iter().all(|t| *t < ms(20) || *t >= ms(40)),
            "{times:?}"
        );
        assert!(times.contains(&ms(40)));
        assert!(
            asked.iter().any(|i| (21..40).contains(i)),
            "the sender was asked for a keyframe after the loss: {asked:?}"
        );
    }

    #[test]
    fn reordered_video_packets_do_not_break_the_picture() {
        let (rec, start) = setup("vreorder", u64::MAX);
        let mut s = camera(&rec);
        // Frames 1 and 2 swap places.
        let seqs = [0u16, 2, 1, 3, 4];
        for (n, i) in seqs.iter().enumerate() {
            s.packet(vp8_frame(
                start + Duration::from_millis(10 * n as u64),
                *i,
                u32::from(*i) * 3000,
                *i == 0,
            ));
        }
        s.finish(start + Duration::from_secs(1), "test");
        let files = rec.files();
        assert_eq!(files.len(), 1);
        assert_eq!(block_times(&files[0].path), [0, 33, 66, 100, 133]);
    }

    #[test]
    fn a_voice_is_rotated_at_once_and_a_video_at_its_next_keyframe() {
        let (rec, start) = setup("rotate", u64::MAX);
        let every = Duration::from_millis(1500);
        let mut voice = voice(&rec).rotating(every);
        speak(&mut voice, start, 0, 0, 200); // 4 s
        voice.finish(start + Duration::from_secs(4), "test");
        let mut cam = camera(&rec).rotating(every);
        for i in 0..90u32 {
            // A keyframe every 2 s (60 frames of 33 ms), a frame every 33 ms.
            cam.packet(vp8_frame(
                start + Duration::from_millis(33 * u64::from(i)),
                i as u16,
                i * 2970,
                i % 60 == 0,
            ));
        }
        cam.finish(start + Duration::from_secs(3), "test");

        let files = rec.files();
        let of = |kind: &str| -> Vec<(i64, i64)> {
            files
                .iter()
                .filter(|f| f.kind == kind)
                .map(|f| (f.start_ms, f.end_ms))
                .collect()
        };
        assert_eq!(
            of(VOICE),
            [(0, 1500), (1500, 3000), (3000, 4000)],
            "a voice is cut at the first packet past the deadline"
        );
        let video = of(CAMERA);
        assert_eq!(video.len(), 2, "{video:?}");
        assert_eq!(
            video[1].0, 1980,
            "the deadline passed at 1.5 s; the next keyframe is frame 60 at 1.98 s"
        );
        assert_eq!(video[0].1, video[1].0, "the old picture is held to the new");
        for f in files.iter().filter(|f| f.kind == CAMERA) {
            let data = std::fs::read(&f.path).unwrap();
            let top = elements(&data, 0, data.len());
            let seg = top.iter().find(|e| e.id == SEGMENT).unwrap();
            let first = find(&data, find(&data, *seg, CLUSTER)[0], SIMPLE_BLOCK)[0];
            assert_eq!(data[first.start + 3], 0x80, "{f:?} opens on a keyframe");
        }
    }

    #[test]
    fn finished_files_can_be_taken_as_they_close() {
        let (rec, start) = setup("take", u64::MAX);
        let mut s = voice(&rec);
        speak(&mut s, start, 0, 0, 10);
        assert!(rec.take_files().is_empty(), "the file is still open");
        s.finish(start + Duration::from_secs(1), "test");
        assert_eq!(rec.take_files().len(), 1);
        assert!(rec.take_files().is_empty(), "each file is handed out once");
    }

    #[test]
    fn the_disk_limit_is_noticed() {
        let (rec, start) = setup("disk", 32 << 10);
        let mut s = voice(&rec);
        speak(&mut s, start, 1, 0, 25);
        s.tick(start + Duration::from_secs(10));
        assert!(
            !rec.disk.is_full(),
            "half a second of speech is under the limit"
        );
        speak(&mut s, start + Duration::from_secs(11), 100, 600_000, 6000);
        s.finish(start + Duration::from_secs(140), "test");
        assert!(
            rec.disk.is_full(),
            "two minutes of speech ({} bytes) passed the 32 KiB limit",
            rec.disk.written()
        );
        let on_disk: u64 = rec.files().iter().map(|f| f.size).sum();
        assert!(on_disk > 0 && rec.disk.written() >= on_disk);
    }

    #[test]
    fn track_ids_name_their_kind() {
        assert_eq!(track_kind("voice_sess_abc"), Some(VOICE));
        assert_eq!(track_kind("video_sess_abc"), Some(CAMERA));
        assert_eq!(track_kind("screen_sess_abc"), Some(SCREEN));
        assert_eq!(track_kind("screen-audio_sess_abc"), Some(SCREEN_AUDIO));
        assert_eq!(track_kind("whatever_sess"), None);
        assert_eq!(track_kind("nounderscore"), None);
    }

    #[test]
    fn the_manifest_entry_is_shaped_like_the_servers() {
        let e = Entry {
            kind: CAMERA.into(),
            session_id: "s".into(),
            user_id: String::new(),
            start_ms: 5,
            end_ms: 9,
            width: 0,
            height: 0,
            size: 7,
            path: PathBuf::from("/x"),
        };
        assert_eq!(
            serde_json::to_string(&e).unwrap(),
            r#"{"kind":"camera","session_id":"s","start_ms":5,"end_ms":9,"size":7}"#
        );
    }

    fn screen(rec: &Arc<Recording>) -> StreamRecorder {
        StreamRecorder::new(
            StreamInfo {
                kind: SCREEN,
                session_id: "s".into(),
                video: true,
            },
            rec.clone(),
            Users::default(),
        )
    }

    #[test]
    fn a_still_screen_stays_in_one_file_across_seconds_of_silence() {
        let (rec, start) = setup("still", u64::MAX);
        let mut s = screen(&rec);
        // A frame a second, then nothing for 6 s (more than the voice gap), then frames again.
        for i in 0..4u32 {
            let now = start + Duration::from_secs(u64::from(i));
            s.packet(vp8_frame(now, i as u16, i * 90_000, i == 0));
            s.tick(now);
        }
        for t in 4..10u64 {
            s.tick(start + Duration::from_secs(t));
        }
        for i in 10..13u32 {
            let now = start + Duration::from_secs(u64::from(i));
            s.packet(vp8_frame(now, i as u16 - 6, i * 90_000, false));
            s.tick(now);
        }
        // The share ends at 14 s: the last picture stays up until then.
        s.finish(start + Duration::from_secs(14), "the track was removed");
        let files = rec.files();
        assert_eq!(
            files.len(),
            1,
            "the silence must not blank the screen: {files:?}"
        );
        assert_eq!((files[0].start_ms, files[0].end_ms), (0, 14_000));
        assert_eq!(block_times(&files[0].path).len(), 7);
    }

    #[test]
    fn a_screen_that_never_says_it_ended_is_closed_after_the_hold() {
        let (rec, start) = setup("hold", u64::MAX);
        let mut s = screen(&rec);
        s.packet(vp8_frame(start, 0, 0, true));
        s.tick(start + VIDEO_HOLD - Duration::from_secs(1));
        assert!(rec.files().is_empty());
        s.tick(start + VIDEO_HOLD + Duration::from_secs(1));
        let files = rec.files();
        assert_eq!(files.len(), 1);
        assert!(
            files[0].end_ms < 1000,
            "ends with its last frame: {files:?}"
        );
    }

    #[test]
    fn a_new_picture_size_starts_a_new_file_without_a_hole() {
        let (rec, start) = setup("resize", u64::MAX);
        let mut s = screen(&rec);
        for i in 0..10u32 {
            let now = start + Duration::from_millis(100 * u64::from(i));
            s.packet(vp8_sized(now, i as u16, i * 9000, i == 0, 1791, 1120));
        }
        // 2.1 s with no frame (the SFU switching layers), then keyframes of the other size: the
        // first is left out until the size lasts, the second starts the file.
        let at = start + Duration::from_millis(3000);
        s.packet(vp8_sized(at, 10, 30 * 9000, true, 3584, 1952));
        let settled = at + SIZE_SETTLE;
        s.packet(vp8_sized(settled, 11, 40 * 9000, true, 3584, 1952));
        s.finish(start + Duration::from_secs(6), "test");
        let files = rec.files();
        assert_eq!(files.len(), 2, "{files:?}");
        assert_eq!((files[0].width, files[0].height), (1791, 1120));
        assert_eq!((files[1].width, files[1].height), (3584, 1952));
        assert_eq!(
            files[0].end_ms, files[1].start_ms,
            "the old picture is held until the new one: {files:?}"
        );
        assert_eq!(files[1].start_ms, 4000, "{files:?}");
    }

    #[test]
    fn a_picture_size_that_does_not_last_leaves_the_file_whole() {
        let (rec, start) = setup("stray", u64::MAX);
        let mut s = screen(&rec);
        let mut asked = false;
        for i in 0..40u32 {
            let now = start + Duration::from_millis(100 * u64::from(i));
            // Frames 10 and 11 are keyframes of other sizes; the sender is back at its size with
            // the keyframe at 12.
            let (key, w, h) = match i {
                0 | 12 => (true, 1920, 1080),
                10 => (true, 1664, 936),
                11 => (true, 1600, 900),
                _ => (false, 1920, 1080),
            };
            asked |= s.packet(vp8_sized(now, i as u16, i * 9000, key, w, h)) && i == 10;
            s.tick(now);
        }
        s.finish(start + Duration::from_secs(5), "test");
        let files = rec.files();
        assert_eq!(files.len(), 1, "{files:?}");
        assert_eq!((files[0].width, files[0].height), (1920, 1080));
        assert!(asked, "the sender was asked for a keyframe of its picture");
        let times = block_times(&files[0].path);
        assert!(
            !times.contains(&1000) && !times.contains(&1100),
            "the other sizes are not in the file: {times:?}"
        );
        assert!(
            times.contains(&1200) && times.contains(&3900),
            "it goes on from the keyframe of its size: {times:?}"
        );
    }

    #[test]
    fn frames_still_in_flight_after_a_camera_is_turned_off_open_no_file() {
        let (rec, start) = setup("tail", u64::MAX);
        let mut s = camera(&rec);
        for i in 0..10u32 {
            s.packet(vp8_frame(
                start + Duration::from_millis(33 * u64::from(i)),
                i as u16,
                i * 3000,
                i == 0,
            ));
        }
        // Turned off at 400 ms; a keyframe and more frames were already on their way.
        s.stop(
            start + Duration::from_millis(400),
            "the sender turned it off",
        );
        for i in 10..20u32 {
            s.packet(vp8_frame(
                start + Duration::from_millis(400 + 33 * u64::from(i - 10)),
                i as u16,
                i * 3000,
                i == 10,
            ));
        }
        s.tick(start + Duration::from_secs(20));
        s.finish(start + Duration::from_secs(20), "test");
        let files = rec.files();
        assert_eq!(files.len(), 1, "{files:?}");
        assert_eq!(
            files[0].end_ms, 400,
            "the picture is held until it was turned off"
        );
        // And it can be turned on again.
        for i in 20..30u32 {
            s.packet(vp8_frame(
                start + Duration::from_secs(5) + Duration::from_millis(33 * u64::from(i - 20)),
                i as u16,
                i * 3000,
                i == 20,
            ));
        }
        s.finish(start + Duration::from_secs(6), "test");
        assert_eq!(rec.files().len(), 2);
    }

    const GOLDEN: [&str; 2] = [
        "voice 1000-2040 0x0 757 66a7051f49b46422",
        "camera 0-3000 640x360 658 2ecb611bfd328cce",
    ];

    /// FNV-1a: a stable fingerprint of a file's bytes, with no dependency.
    fn fingerprint(bytes: &[u8]) -> u64 {
        bytes.iter().fold(0xcbf2_9ce4_8422_2325, |h, b| {
            (h ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3)
        })
    }

    /// The files a fixed feed leaves, as `kind start-end WxH length fingerprint`. The expected
    /// lines were recorded before the muxing was made to take any packet source, so a change in
    /// the bytes written (not only in their meaning) fails this test.
    #[test]
    fn a_fixed_feed_writes_the_same_files_byte_for_byte() {
        let (rec, start) = setup("golden", u64::MAX);
        let mut voice = voice(&rec);
        // 25 packets of 20 ms, a 40 ms packet, a lost packet with a 300 ms hole, then 10 more.
        speak(&mut voice, start + Duration::from_secs(1), 100, 0, 25);
        voice.packet(opus(
            start + Duration::from_millis(1500),
            125,
            25 * 960,
            &[0x10, 7, 7, 7],
        ));
        speak(
            &mut voice,
            start + Duration::from_millis(1840),
            127,
            27 * 960 + 15 * 960,
            10,
        );
        voice.finish(start + Duration::from_millis(2200), "golden");
        let mut camera = camera(&rec);
        for i in 0..40u32 {
            // Frame 15 never arrives: the picture is broken until the keyframe at 20.
            if i == 15 {
                continue;
            }
            camera.packet(vp8_frame(
                start + Duration::from_millis(66 * u64::from(i)),
                i as u16,
                i * 6000,
                i % 20 == 0,
            ));
            camera.tick(start + Duration::from_millis(66 * u64::from(i)));
        }
        camera.finish(start + Duration::from_secs(3), "golden");
        let got: Vec<String> = rec
            .files()
            .iter()
            .map(|e| {
                let bytes = std::fs::read(&e.path).unwrap();
                format!(
                    "{} {}-{} {}x{} {} {:016x}",
                    e.kind,
                    e.start_ms,
                    e.end_ms,
                    e.width,
                    e.height,
                    bytes.len(),
                    fingerprint(&bytes)
                )
            })
            .collect();
        assert_eq!(got, GOLDEN, "{got:#?}");
    }
}
