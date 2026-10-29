//! Writing the media the SFU hands over, on threads of our own.
//!
//! A recording's queue is drained with `try_recv` and a short sleep, never a blocking receive: a
//! receiver parked on the queue would make every packet the shard sends cost a lock and a wake-up
//! (see `rtcd_sfu::record::RecordReceiver`). The writer only appends to files, so a few threads
//! serve all recordings of the node; each recording belongs to one of them, the least loaded when
//! it started.
//!
//! The files come from the `recorder` crate, so a built-in recording writes exactly what the bot
//! writes for the same streams. This module feeds it from the queue, closes files as they rotate,
//! numbers them in the order they close and hands them to the uploader.

use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender, TryRecvError, channel};
use std::thread;
use std::time::{Duration, Instant};

use recorder::media::{CAMERA, Recording, SCREEN, SCREEN_AUDIO, StreamInfo, StreamRecorder, VOICE};
use recorder::tap::TapPacket;
use rtcd_sfu::ids::TrackType;
use rtcd_sfu::record::{RecordEnd, RecordItem, RecordPacket, RecordReceiver, StreamOpen};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, error, info, warn};

use super::pending::Pending;
use super::{Cmd, Ended, Registry, Run};
use crate::metrics::Metrics;

/// A file is closed and the next one begun after it has lasted this long, so that a lesson's files
/// can be uploaded while it runs.
pub const ROTATE_EVERY: Duration = Duration::from_secs(600);
/// What a writer thread sleeps when none of its recordings had anything.
const IDLE_SLEEP: Duration = Duration::from_millis(5);
/// How often the streams of a recording are given the time, when its queue is empty.
const TICK_EVERY: Duration = Duration::from_millis(100);
/// The most items taken from one recording before the others get their turn.
const BATCH: usize = 8192;

/// How a job behaves; the defaults are the service's, tests change them.
#[derive(Debug, Clone, Copy)]
pub struct Settings {
    pub rotate_every: Duration,
    pub tick_every: Duration,
    pub batch: usize,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            rotate_every: ROTATE_EVERY,
            tick_every: TICK_EVERY,
            batch: BATCH,
        }
    }
}

/// The manifest's name for what a track carries.
pub fn kind_of(t: TrackType) -> &'static str {
    match t {
        TrackType::Voice => VOICE,
        TrackType::Video => CAMERA,
        TrackType::Screen => SCREEN,
        TrackType::ScreenAudio => SCREEN_AUDIO,
    }
}

/// A stream of the recording being written.
struct Stream {
    rec: StreamRecorder,
    /// Read by the engine at its upkeep: the sender is asked for a keyframe.
    want_keyframe: Arc<AtomicBool>,
}

/// What a pass over a job found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pump {
    /// It took something off its queue: look again soon.
    Busy,
    Idle,
    /// The capture is over; nothing more will be written.
    Done,
}

/// One recording being captured: its queue, its streams, its files.
pub struct Job {
    run: Arc<Run>,
    rx: RecordReceiver,
    rec: Arc<Recording>,
    users: recorder::media::Users,
    streams: BTreeMap<u32, Stream>,
    /// The index the next file to close gets.
    next_index: u32,
    uploads: UnboundedSender<Cmd>,
    pending: Arc<Pending>,
    registry: Arc<Registry>,
    metrics: Arc<Metrics>,
    settings: Settings,
    /// Packets left out because uploads were behind, and how many of those the metrics know.
    writer_dropped: u64,
    writer_dropped_reported: u64,
    shedding: bool,
    next_tick: Instant,
    ended: bool,
}

impl std::fmt::Debug for Job {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Job")
            .field("recording", &self.run.recording_id)
            .field("streams", &self.streams.len())
            .field("next_index", &self.next_index)
            .finish_non_exhaustive()
    }
}

/// What the job shares with the rest of the service.
#[derive(Debug, Clone)]
pub struct Links {
    pub uploads: UnboundedSender<Cmd>,
    pub pending: Arc<Pending>,
    pub registry: Arc<Registry>,
    pub metrics: Arc<Metrics>,
}

impl Job {
    pub fn new(
        run: Arc<Run>,
        rx: RecordReceiver,
        max_bytes: u64,
        links: Links,
        settings: Settings,
    ) -> Self {
        let rec = Arc::new(Recording::new(&run.dir, run.t0, max_bytes));
        Self {
            next_index: run.file_base,
            run,
            rx,
            rec,
            users: recorder::media::Users::default(),
            streams: BTreeMap::new(),
            uploads: links.uploads,
            pending: links.pending,
            registry: links.registry,
            metrics: links.metrics,
            settings,
            writer_dropped: 0,
            writer_dropped_reported: 0,
            shedding: false,
            next_tick: Instant::now(),
            ended: false,
        }
    }

    /// Takes what the SFU has queued, gives the streams the time when the queue is empty, and
    /// hands finished files on. `clock` is now; while the job is behind, the time of the media is
    /// the time its packets arrived, since a late packet may be queued behind the moment that
    /// would give up on a gap.
    pub fn pump(&mut self, clock: Instant) -> Pump {
        if self.ended {
            return Pump::Done;
        }
        let mut took = 0;
        let mut empty = false;
        while took < self.settings.batch {
            match self.rx.try_recv() {
                Ok(item) => {
                    took += 1;
                    self.item(item);
                    if self.ended {
                        break;
                    }
                }
                Err(TryRecvError::Empty) => {
                    empty = true;
                    break;
                }
                // The SFU let go without an end: its shard is gone.
                Err(TryRecvError::Disconnected) => {
                    self.end("failed", clock);
                    break;
                }
            }
        }
        if !self.ended && empty && clock >= self.next_tick {
            self.next_tick = clock + self.settings.tick_every;
            for s in self.streams.values_mut() {
                if s.rec.tick(clock) {
                    s.want_keyframe.store(true, Ordering::Relaxed);
                }
            }
        }
        if !self.ended && self.rec.disk.is_full() {
            warn!(recording = %self.run.recording_id, bytes = self.rec.disk.written(), "the recording reached its size limit; ending it");
            // The shard lets go of the call; what is queued is left.
            self.rx.shared().cancel();
            self.end("disk_full", clock);
        }
        self.collect_files();
        self.report_dropped();
        if self.ended {
            Pump::Done
        } else if took > 0 {
            Pump::Busy
        } else {
            Pump::Idle
        }
    }

    fn item(&mut self, item: RecordItem) {
        match item {
            RecordItem::Begin(_) => {}
            RecordItem::Open(o) => self.open(*o),
            RecordItem::Packet(p) => self.packet(p),
            RecordItem::Close { stream, at } => {
                if let Some(mut s) = self.streams.remove(&stream) {
                    s.rec.finish(at, "the track was removed");
                }
            }
            RecordItem::Off { stream, at } => {
                if let Some(s) = self.streams.get_mut(&stream) {
                    s.rec.stop(at, "the sender turned it off");
                }
            }
            RecordItem::End { reason, at } => {
                let reason = match reason {
                    RecordEnd::Stopped => "stopped",
                    RecordEnd::CallEnded => "call_ended",
                    RecordEnd::Refused => "refused",
                    // A second request for a recording that goes on: not ours to report.
                    RecordEnd::AlreadyRecording => "",
                };
                self.end(reason, at);
            }
        }
    }

    fn open(&mut self, o: StreamOpen) {
        if let Some(mut old) = self.streams.remove(&o.stream) {
            old.rec
                .finish(Instant::now(), "the stream was opened again");
        }
        self.users
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(o.session_id.clone(), o.user_id.clone());
        info!(recording = %self.run.recording_id, kind = kind_of(o.kind), session_id = %o.session_id, "recording a stream");
        let rec = StreamRecorder::new(
            StreamInfo {
                kind: kind_of(o.kind),
                session_id: o.session_id,
                video: o.kind.is_video(),
            },
            self.rec.clone(),
            self.users.clone(),
        )
        .rotating(self.settings.rotate_every);
        self.streams.insert(
            o.stream,
            Stream {
                rec,
                want_keyframe: o.want_keyframe,
            },
        );
    }

    fn packet(&mut self, p: RecordPacket) {
        let Some(s) = self.streams.get_mut(&p.stream) else {
            return;
        };
        if self.pending.over() {
            // Uploads are far behind: leave the packet out, which is a gap in the recording, rather
            // than fill the disk or end the recording.
            self.writer_dropped += 1;
            if !self.shedding {
                self.shedding = true;
                warn!(recording = %self.run.recording_id, pending = self.pending.bytes(), "uploads are behind; leaving packets out until they catch up");
            }
            return;
        }
        self.shedding = false;
        let ask = s.rec.packet(TapPacket {
            seq: p.seq,
            ts: p.ts,
            marker: p.marker,
            payload: p.payload,
            arrived: p.arrived,
        });
        if ask {
            s.want_keyframe.store(true, Ordering::Relaxed);
        }
    }

    /// Closes every stream and says the capture is over, once.
    fn end(&mut self, reason: &'static str, at: Instant) {
        if self.ended {
            return;
        }
        self.ended = true;
        for s in self.streams.values_mut() {
            s.rec.finish(at, "the recording ended");
        }
        self.streams.clear();
        self.collect_files();
        self.report_dropped();
        let dropped = self.rx.shared().dropped() + self.writer_dropped;
        let ended = Ended {
            reason,
            files: self.next_index - self.run.file_base,
            duration_ms: self.rec.at(at),
            dropped,
        };
        info!(recording = %self.run.recording_id, reason, files = ended.files, duration_ms = ended.duration_ms, dropped, "capture ended");
        self.registry.remove(&self.run.recording_id, self.run.id);
        self.metrics.record_capture(-1);
        let _ = self.uploads.send(Cmd::Ended {
            run: self.run.clone(),
            ended,
        });
    }

    /// Ends a job whose code failed, without touching its streams again.
    fn abandon(&mut self, clock: Instant) {
        if !self.ended {
            error!(recording = %self.run.recording_id, "the writer failed; giving the recording up");
            self.streams.clear();
            self.end("failed", clock);
        }
    }

    /// Numbers the files that closed, in the order they closed, and hands them to the uploader.
    fn collect_files(&mut self) {
        for entry in self.rec.take_files() {
            let index = self.next_index;
            self.next_index += 1;
            self.pending.add(entry.size);
            self.metrics.record_file_written();
            let _ = self.uploads.send(Cmd::File {
                run: self.run.clone(),
                index,
                entry,
            });
        }
    }

    fn report_dropped(&mut self) {
        if self.writer_dropped > self.writer_dropped_reported {
            self.metrics
                .record_writer_dropped(self.writer_dropped - self.writer_dropped_reported);
            self.writer_dropped_reported = self.writer_dropped;
        }
    }
}

/// A thread that serves some recordings.
#[derive(Debug)]
struct Worker {
    jobs: Sender<Job>,
    load: Arc<AtomicUsize>,
}

/// The threads that write recordings.
#[derive(Debug)]
pub struct WriterPool {
    workers: Vec<Worker>,
}

impl WriterPool {
    pub fn start(threads: usize, metrics: &Arc<Metrics>) -> Self {
        let workers = (0..threads.max(1))
            .map(|n| {
                let (tx, rx) = channel();
                let load = Arc::new(AtomicUsize::new(0));
                let (l, m) = (load.clone(), metrics.clone());
                thread::Builder::new()
                    .name(format!("rtcd-record-{n}"))
                    .spawn(move || serve(rx, l, m))
                    .expect("a thread to write recordings");
                Worker { jobs: tx, load }
            })
            .collect();
        Self { workers }
    }

    /// Gives a recording to the thread that has the fewest.
    pub fn add(&self, job: Job) {
        let worker = self
            .workers
            .iter()
            .min_by_key(|w| w.load.load(Ordering::Relaxed))
            .expect("at least one writer");
        worker.load.fetch_add(1, Ordering::Relaxed);
        if worker.jobs.send(job).is_err() {
            worker.load.fetch_sub(1, Ordering::Relaxed);
            error!("a recording writer is gone");
        }
    }
}

/// A writer thread: pumps its jobs until the pool is dropped and none is left.
fn serve(rx: Receiver<Job>, load: Arc<AtomicUsize>, metrics: Arc<Metrics>) {
    let mut jobs: Vec<Job> = Vec::new();
    let mut closed = false;
    loop {
        loop {
            match rx.try_recv() {
                Ok(job) => jobs.push(job),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    closed = true;
                    break;
                }
            }
        }
        if closed && jobs.is_empty() {
            return;
        }
        let clock = Instant::now();
        let mut busy = false;
        jobs.retain_mut(|job| {
            let pumped = catch_unwind(AssertUnwindSafe(|| job.pump(clock)));
            let state = match pumped {
                Ok(state) => state,
                Err(_) => {
                    job.abandon(clock);
                    Pump::Done
                }
            };
            busy |= state == Pump::Busy;
            if state == Pump::Done {
                load.fetch_sub(1, Ordering::Relaxed);
                debug!(recording = %job.run.recording_id, "writer let go of a recording");
            }
            state != Pump::Done
        });
        metrics.record_writer_busy(clock.elapsed());
        if !busy {
            thread::sleep(IDLE_SLEEP);
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use recorder::api::{Api, HttpClients};
    use recorder::media::{Entry, Users, track_kind};
    use rtcd_sfu::record::{RecordBegin, RecordSink, record_channel};
    use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

    use super::*;

    struct Rig {
        job: Job,
        sink: RecordSink,
        cmds: UnboundedReceiver<Cmd>,
        pending: Arc<Pending>,
        registry: Arc<Registry>,
        metrics: Arc<Metrics>,
        run: Arc<Run>,
        t0: Instant,
    }

    fn rig(tag: &str, max_bytes: u64, pending_cap: u64, settings: Settings) -> Rig {
        let dir = std::env::temp_dir().join(format!("rtcd-writer-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let t0 = Instant::now();
        let run = Arc::new(Run {
            id: 1,
            recording_id: "rec1".into(),
            client_id: "mm".into(),
            call_id: "call".into(),
            file_base: 100,
            dir,
            api: Api::with_clients(&HttpClients::new(), "http://127.0.0.1:1", "rec1", "tok"),
            t0,
        });
        let metrics = Arc::new(Metrics::new());
        let pending = Arc::new(Pending::new(pending_cap, metrics.clone()));
        let registry = Arc::new(Registry::default());
        registry.begin("rec1", "mm", "call", 1).unwrap();
        let (uploads, cmds) = unbounded_channel();
        let (sink, rx) = record_channel(65536);
        let job = Job::new(
            run.clone(),
            rx,
            max_bytes,
            Links {
                uploads,
                pending: pending.clone(),
                registry: registry.clone(),
                metrics: metrics.clone(),
            },
            settings,
        );
        Rig {
            job,
            sink,
            cmds,
            pending,
            registry,
            metrics,
            run,
            t0,
        }
    }

    fn eager() -> Settings {
        Settings {
            tick_every: Duration::ZERO,
            ..Settings::default()
        }
    }

    impl Rig {
        fn send(&self, item: RecordItem) {
            assert!(self.sink.try_send(item).is_ok(), "the queue has room");
        }

        fn open(&self, stream: u32, kind: TrackType, session: &str, user: &str) -> Arc<AtomicBool> {
            let want = Arc::new(AtomicBool::new(false));
            self.send(RecordItem::Open(Box::new(StreamOpen {
                stream,
                kind,
                session_id: session.into(),
                user_id: user.into(),
                want_keyframe: want.clone(),
            })));
            want
        }

        fn packet(&self, stream: u32, p: TapPacket) {
            self.send(RecordItem::Packet(RecordPacket {
                stream,
                seq: p.seq,
                ts: p.ts,
                marker: p.marker,
                payload: p.payload,
                arrived: p.arrived,
            }));
        }

        fn end(&self, reason: RecordEnd, at: Instant) {
            self.send(RecordItem::End { reason, at });
        }

        /// Everything the uploader has been told so far.
        fn told(&mut self) -> (Vec<(u32, Entry)>, Option<Ended>) {
            let (mut files, mut ended) = (Vec::new(), None);
            while let Ok(cmd) = self.cmds.try_recv() {
                match cmd {
                    Cmd::File { index, entry, .. } => files.push((index, entry)),
                    Cmd::Ended { ended: e, .. } => ended = Some(e),
                    Cmd::Uploaded { .. } => {}
                }
            }
            (files, ended)
        }
    }

    fn opus(arrived: Instant, seq: u16, ts: u32, payload: &[u8]) -> TapPacket {
        TapPacket {
            seq,
            ts,
            marker: false,
            payload: Bytes::copy_from_slice(payload),
            arrived,
        }
    }

    fn vp8_frame(arrived: Instant, seq: u16, ts: u32, key: bool) -> TapPacket {
        let frame: &[u8] = if key {
            &[
                0x10, 0, 0, 0, 0x9d, 0x01, 0x2a, 0x80, 0x02, 0x68, 0x01, 0xaa,
            ]
        } else {
            &[0x10, 0x01, 0x00, 0x00, 0x00]
        };
        TapPacket {
            seq,
            ts,
            marker: true,
            payload: Bytes::copy_from_slice(frame),
            arrived,
        }
    }

    /// FNV-1a: the same fingerprint the recorder's own golden test uses.
    fn fingerprint(bytes: &[u8]) -> u64 {
        bytes.iter().fold(0xcbf2_9ce4_8422_2325, |h, b| {
            (h ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3)
        })
    }

    fn describe(e: &Entry) -> String {
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
    }

    /// The feed of the recorder's own golden test, as (stream, packet) in the order it happens,
    /// with the moments the streams end. Stream 0 is a voice, stream 1 a camera.
    fn golden_feed(start: Instant) -> (Vec<(u32, TapPacket)>, [Instant; 2]) {
        let ms = |n: u64| start + Duration::from_millis(n);
        let mut feed = Vec::new();
        let mut speak = |at: Instant, seq: u16, ts: u32, count: usize| {
            for i in 0..count {
                feed.push((
                    0,
                    opus(
                        at + Duration::from_millis(20 * i as u64),
                        seq + i as u16,
                        ts + 960 * i as u32,
                        &[0xfc, 1, 2],
                    ),
                ));
            }
        };
        speak(ms(1000), 100, 0, 25);
        feed.push((0, opus(ms(1500), 125, 25 * 960, &[0x10, 7, 7, 7])));
        let mut speak = |at: Instant, seq: u16, ts: u32, count: usize| {
            for i in 0..count {
                feed.push((
                    0,
                    opus(
                        at + Duration::from_millis(20 * i as u64),
                        seq + i as u16,
                        ts + 960 * i as u32,
                        &[0xfc, 1, 2],
                    ),
                ));
            }
        };
        speak(ms(1840), 127, 27 * 960 + 15 * 960, 10);
        for i in 0..40u32 {
            if i == 15 {
                continue;
            }
            feed.push((
                1,
                vp8_frame(ms(66 * u64::from(i)), i as u16, i * 6000, i % 20 == 0),
            ));
        }
        (feed, [ms(2200), ms(3000)])
    }

    const GOLDEN: [&str; 2] = [
        "voice 1000-2040 0x0 757 66a7051f49b46422",
        "camera 0-3000 640x360 658 2ecb611bfd328cce",
    ];

    #[test]
    fn a_fixed_feed_through_the_queue_writes_the_files_it_writes_when_fed_directly() {
        let mut rig = rig("golden", u64::MAX, u64::MAX, eager());
        let t0 = rig.t0;
        let (feed, [voice_ends, camera_ends]) = golden_feed(t0);

        // Directly into the recorder's own types, as the bot does.
        let dir = rig.run.dir.join("direct");
        std::fs::create_dir_all(&dir).unwrap();
        let direct_rec = Arc::new(Recording::new(&dir, t0, u64::MAX));
        let users = Users::default();
        users.lock().unwrap().insert("sess".into(), "user".into());
        let make = |kind, session: &str, video| {
            StreamRecorder::new(
                StreamInfo {
                    kind,
                    session_id: session.into(),
                    video,
                },
                direct_rec.clone(),
                users.clone(),
            )
            .rotating(ROTATE_EVERY)
        };
        let mut voice = make(VOICE, "sess", false);
        let mut camera = make(CAMERA, "s", true);
        for (stream, p) in &feed {
            if *stream == 0 {
                voice.packet(p.clone());
            } else {
                camera.packet(p.clone());
                camera.tick(p.arrived);
            }
        }
        voice.finish(voice_ends, "direct");
        camera.finish(camera_ends, "direct");
        let direct = direct_rec.files();

        // The same through the queue.
        rig.send(RecordItem::Begin(Box::new(RecordBegin {
            recording_id: "rec1".into(),
            t0,
        })));
        rig.open(0, TrackType::Voice, "sess", "user");
        rig.open(1, TrackType::Video, "s", "");
        let mut voice_closed = false;
        for (stream, p) in feed {
            if stream == 1 && !voice_closed {
                voice_closed = true;
                rig.send(RecordItem::Close {
                    stream: 0,
                    at: voice_ends,
                });
            }
            let arrived = p.arrived;
            rig.packet(stream, p);
            rig.job.pump(arrived);
        }
        rig.send(RecordItem::Close {
            stream: 1,
            at: camera_ends,
        });
        rig.end(RecordEnd::Stopped, camera_ends);
        assert_eq!(rig.job.pump(camera_ends), Pump::Done);

        let (files, ended) = rig.told();
        let queued: Vec<String> = files.iter().map(|(_, e)| describe(e)).collect();
        assert_eq!(queued, GOLDEN, "{queued:#?}");
        let direct_lines: Vec<String> = direct.iter().map(describe).collect();
        assert_eq!(queued, direct_lines);
        for ((_, q), d) in files.iter().zip(&direct) {
            assert_eq!(
                (&q.kind, &q.session_id, &q.user_id, q.start_ms, q.end_ms),
                (&d.kind, &d.session_id, &d.user_id, d.start_ms, d.end_ms)
            );
            assert_eq!((q.width, q.height, q.size), (d.width, d.height, d.size));
            assert_eq!(
                std::fs::read(&q.path).unwrap(),
                std::fs::read(&d.path).unwrap()
            );
        }
        assert_eq!(
            files.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
            [100, 101],
            "numbered from the file base, in the order they closed"
        );
        assert_eq!(files[0].1.user_id, "user");
        assert_eq!(
            ended,
            Some(Ended {
                reason: "stopped",
                files: 2,
                duration_ms: 3000,
                dropped: 0
            })
        );
        assert!(rig.registry.is_empty(), "the capture is over");
        assert_eq!(
            rig.pending.bytes(),
            files.iter().map(|(_, e)| e.size).sum::<u64>(),
            "closed files wait for upload"
        );
    }

    #[test]
    fn files_rotate_and_are_numbered_in_the_order_they_close() {
        let every = Duration::from_millis(1500);
        let mut rig = rig(
            "rotate",
            u64::MAX,
            u64::MAX,
            Settings {
                rotate_every: every,
                ..eager()
            },
        );
        let t0 = rig.t0;
        rig.open(0, TrackType::Voice, "a", "ua");
        rig.open(1, TrackType::Video, "a", "ua");
        for i in 0..200u32 {
            rig.packet(
                0,
                opus(
                    t0 + Duration::from_millis(20 * u64::from(i)),
                    i as u16,
                    i * 960,
                    &[0xfc, 1, 2],
                ),
            );
        }
        for i in 0..90u32 {
            rig.packet(
                1,
                vp8_frame(
                    t0 + Duration::from_millis(33 * u64::from(i)),
                    i as u16,
                    i * 2970,
                    i % 60 == 0,
                ),
            );
        }
        rig.job.pump(t0 + Duration::from_secs(4));
        let (during, ended) = rig.told();
        assert_eq!(ended, None);
        // Voice is cut at the first packet past the deadline, video at the next keyframe; only the
        // closed ones have been handed over.
        let kinds: Vec<_> = during
            .iter()
            .map(|(_, e)| (e.kind.as_str(), e.start_ms, e.end_ms))
            .collect();
        assert_eq!(
            kinds,
            [
                ("voice", 0, 1500),
                ("voice", 1500, 3000),
                ("camera", 0, 1980)
            ],
            "{kinds:?}"
        );
        assert_eq!(
            during.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
            [100, 101, 102]
        );

        rig.end(RecordEnd::CallEnded, t0 + Duration::from_secs(4));
        assert_eq!(rig.job.pump(t0 + Duration::from_secs(4)), Pump::Done);
        let (rest, ended) = rig.told();
        let tail: Vec<_> = rest
            .iter()
            .map(|(i, e)| (*i, e.kind.as_str(), e.start_ms))
            .collect();
        assert_eq!(
            tail,
            [(103, "voice", 3000), (104, "camera", 1980)],
            "{tail:?}"
        );
        assert_eq!(
            ended.unwrap().files,
            5,
            "the indexes used, from the file base"
        );
    }

    #[test]
    fn a_recording_that_reaches_its_size_limit_ends_with_what_it_has() {
        let mut rig = rig("size", 32 << 10, u64::MAX, eager());
        let t0 = rig.t0;
        rig.open(0, TrackType::Voice, "a", "ua");
        let big = vec![0xfc; 1000];
        for i in 0..200u32 {
            rig.packet(
                0,
                opus(
                    t0 + Duration::from_millis(20 * u64::from(i)),
                    i as u16,
                    i * 960,
                    &big,
                ),
            );
        }
        assert_eq!(rig.job.pump(t0 + Duration::from_secs(5)), Pump::Done);
        assert!(
            rig.sink.shared().is_cancelled(),
            "the SFU is told to let go of the call"
        );
        let (files, ended) = rig.told();
        assert_eq!(files.len(), 1, "the open file was closed");
        let ended = ended.unwrap();
        assert_eq!((ended.reason, ended.files), ("disk_full", 1));
        assert!(rig.registry.is_empty());
    }

    #[test]
    fn packets_are_left_out_while_uploads_are_behind_and_recording_resumes_after() {
        let mut rig = rig("pending", u64::MAX, 1000, eager());
        let t0 = rig.t0;
        rig.open(0, TrackType::Voice, "a", "ua");
        let feed = |rig: &Rig, from: u32, to: u32| {
            for i in from..to {
                rig.packet(
                    0,
                    opus(
                        t0 + Duration::from_millis(20 * u64::from(i)),
                        i as u16,
                        i * 960,
                        &[0xfc, 1, 2],
                    ),
                );
            }
        };
        feed(&rig, 0, 50);
        rig.job.pump(t0 + Duration::from_secs(1));
        assert_eq!(rig.job.writer_dropped, 0);

        // Closed files that are not uploaded fill the allowance.
        rig.pending.add(1000);
        feed(&rig, 50, 100);
        rig.job.pump(t0 + Duration::from_secs(2));
        assert_eq!(rig.job.writer_dropped, 50, "left out, counted");
        assert!(
            rig.metrics
                .render()
                .contains("rtcd_record_writer_dropped_packets_total 50")
        );

        rig.pending.release(1000);
        feed(&rig, 100, 150);
        rig.job.pump(t0 + Duration::from_secs(3));
        rig.end(RecordEnd::Stopped, t0 + Duration::from_secs(3));
        assert_eq!(rig.job.pump(t0 + Duration::from_secs(3)), Pump::Done);
        let (files, ended) = rig.told();
        assert_eq!(ended.unwrap().dropped, 50, "a gap, not an ended recording");
        // The hole of 50 packets (a second) is bridged with silence or split; either way the
        // recording kept both sides.
        let span = files.last().map(|(_, e)| e.end_ms).unwrap();
        assert!(span >= 2900, "{span}");
    }

    #[test]
    fn a_queue_the_sfu_dropped_without_an_end_is_a_failed_recording() {
        let rig = rig("failed", u64::MAX, u64::MAX, eager());
        rig.open(0, TrackType::Voice, "a", "ua");
        let t0 = rig.t0;
        rig.packet(0, opus(t0, 1, 0, &[0xfc, 1]));
        // The rig's sink is the only sender: dropping it is the shard letting go.
        let Rig {
            mut job,
            sink,
            mut cmds,
            ..
        } = rig;
        drop(sink);
        assert_eq!(job.pump(t0 + Duration::from_secs(1)), Pump::Done);
        let mut ended = None;
        while let Ok(cmd) = cmds.try_recv() {
            if let Cmd::Ended { ended: e, .. } = cmd {
                ended = Some(e);
            }
        }
        assert_eq!(ended.unwrap().reason, "failed");
    }

    #[test]
    fn a_refused_recording_says_so_and_has_no_files() {
        let mut rig = rig("refused", u64::MAX, u64::MAX, eager());
        let now = rig.t0;
        rig.send(RecordItem::Begin(Box::new(RecordBegin {
            recording_id: "rec1".into(),
            t0: now,
        })));
        rig.end(RecordEnd::Refused, now);
        assert_eq!(rig.job.pump(now), Pump::Done);
        let (files, ended) = rig.told();
        assert!(files.is_empty());
        let ended = ended.unwrap();
        assert_eq!((ended.reason, ended.files), ("refused", 0));
    }

    #[test]
    fn a_camera_waiting_for_its_first_keyframe_asks_the_engine_for_one() {
        let mut rig = rig("keyframe", u64::MAX, u64::MAX, eager());
        let t0 = rig.t0;
        let want = rig.open(0, TrackType::Video, "a", "ua");
        rig.packet(0, vp8_frame(t0, 1, 0, false));
        assert!(!want.load(Ordering::Relaxed));
        rig.job.pump(t0);
        assert!(
            want.load(Ordering::Relaxed),
            "a delta frame cannot open a file"
        );
        let voice_wants = rig.open(1, TrackType::Voice, "a", "ua");
        rig.packet(1, opus(t0, 1, 0, &[0xfc, 1]));
        rig.job.pump(t0 + Duration::from_millis(50));
        assert!(
            !voice_wants.load(Ordering::Relaxed),
            "voice has no keyframes"
        );
    }

    #[test]
    fn track_types_map_to_the_kinds_the_bot_writes() {
        for t in TrackType::ALL {
            assert_eq!(
                Some(kind_of(t)),
                track_kind(&format!("{}_session_random", t.as_str())),
                "{t}"
            );
        }
    }

    #[test]
    fn a_writer_thread_serves_a_recording_to_its_end_and_goes_when_the_pool_does() {
        let rig = rig("thread", u64::MAX, u64::MAX, Settings::default());
        let t0 = rig.t0;
        rig.open(0, TrackType::Voice, "a", "ua");
        for i in 0..100u32 {
            rig.packet(
                0,
                opus(
                    t0 + Duration::from_millis(20 * u64::from(i)),
                    i as u16,
                    i * 960,
                    &[0xfc, 1, 2],
                ),
            );
        }
        rig.end(RecordEnd::Stopped, t0 + Duration::from_secs(2));
        let Rig { job, mut cmds, .. } = rig;
        let pool = WriterPool::start(2, &Arc::new(Metrics::new()));
        pool.add(job);
        let deadline = Instant::now() + Duration::from_secs(10);
        let (mut files, mut ended) = (0, None);
        while ended.is_none() && Instant::now() < deadline {
            while let Ok(cmd) = cmds.try_recv() {
                match cmd {
                    Cmd::File { .. } => files += 1,
                    Cmd::Ended { ended: e, .. } => ended = Some(e),
                    Cmd::Uploaded { .. } => {}
                }
            }
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(files, 1);
        assert_eq!(ended.unwrap().reason, "stopped");
        assert_eq!(
            pool.workers
                .iter()
                .map(|w| w.load.load(Ordering::Relaxed))
                .sum::<usize>(),
            0,
            "the thread let go of the recording"
        );
    }
}
