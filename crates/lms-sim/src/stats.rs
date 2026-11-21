//! What a receiving participant measures, and the JSON lines a soak run writes (one object per
//! line, each with `ts` in unix milliseconds, `impl`, `arch` (`x86_64` or `aarch64`), `kind` and `id`; the report tool of
//! `vendor/rtc/benchmarks/soak` reads them).
//!
//! The figures follow the browser's own definitions, so a synthetic receiver and a Chrome receiver
//! can be read side by side:
//!
//! * a **freeze** is a frame that came later than `max(3 x avg, avg + 150 ms)` after the one before,
//!   `avg` being the mean interval of the last 30 frames (the WebRTC statistics definition);
//! * the **audio gap** is the time, summed, that no audio packet arrived for more than 60 ms;
//! * **stalled** is no media for more than 2 s;
//! * **lost** is the packets the sequence numbers say were sent and never came, as the RTP receiver
//!   report counts them (a late packet that comes after all takes one off; a number that comes
//!   twice counts once, and the second time is a **duplicate**, so a loss is not hidden by a copy);
//! * **nack** is the packets asked for again: every sequence number the receiver saw skipped;
//! * the **delay** is the one-way time from a sender to a receiver in the same process: the sender
//!   says where its RTP clock stood on the wall clock ([`RtpClock`]), the receiver maps the
//!   timestamp of every audio packet and of each video frame's marker packet to the wall clock
//!   and subtracts it from the arrival. The processes of one run share the clocks through a file
//!   next to the output (`rtp.clocks`: the same host, so the same wall clock); a receiver with
//!   none to map to (a group on another machine) writes null.

use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value, json};

/// How far back a repeated sequence number is told from a new one.
const SEEN_BITS: i64 = 2048;
/// How many frames the freeze threshold averages over.
const FREEZE_WINDOW: usize = 30;
/// A freeze needs this many intervals to average over first.
const FREEZE_MIN_FRAMES: usize = 5;
/// No audio packet for this long is a gap.
const AUDIO_GAP: Duration = Duration::from_millis(60);
/// No media for this long is a stall.
pub const STALL: Duration = Duration::from_secs(2);
/// Seconds between the lines a receiver writes.
pub const INTERVAL: Duration = Duration::from_secs(5);

/// The file the JSON lines go to.
#[derive(Debug)]
pub struct Sink {
    out: Mutex<File>,
    /// Where each sender's RTP clocks stand, from this process and from the file the processes of
    /// the run share.
    clocks: Mutex<Clocks>,
    clock_file: PathBuf,
}

/// The RTP clocks senders have told of, by `<kind>:<sender>` (`voice`, `video`, `screen`), one
/// per layer, and how much of the shared file has been read.
#[derive(Debug, Default)]
struct Clocks {
    by_key: HashMap<String, Vec<Option<RtpClock>>>,
    read: u64,
    checked: Option<Instant>,
}

impl Clocks {
    fn set(&mut self, key: &str, layer: usize, clock: RtpClock) {
        let layers = self.by_key.entry(key.to_owned()).or_default();
        if layers.len() <= layer {
            layers.resize(layer + 1, None);
        }
        layers[layer] = Some(clock);
    }
}

/// How often the shared file of clocks is read again.
const CLOCK_REREAD: Duration = Duration::from_secs(1);

impl Sink {
    /// Opens `path` for appending.
    pub fn open(path: &Path) -> std::io::Result<Arc<Self>> {
        let out = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Arc::new(Self {
            out: Mutex::new(out),
            clocks: Mutex::default(),
            clock_file: path.with_file_name("rtp.clocks"),
        }))
    }

    /// One line: the common fields, then `fields` (an object).
    pub fn write(&self, imp: &str, kind: &str, id: &str, fields: Value) {
        let mut line = Map::new();
        line.insert("ts".into(), unix_ms().into());
        line.insert("impl".into(), imp.into());
        line.insert("arch".into(), std::env::consts::ARCH.into());
        line.insert("kind".into(), kind.into());
        line.insert("id".into(), id.into());
        if let Value::Object(f) = fields {
            line.extend(f);
        }
        let mut text = Value::Object(line).to_string();
        text.push('\n');
        if let Ok(mut f) = self.out.lock() {
            let _ = f.write_all(text.as_bytes());
        }
    }
}

pub fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// The wall clock, in unix milliseconds, at the instant `at` (earlier or later than now).
pub fn wall_ms_at(at: Instant) -> f64 {
    let now = Instant::now();
    let wall = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64() * 1000.0);
    if at <= now {
        wall - (now - at).as_secs_f64() * 1000.0
    } else {
        wall + (at - now).as_secs_f64() * 1000.0
    }
}

/// Where a sender's RTP clock stood on the wall clock: the packet stamped `ts` was due out at
/// `wall_ms`, and the timestamps go on at `rate` ticks a second (one timeline per source, the
/// same on every simulcast layer).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RtpClock {
    pub ts: u32,
    pub wall_ms: f64,
    pub rate: f64,
}

impl RtpClock {
    /// The one-way delay of a packet stamped `ts` that arrived at `arrival_ms` (unix milliseconds).
    pub fn delay_ms(&self, ts: u32, arrival_ms: f64) -> f64 {
        let ticks = f64::from(ts.wrapping_sub(self.ts) as i32);
        arrival_ms - (self.wall_ms + ticks / self.rate * 1000.0)
    }
}

/// The delay by whichever of a source's layer clocks the timestamp fits best (the smallest delay in
/// size: a packet does not say which layer it is of, and the layers of one camera are on one
/// timeline when they start together, as lms-web's do).
pub fn delay_among(clocks: &[RtpClock], ts: u32, arrival_ms: f64) -> Option<f64> {
    clocks
        .iter()
        .map(|c| c.delay_ms(ts, arrival_ms))
        .min_by(|a, b| a.abs().total_cmp(&b.abs()))
}

/// The median and the 95th percentile of `delays` (milliseconds, rounded to a tenth), which this
/// sorts; `None`s when there are none.
pub fn delay_summary(delays: &mut [f64]) -> (Option<f64>, Option<f64>) {
    let round = |v: f64| (v * 10.0).round() / 10.0;
    (
        percentile(delays, 0.5).map(round),
        percentile(delays, 0.95).map(round),
    )
}

/// The `p`-th percentile (0 to 1, nearest rank) of `values`, which this sorts.
fn percentile(values: &mut [f64], p: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(f64::total_cmp);
    let rank = ((values.len() as f64 * p).ceil() as usize).clamp(1, values.len());
    Some(values[rank - 1])
}

impl Sink {
    /// Every layer's clock of `key` (the sender's own process, or another's through the file).
    fn clocks_of(&self, key: &str) -> Vec<RtpClock> {
        let Ok(mut c) = self.clocks.lock() else {
            return Vec::new();
        };
        if c.checked.is_none_or(|at| at.elapsed() >= CLOCK_REREAD) {
            c.checked = Some(Instant::now());
            self.read_clock_file(&mut c);
        }
        c.by_key
            .get(key)
            .map(|l| l.iter().flatten().copied().collect())
            .unwrap_or_default()
    }

    /// Takes in the lines other processes have added to the shared file since the last read.
    fn read_clock_file(&self, c: &mut Clocks) {
        let Ok(mut file) = File::open(&self.clock_file) else {
            return;
        };
        let mut text = String::new();
        if file.seek(SeekFrom::Start(c.read)).is_err() || file.read_to_string(&mut text).is_err() {
            return;
        }
        // Only whole lines: a writer may be in the middle of one.
        let whole = text.rfind('\n').map_or(0, |i| i + 1);
        for line in text[..whole].lines() {
            let Ok(v) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            if let (Some(key), Some(layer), Some(ts), Some(wall_ms), Some(rate)) = (
                v["key"].as_str(),
                v["layer"].as_u64(),
                v["ts"].as_u64(),
                v["wall_ms"].as_f64(),
                v["rate"].as_f64(),
            ) {
                c.set(
                    key,
                    layer as usize,
                    RtpClock {
                        ts: ts as u32,
                        wall_ms,
                        rate,
                    },
                );
            }
        }
        c.read += whole as u64;
    }

    fn set_clock(&self, key: &str, layer: usize, clock: RtpClock) {
        if let Ok(mut c) = self.clocks.lock() {
            c.set(key, layer, clock);
        }
        let line = json!({
            "key": key, "layer": layer, "ts": clock.ts, "wall_ms": clock.wall_ms, "rate": clock.rate,
        });
        if let Ok(mut f) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.clock_file)
        {
            let _ = f.write_all(format!("{line}\n").as_bytes());
        }
    }
}

/// Where a participant's measurements go and what it is measuring.
#[derive(Debug, Clone)]
pub struct Probe {
    pub sink: Arc<Sink>,
    /// The `impl` of its lines: `rtcd-rs`, `rtcd-go`, with `/chrome` for a browser (`/chrome-remote` for one across the internet).
    pub imp: String,
    /// `clean` or `impaired`: the network the participant is on.
    pub group: &'static str,
    /// The session id of the teacher: of the voices, only the teacher's is steady enough to
    /// measure (a student's microphone is muted most of the lesson).
    pub teacher: String,
    /// Whether this participant writes `recv` lines.
    pub measure: bool,
    /// Whether it also reports the wraps it sees (one receiver per group is enough).
    pub leader: bool,
}

impl Probe {
    pub fn event(&self, id: &str, what: &str, detail: &str) {
        self.sink.write(
            &self.imp,
            "event",
            id,
            json!({ "what": what, "detail": detail, "group": self.group }),
        );
    }

    /// A participant that lost its seat is back: `downtime_ms` since it lost it, after `attempts`.
    pub fn rejoined(&self, id: &str, downtime_ms: u64, attempts: u32) {
        self.sink.write(
            &self.imp,
            "event",
            id,
            json!({
                "what": "rejoin", "detail": "connected again", "group": self.group,
                "downtime_ms": downtime_ms, "attempts": attempts,
            }),
        );
    }

    /// A sender tells where its RTP clock for `kind` (`voice`, `video` or `screen`) and layer
    /// stands on the wall clock, to the receivers of the run and to the log.
    pub fn clock(&self, kind: &str, sender: &str, layer: usize, clock: RtpClock) {
        self.sink
            .set_clock(&format!("{kind}:{sender}"), layer, clock);
        self.sink.write(
            &self.imp,
            "event",
            sender,
            json!({
                "what": "rtp_clock", "group": self.group,
                "detail": format!(
                    "{kind} layer={layer} ts={} wall_ms={:.3} rate={}",
                    clock.ts, clock.wall_ms, clock.rate
                ),
            }),
        );
    }

    /// The one-way delay of a packet of `kind` from `sender` stamped `ts` that arrived at
    /// `arrival_ms`, if the sender said where its clocks stand (see [`delay_among`]).
    pub fn delay_ms(&self, kind: &str, sender: &str, ts: u32, arrival_ms: f64) -> Option<f64> {
        delay_among(
            &self.sink.clocks_of(&format!("{kind}:{sender}")),
            ts,
            arrival_ms,
        )
    }

    pub fn recv(&self, id: &str, fields: Value) {
        self.sink.write(&self.imp, "recv", id, fields);
    }

    /// How good a browser's picture of a camera looked over the last seconds (PSNR and SSIM against
    /// the clip it was sent from).
    pub fn quality(&self, id: &str, fields: Value) {
        self.sink.write(&self.imp, "quality", id, fields);
    }
}

/// What a receiver learned from one packet that the caller should report.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Wrapped {
    pub seq: bool,
    pub ts: bool,
}

/// One received RTP stream.
#[derive(Debug)]
pub struct RecvTrack {
    video: bool,
    clock: f64,
    packets: u64,
    /// The first and the highest sequence number, extended past the 16 bits.
    base: i64,
    max: i64,
    nack: u64,
    last_ts: u32,
    jitter: f64,
    transit: Option<f64>,
    first: Option<Instant>,
    last_packet: Option<Instant>,
    last_frame: Option<Instant>,
    frames: u64,
    intervals: VecDeque<f64>,
    freezes: u64,
    freeze_ms: f64,
    audio_gap_ms: f64,
    bytes: u64,
    /// Arrivals of a sequence number that had come already (a retransmission that was not needed,
    /// or the network's duplicate), and which of the last [`SEEN_BITS`] numbers have arrived.
    duplicates: u64,
    seen: [u64; (SEEN_BITS / 64) as usize],
    /// The one-way delays measured since the last line.
    delays: Vec<f64>,
}

impl RecvTrack {
    pub fn new(video: bool) -> Self {
        Self {
            video,
            clock: if video { 90_000.0 } else { 48_000.0 },
            packets: 0,
            base: 0,
            max: 0,
            nack: 0,
            last_ts: 0,
            jitter: 0.0,
            transit: None,
            first: None,
            last_packet: None,
            last_frame: None,
            frames: 0,
            intervals: VecDeque::new(),
            freezes: 0,
            freeze_ms: 0.0,
            audio_gap_ms: 0.0,
            bytes: 0,
            duplicates: 0,
            seen: [0; (SEEN_BITS / 64) as usize],
            delays: Vec::new(),
        }
    }

    /// Takes a measured one-way delay.
    pub fn add_delay(&mut self, ms: f64) {
        self.delays.push(ms);
    }

    /// The median and the 95th percentile of the delays since the last call, which starts the
    /// next interval; `None`s when there were none.
    pub fn take_delays(&mut self) -> (Option<f64>, Option<f64>) {
        let out = delay_summary(&mut self.delays);
        self.delays.clear();
        out
    }

    /// Counts the size of a packet that arrived (RTP header and payload).
    pub fn add_bytes(&mut self, n: usize) {
        self.bytes += n as u64;
    }

    fn slot(ext: i64) -> (usize, u64) {
        let i = ext.rem_euclid(SEEN_BITS) as usize;
        (i / 64, 1 << (i % 64))
    }

    fn mark(&mut self, ext: i64) {
        let (w, b) = Self::slot(ext);
        self.seen[w] |= b;
    }

    fn clear(&mut self, ext: i64) {
        let (w, b) = Self::slot(ext);
        self.seen[w] &= !b;
    }

    fn seen(&self, ext: i64) -> bool {
        let (w, b) = Self::slot(ext);
        self.seen[w] & b != 0
    }

    /// Takes a packet that arrived at `now`.
    pub fn on_packet(&mut self, now: Instant, seq: u16, ts: u32, marker: bool) -> Wrapped {
        let mut wrapped = Wrapped::default();
        let first = *self.first.get_or_insert(now);
        if self.packets == 0 {
            self.base = i64::from(seq);
            self.max = i64::from(seq);
            self.last_ts = ts;
            self.mark(self.max);
        } else {
            // The distance to the highest number so far, as a signed 16-bit step: a packet that
            // overtook another is behind, one after a gap ahead, the wrap included.
            let step = i64::from(seq.wrapping_sub(self.max as u16) as i16);
            if step > 0 {
                if step > 1 {
                    self.nack += (step - 1) as u64;
                }
                if (self.max + step) >> 16 > self.max >> 16 {
                    wrapped.seq = true;
                }
                // The numbers the step skips have not arrived: their bits are free again.
                for skipped in self.max + 1..=self.max + step.min(SEEN_BITS) {
                    self.clear(skipped);
                }
                self.max += step;
                self.mark(self.max);
            } else if -step < SEEN_BITS {
                let late = self.max + step;
                if self.seen(late) {
                    self.duplicates += 1;
                } else {
                    self.mark(late);
                }
                self.base = self.base.min(late);
            }
            if ts.wrapping_sub(self.last_ts) as i32 > 0 {
                if ts < self.last_ts {
                    wrapped.ts = true;
                }
                self.last_ts = ts;
            }
        }
        self.packets += 1;

        // RFC 3550 interarrival jitter, in the stream's clock.
        let arrival = now.duration_since(first).as_secs_f64() * self.clock;
        let transit = arrival - f64::from(ts);
        if let Some(prev) = self.transit {
            // The timestamp's own wrap must not read as a jump.
            let d = (transit - prev).rem_euclid(4_294_967_296.0);
            let d = if d > 2_147_483_648.0 {
                d - 4_294_967_296.0
            } else {
                d
            };
            self.jitter += (d.abs() - self.jitter) / 16.0;
        }
        self.transit = Some(transit);

        if !self.video
            && let Some(last) = self.last_packet
        {
            let gap = now.duration_since(last);
            if gap > AUDIO_GAP {
                self.audio_gap_ms += gap.as_secs_f64() * 1000.0;
            }
        }
        self.last_packet = Some(now);

        // A video frame is complete when its last packet (the marker) is in.
        if self.video && marker {
            self.frames += 1;
            if let Some(last) = self.last_frame {
                let interval = now.duration_since(last).as_secs_f64();
                if let Some(limit) = self.freeze_limit()
                    && interval > limit
                {
                    self.freezes += 1;
                    self.freeze_ms += interval * 1000.0;
                }
                self.intervals.push_back(interval);
                if self.intervals.len() > FREEZE_WINDOW {
                    self.intervals.pop_front();
                }
            }
            self.last_frame = Some(now);
        }
        wrapped
    }

    /// The longest a frame may take before it is a freeze, once there are enough frames to tell.
    fn freeze_limit(&self) -> Option<f64> {
        if self.intervals.len() < FREEZE_MIN_FRAMES {
            return None;
        }
        let avg = self.intervals.iter().sum::<f64>() / self.intervals.len() as f64;
        Some((3.0 * avg).max(avg + 0.15))
    }

    /// How long nothing has come.
    pub fn silent_for(&self, now: Instant) -> Option<Duration> {
        self.last_packet.map(|t| now.saturating_duration_since(t))
    }

    /// How long no frame has completed.
    pub fn frameless_for(&self, now: Instant) -> Option<Duration> {
        self.last_frame.map(|t| now.saturating_duration_since(t))
    }

    pub fn is_video(&self) -> bool {
        self.video
    }

    /// The counters at `now`: a freeze or a gap that is still going on is counted up to now
    /// (and, once it ends, in full, so the counters never go back).
    pub fn fields(&self, now: Instant, group: &str, pli: u64, rtt_ms: Option<f64>) -> Value {
        let mut freezes = self.freezes;
        let mut freeze_ms = self.freeze_ms;
        let mut gap_ms = self.audio_gap_ms;
        if let Some(open) = self.frameless_for(now)
            && let Some(limit) = self.freeze_limit()
            && open.as_secs_f64() > limit
        {
            freezes += 1;
            freeze_ms += open.as_secs_f64() * 1000.0;
        }
        if !self.video
            && let Some(open) = self.silent_for(now)
            && open > AUDIO_GAP
        {
            gap_ms += open.as_secs_f64() * 1000.0;
        }
        let expected = self.max - self.base + 1;
        // Every number that never arrived, retransmissions that made it taken off; a number that
        // arrived twice counts once, and is reported as a duplicate.
        let lost = expected - (self.packets - self.duplicates) as i64;
        json!({
            "media": if self.video { "video" } else { "audio" },
            "group": group,
            "frames": self.frames,
            "freezes": freezes,
            "freeze_ms": freeze_ms.round() as u64,
            "packets": self.packets,
            "bytes": self.bytes,
            "duplicates": self.duplicates,
            "lost": lost,
            "nack": self.nack,
            "pli": pli,
            "jitter_ms": (self.jitter / self.clock * 10_000.0).round() / 10.0,
            "rtt_ms": rtt_ms,
            "audio_gap_ms": gap_ms.round() as u64,
            "stalled": self.silent_for(now).is_some_and(|d| d > STALL),
        })
    }
}

/// A stream a receiver measures, and what it asked for.
#[derive(Debug)]
pub struct Tracked {
    pub stats: RecvTrack,
    /// `voice`, `video` or `screen`.
    pub kind: String,
    /// The name of the participant that sends it.
    pub sender: String,
    pub pli: u64,
    last_pli: Option<Instant>,
}

/// The streams one participant receives, by the SFU's track id (`<type>_<sender session>_<random>`).
#[derive(Debug, Default)]
pub struct Receiver {
    tracks: HashMap<String, Tracked>,
}

impl Receiver {
    /// Takes a packet of the track `track`; `None` for a track that is not measured. The wraps are
    /// returned with the track's name for an event.
    pub fn on_rtp(
        &mut self,
        probe: &Probe,
        now: Instant,
        track: &str,
        seq: u16,
        ts: u32,
        marker: bool,
    ) -> Option<(String, Wrapped)> {
        if !self.tracks.contains_key(track) {
            let mut fields = track.split('_');
            let (kind, session) = (fields.next()?, fields.next()?);
            let video = match kind {
                "video" | "screen" => true,
                "voice" if session == probe.teacher => false,
                _ => return None,
            };
            // A renegotiation can replace a stream's track with a new one (the SFU names it anew):
            // the old one is gone for the receiver, and silent for ever for these counters.
            let sender = session.strip_prefix("session-").unwrap_or(session);
            self.tracks
                .retain(|_, t| !(t.kind == kind && t.sender == sender));
            self.tracks.insert(
                track.to_owned(),
                Tracked {
                    stats: RecvTrack::new(video),
                    kind: kind.to_owned(),
                    sender: session
                        .strip_prefix("session-")
                        .unwrap_or(session)
                        .to_owned(),
                    pli: 0,
                    last_pli: None,
                },
            );
        }
        let t = self.tracks.get_mut(track)?;
        let duplicates = t.stats.duplicates;
        let wrapped = t.stats.on_packet(now, seq, ts, marker);
        // Of the video, the marker packet of a frame is the one that completes it.
        if t.stats.duplicates == duplicates
            && (marker || !t.stats.video)
            && let Some(delay) = probe.delay_ms(&t.kind, &t.sender, ts, wall_ms_at(now))
        {
            t.stats.add_delay(delay);
        }
        Some((format!("{}:{}", t.kind, t.sender), wrapped))
    }

    /// Counts the size of an RTP packet of `track` (after [`on_rtp`](Self::on_rtp) took it).
    pub fn count_bytes(&mut self, track: &str, n: usize) {
        if let Some(t) = self.tracks.get_mut(track) {
            t.stats.add_bytes(n);
        }
    }

    /// The video tracks that have been without a frame for a second and that a keyframe was not
    /// asked for in the last second: a decoder in that state asks for one.
    pub fn keyframes_due(&mut self, now: Instant) -> Vec<String> {
        let mut due = Vec::new();
        for (id, t) in &mut self.tracks {
            if !t.stats.is_video() || t.stats.frameless_for(now) < Some(Duration::from_secs(1)) {
                continue;
            }
            if t.last_pli
                .is_some_and(|p| now.duration_since(p) < Duration::from_secs(1))
            {
                continue;
            }
            t.last_pli = Some(now);
            t.pli += 1;
            due.push(id.clone());
        }
        due
    }

    /// Writes a `recv` line for every measured stream.
    pub fn write(&mut self, probe: &Probe, me: &str, now: Instant, rtt_ms: Option<f64>) {
        for t in self.tracks.values_mut() {
            let mut fields = t.stats.fields(now, probe.group, t.pli, rtt_ms);
            let (p50, p95) = t.stats.take_delays();
            fields["delay_p50_ms"] = p50.into();
            fields["delay_p95_ms"] = p95.into();
            probe.recv(&format!("{me}/{}:{}", t.kind, t.sender), fields);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clock() -> Instant {
        Instant::now()
    }

    /// An output file in a directory of its own, with no clocks shared in it yet.
    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("lms-sim-{name}"));
        std::fs::create_dir_all(&dir).unwrap();
        let _ = std::fs::remove_file(dir.join("rtp.clocks"));
        dir.join("out.jsonl")
    }

    #[test]
    fn counts_loss_across_the_sequence_wrap() {
        let t0 = clock();
        let mut t = RecvTrack::new(false);
        let mut wrapped = Wrapped::default();
        // 65530..=65535, 0..=5, with 65533 and 1 never arriving.
        for (i, seq) in (65530u32..65542).map(|s| (s % 65536) as u16).enumerate() {
            if seq == 65533 || seq == 1 {
                continue;
            }
            let w = t.on_packet(
                t0 + Duration::from_millis(20 * i as u64),
                seq,
                i as u32 * 960,
                false,
            );
            wrapped.seq |= w.seq;
        }
        assert!(wrapped.seq);
        let f = t.fields(t0 + Duration::from_millis(240), "clean", 0, None);
        assert_eq!(f["packets"], 10);
        assert_eq!(f["lost"], 2);
        assert_eq!(f["nack"], 2);
    }

    #[test]
    fn sees_the_timestamp_wrap_once() {
        let t0 = clock();
        let mut t = RecvTrack::new(true);
        let start = u32::MAX - 90_000 * 2;
        let mut count = 0;
        for i in 0..200u32 {
            let w = t.on_packet(
                t0 + Duration::from_millis(33 * u64::from(i)),
                i as u16,
                start.wrapping_add(i * 3000),
                true,
            );
            count += usize::from(w.ts);
        }
        assert_eq!(count, 1);
        // The jitter stays small: the wrap is not a jump.
        let f = t.fields(t0 + Duration::from_millis(33 * 200), "clean", 0, None);
        assert!(f["jitter_ms"].as_f64().unwrap() < 5.0, "{f}");
    }

    #[test]
    fn a_late_frame_is_a_freeze_and_a_steady_one_is_not() {
        let t0 = clock();
        let mut t = RecvTrack::new(true);
        let mut at = 0u64;
        for i in 0..60u32 {
            at += 33;
            t.on_packet(t0 + Duration::from_millis(at), i as u16, i * 3000, true);
        }
        assert_eq!(
            t.fields(t0 + Duration::from_millis(at), "clean", 0, None)["freezes"],
            0
        );
        at += 400;
        t.on_packet(t0 + Duration::from_millis(at), 60, 60 * 3000, true);
        let f = t.fields(t0 + Duration::from_millis(at), "clean", 0, None);
        assert_eq!(f["freezes"], 1);
        assert_eq!(f["freeze_ms"], 400);
    }

    #[test]
    fn an_open_gap_counts_and_a_stall_shows() {
        let t0 = clock();
        let mut t = RecvTrack::new(false);
        for i in 0..50u32 {
            t.on_packet(
                t0 + Duration::from_millis(20 * u64::from(i)),
                i as u16,
                i * 960,
                false,
            );
        }
        let quiet = t0 + Duration::from_millis(1000 + 3000);
        let f = t.fields(quiet, "impaired", 0, Some(12.5));
        assert_eq!(f["stalled"], true);
        assert!(f["audio_gap_ms"].as_u64().unwrap() >= 3000, "{f}");
        assert_eq!(f["rtt_ms"], 12.5);
        // When it ends the gap is counted in full, never less than it was.
        t.on_packet(quiet, 50, 50 * 960, false);
        let g = t.fields(quiet, "impaired", 0, None);
        assert!(g["audio_gap_ms"].as_u64() >= f["audio_gap_ms"].as_u64());
        assert_eq!(g["stalled"], false);
    }

    #[test]
    fn the_delay_follows_the_senders_clock_across_the_timestamp_wrap() {
        let clock = RtpClock {
            ts: u32::MAX - 959,
            wall_ms: 1_000_000.0,
            rate: 48_000.0,
        };
        // 20 ms after the anchor in sender time (past the wrap), arrived 35 ms after the anchor.
        assert!((clock.delay_ms(0, 1_000_035.0) - 15.0).abs() < 1e-6);
        assert!((clock.delay_ms(u32::MAX - 959, 1_000_012.0) - 12.0).abs() < 1e-6);
    }

    #[test]
    fn another_process_reads_the_clocks_from_the_shared_file() {
        let path = scratch("stats-shared");
        let sender = Sink::open(&path).unwrap();
        let receiver = Sink::open(&path.with_file_name("other.jsonl")).unwrap();
        let sent = wall_ms_at(clock()) - 30.0;
        // Two layers of one camera that start apart: the one that fits the timestamp is the clock.
        sender.set_clock(
            "video:s01",
            0,
            RtpClock {
                ts: 1000,
                wall_ms: sent,
                rate: 90_000.0,
            },
        );
        sender.set_clock(
            "video:s01",
            1,
            RtpClock {
                ts: 900_000,
                wall_ms: sent,
                rate: 90_000.0,
            },
        );
        let clocks = receiver.clocks_of("video:s01");
        assert_eq!(clocks.len(), 2);
        let delay = delay_among(&clocks, 900_000, wall_ms_at(clock())).unwrap();
        assert!((delay - 30.0).abs() < 5.0, "{delay}");
        assert!(receiver.clocks_of("video:nobody").is_empty());
    }

    #[test]
    fn delays_are_taken_per_interval_and_only_for_audio_and_frame_ends() {
        let probe = Probe {
            sink: Sink::open(&scratch("stats-delay")).unwrap(),
            imp: "rtcd-rs".into(),
            group: "clean",
            teacher: "session-teacher".into(),
            measure: true,
            leader: false,
        };
        let now = clock();
        let sent = wall_ms_at(now) - 40.0;
        probe.clock(
            "voice",
            "teacher",
            0,
            RtpClock {
                ts: 0,
                wall_ms: sent,
                rate: 48_000.0,
            },
        );
        probe.clock(
            "video",
            "teacher",
            0,
            RtpClock {
                ts: 0,
                wall_ms: sent,
                rate: 90_000.0,
            },
        );
        let mut r = Receiver::default();
        r.on_rtp(&probe, now, "voice_session-teacher_a", 1, 0, false);
        // Two packets of one frame: only the one with the marker is a delay sample.
        r.on_rtp(&probe, now, "video_session-teacher_a", 1, 0, false);
        r.on_rtp(&probe, now, "video_session-teacher_a", 2, 0, true);
        // A sender that did not say where its clock stands has no delay.
        r.on_rtp(&probe, now, "screen_session-teacher_a", 1, 0, true);
        for t in r.tracks.values_mut() {
            let (p50, p95) = t.stats.take_delays();
            match t.kind.as_str() {
                "screen" => assert_eq!((p50, p95), (None, None)),
                _ => {
                    assert!((p50.unwrap() - 40.0).abs() < 5.0, "{p50:?}");
                    assert_eq!(t.stats.delays.len(), 0);
                }
            }
        }
        let mut t = RecvTrack::new(false);
        for ms in [10.0, 20.0, 30.0, 40.0, 100.0] {
            t.add_delay(ms);
        }
        assert_eq!(t.take_delays(), (Some(30.0), Some(100.0)));
        assert_eq!(t.take_delays(), (None, None));
    }

    #[test]
    fn only_the_teachers_voice_and_the_videos_are_measured() {
        let probe = Probe {
            sink: Sink::open(&scratch("stats")).unwrap(),
            imp: "rtcd-rs".into(),
            group: "clean",
            teacher: "session-teacher".into(),
            measure: true,
            leader: false,
        };
        let mut r = Receiver::default();
        let now = clock();
        assert!(
            r.on_rtp(&probe, now, "voice_session-s01_abcdefgh", 1, 1, false)
                .is_none()
        );
        assert!(
            r.on_rtp(
                &probe,
                now,
                "screen-audio_session-teacher_abcdefgh",
                1,
                1,
                false
            )
            .is_none()
        );
        let id = r.on_rtp(&probe, now, "voice_session-teacher_abcdefgh", 1, 1, false);
        assert_eq!(id.unwrap().0, "voice:teacher");
        let id = r.on_rtp(&probe, now, "screen_session-teacher_abcdefgh", 1, 1, true);
        assert_eq!(id.unwrap().0, "screen:teacher");
        // The SFU names the track anew: the old one is dropped, not left to report a stall.
        r.on_rtp(&probe, now, "screen_session-teacher_zzzzzzzz", 1, 1, true);
        assert_eq!(r.tracks.len(), 2);
    }

    #[test]
    fn bytes_are_counted_and_events_name_their_group() {
        let path = std::env::temp_dir().join("lms-sim-stats-bytes-test.jsonl");
        let _ = std::fs::remove_file(&path);
        let probe = Probe {
            sink: Sink::open(&path).unwrap(),
            imp: "rtcd-rs".into(),
            group: "impaired",
            teacher: "session-teacher".into(),
            measure: true,
            leader: false,
        };
        let mut r = Receiver::default();
        let now = clock();
        let track = "video_session-teacher_abcdefgh";
        r.on_rtp(&probe, now, track, 1, 1, true);
        r.count_bytes(track, 1000);
        r.count_bytes(track, 200);
        r.count_bytes("video_session-nobody_abcdefgh", 5);
        r.write(&probe, "s17", now, None);
        probe.rejoined("s17", 4200, 2);
        let lines: Vec<Value> = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(lines[0]["bytes"], 1200);
        assert_eq!(lines[1]["what"], "rejoin");
        assert_eq!(lines[1]["group"], "impaired");
        assert_eq!(lines[1]["downtime_ms"], 4200);
    }

    #[test]
    fn a_duplicate_does_not_hide_a_loss() {
        let t0 = clock();
        let mut t = RecvTrack::new(false);
        // 1 2 2 4 (3 never comes, 2 comes twice) and then 3 late, then 3 again.
        for (n, seq) in [1u16, 2, 2, 4].into_iter().enumerate() {
            t.on_packet(t0 + Duration::from_millis(20 * n as u64), seq, 0, false);
        }
        let f = t.fields(t0 + Duration::from_millis(80), "impaired", 0, None);
        assert_eq!(
            (
                f["packets"].as_u64(),
                f["duplicates"].as_u64(),
                f["lost"].as_i64()
            ),
            (Some(4), Some(1), Some(1))
        );
        t.on_packet(t0 + Duration::from_millis(100), 3, 0, false);
        t.on_packet(t0 + Duration::from_millis(120), 3, 0, false);
        let f = t.fields(t0 + Duration::from_millis(120), "impaired", 0, None);
        assert_eq!(
            (
                f["packets"].as_u64(),
                f["duplicates"].as_u64(),
                f["lost"].as_i64()
            ),
            (Some(6), Some(2), Some(0))
        );
    }
}
