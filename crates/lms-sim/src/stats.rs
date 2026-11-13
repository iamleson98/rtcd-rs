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
//!   report counts them (a late packet that comes after all takes one off);
//! * **nack** is the packets asked for again: every sequence number the receiver saw skipped.

use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value, json};

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
}

impl Sink {
    /// Opens `path` for appending.
    pub fn open(path: &Path) -> std::io::Result<Arc<Self>> {
        let out = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Arc::new(Self {
            out: Mutex::new(out),
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

/// Where a participant's measurements go and what it is measuring.
#[derive(Debug, Clone)]
pub struct Probe {
    pub sink: Arc<Sink>,
    /// The `impl` of its lines: `rtcd-rs`, `rtcd-go`, with `/chrome` for a browser.
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
            json!({ "what": what, "detail": detail }),
        );
    }

    pub fn recv(&self, id: &str, fields: Value) {
        self.sink.write(&self.imp, "recv", id, fields);
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
        }
    }

    /// Takes a packet that arrived at `now`.
    pub fn on_packet(&mut self, now: Instant, seq: u16, ts: u32, marker: bool) -> Wrapped {
        let mut wrapped = Wrapped::default();
        let first = *self.first.get_or_insert(now);
        if self.packets == 0 {
            self.base = i64::from(seq);
            self.max = i64::from(seq);
            self.last_ts = ts;
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
                self.max += step;
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
        let lost = (expected - self.packets as i64).max(0);
        json!({
            "media": if self.video { "video" } else { "audio" },
            "group": group,
            "frames": self.frames,
            "freezes": freezes,
            "freeze_ms": freeze_ms.round() as u64,
            "packets": self.packets,
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
        let wrapped = t.stats.on_packet(now, seq, ts, marker);
        Some((format!("{}:{}", t.kind, t.sender), wrapped))
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
    pub fn write(&self, probe: &Probe, me: &str, now: Instant, rtt_ms: Option<f64>) {
        for t in self.tracks.values() {
            probe.recv(
                &format!("{me}/{}:{}", t.kind, t.sender),
                t.stats.fields(now, probe.group, t.pli, rtt_ms),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clock() -> Instant {
        Instant::now()
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
    fn only_the_teachers_voice_and_the_videos_are_measured() {
        let probe = Probe {
            sink: Sink::open(&std::env::temp_dir().join("lms-sim-stats-test.jsonl")).unwrap(),
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
}
