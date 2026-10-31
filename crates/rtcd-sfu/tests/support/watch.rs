//! What a viewer makes of a screen share: the frames put together from the packets as they arrive,
//! decoded with the references a browser's VP8 receiver works out (libwebrtc's `RtpVp8RefFinder`:
//! a frame waits for the frames it refers to, and a frame of a temporal layer above the base also
//! for every picture id between it and its base frame), and shown no earlier than a playout delay
//! allows. From that: how long the picture froze, how late it was, and when the browser would have
//! given up on the frames it has and asked for a keyframe.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use rtcd_sfu::vp8::Descriptor;

/// A browser asks for a keyframe when nothing could be decoded for this long while frames arrive
/// (libwebrtc's `max_wait_for_frame`), and again every [`ASK_AGAIN`] until one comes.
pub const GIVE_UP: Duration = Duration::from_secs(3);
pub const ASK_AGAIN: Duration = Duration::from_millis(200);

/// Picture ids are 15 bits.
const PID_MASK: u16 = 0x7fff;

/// The trailer `World::send_screen_frame` puts on every packet: when it was sent, in microseconds
/// of the world's clock, before the four marker bytes.
pub fn sent_at(payload: &[u8], epoch: Instant) -> Option<Instant> {
    let n = payload.len();
    let bytes: [u8; 8] = payload.get(n.checked_sub(12)?..n - 4)?.try_into().ok()?;
    Some(epoch + Duration::from_micros(u64::from_le_bytes(bytes)))
}

#[derive(Debug, Clone)]
struct Frame {
    first: Option<u64>,
    last: Option<u64>,
    seqs: Vec<u64>,
    sent: Instant,
    key: bool,
    pid: Option<u16>,
    tl0: Option<u8>,
    tid: Option<u8>,
    sync: bool,
    /// The layer the sender marked it with (see `support::Layer`).
    layer: u8,
    complete_at: Option<Instant>,
    decoded_at: Option<Instant>,
    /// Older than a keyframe that was decoded, and not decoded itself: it never will be.
    dead: bool,
}

impl Frame {
    fn complete(&self) -> bool {
        match (self.first, self.last) {
            (Some(f), Some(l)) if l >= f => self.seqs.len() as u64 == l - f + 1,
            _ => false,
        }
    }
}

/// One viewer's screen share, as its browser would play it.
#[derive(Debug, Default)]
pub struct ScreenWatch {
    epoch: Option<Instant>,
    highest: Option<u64>,
    /// Extended sequence numbers seen, so a repeat is counted once.
    seen: std::collections::HashSet<u64>,
    /// By RTP timestamp, and the timestamp of each picture id.
    frames: HashMap<u32, Frame>,
    by_pid: HashMap<u16, u32>,
    /// Decoded frames by picture id, and the base frame of each TL0PICIDX.
    decoded: HashMap<u16, Instant>,
    base_of: HashMap<u8, u16>,
    last_pid_decoded: Option<u16>,
    last_decode: Option<Instant>,
    /// Frames complete but not decodable (waiting on a reference) since this instant.
    stuck_since: Option<Instant>,
    last_ask: Option<Instant>,
    pub keyframe_requests: usize,
    pub duplicates: usize,
}

/// How a run looked to the viewer over a span of it.
#[derive(Debug, Clone, Copy, Default)]
pub struct Playback {
    /// Frames shown in the span.
    pub frames: usize,
    /// Seconds the picture stood still for longer than a browser counts as a freeze.
    pub frozen: f64,
    /// Frames that arrived whole but could never be decoded, and frames never completed.
    pub broken: usize,
    pub incomplete: usize,
    /// From sending to showing, seconds: the median, the 95th percentile and the worst.
    pub delay_p50: f64,
    pub delay_p95: f64,
    pub delay_max: f64,
}

impl ScreenWatch {
    pub fn set_epoch(&mut self, epoch: Instant) {
        self.epoch = Some(epoch);
    }

    fn extend(&mut self, seq: u16) -> u64 {
        let Some(h) = self.highest else {
            let e = 1u64 << 20 | u64::from(seq);
            self.highest = Some(e);
            return e;
        };
        let delta = seq.wrapping_sub(h as u16) as i16;
        let e = (h as i64 + i64::from(delta)) as u64;
        if e > h {
            self.highest = Some(e);
        }
        e
    }

    /// A packet of the screen arrived at `now`.
    pub fn on_packet(&mut self, now: Instant, packet: &rtc::rtp::Packet) {
        let Some(epoch) = self.epoch else { return };
        let Some(sent) = sent_at(&packet.payload, epoch) else {
            return;
        };
        let seq = self.extend(packet.header.sequence_number);
        if !self.seen.insert(seq) {
            self.duplicates += 1;
            return;
        }
        let Some(d) = Descriptor::parse(&packet.payload) else {
            return;
        };
        let layer = packet.payload[packet.payload.len() - 4];
        let f = self.frames.entry(packet.header.timestamp).or_insert(Frame {
            first: None,
            last: None,
            seqs: Vec::new(),
            sent,
            key: false,
            pid: d.picture_id.map(|(p, _)| p),
            tl0: d.tl0_pic_idx,
            tid: d.temporal.map(|t| t.tid),
            sync: d.temporal.is_some_and(|t| t.sync),
            layer,
            complete_at: None,
            decoded_at: None,
            dead: false,
        });
        if let Some(pid) = f.pid {
            self.by_pid.entry(pid).or_insert(packet.header.timestamp);
        }
        f.seqs.push(seq);
        if d.start && d.partition == 0 {
            f.first = Some(seq);
            f.key = rtcd_sfu::vp8::is_keyframe(&packet.payload);
        }
        if packet.header.marker {
            f.last = Some(seq);
        }
        if f.complete_at.is_none() && f.complete() {
            f.complete_at = Some(now);
            self.decode(now);
        }
    }

    /// Whether the frame of picture id `pid` may be decoded now.
    fn decodable(&self, f: &Frame) -> bool {
        if f.key {
            return true;
        }
        let Some(pid) = f.pid else { return false };
        match (f.tid, f.tl0) {
            // No temporal layers: each frame refers to the one before.
            (None, _) | (_, None) => self.decoded.contains_key(&(pid.wrapping_sub(1) & PID_MASK)),
            (Some(0), Some(tl0)) => self
                .base_of
                .get(&tl0.wrapping_sub(1))
                .is_some_and(|p| self.decoded.contains_key(p)),
            (Some(tid), Some(tl0)) => {
                let Some(&base) = self.base_of.get(&tl0) else {
                    return false;
                };
                if !self.decoded.contains_key(&base) {
                    return false;
                }
                if f.sync {
                    return true;
                }
                // Every picture between the base and this one must have arrived, and those of
                // this layer or below been decoded.
                let mut p = (base + 1) & PID_MASK;
                while p != pid {
                    match self.by_pid.get(&p).and_then(|ts| self.frames.get(ts)) {
                        Some(g) if g.complete_at.is_some() => {
                            if g.tid.is_some_and(|t| t <= tid) && g.decoded_at.is_none() {
                                return false;
                            }
                        }
                        _ => return false,
                    }
                    p = (p + 1) & PID_MASK;
                }
                true
            }
        }
    }

    /// Decodes whatever has become decodable, oldest picture first, until nothing more is.
    fn decode(&mut self, now: Instant) {
        loop {
            let mut ready: Vec<(u16, u32)> = self
                .frames
                .iter()
                .filter(|(_, f)| f.complete_at.is_some() && f.decoded_at.is_none() && !f.dead)
                .filter(|(_, f)| self.decodable(f))
                .filter_map(|(&ts, f)| Some((f.pid?, ts)))
                .collect();
            if ready.is_empty() {
                break;
            }
            ready.sort_by_key(|&(pid, _)| {
                // Oldest relative to the last decoded picture.
                self.last_pid_decoded
                    .map_or(0, |last| pid.wrapping_sub(last) & PID_MASK)
            });
            let (pid, ts) = ready[0];
            let f = self.frames.get_mut(&ts).expect("listed");
            f.decoded_at = Some(now);
            let (key, tid, tl0, sent) = (f.key, f.tid, f.tl0, f.sent);
            if key {
                // A keyframe starts the stream afresh: what came before it is never shown.
                self.decoded.clear();
                self.base_of.clear();
                for g in self.frames.values_mut() {
                    if g.sent < sent && g.decoded_at.is_none() {
                        g.dead = true;
                    }
                }
            }
            self.decoded.insert(pid, now);
            if tid.unwrap_or(0) == 0
                && let Some(t) = tl0
            {
                self.base_of.insert(t, pid);
            }
            self.last_pid_decoded = Some(pid);
            self.last_decode = Some(now);
            self.stuck_since = None;
        }
        let waiting = self
            .frames
            .values()
            .any(|f| f.complete_at.is_some() && f.decoded_at.is_none() && !f.dead);
        if waiting {
            self.stuck_since.get_or_insert(now);
        }
    }

    /// Whether the browser asks for a keyframe now: it has frames it cannot decode and has decoded
    /// nothing for [`GIVE_UP`], and did not just ask.
    pub fn wants_keyframe(&mut self, now: Instant) -> bool {
        let Some(stuck) = self.stuck_since else {
            return false;
        };
        let idle = self.last_decode.map_or(stuck, |d| d.max(stuck));
        if now.saturating_duration_since(idle) < GIVE_UP
            || self
                .last_ask
                .is_some_and(|t| now.saturating_duration_since(t) < ASK_AGAIN)
        {
            return false;
        }
        self.last_ask = Some(now);
        self.keyframe_requests += 1;
        true
    }

    /// How the frames sent from `from` to `to` played, with the browser holding each frame until
    /// `base + target` after it was sent (`base` is the path's one-way delay; a `target` of zero
    /// shows a frame as soon as it is decoded), and showing a frame no earlier than the one before.
    pub fn playback(
        &self,
        from: Instant,
        to: Instant,
        base: Duration,
        target: Duration,
    ) -> Playback {
        let mut shown: Vec<(Instant, Instant)> = self
            .frames
            .values()
            .filter(|f| f.sent >= from && f.sent < to)
            .filter_map(|f| Some((f.sent, f.decoded_at?.max(f.sent + base + target))))
            .collect();
        shown.sort();
        let mut last = None::<Instant>;
        for s in &mut shown {
            if let Some(l) = last
                && s.1 < l
            {
                s.1 = l;
            }
            last = Some(s.1);
        }
        // A browser counts an interval as a freeze when it is longer than three times the average
        // of the recent ones and longer than it by 150 ms (`totalFreezesDuration`).
        // As the browser does it (`VideoQualityObserver`): the interval joins the last 30 first,
        // and is a freeze from five on.
        let mut frozen = 0.0;
        let mut recent: std::collections::VecDeque<f64> = Default::default();
        for w in shown.windows(2) {
            let gap = w[1].1.saturating_duration_since(w[0].1).as_secs_f64();
            recent.push_back(gap);
            if recent.len() > 30 {
                recent.pop_front();
            }
            let avg = recent.iter().sum::<f64>() / recent.len() as f64;
            if recent.len() >= 5 && gap >= (3.0 * avg).max(avg + 0.15) {
                frozen += gap;
            }
        }
        let mut delays: Vec<f64> = shown
            .iter()
            .map(|(sent, at)| at.saturating_duration_since(*sent).as_secs_f64())
            .collect();
        delays.sort_by(f64::total_cmp);
        let pick = |q: f64| {
            delays
                .get(((delays.len() as f64 - 1.0) * q).round().max(0.0) as usize)
                .copied()
                .unwrap_or(0.0)
        };
        let span = |f: &&Frame| f.sent >= from && f.sent < to;
        Playback {
            frames: shown.len(),
            frozen,
            broken: self
                .frames
                .values()
                .filter(span)
                .filter(|f| f.complete_at.is_some() && f.decoded_at.is_none())
                .count(),
            incomplete: self
                .frames
                .values()
                .filter(span)
                .filter(|f| f.complete_at.is_none())
                .count(),
            delay_p50: pick(0.5),
            delay_p95: pick(0.95),
            delay_max: delays.last().copied().unwrap_or(0.0),
        }
    }

    /// Frames shown in the span by the layer they came from (1 small, 2 large) and temporal layer.
    pub fn shown_by_layer(&self, from: Instant, to: Instant) -> BTreeMap<(u8, Option<u8>), usize> {
        let mut out = BTreeMap::new();
        for f in self.frames.values() {
            if f.sent >= from && f.sent < to && f.decoded_at.is_some() {
                *out.entry((f.layer, f.tid)).or_default() += 1;
            }
        }
        out
    }
}

/// The share of `xs` at or below `q`.
pub fn quantile(xs: &mut [f64], q: f64) -> f64 {
    if xs.is_empty() {
        return 0.0;
    }
    xs.sort_by(f64::total_cmp);
    xs[((xs.len() as f64 - 1.0) * q).round() as usize]
}
