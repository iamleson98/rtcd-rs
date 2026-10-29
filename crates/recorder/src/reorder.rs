//! Puts RTP packets back in order.
//!
//! A gap is waited for a little (the sender's retransmission to a NACK fills it), then given up:
//! the packets after it are released and the gap is reported as lost, so the consumer can drop
//! the frame it broke and ask for a new keyframe.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crate::tap::TapPacket;

/// What leaves the buffer.
#[derive(Debug)]
pub enum Item {
    Packet(TapPacket),
    /// This many packets will not come.
    Lost(u32),
}

#[derive(Debug)]
pub struct Reorder {
    slots: Vec<Option<TapPacket>>,
    /// The ring's size, the most packets that can be held at once.
    size: u64,
    /// Items already decided, ahead of the slots (made when a jump forced gaps to be given up).
    ready: VecDeque<Item>,
    /// The sequence number (extended past 16 bits) expected next.
    next: Option<u64>,
    highest: u64,
    /// When the packet at `next` started to be missed.
    gap_since: Option<Instant>,
    wait: Duration,
    /// Packets held (waiting behind a gap).
    held: usize,
    /// Packets dropped for being late or duplicates.
    pub dropped: u64,
}

impl Reorder {
    /// `wait` is how long a gap is waited for, and `size` how many packets can be held behind it.
    pub fn new(wait: Duration, size: usize) -> Self {
        Self {
            slots: (0..size).map(|_| None).collect(),
            size: size as u64,
            ready: VecDeque::new(),
            next: None,
            highest: 0,
            gap_since: None,
            wait,
            held: 0,
            dropped: 0,
        }
    }

    fn extend(&self, seq: u16) -> u64 {
        // The extended number nearest to the highest seen.
        let base = self.highest & !0xffff;
        let candidate = base | u64::from(seq);
        let high = self.highest;
        if candidate + 0x8000 < high {
            candidate + 0x1_0000
        } else if candidate > high + 0x8000 && candidate >= 0x1_0000 {
            candidate - 0x1_0000
        } else {
            candidate
        }
    }

    pub fn push(&mut self, p: TapPacket) {
        let now = p.arrived;
        let seq = p.seq;
        let ext = if self.next.is_none() {
            // Start in the middle of the number space so that early packets before the first
            // one are never mistaken for wrapped ones.
            let e = 0x1_0000 + u64::from(seq);
            self.next = Some(e);
            self.highest = e;
            e
        } else {
            self.extend(seq)
        };
        if ext < self.next.unwrap_or(ext) {
            self.dropped += 1; // late: its place was given up
            return;
        }
        // Too far ahead for the buffer: gaps are given up until it fits.
        while ext >= self.next.unwrap_or(ext) + self.size {
            match self.take(now, true) {
                Some(item) => self.ready.push_back(item),
                None => {
                    // Nothing held: jump over the empty stretch, which is lost.
                    let next = self.next.unwrap_or(ext);
                    if ext > next {
                        self.ready.push_back(Item::Lost((ext - next) as u32));
                    }
                    self.next = Some(ext);
                    break;
                }
            }
        }
        let slot = &mut self.slots[(ext % self.size) as usize];
        if slot.is_some() {
            self.dropped += 1; // duplicate
            return;
        }
        *slot = Some(p);
        self.held += 1;
        self.highest = self.highest.max(ext);
        if self.next != Some(ext) && self.gap_since.is_none() {
            self.gap_since = Some(now);
        }
    }

    /// The next thing in order, if there is one.
    pub fn pop(&mut self, now: Instant) -> Option<Item> {
        if let Some(item) = self.ready.pop_front() {
            return Some(item);
        }
        self.take(now, false)
    }

    /// The arrival of the earliest packet held after `from`.
    fn earliest_after(&self, from: u64) -> Option<Instant> {
        (from..self.highest + 1)
            .take(self.size as usize)
            .find_map(|e| {
                self.slots[(e % self.size) as usize]
                    .as_ref()
                    .map(|p| p.arrived)
            })
    }

    fn take(&mut self, now: Instant, force: bool) -> Option<Item> {
        let next = self.next?;
        let idx = (next % self.size) as usize;
        if let Some(p) = self.slots[idx].take() {
            self.held -= 1;
            self.next = Some(next + 1);
            self.gap_since =
                if self.held == 0 || self.slots[((next + 1) % self.size) as usize].is_some() {
                    None
                } else {
                    self.earliest_after(next + 1)
                };
            return Some(Item::Packet(p));
        }
        if self.held == 0 {
            return None;
        }
        // Something is held behind the gap: give the gap up once it has been waited for.
        let due = force
            || self
                .gap_since
                .is_some_and(|t| now.saturating_duration_since(t) >= self.wait);
        if !due {
            return None;
        }
        // The next packet held is within the ring; if the count were ever wrong this still ends.
        let mut e = next + 1;
        while e < next + self.size && self.slots[(e % self.size) as usize].is_none() {
            e += 1;
        }
        if self.slots[(e % self.size) as usize].is_none() {
            self.held = 0;
        }
        self.next = Some(e);
        self.gap_since = None;
        Some(Item::Lost((e - next) as u32))
    }

    /// When a gap will have been waited for, if one is open.
    pub fn deadline(&self) -> Option<Instant> {
        self.gap_since.map(|t| t + self.wait)
    }

    pub fn held(&self) -> usize {
        self.held
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn pkt(arrived: Instant, seq: u16) -> TapPacket {
        TapPacket {
            seq,
            ts: 0,
            marker: false,
            payload: Bytes::new(),
            arrived,
        }
    }

    fn drain(r: &mut Reorder, now: Instant) -> Vec<String> {
        let mut out = Vec::new();
        while let Some(i) = r.pop(now) {
            out.push(match i {
                Item::Packet(p) => p.seq.to_string(),
                Item::Lost(n) => format!("lost{n}"),
            });
        }
        out
    }

    #[test]
    fn in_order_passes_through() {
        let now = Instant::now();
        let mut r = Reorder::new(Duration::from_millis(100), 512);
        for s in [10, 11, 12] {
            r.push(pkt(now, s));
        }
        assert_eq!(drain(&mut r, now), ["10", "11", "12"]);
    }

    #[test]
    fn reordered_packets_are_put_right() {
        let now = Instant::now();
        let mut r = Reorder::new(Duration::from_millis(100), 512);
        for s in [10, 12, 11, 14, 13] {
            r.push(pkt(now, s));
        }
        assert_eq!(drain(&mut r, now), ["10", "11", "12", "13", "14"]);
    }

    #[test]
    fn a_gap_is_waited_for_then_given_up() {
        let t0 = Instant::now();
        let mut r = Reorder::new(Duration::from_millis(100), 512);
        for s in [10, 11, 14, 15] {
            r.push(pkt(t0, s));
        }
        assert_eq!(drain(&mut r, t0), ["10", "11"]);
        assert_eq!(r.deadline(), Some(t0 + Duration::from_millis(100)));
        assert_eq!(
            drain(&mut r, t0 + Duration::from_millis(50)),
            Vec::<String>::new()
        );
        assert_eq!(
            drain(&mut r, t0 + Duration::from_millis(100)),
            ["lost2", "14", "15"]
        );
    }

    #[test]
    fn a_retransmission_fills_the_gap() {
        let t0 = Instant::now();
        let mut r = Reorder::new(Duration::from_millis(100), 512);
        for s in [10, 12, 13] {
            r.push(pkt(t0, s));
        }
        assert_eq!(drain(&mut r, t0), ["10"]);
        r.push(pkt(t0 + Duration::from_millis(20), 11));
        assert_eq!(
            drain(&mut r, t0 + Duration::from_millis(20)),
            ["11", "12", "13"]
        );
        assert_eq!(r.deadline(), None);
    }

    #[test]
    fn late_and_duplicate_packets_are_dropped() {
        let t0 = Instant::now();
        let mut r = Reorder::new(Duration::from_millis(10), 512);
        for s in [10, 12] {
            r.push(pkt(t0, s));
        }
        assert_eq!(
            drain(&mut r, t0 + Duration::from_millis(10)),
            ["10", "lost1", "12"]
        );
        r.push(pkt(t0, 11));
        r.push(pkt(t0, 12));
        assert_eq!(r.dropped, 2);
        assert!(drain(&mut r, t0).is_empty());
    }

    #[test]
    fn sequence_numbers_wrap() {
        let now = Instant::now();
        let mut r = Reorder::new(Duration::from_millis(100), 512);
        for s in [65534, 65535, 1, 0, 2] {
            r.push(pkt(now, s));
        }
        assert_eq!(drain(&mut r, now), ["65534", "65535", "0", "1", "2"]);
    }

    #[test]
    fn a_far_jump_gives_up_the_gap_at_once() {
        let now = Instant::now();
        let mut r = Reorder::new(Duration::from_secs(60), 512);
        r.push(pkt(now, 1));
        r.push(pkt(now, 3));
        r.push(pkt(now, 2000));
        assert_eq!(drain(&mut r, now), ["1", "lost1", "3", "lost1996", "2000"]);
    }
}
