//! What a receiver's copy of a stream needs changed: its own SSRC and payload type, and sequence
//! numbers, timestamps and VP8 picture numbers that carry on without a gap when the stream is
//! moved from one simulcast layer of the sender to another, or when frames of it are left out (the
//! temporal layers a receiver is not sent, see `temporal`). To the receiver it is one continuous
//! stream, so switching layers costs no renegotiation, and a frame left out is not one lost.

use std::collections::VecDeque;

use rtc::rtp::Packet;

use crate::vp8;

/// Timestamp step assumed between the last frame of the old layer and the first of the new one,
/// 30 frames per second on the 90 kHz video clock.
const FRAME_GAP: u32 = 3000;
/// Runs of numbers left out that are remembered: a packet older than all of them (a retransmission
/// seconds late) can no longer be placed, and is not sent.
const REMEMBERED_RUNS: usize = 64;
/// Picture ids are 15 bits wide, or 7.
const WIDE_PICTURE_ID: u16 = 0x7fff;
const SHORT_PICTURE_ID: u16 = 0x7f;

/// Whether `a` comes after `b` among numbers that wrap at `mask + 1`.
fn after(a: u16, b: u16, mask: u16) -> bool {
    let d = a.wrapping_sub(b) & mask;
    d != 0 && d <= mask / 2
}

/// The numbers of a source that were left out of the receiver's copy, as runs, so that the numbers
/// after them can close up. Only numbers newer than any seen are added: a late packet of a frame
/// left out fills no slot in the receiver's numbering.
#[derive(Debug, Clone, Default)]
struct Omitted {
    runs: VecDeque<(u16, u16)>,
    total: u16,
    /// Runs were forgotten, so numbers older than the oldest kept one cannot be placed.
    forgot: bool,
}

impl Omitted {
    fn add(&mut self, n: u16, mask: u16) {
        self.total = self.total.wrapping_add(1) & mask;
        if let Some(last) = self.runs.back_mut()
            && last.0.wrapping_add(last.1) & mask == n
        {
            last.1 += 1;
            return;
        }
        self.runs.push_back((n, 1));
        if self.runs.len() > REMEMBERED_RUNS {
            self.runs.pop_front();
            self.forgot = true;
        }
    }

    /// How many numbers before `n` were left out, or `None` if `n` is older than what is kept.
    fn before(&self, n: u16, mask: u16) -> Option<u16> {
        let mut later = 0u16;
        for &(start, len) in self.runs.iter().rev() {
            if !after(n, start, mask) {
                // The whole run is at or after `n`.
                later = later.wrapping_add(len);
                continue;
            }
            let end = start.wrapping_add(len) & mask;
            if after(end, n, mask) {
                later = later.wrapping_add(end.wrapping_sub(n) & mask);
            }
            return Some(self.total.wrapping_sub(later) & mask);
        }
        (!self.forgot).then(|| self.total.wrapping_sub(later) & mask)
    }
}

/// Rewrites one stream for one receiver.
#[derive(Debug, Clone)]
pub struct Rewriter {
    ssrc: u32,
    payload_type: u8,
    /// Whether the stream is VP8 and its picture numbers must be kept continuous.
    vp8: bool,
    started: bool,
    seq_offset: u16,
    ts_offset: u32,
    picture_id_offset: u16,
    tl0_offset: u8,
    last_seq: u16,
    last_ts: u32,
    last_picture_id: Option<u16>,
    last_tl0: Option<u8>,
    /// The newest sequence number and picture id of the current source seen, sent or not.
    newest_seq: Option<u16>,
    newest_picture_id: Option<u16>,
    /// What of the current source was left out.
    omitted_seqs: Omitted,
    omitted_pictures: Omitted,
    /// The width of the source's picture ids.
    picture_mask: u16,
}

impl Rewriter {
    pub fn new(ssrc: u32, payload_type: u8, vp8: bool) -> Self {
        Self {
            ssrc,
            payload_type,
            vp8,
            started: false,
            seq_offset: 0,
            ts_offset: 0,
            picture_id_offset: 0,
            tl0_offset: 0,
            last_seq: 0,
            last_ts: 0,
            last_picture_id: None,
            last_tl0: None,
            newest_seq: None,
            newest_picture_id: None,
            omitted_seqs: Omitted::default(),
            omitted_pictures: Omitted::default(),
            picture_mask: WIDE_PICTURE_ID,
        }
    }

    pub fn set_payload_type(&mut self, pt: u8) {
        self.payload_type = pt;
    }

    pub fn payload_type(&self) -> u8 {
        self.payload_type
    }

    /// Whether a source has fed the stream yet.
    pub fn started(&self) -> bool {
        self.started
    }

    /// Prepares to continue the stream from a different source, whose next packet is `first`.
    /// Call before rewriting that packet.
    pub fn switch_to(&mut self, first: &Packet) {
        self.forget_source();
        if self.started {
            self.seq_offset = self
                .last_seq
                .wrapping_add(1)
                .wrapping_sub(first.header.sequence_number);
            self.ts_offset = self
                .last_ts
                .wrapping_add(FRAME_GAP)
                .wrapping_sub(first.header.timestamp);
            if self.vp8 {
                if let (Some(last), Some(d)) =
                    (self.last_picture_id, vp8::Descriptor::parse(&first.payload))
                    && let Some((id, _)) = d.picture_id
                {
                    self.picture_id_offset = last.wrapping_add(1).wrapping_sub(id);
                }
                // The new source starts at a keyframe, a base layer frame of its own: it takes the
                // next TL0PICIDX. Were it to take the last one, the first base layer frame after it
                // would read as referring to the old source's last one, and a receiver that has it
                // before the keyframe (a keyframe is large, and the last of it to arrive) would
                // decode it against a picture of the other layer.
                if let (Some(last), Some(d)) =
                    (self.last_tl0, vp8::Descriptor::parse(&first.payload))
                    && let Some(t) = d.tl0_pic_idx
                {
                    self.tl0_offset = last.wrapping_add(1).wrapping_sub(t);
                }
            }
        }
    }

    /// Prepares to continue the stream with `first` after the SFU left the source's packets out for a
    /// while (a voice that was not among the loudest): the receiver's sequence numbers carry on from
    /// the last one it saw, so it counts nothing as lost, while the timestamps keep the real time
    /// that passed, which a receiver treats as a pause in the speech. Call before rewriting `first`.
    pub fn resume_after_gap(&mut self, first: &Packet) {
        if self.started {
            self.seq_offset = self
                .last_seq
                .wrapping_add(1)
                .wrapping_sub(first.header.sequence_number);
        }
    }

    /// What was left out of the source before belongs to it alone.
    fn forget_source(&mut self) {
        self.newest_seq = None;
        self.newest_picture_id = None;
        self.omitted_seqs = Omitted::default();
        self.omitted_pictures = Omitted::default();
    }

    /// Notes the sequence number and picture id of a packet of the source, sent or not; returns
    /// whether each is newer than any seen.
    fn note(&mut self, seq: u16, picture_id: Option<(u16, bool)>) -> (bool, bool) {
        let newer_seq = self.newest_seq.is_none_or(|n| after(seq, n, u16::MAX));
        if newer_seq {
            self.newest_seq = Some(seq);
        }
        let newer_picture = picture_id.is_some_and(|(id, wide)| {
            self.picture_mask = if wide {
                WIDE_PICTURE_ID
            } else {
                SHORT_PICTURE_ID
            };
            let newer = self
                .newest_picture_id
                .is_none_or(|n| after(id, n, self.picture_mask));
            if newer {
                self.newest_picture_id = Some(id);
            }
            newer
        });
        (newer_seq, newer_picture)
    }

    /// The receiver is not sent `packet`, which is of the frame `descriptor` describes: the
    /// sequence numbers after it close up, and, for the first packet of a frame left out, the
    /// picture ids too.
    pub fn leave_out(&mut self, packet: &Packet, descriptor: Option<&vp8::Descriptor>) {
        let picture_id = descriptor.and_then(|d| d.picture_id);
        let (newer_seq, newer_picture) = self.note(packet.header.sequence_number, picture_id);
        if newer_seq {
            self.omitted_seqs
                .add(packet.header.sequence_number, u16::MAX);
        }
        if newer_picture && let Some((id, _)) = picture_id {
            self.omitted_pictures.add(id, self.picture_mask);
        }
    }

    /// Rewrites `packet` for the receiver. Returns false, and leaves it as it was, for a packet
    /// too late to be placed among the numbers the receiver was sent (see `REMEMBERED_RUNS`).
    pub fn rewrite(&mut self, packet: &mut Packet) -> bool {
        let seq = packet.header.sequence_number;
        let descriptor = self
            .vp8
            .then(|| vp8::Descriptor::parse(&packet.payload))
            .flatten();
        let picture_id = descriptor.and_then(|d| d.picture_id);
        let Some(seq_gap) = self.omitted_seqs.before(seq, u16::MAX) else {
            return false;
        };
        let picture_gap = match picture_id {
            Some((id, _)) => match self.omitted_pictures.before(id, self.picture_mask) {
                Some(gap) => gap,
                None => return false,
            },
            None => 0,
        };
        let (newer_seq, _) = self.note(seq, picture_id);
        let h = &mut packet.header;
        h.ssrc = self.ssrc;
        h.payload_type = self.payload_type;
        h.sequence_number = seq.wrapping_add(self.seq_offset).wrapping_sub(seq_gap);
        h.timestamp = h.timestamp.wrapping_add(self.ts_offset);
        if let Some(d) = descriptor {
            let picture_offset = self.picture_id_offset.wrapping_sub(picture_gap);
            if let Some((id, _)) = d.picture_id {
                let out = id.wrapping_add(picture_offset) & self.picture_mask;
                if self
                    .last_picture_id
                    .is_none_or(|last| !self.started || after(out, last, self.picture_mask))
                {
                    self.last_picture_id = Some(out);
                }
            }
            if let Some(t) = d.tl0_pic_idx {
                self.last_tl0 = Some(t.wrapping_add(self.tl0_offset));
            }
            if (picture_offset != 0 || self.tl0_offset != 0)
                && let Some(shifted) = vp8::shift(&packet.payload, picture_offset, self.tl0_offset)
            {
                packet.payload = shifted.into();
            }
        }
        // A retransmission of an older packet does not move the numbering back.
        if !self.started || newer_seq {
            self.last_seq = packet.header.sequence_number;
            self.last_ts = packet.header.timestamp;
        }
        self.started = true;
        true
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn packet(seq: u16, ts: u32, picture_id: u16, tl0: u8) -> Packet {
        let payload = vec![
            0x90,
            0xe0,
            0x80 | (picture_id >> 8) as u8,
            picture_id as u8,
            tl0,
            0x00,
            0x00,
            1,
            2,
        ];
        Packet {
            header: rtc::rtp::header::Header {
                version: 2,
                sequence_number: seq,
                timestamp: ts,
                ssrc: 111,
                payload_type: 96,
                ..Default::default()
            },
            payload: Bytes::from(payload),
        }
    }

    fn numbers(p: &Packet) -> (u16, u32, u16, u8) {
        let d = vp8::Descriptor::parse(&p.payload).unwrap();
        (
            p.header.sequence_number,
            p.header.timestamp,
            d.picture_id.unwrap().0,
            d.tl0_pic_idx.unwrap(),
        )
    }

    #[test]
    fn the_first_source_passes_through_with_the_receivers_ssrc_and_payload_type() {
        let mut r = Rewriter::new(555, 100, true);
        let mut p = packet(10, 9000, 20, 3);
        r.rewrite(&mut p);
        assert_eq!((p.header.ssrc, p.header.payload_type), (555, 100));
        assert_eq!(numbers(&p), (10, 9000, 20, 3));
    }

    #[test]
    fn a_second_source_continues_the_numbers_without_a_gap() {
        let mut r = Rewriter::new(555, 100, true);
        // The first layer, a few frames in.
        for i in 0..5u16 {
            let mut p = packet(1000 + i, 90_000 + u32::from(i) * 3000, 300 + i, 40);
            r.rewrite(&mut p);
        }
        // The second layer has numbers of its own, nowhere near the first's.
        let mut first = packet(7, 5_000_000, 9, 200);
        r.switch_to(&first);
        r.rewrite(&mut first);
        let (seq, ts, pid, tl0) = numbers(&first);
        assert_eq!(seq, 1005, "the sequence number carries on");
        assert_eq!(ts, 90_000 + 4 * 3000 + 3000, "and the clock a frame later");
        assert_eq!(pid, 305, "and the picture id");
        assert_eq!(
            tl0, 41,
            "and TL0PICIDX: the keyframe is the next base layer frame"
        );

        let mut next = packet(8, 5_003_000, 10, 200);
        r.rewrite(&mut next);
        assert_eq!(numbers(&next), (1006, ts + 3000, 306, 41));
    }

    #[test]
    fn gaps_in_the_source_stay_gaps() {
        let mut r = Rewriter::new(1, 96, false);
        let mut a = packet(100, 0, 0, 0);
        r.rewrite(&mut a);
        let mut b = packet(103, 9000, 0, 0);
        r.rewrite(&mut b);
        assert_eq!(
            b.header.sequence_number, 103,
            "the lost packets are still missing, so the receiver can ask for them"
        );
    }

    #[test]
    fn numbers_wrap() {
        let mut r = Rewriter::new(1, 96, true);
        let mut p = packet(65_535, u32::MAX - 1000, 0x7fff, 255);
        r.rewrite(&mut p);
        let mut q = packet(50, 100, 4, 5);
        r.switch_to(&q);
        r.rewrite(&mut q);
        let (seq, ts, pid, tl0) = numbers(&q);
        assert_eq!(seq, 0);
        assert_eq!(ts, (u32::MAX - 1000).wrapping_add(3000));
        assert_eq!(pid, 0);
        assert_eq!(tl0, 0);
    }

    #[test]
    fn audio_and_other_codecs_are_not_touched_beyond_the_header() {
        let mut r = Rewriter::new(9, 111, false);
        let mut p = Packet {
            header: rtc::rtp::header::Header {
                sequence_number: 1,
                ..Default::default()
            },
            payload: Bytes::from_static(&[1, 2, 3]),
        };
        r.rewrite(&mut p);
        assert_eq!(&p.payload[..], &[1, 2, 3]);
        assert_eq!(p.header.ssrc, 9);
    }

    /// Frame `f` of a stream of two packets a frame, from sequence number 100 and picture id 500.
    fn frame(f: u16) -> [Packet; 2] {
        [0, 1].map(|n| packet(100 + 2 * f + n, 9000 + u32::from(f) * 6000, 500 + f, 7))
    }

    #[test]
    fn frames_left_out_close_up_the_numbers_after_them() {
        let mut r = Rewriter::new(1, 96, true);
        let mut sent = Vec::new();
        for f in 0..6u16 {
            for mut p in frame(f) {
                if f % 2 == 1 {
                    let d = vp8::Descriptor::parse(&p.payload);
                    r.leave_out(&p, d.as_ref());
                } else {
                    assert!(r.rewrite(&mut p));
                    sent.push(numbers(&p));
                }
            }
        }
        let seqs: Vec<u16> = sent.iter().map(|n| n.0).collect();
        let pids: Vec<u16> = sent.iter().map(|n| n.2).collect();
        assert_eq!(seqs, vec![100, 101, 102, 103, 104, 105]);
        assert_eq!(pids, vec![500, 500, 501, 501, 502, 502]);
        // A retransmission of a packet that was sent keeps the number it had.
        let mut again = frame(2)[1].clone();
        assert!(r.rewrite(&mut again));
        assert_eq!((numbers(&again).0, numbers(&again).2), (103, 501));
        // The next frame carries on.
        let mut next = frame(6)[0].clone();
        assert!(r.rewrite(&mut next));
        assert_eq!((numbers(&next).0, numbers(&next).2), (106, 503));
    }

    #[test]
    fn a_late_packet_of_a_frame_left_out_takes_no_number() {
        let mut r = Rewriter::new(1, 96, true);
        let [mut a, b] = frame(0);
        assert!(r.rewrite(&mut a));
        // The second packet of frame 0 is late; frame 1 is left out, frame 2 sent.
        for p in frame(1) {
            r.leave_out(&p, vp8::Descriptor::parse(&p.payload).as_ref());
        }
        let [mut c, _] = frame(2);
        assert!(r.rewrite(&mut c));
        assert_eq!(numbers(&c).0, 102, "frame 2 follows frame 0");
        let mut late = b;
        assert!(r.rewrite(&mut late));
        assert_eq!(numbers(&late).0, 101, "the late packet fills its own place");
        // A frame left out whose packet arrives late again stays left out of the numbering.
        let p = frame(1)[0].clone();
        r.leave_out(&p, vp8::Descriptor::parse(&p.payload).as_ref());
        let [_, mut d] = frame(2);
        assert!(r.rewrite(&mut d));
        assert_eq!(numbers(&d).0, 103);
    }

    #[test]
    fn a_packet_older_than_what_is_remembered_is_not_placed() {
        let mut r = Rewriter::new(1, 96, true);
        let mut old = frame(0)[0].clone();
        let keep = old.clone();
        assert!(r.rewrite(&mut old));
        for f in 1..(REMEMBERED_RUNS as u16 * 2 + 4) {
            for mut p in frame(f) {
                if f % 2 == 1 {
                    r.leave_out(&p, vp8::Descriptor::parse(&p.payload).as_ref());
                } else {
                    assert!(r.rewrite(&mut p));
                }
            }
        }
        let mut late = keep;
        assert!(!r.rewrite(&mut late), "too late to know its number");
    }

    #[test]
    fn a_switch_forgets_what_the_old_source_left_out() {
        let mut r = Rewriter::new(1, 96, true);
        for f in 0..4u16 {
            for mut p in frame(f) {
                if f == 1 {
                    r.leave_out(&p, vp8::Descriptor::parse(&p.payload).as_ref());
                } else {
                    r.rewrite(&mut p);
                }
            }
        }
        // Last sent: sequence 105, picture 502.
        let mut first = packet(9000, 1, 40, 3);
        r.switch_to(&first);
        assert!(r.rewrite(&mut first));
        assert_eq!((numbers(&first).0, numbers(&first).2), (106, 503));
        let mut next = packet(9001, 1, 40, 3);
        assert!(r.rewrite(&mut next));
        assert_eq!(numbers(&next).0, 107);
    }

    #[test]
    fn a_stream_resumed_after_a_gap_continues_the_sequence_and_keeps_the_time() {
        let mut r = Rewriter::new(7, 111, false);
        for seq in 100..105u16 {
            let mut p = packet(seq, u32::from(seq) * 960, 0, 0);
            r.rewrite(&mut p);
        }
        // 1,000 packets (20 seconds) were left out.
        let mut first = packet(1105, 1105 * 960, 0, 0);
        r.resume_after_gap(&first);
        r.rewrite(&mut first);
        assert_eq!(first.header.sequence_number, 105, "no sequence gap");
        assert_eq!(
            first.header.timestamp,
            1105 * 960,
            "the pause is still there in time"
        );
        let mut next = packet(1106, 1106 * 960, 0, 0);
        r.rewrite(&mut next);
        assert_eq!(next.header.sequence_number, 106);
    }
}
