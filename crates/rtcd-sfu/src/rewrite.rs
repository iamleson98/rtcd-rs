//! What a receiver's copy of a stream needs changed: its own SSRC and payload type, and sequence
//! numbers, timestamps and VP8 picture numbers that carry on without a gap when the stream is
//! moved from one simulcast layer of the sender to another. To the receiver it is one continuous
//! stream, so switching layers costs no renegotiation.

use rtc::rtp::Packet;

use crate::vp8;

/// Timestamp step assumed between the last frame of the old layer and the first of the new one,
/// 30 frames per second on the 90 kHz video clock.
const FRAME_GAP: u32 = 3000;

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
                if let (Some(last), Some(d)) =
                    (self.last_tl0, vp8::Descriptor::parse(&first.payload))
                    && let Some(t) = d.tl0_pic_idx
                {
                    self.tl0_offset = last.wrapping_sub(t);
                }
            }
        }
    }

    /// Rewrites `packet` for the receiver.
    pub fn rewrite(&mut self, packet: &mut Packet) {
        let h = &mut packet.header;
        h.ssrc = self.ssrc;
        h.payload_type = self.payload_type;
        h.sequence_number = h.sequence_number.wrapping_add(self.seq_offset);
        h.timestamp = h.timestamp.wrapping_add(self.ts_offset);
        if self.vp8
            && let Some(d) = vp8::Descriptor::parse(&packet.payload)
        {
            if let Some((id, _)) = d.picture_id {
                self.last_picture_id = Some(id.wrapping_add(self.picture_id_offset));
            }
            if let Some(t) = d.tl0_pic_idx {
                self.last_tl0 = Some(t.wrapping_add(self.tl0_offset));
            }
            if (self.picture_id_offset != 0 || self.tl0_offset != 0)
                && let Some(shifted) =
                    vp8::shift(&packet.payload, self.picture_id_offset, self.tl0_offset)
            {
                packet.payload = shifted.into();
            }
        }
        self.started = true;
        self.last_seq = packet.header.sequence_number;
        self.last_ts = packet.header.timestamp;
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
        assert_eq!(tl0, 40, "and TL0PICIDX stays where it was");

        let mut next = packet(8, 5_003_000, 10, 200);
        r.rewrite(&mut next);
        assert_eq!(numbers(&next), (1006, ts + 3000, 306, 40));
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
        assert_eq!(tl0, 255);
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
}
