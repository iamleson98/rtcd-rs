//! Redundant audio (RFC 2198): a payload that carries the Opus packets before the current one
//! along with it, so a receiver whose downlink lost a packet gets its audio from the next.
//!
//! The SFU builds the payload once per packet of a voice source ([`Encoder`]) and sends it to every
//! receiver that negotiated `red/48000/2`; the others get the plain Opus packet ([`primary`] takes
//! it out of a payload a sender wrapped itself). The packets keep the source's sequence numbers, so
//! reports and NACKs line up as they do without it.

use std::collections::VecDeque;

use bytes::{BufMut, Bytes, BytesMut};

/// The payload type the SFU gives Opus, which both blocks of a payload are.
pub const OPUS_PAYLOAD_TYPE: u8 = 111;
pub const MIME_TYPE_RED: &str = "audio/red";
/// The payload type the SFU gives `red/48000/2`.
pub const RED_PAYLOAD_TYPE: u8 = 63;
/// Both blocks of a payload are Opus: the payload type of the Opus the SFU registers.
pub const RED_FMTP: &str = "111/111";

/// How many earlier packets a payload carries along.
pub const MAX_REDUNDANT: usize = 2;
/// The timestamp offset of a block is 14 bits, in ticks of the 48 kHz clock.
const MAX_OFFSET: u32 = (1 << 14) - 1;
/// The length of a block is 10 bits.
const MAX_BLOCK_LEN: usize = (1 << 10) - 1;
/// Two packets further apart than this (100 ms) have a silence between them, which DTX leaves
/// out: what came before it is not worth repeating.
const MAX_STEP: u32 = 4_800;
/// The most a payload may weigh (bytes): the oldest blocks go first, so that it stays a small
/// packet (about 1,200 bytes with the headers, under the path's MTU).
const MAX_PAYLOAD: usize = 1_000;
/// An Opus packet of this many bytes or fewer carries no speech (a DTX packet is one or two).
const MAX_SILENT_LEN: usize = 2;

/// A block of a payload that is not the last: a packet before the current one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Redundant {
    pub payload_type: u8,
    /// How many ticks before the primary's timestamp the block's packet was stamped.
    pub offset: u32,
    pub data: Bytes,
}

/// A payload taken apart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decoded {
    /// Oldest first.
    pub redundant: Vec<Redundant>,
    pub primary_payload_type: u8,
    pub primary: Bytes,
}

/// Takes a payload apart; `None` if it is cut short or its blocks do not add up.
pub fn decode(payload: &Bytes) -> Option<Decoded> {
    let mut pos = 0;
    // The headers: four bytes for each block that is followed by another, then one for the last.
    let mut headers = Vec::new();
    loop {
        let first = *payload.get(pos)?;
        if first & 0x80 == 0 {
            pos += 1;
            let primary_payload_type = first & 0x7f;
            let mut at = pos;
            let mut redundant = Vec::with_capacity(headers.len());
            for (payload_type, offset, len) in headers {
                let data = payload.get(at..at + len)?;
                redundant.push(Redundant {
                    payload_type,
                    offset,
                    data: payload.slice_ref(data),
                });
                at += len;
            }
            return Some(Decoded {
                redundant,
                primary_payload_type,
                primary: payload.slice(at..),
            });
        }
        let word = u32::from_be_bytes(payload.get(pos..pos + 4)?.try_into().ok()?);
        headers.push((
            ((word >> 24) & 0x7f) as u8,
            (word >> 10) & MAX_OFFSET,
            (word & MAX_BLOCK_LEN as u32) as usize,
        ));
        pos += 4;
    }
}

/// The packet a payload wraps, which is what a receiver that does not take RED is sent; `None` if
/// the payload is not RED of a single Opus packet at most (`opus` is the payload type of Opus).
pub fn primary(payload: &Bytes, opus: u8) -> Option<Bytes> {
    decode(payload)
        .filter(|d| {
            d.primary_payload_type == opus && d.redundant.iter().all(|r| r.payload_type == opus)
        })
        .map(|d| d.primary)
}

/// `primary` as a payload with no redundancy: what is sent under the RED payload type when the
/// packets before it must not be carried (see [`Encoder::encode`]).
pub fn wrap(opus: u8, primary: &Bytes) -> Bytes {
    build(opus, &[], primary)
}

/// The packets before the current one of a source.
#[derive(Debug, Default)]
pub struct Encoder {
    /// Oldest first, each the next one's predecessor in sequence.
    history: VecDeque<Packet>,
}

#[derive(Debug)]
struct Packet {
    seq: u16,
    ts: u32,
    data: Bytes,
}

impl Encoder {
    /// The payload for a packet of the source: `primary`, stamped `ts`, numbered `seq`, behind up to
    /// [`MAX_REDUNDANT`] packets before it (of payload type `opus`). `newest` says the packet
    /// follows the ones before it: a retransmission the sender sent late goes alone and leaves the
    /// history as it is.
    pub fn encode(&mut self, opus: u8, seq: u16, ts: u32, primary: &Bytes, newest: bool) -> Bytes {
        if !newest {
            return build(opus, &[], primary);
        }
        let silent = primary.len() <= MAX_SILENT_LEN;
        // The blocks that follow each other without a gap, up to this packet.
        let mut blocks: Vec<&Packet> = Vec::new();
        let (mut next_seq, mut next_ts) = (seq, ts);
        for p in self.history.iter().rev().take(MAX_REDUNDANT) {
            let step = next_ts.wrapping_sub(p.ts);
            if silent
                || p.seq.wrapping_add(1) != next_seq
                || step == 0
                || step > MAX_STEP
                || ts.wrapping_sub(p.ts) > MAX_OFFSET
                || p.data.len() > MAX_BLOCK_LEN
            {
                break;
            }
            blocks.push(p);
            (next_seq, next_ts) = (p.seq, p.ts);
        }
        blocks.reverse();
        let mut size = 1 + primary.len() + blocks.iter().map(|p| 4 + p.data.len()).sum::<usize>();
        while size > MAX_PAYLOAD && !blocks.is_empty() {
            size -= 4 + blocks.remove(0).data.len();
        }
        let out = build(
            opus,
            &blocks
                .iter()
                .map(|p| (ts.wrapping_sub(p.ts), &p.data))
                .collect::<Vec<_>>(),
            primary,
        );
        if silent {
            self.history.clear();
        } else {
            if self.history.len() == MAX_REDUNDANT {
                self.history.pop_front();
            }
            self.history.push_back(Packet {
                seq,
                ts,
                data: primary.clone(),
            });
        }
        out
    }
}

fn build(opus: u8, blocks: &[(u32, &Bytes)], primary: &Bytes) -> Bytes {
    let len: usize = blocks.iter().map(|(_, d)| 4 + d.len()).sum::<usize>() + 1 + primary.len();
    let mut out = BytesMut::with_capacity(len);
    for (offset, data) in blocks {
        out.put_u32(
            (1 << 31)
                | (u32::from(opus & 0x7f) << 24)
                | ((offset & MAX_OFFSET) << 10)
                | data.len() as u32,
        );
    }
    out.put_u8(opus & 0x7f);
    for (_, data) in blocks {
        out.extend_from_slice(data);
    }
    out.extend_from_slice(primary);
    out.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    const OPUS: u8 = 111;

    fn voice(n: u8, len: usize) -> Bytes {
        Bytes::from(vec![n; len])
    }

    #[test]
    fn a_payload_with_two_earlier_packets_decodes_to_them_and_the_current_one() {
        let mut e = Encoder::default();
        let (a, b, c) = (voice(1, 80), voice(2, 90), voice(3, 70));
        e.encode(OPUS, 10, 1_000, &a, true);
        e.encode(OPUS, 11, 1_960, &b, true);
        let payload = e.encode(OPUS, 12, 2_920, &c, true);
        let d = decode(&payload).unwrap();
        assert_eq!(d.primary, c);
        assert_eq!(d.primary_payload_type, OPUS);
        assert_eq!(
            d.redundant,
            vec![
                Redundant {
                    payload_type: OPUS,
                    offset: 1_920,
                    data: a
                },
                Redundant {
                    payload_type: OPUS,
                    offset: 960,
                    data: b
                },
            ]
        );
        assert_eq!(payload.len(), 4 + 4 + 1 + 80 + 90 + 70);
    }

    #[test]
    fn the_first_packet_has_nothing_before_it() {
        let mut e = Encoder::default();
        let p = voice(1, 60);
        let payload = e.encode(OPUS, 5, 0, &p, true);
        assert_eq!(payload.len(), 1 + 60);
        assert_eq!(primary(&payload, OPUS).unwrap(), p);
        assert!(decode(&payload).unwrap().redundant.is_empty());
    }

    #[test]
    fn a_sequence_gap_leaves_out_what_came_before_it() {
        let mut e = Encoder::default();
        e.encode(OPUS, 10, 0, &voice(1, 50), true);
        e.encode(OPUS, 11, 960, &voice(2, 50), true);
        // 12 never arrived.
        let d = decode(&e.encode(OPUS, 13, 2_880, &voice(3, 50), true)).unwrap();
        assert!(d.redundant.is_empty(), "{d:?}");
        // 14 follows 13, whose own history is 13 alone.
        let d = decode(&e.encode(OPUS, 14, 3_840, &voice(4, 50), true)).unwrap();
        assert_eq!(d.redundant.len(), 1);
        assert_eq!(d.redundant[0].data, voice(3, 50));
    }

    #[test]
    fn the_sequence_numbers_wrap() {
        let mut e = Encoder::default();
        e.encode(OPUS, 65_535, u32::MAX - 100, &voice(1, 50), true);
        let d = decode(&e.encode(OPUS, 0, 859, &voice(2, 50), true)).unwrap();
        assert_eq!(d.redundant.len(), 1);
        assert_eq!(d.redundant[0].offset, 960);
    }

    #[test]
    fn a_silence_between_packets_leaves_out_what_came_before_it() {
        let mut e = Encoder::default();
        e.encode(OPUS, 1, 0, &voice(1, 50), true);
        // DTX: the next packet is 400 ms on, with the next sequence number.
        let d = decode(&e.encode(OPUS, 2, 19_200, &voice(2, 50), true)).unwrap();
        assert!(d.redundant.is_empty());
    }

    #[test]
    fn a_silent_packet_goes_alone_and_nothing_is_repeated_across_it() {
        let mut e = Encoder::default();
        e.encode(OPUS, 1, 0, &voice(1, 50), true);
        let d = decode(&e.encode(OPUS, 2, 960, &voice(0, 1), true)).unwrap();
        assert!(d.redundant.is_empty());
        let d = decode(&e.encode(OPUS, 3, 1_920, &voice(3, 50), true)).unwrap();
        assert!(d.redundant.is_empty());
    }

    #[test]
    fn the_limits_of_the_header_hold() {
        let mut e = Encoder::default();
        // A block of 1,024 bytes does not fit the 10 bits of its length.
        e.encode(OPUS, 1, 0, &voice(1, 1_024), true);
        let d = decode(&e.encode(OPUS, 2, 960, &voice(2, 50), true)).unwrap();
        assert!(d.redundant.is_empty());
        // One of 1,023 does.
        let mut e = Encoder::default();
        e.encode(OPUS, 1, 0, &voice(1, 1_023), true);
        let d = decode(&e.encode(OPUS, 2, 960, &voice(2, 50), true)).unwrap();
        assert_eq!(d.redundant[0].data.len(), 1_023);
        // Two packets of 4,000 ticks each are 8,000 apart: inside the 14 bits (16,383) and the step.
        let mut e = Encoder::default();
        e.encode(OPUS, 1, 0, &voice(1, 50), true);
        e.encode(OPUS, 2, 4_000, &voice(2, 50), true);
        let d = decode(&e.encode(OPUS, 3, 8_000, &voice(3, 50), true)).unwrap();
        assert_eq!(d.redundant.len(), 2);
        assert_eq!(d.redundant[0].offset, 8_000);
    }

    #[test]
    fn a_retransmission_goes_alone_and_leaves_the_history_alone() {
        let mut e = Encoder::default();
        e.encode(OPUS, 1, 0, &voice(1, 50), true);
        e.encode(OPUS, 2, 960, &voice(2, 50), true);
        let late = decode(&e.encode(OPUS, 1, 0, &voice(1, 50), false)).unwrap();
        assert!(late.redundant.is_empty());
        let d = decode(&e.encode(OPUS, 3, 1_920, &voice(3, 50), true)).unwrap();
        assert_eq!(d.redundant.len(), 2);
    }

    #[test]
    fn a_cut_short_payload_does_not_decode() {
        let mut e = Encoder::default();
        e.encode(OPUS, 1, 0, &voice(1, 50), true);
        let payload = e.encode(OPUS, 2, 960, &voice(2, 50), true);
        assert!(decode(&payload.slice(..30)).is_none());
        assert!(decode(&Bytes::new()).is_none());
        assert!(decode(&payload.slice(..3)).is_none());
    }

    #[test]
    fn the_payload_stays_under_the_size_cap_by_dropping_the_oldest_blocks() {
        let mut e = Encoder::default();
        e.encode(OPUS, 1, 0, &voice(1, 400), true);
        e.encode(OPUS, 2, 960, &voice(2, 400), true);
        // 1 + 300 + 4 + 400 + 4 + 400 would be 1,109 bytes: the oldest block goes.
        let payload = e.encode(OPUS, 3, 1_920, &voice(3, 300), true);
        assert_eq!(payload.len(), 1 + 300 + 4 + 400);
        let d = decode(&payload).unwrap();
        assert_eq!(d.redundant.len(), 1);
        assert_eq!(d.redundant[0].data, voice(2, 400));
        // A primary that is too big alone still goes, without blocks.
        let big = e.encode(OPUS, 4, 2_880, &voice(4, 1_100), true);
        assert!(decode(&big).unwrap().redundant.is_empty());
    }

    #[test]
    fn a_wrapped_packet_has_no_blocks_and_only_an_opus_one_is_taken_out() {
        let p = voice(7, 60);
        let payload = wrap(OPUS, &p);
        assert_eq!(payload.len(), 61);
        assert_eq!(primary(&payload, OPUS).unwrap(), p);
        assert!(
            primary(&payload, 109).is_none(),
            "its packet is not of that codec"
        );
        assert!(primary(&Bytes::from_static(&[0x80, 1]), OPUS).is_none());
    }
}
