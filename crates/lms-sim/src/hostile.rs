//! What a hostile peer does once it has joined and negotiated like any browser: it keeps a real
//! peer connection (DTLS, SRTP, data channel) and sends what a browser never would. `class.rs`
//! drives it (`Ctl::Hostile`), this module says what is sent in each 20 ms step.
//!
//! | behaviour | what the peer sends |
//! |---|---|
//! | `rtp-jump` | its camera and screen frames with sequence numbers and timestamps that jump forward and backwards |
//! | `ssrc-change` | its screen's two layers on each other's SSRC, with the other's rid, swapped back and forth |
//! | `pli-flood` | hundreds to thousands of PLI and FIR per second, at known and random SSRCs |
//! | `nack-flood` | NACKs of whole 17-packet ranges at random sequence numbers |
//! | `bogus-rtcp` | transport-wide feedback that contradicts itself, REMB with NaN, zero, infinite and huge rates, receiver reports with absurd statistics |
//! | `rtp-size` | empty RTP packets and packets as large as the MTU allows, on its camera |
//! | `rtcp-garbage` | RTCP compounds with wrong lengths, truncated, of unknown types, random bytes |
//! | `dc-flood` | data channel frames of every type with random contents, as fast as SCTP lets it |
//! | `dc-oversize` | data channel messages of tens of kilobytes, an SDP frame that inflates past rtcd's cap and one just under it that is not a description |
//!
//! Not expressible through `rtc`'s API: an RTP packet with an SSRC the SDP never named (a
//! sender writes only its own tracks' SSRCs), one with an unnegotiated payload type or header
//! extension id (refused before it is sent), or raw bytes under SRTP (so garbage RTP is limited to
//! valid packets with extreme headers and sizes; garbage that is not SRTP at all is `junk` in
//! `abuse.rs`). The RTCP types `rtc` can marshal are sent as typed packets or as raw bytes.

use std::sync::OnceLock;
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use rtc::peer_connection::RTCPeerConnection;
use rtc::rtcp;
use rtc::rtcp::payload_feedbacks::full_intra_request::{FirEntry, FullIntraRequest};
use rtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication;
use rtc::rtcp::payload_feedbacks::receiver_estimated_maximum_bitrate::ReceiverEstimatedMaximumBitrate;
use rtc::rtcp::raw_packet::RawPacket;
use rtc::rtcp::receiver_report::ReceiverReport;
use rtc::rtcp::reception_report::ReceptionReport;
use rtc::rtcp::transport_feedbacks::transport_layer_cc::{
    PacketStatusChunk, RunLengthChunk, StatusChunkTypeTcc, SymbolTypeTcc, TransportLayerCc,
};
use rtc::rtcp::transport_feedbacks::transport_layer_nack::{NackPair, TransportLayerNack};
use rtc::rtp_transceiver::{RTCRtpReceiverId, RTCRtpSenderId};
use rtcd_proto::dc::DcMessage;

use crate::class::Video;

/// A small seeded generator (xorshift64*), so that what a run does is a function of its seed.
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        // splitmix64 once, so that near seeds (0, 1, 2) are far apart and never zero.
        let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        Self((z ^ (z >> 31)) | 1)
    }

    pub fn word(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// A number below `n` (0 when `n` is 0).
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.word() % n }
    }

    /// A number in `lo..=hi`.
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.below(hi.saturating_sub(lo) + 1)
    }

    /// `true` with probability `p`.
    pub fn chance(&mut self, p: f64) -> bool {
        (self.word() >> 11) as f64 / (1u64 << 53) as f64 <= p
    }

    pub fn bytes(&mut self, n: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(n + 8);
        while out.len() < n {
            out.extend_from_slice(&self.word().to_le_bytes());
        }
        out.truncate(n);
        out
    }

    /// Between `lo` and `hi` random bytes.
    pub fn noise(&mut self, lo: u64, hi: u64) -> Vec<u8> {
        let n = self.range(lo, hi) as usize;
        self.bytes(n)
    }

    /// One of `items`.
    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len() as u64) as usize]
    }
}

/// What a hostile peer can be told to send.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Hostile {
    RtpJump,
    SsrcChange,
    PliFlood,
    NackFlood,
    BogusRtcp,
    RtpSize,
    RtcpGarbage,
    DcFlood,
    DcOversize,
}

impl Hostile {
    pub const ALL: [Hostile; 9] = [
        Self::RtpJump,
        Self::SsrcChange,
        Self::PliFlood,
        Self::NackFlood,
        Self::BogusRtcp,
        Self::RtpSize,
        Self::RtcpGarbage,
        Self::DcFlood,
        Self::DcOversize,
    ];

    /// The name of the attack class.
    pub fn name(self) -> &'static str {
        match self {
            Self::RtpJump => "rtp-jump",
            Self::SsrcChange => "ssrc-change",
            Self::PliFlood => "pli-flood",
            Self::NackFlood => "nack-flood",
            Self::BogusRtcp => "bogus-rtcp",
            Self::RtpSize => "rtp-size",
            Self::RtcpGarbage => "rtcp-garbage",
            Self::DcFlood => "dc-flood",
            Self::DcOversize => "dc-oversize",
        }
    }
}

/// What a step may touch.
pub(crate) struct Hot<'a> {
    pub pc: &'a mut RTCPeerConnection,
    pub dc: Option<usize>,
    pub mic: RTCRtpSenderId,
    pub camera: Option<&'a mut Video>,
    pub screen: Option<&'a mut Video>,
    /// The tracks the SFU forwards to the peer and their SSRCs.
    pub receivers: Vec<(RTCRtpReceiverId, u32)>,
    pub now: Instant,
}

impl Hot<'_> {
    /// Sends RTCP on the first receiver, or on the microphone's sender when nothing arrives yet.
    fn rtcp(&mut self, packets: Vec<Box<dyn rtcp::Packet>>) -> bool {
        if let Some((id, _)) = self.receivers.first().copied()
            && let Some(mut r) = self.pc.rtp_receiver(id)
        {
            return r.write_rtcp(self.now, packets).is_ok();
        }
        self.pc
            .rtp_sender(self.mic)
            .is_some_and(|mut s| s.write_rtcp(self.now, packets).is_ok())
    }

    /// An SSRC to aim at: a track the peer receives, or a random one.
    fn target(&self, rng: &mut Rng) -> u32 {
        if !self.receivers.is_empty() && rng.chance(0.5) {
            self.receivers[rng.below(self.receivers.len() as u64) as usize].1
        } else {
            rng.word() as u32
        }
    }

    fn dc_send(&mut self, frame: &[u8]) -> bool {
        let now = self.now;
        self.dc
            .and_then(|id| self.pc.data_channel(id))
            .is_some_and(|mut d| d.send(now, BytesMut::from(frame)).is_ok())
    }
}

/// One 20 ms step of `kind` at `level` (1 to 5); how many packets, frames or messages were sent.
pub(crate) fn step(kind: Hostile, h: &mut Hot<'_>, rng: &mut Rng, level: u8) -> u64 {
    let base = 1u64 << (level.clamp(1, 5) - 1);
    match kind {
        Hostile::RtpJump => jump(h, rng, level),
        Hostile::SsrcChange => swap_ssrcs(h, rng, level),
        Hostile::PliFlood => (0..base * 4)
            .filter(|_| {
                let (media, sender) = (h.target(rng), rng.word() as u32);
                let p: Box<dyn rtcp::Packet> = if rng.chance(0.2) {
                    Box::new(FullIntraRequest {
                        sender_ssrc: sender,
                        media_ssrc: media,
                        fir: vec![FirEntry {
                            ssrc: media,
                            sequence_number: rng.word() as u8,
                        }],
                    })
                } else {
                    Box::new(PictureLossIndication {
                        sender_ssrc: sender,
                        media_ssrc: media,
                    })
                };
                h.rtcp(vec![p])
            })
            .count() as u64,
        Hostile::NackFlood => (0..base * 2)
            .filter(|_| {
                let media = h.target(rng);
                let nacks = (0..rng.range(1, 16))
                    .map(|_| NackPair {
                        packet_id: rng.word() as u16,
                        lost_packets: 0xFFFF,
                    })
                    .collect();
                h.rtcp(vec![Box::new(TransportLayerNack {
                    sender_ssrc: rng.word() as u32,
                    media_ssrc: media,
                    nacks,
                })])
            })
            .count() as u64,
        Hostile::BogusRtcp => (0..base)
            .filter(|_| {
                let p = bogus_feedback(h, rng);
                h.rtcp(vec![p])
            })
            .count() as u64,
        Hostile::RtpSize => odd_sizes(h, rng, base),
        Hostile::RtcpGarbage => (0..base * 2)
            .filter(|_| {
                let raw = garbage_rtcp(rng);
                h.rtcp(vec![Box::new(RawPacket(Bytes::from(raw)))])
            })
            .count() as u64,
        Hostile::DcFlood => (0..base * 8)
            .filter(|_| {
                let frame = junk_frame(rng);
                h.dc_send(&frame)
            })
            .count() as u64,
        Hostile::DcOversize => {
            if !rng.chance(0.1 * f64::from(level)) {
                return 0;
            }
            let frame = match rng.below(3) {
                0 => rng.noise(16 * 1024, 60 * 1024),
                1 => bomb().to_vec(),
                _ => not_a_description().to_vec(),
            };
            u64::from(h.dc_send(&frame))
        }
    }
}

/// Sequence numbers and timestamps that jump, forward, back, or both, now and then.
fn jump(h: &mut Hot<'_>, rng: &mut Rng, level: u8) -> u64 {
    let mut n = 0;
    for v in [h.camera.as_deref_mut(), h.screen.as_deref_mut()]
        .into_iter()
        .flatten()
    {
        if !rng.chance(0.01 * f64::from(level)) {
            continue;
        }
        let l = rng.below(v.ssrcs.len() as u64) as usize;
        match rng.below(4) {
            0 => v.seq[l] = v.seq[l].wrapping_add(rng.range(2, 30_000) as u16),
            1 => v.seq[l] = v.seq[l].wrapping_sub(rng.range(1, 3_000) as u16),
            2 => v.ts[l] = v.ts[l].wrapping_add(rng.word() as u32),
            _ => {
                v.seq[l] = rng.word() as u16;
                v.ts[l] = v.ts[l].wrapping_sub(rng.range(1, 900_000) as u32);
            }
        }
        n += 1;
    }
    n
}

/// A screen's layers on each other's SSRCs (each keeps its rid), back and forth.
fn swap_ssrcs(h: &mut Hot<'_>, rng: &mut Rng, level: u8) -> u64 {
    let mut n = 0;
    for v in [h.camera.as_deref_mut(), h.screen.as_deref_mut()]
        .into_iter()
        .flatten()
    {
        if v.ssrcs.len() > 1 && rng.chance(0.02 * f64::from(level)) {
            v.ssrcs.swap(0, 1);
            n += 1;
        }
    }
    n
}

/// Feedback that makes no sense: transport-wide, a bitrate cap, or a receiver report.
fn bogus_feedback(h: &Hot<'_>, rng: &mut Rng) -> Box<dyn rtcp::Packet> {
    match rng.below(3) {
        0 => Box::new(TransportLayerCc {
            sender_ssrc: rng.word() as u32,
            media_ssrc: h.target(rng),
            base_sequence_number: rng.word() as u16,
            packet_status_count: rng.word() as u16,
            reference_time: rng.word() as u32 & 0x00FF_FFFF,
            fb_pkt_count: rng.word() as u8,
            // Says "received" for tens of thousands of packets and carries no deltas.
            packet_chunks: vec![PacketStatusChunk::RunLengthChunk(RunLengthChunk {
                type_tcc: StatusChunkTypeTcc::RunLengthChunk,
                packet_status_symbol: SymbolTypeTcc::PacketReceivedSmallDelta,
                run_length: rng.range(1, 8_000) as u16,
            })],
            recv_deltas: vec![],
        }),
        1 => Box::new(ReceiverEstimatedMaximumBitrate {
            sender_ssrc: rng.word() as u32,
            bitrate: *rng.pick(&[0.0, f32::NAN, f32::INFINITY, 1.0, 3.0e38, 5.0e9, 1.0e4]),
            ssrcs: (0..rng.range(0, 40)).map(|_| h.target(rng)).collect(),
        }),
        _ => Box::new(ReceiverReport {
            ssrc: rng.word() as u32,
            reports: (0..rng.range(0, 31))
                .map(|_| ReceptionReport {
                    ssrc: h.target(rng),
                    fraction_lost: rng.word() as u8,
                    total_lost: rng.word() as u32 & 0x00FF_FFFF,
                    last_sequence_number: rng.word() as u32,
                    jitter: rng.word() as u32,
                    last_sender_report: rng.word() as u32,
                    delay: rng.word() as u32,
                })
                .collect(),
            profile_extensions: Bytes::from(rng.noise(0, 3).repeat(4)),
        }),
    }
}

/// An RTCP compound that is wrong in one way or another.
fn garbage_rtcp(rng: &mut Rng) -> Vec<u8> {
    let kinds = [200u8, 201, 202, 203, 204, 205, 206, 207, 208, 255, 100];
    match rng.below(5) {
        // Pure noise, 4 to 1200 bytes.
        0 => rng.noise(4, 1200),
        // A valid-looking header that claims far more than follows.
        1 => {
            let mut p = vec![
                0x80 | (rng.word() as u8 & 0x1F),
                *rng.pick(&kinds),
                0xFF,
                0xFF,
            ];
            p.extend(rng.noise(0, 63));
            p
        }
        // A header of length 0, repeated: a compound of empty packets.
        2 => (0..rng.range(1, 30))
            .flat_map(|_| [0x80, *rng.pick(&kinds), 0, 0])
            .collect(),
        // Feedback with a body too short for its type, as many times as fits.
        3 => (0..rng.range(1, 20))
            .flat_map(|_| [0x81, *rng.pick(&[205u8, 206]), 0, 1, 1, 2, 3, 4])
            .collect(),
        // A header and a body with the padding bit set and a padding count that is wrong.
        _ => {
            let mut p = vec![0xA0, *rng.pick(&kinds), 0, 2];
            p.extend(rng.bytes(8));
            p.push(255);
            p
        }
    }
}

/// Empty RTP packets and the largest that fit a datagram, on the camera (or the screen).
fn odd_sizes(h: &mut Hot<'_>, rng: &mut Rng, count: u64) -> u64 {
    let Some(v) = h.camera.as_deref_mut().or(h.screen.as_deref_mut()) else {
        return 0;
    };
    let (pt, mid_ext) = {
        let Some(mut s) = h.pc.rtp_sender(v.sender) else {
            return 0;
        };
        let p = s.get_parameters();
        (
            p.rtp_parameters
                .codecs
                .iter()
                .find(|c| c.rtp_codec.mime_type.eq_ignore_ascii_case("video/vp8"))
                .map_or(96, |c| c.payload_type),
            p.rtp_parameters
                .header_extensions
                .iter()
                .find(|e| e.uri == "urn:ietf:params:rtp-hdrext:sdes:mid")
                .map(|e| e.id as u8),
        )
    };
    let Some(mid) = v.mid.clone() else { return 0 };
    let mut sent = 0;
    for _ in 0..count {
        let size = *rng.pick(&[0usize, 1, 2, 3, 1100, 1380, 1400]);
        let mut header = rtc::rtp::header::Header {
            version: 2,
            marker: rng.chance(0.5),
            payload_type: pt,
            sequence_number: v.seq[0],
            timestamp: v.ts[0],
            ssrc: v.ssrcs[0],
            ..Default::default()
        };
        if let Some(id) = mid_ext {
            let _ = header.set_extension(id, Bytes::from(mid.clone().into_bytes()));
        }
        v.seq[0] = v.seq[0].wrapping_add(1);
        let packet = rtc::rtp::packet::Packet {
            header,
            payload: Bytes::from(rng.bytes(size)),
        };
        if let Some(mut s) = h.pc.rtp_sender(v.sender)
            && s.write_rtp(h.now, packet).is_ok()
        {
            sent += 1;
        }
    }
    sent
}

/// A data channel frame: a valid type with a random payload, or random bytes.
fn junk_frame(rng: &mut Rng) -> Vec<u8> {
    let float = [0.0, f64::NAN, f64::INFINITY, -1.0, 1.0e300];
    match rng.below(9) {
        0 => DcMessage::Ping.encode(),
        1 => DcMessage::Pong.encode(),
        2 => DcMessage::LossRate(*rng.pick(&float)).encode(),
        3 => DcMessage::RoundTripTime(*rng.pick(&float)).encode(),
        4 => DcMessage::Jitter(*rng.pick(&float)).encode(),
        5 => match rng.below(3) {
            0 => DcMessage::Lock(None).encode(),
            1 => DcMessage::Lock(Some(rng.chance(0.5))).encode(),
            _ => DcMessage::Unlock.encode(),
        },
        6 => DcMessage::SharpScreen(rng.chance(0.5)).encode(),
        // A description that is noise.
        7 => DcMessage::Sdp(rng.noise(1, 2000)).encode(),
        // A frame of an unknown type, or no type.
        _ => {
            let mut f = rng.noise(0, 300);
            if let Some(t) = f.first_mut()
                && rng.chance(0.5)
            {
                *t = 0xcc;
            }
            f
        }
    }
}

/// A description that inflates past rtcd's cap from a few kilobytes.
fn bomb() -> &'static [u8] {
    static BOMB: OnceLock<Vec<u8>> = OnceLock::new();
    BOMB.get_or_init(|| {
        DcMessage::Sdp(vec![
            b' ';
            (rtcd_proto::dc::MAX_SDP_BYTES + (1 << 20)) as usize
        ])
        .encode()
    })
}

/// A frame whose payload is just under the cap and is no description.
fn not_a_description() -> &'static [u8] {
    static BIG: OnceLock<Vec<u8>> = OnceLock::new();
    BIG.get_or_init(|| {
        let mut json = br#"{"type":"offer","sdp":""#.to_vec();
        json.resize((rtcd_proto::dc::MAX_SDP_BYTES - 64) as usize, b'a');
        json.extend_from_slice(br#""}"#);
        DcMessage::Sdp(json).encode()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_same_seed_is_the_same_numbers() {
        let (mut a, mut b, mut c) = (Rng::new(7), Rng::new(7), Rng::new(8));
        let xs: Vec<u64> = (0..8).map(|_| a.word()).collect();
        assert_eq!(xs, (0..8).map(|_| b.word()).collect::<Vec<_>>());
        assert_ne!(xs, (0..8).map(|_| c.word()).collect::<Vec<_>>());
        assert!((0..100).all(|_| a.range(3, 5) >= 3));
        assert_eq!(a.bytes(13).len(), 13);
    }

    #[test]
    fn the_frames_are_what_they_say() {
        assert!(bomb().len() < 64 * 1024);
        assert!(not_a_description().len() > 1024);
        assert!(DcMessage::decode(not_a_description()).is_ok());
        assert!(DcMessage::decode(bomb()).is_err());
        let mut rng = Rng::new(1);
        for _ in 0..200 {
            let _ = DcMessage::decode(&junk_frame(&mut rng));
            let _ = garbage_rtcp(&mut rng);
        }
    }
}
