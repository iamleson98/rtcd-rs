//! A receiver's copy of a stream through the rewriter: frames sent and left out, sequence numbers and
//! picture ids that skip, wrap and jump, retransmissions, late packets, a switch of source and a
//! resume after a gap. For input that arrives in order, what is sent carries on without a gap except
//! where the input itself had one; what is left out closes up; what is remembered stays bounded.
#![no_main]

use arbitrary::Arbitrary;
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use rtc::rtp::Packet;
use rtc::rtp::header::Header;
use rtcd_sfu::rewrite::Rewriter;
use rtcd_sfu::vp8::Descriptor;

/// The most runs left out that a rewriter remembers (`REMEMBERED_RUNS`).
const REMEMBERED_RUNS: usize = 64;
/// Retransmissions come from this many of the newest packets.
const HISTORY: usize = 32;
/// Packets of the input that were lost on the way in, kept to arrive late.
const LOST_KEPT: usize = 8;
/// The furthest the source moves on between packets and stays ahead (half the space).
const MAX_SEQ_GAP: u32 = 30_000;

#[derive(Arbitrary, Debug, Clone, Copy)]
enum Mode {
    Audio,
    Vp8Wide,
    Vp8Short,
}

#[derive(Arbitrary, Debug)]
enum Op {
    /// A frame of the source: `packets` of it, `lost` numbers missing before it, `skipped`
    /// pictures missing before it.
    Frame {
        send: bool,
        packets: u8,
        lost: u8,
        skipped: u8,
    },
    /// The sender stopped for a while: the numbers move on.
    Jump { seq: u16, pictures: u8 },
    /// Another source continues the stream, from numbers of its own.
    Switch { seq: u16, picture: u16, ts: u32 },
    /// Audio that was left out for a while, now sent again.
    Resume { skip: u16 },
    /// A packet seen before arrives again.
    Replay { back: u8 },
    /// A packet that was lost on the way in arrives late.
    LateLost { which: u8 },
}

#[derive(Arbitrary, Debug)]
struct Input {
    mode: Mode,
    seq: u16,
    picture: u16,
    ts: u32,
    ops: Vec<Op>,
}

#[derive(Clone, Copy, Default)]
struct Moved {
    seq: u64,
    picture: u64,
}

struct Seen {
    packet: Packet,
    at: Moved,
    /// The sequence number and picture id it was sent with; `None`: left out.
    sent: Option<(u16, Option<u16>)>,
}

struct Run {
    mode: Mode,
    rewriter: Rewriter,
    /// The next numbers of the source.
    seq: u16,
    picture: u16,
    ts: u32,
    /// The last packet sent: its numbers in the source and in the receiver's copy.
    last_in: Option<(u16, Option<u16>)>,
    last_out: Option<(u16, Option<u16>)>,
    /// What was left out since it: packets, and frames.
    left_out_seqs: u16,
    left_out_pictures: u16,
    history: Vec<Seen>,
    /// With whether the frame it belongs to is sent.
    lost: Vec<(Packet, bool, Moved)>,
    mask: u16,
    /// How far the source has moved on in all, without wrapping: what is older than half the space
    /// cannot be told from what is newer.
    moved: Moved,
    /// The newest picture id a late packet was placed with, newer than the last one sent in order.
    late_picture: Option<u16>,
    /// Numbers and pictures the source moved on since the last packet it delivered: numbers further
    /// than half their space ahead of the newest seen read as behind it.
    seq_gap: u32,
    picture_gap: u32,
}

fn numbers(mode: Mode, p: &Packet) -> (u16, Option<u16>) {
    let picture = match mode {
        Mode::Audio => None,
        _ => Descriptor::parse(&p.payload)
            .and_then(|d| d.picture_id)
            .map(|(id, _)| id),
    };
    (p.header.sequence_number, picture)
}

impl Run {
    fn packet(&self) -> Packet {
        let payload: Vec<u8> = match self.mode {
            Mode::Audio => vec![1, 2, 3],
            Mode::Vp8Wide => vec![
                0x90,
                0xe0,
                0x80 | (self.picture >> 8) as u8,
                self.picture as u8,
                7,
                0,
                0,
                1,
                2,
            ],
            Mode::Vp8Short => vec![0x90, 0xe0, (self.picture & 0x7f) as u8, 7, 0, 0, 1, 2],
        };
        Packet {
            header: Header {
                version: 2,
                sequence_number: self.seq,
                timestamp: self.ts,
                ssrc: 111,
                payload_type: 96,
                ..Default::default()
            },
            payload: Bytes::from(payload),
        }
    }

    /// The stream continues from the source's next packet, as if the one before it had been sent.
    fn restart_model(&mut self) {
        let picture =
            (!matches!(self.mode, Mode::Audio)).then(|| self.picture.wrapping_sub(1) & self.mask);
        if let (Some((seq, _)), Some(late)) = (self.last_out, self.late_picture.take()) {
            self.last_out = Some((seq, Some(late)));
        }
        self.last_in = self.last_out.map(|_| (self.seq.wrapping_sub(1), picture));
        self.left_out_seqs = 0;
        self.left_out_pictures = 0;
        self.history.clear();
        self.lost.clear();
    }

    fn remember(&mut self, packet: Packet, sent: Option<(u16, Option<u16>)>) {
        if self.history.len() == HISTORY {
            self.history.remove(0);
        }
        let at = self.moved;
        self.history.push(Seen { packet, at, sent });
    }

    /// The source moves on by `seq` numbers and `pictures` pictures, as far as they stay ahead.
    fn advance(&mut self, seq: u16, pictures: u16) {
        let seq = u32::from(seq).min(MAX_SEQ_GAP - self.seq_gap);
        let room = u32::from(self.mask / 2).saturating_sub(2);
        let pictures = u32::from(pictures).min(room.saturating_sub(self.picture_gap));
        self.seq_gap += seq;
        self.picture_gap += pictures;
        self.seq = self.seq.wrapping_add(seq as u16);
        self.moved.seq += u64::from(seq);
        self.picture = self.picture.wrapping_add(pictures as u16) & self.mask;
        self.moved.picture += u64::from(pictures);
    }

    /// Whether the source is as far ahead of what was sent as it can be and stay ahead.
    fn too_far_ahead(&self) -> bool {
        self.seq_gap + 3 >= MAX_SEQ_GAP
            || self.picture_gap + 2 >= u32::from(self.mask / 2).saturating_sub(2)
    }

    /// A packet of the source, in order, sent or left out.
    fn deliver(&mut self, mut p: Packet, send: bool, first_of_frame: bool) {
        let input = numbers(self.mode, &p);
        if !send {
            let d = Descriptor::parse(&p.payload);
            self.rewriter.leave_out(&p, d.as_ref());
            self.seq_gap += 1;
            self.picture_gap += u32::from(first_of_frame);
            self.left_out_seqs = self.left_out_seqs.wrapping_add(1);
            if first_of_frame {
                self.left_out_pictures = self.left_out_pictures.wrapping_add(1);
            }
            self.remember(p, None);
            return;
        }
        self.seq_gap = 0;
        self.picture_gap = 0;
        let original = p.clone();
        assert!(
            self.rewriter.rewrite(&mut p),
            "a packet in order is sent: {input:?}"
        );
        let out = numbers(self.mode, &p);
        if let (Some((in_seq, in_pic)), Some((out_seq, out_pic))) = (self.last_in, self.last_out) {
            let expected = input
                .0
                .wrapping_sub(in_seq)
                .wrapping_sub(self.left_out_seqs);
            assert_ne!(expected, 0, "a number is never sent twice");
            assert_eq!(
                out.0.wrapping_sub(out_seq),
                expected,
                "sent numbers close up where packets were left out, and only there: in {input:?} out {out:?} after in {:?} out {:?}",
                self.last_in,
                self.last_out
            );
            if let (Some(i), Some(o), Some(li), Some(lo)) = (input.1, out.1, in_pic, out_pic) {
                if !first_of_frame {
                    assert_eq!(o, lo, "a frame keeps one picture id");
                } else {
                    let step = i.wrapping_sub(li) & self.mask;
                    let expected = step.wrapping_sub(self.left_out_pictures) & self.mask;
                    assert_ne!(expected, 0, "a picture id is never sent for two frames");
                    assert_eq!(
                        o.wrapping_sub(lo) & self.mask,
                        expected,
                        "sent pictures close up where frames were left out: in {input:?} out {out:?}"
                    );
                }
            }
        }
        self.last_in = Some(input);
        self.last_out = Some(out);
        self.late_picture = None;
        self.left_out_seqs = 0;
        self.left_out_pictures = 0;
        self.remember(original, Some(out));
    }

    fn step(&mut self, op: &Op) {
        match *op {
            Op::Frame {
                send,
                packets,
                lost,
                skipped,
            } => {
                let video = !matches!(self.mode, Mode::Audio);
                let lost = if lost >= 224 {
                    u16::from(lost - 224)
                } else {
                    0
                };
                let skipped = if video && skipped >= 224 {
                    skipped - 224
                } else {
                    0
                };
                let (lost_from, lost_at) = (self.seq, self.moved);
                self.advance(lost, 0);
                self.advance(0, u16::from(skipped));
                // Audio is never left out: the loudest voices are sent, the rest not at all.
                let send = send || !video || self.too_far_ahead();
                let frame_seq = self.seq;
                for n in 0..frame_seq.wrapping_sub(lost_from).min(LOST_KEPT as u16) {
                    self.seq = lost_from.wrapping_add(n);
                    let p = self.packet();
                    if self.lost.len() == LOST_KEPT {
                        self.lost.remove(0);
                    }
                    let at = Moved {
                        seq: lost_at.seq + u64::from(n),
                        picture: self.moved.picture,
                    };
                    self.lost.push((p, send, at));
                }
                self.seq = frame_seq;
                for i in 0..(packets % 3 + 1) {
                    let p = self.packet();
                    self.deliver(p, send, i == 0);
                    self.seq = self.seq.wrapping_add(1);
                    self.moved.seq += 1;
                }
                self.picture = self.picture.wrapping_add(1) & self.mask;
                self.moved.picture += 1;
                self.ts = self.ts.wrapping_add(3000);
            }
            Op::Jump { seq, pictures } => {
                self.advance(seq % 30_000, u16::from(pictures % 60));
                self.ts = self.ts.wrapping_add(u32::from(seq) * 90);
            }
            Op::Switch { seq, picture, ts } => {
                if matches!(self.mode, Mode::Audio) {
                    return;
                }
                self.seq = seq;
                self.picture = picture & self.mask;
                self.ts = ts;
                let first = self.packet();
                self.rewriter.switch_to(&first);
                self.restart_model();
                self.seq_gap = 0;
                self.picture_gap = 0;
            }
            Op::Resume { skip } => {
                if !matches!(self.mode, Mode::Audio) {
                    return;
                }
                // A voice left out for a while: the source moved on by any amount (all of the
                // sequence space included) and the rewriter saw none of it.
                self.seq = self.seq.wrapping_add(skip);
                self.moved.seq += u64::from(skip);
                self.ts = self.ts.wrapping_add(u32::from(skip) * 960);
                let first = self.packet();
                self.rewriter.resume_after_gap(&first);
                self.restart_model();
            }
            Op::Replay { back } => {
                if self.history.is_empty() {
                    return;
                }
                let at = self.history.len() - 1 - usize::from(back) % self.history.len();
                let (mut p, sent, moved) = (
                    self.history[at].packet.clone(),
                    self.history[at].sent,
                    self.history[at].at,
                );
                if self.beyond_half(moved) {
                    return;
                }
                match sent {
                    Some(out) => {
                        if self.rewriter.rewrite(&mut p) {
                            assert_eq!(
                                numbers(self.mode, &p),
                                out,
                                "a retransmission keeps the numbers it was sent with"
                            );
                        }
                    }
                    None => {
                        let d = Descriptor::parse(&p.payload);
                        self.rewriter.leave_out(&p, d.as_ref());
                    }
                }
            }
            Op::LateLost { which } => {
                // A first packet that arrives late would start the numbering behind the stream.
                if self.lost.is_empty() || self.last_out.is_none() {
                    return;
                }
                let (mut p, send, moved) = self.lost.remove(usize::from(which) % self.lost.len());
                if self.beyond_half(moved) {
                    return;
                }
                if send {
                    if self.rewriter.rewrite(&mut p) {
                        self.note_late(numbers(self.mode, &p).1);
                    }
                } else {
                    let d = Descriptor::parse(&p.payload);
                    self.rewriter.leave_out(&p, d.as_ref());
                }
            }
        }
        self.check_bounded();
    }

    /// A late packet was placed with picture id `picture`: if it is newer than the last picture the
    /// receiver was sent, the next source continues from it.
    fn note_late(&mut self, picture: Option<u16>) {
        let newest = self.late_picture.or(self.last_out.and_then(|(_, p)| p));
        if let (Some(late), Some(newest)) = (picture, newest) {
            let ahead = late.wrapping_sub(newest) & self.mask;
            if (1..=self.mask / 2).contains(&ahead) {
                self.late_picture = Some(late);
            }
        }
    }

    /// Numbers are compared within half their space (see `RUN_HORIZON_SHARE`): a packet of the
    /// source from `at` is older than that when the source has since moved on further.
    fn beyond_half(&self, at: Moved) -> bool {
        self.moved.seq - at.seq >= u64::from(u16::MAX / 2)
            || (self.mask > 0 && self.moved.picture - at.picture >= u64::from(self.mask / 2))
    }

    /// What is remembered of the source's left out numbers stays within its cap.
    fn check_bounded(&self) {
        let shown = format!("{:?}", self.rewriter);
        for list in shown.split("runs: [").skip(1) {
            let runs = list.split(']').next().unwrap_or("").matches('(').count();
            assert!(runs <= REMEMBERED_RUNS, "{runs} runs remembered");
        }
    }
}

fuzz_target!(|input: Input| {
    let (mask, vp8) = match input.mode {
        Mode::Audio => (0, false),
        Mode::Vp8Wide => (0x7fff, true),
        Mode::Vp8Short => (0x7f, true),
    };
    let mut run = Run {
        mode: input.mode,
        rewriter: Rewriter::new(555, 100, vp8),
        seq: input.seq,
        picture: input.picture & mask,
        ts: input.ts,
        last_in: None,
        last_out: None,
        left_out_seqs: 0,
        left_out_pictures: 0,
        history: Vec::new(),
        lost: Vec::new(),
        mask,
        moved: Moved::default(),
        late_picture: None,
        seq_gap: 0,
        picture_gap: 0,
    };
    for op in input.ops.iter().take(400) {
        run.step(op);
    }
});
