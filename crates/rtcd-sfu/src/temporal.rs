//! Sending a receiver fewer of the temporal layers of a stream.
//!
//! A browser encodes each simulcast layer of a screen share (and of a camera) as three temporal
//! layers (`L1T3`, what Chrome does unasked): the base layer has every fourth frame, the second
//! the frames halfway between, the third the rest. A frame refers only to frames of its own layer
//! or below, so a receiver can be sent the base layer alone (a quarter of the frame rate, about
//! two fifths of the bits) or the base and the second (half the frame rate, three fifths), at the
//! full picture size, without asking the sender for anything. That is the step between the small
//! layer and the large one that a path carrying more than the first and less than the second
//! needs.
//!
//! Leaving a layer out is free; taking one up again is possible only at a frame that refers to
//! nothing above the base (the layer sync bit, which the encoder sets on every frame of the second
//! layer and on the first of the third after each base frame) or at a keyframe: the frames of the
//! layer before it were not sent.

use crate::vp8::Temporal;

/// Frames whose decision is remembered, so every packet of a frame goes the same way.
const REMEMBERED: usize = 16;
/// Temporal layers a VP8 stream can have.
const LAYERS: usize = 4;

/// Which frames of one stream one receiver is sent.
#[derive(Debug, Clone)]
pub struct TemporalFilter {
    /// The highest temporal layer to send; `None` sends every frame.
    limit: Option<u8>,
    /// For each layer, whether the receiver has every frame of it that the next one may refer to.
    synced: [bool; LAYERS],
    /// The decision for each recent frame, by its RTP timestamp.
    recent: [(u32, bool); REMEMBERED],
    used: usize,
    next: usize,
}

impl Default for TemporalFilter {
    fn default() -> Self {
        Self {
            limit: None,
            synced: [true; LAYERS],
            recent: [(0, false); REMEMBERED],
            used: 0,
            next: 0,
        }
    }
}

impl TemporalFilter {
    pub fn limit(&self) -> Option<u8> {
        self.limit
    }

    /// Sends no layer above `limit` from the next frame on, or every layer again (`None`) from
    /// the next frame each one can be started at.
    pub fn set_limit(&mut self, limit: Option<u8>) {
        self.limit = limit;
    }

    /// The stream continues from another source, at a keyframe: nothing it refers to is missing.
    pub fn restart(&mut self) {
        self.synced = [true; LAYERS];
        self.used = 0;
        self.next = 0;
    }

    /// Whether to send a packet of the frame with RTP timestamp `ts`, which is a keyframe or not
    /// and is in `temporal` (a stream without temporal layers is all base layer). Every packet of
    /// a frame gets the answer its first one got. `newest`: the packet is the newest of its
    /// stream; one that is not is a retransmission of a frame that went by long ago, which says
    /// nothing of which frames the receiver has since, so it is judged by the state as it is and
    /// changes none of it.
    pub fn admit(&mut self, ts: u32, temporal: Option<Temporal>, key: bool, newest: bool) -> bool {
        if let Some(&(_, send)) = self.recent[..self.used].iter().find(|(t, _)| *t == ts) {
            return send;
        }
        let tid = usize::from(temporal.map_or(0, |t| t.tid)).min(LAYERS - 1);
        if !newest {
            return key
                || tid == 0
                || (self.limit.is_none_or(|l| tid <= usize::from(l))
                    && self.synced[1..=tid].iter().all(|&s| s));
        }
        let send = if key {
            self.synced = [true; LAYERS];
            true
        } else if tid == 0 {
            true
        } else if self.limit.is_some_and(|l| tid > usize::from(l)) {
            // Left out: the next frames of this layer and above refer to one the receiver lacks.
            for s in &mut self.synced[tid..] {
                *s = false;
            }
            false
        } else if self.synced[1..=tid].iter().all(|&s| s) {
            true
        } else if temporal.is_some_and(|t| t.sync) {
            self.synced[tid] = true;
            true
        } else {
            false
        };
        self.recent[self.next] = (ts, send);
        self.next = (self.next + 1) % REMEMBERED;
        self.used = (self.used + 1).min(REMEMBERED);
        send
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The pattern of a browser's encoder: layers 0, 2, 1, 2, the sync bit on layer 1 and on the
    /// first layer-2 frame after the base.
    fn pattern(n: usize) -> Temporal {
        let step = n % 4;
        let tid = [0, 2, 1, 2][step];
        Temporal {
            tid,
            sync: tid == 1 || step == 1,
        }
    }

    /// Runs `frames` frames from frame number `from` through the filter, and returns the temporal
    /// layers of those sent.
    fn run(f: &mut TemporalFilter, from: usize, frames: usize) -> Vec<u8> {
        (from..from + frames)
            .filter_map(|n| {
                let t = pattern(n);
                f.admit(n as u32 * 6000, Some(t), n == 0, true)
                    .then_some(t.tid)
            })
            .collect()
    }

    #[test]
    fn without_a_limit_every_frame_is_sent() {
        let mut f = TemporalFilter::default();
        assert_eq!(run(&mut f, 0, 8), vec![0, 2, 1, 2, 0, 2, 1, 2]);
    }

    #[test]
    fn a_limit_leaves_the_layers_above_it_out_from_the_next_frame() {
        let mut f = TemporalFilter::default();
        run(&mut f, 0, 4);
        f.set_limit(Some(1));
        assert_eq!(run(&mut f, 4, 8), vec![0, 1, 0, 1]);
        f.set_limit(Some(0));
        assert_eq!(run(&mut f, 12, 8), vec![0, 0]);
    }

    #[test]
    fn a_layer_is_taken_up_again_at_a_frame_that_refers_only_to_the_base() {
        let mut f = TemporalFilter::default();
        f.set_limit(Some(0));
        assert_eq!(run(&mut f, 0, 8), vec![0, 0]);
        f.set_limit(None);
        // Frame 9 is the first layer-2 frame after a base frame (sync); 10 is layer 1 (sync);
        // 11 refers to 10, which was sent.
        assert_eq!(run(&mut f, 8, 4), vec![0, 2, 1, 2]);
    }

    #[test]
    fn a_layer_whose_frames_refer_to_ones_left_out_waits_for_a_sync_frame() {
        let mut f = TemporalFilter::default();
        f.set_limit(Some(0));
        run(&mut f, 0, 4);
        f.set_limit(None);
        // Frame 6 is layer 1 with the sync bit; 7 is layer 2 without it, and may refer to frames
        // of layer 2 that were left out: layer 2 starts again at frame 9.
        assert_eq!(run(&mut f, 6, 4), vec![1, 0, 2]);
        let mut g = TemporalFilter::default();
        g.set_limit(Some(1));
        run(&mut g, 0, 5);
        g.set_limit(None);
        // Frame 7 is layer 2 without the sync bit: it refers to frame 5... which was never sent.
        assert!(!g.admit(7 * 6000, Some(pattern(7)), false, true));
        assert!(
            g.admit(9 * 6000, Some(pattern(9)), false, true),
            "frame 9 has it"
        );
    }

    #[test]
    fn every_packet_of_a_frame_goes_the_way_the_first_went() {
        let mut f = TemporalFilter::default();
        f.set_limit(Some(0));
        let t = Some(pattern(1));
        assert!(!f.admit(6000, t, false, true));
        f.set_limit(None);
        assert!(
            !f.admit(6000, t, false, true),
            "the rest of the frame left out too"
        );
        assert!(f.admit(0, Some(pattern(0)), false, true));
    }

    #[test]
    fn a_keyframe_puts_every_layer_back_in_step() {
        let mut f = TemporalFilter::default();
        f.set_limit(Some(0));
        run(&mut f, 0, 8);
        f.set_limit(None);
        assert!(f.admit(100, Some(pattern(0)), true, true));
        // The first layer-2 frame without the sync bit right after the keyframe refers to it.
        assert!(f.admit(
            200,
            Some(Temporal {
                tid: 2,
                sync: false
            }),
            false,
            true
        ));
    }

    #[test]
    fn a_stream_without_temporal_layers_is_all_base() {
        let mut f = TemporalFilter::default();
        f.set_limit(Some(0));
        assert!((0..10).all(|n| f.admit(n * 3000, None, n == 0, true)));
    }

    #[test]
    fn a_browsers_eight_frame_pattern_is_followed_through_every_limit() {
        // Layers 0, 2, 1, 2 twice; the sync bit on the layer-1 frames and on the first layer-2
        // frame after a base frame, as a browser sets it.
        const FRAMES: [(u8, bool); 8] = [
            (0, false),
            (2, true),
            (1, true),
            (2, false),
            (0, false),
            (2, true),
            (1, true),
            (2, false),
        ];
        let frame = |f: &mut TemporalFilter, n: usize| {
            let (tid, sync) = FRAMES[n];
            f.admit(n as u32 * 6000, Some(Temporal { tid, sync }), n == 0, true)
        };
        let sent = |limit: Option<u8>, from: usize| {
            let mut f = TemporalFilter::default();
            f.set_limit(limit);
            (0..8)
                .map(|n| {
                    if n == from {
                        f.set_limit(None);
                    }
                    frame(&mut f, n)
                })
                .collect::<Vec<_>>()
        };
        let t = true;
        let n = false;
        assert_eq!(sent(None, 8), [t, t, t, t, t, t, t, t]);
        assert_eq!(sent(Some(1), 8), [t, n, t, n, t, n, t, n]);
        assert_eq!(sent(Some(0), 8), [t, n, n, n, t, n, n, n]);
        // The limit lifted at frame 3 (layer 2, no sync bit): it refers to frame 1, which was left
        // out, so it waits; the next frame 5 has the bit.
        assert_eq!(sent(Some(1), 3), [t, n, t, n, t, t, t, t]);
        // Lifted at frame 4 (base): frame 5 is the first layer-2 frame after it and has the bit,
        // frame 6 is layer 1 with it, and frame 7 refers only to what was sent since.
        assert_eq!(sent(Some(0), 4), [t, n, n, n, t, t, t, t]);
    }

    #[test]
    fn a_packet_that_came_late_changes_nothing_of_what_the_receiver_has() {
        let mut f = TemporalFilter::default();
        f.set_limit(Some(0));
        run(&mut f, 0, 4);
        f.set_limit(None);
        // The first packet of a keyframe, sent again long after the rest of it: it is passed on,
        // but the receiver has not got the frames of the layers it left out.
        assert!(f.admit(777, Some(pattern(0)), true, false));
        assert!(
            !f.admit(
                888,
                Some(Temporal {
                    tid: 2,
                    sync: false
                }),
                false,
                true
            ),
            "frames of the layer above the base are still missing"
        );
        // A late frame of a layer that is left out is left out; one of the base goes through.
        let mut g = TemporalFilter::default();
        g.set_limit(Some(0));
        assert!(!g.admit(10, Some(pattern(2)), false, false));
        assert!(g.admit(20, Some(pattern(0)), false, false));
    }

    #[test]
    fn a_restart_forgets_the_frames_it_remembered() {
        let mut f = TemporalFilter::default();
        run(&mut f, 0, REMEMBERED + 5);
        f.restart();
        f.set_limit(Some(0));
        // A frame of layer 2 with the sync bit is left out, and so are the rest of its packets
        // once the limit is lifted.
        let t = Some(Temporal { tid: 2, sync: true });
        assert!(!f.admit(5, t, false, true));
        f.set_limit(None);
        assert!(!f.admit(5, t, false, true));
    }
}
