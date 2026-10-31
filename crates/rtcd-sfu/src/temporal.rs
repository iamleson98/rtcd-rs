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
    }

    /// Whether to send a packet of the frame with RTP timestamp `ts`, which is a keyframe or not
    /// and is in `temporal` (a stream without temporal layers is all base layer). Every packet of
    /// a frame gets the answer its first one got.
    pub fn admit(&mut self, ts: u32, temporal: Option<Temporal>, key: bool) -> bool {
        if let Some(&(_, send)) = self.recent[..self.used].iter().find(|(t, _)| *t == ts) {
            return send;
        }
        let tid = usize::from(temporal.map_or(0, |t| t.tid)).min(LAYERS - 1);
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
                f.admit(n as u32 * 6000, Some(t), n == 0).then_some(t.tid)
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
        assert!(!g.admit(7 * 6000, Some(pattern(7)), false));
        assert!(g.admit(9 * 6000, Some(pattern(9)), false), "frame 9 has it");
    }

    #[test]
    fn every_packet_of_a_frame_goes_the_way_the_first_went() {
        let mut f = TemporalFilter::default();
        f.set_limit(Some(0));
        let t = Some(pattern(1));
        assert!(!f.admit(6000, t, false));
        f.set_limit(None);
        assert!(
            !f.admit(6000, t, false),
            "the rest of the frame left out too"
        );
        assert!(f.admit(0, Some(pattern(0)), false));
    }

    #[test]
    fn a_keyframe_puts_every_layer_back_in_step() {
        let mut f = TemporalFilter::default();
        f.set_limit(Some(0));
        run(&mut f, 0, 8);
        f.set_limit(None);
        assert!(f.admit(100, Some(pattern(0)), true));
        // The first layer-2 frame without the sync bit right after the keyframe refers to it.
        assert!(f.admit(
            200,
            Some(Temporal {
                tid: 2,
                sync: false
            }),
            false
        ));
    }

    #[test]
    fn a_stream_without_temporal_layers_is_all_base() {
        let mut f = TemporalFilter::default();
        f.set_limit(Some(0));
        assert!((0..10).all(|n| f.admit(n * 3000, None, n == 0)));
    }
}
