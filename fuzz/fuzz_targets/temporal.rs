//! The temporal layer filter of one receiver given any stream of frames, limits and restarts. A
//! frame is never sent whose reference the receiver lacks, no layer above the limit is started,
//! every packet of a frame goes the way its first went, a late packet changes nothing, and a restart
//! puts every layer back in step.
#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use rtcd_sfu::temporal::TemporalFilter;
use rtcd_sfu::vp8::Temporal;

/// Temporal layers a VP8 stream can have.
const LAYERS: usize = 4;
/// Frames whose decision the filter remembers (`REMEMBERED`).
const REMEMBERED: usize = 16;

#[derive(Arbitrary, Debug, Clone, Copy)]
struct Frame {
    tid: u8,
    sync: bool,
    key: bool,
    described: bool,
}

impl Frame {
    fn temporal(self) -> Option<Temporal> {
        self.described.then_some(Temporal {
            tid: self.tid,
            sync: self.sync,
        })
    }

    fn tid(self) -> usize {
        usize::from(self.temporal().map_or(0, |t| t.tid)).min(LAYERS - 1)
    }
}

#[derive(Arbitrary, Debug)]
enum Op {
    /// A new frame, in `packets` packets (the first decides).
    Frame {
        frame: Frame,
        packets: u8,
    },
    /// A packet of a frame that went by `back` frames ago.
    Late {
        back: u8,
        frame: Frame,
    },
    SetLimit(Option<u8>),
    Restart,
}

/// What the receiver lacks, as the decisions made so far say.
struct Receiver {
    synced: [bool; LAYERS],
    limit: Option<u8>,
    /// The newest frames and what became of them, oldest first.
    recent: Vec<(u32, bool)>,
    ts: u32,
    restarted: bool,
}

impl Receiver {
    fn refers_to_nothing_missing(&self, tid: usize) -> bool {
        self.synced[1..=tid].iter().all(|&s| s)
    }

    fn within_limit(&self, tid: usize) -> bool {
        self.limit.is_none_or(|l| tid <= usize::from(l))
    }

    fn fresh(&mut self, f: &mut TemporalFilter, frame: Frame, packets: u8) {
        self.ts = self.ts.wrapping_add(6000);
        let (tid, temporal) = (frame.tid(), frame.temporal());
        let was_in_step = self.synced;
        let send = f.admit(self.ts, temporal, frame.key, true);
        if frame.key {
            assert!(send, "a keyframe is always sent");
            self.synced = [true; LAYERS];
        } else if tid == 0 {
            assert!(send, "the base layer is always sent");
        } else if send {
            assert!(
                self.within_limit(tid),
                "layer {tid} sent above the limit {:?}",
                self.limit
            );
            let sync = temporal.is_some_and(|t| t.sync);
            assert!(
                self.refers_to_nothing_missing(tid) || sync,
                "a frame of layer {tid} sent that refers to frames left out: {was_in_step:?}"
            );
            if sync {
                self.synced[tid] = true;
            }
        } else {
            for s in &mut self.synced[tid..] {
                *s = false;
            }
        }
        if self.restarted && !frame.key && tid > 0 && self.within_limit(tid) {
            assert!(send, "right after a restart every layer is in step");
        }
        self.restarted = false;
        for extra in 1..packets % 4 {
            let again = Frame {
                tid: frame.tid.wrapping_add(extra),
                sync: !frame.sync,
                ..frame
            };
            assert_eq!(
                f.admit(self.ts, again.temporal(), again.key, true),
                send,
                "every packet of a frame goes the way its first went"
            );
        }
        if self.recent.len() == REMEMBERED {
            self.recent.remove(0);
        }
        self.recent.push((self.ts, send));
    }

    fn late(&mut self, f: &mut TemporalFilter, back: u8, frame: Frame) {
        if self.recent.is_empty() {
            return;
        }
        let at = self.recent.len() - 1 - usize::from(back) % self.recent.len();
        let (ts, first) = self.recent[at];
        let before = format!("{f:?}");
        let send = f.admit(ts, frame.temporal(), frame.key, false);
        assert_eq!(
            send, first,
            "a packet of a frame still remembered goes as its first did"
        );
        // A frame that went by so long ago that it is forgotten is judged as things stand.
        let forgotten = ts.wrapping_sub(0x4000_0000);
        let tid = frame.tid();
        let send = f.admit(forgotten, frame.temporal(), frame.key, false);
        if send && !frame.key && tid > 0 {
            assert!(
                self.within_limit(tid) && self.refers_to_nothing_missing(tid),
                "a late packet of layer {tid} sent that refers to frames left out"
            );
        }
        assert_eq!(before, format!("{f:?}"), "a late packet changes nothing");
    }
}

fuzz_target!(|ops: Vec<Op>| {
    let mut f = TemporalFilter::default();
    let mut r = Receiver {
        synced: [true; LAYERS],
        limit: None,
        recent: Vec::new(),
        ts: 0,
        restarted: false,
    };
    for op in ops.iter().take(600) {
        match *op {
            Op::Frame { frame, packets } => r.fresh(&mut f, frame, packets),
            Op::Late { back, frame } => r.late(&mut f, back, frame),
            Op::SetLimit(limit) => {
                f.set_limit(limit);
                r.limit = limit;
                assert_eq!(f.limit(), limit);
            }
            Op::Restart => {
                f.restart();
                r.synced = [true; LAYERS];
                r.recent.clear();
                r.restarted = true;
            }
        }
    }
});
