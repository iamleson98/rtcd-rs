//! A stream the recorder writes: packets of any shape in any order, with gaps, on a video or an
//! audio stream, and the files it leaves must be finished (or removed) without a panic.
#![no_main]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use recorder::media::{Recording, StreamInfo, StreamRecorder, Users};
use recorder::tap::TapPacket;

#[derive(Arbitrary, Debug)]
struct Input {
    video: bool,
    packets: Vec<(u16, u32, u16, bool, Vec<u8>)>,
}

fuzz_target!(|input: Input| {
    let dir: PathBuf = std::env::temp_dir().join(format!("fuzz-recorder-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let start = Instant::now();
    let rec = Arc::new(Recording::new(&dir, start, 1 << 20));
    let mut s = StreamRecorder::new(
        StreamInfo {
            kind: if input.video { "screen" } else { "voice" },
            session_id: "s".into(),
            video: input.video,
        },
        rec,
        Users::default(),
    );
    let mut now = start;
    for (seq, ts, wait_ms, marker, payload) in input.packets.into_iter().take(512) {
        now += Duration::from_millis(u64::from(wait_ms) * 4);
        s.packet(TapPacket {
            seq,
            ts,
            marker,
            payload: payload.into(),
            arrived: now,
        });
        s.tick(now);
    }
    s.finish(now, "fuzz");
    let _ = std::fs::remove_dir_all(&dir);
});
