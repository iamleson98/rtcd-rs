//! A streaming WebM muxer for one track.
//!
//! A recording's files are written while the media arrives and are never rewritten whole: the
//! only places revisited are the sizes of the open cluster and segment, the duration and the
//! position of the cues, patched in place when the file is finished. That keeps the memory of a
//! stream at one small buffer, whatever its length, and leaves a file that browsers seek in
//! (cues) and report a duration for.
//!
//! The layout is `EBML header, Segment { SeekHead, Info, Tracks, Cluster..., Cues }`. A video
//! cluster starts at every keyframe, so each cue points at a place a decoder can start from.

use std::fs::File;
use std::io::{self, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;

use crate::disk::Disk;

/// What a file carries.
#[derive(Debug, Clone)]
pub enum Track {
    /// VP8 video of the keyframe's size.
    Vp8 { width: u32, height: u32 },
    /// Opus audio, 48 kHz.
    Opus,
}

impl Track {
    fn is_video(&self) -> bool {
        matches!(self, Self::Vp8 { .. })
    }
}

/// A cluster longer than this starts a new one: block times inside a cluster are 16 bits.
const MAX_CLUSTER_MS: i64 = 30_000;
/// Audio is cut into clusters of about this length (each is a cue).
const AUDIO_CLUSTER_MS: i64 = 5_000;
/// Bytes held before a write to the file.
const BUFFER: usize = 64 << 10;

// Element ids.
const EBML: u32 = 0x1A45_DFA3;
const SEGMENT: u32 = 0x1853_8067;
const SEEK_HEAD: u32 = 0x114D_9B74;
const SEEK: u32 = 0x4DBB;
const SEEK_ID: u32 = 0x53AB;
const SEEK_POSITION: u32 = 0x53AC;
const INFO: u32 = 0x1549_A966;
const TRACKS: u32 = 0x1654_AE6B;
const CLUSTER: u32 = 0x1F43_B675;
const CUES: u32 = 0x1C53_BB6B;
const TIMECODE_SCALE: u32 = 0x2A_D7B1;
const DURATION: u32 = 0x4489;
const MUXING_APP: u32 = 0x4D80;
const WRITING_APP: u32 = 0x5741;
const TRACK_ENTRY: u32 = 0xAE;
const TRACK_NUMBER: u32 = 0xD7;
const TRACK_UID: u32 = 0x73C5;
const TRACK_TYPE: u32 = 0x83;
const FLAG_LACING: u32 = 0x9C;
const CODEC_ID: u32 = 0x86;
const CODEC_PRIVATE: u32 = 0x63A2;
const CODEC_DELAY: u32 = 0x56AA;
const SEEK_PRE_ROLL: u32 = 0x56BB;
const NAME: u32 = 0x536E;
const VIDEO: u32 = 0xE0;
const PIXEL_WIDTH: u32 = 0xB0;
const PIXEL_HEIGHT: u32 = 0xBA;
const AUDIO: u32 = 0xE1;
const SAMPLING_FREQUENCY: u32 = 0xB5;
const CHANNELS: u32 = 0x9F;
const TIMECODE: u32 = 0xE7;
const SIMPLE_BLOCK: u32 = 0xA3;
const CUE_POINT: u32 = 0xBB;
const CUE_TIME: u32 = 0xB3;
const CUE_TRACK_POSITIONS: u32 = 0xB7;
const CUE_TRACK: u32 = 0xF7;
const CUE_CLUSTER_POSITION: u32 = 0xF1;

const APP: &str = "rtcd-recorder";

/// The size of an element that is not known yet: eight bytes, patched when it is.
const SIZE_FIELD: usize = 8;

fn put_id(out: &mut Vec<u8>, id: u32) {
    let bytes = id.to_be_bytes();
    let skip = bytes.iter().take_while(|b| **b == 0).count();
    out.extend_from_slice(&bytes[skip..]);
}

/// A size in the shortest form.
fn put_size(out: &mut Vec<u8>, size: u64) {
    let mut len = 1;
    while len < 8 && size >= (1u64 << (7 * len)) - 1 {
        len += 1;
    }
    let marked = size | (1u64 << (7 * len));
    out.extend_from_slice(&marked.to_be_bytes()[8 - len..]);
}

/// A size in eight bytes, which can be rewritten in place.
fn size_field(size: u64) -> [u8; SIZE_FIELD] {
    (size | (1u64 << 56)).to_be_bytes()
}

fn put_uint(out: &mut Vec<u8>, id: u32, value: u64) {
    let bytes = value.to_be_bytes();
    let skip = bytes.iter().take_while(|b| **b == 0).count().min(7);
    put_id(out, id);
    put_size(out, (8 - skip) as u64);
    out.extend_from_slice(&bytes[skip..]);
}

fn put_uint8(out: &mut Vec<u8>, id: u32, value: u64) {
    put_id(out, id);
    put_size(out, 8);
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_float(out: &mut Vec<u8>, id: u32, value: f64) {
    put_id(out, id);
    put_size(out, 8);
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_bytes(out: &mut Vec<u8>, id: u32, value: &[u8]) {
    put_id(out, id);
    put_size(out, value.len() as u64);
    out.extend_from_slice(value);
}

/// A master element holding what `body` writes.
fn put_master(out: &mut Vec<u8>, id: u32, body: impl FnOnce(&mut Vec<u8>)) {
    let mut inner = Vec::new();
    body(&mut inner);
    put_id(out, id);
    put_size(out, inner.len() as u64);
    out.extend_from_slice(&inner);
}

/// The Opus identification header WebM carries as the codec's private data (RFC 7845 §5.1):
/// version 1, two channels, 312 samples of pre-skip, 48 kHz input, no gain, mono/stereo mapping.
pub fn opus_head() -> Vec<u8> {
    let mut h = b"OpusHead".to_vec();
    h.extend_from_slice(&[1, 2]);
    h.extend_from_slice(&312u16.to_le_bytes());
    h.extend_from_slice(&48_000u32.to_le_bytes());
    h.extend_from_slice(&0u16.to_le_bytes());
    h.push(0);
    h
}

/// A finished file's facts.
#[derive(Debug, Clone, Copy)]
pub struct Finished {
    pub size: u64,
}

/// One WebM file being written.
#[derive(Debug)]
pub struct WebmWriter {
    file: File,
    disk: Disk,
    video: bool,
    buf: Vec<u8>,
    /// Bytes handed to the file; `buf` follows them.
    flushed: u64,
    /// Where the segment's content starts, and where its size field is.
    segment_data: u64,
    segment_size_at: u64,
    duration_at: u64,
    cues_seek_at: u64,
    cluster: Option<OpenCluster>,
    cues: Vec<(i64, u64)>,
    failed: Option<io::ErrorKind>,
}

#[derive(Debug)]
struct OpenCluster {
    /// The cluster's time, ms, and where its size field is.
    base: i64,
    size_at: u64,
    start: u64,
}

impl WebmWriter {
    /// Creates the file and writes everything up to the first cluster.
    pub fn create(path: &Path, track: &Track, name: &str, disk: Disk) -> io::Result<Self> {
        let file = File::create(path)?;
        let mut w = Self {
            file,
            disk,
            video: track.is_video(),
            buf: Vec::with_capacity(BUFFER + 4096),
            flushed: 0,
            segment_data: 0,
            segment_size_at: 0,
            duration_at: 0,
            cues_seek_at: 0,
            cluster: None,
            cues: Vec::new(),
            failed: None,
        };
        w.header(track, name);
        Ok(w)
    }

    fn pos(&self) -> u64 {
        self.flushed + self.buf.len() as u64
    }

    fn header(&mut self, track: &Track, name: &str) {
        let mut head = Vec::new();
        put_master(&mut head, EBML, |b| {
            put_uint(b, 0x4286, 1); // EBMLVersion
            put_uint(b, 0x42F7, 1); // EBMLReadVersion
            put_uint(b, 0x42F2, 4); // EBMLMaxIDLength
            put_uint(b, 0x42F3, 8); // EBMLMaxSizeLength
            put_bytes(b, 0x4282, b"webm");
            put_uint(b, 0x4287, 4); // DocTypeVersion
            put_uint(b, 0x4285, 2); // DocTypeReadVersion
        });
        put_id(&mut head, SEGMENT);
        let size_at = head.len() as u64;
        head.extend_from_slice(&size_field(0));
        self.segment_size_at = size_at;
        self.segment_data = head.len() as u64;

        // The seek head has a fixed size, so what follows it has known positions.
        let seek_entry = |id: u32, position: u64, wide: bool| {
            let mut e = Vec::new();
            put_master(&mut e, SEEK, |b| {
                let mut idb = Vec::new();
                put_id(&mut idb, id);
                put_bytes(b, SEEK_ID, &idb);
                if wide {
                    put_uint8(b, SEEK_POSITION, position);
                } else {
                    put_uint(b, SEEK_POSITION, position);
                }
            });
            e
        };
        let seek_len = {
            let mut probe = Vec::new();
            put_master(&mut probe, SEEK_HEAD, |b| {
                for id in [INFO, TRACKS, CUES] {
                    b.extend_from_slice(&seek_entry(id, 0, true));
                }
            });
            probe.len() as u64
        };

        let mut info = Vec::new();
        put_master(&mut info, INFO, |b| {
            put_uint(b, TIMECODE_SCALE, 1_000_000);
            put_bytes(b, MUXING_APP, APP.as_bytes());
            put_bytes(b, WRITING_APP, APP.as_bytes());
            put_float(b, DURATION, 0.0);
        });
        let mut tracks = Vec::new();
        put_master(&mut tracks, TRACKS, |b| {
            put_master(b, TRACK_ENTRY, |e| {
                put_uint(e, TRACK_NUMBER, 1);
                put_uint(e, TRACK_UID, 1);
                put_uint(e, FLAG_LACING, 0);
                put_bytes(e, NAME, name.as_bytes());
                match track {
                    Track::Vp8 { width, height } => {
                        put_uint(e, TRACK_TYPE, 1);
                        put_bytes(e, CODEC_ID, b"V_VP8");
                        put_master(e, VIDEO, |v| {
                            put_uint(v, PIXEL_WIDTH, u64::from(*width));
                            put_uint(v, PIXEL_HEIGHT, u64::from(*height));
                        });
                    }
                    Track::Opus => {
                        put_uint(e, TRACK_TYPE, 2);
                        put_bytes(e, CODEC_ID, b"A_OPUS");
                        put_bytes(e, CODEC_PRIVATE, &opus_head());
                        put_uint(e, CODEC_DELAY, 6_500_000);
                        put_uint(e, SEEK_PRE_ROLL, 80_000_000);
                        put_master(e, AUDIO, |a| {
                            put_float(a, SAMPLING_FREQUENCY, 48_000.0);
                            put_uint(a, CHANNELS, 2);
                        });
                    }
                }
            });
        });

        let info_at = seek_len;
        let tracks_at = info_at + info.len() as u64;
        let mut seek = Vec::new();
        put_master(&mut seek, SEEK_HEAD, |b| {
            b.extend_from_slice(&seek_entry(INFO, info_at, true));
            b.extend_from_slice(&seek_entry(TRACKS, tracks_at, true));
            // The cues come last; their position is written when the file ends.
            b.extend_from_slice(&seek_entry(CUES, 0, true));
        });
        debug_assert_eq!(seek.len() as u64, seek_len);
        // The position field is the last eight bytes of the third entry.
        self.cues_seek_at = self.segment_data + seek.len() as u64 - 8;
        // The duration is the last child of the info element: its field is the last eight bytes.
        self.duration_at = self.segment_data + info_at + info.len() as u64 - 8;

        head.extend_from_slice(&seek);
        head.extend_from_slice(&info);
        head.extend_from_slice(&tracks);
        self.push(&head);
    }

    fn push(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
        if self.buf.len() >= BUFFER {
            self.flush_buf();
        }
    }

    fn flush_buf(&mut self) {
        if self.buf.is_empty() {
            return;
        }
        if self.failed.is_none() {
            match self.file.write_all(&self.buf) {
                Ok(()) => self.disk.wrote(self.buf.len() as u64),
                Err(e) => self.failed = Some(e.kind()),
            }
        }
        self.flushed += self.buf.len() as u64;
        self.buf.clear();
    }

    /// Rewrites bytes already written (buffered or not).
    fn patch(&mut self, at: u64, bytes: &[u8]) {
        if at >= self.flushed {
            let i = (at - self.flushed) as usize;
            self.buf[i..i + bytes.len()].copy_from_slice(bytes);
        } else if self.failed.is_none()
            && let Err(e) = self.file.write_all_at(bytes, at)
        {
            self.failed = Some(e.kind());
        }
    }

    fn close_cluster(&mut self) {
        if let Some(c) = self.cluster.take() {
            let size = self.pos() - c.start;
            self.patch(c.size_at, &size_field(size));
        }
    }

    /// Writes one frame at `ms` (the file's own clock, from zero). A video keyframe opens a new
    /// cluster, and a cluster that has run long is closed.
    pub fn write_block(&mut self, key: bool, ms: i64, data: &[u8]) {
        let ms = ms.max(0);
        let limit = if self.video {
            MAX_CLUSTER_MS
        } else {
            AUDIO_CLUSTER_MS
        };
        let due = match &self.cluster {
            None => true,
            Some(c) => (self.video && key) || ms - c.base >= limit.min(MAX_CLUSTER_MS),
        };
        if due {
            self.close_cluster();
            let at = self.pos();
            let mut head = Vec::with_capacity(32);
            put_id(&mut head, CLUSTER);
            let size_at = at + head.len() as u64;
            head.extend_from_slice(&size_field(0));
            let start = at + head.len() as u64;
            put_uint8(&mut head, TIMECODE, ms as u64);
            self.push(&head);
            self.cluster = Some(OpenCluster {
                base: ms,
                size_at,
                start,
            });
            // A cue is a place to start decoding: every audio cluster, and a video one when it
            // starts on a keyframe.
            if key || !self.video {
                self.cues.push((ms, at - self.segment_data));
            }
        }
        let base = self.cluster.as_ref().map_or(0, |c| c.base);
        let rel = (ms - base).clamp(i16::MIN.into(), i16::MAX.into()) as i16;
        let mut head = [0u8; 16];
        let mut n = 0;
        head[n] = SIMPLE_BLOCK as u8;
        n += 1;
        let mut size = Vec::with_capacity(8);
        put_size(&mut size, (data.len() + 4) as u64);
        head[n..n + size.len()].copy_from_slice(&size);
        n += size.len();
        head[n] = 0x81; // track 1
        head[n + 1..n + 3].copy_from_slice(&rel.to_be_bytes());
        head[n + 3] = if key { 0x80 } else { 0 };
        n += 4;
        // The block's bytes go straight into the buffer: one copy, no allocation.
        self.buf.extend_from_slice(&head[..n]);
        self.buf.extend_from_slice(data);
        if self.buf.len() >= BUFFER {
            self.flush_buf();
        }
    }

    /// Ends the file: the last cluster's size, the cues, the duration and the segment's size.
    pub fn finish(mut self, duration_ms: i64) -> io::Result<Finished> {
        self.close_cluster();
        let cues_at = self.pos();
        let mut cues = Vec::with_capacity(self.cues.len() * 16 + 8);
        put_master(&mut cues, CUES, |b| {
            for (time, pos) in &self.cues {
                put_master(b, CUE_POINT, |p| {
                    put_uint(p, CUE_TIME, *time as u64);
                    put_master(p, CUE_TRACK_POSITIONS, |t| {
                        put_uint(t, CUE_TRACK, 1);
                        put_uint(t, CUE_CLUSTER_POSITION, *pos);
                    });
                });
            }
        });
        self.push(&cues);
        let end = self.pos();
        self.patch(
            self.cues_seek_at,
            &(cues_at - self.segment_data).to_be_bytes(),
        );
        self.patch(self.duration_at, &(duration_ms.max(1) as f64).to_be_bytes());
        let segment_size = end - self.segment_data;
        self.patch(self.segment_size_at, &size_field(segment_size));
        self.flush_buf();
        if let Some(kind) = self.failed {
            return Err(io::Error::from(kind));
        }
        self.file.flush()?;
        Ok(Finished { size: end })
    }

    /// Bytes written so far, including the unflushed ones.
    pub fn written(&self) -> u64 {
        self.pos()
    }

    /// The first write error, if any.
    pub fn error(&self) -> Option<io::ErrorKind> {
        self.failed
    }
}

#[cfg(test)]
pub(crate) mod parse {
    //! A reader of the files this module writes, for the tests.

    /// One element: its id, the offset of its content and the content's length.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Element {
        pub id: u32,
        pub at: usize,
        pub start: usize,
        pub len: usize,
    }

    pub fn elements(data: &[u8], mut at: usize, end: usize) -> Vec<Element> {
        let mut out = Vec::new();
        while at < end {
            let first = data[at];
            let idlen = first.leading_zeros() as usize + 1;
            let id = data[at..at + idlen]
                .iter()
                .fold(0u32, |a, b| a << 8 | u32::from(*b));
            let s0 = data[at + idlen];
            let slen = s0.leading_zeros() as usize + 1;
            let mut len = u64::from(s0) & ((1u64 << (8 - slen)) - 1);
            for b in &data[at + idlen + 1..at + idlen + slen] {
                len = len << 8 | u64::from(*b);
            }
            let start = at + idlen + slen;
            out.push(Element {
                id,
                at,
                start,
                len: len as usize,
            });
            at = start + len as usize;
        }
        assert_eq!(at, end, "elements end where their parent does");
        out
    }

    pub fn find(data: &[u8], parent: Element, id: u32) -> Vec<Element> {
        elements(data, parent.start, parent.start + parent.len)
            .into_iter()
            .filter(|e| e.id == id)
            .collect()
    }

    pub fn uint(data: &[u8], e: Element) -> u64 {
        data[e.start..e.start + e.len]
            .iter()
            .fold(0, |a, b| a << 8 | u64::from(*b))
    }

    pub fn float(data: &[u8], e: Element) -> f64 {
        f64::from_be_bytes(data[e.start..e.start + 8].try_into().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::parse::*;
    use super::*;

    fn write(
        track: Track,
        frames: &[(bool, i64, usize)],
        tag: &str,
    ) -> (std::path::PathBuf, Finished) {
        let path =
            std::env::temp_dir().join(format!("webm-test-{}-{tag}.webm", std::process::id()));
        let mut w = WebmWriter::create(&path, &track, "test", Disk::new(u64::MAX)).unwrap();
        for (key, ms, len) in frames {
            w.write_block(*key, *ms, &vec![(*ms % 251) as u8; *len]);
        }
        let end = frames.last().map_or(0, |f| f.1 + 33);
        let done = w.finish(end).unwrap();
        (path, done)
    }

    #[test]
    fn video_file_is_well_formed() {
        // A keyframe every 60 frames (about 2 s) at 30 fps; 40 kB keyframes push a cluster through
        // the write buffer.
        let frames: Vec<_> = (0..180)
            .map(|i| {
                (
                    i % 60 == 0,
                    i * 33,
                    if i % 60 == 0 { 40_000 } else { 3_000 },
                )
            })
            .collect();
        let (path, done) = write(
            Track::Vp8 {
                width: 640,
                height: 360,
            },
            &frames,
            "video",
        );
        let data = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        assert_eq!(data.len() as u64, done.size);
        let top = elements(&data, 0, data.len());
        assert_eq!(
            top.iter().map(|e| e.id).collect::<Vec<_>>(),
            [EBML, SEGMENT]
        );
        let segment = top[1];
        assert_eq!(
            segment.start + segment.len,
            data.len(),
            "the segment's size is patched"
        );
        let kids = elements(&data, segment.start, segment.start + segment.len);
        let ids: Vec<u32> = kids.iter().map(|e| e.id).collect();
        assert_eq!(&ids[..3], [SEEK_HEAD, INFO, TRACKS]);
        assert_eq!(*ids.last().unwrap(), CUES);
        let clusters: Vec<_> = kids.iter().filter(|e| e.id == CLUSTER).collect();
        assert_eq!(clusters.len(), 3, "a cluster per keyframe");

        // The duration is patched in.
        let info = kids[1];
        let duration = find(&data, info, DURATION)[0];
        assert!(
            float(&data, duration) > 5900.0,
            "{}",
            float(&data, duration)
        );

        // Every cue points at a cluster that starts with a keyframe at the cue's time.
        let cues = *kids.last().unwrap();
        let points = find(&data, cues, CUE_POINT);
        assert_eq!(points.len(), clusters.len());
        for p in points {
            let time = uint(&data, find(&data, p, CUE_TIME)[0]);
            let tp = find(&data, p, CUE_TRACK_POSITIONS)[0];
            let pos = uint(&data, find(&data, tp, CUE_CLUSTER_POSITION)[0]) as usize;
            let cluster = clusters
                .iter()
                .find(|c| c.at == segment.start + pos)
                .expect("a cue points at a cluster");
            let inner = elements(&data, cluster.start, cluster.start + cluster.len);
            assert_eq!(uint(&data, inner[0]), time);
            assert_eq!(inner[1].id, SIMPLE_BLOCK);
            assert_eq!(data[inner[1].start + 3], 0x80, "the block is a keyframe");
        }

        // The seek head names the cues.
        let seek = find(&data, kids[0], SEEK);
        let cues_pos = uint(&data, find(&data, seek[2], SEEK_POSITION)[0]) as usize;
        assert_eq!(segment.start + cues_pos, cues.at);
    }

    #[test]
    fn audio_file_has_cues_and_codec_private() {
        let frames: Vec<_> = (0..1000).map(|i| (true, i * 20, 80usize)).collect();
        let (path, _) = write(Track::Opus, &frames, "audio");
        let data = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        let top = elements(&data, 0, data.len());
        let kids = elements(&data, top[1].start, top[1].start + top[1].len);
        let clusters = kids.iter().filter(|e| e.id == CLUSTER).count();
        assert_eq!(clusters, 4, "20 s of audio is four 5 s clusters");
        let cues = find(&data, *kids.last().unwrap(), CUE_POINT);
        assert_eq!(cues.len(), 4);
        let tracks = kids[2];
        let entry = find(&data, tracks, TRACK_ENTRY)[0];
        let private = find(&data, entry, CODEC_PRIVATE)[0];
        assert_eq!(&data[private.start..private.start + 8], b"OpusHead");
    }

    #[test]
    fn long_video_without_keyframes_still_fits_cluster_time() {
        // 70 s with a single keyframe: clusters are cut before block times overflow 16 bits.
        let frames: Vec<_> = (0..2100).map(|i| (i == 0, i * 33, 500usize)).collect();
        let (path, _) = write(
            Track::Vp8 {
                width: 320,
                height: 180,
            },
            &frames,
            "long",
        );
        let data = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        let top = elements(&data, 0, data.len());
        let kids = elements(&data, top[1].start, top[1].start + top[1].len);
        let clusters: Vec<_> = kids.iter().filter(|e| e.id == CLUSTER).collect();
        assert!(clusters.len() >= 3, "{}", clusters.len());
        for c in &clusters {
            for b in find(&data, **c, SIMPLE_BLOCK) {
                let rel = i16::from_be_bytes([data[b.start + 1], data[b.start + 2]]);
                assert!((0..=MAX_CLUSTER_MS as i16 + 100).contains(&rel), "{rel}");
            }
        }
        // Only the first cluster is a cue.
        let cues = find(&data, *kids.last().unwrap(), CUE_POINT);
        assert_eq!(cues.len(), 1);
    }

    #[test]
    fn ffprobe_reads_it() {
        let Ok(out) = std::process::Command::new("ffprobe")
            .arg("-version")
            .output()
        else {
            return;
        };
        if !out.status.success() {
            return;
        }
        let frames: Vec<_> = (0..300).map(|i| (i % 60 == 0, i * 33, 200usize)).collect();
        let (path, _) = write(Track::Opus, &frames, "ffprobe");
        let out = std::process::Command::new("ffprobe")
            .args([
                "-v",
                "error",
                "-show_entries",
                "format=duration,format_name",
                "-of",
                "default=nw=1",
            ])
            .arg(&path)
            .output()
            .unwrap();
        std::fs::remove_file(&path).unwrap();
        let text = String::from_utf8_lossy(&out.stdout);
        assert!(
            out.stderr.is_empty(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(text.contains("matroska,webm"), "{text}");
    }
}
