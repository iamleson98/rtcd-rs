//! VP8 in RTP (RFC 7741) and in frames (RFC 6386).

use std::time::Instant;

use rtc::rtp::packet::Packet;

/// A frame is a keyframe when the frame tag's inverse keyframe bit is clear.
pub fn is_keyframe(frame: &[u8]) -> bool {
    frame.first().is_some_and(|b| b & 1 == 0)
}

/// A keyframe's dimensions (RFC 6386 §9.1).
pub fn size(frame: &[u8]) -> Option<(u32, u32)> {
    if frame.len() < 10 || frame[3..6] != [0x9d, 0x01, 0x2a] {
        return None;
    }
    let w = u16::from_le_bytes([frame[6], frame[7]]) & 0x3fff;
    let h = u16::from_le_bytes([frame[8], frame[9]]) & 0x3fff;
    Some((u32::from(w), u32::from(h)))
}

/// The payload descriptor's reading: where the VP8 data starts, and whether this packet starts
/// a frame.
pub fn descriptor(payload: &[u8]) -> Option<(usize, bool)> {
    let b0 = *payload.first()?;
    let mut i = 1;
    if b0 & 0x80 != 0 {
        let ext = *payload.get(i)?;
        i += 1;
        if ext & 0x80 != 0 {
            // PictureID: 15 bits when the top bit of its first byte is set.
            let p = *payload.get(i)?;
            i += if p & 0x80 != 0 { 2 } else { 1 };
        }
        if ext & 0x40 != 0 {
            i += 1; // TL0PICIDX
        }
        if ext & 0x30 != 0 {
            i += 1; // TID / KEYIDX
        }
    }
    if i >= payload.len() {
        return None;
    }
    let start = b0 & 0x10 != 0 && b0 & 0x07 == 0;
    Some((i, start))
}

/// A frame the assembler finished.
#[derive(Debug, Clone, Copy)]
pub struct Frame {
    pub key: bool,
    /// The RTP timestamp.
    pub timestamp: u32,
    /// When the frame's first packet arrived.
    pub arrived: Instant,
}

/// Builds frames out of ordered packets.
#[derive(Debug)]
pub struct Assembler {
    buf: Vec<u8>,
    timestamp: u32,
    arrived: Instant,
    in_frame: bool,
    ready: Option<Frame>,
    spare: Vec<u8>,
}

impl Default for Assembler {
    fn default() -> Self {
        Self::new()
    }
}

impl Assembler {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(32 << 10),
            timestamp: 0,
            arrived: Instant::now(),
            in_frame: false,
            ready: None,
            spare: Vec::new(),
        }
    }

    /// Takes the next packet in order. A finished frame is available from [`Self::frame`].
    pub fn push(&mut self, arrived: Instant, p: &Packet) {
        let Some((at, start)) = descriptor(&p.payload) else {
            return;
        };
        if start {
            // A frame that never finished is dropped by its successor.
            self.buf.clear();
            self.in_frame = true;
            self.timestamp = p.header.timestamp;
            self.arrived = arrived;
        } else if !self.in_frame || p.header.timestamp != self.timestamp {
            return; // the rest of a frame whose start is gone
        }
        self.buf.extend_from_slice(&p.payload[at..]);
        if p.header.marker {
            self.in_frame = false;
            if !self.buf.is_empty() {
                self.ready = Some(Frame {
                    key: is_keyframe(&self.buf),
                    timestamp: self.timestamp,
                    arrived: self.arrived,
                });
            }
        }
    }

    /// Packets went missing: the frame being built is no good.
    pub fn lost(&mut self) {
        self.in_frame = false;
        self.buf.clear();
    }

    /// The finished frame and its bytes, once. Give the bytes back with [`Self::recycle`].
    pub fn frame(&mut self) -> Option<(Frame, Vec<u8>)> {
        let f = self.ready.take()?;
        let data = std::mem::replace(&mut self.buf, std::mem::take(&mut self.spare));
        Some((f, data))
    }

    /// Takes a frame's buffer back for the next one.
    pub fn recycle(&mut self, mut data: Vec<u8>) {
        data.clear();
        if data.capacity() > self.spare.capacity() {
            self.spare = data;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pkt(seq: u16, ts: u32, marker: bool, desc: &[u8], data: &[u8]) -> Packet {
        let mut p = Packet::default();
        p.header.sequence_number = seq;
        p.header.timestamp = ts;
        p.header.marker = marker;
        let mut v = desc.to_vec();
        v.extend_from_slice(data);
        p.payload = v.into();
        p
    }

    #[test]
    fn descriptor_forms() {
        assert_eq!(descriptor(&[0x10, 1, 2]), Some((1, true)));
        assert_eq!(descriptor(&[0x00, 1, 2]), Some((1, false)));
        // X with a 7-bit picture id
        assert_eq!(descriptor(&[0x90, 0x80, 0x12, 9, 9]), Some((3, true)));
        // X with a 15-bit picture id, TL0PICIDX and TID
        assert_eq!(
            descriptor(&[0x90, 0xe0 | 0x10, 0x81, 0x00, 5, 6, 9]),
            Some((6, true))
        );
        // A start with a non-zero partition is not a frame start.
        assert_eq!(descriptor(&[0x11, 1]), Some((1, false)));
        assert_eq!(descriptor(&[0x10]), None);
        assert_eq!(descriptor(&[]), None);
    }

    #[test]
    fn keyframe_and_size() {
        let key = [
            0x10, 0x02, 0x00, 0x9d, 0x01, 0x2a, 0x80, 0x02, 0x68, 0x01, 0xff,
        ];
        assert!(is_keyframe(&key));
        assert_eq!(size(&key), Some((640, 360)));
        assert!(!is_keyframe(&[0x11, 0, 0]));
        assert_eq!(size(&[0; 4]), None);
    }

    #[test]
    fn frames_are_assembled_from_packets() {
        let t = Instant::now();
        let mut a = Assembler::new();
        a.push(t, &pkt(1, 100, false, &[0x10], &[0x10, 1, 2]));
        assert!(a.frame().is_none());
        a.push(t, &pkt(2, 100, true, &[0x00], &[3, 4]));
        let (f, data) = a.frame().unwrap();
        assert!(f.key);
        assert_eq!(f.timestamp, 100);
        assert_eq!(data, [0x10, 1, 2, 3, 4]);
        assert!(a.frame().is_none());
    }

    #[test]
    fn a_lost_packet_drops_its_frame() {
        let t = Instant::now();
        let mut a = Assembler::new();
        a.push(t, &pkt(1, 100, false, &[0x10], &[0x11, 1]));
        a.lost();
        a.push(t, &pkt(3, 100, true, &[0x00], &[3]));
        assert!(a.frame().is_none(), "the tail of a frame without its start");
        a.push(t, &pkt(4, 200, true, &[0x10], &[0x11, 7]));
        let (f, data) = a.frame().unwrap();
        assert!(!f.key);
        assert_eq!(data, [0x11, 7]);
    }

    #[test]
    fn a_new_start_replaces_an_unfinished_frame() {
        let t = Instant::now();
        let mut a = Assembler::new();
        a.push(t, &pkt(1, 100, false, &[0x10], &[0x11, 1]));
        a.push(t, &pkt(2, 200, true, &[0x10], &[0x11, 2]));
        let (f, data) = a.frame().unwrap();
        assert_eq!(f.timestamp, 200);
        assert_eq!(data, [0x11, 2]);
    }
}
