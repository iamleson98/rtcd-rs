//! The VP8 RTP payload descriptor (RFC 7741): what a forwarder needs of it to find keyframes, to
//! keep picture numbering continuous when a receiver is moved from one simulcast layer to another,
//! and to tell the temporal layers of a stream apart.

/// The parsed front of a VP8 RTP payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Descriptor {
    /// Bytes the descriptor occupies; the VP8 payload follows.
    pub len: usize,
    /// First packet of a partition.
    pub start: bool,
    /// Partition index (0 for the one that holds the frame header).
    pub partition: u8,
    /// Picture id and whether it is the 15-bit form.
    pub picture_id: Option<(u16, bool)>,
    /// Offset of the picture id in the payload.
    picture_id_at: usize,
    pub tl0_pic_idx: Option<u8>,
    tl0_at: usize,
    /// The temporal layer of the frame, when the sender says.
    pub temporal: Option<Temporal>,
}

/// Where a frame sits among the temporal layers of its stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Temporal {
    /// The temporal layer (TID): 0 is the base, each layer above doubles the frame rate.
    pub tid: u8,
    /// The layer sync bit (Y): the frame refers to nothing above the base layer, so a receiver
    /// that was not sent this layer can start on it here.
    pub sync: bool,
}

impl Descriptor {
    /// Parses the descriptor at the start of `payload`; `None` if it is truncated.
    pub fn parse(payload: &[u8]) -> Option<Descriptor> {
        let b0 = *payload.first()?;
        let mut d = Descriptor {
            len: 1,
            start: b0 & 0x10 != 0,
            partition: b0 & 0x07,
            picture_id: None,
            picture_id_at: 0,
            tl0_pic_idx: None,
            tl0_at: 0,
            temporal: None,
        };
        if b0 & 0x80 == 0 {
            return Some(d);
        }
        let ext = *payload.get(1)?;
        d.len = 2;
        if ext & 0x80 != 0 {
            let first = *payload.get(d.len)?;
            d.picture_id_at = d.len;
            if first & 0x80 != 0 {
                let second = *payload.get(d.len + 1)?;
                d.picture_id = Some(((u16::from(first & 0x7f) << 8) | u16::from(second), true));
                d.len += 2;
            } else {
                d.picture_id = Some((u16::from(first), false));
                d.len += 1;
            }
        }
        if ext & 0x40 != 0 {
            d.tl0_at = d.len;
            d.tl0_pic_idx = Some(*payload.get(d.len)?);
            d.len += 1;
        }
        if ext & 0x30 != 0 {
            // TID/Y/KEYIDX byte; the first two are meaningful only with the T bit.
            let b = *payload.get(d.len)?;
            if ext & 0x20 != 0 {
                d.temporal = Some(Temporal {
                    tid: b >> 6,
                    sync: b & 0x20 != 0,
                });
            }
            d.len += 1;
        }
        Some(d)
    }
}

/// Whether `payload` begins a keyframe: the first packet of a frame, whose frame tag has the
/// "not a keyframe" bit (P) clear.
pub fn is_keyframe(payload: &[u8]) -> bool {
    let Some(d) = Descriptor::parse(payload) else {
        return false;
    };
    d.start && d.partition == 0 && payload.get(d.len).is_some_and(|tag| tag & 0x01 == 0)
}

/// Shifts the picture numbering of a payload, so frames of another layer continue the numbers of
/// the one before. Returns the payload unchanged when both offsets are zero.
pub fn shift(payload: &[u8], picture_id_offset: u16, tl0_offset: u8) -> Option<Vec<u8>> {
    let d = Descriptor::parse(payload)?;
    let mut out = payload.to_vec();
    if let Some((id, wide)) = d.picture_id {
        if wide {
            let shifted = id.wrapping_add(picture_id_offset) & 0x7fff;
            out[d.picture_id_at] = 0x80 | (shifted >> 8) as u8;
            out[d.picture_id_at + 1] = shifted as u8;
        } else {
            out[d.picture_id_at] = (id.wrapping_add(picture_id_offset) & 0x7f) as u8;
        }
    }
    if let Some(t) = d.tl0_pic_idx {
        out[d.tl0_at] = t.wrapping_add(tl0_offset);
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A descriptor with a 15-bit picture id, TL0PICIDX and a TID byte, then a frame tag.
    fn wide(start: bool, partition: u8, picture_id: u16, tl0: u8, tag: u8) -> Vec<u8> {
        vec![
            0x80 | if start { 0x10 } else { 0 } | partition,
            0xe0,
            0x80 | (picture_id >> 8) as u8,
            picture_id as u8,
            tl0,
            0x20,
            tag,
            0xaa,
            0xbb,
        ]
    }

    #[test]
    fn parses_every_optional_field() {
        let d = Descriptor::parse(&wide(true, 0, 0x1234, 9, 0)).unwrap();
        assert_eq!(d.len, 6);
        assert!(d.start);
        assert_eq!(d.picture_id, Some((0x1234, true)));
        assert_eq!(d.tl0_pic_idx, Some(9));
        assert_eq!(d.temporal, Some(Temporal { tid: 0, sync: true }));
    }

    #[test]
    fn reads_the_temporal_layer_and_its_sync_bit() {
        let mut p = wide(true, 0, 7, 1, 0x01);
        p[5] = 0x80; // TID 2, no sync
        assert_eq!(
            Descriptor::parse(&p).unwrap().temporal,
            Some(Temporal {
                tid: 2,
                sync: false
            })
        );
        p[5] = 0x60; // TID 1, sync
        assert_eq!(
            Descriptor::parse(&p).unwrap().temporal,
            Some(Temporal { tid: 1, sync: true })
        );
        // KEYIDX alone (K without T) says nothing about temporal layers.
        p[1] = 0xd0;
        let d = Descriptor::parse(&p).unwrap();
        assert_eq!((d.temporal, d.len), (None, 6));
    }

    #[test]
    fn parses_the_short_forms() {
        assert_eq!(
            Descriptor::parse(&[0x10, 0x00]).unwrap().len,
            1,
            "no extension"
        );
        let d = Descriptor::parse(&[0x90, 0x80, 0x45, 0x00]).unwrap();
        assert_eq!((d.len, d.picture_id), (3, Some((0x45, false))));
    }

    #[test]
    fn truncated_descriptors_are_rejected() {
        for payload in [
            &[][..],
            &[0x80],
            &[0x80, 0x80],
            &[0x80, 0x80, 0x80],
            &[0x80, 0x40],
            &[0x80, 0x20],
        ] {
            assert_eq!(Descriptor::parse(payload), None, "{payload:?}");
        }
    }

    #[test]
    fn keyframes_are_the_start_of_a_frame_with_the_p_bit_clear() {
        assert!(is_keyframe(&wide(true, 0, 1, 0, 0x00)));
        assert!(!is_keyframe(&wide(true, 0, 1, 0, 0x01)), "an interframe");
        assert!(
            !is_keyframe(&wide(false, 0, 1, 0, 0x00)),
            "not the first packet of the frame"
        );
        assert!(!is_keyframe(&wide(true, 1, 1, 0, 0x00)), "not partition 0");
        assert!(!is_keyframe(&[]));
        assert!(!is_keyframe(&[0x10]), "no frame tag");
    }

    #[test]
    fn shifting_moves_the_picture_id_and_tl0_and_nothing_else() {
        let p = wide(true, 0, 100, 7, 0x00);
        let out = shift(&p, 50, 3).unwrap();
        let d = Descriptor::parse(&out).unwrap();
        assert_eq!(d.picture_id, Some((150, true)));
        assert_eq!(d.tl0_pic_idx, Some(10));
        assert_eq!(out[5..], p[5..], "the rest of the payload is untouched");
        assert_eq!(out[..2], p[..2]);
    }

    #[test]
    fn picture_ids_wrap_within_their_width() {
        let out = shift(&wide(true, 0, 0x7fff, 255, 0), 2, 1).unwrap();
        let d = Descriptor::parse(&out).unwrap();
        assert_eq!(d.picture_id, Some((1, true)));
        assert_eq!(d.tl0_pic_idx, Some(0));
        let short = shift(&[0x90, 0x80, 0x7f, 0x00], 1, 0).unwrap();
        assert_eq!(
            Descriptor::parse(&short).unwrap().picture_id,
            Some((0, false))
        );
    }
}
