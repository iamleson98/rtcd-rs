//! VP8 RTP payloads from a sender, through the SFU's descriptor parser and rewriter and the
//! recorder's frame assembler.
#![no_main]

use std::time::Instant;

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = rtcd_sfu::vp8::Descriptor::parse(data);
    let _ = rtcd_sfu::vp8::is_keyframe(data);
    let _ = rtcd_sfu::vp8::shift(data, 7, 3);
    let _ = recorder::vp8::descriptor(data);
    let _ = recorder::vp8::size(data);
    let _ = recorder::vp8::is_keyframe(data);

    // Packets cut out of the input: [len, bytes...] repeated, each a payload of a consecutive packet.
    let mut asm = recorder::vp8::Assembler::new();
    let mut rest = data;
    let mut seq = 0u16;
    let now = Instant::now();
    while let Some((&n, tail)) = rest.split_first() {
        let n = (n as usize).min(tail.len());
        let (payload, tail) = tail.split_at(n);
        let mut p = rtc::rtp::packet::Packet::default();
        p.header.sequence_number = seq;
        p.header.marker = n % 3 == 0;
        p.payload = payload.to_vec().into();
        asm.push(now, &p);
        while let Some((_, f)) = asm.frame() {
            asm.recycle(f);
        }
        if n % 7 == 0 {
            asm.lost();
        }
        seq = seq.wrapping_add(1 + (n % 2) as u16);
        rest = tail;
    }
});
