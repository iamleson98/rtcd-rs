//! A data channel frame comes from a browser: whatever it is, decoding it must not panic, and what
//! decodes must encode again.
#![no_main]

use libfuzzer_sys::fuzz_target;
use rtcd_proto::dc::DcMessage;

fuzz_target!(|data: &[u8]| {
    if let Ok(msg) = DcMessage::decode(data) {
        let again = DcMessage::decode(&msg.encode()).expect("an encoded frame decodes");
        // NaN is not equal to itself.
        if !format!("{msg:?}").contains("NaN") {
            assert_eq!(msg, again);
        }
    }
});
