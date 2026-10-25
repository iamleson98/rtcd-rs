//! A websocket frame from the LMS server.
#![no_main]

use libfuzzer_sys::fuzz_target;
use rtcd_proto::ClientMessage;

fuzz_target!(|data: &[u8]| {
    if let Ok(msg) = ClientMessage::unpack(data) {
        let _ = ClientMessage::unpack(&msg.pack());
    }
});
