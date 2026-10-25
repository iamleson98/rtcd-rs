//! The SFU's description or candidate as the recorder reads it from the websocket.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &str| {
    let _ = recorder::call::parse_signal(data);
});
