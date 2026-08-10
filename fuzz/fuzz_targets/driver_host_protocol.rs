#![no_main]

use libfuzzer_sys::fuzz_target;
use riffdb_driver_host::FrameCodec;

fuzz_target!(|data: &[u8]| {
    let _ = FrameCodec::decode_request(data);
    let _ = FrameCodec::decode_response(data);
});
