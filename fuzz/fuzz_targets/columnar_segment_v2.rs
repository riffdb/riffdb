#![no_main]

use libfuzzer_sys::fuzz_target;
use riffdb_columnar::SegmentV2Codec;

fuzz_target!(|bytes: &[u8]| {
    if let Ok(segment) = SegmentV2Codec::decode(bytes) {
        let canonical = SegmentV2Codec::encode(&segment).expect("decoded segment re-encodes");
        assert_eq!(canonical, bytes, "accepted V2 bytes are canonical");
    }
});
