//! Property coverage for all byte-oriented WP-020 decoders.

use proptest::prelude::*;
use riffdb_proto::{
    decode_execute_request, decode_execute_response, decode_public_error, decode_value,
    envelope::RecordRegistry,
};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn bounded_arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..4096)) {
        let registry = RecordRegistry::new(&[]).expect("empty registry is valid");

        let _ = decode_value(&bytes);
        let _ = decode_public_error(&bytes);
        let _ = decode_execute_request(&bytes);
        let _ = decode_execute_response(&bytes);
        let _ = registry.decode(&bytes);
    }
}
