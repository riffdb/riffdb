//! Property coverage for all byte-oriented WP-020 decoders.

use proptest::prelude::*;
use prost::Message;
use riffdb_proto::{
    decode_execute_request, decode_execute_response, decode_public_error, decode_value,
    durable::{CURRENT_RECORD_SCHEMAS, current_record_registry},
    envelope::{RecordRegistry, STORAGE_FORMAT_VERSION_V1, payload_crc32c},
    storage::v1::StoredEnvelope,
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

        let registry = current_record_registry();
        for schema in &CURRENT_RECORD_SCHEMAS {
            let envelope = StoredEnvelope {
                storage_format_version: STORAGE_FORMAT_VERSION_V1,
                record_type: schema.record_type().to_owned(),
                payload: bytes.clone(),
                payload_crc32c: payload_crc32c(&bytes),
                schema_hash: schema.schema_hash().as_bytes().to_vec(),
            }
            .encode_to_vec();
            let result = std::panic::catch_unwind(|| registry.decode(&envelope));
            prop_assert!(result.is_ok(), "{}", schema.record_type());
        }
    }
}
