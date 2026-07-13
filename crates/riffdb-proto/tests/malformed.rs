//! Property coverage for all byte-oriented WP-020 decoders.

use proptest::prelude::*;
use prost::Message;
use riffdb_proto::{
    decode_execute_request, decode_execute_response, decode_public_error, decode_value,
    envelope::{PayloadValidationError, RecordRegistry, RecordSchema},
};

const RECORD_TYPE: &str = "riffdb.testing.v1.CompatibilityProbe";
const DESCRIPTOR: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/descriptors/compatibility-probe-descriptor-set.bin"
));

#[derive(Clone, Copy, PartialEq, Eq, Message)]
struct CompatibilityProbe {
    #[prost(uint64, tag = "1")]
    value: u64,
}

fn validate_probe(payload: &[u8]) -> Result<(), PayloadValidationError> {
    let value =
        CompatibilityProbe::decode(payload).map_err(|_| PayloadValidationError::Malformed)?;
    if value.encode_to_vec() != payload {
        return Err(PayloadValidationError::NonCanonical);
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn bounded_arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..4096)) {
        let schema = RecordSchema::new(RECORD_TYPE, DESCRIPTOR, 32, validate_probe)
            .expect("valid probe schema");
        let schemas = [schema];
        let registry = RecordRegistry::new(&schemas).expect("valid probe registry");

        let _ = decode_value(&bytes);
        let _ = decode_public_error(&bytes);
        let _ = decode_execute_request(&bytes);
        let _ = decode_execute_response(&bytes);
        let _ = registry.decode(&bytes);
    }
}
