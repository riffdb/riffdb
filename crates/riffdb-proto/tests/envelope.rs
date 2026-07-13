//! Public durable-envelope boundary tests before production records exist.

use proptest::prelude::*;
use riffdb_proto::envelope::{
    EnvelopeError, MAX_STORED_ENVELOPE_BYTES, RecordRegistry, payload_crc32c,
};

const PROBE_ENVELOPE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/compatibility-probe-envelope.bin"
));

#[test]
fn crc32c_matches_the_castagnoli_golden() {
    assert_eq!(payload_crc32c(b"123456789"), 0xe306_9283);
}

#[test]
fn empty_registry_refuses_test_only_records() {
    let registry = RecordRegistry::new(&[]).expect("empty registry is valid");
    assert_eq!(
        registry
            .decode(PROBE_ENVELOPE)
            .expect_err("no production durable records are registered"),
        EnvelopeError::UnknownRecordType
    );
}

#[test]
fn absolute_envelope_limit_is_enforced_without_a_registry_entry() {
    let registry = RecordRegistry::new(&[]).expect("empty registry is valid");
    let oversized = vec![0; MAX_STORED_ENVELOPE_BYTES + 1];
    assert_eq!(
        registry
            .decode(&oversized)
            .expect_err("oversized envelope must fail first"),
        EnvelopeError::EnvelopeTooLarge
    );
}

proptest! {
    #[test]
    fn empty_registry_decoder_never_panics(
        bytes in proptest::collection::vec(any::<u8>(), 0..4096),
    ) {
        let registry = RecordRegistry::new(&[]).expect("empty registry is valid");
        let result = std::panic::catch_unwind(|| registry.decode(&bytes));
        prop_assert!(result.is_ok());
    }
}
