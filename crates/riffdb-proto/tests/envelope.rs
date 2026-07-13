//! Durable-envelope compatibility and fail-closed integration tests.

use proptest::prelude::*;
use prost::Message;
use riffdb_proto::{
    envelope::{
        EnvelopeError, MAX_REGISTERED_RECORD_SCHEMAS, MAX_STORED_ENVELOPE_BYTES,
        PayloadValidationError, RecordRegistry, RecordRegistryError, RecordSchema,
        RecordSchemaError, STORAGE_FORMAT_VERSION_V1, durable_schema_hash, encode, payload_crc32c,
    },
    storage::v1::StoredEnvelope,
};

const RECORD_TYPE: &str = "riffdb.testing.v1.CompatibilityProbe";
const OTHER_RECORD_TYPE: &str = "riffdb.testing.v1.OtherProbe";
const EXPECTED_SCHEMA_HASH: [u8; 32] = [
    0x31, 0x57, 0x3d, 0x83, 0xac, 0x11, 0xe9, 0xbb, 0x2b, 0xec, 0xf3, 0xf7, 0x9e, 0x3e, 0x02, 0xb4,
    0xa0, 0x4c, 0x4c, 0x89, 0x3a, 0x92, 0x2a, 0xad, 0x86, 0x99, 0xf8, 0x17, 0x13, 0x1a, 0xf7, 0x35,
];
const DESCRIPTOR: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/descriptors/compatibility-probe-descriptor-set.bin"
));
const PROBE_PAYLOAD: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/compatibility-probe-payload.bin"
));
const PROBE_ENVELOPE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/compatibility-probe-envelope.bin"
));

#[derive(Clone, PartialEq, Message)]
struct CompatibilityProbe {
    #[prost(uint64, tag = "1")]
    value: u64,
}

fn validate_probe(payload: &[u8]) -> Result<(), PayloadValidationError> {
    let probe =
        CompatibilityProbe::decode(payload).map_err(|_| PayloadValidationError::Malformed)?;
    if probe.encode_to_vec() != payload {
        return Err(PayloadValidationError::NonCanonical);
    }
    Ok(())
}

fn schema() -> RecordSchema<'static> {
    RecordSchema::new(RECORD_TYPE, DESCRIPTOR, 32, validate_probe).expect("valid test schema")
}

fn registry<'a>(schemas: &'a [RecordSchema<'a>]) -> RecordRegistry<'a> {
    RecordRegistry::new(schemas).expect("valid test registry")
}

fn raw_envelope(schema: &RecordSchema<'_>, payload: Vec<u8>) -> StoredEnvelope {
    StoredEnvelope {
        storage_format_version: STORAGE_FORMAT_VERSION_V1,
        record_type: schema.record_type().to_owned(),
        payload_crc32c: payload_crc32c(&payload),
        payload,
        schema_hash: schema.schema_hash().as_bytes().to_vec(),
    }
}

fn assert_decode_error(registry: &RecordRegistry<'_>, encoded: &[u8], expected: EnvelopeError) {
    assert_eq!(
        registry.decode(encoded).expect_err("decode must fail"),
        expected
    );
}

#[test]
fn crc32c_matches_the_castagnoli_golden() {
    assert_eq!(payload_crc32c(b"123456789"), 0xe306_9283);
}

#[test]
fn supported_payload_round_trips_exactly() {
    let schema = schema();
    let schemas = [schema];
    let encoded = encode(&schema, PROBE_PAYLOAD).expect("canonical payload encodes");
    assert_eq!(encoded, PROBE_ENVELOPE);
    let decoded = registry(&schemas)
        .decode(&encoded)
        .expect("canonical envelope decodes");

    assert_eq!(decoded.record_type(), RECORD_TYPE);
    assert_eq!(decoded.schema_hash(), schema.schema_hash());
    assert_eq!(decoded.payload(), PROBE_PAYLOAD);
}

#[test]
fn version_type_hash_and_checksum_fail_closed() {
    let schema = schema();
    let schemas = [schema];
    let registry = registry(&schemas);

    let mut envelope = raw_envelope(&schema, vec![0x08, 0x2a]);
    envelope.storage_format_version = 2;
    assert_decode_error(
        &registry,
        &envelope.encode_to_vec(),
        EnvelopeError::UnsupportedStorageFormatVersion,
    );

    let mut envelope = raw_envelope(&schema, vec![0x08, 0x2a]);
    envelope.record_type = OTHER_RECORD_TYPE.to_owned();
    assert_decode_error(
        &registry,
        &envelope.encode_to_vec(),
        EnvelopeError::UnknownRecordType,
    );

    let mut envelope = raw_envelope(&schema, vec![0x08, 0x2a]);
    envelope.record_type = ".riffdb.testing.v1.CompatibilityProbe".to_owned();
    assert_decode_error(
        &registry,
        &envelope.encode_to_vec(),
        EnvelopeError::InvalidRecordType,
    );

    let mut envelope = raw_envelope(&schema, vec![0x08, 0x2a]);
    envelope.schema_hash = vec![0; 31];
    assert_decode_error(
        &registry,
        &envelope.encode_to_vec(),
        EnvelopeError::InvalidSchemaHashLength,
    );

    let mut envelope = raw_envelope(&schema, vec![0x08, 0x2a]);
    envelope.schema_hash = vec![0; 32];
    assert_decode_error(
        &registry,
        &envelope.encode_to_vec(),
        EnvelopeError::UnsupportedSchemaHash,
    );

    let mut envelope = raw_envelope(&schema, vec![0x08, 0x2a]);
    envelope.payload_crc32c ^= 1;
    assert_decode_error(
        &registry,
        &envelope.encode_to_vec(),
        EnvelopeError::ChecksumMismatch,
    );
}

#[test]
fn record_and_absolute_size_limits_are_enforced() {
    let schema = schema();
    let schemas = [schema];
    let registry = registry(&schemas);
    let payload = vec![0; 33];
    let envelope = raw_envelope(&schema, payload);
    assert_decode_error(
        &registry,
        &envelope.encode_to_vec(),
        EnvelopeError::PayloadTooLarge,
    );

    let oversized = vec![0; MAX_STORED_ENVELOPE_BYTES + 1];
    assert_decode_error(&registry, &oversized, EnvelopeError::EnvelopeTooLarge);
    assert_eq!(
        RecordSchema::new(
            RECORD_TYPE,
            DESCRIPTOR,
            MAX_STORED_ENVELOPE_BYTES + 1,
            validate_probe
        )
        .expect_err("oversized record limit must fail"),
        RecordSchemaError::InvalidPayloadLimit
    );
}

#[test]
fn malformed_and_noncanonical_outer_encodings_are_rejected() {
    let schema = schema();
    let schemas = [schema];
    let registry = registry(&schemas);
    assert_decode_error(&registry, &[0xff], EnvelopeError::Malformed);

    let mut encoded = encode(&schema, PROBE_PAYLOAD).expect("canonical payload encodes");
    // Unknown field 99 with varint value zero. Prost drops it during decode.
    encoded.extend_from_slice(&[0x98, 0x06, 0x00]);
    assert_decode_error(&registry, &encoded, EnvelopeError::NonCanonicalEnvelope);
}

#[test]
fn semantic_decoder_rejects_noncanonical_payload() {
    let schema = schema();
    let schemas = [schema];
    let registry = registry(&schemas);
    let payload = vec![0x08, 0x81, 0x00];
    let envelope = raw_envelope(&schema, payload);
    assert_decode_error(
        &registry,
        &envelope.encode_to_vec(),
        EnvelopeError::InvalidPayload(PayloadValidationError::NonCanonical),
    );
}

#[test]
fn malformed_inputs_do_not_panic() {
    let schema = schema();
    let schemas = [schema];
    let registry = registry(&schemas);
    for length in 0..512 {
        let bytes = (0..length)
            .map(|index| ((index * 131 + length * 17) & 0xff) as u8)
            .collect::<Vec<_>>();
        let result = std::panic::catch_unwind(|| registry.decode(&bytes));
        assert!(result.is_ok(), "decoder panicked for length {length}");
    }
}

proptest! {
    #[test]
    fn arbitrary_bounded_envelope_bytes_do_not_panic(bytes in proptest::collection::vec(any::<u8>(), 0..4096)) {
        let schema = schema();
        let schemas = [schema];
        let registry = registry(&schemas);
        let result = std::panic::catch_unwind(|| registry.decode(&bytes));
        prop_assert!(result.is_ok());
    }
}

#[test]
fn schema_hash_framing_and_record_type_validation_are_stable() {
    let hash = durable_schema_hash(RECORD_TYPE, DESCRIPTOR).expect("valid schema hash");
    assert_eq!(hash, schema().schema_hash());
    assert_eq!(hash.as_bytes(), &EXPECTED_SCHEMA_HASH);

    for invalid in [
        "",
        ".riffdb.storage.Message",
        "riffdb..Message",
        "unqualified",
        "riffdb.storage.1Message",
        "riffdb.storage.Message-name",
        "riffdb.storage.Mes\u{e9}sage",
    ] {
        let error = RecordSchema::new(invalid, DESCRIPTOR, 32, validate_probe)
            .expect_err("invalid record type must fail");
        assert_eq!(error, RecordSchemaError::InvalidRecordType, "{invalid:?}");
    }
}

#[test]
fn registry_is_bounded_and_rejects_exact_duplicates() {
    let schema = schema();
    assert_eq!(
        RecordRegistry::new(&[schema, schema]).expect_err("duplicate schema must fail"),
        RecordRegistryError::DuplicateSchema
    );

    let schemas = vec![schema; MAX_REGISTERED_RECORD_SCHEMAS + 1];
    assert_eq!(
        RecordRegistry::new(&schemas).expect_err("oversized registry must fail"),
        RecordRegistryError::TooManySchemas
    );
}
