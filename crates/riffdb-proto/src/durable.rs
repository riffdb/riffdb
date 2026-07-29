//! Closed current-schema registry for durable semantic records.
//!
//! This module validates the canonical Protobuf wire form. The storage-owned
//! codec remains responsible for semantic DTO reconstruction and validation.

use prost::Message;
use riffdb_types::SchemaHash;

use crate::durable_wire::DurablePreflightError;
use crate::envelope::{PayloadValidationError, RecordRegistry, RecordSchema};
use crate::storage::v1;

/// Number of durable semantic payload tuples accepted while opening or migrating storage.
pub const READABLE_RECORD_SCHEMA_COUNT: usize = 30;
/// Number of durable semantic roles accepted for current writes.
pub const WRITABLE_RECORD_SCHEMA_COUNT: usize = 29;
/// Number of durable semantic roles accepted for current writes.
pub const CURRENT_RECORD_SCHEMA_COUNT: usize = WRITABLE_RECORD_SCHEMA_COUNT;

const LEGACY_SCHEMA_HASH_BYTES: &[u8; WRITABLE_RECORD_SCHEMA_COUNT * 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/durable-schema-hashes.bin"
));
const LEGACY_RECORD_BOUND_BYTES: &[u8; WRITABLE_RECORD_SCHEMA_COUNT * 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/durable-record-bounds.bin"
));
const INDEX_V2_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/durable-index-v2-schema-hash.bin"
));
const INDEX_V2_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/durable-index-v2-record-bound.bin"
));

const fn legacy_schema_hash(index: usize) -> SchemaHash {
    let mut bytes = [0_u8; 32];
    let mut offset = 0;
    while offset < bytes.len() {
        bytes[offset] = LEGACY_SCHEMA_HASH_BYTES[index * 32 + offset];
        offset += 1;
    }
    SchemaHash::from_bytes(bytes)
}

const fn legacy_record_bound(index: usize, offset: usize) -> usize {
    let start = index * 8 + offset;
    u32::from_be_bytes([
        LEGACY_RECORD_BOUND_BYTES[start],
        LEGACY_RECORD_BOUND_BYTES[start + 1],
        LEGACY_RECORD_BOUND_BYTES[start + 2],
        LEGACY_RECORD_BOUND_BYTES[start + 3],
    ]) as usize
}

const fn index_v2_schema_hash() -> SchemaHash {
    SchemaHash::from_bytes(*INDEX_V2_SCHEMA_HASH_BYTES)
}

const fn index_v2_record_bound(offset: usize) -> usize {
    u32::from_be_bytes([
        INDEX_V2_RECORD_BOUND_BYTES[offset],
        INDEX_V2_RECORD_BOUND_BYTES[offset + 1],
        INDEX_V2_RECORD_BOUND_BYTES[offset + 2],
        INDEX_V2_RECORD_BOUND_BYTES[offset + 3],
    ]) as usize
}

fn validate_payload<const RECORD_INDEX: usize, M>(
    payload: &[u8],
) -> Result<(), PayloadValidationError>
where
    M: Message + Default,
{
    preflight_payload::<RECORD_INDEX>(payload)?;
    let message = M::decode(payload).map_err(|_| PayloadValidationError::Malformed)?;
    if message.encode_to_vec() != payload {
        return Err(PayloadValidationError::NonCanonical);
    }
    Ok(())
}

fn preflight_payload<const RECORD_INDEX: usize>(
    payload: &[u8],
) -> Result<(), PayloadValidationError> {
    crate::durable_wire::payload(RECORD_INDEX, payload).map_err(|error| match error {
        DurablePreflightError::Malformed => PayloadValidationError::Malformed,
        DurablePreflightError::NonCanonical => PayloadValidationError::NonCanonical,
        DurablePreflightError::LimitExceeded => PayloadValidationError::LimitExceeded,
    })
}

macro_rules! current_schema {
    ($index:literal, $name:literal, $message:ty) => {
        RecordSchema::new_current(
            concat!("riffdb.storage.v1.", $name),
            legacy_schema_hash($index),
            legacy_record_bound($index, 0),
            legacy_record_bound($index, 4),
            preflight_payload::<$index>,
            validate_payload::<$index, $message>,
        )
    };
}

const CURRENT_V1_RECORD_SCHEMAS: [RecordSchema<'static>; WRITABLE_RECORD_SCHEMA_COUNT] = [
    current_schema!(
        0,
        "StoredStorageFormatVersionV1",
        v1::StoredStorageFormatVersionV1
    ),
    current_schema!(1, "StoredDatabaseIdentityV1", v1::StoredDatabaseIdentityV1),
    current_schema!(
        2,
        "StoredApplicationSequenceAllocatorV1",
        v1::StoredApplicationSequenceAllocatorV1
    ),
    current_schema!(
        3,
        "StoredAdministrationSequenceAllocatorV1",
        v1::StoredAdministrationSequenceAllocatorV1
    ),
    current_schema!(4, "StoredContractBundleV1", v1::StoredContractBundleV1),
    current_schema!(5, "ActiveCatalogPointerV1", v1::ActiveCatalogPointerV1),
    current_schema!(
        6,
        "StoredCatalogAdministrationV1",
        v1::StoredCatalogAdministrationV1
    ),
    current_schema!(7, "StoredEntityRecordV1", v1::StoredEntityRecordV1),
    current_schema!(8, "StoredIndexEntryV1", v1::StoredIndexEntryV1),
    current_schema!(9, "StoredIndexEpochV1", v1::StoredIndexEpochV1),
    current_schema!(10, "StoredPendingAdmissionV1", v1::StoredPendingAdmissionV1),
    current_schema!(11, "StoredExecutionFailedV1", v1::StoredExecutionFailedV1),
    current_schema!(12, "StoredOutcomeV1", v1::StoredOutcomeV1),
    current_schema!(13, "StoredDurableEventV1", v1::StoredDurableEventV1),
    current_schema!(14, "StoredOutboxIntentV1", v1::StoredOutboxIntentV1),
    current_schema!(15, "StoredProvenanceRecordV1", v1::StoredProvenanceRecordV1),
    current_schema!(16, "StoredCommitRecordV1", v1::StoredCommitRecordV1),
    current_schema!(17, "CapabilityRecordV1", v1::CapabilityRecordV1),
    current_schema!(18, "CapabilityTokenLookupV1", v1::CapabilityTokenLookupV1),
    current_schema!(
        19,
        "CapabilityBootstrapMarkerV1",
        v1::CapabilityBootstrapMarkerV1
    ),
    current_schema!(
        20,
        "CapabilityAdministrationAuditV1",
        v1::CapabilityAdministrationAuditV1
    ),
    current_schema!(21, "ServiceAuditRecordV1", v1::ServiceAuditRecordV1),
    current_schema!(22, "StoredOutboxStatusV1", v1::StoredOutboxStatusV1),
    current_schema!(23, "StoredProjectionStateV1", v1::StoredProjectionStateV1),
    current_schema!(24, "StoredProjectionApplyV1", v1::StoredProjectionApplyV1),
    current_schema!(
        25,
        "StoredProjectionControlV1",
        v1::StoredProjectionControlV1
    ),
    current_schema!(26, "StoredQueryModuleV1", v1::StoredQueryModuleV1),
    current_schema!(
        27,
        "ActiveQueryModulePointerV1",
        v1::ActiveQueryModulePointerV1
    ),
    current_schema!(
        28,
        "StoredQueryModuleAdministrationV1",
        v1::StoredQueryModuleAdministrationV1
    ),
];

const INDEX_V2_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.StoredIndexEntryV2",
    index_v2_schema_hash(),
    index_v2_record_bound(0),
    index_v2_record_bound(4),
    preflight_payload::<29>,
    validate_payload::<29, v1::StoredIndexEntryV2>,
);

/// Readable durable schemas in immutable compatibility order.
pub static READABLE_RECORD_SCHEMAS: [RecordSchema<'static>; READABLE_RECORD_SCHEMA_COUNT] = [
    CURRENT_V1_RECORD_SCHEMAS[0],
    CURRENT_V1_RECORD_SCHEMAS[1],
    CURRENT_V1_RECORD_SCHEMAS[2],
    CURRENT_V1_RECORD_SCHEMAS[3],
    CURRENT_V1_RECORD_SCHEMAS[4],
    CURRENT_V1_RECORD_SCHEMAS[5],
    CURRENT_V1_RECORD_SCHEMAS[6],
    CURRENT_V1_RECORD_SCHEMAS[7],
    CURRENT_V1_RECORD_SCHEMAS[8],
    CURRENT_V1_RECORD_SCHEMAS[9],
    CURRENT_V1_RECORD_SCHEMAS[10],
    CURRENT_V1_RECORD_SCHEMAS[11],
    CURRENT_V1_RECORD_SCHEMAS[12],
    CURRENT_V1_RECORD_SCHEMAS[13],
    CURRENT_V1_RECORD_SCHEMAS[14],
    CURRENT_V1_RECORD_SCHEMAS[15],
    CURRENT_V1_RECORD_SCHEMAS[16],
    CURRENT_V1_RECORD_SCHEMAS[17],
    CURRENT_V1_RECORD_SCHEMAS[18],
    CURRENT_V1_RECORD_SCHEMAS[19],
    CURRENT_V1_RECORD_SCHEMAS[20],
    CURRENT_V1_RECORD_SCHEMAS[21],
    CURRENT_V1_RECORD_SCHEMAS[22],
    CURRENT_V1_RECORD_SCHEMAS[23],
    CURRENT_V1_RECORD_SCHEMAS[24],
    CURRENT_V1_RECORD_SCHEMAS[25],
    CURRENT_V1_RECORD_SCHEMAS[26],
    CURRENT_V1_RECORD_SCHEMAS[27],
    CURRENT_V1_RECORD_SCHEMAS[28],
    INDEX_V2_RECORD_SCHEMA,
];

/// Writable durable schemas in immutable role order.
pub static WRITABLE_RECORD_SCHEMAS: [RecordSchema<'static>; WRITABLE_RECORD_SCHEMA_COUNT] = [
    CURRENT_V1_RECORD_SCHEMAS[0],
    CURRENT_V1_RECORD_SCHEMAS[1],
    CURRENT_V1_RECORD_SCHEMAS[2],
    CURRENT_V1_RECORD_SCHEMAS[3],
    CURRENT_V1_RECORD_SCHEMAS[4],
    CURRENT_V1_RECORD_SCHEMAS[5],
    CURRENT_V1_RECORD_SCHEMAS[6],
    CURRENT_V1_RECORD_SCHEMAS[7],
    INDEX_V2_RECORD_SCHEMA,
    CURRENT_V1_RECORD_SCHEMAS[9],
    CURRENT_V1_RECORD_SCHEMAS[10],
    CURRENT_V1_RECORD_SCHEMAS[11],
    CURRENT_V1_RECORD_SCHEMAS[12],
    CURRENT_V1_RECORD_SCHEMAS[13],
    CURRENT_V1_RECORD_SCHEMAS[14],
    CURRENT_V1_RECORD_SCHEMAS[15],
    CURRENT_V1_RECORD_SCHEMAS[16],
    CURRENT_V1_RECORD_SCHEMAS[17],
    CURRENT_V1_RECORD_SCHEMAS[18],
    CURRENT_V1_RECORD_SCHEMAS[19],
    CURRENT_V1_RECORD_SCHEMAS[20],
    CURRENT_V1_RECORD_SCHEMAS[21],
    CURRENT_V1_RECORD_SCHEMAS[22],
    CURRENT_V1_RECORD_SCHEMAS[23],
    CURRENT_V1_RECORD_SCHEMAS[24],
    CURRENT_V1_RECORD_SCHEMAS[25],
    CURRENT_V1_RECORD_SCHEMAS[26],
    CURRENT_V1_RECORD_SCHEMAS[27],
    CURRENT_V1_RECORD_SCHEMAS[28],
];

/// Current durable schemas. `current` is exactly synonymous with writable roles.
pub static CURRENT_RECORD_SCHEMAS: [RecordSchema<'static>; CURRENT_RECORD_SCHEMA_COUNT] =
    WRITABLE_RECORD_SCHEMAS;

/// Returns the closed registry accepted while opening or migrating storage.
#[must_use]
pub fn readable_record_registry() -> RecordRegistry<'static> {
    RecordRegistry::new(&READABLE_RECORD_SCHEMAS)
        .expect("the generated readable durable registry is unique and bounded")
}

/// Finds one readable schema by its exact durable record-type FQN.
#[must_use]
pub fn readable_record_schema(record_type: &str) -> Option<&'static RecordSchema<'static>> {
    READABLE_RECORD_SCHEMAS
        .iter()
        .find(|schema| schema.record_type() == record_type)
}

/// Returns the closed registry of current writable roles.
#[must_use]
pub fn writable_record_registry() -> RecordRegistry<'static> {
    RecordRegistry::new(&WRITABLE_RECORD_SCHEMAS)
        .expect("the generated writable durable registry is unique and bounded")
}

/// Finds one current writable schema by its exact durable record-type FQN.
#[must_use]
pub fn writable_record_schema(record_type: &str) -> Option<&'static RecordSchema<'static>> {
    WRITABLE_RECORD_SCHEMAS
        .iter()
        .find(|schema| schema.record_type() == record_type)
}

/// Returns the current closed durable-record registry.
#[must_use]
pub fn current_record_registry() -> RecordRegistry<'static> {
    writable_record_registry()
}

/// Finds one current schema by its exact durable record-type FQN.
#[must_use]
pub fn current_record_schema(record_type: &str) -> Option<&'static RecordSchema<'static>> {
    writable_record_schema(record_type)
}

/// Returns the conservative maximum complete envelope size for a current type.
#[must_use]
pub fn maximum_current_envelope_bytes(record_type: &str) -> Option<usize> {
    current_record_schema(record_type).map(RecordSchema::max_envelope_bytes)
}
