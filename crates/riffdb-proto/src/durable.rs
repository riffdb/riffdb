//! Closed current-schema registry for durable semantic records.
//!
//! This module validates the canonical Protobuf wire form. The storage-owned
//! codec remains responsible for semantic DTO reconstruction and validation.

use prost::Message;
use riffdb_types::SchemaHash;

use crate::durable_wire::DurablePreflightError;
use crate::envelope::{PayloadValidationError, RecordRegistry, RecordSchema};
use crate::storage::v1;

/// Number of durable semantic payload types in the accepted v1 registry.
pub const CURRENT_RECORD_SCHEMA_COUNT: usize = 26;

const SCHEMA_HASH_BYTES: &[u8; CURRENT_RECORD_SCHEMA_COUNT * 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/durable-schema-hashes.bin"
));
const RECORD_BOUND_BYTES: &[u8; CURRENT_RECORD_SCHEMA_COUNT * 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/durable-record-bounds.bin"
));

const fn schema_hash(index: usize) -> SchemaHash {
    let mut bytes = [0_u8; 32];
    let mut offset = 0;
    while offset < bytes.len() {
        bytes[offset] = SCHEMA_HASH_BYTES[index * 32 + offset];
        offset += 1;
    }
    SchemaHash::from_bytes(bytes)
}

const fn record_bound(index: usize, offset: usize) -> usize {
    let start = index * 8 + offset;
    u32::from_be_bytes([
        RECORD_BOUND_BYTES[start],
        RECORD_BOUND_BYTES[start + 1],
        RECORD_BOUND_BYTES[start + 2],
        RECORD_BOUND_BYTES[start + 3],
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
            schema_hash($index),
            record_bound($index, 0),
            record_bound($index, 4),
            preflight_payload::<$index>,
            validate_payload::<$index, $message>,
        )
    };
}

/// Current durable schemas in the immutable ADR-0022 registry order.
pub static CURRENT_RECORD_SCHEMAS: [RecordSchema<'static>; CURRENT_RECORD_SCHEMA_COUNT] = [
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
];

/// Returns the current closed durable-record registry.
#[must_use]
pub fn current_record_registry() -> RecordRegistry<'static> {
    RecordRegistry::new(&CURRENT_RECORD_SCHEMAS)
        .expect("the generated current durable registry is unique and bounded")
}

/// Finds one current schema by its exact durable record-type FQN.
#[must_use]
pub fn current_record_schema(record_type: &str) -> Option<&'static RecordSchema<'static>> {
    CURRENT_RECORD_SCHEMAS
        .iter()
        .find(|schema| schema.record_type() == record_type)
}

/// Returns the conservative maximum complete envelope size for a current type.
#[must_use]
pub fn maximum_current_envelope_bytes(record_type: &str) -> Option<usize> {
    current_record_schema(record_type).map(RecordSchema::max_envelope_bytes)
}
