//! Closed current-schema registry for durable semantic records.
//!
//! This module validates the canonical Protobuf wire form. The storage-owned
//! codec remains responsible for semantic DTO reconstruction and validation.

use prost::Message;
use riffdb_types::{SchemaHash, hash_schema};
use std::sync::OnceLock;

use crate::durable_wire::DurablePreflightError;
use crate::envelope::{PayloadValidationError, RecordRegistry, RecordSchema};
use crate::storage::v1;

/// Number of durable semantic payload tuples accepted while opening or migrating storage.
pub const READABLE_RECORD_SCHEMA_COUNT: usize = 44;
/// Number of durable semantic roles accepted for current writes.
pub const WRITABLE_RECORD_SCHEMA_COUNT: usize = 38;
/// Number of durable semantic roles accepted for current writes.
pub const CURRENT_RECORD_SCHEMA_COUNT: usize = WRITABLE_RECORD_SCHEMA_COUNT;

const LEGACY_RECORD_SCHEMA_COUNT: usize = 29;
const LEGACY_SCHEMA_HASH_BYTES: &[u8; LEGACY_RECORD_SCHEMA_COUNT * 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/durable-schema-hashes.bin"
));
const LEGACY_RECORD_BOUND_BYTES: &[u8; LEGACY_RECORD_SCHEMA_COUNT * 8] = include_bytes!(concat!(
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
const REGISTRY_V2_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/durable-registry-v2-schema-hash.bin"
));
const REGISTRY_V2_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/durable-registry-v2-record-bound.bin"
));
const EVENT_REFERENCE_V2_SCHEMA_HASH_BYTES: &[u8; 64] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/durable-event-reference-v2-schema-hashes.bin"
));
const EVENT_REFERENCE_V2_RECORD_BOUND_BYTES: &[u8; 16] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/durable-event-reference-v2-record-bounds.bin"
));
const INDEX_GENERATION_V2_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/durable-index-generation-v2-schema-hash.bin"
));
const INDEX_GENERATION_V2_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/durable-index-generation-v2-record-bound.bin"
));
const HISTORY_INCARNATION_V1_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/durable-history-incarnation-v1-schema-hash.bin"
));
const HISTORY_INCARNATION_V1_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/durable-history-incarnation-v1-record-bound.bin"
));
const SERVICE_AUDIT_REQUEST_INDEX_V1_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/durable-service-audit-request-index-v1-schema-hash.bin"
));
const SERVICE_AUDIT_REQUEST_INDEX_V1_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/durable-service-audit-request-index-v1-record-bound.bin"
));
const EVENT_ROUTE_V1_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/durable-event-route-v1-schema-hash.bin"
));
const EVENT_ROUTE_V1_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/durable-event-route-v1-record-bound.bin"
));
const ENTITY_REFERENCE_V3_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/durable-entity-reference-v3-schema-hash.bin"
));
const ENTITY_REFERENCE_V3_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/durable-entity-reference-v3-record-bound.bin"
));
const MIGRATION_V1_SCHEMA_HASH_BYTES: &[u8; 128] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/durable-migration-v1-schema-hashes.bin"
));
const MIGRATION_V1_RECORD_BOUND_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/durable-migration-v1-record-bounds.bin"
));
const CAPABILITY_V2_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/durable-capability-v2-schema-hash.bin"
));
const CAPABILITY_V2_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/durable-capability-v2-record-bound.bin"
));
const PRE_WP280_CAPABILITY_SCHEMA_HASH: SchemaHash = SchemaHash::from_bytes([
    0xcb, 0x42, 0xc4, 0xeb, 0xbc, 0xe8, 0x28, 0x01, 0x23, 0xf8, 0xb3, 0x4d, 0x4d, 0xcd, 0xe7, 0x4c,
    0xa9, 0x48, 0x34, 0x06, 0x84, 0x75, 0x31, 0xf3, 0x4d, 0x5f, 0xb3, 0xf1, 0x8d, 0x40, 0xb3, 0x42,
]);

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

const fn registry_v2_schema_hash() -> SchemaHash {
    SchemaHash::from_bytes(*REGISTRY_V2_SCHEMA_HASH_BYTES)
}

const fn registry_v2_record_bound(offset: usize) -> usize {
    u32::from_be_bytes([
        REGISTRY_V2_RECORD_BOUND_BYTES[offset],
        REGISTRY_V2_RECORD_BOUND_BYTES[offset + 1],
        REGISTRY_V2_RECORD_BOUND_BYTES[offset + 2],
        REGISTRY_V2_RECORD_BOUND_BYTES[offset + 3],
    ]) as usize
}

const fn event_reference_v2_schema_hash(index: usize) -> SchemaHash {
    let mut bytes = [0_u8; 32];
    let mut offset = 0;
    while offset < bytes.len() {
        bytes[offset] = EVENT_REFERENCE_V2_SCHEMA_HASH_BYTES[index * 32 + offset];
        offset += 1;
    }
    SchemaHash::from_bytes(bytes)
}

const fn event_reference_v2_record_bound(index: usize, offset: usize) -> usize {
    let start = index * 8 + offset;
    u32::from_be_bytes([
        EVENT_REFERENCE_V2_RECORD_BOUND_BYTES[start],
        EVENT_REFERENCE_V2_RECORD_BOUND_BYTES[start + 1],
        EVENT_REFERENCE_V2_RECORD_BOUND_BYTES[start + 2],
        EVENT_REFERENCE_V2_RECORD_BOUND_BYTES[start + 3],
    ]) as usize
}

const fn index_generation_v2_schema_hash() -> SchemaHash {
    SchemaHash::from_bytes(*INDEX_GENERATION_V2_SCHEMA_HASH_BYTES)
}

const fn index_generation_v2_record_bound(offset: usize) -> usize {
    u32::from_be_bytes([
        INDEX_GENERATION_V2_RECORD_BOUND_BYTES[offset],
        INDEX_GENERATION_V2_RECORD_BOUND_BYTES[offset + 1],
        INDEX_GENERATION_V2_RECORD_BOUND_BYTES[offset + 2],
        INDEX_GENERATION_V2_RECORD_BOUND_BYTES[offset + 3],
    ]) as usize
}

const fn history_incarnation_v1_schema_hash() -> SchemaHash {
    SchemaHash::from_bytes(*HISTORY_INCARNATION_V1_SCHEMA_HASH_BYTES)
}

const fn history_incarnation_v1_record_bound(offset: usize) -> usize {
    u32::from_be_bytes([
        HISTORY_INCARNATION_V1_RECORD_BOUND_BYTES[offset],
        HISTORY_INCARNATION_V1_RECORD_BOUND_BYTES[offset + 1],
        HISTORY_INCARNATION_V1_RECORD_BOUND_BYTES[offset + 2],
        HISTORY_INCARNATION_V1_RECORD_BOUND_BYTES[offset + 3],
    ]) as usize
}

const fn service_audit_request_index_v1_schema_hash() -> SchemaHash {
    SchemaHash::from_bytes(*SERVICE_AUDIT_REQUEST_INDEX_V1_SCHEMA_HASH_BYTES)
}

const fn service_audit_request_index_v1_record_bound(offset: usize) -> usize {
    u32::from_be_bytes([
        SERVICE_AUDIT_REQUEST_INDEX_V1_RECORD_BOUND_BYTES[offset],
        SERVICE_AUDIT_REQUEST_INDEX_V1_RECORD_BOUND_BYTES[offset + 1],
        SERVICE_AUDIT_REQUEST_INDEX_V1_RECORD_BOUND_BYTES[offset + 2],
        SERVICE_AUDIT_REQUEST_INDEX_V1_RECORD_BOUND_BYTES[offset + 3],
    ]) as usize
}

const fn event_route_v1_schema_hash() -> SchemaHash {
    SchemaHash::from_bytes(*EVENT_ROUTE_V1_SCHEMA_HASH_BYTES)
}

const fn event_route_v1_record_bound(offset: usize) -> usize {
    u32::from_be_bytes([
        EVENT_ROUTE_V1_RECORD_BOUND_BYTES[offset],
        EVENT_ROUTE_V1_RECORD_BOUND_BYTES[offset + 1],
        EVENT_ROUTE_V1_RECORD_BOUND_BYTES[offset + 2],
        EVENT_ROUTE_V1_RECORD_BOUND_BYTES[offset + 3],
    ]) as usize
}

const fn entity_reference_v3_schema_hash() -> SchemaHash {
    SchemaHash::from_bytes(*ENTITY_REFERENCE_V3_SCHEMA_HASH_BYTES)
}

const fn entity_reference_v3_record_bound(offset: usize) -> usize {
    u32::from_be_bytes([
        ENTITY_REFERENCE_V3_RECORD_BOUND_BYTES[offset],
        ENTITY_REFERENCE_V3_RECORD_BOUND_BYTES[offset + 1],
        ENTITY_REFERENCE_V3_RECORD_BOUND_BYTES[offset + 2],
        ENTITY_REFERENCE_V3_RECORD_BOUND_BYTES[offset + 3],
    ]) as usize
}

const fn migration_v1_schema_hash(index: usize) -> SchemaHash {
    let mut bytes = [0_u8; 32];
    let mut offset = 0;
    while offset < bytes.len() {
        bytes[offset] = MIGRATION_V1_SCHEMA_HASH_BYTES[index * 32 + offset];
        offset += 1;
    }
    SchemaHash::from_bytes(bytes)
}

const fn migration_v1_record_bound(index: usize, offset: usize) -> usize {
    let start = index * 8 + offset;
    u32::from_be_bytes([
        MIGRATION_V1_RECORD_BOUND_BYTES[start],
        MIGRATION_V1_RECORD_BOUND_BYTES[start + 1],
        MIGRATION_V1_RECORD_BOUND_BYTES[start + 2],
        MIGRATION_V1_RECORD_BOUND_BYTES[start + 3],
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

fn validate_pre_wp280_capability_payload(payload: &[u8]) -> Result<(), PayloadValidationError> {
    preflight_payload::<17>(payload)?;
    let message =
        v1::CapabilityRecordV1::decode(payload).map_err(|_| PayloadValidationError::Malformed)?;
    let permissions = message
        .grant
        .as_ref()
        .and_then(|grant| grant.permissions.as_ref())
        .map(|permissions| permissions.values.as_slice())
        .unwrap_or_default();
    if permissions.iter().any(|permission| {
        permission.kind > 19
            || permission.query_module_hash.is_some()
            || permission.query_name.is_some()
    }) {
        return Err(PayloadValidationError::Malformed);
    }
    if message.encode_to_vec() != payload {
        return Err(PayloadValidationError::NonCanonical);
    }
    Ok(())
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
        .with_compact_identity(($index + 1) as u8, if $index == 17 { 2 } else { 1 })
    };
}

const CURRENT_V1_RECORD_SCHEMAS: [RecordSchema<'static>; LEGACY_RECORD_SCHEMA_COUNT] = [
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
)
.with_compact_identity(9, 2);

const REGISTRY_V2_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.StoredRecordRegistryV2",
    registry_v2_schema_hash(),
    registry_v2_record_bound(0),
    registry_v2_record_bound(4),
    preflight_payload::<30>,
    validate_payload::<30, v1::StoredRecordRegistryV2>,
)
.with_compact_identity(30, 1);

const COMMIT_V2_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.StoredCommitRecordV2",
    event_reference_v2_schema_hash(0),
    event_reference_v2_record_bound(0, 0),
    event_reference_v2_record_bound(0, 4),
    preflight_payload::<31>,
    validate_payload::<31, v1::StoredCommitRecordV2>,
)
.with_compact_identity(17, 2);

const OUTBOX_INTENT_V2_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.StoredOutboxIntentV2",
    event_reference_v2_schema_hash(1),
    event_reference_v2_record_bound(1, 0),
    event_reference_v2_record_bound(1, 4),
    preflight_payload::<32>,
    validate_payload::<32, v1::StoredOutboxIntentV2>,
)
.with_compact_identity(15, 2);

const INDEX_GENERATION_V2_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.StoredIndexGenerationV2",
    index_generation_v2_schema_hash(),
    index_generation_v2_record_bound(0),
    index_generation_v2_record_bound(4),
    preflight_payload::<33>,
    validate_payload::<33, v1::StoredIndexGenerationV2>,
)
.with_compact_identity(10, 2);

const HISTORY_INCARNATION_V1_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.StoredHistoryIncarnationV1",
    history_incarnation_v1_schema_hash(),
    history_incarnation_v1_record_bound(0),
    history_incarnation_v1_record_bound(4),
    preflight_payload::<34>,
    validate_payload::<34, v1::StoredHistoryIncarnationV1>,
)
.with_compact_identity(31, 1);

const SERVICE_AUDIT_REQUEST_INDEX_V1_RECORD_SCHEMA: RecordSchema<'static> =
    RecordSchema::new_current(
        "riffdb.storage.v1.StoredServiceAuditRequestIndexV1",
        service_audit_request_index_v1_schema_hash(),
        service_audit_request_index_v1_record_bound(0),
        service_audit_request_index_v1_record_bound(4),
        preflight_payload::<35>,
        validate_payload::<35, v1::StoredServiceAuditRequestIndexV1>,
    )
    .with_compact_identity(32, 1);

const EVENT_ROUTE_V1_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.StoredEventRouteV1",
    event_route_v1_schema_hash(),
    event_route_v1_record_bound(0),
    event_route_v1_record_bound(4),
    preflight_payload::<36>,
    validate_payload::<36, v1::StoredEventRouteV1>,
)
.with_compact_identity(33, 1);
const COMMIT_V3_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.StoredCommitRecordV3",
    entity_reference_v3_schema_hash(),
    entity_reference_v3_record_bound(0),
    entity_reference_v3_record_bound(4),
    preflight_payload::<37>,
    validate_payload::<37, v1::StoredCommitRecordV3>,
)
.with_compact_identity(17, 3);

macro_rules! migration_v1_schema {
    ($name:literal, $message:ty, $index:literal, $role:literal) => {
        RecordSchema::new_current(
            concat!("riffdb.storage.v1.", $name),
            migration_v1_schema_hash($index),
            migration_v1_record_bound($index, 0),
            migration_v1_record_bound($index, 4),
            preflight_payload::<{ 38 + $index }>,
            validate_payload::<{ 38 + $index }, $message>,
        )
        .with_compact_identity($role, 1)
    };
}

const CONTRACT_MIGRATION_JOURNAL_V1_RECORD_SCHEMA: RecordSchema<'static> = migration_v1_schema!(
    "StoredContractMigrationJournalV1",
    v1::StoredContractMigrationJournalV1,
    0,
    34
);
const CONTRACT_MIGRATION_RECORD_V1_RECORD_SCHEMA: RecordSchema<'static> = migration_v1_schema!(
    "StoredContractMigrationRecordV1",
    v1::StoredContractMigrationRecordV1,
    1,
    35
);
const CONTRACT_WRITE_RETIREMENT_V1_RECORD_SCHEMA: RecordSchema<'static> = migration_v1_schema!(
    "StoredContractWriteRetirementV1",
    v1::StoredContractWriteRetirementV1,
    2,
    36
);
const RETIRED_ENTITY_RECORD_V1_RECORD_SCHEMA: RecordSchema<'static> = migration_v1_schema!(
    "StoredRetiredEntityRecordV1",
    v1::StoredRetiredEntityRecordV1,
    3,
    37
);

const CAPABILITY_V2_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.CapabilityRecordV2",
    SchemaHash::from_bytes(*CAPABILITY_V2_SCHEMA_HASH_BYTES),
    u32::from_be_bytes([
        CAPABILITY_V2_RECORD_BOUND_BYTES[0],
        CAPABILITY_V2_RECORD_BOUND_BYTES[1],
        CAPABILITY_V2_RECORD_BOUND_BYTES[2],
        CAPABILITY_V2_RECORD_BOUND_BYTES[3],
    ]) as usize,
    u32::from_be_bytes([
        CAPABILITY_V2_RECORD_BOUND_BYTES[4],
        CAPABILITY_V2_RECORD_BOUND_BYTES[5],
        CAPABILITY_V2_RECORD_BOUND_BYTES[6],
        CAPABILITY_V2_RECORD_BOUND_BYTES[7],
    ]) as usize,
    preflight_payload::<42>,
    validate_payload::<42, v1::CapabilityRecordV2>,
)
.with_compact_identity(18, 3);

mod sealed {
    pub trait ReadableRecordMessage {}
    pub trait WritableRecordMessage: ReadableRecordMessage {}
}

/// A generated Protobuf message bound to exactly one readable durable schema.
pub trait ReadableRecordMessage: Message + sealed::ReadableRecordMessage {
    /// Returns the exact readable schema for this generated message.
    fn record_schema() -> &'static RecordSchema<'static>;
}

/// A generated Protobuf message bound to exactly one current writable durable schema.
///
/// This trait is sealed so only this crate can assert that a Prost-produced
/// payload is canonical for the selected record type.
pub trait WritableRecordMessage: ReadableRecordMessage + sealed::WritableRecordMessage {}

/// Decodes a generated durable message through its exact sealed readable schema.
///
/// Compact V2 records are preflighted and decoded once. Legacy V1 records keep
/// the complete compatibility validation path.
pub fn decode_readable_message<M>(encoded: &[u8]) -> Result<M, crate::envelope::EnvelopeError>
where
    M: ReadableRecordMessage + Default,
{
    readable_record_registry().decode_current_message(encoded, M::record_schema())
}

macro_rules! readable_v1_message {
    ($index:literal, $message:ty) => {
        impl sealed::ReadableRecordMessage for $message {}

        impl ReadableRecordMessage for $message {
            fn record_schema() -> &'static RecordSchema<'static> {
                &CURRENT_V1_RECORD_SCHEMAS[$index]
            }
        }
    };
}

macro_rules! readable_message {
    ($message:ty, $schema:ident) => {
        impl sealed::ReadableRecordMessage for $message {}

        impl ReadableRecordMessage for $message {
            fn record_schema() -> &'static RecordSchema<'static> {
                &$schema
            }
        }
    };
}

macro_rules! writable_message {
    ($message:ty) => {
        impl sealed::WritableRecordMessage for $message {}
        impl WritableRecordMessage for $message {}
    };
}

readable_v1_message!(0, v1::StoredStorageFormatVersionV1);
readable_v1_message!(1, v1::StoredDatabaseIdentityV1);
readable_v1_message!(2, v1::StoredApplicationSequenceAllocatorV1);
readable_v1_message!(3, v1::StoredAdministrationSequenceAllocatorV1);
readable_v1_message!(4, v1::StoredContractBundleV1);
readable_v1_message!(5, v1::ActiveCatalogPointerV1);
readable_v1_message!(6, v1::StoredCatalogAdministrationV1);
readable_v1_message!(7, v1::StoredEntityRecordV1);
readable_v1_message!(8, v1::StoredIndexEntryV1);
readable_v1_message!(9, v1::StoredIndexEpochV1);
readable_v1_message!(10, v1::StoredPendingAdmissionV1);
readable_v1_message!(11, v1::StoredExecutionFailedV1);
readable_v1_message!(12, v1::StoredOutcomeV1);
readable_v1_message!(13, v1::StoredDurableEventV1);
readable_v1_message!(14, v1::StoredOutboxIntentV1);
readable_v1_message!(15, v1::StoredProvenanceRecordV1);
readable_v1_message!(16, v1::StoredCommitRecordV1);
readable_v1_message!(17, v1::CapabilityRecordV1);
readable_v1_message!(18, v1::CapabilityTokenLookupV1);
readable_v1_message!(19, v1::CapabilityBootstrapMarkerV1);
readable_v1_message!(20, v1::CapabilityAdministrationAuditV1);
readable_v1_message!(21, v1::ServiceAuditRecordV1);
readable_v1_message!(22, v1::StoredOutboxStatusV1);
readable_v1_message!(23, v1::StoredProjectionStateV1);
readable_v1_message!(24, v1::StoredProjectionApplyV1);
readable_v1_message!(25, v1::StoredProjectionControlV1);
readable_v1_message!(26, v1::StoredQueryModuleV1);
readable_v1_message!(27, v1::ActiveQueryModulePointerV1);
readable_v1_message!(28, v1::StoredQueryModuleAdministrationV1);
readable_message!(v1::StoredIndexEntryV2, INDEX_V2_RECORD_SCHEMA);
readable_message!(v1::StoredRecordRegistryV2, REGISTRY_V2_RECORD_SCHEMA);
readable_message!(v1::StoredCommitRecordV2, COMMIT_V2_RECORD_SCHEMA);
readable_message!(v1::StoredOutboxIntentV2, OUTBOX_INTENT_V2_RECORD_SCHEMA);
readable_message!(
    v1::StoredIndexGenerationV2,
    INDEX_GENERATION_V2_RECORD_SCHEMA
);
readable_message!(
    v1::StoredHistoryIncarnationV1,
    HISTORY_INCARNATION_V1_RECORD_SCHEMA
);
readable_message!(
    v1::StoredServiceAuditRequestIndexV1,
    SERVICE_AUDIT_REQUEST_INDEX_V1_RECORD_SCHEMA
);
readable_message!(v1::StoredEventRouteV1, EVENT_ROUTE_V1_RECORD_SCHEMA);
readable_message!(v1::StoredCommitRecordV3, COMMIT_V3_RECORD_SCHEMA);
readable_message!(
    v1::StoredContractMigrationJournalV1,
    CONTRACT_MIGRATION_JOURNAL_V1_RECORD_SCHEMA
);
readable_message!(
    v1::StoredContractMigrationRecordV1,
    CONTRACT_MIGRATION_RECORD_V1_RECORD_SCHEMA
);
readable_message!(
    v1::StoredContractWriteRetirementV1,
    CONTRACT_WRITE_RETIREMENT_V1_RECORD_SCHEMA
);
readable_message!(
    v1::StoredRetiredEntityRecordV1,
    RETIRED_ENTITY_RECORD_V1_RECORD_SCHEMA
);
readable_message!(v1::CapabilityRecordV2, CAPABILITY_V2_RECORD_SCHEMA);

writable_message!(v1::StoredStorageFormatVersionV1);
writable_message!(v1::StoredDatabaseIdentityV1);
writable_message!(v1::StoredApplicationSequenceAllocatorV1);
writable_message!(v1::StoredAdministrationSequenceAllocatorV1);
writable_message!(v1::StoredContractBundleV1);
writable_message!(v1::ActiveCatalogPointerV1);
writable_message!(v1::StoredCatalogAdministrationV1);
writable_message!(v1::StoredEntityRecordV1);
writable_message!(v1::StoredPendingAdmissionV1);
writable_message!(v1::StoredExecutionFailedV1);
writable_message!(v1::StoredOutcomeV1);
writable_message!(v1::StoredDurableEventV1);
writable_message!(v1::StoredProvenanceRecordV1);
writable_message!(v1::CapabilityRecordV1);
writable_message!(v1::CapabilityTokenLookupV1);
writable_message!(v1::CapabilityBootstrapMarkerV1);
writable_message!(v1::CapabilityAdministrationAuditV1);
writable_message!(v1::ServiceAuditRecordV1);
writable_message!(v1::StoredOutboxStatusV1);
writable_message!(v1::StoredProjectionStateV1);
writable_message!(v1::StoredProjectionApplyV1);
writable_message!(v1::StoredProjectionControlV1);
writable_message!(v1::StoredQueryModuleV1);
writable_message!(v1::ActiveQueryModulePointerV1);
writable_message!(v1::StoredQueryModuleAdministrationV1);
writable_message!(v1::StoredIndexEntryV2);
writable_message!(v1::StoredRecordRegistryV2);
writable_message!(v1::StoredCommitRecordV3);
writable_message!(v1::StoredOutboxIntentV2);
writable_message!(v1::StoredIndexGenerationV2);
writable_message!(v1::StoredHistoryIncarnationV1);
writable_message!(v1::StoredServiceAuditRequestIndexV1);
writable_message!(v1::StoredEventRouteV1);
writable_message!(v1::StoredContractMigrationJournalV1);
writable_message!(v1::StoredContractMigrationRecordV1);
writable_message!(v1::StoredContractWriteRetirementV1);
writable_message!(v1::StoredRetiredEntityRecordV1);
writable_message!(v1::CapabilityRecordV2);

/// Encodes one sealed generated message after the same allocation-free shape preflight.
pub fn encode_current_message<M: WritableRecordMessage>(
    message: &M,
) -> Result<Vec<u8>, crate::envelope::EnvelopeError> {
    crate::envelope::encode_preflighted(M::record_schema(), &message.encode_to_vec())
}

const PRE_WP280_CAPABILITY_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.CapabilityRecordV1",
    PRE_WP280_CAPABILITY_SCHEMA_HASH,
    legacy_record_bound(17, 0),
    legacy_record_bound(17, 4),
    validate_pre_wp280_capability_payload,
    validate_pre_wp280_capability_payload,
)
.with_compact_identity(18, 1);

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
    REGISTRY_V2_RECORD_SCHEMA,
    COMMIT_V2_RECORD_SCHEMA,
    OUTBOX_INTENT_V2_RECORD_SCHEMA,
    INDEX_GENERATION_V2_RECORD_SCHEMA,
    HISTORY_INCARNATION_V1_RECORD_SCHEMA,
    SERVICE_AUDIT_REQUEST_INDEX_V1_RECORD_SCHEMA,
    EVENT_ROUTE_V1_RECORD_SCHEMA,
    COMMIT_V3_RECORD_SCHEMA,
    CONTRACT_MIGRATION_JOURNAL_V1_RECORD_SCHEMA,
    CONTRACT_MIGRATION_RECORD_V1_RECORD_SCHEMA,
    CONTRACT_WRITE_RETIREMENT_V1_RECORD_SCHEMA,
    RETIRED_ENTITY_RECORD_V1_RECORD_SCHEMA,
    CAPABILITY_V2_RECORD_SCHEMA,
    PRE_WP280_CAPABILITY_RECORD_SCHEMA,
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
    INDEX_GENERATION_V2_RECORD_SCHEMA,
    CURRENT_V1_RECORD_SCHEMAS[10],
    CURRENT_V1_RECORD_SCHEMAS[11],
    CURRENT_V1_RECORD_SCHEMAS[12],
    CURRENT_V1_RECORD_SCHEMAS[13],
    OUTBOX_INTENT_V2_RECORD_SCHEMA,
    CURRENT_V1_RECORD_SCHEMAS[15],
    COMMIT_V3_RECORD_SCHEMA,
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
    HISTORY_INCARNATION_V1_RECORD_SCHEMA,
    SERVICE_AUDIT_REQUEST_INDEX_V1_RECORD_SCHEMA,
    EVENT_ROUTE_V1_RECORD_SCHEMA,
    CONTRACT_MIGRATION_JOURNAL_V1_RECORD_SCHEMA,
    CONTRACT_MIGRATION_RECORD_V1_RECORD_SCHEMA,
    CONTRACT_WRITE_RETIREMENT_V1_RECORD_SCHEMA,
    RETIRED_ENTITY_RECORD_V1_RECORD_SCHEMA,
    CAPABILITY_V2_RECORD_SCHEMA,
    REGISTRY_V2_RECORD_SCHEMA,
];

/// Current durable schemas. `current` is exactly synonymous with writable roles.
pub static CURRENT_RECORD_SCHEMAS: [RecordSchema<'static>; CURRENT_RECORD_SCHEMA_COUNT] =
    WRITABLE_RECORD_SCHEMAS;

static READABLE_RECORD_REGISTRY: OnceLock<RecordRegistry<'static>> = OnceLock::new();
static WRITABLE_RECORD_REGISTRY: OnceLock<RecordRegistry<'static>> = OnceLock::new();

/// Returns the closed registry accepted while opening or migrating storage.
#[must_use]
pub fn readable_record_registry() -> RecordRegistry<'static> {
    *READABLE_RECORD_REGISTRY.get_or_init(|| {
        RecordRegistry::new(&READABLE_RECORD_SCHEMAS)
            .expect("the generated readable durable registry is unique and bounded")
    })
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
    *WRITABLE_RECORD_REGISTRY.get_or_init(|| {
        RecordRegistry::new(&WRITABLE_RECORD_SCHEMAS)
            .expect("the generated writable durable registry is unique and bounded")
    })
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
    current_record_schema(record_type).map(|schema| {
        crate::envelope::maximum_encoded_compact_record_bytes(schema, schema.max_payload_bytes())
            .expect("generated compact record bound is valid")
    })
}

/// Returns the immutable digest of every readable compact tag/revision binding.
#[must_use]
pub fn record_registry_digest() -> SchemaHash {
    let mut schemas = READABLE_RECORD_SCHEMAS.iter().collect::<Vec<_>>();
    schemas.sort_by_key(|schema| (schema.compact_tag(), schema.schema_revision()));
    let mut canonical = Vec::with_capacity(schemas.len() * 48);
    canonical.extend_from_slice(b"riffdb-compact-record-registry-v2\0");
    canonical.extend_from_slice(
        &u16::try_from(schemas.len())
            .expect("registry count fits u16")
            .to_be_bytes(),
    );
    for schema in schemas {
        canonical.push(schema.compact_tag());
        canonical.extend_from_slice(&schema.schema_revision().to_be_bytes());
        canonical.extend_from_slice(
            &u16::try_from(schema.record_type().len())
                .expect("record type bound fits u16")
                .to_be_bytes(),
        );
        canonical.extend_from_slice(schema.record_type().as_bytes());
        canonical.extend_from_slice(schema.schema_hash().as_bytes());
    }
    hash_schema(&canonical)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_registry_record_is_itself_registered_and_canonical() {
        let message = v1::StoredRecordRegistryV2 {
            registry_digest: record_registry_digest().as_bytes().to_vec(),
        };
        let encoded = encode_current_message(&message).expect("registry record encodes");
        let typed = decode_readable_message::<v1::StoredRecordRegistryV2>(&encoded)
            .expect("sealed current message decodes through the typed path");
        assert_eq!(typed, message);
        let decoded = readable_record_registry()
            .decode(&encoded)
            .expect("registry record decodes");
        assert_eq!(
            decoded.record_type(),
            "riffdb.storage.v1.StoredRecordRegistryV2"
        );
        assert_eq!(decoded.payload(), message.encode_to_vec());
    }
}
