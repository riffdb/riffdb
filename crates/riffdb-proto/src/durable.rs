//! Closed current-schema registry for durable semantic records.
//!
//! This module validates the canonical Protobuf wire form. The storage-owned
//! codec remains responsible for semantic DTO reconstruction and validation.

use prost::Message;
use riffdb_types::{SchemaHash, hash_schema};
use std::collections::HashMap;
use std::sync::OnceLock;

use crate::durable_wire::DurablePreflightError;
use crate::envelope::{PayloadValidationError, RecordRegistry, RecordSchema};
use crate::storage::v1;

/// Number of durable semantic payload tuples accepted while opening or migrating storage.
pub const READABLE_RECORD_SCHEMA_COUNT: usize = 111;
/// Number of durable semantic roles accepted for current writes.
pub const WRITABLE_RECORD_SCHEMA_COUNT: usize = 88;
/// Number of durable semantic roles accepted for current writes.
pub const CURRENT_RECORD_SCHEMA_COUNT: usize = WRITABLE_RECORD_SCHEMA_COUNT;

const LEGACY_RECORD_SCHEMA_COUNT: usize = 29;
const LEGACY_SCHEMA_HASH_BYTES: &[u8; LEGACY_RECORD_SCHEMA_COUNT * 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-schema-hashes.bin"
));
const LEGACY_RECORD_BOUND_BYTES: &[u8; LEGACY_RECORD_SCHEMA_COUNT * 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-record-bounds.bin"
));
const INDEX_V2_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-index-v2-schema-hash.bin"
));
const INDEX_V2_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-index-v2-record-bound.bin"
));
const REGISTRY_V2_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-registry-v2-schema-hash.bin"
));
const REGISTRY_V2_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-registry-v2-record-bound.bin"
));
const EVENT_REFERENCE_V2_SCHEMA_HASH_BYTES: &[u8; 64] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-event-reference-v2-schema-hashes.bin"
));
const EVENT_REFERENCE_V2_RECORD_BOUND_BYTES: &[u8; 16] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-event-reference-v2-record-bounds.bin"
));
const INDEX_GENERATION_V2_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-index-generation-v2-schema-hash.bin"
));
const INDEX_GENERATION_V2_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-index-generation-v2-record-bound.bin"
));
const HISTORY_INCARNATION_V1_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-history-incarnation-v1-schema-hash.bin"
));
const HISTORY_INCARNATION_V1_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-history-incarnation-v1-record-bound.bin"
));
const SERVICE_AUDIT_REQUEST_INDEX_V1_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-service-audit-request-index-v1-schema-hash.bin"
));
const SERVICE_AUDIT_REQUEST_INDEX_V1_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-service-audit-request-index-v1-record-bound.bin"
));
const EVENT_ROUTE_V1_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-event-route-v1-schema-hash.bin"
));
const EVENT_ROUTE_V1_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-event-route-v1-record-bound.bin"
));
const ENTITY_REFERENCE_V3_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-entity-reference-v3-schema-hash.bin"
));
const ENTITY_REFERENCE_V3_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-entity-reference-v3-record-bound.bin"
));
const MIGRATION_V1_SCHEMA_HASH_BYTES: &[u8; 128] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-migration-v1-schema-hashes.bin"
));
const MIGRATION_V1_RECORD_BOUND_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-migration-v1-record-bounds.bin"
));
const CAPABILITY_V2_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-capability-v2-schema-hash.bin"
));
const CAPABILITY_V2_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-capability-v2-record-bound.bin"
));
const CAPABILITY_V3_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-capability-v3-schema-hash.bin"
));
const CAPABILITY_V3_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-capability-v3-record-bound.bin"
));
const CAPABILITY_V4_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-capability-v4-schema-hash.bin"
));
const CAPABILITY_V4_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-capability-v4-record-bound.bin"
));
const EVENT_V2_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-event-v2-schema-hash.bin"
));
const EVENT_V2_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-event-v2-record-bound.bin"
));
const CAPABILITY_V5_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-capability-v5-schema-hash.bin"
));
const CAPABILITY_V5_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-capability-v5-record-bound.bin"
));
const CAPABILITY_V6_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-capability-v6-schema-hash.bin"
));
const CAPABILITY_V6_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-capability-v6-record-bound.bin"
));
const CAPABILITY_V7_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-capability-v7-schema-hash.bin"
));
const CAPABILITY_V7_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-capability-v7-record-bound.bin"
));
const CAPABILITY_V8_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-capability-v8-schema-hash.bin"
));
const CAPABILITY_V8_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-capability-v8-record-bound.bin"
));
const VALIDATED_PREFIX_CHECKPOINT_V1_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-validated-prefix-checkpoint-v1-schema-hash.bin"
));
const VALIDATED_PREFIX_CHECKPOINT_V1_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-validated-prefix-checkpoint-v1-record-bound.bin"
));
const RETENTION_WATERMARK_V1_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-retention-watermark-v1-schema-hash.bin"
));
const RETENTION_WATERMARK_V1_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-retention-watermark-v1-record-bound.bin"
));
const RETENTION_HOLDS_V1_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-retention-holds-v1-schema-hash.bin"
));
const RETENTION_HOLDS_V1_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-retention-holds-v1-record-bound.bin"
));
const HISTORY_TOMBSTONE_V1_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-history-tombstone-v1-schema-hash.bin"
));
const HISTORY_TOMBSTONE_V1_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-history-tombstone-v1-record-bound.bin"
));
const RETENTION_ADMINISTRATION_V1_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-retention-administration-v1-schema-hash.bin"
));
const RETENTION_ADMINISTRATION_V1_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-retention-administration-v1-record-bound.bin"
));
const REACTIVE_CONSUMER_V1_SCHEMA_HASH_BYTES: &[u8; 128] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-reactive-consumer-v1-schema-hashes.bin"
));
const REACTIVE_CONSUMER_V1_RECORD_BOUND_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-reactive-consumer-v1-record-bounds.bin"
));
const SERVICE_AUDIT_V2_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-service-audit-v2-schema-hash.bin"
));
const SERVICE_AUDIT_V2_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-service-audit-v2-record-bound.bin"
));
const SERVICE_AUDIT_V3_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-service-audit-v3-schema-hash.bin"
));
const SERVICE_AUDIT_V3_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-service-audit-v3-record-bound.bin"
));
const REPLICATION_ADMINISTRATION_V1_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-replication-administration-v1-schema-hash.bin"
));
const REPLICATION_ADMINISTRATION_V1_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-replication-administration-v1-record-bound.bin"
));
const CONTEXTUAL_CAUSATION_V2_SCHEMA_HASH_BYTES: &[u8; 128] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-contextual-causation-v2-schema-hashes.bin"
));
const CONTEXTUAL_CAUSATION_V2_RECORD_BOUND_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-contextual-causation-v2-record-bounds.bin"
));
const COMMAND_CAPSULE_V1_SCHEMA_HASH_BYTES: &[u8; 96] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-command-capsule-v1-schema-hashes.bin"
));
const COMMAND_CAPSULE_V1_RECORD_BOUND_BYTES: &[u8; 24] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-command-capsule-v1-record-bounds.bin"
));
const COMMAND_SEGMENT_V1_SCHEMA_HASH_BYTES: &[u8; 96] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-command-segment-v1-schema-hashes.bin"
));
const COMMAND_SEGMENT_V1_RECORD_BOUND_BYTES: &[u8; 24] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-command-segment-v1-record-bounds.bin"
));
const WORKFLOW_SERVICE_VALUES_V3_SCHEMA_HASH_BYTES: &[u8; 160] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-workflow-service-values-v3-schema-hashes.bin"
));
const WORKFLOW_SERVICE_VALUES_V3_RECORD_BOUND_BYTES: &[u8; 40] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-workflow-service-values-v3-record-bounds.bin"
));
const INSTALLATION_V1_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-installation-v1-schema-hash.bin"
));
const INSTALLATION_V1_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-installation-v1-record-bound.bin"
));
const EXPORT_V1_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-export-v1-schema-hash.bin"
));
const EXPORT_V1_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-export-v1-record-bound.bin"
));
const EVENT_POLICY_COMMAND_AUTHORITY_V5_SCHEMA_HASH_BYTES: &[u8; 64] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-event-policy-command-authority-v5-schema-hashes.bin"
));
const EVENT_POLICY_COMMAND_AUTHORITY_V5_RECORD_BOUND_BYTES: &[u8; 16] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-event-policy-command-authority-v5-record-bounds.bin"
));
const CORRELATED_INDEX_WORK_COMMAND_AUTHORITY_V6_SCHEMA_HASH_BYTES: &[u8] =
    include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/fixtures/durable-correlated-index-work-command-authority-v6-schema-hashes.bin"
    ));
const CORRELATED_INDEX_WORK_COMMAND_AUTHORITY_V6_RECORD_BOUND_BYTES: &[u8] =
    include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/fixtures/durable-correlated-index-work-command-authority-v6-record-bounds.bin"
    ));
const COMMAND_PREFIX_AUTHORITY_V7_SCHEMA_HASH_BYTES: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-command-prefix-authority-v7-schema-hashes.bin"
));
const COMMAND_PREFIX_AUTHORITY_V7_RECORD_BOUND_BYTES: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-command-prefix-authority-v7-record-bounds.bin"
));
const ENTITY_TRANSITIONS_V4_SCHEMA_HASH_BYTES: &[u8; 160] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-entity-transitions-v4-schema-hashes.bin"
));
const ENTITY_TRANSITIONS_V4_RECORD_BOUND_BYTES: &[u8; 40] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-entity-transitions-v4-record-bounds.bin"
));
const VECTOR_EVIDENCE_V1_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-vector-evidence-v1-schema-hash.bin"
));
const VECTOR_EVIDENCE_V1_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-vector-evidence-v1-record-bound.bin"
));
const VECTOR_OBSERVATION_V1_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-vector-observation-v1-schema-hash.bin"
));
const VECTOR_OBSERVATION_V1_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-vector-observation-v1-record-bound.bin"
));
const VECTOR_HEALTH_OBSERVATION_V1_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-vector-health-observation-v1-schema-hash.bin"
));
const VECTOR_HEALTH_OBSERVATION_V1_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-vector-health-observation-v1-record-bound.bin"
));
const VECTOR_PROJECTION_CONTROL_V1_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-vector-projection-control-v1-schema-hash.bin"
));
const VECTOR_PROJECTION_CONTROL_V1_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-vector-projection-control-v1-record-bound.bin"
));
const VECTOR_EVIDENCE_INDEX_V1_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-vector-evidence-index-v1-schema-hash.bin"
));
const VECTOR_EVIDENCE_INDEX_V1_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-vector-evidence-index-v1-record-bound.bin"
));
const CLEAN_CLOSE_LIFECYCLE_V1_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-clean-close-lifecycle-v1-schema-hash.bin"
));
const CLEAN_CLOSE_LIFECYCLE_V1_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-clean-close-lifecycle-v1-record-bound.bin"
));
const COLUMNAR_PROJECTION_CONTROL_V1_SCHEMA_HASH_BYTES: &[u8; 32] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-columnar-projection-control-v1-schema-hash.bin"
));
const COLUMNAR_PROJECTION_CONTROL_V1_RECORD_BOUND_BYTES: &[u8; 8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-columnar-projection-control-v1-record-bound.bin"
));
const PRE_WP280_CAPABILITY_SCHEMA_HASH: SchemaHash = SchemaHash::from_bytes([
    0xcb, 0x42, 0xc4, 0xeb, 0xbc, 0xe8, 0x28, 0x01, 0x23, 0xf8, 0xb3, 0x4d, 0x4d, 0xcd, 0xe7, 0x4c,
    0xa9, 0x48, 0x34, 0x06, 0x84, 0x75, 0x31, 0xf3, 0x4d, 0x5f, 0xb3, 0xf1, 0x8d, 0x40, 0xb3, 0x42,
]);
const PRE_WP416_CAPABILITY_SCHEMA_HASH: SchemaHash = SchemaHash::from_bytes([
    0xde, 0xe2, 0x39, 0x8e, 0xbb, 0xc7, 0x18, 0x24, 0x47, 0x1f, 0xe5, 0xa5, 0xf9, 0x6f, 0xcb, 0xeb,
    0xdd, 0xe0, 0x9e, 0x51, 0x1f, 0xe6, 0xc1, 0x15, 0x31, 0xc2, 0xb3, 0x02, 0x10, 0xf9, 0xe6, 0xbf,
]);
const PRE_WP416_CAPABILITY_TOKEN_LOOKUP_SCHEMA_HASH: SchemaHash = SchemaHash::from_bytes([
    0xe0, 0x06, 0x96, 0xf3, 0xd2, 0xc1, 0x10, 0xc5, 0xb2, 0xb9, 0x96, 0x85, 0xe7, 0x98, 0x7f, 0x72,
    0x04, 0xd2, 0xf9, 0x57, 0x6f, 0x8b, 0x41, 0xc5, 0xfc, 0xd5, 0xa8, 0x94, 0xc7, 0x46, 0xbc, 0x86,
]);
const PRE_WP416_CAPABILITY_BOOTSTRAP_SCHEMA_HASH: SchemaHash = SchemaHash::from_bytes([
    0x12, 0x04, 0xe2, 0x70, 0x16, 0x96, 0x88, 0x24, 0x4b, 0xc4, 0x89, 0x07, 0x1d, 0xb7, 0x0c, 0x7d,
    0x24, 0x81, 0x11, 0x1a, 0x97, 0x0d, 0x5a, 0x9d, 0xa4, 0x20, 0xad, 0x7b, 0x93, 0x34, 0xe4, 0xea,
]);
const PRE_WP416_CAPABILITY_ADMINISTRATION_SCHEMA_HASH: SchemaHash = SchemaHash::from_bytes([
    0x28, 0xd4, 0xa9, 0xd5, 0xf6, 0x3e, 0xb9, 0xf5, 0xba, 0xcb, 0x20, 0x42, 0xaf, 0x50, 0xd4, 0x7e,
    0xa3, 0x95, 0x32, 0xbc, 0x2e, 0xf6, 0x0c, 0x3d, 0xc5, 0x2a, 0x45, 0x0c, 0xc7, 0xb4, 0x9a, 0x46,
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
        .with_compact_identity(
            if $index >= 17 && $index <= 20 {
                (30 + $index) as u8
            } else {
                ($index + 1) as u8
            },
            1,
        )
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
    // Revision 3 was briefly emitted with the later installation field under
    // the same compact identity. Accept that exact additive payload here so
    // affected pre-alpha databases can be decoded and rewritten as V3. New
    // V2 writes remain migration-only in the semantic codec.
    preflight_payload::<74>,
    validate_payload::<74, v1::CapabilityRecordV3>,
)
.with_compact_identity(18, 3);

const CAPABILITY_V3_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.CapabilityRecordV3",
    SchemaHash::from_bytes(*CAPABILITY_V3_SCHEMA_HASH_BYTES),
    u32::from_be_bytes([
        CAPABILITY_V3_RECORD_BOUND_BYTES[0],
        CAPABILITY_V3_RECORD_BOUND_BYTES[1],
        CAPABILITY_V3_RECORD_BOUND_BYTES[2],
        CAPABILITY_V3_RECORD_BOUND_BYTES[3],
    ]) as usize,
    u32::from_be_bytes([
        CAPABILITY_V3_RECORD_BOUND_BYTES[4],
        CAPABILITY_V3_RECORD_BOUND_BYTES[5],
        CAPABILITY_V3_RECORD_BOUND_BYTES[6],
        CAPABILITY_V3_RECORD_BOUND_BYTES[7],
    ]) as usize,
    preflight_payload::<74>,
    validate_payload::<74, v1::CapabilityRecordV3>,
)
.with_compact_identity(18, 4);

const CAPABILITY_V4_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.CapabilityRecordV4",
    SchemaHash::from_bytes(*CAPABILITY_V4_SCHEMA_HASH_BYTES),
    u32::from_be_bytes([
        CAPABILITY_V4_RECORD_BOUND_BYTES[0],
        CAPABILITY_V4_RECORD_BOUND_BYTES[1],
        CAPABILITY_V4_RECORD_BOUND_BYTES[2],
        CAPABILITY_V4_RECORD_BOUND_BYTES[3],
    ]) as usize,
    u32::from_be_bytes([
        CAPABILITY_V4_RECORD_BOUND_BYTES[4],
        CAPABILITY_V4_RECORD_BOUND_BYTES[5],
        CAPABILITY_V4_RECORD_BOUND_BYTES[6],
        CAPABILITY_V4_RECORD_BOUND_BYTES[7],
    ]) as usize,
    preflight_payload::<75>,
    validate_payload::<75, v1::CapabilityRecordV4>,
)
.with_compact_identity(18, 5);

const DURABLE_EVENT_V2_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.StoredDurableEventV2",
    SchemaHash::from_bytes(*EVENT_V2_SCHEMA_HASH_BYTES),
    u32::from_be_bytes([
        EVENT_V2_RECORD_BOUND_BYTES[0],
        EVENT_V2_RECORD_BOUND_BYTES[1],
        EVENT_V2_RECORD_BOUND_BYTES[2],
        EVENT_V2_RECORD_BOUND_BYTES[3],
    ]) as usize,
    u32::from_be_bytes([
        EVENT_V2_RECORD_BOUND_BYTES[4],
        EVENT_V2_RECORD_BOUND_BYTES[5],
        EVENT_V2_RECORD_BOUND_BYTES[6],
        EVENT_V2_RECORD_BOUND_BYTES[7],
    ]) as usize,
    preflight_payload::<76>,
    validate_payload::<76, v1::StoredDurableEventV2>,
)
.with_compact_identity(14, 2);

const CAPABILITY_V5_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.CapabilityRecordV5",
    SchemaHash::from_bytes(*CAPABILITY_V5_SCHEMA_HASH_BYTES),
    u32::from_be_bytes([
        CAPABILITY_V5_RECORD_BOUND_BYTES[0],
        CAPABILITY_V5_RECORD_BOUND_BYTES[1],
        CAPABILITY_V5_RECORD_BOUND_BYTES[2],
        CAPABILITY_V5_RECORD_BOUND_BYTES[3],
    ]) as usize,
    u32::from_be_bytes([
        CAPABILITY_V5_RECORD_BOUND_BYTES[4],
        CAPABILITY_V5_RECORD_BOUND_BYTES[5],
        CAPABILITY_V5_RECORD_BOUND_BYTES[6],
        CAPABILITY_V5_RECORD_BOUND_BYTES[7],
    ]) as usize,
    preflight_payload::<77>,
    validate_payload::<77, v1::CapabilityRecordV5>,
)
.with_compact_identity(18, 6);

const CAPABILITY_V6_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.CapabilityRecordV6",
    SchemaHash::from_bytes(*CAPABILITY_V6_SCHEMA_HASH_BYTES),
    u32::from_be_bytes([
        CAPABILITY_V6_RECORD_BOUND_BYTES[0],
        CAPABILITY_V6_RECORD_BOUND_BYTES[1],
        CAPABILITY_V6_RECORD_BOUND_BYTES[2],
        CAPABILITY_V6_RECORD_BOUND_BYTES[3],
    ]) as usize,
    u32::from_be_bytes([
        CAPABILITY_V6_RECORD_BOUND_BYTES[4],
        CAPABILITY_V6_RECORD_BOUND_BYTES[5],
        CAPABILITY_V6_RECORD_BOUND_BYTES[6],
        CAPABILITY_V6_RECORD_BOUND_BYTES[7],
    ]) as usize,
    preflight_payload::<81>,
    validate_payload::<81, v1::CapabilityRecordV6>,
)
.with_compact_identity(18, 7);

const CAPABILITY_V7_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.CapabilityRecordV7",
    SchemaHash::from_bytes(*CAPABILITY_V7_SCHEMA_HASH_BYTES),
    u32::from_be_bytes([
        CAPABILITY_V7_RECORD_BOUND_BYTES[0],
        CAPABILITY_V7_RECORD_BOUND_BYTES[1],
        CAPABILITY_V7_RECORD_BOUND_BYTES[2],
        CAPABILITY_V7_RECORD_BOUND_BYTES[3],
    ]) as usize,
    u32::from_be_bytes([
        CAPABILITY_V7_RECORD_BOUND_BYTES[4],
        CAPABILITY_V7_RECORD_BOUND_BYTES[5],
        CAPABILITY_V7_RECORD_BOUND_BYTES[6],
        CAPABILITY_V7_RECORD_BOUND_BYTES[7],
    ]) as usize,
    preflight_payload::<82>,
    validate_payload::<82, v1::CapabilityRecordV7>,
)
.with_compact_identity(18, 8);

const CAPABILITY_V8_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.CapabilityRecordV8",
    SchemaHash::from_bytes(*CAPABILITY_V8_SCHEMA_HASH_BYTES),
    u32::from_be_bytes([
        CAPABILITY_V8_RECORD_BOUND_BYTES[0],
        CAPABILITY_V8_RECORD_BOUND_BYTES[1],
        CAPABILITY_V8_RECORD_BOUND_BYTES[2],
        CAPABILITY_V8_RECORD_BOUND_BYTES[3],
    ]) as usize,
    u32::from_be_bytes([
        CAPABILITY_V8_RECORD_BOUND_BYTES[4],
        CAPABILITY_V8_RECORD_BOUND_BYTES[5],
        CAPABILITY_V8_RECORD_BOUND_BYTES[6],
        CAPABILITY_V8_RECORD_BOUND_BYTES[7],
    ]) as usize,
    preflight_payload::<83>,
    validate_payload::<83, v1::CapabilityRecordV8>,
)
.with_compact_identity(18, 9);

const VALIDATED_PREFIX_CHECKPOINT_V1_RECORD_SCHEMA: RecordSchema<'static> =
    RecordSchema::new_current(
        "riffdb.storage.v1.StoredValidatedPrefixCheckpointV1",
        SchemaHash::from_bytes(*VALIDATED_PREFIX_CHECKPOINT_V1_SCHEMA_HASH_BYTES),
        u32::from_be_bytes([
            VALIDATED_PREFIX_CHECKPOINT_V1_RECORD_BOUND_BYTES[0],
            VALIDATED_PREFIX_CHECKPOINT_V1_RECORD_BOUND_BYTES[1],
            VALIDATED_PREFIX_CHECKPOINT_V1_RECORD_BOUND_BYTES[2],
            VALIDATED_PREFIX_CHECKPOINT_V1_RECORD_BOUND_BYTES[3],
        ]) as usize,
        u32::from_be_bytes([
            VALIDATED_PREFIX_CHECKPOINT_V1_RECORD_BOUND_BYTES[4],
            VALIDATED_PREFIX_CHECKPOINT_V1_RECORD_BOUND_BYTES[5],
            VALIDATED_PREFIX_CHECKPOINT_V1_RECORD_BOUND_BYTES[6],
            VALIDATED_PREFIX_CHECKPOINT_V1_RECORD_BOUND_BYTES[7],
        ]) as usize,
        preflight_payload::<43>,
        validate_payload::<43, v1::StoredValidatedPrefixCheckpointV1>,
    )
    .with_compact_identity(38, 1);

const RETENTION_WATERMARK_V1_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.StoredRetentionWatermarkV1",
    SchemaHash::from_bytes(*RETENTION_WATERMARK_V1_SCHEMA_HASH_BYTES),
    u32::from_be_bytes([
        RETENTION_WATERMARK_V1_RECORD_BOUND_BYTES[0],
        RETENTION_WATERMARK_V1_RECORD_BOUND_BYTES[1],
        RETENTION_WATERMARK_V1_RECORD_BOUND_BYTES[2],
        RETENTION_WATERMARK_V1_RECORD_BOUND_BYTES[3],
    ]) as usize,
    u32::from_be_bytes([
        RETENTION_WATERMARK_V1_RECORD_BOUND_BYTES[4],
        RETENTION_WATERMARK_V1_RECORD_BOUND_BYTES[5],
        RETENTION_WATERMARK_V1_RECORD_BOUND_BYTES[6],
        RETENTION_WATERMARK_V1_RECORD_BOUND_BYTES[7],
    ]) as usize,
    preflight_payload::<44>,
    validate_payload::<44, v1::StoredRetentionWatermarkV1>,
)
.with_compact_identity(39, 1);

const RETENTION_HOLDS_V1_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.StoredRetentionHoldsV1",
    SchemaHash::from_bytes(*RETENTION_HOLDS_V1_SCHEMA_HASH_BYTES),
    u32::from_be_bytes([
        RETENTION_HOLDS_V1_RECORD_BOUND_BYTES[0],
        RETENTION_HOLDS_V1_RECORD_BOUND_BYTES[1],
        RETENTION_HOLDS_V1_RECORD_BOUND_BYTES[2],
        RETENTION_HOLDS_V1_RECORD_BOUND_BYTES[3],
    ]) as usize,
    u32::from_be_bytes([
        RETENTION_HOLDS_V1_RECORD_BOUND_BYTES[4],
        RETENTION_HOLDS_V1_RECORD_BOUND_BYTES[5],
        RETENTION_HOLDS_V1_RECORD_BOUND_BYTES[6],
        RETENTION_HOLDS_V1_RECORD_BOUND_BYTES[7],
    ]) as usize,
    preflight_payload::<45>,
    validate_payload::<45, v1::StoredRetentionHoldsV1>,
)
.with_compact_identity(40, 1);

const HISTORY_TOMBSTONE_V1_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.StoredHistoryTombstoneV1",
    SchemaHash::from_bytes(*HISTORY_TOMBSTONE_V1_SCHEMA_HASH_BYTES),
    u32::from_be_bytes([
        HISTORY_TOMBSTONE_V1_RECORD_BOUND_BYTES[0],
        HISTORY_TOMBSTONE_V1_RECORD_BOUND_BYTES[1],
        HISTORY_TOMBSTONE_V1_RECORD_BOUND_BYTES[2],
        HISTORY_TOMBSTONE_V1_RECORD_BOUND_BYTES[3],
    ]) as usize,
    u32::from_be_bytes([
        HISTORY_TOMBSTONE_V1_RECORD_BOUND_BYTES[4],
        HISTORY_TOMBSTONE_V1_RECORD_BOUND_BYTES[5],
        HISTORY_TOMBSTONE_V1_RECORD_BOUND_BYTES[6],
        HISTORY_TOMBSTONE_V1_RECORD_BOUND_BYTES[7],
    ]) as usize,
    preflight_payload::<46>,
    validate_payload::<46, v1::StoredHistoryTombstoneV1>,
)
.with_compact_identity(41, 1);

const RETENTION_ADMINISTRATION_V1_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.StoredRetentionAdministrationV1",
    SchemaHash::from_bytes(*RETENTION_ADMINISTRATION_V1_SCHEMA_HASH_BYTES),
    u32::from_be_bytes([
        RETENTION_ADMINISTRATION_V1_RECORD_BOUND_BYTES[0],
        RETENTION_ADMINISTRATION_V1_RECORD_BOUND_BYTES[1],
        RETENTION_ADMINISTRATION_V1_RECORD_BOUND_BYTES[2],
        RETENTION_ADMINISTRATION_V1_RECORD_BOUND_BYTES[3],
    ]) as usize,
    u32::from_be_bytes([
        RETENTION_ADMINISTRATION_V1_RECORD_BOUND_BYTES[4],
        RETENTION_ADMINISTRATION_V1_RECORD_BOUND_BYTES[5],
        RETENTION_ADMINISTRATION_V1_RECORD_BOUND_BYTES[6],
        RETENTION_ADMINISTRATION_V1_RECORD_BOUND_BYTES[7],
    ]) as usize,
    preflight_payload::<47>,
    validate_payload::<47, v1::StoredRetentionAdministrationV1>,
)
.with_compact_identity(42, 1);

const fn reactive_consumer_schema_hash(index: usize) -> SchemaHash {
    let mut bytes = [0_u8; 32];
    let mut offset = 0;
    while offset < bytes.len() {
        bytes[offset] = REACTIVE_CONSUMER_V1_SCHEMA_HASH_BYTES[index * 32 + offset];
        offset += 1;
    }
    SchemaHash::from_bytes(bytes)
}

const fn reactive_consumer_record_bound(index: usize, offset: usize) -> usize {
    let start = index * 8 + offset;
    u32::from_be_bytes([
        REACTIVE_CONSUMER_V1_RECORD_BOUND_BYTES[start],
        REACTIVE_CONSUMER_V1_RECORD_BOUND_BYTES[start + 1],
        REACTIVE_CONSUMER_V1_RECORD_BOUND_BYTES[start + 2],
        REACTIVE_CONSUMER_V1_RECORD_BOUND_BYTES[start + 3],
    ]) as usize
}

macro_rules! reactive_consumer_schema {
    ($index:literal, $name:literal, $message:ty, $compact_tag:literal) => {
        RecordSchema::new_current(
            concat!("riffdb.storage.v1.", $name),
            reactive_consumer_schema_hash($index),
            reactive_consumer_record_bound($index, 0),
            reactive_consumer_record_bound($index, 4),
            preflight_payload::<{ 48 + $index }>,
            validate_payload::<{ 48 + $index }, $message>,
        )
        .with_compact_identity($compact_tag, 1)
    };
}

const REACTIVE_MODULE_V1_RECORD_SCHEMA: RecordSchema<'static> =
    reactive_consumer_schema!(0, "StoredReactiveModuleV1", v1::StoredReactiveModuleV1, 43);
const REACTIVE_MODULE_ADMINISTRATION_V1_RECORD_SCHEMA: RecordSchema<'static> = reactive_consumer_schema!(
    1,
    "StoredReactiveModuleAdministrationV1",
    v1::StoredReactiveModuleAdministrationV1,
    44
);
const EVENT_CONSUMER_V1_RECORD_SCHEMA: RecordSchema<'static> =
    reactive_consumer_schema!(2, "StoredEventConsumerV1", v1::StoredEventConsumerV1, 45);
const EVENT_CONSUMER_DELIVERY_V1_RECORD_SCHEMA: RecordSchema<'static> = reactive_consumer_schema!(
    3,
    "StoredEventConsumerDeliveryV1",
    v1::StoredEventConsumerDeliveryV1,
    46
);

const SERVICE_AUDIT_V2_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.ServiceAuditRecordV2",
    SchemaHash::from_bytes(*SERVICE_AUDIT_V2_SCHEMA_HASH_BYTES),
    u32::from_be_bytes([
        SERVICE_AUDIT_V2_RECORD_BOUND_BYTES[0],
        SERVICE_AUDIT_V2_RECORD_BOUND_BYTES[1],
        SERVICE_AUDIT_V2_RECORD_BOUND_BYTES[2],
        SERVICE_AUDIT_V2_RECORD_BOUND_BYTES[3],
    ]) as usize,
    u32::from_be_bytes([
        SERVICE_AUDIT_V2_RECORD_BOUND_BYTES[4],
        SERVICE_AUDIT_V2_RECORD_BOUND_BYTES[5],
        SERVICE_AUDIT_V2_RECORD_BOUND_BYTES[6],
        SERVICE_AUDIT_V2_RECORD_BOUND_BYTES[7],
    ]) as usize,
    preflight_payload::<52>,
    validate_payload::<52, v1::ServiceAuditRecordV2>,
)
.with_compact_identity(22, 2);
const SERVICE_AUDIT_V3_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.ServiceAuditRecordV3",
    SchemaHash::from_bytes(*SERVICE_AUDIT_V3_SCHEMA_HASH_BYTES),
    u32::from_be_bytes([
        SERVICE_AUDIT_V3_RECORD_BOUND_BYTES[0],
        SERVICE_AUDIT_V3_RECORD_BOUND_BYTES[1],
        SERVICE_AUDIT_V3_RECORD_BOUND_BYTES[2],
        SERVICE_AUDIT_V3_RECORD_BOUND_BYTES[3],
    ]) as usize,
    u32::from_be_bytes([
        SERVICE_AUDIT_V3_RECORD_BOUND_BYTES[4],
        SERVICE_AUDIT_V3_RECORD_BOUND_BYTES[5],
        SERVICE_AUDIT_V3_RECORD_BOUND_BYTES[6],
        SERVICE_AUDIT_V3_RECORD_BOUND_BYTES[7],
    ]) as usize,
    preflight_payload::<104>,
    validate_payload::<104, v1::ServiceAuditRecordV3>,
)
.with_compact_identity(22, 3);
const REPLICATION_ADMINISTRATION_V1_RECORD_SCHEMA: RecordSchema<'static> =
    RecordSchema::new_current(
        "riffdb.storage.v1.StoredReplicationAdministrationV1",
        SchemaHash::from_bytes(*REPLICATION_ADMINISTRATION_V1_SCHEMA_HASH_BYTES),
        u32::from_be_bytes([
            REPLICATION_ADMINISTRATION_V1_RECORD_BOUND_BYTES[0],
            REPLICATION_ADMINISTRATION_V1_RECORD_BOUND_BYTES[1],
            REPLICATION_ADMINISTRATION_V1_RECORD_BOUND_BYTES[2],
            REPLICATION_ADMINISTRATION_V1_RECORD_BOUND_BYTES[3],
        ]) as usize,
        u32::from_be_bytes([
            REPLICATION_ADMINISTRATION_V1_RECORD_BOUND_BYTES[4],
            REPLICATION_ADMINISTRATION_V1_RECORD_BOUND_BYTES[5],
            REPLICATION_ADMINISTRATION_V1_RECORD_BOUND_BYTES[6],
            REPLICATION_ADMINISTRATION_V1_RECORD_BOUND_BYTES[7],
        ]) as usize,
        preflight_payload::<105>,
        validate_payload::<105, v1::StoredReplicationAdministrationV1>,
    )
    .with_compact_identity(75, 1);

const fn contextual_causation_v2_schema_hash(index: usize) -> SchemaHash {
    let mut bytes = [0_u8; 32];
    let mut offset = 0;
    while offset < bytes.len() {
        bytes[offset] = CONTEXTUAL_CAUSATION_V2_SCHEMA_HASH_BYTES[index * 32 + offset];
        offset += 1;
    }
    SchemaHash::from_bytes(bytes)
}

const fn contextual_causation_v2_record_bound(index: usize, offset: usize) -> usize {
    let start = index * 8 + offset;
    u32::from_be_bytes([
        CONTEXTUAL_CAUSATION_V2_RECORD_BOUND_BYTES[start],
        CONTEXTUAL_CAUSATION_V2_RECORD_BOUND_BYTES[start + 1],
        CONTEXTUAL_CAUSATION_V2_RECORD_BOUND_BYTES[start + 2],
        CONTEXTUAL_CAUSATION_V2_RECORD_BOUND_BYTES[start + 3],
    ]) as usize
}

macro_rules! contextual_causation_v2_schema {
    ($index:literal, $name:literal, $message:ty, $compact_tag:literal) => {
        RecordSchema::new_current(
            concat!("riffdb.storage.v1.", $name),
            contextual_causation_v2_schema_hash($index),
            contextual_causation_v2_record_bound($index, 0),
            contextual_causation_v2_record_bound($index, 4),
            preflight_payload::<{ 53 + $index }>,
            validate_payload::<{ 53 + $index }, $message>,
        )
        .with_compact_identity($compact_tag, 2)
    };
}

const PENDING_ADMISSION_V2_RECORD_SCHEMA: RecordSchema<'static> = contextual_causation_v2_schema!(
    0,
    "StoredPendingAdmissionV2",
    v1::StoredPendingAdmissionV2,
    11
);
const EXECUTION_FAILED_V2_RECORD_SCHEMA: RecordSchema<'static> = contextual_causation_v2_schema!(
    1,
    "StoredExecutionFailedV2",
    v1::StoredExecutionFailedV2,
    12
);
const OUTCOME_V2_RECORD_SCHEMA: RecordSchema<'static> =
    contextual_causation_v2_schema!(2, "StoredOutcomeV2", v1::StoredOutcomeV2, 13);
const PROVENANCE_V2_RECORD_SCHEMA: RecordSchema<'static> = contextual_causation_v2_schema!(
    3,
    "StoredProvenanceRecordV2",
    v1::StoredProvenanceRecordV2,
    16
);

const fn command_capsule_v1_schema_hash(index: usize) -> SchemaHash {
    let mut bytes = [0_u8; 32];
    let mut offset = 0;
    while offset < bytes.len() {
        bytes[offset] = COMMAND_CAPSULE_V1_SCHEMA_HASH_BYTES[index * 32 + offset];
        offset += 1;
    }
    SchemaHash::from_bytes(bytes)
}

const fn command_capsule_v1_record_bound(index: usize, offset: usize) -> usize {
    let start = index * 8 + offset;
    u32::from_be_bytes([
        COMMAND_CAPSULE_V1_RECORD_BOUND_BYTES[start],
        COMMAND_CAPSULE_V1_RECORD_BOUND_BYTES[start + 1],
        COMMAND_CAPSULE_V1_RECORD_BOUND_BYTES[start + 2],
        COMMAND_CAPSULE_V1_RECORD_BOUND_BYTES[start + 3],
    ]) as usize
}

macro_rules! command_capsule_v1_schema {
    ($index:literal, $name:literal, $message:ty, $compact_tag:literal) => {
        RecordSchema::new_current(
            concat!("riffdb.storage.v1.", $name),
            command_capsule_v1_schema_hash($index),
            command_capsule_v1_record_bound($index, 0),
            command_capsule_v1_record_bound($index, 4),
            preflight_payload::<{ 57 + $index }>,
            validate_payload::<{ 57 + $index }, $message>,
        )
        .with_compact_identity($compact_tag, 1)
    };
}

const COMMAND_CAPSULE_V1_RECORD_SCHEMA: RecordSchema<'static> =
    command_capsule_v1_schema!(0, "StoredCommandCapsuleV1", v1::StoredCommandCapsuleV1, 51);
const COMMAND_LOCATOR_V1_RECORD_SCHEMA: RecordSchema<'static> =
    command_capsule_v1_schema!(1, "StoredCommandLocatorV1", v1::StoredCommandLocatorV1, 52);
const COMMAND_AUDIT_LOCATOR_V1_RECORD_SCHEMA: RecordSchema<'static> = command_capsule_v1_schema!(
    2,
    "StoredCommandAuditLocatorV1",
    v1::StoredCommandAuditLocatorV1,
    53
);

const fn command_segment_v1_schema_hash(index: usize) -> SchemaHash {
    let mut bytes = [0_u8; 32];
    let mut offset = 0;
    while offset < bytes.len() {
        bytes[offset] = COMMAND_SEGMENT_V1_SCHEMA_HASH_BYTES[index * 32 + offset];
        offset += 1;
    }
    SchemaHash::from_bytes(bytes)
}

const fn command_segment_v1_record_bound(index: usize, offset: usize) -> usize {
    let start = index * 8 + offset;
    u32::from_be_bytes([
        COMMAND_SEGMENT_V1_RECORD_BOUND_BYTES[start],
        COMMAND_SEGMENT_V1_RECORD_BOUND_BYTES[start + 1],
        COMMAND_SEGMENT_V1_RECORD_BOUND_BYTES[start + 2],
        COMMAND_SEGMENT_V1_RECORD_BOUND_BYTES[start + 3],
    ]) as usize
}

macro_rules! command_segment_v1_schema {
    ($index:literal, $name:literal, $message:ty, $compact_tag:literal) => {
        RecordSchema::new_current(
            concat!("riffdb.storage.v1.", $name),
            command_segment_v1_schema_hash($index),
            command_segment_v1_record_bound($index, 0),
            command_segment_v1_record_bound($index, 4),
            preflight_payload::<{ 60 + $index }>,
            validate_payload::<{ 60 + $index }, $message>,
        )
        .with_compact_identity($compact_tag, 1)
    };
}

const COMMAND_CAPSULE_V2_RECORD_SCHEMA: RecordSchema<'static> =
    command_segment_v1_schema!(0, "StoredCommandCapsuleV2", v1::StoredCommandCapsuleV2, 54);
const COMMAND_SEGMENT_V1_RECORD_SCHEMA: RecordSchema<'static> =
    command_segment_v1_schema!(1, "StoredCommandSegmentV1", v1::StoredCommandSegmentV1, 55);
const COMMAND_DERIVED_INDEX_CHECKPOINT_V1_RECORD_SCHEMA: RecordSchema<'static> = command_segment_v1_schema!(
    2,
    "StoredCommandDerivedIndexCheckpointV1",
    v1::StoredCommandDerivedIndexCheckpointV1,
    56
);

const fn workflow_service_values_v3_schema_hash(index: usize) -> SchemaHash {
    let mut bytes = [0_u8; 32];
    let mut offset = 0;
    while offset < bytes.len() {
        bytes[offset] = WORKFLOW_SERVICE_VALUES_V3_SCHEMA_HASH_BYTES[index * 32 + offset];
        offset += 1;
    }
    SchemaHash::from_bytes(bytes)
}

const fn workflow_service_values_v3_record_bound(index: usize, offset: usize) -> usize {
    let start = index * 8 + offset;
    u32::from_be_bytes([
        WORKFLOW_SERVICE_VALUES_V3_RECORD_BOUND_BYTES[start],
        WORKFLOW_SERVICE_VALUES_V3_RECORD_BOUND_BYTES[start + 1],
        WORKFLOW_SERVICE_VALUES_V3_RECORD_BOUND_BYTES[start + 2],
        WORKFLOW_SERVICE_VALUES_V3_RECORD_BOUND_BYTES[start + 3],
    ]) as usize
}

macro_rules! workflow_service_values_v3_schema {
    ($index:literal, $name:literal, $message:ty, $compact_tag:literal, $revision:literal) => {
        RecordSchema::new_current(
            concat!("riffdb.storage.v1.", $name),
            workflow_service_values_v3_schema_hash($index),
            workflow_service_values_v3_record_bound($index, 0),
            workflow_service_values_v3_record_bound($index, 4),
            preflight_payload::<{ 63 + $index }>,
            validate_payload::<{ 63 + $index }, $message>,
        )
        .with_compact_identity($compact_tag, $revision)
    };
}

const PENDING_ADMISSION_V3_RECORD_SCHEMA: RecordSchema<'static> = workflow_service_values_v3_schema!(
    0,
    "StoredPendingAdmissionV3",
    v1::StoredPendingAdmissionV3,
    11,
    3
);
const EXECUTION_FAILED_V3_RECORD_SCHEMA: RecordSchema<'static> = workflow_service_values_v3_schema!(
    1,
    "StoredExecutionFailedV3",
    v1::StoredExecutionFailedV3,
    12,
    3
);
const OUTCOME_V3_RECORD_SCHEMA: RecordSchema<'static> =
    workflow_service_values_v3_schema!(2, "StoredOutcomeV3", v1::StoredOutcomeV3, 13, 3);
const COMMAND_CAPSULE_V3_RECORD_SCHEMA: RecordSchema<'static> = workflow_service_values_v3_schema!(
    3,
    "StoredCommandCapsuleV3",
    v1::StoredCommandCapsuleV3,
    54,
    2
);
const COMMAND_SEGMENT_V2_RECORD_SCHEMA: RecordSchema<'static> = workflow_service_values_v3_schema!(
    4,
    "StoredCommandSegmentV2",
    v1::StoredCommandSegmentV2,
    55,
    2
);

const APPLICATION_INSTALLATION_CAMPAIGN_V1_RECORD_SCHEMA: RecordSchema<'static> =
    RecordSchema::new_current(
        "riffdb.storage.v1.StoredApplicationInstallationCampaignV1",
        SchemaHash::from_bytes(*INSTALLATION_V1_SCHEMA_HASH_BYTES),
        u32::from_be_bytes([
            INSTALLATION_V1_RECORD_BOUND_BYTES[0],
            INSTALLATION_V1_RECORD_BOUND_BYTES[1],
            INSTALLATION_V1_RECORD_BOUND_BYTES[2],
            INSTALLATION_V1_RECORD_BOUND_BYTES[3],
        ]) as usize,
        u32::from_be_bytes([
            INSTALLATION_V1_RECORD_BOUND_BYTES[4],
            INSTALLATION_V1_RECORD_BOUND_BYTES[5],
            INSTALLATION_V1_RECORD_BOUND_BYTES[6],
            INSTALLATION_V1_RECORD_BOUND_BYTES[7],
        ]) as usize,
        preflight_payload::<68>,
        validate_payload::<68, v1::StoredApplicationInstallationCampaignV1>,
    )
    .with_compact_identity(57, 1);

const APPLICATION_EXPORT_OPERATION_V1_RECORD_SCHEMA: RecordSchema<'static> =
    RecordSchema::new_current(
        "riffdb.storage.v1.StoredApplicationExportOperationV1",
        SchemaHash::from_bytes(*EXPORT_V1_SCHEMA_HASH_BYTES),
        u32::from_be_bytes([
            EXPORT_V1_RECORD_BOUND_BYTES[0],
            EXPORT_V1_RECORD_BOUND_BYTES[1],
            EXPORT_V1_RECORD_BOUND_BYTES[2],
            EXPORT_V1_RECORD_BOUND_BYTES[3],
        ]) as usize,
        u32::from_be_bytes([
            EXPORT_V1_RECORD_BOUND_BYTES[4],
            EXPORT_V1_RECORD_BOUND_BYTES[5],
            EXPORT_V1_RECORD_BOUND_BYTES[6],
            EXPORT_V1_RECORD_BOUND_BYTES[7],
        ]) as usize,
        preflight_payload::<78>,
        validate_payload::<78, v1::StoredApplicationExportOperationV1>,
    )
    .with_compact_identity(60, 1);

const fn event_policy_command_authority_v5_schema_hash(index: usize) -> SchemaHash {
    let mut bytes = [0_u8; 32];
    let mut offset = 0;
    while offset < bytes.len() {
        bytes[offset] = EVENT_POLICY_COMMAND_AUTHORITY_V5_SCHEMA_HASH_BYTES[index * 32 + offset];
        offset += 1;
    }
    SchemaHash::from_bytes(bytes)
}

const fn event_policy_command_authority_v5_record_bound(index: usize, offset: usize) -> usize {
    let start = index * 8 + offset;
    u32::from_be_bytes([
        EVENT_POLICY_COMMAND_AUTHORITY_V5_RECORD_BOUND_BYTES[start],
        EVENT_POLICY_COMMAND_AUTHORITY_V5_RECORD_BOUND_BYTES[start + 1],
        EVENT_POLICY_COMMAND_AUTHORITY_V5_RECORD_BOUND_BYTES[start + 2],
        EVENT_POLICY_COMMAND_AUTHORITY_V5_RECORD_BOUND_BYTES[start + 3],
    ]) as usize
}

macro_rules! event_policy_command_authority_v5_schema {
    ($index:literal, $name:literal, $message:ty, $compact_tag:literal, $revision:literal) => {
        RecordSchema::new_current(
            concat!("riffdb.storage.v1.", $name),
            event_policy_command_authority_v5_schema_hash($index),
            event_policy_command_authority_v5_record_bound($index, 0),
            event_policy_command_authority_v5_record_bound($index, 4),
            preflight_payload::<{ 79 + $index }>,
            validate_payload::<{ 79 + $index }, $message>,
        )
        .with_compact_identity($compact_tag, $revision)
    };
}

const COMMAND_CAPSULE_V5_RECORD_SCHEMA: RecordSchema<'static> = event_policy_command_authority_v5_schema!(
    0,
    "StoredCommandCapsuleV5",
    v1::StoredCommandCapsuleV5,
    54,
    4
);
const COMMAND_SEGMENT_V4_RECORD_SCHEMA: RecordSchema<'static> = event_policy_command_authority_v5_schema!(
    1,
    "StoredCommandSegmentV4",
    v1::StoredCommandSegmentV4,
    55,
    4
);

const fn correlated_index_work_command_authority_v6_schema_hash(index: usize) -> SchemaHash {
    let mut bytes = [0_u8; 32];
    let mut offset = 0;
    while offset < bytes.len() {
        bytes[offset] =
            CORRELATED_INDEX_WORK_COMMAND_AUTHORITY_V6_SCHEMA_HASH_BYTES[index * 32 + offset];
        offset += 1;
    }
    SchemaHash::from_bytes(bytes)
}

const fn correlated_index_work_command_authority_v6_record_bound(
    index: usize,
    offset: usize,
) -> usize {
    let start = index * 8 + offset;
    u32::from_be_bytes([
        CORRELATED_INDEX_WORK_COMMAND_AUTHORITY_V6_RECORD_BOUND_BYTES[start],
        CORRELATED_INDEX_WORK_COMMAND_AUTHORITY_V6_RECORD_BOUND_BYTES[start + 1],
        CORRELATED_INDEX_WORK_COMMAND_AUTHORITY_V6_RECORD_BOUND_BYTES[start + 2],
        CORRELATED_INDEX_WORK_COMMAND_AUTHORITY_V6_RECORD_BOUND_BYTES[start + 3],
    ]) as usize
}

macro_rules! correlated_index_work_command_authority_v6_schema {
    ($index:literal, $name:literal, $message:ty, $compact_tag:literal, $revision:literal) => {
        RecordSchema::new_current(
            concat!("riffdb.storage.v1.", $name),
            correlated_index_work_command_authority_v6_schema_hash($index),
            correlated_index_work_command_authority_v6_record_bound($index, 0),
            correlated_index_work_command_authority_v6_record_bound($index, 4),
            preflight_payload::<{ 89 + $index }>,
            validate_payload::<{ 89 + $index }, $message>,
        )
        .with_compact_identity($compact_tag, $revision)
    };
}

const COMMAND_CAPSULE_V6_RECORD_SCHEMA: RecordSchema<'static> = correlated_index_work_command_authority_v6_schema!(
    0,
    "StoredCommandCapsuleV6",
    v1::StoredCommandCapsuleV6,
    54,
    5
);
const COMMAND_SEGMENT_V5_RECORD_SCHEMA: RecordSchema<'static> = correlated_index_work_command_authority_v6_schema!(
    1,
    "StoredCommandSegmentV5",
    v1::StoredCommandSegmentV5,
    55,
    5
);

const fn command_prefix_authority_v7_schema_hash(index: usize) -> SchemaHash {
    let mut bytes = [0_u8; 32];
    let mut offset = 0;
    while offset < bytes.len() {
        bytes[offset] = COMMAND_PREFIX_AUTHORITY_V7_SCHEMA_HASH_BYTES[index * 32 + offset];
        offset += 1;
    }
    SchemaHash::from_bytes(bytes)
}

const fn command_prefix_authority_v7_record_bound(index: usize, offset: usize) -> usize {
    let start = index * 8 + offset;
    u32::from_be_bytes([
        COMMAND_PREFIX_AUTHORITY_V7_RECORD_BOUND_BYTES[start],
        COMMAND_PREFIX_AUTHORITY_V7_RECORD_BOUND_BYTES[start + 1],
        COMMAND_PREFIX_AUTHORITY_V7_RECORD_BOUND_BYTES[start + 2],
        COMMAND_PREFIX_AUTHORITY_V7_RECORD_BOUND_BYTES[start + 3],
    ]) as usize
}

macro_rules! command_prefix_authority_v7_schema {
    ($index:literal, $name:literal, $message:ty, $compact_tag:literal, $revision:literal) => {
        RecordSchema::new_current(
            concat!("riffdb.storage.v1.", $name),
            command_prefix_authority_v7_schema_hash($index),
            command_prefix_authority_v7_record_bound($index, 0),
            command_prefix_authority_v7_record_bound($index, 4),
            preflight_payload::<{ 99 + $index }>,
            validate_payload::<{ 99 + $index }, $message>,
        )
        .with_compact_identity($compact_tag, $revision)
    };
}

const COMMAND_CAPSULE_V7_RECORD_SCHEMA: RecordSchema<'static> = command_prefix_authority_v7_schema!(
    0,
    "StoredCommandCapsuleV7",
    v1::StoredCommandCapsuleV7,
    54,
    6
);
const COMMAND_SEGMENT_V6_RECORD_SCHEMA: RecordSchema<'static> = command_prefix_authority_v7_schema!(
    1,
    "StoredCommandSegmentV6",
    v1::StoredCommandSegmentV6,
    55,
    6
);

const fn entity_transitions_v4_schema_hash(index: usize) -> SchemaHash {
    let mut bytes = [0_u8; 32];
    let mut offset = 0;
    while offset < bytes.len() {
        bytes[offset] = ENTITY_TRANSITIONS_V4_SCHEMA_HASH_BYTES[index * 32 + offset];
        offset += 1;
    }
    SchemaHash::from_bytes(bytes)
}

const fn entity_transitions_v4_record_bound(index: usize, offset: usize) -> usize {
    let start = index * 8 + offset;
    u32::from_be_bytes([
        ENTITY_TRANSITIONS_V4_RECORD_BOUND_BYTES[start],
        ENTITY_TRANSITIONS_V4_RECORD_BOUND_BYTES[start + 1],
        ENTITY_TRANSITIONS_V4_RECORD_BOUND_BYTES[start + 2],
        ENTITY_TRANSITIONS_V4_RECORD_BOUND_BYTES[start + 3],
    ]) as usize
}

macro_rules! entity_transitions_v4_schema {
    ($index:literal, $name:literal, $message:ty, $compact_tag:literal, $revision:literal) => {
        RecordSchema::new_current(
            concat!("riffdb.storage.v1.", $name),
            entity_transitions_v4_schema_hash($index),
            entity_transitions_v4_record_bound($index, 0),
            entity_transitions_v4_record_bound($index, 4),
            preflight_payload::<{ 69 + $index }>,
            validate_payload::<{ 69 + $index }, $message>,
        )
        .with_compact_identity($compact_tag, $revision)
    };
}

const ENTITY_CHAIN_HEAD_V1_RECORD_SCHEMA: RecordSchema<'static> = entity_transitions_v4_schema!(
    0,
    "StoredEntityChainHeadV1",
    v1::StoredEntityChainHeadV1,
    58,
    1
);
const CHANGELOG_V2_ROTATION_RECEIPT_V1_RECORD_SCHEMA: RecordSchema<'static> = entity_transitions_v4_schema!(
    1,
    "StoredChangelogV2RotationReceiptV1",
    v1::StoredChangelogV2RotationReceiptV1,
    59,
    1
);
const COMMAND_CAPSULE_V4_RECORD_SCHEMA: RecordSchema<'static> = entity_transitions_v4_schema!(
    2,
    "StoredCommandCapsuleV4",
    v1::StoredCommandCapsuleV4,
    54,
    3
);
const COMMAND_SEGMENT_V3_RECORD_SCHEMA: RecordSchema<'static> = entity_transitions_v4_schema!(
    3,
    "StoredCommandSegmentV3",
    v1::StoredCommandSegmentV3,
    55,
    3
);
const VALIDATED_PREFIX_CHECKPOINT_V2_RECORD_SCHEMA: RecordSchema<'static> = entity_transitions_v4_schema!(
    4,
    "StoredValidatedPrefixCheckpointV2",
    v1::StoredValidatedPrefixCheckpointV2,
    38,
    2
);

const VECTOR_EVIDENCE_V1_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.StoredVectorEvidenceV1",
    SchemaHash::from_bytes(*VECTOR_EVIDENCE_V1_SCHEMA_HASH_BYTES),
    u32::from_be_bytes([
        VECTOR_EVIDENCE_V1_RECORD_BOUND_BYTES[0],
        VECTOR_EVIDENCE_V1_RECORD_BOUND_BYTES[1],
        VECTOR_EVIDENCE_V1_RECORD_BOUND_BYTES[2],
        VECTOR_EVIDENCE_V1_RECORD_BOUND_BYTES[3],
    ]) as usize,
    u32::from_be_bytes([
        VECTOR_EVIDENCE_V1_RECORD_BOUND_BYTES[4],
        VECTOR_EVIDENCE_V1_RECORD_BOUND_BYTES[5],
        VECTOR_EVIDENCE_V1_RECORD_BOUND_BYTES[6],
        VECTOR_EVIDENCE_V1_RECORD_BOUND_BYTES[7],
    ]) as usize,
    preflight_payload::<84>,
    validate_payload::<84, v1::StoredVectorEvidenceV1>,
)
.with_compact_identity(61, 1);

const VECTOR_OBSERVATION_V1_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.StoredVectorObservationV1",
    SchemaHash::from_bytes(*VECTOR_OBSERVATION_V1_SCHEMA_HASH_BYTES),
    u32::from_be_bytes([
        VECTOR_OBSERVATION_V1_RECORD_BOUND_BYTES[0],
        VECTOR_OBSERVATION_V1_RECORD_BOUND_BYTES[1],
        VECTOR_OBSERVATION_V1_RECORD_BOUND_BYTES[2],
        VECTOR_OBSERVATION_V1_RECORD_BOUND_BYTES[3],
    ]) as usize,
    u32::from_be_bytes([
        VECTOR_OBSERVATION_V1_RECORD_BOUND_BYTES[4],
        VECTOR_OBSERVATION_V1_RECORD_BOUND_BYTES[5],
        VECTOR_OBSERVATION_V1_RECORD_BOUND_BYTES[6],
        VECTOR_OBSERVATION_V1_RECORD_BOUND_BYTES[7],
    ]) as usize,
    preflight_payload::<85>,
    validate_payload::<85, v1::StoredVectorObservationV1>,
)
.with_compact_identity(62, 1);

const VECTOR_EVIDENCE_INDEX_V1_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.StoredVectorEvidenceIndexV1",
    SchemaHash::from_bytes(*VECTOR_EVIDENCE_INDEX_V1_SCHEMA_HASH_BYTES),
    u32::from_be_bytes([
        VECTOR_EVIDENCE_INDEX_V1_RECORD_BOUND_BYTES[0],
        VECTOR_EVIDENCE_INDEX_V1_RECORD_BOUND_BYTES[1],
        VECTOR_EVIDENCE_INDEX_V1_RECORD_BOUND_BYTES[2],
        VECTOR_EVIDENCE_INDEX_V1_RECORD_BOUND_BYTES[3],
    ]) as usize,
    u32::from_be_bytes([
        VECTOR_EVIDENCE_INDEX_V1_RECORD_BOUND_BYTES[4],
        VECTOR_EVIDENCE_INDEX_V1_RECORD_BOUND_BYTES[5],
        VECTOR_EVIDENCE_INDEX_V1_RECORD_BOUND_BYTES[6],
        VECTOR_EVIDENCE_INDEX_V1_RECORD_BOUND_BYTES[7],
    ]) as usize,
    preflight_payload::<86>,
    validate_payload::<86, v1::StoredVectorEvidenceIndexV1>,
)
.with_compact_identity(63, 1);

const VECTOR_HEALTH_OBSERVATION_V1_RECORD_SCHEMA: RecordSchema<'static> =
    RecordSchema::new_current(
        "riffdb.storage.v1.StoredVectorHealthObservationV1",
        SchemaHash::from_bytes(*VECTOR_HEALTH_OBSERVATION_V1_SCHEMA_HASH_BYTES),
        u32::from_be_bytes([
            VECTOR_HEALTH_OBSERVATION_V1_RECORD_BOUND_BYTES[0],
            VECTOR_HEALTH_OBSERVATION_V1_RECORD_BOUND_BYTES[1],
            VECTOR_HEALTH_OBSERVATION_V1_RECORD_BOUND_BYTES[2],
            VECTOR_HEALTH_OBSERVATION_V1_RECORD_BOUND_BYTES[3],
        ]) as usize,
        u32::from_be_bytes([
            VECTOR_HEALTH_OBSERVATION_V1_RECORD_BOUND_BYTES[4],
            VECTOR_HEALTH_OBSERVATION_V1_RECORD_BOUND_BYTES[5],
            VECTOR_HEALTH_OBSERVATION_V1_RECORD_BOUND_BYTES[6],
            VECTOR_HEALTH_OBSERVATION_V1_RECORD_BOUND_BYTES[7],
        ]) as usize,
        preflight_payload::<87>,
        validate_payload::<87, v1::StoredVectorHealthObservationV1>,
    )
    .with_compact_identity(64, 1);

const VECTOR_PROJECTION_CONTROL_V1_RECORD_SCHEMA: RecordSchema<'static> =
    RecordSchema::new_current(
        "riffdb.storage.v1.StoredVectorProjectionControlV1",
        SchemaHash::from_bytes(*VECTOR_PROJECTION_CONTROL_V1_SCHEMA_HASH_BYTES),
        u32::from_be_bytes([
            VECTOR_PROJECTION_CONTROL_V1_RECORD_BOUND_BYTES[0],
            VECTOR_PROJECTION_CONTROL_V1_RECORD_BOUND_BYTES[1],
            VECTOR_PROJECTION_CONTROL_V1_RECORD_BOUND_BYTES[2],
            VECTOR_PROJECTION_CONTROL_V1_RECORD_BOUND_BYTES[3],
        ]) as usize,
        u32::from_be_bytes([
            VECTOR_PROJECTION_CONTROL_V1_RECORD_BOUND_BYTES[4],
            VECTOR_PROJECTION_CONTROL_V1_RECORD_BOUND_BYTES[5],
            VECTOR_PROJECTION_CONTROL_V1_RECORD_BOUND_BYTES[6],
            VECTOR_PROJECTION_CONTROL_V1_RECORD_BOUND_BYTES[7],
        ]) as usize,
        preflight_payload::<88>,
        validate_payload::<88, v1::StoredVectorProjectionControlV1>,
    )
    .with_compact_identity(65, 1);

const CLEAN_CLOSE_LIFECYCLE_V1_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.StoredCleanCloseLifecycleV1",
    SchemaHash::from_bytes(*CLEAN_CLOSE_LIFECYCLE_V1_SCHEMA_HASH_BYTES),
    u32::from_be_bytes([
        CLEAN_CLOSE_LIFECYCLE_V1_RECORD_BOUND_BYTES[0],
        CLEAN_CLOSE_LIFECYCLE_V1_RECORD_BOUND_BYTES[1],
        CLEAN_CLOSE_LIFECYCLE_V1_RECORD_BOUND_BYTES[2],
        CLEAN_CLOSE_LIFECYCLE_V1_RECORD_BOUND_BYTES[3],
    ]) as usize,
    u32::from_be_bytes([
        CLEAN_CLOSE_LIFECYCLE_V1_RECORD_BOUND_BYTES[4],
        CLEAN_CLOSE_LIFECYCLE_V1_RECORD_BOUND_BYTES[5],
        CLEAN_CLOSE_LIFECYCLE_V1_RECORD_BOUND_BYTES[6],
        CLEAN_CLOSE_LIFECYCLE_V1_RECORD_BOUND_BYTES[7],
    ]) as usize,
    preflight_payload::<91>,
    validate_payload::<91, v1::StoredCleanCloseLifecycleV1>,
)
.with_compact_identity(66, 1);

const COLUMNAR_PROJECTION_CONTROL_V1_RECORD_SCHEMA: RecordSchema<'static> =
    RecordSchema::new_current(
        "riffdb.storage.v1.StoredColumnarProjectionControlV1",
        SchemaHash::from_bytes(*COLUMNAR_PROJECTION_CONTROL_V1_SCHEMA_HASH_BYTES),
        u32::from_be_bytes([
            COLUMNAR_PROJECTION_CONTROL_V1_RECORD_BOUND_BYTES[0],
            COLUMNAR_PROJECTION_CONTROL_V1_RECORD_BOUND_BYTES[1],
            COLUMNAR_PROJECTION_CONTROL_V1_RECORD_BOUND_BYTES[2],
            COLUMNAR_PROJECTION_CONTROL_V1_RECORD_BOUND_BYTES[3],
        ]) as usize,
        u32::from_be_bytes([
            COLUMNAR_PROJECTION_CONTROL_V1_RECORD_BOUND_BYTES[4],
            COLUMNAR_PROJECTION_CONTROL_V1_RECORD_BOUND_BYTES[5],
            COLUMNAR_PROJECTION_CONTROL_V1_RECORD_BOUND_BYTES[6],
            COLUMNAR_PROJECTION_CONTROL_V1_RECORD_BOUND_BYTES[7],
        ]) as usize,
        preflight_payload::<92>,
        validate_payload::<92, v1::StoredColumnarProjectionControlV1>,
    )
    .with_compact_identity(67, 1);

const CHANGELOG_ALLOCATOR_V3_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.StoredChangelogTransactionAllocatorV3",
    SchemaHash::from_bytes(*include_bytes!(
        "../fixtures/durable-changelog-allocator-v3-schema-hash.bin"
    )),
    11,
    u32::from_be_bytes({
        let bounds = *include_bytes!("../fixtures/durable-changelog-allocator-v3-record-bound.bin");
        [bounds[4], bounds[5], bounds[6], bounds[7]]
    }) as usize,
    preflight_payload::<93>,
    validate_payload::<93, v1::StoredChangelogTransactionAllocatorV3>,
)
.with_compact_identity(68, 1);

const AUTHORITATIVE_STATE_CATALOG_V1_RECORD_SCHEMA: RecordSchema<'static> =
    RecordSchema::new_current(
        "riffdb.storage.v1.StoredAuthoritativeStateCatalogV1",
        SchemaHash::from_bytes(*include_bytes!(
            "../fixtures/durable-authoritative-state-catalog-v1-schema-hash.bin"
        )),
        34,
        u32::from_be_bytes({
            let bounds = *include_bytes!(
                "../fixtures/durable-authoritative-state-catalog-v1-record-bound.bin"
            );
            [bounds[4], bounds[5], bounds[6], bounds[7]]
        }) as usize,
        preflight_payload::<94>,
        validate_payload::<94, v1::StoredAuthoritativeStateCatalogV1>,
    )
    .with_compact_identity(69, 1);

const LEADERSHIP_EPOCH_V1_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.StoredLeadershipEpochV1",
    SchemaHash::from_bytes(*include_bytes!(
        "../fixtures/durable-leadership-epoch-v1-schema-hash.bin"
    )),
    11,
    u32::from_be_bytes({
        let bounds = *include_bytes!("../fixtures/durable-leadership-epoch-v1-record-bound.bin");
        [bounds[4], bounds[5], bounds[6], bounds[7]]
    }) as usize,
    preflight_payload::<95>,
    validate_payload::<95, v1::StoredLeadershipEpochV1>,
)
.with_compact_identity(70, 1);

const CHANGELOG_HISTORY_STATE_V3_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.StoredChangelogHistoryStateV3",
    SchemaHash::from_bytes(*include_bytes!(
        "../fixtures/durable-changelog-history-state-v3-schema-hash.bin"
    )),
    281,
    u32::from_be_bytes({
        let bounds =
            *include_bytes!("../fixtures/durable-changelog-history-state-v3-record-bound.bin");
        [bounds[4], bounds[5], bounds[6], bounds[7]]
    }) as usize,
    preflight_payload::<96>,
    validate_payload::<96, v1::StoredChangelogHistoryStateV3>,
)
.with_compact_identity(71, 1);

const REPLICATION_FOLLOWER_STATE_V3_RECORD_SCHEMA: RecordSchema<'static> =
    RecordSchema::new_current(
        "riffdb.storage.v1.StoredReplicationFollowerStateV3",
        SchemaHash::from_bytes(*include_bytes!(
            "../fixtures/durable-replication-follower-state-v3-schema-hash.bin"
        )),
        215,
        u32::from_be_bytes({
            let bounds = *include_bytes!(
                "../fixtures/durable-replication-follower-state-v3-record-bound.bin"
            );
            [bounds[4], bounds[5], bounds[6], bounds[7]]
        }) as usize,
        preflight_payload::<97>,
        validate_payload::<97, v1::StoredReplicationFollowerStateV3>,
    )
    .with_compact_identity(72, 1);

const REPLICATION_SOURCE_HOLD_V1_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.StoredReplicationSourceHoldV1",
    SchemaHash::from_bytes(*include_bytes!(
        "../fixtures/durable-replication-source-hold-v1-schema-hash.bin"
    )),
    163,
    u32::from_be_bytes({
        let bounds =
            *include_bytes!("../fixtures/durable-replication-source-hold-v1-record-bound.bin");
        [bounds[4], bounds[5], bounds[6], bounds[7]]
    }) as usize,
    preflight_payload::<98>,
    validate_payload::<98, v1::StoredReplicationSourceHoldV1>,
)
.with_compact_identity(73, 1);

const REPLICATION_SOURCE_HOLD_V2_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.StoredReplicationSourceHoldV2",
    SchemaHash::from_bytes(*include_bytes!(
        "../fixtures/durable-replication-source-hold-v2-schema-hash.bin"
    )),
    328,
    u32::from_be_bytes({
        let bounds =
            *include_bytes!("../fixtures/durable-replication-source-hold-v2-record-bound.bin");
        [bounds[4], bounds[5], bounds[6], bounds[7]]
    }) as usize,
    preflight_payload::<101>,
    validate_payload::<101, v1::StoredReplicationSourceHoldV2>,
)
.with_compact_identity(73, 2);

const APPLICATION_EXPORT_OPERATION_V2_RECORD_SCHEMA: RecordSchema<'static> =
    RecordSchema::new_current(
        "riffdb.storage.v1.StoredApplicationExportOperationV2",
        SchemaHash::from_bytes(*include_bytes!(
            "../fixtures/durable-export-operation-v2-schema-hash.bin"
        )),
        256 * 1024,
        u32::from_be_bytes({
            let bounds =
                *include_bytes!("../fixtures/durable-export-operation-v2-record-bound.bin");
            [bounds[4], bounds[5], bounds[6], bounds[7]]
        }) as usize,
        preflight_payload::<102>,
        validate_payload::<102, v1::StoredApplicationExportOperationV2>,
    )
    .with_compact_identity(60, 2);

const APPLICATION_EXPORT_PAGE_COMMITMENT_V1_RECORD_SCHEMA: RecordSchema<'static> =
    RecordSchema::new_current(
        "riffdb.storage.v1.StoredApplicationExportPageCommitmentV1",
        SchemaHash::from_bytes(*include_bytes!(
            "../fixtures/durable-export-page-commitment-v1-schema-hash.bin"
        )),
        74,
        u32::from_be_bytes({
            let bounds =
                *include_bytes!("../fixtures/durable-export-page-commitment-v1-record-bound.bin");
            [bounds[4], bounds[5], bounds[6], bounds[7]]
        }) as usize,
        preflight_payload::<103>,
        validate_payload::<103, v1::StoredApplicationExportPageCommitmentV1>,
    )
    .with_compact_identity(74, 1);

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

/// Fixed-cardinality phase timing for one exact current readable decode.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReadableDecodeProfileV1 {
    pub identity_bounds_ns: u64,
    pub checksum_ns: u64,
    pub wire_preflight_ns: u64,
    pub prost_decode_ns: u64,
    pub canonical_reencode_ns: u64,
}

/// Decodes through the identical readable path while returning phase timing.
#[doc(hidden)]
pub fn decode_readable_message_profiled<M>(
    encoded: &[u8],
) -> Result<(M, ReadableDecodeProfileV1), crate::envelope::EnvelopeError>
where
    M: ReadableRecordMessage + Default,
{
    readable_record_registry()
        .decode_current_message_profiled(encoded, M::record_schema())
        .map(|(message, profile)| {
            (
                message,
                ReadableDecodeProfileV1 {
                    identity_bounds_ns: profile.identity_bounds_ns,
                    checksum_ns: profile.checksum_ns,
                    wire_preflight_ns: profile.wire_preflight_ns,
                    prost_decode_ns: profile.prost_decode_ns,
                    canonical_reencode_ns: profile.canonical_reencode_ns,
                },
            )
        })
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
readable_message!(v1::CapabilityRecordV3, CAPABILITY_V3_RECORD_SCHEMA);
readable_message!(v1::CapabilityRecordV4, CAPABILITY_V4_RECORD_SCHEMA);
readable_message!(v1::StoredDurableEventV2, DURABLE_EVENT_V2_RECORD_SCHEMA);
readable_message!(v1::CapabilityRecordV5, CAPABILITY_V5_RECORD_SCHEMA);
readable_message!(v1::CapabilityRecordV6, CAPABILITY_V6_RECORD_SCHEMA);
readable_message!(v1::CapabilityRecordV7, CAPABILITY_V7_RECORD_SCHEMA);
readable_message!(v1::CapabilityRecordV8, CAPABILITY_V8_RECORD_SCHEMA);
readable_message!(v1::StoredVectorEvidenceV1, VECTOR_EVIDENCE_V1_RECORD_SCHEMA);
readable_message!(
    v1::StoredVectorObservationV1,
    VECTOR_OBSERVATION_V1_RECORD_SCHEMA
);
readable_message!(
    v1::StoredVectorEvidenceIndexV1,
    VECTOR_EVIDENCE_INDEX_V1_RECORD_SCHEMA
);
readable_message!(
    v1::StoredVectorHealthObservationV1,
    VECTOR_HEALTH_OBSERVATION_V1_RECORD_SCHEMA
);
readable_message!(
    v1::StoredVectorProjectionControlV1,
    VECTOR_PROJECTION_CONTROL_V1_RECORD_SCHEMA
);
readable_message!(
    v1::StoredApplicationExportOperationV1,
    APPLICATION_EXPORT_OPERATION_V1_RECORD_SCHEMA
);
readable_message!(
    v1::StoredApplicationExportOperationV2,
    APPLICATION_EXPORT_OPERATION_V2_RECORD_SCHEMA
);
readable_message!(
    v1::StoredApplicationExportPageCommitmentV1,
    APPLICATION_EXPORT_PAGE_COMMITMENT_V1_RECORD_SCHEMA
);
readable_message!(v1::StoredCommandCapsuleV5, COMMAND_CAPSULE_V5_RECORD_SCHEMA);
readable_message!(v1::StoredCommandSegmentV4, COMMAND_SEGMENT_V4_RECORD_SCHEMA);
readable_message!(v1::StoredCommandCapsuleV6, COMMAND_CAPSULE_V6_RECORD_SCHEMA);
readable_message!(v1::StoredCommandSegmentV5, COMMAND_SEGMENT_V5_RECORD_SCHEMA);
readable_message!(v1::StoredCommandCapsuleV7, COMMAND_CAPSULE_V7_RECORD_SCHEMA);
readable_message!(v1::StoredCommandSegmentV6, COMMAND_SEGMENT_V6_RECORD_SCHEMA);
readable_message!(
    v1::StoredCleanCloseLifecycleV1,
    CLEAN_CLOSE_LIFECYCLE_V1_RECORD_SCHEMA
);
readable_message!(
    v1::StoredColumnarProjectionControlV1,
    COLUMNAR_PROJECTION_CONTROL_V1_RECORD_SCHEMA
);
readable_message!(
    v1::StoredChangelogTransactionAllocatorV3,
    CHANGELOG_ALLOCATOR_V3_RECORD_SCHEMA
);
readable_message!(
    v1::StoredAuthoritativeStateCatalogV1,
    AUTHORITATIVE_STATE_CATALOG_V1_RECORD_SCHEMA
);
readable_message!(
    v1::StoredLeadershipEpochV1,
    LEADERSHIP_EPOCH_V1_RECORD_SCHEMA
);
readable_message!(
    v1::StoredChangelogHistoryStateV3,
    CHANGELOG_HISTORY_STATE_V3_RECORD_SCHEMA
);
readable_message!(
    v1::StoredReplicationFollowerStateV3,
    REPLICATION_FOLLOWER_STATE_V3_RECORD_SCHEMA
);
readable_message!(
    v1::StoredReplicationSourceHoldV2,
    REPLICATION_SOURCE_HOLD_V2_RECORD_SCHEMA
);
readable_message!(
    v1::StoredReplicationSourceHoldV1,
    REPLICATION_SOURCE_HOLD_V1_RECORD_SCHEMA
);
readable_message!(
    v1::StoredValidatedPrefixCheckpointV1,
    VALIDATED_PREFIX_CHECKPOINT_V1_RECORD_SCHEMA
);
readable_message!(
    v1::StoredRetentionWatermarkV1,
    RETENTION_WATERMARK_V1_RECORD_SCHEMA
);
readable_message!(v1::StoredRetentionHoldsV1, RETENTION_HOLDS_V1_RECORD_SCHEMA);
readable_message!(
    v1::StoredHistoryTombstoneV1,
    HISTORY_TOMBSTONE_V1_RECORD_SCHEMA
);
readable_message!(
    v1::StoredRetentionAdministrationV1,
    RETENTION_ADMINISTRATION_V1_RECORD_SCHEMA
);
readable_message!(v1::StoredReactiveModuleV1, REACTIVE_MODULE_V1_RECORD_SCHEMA);
readable_message!(
    v1::StoredReactiveModuleAdministrationV1,
    REACTIVE_MODULE_ADMINISTRATION_V1_RECORD_SCHEMA
);
readable_message!(v1::StoredEventConsumerV1, EVENT_CONSUMER_V1_RECORD_SCHEMA);
readable_message!(
    v1::StoredEventConsumerDeliveryV1,
    EVENT_CONSUMER_DELIVERY_V1_RECORD_SCHEMA
);
readable_message!(v1::ServiceAuditRecordV2, SERVICE_AUDIT_V2_RECORD_SCHEMA);
readable_message!(v1::ServiceAuditRecordV3, SERVICE_AUDIT_V3_RECORD_SCHEMA);
readable_message!(
    v1::StoredReplicationAdministrationV1,
    REPLICATION_ADMINISTRATION_V1_RECORD_SCHEMA
);
readable_message!(
    v1::StoredPendingAdmissionV2,
    PENDING_ADMISSION_V2_RECORD_SCHEMA
);
readable_message!(
    v1::StoredExecutionFailedV2,
    EXECUTION_FAILED_V2_RECORD_SCHEMA
);
readable_message!(v1::StoredOutcomeV2, OUTCOME_V2_RECORD_SCHEMA);
readable_message!(v1::StoredProvenanceRecordV2, PROVENANCE_V2_RECORD_SCHEMA);
readable_message!(v1::StoredCommandCapsuleV1, COMMAND_CAPSULE_V1_RECORD_SCHEMA);
readable_message!(v1::StoredCommandLocatorV1, COMMAND_LOCATOR_V1_RECORD_SCHEMA);
readable_message!(
    v1::StoredCommandAuditLocatorV1,
    COMMAND_AUDIT_LOCATOR_V1_RECORD_SCHEMA
);
readable_message!(v1::StoredCommandCapsuleV2, COMMAND_CAPSULE_V2_RECORD_SCHEMA);
readable_message!(v1::StoredCommandSegmentV1, COMMAND_SEGMENT_V1_RECORD_SCHEMA);
readable_message!(
    v1::StoredCommandDerivedIndexCheckpointV1,
    COMMAND_DERIVED_INDEX_CHECKPOINT_V1_RECORD_SCHEMA
);
readable_message!(
    v1::StoredPendingAdmissionV3,
    PENDING_ADMISSION_V3_RECORD_SCHEMA
);
readable_message!(
    v1::StoredExecutionFailedV3,
    EXECUTION_FAILED_V3_RECORD_SCHEMA
);
readable_message!(v1::StoredOutcomeV3, OUTCOME_V3_RECORD_SCHEMA);
readable_message!(v1::StoredCommandCapsuleV3, COMMAND_CAPSULE_V3_RECORD_SCHEMA);
readable_message!(v1::StoredCommandSegmentV2, COMMAND_SEGMENT_V2_RECORD_SCHEMA);
readable_message!(
    v1::StoredApplicationInstallationCampaignV1,
    APPLICATION_INSTALLATION_CAMPAIGN_V1_RECORD_SCHEMA
);
readable_message!(
    v1::StoredEntityChainHeadV1,
    ENTITY_CHAIN_HEAD_V1_RECORD_SCHEMA
);
readable_message!(
    v1::StoredChangelogV2RotationReceiptV1,
    CHANGELOG_V2_ROTATION_RECEIPT_V1_RECORD_SCHEMA
);
readable_message!(v1::StoredCommandCapsuleV4, COMMAND_CAPSULE_V4_RECORD_SCHEMA);
readable_message!(v1::StoredCommandSegmentV3, COMMAND_SEGMENT_V3_RECORD_SCHEMA);
readable_message!(
    v1::StoredValidatedPrefixCheckpointV2,
    VALIDATED_PREFIX_CHECKPOINT_V2_RECORD_SCHEMA
);

writable_message!(v1::StoredStorageFormatVersionV1);
writable_message!(v1::StoredDatabaseIdentityV1);
writable_message!(v1::StoredApplicationSequenceAllocatorV1);
writable_message!(v1::StoredAdministrationSequenceAllocatorV1);
writable_message!(v1::StoredContractBundleV1);
writable_message!(v1::ActiveCatalogPointerV1);
writable_message!(v1::StoredCatalogAdministrationV1);
writable_message!(v1::StoredEntityRecordV1);
writable_message!(v1::StoredDurableEventV1);
writable_message!(v1::StoredDurableEventV2);
writable_message!(v1::CapabilityRecordV1);
writable_message!(v1::CapabilityTokenLookupV1);
writable_message!(v1::CapabilityBootstrapMarkerV1);
writable_message!(v1::CapabilityAdministrationAuditV1);
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
writable_message!(v1::CapabilityRecordV3);
writable_message!(v1::CapabilityRecordV4);
writable_message!(v1::CapabilityRecordV5);
writable_message!(v1::CapabilityRecordV6);
writable_message!(v1::CapabilityRecordV7);
writable_message!(v1::CapabilityRecordV8);
writable_message!(v1::StoredVectorEvidenceV1);
writable_message!(v1::StoredVectorObservationV1);
writable_message!(v1::StoredVectorEvidenceIndexV1);
writable_message!(v1::StoredVectorHealthObservationV1);
writable_message!(v1::StoredVectorProjectionControlV1);
writable_message!(v1::StoredRetentionWatermarkV1);
writable_message!(v1::StoredRetentionHoldsV1);
writable_message!(v1::StoredHistoryTombstoneV1);
writable_message!(v1::StoredRetentionAdministrationV1);
writable_message!(v1::StoredReactiveModuleV1);
writable_message!(v1::StoredReactiveModuleAdministrationV1);
writable_message!(v1::StoredEventConsumerV1);
writable_message!(v1::StoredEventConsumerDeliveryV1);
writable_message!(v1::ServiceAuditRecordV2);
writable_message!(v1::ServiceAuditRecordV3);
writable_message!(v1::StoredReplicationAdministrationV1);
writable_message!(v1::StoredProvenanceRecordV2);
writable_message!(v1::StoredCommandCapsuleV1);
writable_message!(v1::StoredCommandLocatorV1);
writable_message!(v1::StoredCommandAuditLocatorV1);
writable_message!(v1::StoredCommandDerivedIndexCheckpointV1);
writable_message!(v1::StoredPendingAdmissionV3);
writable_message!(v1::StoredExecutionFailedV3);
writable_message!(v1::StoredOutcomeV3);
writable_message!(v1::StoredApplicationInstallationCampaignV1);
writable_message!(v1::StoredApplicationExportOperationV1);
writable_message!(v1::StoredApplicationExportOperationV2);
writable_message!(v1::StoredApplicationExportPageCommitmentV1);
writable_message!(v1::StoredCommandCapsuleV5);
writable_message!(v1::StoredCommandSegmentV4);
writable_message!(v1::StoredCommandCapsuleV6);
writable_message!(v1::StoredCommandSegmentV5);
writable_message!(v1::StoredCommandCapsuleV7);
writable_message!(v1::StoredCommandSegmentV6);
writable_message!(v1::StoredCleanCloseLifecycleV1);
writable_message!(v1::StoredColumnarProjectionControlV1);
writable_message!(v1::StoredChangelogTransactionAllocatorV3);
writable_message!(v1::StoredAuthoritativeStateCatalogV1);
writable_message!(v1::StoredLeadershipEpochV1);
writable_message!(v1::StoredChangelogHistoryStateV3);
writable_message!(v1::StoredReplicationFollowerStateV3);
writable_message!(v1::StoredReplicationSourceHoldV1);
writable_message!(v1::StoredReplicationSourceHoldV2);
writable_message!(v1::StoredEntityChainHeadV1);
writable_message!(v1::StoredChangelogV2RotationReceiptV1);
writable_message!(v1::StoredCommandCapsuleV4);
writable_message!(v1::StoredCommandSegmentV3);
writable_message!(v1::StoredValidatedPrefixCheckpointV2);

/// Encodes one sealed generated message after the same allocation-free shape preflight.
pub fn encode_current_message<M: WritableRecordMessage>(
    message: &M,
) -> Result<Vec<u8>, crate::envelope::EnvelopeError> {
    crate::envelope::encode_preflighted(M::record_schema(), &message.encode_to_vec())
}

/// Frames already-canonical bytes for one sealed current record type.
///
/// This is reserved for first-party encoders that assemble a payload from
/// canonical nested-message bytes they already needed for a content digest.
/// The same generated durable-wire preflight used by [`encode_current_message`]
/// still rejects malformed, noncanonical, or over-limit payloads.
pub fn encode_current_payload<M: WritableRecordMessage>(
    payload: &[u8],
) -> Result<Vec<u8>, crate::envelope::EnvelopeError> {
    crate::envelope::encode_preflighted(M::record_schema(), payload)
}

/// Frames current-schema payload bytes after a first-party typed encoder has
/// already proved their canonical structural shape.
///
/// This is a narrow internal optimization boundary for payloads assembled by
/// a checked semantic pipeline. The caller must have established every field,
/// nesting, cardinality, canonical-order, and scalar bound normally checked by
/// generated durable-wire preflight. The sealed [`WritableRecordMessage`]
/// selects the exact registered compact identity; framing still enforces the
/// payload ceiling and writes the CRC-32C over the exact supplied bytes.
///
/// External, recovered, migrated, compatibility, or otherwise untyped bytes
/// must use [`encode_current_payload`] or the readable registry instead.
#[doc(hidden)]
pub fn encode_current_payload_after_structural_proof<M: WritableRecordMessage>(
    payload: &[u8],
) -> Result<Vec<u8>, crate::envelope::EnvelopeError> {
    crate::envelope::encode_after_structural_proof(M::record_schema(), payload)
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

const PRE_WP416_CAPABILITY_RECORD_SCHEMA: RecordSchema<'static> = RecordSchema::new_current(
    "riffdb.storage.v1.CapabilityRecordV1",
    PRE_WP416_CAPABILITY_SCHEMA_HASH,
    legacy_record_bound(17, 0),
    legacy_record_bound(17, 4),
    validate_pre_wp280_capability_payload,
    validate_pre_wp280_capability_payload,
)
.with_compact_identity(18, 2);

macro_rules! pre_wp416_capability_schema {
    ($name:literal, $hash:expr, $index:literal, $message:ty, $tag:literal) => {
        RecordSchema::new_current(
            concat!("riffdb.storage.v1.", $name),
            $hash,
            legacy_record_bound($index, 0),
            legacy_record_bound($index, 4),
            preflight_payload::<$index>,
            validate_payload::<$index, $message>,
        )
        .with_compact_identity($tag, 1)
    };
}

const PRE_WP416_CAPABILITY_TOKEN_LOOKUP_RECORD_SCHEMA: RecordSchema<'static> = pre_wp416_capability_schema!(
    "CapabilityTokenLookupV1",
    PRE_WP416_CAPABILITY_TOKEN_LOOKUP_SCHEMA_HASH,
    18,
    v1::CapabilityTokenLookupV1,
    19
);
const PRE_WP416_CAPABILITY_BOOTSTRAP_RECORD_SCHEMA: RecordSchema<'static> = pre_wp416_capability_schema!(
    "CapabilityBootstrapMarkerV1",
    PRE_WP416_CAPABILITY_BOOTSTRAP_SCHEMA_HASH,
    19,
    v1::CapabilityBootstrapMarkerV1,
    20
);
const PRE_WP416_CAPABILITY_ADMINISTRATION_RECORD_SCHEMA: RecordSchema<'static> = pre_wp416_capability_schema!(
    "CapabilityAdministrationAuditV1",
    PRE_WP416_CAPABILITY_ADMINISTRATION_SCHEMA_HASH,
    20,
    v1::CapabilityAdministrationAuditV1,
    21
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
    VALIDATED_PREFIX_CHECKPOINT_V1_RECORD_SCHEMA,
    RETENTION_WATERMARK_V1_RECORD_SCHEMA,
    RETENTION_HOLDS_V1_RECORD_SCHEMA,
    HISTORY_TOMBSTONE_V1_RECORD_SCHEMA,
    RETENTION_ADMINISTRATION_V1_RECORD_SCHEMA,
    REACTIVE_MODULE_V1_RECORD_SCHEMA,
    REACTIVE_MODULE_ADMINISTRATION_V1_RECORD_SCHEMA,
    EVENT_CONSUMER_V1_RECORD_SCHEMA,
    EVENT_CONSUMER_DELIVERY_V1_RECORD_SCHEMA,
    SERVICE_AUDIT_V2_RECORD_SCHEMA,
    PENDING_ADMISSION_V2_RECORD_SCHEMA,
    EXECUTION_FAILED_V2_RECORD_SCHEMA,
    OUTCOME_V2_RECORD_SCHEMA,
    PROVENANCE_V2_RECORD_SCHEMA,
    COMMAND_CAPSULE_V1_RECORD_SCHEMA,
    COMMAND_LOCATOR_V1_RECORD_SCHEMA,
    COMMAND_AUDIT_LOCATOR_V1_RECORD_SCHEMA,
    COMMAND_CAPSULE_V2_RECORD_SCHEMA,
    COMMAND_SEGMENT_V1_RECORD_SCHEMA,
    COMMAND_DERIVED_INDEX_CHECKPOINT_V1_RECORD_SCHEMA,
    PENDING_ADMISSION_V3_RECORD_SCHEMA,
    EXECUTION_FAILED_V3_RECORD_SCHEMA,
    OUTCOME_V3_RECORD_SCHEMA,
    COMMAND_CAPSULE_V3_RECORD_SCHEMA,
    COMMAND_SEGMENT_V2_RECORD_SCHEMA,
    APPLICATION_INSTALLATION_CAMPAIGN_V1_RECORD_SCHEMA,
    ENTITY_CHAIN_HEAD_V1_RECORD_SCHEMA,
    CHANGELOG_V2_ROTATION_RECEIPT_V1_RECORD_SCHEMA,
    COMMAND_CAPSULE_V4_RECORD_SCHEMA,
    COMMAND_SEGMENT_V3_RECORD_SCHEMA,
    VALIDATED_PREFIX_CHECKPOINT_V2_RECORD_SCHEMA,
    CAPABILITY_V3_RECORD_SCHEMA,
    CAPABILITY_V4_RECORD_SCHEMA,
    DURABLE_EVENT_V2_RECORD_SCHEMA,
    CAPABILITY_V5_RECORD_SCHEMA,
    APPLICATION_EXPORT_OPERATION_V1_RECORD_SCHEMA,
    COMMAND_CAPSULE_V5_RECORD_SCHEMA,
    COMMAND_SEGMENT_V4_RECORD_SCHEMA,
    CAPABILITY_V6_RECORD_SCHEMA,
    CAPABILITY_V7_RECORD_SCHEMA,
    CAPABILITY_V8_RECORD_SCHEMA,
    VECTOR_EVIDENCE_V1_RECORD_SCHEMA,
    VECTOR_OBSERVATION_V1_RECORD_SCHEMA,
    VECTOR_EVIDENCE_INDEX_V1_RECORD_SCHEMA,
    VECTOR_HEALTH_OBSERVATION_V1_RECORD_SCHEMA,
    VECTOR_PROJECTION_CONTROL_V1_RECORD_SCHEMA,
    COMMAND_CAPSULE_V6_RECORD_SCHEMA,
    COMMAND_SEGMENT_V5_RECORD_SCHEMA,
    CLEAN_CLOSE_LIFECYCLE_V1_RECORD_SCHEMA,
    COLUMNAR_PROJECTION_CONTROL_V1_RECORD_SCHEMA,
    CHANGELOG_ALLOCATOR_V3_RECORD_SCHEMA,
    AUTHORITATIVE_STATE_CATALOG_V1_RECORD_SCHEMA,
    LEADERSHIP_EPOCH_V1_RECORD_SCHEMA,
    CHANGELOG_HISTORY_STATE_V3_RECORD_SCHEMA,
    REPLICATION_FOLLOWER_STATE_V3_RECORD_SCHEMA,
    REPLICATION_SOURCE_HOLD_V1_RECORD_SCHEMA,
    COMMAND_CAPSULE_V7_RECORD_SCHEMA,
    COMMAND_SEGMENT_V6_RECORD_SCHEMA,
    REPLICATION_SOURCE_HOLD_V2_RECORD_SCHEMA,
    APPLICATION_EXPORT_OPERATION_V2_RECORD_SCHEMA,
    APPLICATION_EXPORT_PAGE_COMMITMENT_V1_RECORD_SCHEMA,
    SERVICE_AUDIT_V3_RECORD_SCHEMA,
    REPLICATION_ADMINISTRATION_V1_RECORD_SCHEMA,
    PRE_WP280_CAPABILITY_RECORD_SCHEMA,
    PRE_WP416_CAPABILITY_RECORD_SCHEMA,
    PRE_WP416_CAPABILITY_TOKEN_LOOKUP_RECORD_SCHEMA,
    PRE_WP416_CAPABILITY_BOOTSTRAP_RECORD_SCHEMA,
    PRE_WP416_CAPABILITY_ADMINISTRATION_RECORD_SCHEMA,
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
    PENDING_ADMISSION_V3_RECORD_SCHEMA,
    EXECUTION_FAILED_V3_RECORD_SCHEMA,
    OUTCOME_V3_RECORD_SCHEMA,
    CURRENT_V1_RECORD_SCHEMAS[13],
    DURABLE_EVENT_V2_RECORD_SCHEMA,
    OUTBOX_INTENT_V2_RECORD_SCHEMA,
    PROVENANCE_V2_RECORD_SCHEMA,
    COMMIT_V3_RECORD_SCHEMA,
    CURRENT_V1_RECORD_SCHEMAS[17],
    CURRENT_V1_RECORD_SCHEMAS[18],
    CURRENT_V1_RECORD_SCHEMAS[19],
    CURRENT_V1_RECORD_SCHEMAS[20],
    SERVICE_AUDIT_V2_RECORD_SCHEMA,
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
    RETENTION_WATERMARK_V1_RECORD_SCHEMA,
    RETENTION_HOLDS_V1_RECORD_SCHEMA,
    HISTORY_TOMBSTONE_V1_RECORD_SCHEMA,
    RETENTION_ADMINISTRATION_V1_RECORD_SCHEMA,
    REACTIVE_MODULE_V1_RECORD_SCHEMA,
    REACTIVE_MODULE_ADMINISTRATION_V1_RECORD_SCHEMA,
    EVENT_CONSUMER_V1_RECORD_SCHEMA,
    EVENT_CONSUMER_DELIVERY_V1_RECORD_SCHEMA,
    COMMAND_CAPSULE_V1_RECORD_SCHEMA,
    COMMAND_LOCATOR_V1_RECORD_SCHEMA,
    COMMAND_AUDIT_LOCATOR_V1_RECORD_SCHEMA,
    COMMAND_DERIVED_INDEX_CHECKPOINT_V1_RECORD_SCHEMA,
    APPLICATION_INSTALLATION_CAMPAIGN_V1_RECORD_SCHEMA,
    ENTITY_CHAIN_HEAD_V1_RECORD_SCHEMA,
    CHANGELOG_V2_ROTATION_RECEIPT_V1_RECORD_SCHEMA,
    COMMAND_CAPSULE_V4_RECORD_SCHEMA,
    COMMAND_SEGMENT_V3_RECORD_SCHEMA,
    VALIDATED_PREFIX_CHECKPOINT_V2_RECORD_SCHEMA,
    CAPABILITY_V3_RECORD_SCHEMA,
    CAPABILITY_V4_RECORD_SCHEMA,
    CAPABILITY_V5_RECORD_SCHEMA,
    APPLICATION_EXPORT_OPERATION_V1_RECORD_SCHEMA,
    COMMAND_CAPSULE_V5_RECORD_SCHEMA,
    COMMAND_SEGMENT_V4_RECORD_SCHEMA,
    CAPABILITY_V6_RECORD_SCHEMA,
    CAPABILITY_V7_RECORD_SCHEMA,
    CAPABILITY_V8_RECORD_SCHEMA,
    REGISTRY_V2_RECORD_SCHEMA,
    VECTOR_EVIDENCE_V1_RECORD_SCHEMA,
    VECTOR_OBSERVATION_V1_RECORD_SCHEMA,
    VECTOR_EVIDENCE_INDEX_V1_RECORD_SCHEMA,
    VECTOR_HEALTH_OBSERVATION_V1_RECORD_SCHEMA,
    VECTOR_PROJECTION_CONTROL_V1_RECORD_SCHEMA,
    COMMAND_CAPSULE_V6_RECORD_SCHEMA,
    COMMAND_SEGMENT_V5_RECORD_SCHEMA,
    CLEAN_CLOSE_LIFECYCLE_V1_RECORD_SCHEMA,
    COLUMNAR_PROJECTION_CONTROL_V1_RECORD_SCHEMA,
    CHANGELOG_ALLOCATOR_V3_RECORD_SCHEMA,
    AUTHORITATIVE_STATE_CATALOG_V1_RECORD_SCHEMA,
    LEADERSHIP_EPOCH_V1_RECORD_SCHEMA,
    CHANGELOG_HISTORY_STATE_V3_RECORD_SCHEMA,
    REPLICATION_FOLLOWER_STATE_V3_RECORD_SCHEMA,
    REPLICATION_SOURCE_HOLD_V1_RECORD_SCHEMA,
    COMMAND_CAPSULE_V7_RECORD_SCHEMA,
    COMMAND_SEGMENT_V6_RECORD_SCHEMA,
    REPLICATION_SOURCE_HOLD_V2_RECORD_SCHEMA,
    APPLICATION_EXPORT_OPERATION_V2_RECORD_SCHEMA,
    APPLICATION_EXPORT_PAGE_COMMITMENT_V1_RECORD_SCHEMA,
    SERVICE_AUDIT_V3_RECORD_SCHEMA,
    REPLICATION_ADMINISTRATION_V1_RECORD_SCHEMA,
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
    READABLE_RECORD_INDEX
        .get_or_init(|| schema_index(&READABLE_RECORD_SCHEMAS))
        .get(record_type)
        .copied()
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
    WRITABLE_RECORD_INDEX
        .get_or_init(|| schema_index(&WRITABLE_RECORD_SCHEMAS))
        .get(record_type)
        .copied()
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

static READABLE_RECORD_INDEX: OnceLock<HashMap<&'static str, &'static RecordSchema<'static>>> =
    OnceLock::new();
static WRITABLE_RECORD_INDEX: OnceLock<HashMap<&'static str, &'static RecordSchema<'static>>> =
    OnceLock::new();

/// Indexes a closed registry by record type, keeping the first entry.
///
/// Record types are NOT unique in the readable registry: four capability types
/// carry more than one compact tag, and `CapabilityRecordV1` carries three. The
/// linear scan this replaces resolved by `find`, which answers the first
/// matching entry, so the index must do the same or every decode of those types
/// would resolve a different schema. First-wins is the whole contract here.
///
/// Every encode and decode resolves a schema by name, and the scan compared up
/// to 111 long strings that share a common prefix.
fn schema_index(
    schemas: &'static [RecordSchema<'static>],
) -> HashMap<&'static str, &'static RecordSchema<'static>> {
    let mut index = HashMap::with_capacity(schemas.len());
    for schema in schemas {
        index.entry(schema.record_type()).or_insert(schema);
    }
    index
}

static RECORD_REGISTRY_DIGEST: OnceLock<SchemaHash> = OnceLock::new();

/// Returns the immutable digest of every readable compact tag/revision binding.
///
/// The registry is a compile-time constant, so the digest is invariant for the
/// life of the process. It is computed once and reused: the canonical ordering
/// is still established by that first computation, never skipped. Callers on
/// the journal and checkpoint-root validation paths reach this per operation,
/// and each uncached call sorted the whole registry, rebuilt a multi-kilobyte
/// canonical buffer and hashed it.
#[must_use]
pub fn record_registry_digest() -> SchemaHash {
    *RECORD_REGISTRY_DIGEST.get_or_init(compute_record_registry_digest)
}

fn compute_record_registry_digest() -> SchemaHash {
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

    #[test]
    fn prebuilt_current_payload_matches_the_typed_current_encoder_exactly() {
        let message = v1::StoredRecordRegistryV2 {
            registry_digest: record_registry_digest().as_bytes().to_vec(),
        };
        let payload = message.encode_to_vec();
        let typed = encode_current_message(&message).expect("typed current record encodes");
        let prebuilt = encode_current_payload::<v1::StoredRecordRegistryV2>(&payload)
            .expect("canonical prebuilt payload encodes");

        assert_eq!(prebuilt, typed);
    }

    #[test]
    fn structurally_proven_current_payload_matches_checked_framing_and_keeps_bounds() {
        let message = v1::StoredRecordRegistryV2 {
            registry_digest: record_registry_digest().as_bytes().to_vec(),
        };
        let payload = message.encode_to_vec();
        let checked = encode_current_payload::<v1::StoredRecordRegistryV2>(&payload)
            .expect("canonical prebuilt payload encodes");
        let proven =
            encode_current_payload_after_structural_proof::<v1::StoredRecordRegistryV2>(&payload)
                .expect("structurally proven payload frames");

        assert_eq!(proven, checked);

        let oversized =
            vec![0_u8; v1::StoredRecordRegistryV2::record_schema().max_payload_bytes() + 1];
        assert!(
            encode_current_payload_after_structural_proof::<v1::StoredRecordRegistryV2>(&oversized)
                .is_err()
        );
    }

    // req: GOV-001
    /// The registry digest is compared against digests already written into
    /// durable roots, so memoizing it may only ever return the value a fresh
    /// computation produces. A drift here would reject existing databases.
    #[test]
    fn memoized_registry_digest_equals_a_fresh_computation() {
        let fresh = super::compute_record_registry_digest();
        let cached = super::record_registry_digest();
        assert_eq!(cached, fresh);
        // Stable across repeated reads, and still equal after the cache is warm.
        assert_eq!(super::record_registry_digest(), fresh);
        assert_eq!(super::compute_record_registry_digest(), fresh);
    }

    // req: GOV-001
    /// Indexing the registries may only ever answer what the linear scan did,
    /// including for absent and near-miss record types, since every encode and
    /// decode resolves its schema through these lookups.
    #[test]
    fn schema_indexes_answer_exactly_what_a_linear_scan_answers() {
        for schema in &READABLE_RECORD_SCHEMAS {
            let scanned = READABLE_RECORD_SCHEMAS
                .iter()
                .find(|candidate| candidate.record_type() == schema.record_type())
                .expect("present by construction");
            let indexed = super::readable_record_schema(schema.record_type())
                .expect("index must find every readable schema");
            assert_eq!(indexed.record_type(), scanned.record_type());
            assert_eq!(indexed.compact_tag(), scanned.compact_tag());
            assert_eq!(indexed.schema_revision(), scanned.schema_revision());
        }
        for schema in &WRITABLE_RECORD_SCHEMAS {
            let indexed = super::writable_record_schema(schema.record_type())
                .expect("index must find every writable schema");
            assert_eq!(indexed.compact_tag(), schema.compact_tag());
        }
        // Absent and near-miss names still answer None.
        for absent in [
            "",
            "riffdb.storage.v1.StoredCommandCapsuleV999",
            "riffdb.storage.v1.StoredCommandCapsuleV",
            "unrelated",
        ] {
            assert!(super::readable_record_schema(absent).is_none());
            assert!(super::writable_record_schema(absent).is_none());
        }
        // Record types are not unique in the readable registry: four capability
        // types carry more than one compact tag. The lookup must answer the
        // first, exactly as the linear scan did.
        let mut first_seen: std::collections::BTreeMap<&str, u8> =
            std::collections::BTreeMap::new();
        let mut duplicated = 0_usize;
        for schema in &READABLE_RECORD_SCHEMAS {
            // or_insert keeps the FIRST tag, which is exactly what `find`
            // answered before the index existed.
            let before = first_seen.len();
            first_seen
                .entry(schema.record_type())
                .or_insert(schema.compact_tag());
            if first_seen.len() == before {
                duplicated += 1;
            }
        }
        assert!(
            duplicated > 0,
            "the first-wins contract is only meaningful while duplicates exist"
        );
        for (record_type, first_tag) in first_seen {
            assert_eq!(
                super::readable_record_schema(record_type)
                    .expect("indexed")
                    .compact_tag(),
                first_tag,
                "{record_type} must resolve to its first registered tag"
            );
        }

        // A readable-only type must still be absent from the writable registry.
        let readable_only = READABLE_RECORD_SCHEMAS.iter().find(|schema| {
            !WRITABLE_RECORD_SCHEMAS
                .iter()
                .any(|writable| writable.record_type() == schema.record_type())
        });
        if let Some(schema) = readable_only {
            assert!(super::readable_record_schema(schema.record_type()).is_some());
            assert!(super::writable_record_schema(schema.record_type()).is_none());
        }
    }
}
