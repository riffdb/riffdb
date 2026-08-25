//! Process hard limits for canonical values and identifiers.

/// Maximum encoded size of one canonical value document, in bytes.
pub const MAX_CANONICAL_DOCUMENT_BYTES: usize = 1024 * 1024;

/// Maximum structurally decoded bytes in one public application-service request.
pub const MAX_APPLICATION_REQUEST_BYTES_V1: usize = 1024 * 1024;

/// Maximum UTF-8 byte length of one canonical string value.
pub const MAX_STRING_BYTES: usize = 1024 * 1024;

/// Maximum length of one canonical byte value.
pub const MAX_BYTES_VALUE_BYTES: usize = 1024 * 1024;

/// Maximum number of values in one canonical list.
pub const MAX_LIST_ENTRIES: usize = 65_535;

/// Maximum number of fields in one canonical record.
pub const MAX_RECORD_FIELDS: usize = 65_535;

/// Maximum encoded size of one durable key, in bytes.
pub const MAX_KEY_BYTES: usize = 4_096;

/// Maximum conflict-key derivations and acquisition entries for one v1 command.
pub const MAX_COMMAND_CONFLICT_KEYS_V1: usize = 256;

/// Maximum worst-case index-entry deltas produced by one v1 command.
pub const MAX_COMMAND_INDEX_DELTAS_V1: usize = 4_096;

/// Maximum mutation-affected index-prefix epoch targets produced by one command.
pub const MAX_COMMAND_AFFECTED_INDEX_PREFIXES_V1: usize = 65_535;

/// Maximum binding/root validation positions represented by one v1 plan mask.
pub const MAX_COMMAND_VALIDATION_TARGETS_V1: usize = 4_096;

/// Maximum complete transaction-current validation positions for one command.
pub const MAX_COMMAND_INDEX_VALIDATION_POSITIONS_V1: usize = 65_535;

/// Maximum correlated index work for one command.
///
/// The charge is index-entry deltas plus mutation-affected prefix epochs plus
/// complete transaction-current validation positions.
pub const MAX_COMMAND_INDEX_WORK_UNITS_V1: usize = 65_535;

/// Maximum semantic bytes retained for one v1 command's transaction-current read state.
pub const MAX_COMMAND_READ_STATE_SEMANTIC_BYTES_V1: usize = 16 * 1024 * 1024;

/// Maximum nesting depth of one canonical value.
pub const MAX_NESTING_DEPTH: usize = 32;

/// Maximum UTF-8 byte length of an actor identifier.
pub const MAX_ACTOR_ID_BYTES: usize = 256;

/// Maximum UTF-8 byte length of a caller idempotency key.
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = 128;

/// Maximum UTF-8 byte length of a contract lineage name.
pub const MAX_CONTRACT_LINEAGE_BYTES: usize = 256;

/// Maximum ASCII byte length of an environment slug.
pub const MAX_ENVIRONMENT_BYTES: usize = 64;

/// Maximum UTF-8 byte length of a tenant identifier.
pub const MAX_TENANT_ID_BYTES: usize = 256;

/// Maximum visible-ASCII byte length of one configured audience identity.
pub const MAX_AUDIENCE_BYTES: usize = 512;

/// Maximum visible-ASCII byte length of one validated approval identifier.
pub const MAX_APPROVAL_ID_BYTES: usize = 256;

/// Maximum UTF-8 byte length of one source-repository provenance claim.
pub const MAX_SOURCE_REPOSITORY_BYTES: usize = 512;

/// Maximum visible-ASCII byte length of one source-commit provenance claim.
pub const MAX_SOURCE_COMMIT_BYTES: usize = 128;

/// Maximum UTF-8 byte length of one admitted provenance reason.
pub const MAX_PROVENANCE_REASON_BYTES: usize = 1_024;

/// Maximum visible-ASCII byte length of one untrusted approval reference.
pub const MAX_APPROVAL_REFERENCE_BYTES: usize = 256;

/// Maximum number of protected-object targets in one service-audit record.
pub const MAX_SERVICE_AUDIT_TARGETS: usize = 16;

/// Maximum semantic encoded size of one pre-commit intent.
pub const MAX_COMMIT_INTENT_SEMANTIC_BYTES: usize = 15 * 1024 * 1024;

/// Fixed portion of a pre-commit intent reserved for non-runtime fields.
pub const COMMIT_INTENT_NON_RUNTIME_RESERVE_BYTES: usize = 64 * 1024;

/// Maximum semantic encoded size of one runtime-evaluated command.
pub const MAX_EVALUATED_COMMAND_SEMANTIC_BYTES: usize =
    MAX_COMMIT_INTENT_SEMANTIC_BYTES - COMMIT_INTENT_NON_RUNTIME_RESERVE_BYTES;

/// Maximum number of components in one projection group schema or key.
pub const MAX_PROJECTION_GROUP_COMPONENTS: usize = 1_024;

/// Maximum semantic encoded size of one stored projection-state payload.
pub const MAX_PROJECTION_STATE_SEMANTIC_BYTES: usize = 1024 * 1024;

/// Maximum number of distinct row updates in one projection apply request.
pub const MAX_PROJECTION_ROW_UPDATES: usize = 4_096;

/// Maximum canonical semantic content in one projection apply request.
pub const MAX_PROJECTION_APPLY_SEMANTIC_BYTES: usize = 15 * 1024 * 1024;

/// Maximum complete staged write set for one projection apply request.
pub const MAX_PROJECTION_WRITE_SET_BYTES: usize = 16 * 1024 * 1024;

/// Maximum semantic content returned by one projection apply snapshot.
pub const MAX_PROJECTION_APPLY_SNAPSHOT_BYTES: usize = 16 * 1024 * 1024;

/// Maximum rows returned by one projection query.
pub const MAX_PROJECTION_QUERY_ROWS: usize = 500;

/// Maximum encoded row content returned by one projection query.
pub const MAX_PROJECTION_QUERY_CONTENT_BYTES: usize = 4 * 1024 * 1024;

/// Maximum canonical bytes retained for one exact application-installation campaign state.
///
/// The value includes the immutable installation plan plus bounded completed-stage evidence.
pub const MAX_APPLICATION_INSTALLATION_CAMPAIGN_STATE_BYTES: usize = 8 * 1024 * 1024;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_match_the_v1_contract() {
        assert_eq!(MAX_CANONICAL_DOCUMENT_BYTES, 1_048_576);
        assert_eq!(MAX_APPLICATION_REQUEST_BYTES_V1, 1_048_576);
        assert_eq!(MAX_STRING_BYTES, 1_048_576);
        assert_eq!(MAX_BYTES_VALUE_BYTES, 1_048_576);
        assert_eq!(MAX_LIST_ENTRIES, 65_535);
        assert_eq!(MAX_RECORD_FIELDS, 65_535);
        assert_eq!(MAX_KEY_BYTES, 4_096);
        assert_eq!(MAX_COMMAND_CONFLICT_KEYS_V1, 256);
        assert_eq!(MAX_COMMAND_INDEX_DELTAS_V1, 4_096);
        assert_eq!(MAX_COMMAND_AFFECTED_INDEX_PREFIXES_V1, 65_535);
        assert_eq!(MAX_COMMAND_VALIDATION_TARGETS_V1, 4_096);
        assert_eq!(MAX_COMMAND_INDEX_VALIDATION_POSITIONS_V1, 65_535);
        assert_eq!(MAX_COMMAND_INDEX_WORK_UNITS_V1, 65_535);
        assert_eq!(MAX_COMMAND_READ_STATE_SEMANTIC_BYTES_V1, 16_777_216);
        assert_eq!(MAX_NESTING_DEPTH, 32);
        assert_eq!(MAX_ACTOR_ID_BYTES, 256);
        assert_eq!(MAX_IDEMPOTENCY_KEY_BYTES, 128);
        assert_eq!(MAX_CONTRACT_LINEAGE_BYTES, 256);
        assert_eq!(MAX_ENVIRONMENT_BYTES, 64);
        assert_eq!(MAX_TENANT_ID_BYTES, 256);
        assert_eq!(MAX_AUDIENCE_BYTES, 512);
        assert_eq!(MAX_APPROVAL_ID_BYTES, 256);
        assert_eq!(MAX_SOURCE_REPOSITORY_BYTES, 512);
        assert_eq!(MAX_SOURCE_COMMIT_BYTES, 128);
        assert_eq!(MAX_PROVENANCE_REASON_BYTES, 1_024);
        assert_eq!(MAX_APPROVAL_REFERENCE_BYTES, 256);
        assert_eq!(MAX_SERVICE_AUDIT_TARGETS, 16);
        assert_eq!(MAX_COMMIT_INTENT_SEMANTIC_BYTES, 15_728_640);
        assert_eq!(COMMIT_INTENT_NON_RUNTIME_RESERVE_BYTES, 65_536);
        assert_eq!(MAX_EVALUATED_COMMAND_SEMANTIC_BYTES, 15_663_104);
        assert_eq!(MAX_PROJECTION_GROUP_COMPONENTS, 1_024);
        assert_eq!(MAX_PROJECTION_STATE_SEMANTIC_BYTES, 1_048_576);
        assert_eq!(MAX_PROJECTION_ROW_UPDATES, 4_096);
        assert_eq!(MAX_PROJECTION_APPLY_SEMANTIC_BYTES, 15_728_640);
        assert_eq!(MAX_PROJECTION_WRITE_SET_BYTES, 16_777_216);
        assert_eq!(MAX_PROJECTION_APPLY_SNAPSHOT_BYTES, 16_777_216);
        assert_eq!(MAX_PROJECTION_QUERY_ROWS, 500);
        assert_eq!(MAX_PROJECTION_QUERY_CONTENT_BYTES, 4_194_304);
        // Vector bounds live in `crate::vector` beside their types; the v1
        // pin covers them like every other bound (previously they escaped it).
        assert_eq!(crate::MAX_VECTOR_DIMENSION, 4_096);
        assert_eq!(crate::EmbeddingMetadata::MAX_MODEL_STRING_LEN, 256);
    }
}
