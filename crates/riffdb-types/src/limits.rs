//! Process hard limits for canonical values and identifiers.

/// Maximum encoded size of one canonical value document, in bytes.
pub const MAX_CANONICAL_DOCUMENT_BYTES: usize = 1024 * 1024;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_match_the_v1_contract() {
        assert_eq!(MAX_CANONICAL_DOCUMENT_BYTES, 1_048_576);
        assert_eq!(MAX_STRING_BYTES, 1_048_576);
        assert_eq!(MAX_BYTES_VALUE_BYTES, 1_048_576);
        assert_eq!(MAX_LIST_ENTRIES, 65_535);
        assert_eq!(MAX_RECORD_FIELDS, 65_535);
        assert_eq!(MAX_KEY_BYTES, 4_096);
        assert_eq!(MAX_NESTING_DEPTH, 32);
        assert_eq!(MAX_ACTOR_ID_BYTES, 256);
        assert_eq!(MAX_IDEMPOTENCY_KEY_BYTES, 128);
        assert_eq!(MAX_CONTRACT_LINEAGE_BYTES, 256);
        assert_eq!(MAX_ENVIRONMENT_BYTES, 64);
        assert_eq!(MAX_TENANT_ID_BYTES, 256);
    }
}
