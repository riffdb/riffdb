//! Allocation-free structural preflight for durable semantic payloads.

use crate::wire::{Cursor, PreflightError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DurablePreflightError {
    Malformed,
    NonCanonical,
    LimitExceeded,
}

impl From<PreflightError> for DurablePreflightError {
    fn from(error: PreflightError) -> Self {
        match error {
            PreflightError::Malformed => Self::Malformed,
            PreflightError::LimitExceeded => Self::LimitExceeded,
        }
    }
}

const MAX_PREFLIGHT_DEPTH: usize = 32;
const MAX_VISITED_FIELDS: usize = 300_000;
const MAX_TEXT_ID_BYTES: usize = 256;
const MAX_ENVIRONMENT_BYTES: usize = 64;
const MAX_KEY_BYTES: usize = 4_096;
const MAX_DOCUMENT_BYTES: usize = 1024 * 1024;
const MAX_BUNDLE_BYTES: usize = 15 * 1024 * 1024;
const MAX_QUERY_MODULE_BYTES: usize = 16 * 1024 * 1024;
const MAX_COMMAND_ITEMS: usize = 4_096;
const MAX_CONFLICT_HASHES: usize = 2_046;
const MAX_PACKED_ITEMS: usize = 65_535 + 19;

const RECORD_REGISTRY_V2_RULES: [Rule; 1] = [fixed_bytes(1, 32)];
const RECORD_REGISTRY_V2: Shape = Shape {
    rules: &RECORD_REGISTRY_V2_RULES,
};

#[derive(Clone, Copy)]
struct Shape {
    rules: &'static [Rule],
}

#[derive(Clone, Copy)]
struct Rule {
    number: u32,
    maximum_occurrences: usize,
    kind: Kind,
}

#[derive(Clone, Copy)]
enum Kind {
    Opaque {
        minimum_bytes: usize,
        maximum_bytes: usize,
        utf8: bool,
    },
    Message(&'static Shape),
    PackedVarints {
        maximum_items: usize,
    },
}

const fn bytes(number: u32, maximum_bytes: usize) -> Rule {
    opaque(number, 1, 0, maximum_bytes, false)
}

const fn nonempty_bytes(number: u32, maximum_bytes: usize) -> Rule {
    opaque(number, 1, 1, maximum_bytes, false)
}

const fn fixed_bytes(number: u32, bytes: usize) -> Rule {
    opaque(number, 1, bytes, bytes, false)
}

const fn string(number: u32, maximum_bytes: usize) -> Rule {
    opaque(number, 1, 0, maximum_bytes, true)
}

const fn repeated_string(number: u32, maximum: usize, maximum_bytes: usize) -> Rule {
    opaque(number, maximum, 0, maximum_bytes, true)
}

const fn repeated_bytes(number: u32, maximum: usize, maximum_bytes: usize) -> Rule {
    opaque(number, maximum, 0, maximum_bytes, false)
}

const fn repeated_fixed_bytes(number: u32, maximum: usize, bytes: usize) -> Rule {
    opaque(number, maximum, bytes, bytes, false)
}

const fn opaque(
    number: u32,
    maximum_occurrences: usize,
    minimum_bytes: usize,
    maximum_bytes: usize,
    utf8: bool,
) -> Rule {
    Rule {
        number,
        maximum_occurrences,
        kind: Kind::Opaque {
            minimum_bytes,
            maximum_bytes,
            utf8,
        },
    }
}

const fn message(number: u32, shape: &'static Shape) -> Rule {
    repeated_message(number, 1, shape)
}

const fn repeated_message(number: u32, maximum: usize, shape: &'static Shape) -> Rule {
    Rule {
        number,
        maximum_occurrences: maximum,
        kind: Kind::Message(shape),
    }
}

const fn packed_varints(number: u32, maximum_items: usize) -> Rule {
    Rule {
        number,
        maximum_occurrences: 1,
        kind: Kind::PackedVarints { maximum_items },
    }
}

macro_rules! shape {
    ($name:ident [$($rule:expr),* $(,)?]) => {
        static $name: Shape = Shape { rules: &[$($rule),*] };
    };
}

shape!(UNIT []);
shape!(TIMESTAMP []);
shape!(EVENT_ID []);
shape!(TENANT_SCOPE [message(1, &UNIT), string(2, MAX_TEXT_ID_BYTES)]);
shape!(ADMITTED_ACTOR [
    string(1, MAX_TEXT_ID_BYTES),
    message(3, &TENANT_SCOPE),
    fixed_bytes(4, 16),
]);
shape!(AUDIT_PRINCIPAL [
    string(1, MAX_TEXT_ID_BYTES),
    fixed_bytes(3, 16),
]);

shape!(PLAN [
    string(1, MAX_TEXT_ID_BYTES),
    fixed_bytes(3, 32),
    fixed_bytes(5, 32),
]);
shape!(SCHEMA_BINDING [
    string(1, MAX_TEXT_ID_BYTES),
    fixed_bytes(3, 32),
]);
shape!(IDEMPOTENCY_DIGEST[fixed_bytes(3, 32)]);
shape!(IDEMPOTENCY_IDENTITY [
    fixed_bytes(1, 16),
    string(2, MAX_ENVIRONMENT_BYTES),
    message(3, &TENANT_SCOPE),
    string(4, MAX_TEXT_ID_BYTES),
    string(5, MAX_TEXT_ID_BYTES),
    message(7, &IDEMPOTENCY_DIGEST),
]);
shape!(PROVENANCE_CLAIMS [
    string(1, 512),
    string(2, 128),
    string(3, 1_024),
    string(4, MAX_TEXT_ID_BYTES),
]);
shape!(ENTITY_TARGET[bytes(2, MAX_KEY_BYTES)]);
shape!(EXPECTED_ENTITY_STATE[message(1, &UNIT)]);
shape!(INDEX_EPOCH_POSITION[message(1, &UNIT)]);
shape!(ENTITY_READ_DEPENDENCY [
    message(1, &ENTITY_TARGET),
    message(2, &EXPECTED_ENTITY_STATE),
]);
shape!(INDEX_RANGE_DEPENDENCY [
    bytes(1, MAX_KEY_BYTES),
    message(2, &INDEX_EPOCH_POSITION),
]);
shape!(READ_DEPENDENCY [
    message(1, &ENTITY_READ_DEPENDENCY),
    message(2, &INDEX_RANGE_DEPENDENCY),
]);
shape!(READ_DEPENDENCIES[repeated_message(1, MAX_COMMAND_ITEMS, &READ_DEPENDENCY,)]);
shape!(DECLARED_OUTCOME[bytes(2, MAX_DOCUMENT_BYTES)]);
shape!(ENTITY_RECORD [
    message(1, &ENTITY_TARGET),
    message(4, &SCHEMA_BINDING),
    bytes(5, MAX_DOCUMENT_BYTES),
]);
shape!(INDEX_ENTRY [
    bytes(1, MAX_KEY_BYTES),
    message(2, &SCHEMA_BINDING),
    bytes(3, MAX_DOCUMENT_BYTES),
]);
shape!(INDEX_ENTRY_V2 [
    bytes(1, MAX_KEY_BYTES),
    message(2, &SCHEMA_BINDING),
    bytes(3, MAX_DOCUMENT_BYTES),
    bytes(4, MAX_KEY_BYTES),
]);
shape!(INDEX_EPOCH [
    bytes(1, MAX_KEY_BYTES),
    message(2, &SCHEMA_BINDING),
]);
shape!(INDEX_GENERATION_V2 [
    bytes(1, MAX_KEY_BYTES),
    message(3, &SCHEMA_BINDING),
]);
shape!(PENDING_ADMISSION [
    message(1, &IDEMPOTENCY_IDENTITY),
    fixed_bytes(2, 32),
    fixed_bytes(3, 16),
    message(4, &PLAN),
    message(5, &TIMESTAMP),
    message(6, &ADMITTED_ACTOR),
    bytes(7, MAX_KEY_BYTES),
    message(8, &PROVENANCE_CLAIMS),
]);
shape!(EXECUTION_FAILED[message(1, &PENDING_ADMISSION)]);
shape!(OUTCOME [
    message(1, &IDEMPOTENCY_IDENTITY),
    fixed_bytes(3, 16),
    message(4, &PLAN),
    fixed_bytes(5, 32),
    message(6, &ADMITTED_ACTOR),
    message(7, &TIMESTAMP),
    fixed_bytes(8, 32),
    repeated_fixed_bytes(9, MAX_CONFLICT_HASHES, 32),
    message(10, &DECLARED_OUTCOME),
    message(11, &PROVENANCE_CLAIMS),
    fixed_bytes(12, 16),
    bytes(14, MAX_KEY_BYTES),
]);
shape!(DURABLE_EVENT [
    message(1, &EVENT_ID),
    bytes(3, MAX_DOCUMENT_BYTES),
    fixed_bytes(4, 32),
]);
shape!(AFFECTED_ENTITY[message(1, &ENTITY_TARGET)]);
shape!(PROVENANCE [
    fixed_bytes(1, 16),
    message(3, &IDEMPOTENCY_IDENTITY),
    fixed_bytes(4, 16),
    message(5, &PLAN),
    fixed_bytes(6, 32),
    message(7, &ADMITTED_ACTOR),
    message(8, &TIMESTAMP),
    fixed_bytes(9, 32),
    repeated_fixed_bytes(10, MAX_CONFLICT_HASHES, 32),
    repeated_message(12, MAX_COMMAND_ITEMS, &AFFECTED_ENTITY),
    repeated_message(13, MAX_COMMAND_ITEMS, &EVENT_ID),
    message(14, &PROVENANCE_CLAIMS),
]);
shape!(COMMITTED_MUTATION [
    message(1, &EXPECTED_ENTITY_STATE),
    message(2, &ENTITY_RECORD),
]);
shape!(EVENT_REFERENCE_V2 [
    message(1, &EVENT_ID),
    fixed_bytes(2, 32),
]);
shape!(COMMIT [
    fixed_bytes(2, 16),
    message(3, &PLAN),
    fixed_bytes(4, 32),
    message(5, &ADMITTED_ACTOR),
    message(6, &TIMESTAMP),
    fixed_bytes(7, 32),
    repeated_fixed_bytes(8, MAX_CONFLICT_HASHES, 32),
    message(9, &READ_DEPENDENCIES),
    repeated_message(10, MAX_COMMAND_ITEMS, &COMMITTED_MUTATION),
    repeated_message(11, MAX_COMMAND_ITEMS, &DURABLE_EVENT),
    message(12, &DECLARED_OUTCOME),
    fixed_bytes(13, 16),
    repeated_message(14, MAX_COMMAND_ITEMS, &EVENT_ID),
]);
shape!(COMMIT_V2 [
    fixed_bytes(2, 16),
    message(3, &PLAN),
    fixed_bytes(4, 32),
    message(5, &ADMITTED_ACTOR),
    message(6, &TIMESTAMP),
    fixed_bytes(7, 32),
    repeated_fixed_bytes(8, MAX_CONFLICT_HASHES, 32),
    message(9, &READ_DEPENDENCIES),
    repeated_message(10, MAX_COMMAND_ITEMS, &COMMITTED_MUTATION),
    repeated_message(11, MAX_COMMAND_ITEMS, &EVENT_REFERENCE_V2),
    message(12, &DECLARED_OUTCOME),
    fixed_bytes(13, 16),
    repeated_message(14, MAX_COMMAND_ITEMS, &EVENT_ID),
]);

shape!(CONTRACT_BUNDLE [
    string(1, MAX_TEXT_ID_BYTES),
    fixed_bytes(3, 32),
    nonempty_bytes(4, MAX_BUNDLE_BYTES),
]);
shape!(ACTIVE_CATALOG [
    string(1, MAX_TEXT_ID_BYTES),
    fixed_bytes(3, 32),
]);
shape!(CATALOG_ADMINISTRATION [
    fixed_bytes(2, 16),
    message(3, &TIMESTAMP),
    message(4, &AUDIT_PRINCIPAL),
    message(5, &ACTIVE_CATALOG),
    message(6, &ACTIVE_CATALOG),
    string(7, MAX_TEXT_ID_BYTES),
]);
shape!(QUERY_MODULE [
    string(1, MAX_TEXT_ID_BYTES),
    fixed_bytes(3, 32),
    string(4, MAX_TEXT_ID_BYTES),
    fixed_bytes(6, 32),
    nonempty_bytes(7, MAX_QUERY_MODULE_BYTES),
]);
shape!(ACTIVE_QUERY_MODULE [
    string(1, MAX_TEXT_ID_BYTES),
    fixed_bytes(3, 32),
    string(4, MAX_TEXT_ID_BYTES),
    fixed_bytes(6, 32),
]);
shape!(QUERY_MODULE_ADMINISTRATION [
    fixed_bytes(2, 16),
    message(3, &TIMESTAMP),
    message(4, &AUDIT_PRINCIPAL),
    message(5, &ACTIVE_QUERY_MODULE),
    message(6, &ACTIVE_QUERY_MODULE),
    string(7, MAX_TEXT_ID_BYTES),
]);

shape!(CAPABILITY_TOKEN_DIGEST[fixed_bytes(3, 32)]);
shape!(CAPABILITY_PERMISSION[string(2, MAX_TEXT_ID_BYTES)]);
shape!(CAPABILITY_PERMISSIONS[repeated_message(1, 8_192, &CAPABILITY_PERMISSION,)]);
shape!(SCOPED_PARTITION [
    string(1, MAX_TEXT_ID_BYTES),
    bytes(2, MAX_KEY_BYTES),
]);
shape!(EXPLICIT_PARTITION_SCOPE[repeated_message(1, 1_024, &SCOPED_PARTITION,)]);
shape!(PARTITION_SCOPE [
    message(1, &UNIT),
    message(2, &EXPLICIT_PARTITION_SCOPE),
]);
shape!(ENTITY_FIELD_VISIBILITY [
    string(1, MAX_TEXT_ID_BYTES),
    packed_varints(3, 65_535),
]);
shape!(CAPABILITY_GRANT [
    message(1, &TENANT_SCOPE),
    message(2, &PARTITION_SCOPE),
    message(3, &CAPABILITY_PERMISSIONS),
    repeated_message(4, 65_535, &ENTITY_FIELD_VISIBILITY),
    packed_varints(6, 19),
]);
shape!(REVOKED_CAPABILITY[message(1, &TIMESTAMP)]);
shape!(CAPABILITY_LIFECYCLE [
    message(1, &UNIT),
    message(2, &REVOKED_CAPABILITY),
]);
shape!(CAPABILITY_RECORD [
    fixed_bytes(1, 16),
    message(3, &CAPABILITY_TOKEN_DIGEST),
    fixed_bytes(4, 16),
    string(5, MAX_ENVIRONMENT_BYTES),
    string(6, MAX_TEXT_ID_BYTES),
    repeated_string(8, 8, 512),
    message(9, &TIMESTAMP),
    message(10, &TIMESTAMP),
    fixed_bytes(12, 16),
    message(13, &CAPABILITY_GRANT),
    message(14, &CAPABILITY_LIFECYCLE),
]);
shape!(CAPABILITY_LOOKUP[fixed_bytes(1, 16)]);
shape!(CAPABILITY_BOOTSTRAP [
    fixed_bytes(1, 16),
    fixed_bytes(2, 16),
]);
shape!(CAPABILITY_ADMINISTRATION [
    fixed_bytes(2, 16),
    message(4, &TIMESTAMP),
    message(5, &AUDIT_PRINCIPAL),
    fixed_bytes(6, 16),
    string(8, MAX_TEXT_ID_BYTES),
]);

shape!(CONTRACT_VERSION_TARGET[string(1, MAX_TEXT_ID_BYTES)]);
shape!(ENTITY_TYPE_TARGET[string(1, MAX_TEXT_ID_BYTES)]);
shape!(COMMAND_TARGET[string(1, MAX_TEXT_ID_BYTES)]);
shape!(PROJECTION_TARGET[string(1, MAX_TEXT_ID_BYTES)]);
shape!(INDEX_TARGET[string(1, MAX_TEXT_ID_BYTES)]);
shape!(SERVICE_AUDIT_TARGET [
    string(1, MAX_TEXT_ID_BYTES),
    message(2, &CONTRACT_VERSION_TARGET),
    message(3, &ENTITY_TYPE_TARGET),
    message(4, &COMMAND_TARGET),
    message(5, &PROJECTION_TARGET),
    message(6, &INDEX_TARGET),
    fixed_bytes(8, 16),
    fixed_bytes(9, 16),
]);
shape!(COMMAND_AUDIT_LINK[fixed_bytes(2, 16)]);
shape!(CONTROL_AUDIT_LINK []);
shape!(SERVICE_AUDIT_LINK [
    message(1, &UNIT),
    message(2, &COMMAND_AUDIT_LINK),
    message(3, &CONTROL_AUDIT_LINK),
]);
shape!(SERVICE_AUDIT [
    fixed_bytes(2, 16),
    message(3, &TIMESTAMP),
    message(6, &AUDIT_PRINCIPAL),
    repeated_message(8, 16, &SERVICE_AUDIT_TARGET),
    string(9, MAX_TEXT_ID_BYTES),
    message(10, &SERVICE_AUDIT_LINK),
]);

shape!(OUTBOX_INTENT[message(1, &DURABLE_EVENT)]);
shape!(OUTBOX_INTENT_V2[message(1, &EVENT_REFERENCE_V2)]);
shape!(OUTBOX_RETRY [
    message(2, &TIMESTAMP),
    message(3, &TIMESTAMP),
    string(4, 512),
    string(5, 1_024),
]);
shape!(OUTBOX_DELIVERING [
    string(2, 512),
    message(3, &TIMESTAMP),
    message(4, &TIMESTAMP),
]);
shape!(OUTBOX_DELIVERED [string(2, 512), message(3, &TIMESTAMP)]);
shape!(OUTBOX_DEAD_LETTER [
    string(2, 512),
    message(3, &TIMESTAMP),
    string(4, 1_024),
]);
shape!(OUTBOX_STATUS [
    message(1, &EVENT_ID),
    message(2, &OUTBOX_RETRY),
    message(3, &OUTBOX_DELIVERING),
    message(4, &OUTBOX_DELIVERED),
    message(5, &OUTBOX_DEAD_LETTER),
]);

shape!(PROJECTION_IDENTITY [
    string(1, MAX_TEXT_ID_BYTES),
    fixed_bytes(3, 32),
]);
shape!(FRONTIER_POSITION[message(1, &UNIT)]);
shape!(GENERATION_POSITION[message(2, &FRONTIER_POSITION)]);
shape!(PROJECTION_FAILURE []);
shape!(PROJECTION_STATE [
    message(1, &PROJECTION_IDENTITY),
    repeated_bytes(3, 1_024, MAX_DOCUMENT_BYTES),
    bytes(4, MAX_DOCUMENT_BYTES),
]);
shape!(PROJECTION_APPLY [
    message(1, &PROJECTION_IDENTITY),
    fixed_bytes(4, 32),
]);
shape!(PROJECTION_CONTROL [
    message(1, &PROJECTION_IDENTITY),
    message(3, &GENERATION_POSITION),
    message(4, &GENERATION_POSITION),
    message(7, &PROJECTION_FAILURE),
]);

const ROOTS: [&Shape; 36] = [
    &Shape { rules: &[] },
    &Shape {
        rules: &[fixed_bytes(1, 16)],
    },
    &Shape {
        rules: &[message(2, &UNIT)],
    },
    &Shape {
        rules: &[message(2, &UNIT)],
    },
    &CONTRACT_BUNDLE,
    &ACTIVE_CATALOG,
    &CATALOG_ADMINISTRATION,
    &ENTITY_RECORD,
    &INDEX_ENTRY,
    &INDEX_EPOCH,
    &PENDING_ADMISSION,
    &EXECUTION_FAILED,
    &OUTCOME,
    &DURABLE_EVENT,
    &OUTBOX_INTENT,
    &PROVENANCE,
    &COMMIT,
    &CAPABILITY_RECORD,
    &CAPABILITY_LOOKUP,
    &CAPABILITY_BOOTSTRAP,
    &CAPABILITY_ADMINISTRATION,
    &SERVICE_AUDIT,
    &OUTBOX_STATUS,
    &PROJECTION_STATE,
    &PROJECTION_APPLY,
    &PROJECTION_CONTROL,
    &QUERY_MODULE,
    &ACTIVE_QUERY_MODULE,
    &QUERY_MODULE_ADMINISTRATION,
    &INDEX_ENTRY_V2,
    &RECORD_REGISTRY_V2,
    &COMMIT_V2,
    &OUTBOX_INTENT_V2,
    &INDEX_GENERATION_V2,
    // StoredHistoryIncarnationV1 is a single scalar varint; preflight ignores wire-type 0.
    &Shape { rules: &[] },
    // StoredServiceAuditRequestIndexV1: fixed 16-byte request id; sequence is wire-type 0.
    &Shape {
        rules: &[fixed_bytes(1, 16)],
    },
];

pub(crate) fn payload(record_index: usize, input: &[u8]) -> Result<(), DurablePreflightError> {
    let shape = ROOTS
        .get(record_index)
        .ok_or(DurablePreflightError::Malformed)?;
    let mut budget = Budget {
        visited_fields: 0,
        packed_items: 0,
    };
    preflight(input, shape, 0, &mut budget)
}

struct Budget {
    visited_fields: usize,
    packed_items: usize,
}

impl Budget {
    fn claim(&mut self, count: usize) -> Result<(), DurablePreflightError> {
        self.visited_fields = self
            .visited_fields
            .checked_add(count)
            .ok_or(DurablePreflightError::LimitExceeded)?;
        if self.visited_fields > MAX_VISITED_FIELDS {
            return Err(DurablePreflightError::LimitExceeded);
        }
        Ok(())
    }

    fn claim_packed_item(&mut self) -> Result<(), DurablePreflightError> {
        self.packed_items = self
            .packed_items
            .checked_add(1)
            .ok_or(DurablePreflightError::LimitExceeded)?;
        if self.packed_items > MAX_PACKED_ITEMS {
            return Err(DurablePreflightError::LimitExceeded);
        }
        self.claim(1)
    }
}

fn preflight(
    input: &[u8],
    shape: &Shape,
    depth: usize,
    budget: &mut Budget,
) -> Result<(), DurablePreflightError> {
    if depth > MAX_PREFLIGHT_DEPTH || shape.rules.len() > 16 {
        return Err(DurablePreflightError::LimitExceeded);
    }
    let mut occurrences = [0_usize; 16];
    let mut packed_items = [0_usize; 16];
    let mut cursor = Cursor::new(input);
    while let Some(field) = cursor.next()? {
        budget.claim(1)?;
        let Some((rule_index, rule)) = shape
            .rules
            .iter()
            .enumerate()
            .find(|(_, rule)| rule.number == field.number)
        else {
            continue;
        };
        if field.wire_type != 2 {
            return Err(DurablePreflightError::Malformed);
        }
        occurrences[rule_index] = occurrences[rule_index]
            .checked_add(1)
            .ok_or(DurablePreflightError::LimitExceeded)?;
        if occurrences[rule_index] > rule.maximum_occurrences {
            return Err(if rule.maximum_occurrences == 1 {
                DurablePreflightError::NonCanonical
            } else {
                DurablePreflightError::LimitExceeded
            });
        }
        match rule.kind {
            Kind::Opaque {
                minimum_bytes,
                maximum_bytes,
                utf8,
            } => {
                if (minimum_bytes == maximum_bytes && field.bytes.len() != minimum_bytes)
                    || field.bytes.len() < minimum_bytes
                {
                    return Err(DurablePreflightError::Malformed);
                }
                if field.bytes.len() > maximum_bytes {
                    return Err(DurablePreflightError::LimitExceeded);
                }
                if utf8 && std::str::from_utf8(field.bytes).is_err() {
                    return Err(DurablePreflightError::Malformed);
                }
            }
            Kind::Message(nested) => preflight(field.bytes, nested, depth + 1, budget)?,
            Kind::PackedVarints { maximum_items } => {
                let mut packed = Cursor::new(field.bytes);
                while !packed.input_is_empty() {
                    packed.read_varint()?;
                    packed_items[rule_index] = packed_items[rule_index]
                        .checked_add(1)
                        .ok_or(DurablePreflightError::LimitExceeded)?;
                    budget.claim_packed_item()?;
                    if packed_items[rule_index] > maximum_items {
                        return Err(DurablePreflightError::LimitExceeded);
                    }
                }
            }
        }
    }
    Ok(())
}
