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
const MAX_COMMAND_INDEX_TRANSITIONS: usize = 65_535;
const MAX_COMMAND_SEGMENT_COMMANDS: usize = 256;
const MAX_COMMAND_PREFIX_BYTES: usize = 16 * 1024 * 1024;
const MAX_COMMAND_SEGMENT_INDEX_ENTRIES: usize = 65_535;
const MAX_CONFLICT_HASHES: usize = 2_046;
const MAX_PACKED_ITEMS: usize = 65_535 + 19;
const MAX_SHAPE_RULES: usize = 20;
const MAX_SHAPE_FIELD_NUMBER: usize = 20;

const RECORD_REGISTRY_V2_RULES: [Rule; 1] = [fixed_bytes(1, 32)];
const RECORD_REGISTRY_V2: Shape = Shape::new(&RECORD_REGISTRY_V2_RULES);

#[derive(Clone, Copy)]
struct Shape {
    rules: &'static [Rule],
    rule_slots: [u8; MAX_SHAPE_FIELD_NUMBER + 1],
    locator_wire_bytes: Option<usize>,
}

impl Shape {
    const fn new(rules: &'static [Rule]) -> Self {
        assert!(rules.len() <= MAX_SHAPE_RULES);
        let mut rule_slots = [0_u8; MAX_SHAPE_FIELD_NUMBER + 1];
        let mut index = 0;
        while index < rules.len() {
            let number = rules[index].number as usize;
            assert!(number > 0 && number <= MAX_SHAPE_FIELD_NUMBER);
            assert!(rule_slots[number] == 0);
            rule_slots[number] = (index + 1) as u8;
            index += 1;
        }
        Self {
            rules,
            rule_slots,
            locator_wire_bytes: None,
        }
    }

    const fn with_locator_wire_type<T>(mut self) -> Self {
        self.locator_wire_bytes = Some(std::mem::size_of::<T>());
        self
    }

    fn rule(&self, number: u32) -> Option<(usize, &Rule)> {
        let slot = self.rule_slots.get(number as usize).copied().unwrap_or(0);
        let index = usize::from(slot).checked_sub(1)?;
        self.rules.get(index).map(|rule| (index, rule))
    }
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
    ($name:ident : $wire:ident [$($rule:expr),* $(,)?]) => {
        static $name: Shape = Shape::new(&[$($rule),*])
            .with_locator_wire_type::<crate::storage::v1::$wire>();
    };
    ($name:ident [$($rule:expr),* $(,)?]) => {
        static $name: Shape = Shape::new(&[$($rule),*]);
    };
}

shape!(UNIT: UnitV1 []);
shape!(TIMESTAMP: TimestampV1 []);
shape!(EVENT_ID: EventIdV1 []);
shape!(TENANT_SCOPE: TenantScopeV1 [message(1, &UNIT), string(2, MAX_TEXT_ID_BYTES)]);
shape!(ADMITTED_ACTOR: AdmittedActorContextV1 [
    string(1, MAX_TEXT_ID_BYTES),
    message(3, &TENANT_SCOPE),
    fixed_bytes(4, 16),
]);
shape!(AUDIT_PRINCIPAL: AuditPrincipalV1 [
    string(1, MAX_TEXT_ID_BYTES),
    fixed_bytes(3, 16),
]);

shape!(PLAN: ExecutablePlanRefV1 [
    string(1, MAX_TEXT_ID_BYTES),
    fixed_bytes(3, 32),
    fixed_bytes(5, 32),
]);
shape!(SCHEMA_BINDING: DurableKeySchemaBindingV1 [
    string(1, MAX_TEXT_ID_BYTES),
    fixed_bytes(3, 32),
]);
shape!(IDEMPOTENCY_DIGEST: IdempotencyKeyDigestV1 [fixed_bytes(3, 32)]);
shape!(IDEMPOTENCY_IDENTITY: IdempotencyIdentityV1 [
    fixed_bytes(1, 16),
    string(2, MAX_ENVIRONMENT_BYTES),
    message(3, &TENANT_SCOPE),
    string(4, MAX_TEXT_ID_BYTES),
    string(5, MAX_TEXT_ID_BYTES),
    message(7, &IDEMPOTENCY_DIGEST),
]);
shape!(PROVENANCE_CLAIMS: StoredAdmittedProvenanceClaimsV1 [
    string(1, 512),
    string(2, 128),
    string(3, 1_024),
    string(4, MAX_TEXT_ID_BYTES),
]);
shape!(ENTITY_TARGET: EntityTargetV1 [bytes(2, MAX_KEY_BYTES)]);
shape!(EXPECTED_ENTITY_STATE: ExpectedEntityStateV1 [message(1, &UNIT)]);
shape!(INDEX_EPOCH_POSITION: IndexEpochPositionV1 [message(1, &UNIT)]);
shape!(ENTITY_READ_DEPENDENCY: EntityReadDependencyV1 [
    message(1, &ENTITY_TARGET),
    message(2, &EXPECTED_ENTITY_STATE),
]);
shape!(INDEX_RANGE_DEPENDENCY: IndexRangeReadDependencyV1 [
    bytes(1, MAX_KEY_BYTES),
    message(2, &INDEX_EPOCH_POSITION),
]);
shape!(READ_DEPENDENCY: StoredReadDependencyV1 [
    message(1, &ENTITY_READ_DEPENDENCY),
    message(2, &INDEX_RANGE_DEPENDENCY),
]);
shape!(READ_DEPENDENCIES: StoredReadDependenciesV1 [repeated_message(1, MAX_COMMAND_ITEMS, &READ_DEPENDENCY,)]);
shape!(DECLARED_OUTCOME: DeclaredOutcomeV1 [bytes(2, MAX_DOCUMENT_BYTES)]);
shape!(ENTITY_RECORD: StoredEntityRecordV1 [
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
shape!(INDEX_GENERATION_V2: StoredIndexGenerationV2 [
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
shape!(DURABLE_EVENT: StoredDurableEventV1 [
    message(1, &EVENT_ID),
    bytes(3, MAX_DOCUMENT_BYTES),
    fixed_bytes(4, 32),
]);
shape!(EVENT_POLICY_ANCHOR: StoredEventPolicyAnchorV1 [
    message(1, &SCHEMA_BINDING),
    message(3, &ENTITY_TARGET),
    string(4, MAX_TEXT_ID_BYTES),
]);
shape!(DURABLE_EVENT_V2: StoredDurableEventV2 [
    message(1, &EVENT_ID),
    bytes(3, MAX_DOCUMENT_BYTES),
    fixed_bytes(4, 32),
    message(5, &EVENT_POLICY_ANCHOR),
]);
shape!(DURABLE_EVENT_VARIANT_V1: StoredDurableEventVariantV1 [
    message(1, &DURABLE_EVENT),
    message(2, &DURABLE_EVENT_V2),
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
shape!(COMMAND_CAUSATION: StoredCommandCausationV1 [
    message(1, &EVENT_ID),
    fixed_bytes(2, 16),
]);
shape!(PENDING_ADMISSION_V2 [
    message(1, &PENDING_ADMISSION),
    message(2, &COMMAND_CAUSATION),
]);
shape!(EXECUTION_FAILED_V2[message(1, &PENDING_ADMISSION_V2)]);
shape!(OUTCOME_V2 [
    message(1, &OUTCOME),
    message(2, &COMMAND_CAUSATION),
]);
shape!(PROVENANCE_V2 [
    message(1, &PROVENANCE),
    message(2, &COMMAND_CAUSATION),
]);
shape!(PENDING_ADMISSION_V3 [
    message(1, &PENDING_ADMISSION_V2),
    bytes(2, MAX_DOCUMENT_BYTES),
]);
shape!(EXECUTION_FAILED_V3[message(1, &PENDING_ADMISSION_V3)]);
shape!(OUTCOME_V3 [
    message(1, &OUTCOME_V2),
    bytes(2, MAX_DOCUMENT_BYTES),
]);
shape!(COMMAND_AUDIT_INVOCATION_V1: StoredCommandAuditInvocationV1 [
    fixed_bytes(1, 16),
    message(3, &AUDIT_PRINCIPAL),
    repeated_message(5, 16, &SERVICE_AUDIT_TARGET_V2),
    string(6, MAX_TEXT_ID_BYTES),
    message(8, &TIMESTAMP),
    message(10, &TIMESTAMP),
]);
shape!(COMMAND_CAPSULE_V1: StoredCommandCapsuleV1 [
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
    message(15, &COMMAND_CAUSATION),
    message(16, &READ_DEPENDENCIES),
    repeated_message(17, MAX_COMMAND_ITEMS, &COMMITTED_ENTITY_REFERENCE_V2),
    repeated_message(18, MAX_COMMAND_ITEMS, &EVENT_REFERENCE_V2),
    repeated_message(19, MAX_COMMAND_ITEMS, &EVENT_ID),
    message(20, &COMMAND_AUDIT_INVOCATION_V1),
]);
shape!(INDEX_GENERATION_TRANSITION_V1: StoredIndexGenerationTransitionV1 [message(1, &INDEX_GENERATION_V2)]);
shape!(COMMAND_CAPSULE_V2: StoredCommandCapsuleV2 [
    message(1, &COMMAND_CAPSULE_V1),
    repeated_message(2, MAX_COMMAND_ITEMS, &DURABLE_EVENT),
    repeated_message(3, MAX_COMMAND_ITEMS, &INDEX_GENERATION_TRANSITION_V1),
]);
shape!(COMMAND_DERIVED_INDEX_MANIFEST_ENTRY_V1: CommandDerivedIndexManifestEntryV1 [bytes(3, MAX_KEY_BYTES)]);
shape!(COMMAND_SEGMENT_MANIFEST_V1: CommandSegmentManifestV1 [
    repeated_message(
        1,
        MAX_COMMAND_SEGMENT_INDEX_ENTRIES,
        &COMMAND_DERIVED_INDEX_MANIFEST_ENTRY_V1,
    ),
    fixed_bytes(2, 32),
]);
shape!(COMMAND_SEGMENT_BODY_V1: StoredCommandSegmentBodyV1 [
    fixed_bytes(1, 16),
    fixed_bytes(3, 32),
    repeated_message(8, MAX_COMMAND_SEGMENT_COMMANDS, &COMMAND_CAPSULE_V2),
    message(9, &COMMAND_SEGMENT_MANIFEST_V1),
]);
shape!(COMMAND_SEGMENT_V1: StoredCommandSegmentV1 [
    message(1, &COMMAND_SEGMENT_BODY_V1),
    fixed_bytes(2, 32),
]);
shape!(COMMAND_DERIVED_INDEX_CHECKPOINT_V1 [
    fixed_bytes(1, 16),
    fixed_bytes(3, 32),
    fixed_bytes(5, 32),
    repeated_message(
        6,
        MAX_COMMAND_SEGMENT_INDEX_ENTRIES,
        &COMMAND_DERIVED_INDEX_MANIFEST_ENTRY_V1,
    ),
    fixed_bytes(7, 32),
]);
shape!(COMMAND_CAPSULE_V3: StoredCommandCapsuleV3 [
    message(1, &COMMAND_CAPSULE_V2),
    bytes(2, MAX_DOCUMENT_BYTES),
]);
shape!(COMMAND_SEGMENT_BODY_V2: StoredCommandSegmentBodyV2 [
    fixed_bytes(1, 16),
    fixed_bytes(3, 32),
    repeated_message(8, MAX_COMMAND_SEGMENT_COMMANDS, &COMMAND_CAPSULE_V3),
    message(9, &COMMAND_SEGMENT_MANIFEST_V1),
]);
shape!(COMMAND_SEGMENT_V2: StoredCommandSegmentV2 [
    message(1, &COMMAND_SEGMENT_BODY_V2),
    fixed_bytes(2, 32),
]);
shape!(ENTITY_CHAIN_STATE_V1: StoredEntityChainStateV1 [fixed_bytes(3, 32)]);
shape!(COMMITTED_ENTITY_TRANSITION_V1: StoredCommittedEntityTransitionV1 [
    message(3, &ENTITY_TARGET),
    message(4, &ENTITY_CHAIN_STATE_V1),
    fixed_bytes(6, 32),
    message(7, &ENTITY_CHAIN_STATE_V1),
    fixed_bytes(8, 32),
]);
shape!(ENTITY_CHAIN_HEAD_V1 [
    message(1, &ENTITY_TARGET),
    message(3, &ENTITY_CHAIN_STATE_V1),
    fixed_bytes(5, 32),
]);
shape!(CHANGELOG_V2_ROTATION_RECEIPT_V1 [
    fixed_bytes(1, 16),
    fixed_bytes(5, 32),
    fixed_bytes(6, 32),
    fixed_bytes(7, 32),
]);
shape!(COMMAND_CAPSULE_V4: StoredCommandCapsuleV4 [
    message(1, &COMMAND_CAPSULE_V3),
    repeated_message(2, MAX_COMMAND_ITEMS, &COMMITTED_ENTITY_TRANSITION_V1),
]);
shape!(COMMAND_SEGMENT_BODY_V3: StoredCommandSegmentBodyV3 [
    fixed_bytes(1, 16),
    fixed_bytes(3, 32),
    repeated_message(8, MAX_COMMAND_SEGMENT_COMMANDS, &COMMAND_CAPSULE_V4),
    message(9, &COMMAND_SEGMENT_MANIFEST_V1),
]);
shape!(COMMAND_SEGMENT_V3: StoredCommandSegmentV3 [
    message(1, &COMMAND_SEGMENT_BODY_V3),
    fixed_bytes(2, 32),
]);
shape!(COMMAND_CAPSULE_V5: StoredCommandCapsuleV5 [
    message(1, &COMMAND_CAPSULE_V1),
    repeated_message(2, MAX_COMMAND_ITEMS, &DURABLE_EVENT_VARIANT_V1),
    repeated_message(3, MAX_COMMAND_ITEMS, &INDEX_GENERATION_TRANSITION_V1),
    bytes(4, MAX_DOCUMENT_BYTES),
    repeated_message(5, MAX_COMMAND_ITEMS, &COMMITTED_ENTITY_TRANSITION_V1),
]);
shape!(COMMAND_SEGMENT_BODY_V4: StoredCommandSegmentBodyV4 [
    fixed_bytes(1, 16),
    fixed_bytes(3, 32),
    repeated_message(8, MAX_COMMAND_SEGMENT_COMMANDS, &COMMAND_CAPSULE_V5),
    message(9, &COMMAND_SEGMENT_MANIFEST_V1),
]);
shape!(COMMAND_SEGMENT_V4: StoredCommandSegmentV4 [
    message(1, &COMMAND_SEGMENT_BODY_V4),
    fixed_bytes(2, 32),
]);
shape!(COMMAND_CAPSULE_V6: StoredCommandCapsuleV6 [
    message(1, &COMMAND_CAPSULE_V1),
    repeated_message(2, MAX_COMMAND_ITEMS, &DURABLE_EVENT_VARIANT_V1),
    repeated_message(3, MAX_COMMAND_INDEX_TRANSITIONS, &INDEX_GENERATION_TRANSITION_V1),
    bytes(4, MAX_DOCUMENT_BYTES),
    repeated_message(5, MAX_COMMAND_ITEMS, &COMMITTED_ENTITY_TRANSITION_V1),
]);
shape!(COMMAND_SEGMENT_BODY_V5: StoredCommandSegmentBodyV5 [
    fixed_bytes(1, 16),
    fixed_bytes(3, 32),
    repeated_message(8, MAX_COMMAND_SEGMENT_COMMANDS, &COMMAND_CAPSULE_V6),
    message(9, &COMMAND_SEGMENT_MANIFEST_V1),
]);
shape!(COMMAND_SEGMENT_V5: StoredCommandSegmentV5 [
    message(1, &COMMAND_SEGMENT_BODY_V5),
    fixed_bytes(2, 32),
]);
shape!(COMMAND_CAPSULE_V7: StoredCommandCapsuleV7 [
    message(1, &COMMAND_CAPSULE_V1),
    repeated_message(2, MAX_COMMAND_ITEMS, &DURABLE_EVENT_VARIANT_V1),
    repeated_message(3, MAX_COMMAND_INDEX_TRANSITIONS, &INDEX_GENERATION_TRANSITION_V1),
    bytes(4, MAX_DOCUMENT_BYTES),
    repeated_message(5, MAX_COMMAND_ITEMS, &COMMITTED_ENTITY_TRANSITION_V1),
    bytes(6, MAX_COMMAND_PREFIX_BYTES),
]);
shape!(COMMAND_SEGMENT_BODY_V6: StoredCommandSegmentBodyV6 [
    fixed_bytes(1, 16),
    fixed_bytes(3, 32),
    repeated_message(8, MAX_COMMAND_SEGMENT_COMMANDS, &COMMAND_CAPSULE_V7),
    message(9, &COMMAND_SEGMENT_MANIFEST_V1),
]);
shape!(COMMAND_SEGMENT_V6: StoredCommandSegmentV6 [
    message(1, &COMMAND_SEGMENT_BODY_V6),
    fixed_bytes(2, 32),
]);
shape!(VALIDATED_PREFIX_CHECKPOINT_V2 [
    message(1, &ROOT_VALIDATED_PREFIX_CHECKPOINT),
    fixed_bytes(5, 32),
    fixed_bytes(6, 32),
]);
shape!(COMMITTED_MUTATION: CommittedEntityMutationV1 [
    message(1, &EXPECTED_ENTITY_STATE),
    message(2, &ENTITY_RECORD),
]);
shape!(EVENT_REFERENCE_V2: EventReferenceV2 [
    message(1, &EVENT_ID),
    fixed_bytes(2, 32),
]);
shape!(COMMITTED_ENTITY_REFERENCE_V2: CommittedEntityReferenceV2 [
    message(1, &ENTITY_TARGET),
    fixed_bytes(3, 32),
]);
shape!(COMMIT: StoredCommitRecordV1 [
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
shape!(COMMIT_V2: StoredCommitRecordV2 [
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
shape!(COMMIT_V3: StoredCommitRecordV3 [
    fixed_bytes(2, 16),
    message(3, &PLAN),
    fixed_bytes(4, 32),
    message(5, &ADMITTED_ACTOR),
    message(6, &TIMESTAMP),
    fixed_bytes(7, 32),
    repeated_fixed_bytes(8, MAX_CONFLICT_HASHES, 32),
    message(9, &READ_DEPENDENCIES),
    repeated_message(10, MAX_COMMAND_ITEMS, &COMMITTED_ENTITY_REFERENCE_V2),
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
shape!(CAPABILITY_MIGRATION_GRANT_EXTENSION [
    repeated_string(1, 8_192, MAX_TEXT_ID_BYTES),
]);
shape!(CAPABILITY_RECORD_V2 [
    message(1, &CAPABILITY_RECORD),
    message(2, &CAPABILITY_MIGRATION_GRANT_EXTENSION),
]);
shape!(CAPABILITY_INSTALLATION_GRANT_EXTENSION [
    repeated_string(1, 8_192, MAX_TEXT_ID_BYTES),
]);
shape!(CAPABILITY_RECORD_V3 [
    message(1, &CAPABILITY_RECORD),
    message(2, &CAPABILITY_MIGRATION_GRANT_EXTENSION),
    message(3, &CAPABILITY_INSTALLATION_GRANT_EXTENSION),
]);
shape!(CAPABILITY_ROW_POLICY_BINDING [
    string(1, MAX_TEXT_ID_BYTES),
    string(2, MAX_TEXT_ID_BYTES),
    packed_varints(4, 4),
]);
shape!(CAPABILITY_ROW_POLICY_GRANT_EXTENSION [
    fixed_bytes(1, 32),
    nonempty_bytes(2, 64 * 1024),
    repeated_message(3, 1_024, &CAPABILITY_ROW_POLICY_BINDING),
]);
shape!(CAPABILITY_RECORD_V4 [
    message(1, &CAPABILITY_RECORD),
    message(2, &CAPABILITY_MIGRATION_GRANT_EXTENSION),
    message(3, &CAPABILITY_INSTALLATION_GRANT_EXTENSION),
    message(4, &CAPABILITY_ROW_POLICY_GRANT_EXTENSION),
]);
shape!(CAPABILITY_APPLICATION_EXPORT_GRANT [
    string(1, MAX_TEXT_ID_BYTES),
]);
shape!(CAPABILITY_EXPORT_GRANT_EXTENSION [
    repeated_message(1, 256, &CAPABILITY_APPLICATION_EXPORT_GRANT),
]);
shape!(CAPABILITY_RECORD_V5 [
    message(1, &CAPABILITY_RECORD),
    message(2, &CAPABILITY_MIGRATION_GRANT_EXTENSION),
    message(3, &CAPABILITY_INSTALLATION_GRANT_EXTENSION),
    message(4, &CAPABILITY_ROW_POLICY_GRANT_EXTENSION),
    message(5, &CAPABILITY_EXPORT_GRANT_EXTENSION),
]);
shape!(CAPABILITY_SECRET_VISIBILITY [
    string(1, MAX_TEXT_ID_BYTES),
    packed_varints(3, 65_535),
]);
shape!(CAPABILITY_SECRET_GRANT_EXTENSION [
    repeated_message(1, 65_535, &CAPABILITY_SECRET_VISIBILITY),
]);
shape!(CAPABILITY_RECORD_V6 [
    message(1, &CAPABILITY_RECORD),
    message(2, &CAPABILITY_MIGRATION_GRANT_EXTENSION),
    message(3, &CAPABILITY_INSTALLATION_GRANT_EXTENSION),
    message(4, &CAPABILITY_ROW_POLICY_GRANT_EXTENSION),
    message(5, &CAPABILITY_EXPORT_GRANT_EXTENSION),
    message(6, &CAPABILITY_SECRET_GRANT_EXTENSION),
]);
shape!(CAPABILITY_APPLICATION_REIMPORT_GRANT [
    string(1, MAX_TEXT_ID_BYTES),
    fixed_bytes(2, 16),
    fixed_bytes(3, 32),
]);
shape!(CAPABILITY_RECORD_V7 [
    message(1, &CAPABILITY_RECORD),
    message(2, &CAPABILITY_MIGRATION_GRANT_EXTENSION),
    message(3, &CAPABILITY_INSTALLATION_GRANT_EXTENSION),
    message(4, &CAPABILITY_ROW_POLICY_GRANT_EXTENSION),
    message(5, &CAPABILITY_EXPORT_GRANT_EXTENSION),
    message(6, &CAPABILITY_SECRET_GRANT_EXTENSION),
    message(7, &CAPABILITY_APPLICATION_REIMPORT_GRANT),
]);
shape!(CAPABILITY_VECTOR_INSPECTION_TARGET [string(1, MAX_TEXT_ID_BYTES),]);
shape!(CAPABILITY_VECTOR_INSPECTION_GRANT [
    fixed_bytes(1, 32),
    repeated_message(2, 256, &CAPABILITY_VECTOR_INSPECTION_TARGET),
]);
shape!(CAPABILITY_RECORD_V8 [
    message(1, &CAPABILITY_RECORD),
    message(2, &CAPABILITY_MIGRATION_GRANT_EXTENSION),
    message(3, &CAPABILITY_INSTALLATION_GRANT_EXTENSION),
    message(4, &CAPABILITY_ROW_POLICY_GRANT_EXTENSION),
    message(5, &CAPABILITY_EXPORT_GRANT_EXTENSION),
    message(6, &CAPABILITY_SECRET_GRANT_EXTENSION),
    message(7, &CAPABILITY_APPLICATION_REIMPORT_GRANT),
    message(8, &CAPABILITY_VECTOR_INSPECTION_GRANT),
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

shape!(CONTRACT_VERSION_TARGET: ContractVersionAuditTargetV1 [string(1, MAX_TEXT_ID_BYTES)]);
shape!(ENTITY_TYPE_TARGET: EntityTypeAuditTargetV1 [string(1, MAX_TEXT_ID_BYTES)]);
shape!(COMMAND_TARGET: CommandAuditTargetV1 [string(1, MAX_TEXT_ID_BYTES)]);
shape!(PROJECTION_TARGET: ProjectionAuditTargetV1 [string(1, MAX_TEXT_ID_BYTES)]);
shape!(INDEX_TARGET: IndexAuditTargetV1 [string(1, MAX_TEXT_ID_BYTES)]);
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
shape!(EVENT_CONSUMER_AUDIT_TARGET: EventConsumerAuditTargetV2 [
    string(1, MAX_TEXT_ID_BYTES),
    fixed_bytes(2, 32),
    string(3, MAX_TEXT_ID_BYTES),
    fixed_bytes(4, 32),
]);
shape!(SERVICE_AUDIT_TARGET_V2: ServiceAuditTargetV2 [
    string(1, MAX_TEXT_ID_BYTES),
    message(2, &CONTRACT_VERSION_TARGET),
    message(3, &ENTITY_TYPE_TARGET),
    message(4, &COMMAND_TARGET),
    message(5, &PROJECTION_TARGET),
    message(6, &INDEX_TARGET),
    fixed_bytes(8, 16),
    fixed_bytes(9, 16),
    message(10, &EVENT_CONSUMER_AUDIT_TARGET),
]);
shape!(REPLICATION_FOLLOWER_AUDIT_TARGET [fixed_bytes(1, 16), fixed_bytes(4, 16)]);
shape!(SERVICE_AUDIT_TARGET_V3 [
    string(1, MAX_TEXT_ID_BYTES),
    message(2, &CONTRACT_VERSION_TARGET),
    message(3, &ENTITY_TYPE_TARGET),
    message(4, &COMMAND_TARGET),
    message(5, &PROJECTION_TARGET),
    message(6, &INDEX_TARGET),
    fixed_bytes(8, 16),
    fixed_bytes(9, 16),
    message(10, &EVENT_CONSUMER_AUDIT_TARGET),
    message(11, &REPLICATION_FOLLOWER_AUDIT_TARGET),
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
shape!(SERVICE_AUDIT_V2 [
    fixed_bytes(2, 16),
    message(3, &TIMESTAMP),
    message(6, &AUDIT_PRINCIPAL),
    repeated_message(8, 16, &SERVICE_AUDIT_TARGET_V2),
    string(9, MAX_TEXT_ID_BYTES),
    message(10, &SERVICE_AUDIT_LINK),
]);
shape!(SERVICE_AUDIT_V3 [
    fixed_bytes(2, 16),
    message(3, &TIMESTAMP),
    message(6, &AUDIT_PRINCIPAL),
    repeated_message(8, 16, &SERVICE_AUDIT_TARGET_V3),
    string(9, MAX_TEXT_ID_BYTES),
    message(10, &SERVICE_AUDIT_LINK),
]);

shape!(REACTIVE_MODULE [
    string(1, MAX_TEXT_ID_BYTES),
    fixed_bytes(3, 32),
    string(4, MAX_TEXT_ID_BYTES),
    fixed_bytes(6, 32),
    fixed_bytes(7, 32),
    repeated_fixed_bytes(8, 1_024, 32),
    bytes(9, 1024 * 1024),
    bytes(10, MAX_QUERY_MODULE_BYTES),
]);
shape!(REACTIVE_MODULE_ADMINISTRATION [
    fixed_bytes(2, 16),
    message(3, &TIMESTAMP),
    message(4, &AUDIT_PRINCIPAL),
    fixed_bytes(5, 32),
    string(6, MAX_TEXT_ID_BYTES),
]);
shape!(EVENT_CONSUMER_IDENTITY [
    fixed_bytes(1, 16),
    fixed_bytes(2, 32),
    string(3, MAX_TEXT_ID_BYTES),
    fixed_bytes(4, 32),
    string(5, MAX_TEXT_ID_BYTES),
]);
shape!(SPARSE_CONSUMER_RESOLUTION[message(1, &EVENT_ID)]);
shape!(EVENT_CONSUMER [
    message(1, &EVENT_CONSUMER_IDENTITY),
    fixed_bytes(2, 32),
    message(5, &EVENT_ID),
    repeated_message(6, 64, &SPARSE_CONSUMER_RESOLUTION),
]);
shape!(LEASED_CONSUMER_DELIVERY [
    fixed_bytes(2, 32),
    message(3, &TIMESTAMP),
]);
shape!(RETRY_CONSUMER_DELIVERY[message(2, &TIMESTAMP)]);
shape!(DEAD_LETTERED_CONSUMER_DELIVERY[message(2, &TIMESTAMP)]);
shape!(EVENT_CONSUMER_DELIVERY [
    fixed_bytes(1, 32),
    message(2, &EVENT_ID),
    message(4, &LEASED_CONSUMER_DELIVERY),
    message(5, &RETRY_CONSUMER_DELIVERY),
    message(6, &DEAD_LETTERED_CONSUMER_DELIVERY),
]);

shape!(OUTBOX_INTENT[message(1, &DURABLE_EVENT)]);
shape!(OUTBOX_INTENT_V2[message(1, &EVENT_REFERENCE_V2)]);
shape!(EVENT_ROUTE_V1 [
    message(1, &EVENT_ID),
    fixed_bytes(3, 32),
]);
shape!(CONTRACT_MIGRATION_ARTIFACTS_V1 [
    fixed_bytes(1, 32),
    fixed_bytes(2, 32),
    fixed_bytes(3, 32),
]);
shape!(CONTRACT_MIGRATION_OPERATION_ARTIFACTS_V1 [
    fixed_bytes(2, 32),
    fixed_bytes(4, 32),
]);
shape!(CONTRACT_MIGRATION_JOURNAL_V1 [
    fixed_bytes(1, 16),
    fixed_bytes(2, 16),
    fixed_bytes(3, 32),
    message(4, &CONTRACT_MIGRATION_ARTIFACTS_V1),
    message(6, &ENTITY_TARGET),
    packed_varints(11, 4_096),
    fixed_bytes(12, 32),
    fixed_bytes(13, 32),
]);
shape!(CONTRACT_MIGRATION_RECORD_V1 [
    fixed_bytes(1, 16),
    fixed_bytes(2, 16),
    fixed_bytes(3, 32),
    message(4, &CONTRACT_MIGRATION_ARTIFACTS_V1),
    message(5, &CONTRACT_MIGRATION_OPERATION_ARTIFACTS_V1),
    string(6, MAX_TEXT_ID_BYTES),
    fixed_bytes(7, 32),
    message(8, &AUDIT_PRINCIPAL),
    string(9, MAX_TEXT_ID_BYTES),
    fixed_bytes(15, 32),
]);
shape!(CONTRACT_WRITE_RETIREMENT_V1 [
    fixed_bytes(1, 32),
    fixed_bytes(2, 32),
    fixed_bytes(3, 32),
    fixed_bytes(4, 16),
]);
shape!(RETIRED_ENTITY_RECORD_V1 [
    fixed_bytes(1, 16),
    fixed_bytes(2, 32),
    message(3, &ENTITY_TARGET),
    nonempty_bytes(4, MAX_DOCUMENT_BYTES),
]);
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
shape!(ROOT_EMPTY []);
shape!(ROOT_DATABASE_ID[fixed_bytes(1, 16)]);
shape!(ROOT_OPTIONAL_UNIT_FIELD_TWO[message(2, &UNIT)]);
shape!(ROOT_VALIDATED_PREFIX_CHECKPOINT [
    fixed_bytes(1, 16),
    fixed_bytes(3, 32),
    fixed_bytes(14, 32),
    fixed_bytes(19, 32),
    fixed_bytes(20, 32),
]);
shape!(ROOT_RETENTION_WATERMARK [
    fixed_bytes(3, 32),
    fixed_bytes(4, 32),
]);
shape!(ROOT_HISTORY_TOMBSTONE [
    fixed_bytes(7, 32),
    fixed_bytes(8, 32),
    fixed_bytes(9, 32),
]);
shape!(ROOT_RETENTION_ADMINISTRATION [
    string(4, MAX_TEXT_ID_BYTES),
    message(5, &TIMESTAMP),
]);
shape!(ROOT_APPLICATION_INSTALLATION_CAMPAIGN [
    fixed_bytes(1, 16),
    string(2, MAX_TEXT_ID_BYTES),
    fixed_bytes(3, 32),
    nonempty_bytes(4, 8 * 1024 * 1024),
]);
shape!(ROOT_APPLICATION_EXPORT_OPERATION [
    fixed_bytes(1, 16),
    string(2, MAX_TEXT_ID_BYTES),
    nonempty_bytes(3, 256 * 1024),
]);
shape!(ROOT_APPLICATION_EXPORT_OPERATION_V2 [
    fixed_bytes(1, 16),
    string(2, MAX_TEXT_ID_BYTES),
    nonempty_bytes(3, 256 * 1024),
    nonempty_bytes(4, 256 * 1024),
    fixed_bytes(5, 163),
]);
shape!(ROOT_APPLICATION_EXPORT_PAGE_COMMITMENT [
    fixed_bytes(1, 72),
]);
shape!(VECTOR_EMBEDDING_WRITE [
    string(2, MAX_TEXT_ID_BYTES),
    string(3, MAX_TEXT_ID_BYTES),
]);
shape!(VECTOR_EVIDENCE [
    message(1, &ENTITY_TARGET),
    bytes(2, MAX_KEY_BYTES),
    message(7, &VECTOR_EMBEDDING_WRITE),
    message(8, &SCHEMA_BINDING),
    fixed_bytes(9, 16),
    message(10, &PLAN),
]);
shape!(VECTOR_MODEL_COUNT [
    string(1, MAX_TEXT_ID_BYTES),
    string(2, MAX_TEXT_ID_BYTES),
]);
shape!(VECTOR_OBSERVATION [
    string(1, MAX_TEXT_ID_BYTES),
    bytes(2, MAX_KEY_BYTES),
    repeated_message(7, 256, &VECTOR_MODEL_COUNT),
]);
shape!(VECTOR_EVIDENCE_INDEX [
    string(1, MAX_TEXT_ID_BYTES),
    bytes(2, MAX_KEY_BYTES),
    bytes(5, MAX_KEY_BYTES),
    message(8, &VECTOR_EMBEDDING_WRITE),
]);
shape!(VECTOR_HEALTH_FIELD []);
shape!(VECTOR_HEALTH_OBSERVATION [
    string(1, MAX_TEXT_ID_BYTES),
    repeated_message(2, 256, &VECTOR_HEALTH_FIELD),
]);
shape!(VECTOR_PROJECTION_FRONTIER []);
shape!(VECTOR_PROJECTION_CONTROL [
    string(1, MAX_TEXT_ID_BYTES),
    fixed_bytes(5, 32),
    message(7, &VECTOR_PROJECTION_FRONTIER),
    message(8, &VECTOR_PROJECTION_FRONTIER),
]);
shape!(CLEAN_CLOSE_LIFECYCLE [
    fixed_bytes(1, 16),
    fixed_bytes(3, 32),
    bytes(6, 32),
    fixed_bytes(7, 32),
]);
shape!(COLUMNAR_SCALAR_SOURCE [
    string(1, MAX_TEXT_ID_BYTES),
    fixed_bytes(2, 32),
]);
shape!(COLUMNAR_VECTOR_SOURCE[string(1, MAX_TEXT_ID_BYTES)]);
shape!(COLUMNAR_SOURCE [
    message(1, &COLUMNAR_SCALAR_SOURCE),
    message(2, &COLUMNAR_VECTOR_SOURCE),
]);
shape!(COLUMNAR_FRONTIER []);
shape!(COLUMNAR_FAILURE []);
shape!(COLUMNAR_GENERATION [
    message(3, &COLUMNAR_FRONTIER),
    fixed_bytes(6, 32),
    fixed_bytes(7, 32),
    fixed_bytes(8, 32),
    fixed_bytes(9, 32),
    message(10, &COLUMNAR_FRONTIER),
]);
shape!(COLUMNAR_CONTROL [
    message(1, &COLUMNAR_SOURCE),
    fixed_bytes(2, 32),
    fixed_bytes(3, 32),
    message(5, &COLUMNAR_GENERATION),
    message(6, &COLUMNAR_GENERATION),
    message(7, &COLUMNAR_GENERATION),
    message(9, &COLUMNAR_FAILURE),
]);

shape!(CHANGELOG_POSITION_V3[fixed_bytes(2, 32)]);
shape!(CHANGELOG_HISTORY_STATE_V3 [
    fixed_bytes(1, 16), fixed_bytes(4, 32),
    message(5, &CHANGELOG_POSITION_V3), message(6, &CHANGELOG_POSITION_V3),
    message(7, &CHANGELOG_POSITION_V3),
]);
shape!(REPLICATION_FOLLOWER_ATTACHED_V3 [
    fixed_bytes(1, 16), fixed_bytes(4, 32),
    message(5, &CHANGELOG_POSITION_V3), message(6, &CHANGELOG_POSITION_V3),
]);
shape!(REPLICATION_FOLLOWER_STATE_V3 [
    message(1, &UNIT), message(2, &REPLICATION_FOLLOWER_ATTACHED_V3),
]);

shape!(REPLICATION_SOURCE_HOLD_V1 [
    fixed_bytes(2, 16), fixed_bytes(3, 16), fixed_bytes(6, 32),
    message(7, &CHANGELOG_POSITION_V3),
]);

shape!(REPLICATION_SOURCE_HOLD_V2 [
    message(1, &REPLICATION_SOURCE_HOLD_V1),
    message(2, &CHANGELOG_POSITION_V3),
    message(6, &CHANGELOG_POSITION_V3),
]);

shape!(REPLICATION_ADMINISTRATION_EXPLICIT [fixed_bytes(1, 16), message(2, &AUDIT_PRINCIPAL), string(3, MAX_TEXT_ID_BYTES)]);
shape!(REPLICATION_ADMINISTRATION_EXPIRY []);
shape!(REPLICATION_ADMINISTRATION_V1 [
    message(2, &TIMESTAMP), message(4, &REPLICATION_FOLLOWER_AUDIT_TARGET),
    message(6, &CHANGELOG_POSITION_V3), message(7, &ROOT_EMPTY),
    message(8, &REPLICATION_SOURCE_HOLD_V1), message(9, &REPLICATION_SOURCE_HOLD_V2),
    message(10, &REPLICATION_SOURCE_HOLD_V2), message(11, &REPLICATION_ADMINISTRATION_EXPLICIT),
    message(12, &REPLICATION_ADMINISTRATION_EXPIRY),
]);
shape!(PRIMARY_FENCE_ADMINISTRATION_V1 [
    message(2, &TIMESTAMP), fixed_bytes(3, 16), fixed_bytes(4, 16),
    message(5, &AUDIT_PRINCIPAL), string(6, MAX_TEXT_ID_BYTES),
    message(7, &REPLICATION_FOLLOWER_AUDIT_TARGET), message(9, &CHANGELOG_POSITION_V3),
]);
shape!(PRIMARY_ADMISSION_ACTIVE_V1[fixed_bytes(1, 16)]);
shape!(REPLICATION_PRIMARY_ADMISSION_V1 [message(1, &PRIMARY_ADMISSION_ACTIVE_V1), message(2, &PRIMARY_FENCE_ADMINISTRATION_V1)]);
shape!(PROMOTION_ADMINISTRATION_V1 [
    message(2, &TIMESTAMP), fixed_bytes(3, 16), fixed_bytes(4, 16),
    message(5, &AUDIT_PRINCIPAL), string(6, MAX_TEXT_ID_BYTES), message(7, &TIMESTAMP),
    message(8, &PRIMARY_FENCE_ADMINISTRATION_V1), message(9, &CHANGELOG_POSITION_V3),
    message(10, &CHANGELOG_HISTORY_STATE_V3), nonempty_bytes(14, 16),
]);
shape!(RECORD_REGISTRY_COMPLETED_V3 [fixed_bytes(1, 16), fixed_bytes(3, 32)]);
shape!(RECORD_REGISTRY_V3 [fixed_bytes(1, 32), message(2, &ROOT_EMPTY), message(3, &RECORD_REGISTRY_COMPLETED_V3)]);

#[path = "durable_wire_projection_source.rs"]
mod projection_source;

#[path = "durable_wire_fencing_v2.rs"]
mod catalog_fencing;
#[path = "durable_wire_native_transition.rs"]
mod native_transition;

const ROOTS: [&Shape; 123] = [
    &ROOT_EMPTY,
    &ROOT_DATABASE_ID,
    &ROOT_OPTIONAL_UNIT_FIELD_TWO,
    &ROOT_OPTIONAL_UNIT_FIELD_TWO,
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
    &ROOT_EMPTY,
    // StoredServiceAuditRequestIndexV1: fixed 16-byte request id; sequence is wire-type 0.
    &ROOT_DATABASE_ID,
    &EVENT_ROUTE_V1,
    &COMMIT_V3,
    &CONTRACT_MIGRATION_JOURNAL_V1,
    &CONTRACT_MIGRATION_RECORD_V1,
    &CONTRACT_WRITE_RETIREMENT_V1,
    &RETIRED_ENTITY_RECORD_V1,
    &CAPABILITY_RECORD_V2,
    // StoredValidatedPrefixCheckpointV1: fixed digests/hashes plus scalar counters.
    &ROOT_VALIDATED_PREFIX_CHECKPOINT,
    // StoredRetentionWatermarkV1: self-hash and recorded chain-root registry
    // digest are fixed 32 bytes; sequences are wire-type 0.
    &ROOT_RETENTION_WATERMARK,
    // StoredRetentionHoldsV1: repeated holds with string fields — no fixed-length roots.
    &ROOT_EMPTY,
    // StoredHistoryTombstoneV1: content digest + hashes fixed 32; counts/sequences wire-type 0.
    &ROOT_HISTORY_TOMBSTONE,
    // StoredRetentionAdministrationV1: reason string + timestamp message;
    // sequence/action/projection-id are wire-type 0.
    &ROOT_RETENTION_ADMINISTRATION,
    &REACTIVE_MODULE,
    &REACTIVE_MODULE_ADMINISTRATION,
    &EVENT_CONSUMER,
    &EVENT_CONSUMER_DELIVERY,
    &SERVICE_AUDIT_V2,
    &PENDING_ADMISSION_V2,
    &EXECUTION_FAILED_V2,
    &OUTCOME_V2,
    &PROVENANCE_V2,
    &COMMAND_CAPSULE_V1,
    // StoredCommandLocatorV1 contains only one nonzero scalar sequence.
    &ROOT_EMPTY,
    // StoredCommandAuditLocatorV1 contains a sequence and closed enum scalar.
    &ROOT_EMPTY,
    &COMMAND_CAPSULE_V2,
    &COMMAND_SEGMENT_V1,
    &COMMAND_DERIVED_INDEX_CHECKPOINT_V1,
    &PENDING_ADMISSION_V3,
    &EXECUTION_FAILED_V3,
    &OUTCOME_V3,
    &COMMAND_CAPSULE_V3,
    &COMMAND_SEGMENT_V2,
    &ROOT_APPLICATION_INSTALLATION_CAMPAIGN,
    &ENTITY_CHAIN_HEAD_V1,
    &CHANGELOG_V2_ROTATION_RECEIPT_V1,
    &COMMAND_CAPSULE_V4,
    &COMMAND_SEGMENT_V3,
    &VALIDATED_PREFIX_CHECKPOINT_V2,
    &CAPABILITY_RECORD_V3,
    &CAPABILITY_RECORD_V4,
    &DURABLE_EVENT_V2,
    &CAPABILITY_RECORD_V5,
    &ROOT_APPLICATION_EXPORT_OPERATION,
    &COMMAND_CAPSULE_V5,
    &COMMAND_SEGMENT_V4,
    &CAPABILITY_RECORD_V6,
    &CAPABILITY_RECORD_V7,
    &CAPABILITY_RECORD_V8,
    &VECTOR_EVIDENCE,
    &VECTOR_OBSERVATION,
    &VECTOR_EVIDENCE_INDEX,
    &VECTOR_HEALTH_OBSERVATION,
    &VECTOR_PROJECTION_CONTROL,
    &COMMAND_CAPSULE_V6,
    &COMMAND_SEGMENT_V5,
    &CLEAN_CLOSE_LIFECYCLE,
    &COLUMNAR_CONTROL,
    &ROOT_OPTIONAL_UNIT_FIELD_TWO,
    &RECORD_REGISTRY_V2, // Catalog digest has the same fixed 32-byte wire shape, not the same role.
    &ROOT_EMPTY,         // Nonzero leadership is checked by the semantic codec.
    &CHANGELOG_HISTORY_STATE_V3,
    &REPLICATION_FOLLOWER_STATE_V3,
    &REPLICATION_SOURCE_HOLD_V1,
    &COMMAND_CAPSULE_V7,
    &COMMAND_SEGMENT_V6,
    &REPLICATION_SOURCE_HOLD_V2,
    &ROOT_APPLICATION_EXPORT_OPERATION_V2,
    &ROOT_APPLICATION_EXPORT_PAGE_COMMITMENT,
    &SERVICE_AUDIT_V3,
    &REPLICATION_ADMINISTRATION_V1,
    &RECORD_REGISTRY_V2, // V2 catalog: same bounded digest shape, distinct durable role.
    &REPLICATION_PRIMARY_ADMISSION_V1,
    &PRIMARY_FENCE_ADMINISTRATION_V1,
    &PROMOTION_ADMINISTRATION_V1,
    &RECORD_REGISTRY_V3,
    &RECORD_REGISTRY_V2, // V3 catalog: exact digest shape, distinct role/revision at index111.
    &projection_source::PROJECTION_SOURCE_ROW_V1,
    &projection_source::PROJECTION_SOURCE_OBJECT_V1,
    &projection_source::PROJECTION_SOURCE_ROOT_V1,
    &projection_source::PROJECTION_SOURCE_MAINTENANCE_V1,
    &projection_source::GENERATION_SOURCE_BASE_V1,
    &projection_source::PROJECTION_CHECKPOINT_ORDINAL_ALLOCATOR_V1,
    catalog_fencing::ADMISSION,
    catalog_fencing::FENCE,
    catalog_fencing::PROMOTION,
    native_transition::CATALOG_TRANSITION,
    native_transition::SOURCE_INHERITANCE,
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

// Destination-only shape. It does not enter ROOTS or the primary registry.
shape!(LOCAL_OUTBOX_ARCHIVE_V1 [fixed_bytes(1, 16), nonempty_bytes(3, 3 * 1024 * 1024)]);

pub(crate) fn local_outbox_archive(input: &[u8]) -> Result<(), DurablePreflightError> {
    let mut budget = Budget {
        visited_fields: 0,
        packed_items: 0,
    };
    preflight(input, &LOCAL_OUTBOX_ARCHIVE_V1, 0, &mut budget)
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
    if depth > MAX_PREFLIGHT_DEPTH || shape.rules.len() > MAX_SHAPE_RULES {
        return Err(DurablePreflightError::LimitExceeded);
    }
    let mut occurrences = [0_usize; MAX_SHAPE_RULES];
    let mut packed_items = [0_usize; MAX_SHAPE_RULES];
    let mut cursor = Cursor::new(input);
    while let Some(field) = cursor.next()? {
        budget.claim(1)?;
        let Some((rule_index, rule)) = shape.rule(field.number) else {
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

/// Allocation counts for construction admission, never semantic authority.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct LocatorAllocationShape {
    pub(crate) messages: u64,
    pub(crate) wire_inline_bytes: u64,
    pub(crate) semantic_events: u64,
    pub(crate) semantic_read_dependencies: u64,
    pub(crate) semantic_entity_references: u64,
    pub(crate) semantic_entity_mutations: u64,
    pub(crate) semantic_event_references: u64,
    pub(crate) semantic_event_ids: u64,
    pub(crate) semantic_index_transitions: u64,
    pub(crate) semantic_entity_transitions: u64,
    pub(crate) semantic_audit_targets: u64,
    pub(crate) semantic_conflict_hashes: u64,
    pub(crate) buffers: u64,
    pub(crate) packed_items: u64,
    pub(crate) capsules: u64,
    pub(crate) manifest_entries: u64,
    pub(crate) canonical_heap_bytes: u64,
    pub(crate) prefix_work_bytes: u64,
}

pub(crate) fn locator_allocation_shape(
    record_type: &str,
    input: &[u8],
    prefix_work: &mut impl FnMut(&[u8]) -> Result<u64, DurablePreflightError>,
) -> Result<Option<LocatorAllocationShape>, DurablePreflightError> {
    allocation_shape_for(record_type, input, prefix_work, false)
}

pub(crate) fn command_source_allocation_shape(
    record_type: &str,
    input: &[u8],
    prefix_work: &mut impl FnMut(&[u8]) -> Result<u64, DurablePreflightError>,
) -> Result<Option<LocatorAllocationShape>, DurablePreflightError> {
    allocation_shape_for(record_type, input, prefix_work, true)
}

fn allocation_shape_for(
    record_type: &str,
    input: &[u8],
    prefix_work: &mut impl FnMut(&[u8]) -> Result<u64, DurablePreflightError>,
    include_historical: bool,
) -> Result<Option<LocatorAllocationShape>, DurablePreflightError> {
    let shape = match record_type {
        "riffdb.storage.v1.StoredCommandSegmentV1" => &COMMAND_SEGMENT_V1,
        "riffdb.storage.v1.StoredCommandSegmentV2" => &COMMAND_SEGMENT_V2,
        "riffdb.storage.v1.StoredCommandSegmentV3" => &COMMAND_SEGMENT_V3,
        "riffdb.storage.v1.StoredCommandSegmentV4" => &COMMAND_SEGMENT_V4,
        "riffdb.storage.v1.StoredCommandSegmentV5" => &COMMAND_SEGMENT_V5,
        "riffdb.storage.v1.StoredCommandSegmentV6" => &COMMAND_SEGMENT_V6,
        "riffdb.storage.v1.StoredCommandCapsuleV1" if include_historical => &COMMAND_CAPSULE_V1,
        "riffdb.storage.v1.StoredCommandCapsuleV2" if include_historical => &COMMAND_CAPSULE_V2,
        "riffdb.storage.v1.StoredCommandCapsuleV3" if include_historical => &COMMAND_CAPSULE_V3,
        "riffdb.storage.v1.StoredCommandCapsuleV4" if include_historical => &COMMAND_CAPSULE_V4,
        "riffdb.storage.v1.StoredCommandCapsuleV5" if include_historical => &COMMAND_CAPSULE_V5,
        "riffdb.storage.v1.StoredCommandCapsuleV6" if include_historical => &COMMAND_CAPSULE_V6,
        "riffdb.storage.v1.StoredCommandCapsuleV7" if include_historical => &COMMAND_CAPSULE_V7,
        "riffdb.storage.v1.StoredCommitRecordV1" if include_historical => &COMMIT,
        "riffdb.storage.v1.StoredCommitRecordV2" if include_historical => &COMMIT_V2,
        "riffdb.storage.v1.StoredCommitRecordV3" if include_historical => &COMMIT_V3,
        "riffdb.storage.v1.StoredDurableEventV1" if include_historical => &DURABLE_EVENT,
        "riffdb.storage.v1.StoredDurableEventV2" if include_historical => &DURABLE_EVENT_V2,
        _ => return Ok(None),
    };
    let mut budget = Budget {
        visited_fields: 0,
        packed_items: 0,
    };
    preflight(input, shape, 0, &mut budget)?;
    let mut result = LocatorAllocationShape::default();
    inspect_locator_allocations(input, shape, 0, &mut result, prefix_work)?;
    Ok(Some(result))
}

fn inspect_locator_allocations(
    input: &[u8],
    shape: &Shape,
    depth: usize,
    result: &mut LocatorAllocationShape,
    prefix_work: &mut impl FnMut(&[u8]) -> Result<u64, DurablePreflightError>,
) -> Result<(), DurablePreflightError> {
    if depth > MAX_PREFLIGHT_DEPTH {
        return Err(DurablePreflightError::LimitExceeded);
    }
    let add = |target: &mut u64, bytes: u64| {
        *target = target
            .checked_add(bytes)
            .ok_or(DurablePreflightError::LimitExceeded)?;
        Ok::<(), DurablePreflightError>(())
    };
    let wire_bytes = shape
        .locator_wire_bytes
        .ok_or(DurablePreflightError::Malformed)?;
    add(
        &mut result.wire_inline_bytes,
        u64::try_from(wire_bytes).map_err(|_| DurablePreflightError::LimitExceeded)?,
    )?;
    add(&mut result.messages, 1)?;
    // Semantic fixed children live inside their containing capsule. Count the
    // repeated element kinds whose buffers exist outside that inline storage.
    if std::ptr::eq(shape, &READ_DEPENDENCY) {
        add(&mut result.semantic_read_dependencies, 1)?;
    }
    if std::ptr::eq(shape, &COMMITTED_MUTATION) {
        add(&mut result.semantic_entity_mutations, 1)?;
    }
    if std::ptr::eq(shape, &COMMITTED_ENTITY_REFERENCE_V2) {
        add(&mut result.semantic_entity_references, 1)?;
    }
    if std::ptr::eq(shape, &EVENT_REFERENCE_V2) {
        add(&mut result.semantic_event_references, 1)?;
    }
    if std::ptr::eq(shape, &EVENT_ID) {
        add(&mut result.semantic_event_ids, 1)?;
    }
    if std::ptr::eq(shape, &INDEX_GENERATION_TRANSITION_V1) {
        add(&mut result.semantic_index_transitions, 1)?;
    }
    if std::ptr::eq(shape, &COMMITTED_ENTITY_TRANSITION_V1) {
        add(&mut result.semantic_entity_transitions, 1)?;
    }
    if std::ptr::eq(shape, &SERVICE_AUDIT_TARGET_V2) {
        add(&mut result.semantic_audit_targets, 1)?;
    }
    if std::ptr::eq(shape, &DURABLE_EVENT) || std::ptr::eq(shape, &DURABLE_EVENT_V2) {
        add(&mut result.semantic_events, 1)?;
    }
    if std::ptr::eq(shape, &COMMAND_CAPSULE_V1)
        || std::ptr::eq(shape, &COMMIT)
        || std::ptr::eq(shape, &COMMIT_V2)
        || std::ptr::eq(shape, &COMMIT_V3)
    {
        add(&mut result.capsules, 1)?;
    }
    if std::ptr::eq(shape, &COMMAND_DERIVED_INDEX_MANIFEST_ENTRY_V1) {
        add(&mut result.manifest_entries, 1)?;
    }
    let mut cursor = Cursor::new(input);
    while let Some(field) = cursor.next()? {
        let Some((_, rule)) = shape.rule(field.number) else {
            continue;
        };
        match rule.kind {
            Kind::Message(nested) => {
                inspect_locator_allocations(field.bytes, nested, depth + 1, result, prefix_work)?
            }
            Kind::Opaque { .. } => {
                add(&mut result.buffers, 1)?;
                if (std::ptr::eq(shape, &COMMAND_CAPSULE_V1) && field.number == 9)
                    || ((std::ptr::eq(shape, &COMMIT)
                        || std::ptr::eq(shape, &COMMIT_V2)
                        || std::ptr::eq(shape, &COMMIT_V3))
                        && field.number == 8)
                {
                    add(&mut result.semantic_conflict_hashes, 1)?;
                }
                let canonical = (std::ptr::eq(shape, &ENTITY_RECORD) && field.number == 5)
                    || (std::ptr::eq(shape, &DECLARED_OUTCOME) && field.number == 2)
                    || ((std::ptr::eq(shape, &DURABLE_EVENT)
                        || std::ptr::eq(shape, &DURABLE_EVENT_V2))
                        && field.number == 3)
                    || (std::ptr::eq(shape, &COMMAND_CAPSULE_V3) && field.number == 2)
                    || ((std::ptr::eq(shape, &COMMAND_CAPSULE_V5)
                        || std::ptr::eq(shape, &COMMAND_CAPSULE_V6)
                        || std::ptr::eq(shape, &COMMAND_CAPSULE_V7))
                        && field.number == 4);
                if canonical {
                    let bound = riffdb_types::inspect_canonical_decode_capacity_v1(field.bytes)
                        .map_err(|_| DurablePreflightError::Malformed)?;
                    add(&mut result.canonical_heap_bytes, bound.owned_heap_bytes())?;
                }
                if std::ptr::eq(shape, &COMMAND_CAPSULE_V7) && field.number == 6 {
                    add(&mut result.prefix_work_bytes, prefix_work(field.bytes)?)?;
                }
            }
            Kind::PackedVarints { .. } => {
                let mut packed = Cursor::new(field.bytes);
                while !packed.input_is_empty() {
                    packed.read_varint()?;
                    add(&mut result.packed_items, 1)?;
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;
    use crate::storage::v1;

    // req: TXN-042, STO-032
    #[test]
    fn locator_wire_footprints_cover_all_readable_segment_shapes() {
        fn visit(shape: &'static Shape, seen: &mut Vec<&'static Shape>, depth: usize) {
            assert!(depth <= MAX_PREFLIGHT_DEPTH);
            if seen.iter().any(|known| std::ptr::eq(*known, shape)) {
                return;
            }
            assert!(shape.locator_wire_bytes.is_some());
            seen.push(shape);
            for rule in shape.rules {
                if let Kind::Message(child) = rule.kind {
                    visit(child, seen, depth + 1);
                }
            }
        }
        let mut seen = Vec::new();
        for root in [
            &COMMAND_SEGMENT_V1,
            &COMMAND_SEGMENT_V2,
            &COMMAND_SEGMENT_V3,
            &COMMAND_SEGMENT_V4,
            &COMMAND_SEGMENT_V5,
            &COMMAND_SEGMENT_V6,
        ] {
            visit(root, &mut seen, 0);
        }
        // Includes every closed message arm, not just variants in a fixture.
        assert_eq!(seen.len(), 59);
        assert_eq!(UNIT.locator_wire_bytes, Some(0));
    }

    // req: TXN-042, STO-032
    #[test]
    fn locator_wire_footprint_refuses_a_new_unbound_nested_shape() {
        static UNBOUND: Shape = Shape::new(&[]);
        static PARENT: Shape =
            Shape::new(&[message(1, &UNBOUND)]).with_locator_wire_type::<v1::TenantScopeV1>();
        let mut result = LocatorAllocationShape::default();
        assert_eq!(
            inspect_locator_allocations(&[0x0a, 0], &PARENT, 0, &mut result, &mut |_| Ok(0)),
            Err(DurablePreflightError::Malformed)
        );
    }

    // req: TXN-042, STO-032
    #[test]
    fn locator_wire_footprints_cover_every_audit_target_variant() {
        use v1::service_audit_target_v2::Target;
        let variants = [
            Target::ContractLineage("lineage".to_owned()),
            Target::ContractVersion(v1::ContractVersionAuditTargetV1 {
                contract_lineage: "lineage".to_owned(),
                contract_version: 1,
            }),
            Target::EntityType(v1::EntityTypeAuditTargetV1 {
                contract_lineage: "lineage".to_owned(),
                entity_type_id: 1,
            }),
            Target::Command(v1::CommandAuditTargetV1 {
                contract_lineage: "lineage".to_owned(),
                command_id: 1,
            }),
            Target::Projection(v1::ProjectionAuditTargetV1 {
                contract_lineage: "lineage".to_owned(),
                projection_id: 1,
            }),
            Target::Index(v1::IndexAuditTargetV1 {
                contract_lineage: "lineage".to_owned(),
                index_id: 1,
            }),
            Target::CommitSequence(1),
            Target::ProvenanceId(vec![1; 16]),
            Target::CapabilityId(vec![2; 16]),
            Target::EventConsumer(v1::EventConsumerAuditTargetV2 {
                contract_lineage: "lineage".to_owned(),
                reactive_module_hash: vec![3; 32],
                operation_name: "operation".to_owned(),
                consumer_identity_hash: vec![4; 32],
            }),
        ];
        for variant in variants {
            let message = v1::ServiceAuditTargetV2 {
                target: Some(variant),
            };
            let encoded = message.encode_to_vec();
            let decoded = v1::ServiceAuditTargetV2::decode(encoded.as_slice()).unwrap();
            assert_eq!(decoded, message);
            let mut result = LocatorAllocationShape::default();
            inspect_locator_allocations(
                &encoded,
                &SERVICE_AUDIT_TARGET_V2,
                0,
                &mut result,
                &mut |_| Ok(0),
            )
            .unwrap();
            assert!(result.wire_inline_bytes >= std::mem::size_of_val(&decoded) as u64);
            // The oneof's actual enum storage is already included in its DTO.
            // Message arms also account their actual nested DTO, unlike scalar arms.
            assert!(result.messages == 1 || result.messages == 2);
            assert_eq!(result.semantic_audit_targets, 1);
            assert!(
                result.wire_inline_bytes < std::mem::size_of::<v1::StoredCommandCapsuleV7>() as u64
            );
        }
    }

    // req: TXN-042, STO-032
    #[test]
    fn locator_wire_and_semantic_slots_cover_both_dependency_variants() {
        use v1::stored_read_dependency_v1::Dependency;
        let message = v1::StoredReadDependenciesV1 {
            dependencies: vec![
                v1::StoredReadDependencyV1 {
                    dependency: Some(Dependency::EntityObservation(v1::EntityReadDependencyV1 {
                        target: Some(v1::EntityTargetV1 {
                            entity_type_id: 1,
                            entity_key: vec![1; 32],
                        }),
                        expected: Some(v1::ExpectedEntityStateV1 {
                            state: Some(v1::expected_entity_state_v1::State::Absent(v1::UnitV1 {})),
                        }),
                    })),
                },
                v1::StoredReadDependencyV1 {
                    dependency: Some(Dependency::EntityObservation(v1::EntityReadDependencyV1 {
                        target: Some(v1::EntityTargetV1 {
                            entity_type_id: 1,
                            entity_key: vec![2; 32],
                        }),
                        expected: Some(v1::ExpectedEntityStateV1 {
                            state: Some(v1::expected_entity_state_v1::State::PresentEntityVersion(
                                1,
                            )),
                        }),
                    })),
                },
                v1::StoredReadDependencyV1 {
                    dependency: Some(Dependency::IndexRangeEpoch(
                        v1::IndexRangeReadDependencyV1 {
                            canonical_index_range_prefix: vec![3; 32],
                            expected: Some(v1::IndexEpochPositionV1 {
                                position: Some(v1::index_epoch_position_v1::Position::BeforeFirst(
                                    v1::UnitV1 {},
                                )),
                            }),
                        },
                    )),
                },
                v1::StoredReadDependencyV1 {
                    dependency: Some(Dependency::IndexRangeEpoch(
                        v1::IndexRangeReadDependencyV1 {
                            canonical_index_range_prefix: vec![4; 32],
                            expected: Some(v1::IndexEpochPositionV1 {
                                position: Some(v1::index_epoch_position_v1::Position::Epoch(1)),
                            }),
                        },
                    )),
                },
            ],
        };
        let encoded = message.encode_to_vec();
        let decoded = v1::StoredReadDependenciesV1::decode(encoded.as_slice()).unwrap();
        assert_eq!(decoded, message);
        let mut result = LocatorAllocationShape::default();
        inspect_locator_allocations(&encoded, &READ_DEPENDENCIES, 0, &mut result, &mut |_| Ok(0))
            .unwrap();
        assert_eq!(
            result.semantic_read_dependencies,
            decoded.dependencies.len() as u64
        );
        // Measure the independent generated decoder's actual vector capacity,
        // including the oneof enum's inline storage, rather than wire length.
        let observed = std::mem::size_of_val(&decoded)
            + decoded.dependencies.capacity() * std::mem::size_of::<v1::StoredReadDependencyV1>();
        assert!(result.wire_inline_bytes * 4 >= observed as u64);
    }

    fn assert_exact_rule_dispatch(shape: &Shape, depth: usize) {
        assert!(depth <= MAX_PREFLIGHT_DEPTH);
        for number in 0..=(MAX_SHAPE_FIELD_NUMBER as u32 + 1) {
            let expected = shape
                .rules
                .iter()
                .enumerate()
                .find(|(_, rule)| rule.number == number)
                .map(|(index, rule)| (index, rule.number));
            let actual = shape.rule(number).map(|(index, rule)| (index, rule.number));
            assert_eq!(actual, expected, "field {number} at depth {depth}");
        }
        assert!(shape.rule(u32::MAX).is_none());

        for rule in shape.rules {
            if let Kind::Message(nested) = rule.kind {
                assert_exact_rule_dispatch(nested, depth + 1);
            }
        }
    }

    #[test]
    fn every_durable_shape_has_exact_constant_time_rule_dispatch() {
        for shape in ROOTS {
            assert_exact_rule_dispatch(shape, 0);
        }
    }

    #[test]
    fn every_migration_v1_record_has_a_real_structural_preflight_shape() {
        let artifacts = v1::ContractMigrationArtifactIdentityV1 {
            parent_bundle_hash: vec![1; 32],
            candidate_bundle_hash: vec![2; 32],
            migration_bundle_hash: vec![3; 32],
        };
        let operation_artifacts = v1::ContractMigrationOperationArtifactsV1 {
            candidate_bundle_length: 1,
            candidate_bundle_sha256: vec![4; 32],
            migration_bundle_length: 1,
            migration_bundle_sha256: vec![5; 32],
        };
        let journal = v1::StoredContractMigrationJournalV1 {
            database_id: vec![6; 16],
            operation_id: vec![7; 16],
            semantic_input_hash: vec![8; 32],
            artifacts: Some(artifacts.clone()),
            step: 1,
            exclusive_cursor: None,
            checked_rows: 0,
            changed_rows: 0,
            batch_count: 1,
            frozen_application_frontier: None,
            required_projection_ids: Vec::new(),
            previous_journal_hash: None,
            journal_hash: vec![9; 32],
        };
        let record = v1::StoredContractMigrationRecordV1 {
            database_id: vec![6; 16],
            operation_id: vec![7; 16],
            semantic_input_hash: vec![8; 32],
            artifacts: Some(artifacts),
            operation_artifacts: Some(operation_artifacts),
            source_backup_name: "pre-migration-fixture".to_owned(),
            source_backup_manifest_checksum: vec![10; 32],
            principal: Some(v1::AuditPrincipalV1 {
                principal_id: "operator".to_owned(),
                actor_kind: 1,
                capability_id: vec![11; 16],
                capability_revision: 1,
            }),
            approval_id: None,
            predecessor_application_frontier: None,
            successor_application_frontier: None,
            checked_rows: 0,
            changed_rows: 0,
            batch_count: 1,
            terminal_validation_digest: vec![12; 32],
            administration_sequence: 1,
        };
        let retirement = v1::StoredContractWriteRetirementV1 {
            parent_bundle_hash: vec![1; 32],
            candidate_bundle_hash: vec![2; 32],
            migration_bundle_hash: vec![3; 32],
            operation_id: vec![7; 16],
            administration_sequence: 1,
        };
        let retired = v1::StoredRetiredEntityRecordV1 {
            operation_id: vec![7; 16],
            migration_bundle_hash: vec![3; 32],
            original_target: Some(v1::EntityTargetV1 {
                entity_type_id: 1,
                entity_key: vec![1],
            }),
            original_entity_envelope: vec![1],
        };

        for (index, bytes) in [
            (38, journal.encode_to_vec()),
            (39, record.encode_to_vec()),
            (40, retirement.encode_to_vec()),
            (41, retired.encode_to_vec()),
        ] {
            assert_eq!(payload(index, &bytes), Ok(()));
        }
    }
}
