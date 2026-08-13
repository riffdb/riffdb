//! Stable lineage ledger, plan hashes, and canonical contract bundle bytes.

use std::collections::{BTreeMap, BTreeSet};

use riffdb_types::{
    AggregateTypeId, CommandId, ContractBundleHash, ContractLineage, ContractPlanRootHash,
    ContractVersion, CurrencyCode, DecimalSpec, EntityTypeId, EnumTypeId, EnumVariantId,
    EventTypeId, FieldId, IndexId, InvariantId, MAX_COMMAND_CONFLICT_KEYS_V1, OutcomeId, PlanHash,
    ProjectionId, ProjectionPlanHash, SchemaHash, SourceHash, decode_canonical_value,
    encode_canonical_value, hash_contract_bundle, hash_contract_plan_root, hash_plan,
    hash_projection_plan, hash_schema,
};

use crate::codec::{Reader, Writer};
use crate::format_registry::{
    binary_operator as binary_tag, binding_mode as binding_tag,
    capability_requirement as capability_tag, compatibility_class as compatibility_tag,
    enum_variant_owner as variant_owner_tag, execution_class as execution_tag,
    expression as expression_tag, index_owner as index_owner_tag, instruction as instruction_tag,
    invariant_owner as invariant_owner_tag, key_purpose as key_purpose_tag,
    lineage_entry_state as lineage_state_tag, outcome_owner as outcome_owner_tag,
    projection_aggregation as aggregation_tag, projection_frontier as frontier_tag,
    record_owner as record_owner_tag, record_reference as record_tag, retry_policy as retry_tag,
    stable_id_namespace as namespace_tag, unary_operator as unary_tag,
    value_type as value_type_tag, workflow_lease_operation as lease_operation_tag,
};
use crate::row_policy::{
    decode_catalog as decode_row_policy_catalog, encode_catalog as encode_row_policy_catalog,
};
use crate::{
    AggregateKeyPlan, AggregateSchema, BinaryOperator, BindingId, BindingMode, BindingPlan,
    CapabilityRequirement, CollectionDuplicatePolicyV1, CollectionExpansionPlanV1,
    CommandInputSchema, CommandPlan, CompatibilityClass, CompatibilityCode, CompatibilityEntry,
    CompatibilityReport, ConflictDerivationPlan, EntitySchema, EnumSchema, EnumVariantSchema,
    EventConstruction, EventPolicyAnchorFieldV1, EventPolicyAnchorV1, EventSchema, ExecutionClass,
    ExprId, ExpressionArena, ExpressionKind, FieldExpression, FieldSchema, GeneratedSchemaArtifact,
    IndexFieldEncodingV1, IndexSchema, Instruction, InvariantPlan, IrValidationError,
    KeyComponentCodecV1, KeyComponentSchema, KeyPurpose, KeySchema, LocalityPlan,
    McpCommandNameEntryV2, McpCommandNameRegistryV2, ObjectConstruction, OutcomeConstruction,
    OutcomeSchema, ProjectionFrontierPolicy, ProjectionGroupComponentSchema, ProjectionGroupSchema,
    ProjectionMeasurePlan, ProjectionPlan, RecordSchema, RecordTypeRef, RetryPolicy,
    RowPolicyCatalogV1, RowPolicyOperationV1, SchemaIr, TextKeyProfileV1, UnaryOperator, ValueType,
    ValueTypeTag, WorkflowCatalog, WorkflowLeaseFields, WorkflowLeaseOperation,
    WorkflowLeaseSchema, WorkflowSchema, WorkflowTransitionSchema, checked_len,
    validate_source_name,
};

/// Canonical bundle format version emitted and executed by the POC.
pub const BUNDLE_FORMAT_VERSION_V1: u32 = 1;
/// Bundle framing for workflow and service-value executable IR.
pub const BUNDLE_FORMAT_VERSION_V2: u32 = 2;
/// Bundle framing for fenced workflow lease executable IR.
pub const BUNDLE_FORMAT_VERSION_V3: u32 = 3;
/// Bundle framing containing compiled principal-aware row policies.
pub const BUNDLE_FORMAT_VERSION_V4: u32 = 4;
/// Bundle framing containing compiler-bounded collection command plans.
pub const BUNDLE_FORMAT_VERSION_V5: u32 = 5;
/// Bundle framing containing distinct checked-delete restrict outcomes.
pub const BUNDLE_FORMAT_VERSION_V6: u32 = 6;
/// Bundle framing containing compiler-owned current-row event policy anchors.
pub const BUNDLE_FORMAT_VERSION_V7: u32 = 7;
/// Bundle framing containing secret-field classifications (ADR-0118).
pub const BUNDLE_FORMAT_VERSION_V8: u32 = 8;
/// Bundle framing containing compiler-owned workflow initialization.
pub const BUNDLE_FORMAT_VERSION_V9: u32 = 9;
/// Bundle framing containing the operator-only reimport command class.
pub const BUNDLE_FORMAT_VERSION_V10: u32 = 10;
/// Canonical grammar version represented by a bundle.
pub const GRAMMAR_VERSION_V1: u32 = 1;
/// Contract grammar containing compiled workflows and service-owned values.
pub const GRAMMAR_VERSION_V2: u32 = 2;
/// Contract grammar containing closed fenced lease command effects.
pub const GRAMMAR_VERSION_V3: u32 = 3;
/// Contract grammar containing principal facts and closed row policies.
pub const GRAMMAR_VERSION_V4: u32 = 4;
/// Contract grammar containing bounded collection commands and checked deletes.
pub const GRAMMAR_VERSION_V5: u32 = 5;
/// Contract grammar containing a declared indexed-restrict delete outcome.
pub const GRAMMAR_VERSION_V6: u32 = 6;
/// Contract grammar containing current-row event policy anchors.
pub const GRAMMAR_VERSION_V7: u32 = 7;
/// Contract grammar containing the contextual `secret` field classification.
pub const GRAMMAR_VERSION_V8: u32 = 8;
/// Contract grammar containing workflow initial states and self-transitions.
pub const GRAMMAR_VERSION_V9: u32 = 9;
/// Contract grammar containing closed compiler-owned reimport commands.
pub const GRAMMAR_VERSION_V10: u32 = 10;
/// Executable IR version represented by a bundle.
pub const EXECUTABLE_IR_VERSION_V1: u32 = 1;
/// Executable IR containing compiled workflow transitions and service values.
pub const EXECUTABLE_IR_VERSION_V2: u32 = 2;
/// Executable IR containing closed fenced lease operations.
pub const EXECUTABLE_IR_VERSION_V3: u32 = 3;
/// Executable IR containing the compiled row-policy catalog.
pub const EXECUTABLE_IR_VERSION_V4: u32 = 4;
/// Executable IR containing one finite collection expansion template.
pub const EXECUTABLE_IR_VERSION_V5: u32 = 5;
/// Executable IR containing the distinct indexed-restrict delete outcome.
pub const EXECUTABLE_IR_VERSION_V6: u32 = 6;
/// Executable IR containing current-row event policy anchors.
pub const EXECUTABLE_IR_VERSION_V7: u32 = 7;
/// Executable IR whose schema carries secret-field classifications.
pub const EXECUTABLE_IR_VERSION_V8: u32 = 8;
/// Executable IR containing workflow initial states and self-transitions.
pub const EXECUTABLE_IR_VERSION_V9: u32 = 9;
/// Executable IR containing a distinct command invocation class.
pub const EXECUTABLE_IR_VERSION_V10: u32 = 10;

const RELATIONSHIP_SCHEMA_EXTENSION: u32 = 0xffff_fffe;
const UNIQUE_KEY_SCHEMA_EXTENSION: u32 = 0xffff_fffd;
const DELETE_POLICY_SCHEMA_EXTENSION: u32 = 0xffff_fffc;
const VECTOR_FIELD_SPEC_SCHEMA_EXTENSION: u32 = 0xffff_fffb;
const INDEX_FIELD_ENCODING_EXTENSION: u32 = 0xffff_fffa;
// 0xffff_fff9 is the high word of the event-policy-anchor magic below;
// the secret extension takes the next clean value.
const SECRET_FIELD_SPEC_SCHEMA_EXTENSION: u32 = 0xffff_fff8;
// The second word cannot be a valid following source-name length. Keeping the
// extension magic eight bytes wide prevents a future stable event ID equal to
// the first word from being misread as a partition extension.
const EVENT_PARTITION_SCHEMA_EXTENSION: u64 = 0xffff_fffc_ffff_ffff;
// Like the partition extension, the second word cannot be a valid following
// source-name length. This keeps an optional per-event anchor self-delimiting.
const EVENT_POLICY_ANCHOR_SCHEMA_EXTENSION: u64 = 0xffff_fff9_ffff_fffe;
/// Immutable stable-ID lineage-ledger format version.
pub const LINEAGE_LEDGER_VERSION_V1: u32 = 1;
/// Stable-ID lineage-ledger format with permanent rename aliases.
pub const LINEAGE_LEDGER_VERSION_V2: u32 = 2;
/// Maximum canonical bundle bytes below the durable envelope limit.
pub const MAX_BUNDLE_BYTES: usize = 15 * 1024 * 1024;
/// Maximum stable lineage ledger entries including tombstones.
pub const MAX_LINEAGE_LEDGER_ENTRIES: usize = 262_144;
/// Maximum historical allocation-state namespaces in one lineage ledger.
pub const MAX_LINEAGE_ALLOCATION_STATES: usize = 262_144;

const BUNDLE_MAGIC: &[u8] = b"RIFFDB-BUNDLE\0";
const COMMAND_PLAN_MAGIC: &[u8] = b"RIFFDB-COMMAND-PLAN\0";
const PROJECTION_PLAN_MAGIC: &[u8] = b"RIFFDB-PROJECTION-PLAN\0";
const ROOT_PLAN_MAGIC: &[u8] = b"RIFFDB-CONTRACT-PLAN-ROOT\0";
const SCHEMA_IR_MAGIC: &[u8] = b"RIFFDB-SCHEMA-IR\0";

/// Stable semantic ID namespace tags.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum StableIdNamespaceTag {
    /// Entity type.
    Entity = crate::format_registry::stable_id_namespace::ENTITY,
    /// Event type.
    Event = crate::format_registry::stable_id_namespace::EVENT,
    /// Enum type.
    Enum = crate::format_registry::stable_id_namespace::ENUM,
    /// Aggregate type.
    Aggregate = crate::format_registry::stable_id_namespace::AGGREGATE,
    /// Command.
    Command = crate::format_registry::stable_id_namespace::COMMAND,
    /// Projection.
    Projection = crate::format_registry::stable_id_namespace::PROJECTION,
    /// Entity-local index.
    Index = crate::format_registry::stable_id_namespace::INDEX,
    /// Entity- or aggregate-owned invariant.
    Invariant = crate::format_registry::stable_id_namespace::INVARIANT,
    /// Record field.
    Field = crate::format_registry::stable_id_namespace::FIELD,
    /// Command outcome.
    Outcome = crate::format_registry::stable_id_namespace::OUTCOME,
    /// Enum variant.
    EnumVariant = crate::format_registry::stable_id_namespace::ENUM_VARIANT,
}

/// One exact stable-ID allocation namespace including its owner path.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StableIdNamespace {
    tag: StableIdNamespaceTag,
    owner_kind: u8,
    owner_ids: Vec<u32>,
}

impl StableIdNamespace {
    /// Creates a checked namespace/owner path.
    pub fn new(
        tag: StableIdNamespaceTag,
        owner_kind: u8,
        owner_ids: Vec<u32>,
    ) -> Result<Self, IrValidationError> {
        let valid = match tag {
            StableIdNamespaceTag::Entity
            | StableIdNamespaceTag::Event
            | StableIdNamespaceTag::Enum
            | StableIdNamespaceTag::Aggregate
            | StableIdNamespaceTag::Command
            | StableIdNamespaceTag::Projection => owner_kind == 0 && owner_ids.is_empty(),
            StableIdNamespaceTag::Index => {
                owner_kind == index_owner_tag::ENTITY && owner_ids.len() == 1
            }
            StableIdNamespaceTag::Invariant => {
                matches!(
                    owner_kind,
                    invariant_owner_tag::ENTITY | invariant_owner_tag::AGGREGATE
                ) && owner_ids.len() == 1
            }
            StableIdNamespaceTag::Field => {
                matches!(
                    owner_kind,
                    record_owner_tag::ENTITY
                        | record_owner_tag::EVENT
                        | record_owner_tag::COMMAND_INPUT
                        | record_owner_tag::COMMAND_OUTCOME
                        | record_owner_tag::PROJECTION_RESULT
                        | record_owner_tag::COMMAND_SERVICE_VALUE
                ) && matches!(owner_ids.len(), 1 | 2)
                    && (owner_kind == record_owner_tag::COMMAND_OUTCOME) == (owner_ids.len() == 2)
            }
            StableIdNamespaceTag::Outcome => {
                owner_kind == outcome_owner_tag::COMMAND && owner_ids.len() == 1
            }
            StableIdNamespaceTag::EnumVariant => {
                owner_kind == variant_owner_tag::ENUM && owner_ids.len() == 1
            }
        };
        if !valid || owner_ids.contains(&0) {
            return Err(IrValidationError::InvalidLineageLedger {
                reason: "invalid stable-ID namespace owner path",
            });
        }
        Ok(Self {
            tag,
            owner_kind,
            owner_ids,
        })
    }

    /// Namespace kind.
    #[must_use]
    pub const fn tag(&self) -> StableIdNamespaceTag {
        self.tag
    }
    /// Closed owner-kind tag.
    #[must_use]
    pub const fn owner_kind(&self) -> u8 {
        self.owner_kind
    }
    /// Stable owner components.
    #[must_use]
    pub fn owner_ids(&self) -> &[u32] {
        &self.owner_ids
    }

    fn identity_key(&self, name: &str) -> Result<Vec<u8>, IrValidationError> {
        let mut writer = Writer::new(1_024);
        writer.u8(self.tag as u8)?;
        writer.u8(self.owner_kind)?;
        writer.u8(u8::try_from(self.owner_ids.len()).map_err(|_| {
            IrValidationError::InvalidLineageLedger {
                reason: "too many stable-ID owners",
            }
        })?)?;
        for id in &self.owner_ids {
            writer.u32(*id)?;
        }
        let length = u16::try_from(name.len()).map_err(|_| IrValidationError::InvalidName {
            kind: "stable identity",
        })?;
        writer.raw(&length.to_be_bytes())?;
        writer.raw(name.as_bytes())?;
        Ok(writer.finish())
    }

    pub(crate) fn allocation_namespace(&self) -> StableIdAllocationNamespace {
        match self.tag {
            StableIdNamespaceTag::Entity
            | StableIdNamespaceTag::Event
            | StableIdNamespaceTag::Enum
            | StableIdNamespaceTag::Aggregate
            | StableIdNamespaceTag::Command
            | StableIdNamespaceTag::Projection
            | StableIdNamespaceTag::Index
            | StableIdNamespaceTag::Invariant => StableIdAllocationNamespace {
                tag: self.tag,
                owner_kind: 0,
                owner_ids: Vec::new(),
            },
            StableIdNamespaceTag::Field
            | StableIdNamespaceTag::Outcome
            | StableIdNamespaceTag::EnumVariant => StableIdAllocationNamespace {
                tag: self.tag,
                owner_kind: self.owner_kind,
                owner_ids: self.owner_ids.clone(),
            },
        }
    }
}

/// One numeric allocation state, distinct from a declaration identity path.
///
/// Index and invariant identities carry owner paths, but each uses one global
/// numeric sequence. Fields, outcomes, and enum variants have scoped sequences.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StableIdAllocationNamespace {
    tag: StableIdNamespaceTag,
    owner_kind: u8,
    owner_ids: Vec<u32>,
}

impl StableIdAllocationNamespace {
    /// Creates one of the eight required lineage-global allocation states.
    pub fn global(tag: StableIdNamespaceTag) -> Result<Self, IrValidationError> {
        if !matches!(
            tag,
            StableIdNamespaceTag::Entity
                | StableIdNamespaceTag::Event
                | StableIdNamespaceTag::Enum
                | StableIdNamespaceTag::Aggregate
                | StableIdNamespaceTag::Command
                | StableIdNamespaceTag::Projection
                | StableIdNamespaceTag::Index
                | StableIdNamespaceTag::Invariant
        ) {
            return Err(IrValidationError::InvalidLineageLedger {
                reason: "scoped stable-ID kind cannot use a global allocation state",
            });
        }
        Ok(Self {
            tag,
            owner_kind: 0,
            owner_ids: Vec::new(),
        })
    }

    /// Creates one required field, outcome, or enum-variant allocation state.
    pub fn scoped(
        tag: StableIdNamespaceTag,
        owner_kind: u8,
        owner_ids: Vec<u32>,
    ) -> Result<Self, IrValidationError> {
        let valid = match tag {
            StableIdNamespaceTag::Field => {
                matches!(
                    owner_kind,
                    record_owner_tag::ENTITY
                        | record_owner_tag::EVENT
                        | record_owner_tag::COMMAND_INPUT
                        | record_owner_tag::COMMAND_OUTCOME
                        | record_owner_tag::PROJECTION_RESULT
                        | record_owner_tag::COMMAND_SERVICE_VALUE
                ) && matches!(owner_ids.len(), 1 | 2)
                    && (owner_kind == record_owner_tag::COMMAND_OUTCOME) == (owner_ids.len() == 2)
            }
            StableIdNamespaceTag::Outcome => {
                owner_kind == outcome_owner_tag::COMMAND && owner_ids.len() == 1
            }
            StableIdNamespaceTag::EnumVariant => {
                owner_kind == variant_owner_tag::ENUM && owner_ids.len() == 1
            }
            _ => false,
        };
        if !valid || owner_ids.contains(&0) {
            return Err(IrValidationError::InvalidLineageLedger {
                reason: "invalid scoped allocation-state owner path",
            });
        }
        Ok(Self {
            tag,
            owner_kind,
            owner_ids,
        })
    }

    /// Stable ID kind allocated by this state.
    #[must_use]
    pub const fn tag(&self) -> StableIdNamespaceTag {
        self.tag
    }
    /// Closed allocation owner kind, or zero for a global state.
    #[must_use]
    pub const fn owner_kind(&self) -> u8 {
        self.owner_kind
    }
    /// Exact scoped owner IDs, empty for a global state.
    #[must_use]
    pub fn owner_ids(&self) -> &[u32] {
        &self.owner_ids
    }
}

/// Compiler-supplied canonical semantic identity before numeric allocation.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StableIdentity {
    namespace: StableIdNamespace,
    name: String,
}

/// One compiler-sealed semantic rename within an exact allocation namespace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StableIdentityRename {
    from: StableIdentity,
    to: StableIdentity,
}

impl StableIdentityRename {
    /// Creates a non-reflexive rename that cannot move an identity between namespaces.
    pub fn new(from: StableIdentity, to: StableIdentity) -> Result<Self, IrValidationError> {
        if from == to
            || from.namespace.allocation_namespace() != to.namespace.allocation_namespace()
        {
            return Err(IrValidationError::InvalidLineageLedger {
                reason: "stable identity rename changes allocation namespace",
            });
        }
        Ok(Self { from, to })
    }

    /// Predecessor identity key.
    #[must_use]
    pub const fn from(&self) -> &StableIdentity {
        &self.from
    }

    /// Successor identity key.
    #[must_use]
    pub const fn to(&self) -> &StableIdentity {
        &self.to
    }
}

impl StableIdentity {
    /// Creates one exact source identity path.
    pub fn new(
        namespace: StableIdNamespace,
        name: impl Into<String>,
    ) -> Result<Self, IrValidationError> {
        let name = name.into();
        validate_source_name(&name, "stable identity")?;
        Ok(Self { namespace, name })
    }
    /// Exact allocation namespace.
    #[must_use]
    pub const fn namespace(&self) -> &StableIdNamespace {
        &self.namespace
    }
    /// Exact identity name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Active or permanently tombstoned stable identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum LineageEntryState {
    /// Present in this bundle.
    Active = crate::format_registry::lineage_entry_state::ACTIVE,
    /// Removed and never reusable.
    Tombstone = crate::format_registry::lineage_entry_state::TOMBSTONE,
}

/// One assigned stable lineage entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LineageEntry {
    id: u32,
    identity: StableIdentity,
    state: LineageEntryState,
}

impl LineageEntry {
    /// One-based assigned numeric ID.
    #[must_use]
    pub const fn id(&self) -> u32 {
        self.id
    }
    /// Exact identity name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.identity.name
    }
    /// Full semantic identity, including its declaration owner path.
    #[must_use]
    pub const fn identity(&self) -> &StableIdentity {
        &self.identity
    }
    /// Active/tombstone state.
    #[must_use]
    pub const fn state(&self) -> LineageEntryState {
        self.state
    }
}

/// One exact allocation state's contiguous history.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LineageAllocation {
    namespace: StableIdAllocationNamespace,
    max_allocated: u32,
    entries: Vec<LineageEntry>,
}

/// One permanent historical name bound to its original numeric stable ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LineageAlias {
    identity: StableIdentity,
    id: u32,
}

impl LineageAlias {
    /// Historical semantic identity key.
    #[must_use]
    pub const fn identity(&self) -> &StableIdentity {
        &self.identity
    }

    /// Original numeric stable ID.
    #[must_use]
    pub const fn id(&self) -> u32 {
        self.id
    }
}

impl LineageAllocation {
    /// Exact allocation namespace.
    #[must_use]
    pub const fn namespace(&self) -> &StableIdAllocationNamespace {
        &self.namespace
    }
    /// Highest ever allocated ID, or zero for an empty namespace.
    #[must_use]
    pub const fn max_allocated(&self) -> u32 {
        self.max_allocated
    }
    /// Complete contiguous entries in numeric-ID order.
    #[must_use]
    pub fn entries(&self) -> &[LineageEntry] {
        &self.entries
    }
}

/// Complete immutable stable-ID lineage ledger v1.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LineageLedgerV1 {
    version: u32,
    allocations: Vec<LineageAllocation>,
    aliases: Vec<LineageAlias>,
}

impl LineageLedgerV1 {
    /// Allocates a genesis ledger deterministically from canonical identities.
    pub fn genesis(identities: Vec<StableIdentity>) -> Result<Self, IrValidationError> {
        let required = inferred_allocation_namespaces(&identities)?;
        Self::from_parent(None, identities, required, Vec::new())
    }

    /// Allocates genesis while retaining explicitly required empty scoped states.
    pub fn genesis_complete(
        identities: Vec<StableIdentity>,
        required: Vec<StableIdAllocationNamespace>,
    ) -> Result<Self, IrValidationError> {
        Self::from_parent(None, identities, required, Vec::new())
    }

    /// Allocates a successor, preserving surviving IDs and permanent tombstones.
    pub fn successor(
        parent: &Self,
        identities: Vec<StableIdentity>,
    ) -> Result<Self, IrValidationError> {
        let required = inferred_allocation_namespaces(&identities)?;
        Self::from_parent(Some(parent), identities, required, Vec::new())
    }

    /// Allocates a successor while retaining explicitly required empty states.
    pub fn successor_complete(
        parent: &Self,
        identities: Vec<StableIdentity>,
        required: Vec<StableIdAllocationNamespace>,
    ) -> Result<Self, IrValidationError> {
        Self::from_parent(Some(parent), identities, required, Vec::new())
    }

    /// Allocates a successor while applying exact compiler-sealed semantic renames.
    pub fn successor_with_renames(
        parent: &Self,
        identities: Vec<StableIdentity>,
        renames: Vec<StableIdentityRename>,
    ) -> Result<Self, IrValidationError> {
        let required = inferred_allocation_namespaces(&identities)?;
        Self::from_parent(Some(parent), identities, required, renames)
    }

    /// Allocates a complete successor and applies exact semantic renames.
    pub fn successor_complete_with_renames(
        parent: &Self,
        identities: Vec<StableIdentity>,
        required: Vec<StableIdAllocationNamespace>,
        renames: Vec<StableIdentityRename>,
    ) -> Result<Self, IrValidationError> {
        Self::from_parent(Some(parent), identities, required, renames)
    }

    fn from_parent(
        parent: Option<&Self>,
        identities: Vec<StableIdentity>,
        required: Vec<StableIdAllocationNamespace>,
        renames: Vec<StableIdentityRename>,
    ) -> Result<Self, IrValidationError> {
        checked_len(
            "stable identities",
            identities.len(),
            MAX_LINEAGE_LEDGER_ENTRIES,
        )?;
        checked_len(
            "required lineage allocation states",
            required.len(),
            MAX_LINEAGE_ALLOCATION_STATES,
        )?;
        if let Some(parent) = parent {
            checked_len(
                "parent lineage allocation states",
                parent.allocations.len(),
                MAX_LINEAGE_ALLOCATION_STATES,
            )?;
        }
        if parent.is_none() && !renames.is_empty() {
            return Err(IrValidationError::InvalidLineageLedger {
                reason: "genesis ledger cannot contain rename aliases",
            });
        }
        checked_len(
            "stable identity renames",
            renames.len(),
            MAX_LINEAGE_LEDGER_ENTRIES,
        )?;
        let mut desired: BTreeMap<StableIdAllocationNamespace, BTreeMap<Vec<u8>, StableIdentity>> =
            BTreeMap::new();
        for identity in identities {
            let key = identity.namespace.identity_key(&identity.name)?;
            let allocation = identity.namespace.allocation_namespace();
            if desired
                .entry(allocation)
                .or_default()
                .insert(key, identity)
                .is_some()
            {
                return Err(IrValidationError::InvalidLineageLedger {
                    reason: "duplicate stable identity",
                });
            }
        }
        let mut aliases = parent.map_or_else(Vec::new, |ledger| ledger.aliases.clone());
        let historical = aliases
            .iter()
            .map(|alias| alias.identity.clone())
            .collect::<BTreeSet<_>>();
        if desired
            .values()
            .flat_map(BTreeMap::values)
            .any(|identity| historical.contains(identity))
        {
            return Err(IrValidationError::InvalidLineageLedger {
                reason: "renamed stable identity cannot be reintroduced",
            });
        }
        let mut renames_by_allocation =
            BTreeMap::<StableIdAllocationNamespace, Vec<StableIdentityRename>>::new();
        let mut rename_sources = BTreeSet::new();
        let mut rename_targets = BTreeSet::new();
        for rename in renames {
            if !rename_sources.insert(rename.from.clone())
                || !rename_targets.insert(rename.to.clone())
                || historical.contains(&rename.from)
                || historical.contains(&rename.to)
            {
                return Err(IrValidationError::InvalidLineageLedger {
                    reason: "stable identity rename is duplicate or collides with history",
                });
            }
            renames_by_allocation
                .entry(rename.from.namespace.allocation_namespace())
                .or_default()
                .push(rename);
        }
        let mut namespaces = required.into_iter().collect::<BTreeSet<_>>();
        namespaces.extend(required_global_allocation_namespaces()?);
        namespaces.extend(desired.keys().cloned());
        if let Some(parent) = parent {
            namespaces.extend(
                parent
                    .allocations
                    .iter()
                    .filter(|value| !value.entries.is_empty())
                    .map(|value| value.namespace.clone()),
            );
        }
        checked_len(
            "lineage allocation states",
            namespaces.len(),
            MAX_LINEAGE_ALLOCATION_STATES,
        )?;
        let mut allocations = Vec::with_capacity(namespaces.len());
        let mut total = 0usize;
        for namespace in namespaces {
            let parent_allocation = parent.and_then(|ledger| {
                ledger
                    .allocations
                    .binary_search_by(|value| value.namespace.cmp(&namespace))
                    .ok()
                    .map(|index| &ledger.allocations[index])
            });
            let wanted = desired.remove(&namespace).unwrap_or_default();
            let mut by_name = parent_allocation
                .into_iter()
                .flat_map(|allocation| allocation.entries.iter())
                .map(|entry| (entry.identity.clone(), entry.clone()))
                .collect::<BTreeMap<_, _>>();
            for rename in renames_by_allocation.remove(&namespace).unwrap_or_default() {
                if !wanted.values().any(|identity| identity == &rename.to)
                    || by_name.contains_key(&rename.to)
                {
                    return Err(IrValidationError::InvalidLineageLedger {
                        reason: "stable identity rename target is absent or already allocated",
                    });
                }
                let Some(mut entry) = by_name.remove(&rename.from) else {
                    return Err(IrValidationError::InvalidLineageLedger {
                        reason: "stable identity rename source is absent",
                    });
                };
                if entry.state != LineageEntryState::Active {
                    return Err(IrValidationError::InvalidLineageLedger {
                        reason: "stable identity rename source is not active",
                    });
                }
                aliases.push(LineageAlias {
                    identity: rename.from,
                    id: entry.id,
                });
                entry.identity = rename.to.clone();
                by_name.insert(rename.to, entry);
            }
            for identity in wanted.values() {
                if by_name
                    .get(identity)
                    .is_some_and(|entry| entry.state == LineageEntryState::Tombstone)
                {
                    return Err(IrValidationError::InvalidLineageLedger {
                        reason: "tombstoned stable identity cannot be reintroduced",
                    });
                }
            }
            for entry in by_name.values_mut() {
                entry.state = if wanted.values().any(|identity| identity == &entry.identity) {
                    LineageEntryState::Active
                } else {
                    LineageEntryState::Tombstone
                };
            }
            let mut next = parent_allocation.map_or(1, |allocation| {
                allocation.max_allocated.checked_add(1).unwrap_or(0)
            });
            for (_, identity) in wanted {
                if by_name.contains_key(&identity) {
                    continue;
                }
                if next == 0 {
                    return Err(IrValidationError::InvalidLineageLedger {
                        reason: "stable-ID allocation exhausted",
                    });
                }
                by_name.insert(
                    identity.clone(),
                    LineageEntry {
                        id: next,
                        identity,
                        state: LineageEntryState::Active,
                    },
                );
                next = next.checked_add(1).unwrap_or(0);
            }
            let mut entries = by_name.into_values().collect::<Vec<_>>();
            entries.sort_unstable_by_key(|entry| entry.id);
            let max_allocated = entries.last().map_or(0, |entry| entry.id);
            if entries
                .iter()
                .enumerate()
                .any(|(index, entry)| entry.id as usize != index + 1)
            {
                return Err(IrValidationError::InvalidLineageLedger {
                    reason: "stable-ID ledger contains a numeric gap",
                });
            }
            total = total
                .checked_add(entries.len())
                .ok_or(IrValidationError::SizeOverflow {
                    kind: "lineage ledger",
                })?;
            allocations.push(LineageAllocation {
                namespace,
                max_allocated,
                entries,
            });
        }
        checked_len("lineage ledger entries", total, MAX_LINEAGE_LEDGER_ENTRIES)?;
        if !renames_by_allocation.is_empty() {
            return Err(IrValidationError::InvalidLineageLedger {
                reason: "stable identity rename uses an unknown allocation namespace",
            });
        }
        aliases.sort_unstable_by(|left, right| left.identity.cmp(&right.identity));
        if aliases
            .windows(2)
            .any(|pair| pair[0].identity >= pair[1].identity)
        {
            return Err(IrValidationError::InvalidLineageLedger {
                reason: "lineage aliases are duplicate or unordered",
            });
        }
        checked_len("lineage aliases", aliases.len(), MAX_LINEAGE_LEDGER_ENTRIES)?;
        Ok(Self {
            version: if parent.is_some_and(|ledger| ledger.version == LINEAGE_LEDGER_VERSION_V2)
                || !aliases.is_empty()
            {
                LINEAGE_LEDGER_VERSION_V2
            } else {
                LINEAGE_LEDGER_VERSION_V1
            },
            allocations,
            aliases,
        })
    }

    /// Ledger format version.
    #[must_use]
    pub const fn version(&self) -> u32 {
        self.version
    }
    /// Allocation states in namespace/owner-path order.
    #[must_use]
    pub fn allocations(&self) -> &[LineageAllocation] {
        &self.allocations
    }
    /// Permanent historical rename aliases in canonical identity order.
    #[must_use]
    pub fn aliases(&self) -> &[LineageAlias] {
        &self.aliases
    }
    /// Finds the assigned active ID for a compiler identity.
    #[must_use]
    pub fn active_id(&self, identity: &StableIdentity) -> Option<u32> {
        let allocation = self
            .allocations
            .binary_search_by(|value| {
                value
                    .namespace
                    .cmp(&identity.namespace.allocation_namespace())
            })
            .ok()
            .map(|index| &self.allocations[index])?;
        allocation
            .entries
            .iter()
            .find(|entry| entry.identity == *identity && entry.state == LineageEntryState::Active)
            .map(|entry| entry.id)
    }
}

fn required_global_allocation_namespaces()
-> Result<Vec<StableIdAllocationNamespace>, IrValidationError> {
    [
        StableIdNamespaceTag::Entity,
        StableIdNamespaceTag::Event,
        StableIdNamespaceTag::Enum,
        StableIdNamespaceTag::Aggregate,
        StableIdNamespaceTag::Command,
        StableIdNamespaceTag::Projection,
        StableIdNamespaceTag::Index,
        StableIdNamespaceTag::Invariant,
    ]
    .into_iter()
    .map(StableIdAllocationNamespace::global)
    .collect()
}

fn inferred_allocation_namespaces(
    identities: &[StableIdentity],
) -> Result<Vec<StableIdAllocationNamespace>, IrValidationError> {
    let mut namespaces = required_global_allocation_namespaces()?;
    namespaces.extend(
        identities
            .iter()
            .map(|identity| identity.namespace.allocation_namespace()),
    );
    namespaces.sort_unstable();
    namespaces.dedup();
    Ok(namespaces)
}

/// Exact optional predecessor identity for a successor bundle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParentBundleRef {
    contract_version: ContractVersion,
    bundle_hash: ContractBundleHash,
}

impl ParentBundleRef {
    /// Creates an exact predecessor reference.
    #[must_use]
    pub const fn new(contract_version: ContractVersion, bundle_hash: ContractBundleHash) -> Self {
        Self {
            contract_version,
            bundle_hash,
        }
    }
    /// Parent application version.
    #[must_use]
    pub const fn contract_version(self) -> ContractVersion {
        self.contract_version
    }
    /// Parent canonical bundle hash.
    #[must_use]
    pub const fn bundle_hash(self) -> ContractBundleHash {
        self.bundle_hash
    }
}

/// A fully validated immutable executable contract bundle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContractBundle {
    format_version: u32,
    grammar_version: u32,
    ir_version: u32,
    compiler_version: String,
    lineage: ContractLineage,
    contract_version: ContractVersion,
    parent: Option<ParentBundleRef>,
    source_hash: SourceHash,
    plan_root_hash: ContractPlanRootHash,
    ledger: LineageLedgerV1,
    schema: SchemaIr,
    workflows: WorkflowCatalog,
    row_policies: RowPolicyCatalogV1,
    commands: Vec<CommandPlan>,
    projections: Vec<ProjectionPlan>,
    schema_artifacts: Vec<GeneratedSchemaArtifact>,
    mcp_command_names: McpCommandNameRegistryV2,
    compatibility: CompatibilityReport,
    canonical_bytes: Vec<u8>,
    bundle_hash: ContractBundleHash,
}

impl ContractBundle {
    /// Creates and canonicalizes one bundle, recomputing every aggregate hash.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        compiler_version: impl Into<String>,
        lineage: ContractLineage,
        contract_version: ContractVersion,
        parent: Option<ParentBundleRef>,
        source_hash: SourceHash,
        ledger: LineageLedgerV1,
        schema: SchemaIr,
        commands: Vec<CommandPlan>,
        projections: Vec<ProjectionPlan>,
        schema_artifacts: Vec<GeneratedSchemaArtifact>,
        mcp_command_names: McpCommandNameRegistryV2,
        compatibility: CompatibilityReport,
    ) -> Result<Self, IrValidationError> {
        Self::new_with_workflows(
            compiler_version,
            lineage,
            contract_version,
            parent,
            source_hash,
            ledger,
            schema,
            Vec::new(),
            commands,
            projections,
            schema_artifacts,
            mcp_command_names,
            compatibility,
        )
    }

    /// Creates a bundle containing the complete checked workflow catalog.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_workflows(
        compiler_version: impl Into<String>,
        lineage: ContractLineage,
        contract_version: ContractVersion,
        parent: Option<ParentBundleRef>,
        source_hash: SourceHash,
        ledger: LineageLedgerV1,
        schema: SchemaIr,
        workflows: Vec<WorkflowSchema>,
        commands: Vec<CommandPlan>,
        projections: Vec<ProjectionPlan>,
        schema_artifacts: Vec<GeneratedSchemaArtifact>,
        mcp_command_names: McpCommandNameRegistryV2,
        compatibility: CompatibilityReport,
    ) -> Result<Self, IrValidationError> {
        Self::new_with_workflows_and_row_policies(
            compiler_version,
            lineage,
            contract_version,
            parent,
            source_hash,
            ledger,
            schema,
            workflows,
            RowPolicyCatalogV1::empty(),
            commands,
            projections,
            schema_artifacts,
            mcp_command_names,
            compatibility,
        )
    }

    /// Creates a bundle containing workflows and the complete row-policy catalog.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_workflows_and_row_policies(
        compiler_version: impl Into<String>,
        lineage: ContractLineage,
        contract_version: ContractVersion,
        parent: Option<ParentBundleRef>,
        source_hash: SourceHash,
        ledger: LineageLedgerV1,
        schema: SchemaIr,
        workflows: Vec<WorkflowSchema>,
        row_policies: RowPolicyCatalogV1,
        commands: Vec<CommandPlan>,
        projections: Vec<ProjectionPlan>,
        schema_artifacts: Vec<GeneratedSchemaArtifact>,
        mcp_command_names: McpCommandNameRegistryV2,
        compatibility: CompatibilityReport,
    ) -> Result<Self, IrValidationError> {
        let version = if commands.iter().any(CommandPlan::requires_ir_v10) {
            BUNDLE_FORMAT_VERSION_V10
        } else if workflows.iter().any(WorkflowSchema::requires_ir_v9) {
            BUNDLE_FORMAT_VERSION_V9
        } else if schema.requires_ir_v8() {
            BUNDLE_FORMAT_VERSION_V8
        } else if schema.requires_ir_v7() {
            BUNDLE_FORMAT_VERSION_V7
        } else if schema.requires_ir_v6() || commands.iter().any(CommandPlan::requires_ir_v6) {
            BUNDLE_FORMAT_VERSION_V6
        } else if schema.requires_ir_v5() || commands.iter().any(CommandPlan::requires_ir_v5) {
            BUNDLE_FORMAT_VERSION_V5
        } else if !row_policies.is_empty() {
            BUNDLE_FORMAT_VERSION_V4
        } else if commands.iter().any(CommandPlan::requires_ir_v3) {
            BUNDLE_FORMAT_VERSION_V3
        } else if !workflows.is_empty() || commands.iter().any(CommandPlan::requires_ir_v2) {
            BUNDLE_FORMAT_VERSION_V2
        } else {
            BUNDLE_FORMAT_VERSION_V1
        };
        Self::new_with_versions(
            version,
            version,
            version,
            compiler_version,
            lineage,
            contract_version,
            parent,
            source_hash,
            ledger,
            schema,
            workflows,
            row_policies,
            commands,
            projections,
            schema_artifacts,
            mcp_command_names,
            compatibility,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_with_versions(
        format_version: u32,
        grammar_version: u32,
        ir_version: u32,
        compiler_version: impl Into<String>,
        lineage: ContractLineage,
        contract_version: ContractVersion,
        parent: Option<ParentBundleRef>,
        source_hash: SourceHash,
        ledger: LineageLedgerV1,
        schema: SchemaIr,
        workflows: Vec<WorkflowSchema>,
        row_policies: RowPolicyCatalogV1,
        mut commands: Vec<CommandPlan>,
        mut projections: Vec<ProjectionPlan>,
        mut schema_artifacts: Vec<GeneratedSchemaArtifact>,
        mcp_command_names: McpCommandNameRegistryV2,
        compatibility: CompatibilityReport,
    ) -> Result<Self, IrValidationError> {
        if !matches!(
            (format_version, grammar_version, ir_version),
            (
                BUNDLE_FORMAT_VERSION_V1,
                GRAMMAR_VERSION_V1,
                EXECUTABLE_IR_VERSION_V1
            ) | (
                BUNDLE_FORMAT_VERSION_V2,
                GRAMMAR_VERSION_V2,
                EXECUTABLE_IR_VERSION_V2
            ) | (
                BUNDLE_FORMAT_VERSION_V3,
                GRAMMAR_VERSION_V3,
                EXECUTABLE_IR_VERSION_V3
            ) | (
                BUNDLE_FORMAT_VERSION_V4,
                GRAMMAR_VERSION_V4,
                EXECUTABLE_IR_VERSION_V4
            ) | (
                BUNDLE_FORMAT_VERSION_V5,
                GRAMMAR_VERSION_V5,
                EXECUTABLE_IR_VERSION_V5
            ) | (
                BUNDLE_FORMAT_VERSION_V6,
                GRAMMAR_VERSION_V6,
                EXECUTABLE_IR_VERSION_V6
            ) | (
                BUNDLE_FORMAT_VERSION_V7,
                GRAMMAR_VERSION_V7,
                EXECUTABLE_IR_VERSION_V7
            ) | (
                BUNDLE_FORMAT_VERSION_V8,
                GRAMMAR_VERSION_V8,
                EXECUTABLE_IR_VERSION_V8
            ) | (
                BUNDLE_FORMAT_VERSION_V9,
                GRAMMAR_VERSION_V9,
                EXECUTABLE_IR_VERSION_V9
            ) | (
                BUNDLE_FORMAT_VERSION_V10,
                GRAMMAR_VERSION_V10,
                EXECUTABLE_IR_VERSION_V10
            )
        ) || (ir_version < EXECUTABLE_IR_VERSION_V2
            && (!workflows.is_empty() || commands.iter().any(CommandPlan::requires_ir_v2)))
            || (ir_version < EXECUTABLE_IR_VERSION_V3
                && commands.iter().any(CommandPlan::requires_ir_v3))
            || (ir_version < EXECUTABLE_IR_VERSION_V4 && !row_policies.is_empty())
            || (ir_version < EXECUTABLE_IR_VERSION_V5
                && (schema.requires_ir_v5() || commands.iter().any(CommandPlan::requires_ir_v5)))
            || (ir_version < EXECUTABLE_IR_VERSION_V6
                && commands.iter().any(CommandPlan::requires_ir_v6))
            || (ir_version < EXECUTABLE_IR_VERSION_V7 && schema.requires_ir_v7())
            || (ir_version < EXECUTABLE_IR_VERSION_V8 && schema.requires_ir_v8())
            || (ir_version < EXECUTABLE_IR_VERSION_V9
                && workflows.iter().any(WorkflowSchema::requires_ir_v9))
            || (ir_version < EXECUTABLE_IR_VERSION_V10
                && commands.iter().any(CommandPlan::requires_ir_v10))
            || (ir_version >= EXECUTABLE_IR_VERSION_V6
                && commands
                    .iter()
                    .any(|command| !command.delete_restriction_failures_are_complete()))
        {
            return Err(IrValidationError::UnsupportedVersion {
                kind: "contract bundle version tuple",
                value: ir_version,
            });
        }
        let compiler_version = compiler_version.into();
        if compiler_version.is_empty()
            || compiler_version.len() > 64
            || !compiler_version.is_ascii()
        {
            return Err(IrValidationError::InvalidText {
                kind: "compiler version",
            });
        }
        validate_source_name(lineage.as_str(), "contract lineage")?;
        let workflows = WorkflowCatalog::new(workflows, &schema)?;
        validate_event_policy_anchors(&schema, &row_policies)?;
        if let Some(parent) = parent {
            if contract_version <= parent.contract_version {
                return Err(IrValidationError::InvalidReference {
                    kind: "parent contract version",
                });
            }
        } else if !compatibility.entries().is_empty() {
            return Err(IrValidationError::InvalidCompatibilityReport);
        }
        commands.sort_unstable_by_key(CommandPlan::command_id);
        projections.sort_unstable_by_key(ProjectionPlan::projection_id);
        schema_artifacts.sort_unstable_by_key(GeneratedSchemaArtifact::key);
        reject_duplicate_by(&commands, CommandPlan::command_id, "commands")?;
        reject_duplicate_by(&projections, ProjectionPlan::projection_id, "projections")?;
        reject_duplicate_by(
            &schema_artifacts,
            GeneratedSchemaArtifact::key,
            "schema artifacts",
        )?;
        if commands
            .iter()
            .any(|command| command.contract_version() != contract_version)
        {
            return Err(IrValidationError::InvalidReference {
                kind: "command contract version",
            });
        }
        if commands.iter().any(|command| {
            command.required_capability().lineage() != &lineage
                || command.required_capability().command_id() != command.command_id()
        }) {
            return Err(IrValidationError::InvalidReference {
                kind: "command capability requirement",
            });
        }
        validate_bundle_global_bounds(&schema, &commands, &projections)?;
        for command in &commands {
            let recomputed = compute_command_plan_hash(command, &schema)?;
            if recomputed != command.plan_hash() {
                return Err(IrValidationError::HashMismatch {
                    kind: "command plan",
                });
            }
        }
        for projection in &projections {
            let recomputed = compute_projection_plan_hash(projection, &schema)?;
            if recomputed != projection.plan_hash() {
                return Err(IrValidationError::HashMismatch {
                    kind: "projection plan",
                });
            }
        }
        validate_ledger(&ledger, &schema, &commands, &projections)?;
        validate_mcp_registry(&lineage, &commands, &mcp_command_names)?;
        validate_schema_artifacts(&schema, &commands, &projections, &schema_artifacts)?;
        let plan_root_hash = compute_plan_root_hash_versioned(
            &schema,
            &workflows,
            &row_policies,
            &commands,
            &projections,
            ir_version,
        )?;
        let mut bundle = Self {
            format_version,
            grammar_version,
            ir_version,
            compiler_version,
            lineage,
            contract_version,
            parent,
            source_hash,
            plan_root_hash,
            ledger,
            schema,
            workflows,
            row_policies,
            commands,
            projections,
            schema_artifacts,
            mcp_command_names,
            compatibility,
            canonical_bytes: Vec::new(),
            bundle_hash: ContractBundleHash::from_bytes([0; 32]),
        };
        bundle.canonical_bytes = encode_bundle(&bundle)?;
        bundle.bundle_hash = hash_contract_bundle(&bundle.canonical_bytes);
        Ok(bundle)
    }

    /// Decodes canonical bundle bytes and rechecks every semantic constructor,
    /// stored hash, generated artifact, registry, and canonical ordering choice.
    pub fn decode(bytes: &[u8]) -> Result<Self, IrValidationError> {
        decode_bundle(bytes)
    }

    /// Bundle format version.
    #[must_use]
    pub const fn format_version(&self) -> u32 {
        self.format_version
    }

    /// Complete compiler-owned row-policy catalog.
    #[must_use]
    pub const fn row_policies(&self) -> &RowPolicyCatalogV1 {
        &self.row_policies
    }
    /// Grammar version.
    #[must_use]
    pub const fn grammar_version(&self) -> u32 {
        self.grammar_version
    }
    /// Executable IR version.
    #[must_use]
    pub const fn ir_version(&self) -> u32 {
        self.ir_version
    }
    /// Compiler semantic version.
    #[must_use]
    pub fn compiler_version(&self) -> &str {
        &self.compiler_version
    }
    /// Contract lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }
    /// Application contract version.
    #[must_use]
    pub const fn contract_version(&self) -> ContractVersion {
        self.contract_version
    }
    /// Exact optional parent.
    #[must_use]
    pub const fn parent(&self) -> Option<ParentBundleRef> {
        self.parent
    }
    /// Exact source hash.
    #[must_use]
    pub const fn source_hash(&self) -> SourceHash {
        self.source_hash
    }
    /// Ordered semantic plan root hash.
    #[must_use]
    pub const fn plan_root_hash(&self) -> ContractPlanRootHash {
        self.plan_root_hash
    }
    /// Stable lineage ledger.
    #[must_use]
    pub const fn ledger(&self) -> &LineageLedgerV1 {
        &self.ledger
    }
    /// Structural schema.
    #[must_use]
    pub const fn schema(&self) -> &SchemaIr {
        &self.schema
    }
    /// Complete compiler-checked aggregate-local workflow catalog.
    #[must_use]
    pub const fn workflows(&self) -> &WorkflowCatalog {
        &self.workflows
    }
    /// Commands in stable-ID order.
    #[must_use]
    pub fn commands(&self) -> &[CommandPlan] {
        &self.commands
    }
    /// Projections in stable-ID order.
    #[must_use]
    pub fn projections(&self) -> &[ProjectionPlan] {
        &self.projections
    }
    /// Generated schema artifacts in closed-key order.
    #[must_use]
    pub fn schema_artifacts(&self) -> &[GeneratedSchemaArtifact] {
        &self.schema_artifacts
    }
    /// Checked compiler-owned MCP registry.
    #[must_use]
    pub const fn mcp_command_names(&self) -> &McpCommandNameRegistryV2 {
        &self.mcp_command_names
    }
    /// Reproducible compatibility report.
    #[must_use]
    pub const fn compatibility(&self) -> &CompatibilityReport {
        &self.compatibility
    }
    /// Canonical immutable bundle bytes.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }
    /// External typed hash of canonical bundle bytes.
    #[must_use]
    pub const fn bundle_hash(&self) -> ContractBundleHash {
        self.bundle_hash
    }
    /// Resolves one exact checked command.
    #[must_use]
    pub fn command(&self, id: CommandId) -> Option<&CommandPlan> {
        self.commands
            .binary_search_by_key(&id, CommandPlan::command_id)
            .ok()
            .map(|i| &self.commands[i])
    }
    /// Resolves one exact checked projection.
    #[must_use]
    pub fn projection(&self, id: ProjectionId) -> Option<&ProjectionPlan> {
        self.projections
            .binary_search_by_key(&id, ProjectionPlan::projection_id)
            .ok()
            .map(|i| &self.projections[i])
    }
    /// Exposes the only bundle-validated bound projection schema.
    #[must_use]
    pub fn bound_projection_group_schema(
        &self,
        id: ProjectionId,
    ) -> Option<crate::BoundProjectionGroupSchema> {
        self.projection(id).map(|projection| {
            projection
                .group_schema()
                .clone()
                .bind_checked(self.lineage.clone(), projection.plan_hash())
        })
    }
}

fn validate_event_policy_anchors(
    schema: &SchemaIr,
    policies: &RowPolicyCatalogV1,
) -> Result<(), IrValidationError> {
    for event in schema.events() {
        let Some(anchor) = event.policy_anchor() else {
            continue;
        };
        let policy = policies
            .policies()
            .iter()
            .find(|policy| policy.name() == anchor.read_policy())
            .ok_or(IrValidationError::InvalidReference {
                kind: "event policy anchor read policy",
            })?;
        if policy.entity() != anchor.source_entity()
            || !policy
                .rules()
                .iter()
                .any(|rule| rule.operation() == RowPolicyOperationV1::Read)
        {
            return Err(IrValidationError::InvalidReference {
                kind: "event policy anchor read policy",
            });
        }
    }
    Ok(())
}

/// Computes every allocation state required by one complete executable bundle.
///
/// Callers pass this list to `genesis_complete` or `successor_complete` so
/// empty dynamic record/outcome/variant namespaces remain explicit.
pub fn required_lineage_allocation_namespaces(
    schema: &SchemaIr,
    commands: &[CommandPlan],
    projections: &[ProjectionPlan],
) -> Result<Vec<StableIdAllocationNamespace>, IrValidationError> {
    let mut required = required_global_allocation_namespaces()?;
    for entity in schema.entities() {
        required.push(record_allocation_namespace(entity.record().owner())?);
    }
    for event in schema.events() {
        required.push(record_allocation_namespace(event.payload().owner())?);
    }
    for enumeration in schema.enums() {
        required.push(StableIdAllocationNamespace::scoped(
            StableIdNamespaceTag::EnumVariant,
            variant_owner_tag::ENUM,
            vec![enumeration.id().get()],
        )?);
    }
    for command in commands {
        required.push(record_allocation_namespace(
            command.input().record().owner(),
        )?);
        required.push(StableIdAllocationNamespace::scoped(
            StableIdNamespaceTag::Outcome,
            outcome_owner_tag::COMMAND,
            vec![command.command_id().get()],
        )?);
        for outcome in command.outcomes() {
            required.push(record_allocation_namespace(outcome.payload().owner())?);
        }
    }
    for projection in projections {
        required.push(record_allocation_namespace(
            projection.group_schema().measures().owner(),
        )?);
    }
    required.sort_unstable();
    required.dedup();
    Ok(required)
}

fn record_allocation_namespace(
    owner: &RecordTypeRef,
) -> Result<StableIdAllocationNamespace, IrValidationError> {
    let (owner_kind, owner_ids) = match owner {
        RecordTypeRef::Entity(id) => (record_owner_tag::ENTITY, vec![id.get()]),
        RecordTypeRef::Event(id) => (record_owner_tag::EVENT, vec![id.get()]),
        RecordTypeRef::CommandInput(id) => (record_owner_tag::COMMAND_INPUT, vec![id.get()]),
        RecordTypeRef::CommandOutcome {
            command_id,
            outcome_id,
        } => (
            record_owner_tag::COMMAND_OUTCOME,
            vec![command_id.get(), outcome_id.get()],
        ),
        RecordTypeRef::ProjectionResult(id) => {
            (record_owner_tag::PROJECTION_RESULT, vec![id.get()])
        }
    };
    StableIdAllocationNamespace::scoped(StableIdNamespaceTag::Field, owner_kind, owner_ids)
}

fn validate_ledger(
    ledger: &LineageLedgerV1,
    schema: &SchemaIr,
    commands: &[CommandPlan],
    projections: &[ProjectionPlan],
) -> Result<(), IrValidationError> {
    if !matches!(
        ledger.version,
        LINEAGE_LEDGER_VERSION_V1 | LINEAGE_LEDGER_VERSION_V2
    ) || ledger.version == LINEAGE_LEDGER_VERSION_V1 && !ledger.aliases.is_empty()
        || ledger
            .allocations
            .windows(2)
            .any(|pair| pair[0].namespace >= pair[1].namespace)
    {
        return Err(IrValidationError::InvalidLineageLedger {
            reason: "ledger version or allocation-state order is invalid",
        });
    }
    let required = required_lineage_allocation_namespaces(schema, commands, projections)?;
    if required.iter().any(|namespace| {
        ledger
            .allocations
            .binary_search_by(|allocation| allocation.namespace.cmp(namespace))
            .is_err()
    }) {
        return Err(IrValidationError::InvalidLineageLedger {
            reason: "ledger omits a required allocation state",
        });
    }
    if ledger.allocations.iter().any(|allocation| {
        allocation.entries.is_empty() && required.binary_search(&allocation.namespace).is_err()
    }) {
        return Err(IrValidationError::InvalidLineageLedger {
            reason: "ledger contains an extra empty allocation state",
        });
    }
    let mut expected = Vec::<(StableIdentity, u32)>::new();
    let mut push = |tag, owner_kind, owner_ids, name: &str, id| {
        let namespace = StableIdNamespace::new(tag, owner_kind, owner_ids)?;
        expected.push((StableIdentity::new(namespace, name)?, id));
        Ok::<_, IrValidationError>(())
    };
    for entity in schema.entities() {
        push(
            StableIdNamespaceTag::Entity,
            0,
            vec![],
            entity.name(),
            entity.id().get(),
        )?;
        for field in entity.record().fields() {
            push(
                StableIdNamespaceTag::Field,
                record_owner_tag::ENTITY,
                vec![entity.id().get()],
                field.name(),
                field.id().get(),
            )?;
        }
        for invariant in entity.invariants() {
            push(
                StableIdNamespaceTag::Invariant,
                invariant_owner_tag::ENTITY,
                vec![entity.id().get()],
                invariant.name(),
                invariant.id().get(),
            )?;
        }
        for index in entity.indexes() {
            push(
                StableIdNamespaceTag::Index,
                index_owner_tag::ENTITY,
                vec![entity.id().get()],
                index.name(),
                index.id().get(),
            )?;
        }
    }
    for event in schema.events() {
        push(
            StableIdNamespaceTag::Event,
            0,
            vec![],
            event.name(),
            event.id().get(),
        )?;
        for field in event.payload().fields() {
            push(
                StableIdNamespaceTag::Field,
                record_owner_tag::EVENT,
                vec![event.id().get()],
                field.name(),
                field.id().get(),
            )?;
        }
    }
    for enumeration in schema.enums() {
        push(
            StableIdNamespaceTag::Enum,
            0,
            vec![],
            enumeration.name(),
            enumeration.id().get(),
        )?;
        for variant in enumeration.variants() {
            push(
                StableIdNamespaceTag::EnumVariant,
                variant_owner_tag::ENUM,
                vec![enumeration.id().get()],
                variant.name(),
                variant.id().get(),
            )?;
        }
    }
    for aggregate in schema.aggregates() {
        push(
            StableIdNamespaceTag::Aggregate,
            0,
            vec![],
            aggregate.name(),
            aggregate.id().get(),
        )?;
        for invariant in aggregate.invariants() {
            push(
                StableIdNamespaceTag::Invariant,
                invariant_owner_tag::AGGREGATE,
                vec![aggregate.id().get()],
                invariant.name(),
                invariant.id().get(),
            )?;
        }
    }
    for command in commands {
        push(
            StableIdNamespaceTag::Command,
            0,
            vec![],
            command.name(),
            command.command_id().get(),
        )?;
        for field in command.input().record().fields() {
            push(
                StableIdNamespaceTag::Field,
                record_owner_tag::COMMAND_INPUT,
                vec![command.command_id().get()],
                field.name(),
                field.id().get(),
            )?;
        }
        for outcome in command.outcomes() {
            push(
                StableIdNamespaceTag::Outcome,
                outcome_owner_tag::COMMAND,
                vec![command.command_id().get()],
                outcome.name(),
                outcome.id().get(),
            )?;
            for field in outcome.payload().fields() {
                push(
                    StableIdNamespaceTag::Field,
                    record_owner_tag::COMMAND_OUTCOME,
                    vec![command.command_id().get(), outcome.id().get()],
                    field.name(),
                    field.id().get(),
                )?;
            }
        }
    }
    for projection in projections {
        push(
            StableIdNamespaceTag::Projection,
            0,
            vec![],
            projection.name(),
            projection.projection_id().get(),
        )?;
        for field in projection.group_schema().measures().fields() {
            push(
                StableIdNamespaceTag::Field,
                record_owner_tag::PROJECTION_RESULT,
                vec![projection.projection_id().get()],
                field.name(),
                field.id().get(),
            )?;
        }
    }
    let active_count = ledger
        .allocations
        .iter()
        .flat_map(|allocation| &allocation.entries)
        .filter(|entry| entry.state == LineageEntryState::Active)
        .count();
    checked_len(
        "active semantic declarations",
        active_count,
        crate::MAX_EXPRESSION_NODES,
    )?;
    if active_count != expected.len()
        || expected
            .iter()
            .any(|(identity, id)| ledger.active_id(identity) != Some(*id))
    {
        return Err(IrValidationError::InvalidLineageLedger {
            reason: "ledger active identities disagree with executable structures",
        });
    }
    let mut identities = BTreeSet::new();
    for allocation in &ledger.allocations {
        if allocation.max_allocated as usize != allocation.entries.len()
            || allocation.entries.iter().enumerate().any(|(index, entry)| {
                entry.id as usize != index + 1
                    || entry.identity.namespace.allocation_namespace() != allocation.namespace
                    || !identities.insert(entry.identity.clone())
            })
        {
            return Err(IrValidationError::InvalidLineageLedger {
                reason: "ledger allocation history is noncontiguous or mis-scoped",
            });
        }
    }
    if ledger
        .aliases
        .windows(2)
        .any(|pair| pair[0].identity >= pair[1].identity)
    {
        return Err(IrValidationError::InvalidLineageLedger {
            reason: "lineage aliases are duplicate or unordered",
        });
    }
    for alias in &ledger.aliases {
        if !identities.insert(alias.identity.clone()) {
            return Err(IrValidationError::InvalidLineageLedger {
                reason: "lineage alias collides with an allocated identity",
            });
        }
        let namespace = alias.identity.namespace.allocation_namespace();
        let Some(allocation) = ledger
            .allocations
            .iter()
            .find(|allocation| allocation.namespace == namespace)
        else {
            return Err(IrValidationError::InvalidLineageLedger {
                reason: "lineage alias allocation namespace is absent",
            });
        };
        let Some(entry) = allocation.entries.iter().find(|entry| entry.id == alias.id) else {
            return Err(IrValidationError::InvalidLineageLedger {
                reason: "lineage alias target is absent",
            });
        };
        if entry.identity == alias.identity {
            return Err(IrValidationError::InvalidLineageLedger {
                reason: "lineage alias duplicates its current identity",
            });
        }
    }
    Ok(())
}

pub(crate) fn validate_bundle_global_bounds(
    schema: &SchemaIr,
    commands: &[CommandPlan],
    projections: &[ProjectionPlan],
) -> Result<(), IrValidationError> {
    let mut expression_count = 0usize;
    let mut add_expressions = |count: usize| -> Result<(), IrValidationError> {
        expression_count =
            expression_count
                .checked_add(count)
                .ok_or(IrValidationError::SizeOverflow {
                    kind: "bundle expression nodes",
                })?;
        checked_len(
            "bundle expression nodes",
            expression_count,
            crate::MAX_EXPRESSION_NODES,
        )
    };
    for entity in schema.entities() {
        for invariant in entity.invariants() {
            add_expressions(invariant.expressions().len())?;
        }
    }
    for aggregate in schema.aggregates() {
        add_expressions(aggregate.keys().expressions().len())?;
        for invariant in aggregate.invariants() {
            add_expressions(invariant.expressions().len())?;
        }
    }
    for command in commands {
        add_expressions(command.expressions().len())?;
    }
    for projection in projections {
        add_expressions(projection.expressions().len())?;
    }

    let mut declaration_count = 0usize;
    let mut add_declarations = |count: usize| -> Result<(), IrValidationError> {
        declaration_count =
            declaration_count
                .checked_add(count)
                .ok_or(IrValidationError::SizeOverflow {
                    kind: "active semantic declarations",
                })?;
        checked_len(
            "active semantic declarations",
            declaration_count,
            crate::MAX_EXPRESSION_NODES,
        )
    };
    for entity in schema.entities() {
        add_declarations(1 + entity.record().fields().len())?;
        add_declarations(entity.invariants().len() + entity.indexes().len())?;
    }
    for event in schema.events() {
        add_declarations(1 + event.payload().fields().len())?;
    }
    for enumeration in schema.enums() {
        add_declarations(1 + enumeration.variants().len())?;
    }
    for aggregate in schema.aggregates() {
        add_declarations(1 + aggregate.invariants().len())?;
    }
    for command in commands {
        add_declarations(1 + command.input().record().fields().len())?;
        add_declarations(command.outcomes().len())?;
        for outcome in command.outcomes() {
            add_declarations(outcome.payload().fields().len())?;
        }
    }
    for projection in projections {
        add_declarations(1 + projection.group_schema().measures().fields().len())?;
    }
    Ok(())
}

pub(crate) fn compute_command_plan_hash(
    plan: &CommandPlan,
    schema: &SchemaIr,
) -> Result<PlanHash, IrValidationError> {
    let mut writer = Writer::new(MAX_BUNDLE_BYTES);
    writer.raw(COMMAND_PLAN_MAGIC)?;
    let ir_version = if plan.requires_ir_v10() {
        EXECUTABLE_IR_VERSION_V10
    } else if plan.requires_ir_v6() {
        EXECUTABLE_IR_VERSION_V6
    } else if plan.requires_ir_v5() {
        EXECUTABLE_IR_VERSION_V5
    } else if plan.requires_ir_v3() {
        EXECUTABLE_IR_VERSION_V3
    } else if plan.requires_ir_v2() {
        EXECUTABLE_IR_VERSION_V2
    } else {
        EXECUTABLE_IR_VERSION_V1
    };
    writer.u32(ir_version)?;
    writer.u32(plan.command_id().get())?;
    encode_command_semantics_versioned(&mut writer, plan, schema, false, ir_version)?;
    encode_enum_closure(
        &mut writer,
        &collect_command_enum_closure(plan, schema)?,
        schema,
    )?;
    Ok(hash_plan(&writer.finish()))
}

pub(crate) fn compute_projection_plan_hash(
    plan: &ProjectionPlan,
    schema: &SchemaIr,
) -> Result<ProjectionPlanHash, IrValidationError> {
    let mut writer = Writer::new(MAX_BUNDLE_BYTES);
    writer.raw(PROJECTION_PLAN_MAGIC)?;
    writer.u32(EXECUTABLE_IR_VERSION_V1)?;
    writer.u32(plan.projection_id().get())?;
    let source = schema
        .event(plan.source_event())
        .ok_or(IrValidationError::InvalidReference {
            kind: "projection source event",
        })?;
    encode_event_schema(&mut writer, source, false)?;
    encode_projection_semantics(&mut writer, plan)?;
    encode_enum_closure(
        &mut writer,
        &collect_projection_enum_closure(plan, source, schema)?,
        schema,
    )?;
    Ok(hash_projection_plan(&writer.finish()))
}

fn encode_enum_closure(
    writer: &mut Writer,
    enum_ids: &BTreeSet<EnumTypeId>,
    schema: &SchemaIr,
) -> Result<(), IrValidationError> {
    writer.u32(enum_ids.len() as u32)?;
    for id in enum_ids {
        let enumeration = schema
            .enumeration(*id)
            .ok_or(IrValidationError::InvalidReference {
                kind: "plan enum closure",
            })?;
        writer.u32(enumeration.id().get())?;
        writer.u32(enumeration.variants().len() as u32)?;
        for variant in enumeration.variants() {
            writer.u32(variant.id().get())?;
            writer.string(variant.name())?;
        }
    }
    Ok(())
}

fn collect_command_enum_closure(
    plan: &CommandPlan,
    schema: &SchemaIr,
) -> Result<BTreeSet<EnumTypeId>, IrValidationError> {
    let mut enum_ids = BTreeSet::new();
    let mut visited_records = BTreeSet::new();
    collect_record_enum_ids(
        plan.input().record(),
        schema,
        &mut enum_ids,
        &mut visited_records,
    )?;
    for outcome in plan.outcomes() {
        collect_record_enum_ids(
            outcome.payload(),
            schema,
            &mut enum_ids,
            &mut visited_records,
        )?;
    }
    collect_expression_enum_ids(
        plan.expressions(),
        schema,
        &mut enum_ids,
        &mut visited_records,
    )?;

    let entity_ids = plan
        .bindings()
        .iter()
        .map(BindingPlan::entity_type)
        .chain(
            plan.root_validation_reads()
                .iter()
                .map(crate::RootValidationReadPlan::entity_type),
        )
        .collect::<BTreeSet<_>>();
    for binding in plan.bindings() {
        collect_key_schema_enum_ids(
            binding.key_schema(),
            schema,
            &mut enum_ids,
            &mut visited_records,
        )?;
    }
    for read in plan.root_validation_reads() {
        collect_key_schema_enum_ids(
            read.key_schema(),
            schema,
            &mut enum_ids,
            &mut visited_records,
        )?;
    }
    for entity_id in &entity_ids {
        let entity = schema
            .entity(*entity_id)
            .ok_or(IrValidationError::InvalidReference {
                kind: "command entity enum closure",
            })?;
        collect_entity_enum_ids(entity, schema, &mut enum_ids, &mut visited_records)?;
    }

    collect_key_schema_enum_ids(
        plan.locality().partition_schema(),
        schema,
        &mut enum_ids,
        &mut visited_records,
    )?;
    for conflict in plan.locality().conflict_keys() {
        collect_key_schema_enum_ids(
            conflict.schema(),
            schema,
            &mut enum_ids,
            &mut visited_records,
        )?;
    }
    let aggregate = schema.aggregate(plan.locality().aggregate_id()).ok_or(
        IrValidationError::InvalidReference {
            kind: "command aggregate enum closure",
        },
    )?;
    collect_expression_enum_ids(
        aggregate.keys().expressions(),
        schema,
        &mut enum_ids,
        &mut visited_records,
    )?;
    collect_key_schema_enum_ids(
        aggregate.keys().partition_schema(),
        schema,
        &mut enum_ids,
        &mut visited_records,
    )?;
    collect_key_schema_enum_ids(
        aggregate.keys().conflict_schema(),
        schema,
        &mut enum_ids,
        &mut visited_records,
    )?;
    for invariant in aggregate.invariants() {
        collect_expression_enum_ids(
            invariant.expressions(),
            schema,
            &mut enum_ids,
            &mut visited_records,
        )?;
    }

    for event_id in plan
        .instructions()
        .iter()
        .filter_map(|instruction| match instruction {
            Instruction::EmitEvent(event) => Some(event.event_type()),
            _ => None,
        })
        .collect::<BTreeSet<_>>()
    {
        let event = schema
            .event(event_id)
            .ok_or(IrValidationError::InvalidReference {
                kind: "command event enum closure",
            })?;
        collect_record_enum_ids(event.payload(), schema, &mut enum_ids, &mut visited_records)?;
    }
    Ok(enum_ids)
}

fn collect_projection_enum_closure(
    plan: &ProjectionPlan,
    source: &EventSchema,
    schema: &SchemaIr,
) -> Result<BTreeSet<EnumTypeId>, IrValidationError> {
    let mut enum_ids = BTreeSet::new();
    let mut visited_records = BTreeSet::new();
    collect_record_enum_ids(
        source.payload(),
        schema,
        &mut enum_ids,
        &mut visited_records,
    )?;
    collect_expression_enum_ids(
        plan.expressions(),
        schema,
        &mut enum_ids,
        &mut visited_records,
    )?;
    for measure in plan.measures() {
        collect_value_type_enum_ids(
            measure.field().value_type(),
            schema,
            &mut enum_ids,
            &mut visited_records,
        )?;
    }
    for component in plan.group_schema().group_components() {
        collect_value_type_enum_ids(
            component.value_type(),
            schema,
            &mut enum_ids,
            &mut visited_records,
        )?;
    }
    collect_record_enum_ids(
        plan.group_schema().measures(),
        schema,
        &mut enum_ids,
        &mut visited_records,
    )?;
    Ok(enum_ids)
}

fn collect_entity_enum_ids(
    entity: &EntitySchema,
    schema: &SchemaIr,
    enum_ids: &mut BTreeSet<EnumTypeId>,
    visited_records: &mut BTreeSet<RecordTypeRef>,
) -> Result<(), IrValidationError> {
    collect_record_enum_ids(entity.record(), schema, enum_ids, visited_records)?;
    collect_key_schema_enum_ids(entity.primary_key(), schema, enum_ids, visited_records)?;
    for invariant in entity.invariants() {
        collect_expression_enum_ids(invariant.expressions(), schema, enum_ids, visited_records)?;
    }
    for index in entity.indexes() {
        collect_key_schema_enum_ids(index.key_schema(), schema, enum_ids, visited_records)?;
    }
    Ok(())
}

fn collect_expression_enum_ids(
    arena: &ExpressionArena,
    schema: &SchemaIr,
    enum_ids: &mut BTreeSet<EnumTypeId>,
    visited_records: &mut BTreeSet<RecordTypeRef>,
) -> Result<(), IrValidationError> {
    for node in arena.nodes() {
        collect_value_type_enum_ids(node.result_type(), schema, enum_ids, visited_records)?;
    }
    Ok(())
}

fn collect_key_schema_enum_ids(
    key: &KeySchema,
    schema: &SchemaIr,
    enum_ids: &mut BTreeSet<EnumTypeId>,
    visited_records: &mut BTreeSet<RecordTypeRef>,
) -> Result<(), IrValidationError> {
    for component in key.components() {
        collect_value_type_enum_ids(component.value_type(), schema, enum_ids, visited_records)?;
    }
    if let Some(entity_key) = key.entity_key_schema() {
        collect_key_schema_enum_ids(entity_key, schema, enum_ids, visited_records)?;
    }
    Ok(())
}

fn collect_record_enum_ids(
    record: &RecordSchema,
    schema: &SchemaIr,
    enum_ids: &mut BTreeSet<EnumTypeId>,
    visited_records: &mut BTreeSet<RecordTypeRef>,
) -> Result<(), IrValidationError> {
    if !visited_records.insert(record.owner().clone()) {
        return Ok(());
    }
    for field in record.fields() {
        collect_value_type_enum_ids(field.value_type(), schema, enum_ids, visited_records)?;
    }
    Ok(())
}

fn collect_value_type_enum_ids(
    value_type: &ValueType,
    schema: &SchemaIr,
    enum_ids: &mut BTreeSet<EnumTypeId>,
    visited_records: &mut BTreeSet<RecordTypeRef>,
) -> Result<(), IrValidationError> {
    if let Some(id) = value_type.enum_type_id() {
        enum_ids.insert(id);
    }
    if let Some(inner) = value_type.optional_inner() {
        collect_value_type_enum_ids(inner, schema, enum_ids, visited_records)?;
    }
    if let Some((element, _)) = value_type.list_parts() {
        collect_value_type_enum_ids(element, schema, enum_ids, visited_records)?;
    }
    if let Some(record) = value_type.record_ref() {
        let referenced = match record {
            RecordTypeRef::Entity(id) => schema.entity(*id).map(EntitySchema::record),
            RecordTypeRef::Event(id) => schema.event(*id).map(EventSchema::payload),
            _ => None,
        }
        .ok_or(IrValidationError::InvalidReference {
            kind: "plan enum record closure",
        })?;
        collect_record_enum_ids(referenced, schema, enum_ids, visited_records)?;
    }
    Ok(())
}

#[cfg(test)]
fn compute_plan_root_hash(
    schema: &SchemaIr,
    commands: &[CommandPlan],
    projections: &[ProjectionPlan],
) -> Result<ContractPlanRootHash, IrValidationError> {
    let ir_version = if commands.iter().any(CommandPlan::requires_ir_v3) {
        EXECUTABLE_IR_VERSION_V3
    } else if commands.iter().any(CommandPlan::requires_ir_v2) {
        EXECUTABLE_IR_VERSION_V2
    } else {
        EXECUTABLE_IR_VERSION_V1
    };
    let workflows = WorkflowCatalog::new(Vec::new(), schema)?;
    compute_plan_root_hash_versioned(
        schema,
        &workflows,
        &RowPolicyCatalogV1::empty(),
        commands,
        projections,
        ir_version,
    )
}

fn compute_plan_root_hash_versioned(
    schema: &SchemaIr,
    workflows: &WorkflowCatalog,
    row_policies: &RowPolicyCatalogV1,
    commands: &[CommandPlan],
    projections: &[ProjectionPlan],
    ir_version: u32,
) -> Result<ContractPlanRootHash, IrValidationError> {
    let schema_bytes = encode_structural_schema(schema)?;
    let mut schema_preimage = Writer::new(MAX_BUNDLE_BYTES);
    schema_preimage.raw(SCHEMA_IR_MAGIC)?;
    schema_preimage.u32(EXECUTABLE_IR_VERSION_V1)?;
    schema_preimage.raw(&schema_bytes)?;
    let structural_hash: SchemaHash = hash_schema(&schema_preimage.finish());

    let mut writer = Writer::new(MAX_BUNDLE_BYTES);
    writer.raw(ROOT_PLAN_MAGIC)?;
    writer.u32(ir_version)?;
    writer.raw(structural_hash.as_bytes())?;
    if ir_version >= EXECUTABLE_IR_VERSION_V2 {
        encode_workflows(&mut writer, workflows, ir_version)?;
    }
    if ir_version >= EXECUTABLE_IR_VERSION_V4 {
        writer.bytes(&row_policies.canonical_bytes()?)?;
    }
    writer.u32(commands.len() as u32)?;
    for command in commands {
        writer.u32(command.command_id().get())?;
        writer.raw(command.plan_hash().as_bytes())?;
    }
    writer.u32(projections.len() as u32)?;
    for projection in projections {
        writer.u32(projection.projection_id().get())?;
        writer.raw(projection.plan_hash().as_bytes())?;
    }
    Ok(hash_contract_plan_root(&writer.finish()))
}

pub(crate) fn validate_mcp_registry(
    lineage: &ContractLineage,
    commands: &[CommandPlan],
    registry: &McpCommandNameRegistryV2,
) -> Result<(), IrValidationError> {
    let application_commands = commands
        .iter()
        .filter(|command| !command.is_reimport())
        .collect::<Vec<_>>();
    if registry.lineage() != lineage || registry.entries().len() != application_commands.len() {
        return Err(IrValidationError::InvalidMcpName {
            reason: "MCP registry lineage or completeness mismatch",
        });
    }
    for (entry, command) in registry.entries().iter().zip(application_commands) {
        if entry.command_id() != command.command_id()
            || entry.source_command_name() != command.name()
        {
            return Err(IrValidationError::InvalidMcpName {
                reason: "MCP registry command binding mismatch",
            });
        }
    }
    Ok(())
}

fn validate_schema_artifacts(
    schema: &SchemaIr,
    commands: &[CommandPlan],
    projections: &[ProjectionPlan],
    artifacts: &[GeneratedSchemaArtifact],
) -> Result<(), IrValidationError> {
    if artifacts.len() != expected_schema_artifact_count(schema, commands, projections)? {
        return Err(IrValidationError::HashMismatch {
            kind: "generated schema artifact registry",
        });
    }
    let mut actual = artifacts.iter();
    visit_expected_schema_artifacts(schema, commands, projections, |expected| {
        if actual.next() != Some(&expected) {
            return Err(IrValidationError::HashMismatch {
                kind: "generated schema artifact registry",
            });
        }
        Ok(())
    })?;
    if actual.next().is_some() {
        return Err(IrValidationError::HashMismatch {
            kind: "generated schema artifact registry",
        });
    }
    Ok(())
}

fn expected_schema_artifact_count(
    schema: &SchemaIr,
    commands: &[CommandPlan],
    projections: &[ProjectionPlan],
) -> Result<usize, IrValidationError> {
    let command_artifacts =
        commands
            .len()
            .checked_mul(2)
            .ok_or(IrValidationError::SizeOverflow {
                kind: "generated schema artifacts",
            })?;
    let count = schema
        .entities()
        .len()
        .checked_add(schema.events().len())
        .and_then(|count| count.checked_add(command_artifacts))
        .and_then(|count| count.checked_add(projections.len()))
        .ok_or(IrValidationError::SizeOverflow {
            kind: "generated schema artifacts",
        })?;
    checked_len("generated schema artifacts", count, 20_480)?;
    Ok(count)
}

fn visit_expected_schema_artifacts(
    schema: &SchemaIr,
    commands: &[CommandPlan],
    projections: &[ProjectionPlan],
    mut visit: impl FnMut(GeneratedSchemaArtifact) -> Result<(), IrValidationError>,
) -> Result<(), IrValidationError> {
    for entity in schema.entities() {
        visit(GeneratedSchemaArtifact::entity(
            entity.id(),
            entity.record(),
            schema,
        )?)?;
    }
    for event in schema.events() {
        visit(GeneratedSchemaArtifact::event(
            event.id(),
            event.payload(),
            schema,
        )?)?;
    }
    for command in commands {
        visit(GeneratedSchemaArtifact::command_input(
            command.command_id(),
            command.input().record(),
            schema,
            command.idempotency_input(),
        )?)?;
    }
    for command in commands {
        visit(GeneratedSchemaArtifact::command_outcomes(
            command.command_id(),
            command.outcomes(),
            schema,
        )?)?;
    }
    for projection in projections {
        let types = projection
            .group_schema()
            .group_components()
            .iter()
            .map(|component| component.value_type().clone())
            .collect::<Vec<_>>();
        visit(GeneratedSchemaArtifact::projection_result(
            projection.projection_id(),
            &types,
            projection.group_schema().measures(),
            schema,
        )?)?;
    }
    Ok(())
}

fn encode_bundle(bundle: &ContractBundle) -> Result<Vec<u8>, IrValidationError> {
    let mut writer = Writer::new(MAX_BUNDLE_BYTES);
    writer.raw(BUNDLE_MAGIC)?;
    writer.u32(bundle.format_version)?;
    writer.u32(bundle.grammar_version)?;
    writer.u32(bundle.ir_version)?;
    writer.string(&bundle.compiler_version)?;
    writer.string(bundle.lineage.as_str())?;
    writer.u64(bundle.contract_version.get())?;
    writer.bool(bundle.parent.is_some())?;
    if let Some(parent) = bundle.parent {
        writer.u64(parent.contract_version.get())?;
        writer.raw(parent.bundle_hash.as_bytes())?;
    }
    writer.raw(bundle.source_hash.as_bytes())?;
    writer.raw(bundle.plan_root_hash.as_bytes())?;
    encode_ledger(&mut writer, &bundle.ledger)?;
    encode_schema(&mut writer, &bundle.schema)?;
    if bundle.ir_version >= EXECUTABLE_IR_VERSION_V2 {
        encode_workflows(&mut writer, &bundle.workflows, bundle.ir_version)?;
    }
    if bundle.ir_version >= EXECUTABLE_IR_VERSION_V4 {
        encode_row_policy_catalog(&mut writer, &bundle.row_policies)?;
    }
    writer.u32(bundle.commands.len() as u32)?;
    for command in &bundle.commands {
        encode_command_bundle_entry_versioned(
            &mut writer,
            command,
            &bundle.schema,
            bundle.ir_version,
        )?;
    }
    writer.u32(bundle.projections.len() as u32)?;
    for projection in &bundle.projections {
        encode_projection_bundle_entry(&mut writer, projection)?;
    }
    writer.u32(bundle.schema_artifacts.len() as u32)?;
    for artifact in &bundle.schema_artifacts {
        writer.raw(&artifact.key().to_bytes())?;
        writer.bytes(artifact.canonical_json().as_bytes())?;
        writer.raw(artifact.hash().as_bytes())?;
    }
    encode_mcp_registry(&mut writer, &bundle.mcp_command_names)?;
    encode_compatibility(&mut writer, &bundle.compatibility)?;
    writer.finish().pipe(Ok)
}

trait Pipe: Sized {
    fn pipe<T>(self, operation: impl FnOnce(Self) -> T) -> T {
        operation(self)
    }
}
impl<T> Pipe for T {}

fn encode_ledger(writer: &mut Writer, ledger: &LineageLedgerV1) -> Result<(), IrValidationError> {
    writer.u32(ledger.version)?;
    writer.u32(ledger.allocations.len() as u32)?;
    for allocation in &ledger.allocations {
        writer.u8(allocation.namespace.tag as u8)?;
        writer.u8(allocation.namespace.owner_kind)?;
        writer.u8(allocation.namespace.owner_ids.len() as u8)?;
        for id in &allocation.namespace.owner_ids {
            writer.u32(*id)?;
        }
        writer.u32(allocation.max_allocated)?;
        writer.u32(allocation.entries.len() as u32)?;
        for entry in &allocation.entries {
            writer.u32(entry.id)?;
            writer.u8(entry.identity.namespace.owner_kind)?;
            writer.u8(entry.identity.namespace.owner_ids.len() as u8)?;
            for id in &entry.identity.namespace.owner_ids {
                writer.u32(*id)?;
            }
            writer.string(&entry.identity.name)?;
            writer.u8(entry.state as u8)?;
        }
    }
    if ledger.version == LINEAGE_LEDGER_VERSION_V2 {
        writer.u32(ledger.aliases.len() as u32)?;
        for alias in &ledger.aliases {
            writer.u8(alias.identity.namespace.tag as u8)?;
            writer.u8(alias.identity.namespace.owner_kind)?;
            writer.u8(alias.identity.namespace.owner_ids.len() as u8)?;
            for id in &alias.identity.namespace.owner_ids {
                writer.u32(*id)?;
            }
            writer.string(&alias.identity.name)?;
            writer.u32(alias.id)?;
        }
    }
    Ok(())
}

fn encode_structural_schema(schema: &SchemaIr) -> Result<Vec<u8>, IrValidationError> {
    let mut writer = Writer::new(MAX_BUNDLE_BYTES);
    encode_schema(&mut writer, schema)?;
    Ok(writer.finish())
}

fn encode_workflows(
    writer: &mut Writer,
    catalog: &WorkflowCatalog,
    ir_version: u32,
) -> Result<(), IrValidationError> {
    writer.u32(catalog.workflows().len() as u32)?;
    for workflow in catalog.workflows() {
        writer.string(workflow.name())?;
        writer.u32(workflow.entity().get())?;
        writer.u32(workflow.state_field().get())?;
        writer.u32(workflow.state_enum().get())?;
        if ir_version >= EXECUTABLE_IR_VERSION_V9 {
            writer.bool(workflow.initial_state().is_some())?;
            if let Some(initial_state) = workflow.initial_state() {
                writer.u32(initial_state.get())?;
            }
        }
        writer.u32(workflow.transitions().len() as u32)?;
        for transition in workflow.transitions() {
            writer.string(transition.name())?;
            writer.u32(transition.source_states().len() as u32)?;
            for state in transition.source_states() {
                writer.u32(state.get())?;
            }
            writer.u32(transition.destination().get())?;
        }
        writer.bool(workflow.lease().is_some())?;
        if let Some(lease) = workflow.lease() {
            writer.string(lease.name())?;
            writer.u32(lease.owner_field().get())?;
            writer.u32(lease.expiry_field().get())?;
            writer.u32(lease.fencing_token_field().get())?;
            writer.bool(lease.attempt_field().is_some())?;
            if let Some(field) = lease.attempt_field() {
                writer.u32(field.get())?;
            }
            writer.u64(lease.minimum_duration_seconds())?;
            writer.u64(lease.maximum_duration_seconds())?;
        }
    }
    Ok(())
}

fn encode_schema(writer: &mut Writer, schema: &SchemaIr) -> Result<(), IrValidationError> {
    writer.u32(schema.entities().len() as u32)?;
    for entity in schema.entities() {
        encode_entity_schema(writer, entity, true)?;
    }
    writer.u32(schema.events().len() as u32)?;
    for event in schema.events() {
        encode_event_schema(writer, event, true)?;
    }
    writer.u32(schema.enums().len() as u32)?;
    for enumeration in schema.enums() {
        writer.u32(enumeration.id().get())?;
        writer.string(enumeration.name())?;
        writer.u32(enumeration.variants().len() as u32)?;
        for variant in enumeration.variants() {
            writer.u32(variant.id().get())?;
            writer.string(variant.name())?;
        }
    }
    writer.u32(schema.aggregates().len() as u32)?;
    for aggregate in schema.aggregates() {
        encode_aggregate_schema(writer, aggregate, true)?;
    }
    if !schema.relationships().is_empty() {
        writer.u32(RELATIONSHIP_SCHEMA_EXTENSION)?;
        writer.u32(schema.relationships().len() as u32)?;
        for relationship in schema.relationships() {
            writer.string(relationship.name())?;
            writer.u32(relationship.source_entity().get())?;
            writer.u32(relationship.source_fields().len() as u32)?;
            for field in relationship.source_fields() {
                writer.u32(field.get())?;
            }
            writer.u32(relationship.target_entity().get())?;
            writer.u32(relationship.target_fields().len() as u32)?;
            for field in relationship.target_fields() {
                writer.u32(field.get())?;
            }
        }
    }
    if !schema.unique_keys().is_empty() {
        writer.u32(UNIQUE_KEY_SCHEMA_EXTENSION)?;
        writer.u32(schema.unique_keys().len() as u32)?;
        for unique in schema.unique_keys() {
            writer.string(unique.name())?;
            writer.u32(unique.source_entity().get())?;
            writer.u32(unique.index_id().get())?;
            writer.u32(unique.fields().len() as u32)?;
            for field in unique.fields() {
                writer.u32(field.get())?;
            }
        }
    }
    if !schema.delete_policies().is_empty() {
        writer.u32(DELETE_POLICY_SCHEMA_EXTENSION)?;
        writer.u32(schema.delete_policies().len() as u32)?;
        for policy in schema.delete_policies() {
            writer.u32(policy.target_entity().get())?;
            match policy.mode() {
                crate::DeletePolicyModeV1::NoInbound => {
                    writer.u8(crate::format_registry::delete_policy_mode::NO_INBOUND)?;
                }
                crate::DeletePolicyModeV1::Restrict {
                    source_entity,
                    index_id,
                } => {
                    writer.u8(crate::format_registry::delete_policy_mode::RESTRICT)?;
                    writer.u32(source_entity.get())?;
                    writer.u32(index_id.get())?;
                }
            }
        }
    }
    if !schema.vector_field_specs().is_empty() {
        writer.u32(VECTOR_FIELD_SPEC_SCHEMA_EXTENSION)?;
        writer.u32(schema.vector_field_specs().len() as u32)?;
        for spec in schema.vector_field_specs() {
            writer.u32(spec.entity().get())?;
            writer.u32(spec.field().get())?;
            writer.u8(spec.metric().tag())?;
            writer.u32(spec.source_fields().len() as u32)?;
            for source in spec.source_fields() {
                writer.u32(source.get())?;
            }
            writer.u64(spec.staleness_slo_secs())?;
        }
    }
    // Conditional extension (ADR-0118): contracts without secret-classified
    // fields encode byte-identically to the prior schema, so their bundle
    // hash does not rotate.
    if !schema.secret_field_specs().is_empty() {
        writer.u32(SECRET_FIELD_SPEC_SCHEMA_EXTENSION)?;
        writer.u32(schema.secret_field_specs().len() as u32)?;
        for spec in schema.secret_field_specs() {
            writer.u32(spec.entity().get())?;
            writer.u32(spec.field().get())?;
        }
    }
    Ok(())
}

fn encode_entity_schema(
    writer: &mut Writer,
    entity: &crate::EntitySchema,
    include_display_names: bool,
) -> Result<(), IrValidationError> {
    writer.u32(entity.id().get())?;
    if include_display_names {
        writer.string(entity.name())?;
    }
    encode_record_schema(writer, entity.record())?;
    writer.u32(entity.primary_key_fields().len() as u32)?;
    for field in entity.primary_key_fields() {
        writer.u32(field.get())?;
    }
    encode_key_schema(writer, entity.primary_key())?;
    writer.u32(entity.invariants().len() as u32)?;
    for invariant in entity.invariants() {
        encode_invariant(writer, invariant, include_display_names)?;
    }
    writer.u32(entity.indexes().len() as u32)?;
    for index in entity.indexes() {
        encode_index_schema(writer, index, include_display_names)?;
    }
    Ok(())
}

fn encode_event_schema(
    writer: &mut Writer,
    event: &crate::EventSchema,
    include_display_names: bool,
) -> Result<(), IrValidationError> {
    writer.u32(event.id().get())?;
    if include_display_names {
        writer.string(event.name())?;
    }
    encode_record_schema(writer, event.payload())?;
    if let Some(partition) = event.partition() {
        writer.u64(EVENT_PARTITION_SCHEMA_EXTENSION)?;
        writer.u32(partition.fields().len() as u32)?;
        for field in partition.fields() {
            writer.u32(field.get())?;
        }
        encode_key_schema(writer, partition.key_schema())?;
    }
    if let Some(anchor) = event.policy_anchor() {
        writer.u64(EVENT_POLICY_ANCHOR_SCHEMA_EXTENSION)?;
        writer.u32(anchor.source_entity().get())?;
        writer.string(anchor.read_policy())?;
        writer.u32(anchor.key_fields().len() as u32)?;
        for mapping in anchor.key_fields() {
            writer.u32(mapping.source_field().get())?;
            writer.u32(mapping.payload_field().get())?;
        }
    }
    Ok(())
}

fn encode_aggregate_schema(
    writer: &mut Writer,
    aggregate: &AggregateSchema,
    include_display_names: bool,
) -> Result<(), IrValidationError> {
    writer.u32(aggregate.id().get())?;
    if include_display_names {
        writer.string(aggregate.name())?;
    }
    writer.u32(aggregate.root().get())?;
    writer.u32(aggregate.children().len() as u32)?;
    for child in aggregate.children() {
        writer.u32(child.get())?;
    }
    encode_aggregate_keys(writer, aggregate.keys())?;
    writer.u32(aggregate.invariants().len() as u32)?;
    for invariant in aggregate.invariants() {
        encode_invariant(writer, invariant, include_display_names)?;
    }
    Ok(())
}

fn encode_aggregate_keys(
    writer: &mut Writer,
    keys: &AggregateKeyPlan,
) -> Result<(), IrValidationError> {
    encode_expression_arena(writer, keys.expressions())?;
    writer.u32(keys.partition_expression().get())?;
    writer.u32(keys.conflict_expressions().len() as u32)?;
    for expression in keys.conflict_expressions() {
        writer.u32(expression.get())?;
    }
    encode_key_schema(writer, keys.partition_schema())?;
    encode_key_schema(writer, keys.conflict_schema())
}

fn encode_invariant(
    writer: &mut Writer,
    invariant: &InvariantPlan,
    include_display_names: bool,
) -> Result<(), IrValidationError> {
    writer.u32(invariant.id().get())?;
    if include_display_names {
        writer.string(invariant.name())?;
    }
    encode_expression_arena(writer, invariant.expressions())?;
    writer.u32(invariant.predicate().get())
}

fn encode_index_schema(
    writer: &mut Writer,
    index: &IndexSchema,
    include_display_names: bool,
) -> Result<(), IrValidationError> {
    writer.u32(index.id().get())?;
    if include_display_names {
        writer.string(index.name())?;
    }
    writer.u32(index.fields().len() as u32)?;
    for field in index.fields() {
        writer.u32(field.get())?;
    }
    encode_key_schema(writer, index.key_schema())?;
    if index
        .encodings()
        .iter()
        .any(|encoding| *encoding != IndexFieldEncodingV1::Canonical)
    {
        writer.u32(INDEX_FIELD_ENCODING_EXTENSION)?;
        writer.u32(index.encodings().len() as u32)?;
        for encoding in index.encodings() {
            writer.u8(match encoding {
                IndexFieldEncodingV1::Canonical => 0,
                IndexFieldEncodingV1::Presence => 1,
                IndexFieldEncodingV1::TextKey(TextKeyProfileV1::BinaryUtf8) => 2,
                IndexFieldEncodingV1::TextKey(TextKeyProfileV1::UnicodeFold) => 3,
            })?;
        }
    }
    Ok(())
}

fn encode_record_schema(
    writer: &mut Writer,
    record: &RecordSchema,
) -> Result<(), IrValidationError> {
    encode_record_ref(writer, record.owner())?;
    writer.u32(record.fields().len() as u32)?;
    for field in record.fields() {
        writer.u32(field.id().get())?;
        writer.string(field.name())?;
        encode_value_type(writer, field.value_type())?;
    }
    Ok(())
}

fn encode_record_ref(writer: &mut Writer, record: &RecordTypeRef) -> Result<(), IrValidationError> {
    writer.u8(record.tag())?;
    match record {
        RecordTypeRef::Entity(id) => writer.u32(id.get()),
        RecordTypeRef::Event(id) => writer.u32(id.get()),
        RecordTypeRef::CommandInput(id) => writer.u32(id.get()),
        RecordTypeRef::CommandOutcome {
            command_id,
            outcome_id,
        } => {
            writer.u32(command_id.get())?;
            writer.u32(outcome_id.get())
        }
        RecordTypeRef::ProjectionResult(id) => writer.u32(id.get()),
    }
}

pub(crate) fn encode_value_type(
    writer: &mut Writer,
    value_type: &ValueType,
) -> Result<(), IrValidationError> {
    writer.u8(value_type.tag() as u8)?;
    match value_type.tag() {
        ValueTypeTag::Decimal => {
            let spec = value_type.decimal_spec().expect("decimal tag");
            writer.u8(spec.precision())?;
            writer.u8(spec.scale())
        }
        ValueTypeTag::Money => writer.raw(value_type.currency().expect("money tag").as_bytes()),
        ValueTypeTag::String | ValueTypeTag::Bytes => {
            writer.u32(value_type.byte_bound().expect("byte bound") as u32)
        }
        ValueTypeTag::Enum => writer.u32(value_type.enum_type_id().expect("enum tag").get()),
        ValueTypeTag::Optional => {
            encode_value_type(writer, value_type.optional_inner().expect("optional tag"))
        }
        ValueTypeTag::List => {
            let (element, maximum) = value_type.list_parts().expect("list tag");
            encode_value_type(writer, element)?;
            writer.u32(maximum as u32)
        }
        ValueTypeTag::Record => {
            encode_record_ref(writer, value_type.record_ref().expect("record tag"))
        }
        ValueTypeTag::Vector => {
            writer.u32(value_type.vector_dimension().expect("vector tag").get())
        }
        // Payload-free tags, listed explicitly so a new tag carrying a payload
        // cannot silently fall into a no-op arm (the catch-all this replaces
        // dropped the vector dimension from every encoded bundle).
        ValueTypeTag::Bool
        | ValueTypeTag::I64
        | ValueTypeTag::U64
        | ValueTypeTag::Timestamp
        | ValueTypeTag::Date
        | ValueTypeTag::Uuid => Ok(()),
    }
}

fn encode_key_schema(writer: &mut Writer, schema: &KeySchema) -> Result<(), IrValidationError> {
    let codec_version = schema.codec_version();
    writer.u32(codec_version)?;
    writer.u8(schema.purpose().tag())?;
    match schema.purpose() {
        KeyPurpose::Entity(id) => writer.u32(id.get())?,
        KeyPurpose::Partition(id) | KeyPurpose::Conflict(id) => writer.u32(id.get())?,
        KeyPurpose::Index {
            index_id,
            entity_type,
        } => {
            writer.u32(index_id.get())?;
            writer.u32(entity_type.get())?;
        }
    }
    writer.u32(schema.components().len() as u32)?;
    for component in schema.components() {
        encode_value_type(writer, component.value_type())?;
        if codec_version >= crate::KEY_CODEC_VERSION_V2 {
            writer.u8(match component.codec() {
                KeyComponentCodecV1::Canonical => 0,
                KeyComponentCodecV1::OrderedBytes => 1,
            })?;
        }
        writer.u32(component.enum_variants().len() as u32)?;
        for variant in component.enum_variants() {
            writer.u32(variant.get())?;
        }
        writer.u32(component.maximum_payload_bytes() as u32)?;
    }
    writer.u32(schema.maximum_encoded_bytes() as u32)?;
    writer.bool(schema.entity_key_schema().is_some())?;
    if let Some(entity) = schema.entity_key_schema() {
        encode_key_schema(writer, entity)?;
    }
    Ok(())
}

pub(crate) fn encode_expression_arena(
    writer: &mut Writer,
    arena: &ExpressionArena,
) -> Result<(), IrValidationError> {
    writer.u32(arena.len() as u32)?;
    for node in arena.nodes() {
        writer.u8(node.kind().tag())?;
        encode_value_type(writer, node.result_type())?;
        match node.kind() {
            ExpressionKind::Constant(value) => {
                writer.bytes(&encode_canonical_value(value).map_err(|_| {
                    IrValidationError::TypeMismatch {
                        context: "expression constant encoding",
                    }
                })?)?
            }
            ExpressionKind::InputField(field)
            | ExpressionKind::ServiceValue(field)
            | ExpressionKind::CollectionElementField(field)
            | ExpressionKind::SourceEventField(field) => {
                writer.u32(field.get())?;
            }
            ExpressionKind::CollectionElement => {}
            ExpressionKind::CompleteBinding(binding) => writer.u32(binding.get())?,
            ExpressionKind::BoundField { binding, field } => {
                writer.u32(binding.get())?;
                writer.u32(field.get())?;
            }
            ExpressionKind::SchemaField { entity_type, field } => {
                writer.u32(entity_type.get())?;
                writer.u32(field.get())?;
            }
            ExpressionKind::RootValidationField { read, field } => {
                writer.u32(read.get())?;
                writer.u32(field.get())?;
            }
            ExpressionKind::TransactionTime | ExpressionKind::TransactionDate => {}
            ExpressionKind::Unary { operator, operand } => {
                writer.u8(*operator as u8)?;
                writer.u32(operand.get())?;
            }
            ExpressionKind::Binary {
                operator,
                left,
                right,
            } => {
                writer.u8(*operator as u8)?;
                writer.u32(left.get())?;
                writer.u32(right.get())?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
fn encode_command_bundle_entry(
    writer: &mut Writer,
    command: &CommandPlan,
    schema: &SchemaIr,
) -> Result<(), IrValidationError> {
    let ir_version = if command.requires_ir_v10() {
        EXECUTABLE_IR_VERSION_V10
    } else if command.requires_ir_v6() {
        EXECUTABLE_IR_VERSION_V6
    } else if command.requires_ir_v5() {
        EXECUTABLE_IR_VERSION_V5
    } else if command.requires_ir_v3() {
        EXECUTABLE_IR_VERSION_V3
    } else if command.requires_ir_v2() {
        EXECUTABLE_IR_VERSION_V2
    } else {
        EXECUTABLE_IR_VERSION_V1
    };
    encode_command_bundle_entry_versioned(writer, command, schema, ir_version)
}

fn encode_command_bundle_entry_versioned(
    writer: &mut Writer,
    command: &CommandPlan,
    schema: &SchemaIr,
    ir_version: u32,
) -> Result<(), IrValidationError> {
    writer.u32(command.command_id().get())?;
    writer.string(command.name())?;
    writer.u64(command.contract_version().get())?;
    writer.raw(command.plan_hash().as_bytes())?;
    encode_command_semantics_versioned(writer, command, schema, true, ir_version)
}

#[cfg(test)]
fn encode_command_semantics(
    writer: &mut Writer,
    command: &CommandPlan,
    schema: &SchemaIr,
    include_display_names: bool,
) -> Result<(), IrValidationError> {
    let ir_version = if command.requires_ir_v10() {
        EXECUTABLE_IR_VERSION_V10
    } else if command.requires_ir_v6() {
        EXECUTABLE_IR_VERSION_V6
    } else if command.requires_ir_v5() {
        EXECUTABLE_IR_VERSION_V5
    } else if command.requires_ir_v3() {
        EXECUTABLE_IR_VERSION_V3
    } else if command.requires_ir_v2() {
        EXECUTABLE_IR_VERSION_V2
    } else {
        EXECUTABLE_IR_VERSION_V1
    };
    encode_command_semantics_versioned(writer, command, schema, include_display_names, ir_version)
}

fn encode_command_semantics_versioned(
    writer: &mut Writer,
    command: &CommandPlan,
    schema: &SchemaIr,
    include_display_names: bool,
    ir_version: u32,
) -> Result<(), IrValidationError> {
    encode_record_schema(writer, command.input().record())?;
    if ir_version >= EXECUTABLE_IR_VERSION_V2 {
        writer.u32(command.service_values().len() as u32)?;
        for value in command.service_values() {
            writer.u32(value.field().id().get())?;
            if include_display_names {
                writer.string(value.field().name())?;
            }
            encode_value_type(writer, value.field().value_type())?;
            writer.u8(value.kind() as u8)?;
        }
    }
    writer.u32(command.outcomes().len() as u32)?;
    for outcome in command.outcomes() {
        encode_outcome_schema(writer, outcome)?;
    }
    writer.u32(command.success_outcome().get())?;
    writer.bool(command.idempotency_input().is_some())?;
    if let Some(field) = command.idempotency_input() {
        writer.u32(field.get())?;
    }

    let input_artifact = GeneratedSchemaArtifact::command_input(
        command.command_id(),
        command.input().record(),
        schema,
        command.idempotency_input(),
    )?;
    let output_artifact = GeneratedSchemaArtifact::command_outcomes(
        command.command_id(),
        command.outcomes(),
        schema,
    )?;
    writer.raw(input_artifact.hash().as_bytes())?;
    writer.raw(output_artifact.hash().as_bytes())?;

    if ir_version >= EXECUTABLE_IR_VERSION_V5 {
        writer.bool(command.collection_expansion().is_some())?;
        if let Some(expansion) = command.collection_expansion() {
            encode_collection_expansion(writer, expansion)?;
        }
    }

    encode_expression_arena(writer, command.expressions())?;
    writer.u32(command.bindings().len() as u32)?;
    for binding in command.bindings() {
        encode_binding(writer, binding, include_display_names, ir_version)?;
    }
    writer.u32(command.root_validation_reads().len() as u32)?;
    for read in command.root_validation_reads() {
        writer.u32(read.id().get())?;
        writer.u32(read.source_binding().get())?;
        writer.u32(read.entity_type().get())?;
        encode_key_schema(writer, read.key_schema())?;
        writer.u32(read.key_expressions().len() as u32)?;
        for expression in read.key_expressions() {
            writer.u32(expression.get())?;
        }
        writer.u32(read.accessed_fields().len() as u32)?;
        for field in read.accessed_fields() {
            writer.u32(field.get())?;
        }
    }
    if !schema.relationships().is_empty() {
        writer.u32(command.relationship_checks().len() as u32)?;
        for check in command.relationship_checks() {
            writer.string(check.relationship_name())?;
            writer.u32(check.source_binding().get())?;
            writer.u32(check.target_binding().get())?;
        }
    }
    if ir_version >= EXECUTABLE_IR_VERSION_V5 {
        writer.u32(command.delete_checks().len() as u32)?;
        for check in command.delete_checks() {
            writer.u32(check.binding().get())?;
            match check.mode() {
                crate::DeleteCheckModeV1::NoInbound => {
                    writer.u8(crate::format_registry::delete_check_mode::NO_INBOUND)?;
                }
                crate::DeleteCheckModeV1::Restrict {
                    source_entity,
                    index_id,
                } => {
                    writer.u8(crate::format_registry::delete_check_mode::RESTRICT)?;
                    writer.u32(source_entity.get())?;
                    writer.u32(index_id.get())?;
                }
            }
        }
    }
    encode_locality(writer, command.locality())?;
    writer.u32(command.commit_checks().len() as u32)?;
    for check in command.commit_checks() {
        writer.u32(check.invariant_id().get())?;
        writer.u32(check.predicate().get())?;
        writer.u32(check.source_bindings().len() as u32)?;
        for binding in check.source_bindings() {
            writer.u32(binding.get())?;
        }
        writer.u32(check.root_validation_reads().len() as u32)?;
        for read in check.root_validation_reads() {
            writer.u32(read.get())?;
        }
    }
    writer.u32(command.instructions().len() as u32)?;
    for instruction in command.instructions() {
        encode_instruction(writer, instruction)?;
    }
    if ir_version >= EXECUTABLE_IR_VERSION_V10 {
        writer.u8(command.invocation_class() as u8)?;
    }
    writer.u8(command.execution_class() as u8)?;
    writer.u8(command.retry_policy() as u8)?;
    match command.required_capability() {
        CapabilityRequirement::InvokeCommand {
            lineage,
            command_id,
        } => {
            writer.u8(capability_tag::INVOKE_COMMAND)?;
            writer.string(lineage.as_str())?;
            writer.u32(command_id.get())?;
        }
    }

    // Transitive schema closure: bound entities, aggregate, and emitted events.
    let mut entities = command
        .bindings()
        .iter()
        .map(BindingPlan::entity_type)
        .chain(
            command
                .root_validation_reads()
                .iter()
                .map(crate::RootValidationReadPlan::entity_type),
        )
        .collect::<Vec<_>>();
    entities.sort_unstable();
    entities.dedup();
    writer.u32(entities.len() as u32)?;
    for id in entities {
        encode_entity_schema(
            writer,
            schema
                .entity(id)
                .ok_or(IrValidationError::InvalidReference {
                    kind: "command entity closure",
                })?,
            include_display_names,
        )?;
    }
    encode_aggregate_schema(
        writer,
        schema.aggregate(command.locality().aggregate_id()).ok_or(
            IrValidationError::InvalidReference {
                kind: "command aggregate closure",
            },
        )?,
        include_display_names,
    )?;
    let mut events = command
        .instructions()
        .iter()
        .filter_map(|instruction| match instruction {
            Instruction::EmitEvent(event) => Some(event.event_type()),
            _ => None,
        })
        .collect::<Vec<_>>();
    events.sort_unstable();
    events.dedup();
    writer.u32(events.len() as u32)?;
    for id in events {
        encode_event_schema(
            writer,
            schema
                .event(id)
                .ok_or(IrValidationError::InvalidReference {
                    kind: "command event closure",
                })?,
            include_display_names,
        )?;
    }
    Ok(())
}

fn encode_collection_expansion(
    writer: &mut Writer,
    expansion: &CollectionExpansionPlanV1,
) -> Result<(), IrValidationError> {
    writer.u32(expansion.input_field().get())?;
    writer.u32(expansion.minimum_elements() as u32)?;
    writer.u32(expansion.maximum_elements() as u32)?;
    encode_value_type(writer, expansion.element_type())?;
    writer.u32(expansion.first_binding().get())?;
    writer.u32(expansion.binding_count() as u32)?;
    writer.u32(expansion.first_instruction())?;
    writer.u32(expansion.instruction_count() as u32)?;
    writer.u8(expansion.duplicate_policy() as u8)
}

fn encode_outcome_schema(
    writer: &mut Writer,
    outcome: &OutcomeSchema,
) -> Result<(), IrValidationError> {
    writer.u32(outcome.id().get())?;
    writer.string(outcome.name())?;
    encode_record_schema(writer, outcome.payload())
}

fn encode_binding(
    writer: &mut Writer,
    binding: &BindingPlan,
    include_display_names: bool,
    ir_version: u32,
) -> Result<(), IrValidationError> {
    writer.u32(binding.id().get())?;
    if include_display_names {
        writer.string(binding.name())?;
    }
    writer.u8(binding.mode() as u8)?;
    writer.u32(binding.entity_type().get())?;
    encode_key_schema(writer, binding.key_schema())?;
    writer.u32(binding.key_expressions().len() as u32)?;
    for expression in binding.key_expressions() {
        writer.u32(expression.get())?;
    }
    writer.u32(binding.accessed_fields().len() as u32)?;
    for field in binding.accessed_fields() {
        writer.u32(field.get())?;
    }
    writer.bool(binding.complete_record_access())?;
    encode_outcome_construction(writer, binding.failure())?;
    if ir_version >= EXECUTABLE_IR_VERSION_V6 {
        writer.bool(binding.restriction_failure().is_some())?;
        if let Some(failure) = binding.restriction_failure() {
            encode_outcome_construction(writer, failure)?;
        }
    }
    Ok(())
}

fn encode_locality(
    writer: &mut Writer,
    locality: &crate::LocalityPlan,
) -> Result<(), IrValidationError> {
    writer.u32(locality.aggregate_id().get())?;
    encode_key_schema(writer, locality.partition_schema())?;
    writer.u32(locality.partition_expression().get())?;
    writer.u32(locality.conflict_keys().len() as u32)?;
    for conflict in locality.conflict_keys() {
        encode_conflict_derivation(writer, conflict)?;
    }
    Ok(())
}

fn encode_conflict_derivation(
    writer: &mut Writer,
    conflict: &ConflictDerivationPlan,
) -> Result<(), IrValidationError> {
    encode_key_schema(writer, conflict.schema())?;
    writer.u32(conflict.expressions().len() as u32)?;
    for expression in conflict.expressions() {
        writer.u32(expression.get())?;
    }
    Ok(())
}

fn encode_instruction(
    writer: &mut Writer,
    instruction: &Instruction,
) -> Result<(), IrValidationError> {
    writer.u8(instruction.tag())?;
    match instruction {
        Instruction::Require {
            requirement_index,
            predicate,
            reject,
        } => {
            writer.u32(*requirement_index)?;
            writer.u32(predicate.get())?;
            encode_outcome_construction(writer, reject)
        }
        Instruction::SetField {
            binding,
            field,
            value,
        } => {
            writer.u32(binding.get())?;
            writer.u32(field.get())?;
            writer.u32(value.get())
        }
        Instruction::WorkflowTransition {
            binding,
            state_field,
            source_states,
            destination,
            expected_revision,
            stale,
            illegal,
        } => {
            writer.u32(binding.get())?;
            writer.u32(state_field.get())?;
            writer.u32(source_states.len() as u32)?;
            for source in source_states {
                writer.u32(source.get())?;
            }
            writer.u32(destination.get())?;
            writer.u32(expected_revision.get())?;
            encode_outcome_construction(writer, stale)?;
            encode_outcome_construction(writer, illegal)
        }
        Instruction::WorkflowLease {
            binding,
            fields,
            operation,
        } => {
            writer.u32(binding.get())?;
            writer.u32(fields.owner_field.get())?;
            writer.u32(fields.expiry_field.get())?;
            writer.u32(fields.fencing_token_field.get())?;
            writer.bool(fields.attempt_field.is_some())?;
            if let Some(field) = fields.attempt_field {
                writer.u32(field.get())?;
            }
            writer.u64(fields.minimum_duration_seconds)?;
            writer.u64(fields.maximum_duration_seconds)?;
            match operation {
                WorkflowLeaseOperation::Claim {
                    owner,
                    duration_seconds,
                    expected_revision,
                    stale,
                    unavailable,
                    invalid,
                    exhausted,
                } => {
                    writer.u8(lease_operation_tag::CLAIM)?;
                    for expression in [owner, duration_seconds, expected_revision] {
                        writer.u32(expression.get())?;
                    }
                    for outcome in [stale, unavailable, invalid, exhausted] {
                        encode_outcome_construction(writer, outcome)?;
                    }
                }
                WorkflowLeaseOperation::Renew {
                    owner,
                    fencing_token,
                    duration_seconds,
                    expected_revision,
                    stale,
                    invalid,
                    expired,
                    exhausted,
                } => {
                    writer.u8(lease_operation_tag::RENEW)?;
                    for expression in [owner, fencing_token, duration_seconds, expected_revision] {
                        writer.u32(expression.get())?;
                    }
                    for outcome in [stale, invalid, expired, exhausted] {
                        encode_outcome_construction(writer, outcome)?;
                    }
                }
                WorkflowLeaseOperation::Release {
                    owner,
                    fencing_token,
                    expected_revision,
                    stale,
                    invalid,
                } => {
                    writer.u8(lease_operation_tag::RELEASE)?;
                    for expression in [owner, fencing_token, expected_revision] {
                        writer.u32(expression.get())?;
                    }
                    for outcome in [stale, invalid] {
                        encode_outcome_construction(writer, outcome)?;
                    }
                }
                WorkflowLeaseOperation::Expire {
                    expected_revision,
                    stale,
                    active,
                } => {
                    writer.u8(lease_operation_tag::EXPIRE)?;
                    writer.u32(expected_revision.get())?;
                    for outcome in [stale, active] {
                        encode_outcome_construction(writer, outcome)?;
                    }
                }
                WorkflowLeaseOperation::Fence {
                    owner,
                    fencing_token,
                    expected_revision,
                    stale,
                    invalid,
                    expired,
                } => {
                    writer.u8(lease_operation_tag::FENCE)?;
                    for expression in [owner, fencing_token, expected_revision] {
                        writer.u32(expression.get())?;
                    }
                    for outcome in [stale, invalid, expired] {
                        encode_outcome_construction(writer, outcome)?;
                    }
                }
            }
            Ok(())
        }
        Instruction::EmitEvent(event) => encode_event_construction(writer, event),
        Instruction::Return(outcome) => encode_outcome_construction(writer, outcome),
    }
}

fn encode_object(
    writer: &mut Writer,
    object: &ObjectConstruction,
) -> Result<(), IrValidationError> {
    encode_record_ref(writer, object.record())?;
    writer.u32(object.fields().len() as u32)?;
    for field in object.fields() {
        writer.u32(field.field_id().get())?;
        writer.u32(field.expression().get())?;
    }
    Ok(())
}

fn encode_outcome_construction(
    writer: &mut Writer,
    outcome: &OutcomeConstruction,
) -> Result<(), IrValidationError> {
    writer.u32(outcome.outcome_id().get())?;
    encode_object(writer, outcome.payload())
}

fn encode_event_construction(
    writer: &mut Writer,
    event: &EventConstruction,
) -> Result<(), IrValidationError> {
    writer.u32(event.event_type().get())?;
    encode_object(writer, event.payload())
}

fn encode_projection_bundle_entry(
    writer: &mut Writer,
    projection: &ProjectionPlan,
) -> Result<(), IrValidationError> {
    writer.u32(projection.projection_id().get())?;
    writer.string(projection.name())?;
    writer.raw(projection.plan_hash().as_bytes())?;
    encode_projection_semantics(writer, projection)
}

fn encode_projection_semantics(
    writer: &mut Writer,
    projection: &ProjectionPlan,
) -> Result<(), IrValidationError> {
    writer.u32(projection.source_event().get())?;
    encode_expression_arena(writer, projection.expressions())?;
    writer.bool(projection.filter().is_some())?;
    if let Some(filter) = projection.filter() {
        writer.u32(filter.get())?;
    }
    writer.u32(projection.key_expressions().len() as u32)?;
    for expression in projection.key_expressions() {
        writer.u32(expression.get())?;
    }
    writer.u32(projection.measures().len() as u32)?;
    for measure in projection.measures() {
        encode_projection_measure(writer, measure)?;
    }
    writer.u8(projection.frontier() as u8)?;
    encode_projection_group_schema(writer, projection.group_schema())
}

fn encode_projection_measure(
    writer: &mut Writer,
    measure: &ProjectionMeasurePlan,
) -> Result<(), IrValidationError> {
    writer.u32(measure.field().id().get())?;
    writer.string(measure.field().name())?;
    encode_value_type(writer, measure.field().value_type())?;
    writer.u8(measure.aggregation() as u8)?;
    writer.bool(measure.expression().is_some())?;
    if let Some(expression) = measure.expression() {
        writer.u32(expression.get())?;
    }
    Ok(())
}

fn encode_projection_group_schema(
    writer: &mut Writer,
    schema: &ProjectionGroupSchema,
) -> Result<(), IrValidationError> {
    writer.u32(schema.projection_id().get())?;
    writer.u32(schema.codec_version())?;
    writer.u32(schema.group_components().len() as u32)?;
    for component in schema.group_components() {
        encode_projection_component(writer, component)?;
    }
    encode_record_schema(writer, schema.measures())?;
    writer.u32(schema.maximum_complete_key_bytes() as u32)?;
    writer.u32(schema.maximum_stored_state_bytes() as u32)
}

fn encode_projection_component(
    writer: &mut Writer,
    component: &ProjectionGroupComponentSchema,
) -> Result<(), IrValidationError> {
    encode_value_type(writer, component.value_type())?;
    writer.u32(component.enum_variants().len() as u32)?;
    for variant in component.enum_variants() {
        writer.u32(variant.get())?;
    }
    writer.u32(component.maximum_framed_bytes() as u32)
}

fn encode_mcp_registry(
    writer: &mut Writer,
    registry: &McpCommandNameRegistryV2,
) -> Result<(), IrValidationError> {
    writer.u32(registry.version())?;
    writer.string(registry.lineage().as_str())?;
    writer.string(registry.source_contract_name())?;
    writer.u32(registry.entries().len() as u32)?;
    for entry in registry.entries() {
        writer.u32(entry.command_id().get())?;
        writer.string(entry.source_command_name())?;
        writer.string(entry.tool_name().as_str())?;
    }
    Ok(())
}

fn encode_compatibility(
    writer: &mut Writer,
    report: &CompatibilityReport,
) -> Result<(), IrValidationError> {
    writer.u8(report.overall() as u8)?;
    writer.u32(report.entries().len() as u32)?;
    for entry in report.entries() {
        writer.string(entry.code().as_str())?;
        writer.string(entry.affected_path())?;
    }
    Ok(())
}

fn decode_bundle(bytes: &[u8]) -> Result<ContractBundle, IrValidationError> {
    checked_len("canonical contract bundle", bytes.len(), MAX_BUNDLE_BYTES)?;
    let mut reader = Reader::new(bytes);
    if reader.read(BUNDLE_MAGIC.len())? != BUNDLE_MAGIC {
        return Err(IrValidationError::InvalidText {
            kind: "bundle magic",
        });
    }
    let format_version = reader.u32()?;
    let grammar_version = reader.u32()?;
    let ir_version = reader.u32()?;
    if !matches!(
        (format_version, grammar_version, ir_version),
        (
            BUNDLE_FORMAT_VERSION_V1,
            GRAMMAR_VERSION_V1,
            EXECUTABLE_IR_VERSION_V1
        ) | (
            BUNDLE_FORMAT_VERSION_V2,
            GRAMMAR_VERSION_V2,
            EXECUTABLE_IR_VERSION_V2
        ) | (
            BUNDLE_FORMAT_VERSION_V3,
            GRAMMAR_VERSION_V3,
            EXECUTABLE_IR_VERSION_V3
        ) | (
            BUNDLE_FORMAT_VERSION_V4,
            GRAMMAR_VERSION_V4,
            EXECUTABLE_IR_VERSION_V4
        ) | (
            BUNDLE_FORMAT_VERSION_V5,
            GRAMMAR_VERSION_V5,
            EXECUTABLE_IR_VERSION_V5
        ) | (
            BUNDLE_FORMAT_VERSION_V6,
            GRAMMAR_VERSION_V6,
            EXECUTABLE_IR_VERSION_V6
        ) | (
            BUNDLE_FORMAT_VERSION_V7,
            GRAMMAR_VERSION_V7,
            EXECUTABLE_IR_VERSION_V7
        ) | (
            BUNDLE_FORMAT_VERSION_V8,
            GRAMMAR_VERSION_V8,
            EXECUTABLE_IR_VERSION_V8
        ) | (
            BUNDLE_FORMAT_VERSION_V9,
            GRAMMAR_VERSION_V9,
            EXECUTABLE_IR_VERSION_V9
        ) | (
            BUNDLE_FORMAT_VERSION_V10,
            GRAMMAR_VERSION_V10,
            EXECUTABLE_IR_VERSION_V10
        )
    ) {
        return Err(IrValidationError::UnsupportedVersion {
            kind: "contract bundle version tuple",
            value: ir_version,
        });
    }
    let compiler_version = reader.string(64)?;
    let lineage_text = reader.string(256)?;
    validate_source_name(&lineage_text, "contract lineage")?;
    let lineage =
        ContractLineage::new(lineage_text).map_err(|_| IrValidationError::InvalidText {
            kind: "contract lineage",
        })?;
    let contract_version =
        ContractVersion::new(reader.u64()?).ok_or(IrValidationError::InvalidReference {
            kind: "contract version",
        })?;
    let parent = if reader.bool()? {
        Some(ParentBundleRef::new(
            ContractVersion::new(reader.u64()?).ok_or(IrValidationError::InvalidReference {
                kind: "parent contract version",
            })?,
            ContractBundleHash::from_bytes(reader.array()?),
        ))
    } else {
        None
    };
    let source_hash = SourceHash::from_bytes(reader.array()?);
    let stored_root_hash = ContractPlanRootHash::from_bytes(reader.array()?);
    let ledger = decode_ledger(&mut reader)?;
    let schema = decode_schema(&mut reader)?;
    let workflows = if ir_version >= EXECUTABLE_IR_VERSION_V2 {
        decode_workflows(&mut reader, &schema, ir_version)?
    } else {
        WorkflowCatalog::new(Vec::new(), &schema)?
    };
    let row_policies = if ir_version >= EXECUTABLE_IR_VERSION_V4 {
        decode_row_policy_catalog(&mut reader, &schema)?
    } else {
        RowPolicyCatalogV1::empty()
    };
    let commands = decode_commands(&mut reader, &lineage, &schema, ir_version)?;
    let projections = decode_projections(&mut reader, &schema)?;
    let schema_artifacts = decode_schema_artifacts(&mut reader, &schema, &commands, &projections)?;
    let mcp_command_names = decode_mcp_registry(&mut reader)?;
    let compatibility = decode_compatibility(&mut reader, parent.is_some())?;
    reader.finish()?;

    let bundle = ContractBundle::new_with_versions(
        format_version,
        grammar_version,
        ir_version,
        compiler_version,
        lineage,
        contract_version,
        parent,
        source_hash,
        ledger,
        schema,
        workflows.workflows().to_vec(),
        row_policies,
        commands,
        projections,
        schema_artifacts,
        mcp_command_names,
        compatibility,
    )?;
    if bundle.plan_root_hash != stored_root_hash {
        return Err(IrValidationError::HashMismatch {
            kind: "contract plan root",
        });
    }
    if bundle.canonical_bytes != bytes {
        return Err(IrValidationError::NonCanonicalOrder {
            kind: "canonical contract bundle bytes",
        });
    }
    Ok(bundle)
}

fn decode_workflows(
    reader: &mut Reader<'_>,
    schema: &SchemaIr,
    ir_version: u32,
) -> Result<WorkflowCatalog, IrValidationError> {
    let count = decode_len_with_minimum(reader, "workflows", crate::MAX_DECLARATIONS_PER_KIND, 21)?;
    let mut workflows = Vec::with_capacity(count);
    for _ in 0..count {
        let name = reader.string(256)?;
        let entity = decode_entity_id(reader)?;
        let state_field = decode_field_id(reader)?;
        let state_enum = decode_enum_id(reader)?;
        let initial_state = if ir_version >= EXECUTABLE_IR_VERSION_V9 && reader.bool()? {
            Some(decode_enum_variant_id(reader)?)
        } else {
            None
        };
        let transition_count = decode_len_with_minimum(
            reader,
            "workflow transitions",
            crate::MAX_DECLARATIONS_PER_KIND,
            17,
        )?;
        let mut transitions = Vec::with_capacity(transition_count);
        for _ in 0..transition_count {
            let transition_name = reader.string(256)?;
            let source_count = decode_len_with_minimum(
                reader,
                "workflow transition source states",
                crate::MAX_DECLARATIONS_PER_KIND,
                4,
            )?;
            let mut source_states = Vec::with_capacity(source_count);
            for _ in 0..source_count {
                source_states.push(decode_enum_variant_id(reader)?);
            }
            transitions.push(WorkflowTransitionSchema::new(
                transition_name,
                source_states,
                decode_enum_variant_id(reader)?,
            )?);
        }
        let lease = if reader.bool()? {
            let lease_name = reader.string(256)?;
            let owner_field = decode_field_id(reader)?;
            let expiry_field = decode_field_id(reader)?;
            let fencing_token_field = decode_field_id(reader)?;
            let attempt_field = if reader.bool()? {
                Some(decode_field_id(reader)?)
            } else {
                None
            };
            Some(WorkflowLeaseSchema::new(
                lease_name,
                owner_field,
                expiry_field,
                fencing_token_field,
                attempt_field,
                reader.u64()?,
                reader.u64()?,
            )?)
        } else {
            None
        };
        workflows.push(WorkflowSchema::new(
            name,
            entity,
            state_field,
            state_enum,
            initial_state,
            transitions,
            lease,
        )?);
    }
    WorkflowCatalog::new(workflows, schema)
}

fn require_version(value: u32, expected: u32, kind: &'static str) -> Result<(), IrValidationError> {
    if value == expected {
        Ok(())
    } else {
        Err(IrValidationError::UnsupportedVersion { kind, value })
    }
}

fn decode_len(
    reader: &mut Reader<'_>,
    kind: &'static str,
    maximum: usize,
) -> Result<usize, IrValidationError> {
    decode_len_with_minimum(reader, kind, maximum, 1)
}

fn decode_len_with_minimum(
    reader: &mut Reader<'_>,
    kind: &'static str,
    maximum: usize,
    minimum_encoded_bytes: usize,
) -> Result<usize, IrValidationError> {
    let value = reader.u32()? as usize;
    checked_len(kind, value, maximum)?;
    let minimum = value
        .checked_mul(minimum_encoded_bytes)
        .ok_or(IrValidationError::SizeOverflow { kind })?;
    if minimum > reader.remaining() {
        return Err(IrValidationError::UnexpectedEnd);
    }
    Ok(value)
}

fn ensure_fixed_width_items(
    reader: &Reader<'_>,
    count: usize,
    width: usize,
    kind: &'static str,
) -> Result<(), IrValidationError> {
    let minimum = count
        .checked_mul(width)
        .ok_or(IrValidationError::SizeOverflow { kind })?;
    if minimum > reader.remaining() {
        return Err(IrValidationError::UnexpectedEnd);
    }
    Ok(())
}

macro_rules! decode_u32_id {
    ($name:ident, $type:ty, $kind:literal) => {
        fn $name(reader: &mut Reader<'_>) -> Result<$type, IrValidationError> {
            <$type>::new(reader.u32()?).ok_or(IrValidationError::InvalidReference { kind: $kind })
        }
    };
}

decode_u32_id!(decode_entity_id, EntityTypeId, "entity ID");
decode_u32_id!(decode_event_id, EventTypeId, "event ID");
decode_u32_id!(decode_enum_id, EnumTypeId, "enum ID");
decode_u32_id!(decode_enum_variant_id, EnumVariantId, "enum variant ID");
decode_u32_id!(decode_aggregate_id, AggregateTypeId, "aggregate ID");
decode_u32_id!(decode_command_id, CommandId, "command ID");
decode_u32_id!(decode_projection_id, ProjectionId, "projection ID");
decode_u32_id!(decode_field_id, FieldId, "field ID");
decode_u32_id!(decode_outcome_id, OutcomeId, "outcome ID");
decode_u32_id!(decode_index_id, IndexId, "index ID");
decode_u32_id!(decode_invariant_id, InvariantId, "invariant ID");

fn decode_namespace_tag(tag: u8) -> Result<StableIdNamespaceTag, IrValidationError> {
    match tag {
        namespace_tag::ENTITY => Ok(StableIdNamespaceTag::Entity),
        namespace_tag::EVENT => Ok(StableIdNamespaceTag::Event),
        namespace_tag::ENUM => Ok(StableIdNamespaceTag::Enum),
        namespace_tag::AGGREGATE => Ok(StableIdNamespaceTag::Aggregate),
        namespace_tag::COMMAND => Ok(StableIdNamespaceTag::Command),
        namespace_tag::PROJECTION => Ok(StableIdNamespaceTag::Projection),
        namespace_tag::INDEX => Ok(StableIdNamespaceTag::Index),
        namespace_tag::INVARIANT => Ok(StableIdNamespaceTag::Invariant),
        namespace_tag::FIELD => Ok(StableIdNamespaceTag::Field),
        namespace_tag::OUTCOME => Ok(StableIdNamespaceTag::Outcome),
        namespace_tag::ENUM_VARIANT => Ok(StableIdNamespaceTag::EnumVariant),
        tag => Err(IrValidationError::UnknownTag {
            kind: "stable-ID namespace",
            tag,
        }),
    }
}

fn decode_lineage_entry_state(tag: u8) -> Result<LineageEntryState, IrValidationError> {
    match tag {
        lineage_state_tag::ACTIVE => Ok(LineageEntryState::Active),
        lineage_state_tag::TOMBSTONE => Ok(LineageEntryState::Tombstone),
        tag => Err(IrValidationError::UnknownTag {
            kind: "lineage entry state",
            tag,
        }),
    }
}

fn decode_ledger(reader: &mut Reader<'_>) -> Result<LineageLedgerV1, IrValidationError> {
    let version = reader.u32()?;
    if !matches!(
        version,
        LINEAGE_LEDGER_VERSION_V1 | LINEAGE_LEDGER_VERSION_V2
    ) {
        return Err(IrValidationError::UnsupportedVersion {
            kind: "lineage ledger",
            value: version,
        });
    }
    let count = decode_len_with_minimum(
        reader,
        "lineage allocations",
        MAX_LINEAGE_ALLOCATION_STATES,
        11,
    )?;
    let mut allocations = Vec::with_capacity(count);
    let mut total_entries = 0usize;
    for _ in 0..count {
        let tag = decode_namespace_tag(reader.u8()?)?;
        let owner_kind = reader.u8()?;
        let owner_count = reader.u8()? as usize;
        ensure_fixed_width_items(reader, owner_count, 4, "lineage allocation owner path")?;
        let mut owner_ids = Vec::with_capacity(owner_count);
        for _ in 0..owner_count {
            owner_ids.push(reader.u32()?);
        }
        let namespace = if matches!(
            tag,
            StableIdNamespaceTag::Entity
                | StableIdNamespaceTag::Event
                | StableIdNamespaceTag::Enum
                | StableIdNamespaceTag::Aggregate
                | StableIdNamespaceTag::Command
                | StableIdNamespaceTag::Projection
                | StableIdNamespaceTag::Index
                | StableIdNamespaceTag::Invariant
        ) {
            if owner_kind != 0 || !owner_ids.is_empty() {
                return Err(IrValidationError::InvalidLineageLedger {
                    reason: "global allocation state has an owner path",
                });
            }
            StableIdAllocationNamespace::global(tag)?
        } else {
            StableIdAllocationNamespace::scoped(tag, owner_kind, owner_ids)?
        };
        let max_allocated = reader.u32()?;
        let entry_count =
            decode_len_with_minimum(reader, "lineage entries", MAX_LINEAGE_LEDGER_ENTRIES, 11)?;
        total_entries =
            total_entries
                .checked_add(entry_count)
                .ok_or(IrValidationError::SizeOverflow {
                    kind: "lineage entries",
                })?;
        checked_len("lineage entries", total_entries, MAX_LINEAGE_LEDGER_ENTRIES)?;
        let mut entries = Vec::with_capacity(entry_count);
        for expected in 1..=entry_count {
            let id = reader.u32()?;
            if id as usize != expected {
                return Err(IrValidationError::InvalidLineageLedger {
                    reason: "lineage entry IDs are not contiguous",
                });
            }
            let identity_owner_kind = reader.u8()?;
            let identity_owner_count = reader.u8()? as usize;
            ensure_fixed_width_items(
                reader,
                identity_owner_count,
                4,
                "lineage identity owner path",
            )?;
            let mut identity_owner_ids = Vec::with_capacity(identity_owner_count);
            for _ in 0..identity_owner_count {
                identity_owner_ids.push(reader.u32()?);
            }
            let identity = StableIdentity::new(
                StableIdNamespace::new(tag, identity_owner_kind, identity_owner_ids)?,
                reader.string(256)?,
            )?;
            if identity.namespace.allocation_namespace() != namespace {
                return Err(IrValidationError::InvalidLineageLedger {
                    reason: "lineage identity is in the wrong allocation state",
                });
            }
            let state = decode_lineage_entry_state(reader.u8()?)?;
            entries.push(LineageEntry {
                id,
                identity,
                state,
            });
        }
        if max_allocated as usize != entries.len() {
            return Err(IrValidationError::InvalidLineageLedger {
                reason: "lineage max_allocated does not equal its complete history",
            });
        }
        allocations.push(LineageAllocation {
            namespace,
            max_allocated,
            entries,
        });
    }
    if allocations
        .windows(2)
        .any(|pair| pair[0].namespace >= pair[1].namespace)
    {
        return Err(IrValidationError::NonCanonicalOrder {
            kind: "lineage allocation states",
        });
    }
    let aliases = if version == LINEAGE_LEDGER_VERSION_V2 {
        let count =
            decode_len_with_minimum(reader, "lineage aliases", MAX_LINEAGE_LEDGER_ENTRIES, 13)?;
        let mut aliases = Vec::with_capacity(count);
        for _ in 0..count {
            let tag = decode_namespace_tag(reader.u8()?)?;
            let owner_kind = reader.u8()?;
            let owner_count = reader.u8()? as usize;
            ensure_fixed_width_items(reader, owner_count, 4, "lineage alias owner path")?;
            let mut owner_ids = Vec::with_capacity(owner_count);
            for _ in 0..owner_count {
                owner_ids.push(reader.u32()?);
            }
            aliases.push(LineageAlias {
                identity: StableIdentity::new(
                    StableIdNamespace::new(tag, owner_kind, owner_ids)?,
                    reader.string(256)?,
                )?,
                id: reader.u32()?,
            });
        }
        aliases
    } else {
        Vec::new()
    };
    Ok(LineageLedgerV1 {
        version,
        allocations,
        aliases,
    })
}

fn decode_schema(reader: &mut Reader<'_>) -> Result<SchemaIr, IrValidationError> {
    let entity_count = decode_len(reader, "entities", crate::MAX_DECLARATIONS_PER_KIND)?;
    let mut entities = Vec::with_capacity(entity_count);
    for _ in 0..entity_count {
        entities.push(decode_entity_schema(reader)?);
    }
    let event_count = decode_len(reader, "events", crate::MAX_DECLARATIONS_PER_KIND)?;
    let mut events = Vec::with_capacity(event_count);
    for _ in 0..event_count {
        events.push(decode_event_schema(reader, &entities)?);
    }
    let enum_count = decode_len(reader, "enums", crate::MAX_DECLARATIONS_PER_KIND)?;
    let mut enums = Vec::with_capacity(enum_count);
    for _ in 0..enum_count {
        enums.push(decode_enum_schema(reader)?);
    }
    let aggregate_count = decode_len(reader, "aggregates", crate::MAX_DECLARATIONS_PER_KIND)?;
    let mut aggregates = Vec::with_capacity(aggregate_count);
    for _ in 0..aggregate_count {
        aggregates.push(decode_aggregate_schema(reader)?);
    }
    let mut relationships = Vec::new();
    if reader.remaining() >= 4 && reader.peek_u32()? == RELATIONSHIP_SCHEMA_EXTENSION {
        let _marker = reader.u32()?;
        let relationship_count =
            decode_len(reader, "relationships", crate::MAX_DECLARATIONS_PER_KIND)?;
        relationships.reserve(relationship_count);
        for _ in 0..relationship_count {
            let name = reader.string(256)?;
            let source_entity = decode_entity_id(reader)?;
            let source_count = decode_len(reader, "relationship source fields", 1_024)?;
            let mut source_fields = Vec::with_capacity(source_count);
            for _ in 0..source_count {
                source_fields.push(decode_field_id(reader)?);
            }
            let target_entity = decode_entity_id(reader)?;
            let target_count = decode_len(reader, "relationship target fields", 1_024)?;
            let mut target_fields = Vec::with_capacity(target_count);
            for _ in 0..target_count {
                target_fields.push(decode_field_id(reader)?);
            }
            relationships.push(crate::RelationshipSchema::new(
                name,
                source_entity,
                source_fields,
                target_entity,
                target_fields,
            )?);
        }
    }
    let mut unique_keys = Vec::new();
    if reader.remaining() >= 4 && reader.peek_u32()? == UNIQUE_KEY_SCHEMA_EXTENSION {
        let _marker = reader.u32()?;
        let unique_count = decode_len(reader, "unique keys", crate::MAX_DECLARATIONS_PER_KIND)?;
        unique_keys.reserve(unique_count);
        for _ in 0..unique_count {
            let name = reader.string(256)?;
            let source_entity = decode_entity_id(reader)?;
            let index_id = decode_index_id(reader)?;
            let field_count = decode_len(reader, "unique key fields", 1_024)?;
            let mut fields = Vec::with_capacity(field_count);
            for _ in 0..field_count {
                fields.push(decode_field_id(reader)?);
            }
            unique_keys.push(crate::UniqueKeySchema::new(
                name,
                source_entity,
                index_id,
                fields,
            )?);
        }
    }
    let mut delete_policies = Vec::new();
    if reader.remaining() >= 4 && reader.peek_u32()? == DELETE_POLICY_SCHEMA_EXTENSION {
        let _marker = reader.u32()?;
        let policy_count = decode_len(reader, "delete policies", crate::MAX_DECLARATIONS_PER_KIND)?;
        delete_policies.reserve(policy_count);
        for _ in 0..policy_count {
            let target_entity = decode_entity_id(reader)?;
            let policy = match reader.u8()? {
                crate::format_registry::delete_policy_mode::NO_INBOUND => {
                    crate::DeletePolicySchemaV1::no_inbound(target_entity)
                }
                crate::format_registry::delete_policy_mode::RESTRICT => {
                    crate::DeletePolicySchemaV1::restrict(
                        target_entity,
                        decode_entity_id(reader)?,
                        decode_index_id(reader)?,
                    )
                }
                tag => {
                    return Err(IrValidationError::UnknownTag {
                        kind: "delete policy mode",
                        tag,
                    });
                }
            };
            delete_policies.push(policy);
        }
    }
    let mut vector_field_specs = Vec::new();
    if reader.remaining() >= 4 && reader.peek_u32()? == VECTOR_FIELD_SPEC_SCHEMA_EXTENSION {
        let _marker = reader.u32()?;
        let spec_count = decode_len(
            reader,
            "vector field specs",
            crate::MAX_DECLARATIONS_PER_KIND,
        )?;
        vector_field_specs.reserve(spec_count);
        for _ in 0..spec_count {
            let entity = decode_entity_id(reader)?;
            let field = decode_field_id(reader)?;
            let metric_tag = reader.u8()?;
            let metric = riffdb_types::DistanceMetric::from_tag(metric_tag).ok_or(
                IrValidationError::UnknownTag {
                    kind: "distance metric",
                    tag: metric_tag,
                },
            )?;
            let source_count = decode_len(
                reader,
                "vector spec source fields",
                crate::schema::MAX_VECTOR_SOURCE_FIELDS,
            )?;
            let mut source_fields = Vec::with_capacity(source_count);
            for _ in 0..source_count {
                source_fields.push(decode_field_id(reader)?);
            }
            let staleness_slo_secs = reader.u64()?;
            vector_field_specs.push(crate::VectorFieldSpecV1::new(
                entity,
                field,
                metric,
                source_fields,
                staleness_slo_secs,
            )?);
        }
    }
    let mut secret_field_specs = Vec::new();
    if reader.remaining() >= 4 && reader.peek_u32()? == SECRET_FIELD_SPEC_SCHEMA_EXTENSION {
        let _marker = reader.u32()?;
        let spec_count = decode_len(
            reader,
            "secret field specs",
            crate::MAX_DECLARATIONS_PER_KIND,
        )?;
        secret_field_specs.reserve(spec_count);
        for _ in 0..spec_count {
            let entity = decode_entity_id(reader)?;
            let field = decode_field_id(reader)?;
            secret_field_specs.push(crate::SecretFieldSpecV1::new(entity, field));
        }
    }
    SchemaIr::with_integrity_and_delete_policies(
        entities,
        events,
        enums,
        aggregates,
        relationships,
        unique_keys,
        delete_policies,
    )?
    .with_vector_field_specs(vector_field_specs)?
    .with_secret_field_specs(secret_field_specs)
}

fn decode_entity_schema(reader: &mut Reader<'_>) -> Result<EntitySchema, IrValidationError> {
    let id = decode_entity_id(reader)?;
    let name = reader.string(256)?;
    let record = decode_record_schema(reader)?;
    let primary_count = decode_len(reader, "primary-key fields", 1_024)?;
    let mut primary_key_fields = Vec::with_capacity(primary_count);
    for _ in 0..primary_count {
        primary_key_fields.push(decode_field_id(reader)?);
    }
    let primary_key = decode_key_schema(reader, 0)?;
    let invariant_count = decode_len(
        reader,
        "entity invariants",
        crate::MAX_DECLARATIONS_PER_KIND,
    )?;
    let mut invariants = Vec::with_capacity(invariant_count);
    for _ in 0..invariant_count {
        invariants.push(decode_invariant(reader)?);
    }
    let index_count = decode_len(reader, "entity indexes", crate::MAX_DECLARATIONS_PER_KIND)?;
    let mut indexes = Vec::with_capacity(index_count);
    for _ in 0..index_count {
        indexes.push(decode_index_schema(reader)?);
    }
    EntitySchema::new(
        id,
        name,
        record,
        primary_key_fields,
        primary_key,
        invariants,
        indexes,
    )
}

fn decode_event_schema(
    reader: &mut Reader<'_>,
    entities: &[EntitySchema],
) -> Result<EventSchema, IrValidationError> {
    let id = decode_event_id(reader)?;
    let name = reader.string(256)?;
    let payload = decode_record_schema(reader)?;
    let mut event =
        if reader.remaining() >= 8 && reader.peek_u64()? == EVENT_PARTITION_SCHEMA_EXTENSION {
            let _marker = reader.u64()?;
            let field_count = decode_len(reader, "event partition fields", 1_024)?;
            let mut fields = Vec::with_capacity(field_count);
            for _ in 0..field_count {
                fields.push(decode_field_id(reader)?);
            }
            let key_schema = decode_key_schema(reader, 0)?;
            let partition = crate::EventPartitionSchema::new(fields, key_schema, &payload)?;
            EventSchema::partitioned(id, name, payload, partition)?
        } else {
            EventSchema::new(id, name, payload)?
        };
    if reader.remaining() >= 8 && reader.peek_u64()? == EVENT_POLICY_ANCHOR_SCHEMA_EXTENSION {
        let _marker = reader.u64()?;
        let source_entity = decode_entity_id(reader)?;
        let read_policy = reader.string(256)?;
        let field_count = decode_len(reader, "event policy anchor fields", 1_024)?;
        let mut key_fields = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            key_fields.push(EventPolicyAnchorFieldV1::new(
                decode_field_id(reader)?,
                decode_field_id(reader)?,
            ));
        }
        let entity = entities
            .iter()
            .find(|entity| entity.id() == source_entity)
            .ok_or(IrValidationError::InvalidReference {
                kind: "event policy anchor entity",
            })?;
        let partition = event
            .partition()
            .ok_or(IrValidationError::InvalidReference {
                kind: "event policy anchor partition",
            })?;
        let anchor = EventPolicyAnchorV1::new(
            source_entity,
            key_fields,
            read_policy,
            entity,
            event.payload(),
            partition,
        )?;
        event = event.with_policy_anchor(anchor, entity)?;
    }
    Ok(event)
}

fn decode_enum_schema(reader: &mut Reader<'_>) -> Result<EnumSchema, IrValidationError> {
    let id = decode_enum_id(reader)?;
    let name = reader.string(256)?;
    let count = decode_len(reader, "enum variants", crate::MAX_DECLARATIONS_PER_KIND)?;
    let mut variants = Vec::with_capacity(count);
    for _ in 0..count {
        variants.push(EnumVariantSchema::new(
            decode_enum_variant_id(reader)?,
            reader.string(256)?,
        )?);
    }
    EnumSchema::new(id, name, variants)
}

fn decode_aggregate_schema(reader: &mut Reader<'_>) -> Result<AggregateSchema, IrValidationError> {
    let id = decode_aggregate_id(reader)?;
    let name = reader.string(256)?;
    let root = decode_entity_id(reader)?;
    let child_count = decode_len(
        reader,
        "aggregate children",
        crate::MAX_DECLARATIONS_PER_KIND,
    )?;
    let mut children = Vec::with_capacity(child_count);
    for _ in 0..child_count {
        children.push(decode_entity_id(reader)?);
    }
    let keys = decode_aggregate_keys(reader)?;
    let invariant_count = decode_len(
        reader,
        "aggregate invariants",
        crate::MAX_DECLARATIONS_PER_KIND,
    )?;
    let mut invariants = Vec::with_capacity(invariant_count);
    for _ in 0..invariant_count {
        invariants.push(decode_invariant(reader)?);
    }
    AggregateSchema::new(id, name, root, children, keys, invariants)
}

fn decode_aggregate_keys(reader: &mut Reader<'_>) -> Result<AggregateKeyPlan, IrValidationError> {
    let expressions = decode_expression_arena(reader)?;
    let partition_expression = ExprId::new(reader.u32()?);
    let count = decode_len(reader, "aggregate conflict expressions", 1_024)?;
    let mut conflict_expressions = Vec::with_capacity(count);
    for _ in 0..count {
        conflict_expressions.push(ExprId::new(reader.u32()?));
    }
    AggregateKeyPlan::new(
        expressions,
        partition_expression,
        conflict_expressions,
        decode_key_schema(reader, 0)?,
        decode_key_schema(reader, 0)?,
    )
}

fn decode_invariant(reader: &mut Reader<'_>) -> Result<InvariantPlan, IrValidationError> {
    let id = decode_invariant_id(reader)?;
    let name = reader.string(256)?;
    let expressions = decode_expression_arena(reader)?;
    let predicate = ExprId::new(reader.u32()?);
    InvariantPlan::new(id, name, expressions, predicate)
}

fn decode_index_schema(reader: &mut Reader<'_>) -> Result<IndexSchema, IrValidationError> {
    let id = decode_index_id(reader)?;
    let name = reader.string(256)?;
    let count = decode_len(reader, "index fields", 1_024)?;
    let mut fields = Vec::with_capacity(count);
    for _ in 0..count {
        fields.push(decode_field_id(reader)?);
    }
    let key_schema = decode_key_schema(reader, 0)?;
    let encodings =
        if reader.remaining() >= 4 && reader.peek_u32()? == INDEX_FIELD_ENCODING_EXTENSION {
            let _extension = reader.u32()?;
            let encoding_count = decode_len(reader, "index field encodings", 1_024)?;
            if encoding_count != fields.len() {
                return Err(IrValidationError::InvalidKey {
                    reason: "index field encoding arity mismatch",
                });
            }
            (0..encoding_count)
                .map(|_| match reader.u8()? {
                    0 => Ok(IndexFieldEncodingV1::Canonical),
                    1 => Ok(IndexFieldEncodingV1::Presence),
                    2 => Ok(IndexFieldEncodingV1::TextKey(TextKeyProfileV1::BinaryUtf8)),
                    3 => Ok(IndexFieldEncodingV1::TextKey(TextKeyProfileV1::UnicodeFold)),
                    tag => Err(IrValidationError::UnknownTag {
                        kind: "index field encoding",
                        tag,
                    }),
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            vec![IndexFieldEncodingV1::Canonical; fields.len()]
        };
    IndexSchema::with_encodings(id, name, fields, encodings, key_schema)
}

fn decode_record_schema(reader: &mut Reader<'_>) -> Result<RecordSchema, IrValidationError> {
    let owner = decode_record_ref(reader)?;
    let count = decode_len(reader, "record fields", crate::MAX_DECLARATIONS_PER_KIND)?;
    let mut fields = Vec::with_capacity(count);
    for _ in 0..count {
        fields.push(FieldSchema::new(
            decode_field_id(reader)?,
            reader.string(256)?,
            decode_value_type(reader, 0)?,
        )?);
    }
    RecordSchema::new(owner, fields)
}

fn decode_record_ref(reader: &mut Reader<'_>) -> Result<RecordTypeRef, IrValidationError> {
    match reader.u8()? {
        record_tag::ENTITY => Ok(RecordTypeRef::Entity(decode_entity_id(reader)?)),
        record_tag::EVENT => Ok(RecordTypeRef::Event(decode_event_id(reader)?)),
        record_tag::COMMAND_INPUT => Ok(RecordTypeRef::CommandInput(decode_command_id(reader)?)),
        record_tag::COMMAND_OUTCOME => Ok(RecordTypeRef::CommandOutcome {
            command_id: decode_command_id(reader)?,
            outcome_id: decode_outcome_id(reader)?,
        }),
        record_tag::PROJECTION_RESULT => Ok(RecordTypeRef::ProjectionResult(decode_projection_id(
            reader,
        )?)),
        tag => Err(IrValidationError::UnknownTag {
            kind: "record reference",
            tag,
        }),
    }
}

pub(crate) fn decode_value_type(
    reader: &mut Reader<'_>,
    depth: usize,
) -> Result<ValueType, IrValidationError> {
    if depth >= 32 {
        return Err(IrValidationError::LimitExceeded {
            kind: "type nesting",
            actual: depth + 1,
            maximum: 32,
        });
    }
    match reader.u8()? {
        value_type_tag::BOOL => Ok(ValueType::bool()),
        value_type_tag::I64 => Ok(ValueType::i64()),
        value_type_tag::U64 => Ok(ValueType::u64()),
        value_type_tag::DECIMAL => Ok(ValueType::decimal(
            DecimalSpec::new(reader.u8()?, reader.u8()?).map_err(|_| {
                IrValidationError::TypeMismatch {
                    context: "decimal type",
                }
            })?,
        )),
        value_type_tag::MONEY => Ok(ValueType::money(
            CurrencyCode::new(reader.array::<3>()?).map_err(|_| {
                IrValidationError::TypeMismatch {
                    context: "money type",
                }
            })?,
        )),
        value_type_tag::STRING => ValueType::string(reader.u32()? as usize),
        value_type_tag::BYTES => ValueType::bytes(reader.u32()? as usize),
        value_type_tag::TIMESTAMP => Ok(ValueType::timestamp()),
        value_type_tag::DATE => Ok(ValueType::date()),
        value_type_tag::UUID => Ok(ValueType::uuid()),
        value_type_tag::ENUM => Ok(ValueType::enumeration(decode_enum_id(reader)?)),
        value_type_tag::OPTIONAL => ValueType::optional(decode_value_type(reader, depth + 1)?),
        value_type_tag::LIST => {
            let element = decode_value_type(reader, depth + 1)?;
            ValueType::list(element, reader.u32()? as usize)
        }
        value_type_tag::RECORD => Ok(ValueType::record(decode_record_ref(reader)?)),
        value_type_tag::VECTOR => {
            let dimension = riffdb_types::VectorDimension::new(reader.u32()?).ok_or(
                IrValidationError::TypeMismatch {
                    context: "vector type",
                },
            )?;
            Ok(ValueType::vector(dimension))
        }
        tag => Err(IrValidationError::UnknownTag {
            kind: "value type",
            tag,
        }),
    }
}

fn decode_key_schema(
    reader: &mut Reader<'_>,
    depth: usize,
) -> Result<KeySchema, IrValidationError> {
    if depth > 1 {
        return Err(IrValidationError::InvalidKey {
            reason: "key schema nesting exceeds the index/entity shape",
        });
    }
    let codec_version = reader.u32()?;
    if !matches!(
        codec_version,
        crate::KEY_CODEC_VERSION_V1 | crate::KEY_CODEC_VERSION_V2
    ) {
        return Err(IrValidationError::UnsupportedVersion {
            kind: "key codec",
            value: codec_version,
        });
    }
    let purpose = match reader.u8()? {
        key_purpose_tag::ENTITY => KeyPurpose::Entity(decode_entity_id(reader)?),
        key_purpose_tag::PARTITION => KeyPurpose::Partition(decode_aggregate_id(reader)?),
        key_purpose_tag::CONFLICT => KeyPurpose::Conflict(decode_aggregate_id(reader)?),
        key_purpose_tag::INDEX => KeyPurpose::Index {
            index_id: decode_index_id(reader)?,
            entity_type: decode_entity_id(reader)?,
        },
        tag => {
            return Err(IrValidationError::UnknownTag {
                kind: "key purpose",
                tag,
            });
        }
    };
    let count = decode_len(reader, "key components", 1_024)?;
    let mut components = Vec::with_capacity(count);
    for _ in 0..count {
        let value_type = decode_value_type(reader, 0)?;
        let codec = if codec_version >= crate::KEY_CODEC_VERSION_V2 {
            match reader.u8()? {
                0 => KeyComponentCodecV1::Canonical,
                1 => KeyComponentCodecV1::OrderedBytes,
                tag => {
                    return Err(IrValidationError::UnknownTag {
                        kind: "key component codec",
                        tag,
                    });
                }
            }
        } else {
            KeyComponentCodecV1::Canonical
        };
        let variant_count = decode_len(
            reader,
            "key enum variants",
            crate::MAX_DECLARATIONS_PER_KIND,
        )?;
        let mut variants = Vec::with_capacity(variant_count);
        for _ in 0..variant_count {
            variants.push(decode_enum_variant_id(reader)?);
        }
        let stored_maximum = reader.u32()? as usize;
        let component = match codec {
            KeyComponentCodecV1::Canonical => KeyComponentSchema::new(value_type, variants)?,
            KeyComponentCodecV1::OrderedBytes => {
                if !variants.is_empty() {
                    return Err(IrValidationError::InvalidKey {
                        reason: "ordered bytes component has enum variants",
                    });
                }
                KeyComponentSchema::ordered_bytes(value_type.byte_bound().ok_or(
                    IrValidationError::InvalidKey {
                        reason: "ordered bytes component has no byte bound",
                    },
                )?)?
            }
        };
        if component.maximum_payload_bytes() != stored_maximum {
            return Err(IrValidationError::HashMismatch {
                kind: "key component maximum",
            });
        }
        components.push(component);
    }
    let stored_maximum = reader.u32()? as usize;
    let embedded = if reader.bool()? {
        Some(decode_key_schema(reader, depth + 1)?)
    } else {
        None
    };
    let schema = match purpose {
        KeyPurpose::Index {
            index_id,
            entity_type,
        } => KeySchema::index(
            index_id,
            entity_type,
            components,
            embedded.ok_or(IrValidationError::InvalidKey {
                reason: "index key schema omits its entity key",
            })?,
        )?,
        _ if embedded.is_none() => KeySchema::new(purpose, components)?,
        _ => {
            return Err(IrValidationError::InvalidKey {
                reason: "non-index key schema embeds another schema",
            });
        }
    };
    if schema.maximum_encoded_bytes() != stored_maximum {
        return Err(IrValidationError::HashMismatch {
            kind: "key schema maximum",
        });
    }
    Ok(schema)
}

pub(crate) fn decode_expression_arena(
    reader: &mut Reader<'_>,
) -> Result<ExpressionArena, IrValidationError> {
    decode_expression_arena_versioned(reader, EXECUTABLE_IR_VERSION_V1)
}

fn decode_expression_arena_versioned(
    reader: &mut Reader<'_>,
    ir_version: u32,
) -> Result<ExpressionArena, IrValidationError> {
    let count =
        decode_len_with_minimum(reader, "expression arena", crate::MAX_EXPRESSION_NODES, 2)?;
    let mut nodes = Vec::with_capacity(count);
    for _ in 0..count {
        let tag = reader.u8()?;
        let result_type = decode_value_type(reader, 0)?;
        let kind = match tag {
            expression_tag::CONSTANT => ExpressionKind::Constant({
                let bytes = reader.bytes(1024 * 1024)?;
                preflight_ir_canonical_value(bytes)?;
                decode_canonical_value(bytes).map_err(|_| IrValidationError::TypeMismatch {
                    context: "expression constant",
                })?
            }),
            expression_tag::INPUT_FIELD => ExpressionKind::InputField(decode_field_id(reader)?),
            expression_tag::SERVICE_VALUE if ir_version >= EXECUTABLE_IR_VERSION_V2 => {
                ExpressionKind::ServiceValue(decode_field_id(reader)?)
            }
            expression_tag::COLLECTION_ELEMENT if ir_version >= EXECUTABLE_IR_VERSION_V5 => {
                ExpressionKind::CollectionElement
            }
            expression_tag::COLLECTION_ELEMENT_FIELD if ir_version >= EXECUTABLE_IR_VERSION_V5 => {
                ExpressionKind::CollectionElementField(decode_field_id(reader)?)
            }
            expression_tag::COMPLETE_BINDING => {
                ExpressionKind::CompleteBinding(BindingId::new(reader.u32()?))
            }
            expression_tag::BOUND_FIELD => ExpressionKind::BoundField {
                binding: BindingId::new(reader.u32()?),
                field: decode_field_id(reader)?,
            },
            expression_tag::SCHEMA_FIELD => ExpressionKind::SchemaField {
                entity_type: decode_entity_id(reader)?,
                field: decode_field_id(reader)?,
            },
            expression_tag::SOURCE_EVENT_FIELD => {
                ExpressionKind::SourceEventField(decode_field_id(reader)?)
            }
            expression_tag::TRANSACTION_TIME => ExpressionKind::TransactionTime,
            expression_tag::TRANSACTION_DATE => ExpressionKind::TransactionDate,
            expression_tag::UNARY => ExpressionKind::Unary {
                operator: decode_unary_operator(reader.u8()?)?,
                operand: ExprId::new(reader.u32()?),
            },
            expression_tag::BINARY => ExpressionKind::Binary {
                operator: decode_binary_operator(reader.u8()?)?,
                left: ExprId::new(reader.u32()?),
                right: ExprId::new(reader.u32()?),
            },
            expression_tag::ROOT_VALIDATION_FIELD => ExpressionKind::RootValidationField {
                read: crate::RootValidationReadId::new(reader.u32()?),
                field: decode_field_id(reader)?,
            },
            tag => {
                return Err(IrValidationError::UnknownTag {
                    kind: "expression",
                    tag,
                });
            }
        };
        nodes.push((kind, result_type));
    }
    ExpressionArena::new(nodes)
}

fn preflight_ir_canonical_value(bytes: &[u8]) -> Result<(), IrValidationError> {
    struct Preflight<'a> {
        bytes: &'a [u8],
        position: usize,
    }

    impl Preflight<'_> {
        fn read(&mut self, length: usize) -> Result<&[u8], IrValidationError> {
            let end = self
                .position
                .checked_add(length)
                .ok_or(IrValidationError::UnexpectedEnd)?;
            let value = self
                .bytes
                .get(self.position..end)
                .ok_or(IrValidationError::UnexpectedEnd)?;
            self.position = end;
            Ok(value)
        }

        fn u8(&mut self) -> Result<u8, IrValidationError> {
            Ok(self.read(1)?[0])
        }

        fn u32(&mut self) -> Result<u32, IrValidationError> {
            Ok(u32::from_be_bytes(
                self.read(4)?
                    .try_into()
                    .map_err(|_| IrValidationError::UnexpectedEnd)?,
            ))
        }

        fn value(&mut self, depth: usize) -> Result<(), IrValidationError> {
            if depth > riffdb_types::MAX_NESTING_DEPTH {
                return Err(IrValidationError::LimitExceeded {
                    kind: "IR constant nesting",
                    actual: depth,
                    maximum: riffdb_types::MAX_NESTING_DEPTH,
                });
            }
            let version = self.u8()?;
            if version != riffdb_types::CANONICAL_VALUE_VERSION {
                return Err(IrValidationError::UnsupportedVersion {
                    kind: "canonical value",
                    value: u32::from(version),
                });
            }
            // These immutable tags are owned by accepted ADR-0011/riffdb-types.
            match self.u8()? {
                0x00 => {}
                0x01 => {
                    self.read(1)?;
                }
                0x02 | 0x03 => {
                    self.read(8)?;
                }
                0x04 => {
                    self.read(18)?;
                }
                0x05 => {
                    self.read(21)?;
                }
                0x06 | 0x07 => {
                    let length = self.u32()? as usize;
                    self.read(length)?;
                }
                0x08 => {
                    self.read(12)?;
                }
                0x09 => {
                    self.read(4)?;
                }
                0x0a => {
                    self.read(16)?;
                }
                0x0b => {
                    self.read(8)?;
                }
                0x0c => {
                    let count = self.u32()? as usize;
                    checked_len("IR constant list entries", count, crate::MAX_OBJECT_FIELDS)?;
                    for _ in 0..count {
                        self.value(depth + 1)?;
                    }
                }
                0x0d => {
                    let count = self.u32()? as usize;
                    checked_len("IR constant record fields", count, crate::MAX_OBJECT_FIELDS)?;
                    for _ in 0..count {
                        self.read(4)?;
                        self.value(depth + 1)?;
                    }
                }
                tag => {
                    return Err(IrValidationError::UnknownTag {
                        kind: "canonical value",
                        tag,
                    });
                }
            }
            Ok(())
        }
    }

    let mut preflight = Preflight { bytes, position: 0 };
    preflight.value(0)?;
    if preflight.position != bytes.len() {
        return Err(IrValidationError::TrailingBytes);
    }
    Ok(())
}

fn decode_unary_operator(tag: u8) -> Result<UnaryOperator, IrValidationError> {
    match tag {
        unary_tag::NOT => Ok(UnaryOperator::Not),
        unary_tag::NEGATE => Ok(UnaryOperator::Negate),
        tag => Err(IrValidationError::UnknownTag {
            kind: "unary operator",
            tag,
        }),
    }
}

fn decode_binary_operator(tag: u8) -> Result<BinaryOperator, IrValidationError> {
    match tag {
        binary_tag::MULTIPLY => Ok(BinaryOperator::Multiply),
        binary_tag::DIVIDE => Ok(BinaryOperator::Divide),
        binary_tag::ADD => Ok(BinaryOperator::Add),
        binary_tag::SUBTRACT => Ok(BinaryOperator::Subtract),
        binary_tag::EQUAL => Ok(BinaryOperator::Equal),
        binary_tag::NOT_EQUAL => Ok(BinaryOperator::NotEqual),
        binary_tag::LESS => Ok(BinaryOperator::Less),
        binary_tag::LESS_EQUAL => Ok(BinaryOperator::LessEqual),
        binary_tag::GREATER => Ok(BinaryOperator::Greater),
        binary_tag::GREATER_EQUAL => Ok(BinaryOperator::GreaterEqual),
        binary_tag::AND => Ok(BinaryOperator::And),
        binary_tag::OR => Ok(BinaryOperator::Or),
        tag => Err(IrValidationError::UnknownTag {
            kind: "binary operator",
            tag,
        }),
    }
}

fn decode_field_expressions(
    reader: &mut Reader<'_>,
) -> Result<Vec<FieldExpression>, IrValidationError> {
    let count = decode_len(reader, "object fields", crate::MAX_OBJECT_FIELDS)?;
    let mut fields = Vec::with_capacity(count);
    for _ in 0..count {
        fields.push(FieldExpression::new(
            decode_field_id(reader)?,
            ExprId::new(reader.u32()?),
        ));
    }
    Ok(fields)
}

fn decode_outcome_construction(
    reader: &mut Reader<'_>,
    outcomes: &[OutcomeSchema],
    arena: &ExpressionArena,
) -> Result<OutcomeConstruction, IrValidationError> {
    let outcome_id = decode_outcome_id(reader)?;
    let encoded_owner = decode_record_ref(reader)?;
    let fields = decode_field_expressions(reader)?;
    let outcome = outcomes
        .iter()
        .find(|outcome| outcome.id() == outcome_id)
        .ok_or(IrValidationError::InvalidReference {
            kind: "outcome construction",
        })?;
    if &encoded_owner != outcome.payload().owner() {
        return Err(IrValidationError::InvalidReference {
            kind: "outcome construction owner",
        });
    }
    OutcomeConstruction::new(outcome, fields, arena)
}

fn decode_event_construction(
    reader: &mut Reader<'_>,
    schema: &SchemaIr,
    arena: &ExpressionArena,
) -> Result<EventConstruction, IrValidationError> {
    let event_type = decode_event_id(reader)?;
    let encoded_owner = decode_record_ref(reader)?;
    let fields = decode_field_expressions(reader)?;
    let expected = schema
        .event(event_type)
        .ok_or(IrValidationError::InvalidReference {
            kind: "event construction",
        })?;
    if &encoded_owner != expected.payload().owner() {
        return Err(IrValidationError::InvalidReference {
            kind: "event construction owner",
        });
    }
    EventConstruction::new(event_type, fields, schema, arena)
}

fn decode_commands(
    reader: &mut Reader<'_>,
    lineage: &ContractLineage,
    schema: &SchemaIr,
    ir_version: u32,
) -> Result<Vec<CommandPlan>, IrValidationError> {
    let count = decode_len_with_minimum(reader, "commands", crate::MAX_DECLARATIONS_PER_KIND, 48)?;
    let mut commands = Vec::with_capacity(count);
    for _ in 0..count {
        commands.push(decode_command_versioned(
            reader, lineage, schema, ir_version,
        )?);
    }
    Ok(commands)
}

#[cfg(test)]
fn decode_command(
    reader: &mut Reader<'_>,
    lineage: &ContractLineage,
    schema: &SchemaIr,
) -> Result<CommandPlan, IrValidationError> {
    decode_command_versioned(reader, lineage, schema, EXECUTABLE_IR_VERSION_V1)
}

fn decode_command_versioned(
    reader: &mut Reader<'_>,
    lineage: &ContractLineage,
    schema: &SchemaIr,
    ir_version: u32,
) -> Result<CommandPlan, IrValidationError> {
    let command_id = decode_command_id(reader)?;
    let name = reader.string(256)?;
    let contract_version =
        ContractVersion::new(reader.u64()?).ok_or(IrValidationError::InvalidReference {
            kind: "command contract version",
        })?;
    let stored_plan_hash = PlanHash::from_bytes(reader.array()?);
    let input = CommandInputSchema::new(command_id, decode_record_schema(reader)?)?;
    let service_values = if ir_version >= EXECUTABLE_IR_VERSION_V2 {
        let count = decode_len_with_minimum(
            reader,
            "command service values",
            crate::MAX_COMMAND_ITEMS,
            4,
        )?;
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            let field = FieldSchema::new(
                decode_field_id(reader)?,
                reader.string(256)?,
                decode_value_type(reader, 0)?,
            )?;
            let kind = match reader.u8()? {
                crate::format_registry::service_value_kind::UUID_V7 => {
                    crate::ServiceValueKind::UuidV7
                }
                crate::format_registry::service_value_kind::TRANSACTION_TIME => {
                    crate::ServiceValueKind::TransactionTime
                }
                tag => {
                    return Err(IrValidationError::UnknownTag {
                        kind: "service-owned command value",
                        tag,
                    });
                }
            };
            values.push(crate::ServiceValueSchema::new(field, kind)?);
        }
        values
    } else {
        Vec::new()
    };
    let outcome_count = decode_len(reader, "command outcomes", crate::MAX_COMMAND_ITEMS)?;
    let mut outcomes = Vec::with_capacity(outcome_count);
    for _ in 0..outcome_count {
        outcomes.push(decode_outcome_schema(reader, command_id)?);
    }
    let success_outcome = decode_outcome_id(reader)?;
    let idempotency_input = if reader.bool()? {
        Some(decode_field_id(reader)?)
    } else {
        None
    };
    let stored_input_hash = SchemaHash::from_bytes(reader.array()?);
    let stored_output_hash = SchemaHash::from_bytes(reader.array()?);
    if GeneratedSchemaArtifact::command_input(
        command_id,
        input.record(),
        schema,
        idempotency_input,
    )?
    .hash()
        != stored_input_hash
        || GeneratedSchemaArtifact::command_outcomes(command_id, &outcomes, schema)?.hash()
            != stored_output_hash
    {
        return Err(IrValidationError::HashMismatch {
            kind: "command generated schema",
        });
    }
    let collection_expansion = if ir_version >= EXECUTABLE_IR_VERSION_V5 && reader.bool()? {
        Some(decode_collection_expansion(reader)?)
    } else {
        None
    };
    let expressions = decode_expression_arena_versioned(reader, ir_version)?;
    let binding_count = decode_len(reader, "command bindings", crate::MAX_COMMAND_ITEMS)?;
    let mut bindings = Vec::with_capacity(binding_count);
    for _ in 0..binding_count {
        bindings.push(decode_binding(reader, &outcomes, &expressions, ir_version)?);
    }
    let root_read_count = decode_len(
        reader,
        "command root-validation reads",
        crate::MAX_COMMAND_ITEMS,
    )?;
    let mut root_validation_reads = Vec::with_capacity(root_read_count);
    for _ in 0..root_read_count {
        let id = crate::RootValidationReadId::new(reader.u32()?);
        let source_binding = BindingId::new(reader.u32()?);
        let entity_type = decode_entity_id(reader)?;
        let key_schema = decode_key_schema(reader, 0)?;
        let key_count = decode_len(
            reader,
            "root-validation key expressions",
            crate::MAX_COMMAND_ITEMS,
        )?;
        let mut key_expressions = Vec::with_capacity(key_count);
        for _ in 0..key_count {
            key_expressions.push(ExprId::new(reader.u32()?));
        }
        let field_count = decode_len(
            reader,
            "root-validation accessed fields",
            crate::MAX_COMMAND_ITEMS,
        )?;
        let mut accessed_fields = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            accessed_fields.push(decode_field_id(reader)?);
        }
        root_validation_reads.push(crate::RootValidationReadPlan::new(
            id,
            source_binding,
            entity_type,
            key_schema,
            key_expressions,
            accessed_fields,
        )?);
    }
    let mut encoded_relationship_checks = Vec::new();
    if !schema.relationships().is_empty() {
        let count = decode_len(
            reader,
            "command relationship checks",
            crate::MAX_COMMAND_ITEMS,
        )?;
        encoded_relationship_checks.reserve(count);
        for _ in 0..count {
            encoded_relationship_checks.push((
                reader.string(256)?,
                BindingId::new(reader.u32()?),
                BindingId::new(reader.u32()?),
            ));
        }
    }
    let mut encoded_delete_checks = Vec::new();
    if ir_version >= EXECUTABLE_IR_VERSION_V5 {
        let count = decode_len(reader, "command delete checks", crate::MAX_COMMAND_ITEMS)?;
        encoded_delete_checks.reserve(count);
        for _ in 0..count {
            let binding = BindingId::new(reader.u32()?);
            let mode = match reader.u8()? {
                crate::format_registry::delete_check_mode::NO_INBOUND => {
                    crate::DeleteCheckModeV1::NoInbound
                }
                crate::format_registry::delete_check_mode::RESTRICT => {
                    crate::DeleteCheckModeV1::Restrict {
                        source_entity: decode_entity_id(reader)?,
                        index_id: decode_index_id(reader)?,
                    }
                }
                tag => {
                    return Err(IrValidationError::UnknownTag {
                        kind: "delete check mode",
                        tag,
                    });
                }
            };
            encoded_delete_checks.push((binding, mode));
        }
    }
    let locality = decode_locality(reader)?;
    let check_count = decode_len(reader, "commit checks", crate::MAX_COMMAND_ITEMS)?;
    let mut commit_checks = Vec::with_capacity(check_count);
    for _ in 0..check_count {
        let invariant_id = decode_invariant_id(reader)?;
        let predicate = ExprId::new(reader.u32()?);
        let binding_count = decode_len(
            reader,
            "commit-check source bindings",
            crate::MAX_COMMAND_ITEMS,
        )?;
        let mut check_bindings = Vec::with_capacity(binding_count);
        for _ in 0..binding_count {
            check_bindings.push(BindingId::new(reader.u32()?));
        }
        let root_read_count = decode_len(
            reader,
            "commit-check root-validation reads",
            crate::MAX_COMMAND_ITEMS,
        )?;
        let mut check_root_reads = Vec::with_capacity(root_read_count);
        for _ in 0..root_read_count {
            check_root_reads.push(crate::RootValidationReadId::new(reader.u32()?));
        }
        commit_checks.push(crate::CommitCheckPlan::new(
            invariant_id,
            predicate,
            check_bindings,
            check_root_reads,
        )?);
    }
    let instruction_count = decode_len(reader, "command instructions", crate::MAX_COMMAND_ITEMS)?;
    let mut instructions = Vec::with_capacity(instruction_count);
    for _ in 0..instruction_count {
        instructions.push(decode_instruction_versioned(
            reader,
            &outcomes,
            schema,
            &expressions,
            ir_version,
        )?);
    }
    let invocation_class = if ir_version >= EXECUTABLE_IR_VERSION_V10 {
        decode_command_invocation_class(reader.u8()?)?
    } else {
        crate::CommandInvocationClass::Application
    };
    let execution_class = decode_execution_class(reader.u8()?)?;
    let _retry_policy = decode_retry_policy(reader.u8()?)?;
    if decode_capability_requirement(reader)?
        != (CapabilityRequirement::InvokeCommand {
            lineage: lineage.clone(),
            command_id,
        })
    {
        return Err(IrValidationError::InvalidReference {
            kind: "command capability requirement",
        });
    }

    decode_command_schema_closure(
        reader,
        schema,
        &bindings,
        &root_validation_reads,
        locality.aggregate_id(),
        &instructions,
    )?;
    let plan = match (invocation_class, collection_expansion) {
        (crate::CommandInvocationClass::Reimport, Some(expansion)) => {
            CommandPlan::new_reimport_collection(
                command_id,
                lineage.clone(),
                name,
                contract_version,
                input,
                outcomes,
                success_outcome,
                expressions,
                bindings,
                root_validation_reads,
                locality,
                commit_checks,
                instructions,
                expansion,
                schema,
            )?
        }
        (crate::CommandInvocationClass::Reimport, None) => {
            return Err(IrValidationError::InvalidDependency {
                reason: "reimport command requires one bounded collection expansion",
            });
        }
        (crate::CommandInvocationClass::Application, Some(expansion)) => {
            CommandPlan::new_collection(
                command_id,
                lineage.clone(),
                name,
                contract_version,
                input,
                service_values,
                outcomes,
                success_outcome,
                idempotency_input,
                expressions,
                bindings,
                root_validation_reads,
                locality,
                commit_checks,
                instructions,
                expansion,
                execution_class,
                schema,
            )?
        }
        (crate::CommandInvocationClass::Application, None) => CommandPlan::new_with_service_values(
            command_id,
            lineage.clone(),
            name,
            contract_version,
            input,
            service_values,
            outcomes,
            success_outcome,
            idempotency_input,
            expressions,
            bindings,
            root_validation_reads,
            locality,
            commit_checks,
            instructions,
            execution_class,
            schema,
        )?,
    };
    if encoded_relationship_checks
        != plan
            .relationship_checks()
            .iter()
            .map(|check| {
                (
                    check.relationship_name().to_owned(),
                    check.source_binding(),
                    check.target_binding(),
                )
            })
            .collect::<Vec<_>>()
    {
        return Err(IrValidationError::InvalidDependency {
            reason: "relationship proof does not match the derived command plan",
        });
    }
    if encoded_delete_checks
        != plan
            .delete_checks()
            .iter()
            .map(|check| (check.binding(), check.mode()))
            .collect::<Vec<_>>()
    {
        return Err(IrValidationError::InvalidDependency {
            reason: "delete proof does not match the derived command plan",
        });
    }
    if plan.plan_hash() != stored_plan_hash {
        return Err(IrValidationError::HashMismatch {
            kind: "command plan",
        });
    }
    Ok(plan)
}

fn decode_command_invocation_class(
    tag: u8,
) -> Result<crate::CommandInvocationClass, IrValidationError> {
    match tag {
        crate::format_registry::command_invocation_class::APPLICATION => {
            Ok(crate::CommandInvocationClass::Application)
        }
        crate::format_registry::command_invocation_class::REIMPORT => {
            Ok(crate::CommandInvocationClass::Reimport)
        }
        tag => Err(IrValidationError::UnknownTag {
            kind: "command invocation class",
            tag,
        }),
    }
}

fn decode_execution_class(tag: u8) -> Result<ExecutionClass, IrValidationError> {
    match tag {
        execution_tag::READ_ONLY => Ok(ExecutionClass::ReadOnly),
        execution_tag::IDEMPOTENT_MUTATION => Ok(ExecutionClass::IdempotentMutation),
        tag => Err(IrValidationError::UnknownTag {
            kind: "execution class",
            tag,
        }),
    }
}

fn decode_retry_policy(tag: u8) -> Result<RetryPolicy, IrValidationError> {
    match tag {
        retry_tag::BOUNDED_FULL_REEVALUATION => Ok(RetryPolicy::BoundedFullReevaluation),
        tag => Err(IrValidationError::UnknownTag {
            kind: "retry policy",
            tag,
        }),
    }
}

fn decode_capability_requirement(
    reader: &mut Reader<'_>,
) -> Result<CapabilityRequirement, IrValidationError> {
    match reader.u8()? {
        capability_tag::INVOKE_COMMAND => {
            let lineage = ContractLineage::new(reader.string(256)?).map_err(|_| {
                IrValidationError::InvalidText {
                    kind: "capability contract lineage",
                }
            })?;
            Ok(CapabilityRequirement::InvokeCommand {
                lineage,
                command_id: decode_command_id(reader)?,
            })
        }
        tag => Err(IrValidationError::UnknownTag {
            kind: "capability requirement",
            tag,
        }),
    }
}

fn decode_outcome_schema(
    reader: &mut Reader<'_>,
    command_id: CommandId,
) -> Result<OutcomeSchema, IrValidationError> {
    let outcome_id = decode_outcome_id(reader)?;
    OutcomeSchema::new(
        command_id,
        outcome_id,
        reader.string(256)?,
        decode_record_schema(reader)?,
    )
}

fn decode_binding(
    reader: &mut Reader<'_>,
    outcomes: &[OutcomeSchema],
    arena: &ExpressionArena,
    ir_version: u32,
) -> Result<BindingPlan, IrValidationError> {
    let id = BindingId::new(reader.u32()?);
    let name = reader.string(256)?;
    let mode = decode_binding_mode(reader.u8()?, ir_version)?;
    let entity_type = decode_entity_id(reader)?;
    let key_schema = decode_key_schema(reader, 0)?;
    let key_count = decode_len(reader, "binding key expressions", 1_024)?;
    let mut key_expressions = Vec::with_capacity(key_count);
    for _ in 0..key_count {
        key_expressions.push(ExprId::new(reader.u32()?));
    }
    let field_count = decode_len(reader, "binding accessed fields", crate::MAX_COMMAND_ITEMS)?;
    let mut accessed_fields = Vec::with_capacity(field_count);
    for _ in 0..field_count {
        accessed_fields.push(decode_field_id(reader)?);
    }
    let complete_record_access = reader.bool()?;
    let failure = decode_outcome_construction(reader, outcomes, arena)?;
    let restriction_failure = if ir_version >= EXECUTABLE_IR_VERSION_V6 && reader.bool()? {
        Some(decode_outcome_construction(reader, outcomes, arena)?)
    } else {
        None
    };
    BindingPlan::new_with_restriction_failure(
        id,
        name,
        mode,
        entity_type,
        key_schema,
        key_expressions,
        accessed_fields,
        complete_record_access,
        failure,
        restriction_failure,
    )
}

fn decode_binding_mode(tag: u8, ir_version: u32) -> Result<BindingMode, IrValidationError> {
    match tag {
        binding_tag::READ => Ok(BindingMode::Read),
        binding_tag::MUTATE => Ok(BindingMode::Mutate),
        binding_tag::CREATE => Ok(BindingMode::Create),
        binding_tag::DELETE if ir_version >= EXECUTABLE_IR_VERSION_V5 => Ok(BindingMode::Delete),
        tag => Err(IrValidationError::UnknownTag {
            kind: "binding mode",
            tag,
        }),
    }
}

fn decode_collection_expansion(
    reader: &mut Reader<'_>,
) -> Result<CollectionExpansionPlanV1, IrValidationError> {
    let input_field = decode_field_id(reader)?;
    let minimum_elements = decode_len(
        reader,
        "collection minimum elements",
        crate::MAX_COLLECTION_COMMAND_ELEMENTS_V1,
    )?;
    let maximum_elements = decode_len(
        reader,
        "collection maximum elements",
        crate::MAX_COLLECTION_COMMAND_ELEMENTS_V1,
    )?;
    let element_type = decode_value_type(reader, 0)?;
    let first_binding = BindingId::new(reader.u32()?);
    let binding_count = decode_len(
        reader,
        "collection binding templates",
        crate::MAX_COMMAND_ITEMS,
    )?;
    let first_instruction = reader.u32()?;
    let instruction_count = decode_len(
        reader,
        "collection instruction templates",
        crate::MAX_COMMAND_ITEMS,
    )?;
    let duplicate_policy = match reader.u8()? {
        0x01 => CollectionDuplicatePolicyV1::Reject,
        tag => {
            return Err(IrValidationError::UnknownTag {
                kind: "collection duplicate policy",
                tag,
            });
        }
    };
    CollectionExpansionPlanV1::new(
        input_field,
        minimum_elements,
        maximum_elements,
        element_type,
        first_binding,
        binding_count,
        first_instruction,
        instruction_count,
        duplicate_policy,
    )
}

fn decode_locality(reader: &mut Reader<'_>) -> Result<LocalityPlan, IrValidationError> {
    let aggregate_id = decode_aggregate_id(reader)?;
    let partition_schema = decode_key_schema(reader, 0)?;
    let partition_expression = ExprId::new(reader.u32()?);
    let count = decode_len(
        reader,
        "command conflict derivations",
        MAX_COMMAND_CONFLICT_KEYS_V1,
    )?;
    let mut conflicts = Vec::with_capacity(count);
    for _ in 0..count {
        let key_schema = decode_key_schema(reader, 0)?;
        let expression_count = decode_len(reader, "conflict expressions", 1_024)?;
        let mut expressions = Vec::with_capacity(expression_count);
        for _ in 0..expression_count {
            expressions.push(ExprId::new(reader.u32()?));
        }
        conflicts.push(ConflictDerivationPlan::new(key_schema, expressions)?);
    }
    LocalityPlan::new(
        aggregate_id,
        partition_schema,
        partition_expression,
        conflicts,
    )
}

#[cfg(test)]
fn decode_instruction(
    reader: &mut Reader<'_>,
    outcomes: &[OutcomeSchema],
    schema: &SchemaIr,
    arena: &ExpressionArena,
) -> Result<Instruction, IrValidationError> {
    decode_instruction_versioned(reader, outcomes, schema, arena, EXECUTABLE_IR_VERSION_V1)
}

fn decode_instruction_versioned(
    reader: &mut Reader<'_>,
    outcomes: &[OutcomeSchema],
    schema: &SchemaIr,
    arena: &ExpressionArena,
    ir_version: u32,
) -> Result<Instruction, IrValidationError> {
    match reader.u8()? {
        instruction_tag::REQUIRE => Ok(Instruction::Require {
            requirement_index: reader.u32()?,
            predicate: ExprId::new(reader.u32()?),
            reject: decode_outcome_construction(reader, outcomes, arena)?,
        }),
        instruction_tag::SET_FIELD => Ok(Instruction::SetField {
            binding: BindingId::new(reader.u32()?),
            field: decode_field_id(reader)?,
            value: ExprId::new(reader.u32()?),
        }),
        instruction_tag::WORKFLOW_TRANSITION if ir_version >= EXECUTABLE_IR_VERSION_V2 => {
            let binding = BindingId::new(reader.u32()?);
            let state_field = decode_field_id(reader)?;
            let count = decode_len(
                reader,
                "workflow transition source states",
                crate::MAX_COMMAND_ITEMS,
            )?;
            let mut source_states = Vec::with_capacity(count);
            for _ in 0..count {
                source_states.push(decode_enum_variant_id(reader)?);
            }
            Ok(Instruction::WorkflowTransition {
                binding,
                state_field,
                source_states,
                destination: decode_enum_variant_id(reader)?,
                expected_revision: ExprId::new(reader.u32()?),
                stale: decode_outcome_construction(reader, outcomes, arena)?,
                illegal: decode_outcome_construction(reader, outcomes, arena)?,
            })
        }
        instruction_tag::WORKFLOW_LEASE if ir_version >= EXECUTABLE_IR_VERSION_V3 => {
            let binding = BindingId::new(reader.u32()?);
            let fields = WorkflowLeaseFields {
                owner_field: decode_field_id(reader)?,
                expiry_field: decode_field_id(reader)?,
                fencing_token_field: decode_field_id(reader)?,
                attempt_field: if reader.bool()? {
                    Some(decode_field_id(reader)?)
                } else {
                    None
                },
                minimum_duration_seconds: reader.u64()?,
                maximum_duration_seconds: reader.u64()?,
            };
            macro_rules! expression {
                () => {
                    ExprId::new(reader.u32()?)
                };
            }
            macro_rules! outcome {
                () => {
                    decode_outcome_construction(reader, outcomes, arena)?
                };
            }
            let operation = match reader.u8()? {
                lease_operation_tag::CLAIM => WorkflowLeaseOperation::Claim {
                    owner: expression!(),
                    duration_seconds: expression!(),
                    expected_revision: expression!(),
                    stale: outcome!(),
                    unavailable: outcome!(),
                    invalid: outcome!(),
                    exhausted: outcome!(),
                },
                lease_operation_tag::RENEW => WorkflowLeaseOperation::Renew {
                    owner: expression!(),
                    fencing_token: expression!(),
                    duration_seconds: expression!(),
                    expected_revision: expression!(),
                    stale: outcome!(),
                    invalid: outcome!(),
                    expired: outcome!(),
                    exhausted: outcome!(),
                },
                lease_operation_tag::RELEASE => WorkflowLeaseOperation::Release {
                    owner: expression!(),
                    fencing_token: expression!(),
                    expected_revision: expression!(),
                    stale: outcome!(),
                    invalid: outcome!(),
                },
                lease_operation_tag::EXPIRE => WorkflowLeaseOperation::Expire {
                    expected_revision: expression!(),
                    stale: outcome!(),
                    active: outcome!(),
                },
                lease_operation_tag::FENCE => WorkflowLeaseOperation::Fence {
                    owner: expression!(),
                    fencing_token: expression!(),
                    expected_revision: expression!(),
                    stale: outcome!(),
                    invalid: outcome!(),
                    expired: outcome!(),
                },
                tag => {
                    return Err(IrValidationError::UnknownTag {
                        kind: "workflow lease operation",
                        tag,
                    });
                }
            };
            Ok(Instruction::WorkflowLease {
                binding,
                fields,
                operation,
            })
        }
        instruction_tag::EMIT_EVENT => Ok(Instruction::EmitEvent(decode_event_construction(
            reader, schema, arena,
        )?)),
        instruction_tag::RETURN => Ok(Instruction::Return(decode_outcome_construction(
            reader, outcomes, arena,
        )?)),
        tag => Err(IrValidationError::UnknownTag {
            kind: "instruction",
            tag,
        }),
    }
}

fn decode_command_schema_closure(
    reader: &mut Reader<'_>,
    schema: &SchemaIr,
    bindings: &[BindingPlan],
    root_validation_reads: &[crate::RootValidationReadPlan],
    aggregate_id: AggregateTypeId,
    instructions: &[Instruction],
) -> Result<(), IrValidationError> {
    let mut expected_entities = bindings
        .iter()
        .map(BindingPlan::entity_type)
        .chain(
            root_validation_reads
                .iter()
                .map(crate::RootValidationReadPlan::entity_type),
        )
        .collect::<Vec<_>>();
    expected_entities.sort_unstable();
    expected_entities.dedup();
    let count = decode_len(
        reader,
        "command entity closure",
        crate::MAX_DECLARATIONS_PER_KIND,
    )?;
    if count != expected_entities.len() {
        return Err(IrValidationError::InvalidReference {
            kind: "command entity closure",
        });
    }
    for expected in expected_entities {
        let decoded = decode_entity_schema(reader)?;
        if schema.entity(expected) != Some(&decoded) {
            return Err(IrValidationError::InvalidReference {
                kind: "command entity closure",
            });
        }
    }
    let decoded_aggregate = decode_aggregate_schema(reader)?;
    if schema.aggregate(aggregate_id) != Some(&decoded_aggregate) {
        return Err(IrValidationError::InvalidReference {
            kind: "command aggregate closure",
        });
    }
    let mut expected_events = instructions
        .iter()
        .filter_map(|instruction| match instruction {
            Instruction::EmitEvent(event) => Some(event.event_type()),
            _ => None,
        })
        .collect::<Vec<_>>();
    expected_events.sort_unstable();
    expected_events.dedup();
    let count = decode_len(
        reader,
        "command event closure",
        crate::MAX_DECLARATIONS_PER_KIND,
    )?;
    if count != expected_events.len() {
        return Err(IrValidationError::InvalidReference {
            kind: "command event closure",
        });
    }
    for expected in expected_events {
        let decoded = decode_event_schema(reader, schema.entities())?;
        if schema.event(expected) != Some(&decoded) {
            return Err(IrValidationError::InvalidReference {
                kind: "command event closure",
            });
        }
    }
    Ok(())
}

fn decode_projections(
    reader: &mut Reader<'_>,
    schema: &SchemaIr,
) -> Result<Vec<ProjectionPlan>, IrValidationError> {
    let count =
        decode_len_with_minimum(reader, "projections", crate::MAX_DECLARATIONS_PER_KIND, 40)?;
    let mut projections = Vec::with_capacity(count);
    for _ in 0..count {
        projections.push(decode_projection(reader, schema)?);
    }
    Ok(projections)
}

fn decode_projection(
    reader: &mut Reader<'_>,
    schema: &SchemaIr,
) -> Result<ProjectionPlan, IrValidationError> {
    let projection_id = decode_projection_id(reader)?;
    let name = reader.string(256)?;
    let stored_hash = ProjectionPlanHash::from_bytes(reader.array()?);
    let source_event = decode_event_id(reader)?;
    let expressions = decode_expression_arena(reader)?;
    let filter = if reader.bool()? {
        Some(ExprId::new(reader.u32()?))
    } else {
        None
    };
    let key_count = decode_len(
        reader,
        "projection key expressions",
        crate::MAX_COMMAND_ITEMS,
    )?;
    let mut key_expressions = Vec::with_capacity(key_count);
    for _ in 0..key_count {
        key_expressions.push(ExprId::new(reader.u32()?));
    }
    let measure_count = decode_len(reader, "projection measures", crate::MAX_COMMAND_ITEMS)?;
    let mut measures = Vec::with_capacity(measure_count);
    for _ in 0..measure_count {
        measures.push(decode_projection_measure(reader)?);
    }
    let frontier = decode_projection_frontier(reader.u8()?)?;
    let group_schema = decode_projection_group_schema(reader)?;
    let plan = ProjectionPlan::new(
        projection_id,
        name,
        source_event,
        expressions,
        filter,
        key_expressions,
        measures,
        frontier,
        group_schema,
        schema,
    )?;
    if plan.plan_hash() != stored_hash {
        return Err(IrValidationError::HashMismatch {
            kind: "projection plan",
        });
    }
    Ok(plan)
}

fn decode_projection_frontier(tag: u8) -> Result<ProjectionFrontierPolicy, IrValidationError> {
    match tag {
        frontier_tag::TRANSACTIONALLY_ORDERED => {
            Ok(ProjectionFrontierPolicy::TransactionallyOrdered)
        }
        tag => Err(IrValidationError::UnknownTag {
            kind: "projection frontier",
            tag,
        }),
    }
}

fn decode_projection_measure(
    reader: &mut Reader<'_>,
) -> Result<ProjectionMeasurePlan, IrValidationError> {
    let field = FieldSchema::new(
        decode_field_id(reader)?,
        reader.string(256)?,
        decode_value_type(reader, 0)?,
    )?;
    let aggregation = decode_projection_aggregation(reader.u8()?)?;
    let expression = if reader.bool()? {
        Some(ExprId::new(reader.u32()?))
    } else {
        None
    };
    match (aggregation, expression) {
        (ProjectionAggregation::Count, None) => ProjectionMeasurePlan::count(field),
        (ProjectionAggregation::Sum, Some(expression)) => {
            ProjectionMeasurePlan::sum(field, expression)
        }
        (ProjectionAggregation::Count, Some(_)) => Err(IrValidationError::InvalidProjection {
            reason: "count measure carries an expression",
        }),
        (ProjectionAggregation::Sum, None) => Err(IrValidationError::InvalidProjection {
            reason: "sum measure omits its expression",
        }),
    }
}

#[derive(Clone, Copy)]
enum ProjectionAggregation {
    Count,
    Sum,
}

fn decode_projection_aggregation(tag: u8) -> Result<ProjectionAggregation, IrValidationError> {
    match tag {
        aggregation_tag::COUNT => Ok(ProjectionAggregation::Count),
        aggregation_tag::SUM => Ok(ProjectionAggregation::Sum),
        tag => Err(IrValidationError::UnknownTag {
            kind: "projection aggregation",
            tag,
        }),
    }
}

fn decode_projection_group_schema(
    reader: &mut Reader<'_>,
) -> Result<ProjectionGroupSchema, IrValidationError> {
    let projection_id = decode_projection_id(reader)?;
    require_version(
        reader.u32()?,
        crate::PROJECTION_GROUP_CODEC_VERSION_V1,
        "projection group codec",
    )?;
    let count = decode_len(
        reader,
        "projection group components",
        riffdb_types::MAX_PROJECTION_GROUP_COMPONENTS,
    )?;
    let mut components = Vec::with_capacity(count);
    for _ in 0..count {
        let value_type = decode_value_type(reader, 0)?;
        let variant_count = decode_len(
            reader,
            "projection enum variants",
            crate::MAX_DECLARATIONS_PER_KIND,
        )?;
        let mut variants = Vec::with_capacity(variant_count);
        for _ in 0..variant_count {
            variants.push(decode_enum_variant_id(reader)?);
        }
        let stored_maximum = reader.u32()? as usize;
        let component = ProjectionGroupComponentSchema::new(value_type, variants)?;
        if component.maximum_framed_bytes() != stored_maximum {
            return Err(IrValidationError::HashMismatch {
                kind: "projection component maximum",
            });
        }
        components.push(component);
    }
    let measures = decode_record_schema(reader)?;
    let stored_key_maximum = reader.u32()? as usize;
    let stored_state_maximum = reader.u32()? as usize;
    let schema = ProjectionGroupSchema::new(projection_id, components, measures)?;
    if schema.maximum_complete_key_bytes() != stored_key_maximum
        || schema.maximum_stored_state_bytes() != stored_state_maximum
    {
        return Err(IrValidationError::HashMismatch {
            kind: "projection group maximum",
        });
    }
    Ok(schema)
}

fn decode_schema_artifacts(
    reader: &mut Reader<'_>,
    schema: &SchemaIr,
    commands: &[CommandPlan],
    projections: &[ProjectionPlan],
) -> Result<Vec<GeneratedSchemaArtifact>, IrValidationError> {
    let count = decode_len_with_minimum(reader, "generated schema artifacts", 20_480, 41)?;
    if count != expected_schema_artifact_count(schema, commands, projections)? {
        return Err(IrValidationError::HashMismatch {
            kind: "generated schema artifact registry",
        });
    }
    let mut expected = Vec::with_capacity(count);
    visit_expected_schema_artifacts(schema, commands, projections, |artifact| {
        if reader.array::<5>()? != artifact.key().to_bytes()
            || reader.bytes(crate::MAX_JSON_SCHEMA_ARTIFACT_BYTES)?
                != artifact.canonical_json().as_bytes()
            || SchemaHash::from_bytes(reader.array()?) != artifact.hash()
        {
            return Err(IrValidationError::HashMismatch {
                kind: "generated schema artifact registry",
            });
        }
        expected.push(artifact);
        Ok(())
    })?;
    Ok(expected)
}

fn decode_mcp_registry(
    reader: &mut Reader<'_>,
) -> Result<McpCommandNameRegistryV2, IrValidationError> {
    require_version(
        reader.u32()?,
        crate::MCP_COMMAND_NAME_REGISTRY_VERSION_V2,
        "MCP command-name registry",
    )?;
    let lineage =
        ContractLineage::new(reader.string(256)?).map_err(|_| IrValidationError::InvalidText {
            kind: "MCP registry lineage",
        })?;
    let source_contract_name = reader.string(256)?;
    let count = decode_len_with_minimum(reader, "MCP command-name entries", 4_096, 8)?;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        entries.push(McpCommandNameEntryV2::new(
            decode_command_id(reader)?,
            &source_contract_name,
            reader.string(256)?,
            reader.string(crate::MAX_MCP_COMMAND_TOOL_NAME_BYTES)?,
        )?);
    }
    McpCommandNameRegistryV2::new(lineage, source_contract_name, entries)
}

fn decode_compatibility(
    reader: &mut Reader<'_>,
    has_parent: bool,
) -> Result<CompatibilityReport, IrValidationError> {
    let stored_overall = decode_compatibility_class(reader.u8()?)?;
    let count = decode_len_with_minimum(reader, "compatibility entries", 4_096, 13)?;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        entries.push(CompatibilityEntry::new(
            decode_compatibility_code(&reader.string(8)?)?,
            reader.string(1_024)?,
        )?);
    }
    let report = if has_parent {
        CompatibilityReport::successor(entries)?
    } else if entries.is_empty() {
        CompatibilityReport::genesis()
    } else {
        return Err(IrValidationError::InvalidCompatibilityReport);
    };
    if report.overall() != stored_overall {
        return Err(IrValidationError::InvalidCompatibilityReport);
    }
    Ok(report)
}

fn decode_compatibility_class(tag: u8) -> Result<CompatibilityClass, IrValidationError> {
    match tag {
        compatibility_tag::COMPATIBLE => Ok(CompatibilityClass::Compatible),
        compatibility_tag::REQUIRES_EXPLICIT_VERSION => {
            Ok(CompatibilityClass::RequiresExplicitVersion)
        }
        compatibility_tag::REQUIRES_MIGRATION => Ok(CompatibilityClass::RequiresMigration),
        compatibility_tag::INCOMPATIBLE => Ok(CompatibilityClass::Incompatible),
        tag => Err(IrValidationError::UnknownTag {
            kind: "compatibility class",
            tag,
        }),
    }
}

fn decode_compatibility_code(value: &str) -> Result<CompatibilityCode, IrValidationError> {
    CompatibilityCode::from_code(value).ok_or(IrValidationError::InvalidCompatibilityReport)
}

fn reject_duplicate_by<T, K: Eq>(
    values: &[T],
    key: impl Fn(&T) -> K,
    kind: &'static str,
) -> Result<(), IrValidationError> {
    if values.windows(2).any(|pair| key(&pair[0]) == key(&pair[1])) {
        Err(IrValidationError::NonCanonicalOrder { kind })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod conformance;

#[cfg(test)]
mod tests {
    use super::*;

    fn schema_with_no_inbound_delete_policy() -> SchemaIr {
        let entity_id = EntityTypeId::first();
        let field_id = FieldId::first();
        let entity = EntitySchema::new(
            entity_id,
            "Deletable",
            RecordSchema::new(
                RecordTypeRef::Entity(entity_id),
                vec![FieldSchema::new(field_id, "id", ValueType::u64()).expect("field")],
            )
            .expect("record"),
            vec![field_id],
            KeySchema::new(
                KeyPurpose::Entity(entity_id),
                vec![KeyComponentSchema::new(ValueType::u64(), vec![]).expect("component")],
            )
            .expect("key"),
            vec![],
            vec![],
        )
        .expect("entity");
        SchemaIr::with_integrity_and_delete_policies(
            vec![entity],
            vec![],
            vec![],
            vec![],
            vec![],
            vec![],
            vec![crate::DeletePolicySchemaV1::no_inbound(entity_id)],
        )
        .expect("schema")
    }

    #[test]
    fn delete_policy_schema_extension_round_trips_and_requires_v5() {
        let schema = schema_with_no_inbound_delete_policy();
        let mut writer = Writer::new(4_096);
        encode_schema(&mut writer, &schema).expect("encode schema");
        let bytes = writer.finish();
        assert!(bytes.ends_with(&[
            0xff,
            0xff,
            0xff,
            0xfc,
            0,
            0,
            0,
            1,
            0,
            0,
            0,
            1,
            crate::format_registry::delete_policy_mode::NO_INBOUND,
        ]));
        let mut reader = Reader::new(&bytes);
        assert_eq!(decode_schema(&mut reader).expect("decode schema"), schema);
        assert_eq!(reader.remaining(), 0);

        let lineage = ContractLineage::new("DeletePolicyVersion").expect("lineage");
        let result = ContractBundle::new_with_versions(
            BUNDLE_FORMAT_VERSION_V1,
            GRAMMAR_VERSION_V1,
            EXECUTABLE_IR_VERSION_V1,
            "0.1.0",
            lineage.clone(),
            ContractVersion::new(1).expect("version"),
            None,
            SourceHash::from_bytes([7; 32]),
            LineageLedgerV1::genesis(vec![]).expect("ledger"),
            schema,
            vec![],
            RowPolicyCatalogV1::empty(),
            vec![],
            vec![],
            vec![],
            McpCommandNameRegistryV2::new(lineage, "DeletePolicyVersion", vec![])
                .expect("registry"),
            CompatibilityReport::genesis(),
        );
        assert!(matches!(
            result,
            Err(IrValidationError::UnsupportedVersion {
                kind: "contract bundle version tuple",
                value: EXECUTABLE_IR_VERSION_V1,
            })
        ));
    }

    fn empty_bundle() -> ContractBundle {
        let lineage = ContractLineage::new("TestContract").expect("lineage");
        ContractBundle::new(
            "0.1.0",
            lineage.clone(),
            ContractVersion::new(1).expect("version"),
            None,
            SourceHash::from_bytes([3; 32]),
            LineageLedgerV1::genesis(vec![]).expect("ledger"),
            SchemaIr::new(vec![], vec![], vec![], vec![]).expect("schema"),
            vec![],
            vec![],
            vec![],
            McpCommandNameRegistryV2::new(lineage, "TestContract", vec![]).expect("registry"),
            CompatibilityReport::genesis(),
        )
        .expect("bundle")
    }

    #[test]
    fn event_partition_magic_cannot_alias_the_next_stable_event_id() {
        let marker_id = EventTypeId::new(0xffff_fffc).expect("nonzero marker-shaped event ID");
        let next_id = EventTypeId::first();
        let marker_shaped = EventSchema::new(
            marker_id,
            "MarkerShaped",
            RecordSchema::new(RecordTypeRef::Event(marker_id), vec![]).expect("payload"),
        )
        .expect("event");
        let next = EventSchema::new(
            next_id,
            "Next",
            RecordSchema::new(RecordTypeRef::Event(next_id), vec![]).expect("payload"),
        )
        .expect("event");
        let mut writer = Writer::new(256);
        encode_event_schema(&mut writer, &next, true).expect("first event");
        encode_event_schema(&mut writer, &marker_shaped, true).expect("second event");
        let bytes = writer.finish();
        let mut reader = Reader::new(&bytes);

        assert_eq!(
            decode_event_schema(&mut reader, &[]).expect("unpartitioned first event"),
            next
        );
        assert_eq!(
            decode_event_schema(&mut reader, &[]).expect("following event"),
            marker_shaped
        );
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn mcp_registry_encoding_is_identical_for_every_declaration_permutation() {
        fn visit_permutations<T>(values: &mut [T], index: usize, visit: &mut impl FnMut(&[T])) {
            if index == values.len() {
                visit(values);
                return;
            }
            for selected in index..values.len() {
                values.swap(index, selected);
                visit_permutations(values, index + 1, visit);
                values.swap(index, selected);
            }
        }

        let contract_name = "LegalSpend2";
        let lineage = ContractLineage::new(contract_name).expect("lineage");
        let mut entries = [
            (11, "Rebuild2", "riffdb_cmd_legalspend2_rebuild2"),
            (2, "Run", "riffdb_cmd_legalspend2_run"),
            (
                10,
                "Allocate_Budget",
                "riffdb_cmd_legalspend2_allocate_budget",
            ),
            (1, "Z", "riffdb_cmd_legalspend2_z"),
        ]
        .map(|(id, source, tool)| {
            McpCommandNameEntryV2::new(
                CommandId::new(id).expect("command ID"),
                contract_name,
                source,
                tool,
            )
            .expect("entry")
        });
        let expected_registry =
            McpCommandNameRegistryV2::new(lineage.clone(), contract_name, entries.to_vec())
                .expect("registry");
        let mut expected_writer = Writer::new(4_096);
        encode_mcp_registry(&mut expected_writer, &expected_registry).expect("encode registry");
        let expected_bytes = expected_writer.finish();
        assert_eq!(
            expected_registry
                .entries()
                .iter()
                .map(|entry| (
                    entry.command_id().get(),
                    entry.source_command_name(),
                    entry.tool_name().as_str(),
                ))
                .collect::<Vec<_>>(),
            vec![
                (1, "Z", "riffdb_cmd_legalspend2_z"),
                (2, "Run", "riffdb_cmd_legalspend2_run"),
                (
                    10,
                    "Allocate_Budget",
                    "riffdb_cmd_legalspend2_allocate_budget",
                ),
                (11, "Rebuild2", "riffdb_cmd_legalspend2_rebuild2"),
            ]
        );

        let mut permutations = 0;
        visit_permutations(&mut entries, 0, &mut |permutation| {
            let registry =
                McpCommandNameRegistryV2::new(lineage.clone(), contract_name, permutation.to_vec())
                    .expect("permuted registry");
            assert_eq!(registry, expected_registry);
            let mut writer = Writer::new(4_096);
            encode_mcp_registry(&mut writer, &registry).expect("encode registry");
            assert_eq!(writer.finish(), expected_bytes);
            permutations += 1;
        });
        assert_eq!(permutations, 24);
    }

    #[test]
    fn referenced_enum_hash_layout_omits_display_name_and_has_exact_bytes() {
        let enum_id = EnumTypeId::new(2).expect("enum ID");
        let enum_ids = BTreeSet::from([enum_id]);
        let schema = |name: &str, second_variant: &str| {
            SchemaIr::new(
                vec![],
                vec![],
                vec![
                    EnumSchema::new(
                        enum_id,
                        name,
                        vec![
                            EnumVariantSchema::new(
                                EnumVariantId::new(1).expect("variant ID"),
                                "Open",
                            )
                            .expect("variant"),
                            EnumVariantSchema::new(
                                EnumVariantId::new(10).expect("variant ID"),
                                second_variant,
                            )
                            .expect("variant"),
                        ],
                    )
                    .expect("enum"),
                ],
                vec![],
            )
            .expect("schema")
        };
        let encode = |schema: &SchemaIr| {
            let mut writer = Writer::new(128);
            encode_enum_closure(&mut writer, &enum_ids, schema).expect("enum closure");
            writer.finish()
        };

        let expected = [
            0, 0, 0, 1, // enum count
            0, 0, 0, 2, // EnumTypeId
            0, 0, 0, 2, // variant count
            0, 0, 0, 1, // first EnumVariantId
            0, 0, 0, 4, b'O', b'p', b'e', b'n', // first name
            0, 0, 0, 10, // second EnumVariantId
            0, 0, 0, 6, b'C', b'l', b'o', b's', b'e', b'd', // second name
        ];
        let base = encode(&schema("State", "Closed"));
        assert_eq!(base, expected);
        assert_eq!(base, encode(&schema("Status", "Closed")));
        assert_ne!(base, encode(&schema("State", "Sealed")));
    }

    #[test]
    fn genesis_allocation_is_independent_of_input_order() {
        let namespace =
            StableIdNamespace::new(StableIdNamespaceTag::Command, 0, vec![]).expect("namespace");
        let a = StableIdentity::new(namespace.clone(), "Alpha").expect("identity");
        let b = StableIdentity::new(namespace, "Beta").expect("identity");
        let one = LineageLedgerV1::genesis(vec![b.clone(), a.clone()]).expect("ledger");
        let two = LineageLedgerV1::genesis(vec![a.clone(), b.clone()]).expect("ledger");
        assert_eq!(one, two);
        // Identity keys compare the canonical u16 name length before bytes.
        assert_eq!(one.active_id(&a), Some(2));
        assert_eq!(one.active_id(&b), Some(1));
    }

    #[test]
    fn successor_never_resurrects_a_tombstone() {
        let namespace =
            StableIdNamespace::new(StableIdNamespaceTag::Command, 0, vec![]).expect("namespace");
        let identity = StableIdentity::new(namespace, "Alpha").expect("identity");
        let genesis = LineageLedgerV1::genesis(vec![identity.clone()]).expect("genesis");
        let removed = LineageLedgerV1::successor(&genesis, vec![]).expect("removed");
        assert!(LineageLedgerV1::successor(&removed, vec![identity]).is_err());
    }

    #[test]
    fn index_ids_share_one_global_sequence_across_entity_owners() {
        let first = StableIdentity::new(
            StableIdNamespace::new(StableIdNamespaceTag::Index, 0x01, vec![1]).expect("namespace"),
            "by_name",
        )
        .expect("identity");
        let second = StableIdentity::new(
            StableIdNamespace::new(StableIdNamespaceTag::Index, 0x01, vec![2]).expect("namespace"),
            "by_name",
        )
        .expect("identity");
        let ledger = LineageLedgerV1::genesis(vec![second.clone(), first.clone()]).expect("ledger");
        let index_allocations = ledger
            .allocations()
            .iter()
            .filter(|allocation| allocation.namespace().tag() == StableIdNamespaceTag::Index)
            .collect::<Vec<_>>();
        assert_eq!(index_allocations.len(), 1);
        assert_eq!(ledger.active_id(&first), Some(1));
        assert_eq!(ledger.active_id(&second), Some(2));
    }

    #[test]
    fn empty_genesis_retains_all_eight_global_states() {
        let ledger = LineageLedgerV1::genesis(vec![]).expect("ledger");
        assert_eq!(ledger.allocations().len(), 8);
        assert!(ledger.allocations().iter().all(|allocation| {
            allocation.namespace().owner_ids().is_empty()
                && allocation.max_allocated() == 0
                && allocation.entries().is_empty()
        }));
    }

    #[test]
    fn ledger_rejects_empty_scoped_state_without_current_or_historical_owner() {
        let mut ledger = LineageLedgerV1::genesis(vec![]).expect("ledger");
        ledger.allocations.push(LineageAllocation {
            namespace: StableIdAllocationNamespace::scoped(
                StableIdNamespaceTag::Field,
                0x01,
                vec![99],
            )
            .expect("namespace"),
            max_allocated: 0,
            entries: vec![],
        });
        ledger
            .allocations
            .sort_unstable_by(|left, right| left.namespace.cmp(&right.namespace));
        let schema = SchemaIr::new(vec![], vec![], vec![], vec![]).expect("schema");
        assert!(validate_ledger(&ledger, &schema, &[], &[]).is_err());

        let mut bundle = empty_bundle();
        bundle.ledger = ledger;
        let encoded = encode_bundle(&bundle).expect("encode hostile bundle");
        assert!(ContractBundle::decode(&encoded).is_err());
    }

    #[test]
    fn successor_drops_nonrequired_empty_parent_allocation_state() {
        let extra = StableIdAllocationNamespace::scoped(
            StableIdNamespaceTag::Field,
            record_owner_tag::ENTITY,
            vec![99],
        )
        .expect("namespace");
        let parent =
            LineageLedgerV1::genesis_complete(vec![], vec![extra.clone()]).expect("parent ledger");
        assert!(
            parent
                .allocations()
                .iter()
                .any(|item| item.namespace() == &extra)
        );
        let successor = LineageLedgerV1::successor(&parent, vec![]).expect("successor");
        assert!(
            successor
                .allocations()
                .iter()
                .all(|item| item.namespace() != &extra)
        );
    }

    #[test]
    fn canonical_empty_bundle_round_trips_and_rejects_trailing_bytes() {
        let bundle = empty_bundle();
        assert_eq!(
            ContractBundle::decode(bundle.canonical_bytes()).expect("decoded"),
            bundle
        );
        let mut trailing = bundle.canonical_bytes().to_vec();
        trailing.push(0);
        assert!(ContractBundle::decode(&trailing).is_err());
    }

    #[test]
    fn bundle_construction_rejects_a_command_capability_from_another_lineage() {
        let (plan, schema) = crate::plan::tests::minimal_mutation();
        let lineage = ContractLineage::new("DifferentLineage").expect("lineage");
        let result = ContractBundle::new(
            "0.1.0",
            lineage.clone(),
            plan.contract_version(),
            None,
            SourceHash::from_bytes([3; 32]),
            LineageLedgerV1::genesis(vec![]).expect("ledger"),
            schema,
            vec![plan],
            vec![],
            vec![],
            McpCommandNameRegistryV2::new(lineage, "DifferentLineage", vec![]).expect("registry"),
            CompatibilityReport::genesis(),
        );
        assert!(matches!(
            result,
            Err(IrValidationError::InvalidReference {
                kind: "command capability requirement"
            })
        ));
    }

    #[test]
    fn command_entry_round_trips_root_validation_table_and_expression_tag() {
        let (plan, schema) = crate::plan::tests::root_validation_mutation(true);
        let mut writer = Writer::new(MAX_BUNDLE_BYTES);
        encode_command_bundle_entry(&mut writer, &plan, &schema).expect("encode command");
        let bytes = writer.finish();
        let mut reader = Reader::new(&bytes);
        let decoded = decode_command(&mut reader, plan.required_capability().lineage(), &schema)
            .expect("decode command");
        reader.finish().expect("fully consumed");
        assert_eq!(decoded, plan);
        assert_eq!(decoded.root_validation_reads().len(), 1);
        assert!(matches!(
            decoded.expressions().get(ExprId::new(2)).map(|node| node.kind()),
            Some(ExpressionKind::RootValidationField { read, field })
                if *read == crate::RootValidationReadId::new(0)
                    && *field == FieldId::new(2).expect("field")
        ));
    }

    #[test]
    fn collection_command_entry_round_trips_only_in_ir_v5() {
        let (plan, schema) = crate::plan::tests::minimal_collection_mutation();
        let mut writer = Writer::new(MAX_BUNDLE_BYTES);
        encode_command_bundle_entry_versioned(
            &mut writer,
            &plan,
            &schema,
            EXECUTABLE_IR_VERSION_V5,
        )
        .expect("encode collection command");
        let bytes = writer.finish();
        let mut reader = Reader::new(&bytes);
        let decoded = decode_command_versioned(
            &mut reader,
            plan.required_capability().lineage(),
            &schema,
            EXECUTABLE_IR_VERSION_V5,
        )
        .expect("decode collection command");
        reader.finish().expect("fully consumed");
        assert_eq!(decoded, plan);
        assert_eq!(
            decoded
                .collection_expansion()
                .expect("collection expansion")
                .maximum_elements(),
            8
        );

        let field = FieldId::first();
        let expressions = ExpressionArena::new(vec![
            (ExpressionKind::CollectionElement, crate::ValueType::u64()),
            (
                ExpressionKind::CollectionElementField(field),
                crate::ValueType::bool(),
            ),
        ])
        .expect("collection expressions");
        let mut writer = Writer::new(64);
        encode_expression_arena(&mut writer, &expressions).expect("encode collection expressions");
        let bytes = writer.finish();
        let mut reader = Reader::new(&bytes);
        assert_eq!(
            decode_expression_arena_versioned(&mut reader, EXECUTABLE_IR_VERSION_V5)
                .expect("decode IR v5 collection expressions"),
            expressions
        );
        reader.finish().expect("fully consumed");
        assert!(matches!(
            decode_expression_arena_versioned(&mut Reader::new(&bytes), EXECUTABLE_IR_VERSION_V4),
            Err(IrValidationError::UnknownTag {
                kind: "expression",
                ..
            })
        ));
    }

    #[test]
    fn every_truncated_canonical_prefix_and_deterministic_malformed_corpus_rejects() {
        let bundle = empty_bundle();
        for length in 0..bundle.canonical_bytes().len() {
            assert!(ContractBundle::decode(&bundle.canonical_bytes()[..length]).is_err());
        }
        for seed in 0u64..256 {
            let mut state = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let length = (seed as usize * 37) % 513;
            let mut bytes = Vec::with_capacity(length);
            for _ in 0..length {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                bytes.push(state as u8);
            }
            assert!(ContractBundle::decode(&bytes).is_err());
        }
    }

    #[test]
    fn decoder_rejects_count_before_count_controlled_allocation() {
        let encoded_count = 262_144u32.to_be_bytes();
        let mut reader = Reader::new(&encoded_count);
        assert!(matches!(
            decode_len_with_minimum(&mut reader, "hostile count", 262_144, 2),
            Err(IrValidationError::UnexpectedEnd)
        ));
    }

    #[test]
    fn decoder_rejects_unknown_core_tags_and_nonforward_expression() {
        assert!(matches!(
            decode_namespace_tag(0),
            Err(IrValidationError::UnknownTag { .. })
        ));
        assert!(matches!(
            decode_lineage_entry_state(0),
            Err(IrValidationError::UnknownTag { .. })
        ));
        assert!(matches!(
            decode_record_ref(&mut Reader::new(&[0])),
            Err(IrValidationError::UnknownTag { .. })
        ));
        assert!(matches!(
            decode_value_type(&mut Reader::new(&[0]), 0),
            Err(IrValidationError::UnknownTag { .. })
        ));
        assert!(matches!(
            decode_unary_operator(0),
            Err(IrValidationError::UnknownTag { .. })
        ));
        assert!(matches!(
            decode_binary_operator(0),
            Err(IrValidationError::UnknownTag { .. })
        ));
        assert!(matches!(
            decode_binding_mode(0, EXECUTABLE_IR_VERSION_V5),
            Err(IrValidationError::UnknownTag { .. })
        ));
        assert!(matches!(
            decode_binding_mode(binding_tag::DELETE, EXECUTABLE_IR_VERSION_V4),
            Err(IrValidationError::UnknownTag { .. })
        ));
        assert_eq!(
            decode_binding_mode(binding_tag::DELETE, EXECUTABLE_IR_VERSION_V5),
            Ok(BindingMode::Delete)
        );
        assert!(matches!(
            decode_execution_class(0),
            Err(IrValidationError::UnknownTag { .. })
        ));
        assert!(matches!(
            decode_retry_policy(0),
            Err(IrValidationError::UnknownTag { .. })
        ));
        assert!(matches!(
            decode_capability_requirement(&mut Reader::new(&[0])),
            Err(IrValidationError::UnknownTag { .. })
        ));
        assert!(matches!(
            decode_projection_frontier(0),
            Err(IrValidationError::UnknownTag { .. })
        ));
        assert!(matches!(
            decode_projection_aggregation(0),
            Err(IrValidationError::UnknownTag { .. })
        ));
        assert!(matches!(
            decode_compatibility_class(0),
            Err(IrValidationError::UnknownTag { .. })
        ));

        let empty_schema = SchemaIr::new(vec![], vec![], vec![], vec![]).expect("schema");
        assert!(matches!(
            decode_instruction(
                &mut Reader::new(&[0]),
                &[],
                &empty_schema,
                &ExpressionArena::empty(),
            ),
            Err(IrValidationError::UnknownTag { .. })
        ));

        let mut key = Writer::new(32);
        key.u32(crate::KEY_CODEC_VERSION_V1).expect("version");
        key.u8(0).expect("unknown purpose");
        assert!(matches!(
            decode_key_schema(&mut Reader::new(&key.finish()), 0),
            Err(IrValidationError::UnknownTag { .. })
        ));

        let mut unknown = Writer::new(32);
        unknown.u32(1).expect("count");
        unknown.u8(0).expect("unknown expression");
        encode_value_type(&mut unknown, &ValueType::bool()).expect("type");
        assert!(matches!(
            decode_expression_arena(&mut Reader::new(&unknown.finish())),
            Err(IrValidationError::UnknownTag { .. })
        ));

        let mut nonforward = Writer::new(32);
        nonforward.u32(1).expect("count");
        nonforward.u8(expression_tag::UNARY).expect("tag");
        encode_value_type(&mut nonforward, &ValueType::bool()).expect("type");
        nonforward.u8(unary_tag::NOT).expect("operator");
        nonforward.u32(0).expect("self operand");
        assert!(matches!(
            decode_expression_arena(&mut Reader::new(&nonforward.finish())),
            Err(IrValidationError::NonForwardExpression)
        ));

        let count = crate::MAX_EXPRESSION_NESTING + 1;
        let mut too_deep = Writer::new(4_096);
        too_deep.u32(count as u32).expect("count");
        for index in 0..count {
            if index == 0 {
                too_deep.u8(expression_tag::CONSTANT).expect("constant tag");
                encode_value_type(&mut too_deep, &ValueType::bool()).expect("type");
                too_deep
                    .bytes(
                        &encode_canonical_value(&riffdb_types::CanonicalValue::Bool(true))
                            .expect("value"),
                    )
                    .expect("constant");
            } else {
                too_deep.u8(expression_tag::BINARY).expect("binary tag");
                encode_value_type(&mut too_deep, &ValueType::bool()).expect("type");
                too_deep.u8(binary_tag::AND).expect("operator");
                too_deep.u32((index - 1) as u32).expect("left");
                too_deep.u32((index - 1) as u32).expect("right");
            }
        }
        assert!(matches!(
            decode_expression_arena(&mut Reader::new(&too_deep.finish())),
            Err(IrValidationError::LimitExceeded { .. })
        ));
    }

    #[test]
    fn canonical_constant_preflight_enforces_ir_collection_bound_before_allocation() {
        let list = |count| {
            encode_canonical_value(
                &riffdb_types::CanonicalValue::list(vec![
                    riffdb_types::CanonicalValue::Bool(true);
                    count
                ])
                .expect("list"),
            )
            .expect("canonical value")
        };
        assert!(preflight_ir_canonical_value(&list(crate::MAX_OBJECT_FIELDS)).is_ok());
        assert!(matches!(
            preflight_ir_canonical_value(&list(crate::MAX_OBJECT_FIELDS + 1)),
            Err(IrValidationError::LimitExceeded { .. })
        ));
        let record = |count| {
            encode_canonical_value(
                &riffdb_types::CanonicalValue::record(
                    (1..=count)
                        .map(|id| {
                            (
                                FieldId::new(id as u32).expect("field ID"),
                                riffdb_types::CanonicalValue::Bool(true),
                            )
                        })
                        .collect(),
                )
                .expect("record"),
            )
            .expect("canonical value")
        };
        assert!(preflight_ir_canonical_value(&record(crate::MAX_OBJECT_FIELDS)).is_ok());
        assert!(matches!(
            preflight_ir_canonical_value(&record(crate::MAX_OBJECT_FIELDS + 1)),
            Err(IrValidationError::LimitExceeded { .. })
        ));

        let mut truncated = vec![riffdb_types::CANONICAL_VALUE_VERSION, 0x0c];
        truncated.extend_from_slice(&65_535u32.to_be_bytes());
        assert!(matches!(
            preflight_ir_canonical_value(&truncated),
            Err(IrValidationError::LimitExceeded { .. })
        ));

        let mut nested = vec![riffdb_types::CANONICAL_VALUE_VERSION, 0x0c];
        nested.extend_from_slice(&1u32.to_be_bytes());
        nested.extend_from_slice(&[riffdb_types::CANONICAL_VALUE_VERSION, 0x0c]);
        nested.extend_from_slice(&(crate::MAX_OBJECT_FIELDS as u32 + 1).to_be_bytes());
        assert!(matches!(
            preflight_ir_canonical_value(&nested),
            Err(IrValidationError::LimitExceeded { .. })
        ));
    }

    #[test]
    fn decoder_rejects_every_top_level_version_mismatch_and_hash_mismatch() {
        let bundle = empty_bundle();
        for version_index in 0..3 {
            let mut bytes = bundle.canonical_bytes().to_vec();
            let offset = BUNDLE_MAGIC.len() + version_index * 4;
            bytes[offset..offset + 4].copy_from_slice(&0u32.to_be_bytes());
            assert!(matches!(
                ContractBundle::decode(&bytes),
                Err(IrValidationError::UnsupportedVersion { .. })
            ));
        }

        let mut bytes = bundle.canonical_bytes().to_vec();
        let source_hash = bundle.source_hash();
        let source = source_hash.as_bytes();
        let source_offset = bytes
            .windows(source.len())
            .position(|window| window == source)
            .expect("source hash offset");
        bytes[source_offset + source.len()] ^= 0x01;
        assert!(matches!(
            ContractBundle::decode(&bytes),
            Err(IrValidationError::HashMismatch { .. })
        ));
    }

    #[test]
    fn decoder_rejects_every_nested_format_version_mismatch() {
        let unsupported = 0u32.to_be_bytes();
        assert!(matches!(
            decode_ledger(&mut Reader::new(&unsupported)),
            Err(IrValidationError::UnsupportedVersion { .. })
        ));
        assert!(matches!(
            decode_key_schema(&mut Reader::new(&unsupported), 0),
            Err(IrValidationError::UnsupportedVersion { .. })
        ));
        assert!(matches!(
            decode_mcp_registry(&mut Reader::new(&unsupported)),
            Err(IrValidationError::UnsupportedVersion { .. })
        ));

        let mut projection_group = Writer::new(8);
        projection_group.u32(1).expect("projection ID");
        projection_group.u32(0).expect("unsupported version");
        assert!(matches!(
            decode_projection_group_schema(&mut Reader::new(&projection_group.finish())),
            Err(IrValidationError::UnsupportedVersion { .. })
        ));
    }

    #[test]
    fn decoder_rejects_recursive_declared_event_record_types_without_recursing() {
        let encode = |references: &[(EventTypeId, EventTypeId)]| {
            let mut writer = Writer::new(4_096);
            writer.u32(0).expect("entity count");
            writer.u32(references.len() as u32).expect("event count");
            for (event_id, referenced_id) in references {
                writer.u32(event_id.get()).expect("event ID");
                writer
                    .string(&format!("RecursiveEvent{}", event_id.get()))
                    .expect("event name");
                encode_record_ref(&mut writer, &RecordTypeRef::Event(*event_id))
                    .expect("event owner");
                writer.u32(1).expect("field count");
                writer.u32(FieldId::first().get()).expect("field ID");
                writer.string("nested").expect("field name");
                encode_value_type(
                    &mut writer,
                    &ValueType::record(RecordTypeRef::Event(*referenced_id)),
                )
                .expect("record type");
            }
            writer.u32(0).expect("enum count");
            writer.u32(0).expect("aggregate count");
            writer.finish()
        };

        let first = EventTypeId::first();
        let second = EventTypeId::new(2).expect("event ID");
        for bytes in [
            encode(&[(first, first)]),
            encode(&[(first, second), (second, first)]),
        ] {
            assert!(matches!(
                decode_schema(&mut Reader::new(&bytes)),
                Err(IrValidationError::TypeMismatch { .. })
            ));
        }
    }

    #[test]
    fn schema_artifact_count_mismatch_rejects_before_artifact_generation() {
        let event_id = EventTypeId::first();
        let fields = (1..=crate::MAX_DECLARATIONS_PER_KIND as u32)
            .map(|id| {
                FieldSchema::new(
                    FieldId::new(id).expect("field ID"),
                    format!("field_{id}_{}", "x".repeat(220)),
                    ValueType::bool(),
                )
                .expect("field")
            })
            .collect();
        let event = EventSchema::new(
            event_id,
            "LargeEvent",
            RecordSchema::new(RecordTypeRef::Event(event_id), fields).expect("payload"),
        )
        .expect("event");
        let schema = SchemaIr::new(vec![], vec![event], vec![], vec![]).expect("schema");
        let encoded_zero_count = 0u32.to_be_bytes();
        assert!(matches!(
            decode_schema_artifacts(&mut Reader::new(&encoded_zero_count), &schema, &[], &[],),
            Err(IrValidationError::HashMismatch {
                kind: "generated schema artifact registry",
            })
        ));
    }

    #[test]
    fn decoded_count_and_writer_size_boundaries_are_exact() {
        let mut exact = 4_096u32.to_be_bytes().to_vec();
        exact.resize(4 + 4_096, 0);
        assert_eq!(
            decode_len(&mut Reader::new(&exact), "boundary", 4_096).expect("exact maximum"),
            4_096
        );
        let above = 4_097u32.to_be_bytes();
        assert!(matches!(
            decode_len(&mut Reader::new(&above), "boundary", 4_096),
            Err(IrValidationError::LimitExceeded { .. })
        ));

        let mut writer = Writer::new(4);
        writer.raw(&[0; 4]).expect("exact maximum");
        assert!(matches!(
            writer.raw(&[0]),
            Err(IrValidationError::LimitExceeded { .. })
        ));
    }

    #[test]
    fn ledger_rejects_duplicate_identity_across_tombstone_history() {
        let mut ledger = LineageLedgerV1::genesis(vec![]).expect("ledger");
        let identity = StableIdentity::new(
            StableIdNamespace::new(StableIdNamespaceTag::Command, 0, vec![]).expect("namespace"),
            "OldCommand",
        )
        .expect("identity");
        let allocation = ledger
            .allocations
            .iter_mut()
            .find(|allocation| allocation.namespace.tag == StableIdNamespaceTag::Command)
            .expect("command allocation");
        allocation.max_allocated = 2;
        allocation.entries = vec![
            LineageEntry {
                id: 1,
                identity: identity.clone(),
                state: LineageEntryState::Tombstone,
            },
            LineageEntry {
                id: 2,
                identity,
                state: LineageEntryState::Tombstone,
            },
        ];
        let schema = SchemaIr::new(vec![], vec![], vec![], vec![]).expect("schema");
        assert!(validate_ledger(&ledger, &schema, &[], &[]).is_err());
    }

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(128))]

        #[test]
        fn arbitrary_bundle_bytes_decode_totally_and_only_to_canonical_roundtrips(
            bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..=2_048),
        ) {
            if let Ok(bundle) = ContractBundle::decode(&bytes) {
                proptest::prop_assert_eq!(bundle.canonical_bytes(), bytes.as_slice());
                let decoded = ContractBundle::decode(bundle.canonical_bytes())
                    .expect("accepted canonical bytes round trip");
                proptest::prop_assert_eq!(decoded, bundle);
            }
        }
    }
}
