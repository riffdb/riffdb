//! Bounded, stable diagnostics produced after parsing.

use std::error::Error;
use std::fmt;

use riffdb_contract_syntax::Span;

/// Closed compiler-owned resource names permitted in bounded diagnostics.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CompilerBoundResource {
    /// Submitted elements in one compiler-owned collection expansion.
    CollectionCommandElements,
    /// Complete possible authoritative mutations in one collection command.
    CollectionCommandMutationInstances,
    /// Maximum distinct upfront conflict keys for one collection command.
    CommandConflictKeys,
    /// Physical secondary-index removals and additions.
    CommandIndexEntryDeltas,
    /// Mutation-affected complete index-prefix epoch buckets.
    CommandAffectedPrefixEpochs,
    /// Complete transaction-current command validation positions.
    CommandValidationPositions,
    /// ADR-0149's `D + A + V` command work charge.
    CommandCorrelatedIndexWork,
    /// Semantic bytes retained for affected targets and epoch observations.
    CommandAffectedEpochStateBytes,
    /// Complete collection-command canonical graph bytes.
    CommandGraphBytes,
    /// Canonical bytes across one aggregate-bounded collection input.
    AggregateCollectionBytes,
    /// Canonical key bytes.
    KeyBytes,
    /// Canonical value bytes.
    ValueBytes,
    /// A declaration or other structurally bounded compiler collection.
    DeclarationCount,
    /// Redacted fallback for a ceiling not yet assigned a narrower public name.
    CompiledArtifact,
}

impl CompilerBoundResource {
    /// Stable value-free spelling used in public diagnostics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CollectionCommandElements => "collection_command_elements",
            Self::CollectionCommandMutationInstances => "collection_command_mutation_instances",
            Self::CommandConflictKeys => "command_conflict_keys",
            Self::CommandIndexEntryDeltas => "command_index_entry_deltas",
            Self::CommandAffectedPrefixEpochs => "command_affected_prefix_epochs",
            Self::CommandValidationPositions => "command_validation_positions",
            Self::CommandCorrelatedIndexWork => "command_correlated_index_work",
            Self::CommandAffectedEpochStateBytes => "command_affected_epoch_state_bytes",
            Self::CommandGraphBytes => "command_graph_bytes",
            Self::AggregateCollectionBytes => "aggregate_collection_bytes",
            Self::KeyBytes => "key_bytes",
            Self::ValueBytes => "value_bytes",
            Self::DeclarationCount => "declaration_count",
            Self::CompiledArtifact => "compiled_artifact",
        }
    }

    pub(crate) fn from_ir_kind(kind: &'static str) -> Self {
        match kind {
            "collection command elements" => Self::CollectionCommandElements,
            "collection mutation instances" => Self::CollectionCommandMutationInstances,
            "collection command conflict keys" => Self::CommandConflictKeys,
            "command worst-case index entry deltas" | "command worst-case index entry puts" => {
                Self::CommandIndexEntryDeltas
            }
            "command worst-case affected index prefixes" => Self::CommandAffectedPrefixEpochs,
            "command worst-case validation targets" => Self::CommandValidationPositions,
            "command worst-case correlated index work units" => Self::CommandCorrelatedIndexWork,
            "command worst-case affected epoch state bytes" => Self::CommandAffectedEpochStateBytes,
            value if value.contains("collection command") && value.contains("graph") => {
                Self::CommandGraphBytes
            }
            value if value.contains("aggregate") && value.contains("byte") => {
                Self::AggregateCollectionBytes
            }
            value if value.contains("key") && value.contains("byte") => Self::KeyBytes,
            value if value.contains("value") && value.contains("byte") => Self::ValueBytes,
            value
                if value.contains("declaration")
                    || value.contains("fields")
                    || value.contains("entries") =>
            {
                Self::DeclarationCount
            }
            _ => Self::CompiledArtifact,
        }
    }
}

/// Closed, compiler-derived reason a semantic diagnostic fired.
///
/// Several checks refuse for more than one reason under one code. Reporting
/// only the code tells an author that something is wrong with a construct they
/// can see, but not which rule it broke -- and the rules have different
/// repairs, so the author is left to rediscover which one applies by trial
/// compilation. This is the same argument already accepted for
/// [`CompilerBoundObservation`], applied to causes rather than ceilings.
///
/// The vocabulary is closed and value-free: each variant names a rule, never a
/// fragment of the author's source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CompilerDiagnosticCause {
    /// A field was assigned through a binding that cannot be written.
    AssignmentThroughUnwritableBinding,
    /// A primary-key field was assigned; keys are immutable after creation.
    AssignmentToKeyField,
    /// The same field was assigned more than once in one command.
    DuplicateFieldAssignment,
    /// A cascade failure outcome was declared on a binding that does not
    /// delete a collection-local entity through a collection expansion.
    CascadeFailureOutsideCollectionDelete,
    /// A cascade failure outcome reuses the binding's ordinary failure.
    CascadeFailureDuplicatesOrdinaryFailure,
    /// A unique key does not begin with its aggregate's partition route.
    UniqueKeyOutsidePartitionRoute,
    /// A unique key names an optional or non-key-compatible field.
    UniqueKeyFieldNotKeyCompatible,
    /// Two indexes declare different text profiles for one field, so how that
    /// field's text compares would depend on which index a plan happened to
    /// use.
    TextProfileConflictsAcrossIndexes,
    /// A workflow lease names a field that is not a valid owner, expiry,
    /// fencing-token, or attempt field for its declared role.
    WorkflowLeaseFieldRoleInvalid,
    /// A workflow lease reuses one field for more than one lease role.
    WorkflowLeaseFieldsNotDistinct,
    /// A workflow lease names a primary-key field.
    WorkflowLeaseFieldInPrimaryKey,
    /// A workflow lease duration range is empty, inverted, or over the ceiling.
    WorkflowLeaseDurationOutOfRange,
    /// A migration names a lineage other than its parent's or candidate's.
    MigrationLineageMismatch,
    /// A migration's declared `from`/`to` versions do not match the parent and
    /// candidate contract versions.
    MigrationVersionMismatch,
    /// A migration does not move strictly forward in contract version.
    MigrationVersionNotIncreasing,
    /// A tokenized text index declares no source fields.
    TextIndexEmptySources,
    /// A tokenized text-index source is not a bounded string field.
    TextIndexSourceNotString,
    /// A tokenized text index names an analyzer outside the closed v1 set.
    TextIndexAnalyzerUnknown,
    /// A tokenized text index names a result model outside the closed v1 set.
    TextIndexResultModelUnknown,
    /// A tokenized text-index field weight is zero or exceeds its ceiling.
    TextIndexWeightOutOfRange,
    /// A tokenized text-index stale-entity threshold is not positive.
    TextIndexStalenessOutOfRange,
    /// A tokenized text-index replay ceiling is zero or excessive.
    TextIndexReplayOutOfRange,
    /// A tokenized text-index term budget is zero or excessive.
    TextIndexTermBudgetOutOfRange,
    /// A tokenized text-index candidate budget is zero or excessive.
    TextIndexCandidateBudgetOutOfRange,
    /// A tokenized text-index result budget is zero or excessive.
    TextIndexResultBudgetOutOfRange,
    /// A tokenized text-index result window exceeds its candidate ceiling.
    TextIndexResultExceedsCandidate,
}

impl CompilerDiagnosticCause {
    /// Stable, value-free wire and display token.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AssignmentThroughUnwritableBinding => {
                "a read or delete binding cannot be assigned through"
            }
            Self::AssignmentToKeyField => "a primary-key field is immutable after creation",
            Self::DuplicateFieldAssignment => {
                "the same field is assigned more than once in this command"
            }
            Self::CascadeFailureOutsideCollectionDelete => {
                "a cascade failure belongs on a collection-local delete under a collection expansion"
            }
            Self::CascadeFailureDuplicatesOrdinaryFailure => {
                "the cascade failure must differ from the binding's ordinary failure"
            }
            Self::UniqueKeyOutsidePartitionRoute => {
                "the unique key does not begin with the aggregate's partition route"
            }
            Self::UniqueKeyFieldNotKeyCompatible => {
                "a unique key field is optional or not key-compatible"
            }
            Self::TextProfileConflictsAcrossIndexes => {
                "two indexes declare different text profiles for the same field"
            }
            Self::WorkflowLeaseFieldRoleInvalid => {
                "a lease field does not have the type its role requires"
            }
            Self::WorkflowLeaseFieldsNotDistinct => {
                "one field is used for more than one lease role"
            }
            Self::WorkflowLeaseFieldInPrimaryKey => "a lease field is part of the primary key",
            Self::WorkflowLeaseDurationOutOfRange => {
                "the lease duration range is empty, inverted, or over the ceiling"
            }
            Self::MigrationLineageMismatch => {
                "the migration lineage differs from the parent or candidate contract"
            }
            Self::MigrationVersionMismatch => {
                "the declared from/to versions do not match the parent and candidate versions"
            }
            Self::MigrationVersionNotIncreasing => {
                "the candidate contract version must be greater than the parent's"
            }
            Self::TextIndexEmptySources => "the text index declares no source fields",
            Self::TextIndexSourceNotString => "a text-index source is not a bounded string field",
            Self::TextIndexAnalyzerUnknown => "the analyzer is not keyword_v1 or standard_v1",
            Self::TextIndexResultModelUnknown => "the result model is not boolean_v1",
            Self::TextIndexWeightOutOfRange => "a field weight is outside 1..=10000",
            Self::TextIndexStalenessOutOfRange => "the stale-entity threshold must be positive",
            Self::TextIndexReplayOutOfRange => {
                "a replay ceiling is zero or exceeds its compiler maximum"
            }
            Self::TextIndexTermBudgetOutOfRange => "max_terms is outside 1..=1024",
            Self::TextIndexCandidateBudgetOutOfRange => "max_candidates is outside 1..=1000000",
            Self::TextIndexResultBudgetOutOfRange => "max_results is outside 1..=65536",
            Self::TextIndexResultExceedsCandidate => "max_results exceeds max_candidates",
        }
    }
}

/// Safe checked evidence attached to one `RDB-C020` ceiling violation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CompilerBoundObservation {
    resource: CompilerBoundResource,
    actual: usize,
    maximum: usize,
}

impl CompilerBoundObservation {
    /// Closed compiler resource.
    #[must_use]
    pub const fn resource(self) -> CompilerBoundResource {
        self.resource
    }

    /// Checked observed plan/schema amount.
    #[must_use]
    pub const fn actual(self) -> usize {
        self.actual
    }

    /// Checked immutable maximum.
    #[must_use]
    pub const fn maximum(self) -> usize {
        self.maximum
    }
}

/// Maximum number of semantic diagnostics returned by one compilation.
pub const MAX_COMPILER_DIAGNOSTICS: usize = 32;

/// Stable identities for grammar-v1 compiler failures.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CompilerDiagnosticCode {
    /// `RDB-C001`: the application contract version is not a supported nonzero value.
    InvalidContractVersion,
    /// `RDB-C002`: a name is declared more than once in one namespace.
    DuplicateName,
    /// `RDB-C003`: a required declaration or singleton item is missing.
    MissingDeclaration,
    /// `RDB-C004`: a referenced name cannot be resolved in its namespace.
    UnknownName,
    /// `RDB-C005`: a source type is invalid or unsupported in grammar version 1.
    InvalidType,
    /// `RDB-C006`: an expression or constructed field has the wrong exact type.
    TypeMismatch,
    /// `RDB-C007`: an expression is not valid in its semantic context.
    InvalidExpression,
    /// `RDB-C008`: aggregate ownership or root/child shape is invalid.
    InvalidAggregate,
    /// `RDB-C009`: an entity binding is invalid or has ambiguous ownership.
    InvalidBinding,
    /// `RDB-C010`: a mutating command lacks its one required idempotency declaration.
    MissingIdempotency,
    /// `RDB-C011`: the idempotency expression is invalid or leaks its secret input.
    InvalidIdempotency,
    /// `RDB-C012`: creation definite-assignment requirements are not met.
    InvalidCreation,
    /// `RDB-C013`: a mutation target is invalid or written more than once.
    InvalidMutation,
    /// `RDB-C014`: an outcome name or payload shape is inconsistent.
    InvalidOutcome,
    /// `RDB-C015`: an emitted event or payload shape is invalid.
    InvalidEvent,
    /// `RDB-C016`: a partition or conflict key is not derivable from validated inputs.
    ConflictNotInputComputable,
    /// `RDB-C017`: command bindings cannot be proved to use one logical partition.
    CrossPartitionMutation,
    /// `RDB-C018`: a required relationship is incomplete, mistyped, or crosses a partition.
    InvalidRelationship,
    /// `RDB-C019`: a projection operator, filter, key, or measure is unsupported.
    InvalidProjection,
    /// `RDB-C020`: a key, row, schema, or plan maximum exceeds a fixed bound.
    BoundExceeded,
    /// `RDB-C021`: stable lineage identifiers cannot be allocated compatibly.
    StableIdAllocation,
    /// `RDB-C022`: a supplied parent bundle is not a valid predecessor.
    InvalidParent,
    /// `RDB-C023`: checked IR construction rejected compiler output.
    InvalidIr,
    /// `RDB-C024`: a relationship-changing command lacks a dominating exact target read.
    MissingRelationshipRead,
    /// `RDB-C025`: a unique key is optional, mistyped, or lacks its partition prefix.
    InvalidUniqueKey,
    /// `RDB-C026`: a unique-key change cannot be derived entirely from command inputs.
    UniqueKeyNotInputComputable,
    /// `RDB-C027`: migration source does not bind the exact parent and candidate.
    InvalidMigrationIdentity,
    /// `RDB-C028`: a required migration proof is absent.
    MissingMigrationProof,
    /// `RDB-C029`: migration source proves one semantic change more than once.
    DuplicateMigrationProof,
    /// `RDB-C030`: migration source contains a clause unrelated to the exact contract diff.
    UnnecessaryMigrationProof,
    /// `RDB-C031`: a frozen migration step is not executable in the current implementation gate.
    UnsupportedMigrationStep,
    /// `RDB-C032`: a migration conversion or expression is not exact and deterministic.
    InvalidMigrationExpression,
    /// `RDB-C033`: a workflow does not name one aggregate-local entity and enum state field.
    InvalidWorkflow,
    /// `RDB-C034`: a workflow transition is duplicated, mistyped, or not legal for its state graph.
    InvalidWorkflowTransition,
    /// `RDB-C035`: a workflow lease field or duration bound is invalid.
    InvalidWorkflowLease,
    /// `RDB-C036`: a service-owned value is duplicated or used in a caller-owned context.
    InvalidServiceValue,
    /// `RDB-C037`: a workflow transition lacks one direct exact observed-revision input.
    MissingWorkflowRevision,
    /// `RDB-C038`: a workflow lease operation lacks a required direct command input.
    MissingWorkflowLeaseInput,
    /// `RDB-C039`: a principal fact schema is invalid or exceeds its closed bounds.
    InvalidPrincipalFact,
    /// `RDB-C040`: a row-policy rule is mistyped, duplicated, or otherwise invalid.
    InvalidRowPolicy,
    /// `RDB-C041`: a row-policy expression exceeds its static node or disjunction bound.
    UnboundedRowPolicy,
    /// `RDB-C042`: a row-policy dependency cannot be proved to remain in one partition.
    CrossPartitionRowPolicy,
    /// `RDB-C043`: a row-policy relationship probe is absent, unindexed, or unsafe.
    InvalidRowPolicyRelationship,
    /// `RDB-C045`: a deletion policy lacks its complete compiler-owned safety proof.
    InvalidDeletePolicy,
    /// `RDB-C046`: a secret value crosses a non-secret boundary without an exact reveal.
    InvalidSecretReveal,
    /// `RDB-C047`: a tokenized text-index declaration is invalid.
    InvalidTextIndex,
    /// `RDB-C201`: an identifier cannot form an ADR-0064 command tool-name segment.
    InvalidCommandToolName,
    /// `RDB-C202`: a complete ADR-0064 command tool name exceeds 128 bytes.
    CommandToolNameTooLong,
    /// `RDB-C203`: two commands normalize to the same ADR-0020 tool name.
    CommandToolNameCollision,
}

impl CompilerDiagnosticCode {
    /// Complete pre-freeze public semantic diagnostic registry in code order.
    pub const ALL: [Self; 49] = [
        Self::InvalidContractVersion,
        Self::DuplicateName,
        Self::MissingDeclaration,
        Self::UnknownName,
        Self::InvalidType,
        Self::TypeMismatch,
        Self::InvalidExpression,
        Self::InvalidAggregate,
        Self::InvalidBinding,
        Self::MissingIdempotency,
        Self::InvalidIdempotency,
        Self::InvalidCreation,
        Self::InvalidMutation,
        Self::InvalidOutcome,
        Self::InvalidEvent,
        Self::ConflictNotInputComputable,
        Self::CrossPartitionMutation,
        Self::InvalidRelationship,
        Self::InvalidProjection,
        Self::BoundExceeded,
        Self::StableIdAllocation,
        Self::InvalidParent,
        Self::InvalidIr,
        Self::MissingRelationshipRead,
        Self::InvalidUniqueKey,
        Self::UniqueKeyNotInputComputable,
        Self::InvalidMigrationIdentity,
        Self::MissingMigrationProof,
        Self::DuplicateMigrationProof,
        Self::UnnecessaryMigrationProof,
        Self::UnsupportedMigrationStep,
        Self::InvalidMigrationExpression,
        Self::InvalidWorkflow,
        Self::InvalidWorkflowTransition,
        Self::InvalidWorkflowLease,
        Self::InvalidServiceValue,
        Self::MissingWorkflowRevision,
        Self::MissingWorkflowLeaseInput,
        Self::InvalidPrincipalFact,
        Self::InvalidRowPolicy,
        Self::UnboundedRowPolicy,
        Self::CrossPartitionRowPolicy,
        Self::InvalidRowPolicyRelationship,
        Self::InvalidDeletePolicy,
        Self::InvalidSecretReveal,
        Self::InvalidTextIndex,
        Self::InvalidCommandToolName,
        Self::CommandToolNameTooLong,
        Self::CommandToolNameCollision,
    ];

    /// Returns the immutable external code.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidContractVersion => "RDB-C001",
            Self::DuplicateName => "RDB-C002",
            Self::MissingDeclaration => "RDB-C003",
            Self::UnknownName => "RDB-C004",
            Self::InvalidType => "RDB-C005",
            Self::TypeMismatch => "RDB-C006",
            Self::InvalidExpression => "RDB-C007",
            Self::InvalidAggregate => "RDB-C008",
            Self::InvalidBinding => "RDB-C009",
            Self::MissingIdempotency => "RDB-C010",
            Self::InvalidIdempotency => "RDB-C011",
            Self::InvalidCreation => "RDB-C012",
            Self::InvalidMutation => "RDB-C013",
            Self::InvalidOutcome => "RDB-C014",
            Self::InvalidEvent => "RDB-C015",
            Self::ConflictNotInputComputable => "RDB-C016",
            Self::CrossPartitionMutation => "RDB-C017",
            Self::InvalidRelationship => "RDB-C018",
            Self::InvalidProjection => "RDB-C019",
            Self::BoundExceeded => "RDB-C020",
            Self::StableIdAllocation => "RDB-C021",
            Self::InvalidParent => "RDB-C022",
            Self::InvalidIr => "RDB-C023",
            Self::MissingRelationshipRead => "RDB-C024",
            Self::InvalidUniqueKey => "RDB-C025",
            Self::UniqueKeyNotInputComputable => "RDB-C026",
            Self::InvalidMigrationIdentity => "RDB-C027",
            Self::MissingMigrationProof => "RDB-C028",
            Self::DuplicateMigrationProof => "RDB-C029",
            Self::UnnecessaryMigrationProof => "RDB-C030",
            Self::UnsupportedMigrationStep => "RDB-C031",
            Self::InvalidMigrationExpression => "RDB-C032",
            Self::InvalidWorkflow => "RDB-C033",
            Self::InvalidWorkflowTransition => "RDB-C034",
            Self::InvalidWorkflowLease => "RDB-C035",
            Self::InvalidServiceValue => "RDB-C036",
            Self::MissingWorkflowRevision => "RDB-C037",
            Self::MissingWorkflowLeaseInput => "RDB-C038",
            Self::InvalidPrincipalFact => "RDB-C039",
            Self::InvalidRowPolicy => "RDB-C040",
            Self::UnboundedRowPolicy => "RDB-C041",
            Self::CrossPartitionRowPolicy => "RDB-C042",
            Self::InvalidRowPolicyRelationship => "RDB-C043",
            Self::InvalidDeletePolicy => "RDB-C045",
            Self::InvalidSecretReveal => "RDB-C046",
            Self::InvalidTextIndex => "RDB-C047",
            Self::InvalidCommandToolName => "RDB-C201",
            Self::CommandToolNameTooLong => "RDB-C202",
            Self::CommandToolNameCollision => "RDB-C203",
        }
    }

    /// Returns a concise caller-safe summary.
    #[must_use]
    pub const fn summary(self) -> &'static str {
        match self {
            Self::InvalidContractVersion => "contract version must be a supported nonzero integer",
            Self::DuplicateName => "a name is declared more than once in this namespace",
            Self::MissingDeclaration => "a required declaration or singleton item is missing",
            Self::UnknownName => "a referenced declaration, field, or binding is unknown",
            Self::InvalidType => "the declared type is invalid or unsupported",
            Self::TypeMismatch => "an expression does not have the required exact type",
            Self::InvalidExpression => "the expression is invalid in this context",
            Self::InvalidAggregate => "aggregate ownership or key shape is invalid",
            Self::InvalidBinding => "the command binding is invalid or ambiguously owned",
            Self::MissingIdempotency => "a mutating command must declare one idempotency key",
            Self::InvalidIdempotency => {
                "the idempotency expression is invalid or used outside its clause"
            }
            Self::InvalidCreation => "the create binding does not definitely initialize its record",
            Self::InvalidMutation => "the command mutation target is invalid",
            Self::InvalidOutcome => "an outcome name or payload shape is invalid",
            Self::InvalidEvent => "an event name or payload shape is invalid",
            Self::ConflictNotInputComputable => {
                "partition and conflict keys must be computable from validated inputs"
            }
            Self::CrossPartitionMutation => {
                "command bindings must share one partition and writes one aggregate"
            }
            Self::InvalidRelationship => {
                "a required relationship must map stored fields to one complete same-partition target key"
            }
            Self::InvalidProjection => "the projection uses an unsupported or invalid operation",
            Self::BoundExceeded => "a compiled artifact exceeds a fixed semantic bound",
            Self::StableIdAllocation => {
                "stable semantic identifiers cannot be allocated compatibly"
            }
            Self::InvalidParent => "the parent bundle is not a valid predecessor",
            Self::InvalidIr => "checked executable IR construction rejected the compiled plan",
            Self::MissingRelationshipRead => {
                "a relationship change lacks a dominating exact target binding and missing-target outcome"
            }
            Self::InvalidUniqueKey => {
                "a unique key must use required fields and begin with the complete partition route"
            }
            Self::UniqueKeyNotInputComputable => {
                "a changed unique value must be computable from validated command inputs"
            }
            Self::InvalidMigrationIdentity => {
                "migration source does not bind the exact parent and candidate"
            }
            Self::MissingMigrationProof => "a required migration proof is missing",
            Self::DuplicateMigrationProof => "a migration change is proved more than once",
            Self::UnnecessaryMigrationProof => {
                "a migration clause does not correspond to the exact contract change"
            }
            Self::UnsupportedMigrationStep => {
                "the migration step is not executable in the current implementation gate"
            }
            Self::InvalidMigrationExpression => {
                "the migration expression or conversion is not exact and deterministic"
            }
            Self::InvalidWorkflow => {
                "the workflow entity, state field, or aggregate ownership is invalid"
            }
            Self::InvalidWorkflowTransition => {
                "the workflow transition is unknown, duplicated, or illegal for its state graph"
            }
            Self::InvalidWorkflowLease => {
                "the workflow lease fields or duration bounds are invalid"
            }
            Self::InvalidServiceValue => {
                "a service-owned value requires durable idempotent command admission"
            }
            Self::MissingWorkflowRevision => {
                "a workflow transition requires one direct exact observed-revision input"
            }
            Self::MissingWorkflowLeaseInput => {
                "a workflow lease operation requires direct owner, duration, revision, and fencing-token inputs"
            }
            Self::InvalidPrincipalFact => {
                "the principal fact schema is invalid or exceeds a fixed bound"
            }
            Self::InvalidRowPolicy => "the row-policy rule is mistyped, duplicated, or invalid",
            Self::UnboundedRowPolicy => "the row-policy expression exceeds a fixed static bound",
            Self::CrossPartitionRowPolicy => {
                "the row-policy dependency is not provably partition-local"
            }
            Self::InvalidRowPolicyRelationship => {
                "the row-policy relationship must use one exact local declared index"
            }
            Self::InvalidDeletePolicy => {
                "the deletion policy lacks a complete no-inbound or indexed-restrict proof"
            }
            Self::InvalidSecretReveal => {
                "a secret value crosses a non-secret destination without an exact reveals annotation"
            }
            Self::InvalidTextIndex => "the tokenized text-index declaration is invalid",
            Self::InvalidCommandToolName => {
                "an identifier cannot form a valid MCP command tool-name segment"
            }
            Self::CommandToolNameTooLong => "the complete MCP command tool name exceeds 128 bytes",
            Self::CommandToolNameCollision => {
                "two commands normalize to the same MCP command tool name"
            }
        }
    }

    /// Returns static corrective guidance when one applies.
    #[must_use]
    pub const fn help(self) -> Option<&'static str> {
        match self {
            Self::InvalidContractVersion => {
                Some("use a base-10 application version in 1..=u64::MAX")
            }
            Self::DuplicateName => Some("rename or remove one declaration in the shared namespace"),
            Self::MissingDeclaration => Some("add the required grammar-version-1 declaration"),
            Self::UnknownName => Some("reference an exact case-sensitive declared name"),
            Self::InvalidType => Some("use a bounded grammar-version-1 value type"),
            Self::TypeMismatch => Some("make both sides use the same complete static type"),
            Self::InvalidExpression => {
                Some("use an expression allowed by this declaration context")
            }
            Self::InvalidAggregate => {
                Some("declare one root and the required root-key prefix ownership")
            }
            Self::InvalidBinding => Some("bind an entity owned by the command's one aggregate"),
            Self::MissingIdempotency => {
                Some("declare a direct UUID or bounded string input as idempotency_key")
            }
            Self::InvalidIdempotency => Some(
                "use one required UUID or string<1..=128> input only in the idempotency clause",
            ),
            Self::InvalidCreation => {
                Some("assign every required non-key field exactly once before return")
            }
            Self::InvalidMutation => {
                Some("write one declared non-key field through a mutable binding")
            }
            Self::InvalidOutcome => {
                Some("use one consistent typed payload for each declared outcome name")
            }
            Self::InvalidEvent => Some("construct every declared event field with its exact type"),
            Self::ConflictNotInputComputable => {
                Some("derive aggregate keys only from root-key inputs and constants")
            }
            // ADR-0170: writing two aggregate roots on one partition route is
            // admitted, so the only remaining cause is a differing route.
            Self::CrossPartitionMutation => Some(
                "every create and mutate binding must derive the same partition route; bindings may belong to different aggregates within that route, but a write that crosses partitions must be split into one idempotent command per partition",
            ),
            Self::InvalidRelationship => Some(
                "map required non-optional fields to the complete target key in canonical order",
            ),
            Self::InvalidProjection => {
                Some("use equality/conjunction filters and bounded count or sum aggregation")
            }
            Self::BoundExceeded => {
                Some("reduce declared bounds or the number of schema components")
            }
            Self::StableIdAllocation => {
                Some("preserve lineage identities and do not reuse removed identifiers")
            }
            Self::InvalidParent => Some("compile against the exact validated predecessor bundle"),
            Self::InvalidIr => None,
            Self::MissingRelationshipRead => Some(
                "bind the complete referenced key before the mutable binding and declare its failure outcome",
            ),
            // Two repairs, and the second is the one authors miss. Uniqueness
            // is enforced inside one partition, so prefixing the key with the
            // route makes the value unique PER route -- which is vacuous when
            // the route already identifies one row. When the value must be
            // unique across a wider scope, the partition is what has to change,
            // and that is a decision about the aggregate rather than the key.
            // Widening a partition does not serialise writers: `conflict_key`
            // is what they contend on.
            Self::InvalidUniqueKey => Some(
                "a unique key is enforced within one partition, so its fields must be required, key-compatible, and begin with the aggregate's partition route; either prefix the route to make the value unique per route, or, if the value must be unique across a wider scope, partition the aggregate by that scope and keep a finer conflict_key so writers still do not contend",
            ),
            Self::UniqueKeyNotInputComputable => Some(
                "assign every changed unique component from command inputs or input-only expressions",
            ),
            Self::InvalidMigrationIdentity => {
                Some("use the exact lineage, parent version, and candidate version")
            }
            Self::MissingMigrationProof => {
                Some("add the source clause required by the reported compatibility change")
            }
            Self::DuplicateMigrationProof => Some("retain exactly one proof for the change"),
            Self::UnnecessaryMigrationProof => {
                Some("remove the clause or compile it against the intended exact parent")
            }
            Self::UnsupportedMigrationStep => {
                Some("wait for the documented migration implementation gate")
            }
            Self::InvalidMigrationExpression => Some(
                "use only the old row, canonical literals, checked operators, and closed conversions",
            ),
            Self::InvalidWorkflow => {
                Some("name one aggregate-owned entity and one required enum state field")
            }
            Self::InvalidWorkflowTransition => {
                Some("declare unique source states and invoke one legal named transition")
            }
            Self::InvalidWorkflowLease => Some(
                "use optional UUID owner/expiry, nonzero u64 fence, optional u64 attempts, and bounded seconds",
            ),
            Self::InvalidServiceValue => Some(
                "declare an idempotency key and use service uuid_v7 or service transaction_time only as a service-owned value",
            ),
            Self::MissingWorkflowRevision => Some(
                "pass one required u64 command input containing the revision returned by the prior read",
            ),
            Self::MissingWorkflowLeaseInput => Some(
                "pass the exact owner, duration, revision, and fencing token as required command inputs",
            ),
            Self::InvalidPrincipalFact => Some(
                "declare a scalar or list of at most 64 scalar values; at most 32 facts are allowed",
            ),
            Self::InvalidRowPolicy => Some(
                "use row fields, principal identity, declared facts, constants, and closed boolean operators",
            ),
            Self::UnboundedRowPolicy => {
                Some("reduce policy nodes, disjunctions, fact values, or relationship probes")
            }
            Self::CrossPartitionRowPolicy => Some(
                "keep the policy entity and relationship target in one declared aggregate partition",
            ),
            Self::InvalidRowPolicyRelationship => {
                Some("name one declared target index and provide its complete partition-routed key")
            }
            Self::InvalidDeletePolicy => Some(
                "declare no_inbound only when nothing references this entity; otherwise cascade over every inbound relation in this aggregate, naming each reverse index and maximum, or restrict against the one inbound source's reverse index",
            ),
            Self::InvalidSecretReveal => Some(
                "add `reveals binding.secret_field` at the exact non-secret flow site, or keep the destination secret-classified",
            ),
            Self::InvalidTextIndex => Some(
                "use distinct bounded string sources, keyword_v1 or standard_v1, boolean_v1, positive weights and ceilings, and max_results no greater than max_candidates",
            ),
            Self::InvalidCommandToolName => {
                Some("start contract and command identifiers with an ASCII letter")
            }
            Self::CommandToolNameTooLong => {
                Some("shorten the source contract or command identifier")
            }
            Self::CommandToolNameCollision => {
                Some("rename one command so lowercase identifiers remain distinct")
            }
        }
    }
}

/// One semantic compiler failure with an exact primary source span.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompilerDiagnostic {
    code: CompilerDiagnosticCode,
    primary_span: Span,
    related_span: Option<Span>,
    bound: Option<CompilerBoundObservation>,
    cause: Option<CompilerDiagnosticCause>,
}

impl CompilerDiagnostic {
    /// Constructs a diagnostic with one primary source span.
    #[must_use]
    pub const fn new(code: CompilerDiagnosticCode, primary_span: Span) -> Self {
        Self {
            code,
            primary_span,
            related_span: None,
            bound: None,
            cause: None,
        }
    }

    /// Attaches the closed reason this diagnostic fired.
    #[must_use]
    pub const fn with_cause(mut self, cause: CompilerDiagnosticCause) -> Self {
        self.cause = Some(cause);
        self
    }

    /// Borrows the closed reason, when the check supplied one.
    #[must_use]
    pub const fn cause(&self) -> Option<CompilerDiagnosticCause> {
        self.cause
    }

    /// Constructs a precise value-free `RDB-C020` ceiling observation.
    #[must_use]
    pub const fn bound_exceeded(
        resource: CompilerBoundResource,
        actual: usize,
        maximum: usize,
        primary_span: Span,
    ) -> Self {
        Self {
            code: CompilerDiagnosticCode::BoundExceeded,
            primary_span,
            related_span: None,
            cause: None,
            bound: Some(CompilerBoundObservation {
                resource,
                actual,
                maximum,
            }),
        }
    }

    /// Converts checked IR validation while preserving safe ceiling evidence.
    #[must_use]
    pub fn from_ir_error(error: riffdb_contract_ir::IrValidationError, primary_span: Span) -> Self {
        match error {
            riffdb_contract_ir::IrValidationError::LimitExceeded {
                kind,
                actual,
                maximum,
            } => Self::bound_exceeded(
                CompilerBoundResource::from_ir_kind(kind),
                actual,
                maximum,
                primary_span,
            ),
            riffdb_contract_ir::IrValidationError::SizeOverflow { .. } => {
                Self::new(CompilerDiagnosticCode::BoundExceeded, primary_span)
            }
            _ => Self::new(CompilerDiagnosticCode::InvalidIr, primary_span),
        }
    }

    /// Adds one deterministic related source span, such as the first colliding declaration.
    #[must_use]
    pub const fn with_related_span(mut self, related_span: Span) -> Self {
        self.related_span = Some(related_span);
        self
    }

    /// Returns the stable diagnostic identity.
    #[must_use]
    pub const fn code(&self) -> CompilerDiagnosticCode {
        self.code
    }

    /// Returns the primary half-open source span.
    #[must_use]
    pub const fn primary_span(&self) -> Span {
        self.primary_span
    }

    /// Returns the optional related half-open source span.
    #[must_use]
    pub const fn related_span(&self) -> Option<Span> {
        self.related_span
    }

    /// Returns precise checked bound evidence when this is a known ceiling.
    #[must_use]
    pub const fn bound(&self) -> Option<CompilerBoundObservation> {
        self.bound
    }

    /// Returns the bounded public summary, including actionable bound evidence.
    #[must_use]
    pub fn summary(&self) -> String {
        self.bound.map_or_else(
            || self.code.summary().to_owned(),
            |bound| {
                format!(
                    "{} is {}; maximum is {}",
                    bound.resource.as_str(),
                    bound.actual,
                    bound.maximum
                )
            },
        )
    }
}

impl fmt::Display for CompilerDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code.as_str(), self.summary())
    }
}

impl Error for CompilerDiagnostic {}

/// A nonempty, bounded, deterministically ordered semantic diagnostic collection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompilerDiagnostics(Vec<CompilerDiagnostic>);

impl CompilerDiagnostics {
    /// Validates and deterministically orders a nonempty diagnostic collection.
    pub fn new(mut diagnostics: Vec<CompilerDiagnostic>) -> Result<Self, DiagnosticBoundsError> {
        if diagnostics.is_empty() {
            return Err(DiagnosticBoundsError::Empty);
        }
        diagnostics.sort_by_key(|diagnostic| {
            (
                diagnostic.primary_span.start(),
                diagnostic.primary_span.end(),
                diagnostic.code,
                diagnostic.related_span,
                diagnostic.bound,
            )
        });
        diagnostics.dedup();
        if diagnostics.len() > MAX_COMPILER_DIAGNOSTICS {
            diagnostics.truncate(MAX_COMPILER_DIAGNOSTICS);
        }
        Ok(Self(diagnostics))
    }

    /// Constructs a collection containing one diagnostic.
    #[must_use]
    pub fn single(diagnostic: CompilerDiagnostic) -> Self {
        Self(vec![diagnostic])
    }

    /// Returns diagnostics in deterministic source order.
    #[must_use]
    pub fn as_slice(&self) -> &[CompilerDiagnostic] {
        &self.0
    }

    /// Returns the number of diagnostics.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `false`; the type cannot contain an empty collection.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        false
    }
}

impl fmt::Display for CompilerDiagnostics {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0[0].fmt(formatter)
    }
}

impl Error for CompilerDiagnostics {}

/// Invalid construction of a semantic diagnostic collection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticBoundsError {
    /// A semantic diagnostic collection must not be empty.
    Empty,
}

impl fmt::Display for DiagnosticBoundsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("compiler diagnostic collection must be nonempty")
    }
}

impl Error for DiagnosticBoundsError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostics_are_bounded_deduplicated_and_source_ordered() {
        let late = CompilerDiagnostic::new(
            CompilerDiagnosticCode::UnknownName,
            Span::new(20, 25).expect("valid span"),
        );
        let early = CompilerDiagnostic::new(
            CompilerDiagnosticCode::DuplicateName,
            Span::new(3, 8).expect("valid span"),
        );
        let diagnostics = CompilerDiagnostics::new(vec![late.clone(), early.clone(), late.clone()])
            .expect("valid diagnostics");
        assert_eq!(diagnostics.as_slice(), &[early, late]);
    }

    #[test]
    fn external_messages_are_bounded_and_related_spans_are_preserved() {
        let primary = Span::new(10, 20).expect("valid primary span");
        let related = Span::new(1, 9).expect("valid related span");
        let diagnostic =
            CompilerDiagnostic::new(CompilerDiagnosticCode::CommandToolNameCollision, primary)
                .with_related_span(related);
        assert_eq!(
            diagnostic.to_string(),
            "RDB-C203: two commands normalize to the same MCP command tool name"
        );
        assert_eq!(diagnostic.related_span(), Some(related));
    }

    #[test]
    fn bound_diagnostics_preserve_closed_resource_actual_maximum_and_span() {
        let primary = Span::new(30, 44).expect("valid command span");
        let diagnostic = CompilerDiagnostic::from_ir_error(
            riffdb_contract_ir::IrValidationError::LimitExceeded {
                kind: "command worst-case affected index prefixes",
                actual: 23_320,
                maximum: 4_096,
            },
            primary,
        );

        assert_eq!(diagnostic.code(), CompilerDiagnosticCode::BoundExceeded);
        assert_eq!(diagnostic.primary_span(), primary);
        let observation = diagnostic.bound().expect("known bound observation");
        assert_eq!(
            observation.resource(),
            CompilerBoundResource::CommandAffectedPrefixEpochs
        );
        assert_eq!(observation.actual(), 23_320);
        assert_eq!(observation.maximum(), 4_096);
        assert_eq!(
            diagnostic.to_string(),
            "RDB-C020: command_affected_prefix_epochs is 23320; maximum is 4096"
        );
    }

    #[test]
    fn unknown_ir_limit_kinds_are_redacted_without_losing_checked_counts() {
        let primary = Span::new(1, 2).expect("valid span");
        let diagnostic = CompilerDiagnostic::from_ir_error(
            riffdb_contract_ir::IrValidationError::LimitExceeded {
                kind: "internal implementation detail",
                actual: 9,
                maximum: 8,
            },
            primary,
        );

        assert_eq!(diagnostic.summary(), "compiled_artifact is 9; maximum is 8");
        assert_eq!(
            diagnostic.bound().map(CompilerBoundObservation::resource),
            Some(CompilerBoundResource::CompiledArtifact)
        );
    }

    #[test]
    fn public_diagnostic_registry_is_complete_unique_and_code_ordered() {
        assert_eq!(CompilerDiagnosticCode::ALL.len(), 49);
        let codes = CompilerDiagnosticCode::ALL.map(CompilerDiagnosticCode::as_str);
        assert!(codes.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(CompilerDiagnosticCode::ALL.iter().all(|code| {
            !code.summary().is_empty() && code.help().is_none_or(|help| !help.is_empty())
        }));
    }

    #[test]
    fn checked_diagnostic_fixture_covers_every_retained_code_once_with_spans() {
        let fixture = include_str!("../../../fixtures/compiler/diagnostics.txt");
        let mut covered = Vec::new();
        for line in fixture.lines() {
            let Some(code) = line.strip_prefix("expected=") else {
                continue;
            };
            covered.push(
                CompilerDiagnosticCode::ALL
                    .iter()
                    .copied()
                    .find(|candidate| candidate.as_str() == code)
                    .expect("fixture code is retained"),
            );
        }
        assert_eq!(covered, CompilerDiagnosticCode::ALL);
        for section in fixture.split("\n[").skip(1) {
            if section.starts_with("RDB-C") {
                assert!(section.contains("\nkind=semantic\n"));
                assert!(section.contains(".primary="));
                assert!(section.contains(".related="));
            }
        }
    }
}
