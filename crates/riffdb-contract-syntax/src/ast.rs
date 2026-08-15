//! Source-oriented syntax tree for contract grammar version 4.

pub use crate::span::{Span, Spanned};

/// One parsed source document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContractDocument {
    /// The document's single top-level contract.
    pub contract: Spanned<Contract>,
}

/// A contract declaration retaining source order and application version lexeme.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Contract {
    /// The contract's source-spelled identifier.
    pub name: Spanned<String>,
    /// The unsigned application-version lexeme.
    pub version: Spanned<String>,
    /// Top-level declarations in source order.
    pub declarations: Vec<Spanned<Declaration>>,
}

/// Grammar-version-1 top-level declarations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Declaration {
    /// A persistent entity schema.
    Entity(EntityDeclaration),
    /// A durable event schema.
    Event(EventDeclaration),
    /// A named enumeration.
    Enum(EnumDeclaration),
    /// An aggregate ownership declaration.
    Aggregate(AggregateDeclaration),
    /// A typed command declaration.
    Command(CommandDeclaration),
    /// A compiler-lowered aggregate-local workflow declaration.
    Workflow(WorkflowDeclaration),
    /// An event-derived projection declaration.
    Projection(ProjectionDeclaration),
    /// One compiler-visible principal fact schema.
    PrincipalFact(PrincipalFactDeclaration),
    /// One closed principal-aware entity row policy.
    RowPolicy(RowPolicyDeclaration),
}

/// One bounded current-capability fact available to row policies.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrincipalFactDeclaration {
    /// Symbolic fact name.
    pub name: Spanned<String>,
    /// Closed scalar or bounded-list public value type.
    pub ty: Spanned<TypeExpression>,
}

/// One named row policy attached to one entity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowPolicyDeclaration {
    /// Policy symbol.
    pub name: Spanned<String>,
    /// Protected entity symbol.
    pub entity: Spanned<String>,
    /// Nonempty closed operation rules.
    pub rules: Vec<Spanned<RowPolicyRule>>,
}

/// One operation-specific allow rule. Absence is deny.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowPolicyRule {
    /// Protected operation class.
    pub operation: Spanned<RowPolicyOperation>,
    /// Closed boolean predicate.
    pub expression: Spanned<RowPolicyExpression>,
}

/// Closed alpha row-policy operation classes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowPolicyOperation {
    /// Read one current row.
    Read,
    /// Create one proposed row.
    Create,
    /// Update one current row to one proposed row.
    Update,
    /// Delete one current row.
    Delete,
}

/// Source-oriented expression accepted only inside a row policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowPolicyExpression {
    /// A literal lexeme.
    Literal(Spanned<Literal>),
    /// A row, principal, or principal-fact path.
    Path(Spanned<Path>),
    /// A parenthesized policy expression.
    Parenthesized(Box<Spanned<RowPolicyExpression>>),
    /// Boolean negation.
    Not(Box<Spanned<RowPolicyExpression>>),
    /// A closed comparison or boolean operation.
    Binary {
        /// Left operand.
        left: Box<Spanned<RowPolicyExpression>>,
        /// Closed operator.
        operator: Spanned<BinaryOperator>,
        /// Right operand.
        right: Box<Spanned<RowPolicyExpression>>,
    },
    /// Membership in one compiler-declared bounded principal-fact list.
    In {
        /// Candidate scalar.
        needle: Box<Spanned<RowPolicyExpression>>,
        /// Bounded fact-list path.
        haystack: Box<Spanned<RowPolicyExpression>>,
    },
    /// Explicit null/existence test over one optional value.
    IsNull {
        /// Tested value.
        value: Box<Spanned<RowPolicyExpression>>,
        /// `true` for `is not null`.
        negated: bool,
    },
    /// One compiler-resolved indexed partition-local relationship probe.
    Exists {
        /// Related entity symbol.
        entity: Spanned<String>,
        /// Exact declared index symbol.
        index: Spanned<String>,
        /// Complete index arguments.
        arguments: Vec<Spanned<RowPolicyExpression>>,
    },
}

/// One compiler-visible workflow over one aggregate-owned entity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowDeclaration {
    /// The workflow symbol.
    pub name: Spanned<String>,
    /// The entity whose revision and state are protected.
    pub entity: Spanned<String>,
    /// The stored enum field containing authoritative workflow state.
    pub state_field: Spanned<String>,
    /// Compiler-owned state assigned to every ordinary create, when declared.
    pub initial_state: Option<Spanned<String>>,
    /// Legal directed transitions in source order.
    pub transitions: Vec<Spanned<WorkflowTransitionDeclaration>>,
    /// The optional aggregate-local fenced lease.
    pub lease: Option<Spanned<WorkflowLeaseDeclaration>>,
}

/// One named legal transition in a workflow graph.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowTransitionDeclaration {
    /// Transition symbol used by commands.
    pub name: Spanned<String>,
    /// Nonempty legal source-state set.
    pub source_states: Vec<Spanned<String>>,
    /// Exact destination state.
    pub destination: Spanned<String>,
}

/// Stored fields and duration bounds for one workflow lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowLeaseDeclaration {
    /// Lease symbol used by generated operations.
    pub name: Spanned<String>,
    /// Optional owner field.
    pub owner_field: Spanned<String>,
    /// Optional service-owned expiry field.
    pub expiry_field: Spanned<String>,
    /// Nonzero monotonically increasing fencing-token field.
    pub fencing_token_field: Spanned<String>,
    /// Optional bounded-attempt field.
    pub attempt_field: Option<Spanned<String>>,
    /// Inclusive minimum lease duration in seconds.
    pub minimum_duration_seconds: Spanned<String>,
    /// Inclusive maximum lease duration in seconds.
    pub maximum_duration_seconds: Spanned<String>,
}

/// A persistent entity declaration with source-ordered items.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntityDeclaration {
    /// The entity identifier.
    pub name: Spanned<String>,
    /// Entity items in source order.
    pub items: Vec<Spanned<EntityItem>>,
}

/// One item accepted inside an entity declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EntityItem {
    /// The entity's key declaration.
    Key(KeyDeclaration),
    /// A stored entity field.
    Field(FieldDeclaration),
    /// A named entity invariant.
    Invariant(InvariantDeclaration),
    /// An exact-prefix index declaration.
    Index(IndexDeclaration),
    /// A required same-partition unique key.
    Unique(UniqueDeclaration),
    /// A required same-partition relationship.
    Reference(ReferenceDeclaration),
    /// A declared vector field for nearest-neighbor search.
    VectorField(VectorFieldDeclaration),
    /// The only compiler-owned policy under which current state may be deleted.
    DeletePolicy(DeletePolicyDeclaration),
}

/// A closed checked-deletion policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeletePolicyDeclaration {
    /// The compiler must prove that no declared relationship targets this entity.
    NoInbound,
    /// One exact reverse index must prove that no current inbound row exists.
    Restrict {
        /// Entity containing the inbound relationship and reverse index.
        source_entity: Spanned<String>,
        /// Exact declared reverse-reference index.
        index: Spanned<String>,
    },
    /// Every direct inbound relationship is deleted under compiler-fixed bounds.
    Cascade {
        /// Exhaustive direct inbound relationships in source order.
        relationships: Vec<Spanned<CascadeRelationshipDeclaration>>,
    },
}

/// One exact direct inbound relationship admitted by a cascade policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CascadeRelationshipDeclaration {
    /// Entity containing the inbound relationship.
    pub source_entity: Spanned<String>,
    /// Exact relationship declared on `source_entity`.
    pub relationship: Spanned<String>,
    /// Entity qualifying the exact reverse index.
    pub index_entity: Spanned<String>,
    /// Exact reverse index declared on `source_entity`.
    pub index: Spanned<String>,
    /// Compiler-fixed maximum rows discovered through this relationship.
    pub maximum: Spanned<String>,
}

/// The typed fields forming an entity key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyDeclaration {
    /// Key fields in source order.
    pub fields: Vec<Spanned<TypedField>>,
}

/// A stored entity field declaration with its optional classification.
///
/// Only stored entity fields accept the contextual `secret` classification
/// modifier (ADR-0118). Key fields, event fields, and command inputs remain
/// plain [`TypedField`]s, so the classification is unrepresentable there by
/// construction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FieldDeclaration {
    /// Span of the contextual `secret` classification modifier, when present.
    pub secret: Option<Span>,
    /// The field identifier.
    pub name: Spanned<String>,
    /// The unresolved source type.
    pub ty: Spanned<TypeExpression>,
}

/// A source-spelled field name and unresolved type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedField {
    /// The field identifier.
    pub name: Spanned<String>,
    /// The unresolved source type.
    pub ty: Spanned<TypeExpression>,
}

/// A named invariant and its untyped expression.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvariantDeclaration {
    /// The invariant identifier.
    pub name: Spanned<String>,
    /// The invariant expression.
    pub expression: Spanned<Expression>,
}

/// A named entity index over source-spelled fields.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexDeclaration {
    /// The index identifier.
    pub name: Spanned<String>,
    /// Indexed field names in declared order.
    pub fields: Vec<Spanned<String>>,
    /// Compiler-visible physical encodings for selected logical fields.
    pub options: Vec<Spanned<IndexOption>>,
}

/// One closed operational-index encoding declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IndexOption {
    /// Record missing, explicit-null, and non-null as distinct key states.
    Presence {
        /// Indexed optional field receiving the discriminator.
        field: Spanned<String>,
    },
    /// Encode one string field through a versioned text-key profile.
    TextKey {
        /// Indexed string field receiving the transform.
        field: Spanned<String>,
        /// Exact transform profile.
        profile: Spanned<TextKeyProfile>,
    },
}

/// Closed alpha text-key profile names.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TextKeyProfile {
    /// Exact source UTF-8 bytes.
    BinaryUtf8V1,
    /// Unicode 17.0.0 compatibility normalization plus full non-Turkic fold.
    UnicodeFoldV1,
}

/// A named same-partition unique key over stored required fields.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UniqueDeclaration {
    /// The unique-key identifier.
    pub name: Spanned<String>,
    /// Unique field names in canonical declared order.
    pub fields: Vec<Spanned<String>>,
}

/// A required relationship from stored source fields to one complete target key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReferenceDeclaration {
    /// Relationship name scoped to the source entity.
    pub name: Spanned<String>,
    /// Stored source fields in target-key component order.
    pub source_fields: Vec<Spanned<String>>,
    /// Target entity name.
    pub target_entity: Spanned<String>,
    /// Complete target primary-key fields in canonical order.
    pub target_fields: Vec<Spanned<String>>,
}

/// A declared vector field for nearest-neighbor search (ADR-0091).
///
/// Declares: name, dimension, distance metric, source fields (for staleness
/// tracking), and stale-entity count threshold.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorFieldDeclaration {
    /// The vector field identifier.
    pub name: Spanned<String>,
    /// Declared vector dimension (positive integer lexeme).
    pub dimension: Spanned<String>,
    /// Distance metric keyword.
    pub metric: Spanned<VectorMetricKeyword>,
    /// Source-field names whose mutation makes the embedding stale.
    pub source_fields: Vec<Spanned<String>>,
    /// Positive stale-entity count threshold lexeme.
    pub staleness_slo: Spanned<String>,
    /// Optional approximate-nearest-neighbor configuration. Its two fields
    /// are syntactically atomic: an ANN threshold can never exist without a
    /// declared recall target.
    pub ann: Option<VectorAnnDeclaration>,
}

/// Compiler-owned approximate-nearest-neighbor configuration (ADR-0091).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorAnnDeclaration {
    /// Per-organization row count above which ANN engages.
    pub row_threshold: Spanned<String>,
    /// Required recall ratio in integer basis points (`1..=10_000`).
    pub recall_target_bps: Spanned<String>,
}

/// The grammar's closed distance metric keywords.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum VectorMetricKeyword {
    /// Cosine similarity distance.
    Cosine,
    /// Euclidean (L2) distance.
    Euclidean,
    /// Negative dot product distance.
    DotProduct,
}

/// A durable event declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventDeclaration {
    /// The event identifier.
    pub name: Spanned<String>,
    /// Ordered payload fields forming the application-stream partition, when declared.
    pub partition_by: Option<Spanned<Vec<Spanned<String>>>>,
    /// Compiler-owned current-row authorization anchor, when declared.
    pub policy_anchor: Option<Spanned<EventPolicyAnchorDeclaration>>,
    /// Event payload fields in source order.
    pub fields: Vec<Spanned<TypedField>>,
}

/// One explicit mapping from an anchored entity key field to an event payload field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventPolicyAnchorField {
    /// Source entity partition/key field.
    pub entity_field: Spanned<String>,
    /// Event payload field carrying the exact key component.
    pub payload_field: Spanned<String>,
}

/// A compiler-owned current-row event authorization anchor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventPolicyAnchorDeclaration {
    /// Entity whose current read policy controls protected delivery.
    pub entity: Spanned<String>,
    /// Complete entity partition/key-to-payload mapping in source order.
    pub fields: Vec<Spanned<EventPolicyAnchorField>>,
}

/// A named enumeration declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnumDeclaration {
    /// The enumeration identifier.
    pub name: Spanned<String>,
    /// Variant identifiers in source order.
    pub variants: Vec<Spanned<String>>,
}

/// An aggregate ownership declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AggregateDeclaration {
    /// The aggregate identifier.
    pub name: Spanned<String>,
    /// Aggregate items in source order.
    pub items: Vec<Spanned<AggregateItem>>,
}

/// One item accepted inside an aggregate declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AggregateItem {
    /// The root entity name.
    Root(Spanned<String>),
    /// A child entity name.
    Child(Spanned<String>),
    /// The aggregate partition expression.
    PartitionBy(Spanned<Expression>),
    /// Conflict-key components in source order.
    ConflictKey(Vec<Spanned<Expression>>),
    /// A named aggregate invariant.
    Invariant(InvariantDeclaration),
}

/// Source type syntax. Bounds and name resolution belong to WP-040.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TypeExpression {
    /// The `bool` scalar type.
    Bool,
    /// The signed 64-bit integer scalar type.
    I64,
    /// The unsigned 64-bit integer scalar type.
    U64,
    /// The transaction timestamp scalar type.
    Timestamp,
    /// The calendar date scalar type.
    Date,
    /// The UUID scalar type.
    Uuid,
    /// A fixed-scale decimal type.
    Decimal {
        /// The unsigned precision lexeme.
        precision: Spanned<String>,
        /// The unsigned scale lexeme.
        scale: Spanned<String>,
    },
    /// A fixed-currency money type.
    Money {
        /// The three-letter currency lexeme.
        currency: Spanned<String>,
    },
    /// A bounded UTF-8 string type.
    String {
        /// The unsigned maximum-byte-length lexeme.
        maximum: Spanned<String>,
    },
    /// A bounded byte-string type.
    Bytes {
        /// The unsigned maximum-length lexeme.
        maximum: Spanned<String>,
    },
    /// An explicitly nullable nested type.
    Optional(Box<Spanned<TypeExpression>>),
    /// A bounded homogeneous list type.
    List {
        /// The unresolved element type.
        element: Box<Spanned<TypeExpression>>,
        /// Optional inclusive minimum item count. Absence preserves legacy `0..maximum`.
        minimum: Option<Spanned<String>>,
        /// The unsigned maximum-item-count lexeme.
        maximum: Spanned<String>,
    },
    /// An unresolved named type.
    Named(Spanned<String>),
}

/// A command declaration split into the grammar's fixed phases.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandDeclaration {
    /// Ordinary single-plan command or compiler-bounded collection command.
    pub kind: CommandKind,
    /// The command identifier.
    pub name: Spanned<String>,
    /// Input declarations in source order.
    pub inputs: Vec<Spanned<InputDeclaration>>,
    /// Service-owned deterministic values in source order.
    pub service_values: Vec<Spanned<ServiceValueDeclaration>>,
    /// The optional idempotency declaration.
    pub idempotency: Option<Spanned<IdempotencyClause>>,
    /// Up-front entity bindings in source order.
    pub bindings: Vec<Spanned<Binding>>,
    /// The one compiler-owned collection expansion, present only for bulk commands.
    pub bulk_iteration: Option<Spanned<BulkIteration>>,
    /// The compiler-owned exact-record construction, present only for reimport commands.
    pub reconstitution: Option<Spanned<ReconstitutionClause>>,
    /// Business preconditions in source order.
    pub requirements: Vec<Spanned<Requirement>>,
    /// Interleaved state and event effects in source order.
    pub effects: Vec<Spanned<Effect>>,
    /// The command's final success outcome.
    pub return_clause: Spanned<ReturnClause>,
}

/// Closed command declaration family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandKind {
    /// Existing scalar command semantics.
    Ordinary,
    /// One atomic compiler-bounded collection expansion.
    Bulk,
    /// Operator-only exact-record reconstitution; never an application command surface.
    Reimport,
}

/// The sole source-level operation accepted inside a reimport command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconstitutionClause {
    /// Exact entity record type being reconstructed.
    pub entity: Spanned<String>,
    /// Command input containing one record or one bounded list of records.
    pub source: Spanned<String>,
    /// Typed outcome selected when any target primary key already exists.
    pub failure: Spanned<OutcomeExpression>,
}

/// The sole source-level collection expansion accepted by a bulk command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BulkIteration {
    /// Local name of the current submitted element.
    pub element: Spanned<String>,
    /// Command list-input name expanded in submitted order.
    pub collection: Spanned<String>,
    /// Element-local entity bindings.
    pub bindings: Vec<Spanned<Binding>>,
    /// Element-local deterministic business requirements.
    pub requirements: Vec<Spanned<Requirement>>,
    /// Element-local state/event effects.
    pub effects: Vec<Spanned<Effect>>,
}

/// A command input declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InputDeclaration {
    /// The input field syntax.
    pub field: TypedField,
}

/// A named value observed by the service and sealed before evaluation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceValueDeclaration {
    /// Command-local value name.
    pub name: Spanned<String>,
    /// Closed source of the value.
    pub kind: Spanned<ServiceValueKind>,
}

/// Sources permitted for compiler-visible service-owned command values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceValueKind {
    /// A service-generated RFC 9562 UUIDv7 value.
    UuidV7,
    /// The exact admitted transaction time.
    TransactionTime,
}

/// The expression supplying a command's idempotency key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdempotencyClause {
    /// The untyped source expression.
    pub expression: Spanned<Expression>,
}

/// An up-front entity binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Binding {
    /// A read-only existing-entity binding.
    Read(EntityBinding),
    /// A mutable existing-entity binding.
    Mutate(EntityBinding),
    /// A mutable new-entity binding.
    Create(EntityBinding),
    /// A checked removal of one existing entity under its declared deletion policy.
    Delete(EntityBinding),
}

/// The common syntax of every entity binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntityBinding {
    /// The unresolved entity name.
    pub entity: Spanned<String>,
    /// Key expressions in source order.
    pub arguments: Vec<Spanned<Expression>>,
    /// The local binding name.
    pub binding: Spanned<String>,
    /// The outcome returned when an existing entity is absent or a new entity exists.
    pub failure: Spanned<OutcomeExpression>,
    /// The outcome returned when a checked delete is blocked by an inbound reference.
    ///
    /// This clause is accepted only on a delete binding whose entity declares a
    /// `restrict` deletion policy. Semantic validation rejects it everywhere else.
    pub restriction_failure: Option<Spanned<OutcomeExpression>>,
    /// The outcome returned when bounded cascade discovery observes `maximum + 1` rows.
    ///
    /// This clause is accepted only on a delete binding whose entity declares a
    /// `cascade` deletion policy. Semantic validation rejects it everywhere else.
    pub cascade_failure: Option<Spanned<OutcomeExpression>>,
}

/// A named business precondition and rejection outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Requirement {
    /// The requirement identifier.
    pub name: Spanned<String>,
    /// The Boolean condition expression.
    pub condition: Spanned<Expression>,
    /// The outcome returned when the condition is false.
    pub rejection: Spanned<OutcomeExpression>,
}

/// One effect in a command's ordered effect phase.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Effect {
    /// Assign a value to a bound entity path.
    Set(SetEffect),
    /// Emit one durable typed event.
    Emit(EmitEffect),
    /// Apply one declared revision-checked workflow transition.
    WorkflowTransition(WorkflowTransitionEffect),
    /// Apply one declared aggregate-local fenced lease operation.
    WorkflowLease(Box<WorkflowLeaseEffect>),
}

/// A revision-checked invocation of one declared workflow transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowTransitionEffect {
    /// Transition symbol resolved from the bound entity's workflow.
    pub transition: Spanned<String>,
    /// Mutable entity binding receiving the destination state.
    pub binding: Spanned<String>,
    /// Caller-observed exact entity revision.
    pub expected_revision: Spanned<Expression>,
    /// Declared result when the exact revision is stale.
    pub stale: Spanned<OutcomeExpression>,
    /// Declared result when current state is not a legal source.
    pub illegal: Spanned<OutcomeExpression>,
}

/// One compiler-visible operation over a declared workflow lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowLeaseEffect {
    /// Lease symbol resolved from the bound entity's workflow.
    pub lease: Spanned<String>,
    /// Mutable entity binding carrying the lease fields.
    pub binding: Spanned<String>,
    /// Closed operation and its required expressions/outcomes.
    pub operation: WorkflowLeaseOperation,
}

/// Closed source-level fenced lease operation family.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkflowLeaseOperation {
    /// Claim an unowned or transaction-time-expired lease.
    Claim {
        /// Required owner UUID input.
        owner: Spanned<Expression>,
        /// Required bounded duration-seconds input.
        duration_seconds: Spanned<Expression>,
        /// Required exact observed entity revision input.
        expected_revision: Spanned<Expression>,
        /// Revision mismatch outcome.
        stale: Spanned<OutcomeExpression>,
        /// Still-owned and unexpired outcome.
        unavailable: Spanned<OutcomeExpression>,
        /// Duration outside the declared bounds outcome.
        invalid: Spanned<OutcomeExpression>,
        /// Fence or attempt counter exhaustion outcome.
        exhausted: Spanned<OutcomeExpression>,
    },
    /// Renew a currently held, unexpired lease.
    Renew {
        /// Required exact owner UUID input.
        owner: Spanned<Expression>,
        /// Required exact current fencing-token input.
        fencing_token: Spanned<Expression>,
        /// Required bounded duration-seconds input.
        duration_seconds: Spanned<Expression>,
        /// Required exact observed entity revision input.
        expected_revision: Spanned<Expression>,
        /// Revision mismatch outcome.
        stale: Spanned<OutcomeExpression>,
        /// Owner or fencing-token mismatch outcome.
        invalid: Spanned<OutcomeExpression>,
        /// Already-expired lease outcome.
        expired: Spanned<OutcomeExpression>,
        /// Expiration arithmetic overflow outcome.
        exhausted: Spanned<OutcomeExpression>,
    },
    /// Release a currently held lease without resetting its fence.
    Release {
        /// Required exact owner UUID input.
        owner: Spanned<Expression>,
        /// Required exact current fencing-token input.
        fencing_token: Spanned<Expression>,
        /// Required exact observed entity revision input.
        expected_revision: Spanned<Expression>,
        /// Revision mismatch outcome.
        stale: Spanned<OutcomeExpression>,
        /// Owner or fencing-token mismatch outcome.
        invalid: Spanned<OutcomeExpression>,
    },
    /// Authoritatively clear a transaction-time-expired lease.
    Expire {
        /// Required exact observed entity revision input.
        expected_revision: Spanned<Expression>,
        /// Revision mismatch outcome.
        stale: Spanned<OutcomeExpression>,
        /// Unowned or not-yet-expired outcome.
        active: Spanned<OutcomeExpression>,
    },
    /// Fence one lease-protected mutation under the current holder.
    Fence {
        /// Required exact owner UUID input.
        owner: Spanned<Expression>,
        /// Required exact current fencing-token input.
        fencing_token: Spanned<Expression>,
        /// Required exact observed entity revision input.
        expected_revision: Spanned<Expression>,
        /// Revision mismatch outcome.
        stale: Spanned<OutcomeExpression>,
        /// Owner or fencing-token mismatch outcome.
        invalid: Spanned<OutcomeExpression>,
        /// Already-expired lease outcome.
        expired: Spanned<OutcomeExpression>,
    },
}

/// A source-level field assignment effect.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SetEffect {
    /// The assignment target path.
    pub target: Spanned<Path>,
    /// The assigned value expression.
    pub value: Spanned<Expression>,
    /// Explicit secret sources intentionally disclosed by this assignment.
    ///
    /// The compiler resolves each path to one secret-classified bound field;
    /// callers cannot add these annotations dynamically at execution time.
    pub reveals: Vec<Spanned<Path>>,
}

/// A source-level durable event emission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EmitEffect {
    /// The unresolved event name.
    pub event: Spanned<String>,
    /// The event payload object.
    pub payload: Spanned<ObjectLiteral>,
}

/// The final success return clause of a command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReturnClause {
    /// The declared success outcome.
    pub outcome: Spanned<OutcomeExpression>,
}

/// A typed outcome name and source payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutcomeExpression {
    /// The unresolved outcome name.
    pub name: Spanned<String>,
    /// The outcome payload object.
    pub payload: Spanned<ObjectLiteral>,
}

/// An object literal accepted only in outcome and event positions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectLiteral {
    /// Object fields in source order.
    pub fields: Vec<Spanned<ObjectField>>,
}

/// One source field in an object literal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectField {
    /// The field name.
    pub name: Spanned<String>,
    /// The field value expression.
    pub value: Spanned<Expression>,
    /// Explicit secret sources intentionally disclosed by this object field.
    pub reveals: Vec<Spanned<Path>>,
}

/// A bounded event-derived projection declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionDeclaration {
    /// The projection identifier.
    pub name: Spanned<String>,
    /// The unresolved source event name.
    pub source_event: Spanned<String>,
    /// The optional source-event filter.
    pub filter: Option<Spanned<Expression>>,
    /// Group-key expressions in source order.
    pub key: Vec<Spanned<Expression>>,
    /// Projection measures in source order.
    pub measures: Vec<Spanned<Measure>>,
    /// The declared projection frontier mode.
    pub frontier: Spanned<Frontier>,
}

/// A named projection aggregation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Measure {
    /// The measure identifier.
    pub name: Spanned<String>,
    /// The aggregation syntax.
    pub aggregation: Spanned<Aggregation>,
}

/// A grammar-version-1 projection aggregation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Aggregation {
    /// Count matching source events.
    Count,
    /// Sum an expression over matching source events.
    Sum(Spanned<Expression>),
}

/// A grammar-version-1 projection frontier declaration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Frontier {
    /// Process source commits in transaction order.
    TransactionallyOrdered,
}

/// An untyped, source-oriented expression.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Expression {
    /// A literal lexeme.
    Literal(Spanned<Literal>),
    /// An identifier path.
    Path(Spanned<Path>),
    /// A parenthesized expression retained as source shape.
    Parenthesized(Box<Spanned<Expression>>),
    /// A unary operation.
    Unary {
        /// The source operator.
        operator: Spanned<UnaryOperator>,
        /// The operand expression.
        operand: Box<Spanned<Expression>>,
    },
    /// A binary operation.
    Binary {
        /// The left operand.
        left: Box<Spanned<Expression>>,
        /// The source operator.
        operator: Spanned<BinaryOperator>,
        /// The right operand.
        right: Box<Spanned<Expression>>,
    },
}

/// A source literal whose numeric and string forms remain unresolved lexemes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Literal {
    /// A Boolean literal.
    Bool(bool),
    /// The explicit null literal.
    Null,
    /// An unsigned base-10 integer lexeme.
    UInt(String),
    /// A fixed decimal lexeme.
    FixedDecimal(String),
    /// A JSON string lexeme, including source quotes and escapes.
    String(String),
}

/// A nonempty source identifier path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Path {
    /// Path segments in source order.
    pub segments: Vec<Spanned<String>>,
}

/// A grammar-version-1 unary operator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnaryOperator {
    /// Boolean negation (`!`).
    Not,
    /// Numeric negation (`-`).
    Negate,
}

/// A grammar-version-1 binary operator in parsed precedence order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinaryOperator {
    /// Multiplication (`*`).
    Multiply,
    /// Division (`/`).
    Divide,
    /// Addition (`+`).
    Add,
    /// Subtraction (`-`).
    Subtract,
    /// Equality (`==`).
    Equal,
    /// Inequality (`!=`).
    NotEqual,
    /// Strictly less than (`<`).
    Less,
    /// Less than or equal (`<=`).
    LessEqual,
    /// Strictly greater than (`>`).
    Greater,
    /// Greater than or equal (`>=`).
    GreaterEqual,
    /// Boolean conjunction (`&&`).
    And,
    /// Boolean disjunction (`||`).
    Or,
}
