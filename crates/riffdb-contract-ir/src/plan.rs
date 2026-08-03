//! Checked forward-only executable command plans.

use std::collections::{BTreeMap, BTreeSet};

use riffdb_types::{
    AggregateTypeId, CommandId, ContractLineage, ContractVersion, EntityTypeId, EventTypeId,
    FieldId, IndexId, InvariantId, MAX_COMMAND_AFFECTED_INDEX_PREFIXES_V1,
    MAX_COMMAND_CONFLICT_KEYS_V1, MAX_COMMAND_INDEX_DELTAS_V1,
    MAX_COMMAND_READ_STATE_SEMANTIC_BYTES_V1, MAX_COMMAND_VALIDATION_TARGETS_V1, MAX_KEY_BYTES,
    OutcomeId, PlanHash,
};

use crate::{
    BindingId, CommandInputSchema, ExprId, ExpressionArena, ExpressionKind, FieldSchema,
    IrValidationError, KeyPurpose, KeySchema, RecordSchema, RecordTypeRef, RootValidationReadId,
    SchemaIr, ValueTypeTag, checked_len, validate_source_name,
};

/// Maximum bindings, instructions, fields, or outcomes in one owner.
pub const MAX_COMMAND_ITEMS: usize = 4_096;
/// Maximum fields in one encoded IR tuple or object construction.
pub const MAX_OBJECT_FIELDS: usize = 1_024;

/// An entity binding mode and its immutable v1 tag.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum BindingMode {
    /// Immutable entity read.
    Read = crate::format_registry::binding_mode::READ,
    /// Mutable existing entity.
    Mutate = crate::format_registry::binding_mode::MUTATE,
    /// Mutable new entity with an absence dependency.
    Create = crate::format_registry::binding_mode::CREATE,
}

/// One field expression in a typed event or outcome construction.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FieldExpression {
    field_id: FieldId,
    expression: ExprId,
}

impl FieldExpression {
    /// Creates one field-expression mapping.
    #[must_use]
    pub const fn new(field_id: FieldId, expression: ExprId) -> Self {
        Self {
            field_id,
            expression,
        }
    }

    /// Stable destination field.
    #[must_use]
    pub const fn field_id(self) -> FieldId {
        self.field_id
    }
    /// Typed source expression.
    #[must_use]
    pub const fn expression(self) -> ExprId {
        self.expression
    }
}

/// One canonical typed record construction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectConstruction {
    record: RecordTypeRef,
    fields: Vec<FieldExpression>,
}

impl ObjectConstruction {
    /// Creates a checked canonical field-ID ordered construction.
    pub fn new(
        record: RecordTypeRef,
        fields: Vec<FieldExpression>,
        schema: &RecordSchema,
        expressions: &ExpressionArena,
    ) -> Result<Self, IrValidationError> {
        checked_len(
            "object construction fields",
            fields.len(),
            MAX_OBJECT_FIELDS,
        )?;
        if schema.owner() != &record || fields.len() != schema.fields().len() {
            return Err(IrValidationError::TypeMismatch {
                context: "object construction shape",
            });
        }
        let mut previous = None;
        for (mapping, field) in fields.iter().zip(schema.fields()) {
            if previous.is_some_and(|id| id >= mapping.field_id)
                || mapping.field_id != field.id()
                || expressions
                    .get(mapping.expression)
                    .is_none_or(|node| !field.value_type().accepts_contextual(node.result_type()))
            {
                return Err(IrValidationError::TypeMismatch {
                    context: "object construction field",
                });
            }
            previous = Some(mapping.field_id);
        }
        Ok(Self { record, fields })
    }

    /// Destination record type.
    #[must_use]
    pub const fn record(&self) -> &RecordTypeRef {
        &self.record
    }
    /// Fields in stable-ID order.
    #[must_use]
    pub fn fields(&self) -> &[FieldExpression] {
        &self.fields
    }
}

/// One declared command outcome schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutcomeSchema {
    id: OutcomeId,
    name: String,
    payload: RecordSchema,
}

impl OutcomeSchema {
    /// Creates a checked outcome payload declaration.
    pub fn new(
        command_id: CommandId,
        id: OutcomeId,
        name: impl Into<String>,
        payload: RecordSchema,
    ) -> Result<Self, IrValidationError> {
        let name = name.into();
        validate_source_name(&name, "outcome")?;
        if payload.owner()
            != &(RecordTypeRef::CommandOutcome {
                command_id,
                outcome_id: id,
            })
            || payload.fields().iter().any(|field| field.name() == "type")
        {
            return Err(IrValidationError::InvalidName { kind: "outcome" });
        }
        Ok(Self { id, name, payload })
    }

    /// Stable outcome ID.
    #[must_use]
    pub const fn id(&self) -> OutcomeId {
        self.id
    }
    /// Exact source name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Complete payload schema.
    #[must_use]
    pub const fn payload(&self) -> &RecordSchema {
        &self.payload
    }
}

/// One typed declared outcome construction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutcomeConstruction {
    outcome_id: OutcomeId,
    payload: ObjectConstruction,
}

impl OutcomeConstruction {
    /// Creates a construction already checked against its declared payload schema.
    pub fn new(
        outcome: &OutcomeSchema,
        fields: Vec<FieldExpression>,
        expressions: &ExpressionArena,
    ) -> Result<Self, IrValidationError> {
        Ok(Self {
            outcome_id: outcome.id,
            payload: ObjectConstruction::new(
                outcome.payload.owner().clone(),
                fields,
                &outcome.payload,
                expressions,
            )?,
        })
    }

    /// Declared outcome ID.
    #[must_use]
    pub const fn outcome_id(&self) -> OutcomeId {
        self.outcome_id
    }
    /// Canonical payload construction.
    #[must_use]
    pub const fn payload(&self) -> &ObjectConstruction {
        &self.payload
    }
}

/// One up-front entity binding and declared binding-failure outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BindingPlan {
    id: BindingId,
    name: String,
    mode: BindingMode,
    entity_type: EntityTypeId,
    key_schema: KeySchema,
    key_expressions: Vec<ExprId>,
    accessed_fields: Vec<FieldId>,
    complete_record_access: bool,
    failure: OutcomeConstruction,
}

impl BindingPlan {
    /// Creates a checked binding. Command construction rechecks the full context.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: BindingId,
        name: impl Into<String>,
        mode: BindingMode,
        entity_type: EntityTypeId,
        key_schema: KeySchema,
        key_expressions: Vec<ExprId>,
        mut accessed_fields: Vec<FieldId>,
        complete_record_access: bool,
        failure: OutcomeConstruction,
    ) -> Result<Self, IrValidationError> {
        let name = name.into();
        validate_source_name(&name, "binding")?;
        if key_schema.purpose() != KeyPurpose::Entity(entity_type)
            || key_schema.components().len() != key_expressions.len()
        {
            return Err(IrValidationError::InvalidKey {
                reason: "binding key schema/owner/arity mismatch",
            });
        }
        accessed_fields.sort_unstable();
        if accessed_fields.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "binding accessed fields",
            });
        }
        Ok(Self {
            id,
            name,
            mode,
            entity_type,
            key_schema,
            key_expressions,
            accessed_fields,
            complete_record_access,
            failure,
        })
    }

    /// Dense binding ID.
    #[must_use]
    pub const fn id(&self) -> BindingId {
        self.id
    }
    /// Exact source alias.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Read/mutate/create mode.
    #[must_use]
    pub const fn mode(&self) -> BindingMode {
        self.mode
    }
    /// Stable entity type.
    #[must_use]
    pub const fn entity_type(&self) -> EntityTypeId {
        self.entity_type
    }
    /// Exact primary-key schema.
    #[must_use]
    pub const fn key_schema(&self) -> &KeySchema {
        &self.key_schema
    }
    /// Input-computable key expressions.
    #[must_use]
    pub fn key_expressions(&self) -> &[ExprId] {
        &self.key_expressions
    }
    /// Exact influential field reads in stable-ID order.
    #[must_use]
    pub fn accessed_fields(&self) -> &[FieldId] {
        &self.accessed_fields
    }
    /// Whether a complete record value is influential.
    #[must_use]
    pub const fn complete_record_access(&self) -> bool {
        self.complete_record_access
    }
    /// Declared missing/duplicate outcome.
    #[must_use]
    pub const fn failure(&self) -> &OutcomeConstruction {
        &self.failure
    }
}

/// One compiler-proved relationship change backed by an earlier exact target binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationshipCheckPlan {
    relationship_name: String,
    source_binding: BindingId,
    target_binding: BindingId,
}

/// One compiler-derived input-computable unique conflict capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UniqueConflictPlan {
    unique_name: String,
    index_id: IndexId,
    source_binding: BindingId,
    schema: KeySchema,
    expressions: Vec<ExprId>,
}

impl UniqueConflictPlan {
    /// Declared unique-key symbol.
    #[must_use]
    pub fn unique_name(&self) -> &str {
        &self.unique_name
    }
    /// Authoritative unique index.
    #[must_use]
    pub const fn index_id(&self) -> IndexId {
        self.index_id
    }
    /// Mutable binding establishing or changing the unique value.
    #[must_use]
    pub const fn source_binding(&self) -> BindingId {
        self.source_binding
    }
    /// Aggregate-scoped canonical conflict schema.
    #[must_use]
    pub const fn schema(&self) -> &KeySchema {
        &self.schema
    }
    /// Input-computable resulting unique components.
    #[must_use]
    pub fn expressions(&self) -> &[ExprId] {
        &self.expressions
    }
}

impl RelationshipCheckPlan {
    /// Exact relationship symbol.
    #[must_use]
    pub fn relationship_name(&self) -> &str {
        &self.relationship_name
    }
    /// Mutable binding establishing or changing the relationship.
    #[must_use]
    pub const fn source_binding(&self) -> BindingId {
        self.source_binding
    }
    /// Earlier exact read/mutate/create binding proving the target.
    #[must_use]
    pub const fn target_binding(&self) -> BindingId {
        self.target_binding
    }
}

/// One compiler-declared aggregate-root observation used only for commit validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootValidationReadPlan {
    id: RootValidationReadId,
    source_binding: BindingId,
    entity_type: EntityTypeId,
    key_schema: KeySchema,
    key_expressions: Vec<ExprId>,
    accessed_fields: Vec<FieldId>,
}

impl RootValidationReadPlan {
    /// Creates one checked root-validation read shape. Command construction rechecks context.
    pub fn new(
        id: RootValidationReadId,
        source_binding: BindingId,
        entity_type: EntityTypeId,
        key_schema: KeySchema,
        key_expressions: Vec<ExprId>,
        accessed_fields: Vec<FieldId>,
    ) -> Result<Self, IrValidationError> {
        if key_schema.purpose() != KeyPurpose::Entity(entity_type)
            || key_schema.components().len() != key_expressions.len()
        {
            return Err(IrValidationError::InvalidKey {
                reason: "root-validation key schema/owner/arity mismatch",
            });
        }
        if accessed_fields.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "root-validation accessed fields",
            });
        }
        Ok(Self {
            id,
            source_binding,
            entity_type,
            key_schema,
            key_expressions,
            accessed_fields,
        })
    }

    /// Dense root-validation read ID.
    #[must_use]
    pub const fn id(&self) -> RootValidationReadId {
        self.id
    }
    /// Lowest source-declared child binding requiring this read.
    #[must_use]
    pub const fn source_binding(&self) -> BindingId {
        self.source_binding
    }
    /// Aggregate-root entity type.
    #[must_use]
    pub const fn entity_type(&self) -> EntityTypeId {
        self.entity_type
    }
    /// Exact aggregate-root primary-key schema.
    #[must_use]
    pub const fn key_schema(&self) -> &KeySchema {
        &self.key_schema
    }
    /// Input/constant-computable root-key derivation.
    #[must_use]
    pub fn key_expressions(&self) -> &[ExprId] {
        &self.key_expressions
    }
    /// Exact influential root fields in stable-ID order.
    #[must_use]
    pub fn accessed_fields(&self) -> &[FieldId] {
        &self.accessed_fields
    }
}

/// One instantiated entity/aggregate commit validation predicate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitCheckPlan {
    invariant_id: InvariantId,
    predicate: ExprId,
    source_bindings: Vec<BindingId>,
    root_validation_reads: Vec<RootValidationReadId>,
}

impl CommitCheckPlan {
    /// Creates one checked Boolean predicate target.
    pub fn new(
        invariant_id: InvariantId,
        predicate: ExprId,
        source_bindings: Vec<BindingId>,
        root_validation_reads: Vec<RootValidationReadId>,
    ) -> Result<Self, IrValidationError> {
        if source_bindings.is_empty() && root_validation_reads.is_empty() {
            return Err(IrValidationError::Empty {
                kind: "commit-check application subjects",
            });
        }
        if source_bindings.windows(2).any(|pair| pair[0] >= pair[1])
            || root_validation_reads
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
        {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "commit-check application subjects",
            });
        }
        Ok(Self {
            invariant_id,
            predicate,
            source_bindings,
            root_validation_reads,
        })
    }
    /// Stable invariant ID.
    #[must_use]
    pub const fn invariant_id(&self) -> InvariantId {
        self.invariant_id
    }
    /// Boolean predicate.
    #[must_use]
    pub const fn predicate(&self) -> ExprId {
        self.predicate
    }
    /// Influential binding records.
    #[must_use]
    pub fn source_bindings(&self) -> &[BindingId] {
        &self.source_bindings
    }
    /// Influential internal root observations in ascending plan-local ID order.
    #[must_use]
    pub fn root_validation_reads(&self) -> &[RootValidationReadId] {
        &self.root_validation_reads
    }
}

/// One typed durable event construction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventConstruction {
    event_type: EventTypeId,
    payload: ObjectConstruction,
}

impl EventConstruction {
    /// Creates a checked event payload construction.
    pub fn new(
        event_type: EventTypeId,
        fields: Vec<FieldExpression>,
        schema: &SchemaIr,
        expressions: &ExpressionArena,
    ) -> Result<Self, IrValidationError> {
        let event = schema
            .event(event_type)
            .ok_or(IrValidationError::InvalidReference { kind: "event type" })?;
        Ok(Self {
            event_type,
            payload: ObjectConstruction::new(
                event.payload().owner().clone(),
                fields,
                event.payload(),
                expressions,
            )?,
        })
    }
    /// Stable event type.
    #[must_use]
    pub const fn event_type(&self) -> EventTypeId {
        self.event_type
    }
    /// Canonical event payload construction.
    #[must_use]
    pub const fn payload(&self) -> &ObjectConstruction {
        &self.payload
    }
}

/// Immutable forward-only v1 command instructions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Instruction {
    /// Evaluate a Boolean predicate; false returns its declared rejection.
    Require {
        /// Dense source-order requirement index.
        requirement_index: u32,
        /// Boolean predicate expression.
        predicate: ExprId,
        /// Declared business rejection.
        reject: OutcomeConstruction,
    },
    /// Set one mutable non-key field.
    SetField {
        /// Mutable binding.
        binding: BindingId,
        /// Stable destination field.
        field: FieldId,
        /// Typed right-hand expression.
        value: ExprId,
    },
    /// Capture one durable event occurrence.
    EmitEvent(EventConstruction),
    /// Return the one terminal success outcome.
    Return(OutcomeConstruction),
}

impl Instruction {
    pub(crate) fn tag(&self) -> u8 {
        match self {
            Self::Require { .. } => crate::format_registry::instruction::REQUIRE,
            Self::SetField { .. } => crate::format_registry::instruction::SET_FIELD,
            Self::EmitEvent(_) => crate::format_registry::instruction::EMIT_EVENT,
            Self::Return(_) => crate::format_registry::instruction::RETURN,
        }
    }
}

/// Closed execution classification.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum ExecutionClass {
    /// Unjournaled read-only command.
    ReadOnly = crate::format_registry::execution_class::READ_ONLY,
    /// Admitted idempotent mutation.
    IdempotentMutation = crate::format_registry::execution_class::IDEMPOTENT_MUTATION,
}

/// Fixed grammar-v1 retry semantics.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum RetryPolicy {
    /// Restart complete evaluation under the same admitted plan and bounded coordinator policy.
    BoundedFullReevaluation = crate::format_registry::retry_policy::BOUNDED_FULL_REEVALUATION,
}

/// Closed grammar-v1 capability requirement.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CapabilityRequirement {
    /// Derived permission to invoke this stable command.
    InvokeCommand {
        /// Exact contract lineage that scopes the stable command ID.
        lineage: ContractLineage,
        /// Stable command within the lineage.
        command_id: CommandId,
    },
}

impl CapabilityRequirement {
    /// Exact contract lineage scoped by this requirement.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        match self {
            Self::InvokeCommand { lineage, .. } => lineage,
        }
    }

    /// Stable command scoped by this requirement.
    #[must_use]
    pub const fn command_id(&self) -> CommandId {
        match self {
            Self::InvokeCommand { command_id, .. } => *command_id,
        }
    }
}

/// One input-computable conflict-key derivation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConflictDerivationPlan {
    schema: KeySchema,
    expressions: Vec<ExprId>,
}

impl ConflictDerivationPlan {
    /// Creates one exact conflict-domain derivation.
    pub fn new(schema: KeySchema, expressions: Vec<ExprId>) -> Result<Self, IrValidationError> {
        if !matches!(schema.purpose(), KeyPurpose::Conflict(_))
            || expressions.is_empty()
            || expressions.len() != schema.components().len()
        {
            return Err(IrValidationError::InvalidKey {
                reason: "invalid conflict derivation shape",
            });
        }
        Ok(Self {
            schema,
            expressions,
        })
    }
    /// Conflict key schema.
    #[must_use]
    pub const fn schema(&self) -> &KeySchema {
        &self.schema
    }
    /// Component expressions in semantic order.
    #[must_use]
    pub fn expressions(&self) -> &[ExprId] {
        &self.expressions
    }
}

/// Complete one-partition locality and upfront conflict plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalityPlan {
    aggregate_id: AggregateTypeId,
    partition_schema: KeySchema,
    partition_expression: ExprId,
    conflict_keys: Vec<ConflictDerivationPlan>,
}

impl LocalityPlan {
    /// Creates a checked one-aggregate locality declaration.
    pub fn new(
        aggregate_id: AggregateTypeId,
        partition_schema: KeySchema,
        partition_expression: ExprId,
        conflict_keys: Vec<ConflictDerivationPlan>,
    ) -> Result<Self, IrValidationError> {
        checked_len(
            "command conflict derivations",
            conflict_keys.len(),
            MAX_COMMAND_CONFLICT_KEYS_V1,
        )?;
        if partition_schema.purpose() != KeyPurpose::Partition(aggregate_id)
            || partition_schema.components().len() != 1
            || conflict_keys
                .iter()
                .any(|key| key.schema.purpose() != KeyPurpose::Conflict(aggregate_id))
        {
            return Err(IrValidationError::InvalidDependency {
                reason: "locality plan does not describe one complete aggregate partition",
            });
        }
        Ok(Self {
            aggregate_id,
            partition_schema,
            partition_expression,
            conflict_keys,
        })
    }
    /// Stable aggregate owner.
    #[must_use]
    pub const fn aggregate_id(&self) -> AggregateTypeId {
        self.aggregate_id
    }
    /// One-component partition schema.
    #[must_use]
    pub const fn partition_schema(&self) -> &KeySchema {
        &self.partition_schema
    }
    /// Input-computable partition expression.
    #[must_use]
    pub const fn partition_expression(&self) -> ExprId {
        self.partition_expression
    }
    /// Complete upfront conflict derivations.
    #[must_use]
    pub fn conflict_keys(&self) -> &[ConflictDerivationPlan] {
        &self.conflict_keys
    }
}

/// One immutable checked executable command plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandPlan {
    command_id: CommandId,
    name: String,
    contract_version: ContractVersion,
    input: CommandInputSchema,
    outcomes: Vec<OutcomeSchema>,
    success_outcome: OutcomeId,
    idempotency_input: Option<FieldId>,
    expressions: ExpressionArena,
    bindings: Vec<BindingPlan>,
    relationship_checks: Vec<RelationshipCheckPlan>,
    unique_conflicts: Vec<UniqueConflictPlan>,
    root_validation_reads: Vec<RootValidationReadPlan>,
    locality: LocalityPlan,
    commit_checks: Vec<CommitCheckPlan>,
    instructions: Vec<Instruction>,
    execution_class: ExecutionClass,
    retry_policy: RetryPolicy,
    required_capability: CapabilityRequirement,
    plan_hash: PlanHash,
}

impl CommandPlan {
    /// Creates, validates, and hashes a complete command plan.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        command_id: CommandId,
        contract_lineage: ContractLineage,
        name: impl Into<String>,
        contract_version: ContractVersion,
        input: CommandInputSchema,
        mut outcomes: Vec<OutcomeSchema>,
        success_outcome: OutcomeId,
        idempotency_input: Option<FieldId>,
        expressions: ExpressionArena,
        mut bindings: Vec<BindingPlan>,
        mut root_validation_reads: Vec<RootValidationReadPlan>,
        locality: LocalityPlan,
        mut commit_checks: Vec<CommitCheckPlan>,
        instructions: Vec<Instruction>,
        execution_class: ExecutionClass,
        contract_schema: &SchemaIr,
    ) -> Result<Self, IrValidationError> {
        let name = name.into();
        validate_source_name(&name, "command")?;
        if input.command_id() != command_id {
            return Err(IrValidationError::InvalidReference {
                kind: "command input",
            });
        }
        for field in input.record().fields() {
            crate::schema::validate_declared_field_type(field.value_type(), contract_schema)?;
        }
        checked_len("command outcomes", outcomes.len(), MAX_COMMAND_ITEMS)?;
        checked_len("command bindings", bindings.len(), MAX_COMMAND_ITEMS)?;
        checked_len(
            "command commit checks",
            commit_checks.len(),
            MAX_COMMAND_ITEMS,
        )?;
        checked_len(
            "command instructions",
            instructions.len(),
            MAX_COMMAND_ITEMS,
        )?;
        if outcomes.is_empty() || bindings.is_empty() || instructions.is_empty() {
            return Err(IrValidationError::Empty {
                kind: "command plan",
            });
        }
        for outcome in &outcomes {
            for field in outcome.payload().fields() {
                crate::schema::validate_payload_field_type(field.value_type(), contract_schema)?;
            }
        }
        outcomes.sort_unstable_by_key(OutcomeSchema::id);
        if outcomes.windows(2).any(|pair| pair[0].id == pair[1].id)
            || outcomes.iter().all(|outcome| outcome.id != success_outcome)
        {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "command outcomes",
            });
        }
        let mut outcome_names = BTreeSet::new();
        if outcomes
            .iter()
            .any(|outcome| !outcome_names.insert(outcome.name.as_str()))
        {
            return Err(IrValidationError::InvalidName {
                kind: "duplicate outcome",
            });
        }
        bindings.sort_unstable_by_key(BindingPlan::id);
        for (index, binding) in bindings.iter().enumerate() {
            if binding.id.get() as usize != index {
                return Err(IrValidationError::NonCanonicalOrder {
                    kind: "dense bindings",
                });
            }
        }
        checked_len(
            "command root-validation reads",
            root_validation_reads.len(),
            MAX_COMMAND_ITEMS,
        )?;
        root_validation_reads.sort_unstable_by_key(RootValidationReadPlan::id);
        for (index, read) in root_validation_reads.iter().enumerate() {
            if read.id.get() as usize != index {
                return Err(IrValidationError::NonCanonicalOrder {
                    kind: "dense root-validation reads",
                });
            }
        }
        commit_checks.sort_unstable_by(|left, right| {
            left.invariant_id
                .cmp(&right.invariant_id)
                .then_with(|| left.source_bindings.cmp(&right.source_bindings))
                .then_with(|| left.root_validation_reads.cmp(&right.root_validation_reads))
        });
        if commit_checks.windows(2).any(|pair| {
            pair[0].invariant_id == pair[1].invariant_id
                && pair[0].source_bindings == pair[1].source_bindings
                && pair[0].root_validation_reads == pair[1].root_validation_reads
        }) {
            return Err(IrValidationError::NonCanonicalOrder {
                kind: "commit checks",
            });
        }

        validate_declared_constructions(
            &expressions,
            &bindings,
            &instructions,
            &outcomes,
            success_outcome,
            contract_schema,
        )?;

        validate_expression_contexts(
            &expressions,
            &input,
            &bindings,
            &root_validation_reads,
            contract_schema,
        )?;
        contract_schema.validate_expression_enum_constants(&expressions)?;
        validate_binding_plans(&expressions, &bindings, &locality, contract_schema)?;
        validate_root_validation_reads(
            &expressions,
            &bindings,
            &root_validation_reads,
            &locality,
            contract_schema,
        )?;
        validate_locality(&expressions, &bindings, &locality, contract_schema)?;
        validate_commit_checks(
            &expressions,
            &commit_checks,
            &bindings,
            &root_validation_reads,
            &locality,
            contract_schema,
        )?;
        validate_instruction_stream(
            &expressions,
            &instructions,
            &bindings,
            &outcomes,
            success_outcome,
            contract_schema,
        )?;
        validate_root_validation_expression_uses(
            &expressions,
            &bindings,
            &locality,
            &instructions,
        )?;
        validate_read_dependencies(
            &expressions,
            &bindings,
            &root_validation_reads,
            &commit_checks,
            &instructions,
        )?;
        validate_command_expression_reachability(
            &expressions,
            &bindings,
            &root_validation_reads,
            &locality,
            &commit_checks,
            &instructions,
        )?;
        validate_idempotency(
            execution_class,
            idempotency_input,
            &input,
            &expressions,
            &bindings,
            &root_validation_reads,
            &locality,
            &commit_checks,
            &instructions,
        )?;
        if execution_class == ExecutionClass::IdempotentMutation
            && !bindings
                .iter()
                .any(|binding| matches!(binding.mode, BindingMode::Mutate | BindingMode::Create))
        {
            return Err(IrValidationError::InvalidDependency {
                reason: "mutating command has no mutable binding",
            });
        }
        if execution_class == ExecutionClass::ReadOnly
            && (bindings
                .iter()
                .any(|binding| binding.mode != BindingMode::Read)
                || instructions.iter().any(|instruction| {
                    matches!(
                        instruction,
                        Instruction::SetField { .. } | Instruction::EmitEvent(_)
                    )
                }))
        {
            return Err(IrValidationError::InvalidInstructionStream {
                reason: "read-only command contains a mutation or event",
            });
        }

        validate_worst_case_index_derivation(
            &bindings,
            &root_validation_reads,
            &instructions,
            locality.partition_schema().maximum_encoded_bytes(),
            contract_schema,
        )?;
        let relationship_checks =
            derive_relationship_checks(&expressions, &bindings, &instructions, contract_schema)?;
        let unique_conflicts =
            derive_unique_conflicts(&expressions, &bindings, &instructions, contract_schema)?;
        checked_len(
            "all command conflict derivations",
            locality
                .conflict_keys()
                .len()
                .checked_add(unique_conflicts.len())
                .ok_or(IrValidationError::SizeOverflow {
                    kind: "all command conflict derivations",
                })?,
            MAX_COMMAND_CONFLICT_KEYS_V1,
        )?;

        let mut plan = Self {
            command_id,
            name,
            contract_version,
            input,
            outcomes,
            success_outcome,
            idempotency_input,
            expressions,
            bindings,
            relationship_checks,
            unique_conflicts,
            root_validation_reads,
            locality,
            commit_checks,
            instructions,
            execution_class,
            retry_policy: RetryPolicy::BoundedFullReevaluation,
            required_capability: CapabilityRequirement::InvokeCommand {
                lineage: contract_lineage,
                command_id,
            },
            plan_hash: PlanHash::from_bytes([0; 32]),
        };
        plan.plan_hash = crate::bundle::compute_command_plan_hash(&plan, contract_schema)?;
        Ok(plan)
    }

    /// Stable command ID.
    #[must_use]
    pub const fn command_id(&self) -> CommandId {
        self.command_id
    }
    /// Exact source name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Application contract version.
    #[must_use]
    pub const fn contract_version(&self) -> ContractVersion {
        self.contract_version
    }
    /// Complete input record schema.
    #[must_use]
    pub const fn input(&self) -> &CommandInputSchema {
        &self.input
    }
    /// Outcomes in stable-ID order.
    #[must_use]
    pub fn outcomes(&self) -> &[OutcomeSchema] {
        &self.outcomes
    }
    /// Terminal success outcome.
    #[must_use]
    pub const fn success_outcome(&self) -> OutcomeId {
        self.success_outcome
    }
    /// Direct required UUID or bounded-string idempotency input for mutations.
    #[must_use]
    pub const fn idempotency_input(&self) -> Option<FieldId> {
        self.idempotency_input
    }
    /// Typed expression arena.
    #[must_use]
    pub const fn expressions(&self) -> &ExpressionArena {
        &self.expressions
    }
    /// Bindings in dense source order.
    #[must_use]
    pub fn bindings(&self) -> &[BindingPlan] {
        &self.bindings
    }
    /// Declared relationship changes and their ordinary exact-read dependencies.
    #[must_use]
    pub fn relationship_checks(&self) -> &[RelationshipCheckPlan] {
        &self.relationship_checks
    }
    /// Declared unique values and their exact input-derived conflict capabilities.
    #[must_use]
    pub fn unique_conflicts(&self) -> &[UniqueConflictPlan] {
        &self.unique_conflicts
    }
    /// Internal aggregate-root validation reads in dense ID order.
    #[must_use]
    pub fn root_validation_reads(&self) -> &[RootValidationReadPlan] {
        &self.root_validation_reads
    }
    /// One-partition conflict plan.
    #[must_use]
    pub const fn locality(&self) -> &LocalityPlan {
        &self.locality
    }
    /// Commit-time exact validation predicates.
    #[must_use]
    pub fn commit_checks(&self) -> &[CommitCheckPlan] {
        &self.commit_checks
    }
    /// Forward-only instruction stream.
    #[must_use]
    pub fn instructions(&self) -> &[Instruction] {
        &self.instructions
    }
    /// Read-only or admitted mutation classification.
    #[must_use]
    pub const fn execution_class(&self) -> ExecutionClass {
        self.execution_class
    }
    /// Fixed bounded full-reevaluation retry rule.
    #[must_use]
    pub const fn retry_policy(&self) -> RetryPolicy {
        self.retry_policy
    }
    /// Derived invoke-command capability.
    #[must_use]
    pub const fn required_capability(&self) -> &CapabilityRequirement {
        &self.required_capability
    }
    /// Typed transitive command-plan hash.
    #[must_use]
    pub const fn plan_hash(&self) -> PlanHash {
        self.plan_hash
    }
}

// Closed v1 storage framing charges are repeated here to preserve the
// IR/storage dependency direction.
const INDEX_PREFIX_TARGET_FIXED_SEMANTIC_BYTES_V1: usize = 14;
const AFFECTED_EPOCH_CURRENT_FIXED_SEMANTIC_BYTES_V1: usize = 4;
const MAX_INDEX_EPOCH_POSITION_SEMANTIC_BYTES_V1: usize = 9;
// This compiler proof ceiling is independent of the exact pre-sequence
// write-set budget, which may reject a concrete otherwise-valid plan later.
const INDEX_ENTRY_V2_PARTITION_LENGTH_SEMANTIC_BYTES: usize = 4;
const MAX_INDEX_ENTRY_V2_PARTITION_SEMANTIC_BYTES: usize =
    MAX_COMMAND_INDEX_DELTAS_V1 * (INDEX_ENTRY_V2_PARTITION_LENGTH_SEMANTIC_BYTES + MAX_KEY_BYTES);

fn derive_relationship_checks(
    expressions: &ExpressionArena,
    bindings: &[BindingPlan],
    instructions: &[Instruction],
    schema: &SchemaIr,
) -> Result<Vec<RelationshipCheckPlan>, IrValidationError> {
    let writes = instructions
        .iter()
        .filter_map(|instruction| match instruction {
            Instruction::SetField {
                binding,
                field,
                value,
            } => Some(((*binding, *field), *value)),
            Instruction::Require { .. } | Instruction::EmitEvent(_) | Instruction::Return(_) => {
                None
            }
        })
        .collect::<BTreeMap<_, _>>();
    let mut checks = Vec::new();
    for source_binding in bindings
        .iter()
        .filter(|binding| matches!(binding.mode(), BindingMode::Create | BindingMode::Mutate))
    {
        let source_entity = schema.entity(source_binding.entity_type()).ok_or(
            IrValidationError::InvalidReference {
                kind: "relationship source entity",
            },
        )?;
        for relationship in schema
            .relationships()
            .iter()
            .filter(|relationship| relationship.source_entity() == source_binding.entity_type())
        {
            let changes = source_binding.mode() == BindingMode::Create
                || relationship
                    .source_fields()
                    .iter()
                    .any(|field| writes.contains_key(&(source_binding.id(), *field)));
            if !changes {
                continue;
            }
            let resulting = relationship
                .source_fields()
                .iter()
                .map(|field| {
                    writes
                        .get(&(source_binding.id(), *field))
                        .copied()
                        .or_else(|| {
                            source_entity
                                .primary_key_fields()
                                .iter()
                                .position(|key| key == field)
                                .and_then(|position| {
                                    source_binding.key_expressions().get(position).copied()
                                })
                        })
                })
                .collect::<Option<Vec<_>>>()
                .ok_or(IrValidationError::InvalidDependency {
                    reason: "relationship target key is not statically visible",
                })?;
            let mut matching = None;
            for target in bindings {
                if target.id() >= source_binding.id()
                    || !matches!(
                        target.mode(),
                        BindingMode::Read | BindingMode::Mutate | BindingMode::Create
                    )
                    || target.entity_type() != relationship.target_entity()
                    || target.key_expressions().len() != resulting.len()
                {
                    continue;
                }
                let exact = target
                    .key_expressions()
                    .iter()
                    .zip(&resulting)
                    .map(|(left, right)| {
                        expression_trees_equal(expressions, *left, expressions, *right)
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .all(|equal| equal);
                if exact {
                    matching = Some(target.id());
                    break;
                }
            }
            let target_binding = matching.ok_or(IrValidationError::InvalidDependency {
                reason: "relationship change lacks dominating exact target binding",
            })?;
            checks.push(RelationshipCheckPlan {
                relationship_name: relationship.name().to_owned(),
                source_binding: source_binding.id(),
                target_binding,
            });
        }
    }
    checks.sort_unstable_by(|left, right| {
        left.source_binding
            .cmp(&right.source_binding)
            .then_with(|| left.relationship_name.cmp(&right.relationship_name))
    });
    Ok(checks)
}

fn derive_unique_conflicts(
    expressions: &ExpressionArena,
    bindings: &[BindingPlan],
    instructions: &[Instruction],
    schema: &SchemaIr,
) -> Result<Vec<UniqueConflictPlan>, IrValidationError> {
    let writes = instructions
        .iter()
        .filter_map(|instruction| match instruction {
            Instruction::SetField {
                binding,
                field,
                value,
            } => Some(((*binding, *field), *value)),
            Instruction::Require { .. } | Instruction::EmitEvent(_) | Instruction::Return(_) => {
                None
            }
        })
        .collect::<BTreeMap<_, _>>();
    let mut conflicts = Vec::new();
    for binding in bindings
        .iter()
        .filter(|binding| matches!(binding.mode(), BindingMode::Create | BindingMode::Mutate))
    {
        let entity =
            schema
                .entity(binding.entity_type())
                .ok_or(IrValidationError::InvalidReference {
                    kind: "unique conflict source entity",
                })?;
        let aggregate = schema.aggregate_for_entity(entity.id()).ok_or(
            IrValidationError::InvalidReference {
                kind: "unique conflict aggregate",
            },
        )?;
        for unique in schema
            .unique_keys()
            .iter()
            .filter(|unique| unique.source_entity() == entity.id())
        {
            let changes = binding.mode() == BindingMode::Create
                || unique
                    .fields()
                    .iter()
                    .any(|field| writes.contains_key(&(binding.id(), *field)));
            if !changes {
                continue;
            }
            let resulting = unique
                .fields()
                .iter()
                .map(|field| {
                    writes.get(&(binding.id(), *field)).copied().or_else(|| {
                        entity
                            .primary_key_fields()
                            .iter()
                            .position(|key| key == field)
                            .and_then(|position| binding.key_expressions().get(position).copied())
                    })
                })
                .collect::<Option<Vec<_>>>()
                .ok_or(IrValidationError::InvalidDependency {
                    reason: "unique conflict value is not statically visible",
                })?;
            let index = entity
                .indexes()
                .iter()
                .find(|index| index.id() == unique.index_id())
                .ok_or(IrValidationError::InvalidReference {
                    kind: "unique conflict backing index",
                })?;
            let conflict_schema = KeySchema::new(
                KeyPurpose::Conflict(aggregate.id()),
                index.key_schema().components().to_vec(),
            )?;
            for (expression, component) in resulting.iter().zip(conflict_schema.components()) {
                if expressions
                    .get(*expression)
                    .is_none_or(|node| node.result_type() != component.value_type())
                    || !expressions.dependencies(*expression)?.is_input_computable()
                {
                    return Err(IrValidationError::InvalidDependency {
                        reason: "unique conflict value is not input-computable",
                    });
                }
            }
            conflicts.push(UniqueConflictPlan {
                unique_name: unique.name().to_owned(),
                index_id: unique.index_id(),
                source_binding: binding.id(),
                schema: conflict_schema,
                expressions: resulting,
            });
        }
    }
    conflicts.sort_unstable_by(|left, right| {
        left.source_binding
            .cmp(&right.source_binding)
            .then_with(|| left.index_id.cmp(&right.index_id))
    });
    checked_len(
        "command unique conflict derivations",
        conflicts.len(),
        MAX_COMMAND_CONFLICT_KEYS_V1,
    )?;
    Ok(conflicts)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WorstCaseIndexDerivation {
    index_entry_deltas: usize,
    index_entry_puts: usize,
    index_entry_v2_partition_semantic_bytes: usize,
    affected_prefixes: usize,
    affected_target_bytes: usize,
    unique_occupancies: usize,
    unique_occupancy_bytes: usize,
}

fn validate_worst_case_index_derivation(
    bindings: &[BindingPlan],
    root_validation_reads: &[RootValidationReadPlan],
    instructions: &[Instruction],
    maximum_partition_key_bytes: usize,
    schema: &SchemaIr,
) -> Result<(), IrValidationError> {
    let derivation =
        worst_case_index_derivation(bindings, instructions, maximum_partition_key_bytes, schema)?;
    validate_worst_case_index_limits(derivation, bindings.len(), root_validation_reads.len())
}

fn worst_case_index_derivation(
    bindings: &[BindingPlan],
    instructions: &[Instruction],
    maximum_partition_key_bytes: usize,
    schema: &SchemaIr,
) -> Result<WorstCaseIndexDerivation, IrValidationError> {
    let mut assigned_fields = vec![BTreeSet::new(); bindings.len()];
    for instruction in instructions {
        if let Instruction::SetField { binding, field, .. } = instruction {
            assigned_fields
                .get_mut(binding.get() as usize)
                .ok_or(IrValidationError::InvalidReference {
                    kind: "instruction binding",
                })?
                .insert(*field);
        }
    }

    let mut index_entry_deltas = 0usize;
    let mut index_entry_puts = 0usize;
    let mut non_whole_prefixes = 0usize;
    let mut non_whole_prefix_bytes = 0usize;
    let mut affected_indexes = BTreeSet::<IndexId>::new();
    let unique_indexes = schema
        .unique_keys()
        .iter()
        .map(|unique| unique.index_id())
        .collect::<BTreeSet<_>>();
    let mut unique_occupancies = 0usize;
    let mut unique_occupancy_bytes = 0usize;

    for (binding, assigned) in bindings.iter().zip(&assigned_fields) {
        if binding.mode == BindingMode::Read {
            continue;
        }
        let entity =
            schema
                .entity(binding.entity_type)
                .ok_or(IrValidationError::InvalidReference {
                    kind: "binding entity",
                })?;
        for index in entity.indexes() {
            let earliest_changed_component = match binding.mode {
                BindingMode::Read => continue,
                BindingMode::Create => None,
                BindingMode::Mutate => {
                    let Some(position) = index
                        .fields()
                        .iter()
                        .position(|field| assigned.contains(field))
                    else {
                        continue;
                    };
                    Some(position)
                }
            };

            let entry_delta = if binding.mode == BindingMode::Create {
                1
            } else {
                2
            };
            index_entry_deltas = checked_index_derivation_add(index_entry_deltas, entry_delta)?;
            index_entry_puts = checked_index_derivation_add(index_entry_puts, 1)?;
            affected_indexes.insert(index.id());

            let mut cumulative_component_bytes = 0usize;
            for (position, component) in index.key_schema().components().iter().enumerate() {
                cumulative_component_bytes = checked_index_derivation_add(
                    cumulative_component_bytes,
                    component.maximum_payload_bytes(),
                )?;
                let prefix_bytes = checked_index_derivation_add(
                    INDEX_PREFIX_TARGET_FIXED_SEMANTIC_BYTES_V1,
                    cumulative_component_bytes,
                )?;
                let copies =
                    if earliest_changed_component.is_some_and(|earliest| position >= earliest) {
                        2
                    } else {
                        1
                    };
                non_whole_prefixes = checked_index_derivation_add(non_whole_prefixes, copies)?;
                non_whole_prefix_bytes = checked_index_derivation_add(
                    non_whole_prefix_bytes,
                    checked_index_derivation_mul(prefix_bytes, copies)?,
                )?;
            }
            if unique_indexes.contains(&index.id()) {
                unique_occupancies = checked_index_derivation_add(unique_occupancies, 1)?;
                unique_occupancy_bytes = checked_index_derivation_add(
                    unique_occupancy_bytes,
                    checked_index_derivation_add(
                        checked_index_derivation_add(
                            INDEX_PREFIX_TARGET_FIXED_SEMANTIC_BYTES_V1,
                            cumulative_component_bytes,
                        )?,
                        checked_index_derivation_add(MAX_KEY_BYTES, 5)?,
                    )?,
                )?;
            }
        }
    }

    let affected_prefixes =
        checked_index_derivation_add(non_whole_prefixes, affected_indexes.len())?;
    let affected_target_bytes = checked_index_derivation_add(
        non_whole_prefix_bytes,
        checked_index_derivation_mul(
            affected_indexes.len(),
            INDEX_PREFIX_TARGET_FIXED_SEMANTIC_BYTES_V1,
        )?,
    )?;
    let partition_semantic_bytes_per_put = checked_index_derivation_add(
        INDEX_ENTRY_V2_PARTITION_LENGTH_SEMANTIC_BYTES,
        maximum_partition_key_bytes,
    )?;
    let index_entry_v2_partition_semantic_bytes =
        checked_index_derivation_mul(index_entry_puts, partition_semantic_bytes_per_put)?;
    Ok(WorstCaseIndexDerivation {
        index_entry_deltas,
        index_entry_puts,
        index_entry_v2_partition_semantic_bytes,
        affected_prefixes,
        affected_target_bytes,
        unique_occupancies,
        unique_occupancy_bytes,
    })
}

fn validate_worst_case_index_limits(
    derivation: WorstCaseIndexDerivation,
    binding_count: usize,
    root_validation_read_count: usize,
) -> Result<(), IrValidationError> {
    checked_len(
        "command worst-case index entry deltas",
        derivation.index_entry_deltas,
        MAX_COMMAND_INDEX_DELTAS_V1,
    )?;
    if derivation.index_entry_puts > derivation.index_entry_deltas {
        return Err(IrValidationError::InvalidDependency {
            reason: "index put count exceeds complete index-delta count",
        });
    }
    checked_len(
        "command worst-case index entry puts",
        derivation.index_entry_puts,
        MAX_COMMAND_INDEX_DELTAS_V1,
    )?;
    checked_len(
        "command worst-case V2 index partition semantic bytes",
        derivation.index_entry_v2_partition_semantic_bytes,
        MAX_INDEX_ENTRY_V2_PARTITION_SEMANTIC_BYTES,
    )?;
    checked_len(
        "command worst-case affected index prefixes",
        derivation.affected_prefixes,
        MAX_COMMAND_AFFECTED_INDEX_PREFIXES_V1,
    )?;

    let validation_targets = checked_index_derivation_add(
        checked_index_derivation_add(
            checked_index_derivation_add(binding_count, root_validation_read_count)?,
            derivation.affected_prefixes,
        )?,
        derivation.unique_occupancies,
    )?;
    checked_len(
        "command worst-case validation targets",
        validation_targets,
        MAX_COMMAND_VALIDATION_TARGETS_V1,
    )?;

    let affected_epoch_state_bytes = checked_index_derivation_add(
        checked_index_derivation_add(
            AFFECTED_EPOCH_CURRENT_FIXED_SEMANTIC_BYTES_V1,
            derivation.affected_target_bytes,
        )?,
        checked_index_derivation_mul(
            derivation.affected_prefixes,
            MAX_INDEX_EPOCH_POSITION_SEMANTIC_BYTES_V1,
        )?,
    )?;
    let affected_epoch_state_bytes = checked_index_derivation_add(
        affected_epoch_state_bytes,
        derivation.unique_occupancy_bytes,
    )?;
    checked_len(
        "command worst-case affected epoch state bytes",
        affected_epoch_state_bytes,
        MAX_COMMAND_READ_STATE_SEMANTIC_BYTES_V1,
    )
}

fn checked_index_derivation_add(left: usize, right: usize) -> Result<usize, IrValidationError> {
    left.checked_add(right)
        .ok_or(IrValidationError::SizeOverflow {
            kind: "command worst-case index derivation",
        })
}

fn checked_index_derivation_mul(left: usize, right: usize) -> Result<usize, IrValidationError> {
    left.checked_mul(right)
        .ok_or(IrValidationError::SizeOverflow {
            kind: "command worst-case index derivation",
        })
}

fn validate_declared_constructions(
    arena: &ExpressionArena,
    bindings: &[BindingPlan],
    instructions: &[Instruction],
    outcomes: &[OutcomeSchema],
    success_outcome: OutcomeId,
    schema: &SchemaIr,
) -> Result<(), IrValidationError> {
    let validate_outcome = |construction: &OutcomeConstruction,
                            expected_success: bool|
     -> Result<(), IrValidationError> {
        let outcome = outcomes
            .iter()
            .find(|outcome| outcome.id == construction.outcome_id)
            .ok_or(IrValidationError::InvalidReference {
                kind: "constructed command outcome",
            })?;
        if (construction.outcome_id == success_outcome) != expected_success
            || construction.payload.record != *outcome.payload.owner()
        {
            return Err(IrValidationError::InvalidReference {
                kind: "constructed command outcome",
            });
        }
        ObjectConstruction::new(
            outcome.payload.owner().clone(),
            construction.payload.fields.clone(),
            &outcome.payload,
            arena,
        )?;
        Ok(())
    };
    for binding in bindings {
        validate_outcome(&binding.failure, false)?;
    }
    for instruction in instructions {
        match instruction {
            Instruction::Require { reject, .. } => validate_outcome(reject, false)?,
            Instruction::Return(outcome) => validate_outcome(outcome, true)?,
            Instruction::EmitEvent(event) => {
                let declared =
                    schema
                        .event(event.event_type)
                        .ok_or(IrValidationError::InvalidReference {
                            kind: "constructed event",
                        })?;
                if event.payload.record != *declared.payload().owner() {
                    return Err(IrValidationError::InvalidReference {
                        kind: "constructed event",
                    });
                }
                ObjectConstruction::new(
                    declared.payload().owner().clone(),
                    event.payload.fields.clone(),
                    declared.payload(),
                    arena,
                )?;
            }
            Instruction::SetField { .. } => {}
        }
    }
    Ok(())
}

fn validate_expression_contexts(
    arena: &ExpressionArena,
    input: &CommandInputSchema,
    bindings: &[BindingPlan],
    root_validation_reads: &[RootValidationReadPlan],
    schema: &SchemaIr,
) -> Result<(), IrValidationError> {
    for node in arena.nodes() {
        match node.kind() {
            ExpressionKind::InputField(field) => {
                let declared =
                    input
                        .record()
                        .field(*field)
                        .ok_or(IrValidationError::InvalidReference {
                            kind: "command input field",
                        })?;
                if declared.value_type() != node.result_type() {
                    return Err(IrValidationError::TypeMismatch {
                        context: "input field",
                    });
                }
            }
            ExpressionKind::CompleteBinding(binding) => {
                let binding = bindings.get(binding.get() as usize).ok_or(
                    IrValidationError::InvalidReference {
                        kind: "command binding",
                    },
                )?;
                if node.result_type().record_ref()
                    != Some(&RecordTypeRef::Entity(binding.entity_type))
                {
                    return Err(IrValidationError::TypeMismatch {
                        context: "complete binding",
                    });
                }
            }
            ExpressionKind::BoundField { binding, field } => {
                let binding = bindings.get(binding.get() as usize).ok_or(
                    IrValidationError::InvalidReference {
                        kind: "command binding",
                    },
                )?;
                let declared = schema
                    .entity(binding.entity_type)
                    .and_then(|entity| entity.record().field(*field))
                    .ok_or(IrValidationError::InvalidReference {
                        kind: "bound field",
                    })?;
                if declared.value_type() != node.result_type() {
                    return Err(IrValidationError::TypeMismatch {
                        context: "bound field",
                    });
                }
            }
            ExpressionKind::RootValidationField { read, field } => {
                let read = root_validation_reads.get(read.get() as usize).ok_or(
                    IrValidationError::InvalidReference {
                        kind: "root-validation read",
                    },
                )?;
                let declared = schema
                    .entity(read.entity_type)
                    .and_then(|entity| entity.record().field(*field))
                    .ok_or(IrValidationError::InvalidReference {
                        kind: "root-validation field",
                    })?;
                if read.accessed_fields.binary_search(field).is_err()
                    || declared.value_type() != node.result_type()
                {
                    return Err(IrValidationError::TypeMismatch {
                        context: "root-validation field",
                    });
                }
            }
            ExpressionKind::SourceEventField(_)
            | ExpressionKind::SchemaField { .. }
            | ExpressionKind::TransactionDate => {
                return Err(IrValidationError::InvalidDependency {
                    reason: "command expression contains a non-command context node",
                });
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_binding_plans(
    arena: &ExpressionArena,
    bindings: &[BindingPlan],
    locality: &LocalityPlan,
    schema: &SchemaIr,
) -> Result<(), IrValidationError> {
    for binding in bindings {
        let entity =
            schema
                .entity(binding.entity_type)
                .ok_or(IrValidationError::InvalidReference {
                    kind: "binding entity",
                })?;
        let owner = schema.aggregate_for_entity(binding.entity_type).ok_or(
            IrValidationError::InvalidReference {
                kind: "binding aggregate owner",
            },
        )?;
        if entity.primary_key() != &binding.key_schema
            || (matches!(binding.mode, BindingMode::Mutate | BindingMode::Create)
                && owner.id() != locality.aggregate_id)
        {
            return Err(IrValidationError::InvalidDependency {
                reason: "binding key mismatch or mutable binding is outside the locality aggregate",
            });
        }
        for (expression, component) in binding
            .key_expressions
            .iter()
            .zip(binding.key_schema.components())
        {
            let node = arena
                .get(*expression)
                .ok_or(IrValidationError::InvalidReference {
                    kind: "binding key expression",
                })?;
            if node.result_type() != component.value_type()
                || !arena.dependencies(*expression)?.is_input_computable()
            {
                return Err(IrValidationError::InvalidDependency {
                    reason: "binding key is not input-computable with the declared type",
                });
            }
        }
        if binding
            .accessed_fields
            .iter()
            .any(|field| entity.record().field(*field).is_none())
        {
            return Err(IrValidationError::InvalidReference {
                kind: "binding accessed field",
            });
        }
        for field in &binding.failure.payload.fields {
            let dependencies = arena.dependencies(field.expression)?;
            if !dependencies.is_input_computable() || dependencies.uses_transaction_time() {
                return Err(IrValidationError::InvalidDependency {
                    reason: "binding failure outcome is not input/constant-only",
                });
            }
        }
    }
    Ok(())
}

fn validate_root_validation_reads(
    arena: &ExpressionArena,
    bindings: &[BindingPlan],
    reads: &[RootValidationReadPlan],
    locality: &LocalityPlan,
    schema: &SchemaIr,
) -> Result<(), IrValidationError> {
    if bindings
        .len()
        .checked_add(reads.len())
        .is_none_or(|count| count > MAX_COMMAND_ITEMS)
    {
        return Err(IrValidationError::LimitExceeded {
            kind: "source-binding and root-validation targets",
            actual: bindings.len().saturating_add(reads.len()),
            maximum: MAX_COMMAND_ITEMS,
        });
    }
    let aggregate =
        schema
            .aggregate(locality.aggregate_id)
            .ok_or(IrValidationError::InvalidReference {
                kind: "root-validation aggregate",
            })?;
    let root = schema
        .entity(aggregate.root())
        .ok_or(IrValidationError::InvalidReference {
            kind: "root-validation entity",
        })?;

    let mut required = Vec::<(BindingId, Vec<ExprId>)>::new();
    if !aggregate.invariants().is_empty() {
        for binding in bindings.iter().filter(|binding| {
            matches!(binding.mode, BindingMode::Mutate | BindingMode::Create)
                && binding.entity_type != root.id()
        }) {
            let prefix = binding
                .key_expressions
                .iter()
                .take(root.primary_key_fields().len())
                .copied()
                .collect::<Vec<_>>();
            if prefix.len() != root.primary_key_fields().len() {
                return Err(IrValidationError::InvalidDependency {
                    reason: "child binding does not contain the complete aggregate-root key prefix",
                });
            }
            let mut supplied_by_source = false;
            for candidate in bindings
                .iter()
                .filter(|candidate| candidate.entity_type == root.id())
            {
                if expression_tuples_equal(arena, &candidate.key_expressions, arena, &prefix)? {
                    supplied_by_source = true;
                    break;
                }
            }
            if supplied_by_source {
                continue;
            }
            let mut grouped = false;
            for (_, expressions) in &required {
                if expression_tuples_equal(arena, expressions, arena, &prefix)? {
                    grouped = true;
                    break;
                }
            }
            if !grouped {
                required.push((binding.id, prefix));
            }
        }
    }

    if reads.len() != required.len() {
        return Err(IrValidationError::InvalidDependency {
            reason: "root-validation reads do not exactly cover mutable child root requirements",
        });
    }
    for (read, (expected_source, expected_expressions)) in reads.iter().zip(&required) {
        let source = bindings.get(read.source_binding.get() as usize).ok_or(
            IrValidationError::InvalidReference {
                kind: "root-validation source binding",
            },
        )?;
        if !matches!(source.mode, BindingMode::Mutate | BindingMode::Create)
            || source.entity_type == root.id()
            || read.entity_type != root.id()
            || read.key_schema != *root.primary_key()
            || read.key_expressions.len() != root.primary_key_fields().len()
            || read
                .accessed_fields
                .iter()
                .any(|field| root.record().field(*field).is_none())
        {
            return Err(IrValidationError::InvalidDependency {
                reason: "root-validation read does not identify the exact aggregate root",
            });
        }
        for (expression, component) in read
            .key_expressions
            .iter()
            .zip(read.key_schema.components())
        {
            if arena
                .get(*expression)
                .is_none_or(|node| node.result_type() != component.value_type())
                || !arena.dependencies(*expression)?.is_input_computable()
            {
                return Err(IrValidationError::InvalidDependency {
                    reason: "root-validation key is not input/constant-computable with the declared type",
                });
            }
        }
        if read.source_binding != *expected_source
            || !expression_tuples_equal(arena, &read.key_expressions, arena, expected_expressions)?
        {
            return Err(IrValidationError::InvalidDependency {
                reason: "root-validation reads are not canonically ordered by their lowest source binding or use a different checked root-key derivation",
            });
        }
    }
    Ok(())
}

fn validate_locality(
    arena: &ExpressionArena,
    bindings: &[BindingPlan],
    locality: &LocalityPlan,
    schema: &SchemaIr,
) -> Result<(), IrValidationError> {
    let aggregate =
        schema
            .aggregate(locality.aggregate_id)
            .ok_or(IrValidationError::InvalidReference {
                kind: "locality aggregate",
            })?;
    if locality.partition_schema != *aggregate.keys().partition_schema() {
        return Err(IrValidationError::InvalidDependency {
            reason: "locality partition schema does not equal the aggregate template",
        });
    }
    let partition =
        arena
            .get(locality.partition_expression)
            .ok_or(IrValidationError::InvalidReference {
                kind: "partition expression",
            })?;
    if partition.result_type() != locality.partition_schema.components()[0].value_type()
        || !arena
            .dependencies(locality.partition_expression)?
            .is_input_computable()
    {
        return Err(IrValidationError::InvalidDependency {
            reason: "partition derivation is not input-computable",
        });
    }
    let root = schema
        .entity(aggregate.root())
        .ok_or(IrValidationError::InvalidReference {
            kind: "aggregate root entity",
        })?;
    for binding in bindings {
        let binding_aggregate = schema.aggregate_for_entity(binding.entity_type).ok_or(
            IrValidationError::InvalidReference {
                kind: "binding aggregate owner",
            },
        )?;
        let binding_root =
            schema
                .entity(binding_aggregate.root())
                .ok_or(IrValidationError::InvalidReference {
                    kind: "binding aggregate root entity",
                })?;
        if !matches_key_template(
            binding_aggregate.keys().expressions(),
            binding_aggregate.keys().partition_expression(),
            arena,
            locality.partition_expression,
            binding_root,
            binding,
        )? {
            return Err(IrValidationError::InvalidDependency {
                reason: "binding aggregate partition does not equal the command partition derivation",
            });
        }
    }
    let mutable = bindings
        .iter()
        .filter(|binding| matches!(binding.mode, BindingMode::Mutate | BindingMode::Create))
        .collect::<Vec<_>>();
    if locality.conflict_keys.len() != mutable.len() {
        return Err(IrValidationError::InvalidDependency {
            reason: "conflict derivations do not exactly cover mutable bindings",
        });
    }
    for (conflict, binding) in locality.conflict_keys.iter().zip(mutable) {
        if conflict.schema != *aggregate.keys().conflict_schema() {
            return Err(IrValidationError::InvalidDependency {
                reason: "locality conflict schema does not equal the aggregate template",
            });
        }
        for (expression, component) in conflict
            .expressions
            .iter()
            .zip(conflict.schema.components())
        {
            if arena
                .get(*expression)
                .is_none_or(|node| node.result_type() != component.value_type())
                || !arena.dependencies(*expression)?.is_input_computable()
            {
                return Err(IrValidationError::InvalidDependency {
                    reason: "conflict derivation is not input-computable",
                });
            }
        }
        for (template, expression) in aggregate
            .keys()
            .conflict_expressions()
            .iter()
            .zip(&conflict.expressions)
        {
            if !matches_key_template(
                aggregate.keys().expressions(),
                *template,
                arena,
                *expression,
                root,
                binding,
            )? {
                return Err(IrValidationError::InvalidDependency {
                    reason: "conflict derivation is not the aggregate template instantiated for its mutable binding",
                });
            }
        }
    }
    Ok(())
}

fn matches_key_template(
    template_arena: &ExpressionArena,
    template_root: ExprId,
    command_arena: &ExpressionArena,
    command_root: ExprId,
    root_entity: &crate::EntitySchema,
    binding: &BindingPlan,
) -> Result<bool, IrValidationError> {
    let mut pending = vec![(template_root, command_root)];
    let mut visited = BTreeSet::new();
    while let Some((template_id, command_id)) = pending.pop() {
        if !visited.insert((template_id, command_id)) {
            continue;
        }
        checked_len(
            "expression comparison pairs",
            visited.len(),
            crate::MAX_EXPRESSION_NODES,
        )?;
        let template =
            template_arena
                .get(template_id)
                .ok_or(IrValidationError::InvalidReference {
                    kind: "aggregate key template expression",
                })?;
        let command = command_arena
            .get(command_id)
            .ok_or(IrValidationError::InvalidReference {
                kind: "instantiated aggregate key expression",
            })?;
        if template.result_type() != command.result_type() {
            return Ok(false);
        }
        match (template.kind(), command.kind()) {
            (ExpressionKind::Constant(left), ExpressionKind::Constant(right)) if left == right => {}
            (ExpressionKind::SchemaField { entity_type, field }, _) => {
                if *entity_type != root_entity.id() {
                    return Ok(false);
                }
                let Some(position) = root_entity
                    .primary_key_fields()
                    .iter()
                    .position(|candidate| candidate == field)
                else {
                    return Ok(false);
                };
                let Some(substitution) = binding.key_expressions.get(position) else {
                    return Ok(false);
                };
                if !expression_trees_equal(command_arena, *substitution, command_arena, command_id)?
                {
                    return Ok(false);
                }
            }
            (
                ExpressionKind::Unary {
                    operator: left_operator,
                    operand: left_operand,
                },
                ExpressionKind::Unary {
                    operator: right_operator,
                    operand: right_operand,
                },
            ) if left_operator == right_operator => pending.push((*left_operand, *right_operand)),
            (
                ExpressionKind::Binary {
                    operator: left_operator,
                    left: left_left,
                    right: left_right,
                },
                ExpressionKind::Binary {
                    operator: right_operator,
                    left: right_left,
                    right: right_right,
                },
            ) if left_operator == right_operator => {
                pending.push((*left_left, *right_left));
                pending.push((*left_right, *right_right));
            }
            _ => return Ok(false),
        }
    }
    Ok(true)
}

fn matches_invariant_template(
    template_arena: &ExpressionArena,
    template_root: ExprId,
    command_arena: &ExpressionArena,
    command_root: ExprId,
    binding: BindingId,
    expected_entity: EntityTypeId,
) -> Result<bool, IrValidationError> {
    let mut pending = vec![(template_root, command_root)];
    let mut visited = BTreeSet::new();
    while let Some((template_id, command_id)) = pending.pop() {
        if !visited.insert((template_id, command_id)) {
            continue;
        }
        checked_len(
            "expression comparison pairs",
            visited.len(),
            crate::MAX_EXPRESSION_NODES,
        )?;
        let template =
            template_arena
                .get(template_id)
                .ok_or(IrValidationError::InvalidReference {
                    kind: "invariant template expression",
                })?;
        let command = command_arena
            .get(command_id)
            .ok_or(IrValidationError::InvalidReference {
                kind: "instantiated invariant expression",
            })?;
        if template.result_type() != command.result_type() {
            return Ok(false);
        }
        match (template.kind(), command.kind()) {
            (ExpressionKind::Constant(left), ExpressionKind::Constant(right)) if left == right => {}
            (
                ExpressionKind::SchemaField {
                    entity_type,
                    field: expected_field,
                },
                ExpressionKind::BoundField {
                    binding: actual_binding,
                    field: actual_field,
                },
            ) if *entity_type == expected_entity
                && *actual_binding == binding
                && expected_field == actual_field => {}
            (
                ExpressionKind::Unary {
                    operator: left_operator,
                    operand: left_operand,
                },
                ExpressionKind::Unary {
                    operator: right_operator,
                    operand: right_operand,
                },
            ) if left_operator == right_operator => pending.push((*left_operand, *right_operand)),
            (
                ExpressionKind::Binary {
                    operator: left_operator,
                    left: left_left,
                    right: left_right,
                },
                ExpressionKind::Binary {
                    operator: right_operator,
                    left: right_left,
                    right: right_right,
                },
            ) if left_operator == right_operator => {
                pending.push((*left_left, *right_left));
                pending.push((*left_right, *right_right));
            }
            _ => return Ok(false),
        }
    }
    Ok(true)
}

fn matches_root_invariant_template(
    template_arena: &ExpressionArena,
    template_root: ExprId,
    command_arena: &ExpressionArena,
    command_root: ExprId,
    read: RootValidationReadId,
    expected_entity: EntityTypeId,
) -> Result<bool, IrValidationError> {
    let mut pending = vec![(template_root, command_root)];
    let mut visited = BTreeSet::new();
    while let Some((template_id, command_id)) = pending.pop() {
        if !visited.insert((template_id, command_id)) {
            continue;
        }
        checked_len(
            "expression comparison pairs",
            visited.len(),
            crate::MAX_EXPRESSION_NODES,
        )?;
        let template =
            template_arena
                .get(template_id)
                .ok_or(IrValidationError::InvalidReference {
                    kind: "aggregate invariant template expression",
                })?;
        let command = command_arena
            .get(command_id)
            .ok_or(IrValidationError::InvalidReference {
                kind: "instantiated root-validation expression",
            })?;
        if template.result_type() != command.result_type() {
            return Ok(false);
        }
        match (template.kind(), command.kind()) {
            (ExpressionKind::Constant(left), ExpressionKind::Constant(right)) if left == right => {}
            (
                ExpressionKind::SchemaField {
                    entity_type,
                    field: expected_field,
                },
                ExpressionKind::RootValidationField {
                    read: actual_read,
                    field: actual_field,
                },
            ) if *entity_type == expected_entity
                && *actual_read == read
                && expected_field == actual_field => {}
            (
                ExpressionKind::Unary {
                    operator: left_operator,
                    operand: left_operand,
                },
                ExpressionKind::Unary {
                    operator: right_operator,
                    operand: right_operand,
                },
            ) if left_operator == right_operator => pending.push((*left_operand, *right_operand)),
            (
                ExpressionKind::Binary {
                    operator: left_operator,
                    left: left_left,
                    right: left_right,
                },
                ExpressionKind::Binary {
                    operator: right_operator,
                    left: right_left,
                    right: right_right,
                },
            ) if left_operator == right_operator => {
                pending.push((*left_left, *right_left));
                pending.push((*left_right, *right_right));
            }
            _ => return Ok(false),
        }
    }
    Ok(true)
}

fn expression_trees_equal(
    left_arena: &ExpressionArena,
    left_root: ExprId,
    right_arena: &ExpressionArena,
    right_root: ExprId,
) -> Result<bool, IrValidationError> {
    let mut pending = vec![(left_root, right_root)];
    let mut visited = BTreeSet::new();
    while let Some((left_id, right_id)) = pending.pop() {
        if !visited.insert((left_id, right_id)) {
            continue;
        }
        checked_len(
            "expression comparison pairs",
            visited.len(),
            crate::MAX_EXPRESSION_NODES,
        )?;
        let left = left_arena
            .get(left_id)
            .ok_or(IrValidationError::InvalidReference {
                kind: "expression comparison",
            })?;
        let right = right_arena
            .get(right_id)
            .ok_or(IrValidationError::InvalidReference {
                kind: "expression comparison",
            })?;
        if left.result_type() != right.result_type() {
            return Ok(false);
        }
        match (left.kind(), right.kind()) {
            (ExpressionKind::Constant(left), ExpressionKind::Constant(right)) if left == right => {}
            (ExpressionKind::InputField(left), ExpressionKind::InputField(right))
                if left == right => {}
            (ExpressionKind::CompleteBinding(left), ExpressionKind::CompleteBinding(right))
                if left == right => {}
            (
                ExpressionKind::BoundField {
                    binding: left_binding,
                    field: left_field,
                },
                ExpressionKind::BoundField {
                    binding: right_binding,
                    field: right_field,
                },
            ) if left_binding == right_binding && left_field == right_field => {}
            (
                ExpressionKind::SchemaField {
                    entity_type: left_entity,
                    field: left_field,
                },
                ExpressionKind::SchemaField {
                    entity_type: right_entity,
                    field: right_field,
                },
            ) if left_entity == right_entity && left_field == right_field => {}
            (
                ExpressionKind::RootValidationField {
                    read: left_read,
                    field: left_field,
                },
                ExpressionKind::RootValidationField {
                    read: right_read,
                    field: right_field,
                },
            ) if left_read == right_read && left_field == right_field => {}
            (ExpressionKind::SourceEventField(left), ExpressionKind::SourceEventField(right))
                if left == right => {}
            (ExpressionKind::TransactionTime, ExpressionKind::TransactionTime)
            | (ExpressionKind::TransactionDate, ExpressionKind::TransactionDate) => {}
            (
                ExpressionKind::Unary {
                    operator: left_operator,
                    operand: left_operand,
                },
                ExpressionKind::Unary {
                    operator: right_operator,
                    operand: right_operand,
                },
            ) if left_operator == right_operator => pending.push((*left_operand, *right_operand)),
            (
                ExpressionKind::Binary {
                    operator: left_operator,
                    left: left_left,
                    right: left_right,
                },
                ExpressionKind::Binary {
                    operator: right_operator,
                    left: right_left,
                    right: right_right,
                },
            ) if left_operator == right_operator => {
                pending.push((*left_left, *right_left));
                pending.push((*left_right, *right_right));
            }
            _ => return Ok(false),
        }
    }
    Ok(true)
}

fn expression_tuples_equal(
    left_arena: &ExpressionArena,
    left: &[ExprId],
    right_arena: &ExpressionArena,
    right: &[ExprId],
) -> Result<bool, IrValidationError> {
    if left.len() != right.len() {
        return Ok(false);
    }
    for (left, right) in left.iter().zip(right) {
        if !expression_trees_equal(left_arena, *left, right_arena, *right)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn validate_commit_checks(
    arena: &ExpressionArena,
    checks: &[CommitCheckPlan],
    bindings: &[BindingPlan],
    root_validation_reads: &[RootValidationReadPlan],
    locality: &LocalityPlan,
    schema: &SchemaIr,
) -> Result<(), IrValidationError> {
    let aggregate =
        schema
            .aggregate(locality.aggregate_id)
            .ok_or(IrValidationError::InvalidReference {
                kind: "commit-check aggregate",
            })?;
    let root = schema
        .entity(aggregate.root())
        .ok_or(IrValidationError::InvalidReference {
            kind: "aggregate root entity",
        })?;
    for check in checks {
        if arena
            .get(check.predicate)
            .is_none_or(|node| node.result_type().tag() != ValueTypeTag::Bool)
            || check
                .source_bindings
                .iter()
                .any(|binding| binding.get() as usize >= bindings.len())
            || check
                .root_validation_reads
                .iter()
                .any(|read| read.get() as usize >= root_validation_reads.len())
        {
            return Err(IrValidationError::InvalidReference {
                kind: "commit check",
            });
        }
        let dependencies = arena.dependencies(check.predicate)?;
        if dependencies
            .bindings()
            .iter()
            .any(|binding| check.source_bindings.binary_search(binding).is_err())
            || dependencies
                .root_validation_reads()
                .iter()
                .any(|read| check.root_validation_reads.binary_search(read).is_err())
        {
            return Err(IrValidationError::InvalidDependency {
                reason: "commit-check predicate references a record outside its application subjects",
            });
        }
    }

    let mutable = bindings
        .iter()
        .filter(|binding| matches!(binding.mode, BindingMode::Mutate | BindingMode::Create))
        .collect::<Vec<_>>();
    let mut expected = Vec::new();
    for binding in &mutable {
        let entity =
            schema
                .entity(binding.entity_type)
                .ok_or(IrValidationError::InvalidReference {
                    kind: "commit-check entity",
                })?;
        for invariant in entity.invariants() {
            expected.push((
                invariant.id(),
                vec![binding.id],
                Vec::new(),
                invariant,
                binding.entity_type,
            ));
        }
    }

    if !aggregate.invariants().is_empty() {
        let mut root_subjects = BTreeSet::new();
        for binding in mutable {
            if binding.entity_type == aggregate.root() {
                root_subjects.insert((Some(binding.id), None));
                continue;
            }
            let prefix = binding
                .key_expressions
                .iter()
                .take(root.primary_key_fields().len())
                .copied()
                .collect::<Vec<_>>();
            let mut matching = Vec::new();
            for candidate in bindings
                .iter()
                .filter(|candidate| candidate.entity_type == aggregate.root())
            {
                if expression_tuples_equal(arena, &candidate.key_expressions, arena, &prefix)? {
                    matching.push(candidate.id());
                }
            }
            if matching.len() > 1 {
                return Err(IrValidationError::InvalidDependency {
                    reason: "child mutation has multiple exact source aggregate-root bindings",
                });
            }
            if let Some(binding) = matching.first() {
                root_subjects.insert((Some(*binding), None));
                continue;
            }
            let mut matching_reads = Vec::new();
            for read in root_validation_reads {
                if expression_tuples_equal(arena, &read.key_expressions, arena, &prefix)? {
                    matching_reads.push(read.id());
                }
            }
            if matching_reads.len() != 1 {
                return Err(IrValidationError::InvalidDependency {
                    reason: "child mutation requires one exact aggregate-root validation read",
                });
            }
            root_subjects.insert((None, Some(matching_reads[0])));
        }
        for (binding_id, read_id) in root_subjects {
            for invariant in aggregate.invariants() {
                expected.push((
                    invariant.id(),
                    binding_id.into_iter().collect::<Vec<_>>(),
                    read_id.into_iter().collect::<Vec<_>>(),
                    invariant,
                    aggregate.root(),
                ));
            }
        }
    }
    expected.sort_unstable_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
    });
    if checks.len() != expected.len() {
        return Err(IrValidationError::InvalidDependency {
            reason: "commit checks do not exactly cover applicable invariants",
        });
    }
    for (check, (invariant_id, invariant_bindings, invariant_reads, invariant, expected_entity)) in
        checks.iter().zip(expected)
    {
        let exact_template = if let Some(binding) = invariant_bindings.first() {
            matches_invariant_template(
                invariant.expressions(),
                invariant.predicate(),
                arena,
                check.predicate,
                *binding,
                expected_entity,
            )?
        } else if let Some(read) = invariant_reads.first() {
            matches_root_invariant_template(
                invariant.expressions(),
                invariant.predicate(),
                arena,
                check.predicate,
                *read,
                expected_entity,
            )?
        } else {
            false
        };
        if check.invariant_id != invariant_id
            || check.source_bindings != invariant_bindings
            || check.root_validation_reads != invariant_reads
            || !exact_template
        {
            return Err(IrValidationError::InvalidDependency {
                reason: "commit check is not the exact instantiated invariant template",
            });
        }
    }
    Ok(())
}

fn validate_instruction_stream(
    arena: &ExpressionArena,
    instructions: &[Instruction],
    bindings: &[BindingPlan],
    outcomes: &[OutcomeSchema],
    success_outcome: OutcomeId,
    schema: &SchemaIr,
) -> Result<(), IrValidationError> {
    if !matches!(instructions.last(), Some(Instruction::Return(value)) if value.outcome_id == success_outcome)
    {
        return Err(IrValidationError::InvalidInstructionStream {
            reason: "exactly one success return must be last",
        });
    }
    let mut effect_seen = false;
    let mut requirement_index = 0u32;
    let mut assigned = BTreeSet::new();
    let mut initialized = bindings
        .iter()
        .map(|binding| {
            let entity = schema
                .entity(binding.entity_type)
                .expect("validated binding entity");
            if binding.mode == BindingMode::Create {
                entity
                    .record()
                    .fields()
                    .iter()
                    .filter(|field| {
                        entity.primary_key_fields().contains(&field.id())
                            || field.value_type().is_optional()
                    })
                    .map(FieldSchema::id)
                    .collect::<BTreeSet<_>>()
            } else {
                entity
                    .record()
                    .fields()
                    .iter()
                    .map(FieldSchema::id)
                    .collect::<BTreeSet<_>>()
            }
        })
        .collect::<Vec<_>>();
    for (position, instruction) in instructions.iter().enumerate() {
        match instruction {
            Instruction::Require {
                requirement_index: actual,
                predicate,
                reject,
            } => {
                ensure_initialized_reads(arena, *predicate, bindings, schema, &initialized)?;
                for field in &reject.payload.fields {
                    ensure_initialized_reads(
                        arena,
                        field.expression,
                        bindings,
                        schema,
                        &initialized,
                    )?;
                }
                if effect_seen
                    || *actual != requirement_index
                    || arena
                        .get(*predicate)
                        .is_none_or(|node| node.result_type().tag() != ValueTypeTag::Bool)
                    || !outcomes
                        .iter()
                        .any(|outcome| outcome.id == reject.outcome_id)
                    || reject.outcome_id == success_outcome
                {
                    return Err(IrValidationError::InvalidInstructionStream {
                        reason: "invalid requirement phase, index, predicate, or outcome",
                    });
                }
                requirement_index =
                    requirement_index
                        .checked_add(1)
                        .ok_or(IrValidationError::SizeOverflow {
                            kind: "requirements",
                        })?;
            }
            Instruction::SetField {
                binding,
                field,
                value,
            } => {
                effect_seen = true;
                ensure_initialized_reads(arena, *value, bindings, schema, &initialized)?;
                let binding_plan = bindings.get(binding.get() as usize).ok_or(
                    IrValidationError::InvalidReference {
                        kind: "set binding",
                    },
                )?;
                let entity = schema
                    .entity(binding_plan.entity_type)
                    .ok_or(IrValidationError::InvalidReference { kind: "set entity" })?;
                let destination = entity
                    .record()
                    .field(*field)
                    .ok_or(IrValidationError::InvalidReference { kind: "set field" })?;
                if binding_plan.mode == BindingMode::Read
                    || entity.primary_key_fields().contains(field)
                    || !assigned.insert((*binding, *field))
                    || arena.get(*value).is_none_or(|node| {
                        !destination
                            .value_type()
                            .accepts_contextual(node.result_type())
                    })
                {
                    return Err(IrValidationError::InvalidInstructionStream {
                        reason: "invalid, duplicate, key-field, or immutable set",
                    });
                }
                initialized[binding.get() as usize].insert(*field);
            }
            Instruction::EmitEvent(event) => {
                effect_seen = true;
                for field in &event.payload.fields {
                    ensure_initialized_reads(
                        arena,
                        field.expression,
                        bindings,
                        schema,
                        &initialized,
                    )?;
                }
                if schema.event(event.event_type).is_none() {
                    return Err(IrValidationError::InvalidReference {
                        kind: "emitted event",
                    });
                }
            }
            Instruction::Return(outcome) => {
                for field in &outcome.payload.fields {
                    ensure_initialized_reads(
                        arena,
                        field.expression,
                        bindings,
                        schema,
                        &initialized,
                    )?;
                }
                if position + 1 != instructions.len() || outcome.outcome_id != success_outcome {
                    return Err(IrValidationError::InvalidInstructionStream {
                        reason: "return is not the unique terminal success",
                    });
                }
            }
        }
    }
    for binding in bindings
        .iter()
        .filter(|binding| binding.mode == BindingMode::Create)
    {
        let entity = schema
            .entity(binding.entity_type)
            .expect("validated binding entity");
        for field in entity.record().fields().iter().filter(|field| {
            !entity.primary_key_fields().contains(&field.id()) && !field.value_type().is_optional()
        }) {
            if !assigned.contains(&(binding.id, field.id())) {
                return Err(IrValidationError::InvalidInstructionStream {
                    reason: "required create field is not definitely assigned exactly once",
                });
            }
        }
    }
    Ok(())
}

fn ensure_initialized_reads(
    arena: &ExpressionArena,
    expression: ExprId,
    bindings: &[BindingPlan],
    schema: &SchemaIr,
    initialized: &[BTreeSet<FieldId>],
) -> Result<(), IrValidationError> {
    let dependencies = arena.dependencies(expression)?;
    for (binding, field) in dependencies.bound_fields() {
        if initialized
            .get(binding.get() as usize)
            .is_none_or(|fields| !fields.contains(field))
        {
            return Err(IrValidationError::InvalidDependency {
                reason: "expression reads a create field before definite assignment",
            });
        }
    }
    for binding in dependencies.complete_bindings() {
        let index = binding.get() as usize;
        let plan = bindings
            .get(index)
            .ok_or(IrValidationError::InvalidReference {
                kind: "complete binding dependency",
            })?;
        let field_count = schema
            .entity(plan.entity_type)
            .ok_or(IrValidationError::InvalidReference {
                kind: "complete binding entity",
            })?
            .record()
            .fields()
            .len();
        if initialized
            .get(index)
            .is_none_or(|fields| fields.len() != field_count)
        {
            return Err(IrValidationError::InvalidDependency {
                reason: "complete create binding is read before definite assignment",
            });
        }
    }
    Ok(())
}

fn validate_read_dependencies(
    arena: &ExpressionArena,
    bindings: &[BindingPlan],
    root_validation_reads: &[RootValidationReadPlan],
    checks: &[CommitCheckPlan],
    instructions: &[Instruction],
) -> Result<(), IrValidationError> {
    let mut fields = vec![BTreeSet::new(); bindings.len()];
    let mut complete = vec![false; bindings.len()];
    let mut root_fields = vec![BTreeSet::new(); root_validation_reads.len()];
    let mut include = |expression: ExprId| -> Result<(), IrValidationError> {
        let dependencies = arena.dependencies(expression)?;
        for binding in dependencies.complete_bindings() {
            complete[binding.get() as usize] = true;
        }
        for (binding, field) in dependencies.bound_fields() {
            fields[binding.get() as usize].insert(*field);
        }
        for (read, field) in dependencies.root_validation_fields() {
            root_fields[read.get() as usize].insert(*field);
        }
        Ok(())
    };
    for check in checks {
        include(check.predicate)?;
    }
    for binding in bindings {
        for field in &binding.failure.payload.fields {
            include(field.expression)?;
        }
    }
    for instruction in instructions {
        match instruction {
            Instruction::Require {
                predicate, reject, ..
            } => {
                include(*predicate)?;
                for field in &reject.payload.fields {
                    include(field.expression)?;
                }
            }
            Instruction::SetField { value, .. } => include(*value)?,
            Instruction::EmitEvent(event) => {
                for field in &event.payload.fields {
                    include(field.expression)?;
                }
            }
            Instruction::Return(outcome) => {
                for field in &outcome.payload.fields {
                    include(field.expression)?;
                }
            }
        }
    }
    for (index, binding) in bindings.iter().enumerate() {
        if binding.complete_record_access != complete[index]
            || binding
                .accessed_fields
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                != fields[index]
        {
            return Err(IrValidationError::InvalidDependency {
                reason: "binding accessed-field template does not equal influential reads",
            });
        }
    }
    for (index, read) in root_validation_reads.iter().enumerate() {
        if read
            .accessed_fields
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            != root_fields[index]
        {
            return Err(IrValidationError::InvalidDependency {
                reason: "root-validation accessed-field template does not equal influential commit-check reads",
            });
        }
    }
    Ok(())
}

fn validate_root_validation_expression_uses(
    arena: &ExpressionArena,
    bindings: &[BindingPlan],
    locality: &LocalityPlan,
    instructions: &[Instruction],
) -> Result<(), IrValidationError> {
    let reject = |expression: ExprId| -> Result<(), IrValidationError> {
        if arena
            .dependencies(expression)?
            .root_validation_reads()
            .is_empty()
        {
            Ok(())
        } else {
            Err(IrValidationError::InvalidDependency {
                reason: "root-validation fields are legal only in commit-check predicates",
            })
        }
    };
    for binding in bindings {
        for expression in &binding.key_expressions {
            reject(*expression)?;
        }
        for field in &binding.failure.payload.fields {
            reject(field.expression)?;
        }
    }
    reject(locality.partition_expression)?;
    for conflict in &locality.conflict_keys {
        for expression in &conflict.expressions {
            reject(*expression)?;
        }
    }
    for instruction in instructions {
        match instruction {
            Instruction::Require {
                predicate,
                reject: outcome,
                ..
            } => {
                reject(*predicate)?;
                for field in &outcome.payload.fields {
                    reject(field.expression)?;
                }
            }
            Instruction::SetField { value, .. } => reject(*value)?,
            Instruction::EmitEvent(event) => {
                for field in &event.payload.fields {
                    reject(field.expression)?;
                }
            }
            Instruction::Return(outcome) => {
                for field in &outcome.payload.fields {
                    reject(field.expression)?;
                }
            }
        }
    }
    Ok(())
}

fn validate_command_expression_reachability(
    arena: &ExpressionArena,
    bindings: &[BindingPlan],
    root_validation_reads: &[RootValidationReadPlan],
    locality: &LocalityPlan,
    checks: &[CommitCheckPlan],
    instructions: &[Instruction],
) -> Result<(), IrValidationError> {
    let mut roots = Vec::new();
    for binding in bindings {
        roots.extend(binding.key_expressions.iter().copied());
        roots.extend(
            binding
                .failure
                .payload
                .fields
                .iter()
                .map(|field| field.expression),
        );
    }
    for read in root_validation_reads {
        roots.extend(read.key_expressions.iter().copied());
    }
    roots.push(locality.partition_expression);
    for conflict in &locality.conflict_keys {
        roots.extend(conflict.expressions.iter().copied());
    }
    roots.extend(checks.iter().map(|check| check.predicate));
    for instruction in instructions {
        match instruction {
            Instruction::Require {
                predicate, reject, ..
            } => {
                roots.push(*predicate);
                roots.extend(reject.payload.fields.iter().map(|field| field.expression));
            }
            Instruction::SetField { value, .. } => roots.push(*value),
            Instruction::EmitEvent(event) => {
                roots.extend(event.payload.fields.iter().map(|field| field.expression))
            }
            Instruction::Return(outcome) => {
                roots.extend(outcome.payload.fields.iter().map(|field| field.expression))
            }
        }
    }
    arena.validate_reachable_from(&roots, "command expression root")
}

#[allow(clippy::too_many_arguments)]
fn validate_idempotency(
    execution_class: ExecutionClass,
    idempotency_input: Option<FieldId>,
    input: &CommandInputSchema,
    arena: &ExpressionArena,
    bindings: &[BindingPlan],
    root_validation_reads: &[RootValidationReadPlan],
    locality: &LocalityPlan,
    checks: &[CommitCheckPlan],
    instructions: &[Instruction],
) -> Result<(), IrValidationError> {
    if execution_class == ExecutionClass::ReadOnly {
        return if idempotency_input.is_none() {
            Ok(())
        } else {
            Err(IrValidationError::InvalidDependency {
                reason: "read-only command cannot declare durable idempotency",
            })
        };
    }
    let field_id = idempotency_input.ok_or(IrValidationError::InvalidDependency {
        reason: "mutating command requires one direct idempotency input",
    })?;
    let field = input
        .record()
        .field(field_id)
        .ok_or(IrValidationError::InvalidReference {
            kind: "idempotency input",
        })?;
    let supported = match field.value_type().tag() {
        ValueTypeTag::Uuid => true,
        ValueTypeTag::String => field
            .value_type()
            .byte_bound()
            .is_some_and(|bound| bound != 0 && bound <= 128),
        _ => false,
    };
    if !supported || field.value_type().is_optional() {
        return Err(IrValidationError::TypeMismatch {
            context: "idempotency input",
        });
    }
    let mut referenced = BTreeSet::new();
    let mut collect = |expression: ExprId| -> Result<(), IrValidationError> {
        referenced.extend(
            arena
                .dependencies(expression)?
                .input_fields()
                .iter()
                .copied(),
        );
        Ok(())
    };
    for binding in bindings {
        for expression in &binding.key_expressions {
            collect(*expression)?;
        }
        for field in &binding.failure.payload.fields {
            collect(field.expression)?;
        }
    }
    for read in root_validation_reads {
        for expression in &read.key_expressions {
            collect(*expression)?;
        }
    }
    collect(locality.partition_expression)?;
    for conflict in &locality.conflict_keys {
        for expression in &conflict.expressions {
            collect(*expression)?;
        }
    }
    for check in checks {
        collect(check.predicate)?;
    }
    for instruction in instructions {
        match instruction {
            Instruction::Require {
                predicate, reject, ..
            } => {
                collect(*predicate)?;
                for field in &reject.payload.fields {
                    collect(field.expression)?;
                }
            }
            Instruction::SetField { value, .. } => collect(*value)?,
            Instruction::EmitEvent(event) => {
                for field in &event.payload.fields {
                    collect(field.expression)?;
                }
            }
            Instruction::Return(outcome) => {
                for field in &outcome.payload.fields {
                    collect(field.expression)?;
                }
            }
        }
    }
    if referenced.contains(&field_id) {
        return Err(IrValidationError::InvalidDependency {
            reason: "secret idempotency input leaks into executable semantics",
        });
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use riffdb_types::{CanonicalValue, ContractVersion};

    pub(crate) fn test_lineage() -> ContractLineage {
        ContractLineage::new("PlanFixture").expect("lineage")
    }

    fn index_estimator_derivation(
        mode: BindingMode,
        assigned_fields: &[FieldId],
        binding_count: usize,
    ) -> WorstCaseIndexDerivation {
        let entity_id = EntityTypeId::first();
        let index_id = IndexId::first();
        let key_field = FieldId::first();
        let first_index_field = FieldId::new(2).expect("field");
        let middle_index_field = FieldId::new(3).expect("field");
        let last_index_field = FieldId::new(4).expect("field");
        let unrelated_field = FieldId::new(5).expect("field");
        let component =
            crate::KeyComponentSchema::new(crate::ValueType::u64(), vec![]).expect("component");
        let maximum_partition_key_bytes = KeySchema::new(
            KeyPurpose::Partition(AggregateTypeId::first()),
            vec![component.clone()],
        )
        .expect("partition key")
        .maximum_encoded_bytes();
        let entity_key = KeySchema::new(KeyPurpose::Entity(entity_id), vec![component.clone()])
            .expect("entity key");
        let index = crate::IndexSchema::new(
            index_id,
            "by_components",
            vec![first_index_field, middle_index_field, last_index_field],
            KeySchema::index(
                index_id,
                entity_id,
                vec![component.clone(), component.clone(), component],
                entity_key.clone(),
            )
            .expect("index key"),
        )
        .expect("index");
        let entity = crate::EntitySchema::new(
            entity_id,
            "Row",
            RecordSchema::new(
                RecordTypeRef::Entity(entity_id),
                vec![
                    FieldSchema::new(key_field, "id", crate::ValueType::u64()).expect("field"),
                    FieldSchema::new(first_index_field, "first", crate::ValueType::u64())
                        .expect("field"),
                    FieldSchema::new(middle_index_field, "middle", crate::ValueType::u64())
                        .expect("field"),
                    FieldSchema::new(last_index_field, "last", crate::ValueType::u64())
                        .expect("field"),
                    FieldSchema::new(unrelated_field, "enabled", crate::ValueType::bool())
                        .expect("field"),
                ],
            )
            .expect("record"),
            vec![key_field],
            entity_key.clone(),
            vec![],
            vec![index],
        )
        .expect("entity");
        let schema = SchemaIr::new(vec![entity], vec![], vec![], vec![]).expect("schema");

        let command_id = CommandId::first();
        let failure = OutcomeSchema::new(
            command_id,
            OutcomeId::first(),
            "Missing",
            RecordSchema::new(
                RecordTypeRef::CommandOutcome {
                    command_id,
                    outcome_id: OutcomeId::first(),
                },
                vec![],
            )
            .expect("failure record"),
        )
        .expect("failure");
        let expressions = ExpressionArena::new(vec![
            (
                ExpressionKind::Constant(CanonicalValue::U64(1)),
                crate::ValueType::u64(),
            ),
            (
                ExpressionKind::Constant(CanonicalValue::Bool(true)),
                crate::ValueType::bool(),
            ),
        ])
        .expect("expressions");
        let failure =
            OutcomeConstruction::new(&failure, vec![], &expressions).expect("failure outcome");
        let bindings = (0..binding_count)
            .map(|position| {
                BindingPlan::new(
                    BindingId::new(u32::try_from(position).expect("binding position fits u32")),
                    format!("row_{position}"),
                    mode,
                    entity_id,
                    entity_key.clone(),
                    vec![ExprId::new(0)],
                    vec![],
                    false,
                    failure.clone(),
                )
                .expect("binding")
            })
            .collect::<Vec<_>>();
        let instructions = (0..binding_count)
            .flat_map(|position| {
                assigned_fields
                    .iter()
                    .copied()
                    .map(move |field| Instruction::SetField {
                        binding: BindingId::new(
                            u32::try_from(position).expect("binding position fits u32"),
                        ),
                        field,
                        value: if field == unrelated_field {
                            ExprId::new(1)
                        } else {
                            ExprId::new(0)
                        },
                    })
            })
            .collect::<Vec<_>>();
        worst_case_index_derivation(
            &bindings,
            &instructions,
            maximum_partition_key_bytes,
            &schema,
        )
        .expect("derivation")
    }

    #[test]
    fn outcome_name_type_is_legal_but_payload_field_type_is_reserved() {
        let command_id = CommandId::first();
        let outcome_id = OutcomeId::first();
        let owner = RecordTypeRef::CommandOutcome {
            command_id,
            outcome_id,
        };
        assert!(
            OutcomeSchema::new(
                command_id,
                outcome_id,
                "type",
                RecordSchema::new(owner.clone(), vec![]).expect("record"),
            )
            .is_ok()
        );
        assert!(
            OutcomeSchema::new(
                command_id,
                outcome_id,
                "Applied",
                RecordSchema::new(
                    owner,
                    vec![
                        FieldSchema::new(FieldId::first(), "type", crate::ValueType::bool())
                            .expect("field")
                    ],
                )
                .expect("record"),
            )
            .is_err()
        );
    }

    #[test]
    fn locality_conflict_derivations_enforce_the_shared_v1_limit() {
        let aggregate_id = AggregateTypeId::first();
        let component =
            crate::KeyComponentSchema::new(crate::ValueType::u64(), vec![]).expect("component");
        let partition_schema =
            KeySchema::new(KeyPurpose::Partition(aggregate_id), vec![component.clone()])
                .expect("partition schema");
        let conflict_schema = KeySchema::new(KeyPurpose::Conflict(aggregate_id), vec![component])
            .expect("conflict schema");
        let conflict = ConflictDerivationPlan::new(conflict_schema, vec![ExprId::new(0)])
            .expect("conflict derivation");

        let accepted = LocalityPlan::new(
            aggregate_id,
            partition_schema.clone(),
            ExprId::new(0),
            vec![conflict.clone(); MAX_COMMAND_CONFLICT_KEYS_V1],
        )
        .expect("the inclusive v1 limit is accepted");
        assert_eq!(accepted.conflict_keys().len(), MAX_COMMAND_CONFLICT_KEYS_V1);

        assert_eq!(
            LocalityPlan::new(
                aggregate_id,
                partition_schema,
                ExprId::new(0),
                vec![conflict; MAX_COMMAND_CONFLICT_KEYS_V1 + 1],
            ),
            Err(IrValidationError::LimitExceeded {
                kind: "command conflict derivations",
                actual: MAX_COMMAND_CONFLICT_KEYS_V1 + 1,
                maximum: MAX_COMMAND_CONFLICT_KEYS_V1,
            })
        );
    }

    #[test]
    fn worst_case_index_limits_accept_exact_boundaries_and_reject_one_over_in_priority_order() {
        let empty = WorstCaseIndexDerivation {
            index_entry_deltas: 0,
            index_entry_puts: 0,
            index_entry_v2_partition_semantic_bytes: 0,
            affected_prefixes: 0,
            affected_target_bytes: 0,
            unique_occupancies: 0,
            unique_occupancy_bytes: 0,
        };
        assert!(
            validate_worst_case_index_limits(
                WorstCaseIndexDerivation {
                    index_entry_deltas: MAX_COMMAND_INDEX_DELTAS_V1,
                    index_entry_puts: MAX_COMMAND_INDEX_DELTAS_V1,
                    index_entry_v2_partition_semantic_bytes:
                        MAX_INDEX_ENTRY_V2_PARTITION_SEMANTIC_BYTES,
                    ..empty
                },
                0,
                0,
            )
            .is_ok()
        );
        assert_eq!(
            validate_worst_case_index_limits(
                WorstCaseIndexDerivation {
                    index_entry_deltas: MAX_COMMAND_INDEX_DELTAS_V1 + 1,
                    index_entry_puts: 0,
                    index_entry_v2_partition_semantic_bytes: 0,
                    affected_prefixes: MAX_COMMAND_AFFECTED_INDEX_PREFIXES_V1 + 1,
                    affected_target_bytes: 0,
                    unique_occupancies: 0,
                    unique_occupancy_bytes: 0,
                },
                0,
                0,
            ),
            Err(IrValidationError::LimitExceeded {
                kind: "command worst-case index entry deltas",
                actual: MAX_COMMAND_INDEX_DELTAS_V1 + 1,
                maximum: MAX_COMMAND_INDEX_DELTAS_V1,
            })
        );

        let exact_prefixes = WorstCaseIndexDerivation {
            affected_prefixes: MAX_COMMAND_AFFECTED_INDEX_PREFIXES_V1,
            ..empty
        };
        assert!(validate_worst_case_index_limits(exact_prefixes, 0, 0).is_ok());
        assert_eq!(
            validate_worst_case_index_limits(
                WorstCaseIndexDerivation {
                    affected_prefixes: MAX_COMMAND_AFFECTED_INDEX_PREFIXES_V1 + 1,
                    ..empty
                },
                0,
                0,
            ),
            Err(IrValidationError::LimitExceeded {
                kind: "command worst-case affected index prefixes",
                actual: MAX_COMMAND_AFFECTED_INDEX_PREFIXES_V1 + 1,
                maximum: MAX_COMMAND_AFFECTED_INDEX_PREFIXES_V1,
            })
        );

        assert!(
            validate_worst_case_index_limits(
                WorstCaseIndexDerivation {
                    affected_prefixes: MAX_COMMAND_VALIDATION_TARGETS_V1 - 1,
                    ..empty
                },
                1,
                0,
            )
            .is_ok()
        );
        assert_eq!(
            validate_worst_case_index_limits(
                WorstCaseIndexDerivation {
                    affected_prefixes: MAX_COMMAND_VALIDATION_TARGETS_V1 - 1,
                    ..empty
                },
                2,
                0,
            ),
            Err(IrValidationError::LimitExceeded {
                kind: "command worst-case validation targets",
                actual: MAX_COMMAND_VALIDATION_TARGETS_V1 + 1,
                maximum: MAX_COMMAND_VALIDATION_TARGETS_V1,
            })
        );

        let exact_state_target_bytes = MAX_COMMAND_READ_STATE_SEMANTIC_BYTES_V1
            - AFFECTED_EPOCH_CURRENT_FIXED_SEMANTIC_BYTES_V1
            - MAX_INDEX_EPOCH_POSITION_SEMANTIC_BYTES_V1;
        assert!(
            validate_worst_case_index_limits(
                WorstCaseIndexDerivation {
                    affected_prefixes: 1,
                    affected_target_bytes: exact_state_target_bytes,
                    ..empty
                },
                0,
                0,
            )
            .is_ok()
        );
        assert_eq!(
            validate_worst_case_index_limits(
                WorstCaseIndexDerivation {
                    affected_prefixes: 1,
                    affected_target_bytes: exact_state_target_bytes + 1,
                    ..empty
                },
                0,
                0,
            ),
            Err(IrValidationError::LimitExceeded {
                kind: "command worst-case affected epoch state bytes",
                actual: MAX_COMMAND_READ_STATE_SEMANTIC_BYTES_V1 + 1,
                maximum: MAX_COMMAND_READ_STATE_SEMANTIC_BYTES_V1,
            })
        );
        assert_eq!(
            checked_index_derivation_add(usize::MAX, 1),
            Err(IrValidationError::SizeOverflow {
                kind: "command worst-case index derivation",
            })
        );
    }

    #[test]
    fn v2_index_partition_charge_preserves_independent_count_and_write_set_limits() {
        const AGGREGATE_STAGED_WRITE_BYTES_V1: usize = 16 * 1024 * 1024;

        assert_eq!(MAX_INDEX_ENTRY_V2_PARTITION_SEMANTIC_BYTES, 16_793_600);
        assert_eq!(
            MAX_INDEX_ENTRY_V2_PARTITION_SEMANTIC_BYTES
                .checked_sub(AGGREGATE_STAGED_WRITE_BYTES_V1),
            Some(16_384),
            "the exact sequence-free write plan, not the compiler count ceiling, enforces the aggregate"
        );

        let empty = WorstCaseIndexDerivation {
            index_entry_deltas: 0,
            index_entry_puts: 0,
            index_entry_v2_partition_semantic_bytes: 0,
            affected_prefixes: 0,
            affected_target_bytes: 0,
            unique_occupancies: 0,
            unique_occupancy_bytes: 0,
        };
        assert_eq!(
            validate_worst_case_index_limits(
                WorstCaseIndexDerivation {
                    index_entry_deltas: 1,
                    index_entry_puts: 2,
                    ..empty
                },
                0,
                0,
            ),
            Err(IrValidationError::InvalidDependency {
                reason: "index put count exceeds complete index-delta count",
            })
        );
        assert_eq!(
            validate_worst_case_index_limits(
                WorstCaseIndexDerivation {
                    index_entry_deltas: 1,
                    index_entry_puts: 1,
                    index_entry_v2_partition_semantic_bytes:
                        MAX_INDEX_ENTRY_V2_PARTITION_SEMANTIC_BYTES + 1,
                    ..empty
                },
                0,
                0,
            ),
            Err(IrValidationError::LimitExceeded {
                kind: "command worst-case V2 index partition semantic bytes",
                actual: MAX_INDEX_ENTRY_V2_PARTITION_SEMANTIC_BYTES + 1,
                maximum: MAX_INDEX_ENTRY_V2_PARTITION_SEMANTIC_BYTES,
            })
        );
        assert_eq!(
            checked_index_derivation_mul(usize::MAX, 2),
            Err(IrValidationError::SizeOverflow {
                kind: "command worst-case index derivation",
            })
        );
    }

    #[test]
    fn worst_case_index_estimator_freezes_create_replace_and_shared_whole_prefix_formulas() {
        let first = FieldId::new(2).expect("field");
        let middle = FieldId::new(3).expect("field");
        let last = FieldId::new(4).expect("field");
        let unrelated = FieldId::new(5).expect("field");

        assert_eq!(
            index_estimator_derivation(BindingMode::Create, &[first, middle, last, unrelated], 1,),
            WorstCaseIndexDerivation {
                index_entry_deltas: 1,
                index_entry_puts: 1,
                index_entry_v2_partition_semantic_bytes: 18,
                affected_prefixes: 4,
                affected_target_bytes: 104,
                unique_occupancies: 0,
                unique_occupancy_bytes: 0,
            }
        );
        assert_eq!(
            index_estimator_derivation(BindingMode::Mutate, &[first], 1),
            WorstCaseIndexDerivation {
                index_entry_deltas: 2,
                index_entry_puts: 1,
                index_entry_v2_partition_semantic_bytes: 18,
                affected_prefixes: 7,
                affected_target_bytes: 194,
                unique_occupancies: 0,
                unique_occupancy_bytes: 0,
            }
        );
        assert_eq!(
            index_estimator_derivation(BindingMode::Mutate, &[middle], 1),
            WorstCaseIndexDerivation {
                index_entry_deltas: 2,
                index_entry_puts: 1,
                index_entry_v2_partition_semantic_bytes: 18,
                affected_prefixes: 6,
                affected_target_bytes: 172,
                unique_occupancies: 0,
                unique_occupancy_bytes: 0,
            }
        );
        assert_eq!(
            index_estimator_derivation(BindingMode::Mutate, &[last], 1),
            WorstCaseIndexDerivation {
                index_entry_deltas: 2,
                index_entry_puts: 1,
                index_entry_v2_partition_semantic_bytes: 18,
                affected_prefixes: 5,
                affected_target_bytes: 142,
                unique_occupancies: 0,
                unique_occupancy_bytes: 0,
            }
        );
        assert_eq!(
            index_estimator_derivation(BindingMode::Mutate, &[unrelated], 1),
            WorstCaseIndexDerivation {
                index_entry_deltas: 0,
                index_entry_puts: 0,
                index_entry_v2_partition_semantic_bytes: 0,
                affected_prefixes: 0,
                affected_target_bytes: 0,
                unique_occupancies: 0,
                unique_occupancy_bytes: 0,
            }
        );
        assert_eq!(
            index_estimator_derivation(BindingMode::Mutate, &[first], 2),
            WorstCaseIndexDerivation {
                index_entry_deltas: 4,
                index_entry_puts: 2,
                index_entry_v2_partition_semantic_bytes: 36,
                affected_prefixes: 13,
                affected_target_bytes: 374,
                unique_occupancies: 0,
                unique_occupancy_bytes: 0,
            }
        );
    }

    pub(crate) fn minimal_mutation() -> (CommandPlan, SchemaIr) {
        minimal_mutation_with_idempotency(crate::ValueType::string(64).expect("string"))
    }

    fn minimal_mutation_with_idempotency(
        idempotency_type: crate::ValueType,
    ) -> (CommandPlan, SchemaIr) {
        let entity_id = EntityTypeId::first();
        let aggregate_id = AggregateTypeId::first();
        let command_id = CommandId::first();
        let entity_key_field = FieldId::first();
        let entity_flag_field = FieldId::new(2).expect("field");
        let idempotency_field = FieldId::first();
        let input_key_field = FieldId::new(2).expect("field");
        let key_component =
            crate::KeyComponentSchema::new(crate::ValueType::u64(), vec![]).expect("component");
        let entity_key = KeySchema::new(KeyPurpose::Entity(entity_id), vec![key_component.clone()])
            .expect("entity key");
        let entity = crate::EntitySchema::new(
            entity_id,
            "Root",
            RecordSchema::new(
                RecordTypeRef::Entity(entity_id),
                vec![
                    FieldSchema::new(entity_key_field, "id", crate::ValueType::u64())
                        .expect("field"),
                    FieldSchema::new(
                        entity_flag_field,
                        "flag",
                        crate::ValueType::optional(crate::ValueType::bool()).expect("optional"),
                    )
                    .expect("field"),
                ],
            )
            .expect("record"),
            vec![entity_key_field],
            entity_key.clone(),
            vec![],
            vec![],
        )
        .expect("entity");
        let key_template = ExpressionArena::new(vec![(
            ExpressionKind::SchemaField {
                entity_type: entity_id,
                field: entity_key_field,
            },
            crate::ValueType::u64(),
        )])
        .expect("key template");
        let aggregate = crate::AggregateSchema::new(
            aggregate_id,
            "Aggregate",
            entity_id,
            vec![],
            crate::AggregateKeyPlan::new(
                key_template,
                ExprId::new(0),
                vec![ExprId::new(0)],
                KeySchema::new(
                    KeyPurpose::Partition(aggregate_id),
                    vec![key_component.clone()],
                )
                .expect("partition key"),
                KeySchema::new(KeyPurpose::Conflict(aggregate_id), vec![key_component])
                    .expect("conflict key"),
            )
            .expect("aggregate keys"),
            vec![],
        )
        .expect("aggregate");
        let schema =
            SchemaIr::new(vec![entity], vec![], vec![], vec![aggregate]).expect("contract schema");
        let input = CommandInputSchema::new(
            command_id,
            RecordSchema::new(
                RecordTypeRef::CommandInput(command_id),
                vec![
                    FieldSchema::new(idempotency_field, "request_id", idempotency_type)
                        .expect("field"),
                    FieldSchema::new(input_key_field, "root_id", crate::ValueType::u64())
                        .expect("field"),
                ],
            )
            .expect("input record"),
        )
        .expect("input");
        let failure = OutcomeSchema::new(
            command_id,
            OutcomeId::first(),
            "Missing",
            RecordSchema::new(
                RecordTypeRef::CommandOutcome {
                    command_id,
                    outcome_id: OutcomeId::first(),
                },
                vec![],
            )
            .expect("failure record"),
        )
        .expect("failure");
        let success_id = OutcomeId::new(2).expect("outcome");
        let success = OutcomeSchema::new(
            command_id,
            success_id,
            "Applied",
            RecordSchema::new(
                RecordTypeRef::CommandOutcome {
                    command_id,
                    outcome_id: success_id,
                },
                vec![],
            )
            .expect("success record"),
        )
        .expect("success");
        let expressions = ExpressionArena::new(vec![(
            ExpressionKind::InputField(input_key_field),
            crate::ValueType::u64(),
        )])
        .expect("expressions");
        let failure_construction =
            OutcomeConstruction::new(&failure, vec![], &expressions).expect("failure construction");
        let success_construction =
            OutcomeConstruction::new(&success, vec![], &expressions).expect("success construction");
        let binding = BindingPlan::new(
            BindingId::new(0),
            "root",
            BindingMode::Mutate,
            entity_id,
            entity_key,
            vec![ExprId::new(0)],
            vec![],
            false,
            failure_construction,
        )
        .expect("binding");
        let aggregate = schema.aggregate(aggregate_id).expect("aggregate");
        let locality = LocalityPlan::new(
            aggregate_id,
            aggregate.keys().partition_schema().clone(),
            ExprId::new(0),
            vec![
                ConflictDerivationPlan::new(
                    aggregate.keys().conflict_schema().clone(),
                    vec![ExprId::new(0)],
                )
                .expect("conflict"),
            ],
        )
        .expect("locality");
        let plan = CommandPlan::new(
            command_id,
            test_lineage(),
            "Apply",
            ContractVersion::new(1).expect("version"),
            input,
            vec![failure, success],
            success_id,
            Some(idempotency_field),
            expressions,
            vec![binding],
            vec![],
            locality,
            vec![],
            vec![Instruction::Return(success_construction)],
            ExecutionClass::IdempotentMutation,
            &schema,
        )
        .expect("command");
        (plan, schema)
    }

    #[test]
    fn mutation_idempotency_accepts_uuid_and_bounded_string_inputs() {
        assert_eq!(
            minimal_mutation_with_idempotency(crate::ValueType::uuid())
                .0
                .idempotency_input(),
            Some(FieldId::first())
        );
        assert_eq!(
            minimal_mutation().0.idempotency_input(),
            Some(FieldId::first())
        );
    }

    pub(crate) fn root_validation_mutation(field_dependent: bool) -> (CommandPlan, SchemaIr) {
        let root_id = EntityTypeId::first();
        let child_id = EntityTypeId::new(2).expect("child");
        let aggregate_id = AggregateTypeId::first();
        let command_id = CommandId::first();
        let root_key_field = FieldId::first();
        let root_flag_field = FieldId::new(2).expect("root flag");
        let child_local_key = FieldId::new(2).expect("child key");
        let idempotency_field = FieldId::first();
        let input_root_key = FieldId::new(2).expect("root input");
        let input_child_key = FieldId::new(3).expect("child input");
        let invariant_id = InvariantId::first();
        let key_component =
            crate::KeyComponentSchema::new(crate::ValueType::u64(), vec![]).expect("component");
        let root_key = KeySchema::new(KeyPurpose::Entity(root_id), vec![key_component.clone()])
            .expect("root key");
        let child_key = KeySchema::new(
            KeyPurpose::Entity(child_id),
            vec![key_component.clone(), key_component.clone()],
        )
        .expect("child key");
        let root = crate::EntitySchema::new(
            root_id,
            "Root",
            RecordSchema::new(
                RecordTypeRef::Entity(root_id),
                vec![
                    FieldSchema::new(root_key_field, "root_id", crate::ValueType::u64())
                        .expect("field"),
                    FieldSchema::new(root_flag_field, "flag", crate::ValueType::bool())
                        .expect("field"),
                ],
            )
            .expect("root record"),
            vec![root_key_field],
            root_key.clone(),
            vec![],
            vec![],
        )
        .expect("root");
        let child = crate::EntitySchema::new(
            child_id,
            "Child",
            RecordSchema::new(
                RecordTypeRef::Entity(child_id),
                vec![
                    FieldSchema::new(root_key_field, "root_id", crate::ValueType::u64())
                        .expect("field"),
                    FieldSchema::new(child_local_key, "child_id", crate::ValueType::u64())
                        .expect("field"),
                ],
            )
            .expect("child record"),
            vec![root_key_field, child_local_key],
            child_key.clone(),
            vec![],
            vec![],
        )
        .expect("child");
        let invariant_expressions = if field_dependent {
            ExpressionArena::new(vec![(
                ExpressionKind::SchemaField {
                    entity_type: root_id,
                    field: root_flag_field,
                },
                crate::ValueType::bool(),
            )])
            .expect("invariant expressions")
        } else {
            ExpressionArena::new(vec![(
                ExpressionKind::Constant(CanonicalValue::Bool(true)),
                crate::ValueType::bool(),
            )])
            .expect("invariant expressions")
        };
        let invariant = crate::InvariantPlan::new(
            invariant_id,
            "RootPolicy",
            invariant_expressions,
            ExprId::new(0),
        )
        .expect("invariant");
        let key_template = ExpressionArena::new(vec![(
            ExpressionKind::SchemaField {
                entity_type: root_id,
                field: root_key_field,
            },
            crate::ValueType::u64(),
        )])
        .expect("key template");
        let aggregate = crate::AggregateSchema::new(
            aggregate_id,
            "Aggregate",
            root_id,
            vec![child_id],
            crate::AggregateKeyPlan::new(
                key_template,
                ExprId::new(0),
                vec![ExprId::new(0)],
                KeySchema::new(
                    KeyPurpose::Partition(aggregate_id),
                    vec![key_component.clone()],
                )
                .expect("partition"),
                KeySchema::new(KeyPurpose::Conflict(aggregate_id), vec![key_component])
                    .expect("conflict"),
            )
            .expect("keys"),
            vec![invariant],
        )
        .expect("aggregate");
        let schema =
            SchemaIr::new(vec![root, child], vec![], vec![], vec![aggregate]).expect("schema");
        let input = CommandInputSchema::new(
            command_id,
            RecordSchema::new(
                RecordTypeRef::CommandInput(command_id),
                vec![
                    FieldSchema::new(
                        idempotency_field,
                        "request_id",
                        crate::ValueType::string(64).expect("string"),
                    )
                    .expect("field"),
                    FieldSchema::new(input_root_key, "root_id", crate::ValueType::u64())
                        .expect("field"),
                    FieldSchema::new(input_child_key, "child_id", crate::ValueType::u64())
                        .expect("field"),
                ],
            )
            .expect("input record"),
        )
        .expect("input");
        let failure = OutcomeSchema::new(
            command_id,
            OutcomeId::first(),
            "Missing",
            RecordSchema::new(
                RecordTypeRef::CommandOutcome {
                    command_id,
                    outcome_id: OutcomeId::first(),
                },
                vec![],
            )
            .expect("failure record"),
        )
        .expect("failure");
        let success_id = OutcomeId::new(2).expect("success");
        let success = OutcomeSchema::new(
            command_id,
            success_id,
            "Applied",
            RecordSchema::new(
                RecordTypeRef::CommandOutcome {
                    command_id,
                    outcome_id: success_id,
                },
                vec![],
            )
            .expect("success record"),
        )
        .expect("success");
        let predicate = if field_dependent {
            ExpressionKind::RootValidationField {
                read: RootValidationReadId::new(0),
                field: root_flag_field,
            }
        } else {
            ExpressionKind::Constant(CanonicalValue::Bool(true))
        };
        let expressions = ExpressionArena::new(vec![
            (
                ExpressionKind::InputField(input_root_key),
                crate::ValueType::u64(),
            ),
            (
                ExpressionKind::InputField(input_child_key),
                crate::ValueType::u64(),
            ),
            (predicate, crate::ValueType::bool()),
        ])
        .expect("expressions");
        let binding = BindingPlan::new(
            BindingId::new(0),
            "child",
            BindingMode::Mutate,
            child_id,
            child_key,
            vec![ExprId::new(0), ExprId::new(1)],
            vec![],
            false,
            OutcomeConstruction::new(&failure, vec![], &expressions).expect("failure construction"),
        )
        .expect("binding");
        let root_read = RootValidationReadPlan::new(
            RootValidationReadId::new(0),
            BindingId::new(0),
            root_id,
            root_key,
            vec![ExprId::new(0)],
            if field_dependent {
                vec![root_flag_field]
            } else {
                vec![]
            },
        )
        .expect("root read");
        let aggregate = schema.aggregate(aggregate_id).expect("aggregate");
        let locality = LocalityPlan::new(
            aggregate_id,
            aggregate.keys().partition_schema().clone(),
            ExprId::new(0),
            vec![
                ConflictDerivationPlan::new(
                    aggregate.keys().conflict_schema().clone(),
                    vec![ExprId::new(0)],
                )
                .expect("conflict"),
            ],
        )
        .expect("locality");
        let check = CommitCheckPlan::new(
            invariant_id,
            ExprId::new(2),
            vec![],
            vec![RootValidationReadId::new(0)],
        )
        .expect("check");
        let success_construction =
            OutcomeConstruction::new(&success, vec![], &expressions).expect("success construction");
        let plan = CommandPlan::new(
            command_id,
            test_lineage(),
            "ApplyChild",
            ContractVersion::new(1).expect("version"),
            input,
            vec![failure, success],
            success_id,
            Some(idempotency_field),
            expressions,
            vec![binding],
            vec![root_read],
            locality,
            vec![check],
            vec![Instruction::Return(success_construction)],
            ExecutionClass::IdempotentMutation,
            &schema,
        )
        .expect("command");
        (plan, schema)
    }

    pub(crate) fn mutation_with_added_outcome(
        old: &CommandPlan,
        schema: &SchemaIr,
        unrelated_set: bool,
    ) -> CommandPlan {
        let new_outcome_id = OutcomeId::new(3).expect("outcome");
        let new_outcome = OutcomeSchema::new(
            old.command_id(),
            new_outcome_id,
            "PolicyRejected",
            RecordSchema::new(
                RecordTypeRef::CommandOutcome {
                    command_id: old.command_id(),
                    outcome_id: new_outcome_id,
                },
                vec![],
            )
            .expect("record"),
        )
        .expect("outcome");
        let mut nodes = vec![
            (
                ExpressionKind::InputField(FieldId::new(2).expect("input key")),
                crate::ValueType::u64(),
            ),
            (
                ExpressionKind::Constant(CanonicalValue::Bool(true)),
                crate::ValueType::bool(),
            ),
        ];
        if unrelated_set {
            nodes.push((
                ExpressionKind::Constant(CanonicalValue::Bool(false)),
                crate::ValueType::bool(),
            ));
        }
        let expressions = ExpressionArena::new(nodes).expect("expressions");
        let rejection =
            OutcomeConstruction::new(&new_outcome, vec![], &expressions).expect("rejection");
        let success = old
            .outcomes()
            .iter()
            .find(|outcome| outcome.id() == old.success_outcome())
            .expect("success");
        let success =
            OutcomeConstruction::new(success, vec![], &expressions).expect("success value");
        let mut instructions = vec![Instruction::Require {
            requirement_index: 0,
            predicate: ExprId::new(1),
            reject: rejection,
        }];
        if unrelated_set {
            instructions.push(Instruction::SetField {
                binding: BindingId::new(0),
                field: FieldId::new(2).expect("flag"),
                value: ExprId::new(2),
            });
        }
        instructions.push(Instruction::Return(success));
        let mut outcomes = old.outcomes().to_vec();
        outcomes.push(new_outcome);
        CommandPlan::new(
            old.command_id(),
            old.required_capability().lineage().clone(),
            old.name(),
            ContractVersion::new(2).expect("version"),
            old.input().clone(),
            outcomes,
            old.success_outcome(),
            old.idempotency_input(),
            expressions,
            old.bindings().to_vec(),
            old.root_validation_reads().to_vec(),
            old.locality().clone(),
            old.commit_checks().to_vec(),
            instructions,
            old.execution_class(),
            schema,
        )
        .expect("evolved command")
    }

    #[test]
    fn object_construction_injects_scalar_into_optional_field() {
        let owner = RecordTypeRef::CommandOutcome {
            command_id: CommandId::first(),
            outcome_id: OutcomeId::first(),
        };
        let schema = RecordSchema::new(
            owner.clone(),
            vec![
                FieldSchema::new(
                    FieldId::first(),
                    "value",
                    crate::ValueType::optional(crate::ValueType::i64()).expect("optional"),
                )
                .expect("field"),
            ],
        )
        .expect("schema");
        let arena = ExpressionArena::new(vec![(
            ExpressionKind::Constant(CanonicalValue::I64(7)),
            crate::ValueType::i64(),
        )])
        .expect("arena");
        assert!(
            ObjectConstruction::new(
                owner,
                vec![FieldExpression::new(FieldId::first(), ExprId::new(0))],
                &schema,
                &arena,
            )
            .is_ok()
        );
    }

    #[test]
    fn object_construction_enforces_1024_field_boundary() {
        let command_id = CommandId::first();
        let outcome_id = OutcomeId::first();
        let owner = RecordTypeRef::CommandOutcome {
            command_id,
            outcome_id,
        };
        let construct = |count: usize| {
            let schema = RecordSchema::new(
                owner.clone(),
                (1..=count)
                    .map(|value| {
                        FieldSchema::new(
                            FieldId::new(value as u32).expect("field ID"),
                            format!("field_{value}"),
                            crate::ValueType::bool(),
                        )
                        .expect("field")
                    })
                    .collect(),
            )
            .expect("record");
            let arena = ExpressionArena::new(
                (0..count)
                    .map(|_| {
                        (
                            ExpressionKind::Constant(CanonicalValue::Bool(true)),
                            crate::ValueType::bool(),
                        )
                    })
                    .collect(),
            )
            .expect("arena");
            let fields = (1..=count)
                .map(|value| {
                    FieldExpression::new(
                        FieldId::new(value as u32).expect("field ID"),
                        ExprId::new((value - 1) as u32),
                    )
                })
                .collect();
            ObjectConstruction::new(owner.clone(), fields, &schema, &arena)
        };
        assert!(construct(MAX_OBJECT_FIELDS).is_ok());
        assert!(construct(MAX_OBJECT_FIELDS + 1).is_err());
    }

    #[test]
    fn binding_alias_is_not_part_of_command_plan_identity() {
        let (plan, schema) = minimal_mutation();
        let mut renamed = plan.clone();
        renamed.bindings[0].name = "renamed_root".to_owned();
        assert_eq!(
            crate::bundle::compute_command_plan_hash(&plan, &schema).expect("hash"),
            crate::bundle::compute_command_plan_hash(&renamed, &schema).expect("renamed hash")
        );
    }

    #[test]
    fn explain_renders_checked_locality_reads_writes_and_outcomes_deterministically() {
        let (plan, _) = minimal_mutation();
        let explain = crate::CommandExplain::from_plan(&plan);
        let rendered = explain.render_text();
        assert_eq!(rendered, explain.render_text());
        assert_eq!(explain.partition_expression(), ExprId::new(0));
        assert_eq!(explain.conflict_derivations().len(), 1);
        assert_eq!(explain.binding_plans()[0].mode(), BindingMode::Mutate);
        assert!(rendered.contains("execution:IdempotentMutation"));
        assert!(rendered.contains("partition:expr=0"));
        assert!(rendered.contains("conflict:0"));
        assert!(rendered.contains("binding:0 mode=Mutate"));
        assert!(rendered.contains("outcome:1"));
        assert!(rendered.contains("outcome:2"));
    }

    #[test]
    fn locality_rejects_missing_or_extra_mutable_conflict_derivations() {
        let (plan, schema) = minimal_mutation();
        let mut missing = plan.locality.clone();
        missing.conflict_keys.clear();
        assert!(validate_locality(&plan.expressions, &plan.bindings, &missing, &schema).is_err());
        let mut extra = plan.locality.clone();
        extra.conflict_keys.push(extra.conflict_keys[0].clone());
        assert!(validate_locality(&plan.expressions, &plan.bindings, &extra, &schema).is_err());
    }

    #[test]
    fn commit_checks_reject_omitted_true_and_extra_invariant_applications() {
        let entity_id = EntityTypeId::first();
        let aggregate_id = AggregateTypeId::first();
        let command_id = CommandId::first();
        let key_field = FieldId::first();
        let flag_field = FieldId::new(2).expect("field");
        let invariant_id = InvariantId::first();
        let component =
            crate::KeyComponentSchema::new(crate::ValueType::u64(), vec![]).expect("component");
        let entity_key = KeySchema::new(KeyPurpose::Entity(entity_id), vec![component.clone()])
            .expect("entity key");
        let invariant = crate::InvariantPlan::new(
            invariant_id,
            "FlagIsSet",
            ExpressionArena::new(vec![(
                ExpressionKind::SchemaField {
                    entity_type: entity_id,
                    field: flag_field,
                },
                crate::ValueType::bool(),
            )])
            .expect("invariant arena"),
            ExprId::new(0),
        )
        .expect("invariant");
        let entity = crate::EntitySchema::new(
            entity_id,
            "Root",
            RecordSchema::new(
                RecordTypeRef::Entity(entity_id),
                vec![
                    FieldSchema::new(key_field, "id", crate::ValueType::u64()).expect("field"),
                    FieldSchema::new(flag_field, "flag", crate::ValueType::bool()).expect("field"),
                ],
            )
            .expect("record"),
            vec![key_field],
            entity_key.clone(),
            vec![invariant],
            vec![],
        )
        .expect("entity");
        let key_template = ExpressionArena::new(vec![(
            ExpressionKind::SchemaField {
                entity_type: entity_id,
                field: key_field,
            },
            crate::ValueType::u64(),
        )])
        .expect("key template");
        let aggregate = crate::AggregateSchema::new(
            aggregate_id,
            "Aggregate",
            entity_id,
            vec![],
            crate::AggregateKeyPlan::new(
                key_template,
                ExprId::new(0),
                vec![ExprId::new(0)],
                KeySchema::new(KeyPurpose::Partition(aggregate_id), vec![component.clone()])
                    .expect("partition"),
                KeySchema::new(KeyPurpose::Conflict(aggregate_id), vec![component])
                    .expect("conflict"),
            )
            .expect("keys"),
            vec![],
        )
        .expect("aggregate");
        let schema = SchemaIr::new(vec![entity], vec![], vec![], vec![aggregate]).expect("schema");
        let arena = ExpressionArena::new(vec![
            (
                ExpressionKind::InputField(FieldId::new(3).expect("field")),
                crate::ValueType::u64(),
            ),
            (
                ExpressionKind::BoundField {
                    binding: BindingId::new(0),
                    field: flag_field,
                },
                crate::ValueType::bool(),
            ),
            (
                ExpressionKind::Constant(CanonicalValue::Bool(true)),
                crate::ValueType::bool(),
            ),
        ])
        .expect("arena");
        let failure_schema = OutcomeSchema::new(
            command_id,
            OutcomeId::first(),
            "Missing",
            RecordSchema::new(
                RecordTypeRef::CommandOutcome {
                    command_id,
                    outcome_id: OutcomeId::first(),
                },
                vec![],
            )
            .expect("record"),
        )
        .expect("outcome");
        let binding = BindingPlan::new(
            BindingId::new(0),
            "root",
            BindingMode::Mutate,
            entity_id,
            entity_key,
            vec![ExprId::new(0)],
            vec![flag_field],
            false,
            OutcomeConstruction::new(&failure_schema, vec![], &arena).expect("failure"),
        )
        .expect("binding");
        let aggregate = schema.aggregate(aggregate_id).expect("aggregate");
        let locality = LocalityPlan::new(
            aggregate_id,
            aggregate.keys().partition_schema().clone(),
            ExprId::new(0),
            vec![
                ConflictDerivationPlan::new(
                    aggregate.keys().conflict_schema().clone(),
                    vec![ExprId::new(0)],
                )
                .expect("conflict"),
            ],
        )
        .expect("locality");
        let correct = CommitCheckPlan::new(
            invariant_id,
            ExprId::new(1),
            vec![BindingId::new(0)],
            vec![],
        )
        .expect("check");
        assert!(
            validate_commit_checks(
                &arena,
                &[],
                std::slice::from_ref(&binding),
                &[],
                &locality,
                &schema,
            )
            .is_err()
        );
        assert!(
            validate_commit_checks(
                &arena,
                &[CommitCheckPlan::new(
                    invariant_id,
                    ExprId::new(2),
                    vec![BindingId::new(0)],
                    vec![],
                )
                .expect("true check")],
                std::slice::from_ref(&binding),
                &[],
                &locality,
                &schema,
            )
            .is_err()
        );
        assert!(
            validate_commit_checks(
                &arena,
                &[correct.clone(), correct],
                &[binding],
                &[],
                &locality,
                &schema,
            )
            .is_err()
        );
    }

    #[test]
    fn root_validation_read_supports_field_and_constant_aggregate_invariants() {
        let (field_plan, _) = root_validation_mutation(true);
        assert_eq!(field_plan.root_validation_reads().len(), 1);
        assert_eq!(
            field_plan.root_validation_reads()[0].accessed_fields(),
            &[FieldId::new(2).expect("field")]
        );
        assert!(field_plan.commit_checks()[0].source_bindings().is_empty());
        assert_eq!(
            field_plan.commit_checks()[0].root_validation_reads(),
            &[RootValidationReadId::new(0)]
        );
        let explain = crate::CommandExplain::from_plan(&field_plan).render_text();
        assert!(explain.contains("root-validation:0 source=0 entity=1"));
        assert!(explain.contains("root-validation-field:0:2"));
        assert!(explain.contains("root-subjects=[0]"));

        let (constant_plan, _) = root_validation_mutation(false);
        assert_eq!(constant_plan.root_validation_reads().len(), 1);
        assert!(
            constant_plan.root_validation_reads()[0]
                .accessed_fields()
                .is_empty()
        );
        assert_eq!(
            constant_plan.commit_checks()[0].root_validation_reads(),
            &[RootValidationReadId::new(0)]
        );
    }

    #[test]
    fn capability_lineage_is_part_of_the_checked_command_plan_hash() {
        let (plan, schema) = minimal_mutation();
        let other_lineage = ContractLineage::new("OtherLineage").expect("lineage");
        let rebuilt = CommandPlan::new(
            plan.command_id(),
            other_lineage.clone(),
            plan.name(),
            plan.contract_version(),
            plan.input().clone(),
            plan.outcomes().to_vec(),
            plan.success_outcome(),
            plan.idempotency_input(),
            plan.expressions().clone(),
            plan.bindings().to_vec(),
            plan.root_validation_reads().to_vec(),
            plan.locality().clone(),
            plan.commit_checks().to_vec(),
            plan.instructions().to_vec(),
            plan.execution_class(),
            &schema,
        )
        .expect("rebuilt plan");
        assert_ne!(rebuilt.plan_hash(), plan.plan_hash());
        assert_eq!(rebuilt.required_capability().lineage(), &other_lineage);
        assert_eq!(
            rebuilt.required_capability().command_id(),
            rebuilt.command_id()
        );
    }

    #[test]
    fn root_validation_field_is_rejected_outside_commit_checks() {
        let (plan, schema) = root_validation_mutation(true);
        let rejection = OutcomeConstruction::new(&plan.outcomes()[0], vec![], plan.expressions())
            .expect("rejection");
        let success = OutcomeConstruction::new(
            plan.outcomes()
                .iter()
                .find(|outcome| outcome.id() == plan.success_outcome())
                .expect("success schema"),
            vec![],
            plan.expressions(),
        )
        .expect("success");
        assert!(
            CommandPlan::new(
                plan.command_id(),
                plan.required_capability().lineage().clone(),
                plan.name(),
                plan.contract_version(),
                plan.input().clone(),
                plan.outcomes().to_vec(),
                plan.success_outcome(),
                plan.idempotency_input(),
                plan.expressions().clone(),
                plan.bindings().to_vec(),
                plan.root_validation_reads().to_vec(),
                plan.locality().clone(),
                plan.commit_checks().to_vec(),
                vec![
                    Instruction::Require {
                        requirement_index: 0,
                        predicate: ExprId::new(2),
                        reject: rejection,
                    },
                    Instruction::Return(success),
                ],
                plan.execution_class(),
                &schema,
            )
            .is_err()
        );
    }

    #[test]
    fn root_validation_groups_structural_derivations_and_orders_distinct_groups_by_source() {
        let (plan, schema) = root_validation_mutation(false);
        let root_input = FieldId::new(20).expect("field");
        let first_child_input = FieldId::new(21).expect("field");
        let second_root_input = FieldId::new(22).expect("field");
        let second_child_input = FieldId::new(23).expect("field");
        let arena = ExpressionArena::new(vec![
            (
                ExpressionKind::InputField(root_input),
                crate::ValueType::u64(),
            ),
            (
                ExpressionKind::InputField(first_child_input),
                crate::ValueType::u64(),
            ),
            (
                ExpressionKind::InputField(second_root_input),
                crate::ValueType::u64(),
            ),
            (
                ExpressionKind::InputField(second_child_input),
                crate::ValueType::u64(),
            ),
        ])
        .expect("arena");
        let mut first = plan.bindings()[0].clone();
        first.key_expressions = vec![ExprId::new(0), ExprId::new(1)];
        let mut second = first.clone();
        second.id = BindingId::new(1);
        second.name = "second_child".to_owned();
        second.key_expressions = vec![ExprId::new(2), ExprId::new(3)];
        let template = &plan.root_validation_reads()[0];
        let first_read = RootValidationReadPlan::new(
            RootValidationReadId::new(0),
            BindingId::new(0),
            template.entity_type(),
            template.key_schema().clone(),
            vec![ExprId::new(0)],
            vec![],
        )
        .expect("read");
        let second_read = RootValidationReadPlan::new(
            RootValidationReadId::new(1),
            BindingId::new(1),
            template.entity_type(),
            template.key_schema().clone(),
            vec![ExprId::new(2)],
            vec![],
        )
        .expect("read");
        assert!(
            validate_root_validation_reads(
                &arena,
                &[first.clone(), second.clone()],
                &[first_read.clone(), second_read.clone()],
                plan.locality(),
                &schema,
            )
            .is_ok()
        );
        let reversed = [
            RootValidationReadPlan::new(
                RootValidationReadId::new(0),
                BindingId::new(1),
                template.entity_type(),
                template.key_schema().clone(),
                vec![ExprId::new(2)],
                vec![],
            )
            .expect("read"),
            RootValidationReadPlan::new(
                RootValidationReadId::new(1),
                BindingId::new(0),
                template.entity_type(),
                template.key_schema().clone(),
                vec![ExprId::new(0)],
                vec![],
            )
            .expect("read"),
        ];
        assert!(
            validate_root_validation_reads(
                &arena,
                &[first.clone(), second.clone()],
                &reversed,
                plan.locality(),
                &schema,
            )
            .is_err()
        );

        let duplicate_arena = ExpressionArena::new(vec![
            (
                ExpressionKind::InputField(root_input),
                crate::ValueType::u64(),
            ),
            (
                ExpressionKind::InputField(first_child_input),
                crate::ValueType::u64(),
            ),
            (
                ExpressionKind::InputField(root_input),
                crate::ValueType::u64(),
            ),
            (
                ExpressionKind::InputField(second_child_input),
                crate::ValueType::u64(),
            ),
        ])
        .expect("arena");
        assert!(
            validate_root_validation_reads(
                &duplicate_arena,
                &[first, second],
                &[first_read],
                plan.locality(),
                &schema,
            )
            .is_ok()
        );
    }

    #[test]
    fn structural_source_root_derivation_suppresses_internal_read() {
        let (plan, schema) = root_validation_mutation(false);
        let root_input = FieldId::new(20).expect("field");
        let child_input = FieldId::new(21).expect("field");
        let arena = ExpressionArena::new(vec![
            (
                ExpressionKind::InputField(root_input),
                crate::ValueType::u64(),
            ),
            (
                ExpressionKind::InputField(child_input),
                crate::ValueType::u64(),
            ),
            (
                ExpressionKind::InputField(root_input),
                crate::ValueType::u64(),
            ),
        ])
        .expect("arena");
        let mut child = plan.bindings()[0].clone();
        child.key_expressions = vec![ExprId::new(0), ExprId::new(1)];
        let template = &plan.root_validation_reads()[0];
        let root = BindingPlan::new(
            BindingId::new(1),
            "root",
            BindingMode::Read,
            template.entity_type(),
            template.key_schema().clone(),
            vec![ExprId::new(2)],
            vec![],
            false,
            child.failure.clone(),
        )
        .expect("root binding");
        assert!(
            validate_root_validation_reads(&arena, &[child, root], &[], plan.locality(), &schema,)
                .is_ok()
        );
    }

    #[test]
    fn structural_expression_comparison_memoizes_shared_dag_pairs() {
        let mut nodes = vec![(
            ExpressionKind::Constant(CanonicalValue::Bool(true)),
            crate::ValueType::bool(),
        )];
        for index in 1..crate::MAX_EXPRESSION_NESTING {
            nodes.push((
                ExpressionKind::Binary {
                    operator: crate::BinaryOperator::And,
                    left: ExprId::new((index - 1) as u32),
                    right: ExprId::new((index - 1) as u32),
                },
                crate::ValueType::bool(),
            ));
        }
        let left = ExpressionArena::new(nodes.clone()).expect("left arena");
        let right = ExpressionArena::new(nodes).expect("right arena");
        assert!(
            expression_trees_equal(
                &left,
                ExprId::new((crate::MAX_EXPRESSION_NESTING - 1) as u32),
                &right,
                ExprId::new((crate::MAX_EXPRESSION_NESTING - 1) as u32),
            )
            .expect("comparison")
        );
    }
}
