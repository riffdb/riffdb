//! API-neutral explain DTOs derived from checked plans.

use std::fmt::Write;

use riffdb_types::{CommandId, EnumVariantId, EventTypeId, FieldId, InvariantId, OutcomeId};

use crate::{
    BindingId, BindingPlan, CollectionExpansionPlanV1, CommandPlan, CommitCheckPlan,
    ConflictDerivationPlan, DeleteCheckModeV1, DeleteCheckPlanV1, EventConstruction,
    ExecutionClass, ExprId, ExpressionArena, ExpressionKind, KeySchema, OutcomeSchema,
    RelationshipCheckPlan, RootValidationReadPlan, SecretRevealSpecV1, UniqueConflictPlan,
    WorkflowLeaseFields, WorkflowLeaseOperation,
};

/// Value-free structural explanation of one exact workflow transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowTransitionExplain {
    binding: BindingId,
    state_field: FieldId,
    source_states: Vec<EnumVariantId>,
    destination: EnumVariantId,
    expected_revision: ExprId,
    stale_outcome: OutcomeId,
    illegal_outcome: OutcomeId,
}

impl WorkflowTransitionExplain {
    /// Mutable workflow binding.
    #[must_use]
    pub const fn binding(&self) -> BindingId {
        self.binding
    }
    /// Workflow state field.
    #[must_use]
    pub const fn state_field(&self) -> FieldId {
        self.state_field
    }
    /// Canonical legal source states.
    #[must_use]
    pub fn source_states(&self) -> &[EnumVariantId] {
        &self.source_states
    }
    /// Exact destination state.
    #[must_use]
    pub const fn destination(&self) -> EnumVariantId {
        self.destination
    }
    /// Direct caller-supplied revision expression.
    #[must_use]
    pub const fn expected_revision(&self) -> ExprId {
        self.expected_revision
    }
    /// Declared stale-revision outcome.
    #[must_use]
    pub const fn stale_outcome(&self) -> OutcomeId {
        self.stale_outcome
    }
    /// Declared illegal-state outcome.
    #[must_use]
    pub const fn illegal_outcome(&self) -> OutcomeId {
        self.illegal_outcome
    }
}

/// Closed lease operation kind exposed by value-free explain output.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkflowLeaseOperationKind {
    /// Claim.
    Claim,
    /// Renew.
    Renew,
    /// Release.
    Release,
    /// Expire.
    Expire,
    /// Fence protected work.
    Fence,
}

/// Value-free structural explanation of one fenced lease operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowLeaseExplain {
    binding: BindingId,
    fields: WorkflowLeaseFields,
    operation: WorkflowLeaseOperationKind,
    required_inputs: Vec<ExprId>,
    rejection_outcomes: Vec<OutcomeId>,
}

impl WorkflowLeaseExplain {
    /// Mutable workflow binding.
    #[must_use]
    pub const fn binding(&self) -> BindingId {
        self.binding
    }
    /// Declared fields and duration bounds.
    #[must_use]
    pub const fn fields(&self) -> &WorkflowLeaseFields {
        &self.fields
    }
    /// Closed operation kind.
    #[must_use]
    pub const fn operation(&self) -> WorkflowLeaseOperationKind {
        self.operation
    }
    /// Required direct caller input expressions.
    #[must_use]
    pub fn required_inputs(&self) -> &[ExprId] {
        &self.required_inputs
    }
    /// Declared rejection outcomes in evaluation order.
    #[must_use]
    pub fn rejection_outcomes(&self) -> &[OutcomeId] {
        &self.rejection_outcomes
    }
}

/// A bounded stable-ID-only command explanation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandExplain {
    command_id: CommandId,
    execution_class: ExecutionClass,
    partition_component_count: usize,
    conflict_key_count: usize,
    collection_expansion: Option<CollectionExpansionPlanV1>,
    bindings: Vec<BindingId>,
    read_fields: Vec<(BindingId, FieldId)>,
    write_fields: Vec<(BindingId, FieldId)>,
    workflow_transitions: Vec<WorkflowTransitionExplain>,
    workflow_leases: Vec<WorkflowLeaseExplain>,
    invariants: Vec<InvariantId>,
    events: Vec<EventTypeId>,
    outcomes: Vec<OutcomeId>,
    partition_schema: KeySchema,
    partition_expression: ExprId,
    conflict_derivations: Vec<ConflictDerivationPlan>,
    binding_plans: Vec<BindingPlan>,
    relationship_checks: Vec<RelationshipCheckPlan>,
    delete_checks: Vec<DeleteCheckPlanV1>,
    unique_conflicts: Vec<UniqueConflictPlan>,
    root_validation_reads: Vec<RootValidationReadPlan>,
    commit_checks: Vec<CommitCheckPlan>,
    event_constructions: Vec<EventConstruction>,
    secret_reveals: Vec<SecretRevealSpecV1>,
    outcome_schemas: Vec<OutcomeSchema>,
    expressions: ExpressionArena,
}

impl CommandExplain {
    /// Derives explain metadata without evaluating inputs or exposing values.
    #[must_use]
    pub fn from_plan(plan: &CommandPlan) -> Self {
        let bindings = plan.bindings().iter().map(|binding| binding.id()).collect();
        let mut read_fields = plan
            .bindings()
            .iter()
            .flat_map(|binding| {
                binding
                    .accessed_fields()
                    .iter()
                    .map(move |field| (binding.id(), *field))
            })
            .collect::<Vec<_>>();
        read_fields.sort_unstable();
        let mut write_fields = plan
            .instructions()
            .iter()
            .filter_map(|instruction| match instruction {
                crate::Instruction::SetField { binding, field, .. }
                | crate::Instruction::SetEmbedding { binding, field, .. }
                | crate::Instruction::WorkflowTransition {
                    binding,
                    state_field: field,
                    ..
                } => Some((*binding, *field)),
                _ => None,
            })
            .collect::<Vec<_>>();
        write_fields.extend(plan.bindings().iter().flat_map(|binding| {
            binding
                .initializer()
                .iter()
                .map(move |initialized| (binding.id(), initialized.field_id()))
        }));
        for instruction in plan.instructions() {
            if let crate::Instruction::WorkflowLease {
                binding,
                fields,
                operation,
            } = instruction
            {
                let written = match operation {
                    WorkflowLeaseOperation::Claim { .. } => vec![
                        fields.owner_field,
                        fields.expiry_field,
                        fields.fencing_token_field,
                    ]
                    .into_iter()
                    .chain(fields.attempt_field)
                    .collect::<Vec<_>>(),
                    WorkflowLeaseOperation::Renew { .. } => vec![fields.expiry_field],
                    WorkflowLeaseOperation::Release { .. }
                    | WorkflowLeaseOperation::Expire { .. } => {
                        vec![fields.owner_field, fields.expiry_field]
                    }
                    WorkflowLeaseOperation::Fence { .. } => Vec::new(),
                };
                write_fields.extend(written.into_iter().map(|field| (*binding, field)));
            }
        }
        write_fields.sort_unstable();
        write_fields.dedup();
        let workflow_transitions = plan
            .instructions()
            .iter()
            .filter_map(|instruction| match instruction {
                crate::Instruction::WorkflowTransition {
                    binding,
                    state_field,
                    source_states,
                    destination,
                    expected_revision,
                    stale,
                    illegal,
                } => Some(WorkflowTransitionExplain {
                    binding: *binding,
                    state_field: *state_field,
                    source_states: source_states.clone(),
                    destination: *destination,
                    expected_revision: *expected_revision,
                    stale_outcome: stale.outcome_id(),
                    illegal_outcome: illegal.outcome_id(),
                }),
                _ => None,
            })
            .collect();
        let workflow_leases = plan
            .instructions()
            .iter()
            .filter_map(|instruction| {
                let crate::Instruction::WorkflowLease {
                    binding,
                    fields,
                    operation,
                } = instruction
                else {
                    return None;
                };
                let (kind, inputs, outcomes) = match operation {
                    WorkflowLeaseOperation::Claim {
                        owner,
                        duration_seconds,
                        expected_revision,
                        stale,
                        unavailable,
                        invalid,
                        exhausted,
                    } => (
                        WorkflowLeaseOperationKind::Claim,
                        vec![*owner, *duration_seconds, *expected_revision],
                        vec![
                            stale.outcome_id(),
                            unavailable.outcome_id(),
                            invalid.outcome_id(),
                            exhausted.outcome_id(),
                        ],
                    ),
                    WorkflowLeaseOperation::Renew {
                        owner,
                        fencing_token,
                        duration_seconds,
                        expected_revision,
                        stale,
                        invalid,
                        expired,
                        exhausted,
                    } => (
                        WorkflowLeaseOperationKind::Renew,
                        vec![
                            *owner,
                            *fencing_token,
                            *duration_seconds,
                            *expected_revision,
                        ],
                        vec![
                            stale.outcome_id(),
                            invalid.outcome_id(),
                            expired.outcome_id(),
                            exhausted.outcome_id(),
                        ],
                    ),
                    WorkflowLeaseOperation::Release {
                        owner,
                        fencing_token,
                        expected_revision,
                        stale,
                        invalid,
                    } => (
                        WorkflowLeaseOperationKind::Release,
                        vec![*owner, *fencing_token, *expected_revision],
                        vec![stale.outcome_id(), invalid.outcome_id()],
                    ),
                    WorkflowLeaseOperation::Expire {
                        expected_revision,
                        stale,
                        active,
                    } => (
                        WorkflowLeaseOperationKind::Expire,
                        vec![*expected_revision],
                        vec![stale.outcome_id(), active.outcome_id()],
                    ),
                    WorkflowLeaseOperation::Fence {
                        owner,
                        fencing_token,
                        expected_revision,
                        stale,
                        invalid,
                        expired,
                    } => (
                        WorkflowLeaseOperationKind::Fence,
                        vec![*owner, *fencing_token, *expected_revision],
                        vec![
                            stale.outcome_id(),
                            invalid.outcome_id(),
                            expired.outcome_id(),
                        ],
                    ),
                };
                Some(WorkflowLeaseExplain {
                    binding: *binding,
                    fields: fields.clone(),
                    operation: kind,
                    required_inputs: inputs,
                    rejection_outcomes: outcomes,
                })
            })
            .collect();
        let invariants = plan
            .commit_checks()
            .iter()
            .map(|check| check.invariant_id())
            .collect();
        let events = plan
            .instructions()
            .iter()
            .filter_map(|instruction| match instruction {
                crate::Instruction::EmitEvent(event) => Some(event.event_type()),
                _ => None,
            })
            .collect();
        let outcomes = plan.outcomes().iter().map(|outcome| outcome.id()).collect();
        let event_constructions = plan
            .instructions()
            .iter()
            .filter_map(|instruction| match instruction {
                crate::Instruction::EmitEvent(event) => Some(event.clone()),
                _ => None,
            })
            .collect();
        Self {
            command_id: plan.command_id(),
            execution_class: plan.execution_class(),
            partition_component_count: plan.locality().partition_schema().components().len(),
            conflict_key_count: plan.locality().conflict_keys().len()
                + plan.unique_conflicts().len(),
            collection_expansion: plan.collection_expansion().cloned(),
            bindings,
            read_fields,
            write_fields,
            workflow_transitions,
            workflow_leases,
            invariants,
            events,
            outcomes,
            partition_schema: plan.locality().partition_schema().clone(),
            partition_expression: plan.locality().partition_expression(),
            conflict_derivations: plan.locality().conflict_keys().to_vec(),
            binding_plans: plan.bindings().to_vec(),
            relationship_checks: plan.relationship_checks().to_vec(),
            delete_checks: plan.delete_checks().to_vec(),
            unique_conflicts: plan.unique_conflicts().to_vec(),
            root_validation_reads: plan.root_validation_reads().to_vec(),
            commit_checks: plan.commit_checks().to_vec(),
            event_constructions,
            secret_reveals: plan.secret_reveals().to_vec(),
            outcome_schemas: plan.outcomes().to_vec(),
            expressions: plan.expressions().clone(),
        }
    }

    /// Stable command ID.
    #[must_use]
    pub const fn command_id(&self) -> CommandId {
        self.command_id
    }
    /// Estimated execution class.
    #[must_use]
    pub const fn execution_class(&self) -> ExecutionClass {
        self.execution_class
    }
    /// Partition tuple arity (one in v1).
    #[must_use]
    pub const fn partition_component_count(&self) -> usize {
        self.partition_component_count
    }
    /// Number of upfront conflict derivations.
    #[must_use]
    pub const fn conflict_key_count(&self) -> usize {
        self.conflict_key_count
    }
    /// Checked collection expansion, including aggregate byte proof when declared.
    #[must_use]
    pub const fn collection_expansion(&self) -> Option<&CollectionExpansionPlanV1> {
        self.collection_expansion.as_ref()
    }
    /// Dense bindings.
    #[must_use]
    pub fn bindings(&self) -> &[BindingId] {
        &self.bindings
    }
    /// Exact influential bound fields.
    #[must_use]
    pub fn read_fields(&self) -> &[(BindingId, FieldId)] {
        &self.read_fields
    }
    /// Exact mutated fields.
    #[must_use]
    pub fn write_fields(&self) -> &[(BindingId, FieldId)] {
        &self.write_fields
    }
    /// Exact revision/state transition checks in instruction order.
    #[must_use]
    pub fn workflow_transitions(&self) -> &[WorkflowTransitionExplain] {
        &self.workflow_transitions
    }
    /// Exact fenced lease operations in instruction order.
    #[must_use]
    pub fn workflow_leases(&self) -> &[WorkflowLeaseExplain] {
        &self.workflow_leases
    }
    /// Commit-validation invariants.
    #[must_use]
    pub fn invariants(&self) -> &[InvariantId] {
        &self.invariants
    }
    /// Emitted event types in occurrence order.
    #[must_use]
    pub fn events(&self) -> &[EventTypeId] {
        &self.events
    }
    /// Declared outcomes in stable-ID order.
    #[must_use]
    pub fn outcomes(&self) -> &[OutcomeId] {
        &self.outcomes
    }

    /// Exact typed partition-key schema.
    #[must_use]
    pub const fn partition_schema(&self) -> &KeySchema {
        &self.partition_schema
    }

    /// Root expression of the derived partition key.
    #[must_use]
    pub const fn partition_expression(&self) -> ExprId {
        self.partition_expression
    }

    /// Typed conflict-key derivations in mutable binding order.
    #[must_use]
    pub fn conflict_derivations(&self) -> &[ConflictDerivationPlan] {
        &self.conflict_derivations
    }

    /// Checked binding read templates, including mode and accessed fields.
    #[must_use]
    pub fn binding_plans(&self) -> &[BindingPlan] {
        &self.binding_plans
    }

    /// Required relationship changes and the exact reads proving their targets.
    #[must_use]
    pub fn relationship_checks(&self) -> &[RelationshipCheckPlan] {
        &self.relationship_checks
    }

    /// Checked no-inbound or transaction-current restrict dependencies for deletes.
    #[must_use]
    pub fn delete_checks(&self) -> &[DeleteCheckPlanV1] {
        &self.delete_checks
    }

    /// Input-computable conflict keys for changed declared unique values.
    #[must_use]
    pub fn unique_conflicts(&self) -> &[UniqueConflictPlan] {
        &self.unique_conflicts
    }

    /// Internal aggregate-root read templates in dense ID order.
    #[must_use]
    pub fn root_validation_reads(&self) -> &[RootValidationReadPlan] {
        &self.root_validation_reads
    }

    /// Exact invariant applications and their predicate roots.
    #[must_use]
    pub fn commit_checks(&self) -> &[CommitCheckPlan] {
        &self.commit_checks
    }

    /// Durable event constructions in occurrence order.
    #[must_use]
    pub fn event_constructions(&self) -> &[EventConstruction] {
        &self.event_constructions
    }

    /// Exact statically declared secret disclosures in canonical order.
    #[must_use]
    pub fn secret_reveals(&self) -> &[SecretRevealSpecV1] {
        &self.secret_reveals
    }

    /// Complete declared outcome schemas in stable-ID order.
    #[must_use]
    pub fn outcome_schemas(&self) -> &[OutcomeSchema] {
        &self.outcome_schemas
    }

    /// Complete typed expression graph referenced by the structural plan DTOs.
    #[must_use]
    pub const fn expressions(&self) -> &ExpressionArena {
        &self.expressions
    }

    /// Renders a deterministic, value-free structural explanation.
    #[must_use]
    pub fn render_text(&self) -> String {
        let mut output = String::new();
        let _ = writeln!(output, "command:{}", self.command_id.get());
        let _ = writeln!(output, "execution:{:?}", self.execution_class);
        if let Some((expansion, maximum, coefficient)) =
            self.collection_expansion.as_ref().and_then(|expansion| {
                Some((
                    expansion,
                    expansion.maximum_aggregate_element_bytes()?,
                    expansion.maximum_copy_coefficient()?,
                ))
            })
        {
            let _ = writeln!(
                output,
                "collection:field={} elements={}..{} aggregate-element-bytes={} copy-coefficient={}",
                expansion.input_field().get(),
                expansion.minimum_elements(),
                expansion.maximum_elements(),
                maximum,
                coefficient
            );
        }
        for (index, expression) in self.expressions.nodes().iter().enumerate() {
            let operation = match expression.kind() {
                ExpressionKind::Constant(_) => "constant(redacted)".to_owned(),
                ExpressionKind::InputField(field) => format!("input-field:{}", field.get()),
                ExpressionKind::ServiceValue(field) => {
                    format!("service-value:{}", field.get())
                }
                ExpressionKind::CollectionElement => "collection-element".to_owned(),
                ExpressionKind::CollectionElementField(field) => {
                    format!("collection-element-field:{}", field.get())
                }
                ExpressionKind::CompleteBinding(binding) => {
                    format!("complete-binding:{}", binding.get())
                }
                ExpressionKind::BoundField { binding, field } => {
                    format!("bound-field:{}:{}", binding.get(), field.get())
                }
                ExpressionKind::SchemaField { entity_type, field } => {
                    format!("schema-field:{}:{}", entity_type.get(), field.get())
                }
                ExpressionKind::RootValidationField { read, field } => {
                    format!("root-validation-field:{}:{}", read.get(), field.get())
                }
                ExpressionKind::SourceEventField(field) => {
                    format!("source-event-field:{}", field.get())
                }
                ExpressionKind::TransactionTime => "tx.time".to_owned(),
                ExpressionKind::TransactionDate => "tx.date".to_owned(),
                ExpressionKind::Unary { operator, operand } => {
                    format!("unary:{operator:?}:{}", operand.get())
                }
                ExpressionKind::Binary {
                    operator,
                    left,
                    right,
                } => format!("binary:{operator:?}:{}:{}", left.get(), right.get()),
            };
            let _ = writeln!(
                output,
                "expression:{index} type={:?} op={operation}",
                expression.result_type()
            );
        }
        let _ = writeln!(
            output,
            "partition:expr={} purpose={:?} components={}",
            self.partition_expression.get(),
            self.partition_schema.purpose(),
            self.partition_schema.components().len()
        );
        for (index, conflict) in self.conflict_derivations.iter().enumerate() {
            let expressions = conflict
                .expressions()
                .iter()
                .map(|expression| expression.get().to_string())
                .collect::<Vec<_>>()
                .join(",");
            let _ = writeln!(
                output,
                "conflict:{index} purpose={:?} expressions=[{expressions}]",
                conflict.schema().purpose()
            );
        }
        for binding in &self.binding_plans {
            let fields = binding
                .accessed_fields()
                .iter()
                .map(|field| field.get().to_string())
                .collect::<Vec<_>>()
                .join(",");
            let _ = writeln!(
                output,
                "binding:{} mode={:?} entity={} complete={} reads=[{}]",
                binding.id().get(),
                binding.mode(),
                binding.entity_type().get(),
                binding.complete_record_access(),
                fields
            );
            if !binding.initializer().is_empty() {
                let initializer = binding
                    .initializer()
                    .iter()
                    .map(|field| format!("{}:{}", field.field_id().get(), field.expression().get()))
                    .collect::<Vec<_>>()
                    .join(",");
                let _ = writeln!(
                    output,
                    "initialize:{} fields=[{}] state-independent:true",
                    binding.id().get(),
                    initializer
                );
            }
        }
        for check in &self.relationship_checks {
            let _ = writeln!(
                output,
                "relationship:{} source-binding:{} exact-target-read:{} commit-revalidated:true",
                check.relationship_name(),
                check.source_binding().get(),
                check.target_binding().get()
            );
        }
        for check in &self.delete_checks {
            match check.mode() {
                DeleteCheckModeV1::NoInbound => {
                    let _ = writeln!(
                        output,
                        "delete:binding={} policy=no-inbound structurally-proved:true",
                        check.binding().get()
                    );
                }
                DeleteCheckModeV1::Restrict {
                    source_entity,
                    index_id,
                } => {
                    let _ = writeln!(
                        output,
                        "delete:binding={} policy=restrict source-entity={} index={} transaction-current-empty:true",
                        check.binding().get(),
                        source_entity.get(),
                        index_id.get()
                    );
                }
                DeleteCheckModeV1::Cascade { relationships } => {
                    let maximum = relationships
                        .iter()
                        .map(|relationship| u32::from(relationship.maximum()))
                        .sum::<u32>();
                    let _ = writeln!(
                        output,
                        "delete:binding={} policy=cascade relationships={} child-maximum={} transaction-current-bounded:true canonical-order:true",
                        check.binding().get(),
                        relationships.len(),
                        maximum
                    );
                }
            }
        }
        for unique in &self.unique_conflicts {
            let expressions = unique
                .expressions()
                .iter()
                .map(|expression| expression.get().to_string())
                .collect::<Vec<_>>()
                .join(",");
            let _ = writeln!(
                output,
                "unique:{} index={} source-binding={} conflict=[{}] transaction-current:true atomic-index:true",
                unique.unique_name(),
                unique.index_id().get(),
                unique.source_binding().get(),
                expressions
            );
        }
        for read in &self.root_validation_reads {
            let expressions = read
                .key_expressions()
                .iter()
                .map(|expression| expression.get().to_string())
                .collect::<Vec<_>>()
                .join(",");
            let fields = read
                .accessed_fields()
                .iter()
                .map(|field| field.get().to_string())
                .collect::<Vec<_>>()
                .join(",");
            let _ = writeln!(
                output,
                "root-validation:{} source={} entity={} key=[{}] reads=[{}]",
                read.id().get(),
                read.source_binding().get(),
                read.entity_type().get(),
                expressions,
                fields
            );
        }
        for (binding, field) in &self.write_fields {
            let _ = writeln!(output, "write:{}:{}", binding.get(), field.get());
        }
        for transition in &self.workflow_transitions {
            let sources = transition
                .source_states
                .iter()
                .map(|state| state.get().to_string())
                .collect::<Vec<_>>()
                .join(",");
            let _ = writeln!(
                output,
                "workflow-transition:{} field={} sources=[{}] destination={} expected-revision={} stale={} illegal={} transaction-current:true",
                transition.binding.get(),
                transition.state_field.get(),
                sources,
                transition.destination.get(),
                transition.expected_revision.get(),
                transition.stale_outcome.get(),
                transition.illegal_outcome.get(),
            );
        }
        for lease in &self.workflow_leases {
            let inputs = lease
                .required_inputs
                .iter()
                .map(|input| input.get().to_string())
                .collect::<Vec<_>>()
                .join(",");
            let outcomes = lease
                .rejection_outcomes
                .iter()
                .map(|outcome| outcome.get().to_string())
                .collect::<Vec<_>>()
                .join(",");
            let attempt = lease
                .fields
                .attempt_field
                .map_or_else(|| "none".to_owned(), |field| field.get().to_string());
            let _ = writeln!(
                output,
                "workflow-lease:{} operation={:?} owner-field={} expiry-field={} fence-field={} attempt-field={} duration={}..={} inputs=[{}] rejections=[{}] transaction-current:true grants-authority:false",
                lease.binding.get(),
                lease.operation,
                lease.fields.owner_field.get(),
                lease.fields.expiry_field.get(),
                lease.fields.fencing_token_field.get(),
                attempt,
                lease.fields.minimum_duration_seconds,
                lease.fields.maximum_duration_seconds,
                inputs,
                outcomes,
            );
        }
        for check in &self.commit_checks {
            let subjects = check
                .source_bindings()
                .iter()
                .map(|binding| binding.get().to_string())
                .collect::<Vec<_>>()
                .join(",");
            let root_subjects = check
                .root_validation_reads()
                .iter()
                .map(|read| read.get().to_string())
                .collect::<Vec<_>>()
                .join(",");
            let _ = writeln!(
                output,
                "invariant:{} predicate={} source-subjects=[{}] root-subjects=[{}]",
                check.invariant_id().get(),
                check.predicate().get(),
                subjects,
                root_subjects
            );
        }
        for event in &self.event_constructions {
            let fields = event
                .payload()
                .fields()
                .iter()
                .map(|field| field.field_id().get().to_string())
                .collect::<Vec<_>>()
                .join(",");
            let _ = writeln!(
                output,
                "event:{} fields=[{}]",
                event.event_type().get(),
                fields
            );
        }
        for reveal in &self.secret_reveals {
            let _ = writeln!(
                output,
                "reveal:source={}:{} expression={} destination={:?}",
                reveal.source_binding().get(),
                reveal.source_field().get(),
                reveal.expression().get(),
                reveal.destination(),
            );
        }
        for outcome in &self.outcome_schemas {
            let fields = outcome
                .payload()
                .fields()
                .iter()
                .map(|field| format!("{}:{:?}", field.id().get(), field.value_type()))
                .collect::<Vec<_>>()
                .join(",");
            let _ = writeln!(output, "outcome:{} fields=[{}]", outcome.id().get(), fields);
        }
        output
    }
}
