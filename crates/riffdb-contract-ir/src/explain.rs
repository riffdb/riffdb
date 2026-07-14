//! API-neutral explain DTOs derived from checked plans.

use std::fmt::Write;

use riffdb_types::{CommandId, EventTypeId, FieldId, InvariantId, OutcomeId};

use crate::{
    BindingId, BindingPlan, CommandPlan, CommitCheckPlan, ConflictDerivationPlan,
    EventConstruction, ExecutionClass, ExprId, ExpressionArena, ExpressionKind, KeySchema,
    OutcomeSchema, RootValidationReadPlan,
};

/// A bounded stable-ID-only command explanation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandExplain {
    command_id: CommandId,
    execution_class: ExecutionClass,
    partition_component_count: usize,
    conflict_key_count: usize,
    bindings: Vec<BindingId>,
    read_fields: Vec<(BindingId, FieldId)>,
    write_fields: Vec<(BindingId, FieldId)>,
    invariants: Vec<InvariantId>,
    events: Vec<EventTypeId>,
    outcomes: Vec<OutcomeId>,
    partition_schema: KeySchema,
    partition_expression: ExprId,
    conflict_derivations: Vec<ConflictDerivationPlan>,
    binding_plans: Vec<BindingPlan>,
    root_validation_reads: Vec<RootValidationReadPlan>,
    commit_checks: Vec<CommitCheckPlan>,
    event_constructions: Vec<EventConstruction>,
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
                crate::Instruction::SetField { binding, field, .. } => Some((*binding, *field)),
                _ => None,
            })
            .collect::<Vec<_>>();
        write_fields.sort_unstable();
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
            conflict_key_count: plan.locality().conflict_keys().len(),
            bindings,
            read_fields,
            write_fields,
            invariants,
            events,
            outcomes,
            partition_schema: plan.locality().partition_schema().clone(),
            partition_expression: plan.locality().partition_expression(),
            conflict_derivations: plan.locality().conflict_keys().to_vec(),
            binding_plans: plan.bindings().to_vec(),
            root_validation_reads: plan.root_validation_reads().to_vec(),
            commit_checks: plan.commit_checks().to_vec(),
            event_constructions,
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
        for (index, expression) in self.expressions.nodes().iter().enumerate() {
            let operation = match expression.kind() {
                ExpressionKind::Constant(_) => "constant(redacted)".to_owned(),
                ExpressionKind::InputField(field) => format!("input-field:{}", field.get()),
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
