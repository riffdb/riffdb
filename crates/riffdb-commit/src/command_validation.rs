//! Pure transaction-current command validation.
//!
//! The orchestration entrypoint remains intentionally sealed until the commit
//! coordinator can carry one runtime attempt through admission, current-state
//! acquisition, validation, and derived-index planning by construction.

use std::{collections::BTreeMap, error::Error, fmt};

use riffdb_catalog::ResolvedExecutablePlan;
use riffdb_contract_ir::{
    BindingId, BindingMode, CommandPlan, ExecutionClass, Instruction, RecordSchema, RecordTypeRef,
    RootValidationReadId, SchemaIr, ValueType,
};
use riffdb_invariant::{
    CommitCheckResult, EvaluationError, ExpressionValueSource, evaluate_commit_checks,
};
use riffdb_storage_api::{
    CandidateValidationRejection, EntityMutation, EntityObservation, EntityTarget,
    EvaluatedCommand, ExecutablePlanRef, ReadDependencies, ReadDependency, StoredEntityRecordV1,
    TransactionCurrentState,
};
use riffdb_types::{CanonicalRecord, CanonicalValue, FieldId, LogicalTime};

/// Redacted failure for an impossible checked-plan/value combination.
#[derive(Clone, Copy, Eq, PartialEq)]
struct CommandValidationError {
    _private: (),
}

impl CommandValidationError {
    const fn integrity() -> Self {
        Self { _private: () }
    }
}

impl fmt::Debug for CommandValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CommandValidationError([REDACTED])")
    }
}

impl fmt::Display for CommandValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("transaction-current command validation failed")
    }
}

impl Error for CommandValidationError {}

enum CheckedCommandDecision {
    ZeroMutation,
    NonZero(Box<[Option<usize>]>),
    Rejected(CandidateValidationRejection),
}

/// Pure semantic core, deliberately private until candidate identity can be
/// preserved by construction across the full storage admission chain.
fn validate_transaction_current_command_parts(
    resolved: &ResolvedExecutablePlan,
    normalized_input: &CanonicalRecord,
    logical_time: LogicalTime,
    evaluated: &EvaluatedCommand,
    current: &TransactionCurrentState,
) -> Result<CheckedCommandDecision, CommandValidationError> {
    validate_identity_positions_and_output(resolved, normalized_input, evaluated, current)?;

    let current_dependencies = dependencies_from_current(current)?;
    if &current_dependencies != evaluated.read_dependencies() {
        return Ok(CheckedCommandDecision::Rejected(
            CandidateValidationRejection::DependencyChanged,
        ));
    }

    if evaluated.mutations().is_empty() {
        return Ok(CheckedCommandDecision::ZeroMutation);
    }

    let coverage = prove_mutation_coverage(resolved, evaluated, current)?;
    let values = assemble_transaction_current_values(
        resolved,
        normalized_input,
        logical_time,
        evaluated,
        current,
        &coverage,
    )?;
    let decision = match evaluate_commit_checks(
        resolved.plan().expressions(),
        resolved.plan().commit_checks(),
        &values,
    ) {
        Ok(CommitCheckResult::Satisfied) => CheckedCommandDecision::NonZero(coverage),
        Ok(CommitCheckResult::Rejected { .. }) => {
            CheckedCommandDecision::Rejected(CandidateValidationRejection::CommitCheckRejected)
        }
        Err(EvaluationError::Arithmetic) => CheckedCommandDecision::Rejected(
            CandidateValidationRejection::CommitCheckArithmeticFault,
        ),
        Err(EvaluationError::Integrity) => return Err(CommandValidationError::integrity()),
    };
    Ok(decision)
}

fn validate_identity_positions_and_output(
    resolved: &ResolvedExecutablePlan,
    normalized_input: &CanonicalRecord,
    evaluated: &EvaluatedCommand,
    current: &TransactionCurrentState,
) -> Result<(), CommandValidationError> {
    let reference = resolved.reference();
    let plan = resolved.plan();
    let request = evaluated.validation_request();
    if reference != evaluated.plan()
        || request.plan() != evaluated.plan()
        || plan.execution_class() != ExecutionClass::IdempotentMutation
        || plan.command_id() != reference.command_id()
        || plan.contract_version() != reference.contract_version()
        || plan.plan_hash() != reference.command_plan_hash()
        || plan.bindings().len() != request.binding_targets().len()
        || plan.bindings().len() != current.bindings().len()
        || plan.root_validation_reads().len() != request.root_validation_targets().len()
        || plan.root_validation_reads().len() != current.root_validations().len()
        || !request.range_targets().is_empty()
        || !current.ranges().is_empty()
    {
        return Err(CommandValidationError::integrity());
    }

    validate_exact_record(
        resolved.bundle().bundle().schema(),
        plan.input().record(),
        normalized_input,
    )?;
    for (index, ((binding, target), observation)) in plan
        .bindings()
        .iter()
        .zip(request.binding_targets())
        .zip(current.bindings())
        .enumerate()
    {
        if binding.id().get() as usize != index
            || binding.entity_type() != target.entity_type_id()
            || observation.target() != target
        {
            return Err(CommandValidationError::integrity());
        }
    }
    for (index, ((read, target), observation)) in plan
        .root_validation_reads()
        .iter()
        .zip(request.root_validation_targets())
        .zip(current.root_validations())
        .enumerate()
    {
        if read.id().get() as usize != index
            || read.entity_type() != target.entity_type_id()
            || observation.target() != target
        {
            return Err(CommandValidationError::integrity());
        }
    }
    if request
        .range_targets()
        .iter()
        .zip(current.ranges())
        .any(|(target, observation)| target != observation.target())
    {
        return Err(CommandValidationError::integrity());
    }

    validate_evaluated_output(plan, resolved.bundle().bundle().schema(), evaluated)
}

fn validate_evaluated_output(
    plan: &CommandPlan,
    schema: &SchemaIr,
    evaluated: &EvaluatedCommand,
) -> Result<(), CommandValidationError> {
    let outcome = evaluated.outcome();
    let outcome_schema = plan
        .outcomes()
        .iter()
        .find(|candidate| candidate.id() == outcome.outcome_id())
        .ok_or_else(CommandValidationError::integrity)?;
    validate_exact_record(schema, outcome_schema.payload(), outcome.value())?;

    if evaluated.mutations().is_empty() {
        let declared_rejection = outcome.outcome_id() != plan.success_outcome()
            && (plan
                .bindings()
                .iter()
                .any(|binding| binding.failure().outcome_id() == outcome.outcome_id())
                || plan.instructions().iter().any(|instruction| {
                    matches!(instruction, Instruction::Require { reject, .. }
                        if reject.outcome_id() == outcome.outcome_id())
                }));
        if !declared_rejection || !evaluated.event_intents().is_empty() {
            return Err(CommandValidationError::integrity());
        }
        return Ok(());
    }

    if outcome.outcome_id() != plan.success_outcome() {
        return Err(CommandValidationError::integrity());
    }
    let expected_events = plan
        .instructions()
        .iter()
        .filter_map(|instruction| match instruction {
            Instruction::EmitEvent(event) => Some(event),
            Instruction::Require { .. } | Instruction::SetField { .. } | Instruction::Return(_) => {
                None
            }
        });
    let mut actual_events = evaluated.event_intents().iter();
    for expected in expected_events {
        let actual = actual_events
            .next()
            .ok_or_else(CommandValidationError::integrity)?;
        if actual.event_type_id() != expected.event_type() {
            return Err(CommandValidationError::integrity());
        }
        let declared = schema
            .event(expected.event_type())
            .ok_or_else(CommandValidationError::integrity)?;
        validate_exact_record(schema, declared.payload(), actual.payload())?;
    }
    if actual_events.next().is_some() {
        return Err(CommandValidationError::integrity());
    }
    Ok(())
}

fn dependencies_from_current(
    current: &TransactionCurrentState,
) -> Result<ReadDependencies, CommandValidationError> {
    ReadDependencies::new(
        current
            .bindings()
            .iter()
            .chain(current.root_validations())
            .map(ReadDependency::from_entity)
            .chain(
                current
                    .ranges()
                    .iter()
                    .map(|range| ReadDependency::IndexRangeEpoch {
                        target: range.target().clone(),
                        expected: range.epoch(),
                    }),
            ),
    )
    .map_err(|_| CommandValidationError::integrity())
}

fn prove_mutation_coverage(
    resolved: &ResolvedExecutablePlan,
    evaluated: &EvaluatedCommand,
    current: &TransactionCurrentState,
) -> Result<Box<[Option<usize>]>, CommandValidationError> {
    let plan = resolved.plan();
    let request = evaluated.validation_request();
    let mutable_count = plan
        .bindings()
        .iter()
        .filter(|binding| binding.mode() != BindingMode::Read)
        .count();
    if evaluated.mutations().len() != mutable_count {
        return Err(CommandValidationError::integrity());
    }

    let mut mutation_by_target = BTreeMap::<EntityTarget, usize>::new();
    for (index, mutation) in evaluated.mutations().iter().enumerate() {
        if mutation_by_target
            .insert(mutation.target().clone(), index)
            .is_some()
        {
            return Err(CommandValidationError::integrity());
        }
    }

    let mut mutable_targets = BTreeMap::<EntityTarget, BindingId>::new();
    let mut mutation_index_by_binding = vec![None; plan.bindings().len()];
    for (index, ((binding, target), observation)) in plan
        .bindings()
        .iter()
        .zip(request.binding_targets())
        .zip(current.bindings())
        .enumerate()
    {
        if binding.mode() == BindingMode::Read {
            continue;
        }
        if mutable_targets
            .insert(target.clone(), binding.id())
            .is_some()
        {
            return Err(CommandValidationError::integrity());
        }
        let mutation_index = mutation_by_target
            .remove(target)
            .ok_or_else(CommandValidationError::integrity)?;
        let mutation = &evaluated.mutations()[mutation_index];
        if mutation.post_image().target() != target
            || mutation.post_image().written_by_contract() != plan.contract_version()
        {
            return Err(CommandValidationError::integrity());
        }
        match (binding.mode(), mutation, observation) {
            (BindingMode::Create, EntityMutation::Create(_), EntityObservation::Absent(_)) => {}
            (
                BindingMode::Mutate,
                EntityMutation::Replace {
                    expected_version, ..
                },
                EntityObservation::Present(record),
            ) if *expected_version == record.entity_version() => {}
            (BindingMode::Read, _, _) | (BindingMode::Create | BindingMode::Mutate, _, _) => {
                return Err(CommandValidationError::integrity());
            }
        }
        mutation_index_by_binding[index] = Some(mutation_index);
    }
    if !mutation_by_target.is_empty() {
        return Err(CommandValidationError::integrity());
    }
    Ok(mutation_index_by_binding.into_boxed_slice())
}

struct PositionedBindingRecord {
    id: BindingId,
    record: CanonicalRecord,
}

struct PositionedRootRecord {
    id: RootValidationReadId,
    record: CanonicalRecord,
}

/// Owned values passed across the pure invariant-evaluator boundary.
struct TransactionCurrentValues {
    input: CanonicalRecord,
    logical_time: LogicalTime,
    bindings: Box<[PositionedBindingRecord]>,
    roots: Box<[PositionedRootRecord]>,
}

impl fmt::Debug for TransactionCurrentValues {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TransactionCurrentValues([REDACTED])")
    }
}

impl ExpressionValueSource for TransactionCurrentValues {
    fn input_field(&self, field: FieldId) -> Option<CanonicalValue> {
        record_field(&self.input, field).cloned()
    }

    fn complete_binding(&self, binding: BindingId) -> Option<CanonicalValue> {
        self.bindings
            .get(binding.get() as usize)
            .filter(|position| position.id == binding)
            .map(|position| CanonicalValue::Record(position.record.clone()))
    }

    fn bound_field(&self, binding: BindingId, field: FieldId) -> Option<CanonicalValue> {
        self.bindings
            .get(binding.get() as usize)
            .filter(|position| position.id == binding)
            .and_then(|position| record_field(&position.record, field))
            .cloned()
    }

    fn root_validation_field(
        &self,
        read: RootValidationReadId,
        field: FieldId,
    ) -> Option<CanonicalValue> {
        self.roots
            .get(read.get() as usize)
            .filter(|position| position.id == read)
            .and_then(|position| record_field(&position.record, field))
            .cloned()
    }

    fn transaction_time(&self) -> Option<LogicalTime> {
        Some(self.logical_time)
    }
}

fn assemble_transaction_current_values(
    resolved: &ResolvedExecutablePlan,
    normalized_input: &CanonicalRecord,
    logical_time: LogicalTime,
    evaluated: &EvaluatedCommand,
    current: &TransactionCurrentState,
    coverage: &[Option<usize>],
) -> Result<TransactionCurrentValues, CommandValidationError> {
    let plan = resolved.plan();
    let schema = resolved.bundle().bundle().schema();
    let request = evaluated.validation_request();
    let mut bindings = Vec::with_capacity(plan.bindings().len());
    for (index, ((binding, target), observation)) in plan
        .bindings()
        .iter()
        .zip(request.binding_targets())
        .zip(current.bindings())
        .enumerate()
    {
        let entity = schema
            .entity(binding.entity_type())
            .ok_or_else(CommandValidationError::integrity)?;
        if binding.key_schema() != entity.primary_key() {
            return Err(CommandValidationError::integrity());
        }
        let source = match binding.mode() {
            BindingMode::Read => match observation {
                EntityObservation::Present(record) => {
                    let projected = materialize_current_entity_record(
                        schema,
                        entity,
                        target,
                        record,
                        resolved.reference(),
                    )?;
                    bindings.push(PositionedBindingRecord {
                        id: binding.id(),
                        record: projected,
                    });
                    continue;
                }
                EntityObservation::Absent(_) => return Err(CommandValidationError::integrity()),
            },
            BindingMode::Mutate | BindingMode::Create => {
                let mutation_index = coverage
                    .get(index)
                    .copied()
                    .flatten()
                    .ok_or_else(CommandValidationError::integrity)?;
                evaluated
                    .mutations()
                    .get(mutation_index)
                    .filter(|mutation| mutation.target() == target)
                    .ok_or_else(CommandValidationError::integrity)?
                    .post_image()
                    .fields()
            }
        };
        bindings.push(PositionedBindingRecord {
            id: binding.id(),
            record: validate_post_image_and_project(
                schema,
                entity,
                target,
                source,
                binding.mode(),
                observation,
                resolved.reference(),
            )?,
        });
    }

    let mut roots = Vec::with_capacity(plan.root_validation_reads().len());
    for ((read, target), observation) in plan
        .root_validation_reads()
        .iter()
        .zip(request.root_validation_targets())
        .zip(current.root_validations())
    {
        let entity = schema
            .entity(read.entity_type())
            .ok_or_else(CommandValidationError::integrity)?;
        if read.key_schema() != entity.primary_key() {
            return Err(CommandValidationError::integrity());
        }
        let EntityObservation::Present(record) = observation else {
            return Err(CommandValidationError::integrity());
        };
        roots.push(PositionedRootRecord {
            id: read.id(),
            record: materialize_current_entity_record(
                schema,
                entity,
                target,
                record,
                resolved.reference(),
            )?,
        });
    }

    Ok(TransactionCurrentValues {
        input: normalized_input.clone(),
        logical_time,
        bindings: bindings.into_boxed_slice(),
        roots: roots.into_boxed_slice(),
    })
}

/// Projects only physically present fields; version order is not ancestry evidence.
fn materialize_current_entity_record(
    schema: &SchemaIr,
    entity: &riffdb_contract_ir::EntitySchema,
    target: &EntityTarget,
    record: &StoredEntityRecordV1,
    plan: &ExecutablePlanRef,
) -> Result<CanonicalRecord, CommandValidationError> {
    if target.entity_type_id() != entity.id()
        || record.target() != target
        || record.schema_binding().lineage() != plan.contract_lineage()
        || (record.written_by_contract() == plan.contract_version()
            && !record.schema_binding().matches_plan(plan))
    {
        return Err(CommandValidationError::integrity());
    }
    let source = record.fields();
    let key_values = entity
        .primary_key()
        .decode_entity(target.key())
        .map_err(|_| CommandValidationError::integrity())?;
    if key_values.len() != entity.primary_key_fields().len() {
        return Err(CommandValidationError::integrity());
    }

    let mut fields = Vec::with_capacity(entity.record().fields().len());
    for field in entity.record().fields() {
        let value = match record_field(source, field.id()) {
            Some(value) => value.clone(),
            None => return Err(CommandValidationError::integrity()),
        };
        validate_semantic_value(schema, field.value_type(), &value)?;
        fields.push((field.id(), value));
    }
    for (field, expected) in entity.primary_key_fields().iter().zip(key_values) {
        if fields
            .binary_search_by_key(field, |(candidate, _)| *candidate)
            .ok()
            .map(|index| &fields[index].1)
            != Some(&expected)
        {
            return Err(CommandValidationError::integrity());
        }
    }
    CanonicalRecord::new(fields).map_err(|_| CommandValidationError::integrity())
}

fn validate_post_image_and_project(
    schema: &SchemaIr,
    entity: &riffdb_contract_ir::EntitySchema,
    target: &EntityTarget,
    post_image: &CanonicalRecord,
    mode: BindingMode,
    current: &EntityObservation,
    plan: &ExecutablePlanRef,
) -> Result<CanonicalRecord, CommandValidationError> {
    if target.entity_type_id() != entity.id() {
        return Err(CommandValidationError::integrity());
    }
    let key_values = entity
        .primary_key()
        .decode_entity(target.key())
        .map_err(|_| CommandValidationError::integrity())?;
    if key_values.len() != entity.primary_key_fields().len() {
        return Err(CommandValidationError::integrity());
    }

    let mut declared_fields = Vec::with_capacity(entity.record().fields().len());
    for field in entity.record().fields() {
        let value = record_field(post_image, field.id())
            .ok_or_else(CommandValidationError::integrity)?
            .clone();
        validate_semantic_value(schema, field.value_type(), &value)?;
        declared_fields.push((field.id(), value));
    }
    for (field, expected) in entity.primary_key_fields().iter().zip(key_values) {
        if declared_fields
            .binary_search_by_key(field, |(candidate, _)| *candidate)
            .ok()
            .map(|index| &declared_fields[index].1)
            != Some(&expected)
        {
            return Err(CommandValidationError::integrity());
        }
    }

    let post_unknown = post_image
        .fields()
        .iter()
        .filter(|(field, _)| entity.record().field(*field).is_none())
        .collect::<Vec<_>>();
    match (mode, current) {
        (BindingMode::Create, EntityObservation::Absent(_)) if post_unknown.is_empty() => {}
        (BindingMode::Mutate, EntityObservation::Present(record)) => {
            materialize_current_entity_record(schema, entity, target, record, plan)?;
            let current_unknown = record
                .fields()
                .fields()
                .iter()
                .filter(|(field, _)| entity.record().field(*field).is_none())
                .collect::<Vec<_>>();
            if post_unknown != current_unknown {
                return Err(CommandValidationError::integrity());
            }
        }
        (BindingMode::Read | BindingMode::Create | BindingMode::Mutate, _) => {
            return Err(CommandValidationError::integrity());
        }
    }
    CanonicalRecord::new(declared_fields).map_err(|_| CommandValidationError::integrity())
}

fn validate_exact_record(
    schema: &SchemaIr,
    declared: &RecordSchema,
    actual: &CanonicalRecord,
) -> Result<(), CommandValidationError> {
    if declared.fields().len() != actual.fields().len() {
        return Err(CommandValidationError::integrity());
    }
    for (field, (actual_id, value)) in declared.fields().iter().zip(actual.fields()) {
        if field.id() != *actual_id {
            return Err(CommandValidationError::integrity());
        }
        validate_semantic_value(schema, field.value_type(), value)?;
    }
    Ok(())
}

fn validate_semantic_value(
    schema: &SchemaIr,
    value_type: &ValueType,
    value: &CanonicalValue,
) -> Result<(), CommandValidationError> {
    value_type
        .validate_value(value)
        .map_err(|_| CommandValidationError::integrity())?;
    if matches!(value, CanonicalValue::Null) {
        return Ok(());
    }
    if let Some(inner) = value_type.optional_inner() {
        return validate_semantic_value(schema, inner, value);
    }
    if let Some(enum_id) = value_type.enum_type_id() {
        let CanonicalValue::Enum {
            type_id,
            variant_id,
        } = value
        else {
            return Err(CommandValidationError::integrity());
        };
        if *type_id != enum_id
            || schema
                .enumeration(enum_id)
                .is_none_or(|enumeration| !enumeration.contains_variant(*variant_id))
        {
            return Err(CommandValidationError::integrity());
        }
    }
    if let Some((element, _)) = value_type.list_parts() {
        let CanonicalValue::List(values) = value else {
            return Err(CommandValidationError::integrity());
        };
        for value in values.values() {
            validate_semantic_value(schema, element, value)?;
        }
    }
    if let Some(record_ref) = value_type.record_ref() {
        let CanonicalValue::Record(value) = value else {
            return Err(CommandValidationError::integrity());
        };
        let declared = match record_ref {
            RecordTypeRef::Entity(id) => schema.entity(*id).map(|entity| entity.record()),
            RecordTypeRef::Event(id) => schema.event(*id).map(|event| event.payload()),
            RecordTypeRef::CommandInput(_)
            | RecordTypeRef::CommandOutcome { .. }
            | RecordTypeRef::ProjectionResult(_) => None,
        }
        .ok_or_else(CommandValidationError::integrity)?;
        validate_exact_record(schema, declared, value)?;
    }
    Ok(())
}

fn record_field(record: &CanonicalRecord, field: FieldId) -> Option<&CanonicalValue> {
    record
        .fields()
        .binary_search_by_key(&field, |(candidate, _)| *candidate)
        .ok()
        .map(|index| &record.fields()[index].1)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use riffdb_catalog::ValidatedContractBundle;
    use riffdb_contract_compiler::compile_contract_source;
    use riffdb_contract_ir::{CommandPlan, EntitySchema};
    use riffdb_invariant::{CommitCheckResult, derive_input_command_facts};
    use riffdb_runtime::{ExecutionFault, ExecutionResult, TransactionContext, execute_command};
    use riffdb_storage_api::{
        CurrentRangeObservation, DeclaredOutcome, DurableKeySchemaBindingV1, EntityPostImage,
        EvaluationBudget, EventIntent, IndexEpochPosition, IndexRangeObservation,
        IndexRangePrefixBuilder, IndexRangeTarget, SnapshotRequest,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdmittedActorContext, ContractBundleHash, EntityVersion, IndexId,
        RequestId, TenantScope, Timestamp,
    };

    use super::*;

    const ZERO_REJECT_SOURCE: &str = r#"
contract ZeroReject version 1 {
  entity Row {
    key (id: i64)
    field value: i64
    invariant impossible: 1 == 2
  }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Change {
    input request_key: string<128>
    input id: i64
    input next: i64
    idempotency_key request_key
    mutate Row(id) as row else Missing {}
    set row.value = next
    return Changed { row: row }
  }
}
"#;

    const MIXED_SOURCE: &str = r#"
contract MixedValidation version 1 {
  entity Root {
    key (tenant: uuid, root_id: uuid)
    field enabled: bool
    field note: optional<string<32>>
  }
  entity Child {
    key (tenant: uuid, root_id: uuid, child_id: uuid)
    field amount: i64
    field tag: optional<string<32>>
    invariant non_negative: amount >= 0
  }
  aggregate Family {
    root Root
    child Child
    partition_by tenant
    conflict_key (tenant, root_id)
    invariant root_enabled: enabled
  }
  command ChangeChildren {
    input request_key: string<128>
    input tenant: uuid
    input observed_root: uuid
    input created_root: uuid
    input changed_child: uuid
    input new_child: uuid
    input amount: i64
    idempotency_key request_key
    read Root(tenant, observed_root) as observed else MissingRoot {}
    mutate Child(tenant, observed_root, changed_child) as changed else MissingChanged {}
    create Child(tenant, created_root, new_child) as created else AlreadyCreated {}
    set changed.amount = amount
    set created.amount = amount
    return Changed { observed: observed, changed: changed, created: created }
  }
}
"#;

    const FALSE_CHECK_SOURCE: &str = r#"
contract FalseCheck version 1 {
  entity Row {
    key (id: i64)
    field value: i64
    invariant non_negative: value >= 0
  }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Change {
    input request_key: string<128>
    input id: i64
    input next: i64
    idempotency_key request_key
    mutate Row(id) as row else Missing {}
    set row.value = next
    return Changed { row: row }
  }
}
"#;

    const ARITHMETIC_CHECK_SOURCE: &str = r#"
contract ArithmeticCheck version 1 {
  entity Row {
    key (id: i64)
    field numerator: i64
    field divisor: i64
    invariant quotient_non_negative: numerator / divisor >= 0
  }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Change {
    input request_key: string<128>
    input id: i64
    input numerator: i64
    input divisor: i64
    idempotency_key request_key
    mutate Row(id) as row else Missing {}
    set row.numerator = numerator
    set row.divisor = divisor
    return Changed { row: row }
  }
}
"#;

    const EVENT_SOURCE: &str = r#"
contract EventValidation version 1 {
  entity Row {
    key (id: i64)
    field value: i64
    invariant non_negative: value >= 0
  }
  event First { value: i64 }
  event Second { value: i64 }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Change {
    input request_key: string<128>
    input id: i64
    input next: i64
    idempotency_key request_key
    mutate Row(id) as row else Missing {}
    set row.value = next
    emit First { value: row.value }
    emit Second { value: row.value }
    return Changed { value: row.value }
  }
}
"#;

    const READ_ONLY_SOURCE: &str = r#"
contract ReadOnlyValidation version 1 {
  entity Row { key (id: uuid) }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Find {
    input id: uuid
    read Row(id) as row else Missing { id: id }
    return Found { row: row }
  }
}
"#;

    struct PreparedFixture {
        resolved: ResolvedExecutablePlan,
        input: CanonicalRecord,
        logical_time: LogicalTime,
        binding_targets: Vec<EntityTarget>,
        root_targets: Vec<EntityTarget>,
    }

    impl PreparedFixture {
        fn plan(&self) -> &CommandPlan {
            self.resolved.plan()
        }

        fn entity(&self, target: &EntityTarget) -> &EntitySchema {
            self.resolved
                .bundle()
                .bundle()
                .schema()
                .entity(target.entity_type_id())
                .expect("fixture entity")
        }

        fn present_binding(
            &self,
            index: usize,
            version: EntityVersion,
            fields: &[(&str, CanonicalValue)],
            unknowns: Vec<(FieldId, CanonicalValue)>,
            omitted: &[&str],
        ) -> EntityObservation {
            self.present(
                self.binding_targets[index].clone(),
                version,
                fields,
                unknowns,
                omitted,
            )
        }

        fn present_root(
            &self,
            index: usize,
            version: EntityVersion,
            fields: &[(&str, CanonicalValue)],
            unknowns: Vec<(FieldId, CanonicalValue)>,
            omitted: &[&str],
        ) -> EntityObservation {
            self.present(
                self.root_targets[index].clone(),
                version,
                fields,
                unknowns,
                omitted,
            )
        }

        fn present(
            &self,
            target: EntityTarget,
            version: EntityVersion,
            supplied: &[(&str, CanonicalValue)],
            unknowns: Vec<(FieldId, CanonicalValue)>,
            omitted: &[&str],
        ) -> EntityObservation {
            let entity = self.entity(&target);
            let supplied = supplied.iter().cloned().collect::<BTreeMap<_, _>>();
            let key_values = entity
                .primary_key()
                .decode_entity(target.key())
                .expect("fixture key");
            let mut fields = Vec::new();
            for field in entity.record().fields() {
                if omitted.contains(&field.name()) {
                    assert!(field.value_type().is_optional());
                    continue;
                }
                let value = entity
                    .primary_key_fields()
                    .iter()
                    .position(|candidate| *candidate == field.id())
                    .map(|position| key_values[position].clone())
                    .or_else(|| supplied.get(field.name()).cloned())
                    .or_else(|| {
                        field
                            .value_type()
                            .is_optional()
                            .then_some(CanonicalValue::Null)
                    })
                    .unwrap_or_else(|| panic!("missing fixture field {}", field.name()));
                fields.push((field.id(), value));
            }
            fields.extend(unknowns);
            let record = StoredEntityRecordV1::new(
                target,
                version,
                self.plan().contract_version(),
                DurableKeySchemaBindingV1::from_plan(self.resolved.reference()),
                CanonicalRecord::new(fields).expect("fixture entity record"),
            )
            .expect("stored fixture entity");
            EntityObservation::Present(record)
        }
    }

    struct EvaluatedFixture {
        prepared: PreparedFixture,
        snapshot: riffdb_storage_api::ReadSnapshot,
        current: TransactionCurrentState,
        evaluated: EvaluatedCommand,
    }

    fn prepare(
        source: &str,
        command_name: &str,
        fields: &[(&str, CanonicalValue)],
    ) -> PreparedFixture {
        let compiled = compile_contract_source(source).expect("fixture contract compiles");
        let bundle = ValidatedContractBundle::from_compiler_bundle(compiled)
            .expect("fixture bundle revalidates");
        let plan = bundle
            .bundle()
            .commands()
            .iter()
            .find(|candidate| candidate.name() == command_name)
            .expect("fixture command");
        let reference = ExecutablePlanRef::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            plan.command_id(),
            plan.plan_hash(),
        );
        let input = named_record(plan.input().record(), fields, &[]);
        let facts = derive_input_command_facts(plan, input.clone()).expect("fixture input facts");
        let binding_targets = plan
            .bindings()
            .iter()
            .zip(facts.binding_entity_keys())
            .map(|(binding, key)| EntityTarget::new(binding.entity_type(), key.clone()))
            .collect::<Result<Vec<_>, _>>()
            .expect("binding targets");
        let root_targets = plan
            .root_validation_reads()
            .iter()
            .zip(facts.root_validation_entity_keys())
            .map(|(read, key)| EntityTarget::new(read.entity_type(), key.clone()))
            .collect::<Result<Vec<_>, _>>()
            .expect("root targets");
        PreparedFixture {
            resolved: crate::test_support::resolve_genesis_plan(&bundle, &reference)
                .expect("resolved fixture plan"),
            input,
            logical_time: LogicalTime::new(Timestamp::new(123, 456).expect("fixture time")),
            binding_targets,
            root_targets,
        }
    }

    fn evaluate(
        prepared: PreparedFixture,
        bindings: Vec<EntityObservation>,
        roots: Vec<EntityObservation>,
    ) -> EvaluatedFixture {
        let request = SnapshotRequest::new(
            prepared.resolved.reference().clone(),
            prepared.binding_targets.clone(),
            prepared.root_targets.clone(),
            Vec::new(),
        )
        .expect("fixture snapshot request");
        let snapshot = riffdb_storage_api::ReadSnapshot::new(
            &request,
            None,
            bindings.clone(),
            roots.clone(),
            Vec::new(),
        )
        .expect("fixture snapshot");
        let facts = derive_input_command_facts(prepared.plan(), prepared.input.clone())
            .expect("fixture context facts");
        let context = TransactionContext::new(
            RequestId::from_unix_milliseconds_and_random(1, [0x41; 10])
                .expect("fixture request ID"),
            AdmittedActorContext::new(
                ActorId::new("command-validation-test").expect("fixture actor"),
                ActorKind::Service,
                TenantScope::Global,
                None,
            ),
            prepared.resolved.reference().clone(),
            prepared.logical_time,
            facts.partition_key().clone(),
        );
        let ExecutionResult::CommitRequired(evaluated) = execute_command(
            prepared.resolved.bundle().bundle(),
            &prepared.input,
            &snapshot,
            &context,
            EvaluationBudget::v1(),
        )
        .expect("fixture evaluates") else {
            panic!("fixture command must require a commit")
        };
        let current = TransactionCurrentState::new(
            evaluated.validation_request(),
            bindings,
            roots,
            Vec::new(),
        )
        .expect("fixture current state");
        EvaluatedFixture {
            prepared,
            snapshot,
            current,
            evaluated,
        }
    }

    fn named_record(
        schema: &RecordSchema,
        supplied: &[(&str, CanonicalValue)],
        omitted: &[&str],
    ) -> CanonicalRecord {
        let supplied = supplied.iter().cloned().collect::<BTreeMap<_, _>>();
        CanonicalRecord::new(
            schema
                .fields()
                .iter()
                .filter(|field| !omitted.contains(&field.name()))
                .map(|field| {
                    let value = supplied
                        .get(field.name())
                        .cloned()
                        .or_else(|| {
                            field
                                .value_type()
                                .is_optional()
                                .then_some(CanonicalValue::Null)
                        })
                        .unwrap_or_else(|| panic!("missing named field {}", field.name()));
                    (field.id(), value)
                })
                .collect(),
        )
        .expect("named canonical record")
    }

    fn string(value: &str) -> CanonicalValue {
        CanonicalValue::string(value).expect("bounded fixture string")
    }

    fn first_version() -> EntityVersion {
        EntityVersion::first()
    }

    fn next_version() -> EntityVersion {
        EntityVersion::new(2).expect("second entity version")
    }

    fn validation(
        fixture: &EvaluatedFixture,
    ) -> Result<CheckedCommandDecision, CommandValidationError> {
        validate_transaction_current_command_parts(
            &fixture.prepared.resolved,
            &fixture.prepared.input,
            fixture.prepared.logical_time,
            &fixture.evaluated,
            &fixture.current,
        )
    }

    fn assert_integrity(result: Result<CheckedCommandDecision, CommandValidationError>) {
        match result {
            Err(error) => assert_eq!(error, CommandValidationError::integrity()),
            Ok(_) => panic!("must fail closed"),
        }
    }

    fn rebuilt_evaluated(
        fixture: &EvaluatedFixture,
        original: &EvaluatedCommand,
        mutations: Vec<EntityMutation>,
    ) -> EvaluatedCommand {
        EvaluatedCommand::new(
            &fixture.snapshot,
            mutations,
            original.event_intents().to_vec(),
            original.outcome().clone(),
            EvaluationBudget::v1(),
        )
        .expect("structurally valid rebuilt evaluated command")
    }

    fn mutation_with_fields(
        original: &EntityMutation,
        fields: CanonicalRecord,
        contract_version: riffdb_types::ContractVersion,
    ) -> EntityMutation {
        let post_image = EntityPostImage::new(original.target().clone(), contract_version, fields)
            .expect("rebuilt postimage");
        match original {
            EntityMutation::Create(_) => EntityMutation::Create(post_image),
            EntityMutation::Replace {
                expected_version, ..
            } => EntityMutation::Replace {
                expected_version: *expected_version,
                post_image,
            },
        }
    }

    fn without_field(record: &CanonicalRecord, removed: FieldId) -> CanonicalRecord {
        CanonicalRecord::new(
            record
                .fields()
                .iter()
                .filter(|(field, _)| *field != removed)
                .cloned()
                .collect(),
        )
        .expect("record without field")
    }

    fn replace_field(
        record: &CanonicalRecord,
        replaced: FieldId,
        value: CanonicalValue,
    ) -> CanonicalRecord {
        let mut fields = record.fields().to_vec();
        match fields.binary_search_by_key(&replaced, |(field, _)| *field) {
            Ok(index) => fields[index].1 = value,
            Err(index) => fields.insert(index, (replaced, value)),
        }
        CanonicalRecord::new(fields).expect("record with replaced field")
    }

    #[test]
    fn zero_mutation_revalidates_dependencies_then_skips_false_commit_check() {
        let prepared = prepare(
            ZERO_REJECT_SOURCE,
            "Change",
            &[
                ("request_key", string("zero-1")),
                ("id", CanonicalValue::I64(7)),
                ("next", CanonicalValue::I64(9)),
            ],
        );
        let target = prepared.binding_targets[0].clone();
        let fixture = evaluate(
            prepared,
            vec![EntityObservation::Absent(target)],
            Vec::new(),
        );
        assert!(fixture.evaluated.mutations().is_empty());
        assert_eq!(fixture.prepared.plan().commit_checks().len(), 1);
        assert!(matches!(
            validation(&fixture),
            Ok(CheckedCommandDecision::ZeroMutation)
        ));

        let changed = fixture.prepared.present_binding(
            0,
            first_version(),
            &[("value", CanonicalValue::I64(3))],
            Vec::new(),
            &[],
        );
        let changed_current = TransactionCurrentState::new(
            fixture.evaluated.validation_request(),
            vec![changed],
            Vec::new(),
            Vec::new(),
        )
        .expect("changed current");
        assert!(matches!(
            validate_transaction_current_command_parts(
                &fixture.prepared.resolved,
                &fixture.prepared.input,
                fixture.prepared.logical_time,
                &fixture.evaluated,
                &changed_current,
            ),
            Ok(CheckedCommandDecision::Rejected(
                CandidateValidationRejection::DependencyChanged
            ))
        ));
    }

    #[test]
    fn read_only_plan_and_grammar_v1_range_candidates_cannot_gain_commit_proofs() {
        let read_only = prepare(
            READ_ONLY_SOURCE,
            "Find",
            &[("id", CanonicalValue::Uuid([0x61; 16]))],
        );
        assert_eq!(read_only.plan().execution_class(), ExecutionClass::ReadOnly);
        let request = SnapshotRequest::new(
            read_only.resolved.reference().clone(),
            read_only.binding_targets.clone(),
            Vec::new(),
            Vec::new(),
        )
        .expect("read-only snapshot request");
        let absent = EntityObservation::Absent(read_only.binding_targets[0].clone());
        let snapshot = riffdb_storage_api::ReadSnapshot::new(
            &request,
            None,
            vec![absent.clone()],
            Vec::new(),
            Vec::new(),
        )
        .expect("read-only snapshot");
        let facts = derive_input_command_facts(read_only.plan(), read_only.input.clone())
            .expect("read-only input facts");
        let context = TransactionContext::new(
            RequestId::from_unix_milliseconds_and_random(3, [0x43; 10])
                .expect("read-only request ID"),
            AdmittedActorContext::new(
                ActorId::new("command-validation-read-only").expect("read-only actor"),
                ActorKind::Service,
                TenantScope::Global,
                None,
            ),
            read_only.resolved.reference().clone(),
            read_only.logical_time,
            facts.partition_key().clone(),
        );
        let ExecutionResult::ReadOnly(outcome) = execute_command(
            read_only.resolved.bundle().bundle(),
            &read_only.input,
            &snapshot,
            &context,
            EvaluationBudget::v1(),
        )
        .expect("read-only command evaluates") else {
            panic!("fixture must be unjournaled read-only")
        };
        let forged = EvaluatedCommand::new(
            &snapshot,
            Vec::new(),
            Vec::new(),
            outcome,
            EvaluationBudget::v1(),
        )
        .expect("storage value permits structurally valid forged candidate");
        let current = TransactionCurrentState::new(
            forged.validation_request(),
            vec![absent],
            Vec::new(),
            Vec::new(),
        )
        .expect("read-only current state");
        assert_integrity(validate_transaction_current_command_parts(
            &read_only.resolved,
            &read_only.input,
            read_only.logical_time,
            &forged,
            &current,
        ));

        let zero = prepare(
            ZERO_REJECT_SOURCE,
            "Change",
            &[
                ("request_key", string("range-forgery")),
                ("id", CanonicalValue::I64(7)),
                ("next", CanonicalValue::I64(9)),
            ],
        );
        let target = zero.binding_targets[0].clone();
        let ordinary = evaluate(
            zero,
            vec![EntityObservation::Absent(target.clone())],
            Vec::new(),
        );
        let range = IndexRangeTarget::new(IndexRangePrefixBuilder::new(IndexId::first()).finish());
        let forged_request = SnapshotRequest::new(
            ordinary.prepared.resolved.reference().clone(),
            ordinary.prepared.binding_targets.clone(),
            Vec::new(),
            vec![range.clone()],
        )
        .expect("forged range request");
        let range_snapshot = riffdb_storage_api::ReadSnapshot::new(
            &forged_request,
            None,
            vec![EntityObservation::Absent(target.clone())],
            Vec::new(),
            vec![
                IndexRangeObservation::new(
                    range.clone(),
                    IndexEpochPosition::BeforeFirst,
                    Vec::new(),
                )
                .expect("forged range observation"),
            ],
        )
        .expect("forged range snapshot");
        let forged_evaluated = EvaluatedCommand::new(
            &range_snapshot,
            Vec::new(),
            Vec::new(),
            ordinary.evaluated.outcome().clone(),
            EvaluationBudget::v1(),
        )
        .expect("structurally valid range candidate");
        let forged_current = TransactionCurrentState::new(
            forged_evaluated.validation_request(),
            vec![EntityObservation::Absent(target)],
            Vec::new(),
            vec![CurrentRangeObservation::new(
                range,
                IndexEpochPosition::BeforeFirst,
            )],
        )
        .expect("forged range current state");
        assert_integrity(validate_transaction_current_command_parts(
            &ordinary.prepared.resolved,
            &ordinary.prepared.input,
            ordinary.prepared.logical_time,
            &forged_evaluated,
            &forged_current,
        ));
    }

    #[test]
    fn runtime_attempt_provenance_rejects_wrong_binding_and_root_keys() {
        let binding_plan = prepare(
            ZERO_REJECT_SOURCE,
            "Change",
            &[
                ("request_key", string("binding-key")),
                ("id", CanonicalValue::I64(7)),
                ("next", CanonicalValue::I64(9)),
            ],
        );
        let alternate_binding = prepare(
            ZERO_REJECT_SOURCE,
            "Change",
            &[
                ("request_key", string("binding-key")),
                ("id", CanonicalValue::I64(8)),
                ("next", CanonicalValue::I64(9)),
            ],
        );
        assert_eq!(
            binding_plan.resolved.reference(),
            alternate_binding.resolved.reference()
        );
        let wrong_binding = alternate_binding.binding_targets[0].clone();
        let request = SnapshotRequest::new(
            binding_plan.resolved.reference().clone(),
            vec![wrong_binding.clone()],
            Vec::new(),
            Vec::new(),
        )
        .expect("wrong binding request remains structurally valid");
        let snapshot = riffdb_storage_api::ReadSnapshot::new(
            &request,
            None,
            vec![EntityObservation::Absent(wrong_binding)],
            Vec::new(),
            Vec::new(),
        )
        .expect("wrong binding snapshot remains structurally valid");
        let facts = derive_input_command_facts(binding_plan.plan(), binding_plan.input.clone())
            .expect("binding input facts");
        let context = TransactionContext::new(
            RequestId::from_unix_milliseconds_and_random(4, [0x44; 10])
                .expect("binding request ID"),
            AdmittedActorContext::new(
                ActorId::new("command-validation-wrong-binding").expect("binding actor"),
                ActorKind::Service,
                TenantScope::Global,
                None,
            ),
            binding_plan.resolved.reference().clone(),
            binding_plan.logical_time,
            facts.partition_key().clone(),
        );
        assert_eq!(
            execute_command(
                binding_plan.resolved.bundle().bundle(),
                &binding_plan.input,
                &snapshot,
                &context,
                EvaluationBudget::v1(),
            ),
            Err(ExecutionFault::Integrity)
        );

        let root_plan = prepare(
            MIXED_SOURCE,
            "ChangeChildren",
            &[
                ("request_key", string("root-key")),
                ("tenant", CanonicalValue::Uuid([0x14; 16])),
                ("observed_root", CanonicalValue::Uuid([0x24; 16])),
                ("created_root", CanonicalValue::Uuid([0x34; 16])),
                ("changed_child", CanonicalValue::Uuid([0x44; 16])),
                ("new_child", CanonicalValue::Uuid([0x54; 16])),
                ("amount", CanonicalValue::I64(6)),
            ],
        );
        let alternate_root = prepare(
            MIXED_SOURCE,
            "ChangeChildren",
            &[
                ("request_key", string("root-key")),
                ("tenant", CanonicalValue::Uuid([0x14; 16])),
                ("observed_root", CanonicalValue::Uuid([0x24; 16])),
                ("created_root", CanonicalValue::Uuid([0x35; 16])),
                ("changed_child", CanonicalValue::Uuid([0x44; 16])),
                ("new_child", CanonicalValue::Uuid([0x54; 16])),
                ("amount", CanonicalValue::I64(6)),
            ],
        );
        let bindings = vec![
            root_plan.present_binding(
                0,
                first_version(),
                &[("enabled", CanonicalValue::Bool(true))],
                Vec::new(),
                &[],
            ),
            root_plan.present_binding(
                1,
                first_version(),
                &[("amount", CanonicalValue::I64(1))],
                Vec::new(),
                &[],
            ),
            EntityObservation::Absent(root_plan.binding_targets[2].clone()),
        ];
        assert_ne!(root_plan.root_targets[0], alternate_root.root_targets[0]);
        let wrong_root = alternate_root.present_root(
            0,
            first_version(),
            &[("enabled", CanonicalValue::Bool(true))],
            Vec::new(),
            &[],
        );
        let request = SnapshotRequest::new(
            root_plan.resolved.reference().clone(),
            root_plan.binding_targets.clone(),
            vec![alternate_root.root_targets[0].clone()],
            Vec::new(),
        )
        .expect("wrong root request remains structurally valid");
        let snapshot = riffdb_storage_api::ReadSnapshot::new(
            &request,
            None,
            bindings,
            vec![wrong_root],
            Vec::new(),
        )
        .expect("wrong root snapshot remains structurally valid");
        let facts = derive_input_command_facts(root_plan.plan(), root_plan.input.clone())
            .expect("root input facts");
        let context = TransactionContext::new(
            RequestId::from_unix_milliseconds_and_random(5, [0x45; 10]).expect("root request ID"),
            AdmittedActorContext::new(
                ActorId::new("command-validation-wrong-root").expect("root actor"),
                ActorKind::Service,
                TenantScope::Global,
                None,
            ),
            root_plan.resolved.reference().clone(),
            root_plan.logical_time,
            facts.partition_key().clone(),
        );
        assert_eq!(
            execute_command(
                root_plan.resolved.bundle().bundle(),
                &root_plan.input,
                &snapshot,
                &context,
                EvaluationBudget::v1(),
            ),
            Err(ExecutionFault::Integrity)
        );
    }

    #[test]
    fn nonzero_values_use_current_reads_and_roots_but_mutable_postimages() {
        let future_field = FieldId::new(65_000).expect("future field ID");
        let unknown = || vec![(future_field, string("future-private-value"))];
        let prepared = prepare(
            MIXED_SOURCE,
            "ChangeChildren",
            &[
                ("request_key", string("mixed-1")),
                ("tenant", CanonicalValue::Uuid([0x11; 16])),
                ("observed_root", CanonicalValue::Uuid([0x21; 16])),
                ("created_root", CanonicalValue::Uuid([0x31; 16])),
                ("changed_child", CanonicalValue::Uuid([0x41; 16])),
                ("new_child", CanonicalValue::Uuid([0x51; 16])),
                ("amount", CanonicalValue::I64(5)),
            ],
        );
        let read = prepared.present_binding(
            0,
            first_version(),
            &[("enabled", CanonicalValue::Bool(true))],
            unknown(),
            &[],
        );
        let mutate = prepared.present_binding(
            1,
            first_version(),
            &[("amount", CanonicalValue::I64(-100))],
            unknown(),
            &[],
        );
        let create = EntityObservation::Absent(prepared.binding_targets[2].clone());
        let root = prepared.present_root(
            0,
            first_version(),
            &[("enabled", CanonicalValue::Bool(true))],
            unknown(),
            &[],
        );
        let mut fixture = evaluate(prepared, vec![read, mutate, create], vec![root]);
        assert_eq!(fixture.evaluated.mutations().len(), 2);
        assert!(
            fixture
                .evaluated
                .mutations()
                .iter()
                .any(
                    |mutation| record_field(mutation.post_image().fields(), future_field).is_some()
                )
        );

        let missing_exact_optional = fixture.prepared.present_binding(
            1,
            first_version(),
            &[("amount", CanonicalValue::I64(-100))],
            unknown(),
            &["tag"],
        );
        let mut malformed_bindings = fixture.current.bindings().to_vec();
        malformed_bindings[1] = missing_exact_optional;
        let malformed_current = TransactionCurrentState::new(
            fixture.evaluated.validation_request(),
            malformed_bindings,
            fixture.current.root_validations().to_vec(),
            Vec::new(),
        )
        .expect("missing optional remains a structurally valid current DTO");
        assert_integrity(validate_transaction_current_command_parts(
            &fixture.prepared.resolved,
            &fixture.prepared.input,
            fixture.prepared.logical_time,
            &fixture.evaluated,
            &malformed_current,
        ));

        let EntityObservation::Present(exact_record) = &fixture.current.bindings()[1] else {
            panic!("mutable fixture binding must be present")
        };
        let wrong_exact_binding = StoredEntityRecordV1::new(
            exact_record.target().clone(),
            exact_record.entity_version(),
            fixture.prepared.plan().contract_version(),
            DurableKeySchemaBindingV1::new(
                fixture
                    .prepared
                    .resolved
                    .reference()
                    .contract_lineage()
                    .clone(),
                fixture.prepared.plan().contract_version(),
                ContractBundleHash::from_bytes([0xee; 32]),
            ),
            exact_record.fields().clone(),
        )
        .expect("wrong exact bundle remains a structurally valid stored record");
        let mut malformed_bindings = fixture.current.bindings().to_vec();
        malformed_bindings[1] = EntityObservation::Present(wrong_exact_binding);
        let malformed_current = TransactionCurrentState::new(
            fixture.evaluated.validation_request(),
            malformed_bindings,
            fixture.current.root_validations().to_vec(),
            Vec::new(),
        )
        .expect("wrong exact bundle remains a structurally valid current DTO");
        assert_integrity(validate_transaction_current_command_parts(
            &fixture.prepared.resolved,
            &fixture.prepared.input,
            fixture.prepared.logical_time,
            &fixture.evaluated,
            &malformed_current,
        ));

        let coverage = prove_mutation_coverage(
            &fixture.prepared.resolved,
            &fixture.evaluated,
            &fixture.current,
        )
        .expect("exact mixed coverage");
        assert_eq!(coverage.iter().filter(|value| value.is_some()).count(), 2);
        assert_eq!(coverage[BindingId::new(0).get() as usize], None);
        let values = assemble_transaction_current_values(
            &fixture.prepared.resolved,
            &fixture.prepared.input,
            fixture.prepared.logical_time,
            &fixture.evaluated,
            &fixture.current,
            &coverage,
        )
        .expect("owned mixed values");
        let amount = entity_field(&fixture, "Child", "amount");
        let enabled = entity_field(&fixture, "Root", "enabled");
        let note = entity_field(&fixture, "Root", "note");
        assert_eq!(
            values.bound_field(BindingId::new(0), enabled),
            Some(CanonicalValue::Bool(true))
        );
        assert_eq!(
            values.bound_field(BindingId::new(1), amount),
            Some(CanonicalValue::I64(5))
        );
        assert_eq!(
            values.bound_field(BindingId::new(2), amount),
            Some(CanonicalValue::I64(5))
        );
        assert_eq!(
            values.root_validation_field(RootValidationReadId::new(0), enabled),
            Some(CanonicalValue::Bool(true))
        );
        assert_eq!(
            values.bound_field(BindingId::new(0), note),
            Some(CanonicalValue::Null)
        );
        assert_eq!(
            values.root_validation_field(RootValidationReadId::new(0), note),
            Some(CanonicalValue::Null)
        );
        assert_eq!(
            values.input_field(input_field(&fixture, "amount")),
            Some(CanonicalValue::I64(5))
        );
        assert_eq!(
            values.transaction_time(),
            Some(fixture.prepared.logical_time)
        );
        for binding in [BindingId::new(0), BindingId::new(1)] {
            let CanonicalValue::Record(record) =
                values.complete_binding(binding).expect("complete binding")
            else {
                panic!("complete binding must be a record")
            };
            assert!(record_field(&record, future_field).is_none());
        }
        assert_eq!(
            evaluate_commit_checks(
                fixture.prepared.plan().expressions(),
                fixture.prepared.plan().commit_checks(),
                &values,
            ),
            Ok(CommitCheckResult::Satisfied)
        );

        let CheckedCommandDecision::NonZero(proof_coverage) =
            validation(&fixture).expect("mixed validation")
        else {
            panic!("mixed candidate must validate")
        };
        assert_eq!(proof_coverage.as_ref(), coverage.as_ref());
        assert_eq!(proof_coverage[BindingId::new(0).get() as usize], None);

        let original = fixture.evaluated.clone();
        let tag = entity_field(&fixture, "Child", "tag");
        let replace_position = original
            .mutations()
            .iter()
            .position(|mutation| mutation.target() == &fixture.prepared.binding_targets[1])
            .expect("replace mutation position");
        let create_position = original
            .mutations()
            .iter()
            .position(|mutation| mutation.target() == &fixture.prepared.binding_targets[2])
            .expect("create mutation position");

        let mut missing_optional = original.mutations().to_vec();
        let fields = without_field(
            missing_optional[replace_position].post_image().fields(),
            tag,
        );
        missing_optional[replace_position] = mutation_with_fields(
            &missing_optional[replace_position],
            fields,
            fixture.prepared.plan().contract_version(),
        );
        fixture.evaluated = rebuilt_evaluated(&fixture, &original, missing_optional);
        assert_integrity(validation(&fixture));

        let mut dropped_unknown = original.mutations().to_vec();
        let fields = without_field(
            dropped_unknown[replace_position].post_image().fields(),
            future_field,
        );
        dropped_unknown[replace_position] = mutation_with_fields(
            &dropped_unknown[replace_position],
            fields,
            fixture.prepared.plan().contract_version(),
        );
        fixture.evaluated = rebuilt_evaluated(&fixture, &original, dropped_unknown);
        assert_integrity(validation(&fixture));

        let mut altered_unknown = original.mutations().to_vec();
        let fields = replace_field(
            altered_unknown[replace_position].post_image().fields(),
            future_field,
            string("altered-private-value"),
        );
        altered_unknown[replace_position] = mutation_with_fields(
            &altered_unknown[replace_position],
            fields,
            fixture.prepared.plan().contract_version(),
        );
        fixture.evaluated = rebuilt_evaluated(&fixture, &original, altered_unknown);
        assert_integrity(validation(&fixture));

        let mut create_unknown = original.mutations().to_vec();
        let fields = replace_field(
            create_unknown[create_position].post_image().fields(),
            future_field,
            string("invented-private-value"),
        );
        create_unknown[create_position] = mutation_with_fields(
            &create_unknown[create_position],
            fields,
            fixture.prepared.plan().contract_version(),
        );
        fixture.evaluated = rebuilt_evaluated(&fixture, &original, create_unknown);
        assert_integrity(validation(&fixture));

        let mut nested_outcome = original.outcome().value().fields().to_vec();
        let (_, nested) = nested_outcome
            .iter_mut()
            .find(|(_, value)| matches!(value, CanonicalValue::Record(_)))
            .expect("mixed outcome nested entity record");
        *nested =
            CanonicalValue::Record(CanonicalRecord::new(Vec::new()).expect("empty nested record"));
        fixture.evaluated = EvaluatedCommand::new(
            &fixture.snapshot,
            original.mutations().to_vec(),
            original.event_intents().to_vec(),
            DeclaredOutcome::new(
                original.outcome().outcome_id(),
                CanonicalRecord::new(nested_outcome).expect("malformed nested outcome"),
            )
            .expect("bounded nested outcome"),
            EvaluationBudget::v1(),
        )
        .expect("structurally valid malformed nested outcome");
        assert_integrity(validation(&fixture));
    }

    #[test]
    fn false_and_arithmetic_commit_checks_map_to_closed_rejections() {
        let false_prepared = prepare(
            FALSE_CHECK_SOURCE,
            "Change",
            &[
                ("request_key", string("false-1")),
                ("id", CanonicalValue::I64(1)),
                ("next", CanonicalValue::I64(-1)),
            ],
        );
        let false_present = false_prepared.present_binding(
            0,
            first_version(),
            &[("value", CanonicalValue::I64(1))],
            Vec::new(),
            &[],
        );
        let false_fixture = evaluate(false_prepared, vec![false_present], Vec::new());
        assert!(matches!(
            validation(&false_fixture),
            Ok(CheckedCommandDecision::Rejected(
                CandidateValidationRejection::CommitCheckRejected
            ))
        ));

        let arithmetic_prepared = prepare(
            ARITHMETIC_CHECK_SOURCE,
            "Change",
            &[
                ("request_key", string("arithmetic-1")),
                ("id", CanonicalValue::I64(1)),
                ("numerator", CanonicalValue::I64(4)),
                ("divisor", CanonicalValue::I64(0)),
            ],
        );
        let arithmetic_present = arithmetic_prepared.present_binding(
            0,
            first_version(),
            &[
                ("numerator", CanonicalValue::I64(4)),
                ("divisor", CanonicalValue::I64(2)),
            ],
            Vec::new(),
            &[],
        );
        let arithmetic_fixture =
            evaluate(arithmetic_prepared, vec![arithmetic_present], Vec::new());
        assert!(matches!(
            validation(&arithmetic_fixture),
            Ok(CheckedCommandDecision::Rejected(
                CandidateValidationRejection::CommitCheckArithmeticFault
            ))
        ));
    }

    #[test]
    fn nonzero_coverage_rejects_missing_and_extra_binding_mutations_as_integrity() {
        let prepared = prepare(
            MIXED_SOURCE,
            "ChangeChildren",
            &[
                ("request_key", string("coverage-1")),
                ("tenant", CanonicalValue::Uuid([0x12; 16])),
                ("observed_root", CanonicalValue::Uuid([0x22; 16])),
                ("created_root", CanonicalValue::Uuid([0x32; 16])),
                ("changed_child", CanonicalValue::Uuid([0x42; 16])),
                ("new_child", CanonicalValue::Uuid([0x52; 16])),
                ("amount", CanonicalValue::I64(8)),
            ],
        );
        let read = prepared.present_binding(
            0,
            first_version(),
            &[("enabled", CanonicalValue::Bool(true))],
            Vec::new(),
            &[],
        );
        let mutate = prepared.present_binding(
            1,
            first_version(),
            &[("amount", CanonicalValue::I64(1))],
            Vec::new(),
            &[],
        );
        let create = EntityObservation::Absent(prepared.binding_targets[2].clone());
        let root = prepared.present_root(
            0,
            first_version(),
            &[("enabled", CanonicalValue::Bool(true))],
            Vec::new(),
            &[],
        );
        let mut fixture = evaluate(prepared, vec![read, mutate, create], vec![root]);

        fixture.evaluated = EvaluatedCommand::new(
            &fixture.snapshot,
            vec![fixture.evaluated.mutations()[0].clone()],
            fixture.evaluated.event_intents().to_vec(),
            fixture.evaluated.outcome().clone(),
            EvaluationBudget::v1(),
        )
        .expect("structurally valid missing mutation");
        assert_integrity(validation(&fixture));

        let read_record = match &fixture.snapshot.bindings()[0] {
            EntityObservation::Present(record) => record,
            EntityObservation::Absent(_) => panic!("read fixture must be present"),
        };
        let read_mutation = EntityMutation::Replace {
            expected_version: read_record.entity_version(),
            post_image: EntityPostImage::new(
                read_record.target().clone(),
                fixture.prepared.plan().contract_version(),
                read_record.fields().clone(),
            )
            .expect("read-target postimage"),
        };
        let mut extra = fixture
            .snapshot
            .bindings()
            .iter()
            .skip(1)
            .zip(fixture.prepared.plan().bindings().iter().skip(1))
            .map(|_| ())
            .count();
        assert_eq!(extra, 2);
        let original = execute_again(&fixture);
        let mut mutations = original.mutations().to_vec();
        mutations.push(read_mutation);
        mutations.sort_by(|left, right| left.target().cmp(right.target()));
        extra = mutations.len();
        assert_eq!(extra, 3);
        fixture.evaluated = EvaluatedCommand::new(
            &fixture.snapshot,
            mutations,
            original.event_intents().to_vec(),
            original.outcome().clone(),
            EvaluationBudget::v1(),
        )
        .expect("structurally valid extra read-target mutation");
        assert_integrity(validation(&fixture));
    }

    #[test]
    fn event_order_type_payload_and_success_outcome_shape_are_historical_plan_proofs() {
        let prepared = prepare(
            EVENT_SOURCE,
            "Change",
            &[
                ("request_key", string("events-1")),
                ("id", CanonicalValue::I64(4)),
                ("next", CanonicalValue::I64(6)),
            ],
        );
        let present = prepared.present_binding(
            0,
            first_version(),
            &[("value", CanonicalValue::I64(1))],
            Vec::new(),
            &[],
        );
        let mut fixture = evaluate(prepared, vec![present], Vec::new());
        assert!(matches!(
            validation(&fixture),
            Ok(CheckedCommandDecision::NonZero(_))
        ));

        let mut reversed = fixture.evaluated.event_intents().to_vec();
        reversed.reverse();
        fixture.evaluated = EvaluatedCommand::new(
            &fixture.snapshot,
            fixture.evaluated.mutations().to_vec(),
            reversed,
            fixture.evaluated.outcome().clone(),
            EvaluationBudget::v1(),
        )
        .expect("structurally valid reversed events");
        assert_integrity(validation(&fixture));

        let original = execute_again(&fixture);
        let wrong_payload = EventIntent::new(
            original.event_intents()[0].event_type_id(),
            CanonicalRecord::new(Vec::new()).expect("empty event payload"),
        )
        .expect("bounded wrong event");
        let mut events = original.event_intents().to_vec();
        events[0] = wrong_payload;
        fixture.evaluated = EvaluatedCommand::new(
            &fixture.snapshot,
            original.mutations().to_vec(),
            events,
            original.outcome().clone(),
            EvaluationBudget::v1(),
        )
        .expect("structurally valid wrong event payload");
        assert_integrity(validation(&fixture));

        fixture.evaluated = EvaluatedCommand::new(
            &fixture.snapshot,
            original.mutations().to_vec(),
            original.event_intents().to_vec(),
            DeclaredOutcome::new(
                original.outcome().outcome_id(),
                CanonicalRecord::new(Vec::new()).expect("empty outcome payload"),
            )
            .expect("bounded wrong outcome"),
            EvaluationBudget::v1(),
        )
        .expect("structurally valid wrong outcome payload");
        assert_integrity(validation(&fixture));
    }

    #[test]
    fn normalized_input_postimage_schema_and_diagnostics_fail_closed() {
        let prepared = prepare(
            FALSE_CHECK_SOURCE,
            "Change",
            &[
                ("request_key", string("integrity-1")),
                ("id", CanonicalValue::I64(9)),
                ("next", CanonicalValue::I64(10)),
            ],
        );
        let present = prepared.present_binding(
            0,
            first_version(),
            &[("value", CanonicalValue::I64(-7))],
            Vec::new(),
            &[],
        );
        let mut fixture = evaluate(prepared, vec![present], Vec::new());
        assert!(matches!(
            validation(&fixture),
            Ok(CheckedCommandDecision::NonZero(_))
        ));

        let bad_input = CanonicalRecord::new(Vec::new()).expect("empty bad input");
        assert_integrity(validate_transaction_current_command_parts(
            &fixture.prepared.resolved,
            &bad_input,
            fixture.prepared.logical_time,
            &fixture.evaluated,
            &fixture.current,
        ));

        let mutation = &fixture.evaluated.mutations()[0];
        let malformed = match mutation {
            EntityMutation::Replace {
                expected_version,
                post_image,
            } => EntityMutation::Replace {
                expected_version: *expected_version,
                post_image: EntityPostImage::new(
                    post_image.target().clone(),
                    post_image.written_by_contract(),
                    CanonicalRecord::new(Vec::new()).expect("empty bad postimage"),
                )
                .expect("bounded malformed postimage"),
            },
            EntityMutation::Create(_) => panic!("fixture must replace"),
        };
        fixture.evaluated = EvaluatedCommand::new(
            &fixture.snapshot,
            vec![malformed],
            fixture.evaluated.event_intents().to_vec(),
            fixture.evaluated.outcome().clone(),
            EvaluationBudget::v1(),
        )
        .expect("structurally valid malformed postimage");
        assert_integrity(validation(&fixture));

        let error = CommandValidationError::integrity();
        assert_eq!(format!("{error:?}"), "CommandValidationError([REDACTED])");
        assert_eq!(
            format!("{error}"),
            "transaction-current command validation failed"
        );
        assert!(error.source().is_none());
    }

    fn execute_again(fixture: &EvaluatedFixture) -> EvaluatedCommand {
        let facts =
            derive_input_command_facts(fixture.prepared.plan(), fixture.prepared.input.clone())
                .expect("repeat facts");
        let context = TransactionContext::new(
            RequestId::from_unix_milliseconds_and_random(2, [0x42; 10]).expect("repeat request"),
            AdmittedActorContext::new(
                ActorId::new("command-validation-test").expect("repeat actor"),
                ActorKind::Service,
                TenantScope::Global,
                None,
            ),
            fixture.prepared.resolved.reference().clone(),
            fixture.prepared.logical_time,
            facts.partition_key().clone(),
        );
        let ExecutionResult::CommitRequired(evaluated) = execute_command(
            fixture.prepared.resolved.bundle().bundle(),
            &fixture.prepared.input,
            &fixture.snapshot,
            &context,
            EvaluationBudget::v1(),
        )
        .expect("repeat evaluation") else {
            panic!("repeat must require commit")
        };
        evaluated
    }

    fn entity_field(fixture: &EvaluatedFixture, entity: &str, field: &str) -> FieldId {
        fixture
            .prepared
            .resolved
            .bundle()
            .bundle()
            .schema()
            .entities()
            .iter()
            .find(|candidate| candidate.name() == entity)
            .and_then(|candidate| {
                candidate
                    .record()
                    .fields()
                    .iter()
                    .find(|candidate| candidate.name() == field)
            })
            .expect("entity field")
            .id()
    }

    fn input_field(fixture: &EvaluatedFixture, field: &str) -> FieldId {
        fixture
            .prepared
            .plan()
            .input()
            .record()
            .fields()
            .iter()
            .find(|candidate| candidate.name() == field)
            .expect("input field")
            .id()
    }

    #[test]
    fn current_version_change_is_dependency_rejection_not_mutation_precondition_mapping() {
        let prepared = prepare(
            FALSE_CHECK_SOURCE,
            "Change",
            &[
                ("request_key", string("version-1")),
                ("id", CanonicalValue::I64(3)),
                ("next", CanonicalValue::I64(4)),
            ],
        );
        let initial = prepared.present_binding(
            0,
            first_version(),
            &[("value", CanonicalValue::I64(1))],
            Vec::new(),
            &[],
        );
        let mut fixture = evaluate(prepared, vec![initial], Vec::new());
        let changed = fixture.prepared.present_binding(
            0,
            next_version(),
            &[("value", CanonicalValue::I64(1))],
            Vec::new(),
            &[],
        );
        fixture.current = TransactionCurrentState::new(
            fixture.evaluated.validation_request(),
            vec![changed],
            Vec::new(),
            Vec::new(),
        )
        .expect("changed version current");
        assert!(matches!(
            validation(&fixture),
            Ok(CheckedCommandDecision::Rejected(
                CandidateValidationRejection::DependencyChanged
            ))
        ));
    }
}
