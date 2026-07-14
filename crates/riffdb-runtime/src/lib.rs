#![forbid(unsafe_code)]

//! Synchronous deterministic execution of checked command plans.
//!
//! Execution starts from an already-owned [`ReadSnapshot`]. This crate does not
//! perform pre-snapshot admission or capability acquisition and owns no I/O,
//! clock, entropy, provenance, or durable commit authority.

use std::error::Error;
use std::fmt;

use riffdb_contract_ir::{
    BindingId, BindingMode, CommandPlan, ContractBundle, ExecutionClass, Instruction,
    ObjectConstruction, RecordSchema, RootValidationReadId, SchemaIr, ValueType,
};
use riffdb_invariant::{
    EvaluationBatch, EvaluationError, ExpressionEvaluator, ExpressionValueSource,
};
use riffdb_storage_api::{
    DeclaredOutcome, EntityMutation, EntityObservation, EntityPostImage, EntityTarget,
    EvaluatedCommand, EvaluatedCommandBuilder, EvaluationBudget, EventIntent, ExecutablePlanRef,
    ReadSnapshot, StorageValueError,
};
use riffdb_types::{
    AdmittedActorContext, CanonicalCodecError, CanonicalRecord, CanonicalValue, FieldId,
    LogicalTime, MAX_CANONICAL_DOCUMENT_BYTES, PartitionKey, RequestId, ValueError,
    encode_canonical_record, encode_canonical_value,
};

/// Immutable values admitted for one deterministic command evaluation.
#[derive(Clone, Eq, PartialEq)]
pub struct TransactionContext {
    request_id: RequestId,
    actor: AdmittedActorContext,
    plan: ExecutablePlanRef,
    tx_time: LogicalTime,
    partition_key: PartitionKey,
}

impl TransactionContext {
    /// Constructs a context from coordinator- and policy-checked values.
    #[must_use]
    pub const fn new(
        request_id: RequestId,
        actor: AdmittedActorContext,
        plan: ExecutablePlanRef,
        tx_time: LogicalTime,
        partition_key: PartitionKey,
    ) -> Self {
        Self {
            request_id,
            actor,
            plan,
            tx_time,
            partition_key,
        }
    }

    /// Returns the original admitted request identity.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Borrows the immutable admitted actor context.
    #[must_use]
    pub const fn actor(&self) -> &AdmittedActorContext {
        &self.actor
    }

    /// Borrows the exact historical executable-plan identity.
    #[must_use]
    pub const fn plan(&self) -> &ExecutablePlanRef {
        &self.plan
    }

    /// Returns the fixed coordinator-supplied logical time.
    #[must_use]
    pub const fn tx_time(&self) -> LogicalTime {
        self.tx_time
    }

    /// Borrows the validated one-aggregate partition identity.
    #[must_use]
    pub const fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }
}

impl fmt::Debug for TransactionContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TransactionContext([REDACTED])")
    }
}

/// The direct unjournaled representation of one declared read-only outcome.
pub type EncodedOutcome = DeclaredOutcome;

/// Closed deterministic result of one command evaluation.
///
/// The commit-required value stays inline to preserve the accepted semantic
/// boundary; allocation strategy remains an orchestration concern.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Eq, PartialEq)]
pub enum ExecutionResult {
    /// Direct result of an unjournaled read-only command.
    ReadOnly(EncodedOutcome),
    /// Bounded candidate that only the commit coordinator may enrich and commit.
    CommitRequired(EvaluatedCommand),
}

impl fmt::Debug for ExecutionResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadOnly(_) => formatter.write_str("ExecutionResult::ReadOnly([REDACTED])"),
            Self::CommitRequired(_) => {
                formatter.write_str("ExecutionResult::CommitRequired([REDACTED])")
            }
        }
    }
}

/// Closed deterministic failure produced by checked command evaluation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ExecutionFault {
    /// Checked arithmetic overflow, underflow, or invalid division.
    Arithmetic,
    /// A fixed accepted runtime or semantic-output budget was exceeded.
    ResourceLimit,
    /// The checked bundle, context, snapshot, or produced value is inconsistent.
    Integrity,
}

impl fmt::Display for ExecutionFault {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Arithmetic => "command arithmetic failed",
            Self::ResourceLimit => "command evaluation exceeded a fixed limit",
            Self::Integrity => "command evaluation detected inconsistent checked state",
        })
    }
}

impl Error for ExecutionFault {}

/// Executes one exact checked command plan against a complete owned snapshot.
///
/// The caller must perform admission, authorization, capability acquisition, and
/// snapshot materialization before this synchronous call. A returned
/// [`EvaluatedCommand`] still requires transaction-current dependency and
/// commit-check validation by the commit coordinator.
pub fn execute_command(
    bundle: &ContractBundle,
    input: &CanonicalRecord,
    snapshot: &ReadSnapshot,
    context: &TransactionContext,
    budget: EvaluationBudget,
) -> Result<ExecutionResult, ExecutionFault> {
    let plan = validate_execution_identity(bundle, snapshot, context)?;
    validate_record_exact(bundle.schema(), plan.input().record(), input)?;
    let mut evaluator = ExpressionEvaluator::new(plan.expressions());
    validate_snapshot_targets(plan, input, snapshot, context, &mut evaluator)?;

    let mut records = Vec::with_capacity(plan.bindings().len());
    for (binding, observation) in plan.bindings().iter().zip(snapshot.bindings()) {
        match (binding.mode(), observation) {
            (BindingMode::Read | BindingMode::Mutate, EntityObservation::Absent(_))
            | (BindingMode::Create, EntityObservation::Present(_)) => {
                let values = RuntimeValues {
                    schema: bundle.schema(),
                    plan,
                    input,
                    records: &records,
                    roots: &[],
                    tx_time: context.tx_time(),
                };
                let mut evaluation = evaluator.batch(&values);
                let outcome = construct_outcome(binding.failure(), &mut evaluation)?;
                return finish_declared(plan, snapshot, budget, outcome, vec![], vec![]);
            }
            (BindingMode::Read | BindingMode::Mutate, EntityObservation::Present(record)) => {
                let entity = bundle
                    .schema()
                    .entity(binding.entity_type())
                    .ok_or(ExecutionFault::Integrity)?;
                records.push(Some(materialize_entity_record(
                    bundle.schema(),
                    entity.record(),
                    entity.primary_key_fields(),
                    binding.key_schema(),
                    record.target(),
                    record.fields(),
                )?));
            }
            (BindingMode::Create, EntityObservation::Absent(target)) => {
                let entity = bundle
                    .schema()
                    .entity(binding.entity_type())
                    .ok_or(ExecutionFault::Integrity)?;
                records.push(Some(initialize_create_record(
                    entity.record(),
                    entity.primary_key_fields(),
                    binding.key_schema(),
                    target,
                )?));
            }
        }
    }

    let mut roots = Vec::with_capacity(plan.root_validation_reads().len());
    for (read, observation) in plan
        .root_validation_reads()
        .iter()
        .zip(snapshot.root_validations())
    {
        let EntityObservation::Present(record) = observation else {
            return Err(ExecutionFault::Integrity);
        };
        let entity = bundle
            .schema()
            .entity(read.entity_type())
            .ok_or(ExecutionFault::Integrity)?;
        roots.push(materialize_entity_record(
            bundle.schema(),
            entity.record(),
            entity.primary_key_fields(),
            read.key_schema(),
            record.target(),
            record.fields(),
        )?);
    }

    let mut evaluated_builder = None;
    for instruction in plan.instructions() {
        match instruction {
            Instruction::Require {
                predicate, reject, ..
            } => {
                let rejected = {
                    let values = RuntimeValues {
                        schema: bundle.schema(),
                        plan,
                        input,
                        records: &records,
                        roots: &roots,
                        tx_time: context.tx_time(),
                    };
                    let mut evaluation = evaluator.batch(&values);
                    if evaluation.evaluate_predicate(*predicate)? {
                        None
                    } else {
                        Some(construct_outcome(reject, &mut evaluation)?)
                    }
                };
                if let Some(outcome) = rejected {
                    return finish_declared(plan, snapshot, budget, outcome, vec![], vec![]);
                }
            }
            Instruction::SetField {
                binding,
                field,
                value,
            } => {
                let value = {
                    let values = RuntimeValues {
                        schema: bundle.schema(),
                        plan,
                        input,
                        records: &records,
                        roots: &roots,
                        tx_time: context.tx_time(),
                    };
                    evaluator.batch(&values).evaluate(*value)?
                };
                set_working_field(bundle.schema(), plan, &mut records, *binding, *field, value)?;
            }
            Instruction::EmitEvent(event) => {
                let payload = {
                    let values = RuntimeValues {
                        schema: bundle.schema(),
                        plan,
                        input,
                        records: &records,
                        roots: &roots,
                        tx_time: context.tx_time(),
                    };
                    let mut evaluation = evaluator.batch(&values);
                    construct_record(event.payload(), &mut evaluation)?
                };
                if plan.execution_class() != ExecutionClass::IdempotentMutation {
                    return Err(ExecutionFault::Integrity);
                }
                let event = EventIntent::new(event.event_type(), payload)
                    .map_err(map_storage_value_error)?;
                let builder = match evaluated_builder.as_mut() {
                    Some(builder) => builder,
                    None => evaluated_builder.insert(
                        EvaluatedCommandBuilder::new(snapshot, budget)
                            .map_err(map_storage_value_error)?,
                    ),
                };
                builder.push_event(event).map_err(map_storage_value_error)?;
            }
            Instruction::Return(outcome) => {
                let outcome = {
                    let values = RuntimeValues {
                        schema: bundle.schema(),
                        plan,
                        input,
                        records: &records,
                        roots: &roots,
                        tx_time: context.tx_time(),
                    };
                    let mut evaluation = evaluator.batch(&values);
                    construct_outcome(outcome, &mut evaluation)?
                };
                return finish_success(
                    bundle.schema(),
                    plan,
                    snapshot,
                    budget,
                    outcome,
                    &records,
                    evaluated_builder,
                );
            }
        }
    }
    Err(ExecutionFault::Integrity)
}

fn validate_execution_identity<'a>(
    bundle: &'a ContractBundle,
    snapshot: &ReadSnapshot,
    context: &TransactionContext,
) -> Result<&'a CommandPlan, ExecutionFault> {
    let plan = bundle
        .command(context.plan().command_id())
        .ok_or(ExecutionFault::Integrity)?;
    let expected = ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        plan.command_id(),
        plan.plan_hash(),
    );
    if context.plan() != &expected || snapshot.plan() != &expected {
        return Err(ExecutionFault::Integrity);
    }
    Ok(plan)
}

fn validate_snapshot_targets(
    plan: &CommandPlan,
    input: &CanonicalRecord,
    snapshot: &ReadSnapshot,
    context: &TransactionContext,
    evaluator: &mut ExpressionEvaluator<'_>,
) -> Result<(), ExecutionFault> {
    if snapshot.bindings().len() != plan.bindings().len()
        || snapshot.root_validations().len() != plan.root_validation_reads().len()
        || !snapshot.ranges().is_empty()
    {
        return Err(ExecutionFault::Integrity);
    }

    let input_values = InputValues { input };
    let mut evaluation = evaluator.batch(&input_values);
    let partition_value = evaluation
        .evaluate(plan.locality().partition_expression())
        .map_err(map_prepared_evaluation_error)?;
    let partition = plan
        .locality()
        .partition_schema()
        .encode_partition(&[partition_value])
        .map_err(|_| ExecutionFault::Integrity)?;
    if context.partition_key() != &partition {
        return Err(ExecutionFault::Integrity);
    }

    for (binding, observation) in plan.bindings().iter().zip(snapshot.bindings()) {
        let target = derive_target(
            binding.entity_type(),
            binding.key_schema(),
            binding.key_expressions(),
            &mut evaluation,
        )?;
        if observation.target() != &target {
            return Err(ExecutionFault::Integrity);
        }
    }
    for (read, observation) in plan
        .root_validation_reads()
        .iter()
        .zip(snapshot.root_validations())
    {
        let target = derive_target(
            read.entity_type(),
            read.key_schema(),
            read.key_expressions(),
            &mut evaluation,
        )?;
        if observation.target() != &target {
            return Err(ExecutionFault::Integrity);
        }
    }
    Ok(())
}

fn derive_target<Values: ExpressionValueSource + ?Sized>(
    entity_type: riffdb_types::EntityTypeId,
    schema: &riffdb_contract_ir::KeySchema,
    expressions: &[riffdb_contract_ir::ExprId],
    evaluation: &mut EvaluationBatch<'_, '_, '_, Values>,
) -> Result<EntityTarget, ExecutionFault> {
    let components = expressions
        .iter()
        .map(|expression| {
            evaluation
                .evaluate(*expression)
                .map_err(map_prepared_evaluation_error)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let key = schema
        .encode_entity(&components)
        .map_err(|_| ExecutionFault::Integrity)?;
    EntityTarget::new(entity_type, key).map_err(map_storage_value_error)
}

fn validate_record_exact(
    schema: &SchemaIr,
    declared: &RecordSchema,
    actual: &CanonicalRecord,
) -> Result<(), ExecutionFault> {
    if declared.fields().len() != actual.fields().len() {
        return Err(ExecutionFault::Integrity);
    }
    for (field, (actual_id, value)) in declared.fields().iter().zip(actual.fields()) {
        if field.id() != *actual_id {
            return Err(ExecutionFault::Integrity);
        }
        validate_value(schema, field.value_type(), value)?;
    }
    Ok(())
}

fn validate_value(
    schema: &SchemaIr,
    value_type: &ValueType,
    value: &CanonicalValue,
) -> Result<(), ExecutionFault> {
    value_type
        .validate_value(value)
        .map_err(|_| ExecutionFault::Integrity)?;
    if matches!(value, CanonicalValue::Null) {
        return Ok(());
    }
    if let Some(inner) = value_type.optional_inner() {
        return validate_value(schema, inner, value);
    }
    if let Some(enum_id) = value_type.enum_type_id() {
        let CanonicalValue::Enum {
            type_id,
            variant_id,
        } = value
        else {
            return Err(ExecutionFault::Integrity);
        };
        if *type_id != enum_id
            || schema
                .enumeration(enum_id)
                .is_none_or(|enumeration| !enumeration.contains_variant(*variant_id))
        {
            return Err(ExecutionFault::Integrity);
        }
    }
    if let Some((element, _)) = value_type.list_parts() {
        let CanonicalValue::List(values) = value else {
            return Err(ExecutionFault::Integrity);
        };
        for value in values.values() {
            validate_value(schema, element, value)?;
        }
    }
    Ok(())
}

fn materialize_entity_record(
    schema: &SchemaIr,
    declared: &RecordSchema,
    key_fields: &[FieldId],
    key_schema: &riffdb_contract_ir::KeySchema,
    target: &EntityTarget,
    stored: &CanonicalRecord,
) -> Result<CanonicalRecord, ExecutionFault> {
    let key_values = key_schema
        .decode_entity(target.key())
        .map_err(|_| ExecutionFault::Integrity)?;
    if key_values.len() != key_fields.len() {
        return Err(ExecutionFault::Integrity);
    }
    let mut fields = stored.fields().to_vec();
    for declared_field in declared.fields() {
        match record_field(stored, declared_field.id()) {
            Some(value) => validate_value(schema, declared_field.value_type(), value)?,
            None if declared_field.value_type().is_optional() => {
                fields.push((declared_field.id(), CanonicalValue::Null));
            }
            None => return Err(ExecutionFault::Integrity),
        }
    }
    for (field, expected) in key_fields.iter().zip(key_values) {
        if record_field(stored, *field) != Some(&expected) {
            return Err(ExecutionFault::Integrity);
        }
    }
    CanonicalRecord::new(fields).map_err(map_value_error)
}

fn initialize_create_record(
    declared: &RecordSchema,
    key_fields: &[FieldId],
    key_schema: &riffdb_contract_ir::KeySchema,
    target: &EntityTarget,
) -> Result<CanonicalRecord, ExecutionFault> {
    let key_values = key_schema
        .decode_entity(target.key())
        .map_err(|_| ExecutionFault::Integrity)?;
    if key_values.len() != key_fields.len() {
        return Err(ExecutionFault::Integrity);
    }
    let mut fields = Vec::with_capacity(declared.fields().len());
    for field in declared.fields() {
        if let Some(index) = key_fields.iter().position(|key| *key == field.id()) {
            fields.push((field.id(), key_values[index].clone()));
        } else if field.value_type().is_optional() {
            fields.push((field.id(), CanonicalValue::Null));
        }
    }
    CanonicalRecord::new(fields).map_err(map_value_error)
}

fn set_working_field(
    schema: &SchemaIr,
    plan: &CommandPlan,
    records: &mut [Option<CanonicalRecord>],
    binding_id: BindingId,
    field_id: FieldId,
    value: CanonicalValue,
) -> Result<(), ExecutionFault> {
    let binding = plan
        .bindings()
        .get(binding_id.get() as usize)
        .filter(|binding| binding.id() == binding_id)
        .ok_or(ExecutionFault::Integrity)?;
    if !matches!(binding.mode(), BindingMode::Mutate | BindingMode::Create) {
        return Err(ExecutionFault::Integrity);
    }
    let field = schema
        .entity(binding.entity_type())
        .and_then(|entity| entity.record().field(field_id))
        .ok_or(ExecutionFault::Integrity)?;
    validate_value(schema, field.value_type(), &value)?;
    let record = records
        .get_mut(binding_id.get() as usize)
        .and_then(Option::as_mut)
        .ok_or(ExecutionFault::Integrity)?;
    let mut fields = record.fields().to_vec();
    match fields.binary_search_by_key(&field_id, |(field, _)| *field) {
        Ok(index) => fields[index].1 = value,
        Err(index) => fields.insert(index, (field_id, value)),
    }
    *record = CanonicalRecord::new(fields).map_err(map_value_error)?;
    Ok(())
}

fn construct_outcome<Values: ExpressionValueSource + ?Sized>(
    construction: &riffdb_contract_ir::OutcomeConstruction,
    evaluation: &mut EvaluationBatch<'_, '_, '_, Values>,
) -> Result<DeclaredOutcome, ExecutionFault> {
    let value = construct_record(construction.payload(), evaluation)?;
    DeclaredOutcome::new(construction.outcome_id(), value).map_err(map_storage_value_error)
}

fn construct_record<Values: ExpressionValueSource + ?Sized>(
    construction: &ObjectConstruction,
    evaluation: &mut EvaluationBatch<'_, '_, '_, Values>,
) -> Result<CanonicalRecord, ExecutionFault> {
    let mut fields = Vec::with_capacity(construction.fields().len());
    let empty = CanonicalRecord::new(Vec::new()).map_err(map_value_error)?;
    let mut encoded_bytes = encode_canonical_record(&empty)
        .map_err(map_canonical_codec_error)?
        .len();
    for mapping in construction.fields() {
        let value = evaluation.evaluate(mapping.expression())?;
        let value_bytes = encode_canonical_value(&value)
            .map_err(map_canonical_codec_error)?
            .len();
        let next = encoded_bytes
            .checked_add(mapping.field_id().get().to_be_bytes().len())
            .and_then(|total| total.checked_add(value_bytes))
            .ok_or(ExecutionFault::ResourceLimit)?;
        if next > MAX_CANONICAL_DOCUMENT_BYTES {
            return Err(ExecutionFault::ResourceLimit);
        }
        fields.push((mapping.field_id(), value));
        encoded_bytes = next;
    }
    CanonicalRecord::new(fields).map_err(map_value_error)
}

fn push_mutations(
    schema: &SchemaIr,
    plan: &CommandPlan,
    snapshot: &ReadSnapshot,
    records: &[Option<CanonicalRecord>],
    builder: &mut EvaluatedCommandBuilder<'_>,
) -> Result<(), ExecutionFault> {
    let mut mutable_bindings = plan
        .bindings()
        .iter()
        .enumerate()
        .filter(|(_, binding)| binding.mode() != BindingMode::Read)
        .map(|(index, _)| {
            (
                mutation_order_key(snapshot.bindings()[index].target()),
                index,
            )
        })
        .collect::<Vec<_>>();
    mutable_bindings.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    if mutable_bindings
        .windows(2)
        .any(|pair| pair[0].0 == pair[1].0)
    {
        return Err(ExecutionFault::Integrity);
    }

    for (_, index) in mutable_bindings {
        let binding = &plan.bindings()[index];
        let observation = &snapshot.bindings()[index];
        let record = &records[index];
        let record = record.as_ref().ok_or(ExecutionFault::Integrity)?;
        let entity = schema
            .entity(binding.entity_type())
            .ok_or(ExecutionFault::Integrity)?;
        validate_entity_post_image(schema, entity.record(), record)?;
        let post_image = EntityPostImage::new(
            observation.target().clone(),
            plan.contract_version(),
            record.clone(),
        )
        .map_err(map_storage_value_error)?;
        let mutation = match (binding.mode(), observation) {
            (BindingMode::Create, EntityObservation::Absent(_)) => {
                EntityMutation::Create(post_image)
            }
            (BindingMode::Mutate, EntityObservation::Present(record)) => EntityMutation::Replace {
                expected_version: record.entity_version(),
                post_image,
            },
            _ => return Err(ExecutionFault::Integrity),
        };
        builder
            .push_mutation(mutation)
            .map_err(map_storage_value_error)?;
    }
    Ok(())
}

fn validate_entity_post_image(
    schema: &SchemaIr,
    declared: &RecordSchema,
    record: &CanonicalRecord,
) -> Result<(), ExecutionFault> {
    for field in declared.fields() {
        let value = record_field(record, field.id()).ok_or(ExecutionFault::Integrity)?;
        validate_value(schema, field.value_type(), value)?;
    }
    Ok(())
}

fn mutation_order_key(target: &EntityTarget) -> Vec<u8> {
    let mut output = Vec::with_capacity(1 + 4 + 4 + target.key().as_bytes().len());
    output.push(0x01);
    output.extend_from_slice(&target.entity_type_id().to_be_bytes());
    output.extend_from_slice(&(target.key().as_bytes().len() as u32).to_be_bytes());
    output.extend_from_slice(target.key().as_bytes());
    output
}

fn finish_declared(
    plan: &CommandPlan,
    snapshot: &ReadSnapshot,
    budget: EvaluationBudget,
    outcome: DeclaredOutcome,
    mutations: Vec<EntityMutation>,
    events: Vec<EventIntent>,
) -> Result<ExecutionResult, ExecutionFault> {
    match plan.execution_class() {
        ExecutionClass::ReadOnly => {
            if !mutations.is_empty() || !events.is_empty() {
                return Err(ExecutionFault::Integrity);
            }
            Ok(ExecutionResult::ReadOnly(outcome))
        }
        ExecutionClass::IdempotentMutation => {
            let mut builder =
                EvaluatedCommandBuilder::new(snapshot, budget).map_err(map_storage_value_error)?;
            for mutation in mutations {
                builder
                    .push_mutation(mutation)
                    .map_err(map_storage_value_error)?;
            }
            for event in events {
                builder.push_event(event).map_err(map_storage_value_error)?;
            }
            builder
                .set_outcome(outcome)
                .map_err(map_storage_value_error)?;
            builder
                .finish()
                .map(ExecutionResult::CommitRequired)
                .map_err(map_storage_value_error)
        }
    }
}

fn finish_success(
    schema: &SchemaIr,
    plan: &CommandPlan,
    snapshot: &ReadSnapshot,
    budget: EvaluationBudget,
    outcome: DeclaredOutcome,
    records: &[Option<CanonicalRecord>],
    builder: Option<EvaluatedCommandBuilder<'_>>,
) -> Result<ExecutionResult, ExecutionFault> {
    match plan.execution_class() {
        ExecutionClass::ReadOnly => Ok(ExecutionResult::ReadOnly(outcome)),
        ExecutionClass::IdempotentMutation => {
            let mut builder = match builder {
                Some(builder) => builder,
                None => EvaluatedCommandBuilder::new(snapshot, budget)
                    .map_err(map_storage_value_error)?,
            };
            push_mutations(schema, plan, snapshot, records, &mut builder)?;
            builder
                .set_outcome(outcome)
                .map_err(map_storage_value_error)?;
            builder
                .finish()
                .map(ExecutionResult::CommitRequired)
                .map_err(map_storage_value_error)
        }
    }
}

fn record_field(record: &CanonicalRecord, field: FieldId) -> Option<&CanonicalValue> {
    record
        .fields()
        .binary_search_by_key(&field, |(field, _)| *field)
        .ok()
        .map(|index| &record.fields()[index].1)
}

fn map_storage_value_error(error: StorageValueError) -> ExecutionFault {
    match error {
        StorageValueError::LimitExceeded => ExecutionFault::ResourceLimit,
        StorageValueError::Empty
        | StorageValueError::NonCanonicalOrder
        | StorageValueError::Duplicate
        | StorageValueError::IdentityMismatch
        | StorageValueError::InvalidShape
        | StorageValueError::SizeOverflow => ExecutionFault::Integrity,
    }
}

fn map_value_error(error: ValueError) -> ExecutionFault {
    match error {
        ValueError::StringTooLong { .. }
        | ValueError::BytesTooLong { .. }
        | ValueError::TooManyListEntries { .. }
        | ValueError::TooManyRecordFields { .. }
        | ValueError::NestingTooDeep { .. } => ExecutionFault::ResourceLimit,
        ValueError::DuplicateRecordField { .. } => ExecutionFault::Integrity,
    }
}

fn map_canonical_codec_error(error: CanonicalCodecError) -> ExecutionFault {
    match error {
        CanonicalCodecError::DocumentTooLarge { .. }
        | CanonicalCodecError::StringTooLarge { .. }
        | CanonicalCodecError::BytesTooLarge { .. }
        | CanonicalCodecError::TooManyEntries { .. }
        | CanonicalCodecError::NestingTooDeep { .. } => ExecutionFault::ResourceLimit,
        CanonicalCodecError::UnsupportedVersion { .. }
        | CanonicalCodecError::UnknownTag { .. }
        | CanonicalCodecError::InvalidBoolean { .. }
        | CanonicalCodecError::InvalidDecimal
        | CanonicalCodecError::InvalidCurrency
        | CanonicalCodecError::InvalidTimestamp
        | CanonicalCodecError::InvalidUtf8
        | CanonicalCodecError::NonCanonicalRecordOrder
        | CanonicalCodecError::ZeroEnumTypeId
        | CanonicalCodecError::ZeroEnumVariantId
        | CanonicalCodecError::ZeroFieldId
        | CanonicalCodecError::UnexpectedEnd
        | CanonicalCodecError::TrailingBytes { .. } => ExecutionFault::Integrity,
    }
}

fn map_prepared_evaluation_error(_error: EvaluationError) -> ExecutionFault {
    // Service preparation has already evaluated every locality/key expression
    // before admission. Any failure while repeating that derivation means the
    // admitted plan/input pair is inconsistent, not that business execution
    // discovered a dependency-terminalizable arithmetic fault.
    ExecutionFault::Integrity
}

struct InputValues<'a> {
    input: &'a CanonicalRecord,
}

impl ExpressionValueSource for InputValues<'_> {
    fn input_field(&self, field: FieldId) -> Option<CanonicalValue> {
        record_field(self.input, field).cloned()
    }
}

struct RuntimeValues<'a> {
    schema: &'a SchemaIr,
    plan: &'a CommandPlan,
    input: &'a CanonicalRecord,
    records: &'a [Option<CanonicalRecord>],
    roots: &'a [CanonicalRecord],
    tx_time: LogicalTime,
}

impl ExpressionValueSource for RuntimeValues<'_> {
    fn input_field(&self, field: FieldId) -> Option<CanonicalValue> {
        record_field(self.input, field).cloned()
    }

    fn complete_binding(&self, binding: BindingId) -> Option<CanonicalValue> {
        let record = self
            .records
            .get(binding.get() as usize)
            .and_then(Option::as_ref)?;
        let binding = self
            .plan
            .bindings()
            .get(binding.get() as usize)
            .filter(|candidate| candidate.id() == binding)?;
        let declared = self.schema.entity(binding.entity_type())?.record();
        project_declared_record(declared, record).map(CanonicalValue::Record)
    }

    fn bound_field(&self, binding: BindingId, field: FieldId) -> Option<CanonicalValue> {
        self.records
            .get(binding.get() as usize)
            .and_then(Option::as_ref)
            .and_then(|record| record_field(record, field))
            .cloned()
    }

    fn root_validation_field(
        &self,
        read: RootValidationReadId,
        field: FieldId,
    ) -> Option<CanonicalValue> {
        self.roots
            .get(read.get() as usize)
            .and_then(|record| record_field(record, field))
            .cloned()
    }

    fn transaction_time(&self) -> Option<LogicalTime> {
        Some(self.tx_time)
    }
}

fn project_declared_record(
    declared: &RecordSchema,
    complete: &CanonicalRecord,
) -> Option<CanonicalRecord> {
    let fields = declared
        .fields()
        .iter()
        .map(|field| {
            record_field(complete, field.id())
                .cloned()
                .map(|value| (field.id(), value))
        })
        .collect::<Option<Vec<_>>>()?;
    CanonicalRecord::new(fields).ok()
}

impl From<EvaluationError> for ExecutionFault {
    fn from(error: EvaluationError) -> Self {
        match error {
            EvaluationError::Arithmetic => Self::Arithmetic,
            EvaluationError::Integrity => Self::Integrity,
        }
    }
}
