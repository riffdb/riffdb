#![forbid(unsafe_code)]

//! Synchronous deterministic execution of checked command plans.
//!
//! Execution starts from an already-owned [`ReadSnapshot`]. This crate does not
//! perform pre-snapshot admission or capability acquisition and owns no I/O,
//! clock, entropy, provenance, or durable commit authority.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

use riffdb_contract_ir::{
    BindingId, BindingMode, CommandPlan, ContractBundle, DeleteCheckModeV1, ExecutionClass,
    Instruction, ObjectConstruction, RecordSchema, RootValidationReadId, SchemaIr, ValueType,
    WorkflowLeaseOperation,
};
use riffdb_invariant::{
    EvaluationBatch, EvaluationError, ExpressionEvaluator, ExpressionValueSource,
    InputDerivedCommandFacts, derive_input_command_facts,
};
use riffdb_storage_api::{
    DeclaredOutcome, DurableKeySchemaBindingV1, EmbeddingWriteIntentV1, EntityMutation,
    EntityObservation, EntityPostImage, EntityTarget, EvaluatedCommand, EvaluatedCommandBuilder,
    EvaluationBudget, EventIntent, ExecutablePlanRef, ReadSnapshot, StorageValueError,
    StoredEventPolicyAnchorV1,
};
use riffdb_types::{
    AdmittedActorContext, CanonicalCodecError, CanonicalRecord, CanonicalValue, EmbeddingMetadata,
    EntityKey, FieldId, LogicalTime, MAX_CANONICAL_DOCUMENT_BYTES, PartitionKey, RequestId,
    RowPolicyName, Timestamp, ValueError, encode_canonical_record, encode_canonical_value,
};

/// Immutable values admitted for one deterministic command evaluation.
#[derive(Clone, Eq, PartialEq)]
pub struct TransactionContext {
    request_id: RequestId,
    actor: AdmittedActorContext,
    plan: ExecutablePlanRef,
    tx_time: LogicalTime,
    partition_key: PartitionKey,
    service_values: CanonicalRecord,
}

impl TransactionContext {
    /// Constructs a context from coordinator- and policy-checked values.
    #[must_use]
    pub fn new(
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
            service_values: CanonicalRecord::new(Vec::new())
                .expect("the empty canonical service-value record is valid"),
        }
    }

    /// Constructs a context containing service-observed values sealed before evaluation.
    #[must_use]
    pub fn new_with_service_values(
        request_id: RequestId,
        actor: AdmittedActorContext,
        plan: ExecutablePlanRef,
        tx_time: LogicalTime,
        partition_key: PartitionKey,
        service_values: CanonicalRecord,
    ) -> Self {
        Self {
            request_id,
            actor,
            plan,
            tx_time,
            partition_key,
            service_values,
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

    /// Borrows compiler-keyed service values without exposing an observation source.
    #[must_use]
    pub const fn service_values(&self) -> &CanonicalRecord {
        &self.service_values
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
    if plan.collection_expansion().is_some() {
        return execute_collection_command(bundle, plan, input, snapshot, context, budget);
    }
    let mut evaluator = ExpressionEvaluator::new(plan.expressions());
    validate_snapshot_targets(plan, input, snapshot, context, &mut evaluator)?;

    let mut records = Vec::with_capacity(plan.bindings().len());
    for (binding, observation) in plan.bindings().iter().zip(snapshot.bindings()) {
        match (binding.mode(), observation) {
            (BindingMode::Delete, _) => return Err(ExecutionFault::Integrity),
            (BindingMode::Read | BindingMode::Mutate, EntityObservation::Absent(_))
            | (BindingMode::Create, EntityObservation::Present(_)) => {
                let values = RuntimeValues {
                    schema: bundle.schema(),
                    plan,
                    input,
                    records: &records,
                    roots: &[],
                    tx_time: context.tx_time(),
                    service_values: context.service_values(),
                };
                let mut evaluation = evaluator.batch(&values);
                let outcome = construct_outcome(binding.failure(), &mut evaluation)?;
                return finish_declared(plan, snapshot, budget, outcome, vec![], vec![], vec![]);
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
    let mut embedding_writes = Vec::new();
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
                        service_values: context.service_values(),
                    };
                    let mut evaluation = evaluator.batch(&values);
                    if evaluation.evaluate_predicate(*predicate)? {
                        None
                    } else {
                        Some(construct_outcome(reject, &mut evaluation)?)
                    }
                };
                if let Some(outcome) = rejected {
                    return finish_declared(
                        plan,
                        snapshot,
                        budget,
                        outcome,
                        vec![],
                        vec![],
                        vec![],
                    );
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
                        service_values: context.service_values(),
                    };
                    evaluator.batch(&values).evaluate(*value)?
                };
                set_working_field(bundle.schema(), plan, &mut records, *binding, *field, value)?;
            }
            Instruction::SetEmbedding {
                binding,
                field,
                value,
                model_identity,
                model_version,
            } => {
                let (value, model_identity, model_version) = {
                    let values = RuntimeValues {
                        schema: bundle.schema(),
                        plan,
                        input,
                        records: &records,
                        roots: &roots,
                        tx_time: context.tx_time(),
                        service_values: context.service_values(),
                    };
                    let mut evaluation = evaluator.batch(&values);
                    (
                        evaluation.evaluate(*value)?,
                        evaluation.evaluate(*model_identity)?,
                        evaluation.evaluate(*model_version)?,
                    )
                };
                let target = snapshot
                    .bindings()
                    .get(binding.get() as usize)
                    .map(EntityObservation::target)
                    .ok_or(ExecutionFault::Integrity)?;
                embedding_writes.push(embedding_write_intent(
                    bundle.schema(),
                    plan,
                    target,
                    *binding,
                    *field,
                    model_identity,
                    model_version,
                )?);
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
                        service_values: context.service_values(),
                    };
                    let mut evaluation = evaluator.batch(&values);
                    construct_record(event.payload(), &mut evaluation)?
                };
                if plan.execution_class() != ExecutionClass::IdempotentMutation {
                    return Err(ExecutionFault::Integrity);
                }
                let event = event_intent(bundle, context, event.event_type(), payload)?;
                let builder = match evaluated_builder.as_mut() {
                    Some(builder) => builder,
                    None => evaluated_builder.insert(
                        EvaluatedCommandBuilder::new(snapshot, budget)
                            .map_err(map_storage_value_error)?,
                    ),
                };
                builder.push_event(event).map_err(map_storage_value_error)?;
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
                let binding_index = binding.get() as usize;
                let transition_binding = plan
                    .bindings()
                    .get(binding_index)
                    .filter(|candidate| {
                        candidate.id() == *binding && candidate.mode() == BindingMode::Mutate
                    })
                    .ok_or(ExecutionFault::Integrity)?;
                let state_enum = bundle
                    .schema()
                    .entity(transition_binding.entity_type())
                    .and_then(|entity| entity.record().field(*state_field))
                    .and_then(|field| field.value_type().enum_type_id())
                    .ok_or(ExecutionFault::Integrity)?;
                if bundle
                    .schema()
                    .enumeration(state_enum)
                    .is_none_or(|enumeration| !enumeration.contains_variant(*destination))
                {
                    return Err(ExecutionFault::Integrity);
                }
                let expected_revision = {
                    let values = RuntimeValues {
                        schema: bundle.schema(),
                        plan,
                        input,
                        records: &records,
                        roots: &roots,
                        tx_time: context.tx_time(),
                        service_values: context.service_values(),
                    };
                    evaluator.batch(&values).evaluate(*expected_revision)?
                };
                let CanonicalValue::U64(expected_revision) = expected_revision else {
                    return Err(ExecutionFault::Integrity);
                };
                let observation = snapshot
                    .bindings()
                    .get(binding_index)
                    .filter(|observation| {
                        observation.target().entity_type_id() == transition_binding.entity_type()
                    })
                    .ok_or(ExecutionFault::Integrity)?;
                let EntityObservation::Present(stored) = observation else {
                    return Err(ExecutionFault::Integrity);
                };
                if stored.entity_version().get() != expected_revision {
                    let outcome = {
                        let values = RuntimeValues {
                            schema: bundle.schema(),
                            plan,
                            input,
                            records: &records,
                            roots: &roots,
                            tx_time: context.tx_time(),
                            service_values: context.service_values(),
                        };
                        let mut evaluation = evaluator.batch(&values);
                        construct_outcome(stale, &mut evaluation)?
                    };
                    return finish_declared(
                        plan,
                        snapshot,
                        budget,
                        outcome,
                        vec![],
                        vec![],
                        vec![],
                    );
                }

                let state = records
                    .get(binding_index)
                    .and_then(Option::as_ref)
                    .and_then(|record| record_field(record, *state_field))
                    .ok_or(ExecutionFault::Integrity)?;
                let CanonicalValue::Enum {
                    type_id,
                    variant_id,
                } = state
                else {
                    return Err(ExecutionFault::Integrity);
                };
                if *type_id != state_enum {
                    return Err(ExecutionFault::Integrity);
                }
                if source_states.binary_search(variant_id).is_err() {
                    let outcome = {
                        let values = RuntimeValues {
                            schema: bundle.schema(),
                            plan,
                            input,
                            records: &records,
                            roots: &roots,
                            tx_time: context.tx_time(),
                            service_values: context.service_values(),
                        };
                        let mut evaluation = evaluator.batch(&values);
                        construct_outcome(illegal, &mut evaluation)?
                    };
                    return finish_declared(
                        plan,
                        snapshot,
                        budget,
                        outcome,
                        vec![],
                        vec![],
                        vec![],
                    );
                }
                let destination = CanonicalValue::Enum {
                    type_id: *type_id,
                    variant_id: *destination,
                };
                set_working_field(
                    bundle.schema(),
                    plan,
                    &mut records,
                    *binding,
                    *state_field,
                    destination,
                )?;
            }
            Instruction::WorkflowLease {
                binding,
                fields,
                operation,
            } => {
                let binding_index = binding.get() as usize;
                let lease_binding = plan
                    .bindings()
                    .get(binding_index)
                    .filter(|candidate| {
                        candidate.id() == *binding && candidate.mode() == BindingMode::Mutate
                    })
                    .ok_or(ExecutionFault::Integrity)?;
                let observation = snapshot
                    .bindings()
                    .get(binding_index)
                    .filter(|observation| {
                        observation.target().entity_type_id() == lease_binding.entity_type()
                    })
                    .ok_or(ExecutionFault::Integrity)?;
                let EntityObservation::Present(stored) = observation else {
                    return Err(ExecutionFault::Integrity);
                };
                let record = records
                    .get(binding_index)
                    .and_then(Option::as_ref)
                    .ok_or(ExecutionFault::Integrity)?;
                let current_owner = record_field(record, fields.owner_field)
                    .ok_or(ExecutionFault::Integrity)?
                    .clone();
                let current_expiry = record_field(record, fields.expiry_field)
                    .ok_or(ExecutionFault::Integrity)?
                    .clone();
                let current_fence = match record_field(record, fields.fencing_token_field) {
                    Some(CanonicalValue::U64(value)) => *value,
                    _ => return Err(ExecutionFault::Integrity),
                };
                let current_attempt = match fields.attempt_field {
                    Some(field) => match record_field(record, field) {
                        Some(CanonicalValue::U64(value)) => Some(*value),
                        _ => return Err(ExecutionFault::Integrity),
                    },
                    None => None,
                };
                macro_rules! evaluate {
                    ($expression:expr) => {{
                        let values = RuntimeValues {
                            schema: bundle.schema(),
                            plan,
                            input,
                            records: &records,
                            roots: &roots,
                            tx_time: context.tx_time(),
                            service_values: context.service_values(),
                        };
                        evaluator.batch(&values).evaluate($expression)?
                    }};
                }
                macro_rules! reject {
                    ($outcome:expr) => {{
                        let value = {
                            let values = RuntimeValues {
                                schema: bundle.schema(),
                                plan,
                                input,
                                records: &records,
                                roots: &roots,
                                tx_time: context.tx_time(),
                                service_values: context.service_values(),
                            };
                            let mut evaluation = evaluator.batch(&values);
                            construct_outcome($outcome, &mut evaluation)?
                        };
                        return finish_declared(
                            plan,
                            snapshot,
                            budget,
                            value,
                            vec![],
                            vec![],
                            vec![],
                        );
                    }};
                }
                let require_revision = |value: CanonicalValue| -> Result<bool, ExecutionFault> {
                    match value {
                        CanonicalValue::U64(expected) => {
                            Ok(stored.entity_version().get() == expected)
                        }
                        _ => Err(ExecutionFault::Integrity),
                    }
                };
                let tx_time = context.tx_time().timestamp();
                let expiration = |duration: u64| -> Option<Timestamp> {
                    let duration = i64::try_from(duration).ok()?;
                    Timestamp::new(
                        tx_time.seconds().checked_add(duration)?,
                        tx_time.nanoseconds(),
                    )
                    .ok()
                };
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
                        if !require_revision(evaluate!(*expected_revision))? {
                            reject!(stale);
                        }
                        let CanonicalValue::Uuid(owner) = evaluate!(*owner) else {
                            return Err(ExecutionFault::Integrity);
                        };
                        let CanonicalValue::U64(duration) = evaluate!(*duration_seconds) else {
                            return Err(ExecutionFault::Integrity);
                        };
                        if duration < fields.minimum_duration_seconds
                            || duration > fields.maximum_duration_seconds
                        {
                            reject!(invalid);
                        }
                        match (&current_owner, &current_expiry) {
                            (CanonicalValue::Null, CanonicalValue::Null) => {}
                            (CanonicalValue::Uuid(_), CanonicalValue::Timestamp(expiry))
                                if *expiry <= tx_time => {}
                            (CanonicalValue::Uuid(_), CanonicalValue::Timestamp(_)) => {
                                reject!(unavailable)
                            }
                            _ => return Err(ExecutionFault::Integrity),
                        }
                        let Some(next_fence) =
                            current_fence.checked_add(1).filter(|value| *value != 0)
                        else {
                            reject!(exhausted);
                        };
                        let next_attempt = match current_attempt {
                            Some(value) => match value.checked_add(1) {
                                Some(next) => Some(next),
                                None => reject!(exhausted),
                            },
                            None => None,
                        };
                        let Some(expiry) = expiration(duration) else {
                            reject!(exhausted);
                        };
                        set_working_field(
                            bundle.schema(),
                            plan,
                            &mut records,
                            *binding,
                            fields.owner_field,
                            CanonicalValue::Uuid(owner),
                        )?;
                        set_working_field(
                            bundle.schema(),
                            plan,
                            &mut records,
                            *binding,
                            fields.expiry_field,
                            CanonicalValue::Timestamp(expiry),
                        )?;
                        set_working_field(
                            bundle.schema(),
                            plan,
                            &mut records,
                            *binding,
                            fields.fencing_token_field,
                            CanonicalValue::U64(next_fence),
                        )?;
                        if let (Some(field), Some(value)) = (fields.attempt_field, next_attempt) {
                            set_working_field(
                                bundle.schema(),
                                plan,
                                &mut records,
                                *binding,
                                field,
                                CanonicalValue::U64(value),
                            )?;
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
                        if !require_revision(evaluate!(*expected_revision))? {
                            reject!(stale);
                        }
                        let owner = evaluate!(*owner);
                        let fence = evaluate!(*fencing_token);
                        let CanonicalValue::U64(duration) = evaluate!(*duration_seconds) else {
                            return Err(ExecutionFault::Integrity);
                        };
                        if duration < fields.minimum_duration_seconds
                            || duration > fields.maximum_duration_seconds
                        {
                            reject!(invalid);
                        }
                        if owner != current_owner || fence != CanonicalValue::U64(current_fence) {
                            reject!(invalid);
                        }
                        let CanonicalValue::Timestamp(current_expiry) = current_expiry else {
                            reject!(invalid);
                        };
                        if current_expiry <= tx_time {
                            reject!(expired);
                        }
                        let Some(expiry) = expiration(duration) else {
                            reject!(exhausted);
                        };
                        set_working_field(
                            bundle.schema(),
                            plan,
                            &mut records,
                            *binding,
                            fields.expiry_field,
                            CanonicalValue::Timestamp(expiry),
                        )?;
                    }
                    WorkflowLeaseOperation::Release {
                        owner,
                        fencing_token,
                        expected_revision,
                        stale,
                        invalid,
                    } => {
                        if !require_revision(evaluate!(*expected_revision))? {
                            reject!(stale);
                        }
                        if evaluate!(*owner) != current_owner
                            || evaluate!(*fencing_token) != CanonicalValue::U64(current_fence)
                        {
                            reject!(invalid);
                        }
                        if !matches!(current_expiry, CanonicalValue::Timestamp(_)) {
                            reject!(invalid);
                        }
                        set_working_field(
                            bundle.schema(),
                            plan,
                            &mut records,
                            *binding,
                            fields.owner_field,
                            CanonicalValue::Null,
                        )?;
                        set_working_field(
                            bundle.schema(),
                            plan,
                            &mut records,
                            *binding,
                            fields.expiry_field,
                            CanonicalValue::Null,
                        )?;
                    }
                    WorkflowLeaseOperation::Expire {
                        expected_revision,
                        stale,
                        active,
                    } => {
                        if !require_revision(evaluate!(*expected_revision))? {
                            reject!(stale);
                        }
                        match (&current_owner, &current_expiry) {
                            (CanonicalValue::Uuid(_), CanonicalValue::Timestamp(expiry))
                                if *expiry <= tx_time => {}
                            (CanonicalValue::Null, CanonicalValue::Null)
                            | (CanonicalValue::Uuid(_), CanonicalValue::Timestamp(_)) => {
                                reject!(active)
                            }
                            _ => return Err(ExecutionFault::Integrity),
                        }
                        set_working_field(
                            bundle.schema(),
                            plan,
                            &mut records,
                            *binding,
                            fields.owner_field,
                            CanonicalValue::Null,
                        )?;
                        set_working_field(
                            bundle.schema(),
                            plan,
                            &mut records,
                            *binding,
                            fields.expiry_field,
                            CanonicalValue::Null,
                        )?;
                    }
                    WorkflowLeaseOperation::Fence {
                        owner,
                        fencing_token,
                        expected_revision,
                        stale,
                        invalid,
                        expired,
                    } => {
                        if !require_revision(evaluate!(*expected_revision))? {
                            reject!(stale);
                        }
                        if evaluate!(*owner) != current_owner
                            || evaluate!(*fencing_token) != CanonicalValue::U64(current_fence)
                        {
                            reject!(invalid);
                        }
                        let CanonicalValue::Timestamp(expiry) = current_expiry else {
                            reject!(invalid);
                        };
                        if expiry <= tx_time {
                            reject!(expired);
                        }
                    }
                }
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
                        service_values: context.service_values(),
                    };
                    let mut evaluation = evaluator.batch(&values);
                    construct_outcome(outcome, &mut evaluation)?
                };
                return finish_success(
                    bundle.schema(),
                    plan,
                    snapshot,
                    budget,
                    SuccessfulEvaluation {
                        outcome,
                        records: &records,
                        embedding_writes,
                        builder: evaluated_builder,
                    },
                );
            }
        }
    }
    Err(ExecutionFault::Integrity)
}

fn execute_collection_command(
    bundle: &ContractBundle,
    plan: &CommandPlan,
    input: &CanonicalRecord,
    snapshot: &ReadSnapshot,
    context: &TransactionContext,
    budget: EvaluationBudget,
) -> Result<ExecutionResult, ExecutionFault> {
    let expansion = plan
        .collection_expansion()
        .ok_or(ExecutionFault::Integrity)?;
    let CanonicalValue::List(elements) =
        record_field(input, expansion.input_field()).ok_or(ExecutionFault::Integrity)?
    else {
        return Err(ExecutionFault::Integrity);
    };
    if let Some(maximum) = expansion.maximum_aggregate_element_bytes() {
        let mut observed = 0usize;
        for element in elements.values() {
            observed = observed
                .checked_add(
                    encode_canonical_value(element)
                        .map_err(map_canonical_codec_error)?
                        .len(),
                )
                .ok_or(ExecutionFault::ResourceLimit)?;
            if observed > maximum {
                return Err(ExecutionFault::ResourceLimit);
            }
        }
    }
    let facts =
        derive_input_command_facts(plan, input.clone()).map_err(map_prepared_evaluation_error)?;
    if facts.partition_key() != context.partition_key()
        || facts.binding_entity_keys().len() != snapshot.bindings().len()
        || facts.root_validation_entity_keys().len() != snapshot.root_validations().len()
        || snapshot.ranges().len() != delete_range_count(plan, &facts)?
    {
        return Err(ExecutionFault::Integrity);
    }
    for ((index, key), observation) in facts
        .binding_plan_indices()
        .iter()
        .zip(facts.binding_entity_keys())
        .zip(snapshot.bindings())
    {
        let binding = plan
            .bindings()
            .get(*index as usize)
            .ok_or(ExecutionFault::Integrity)?;
        if observation.target().entity_type_id() != binding.entity_type()
            || observation.target().key() != key
        {
            return Err(ExecutionFault::Integrity);
        }
    }
    for ((index, key), observation) in facts
        .root_validation_plan_indices()
        .iter()
        .zip(facts.root_validation_entity_keys())
        .zip(snapshot.root_validations())
    {
        let read = plan
            .root_validation_reads()
            .get(*index as usize)
            .ok_or(ExecutionFault::Integrity)?;
        if observation.target().entity_type_id() != read.entity_type()
            || observation.target().key() != key
        {
            return Err(ExecutionFault::Integrity);
        }
    }

    let first_instruction = expansion.first_instruction() as usize;
    let instruction_end = first_instruction
        .checked_add(expansion.instruction_count())
        .ok_or(ExecutionFault::ResourceLimit)?;
    let first_binding = expansion.first_binding().get() as usize;
    let binding_end = first_binding
        .checked_add(expansion.binding_count())
        .ok_or(ExecutionFault::ResourceLimit)?;
    if first_binding != 0
        || binding_end != plan.bindings().len()
        || first_instruction != 0
        || instruction_end > plan.instructions().len()
    {
        // The first runtime slice deliberately accepts only the compiler's
        // closed all-local template. Shared command bindings/instructions need
        // their own semantic schedule rather than accidental partial reuse.
        return Err(ExecutionFault::Integrity);
    }

    let delete_evidence = validate_collection_delete_evidence(
        bundle,
        plan,
        &facts,
        snapshot,
        context.partition_key(),
    )?;
    let mut evaluator = ExpressionEvaluator::new(plan.expressions());
    let mut mutations = Vec::new();
    let mut events = Vec::new();
    let mut embedding_writes = Vec::new();
    if let Some(binding_slot) = delete_evidence.failure_binding_slot {
        let binding_index = *facts
            .binding_plan_indices()
            .get(binding_slot)
            .ok_or(ExecutionFault::Integrity)? as usize;
        let binding = plan
            .bindings()
            .get(binding_index)
            .ok_or(ExecutionFault::Integrity)?;
        let failure = binding
            .restriction_failure()
            .or_else(|| binding.cascade_failure())
            .ok_or(ExecutionFault::Integrity)?;
        let empty_records = vec![None; plan.bindings().len()];
        let values = RuntimeValues {
            schema: bundle.schema(),
            plan,
            input,
            records: &empty_records,
            roots: &[],
            tx_time: context.tx_time(),
            service_values: context.service_values(),
        };
        let mut evaluation = evaluator.batch(&values);
        let outcome = construct_outcome(failure, &mut evaluation)?;
        return finish_declared(
            plan,
            snapshot,
            budget,
            outcome,
            mutations,
            events,
            embedding_writes,
        );
    }
    for (element_ordinal, element) in elements.values().iter().enumerate() {
        let ordinal = u16::try_from(element_ordinal).map_err(|_| ExecutionFault::ResourceLimit)?;
        let mut records = vec![None; plan.bindings().len()];
        let mut roots = vec![None; plan.root_validation_reads().len()];

        for (slot, observation) in snapshot.bindings().iter().enumerate() {
            if facts.binding_element_ordinals().get(slot) != Some(&Some(ordinal)) {
                continue;
            }
            let binding_index = *facts
                .binding_plan_indices()
                .get(slot)
                .ok_or(ExecutionFault::Integrity)? as usize;
            let binding = plan
                .bindings()
                .get(binding_index)
                .ok_or(ExecutionFault::Integrity)?;
            match (binding.mode(), observation) {
                (
                    BindingMode::Read | BindingMode::Mutate | BindingMode::Delete,
                    EntityObservation::Absent(_),
                )
                | (BindingMode::Create, EntityObservation::Present(_)) => {
                    if !roots.is_empty() {
                        return Err(ExecutionFault::Integrity);
                    }
                    let values = CollectionRuntimeValues::new(
                        bundle.schema(),
                        plan,
                        input,
                        &records,
                        &[],
                        context,
                        element,
                    );
                    let mut evaluation = evaluator.batch(&values);
                    let outcome = construct_outcome(binding.failure(), &mut evaluation)?;
                    return finish_declared(
                        plan,
                        snapshot,
                        budget,
                        outcome,
                        vec![],
                        vec![],
                        vec![],
                    );
                }
                (
                    BindingMode::Read | BindingMode::Mutate | BindingMode::Delete,
                    EntityObservation::Present(record),
                ) => {
                    let entity = bundle
                        .schema()
                        .entity(binding.entity_type())
                        .ok_or(ExecutionFault::Integrity)?;
                    records[binding_index] = Some(materialize_entity_record(
                        bundle.schema(),
                        entity.record(),
                        entity.primary_key_fields(),
                        binding.key_schema(),
                        record.target(),
                        record.fields(),
                    )?);
                }
                (BindingMode::Create, EntityObservation::Absent(target)) => {
                    let entity = bundle
                        .schema()
                        .entity(binding.entity_type())
                        .ok_or(ExecutionFault::Integrity)?;
                    records[binding_index] = Some(initialize_create_record(
                        entity.record(),
                        entity.primary_key_fields(),
                        binding.key_schema(),
                        target,
                    )?);
                }
            }
        }
        for (slot, observation) in snapshot.root_validations().iter().enumerate() {
            if facts.root_validation_element_ordinals().get(slot) != Some(&Some(ordinal)) {
                continue;
            }
            let read_index = *facts
                .root_validation_plan_indices()
                .get(slot)
                .ok_or(ExecutionFault::Integrity)? as usize;
            let read = plan
                .root_validation_reads()
                .get(read_index)
                .ok_or(ExecutionFault::Integrity)?;
            let EntityObservation::Present(record) = observation else {
                return Err(ExecutionFault::Integrity);
            };
            let entity = bundle
                .schema()
                .entity(read.entity_type())
                .ok_or(ExecutionFault::Integrity)?;
            roots[read_index] = Some(materialize_entity_record(
                bundle.schema(),
                entity.record(),
                entity.primary_key_fields(),
                read.key_schema(),
                record.target(),
                record.fields(),
            )?);
        }
        let roots = roots
            .into_iter()
            .map(|root| root.ok_or(ExecutionFault::Integrity))
            .collect::<Result<Vec<_>, _>>()?;

        for instruction in &plan.instructions()[first_instruction..instruction_end] {
            match instruction {
                Instruction::Require {
                    predicate, reject, ..
                } => {
                    let values = CollectionRuntimeValues::new(
                        bundle.schema(),
                        plan,
                        input,
                        &records,
                        &roots,
                        context,
                        element,
                    );
                    let mut evaluation = evaluator.batch(&values);
                    if !evaluation.evaluate_predicate(*predicate)? {
                        let outcome = construct_outcome(reject, &mut evaluation)?;
                        return finish_declared(
                            plan,
                            snapshot,
                            budget,
                            outcome,
                            vec![],
                            vec![],
                            vec![],
                        );
                    }
                }
                Instruction::SetField {
                    binding,
                    field,
                    value,
                } => {
                    let value = {
                        let values = CollectionRuntimeValues::new(
                            bundle.schema(),
                            plan,
                            input,
                            &records,
                            &roots,
                            context,
                            element,
                        );
                        evaluator.batch(&values).evaluate(*value)?
                    };
                    set_working_field(
                        bundle.schema(),
                        plan,
                        &mut records,
                        *binding,
                        *field,
                        value,
                    )?;
                }
                Instruction::SetEmbedding {
                    binding,
                    field,
                    value,
                    model_identity,
                    model_version,
                } => {
                    let (value, model_identity, model_version) = {
                        let values = CollectionRuntimeValues::new(
                            bundle.schema(),
                            plan,
                            input,
                            &records,
                            &roots,
                            context,
                            element,
                        );
                        let mut evaluation = evaluator.batch(&values);
                        (
                            evaluation.evaluate(*value)?,
                            evaluation.evaluate(*model_identity)?,
                            evaluation.evaluate(*model_version)?,
                        )
                    };
                    let slot = facts
                        .binding_plan_indices()
                        .iter()
                        .zip(facts.binding_element_ordinals())
                        .position(|(plan_index, element_ordinal)| {
                            *plan_index as usize == binding.get() as usize
                                && *element_ordinal == Some(ordinal)
                        })
                        .ok_or(ExecutionFault::Integrity)?;
                    let target = snapshot
                        .bindings()
                        .get(slot)
                        .map(EntityObservation::target)
                        .ok_or(ExecutionFault::Integrity)?;
                    embedding_writes.push(embedding_write_intent(
                        bundle.schema(),
                        plan,
                        target,
                        *binding,
                        *field,
                        model_identity,
                        model_version,
                    )?);
                    set_working_field(
                        bundle.schema(),
                        plan,
                        &mut records,
                        *binding,
                        *field,
                        value,
                    )?;
                }
                Instruction::EmitEvent(event) => {
                    let payload = {
                        let values = CollectionRuntimeValues::new(
                            bundle.schema(),
                            plan,
                            input,
                            &records,
                            &roots,
                            context,
                            element,
                        );
                        let mut evaluation = evaluator.batch(&values);
                        construct_record(event.payload(), &mut evaluation)?
                    };
                    events.push(event_intent(bundle, context, event.event_type(), payload)?);
                }
                Instruction::WorkflowTransition { .. }
                | Instruction::WorkflowLease { .. }
                | Instruction::Return(_) => return Err(ExecutionFault::Integrity),
            }
        }

        for (slot, observation) in snapshot.bindings().iter().enumerate() {
            if facts.binding_element_ordinals().get(slot) != Some(&Some(ordinal)) {
                continue;
            }
            let binding_index = facts.binding_plan_indices()[slot] as usize;
            let binding = &plan.bindings()[binding_index];
            if binding.mode() == BindingMode::Read {
                continue;
            }
            if binding.mode() == BindingMode::Delete {
                for child_position in delete_evidence
                    .child_positions_by_binding
                    .get(slot)
                    .ok_or(ExecutionFault::Integrity)?
                {
                    let EntityObservation::Present(child) = snapshot
                        .cascade_predecessors()
                        .get(*child_position)
                        .ok_or(ExecutionFault::Integrity)?
                    else {
                        return Err(ExecutionFault::Integrity);
                    };
                    let child_entity = bundle
                        .schema()
                        .entity(child.target().entity_type_id())
                        .ok_or(ExecutionFault::Integrity)?;
                    validate_entity_post_image(
                        bundle.schema(),
                        child_entity.record(),
                        child.fields(),
                    )?;
                    let prior_image = EntityPostImage::new(
                        child.target().clone(),
                        plan.contract_version(),
                        child.fields().clone(),
                    )
                    .map_err(map_storage_value_error)?;
                    mutations.push(EntityMutation::Delete {
                        expected_version: child.entity_version(),
                        prior_image,
                    });
                }
            }
            let record = records[binding_index]
                .as_ref()
                .ok_or(ExecutionFault::Integrity)?;
            let entity = bundle
                .schema()
                .entity(binding.entity_type())
                .ok_or(ExecutionFault::Integrity)?;
            validate_entity_post_image(bundle.schema(), entity.record(), record)?;
            let image = EntityPostImage::new(
                observation.target().clone(),
                plan.contract_version(),
                record.clone(),
            )
            .map_err(map_storage_value_error)?;
            mutations.push(match (binding.mode(), observation) {
                (BindingMode::Create, EntityObservation::Absent(_)) => {
                    EntityMutation::Create(image)
                }
                (BindingMode::Mutate, EntityObservation::Present(record)) => {
                    EntityMutation::Replace {
                        expected_version: record.entity_version(),
                        post_image: image,
                    }
                }
                (BindingMode::Delete, EntityObservation::Present(record)) => {
                    EntityMutation::Delete {
                        expected_version: record.entity_version(),
                        prior_image: image,
                    }
                }
                _ => return Err(ExecutionFault::Integrity),
            });
        }
    }

    if !delete_evidence.has_cascade {
        mutations.sort_unstable_by_key(|mutation| mutation_order_key(mutation.target()));
    }
    let mut mutation_targets = BTreeSet::new();
    if mutations
        .iter()
        .any(|mutation| !mutation_targets.insert(mutation.target()))
    {
        return Err(ExecutionFault::Integrity);
    }
    let return_instruction = plan
        .instructions()
        .get(instruction_end)
        .filter(|instruction| matches!(instruction, Instruction::Return(_)))
        .ok_or(ExecutionFault::Integrity)?;
    if plan.instructions().len() != instruction_end + 1 {
        return Err(ExecutionFault::Integrity);
    }
    let Instruction::Return(outcome) = return_instruction else {
        return Err(ExecutionFault::Integrity);
    };
    let empty_records = vec![None; plan.bindings().len()];
    let empty_roots = vec![];
    let values = RuntimeValues {
        schema: bundle.schema(),
        plan,
        input,
        records: &empty_records,
        roots: &empty_roots,
        tx_time: context.tx_time(),
        service_values: context.service_values(),
    };
    let mut evaluation = evaluator.batch(&values);
    let outcome = construct_outcome(outcome, &mut evaluation)?;
    finish_declared(
        plan,
        snapshot,
        budget,
        outcome,
        mutations,
        events,
        embedding_writes,
    )
}

fn event_intent(
    bundle: &ContractBundle,
    context: &TransactionContext,
    event_type: riffdb_types::EventTypeId,
    payload: CanonicalRecord,
) -> Result<EventIntent, ExecutionFault> {
    let schema = bundle
        .schema()
        .event(event_type)
        .ok_or(ExecutionFault::Integrity)?;
    let Some(anchor) = schema.policy_anchor() else {
        return EventIntent::new(event_type, payload).map_err(map_storage_value_error);
    };
    let source = bundle
        .schema()
        .entity(anchor.source_entity())
        .ok_or(ExecutionFault::Integrity)?;
    let values = anchor
        .key_fields()
        .iter()
        .map(|mapping| {
            payload
                .fields()
                .binary_search_by_key(&mapping.payload_field(), |(field, _)| *field)
                .ok()
                .map(|index| payload.fields()[index].1.clone())
                .ok_or(ExecutionFault::Integrity)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let key: EntityKey = source
        .primary_key()
        .encode_entity(&values)
        .map_err(|_| ExecutionFault::Integrity)?;
    let partition_width = schema
        .partition()
        .ok_or(ExecutionFault::Integrity)?
        .fields()
        .len();
    let partition = schema
        .partition()
        .ok_or(ExecutionFault::Integrity)?
        .key_schema()
        .encode_partition(
            values
                .get(..partition_width)
                .ok_or(ExecutionFault::Integrity)?,
        )
        .map_err(|_| ExecutionFault::Integrity)?;
    if &partition != context.partition_key() {
        return Err(ExecutionFault::Integrity);
    }
    let policy_anchor = StoredEventPolicyAnchorV1::new(
        DurableKeySchemaBindingV1::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
        ),
        event_type,
        EntityTarget::new(anchor.source_entity(), key).map_err(map_storage_value_error)?,
        RowPolicyName::new(anchor.read_policy().to_owned())
            .map_err(|_| ExecutionFault::Integrity)?,
    );
    EventIntent::new_anchored(event_type, payload, policy_anchor).map_err(map_storage_value_error)
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
    let facts =
        derive_input_command_facts(plan, input.clone()).map_err(map_prepared_evaluation_error)?;
    if snapshot.bindings().len() != plan.bindings().len()
        || snapshot.root_validations().len() != plan.root_validation_reads().len()
        || snapshot.ranges().len() != delete_range_count(plan, &facts)?
        || !snapshot.cascade_predecessors().is_empty()
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

struct CollectionDeleteEvidence {
    child_positions_by_binding: Vec<Vec<usize>>,
    failure_binding_slot: Option<usize>,
    has_cascade: bool,
}

fn validate_collection_delete_evidence(
    bundle: &ContractBundle,
    plan: &CommandPlan,
    facts: &InputDerivedCommandFacts,
    snapshot: &ReadSnapshot,
    partition_key: &PartitionKey,
) -> Result<CollectionDeleteEvidence, ExecutionFault> {
    let mut child_positions_by_binding = vec![Vec::new(); snapshot.bindings().len()];
    let mut expected_children = Vec::new();
    let mut used_ranges = BTreeSet::new();
    let mut failure_binding_slot = None;
    let mut has_cascade = false;

    for (binding_slot, ((plan_index, key), _observation)) in facts
        .binding_plan_indices()
        .iter()
        .zip(facts.binding_entity_keys())
        .zip(snapshot.bindings())
        .enumerate()
    {
        let binding = plan
            .bindings()
            .get(*plan_index as usize)
            .ok_or(ExecutionFault::Integrity)?;
        if binding.mode() != BindingMode::Delete {
            continue;
        }
        let check = plan
            .delete_checks()
            .iter()
            .find(|check| check.binding() == binding.id())
            .ok_or(ExecutionFault::Integrity)?;
        let key_values = binding
            .key_schema()
            .decode_entity(key)
            .map_err(|_| ExecutionFault::Integrity)?;
        match check.mode() {
            DeleteCheckModeV1::NoInbound => {}
            DeleteCheckModeV1::Restrict {
                source_entity,
                index_id,
            } => {
                let range_position = exact_delete_range_position(
                    bundle.schema(),
                    snapshot,
                    partition_key,
                    source_entity,
                    index_id,
                    &key_values,
                )?;
                if !used_ranges.insert(range_position) {
                    return Err(ExecutionFault::Integrity);
                }
                if !snapshot.ranges()[range_position].entries().is_empty()
                    && failure_binding_slot.is_none()
                {
                    failure_binding_slot = Some(binding_slot);
                }
            }
            DeleteCheckModeV1::Cascade { relationships } => {
                has_cascade = true;
                for relationship in relationships {
                    let range_position = exact_delete_range_position(
                        bundle.schema(),
                        snapshot,
                        partition_key,
                        relationship.source_entity(),
                        relationship.index_id(),
                        &key_values,
                    )?;
                    if !used_ranges.insert(range_position) {
                        return Err(ExecutionFault::Integrity);
                    }
                    let range = &snapshot.ranges()[range_position];
                    if range.entries().len() > usize::from(relationship.maximum())
                        && failure_binding_slot.is_none()
                    {
                        failure_binding_slot = Some(binding_slot);
                    }
                    let source = bundle
                        .schema()
                        .entity(relationship.source_entity())
                        .ok_or(ExecutionFault::Integrity)?;
                    let index = source
                        .indexes()
                        .iter()
                        .find(|index| index.id() == relationship.index_id())
                        .ok_or(ExecutionFault::Integrity)?;
                    for entry in range.entries() {
                        let decoded = index
                            .key_schema()
                            .decode_index(entry.key())
                            .map_err(|_| ExecutionFault::Integrity)?;
                        let target = EntityTarget::new(
                            relationship.source_entity(),
                            decoded.entity_key().clone(),
                        )
                        .map_err(map_storage_value_error)?;
                        child_positions_by_binding[binding_slot].push(expected_children.len());
                        expected_children.push(target);
                    }
                }
            }
        }
    }
    if used_ranges.len() != snapshot.ranges().len() {
        return Err(ExecutionFault::Integrity);
    }
    if expected_children.len() != snapshot.cascade_predecessors().len()
        || expected_children
            .iter()
            .zip(snapshot.cascade_predecessors())
            .any(|(target, observation)| target != observation.target())
        || snapshot
            .cascade_predecessors()
            .iter()
            .any(|observation| !matches!(observation, EntityObservation::Present(_)))
    {
        return Err(ExecutionFault::Integrity);
    }

    Ok(CollectionDeleteEvidence {
        child_positions_by_binding,
        failure_binding_slot,
        has_cascade,
    })
}

fn exact_delete_range_position(
    schema: &SchemaIr,
    snapshot: &ReadSnapshot,
    partition_key: &PartitionKey,
    source_entity: riffdb_types::EntityTypeId,
    index_id: riffdb_types::IndexId,
    target_key_values: &[CanonicalValue],
) -> Result<usize, ExecutionFault> {
    let source = schema
        .entity(source_entity)
        .ok_or(ExecutionFault::Integrity)?;
    let index = source
        .indexes()
        .iter()
        .find(|index| index.id() == index_id)
        .ok_or(ExecutionFault::Integrity)?;
    if target_key_values.is_empty()
        || target_key_values.len() > index.key_schema().components().len()
    {
        return Err(ExecutionFault::Integrity);
    }
    let expected = index
        .key_schema()
        .encode_index_prefix(target_key_values)
        .map_err(|_| ExecutionFault::Integrity)?;
    let mut matches = snapshot.ranges().iter().enumerate().filter(|(_, range)| {
        range.target().prefix().index_id() == index_id
            && range.target().prefix().as_bytes() == expected.as_bytes()
            && range.target().generation_target().partition_key() == partition_key
    });
    let (position, _) = matches.next().ok_or(ExecutionFault::Integrity)?;
    if matches.next().is_some() {
        return Err(ExecutionFault::Integrity);
    }
    Ok(position)
}

fn delete_range_count(
    plan: &CommandPlan,
    facts: &InputDerivedCommandFacts,
) -> Result<usize, ExecutionFault> {
    let mut count = 0usize;
    for plan_index in facts.binding_plan_indices() {
        let binding = plan
            .bindings()
            .get(*plan_index as usize)
            .ok_or(ExecutionFault::Integrity)?;
        if binding.mode() != BindingMode::Delete {
            continue;
        }
        let check = plan
            .delete_checks()
            .iter()
            .find(|check| check.binding() == binding.id())
            .ok_or(ExecutionFault::Integrity)?;
        let additional = match check.mode() {
            DeleteCheckModeV1::NoInbound => 0,
            DeleteCheckModeV1::Restrict { .. } => 1,
            DeleteCheckModeV1::Cascade { relationships } => relationships.len(),
        };
        count = count
            .checked_add(additional)
            .ok_or(ExecutionFault::ResourceLimit)?;
    }
    Ok(count)
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
    let fields = stored.fields().to_vec();
    for declared_field in declared.fields() {
        match record_field(stored, declared_field.id()) {
            Some(value) => validate_value(schema, declared_field.value_type(), value)?,
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

fn embedding_write_intent(
    schema: &SchemaIr,
    plan: &CommandPlan,
    target: &EntityTarget,
    binding: BindingId,
    field: FieldId,
    model_identity: CanonicalValue,
    model_version: CanonicalValue,
) -> Result<EmbeddingWriteIntentV1, ExecutionFault> {
    let binding = plan
        .bindings()
        .get(binding.get() as usize)
        .ok_or(ExecutionFault::Integrity)?;
    if target.entity_type_id() != binding.entity_type() {
        return Err(ExecutionFault::Integrity);
    }
    let production = schema
        .vector_production_spec(binding.entity_type(), field)
        .ok_or(ExecutionFault::Integrity)?;
    let (CanonicalValue::String(model_identity), CanonicalValue::String(model_version)) =
        (model_identity, model_version)
    else {
        return Err(ExecutionFault::Integrity);
    };
    let metadata = EmbeddingMetadata::new(
        model_identity.as_str().to_owned(),
        model_version.as_str().to_owned(),
    )
    .ok_or(ExecutionFault::Integrity)?;
    if &metadata != production.metadata() {
        return Err(ExecutionFault::Integrity);
    }
    Ok(EmbeddingWriteIntentV1::new(target.clone(), field, metadata))
}

fn finish_declared(
    plan: &CommandPlan,
    snapshot: &ReadSnapshot,
    budget: EvaluationBudget,
    outcome: DeclaredOutcome,
    mutations: Vec<EntityMutation>,
    events: Vec<EventIntent>,
    mut embedding_writes: Vec<EmbeddingWriteIntentV1>,
) -> Result<ExecutionResult, ExecutionFault> {
    match plan.execution_class() {
        ExecutionClass::ReadOnly => {
            if !mutations.is_empty() || !events.is_empty() || !embedding_writes.is_empty() {
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
            embedding_writes.sort_unstable_by(|left, right| {
                (left.target(), left.vector_field()).cmp(&(right.target(), right.vector_field()))
            });
            for write in embedding_writes {
                builder
                    .push_embedding_write(write)
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

struct SuccessfulEvaluation<'records, 'snapshot> {
    outcome: DeclaredOutcome,
    records: &'records [Option<CanonicalRecord>],
    embedding_writes: Vec<EmbeddingWriteIntentV1>,
    builder: Option<EvaluatedCommandBuilder<'snapshot>>,
}

fn finish_success<'snapshot>(
    schema: &SchemaIr,
    plan: &CommandPlan,
    snapshot: &'snapshot ReadSnapshot,
    budget: EvaluationBudget,
    mut success: SuccessfulEvaluation<'_, 'snapshot>,
) -> Result<ExecutionResult, ExecutionFault> {
    match plan.execution_class() {
        ExecutionClass::ReadOnly => Ok(ExecutionResult::ReadOnly(success.outcome)),
        ExecutionClass::IdempotentMutation => {
            let mut builder = match success.builder {
                Some(builder) => builder,
                None => EvaluatedCommandBuilder::new(snapshot, budget)
                    .map_err(map_storage_value_error)?,
            };
            push_mutations(schema, plan, snapshot, success.records, &mut builder)?;
            success.embedding_writes.sort_unstable_by(|left, right| {
                (left.target(), left.vector_field()).cmp(&(right.target(), right.vector_field()))
            });
            for write in success.embedding_writes {
                builder
                    .push_embedding_write(write)
                    .map_err(map_storage_value_error)?;
            }
            builder
                .set_outcome(success.outcome)
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
        | CanonicalCodecError::TrailingBytes { .. }
        | CanonicalCodecError::VectorDimensionOutOfRange { .. }
        | CanonicalCodecError::NonCanonicalVectorComponent { .. } => ExecutionFault::Integrity,
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
    service_values: &'a CanonicalRecord,
}

struct CollectionRuntimeValues<'a> {
    base: RuntimeValues<'a>,
    element: &'a CanonicalValue,
}

impl<'a> CollectionRuntimeValues<'a> {
    fn new(
        schema: &'a SchemaIr,
        plan: &'a CommandPlan,
        input: &'a CanonicalRecord,
        records: &'a [Option<CanonicalRecord>],
        roots: &'a [CanonicalRecord],
        context: &'a TransactionContext,
        element: &'a CanonicalValue,
    ) -> Self {
        Self {
            base: RuntimeValues {
                schema,
                plan,
                input,
                records,
                roots,
                tx_time: context.tx_time(),
                service_values: context.service_values(),
            },
            element,
        }
    }
}

impl ExpressionValueSource for CollectionRuntimeValues<'_> {
    fn input_field(&self, field: FieldId) -> Option<CanonicalValue> {
        self.base.input_field(field)
    }

    fn service_value(&self, field: FieldId) -> Option<CanonicalValue> {
        self.base.service_value(field)
    }

    fn collection_element(&self) -> Option<CanonicalValue> {
        Some(self.element.clone())
    }

    fn collection_element_field(&self, field: FieldId) -> Option<CanonicalValue> {
        let CanonicalValue::Record(record) = self.element else {
            return None;
        };
        record_field(record, field).cloned()
    }

    fn complete_binding(&self, binding: BindingId) -> Option<CanonicalValue> {
        self.base.complete_binding(binding)
    }

    fn bound_field(&self, binding: BindingId, field: FieldId) -> Option<CanonicalValue> {
        self.base.bound_field(binding, field)
    }

    fn root_validation_field(
        &self,
        read: RootValidationReadId,
        field: FieldId,
    ) -> Option<CanonicalValue> {
        self.base.root_validation_field(read, field)
    }

    fn transaction_time(&self) -> Option<LogicalTime> {
        self.base.transaction_time()
    }
}

impl ExpressionValueSource for RuntimeValues<'_> {
    fn input_field(&self, field: FieldId) -> Option<CanonicalValue> {
        record_field(self.input, field).cloned()
    }

    fn service_value(&self, field: FieldId) -> Option<CanonicalValue> {
        let value = self
            .plan
            .service_values()
            .binary_search_by_key(&field, |value| value.field().id())
            .ok()
            .map(|index| &self.plan.service_values()[index])?;
        let sealed = record_field(self.service_values, field)?.clone();
        match (value.kind(), &sealed) {
            (
                riffdb_contract_ir::ServiceValueKind::TransactionTime,
                CanonicalValue::Timestamp(_),
            )
            | (riffdb_contract_ir::ServiceValueKind::UuidV7, CanonicalValue::Uuid(_)) => {
                Some(sealed)
            }
            _ => None,
        }
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
