//! Pure transaction-current command validation.
//!
//! The orchestration entrypoint remains intentionally sealed until the commit
//! coordinator can carry one runtime attempt through admission, current-state
//! acquisition, validation, and derived-index planning by construction.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
};

use riffdb_catalog::{
    MaterializedTransactionCurrentState, ResolvedExecutablePlan, TransactionCurrentMaterialization,
};
use riffdb_contract_ir::{
    BindingId, BindingMode, CommandPlan, DeleteCheckModeV1, ExecutionClass, Instruction,
    RecordSchema, RecordTypeRef, RootValidationReadId, SchemaIr, ValueType,
};
use riffdb_invariant::{
    CommitCheckResult, EvaluationError, ExpressionValueSource, InputDerivedCommandFacts,
    derive_input_command_facts,
    evaluate_commit_checks, evaluate_expression,
};
use riffdb_policy::AuthorizedCommandRowPolicyContextV1;
use riffdb_storage_api::{
    ApplicationCommandTransactionPort, AtomicCommandRecordSet, CandidateAdmissionResult,
    CandidateCapacityResult, CandidateStartResult, CandidateValidationRejection,
    CapabilityLifecycleV1, CommandCandidateAdmission, CommandCandidateAffectedEpochRead,
    CommandCandidateAwaitingCapacity, CommandCandidateAwaitingValidation,
    CommandCandidateCapacityReserved, CommandCandidateSequenceAssigned, CommandCandidateStateRead,
    CommandWriteSetPlanV1, EmptyCommandBatch, EntityMutation, EntityObservation, EntityTarget,
    EvaluatedCommand, ExecutablePlanRef, NonEmptyCommandBatch, ReadDependencies, ReadDependency,
    StorageError, StoredCapabilityRecordV1, StoredEntityRecordV1, StoredExecutionFailedV1,
    StoredOutcomeV1, TransactionCurrentPolicyLookupV1, TransactionCurrentPolicyRequestV1,
    TransactionCurrentState,
};
use riffdb_types::{CanonicalRecord, CanonicalValue, FieldId, LogicalTime};

use crate::command_attempt::{
    PendingCommandAttempts, PostApplyCommandEvidence, ProvenanceBoundCommandAttempt,
    RolledBackCandidateDisposition,
};

/// Redacted failure for an impossible checked-plan/value combination.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) struct CommandValidationError {
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

pub(super) enum CheckedCommandDecision {
    ZeroMutation,
    NonZero(Box<[Option<usize>]>),
    Rejected(CandidateValidationRejection),
}

/// Closed result of opening and atomically rechecking one exact storage candidate.
pub(super) enum CommandCandidateChainStart<S> {
    /// The exact Pending admission remains and the storage state is inseparably bound.
    Ready(Box<BoundCommandCandidateStateRead<S>>),
    /// A concurrent equal-input command committed before this write transaction opened.
    OutcomeReplay(StoredOutcomeV1),
    /// A concurrent equal-input deterministic failure became terminal.
    ExecutionFailureReplay(StoredExecutionFailedV1),
    /// The same identity retained another canonical input.
    InputMismatch,
    /// Opening or rechecking the short transaction failed.
    StorageFailure(StorageError),
    /// Storage returned state inconsistent with the exact bound attempt.
    Integrity,
}

/// The sole phase-1 carrier after exact storage admission recheck.
pub(super) struct BoundCommandCandidateStateRead<S> {
    state_read: S,
    attempt: ProvenanceBoundCommandAttempt,
}

/// Opens an empty authoritative transaction and binds its exact candidate to the attempt.
pub(super) fn begin_bound_command_candidate<P>(
    port: &P,
    attempt: ProvenanceBoundCommandAttempt,
) -> CommandCandidateChainStart<
    <<P::EmptyBatch as EmptyCommandBatch>::Candidate as CommandCandidateAdmission>::StateRead,
>
where
    P: ApplicationCommandTransactionPort,
{
    let empty = match port.begin_empty_batch() {
        Ok(empty) => empty,
        Err(error) => return CommandCandidateChainStart::StorageFailure(error),
    };
    begin_bound_command_candidate_on_empty(empty, attempt)
}

/// Binds the exact first candidate to an already-open empty transaction.
///
/// The transaction-local serial path uses this closed transition so its first
/// compiler-declared snapshot and first staged command share one private writer
/// transaction. No storage handle or additional read authority escapes.
pub(super) fn begin_bound_command_candidate_on_empty<B>(
    empty: B,
    attempt: ProvenanceBoundCommandAttempt,
) -> CommandCandidateChainStart<<B::Candidate as CommandCandidateAdmission>::StateRead>
where
    B: EmptyCommandBatch,
{
    let candidate = match empty.begin_candidate(attempt.storage_intent()) {
        Ok(candidate) => candidate,
        Err(error) => return CommandCandidateChainStart::StorageFailure(error),
    };
    finish_bound_command_candidate(candidate, attempt)
}

/// Appends one exact attempt to an already nonempty physical batch. Any
/// non-ready result consumes and rolls back the prior batch; retained semantic
/// evidence is owned separately by the group orchestrator.
pub(super) fn begin_bound_command_candidate_on_prior<B>(
    prior: B,
    attempt: ProvenanceBoundCommandAttempt,
) -> CommandCandidateChainStart<
    <<B as NonEmptyCommandBatch>::Candidate as CommandCandidateAdmission>::StateRead,
>
where
    B: NonEmptyCommandBatch,
{
    let candidate = match prior.begin_candidate(attempt.storage_intent()) {
        Ok(CandidateStartResult::Started(candidate)) => candidate,
        Ok(CandidateStartResult::BatchFull { prior, intent }) => {
            drop(prior);
            drop(intent);
            drop(attempt);
            return CommandCandidateChainStart::Integrity;
        }
        Err(error) => return CommandCandidateChainStart::StorageFailure(error),
    };
    finish_bound_command_candidate(candidate, attempt)
}

fn finish_bound_command_candidate<C>(
    candidate: C,
    attempt: ProvenanceBoundCommandAttempt,
) -> CommandCandidateChainStart<C::StateRead>
where
    C: CommandCandidateAdmission,
{
    let rechecked = match candidate.recheck_admission() {
        Ok(rechecked) => rechecked,
        Err(error) => return CommandCandidateChainStart::StorageFailure(error),
    };
    match rechecked {
        CandidateAdmissionResult::Proceed(state_read) => {
            CommandCandidateChainStart::Ready(Box::new(BoundCommandCandidateStateRead {
                attempt,
                state_read,
            }))
        }
        CandidateAdmissionResult::StoredOutcome { prior, outcome } => {
            drop(prior);
            if attempt.matches_terminal_outcome(&outcome) {
                CommandCandidateChainStart::OutcomeReplay(outcome)
            } else {
                CommandCandidateChainStart::Integrity
            }
        }
        CandidateAdmissionResult::ExecutionFailed { prior, failure } => {
            drop(prior);
            if attempt.matches_terminal_failure(&failure) {
                CommandCandidateChainStart::ExecutionFailureReplay(failure)
            } else {
                CommandCandidateChainStart::Integrity
            }
        }
        CandidateAdmissionResult::InputMismatch(abandoned) => {
            let (prior, intent) = abandoned.into_parts();
            let exact_intent = *intent == *attempt.commit_intent();
            drop(prior);
            drop(intent);
            if exact_intent {
                CommandCandidateChainStart::InputMismatch
            } else {
                CommandCandidateChainStart::Integrity
            }
        }
        CandidateAdmissionResult::MissingPending(abandoned)
        | CandidateAdmissionResult::PendingMismatch(abandoned) => {
            let (prior, intent) = abandoned.into_parts();
            drop(prior);
            drop(intent);
            CommandCandidateChainStart::Integrity
        }
    }
}

/// Closed current-state result retaining the exact awaiting-validation state.
pub(super) enum TransactionCurrentAttemptDecision<C> {
    /// Dependencies and every raw physical observation matched the retained snapshot.
    Ready(CheckedTransactionCurrentAttempt<C>),
    /// An influential absence, version, or range epoch changed before catalog recheck.
    DependencyChanged(CheckedDependencyChangedAttempt<C>),
    /// Storage could not read the complete current state.
    StorageFailure(StorageError),
    /// Current state contradicted the retained attempt evidence.
    Integrity,
}

/// The only transaction-current value accepted by semantic validation.
pub(super) struct CheckedTransactionCurrentAttempt<C> {
    candidate: C,
    current: MaterializedTransactionCurrentState,
    attempt: ProvenanceBoundCommandAttempt,
}

/// A changed-dependency decision that can release only its exact storage candidate.
pub(super) struct CheckedDependencyChangedAttempt<C> {
    candidate: C,
    attempt: ProvenanceBoundCommandAttempt,
}

impl<C> CheckedDependencyChangedAttempt<C>
where
    C: CommandCandidateAwaitingValidation,
{
    pub(super) fn reject_storage_and_rollback(self) -> RolledBackCandidateDisposition {
        self.attempt.reject_storage_and_rollback(
            self.candidate,
            CandidateValidationRejection::DependencyChanged,
        )
    }
}

impl<S> BoundCommandCandidateStateRead<S>
where
    S: CommandCandidateStateRead,
{
    /// Reads transaction-current state from the exact retained storage candidate.
    pub(super) fn read_transaction_current(
        self,
    ) -> TransactionCurrentAttemptDecision<S::AwaitingValidation> {
        let Self {
            attempt,
            state_read,
        } = self;
        let (candidate, current) = match state_read.read_transaction_current() {
            Ok(current) => current,
            Err(error) => return TransactionCurrentAttemptDecision::StorageFailure(error),
        };
        if !attempt.has_exact_semantic_join() {
            drop(candidate);
            drop(attempt);
            return TransactionCurrentAttemptDecision::Integrity;
        }
        let dependencies = match dependencies_from_current(&current) {
            Ok(dependencies) => dependencies,
            Err(_) => {
                drop(candidate);
                drop(attempt);
                return TransactionCurrentAttemptDecision::Integrity;
            }
        };
        if dependencies != *attempt.evaluated().read_dependencies() {
            return TransactionCurrentAttemptDecision::DependencyChanged(
                CheckedDependencyChangedAttempt { attempt, candidate },
            );
        }
        let materialized = match attempt
            .materialized_snapshot()
            .materialize_transaction_current(current)
        {
            Ok(materialized) => materialized,
            Err(_) => {
                drop(candidate);
                drop(attempt);
                return TransactionCurrentAttemptDecision::Integrity;
            }
        };
        match materialized {
            TransactionCurrentMaterialization::Ready(current) => {
                TransactionCurrentAttemptDecision::Ready(CheckedTransactionCurrentAttempt {
                    attempt,
                    candidate,
                    current,
                })
            }
            TransactionCurrentMaterialization::DependencyChanged => {
                drop(candidate);
                drop(attempt);
                TransactionCurrentAttemptDecision::Integrity
            }
        }
    }
}

/// Closed result of validating the exact candidate/current aggregate.
pub(super) enum CheckedCandidateDecision<C> {
    Validated(CheckedValidatedCommand<C>),
    Rejected(CheckedCandidateRejection<C>),
}

/// Closed transaction-current row-policy decision before any sequence assignment.
pub(super) enum CheckedRowPolicyDecision<C> {
    /// Every protected mutation was authorized by current capability and evidence.
    Authorized(CheckedValidatedCommand<C>),
    /// Policy denied without distinguishing its capability, row, or relationship cause.
    Denied(CheckedCandidateRejection<C>),
    /// The write transaction could not load the complete policy evidence.
    StorageFailure(StorageError),
    /// Trusted policy and mutation structures disagreed.
    Integrity,
}

/// A proven noncommit decision that owns the exact storage candidate to reject.
pub(super) struct CheckedCandidateRejection<C> {
    candidate: C,
    reason: CandidateValidationRejection,
    attempt: ProvenanceBoundCommandAttempt,
}

impl<C> CheckedCandidateRejection<C>
where
    C: CommandCandidateAwaitingValidation,
{
    pub(super) fn reject_storage_and_rollback(self) -> RolledBackCandidateDisposition {
        self.attempt
            .reject_storage_and_rollback(self.candidate, self.reason)
    }
}

/// Unforgeable authority created only after every transaction-current semantic check.
pub(super) struct CheckedCandidateSeal {
    _private: (),
}

impl CheckedCandidateSeal {
    fn after_successful_validation() -> Self {
        Self { _private: () }
    }

    #[cfg(test)]
    pub(super) fn for_record_graph_test() -> Self {
        Self::after_successful_validation()
    }
}

/// Exact validated values and their inseparable storage candidate.
pub(super) struct CheckedValidatedCommand<C> {
    seal: CheckedCandidateSeal,
    candidate: C,
    current: MaterializedTransactionCurrentState,
    mutation_positions: Box<[Option<usize>]>,
    attempt: ProvenanceBoundCommandAttempt,
}

impl<C> CheckedValidatedCommand<C> {
    pub(super) const fn attempt(&self) -> &ProvenanceBoundCommandAttempt {
        &self.attempt
    }

    pub(super) const fn resolved(&self) -> &ResolvedExecutablePlan {
        self.attempt.resolved_plan()
    }

    pub(super) fn evaluated(&self) -> &EvaluatedCommand {
        self.attempt.evaluated()
    }

    pub(super) const fn current(&self) -> &TransactionCurrentState {
        self.current.state()
    }

    pub(super) fn mutation_positions(&self) -> &[Option<usize>] {
        &self.mutation_positions
    }

    pub(super) fn take_prepared_indexes(
        &mut self,
    ) -> Option<crate::command_index::DerivedCommandIndexes> {
        self.attempt.take_prepared_indexes()
    }

    pub(super) fn take_prepared_capsule(
        &mut self,
    ) -> Option<riffdb_storage_api::PreparedCapsuleCommandFragmentsV1> {
        self.attempt.take_prepared_capsule()
    }
}

impl<C> CheckedValidatedCommand<C>
where
    C: CommandCandidateAwaitingValidation,
{
    /// Rechecks every protected mutation inside the authoritative write transaction.
    pub(super) fn recheck_row_policy(self) -> CheckedRowPolicyDecision<C> {
        let transitions = match command_policy_transitions(&self) {
            Ok(transitions) => transitions,
            Err(()) => return CheckedRowPolicyDecision::Integrity,
        };
        if transitions.is_empty() {
            return CheckedRowPolicyDecision::Authorized(self);
        }
        let Some(context) = self.attempt.row_policy() else {
            return CheckedRowPolicyDecision::Denied(self.reject_row_policy());
        };
        let mut all_lookups = Vec::with_capacity(transitions.len());
        let mut lookup_counts = Vec::with_capacity(transitions.len());
        for transition in &transitions {
            let lookups = match context.relationship_lookups(
                transition.entity_type,
                transition.operation,
                transition.current.as_ref(),
                transition.successor.as_ref(),
            ) {
                Ok(lookups) => lookups,
                Err(_) => return CheckedRowPolicyDecision::Denied(self.reject_row_policy()),
            };
            lookup_counts.push(lookups.len());
            for lookup in lookups {
                let storage_lookup = TransactionCurrentPolicyLookupV1::new(
                    lookup.target_entity(),
                    lookup.index_id(),
                    lookup.partition().clone(),
                    lookup.index_prefix().to_vec(),
                );
                match storage_lookup {
                    Ok(lookup) => all_lookups.push(lookup),
                    Err(_) => return CheckedRowPolicyDecision::Integrity,
                }
            }
        }
        let principal = context.internal_authority().internal_principal();
        let request =
            match TransactionCurrentPolicyRequestV1::new(principal.capability_id(), all_lookups) {
                Ok(request) => request,
                Err(_) => return CheckedRowPolicyDecision::Integrity,
            };
        let current_policy = match self.candidate.read_transaction_current_policy(&request) {
            Ok(current) => current,
            Err(error) => return CheckedRowPolicyDecision::StorageFailure(error),
        };
        let Some(capability) = current_policy.capability() else {
            return CheckedRowPolicyDecision::Denied(self.reject_row_policy());
        };
        if !capability_matches_policy_context(context, capability) {
            return CheckedRowPolicyDecision::Denied(self.reject_row_policy());
        }
        let mut evidence = current_policy.relationship_exists();
        for (transition, lookup_count) in transitions.iter().zip(lookup_counts) {
            let Some((selected, remaining)) = evidence.split_at_checked(lookup_count) else {
                return CheckedRowPolicyDecision::Integrity;
            };
            if !context.allows_transition(
                transition.entity_type,
                transition.operation,
                transition.current.as_ref(),
                transition.successor.as_ref(),
                selected,
            ) {
                return CheckedRowPolicyDecision::Denied(self.reject_row_policy());
            }
            evidence = remaining;
        }
        if !evidence.is_empty() {
            return CheckedRowPolicyDecision::Integrity;
        }
        CheckedRowPolicyDecision::Authorized(self)
    }

    fn reject_row_policy(self) -> CheckedCandidateRejection<C> {
        let Self {
            seal: _,
            candidate,
            current,
            mutation_positions: _,
            attempt,
        } = self;
        drop(current);
        CheckedCandidateRejection {
            candidate,
            reason: CandidateValidationRejection::RowPolicyDenied,
            attempt,
        }
    }
}

struct PolicyTransition {
    entity_type: riffdb_types::EntityTypeId,
    operation: riffdb_contract_ir::RowPolicyOperationV1,
    current: Option<CanonicalRecord>,
    successor: Option<CanonicalRecord>,
}

fn command_policy_transitions<C>(
    command: &CheckedValidatedCommand<C>,
) -> Result<Vec<PolicyTransition>, ()> {
    let mut transitions = Vec::with_capacity(command.evaluated().mutations().len());
    for mutation in command.evaluated().mutations() {
        if !mutation_entity_is_protected(command.resolved(), mutation) {
            continue;
        }
        let Some((operation, current, successor)) =
            mutation_policy_rows(command.current(), mutation)
        else {
            return Err(());
        };
        transitions.push(PolicyTransition {
            entity_type: mutation.target().entity_type_id(),
            operation,
            current,
            successor,
        });
    }
    append_decision_no_effect_policy_transitions(command, &mut transitions)?;
    if !transitions.is_empty() || !is_cascade_failure(command.resolved(), command.evaluated()) {
        return Ok(transitions);
    }

    // A declared cascade overflow has no mutation graph, but the root and
    // maximum-plus-one candidate rows still influenced the released outcome.
    // Treat those observations as delete-policy transitions so current
    // capability and row policy are proven before cardinality is disclosed.
    let facts = derive_input_command_facts(
        command.resolved().plan(),
        command.attempt.normalized_input().clone(),
    )
    .map_err(|_| ())?;
    for (plan_index, observation) in facts
        .binding_plan_indices()
        .iter()
        .zip(command.current().bindings())
    {
        let binding = command
            .resolved()
            .plan()
            .bindings()
            .get(*plan_index as usize)
            .ok_or(())?;
        if binding.mode() != BindingMode::Delete
            || !binding.cascade_failure().is_some_and(|failure| {
                failure.outcome_id() == command.evaluated().outcome().outcome_id()
            })
        {
            continue;
        }
        let EntityObservation::Present(record) = observation else {
            continue;
        };
        if entity_is_policy_protected(command.resolved(), record.target().entity_type_id()) {
            transitions.push(PolicyTransition {
                entity_type: record.target().entity_type_id(),
                operation: riffdb_contract_ir::RowPolicyOperationV1::Delete,
                current: Some(record.fields().clone()),
                successor: None,
            });
        }
    }
    for observation in command.current().cascade_predecessors() {
        let EntityObservation::Present(record) = observation else {
            return Err(());
        };
        if entity_is_policy_protected(command.resolved(), record.target().entity_type_id()) {
            transitions.push(PolicyTransition {
                entity_type: record.target().entity_type_id(),
                operation: riffdb_contract_ir::RowPolicyOperationV1::Delete,
                current: Some(record.fields().clone()),
                successor: None,
            });
        }
    }
    Ok(transitions)
}

fn append_decision_no_effect_policy_transitions<C>(
    command: &CheckedValidatedCommand<C>,
    transitions: &mut Vec<PolicyTransition>,
) -> Result<(), ()> {
    let plan = command.resolved().plan();
    if !plan.requires_ir_v18() {
        return Ok(());
    }
    let facts = derive_input_command_facts(plan, command.attempt.normalized_input().clone())
        .map_err(|_| ())?;
    let mutated = command
        .evaluated()
        .mutations()
        .iter()
        .map(EntityMutation::target)
        .collect::<std::collections::BTreeSet<_>>();
    let elements = plan
        .collection_expansion()
        .map(|expansion| {
            let Some(CanonicalValue::List(elements)) =
                record_field(command.attempt.normalized_input(), expansion.input_field())
            else {
                return Err(());
            };
            Ok(elements)
        })
        .transpose()?;
    let pending = command.attempt.commit_context().pending();
    for ((plan_index, ordinal), observation) in facts
        .binding_plan_indices()
        .iter()
        .zip(facts.binding_element_ordinals())
        .zip(command.current().bindings())
    {
        let binding = plan.bindings().get(*plan_index as usize).ok_or(())?;
        if binding.mode() != BindingMode::ObserveOrInitialize
            || mutated.contains(observation.target())
            || !entity_is_policy_protected(command.resolved(), binding.entity_type())
        {
            continue;
        }
        let (operation, current, successor) = match observation {
            EntityObservation::Present(record) => (
                riffdb_contract_ir::RowPolicyOperationV1::Update,
                Some(record.fields().clone()),
                Some(record.fields().clone()),
            ),
            EntityObservation::Absent(target) => {
                let entity = command
                    .resolved()
                    .bundle()
                    .bundle()
                    .schema()
                    .entity(binding.entity_type())
                    .ok_or(())?;
                let key_values = entity
                    .primary_key()
                    .decode_entity(target.key())
                    .map_err(|_| ())?;
                let mut fields = entity
                    .primary_key_fields()
                    .iter()
                    .copied()
                    .zip(key_values)
                    .collect::<Vec<_>>();
                let element = ordinal.and_then(|ordinal| {
                    elements.and_then(|items| items.values().get(ordinal as usize))
                });
                let values = DecisionInitializerValues {
                    input: command.attempt.normalized_input(),
                    service_values: pending.service_values(),
                    element,
                    logical_time: pending.logical_time(),
                };
                for initializer in binding.initializer() {
                    fields.push((
                        initializer.field_id(),
                        evaluate_expression(plan.expressions(), initializer.expression(), &values)
                            .map_err(|_| ())?,
                    ));
                }
                fields.sort_unstable_by_key(|(field, _)| *field);
                let provisional = CanonicalRecord::new(fields).map_err(|_| ())?;
                (
                    riffdb_contract_ir::RowPolicyOperationV1::Create,
                    None,
                    Some(provisional),
                )
            }
        };
        transitions.push(PolicyTransition {
            entity_type: binding.entity_type(),
            operation,
            current,
            successor,
        });
    }
    Ok(())
}

struct DecisionInitializerValues<'a> {
    input: &'a CanonicalRecord,
    service_values: &'a CanonicalRecord,
    element: Option<&'a CanonicalValue>,
    logical_time: LogicalTime,
}

impl ExpressionValueSource for DecisionInitializerValues<'_> {
    fn input_field(&self, field: FieldId) -> Option<CanonicalValue> {
        record_field(self.input, field).cloned()
    }

    fn service_value(&self, field: FieldId) -> Option<CanonicalValue> {
        record_field(self.service_values, field).cloned()
    }

    fn collection_element(&self) -> Option<CanonicalValue> {
        self.element.cloned()
    }

    fn collection_element_field(&self, field: FieldId) -> Option<CanonicalValue> {
        let CanonicalValue::Record(record) = self.element? else {
            return None;
        };
        record_field(record, field).cloned()
    }

    fn transaction_time(&self) -> Option<LogicalTime> {
        Some(self.logical_time)
    }
}

fn is_cascade_failure(resolved: &ResolvedExecutablePlan, evaluated: &EvaluatedCommand) -> bool {
    resolved.plan().bindings().iter().any(|binding| {
        binding
            .cascade_failure()
            .is_some_and(|failure| failure.outcome_id() == evaluated.outcome().outcome_id())
    })
}

fn mutation_entity_is_protected(
    resolved: &ResolvedExecutablePlan,
    mutation: &EntityMutation,
) -> bool {
    entity_is_policy_protected(resolved, mutation.target().entity_type_id())
}

fn entity_is_policy_protected(
    resolved: &ResolvedExecutablePlan,
    entity_type: riffdb_types::EntityTypeId,
) -> bool {
    resolved
        .bundle()
        .bundle()
        .row_policies()
        .policies()
        .iter()
        .any(|policy| policy.entity() == entity_type)
}

fn mutation_policy_rows(
    current: &TransactionCurrentState,
    mutation: &EntityMutation,
) -> Option<(
    riffdb_contract_ir::RowPolicyOperationV1,
    Option<CanonicalRecord>,
    Option<CanonicalRecord>,
)> {
    let current_row = current
        .bindings()
        .iter()
        .chain(current.cascade_predecessors())
        .find_map(|observation| match observation {
            EntityObservation::Present(record) if record.target() == mutation.target() => {
                Some(record.fields().clone())
            }
            EntityObservation::Absent(_) | EntityObservation::Present(_) => None,
        });
    match mutation {
        EntityMutation::Create(post_image) => Some((
            riffdb_contract_ir::RowPolicyOperationV1::Create,
            None,
            Some(post_image.fields().clone()),
        )),
        EntityMutation::Replace { post_image, .. } => Some((
            riffdb_contract_ir::RowPolicyOperationV1::Update,
            Some(current_row?),
            Some(post_image.fields().clone()),
        )),
        EntityMutation::Delete { .. } => Some((
            riffdb_contract_ir::RowPolicyOperationV1::Delete,
            Some(current_row?),
            None,
        )),
    }
}

fn capability_matches_policy_context(
    context: &AuthorizedCommandRowPolicyContextV1,
    capability: &StoredCapabilityRecordV1,
) -> bool {
    let authority = context.internal_authority();
    let principal = authority.internal_principal();
    capability.capability_id() == principal.capability_id()
        && capability.revision() == principal.revision()
        && capability.database_id() == principal.database_id()
        && capability.environment() == principal.environment()
        && capability.principal_id() == principal.principal_id()
        && capability.actor_kind() == principal.actor_kind()
        && capability.audiences() == principal.internal_audiences()
        && capability.grant().tenant_scope() == principal.tenant_scope()
        && capability.issued_at() == principal.issued_at()
        && capability.expires_at() == principal.expires_at()
        && capability.grant().internal_row_policy() == Some(authority.internal_grant())
        && capability.lifecycle() == &CapabilityLifecycleV1::Active
}

impl<C> CheckedValidatedCommand<C>
where
    C: CommandCandidateAwaitingValidation,
{
    /// Reads exact production-vector predecessors without releasing the
    /// validated writer candidate.
    pub(super) fn read_vector_evidence(
        &self,
        request: &riffdb_storage_api::VectorEvidenceReadRequestV1,
    ) -> Result<riffdb_storage_api::TransactionCurrentVectorEvidenceV1, StorageError> {
        self.candidate
            .read_transaction_current_vector_evidence(request)
    }

    pub(super) fn plan_validated(
        self,
        affected_targets: riffdb_storage_api::AffectedIndexEpochTargets,
    ) -> CheckedValidatedCommand<C::AffectedEpochRead> {
        let Self {
            seal,
            attempt,
            candidate,
            current,
            mutation_positions,
        } = self;
        let candidate = candidate.plan_validated(affected_targets);
        CheckedValidatedCommand {
            seal,
            attempt,
            candidate,
            current,
            mutation_positions,
        }
    }
}

/// Closed result of reading mutation-affected epochs from the exact candidate.
pub(super) enum CheckedAffectedEpochRead<C> {
    Ready(Box<CheckedValidatedCommand<C>>),
    StorageFailure(StorageError),
}

impl<C> CheckedValidatedCommand<C>
where
    C: CommandCandidateAffectedEpochRead,
{
    pub(super) fn read_affected_epoch_current(
        self,
    ) -> CheckedAffectedEpochRead<C::AwaitingCapacity> {
        let Self {
            seal,
            attempt,
            candidate,
            current,
            mutation_positions,
        } = self;
        let candidate = match candidate.read_affected_epoch_current() {
            Ok(candidate) => candidate,
            Err(error) => {
                drop(current);
                drop(mutation_positions);
                drop(attempt);
                return CheckedAffectedEpochRead::StorageFailure(error);
            }
        };
        CheckedAffectedEpochRead::Ready(Box::new(CheckedValidatedCommand {
            seal,
            attempt,
            candidate,
            current,
            mutation_positions,
        }))
    }
}

/// Closed result of capacity reservation with no detached storage state.
pub(super) enum CheckedCapacityReservation<C> {
    Reserved(Box<CheckedValidatedCommand<C>>),
    BatchFull,
    ProvenanceIdCollision,
    StorageFailure(StorageError),
    Integrity,
}

impl<C> CheckedValidatedCommand<C>
where
    C: CommandCandidateAwaitingCapacity,
{
    pub(super) const fn awaiting_capacity(&self) -> &C {
        &self.candidate
    }

    /// Rejects an exact transaction-current uniqueness collision before any
    /// capacity or sequence is assigned.
    pub(super) fn reject_unique_conflict(self) -> RolledBackCandidateDisposition {
        let Self {
            seal: _,
            attempt,
            candidate,
            current,
            mutation_positions,
        } = self;
        drop(current);
        drop(mutation_positions);
        attempt.reject_after_index_read_and_rollback(
            candidate,
            CandidateValidationRejection::UniqueConflict,
        )
    }

    pub(super) fn reserve_capacity(
        self,
        write_plan: CommandWriteSetPlanV1,
    ) -> CheckedCapacityReservation<C::CapacityReserved> {
        let Self {
            seal,
            attempt,
            candidate,
            current,
            mutation_positions,
        } = self;
        if candidate.intent() != attempt.commit_intent()
            || !write_plan.matches_retained_candidate(
                candidate.intent(),
                candidate.affected_targets(),
                candidate.affected_current(),
            )
        {
            drop(candidate);
            drop(current);
            drop(mutation_positions);
            drop(attempt);
            return CheckedCapacityReservation::Integrity;
        }
        match candidate.reserve_capacity(write_plan) {
            Ok(CandidateCapacityResult::Reserved(candidate)) => {
                CheckedCapacityReservation::Reserved(Box::new(CheckedValidatedCommand {
                    seal,
                    attempt,
                    candidate,
                    current,
                    mutation_positions,
                }))
            }
            Ok(CandidateCapacityResult::BatchFull(abandoned)) => {
                let (prior, intent) = abandoned.into_parts();
                let exact_intent = *intent == *attempt.commit_intent();
                drop(prior);
                drop(intent);
                drop(current);
                drop(mutation_positions);
                drop(attempt);
                if exact_intent {
                    CheckedCapacityReservation::BatchFull
                } else {
                    CheckedCapacityReservation::Integrity
                }
            }
            Ok(CandidateCapacityResult::ProvenanceIdCollision(_)) => {
                drop(current);
                drop(mutation_positions);
                drop(attempt);
                CheckedCapacityReservation::ProvenanceIdCollision
            }
            Err(error) => {
                drop(current);
                drop(mutation_positions);
                drop(attempt);
                CheckedCapacityReservation::StorageFailure(error)
            }
        }
    }
}

/// Closed sequence-assignment result retaining the exact reserved candidate.
pub(super) enum CheckedSequenceAssignment<C> {
    Assigned(Box<CheckedValidatedCommand<C>>),
    StorageFailure(StorageError),
    Integrity,
}

impl<C> CheckedValidatedCommand<C>
where
    C: CommandCandidateCapacityReserved,
{
    pub(super) const fn capacity_reserved(&self) -> &C {
        &self.candidate
    }

    pub(super) fn assign_sequence(self) -> CheckedSequenceAssignment<C::SequenceAssigned> {
        let Self {
            seal,
            attempt,
            candidate,
            current,
            mutation_positions,
        } = self;
        if candidate.intent() != attempt.commit_intent() {
            drop(candidate);
            drop(current);
            drop(mutation_positions);
            drop(attempt);
            return CheckedSequenceAssignment::Integrity;
        }
        match candidate.assign_sequence() {
            Ok(candidate) => {
                CheckedSequenceAssignment::Assigned(Box::new(CheckedValidatedCommand {
                    seal,
                    attempt,
                    candidate,
                    current,
                    mutation_positions,
                }))
            }
            Err(error) => {
                drop(current);
                drop(mutation_positions);
                drop(attempt);
                CheckedSequenceAssignment::StorageFailure(error)
            }
        }
    }
}

/// Semantic evidence retained after the exact sequence-assigned state stages.
pub(super) struct StagedValidatedCommand {
    _seal: CheckedCandidateSeal,
    attempt: ProvenanceBoundCommandAttempt,
    _current: MaterializedTransactionCurrentState,
    _mutation_positions: Box<[Option<usize>]>,
}

impl StagedValidatedCommand {
    pub(super) const fn attempt(&self) -> &ProvenanceBoundCommandAttempt {
        &self.attempt
    }

    pub(super) fn into_pending_after_proven_noncommit(self) -> PendingCommandAttempts {
        let Self {
            _seal,
            attempt,
            _current,
            _mutation_positions,
        } = self;
        drop(_current);
        drop(_mutation_positions);
        attempt.into_pending_after_proven_noncommit()
    }

    pub(super) fn into_post_apply_evidence(self) -> Result<PostApplyCommandEvidence, ()> {
        let Self {
            _seal,
            attempt,
            _current,
            _mutation_positions,
        } = self;
        drop(_current);
        drop(_mutation_positions);
        attempt.into_post_apply_evidence()
    }
}

/// Closed staging result; only the storage stage call can construct `Staged`.
pub(super) enum CheckedStorageStage<S> {
    Staged {
        storage: S,
        evidence: Box<StagedValidatedCommand>,
    },
    StorageFailure(StorageError),
    Integrity,
}

/// Closed detachment result retaining semantic evidence while returning the
/// exact writer batch and payload-free reservation separately.
pub(super) enum CheckedDetachedStage<P> {
    Detached {
        prior: P,
        reservation: riffdb_storage_api::DetachedCommandReservationV1,
        evidence: Box<StagedValidatedCommand>,
    },
    StorageFailure(StorageError),
    Integrity,
}

impl<C> CheckedValidatedCommand<C>
where
    C: CommandCandidateSequenceAssigned,
{
    pub(super) const fn sequence_assigned(&self) -> &C {
        &self.candidate
    }

    pub(super) fn stage(self, records: AtomicCommandRecordSet) -> CheckedStorageStage<C::Staged> {
        let Self {
            seal,
            attempt,
            candidate,
            current,
            mutation_positions,
        } = self;
        if !records.matches_reserved_candidate(
            candidate.assignment(),
            candidate.intent(),
            candidate.write_plan(),
        ) || candidate.intent() != attempt.commit_intent()
        {
            drop(candidate);
            drop(current);
            drop(mutation_positions);
            drop(attempt);
            return CheckedStorageStage::Integrity;
        }
        match candidate.stage(records) {
            Ok(storage) => CheckedStorageStage::Staged {
                storage,
                evidence: Box::new(StagedValidatedCommand {
                    _seal: seal,
                    attempt,
                    _current: current,
                    _mutation_positions: mutation_positions,
                }),
            },
            Err(error) => {
                drop(current);
                drop(mutation_positions);
                drop(attempt);
                CheckedStorageStage::StorageFailure(error)
            }
        }
    }

    pub(super) fn detach(self) -> CheckedDetachedStage<C::Prior> {
        let Self {
            seal,
            attempt,
            candidate,
            current,
            mutation_positions,
        } = self;
        let expected_assignment = candidate.assignment();
        if candidate.intent() != attempt.commit_intent() {
            drop(candidate);
            drop(current);
            drop(mutation_positions);
            drop(attempt);
            return CheckedDetachedStage::Integrity;
        }
        match candidate.detach() {
            Ok((prior, reservation)) if reservation.assignment() == expected_assignment => {
                CheckedDetachedStage::Detached {
                    prior,
                    reservation,
                    evidence: Box::new(StagedValidatedCommand {
                        _seal: seal,
                        attempt,
                        _current: current,
                        _mutation_positions: mutation_positions,
                    }),
                }
            }
            Ok((prior, _)) => {
                drop(prior);
                drop(current);
                drop(mutation_positions);
                drop(attempt);
                CheckedDetachedStage::Integrity
            }
            Err(error) => {
                drop(current);
                drop(mutation_positions);
                drop(attempt);
                CheckedDetachedStage::StorageFailure(error)
            }
        }
    }
}

/// Validates only the inseparable attempt/current/storage aggregate.
pub(super) fn validate_checked_transaction_current<C>(
    checked_current: CheckedTransactionCurrentAttempt<C>,
) -> Result<CheckedCandidateDecision<C>, CommandValidationError>
where
    C: CommandCandidateAwaitingValidation,
{
    let CheckedTransactionCurrentAttempt {
        mut attempt,
        candidate,
        current,
    } = checked_current;
    if !attempt.has_exact_semantic_join() {
        drop(candidate);
        drop(current);
        drop(attempt);
        return Err(CommandValidationError::integrity());
    }
    // `MaterializedTransactionCurrentState` can be constructed only after the
    // catalog proves the writer's raw current observations normalize to the
    // exact snapshot retained by this attempt.  A worker-prepared semantic
    // proof for that same attempt is therefore transaction-current here; the
    // writer still owns the fresh materialization and storage candidate.
    if let Some(mutation_positions) = attempt.take_prepared_mutation_positions() {
        return Ok(CheckedCandidateDecision::Validated(
            CheckedValidatedCommand {
                seal: CheckedCandidateSeal::after_successful_validation(),
                attempt,
                candidate,
                current,
                mutation_positions,
            },
        ));
    }
    if !attempt.sealed_decision_evaluation_is_exact() {
        return Err(CommandValidationError::integrity());
    }
    let pending = attempt.commit_context().pending();
    let decision = validate_transaction_current_command_parts(
        attempt.resolved_plan(),
        attempt.normalized_input(),
        attempt.input_facts(),
        pending.logical_time(),
        attempt.evaluated(),
        current.state(),
    );
    let decision = match decision {
        Ok(decision) => decision,
        Err(error) => {
            drop(candidate);
            drop(current);
            drop(attempt);
            return Err(error);
        }
    };
    match decision {
        CheckedCommandDecision::ZeroMutation => Ok(CheckedCandidateDecision::Validated(
            CheckedValidatedCommand {
                seal: CheckedCandidateSeal::after_successful_validation(),
                mutation_positions: vec![None; attempt.resolved_plan().plan().bindings().len()]
                    .into_boxed_slice(),
                attempt,
                candidate,
                current,
            },
        )),
        CheckedCommandDecision::NonZero(mutation_positions) => Ok(
            CheckedCandidateDecision::Validated(CheckedValidatedCommand {
                seal: CheckedCandidateSeal::after_successful_validation(),
                attempt,
                candidate,
                current,
                mutation_positions,
            }),
        ),
        CheckedCommandDecision::Rejected(reason) => {
            drop(current);
            Ok(CheckedCandidateDecision::Rejected(
                CheckedCandidateRejection {
                    attempt,
                    candidate,
                    reason,
                },
            ))
        }
    }
}

/// Pure semantic core, deliberately private until candidate identity can be
/// preserved by construction across the full storage admission chain.
pub(super) fn validate_transaction_current_command_parts(
    resolved: &ResolvedExecutablePlan,
    normalized_input: &CanonicalRecord,
    facts: &InputDerivedCommandFacts,
    logical_time: LogicalTime,
    evaluated: &EvaluatedCommand,
    current: &TransactionCurrentState,
) -> Result<CheckedCommandDecision, CommandValidationError> {
    // The facts used to be derived here from `normalized_input`, so the two
    // agreed by construction. They are now supplied by the caller, so the bind
    // between the proof and this exact plan and input is checked explicitly
    // instead. This is the same coupling, made enforceable.
    if !facts.matches_command(resolved.plan(), normalized_input) {
        return Err(CommandValidationError::integrity());
    }
    validate_identity_positions_and_output(resolved, normalized_input, facts, evaluated, current)?;

    if evaluated.mutations().is_empty() {
        return Ok(CheckedCommandDecision::ZeroMutation);
    }

    let coverage = prove_mutation_coverage(resolved, facts, evaluated, current)?;
    if resolved.plan().commit_checks().is_empty() {
        return Ok(CheckedCommandDecision::NonZero(coverage));
    }
    let element_ordinals = if let Some(expansion) = resolved.plan().collection_expansion() {
        let CanonicalValue::List(elements) =
            record_field(normalized_input, expansion.input_field())
                .ok_or_else(CommandValidationError::integrity)?
        else {
            return Err(CommandValidationError::integrity());
        };
        (0..elements.len())
            .map(|ordinal| {
                u16::try_from(ordinal)
                    .map(Some)
                    .map_err(|_| CommandValidationError::integrity())
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        vec![None]
    };
    for ordinal in element_ordinals {
        let active_bindings = facts
            .binding_plan_indices()
            .iter()
            .zip(facts.binding_element_ordinals())
            .zip(coverage.iter())
            .filter_map(|((plan_index, element), mutation)| {
                (*element == ordinal && mutation.is_some()).then_some(BindingId::new(*plan_index))
            })
            .collect::<std::collections::BTreeSet<_>>();
        let selected_checks = resolved
            .plan()
            .commit_checks()
            .iter()
            .filter(|check| {
                check.source_bindings().iter().all(|binding| {
                    resolved
                        .plan()
                        .bindings()
                        .get(binding.get() as usize)
                        .is_some_and(|plan| {
                            plan.mode() == BindingMode::Read || active_bindings.contains(binding)
                        })
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        if selected_checks.is_empty() {
            continue;
        }
        let values = assemble_transaction_current_values(
            resolved,
            normalized_input,
            logical_time,
            evaluated,
            current,
            &coverage,
            ordinal,
        )?;
        match evaluate_commit_checks(resolved.plan().expressions(), &selected_checks, &values) {
            Ok(CommitCheckResult::Satisfied) => {}
            Ok(CommitCheckResult::Rejected { .. }) => {
                return Ok(CheckedCommandDecision::Rejected(
                    CandidateValidationRejection::CommitCheckRejected,
                ));
            }
            Err(EvaluationError::Arithmetic) => {
                return Ok(CheckedCommandDecision::Rejected(
                    CandidateValidationRejection::CommitCheckArithmeticFault,
                ));
            }
            Err(EvaluationError::Integrity) => {
                return Err(CommandValidationError::integrity());
            }
        }
    }
    Ok(CheckedCommandDecision::NonZero(coverage))
}

fn validate_identity_positions_and_output(
    resolved: &ResolvedExecutablePlan,
    normalized_input: &CanonicalRecord,
    facts: &InputDerivedCommandFacts,
    evaluated: &EvaluatedCommand,
    current: &TransactionCurrentState,
) -> Result<(), CommandValidationError> {
    let reference = resolved.reference();
    let plan = resolved.plan();
    let request = evaluated.validation_request();
    let expected_ranges = crate::command_index::derive_delete_ranges(resolved, facts)
        .map_err(|_| CommandValidationError::integrity())?
        .into_iter()
        .map(|(target, _)| target)
        .collect::<Vec<_>>();
    if reference != evaluated.plan()
        || request.plan() != evaluated.plan()
        || plan.execution_class() != ExecutionClass::IdempotentMutation
        || plan.command_id() != reference.command_id()
        || plan.contract_version() != reference.contract_version()
        || plan.plan_hash() != reference.command_plan_hash()
        || facts.binding_entity_keys().len() != request.binding_targets().len()
        || facts.binding_entity_keys().len() != current.bindings().len()
        || facts.root_validation_entity_keys().len() != request.root_validation_targets().len()
        || facts.root_validation_entity_keys().len() != current.root_validations().len()
        || request.cascade_targets().len() != current.cascade_predecessors().len()
        || request.range_targets() != expected_ranges
        || current.ranges().len() != expected_ranges.len()
    {
        return Err(CommandValidationError::integrity());
    }

    validate_exact_record(
        resolved.bundle().bundle().schema(),
        plan.input().record(),
        normalized_input,
    )?;
    for (slot, (((plan_index, key), target), observation)) in facts
        .binding_plan_indices()
        .iter()
        .zip(facts.binding_entity_keys())
        .zip(request.binding_targets())
        .zip(current.bindings())
        .enumerate()
    {
        let binding = plan
            .bindings()
            .get(*plan_index as usize)
            .ok_or_else(CommandValidationError::integrity)?;
        if facts.binding_element_ordinals().get(slot).is_none()
            || binding.entity_type() != target.entity_type_id()
            || target.key() != key
            || observation.target() != target
        {
            return Err(CommandValidationError::integrity());
        }
    }
    for (slot, (((plan_index, key), target), observation)) in facts
        .root_validation_plan_indices()
        .iter()
        .zip(facts.root_validation_entity_keys())
        .zip(request.root_validation_targets())
        .zip(current.root_validations())
        .enumerate()
    {
        let read = plan
            .root_validation_reads()
            .get(*plan_index as usize)
            .ok_or_else(CommandValidationError::integrity)?;
        if facts.root_validation_element_ordinals().get(slot).is_none()
            || read.entity_type() != target.entity_type_id()
            || target.key() != key
            || observation.target() != target
        {
            return Err(CommandValidationError::integrity());
        }
    }
    if request
        .cascade_targets()
        .iter()
        .zip(current.cascade_predecessors())
        .any(|(target, observation)| target != observation.target())
        || request
            .range_targets()
            .iter()
            .zip(current.ranges())
            .any(|(target, observation)| target != observation.target())
    {
        return Err(CommandValidationError::integrity());
    }

    validate_evaluated_output(
        plan,
        resolved.bundle().bundle().schema(),
        normalized_input,
        evaluated,
    )
}

fn validate_evaluated_output(
    plan: &CommandPlan,
    schema: &SchemaIr,
    normalized_input: &CanonicalRecord,
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
        if plan.requires_ir_v18() && outcome.outcome_id() == plan.success_outcome() {
            if !evaluated.event_intents().is_empty() || !evaluated.embedding_writes().is_empty() {
                return Err(CommandValidationError::integrity());
            }
            return Ok(());
        }
        let declared_rejection = outcome.outcome_id() != plan.success_outcome()
            && (plan.bindings().iter().any(|binding| {
                binding
                    .failure()
                    .is_some_and(|failure| failure.outcome_id() == outcome.outcome_id())
                    || binding
                        .restriction_failure()
                        .is_some_and(|failure| failure.outcome_id() == outcome.outcome_id())
                    || binding
                        .cascade_failure()
                        .is_some_and(|failure| failure.outcome_id() == outcome.outcome_id())
            }) || plan.decisions().iter().any(|decision| {
                decision
                    .when_arms()
                    .iter()
                    .map(|arm| arm.action())
                    .chain(std::iter::once(decision.else_action()))
                    .filter_map(riffdb_contract_ir::CommandDecisionActionV1::rejection)
                    .any(|candidate| candidate.outcome_id() == outcome.outcome_id())
            }) || plan
                .instructions()
                .iter()
                .any(|instruction| match instruction {
                    Instruction::Require { reject, .. } => {
                        reject.outcome_id() == outcome.outcome_id()
                    }
                    Instruction::WorkflowTransition { stale, illegal, .. } => [stale, illegal]
                        .into_iter()
                        .any(|candidate| candidate.outcome_id() == outcome.outcome_id()),
                    Instruction::WorkflowLease { operation, .. } => match operation {
                        riffdb_contract_ir::WorkflowLeaseOperation::Claim {
                            stale,
                            unavailable,
                            invalid,
                            exhausted,
                            ..
                        } => [stale, unavailable, invalid, exhausted]
                            .into_iter()
                            .any(|candidate| candidate.outcome_id() == outcome.outcome_id()),
                        riffdb_contract_ir::WorkflowLeaseOperation::Renew {
                            stale,
                            invalid,
                            expired,
                            exhausted,
                            ..
                        } => [stale, invalid, expired, exhausted]
                            .into_iter()
                            .any(|candidate| candidate.outcome_id() == outcome.outcome_id()),
                        riffdb_contract_ir::WorkflowLeaseOperation::Release {
                            stale,
                            invalid,
                            ..
                        } => [stale, invalid]
                            .into_iter()
                            .any(|candidate| candidate.outcome_id() == outcome.outcome_id()),
                        riffdb_contract_ir::WorkflowLeaseOperation::Expire {
                            stale,
                            active,
                            ..
                        } => [stale, active]
                            .into_iter()
                            .any(|candidate| candidate.outcome_id() == outcome.outcome_id()),
                        riffdb_contract_ir::WorkflowLeaseOperation::Fence {
                            stale,
                            invalid,
                            expired,
                            ..
                        } => [stale, invalid, expired]
                            .into_iter()
                            .any(|candidate| candidate.outcome_id() == outcome.outcome_id()),
                    },
                    Instruction::SetField { .. }
                    | Instruction::SetEmbedding { .. }
                    | Instruction::EmitEvent(_)
                    | Instruction::Return(_) => false,
                }));
        if !declared_rejection
            || !evaluated.event_intents().is_empty()
            || !evaluated.embedding_writes().is_empty()
        {
            return Err(CommandValidationError::integrity());
        }
        return Ok(());
    }

    if outcome.outcome_id() != plan.success_outcome() {
        return Err(CommandValidationError::integrity());
    }
    let instruction_ordinals = if let Some(expansion) = plan.collection_expansion() {
        let CanonicalValue::List(elements) =
            record_field(normalized_input, expansion.input_field())
                .ok_or_else(CommandValidationError::integrity)?
        else {
            return Err(CommandValidationError::integrity());
        };
        let first = expansion.first_instruction() as usize;
        let end = first
            .checked_add(expansion.instruction_count())
            .ok_or_else(CommandValidationError::integrity)?;
        if end > plan.instructions().len() {
            return Err(CommandValidationError::integrity());
        }
        let mut ordinals = Vec::with_capacity(
            first
                .checked_add(
                    expansion
                        .instruction_count()
                        .checked_mul(elements.len())
                        .ok_or_else(CommandValidationError::integrity)?,
                )
                .and_then(|count| count.checked_add(plan.instructions().len() - end))
                .ok_or_else(CommandValidationError::integrity)?,
        );
        ordinals.extend(0..first);
        for _ in elements.values() {
            ordinals.extend(first..end);
        }
        ordinals.extend(end..plan.instructions().len());
        ordinals
    } else {
        (0..plan.instructions().len()).collect()
    };
    let expected_events = instruction_ordinals
        .iter()
        .copied()
        .map(|index| &plan.instructions()[index])
        .filter_map(|instruction| match instruction {
            Instruction::EmitEvent(event) => Some(event),
            Instruction::Require { .. }
            | Instruction::SetField { .. }
            | Instruction::SetEmbedding { .. }
            | Instruction::WorkflowTransition { .. }
            | Instruction::WorkflowLease { .. }
            | Instruction::Return(_) => None,
        });
    if plan.requires_ir_v18() {
        let allowed = plan
            .decisions()
            .iter()
            .flat_map(|decision| {
                decision
                    .when_arms()
                    .iter()
                    .map(riffdb_contract_ir::CommandDecisionArmV1::action)
                    .chain(std::iter::once(decision.else_action()))
                    .flat_map(riffdb_contract_ir::CommandDecisionActionV1::instructions)
            })
            .filter_map(|instruction| match instruction {
                Instruction::EmitEvent(event) => Some(event.event_type()),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        for actual in evaluated.event_intents() {
            if !allowed.contains(&actual.event_type_id()) {
                return Err(CommandValidationError::integrity());
            }
            let declared = schema
                .event(actual.event_type_id())
                .ok_or_else(CommandValidationError::integrity)?;
            validate_exact_record(schema, declared.payload(), actual.payload())?;
        }
    } else {
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
    }
    let mut expected_embeddings = BTreeMap::<(riffdb_types::EntityTypeId, FieldId), usize>::new();
    for instruction in &instruction_ordinals {
        let Instruction::SetEmbedding { binding, field, .. } = &plan.instructions()[*instruction]
        else {
            continue;
        };
        let entity = plan
            .bindings()
            .get(binding.get() as usize)
            .map(riffdb_contract_ir::BindingPlan::entity_type)
            .ok_or_else(CommandValidationError::integrity)?;
        *expected_embeddings.entry((entity, *field)).or_default() += 1;
    }
    let mut actual_embeddings = BTreeMap::new();
    for write in evaluated.embedding_writes() {
        *actual_embeddings
            .entry((write.target().entity_type_id(), write.vector_field()))
            .or_default() += 1;
    }
    if plan.requires_ir_v18() {
        let allowed = plan
            .decisions()
            .iter()
            .flat_map(|decision| {
                decision
                    .when_arms()
                    .iter()
                    .map(riffdb_contract_ir::CommandDecisionArmV1::action)
                    .chain(std::iter::once(decision.else_action()))
                    .flat_map(riffdb_contract_ir::CommandDecisionActionV1::instructions)
            })
            .filter_map(|instruction| match instruction {
                Instruction::SetEmbedding { binding, field, .. } => plan
                    .bindings()
                    .get(binding.get() as usize)
                    .map(|binding| (binding.entity_type(), *field)),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        if actual_embeddings.keys().any(|key| !allowed.contains(key)) {
            return Err(CommandValidationError::integrity());
        }
    } else if actual_embeddings != expected_embeddings {
        return Err(CommandValidationError::integrity());
    }
    Ok(())
}

pub(super) fn dependencies_from_current(
    current: &TransactionCurrentState,
) -> Result<ReadDependencies, CommandValidationError> {
    ReadDependencies::new(
        current
            .bindings()
            .iter()
            .chain(current.root_validations())
            .chain(current.cascade_predecessors())
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
    facts: &InputDerivedCommandFacts,
    evaluated: &EvaluatedCommand,
    current: &TransactionCurrentState,
) -> Result<Box<[Option<usize>]>, CommandValidationError> {
    let plan = resolved.plan();
    let request = evaluated.validation_request();
    let maximum_mutation_count = facts
        .binding_plan_indices()
        .iter()
        .filter(|index| {
            plan.bindings()
                .get(**index as usize)
                .is_some_and(|binding| binding.mode() != BindingMode::Read)
        })
        .count()
        .checked_add(request.cascade_targets().len())
        .ok_or_else(CommandValidationError::integrity)?;
    // Compiler-sealed decisions may select `no_effect` for any subset of
    // decision-owned bindings. The exact target proof below still requires
    // every unconditional mutation, consumes every cascade mutation, and
    // rejects every extra target, so this precheck is an upper bound rather
    // than an equality for V18 plans.
    let invalid_count = if plan.requires_ir_v18() {
        evaluated.mutations().len() > maximum_mutation_count
    } else {
        evaluated.mutations().len() != maximum_mutation_count
    };
    if invalid_count {
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

    let mut cascade_mutations = Vec::with_capacity(request.cascade_targets().len());
    for (target, observation) in request
        .cascade_targets()
        .iter()
        .zip(current.cascade_predecessors())
    {
        let EntityObservation::Present(record) = observation else {
            return Err(CommandValidationError::integrity());
        };
        let mutation_index = mutation_by_target
            .remove(target)
            .ok_or_else(CommandValidationError::integrity)?;
        let mutation = evaluated
            .mutations()
            .get(mutation_index)
            .ok_or_else(CommandValidationError::integrity)?;
        let EntityMutation::Delete {
            expected_version,
            prior_image,
        } = mutation
        else {
            return Err(CommandValidationError::integrity());
        };
        if *expected_version != record.entity_version()
            || prior_image.target() != target
            || prior_image.written_by_contract() != plan.contract_version()
        {
            return Err(CommandValidationError::integrity());
        }
        let entity = resolved
            .bundle()
            .bundle()
            .schema()
            .entity(target.entity_type_id())
            .ok_or_else(CommandValidationError::integrity)?;
        validate_post_image_and_project(
            resolved.bundle().bundle().schema(),
            entity,
            target,
            prior_image.fields(),
            BindingMode::Delete,
            observation,
            resolved.reference(),
        )?;
        cascade_mutations.push((target, mutation_index, record));
    }

    let conditional_bindings = plan
        .decisions()
        .iter()
        .flat_map(|decision| {
            std::iter::once(decision.binding()).chain(
                decision
                    .when_arms()
                    .iter()
                    .map(|arm| arm.action())
                    .chain(std::iter::once(decision.else_action()))
                    .flat_map(riffdb_contract_ir::CommandDecisionActionV1::bindings)
                    .copied(),
            )
        })
        .collect::<std::collections::BTreeSet<_>>();
    let mut mutable_targets = BTreeMap::<EntityTarget, BindingId>::new();
    let mut mutation_index_by_binding = vec![None; request.binding_targets().len()];
    for (slot, ((plan_index, target), observation)) in facts
        .binding_plan_indices()
        .iter()
        .zip(request.binding_targets())
        .zip(current.bindings())
        .enumerate()
    {
        let binding = plan
            .bindings()
            .get(*plan_index as usize)
            .ok_or_else(CommandValidationError::integrity)?;
        if binding.mode() == BindingMode::Read {
            continue;
        }
        if mutable_targets
            .insert(target.clone(), binding.id())
            .is_some()
        {
            return Err(CommandValidationError::integrity());
        }
        let Some(mutation_index) = mutation_by_target.remove(target) else {
            if conditional_bindings.contains(&binding.id()) {
                continue;
            }
            return Err(CommandValidationError::integrity());
        };
        let mutation = &evaluated.mutations()[mutation_index];
        if mutation.post_image().target() != target
            || mutation.post_image().written_by_contract() != plan.contract_version()
        {
            return Err(CommandValidationError::integrity());
        }
        match (binding.mode(), mutation, observation) {
            (BindingMode::Create, EntityMutation::Create(_), EntityObservation::Absent(_)) => {}
            (
                BindingMode::InitOrMutate,
                EntityMutation::Create(_),
                EntityObservation::Absent(_),
            ) => {}
            (
                BindingMode::InitOrMutate,
                EntityMutation::Replace {
                    expected_version, ..
                },
                EntityObservation::Present(record),
            ) if *expected_version == record.entity_version() => {}
            (
                BindingMode::ObserveOrInitialize,
                EntityMutation::Create(_),
                EntityObservation::Absent(_),
            ) => {}
            (
                BindingMode::ObserveOrInitialize,
                EntityMutation::Replace {
                    expected_version, ..
                },
                EntityObservation::Present(record),
            ) if *expected_version == record.entity_version() => {}
            (
                BindingMode::Mutate,
                EntityMutation::Replace {
                    expected_version, ..
                },
                EntityObservation::Present(record),
            ) if *expected_version == record.entity_version() => {}
            (
                BindingMode::Delete,
                EntityMutation::Delete {
                    expected_version, ..
                },
                EntityObservation::Present(record),
            ) if *expected_version == record.entity_version() => {}
            (BindingMode::Read, _, _)
            | (
                BindingMode::Create
                | BindingMode::Mutate
                | BindingMode::InitOrMutate
                | BindingMode::ObserveOrInitialize
                | BindingMode::Delete,
                _,
                _,
            ) => {
                return Err(CommandValidationError::integrity());
            }
        }
        let entity = resolved
            .bundle()
            .bundle()
            .schema()
            .entity(binding.entity_type())
            .ok_or_else(CommandValidationError::integrity)?;
        validate_post_image_and_project(
            resolved.bundle().bundle().schema(),
            entity,
            target,
            mutation.post_image().fields(),
            binding.mode(),
            observation,
            resolved.reference(),
        )?;
        mutation_index_by_binding[slot] = Some(mutation_index);
    }
    for (child_target, child_mutation_index, child_record) in cascade_mutations {
        let mut parent_match = None;
        for (slot, ((plan_index, parent_target), _parent_observation)) in facts
            .binding_plan_indices()
            .iter()
            .zip(request.binding_targets())
            .zip(current.bindings())
            .enumerate()
        {
            let binding = plan
                .bindings()
                .get(*plan_index as usize)
                .ok_or_else(CommandValidationError::integrity)?;
            if binding.mode() != BindingMode::Delete {
                continue;
            }
            let check = plan
                .delete_checks()
                .iter()
                .find(|check| check.binding() == binding.id())
                .ok_or_else(CommandValidationError::integrity)?;
            let DeleteCheckModeV1::Cascade { relationships } = check.mode() else {
                continue;
            };
            let parent_values = binding
                .key_schema()
                .decode_entity(parent_target.key())
                .map_err(|_| CommandValidationError::integrity())?;
            for specification in relationships {
                if specification.source_entity() != child_target.entity_type_id() {
                    continue;
                }
                let relationship = resolved
                    .bundle()
                    .bundle()
                    .schema()
                    .relationships()
                    .iter()
                    .find(|relationship| {
                        relationship.source_entity() == specification.source_entity()
                            && relationship.name() == specification.relationship_name()
                            && relationship.target_entity() == binding.entity_type()
                    })
                    .ok_or_else(CommandValidationError::integrity)?;
                let source_values = relationship
                    .source_fields()
                    .iter()
                    .map(|field| {
                        record_field(child_record.fields(), *field)
                            .cloned()
                            .ok_or_else(CommandValidationError::integrity)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if source_values == parent_values {
                    let parent_mutation_index = mutation_index_by_binding
                        .get(slot)
                        .copied()
                        .flatten()
                        .ok_or_else(CommandValidationError::integrity)?;
                    if child_mutation_index >= parent_mutation_index || parent_match.is_some() {
                        return Err(CommandValidationError::integrity());
                    }
                    parent_match = Some(parent_mutation_index);
                }
            }
        }
        if parent_match.is_none() {
            return Err(CommandValidationError::integrity());
        }
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
    collection_element: Option<CanonicalValue>,
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

    fn collection_element(&self) -> Option<CanonicalValue> {
        self.collection_element.clone()
    }

    fn collection_element_field(&self, field: FieldId) -> Option<CanonicalValue> {
        let CanonicalValue::Record(record) = self.collection_element.as_ref()? else {
            return None;
        };
        record_field(record, field).cloned()
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
    element_ordinal: Option<u16>,
) -> Result<TransactionCurrentValues, CommandValidationError> {
    let plan = resolved.plan();
    let schema = resolved.bundle().bundle().schema();
    let request = evaluated.validation_request();
    let facts = derive_input_command_facts(plan, normalized_input.clone())
        .map_err(|_| CommandValidationError::integrity())?;
    let mut bindings = (0..plan.bindings().len())
        .map(|_| None)
        .collect::<Vec<Option<PositionedBindingRecord>>>();
    for (slot, (((plan_index, ordinal), target), observation)) in facts
        .binding_plan_indices()
        .iter()
        .zip(facts.binding_element_ordinals())
        .zip(request.binding_targets())
        .zip(current.bindings())
        .enumerate()
    {
        if *ordinal != element_ordinal && ordinal.is_some() {
            continue;
        }
        let binding = plan
            .bindings()
            .get(*plan_index as usize)
            .ok_or_else(CommandValidationError::integrity)?;
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
                    let positioned = PositionedBindingRecord {
                        id: binding.id(),
                        record: projected,
                    };
                    let position = bindings
                        .get_mut(*plan_index as usize)
                        .ok_or_else(CommandValidationError::integrity)?;
                    if position.replace(positioned).is_some() {
                        return Err(CommandValidationError::integrity());
                    }
                    continue;
                }
                EntityObservation::Absent(_) => return Err(CommandValidationError::integrity()),
            },
            BindingMode::Mutate
            | BindingMode::Create
            | BindingMode::InitOrMutate
            | BindingMode::Delete => {
                let mutation_index = coverage
                    .get(slot)
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
            BindingMode::ObserveOrInitialize => {
                if let Some(mutation_index) = coverage.get(slot).copied().flatten() {
                    evaluated
                        .mutations()
                        .get(mutation_index)
                        .filter(|mutation| mutation.target() == target)
                        .ok_or_else(CommandValidationError::integrity)?
                        .post_image()
                        .fields()
                } else {
                    match observation {
                        EntityObservation::Present(record) => record.fields(),
                        EntityObservation::Absent(_) => {
                            return Err(CommandValidationError::integrity());
                        }
                    }
                }
            }
        };
        let positioned = PositionedBindingRecord {
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
        };
        let position = bindings
            .get_mut(*plan_index as usize)
            .ok_or_else(CommandValidationError::integrity)?;
        if position.replace(positioned).is_some() {
            return Err(CommandValidationError::integrity());
        }
    }
    let bindings = bindings
        .into_iter()
        .map(|binding| binding.ok_or_else(CommandValidationError::integrity))
        .collect::<Result<Vec<_>, _>>()?;

    let mut roots = (0..plan.root_validation_reads().len())
        .map(|_| None)
        .collect::<Vec<Option<PositionedRootRecord>>>();
    for (((plan_index, ordinal), target), observation) in facts
        .root_validation_plan_indices()
        .iter()
        .zip(facts.root_validation_element_ordinals())
        .zip(request.root_validation_targets())
        .zip(current.root_validations())
    {
        if *ordinal != element_ordinal && ordinal.is_some() {
            continue;
        }
        let read = plan
            .root_validation_reads()
            .get(*plan_index as usize)
            .ok_or_else(CommandValidationError::integrity)?;
        let entity = schema
            .entity(read.entity_type())
            .ok_or_else(CommandValidationError::integrity)?;
        if read.key_schema() != entity.primary_key() {
            return Err(CommandValidationError::integrity());
        }
        let EntityObservation::Present(record) = observation else {
            return Err(CommandValidationError::integrity());
        };
        let positioned = PositionedRootRecord {
            id: read.id(),
            record: materialize_current_entity_record(
                schema,
                entity,
                target,
                record,
                resolved.reference(),
            )?,
        };
        let position = roots
            .get_mut(*plan_index as usize)
            .ok_or_else(CommandValidationError::integrity)?;
        if position.replace(positioned).is_some() {
            return Err(CommandValidationError::integrity());
        }
    }
    let roots = roots
        .into_iter()
        .map(|root| root.ok_or_else(CommandValidationError::integrity))
        .collect::<Result<Vec<_>, _>>()?;

    let collection_element = match (plan.collection_expansion(), element_ordinal) {
        (Some(expansion), Some(ordinal)) => {
            let CanonicalValue::List(elements) =
                record_field(normalized_input, expansion.input_field())
                    .ok_or_else(CommandValidationError::integrity)?
            else {
                return Err(CommandValidationError::integrity());
            };
            Some(
                elements
                    .values()
                    .get(ordinal as usize)
                    .cloned()
                    .ok_or_else(CommandValidationError::integrity)?,
            )
        }
        (None, None) => None,
        _ => return Err(CommandValidationError::integrity()),
    };

    Ok(TransactionCurrentValues {
        input: normalized_input.clone(),
        collection_element,
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
        (BindingMode::InitOrMutate, EntityObservation::Absent(_)) if post_unknown.is_empty() => {}
        (BindingMode::ObserveOrInitialize, EntityObservation::Absent(_))
            if post_unknown.is_empty() => {}
        (BindingMode::InitOrMutate, EntityObservation::Present(record))
        | (BindingMode::ObserveOrInitialize, EntityObservation::Present(record))
        | (BindingMode::Mutate, EntityObservation::Present(record)) => {
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
        (BindingMode::Delete, EntityObservation::Present(record)) => {
            let current = materialize_current_entity_record(schema, entity, target, record, plan)?;
            if post_image != &current {
                return Err(CommandValidationError::integrity());
            }
        }
        (
            BindingMode::Read
            | BindingMode::Create
            | BindingMode::Mutate
            | BindingMode::InitOrMutate
            | BindingMode::ObserveOrInitialize
            | BindingMode::Delete,
            _,
        ) => {
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
        ActorId, ActorKind, AdmittedActorContext, AggregateTypeId, CanonicalList,
        ContractBundleHash, EntityVersion, IndexId, PartitionKeyBuilder, RequestId, TenantScope,
        Timestamp,
    };

    use super::*;

    const BULK_TUPLE_SOURCE: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/contracts/bulk/openfga-tuples.riff"
    ));
    const BULK_INVARIANT_SOURCE: &str = r#"
contract BulkInvariant version 1 {
  entity Row {
    key (tenant_id: uuid, row_id: uuid)
    field value: i64
    invariant non_negative: value >= 0
  }
  aggregate Rows {
    root Row
    partition_by tenant_id
    conflict_key (tenant_id, row_id)
  }
  bulk command PutRows {
    input request_id: uuid
    input rows: list<Row, 1..8>
    idempotency_key request_id
    for row in rows {
      create Row(row.tenant_id, row.row_id) as stored else Exists {}
      set stored.value = row.value
    }
    return Written {}
  }
}
"#;
    const BULK_EVENT_SOURCE: &str = r#"
contract BulkEvent version 1 {
  entity Row {
    key (tenant_id: uuid, row_id: uuid)
    field value: i64
  }
  event RowWritten { row_id: uuid value: i64 }
  aggregate Rows {
    root Row
    partition_by tenant_id
    conflict_key (tenant_id, row_id)
  }
  bulk command PutRows {
    input request_id: uuid
    input rows: list<Row, 1..8>
    idempotency_key request_id
    for row in rows {
      create Row(row.tenant_id, row.row_id) as stored else Exists {}
      set stored.value = row.value
      emit RowWritten { row_id: row.row_id, value: stored.value }
    }
    return Written {}
  }
}
"#;
    const BULK_DELETE_SOURCE: &str = r#"
contract BulkDeleteValidation version 1 {
  entity Row {
    key (tenant_id: uuid, row_id: uuid)
    delete_policy no_inbound
  }
  aggregate Rows { root Row partition_by tenant_id conflict_key (tenant_id, row_id) }
  bulk command DeleteRows {
    input request_id: uuid
    input tenant_id: uuid
    input row_ids: list<uuid, 1..8>
    idempotency_key request_id
    for row_id in row_ids {
      delete Row(tenant_id, row_id) as row else Missing {}
    }
    return Deleted {}
  }
}
"#;
    const UNARY_DELETE_SOURCE: &str = r#"
contract UnaryDeleteValidation version 1 {
  entity Row {
    key (tenant_id: uuid, row_id: uuid)
    field value: i64
    delete_policy no_inbound
  }
  aggregate Rows { root Row partition_by tenant_id conflict_key (tenant_id, row_id) }
  command ConsumeRow {
    input request_id: uuid
    input tenant_id: uuid
    input row_id: uuid
    idempotency_key request_id
    delete Row(tenant_id, row_id) as row else Missing {}
    return Consumed { value: row.value }
  }
}
"#;

    #[test]
    fn collection_mutations_cover_every_concrete_slot_before_commit() {
        let compiled = compile_contract_source(BULK_TUPLE_SOURCE).expect("bulk fixture compiles");
        let tuple = compiled
            .schema()
            .entities()
            .iter()
            .find(|entity| entity.name() == "Tuple")
            .expect("tuple entity");
        let tuple_value = |id: u8, object: &str| {
            CanonicalValue::Record(named_record(
                tuple.record(),
                &[
                    ("store_id", CanonicalValue::Uuid([0x31; 16])),
                    ("tuple_id", CanonicalValue::Uuid([id; 16])),
                    ("object", CanonicalValue::string(object).expect("object")),
                    (
                        "relation",
                        CanonicalValue::string("reader").expect("relation"),
                    ),
                    (
                        "subject",
                        CanonicalValue::string("user:alice").expect("subject"),
                    ),
                ],
                &[],
            ))
        };
        let prepared = prepare(
            BULK_TUPLE_SOURCE,
            "WriteTuples",
            &[
                ("request_id", CanonicalValue::Uuid([0x21; 16])),
                (
                    "tuples",
                    CanonicalValue::List(
                        CanonicalList::new(vec![
                            tuple_value(0x41, "document:first"),
                            tuple_value(0x42, "document:second"),
                        ])
                        .expect("tuple list"),
                    ),
                ),
            ],
        );
        let bindings = prepared
            .binding_targets
            .iter()
            .cloned()
            .map(EntityObservation::Absent)
            .collect();
        let fixture = evaluate(prepared, bindings, vec![]);

        let CheckedCommandDecision::NonZero(positions) =
            validate_transaction_current_command_parts(
                &fixture.prepared.resolved,
                &fixture.prepared.input,
                &derive_input_command_facts(fixture.prepared.resolved.plan(), fixture.prepared.input.clone())
                    .expect("fixture input facts"),
                fixture.prepared.logical_time,
                &fixture.evaluated,
                &fixture.current,
            )
            .expect("collection validates")
        else {
            panic!("collection must retain one nonzero atomic graph");
        };
        assert_eq!(positions.as_ref(), &[Some(0), Some(1)]);
    }

    #[test]
    fn collection_commit_checks_revalidate_each_submitted_element() {
        let compiled = compile_contract_source(BULK_INVARIANT_SOURCE).expect("bulk compiles");
        let row = compiled.schema().entities().first().expect("row entity");
        let row_value = |id: u8, value: i64| {
            CanonicalValue::Record(named_record(
                row.record(),
                &[
                    ("tenant_id", CanonicalValue::Uuid([0x51; 16])),
                    ("row_id", CanonicalValue::Uuid([id; 16])),
                    ("value", CanonicalValue::I64(value)),
                ],
                &[],
            ))
        };
        let prepared = prepare(
            BULK_INVARIANT_SOURCE,
            "PutRows",
            &[
                ("request_id", CanonicalValue::Uuid([0x52; 16])),
                (
                    "rows",
                    CanonicalValue::List(
                        CanonicalList::new(vec![row_value(0x61, 1), row_value(0x62, -2)])
                            .expect("rows"),
                    ),
                ),
            ],
        );
        assert!(!prepared.plan().commit_checks().is_empty());
        let bindings = prepared
            .binding_targets
            .iter()
            .cloned()
            .map(EntityObservation::Absent)
            .collect();
        let fixture = evaluate(prepared, bindings, vec![]);

        assert!(matches!(
            validate_transaction_current_command_parts(
                &fixture.prepared.resolved,
                &fixture.prepared.input,
                &derive_input_command_facts(fixture.prepared.resolved.plan(), fixture.prepared.input.clone())
                    .expect("fixture input facts"),
                fixture.prepared.logical_time,
                &fixture.evaluated,
                &fixture.current,
            ),
            Ok(CheckedCommandDecision::Rejected(
                CandidateValidationRejection::CommitCheckRejected
            ))
        ));
    }

    #[test]
    fn collection_events_validate_in_submitted_element_then_instruction_order() {
        let compiled = compile_contract_source(BULK_EVENT_SOURCE).expect("bulk compiles");
        let row = compiled.schema().entities().first().expect("row entity");
        let row_value = |id: u8, value: i64| {
            CanonicalValue::Record(named_record(
                row.record(),
                &[
                    ("tenant_id", CanonicalValue::Uuid([0x71; 16])),
                    ("row_id", CanonicalValue::Uuid([id; 16])),
                    ("value", CanonicalValue::I64(value)),
                ],
                &[],
            ))
        };
        let prepared = prepare(
            BULK_EVENT_SOURCE,
            "PutRows",
            &[
                ("request_id", CanonicalValue::Uuid([0x72; 16])),
                (
                    "rows",
                    CanonicalValue::List(
                        CanonicalList::new(vec![row_value(0x73, 1), row_value(0x74, 2)])
                            .expect("rows"),
                    ),
                ),
            ],
        );
        let bindings = prepared
            .binding_targets
            .iter()
            .cloned()
            .map(EntityObservation::Absent)
            .collect();
        let fixture = evaluate(prepared, bindings, vec![]);
        assert_eq!(fixture.evaluated.event_intents().len(), 2);

        assert!(matches!(
            validate_transaction_current_command_parts(
                &fixture.prepared.resolved,
                &fixture.prepared.input,
                &derive_input_command_facts(fixture.prepared.resolved.plan(), fixture.prepared.input.clone())
                    .expect("fixture input facts"),
                fixture.prepared.logical_time,
                &fixture.evaluated,
                &fixture.current,
            ),
            Ok(CheckedCommandDecision::NonZero(_))
        ));
    }

    #[test]
    fn collection_delete_validation_requires_each_exact_current_predecessor() {
        let prepared = prepare(
            BULK_DELETE_SOURCE,
            "DeleteRows",
            &[
                ("request_id", CanonicalValue::Uuid([0x91; 16])),
                ("tenant_id", CanonicalValue::Uuid([0x92; 16])),
                (
                    "row_ids",
                    CanonicalValue::List(
                        CanonicalList::new(vec![
                            CanonicalValue::Uuid([0x93; 16]),
                            CanonicalValue::Uuid([0x94; 16]),
                        ])
                        .expect("row IDs"),
                    ),
                ),
            ],
        );
        let bindings = (0..prepared.binding_targets.len())
            .map(|index| prepared.present_binding(index, EntityVersion::first(), &[], vec![], &[]))
            .collect();
        let fixture = evaluate(prepared, bindings, vec![]);
        assert!(
            fixture
                .evaluated
                .mutations()
                .iter()
                .all(EntityMutation::is_delete)
        );
        assert!(matches!(
            validate_transaction_current_command_parts(
                &fixture.prepared.resolved,
                &fixture.prepared.input,
                &derive_input_command_facts(fixture.prepared.resolved.plan(), fixture.prepared.input.clone())
                    .expect("fixture input facts"),
                fixture.prepared.logical_time,
                &fixture.evaluated,
                &fixture.current,
            ),
            Ok(CheckedCommandDecision::NonZero(_))
        ));
    }

    #[test]
    fn unary_delete_validation_binds_outcome_and_mutation_to_one_current_predecessor() {
        let prepared = prepare(
            UNARY_DELETE_SOURCE,
            "ConsumeRow",
            &[
                ("request_id", CanonicalValue::Uuid([0xa1; 16])),
                ("tenant_id", CanonicalValue::Uuid([0xa2; 16])),
                ("row_id", CanonicalValue::Uuid([0xa3; 16])),
            ],
        );
        let present = prepared.present_binding(
            0,
            first_version(),
            &[("value", CanonicalValue::I64(7))],
            Vec::new(),
            &[],
        );
        let fixture = evaluate(prepared, vec![present], Vec::new());
        assert_eq!(fixture.evaluated.mutations().len(), 1);
        assert!(fixture.evaluated.mutations()[0].is_delete());
        assert!(matches!(
            validation(&fixture),
            Ok(CheckedCommandDecision::NonZero(_))
        ));

        let updated = fixture.prepared.present_binding(
            0,
            next_version(),
            &[("value", CanonicalValue::I64(9))],
            Vec::new(),
            &[],
        );
        let changed_current = TransactionCurrentState::new(
            fixture.evaluated.validation_request(),
            vec![updated],
            Vec::new(),
            Vec::new(),
        )
        .expect("changed current state");
        assert_ne!(
            dependencies_from_current(&changed_current).expect("current dependencies"),
            *fixture.evaluated.read_dependencies(),
            "the stale outcome must be discarded before semantic validation",
        );
    }

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
        let binding_targets = facts
            .binding_plan_indices()
            .iter()
            .zip(facts.binding_entity_keys())
            .map(|(index, key)| {
                EntityTarget::new(plan.bindings()[*index as usize].entity_type(), key.clone())
            })
            .collect::<Result<Vec<_>, _>>()
            .expect("binding targets");
        let root_targets = facts
            .root_validation_plan_indices()
            .iter()
            .zip(facts.root_validation_entity_keys())
            .map(|(index, key)| {
                EntityTarget::new(
                    plan.root_validation_reads()[*index as usize].entity_type(),
                    key.clone(),
                )
            })
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
            &derive_input_command_facts(fixture.prepared.resolved.plan(), fixture.prepared.input.clone())
                .expect("fixture input facts"),
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
            EntityMutation::Delete {
                expected_version, ..
            } => EntityMutation::Delete {
                expected_version: *expected_version,
                prior_image: post_image,
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
        assert_ne!(
            dependencies_from_current(&changed_current).expect("changed dependencies"),
            *fixture.evaluated.read_dependencies()
        );
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
            &derive_input_command_facts(read_only.resolved.plan(), read_only.input.clone())
                .expect("fixture input facts"),
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
        let partition = PartitionKeyBuilder::new(AggregateTypeId::first())
            .finish()
            .expect("partition");
        let range = IndexRangeTarget::new(
            partition,
            IndexRangePrefixBuilder::new(IndexId::first()).finish(),
        );
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
            &derive_input_command_facts(ordinary.prepared.resolved.plan(), ordinary.prepared.input.clone())
                .expect("fixture input facts"),
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
            &derive_input_command_facts(fixture.prepared.resolved.plan(), fixture.prepared.input.clone())
                .expect("fixture input facts"),
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
            &derive_input_command_facts(fixture.prepared.resolved.plan(), fixture.prepared.input.clone())
                .expect("fixture input facts"),
            fixture.prepared.logical_time,
            &fixture.evaluated,
            &malformed_current,
        ));

        let coverage = prove_mutation_coverage(
            &fixture.prepared.resolved,
            &derive_input_command_facts(fixture.prepared.resolved.plan(), fixture.prepared.input.clone())
                .expect("fixture input facts"),
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
            None,
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
            &derive_input_command_facts(
                fixture.prepared.resolved.plan(),
                fixture.prepared.input.clone(),
            )
            .expect("fixture input facts"),
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
            EntityMutation::Create(_) | EntityMutation::Delete { .. } => {
                panic!("fixture must replace")
            }
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
        assert!(
            dependencies_from_current(&fixture.current).is_ok_and(|dependencies| {
                dependencies != *fixture.evaluated.read_dependencies()
            })
        );
    }
}
