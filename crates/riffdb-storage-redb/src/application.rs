//! Redb application admission and atomic command transactions.

use std::num::NonZeroU8;
use std::ops::Bound::{Excluded, Included};

use redb::ReadableTable;
use riffdb_storage_api::{
    AbandonedCandidate, AdmissionLookupResultV1, AdmissionRepository, AdmissionRequestV1,
    AdmissionResultV1, AffectedEpochCurrentState, AffectedEpochCurrentStateBuilder,
    AffectedIndexEpochTargets, ApplicationCommandTransactionPort, ApplicationSequenceAllocator,
    AssignedCommandSequence, AtomicCommandRecordSet, AuditedCommittedBatchV1,
    AuditedExecutionFailureV1, CandidateAdmissionResult, CandidateCapacityResult,
    CandidateStartResult, CandidateValidationRejection, CommandAdmissionExpectationV1,
    CommandCandidateAdmission, CommandCandidateAffectedEpochRead, CommandCandidateAwaitingCapacity,
    CommandCandidateAwaitingValidation, CommandCandidateCapacityReserved,
    CommandCandidateSequenceAssigned, CommandCandidateStateRead, CommandWriteSetPlanV1,
    CommitIntent, CommittedBatchV1, CurrentIndexGenerationObservation, CurrentRangeObservation,
    DurabilityMode, EmptyCommandBatch, EntityObservation, EntityTarget,
    ExecutionFailureAdmissionRechecked, ExecutionFailureAdmissionResult,
    ExecutionFailureAwaitingDecision, ExecutionFailureTransitionPort,
    ExecutionFailureTransitionRequestV1, ExpectedEntityState, IdempotencyIdentity,
    IdempotencyIdentityKey, IdempotencyLookupCandidatesV1, IndexEntryMutationV1,
    IndexEpochPosition, NonEmptyCommandBatch, PartitionIndexTarget, ProvenanceIdCollision,
    ReadDependencies, ReadDependency, StagedBatchMetrics, StorageError, StorageErrorKind,
    StorageValueError, StoredAdmissionStateV1, StoredExecutionFailedV1, StoredPendingAdmissionV1,
    TransactionCurrentState, TransactionCurrentStateBuilder, UniqueIndexOccupancy,
    UniqueOccupancyKind, ValidationReadRequest, encode_atomic_command_record_set_v1,
};
use riffdb_types::ProvenanceId;

use crate::administration::stage_service_audit_group_in_write;
use crate::codec::{
    IdempotencyRecordV1, decode_application_sequence_allocator_v1, decode_entity_record_v1,
    decode_idempotency_record_v1, decode_index_epoch_v1, decode_pending_admission_v1,
    encode_execution_failed_v1, encode_pending_admission_v1,
};
use crate::error::{codec_error, precommit_storage_error, table_error};
use crate::hooks::RedbTestOperation;
use crate::keys::{
    encode_application_sequence_key, encode_contract_bundle_key, encode_entity_key,
    encode_event_key, encode_event_route_key, encode_idempotency_key, encode_index_entry_key,
    encode_partition_index_key, encode_provenance_key,
};
use crate::layout::{
    COMMITS, CONTRACT_BUNDLES, ENTITIES, EVENT_ROUTES, EVENTS, IDEMPOTENCY, IDEMPOTENCY_PENDING,
    INDEX_EPOCHS, META, META_APPLICATION_SEQUENCE, OUTBOX, PROVENANCE, SECONDARY_INDEXES,
};
use crate::store::{RedbOperationalPorts, RedbWriteAccess};
use crate::transient::TransientIndexDelta;

struct BatchCore {
    access: RedbWriteAccess,
    allocator: ApplicationSequenceAllocator,
    staged: Vec<AtomicCommandRecordSet>,
    metrics: Option<StagedBatchMetrics>,
}

impl BatchCore {
    fn open(ports: &RedbOperationalPorts) -> Result<Self, StorageError> {
        let access = ports.begin_write()?;
        let allocator = read_application_allocator(access.transaction()?)?;
        Ok(Self {
            access,
            allocator,
            staged: Vec::new(),
            metrics: None,
        })
    }
}

/// Empty redb command batch. This state has no commit operation.
pub struct RedbEmptyBatch {
    core: BatchCore,
}

/// Nonempty redb command batch holding one uncommitted engine transaction.
pub struct RedbNonEmptyBatch {
    core: BatchCore,
}

/// Candidate before exact pending-admission recheck.
pub struct RedbCandidateAdmission<P> {
    prior: P,
    intent: Box<CommitIntent>,
}

/// Candidate permitted to read transaction-current values.
pub struct RedbCandidateStateRead<P> {
    prior: P,
    intent: Box<CommitIntent>,
}

/// Candidate awaiting the coordinator's validation decision.
pub struct RedbCandidateAwaitingValidation<P> {
    prior: P,
    intent: Box<CommitIntent>,
}

/// Candidate permitted to read mutation-affected epochs.
pub struct RedbCandidateAffectedEpochRead<P> {
    prior: P,
    intent: Box<CommitIntent>,
    affected_targets: AffectedIndexEpochTargets,
}

/// Candidate holding complete transaction-current observations.
pub struct RedbCandidateAwaitingCapacity<P> {
    prior: P,
    intent: Box<CommitIntent>,
    affected_targets: AffectedIndexEpochTargets,
    affected_current: AffectedEpochCurrentState,
}

/// Candidate with capacity and provenance uniqueness reserved.
pub struct RedbCandidateCapacityReserved<P> {
    prior: P,
    intent: Box<CommitIntent>,
    write_plan: CommandWriteSetPlanV1,
}

/// Candidate owning a transaction-local, still-invisible sequence.
pub struct RedbCandidateSequenceAssigned<P> {
    prior: P,
    intent: Box<CommitIntent>,
    write_plan: CommandWriteSetPlanV1,
    assignment: AssignedCommandSequence,
}

/// Rechecked short pending-to-failure transition.
pub struct RedbExecutionFailureRechecked {
    access: RedbWriteAccess,
    request: ExecutionFailureTransitionRequestV1,
}

/// Pending-to-failure transition after current reads.
pub struct RedbExecutionFailureAwaitingDecision {
    access: RedbWriteAccess,
    request: ExecutionFailureTransitionRequestV1,
    current: TransactionCurrentState,
}

impl ApplicationCommandTransactionPort for RedbOperationalPorts {
    type EmptyBatch = RedbEmptyBatch;

    fn begin_empty_batch(&self) -> Result<Self::EmptyBatch, StorageError> {
        Ok(RedbEmptyBatch {
            core: BatchCore::open(self)?,
        })
    }
}

impl EmptyCommandBatch for RedbEmptyBatch {
    type Candidate = RedbCandidateAdmission<Self>;

    fn begin_candidate(self, intent: Box<CommitIntent>) -> Result<Self::Candidate, StorageError> {
        Ok(RedbCandidateAdmission {
            prior: self,
            intent,
        })
    }

    fn rollback(self) {}
}

impl NonEmptyCommandBatch for RedbNonEmptyBatch {
    type Candidate = RedbCandidateAdmission<Self>;

    fn metrics(&self) -> StagedBatchMetrics {
        self.core
            .metrics
            .expect("nonempty batch construction installs metrics")
    }

    fn begin_candidate(
        self,
        intent: Box<CommitIntent>,
    ) -> Result<CandidateStartResult<Self, Self::Candidate>, StorageError> {
        if usize::from(self.metrics().command_count().get())
            == riffdb_storage_api::MAX_STAGED_COMMANDS
        {
            return Ok(CandidateStartResult::BatchFull {
                prior: self,
                intent,
            });
        }
        Ok(CandidateStartResult::Started(RedbCandidateAdmission {
            prior: self,
            intent,
        }))
    }

    fn commit(self, durability: DurabilityMode) -> Result<CommittedBatchV1, StorageError> {
        if durability == DurabilityMode::Memory
            || self
                .core
                .staged
                .iter()
                .any(|records| !records.matches_durability_mode(durability))
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let outcomes = self
            .core
            .staged
            .iter()
            .map(|records| records.stored_outcome().clone())
            .collect();
        let committed = CommittedBatchV1::new(outcomes, durability).map_err(invariant_value)?;
        let pending_events = self
            .core
            .staged
            .iter()
            .flat_map(|records| records.events().iter().map(|event| event.event_id()))
            .collect::<Vec<_>>();
        let delta = (!pending_events.is_empty())
            .then_some(TransientIndexDelta::PendingOutboxInserted(pending_events));
        stage_application_allocator(self.core.access.transaction()?, self.core.allocator)?;
        self.core
            .access
            .commit_for_with_delta(RedbTestOperation::CommandBatch, delta)?;
        Ok(committed)
    }

    fn commit_with_service_audit_transitions(
        self,
        durability: DurabilityMode,
        transitions: Vec<riffdb_storage_api::CommandServiceAuditTransitionV1>,
    ) -> Result<AuditedCommittedBatchV1, StorageError> {
        if durability == DurabilityMode::Memory
            || self.core.staged.len() != transitions.len()
            || transitions.is_empty()
            || self
                .core
                .staged
                .iter()
                .any(|records| !records.matches_durability_mode(durability))
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let outcomes = self
            .core
            .staged
            .iter()
            .map(|records| records.stored_outcome().clone())
            .collect();
        let committed = CommittedBatchV1::new(outcomes, durability).map_err(invariant_value)?;
        let mut terminal_positions = Vec::with_capacity(transitions.len());
        let mut intents = Vec::with_capacity(transitions.len().saturating_mul(2));
        for transition in transitions {
            let rows = transition.into_intents();
            intents.extend(rows);
            terminal_positions.push(intents.len() - 1);
        }
        let records = stage_service_audit_group_in_write(&self.core.access, &intents)?;
        let terminal_records = terminal_positions
            .into_iter()
            .map(|index| records[index].clone())
            .collect::<Vec<_>>();
        let audited = AuditedCommittedBatchV1::new(committed, terminal_records.clone())
            .map_err(invariant_value)?;
        let pending_events = self
            .core
            .staged
            .iter()
            .flat_map(|records| records.events().iter().map(|event| event.event_id()))
            .collect::<Vec<_>>();
        // AUDIT_BY_REQUEST is written durably inside stage_service_audit_group_in_write;
        // only outbox accelerators remain in the transient delta path.
        let delta = if pending_events.is_empty() {
            None
        } else {
            Some(TransientIndexDelta::PendingOutboxInserted(pending_events))
        };
        stage_application_allocator(self.core.access.transaction()?, self.core.allocator)?;
        self.core
            .access
            .commit_for_with_delta(RedbTestOperation::CommandBatch, delta)?;
        Ok(audited)
    }

    fn rollback(self) {}
}

impl AdmissionRepository for RedbOperationalPorts {
    fn admit_or_resolve(
        &self,
        request: AdmissionRequestV1,
    ) -> Result<AdmissionResultV1, StorageError> {
        let mut results = self.admit_or_resolve_group(vec![request])?;
        results
            .pop()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))
    }

    fn admit_or_resolve_group(
        &self,
        requests: Vec<AdmissionRequestV1>,
    ) -> Result<Vec<AdmissionResultV1>, StorageError> {
        if requests.is_empty() || requests.len() > riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS
        {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let access = self.begin_write()?;
        let transaction = access.transaction()?;
        let mut created_any = false;
        let mut results = Vec::with_capacity(requests.len());
        for request in requests {
            let (result, created) = stage_admission(transaction, &request)?;
            created_any |= created;
            results.push(result);
        }
        if created_any {
            access.commit_for(RedbTestOperation::Admission)?;
        } else {
            access.abort()?;
        }
        Ok(results)
    }

    fn lookup_admission(
        &self,
        candidates: IdempotencyLookupCandidatesV1,
    ) -> Result<AdmissionLookupResultV1, StorageError> {
        let mut results = self.lookup_admission_group(vec![candidates])?;
        results
            .pop()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))
    }

    fn lookup_admission_group(
        &self,
        candidates: Vec<IdempotencyLookupCandidatesV1>,
    ) -> Result<Vec<AdmissionLookupResultV1>, StorageError> {
        if candidates.is_empty()
            || candidates.len() > riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS
        {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let transaction = self.begin_read()?;
        candidates
            .iter()
            .map(
                |candidate| match matching_admissions(&transaction, candidate)?.as_slice() {
                    [] => Ok(AdmissionLookupResultV1::NotFound),
                    [value] => Ok(AdmissionLookupResultV1::Found(Box::new(value.clone()))),
                    [_, ..] => Ok(AdmissionLookupResultV1::MultipleMatches),
                },
            )
            .collect()
    }
}

pub(crate) fn stage_admission(
    transaction: &redb::WriteTransaction,
    request: &AdmissionRequestV1,
) -> Result<(AdmissionResultV1, bool), StorageError> {
    let matches = matching_admissions(transaction, request.lookup_candidates())?;
    if matches.len() > 1 {
        return Ok((AdmissionResultV1::MultipleMatches, false));
    }
    if let Some(existing) = matches.first() {
        return Ok((
            admission_result(existing, request.proposed_pending()),
            false,
        ));
    }
    if !plan_bundle_exists(transaction, request.proposed_pending().plan())? {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    let key = identity_key(request.proposed_pending().identity())?;
    let encoded = encode_pending_admission_v1(request.proposed_pending())?;
    let mut table = transaction
        .open_table(IDEMPOTENCY_PENDING)
        .map_err(table_error)?;
    if table
        .insert(encode_idempotency_key(&key), encoded.as_bytes())
        .map_err(precommit_storage_error)?
        .is_some()
    {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    drop(table);
    Ok((
        AdmissionResultV1::Created(request.proposed_pending().clone()),
        true,
    ))
}

impl ExecutionFailureTransitionPort for RedbOperationalPorts {
    type Rechecked = RedbExecutionFailureRechecked;

    fn begin_execution_failure(
        &self,
        request: ExecutionFailureTransitionRequestV1,
    ) -> Result<ExecutionFailureAdmissionResult<Self::Rechecked>, StorageError> {
        let access = self.begin_write()?;
        let state = match request.admission_expectation() {
            CommandAdmissionExpectationV1::ExistingPending => {
                read_admission(access.transaction()?, request.expected_pending().identity())?
            }
            CommandAdmissionExpectationV1::Vacant(candidates) => {
                let matches = matching_admissions(access.transaction()?, candidates)?;
                if matches.len() > 1 {
                    return Ok(ExecutionFailureAdmissionResult::PendingMismatch);
                }
                matches.into_iter().next()
            }
        };
        Ok(match state {
            None if matches!(
                request.admission_expectation(),
                CommandAdmissionExpectationV1::Vacant(_)
            ) =>
            {
                ExecutionFailureAdmissionResult::Rechecked(RedbExecutionFailureRechecked {
                    access,
                    request,
                })
            }
            None => ExecutionFailureAdmissionResult::Missing,
            Some(StoredAdmissionStateV1::Pending(value))
                if value == *request.expected_pending() =>
            {
                ExecutionFailureAdmissionResult::Rechecked(RedbExecutionFailureRechecked {
                    access,
                    request,
                })
            }
            Some(StoredAdmissionStateV1::Pending(_)) => {
                ExecutionFailureAdmissionResult::PendingMismatch
            }
            Some(StoredAdmissionStateV1::StoredOutcome(value)) => {
                ExecutionFailureAdmissionResult::StoredOutcome(value)
            }
            Some(StoredAdmissionStateV1::ExecutionFailed(value)) => {
                ExecutionFailureAdmissionResult::ExecutionFailed(value)
            }
        })
    }
}

impl ExecutionFailureAdmissionRechecked for RedbExecutionFailureRechecked {
    type AwaitingDecision = RedbExecutionFailureAwaitingDecision;

    fn read_transaction_current(
        self,
    ) -> Result<(Self::AwaitingDecision, TransactionCurrentState), StorageError> {
        let current = current_state(
            self.access.transaction()?,
            self.request.validation_request(),
        )?;
        Ok((
            RedbExecutionFailureAwaitingDecision {
                access: self.access,
                request: self.request,
                current: current.clone(),
            },
            current,
        ))
    }
}

impl ExecutionFailureAwaitingDecision for RedbExecutionFailureAwaitingDecision {
    fn terminalize(self) -> Result<StoredExecutionFailedV1, StorageError> {
        self.terminalize_inner(None).map(|(failure, _)| failure)
    }

    fn terminalize_with_service_audit(
        self,
        transition: riffdb_storage_api::CommandServiceAuditTransitionV1,
    ) -> Result<AuditedExecutionFailureV1, StorageError> {
        let (failure, terminal) = self.terminalize_inner(Some(transition))?;
        AuditedExecutionFailureV1::new(
            failure,
            terminal.ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
        )
        .map_err(invariant_value)
    }

    fn abandon(self) {}
}

impl RedbExecutionFailureAwaitingDecision {
    fn terminalize_inner(
        self,
        audit: Option<riffdb_storage_api::CommandServiceAuditTransitionV1>,
    ) -> Result<
        (
            StoredExecutionFailedV1,
            Option<riffdb_storage_api::StoredServiceAuditRecordV1>,
        ),
        StorageError,
    > {
        if dependencies_from_current(&self.current)? != *self.request.read_dependencies() {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let terminal = self.request.terminal_record();
        let transaction = self.access.transaction()?;
        if !plan_bundle_exists(transaction, self.request.expected_pending().plan())? {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        match self.request.admission_expectation() {
            CommandAdmissionExpectationV1::ExistingPending => {
                if read_admission(transaction, self.request.expected_pending().identity())?
                    != Some(StoredAdmissionStateV1::Pending(
                        self.request.expected_pending().clone(),
                    ))
                {
                    return Err(storage_error(StorageErrorKind::InvariantViolation));
                }
            }
            CommandAdmissionExpectationV1::Vacant(candidates) => {
                if !matching_admissions(transaction, candidates)?.is_empty() {
                    return Err(storage_error(StorageErrorKind::InvariantViolation));
                }
            }
        }
        let key = identity_key(self.request.expected_pending().identity())?;
        let encoded = encode_execution_failed_v1(&terminal)?;
        if matches!(
            self.request.admission_expectation(),
            CommandAdmissionExpectationV1::ExistingPending
        ) {
            let mut pending = transaction
                .open_table(IDEMPOTENCY_PENDING)
                .map_err(table_error)?;
            if pending
                .remove(encode_idempotency_key(&key))
                .map_err(precommit_storage_error)?
                .is_none()
            {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
        }
        {
            let mut outcomes = transaction.open_table(IDEMPOTENCY).map_err(table_error)?;
            if outcomes
                .insert(encode_idempotency_key(&key), encoded.as_bytes())
                .map_err(precommit_storage_error)?
                .is_some()
            {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
        }
        let terminal_audit = if let Some(transition) = audit {
            let intents = transition.into_intents();
            let records = stage_service_audit_group_in_write(&self.access, &intents)?;
            records.last().cloned()
        } else {
            None
        };
        self.access
            .commit_for(RedbTestOperation::ExecutionFailure)?;
        Ok((terminal, terminal_audit))
    }
}

macro_rules! impl_candidate_chain {
    ($prior:ty) => {
        impl CommandCandidateAdmission for RedbCandidateAdmission<$prior> {
            type Prior = $prior;
            type StateRead = RedbCandidateStateRead<$prior>;

            fn recheck_admission(
                self,
            ) -> Result<CandidateAdmissionResult<Self::Prior, Self::StateRead>, StorageError> {
                let state = match self.intent.admission_expectation() {
                    CommandAdmissionExpectationV1::ExistingPending => read_admission(
                        self.prior.core.access.transaction()?,
                        self.intent.pending().identity(),
                    )?,
                    CommandAdmissionExpectationV1::Vacant(candidates) => {
                        let matches =
                            matching_admissions(self.prior.core.access.transaction()?, candidates)?;
                        match matches.as_slice() {
                            [] => {
                                return Ok(CandidateAdmissionResult::Proceed(
                                    RedbCandidateStateRead {
                                        prior: self.prior,
                                        intent: self.intent,
                                    },
                                ));
                            }
                            [state] => Some(state.clone()),
                            [_, ..] => {
                                return Ok(CandidateAdmissionResult::PendingMismatch(
                                    AbandonedCandidate::new(self.prior, self.intent),
                                ));
                            }
                        }
                    }
                };
                Ok(match state {
                    None => CandidateAdmissionResult::MissingPending(AbandonedCandidate::new(
                        self.prior,
                        self.intent,
                    )),
                    Some(StoredAdmissionStateV1::Pending(pending))
                        if pending == *self.intent.pending() =>
                    {
                        CandidateAdmissionResult::Proceed(RedbCandidateStateRead {
                            prior: self.prior,
                            intent: self.intent,
                        })
                    }
                    Some(StoredAdmissionStateV1::Pending(pending))
                        if pending.canonical_input_hash()
                            != self.intent.pending().canonical_input_hash() =>
                    {
                        CandidateAdmissionResult::InputMismatch(AbandonedCandidate::new(
                            self.prior,
                            self.intent,
                        ))
                    }
                    Some(StoredAdmissionStateV1::Pending(_)) => {
                        CandidateAdmissionResult::PendingMismatch(AbandonedCandidate::new(
                            self.prior,
                            self.intent,
                        ))
                    }
                    Some(StoredAdmissionStateV1::StoredOutcome(outcome))
                        if outcome.canonical_input_hash()
                            == self.intent.pending().canonical_input_hash() =>
                    {
                        CandidateAdmissionResult::StoredOutcome {
                            prior: self.prior,
                            outcome,
                        }
                    }
                    Some(StoredAdmissionStateV1::ExecutionFailed(failure))
                        if failure.pending().canonical_input_hash()
                            == self.intent.pending().canonical_input_hash() =>
                    {
                        CandidateAdmissionResult::ExecutionFailed {
                            prior: self.prior,
                            failure,
                        }
                    }
                    Some(
                        StoredAdmissionStateV1::StoredOutcome(_)
                        | StoredAdmissionStateV1::ExecutionFailed(_),
                    ) => CandidateAdmissionResult::InputMismatch(AbandonedCandidate::new(
                        self.prior,
                        self.intent,
                    )),
                })
            }
        }

        impl CommandCandidateStateRead for RedbCandidateStateRead<$prior> {
            type Prior = $prior;
            type AwaitingValidation = RedbCandidateAwaitingValidation<$prior>;

            fn read_transaction_current(
                self,
            ) -> Result<(Self::AwaitingValidation, TransactionCurrentState), StorageError> {
                let current = current_state(
                    self.prior.core.access.transaction()?,
                    self.intent.evaluated().validation_request(),
                )?;
                Ok((
                    RedbCandidateAwaitingValidation {
                        prior: self.prior,
                        intent: self.intent,
                    },
                    current,
                ))
            }
        }

        impl CommandCandidateAwaitingValidation for RedbCandidateAwaitingValidation<$prior> {
            type Prior = $prior;
            type AffectedEpochRead = RedbCandidateAffectedEpochRead<$prior>;

            fn plan_validated(
                self,
                affected_targets: AffectedIndexEpochTargets,
            ) -> Self::AffectedEpochRead {
                RedbCandidateAffectedEpochRead {
                    prior: self.prior,
                    intent: self.intent,
                    affected_targets,
                }
            }

            fn reject(
                self,
                _reason: CandidateValidationRejection,
            ) -> AbandonedCandidate<Self::Prior> {
                AbandonedCandidate::new(self.prior, self.intent)
            }
        }

        impl CommandCandidateAffectedEpochRead for RedbCandidateAffectedEpochRead<$prior> {
            type Prior = $prior;
            type AwaitingCapacity = RedbCandidateAwaitingCapacity<$prior>;

            fn read_affected_epoch_current(self) -> Result<Self::AwaitingCapacity, StorageError> {
                let current = affected_current_state(
                    self.prior.core.access.transaction()?,
                    &self.affected_targets,
                )?;
                Ok(RedbCandidateAwaitingCapacity {
                    prior: self.prior,
                    intent: self.intent,
                    affected_targets: self.affected_targets,
                    affected_current: current,
                })
            }
        }

        impl CommandCandidateAwaitingCapacity for RedbCandidateAwaitingCapacity<$prior> {
            type Prior = $prior;
            type CapacityReserved = RedbCandidateCapacityReserved<$prior>;

            fn intent(&self) -> &CommitIntent {
                &self.intent
            }

            fn affected_targets(&self) -> &AffectedIndexEpochTargets {
                &self.affected_targets
            }

            fn affected_current(&self) -> &AffectedEpochCurrentState {
                &self.affected_current
            }

            fn reject(
                self,
                _reason: CandidateValidationRejection,
            ) -> AbandonedCandidate<Self::Prior> {
                AbandonedCandidate::new(self.prior, self.intent)
            }

            fn reserve_capacity(
                self,
                write_plan: CommandWriteSetPlanV1,
            ) -> Result<CandidateCapacityResult<Self::Prior, Self::CapacityReserved>, StorageError>
            {
                if !write_plan.matches_retained_candidate(
                    &self.intent,
                    &self.affected_targets,
                    &self.affected_current,
                ) {
                    return Err(storage_error(StorageErrorKind::InvariantViolation));
                }
                if provenance_exists(
                    self.prior.core.access.transaction()?,
                    self.intent.provenance_id(),
                )? {
                    return Ok(CandidateCapacityResult::ProvenanceIdCollision(
                        ProvenanceIdCollision::detected(),
                    ));
                }
                if self
                    .prior
                    .core
                    .metrics
                    .is_some_and(|metrics| !metrics.can_add(write_plan.charge()))
                {
                    return Ok(CandidateCapacityResult::BatchFull(AbandonedCandidate::new(
                        self.prior,
                        self.intent,
                    )));
                }
                Ok(CandidateCapacityResult::Reserved(
                    RedbCandidateCapacityReserved {
                        prior: self.prior,
                        intent: self.intent,
                        write_plan,
                    },
                ))
            }
        }

        impl CommandCandidateCapacityReserved for RedbCandidateCapacityReserved<$prior> {
            type Prior = $prior;
            type SequenceAssigned = RedbCandidateSequenceAssigned<$prior>;

            fn intent(&self) -> &CommitIntent {
                &self.intent
            }

            fn write_plan(&self) -> &CommandWriteSetPlanV1 {
                &self.write_plan
            }

            fn assign_sequence(mut self) -> Result<Self::SequenceAssigned, StorageError> {
                let allocation = self
                    .prior
                    .core
                    .allocator
                    .allocate_one()
                    .map_err(sequence_error)?;
                self.prior.core.allocator = allocation.next();
                Ok(RedbCandidateSequenceAssigned {
                    prior: self.prior,
                    intent: self.intent,
                    write_plan: self.write_plan,
                    assignment: AssignedCommandSequence::from_assigned(allocation.assigned()),
                })
            }
        }

        impl CommandCandidateSequenceAssigned for RedbCandidateSequenceAssigned<$prior> {
            type Prior = $prior;
            type Staged = RedbNonEmptyBatch;

            fn assignment(&self) -> AssignedCommandSequence {
                self.assignment
            }

            fn intent(&self) -> &CommitIntent {
                &self.intent
            }

            fn write_plan(&self) -> &CommandWriteSetPlanV1 {
                &self.write_plan
            }

            fn stage(self, records: AtomicCommandRecordSet) -> Result<Self::Staged, StorageError> {
                if !records.matches_reserved_candidate(
                    self.assignment,
                    &self.intent,
                    &self.write_plan,
                ) || records.next_application_sequence() != self.prior.core.allocator
                {
                    return Err(storage_error(StorageErrorKind::InvariantViolation));
                }
                // Canonical encoding and the complete reservation proof precede
                // every physical write for this candidate.
                let encoded = encode_atomic_command_record_set_v1(&records).map_err(codec_error)?;
                let mut core = self.prior.core;
                apply_record_set(core.access.transaction()?, &records, &encoded)?;
                core.metrics = Some(metrics_after(core.metrics, &records)?);
                core.staged.push(records);
                Ok(RedbNonEmptyBatch { core })
            }
        }
    };
}

impl_candidate_chain!(RedbEmptyBatch);
impl_candidate_chain!(RedbNonEmptyBatch);

fn apply_record_set(
    transaction: &redb::WriteTransaction,
    records: &AtomicCommandRecordSet,
    encoded: &riffdb_storage_api::EncodedAtomicCommandRecordSetV1,
) -> Result<(), StorageError> {
    let identity_key = identity_key(records.expected_pending().identity())?;
    match records.intent().admission_expectation() {
        CommandAdmissionExpectationV1::ExistingPending => {
            if read_admission(transaction, records.expected_pending().identity())?
                != Some(StoredAdmissionStateV1::Pending(
                    records.expected_pending().clone(),
                ))
            {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
        }
        CommandAdmissionExpectationV1::Vacant(candidates) => {
            if !matching_admissions(transaction, candidates)?.is_empty() {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
        }
    }

    apply_entities(transaction, records, encoded)?;
    apply_index_entries(transaction, records, encoded)?;
    apply_index_epochs(transaction, records, encoded)?;

    {
        let mut table = transaction.open_table(PROVENANCE).map_err(table_error)?;
        if table
            .insert(
                encode_provenance_key(records.provenance().provenance_id()).as_slice(),
                encoded.provenance().as_bytes(),
            )
            .map_err(precommit_storage_error)?
            .is_some()
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
    }
    {
        let mut events = transaction.open_table(EVENTS).map_err(table_error)?;
        let mut event_routes = transaction.open_table(EVENT_ROUTES).map_err(table_error)?;
        let mut outbox = transaction.open_table(OUTBOX).map_err(table_error)?;
        for (((event, intent), route_bytes), (event_bytes, intent_bytes)) in records
            .events()
            .iter()
            .zip(records.outbox_intents())
            .zip(encoded.event_routes())
            .zip(encoded.events().iter().zip(encoded.outbox_intents()))
        {
            if event != intent.event() {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            let key = encode_event_key(event.event_id());
            let route_key =
                encode_event_route_key(records.intent().partition_hash(), event.event_id());
            if events
                .insert(key.as_slice(), event_bytes.as_bytes())
                .map_err(precommit_storage_error)?
                .is_some()
                || event_routes
                    .insert(route_key.as_slice(), route_bytes.as_bytes())
                    .map_err(precommit_storage_error)?
                    .is_some()
                || outbox
                    .insert(key.as_slice(), intent_bytes.as_bytes())
                    .map_err(precommit_storage_error)?
                    .is_some()
            {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
        }
    }
    {
        let mut commits = transaction.open_table(COMMITS).map_err(table_error)?;
        if commits
            .insert(
                encode_application_sequence_key(records.assignment().assigned()).as_slice(),
                encoded.commit().as_bytes(),
            )
            .map_err(precommit_storage_error)?
            .is_some()
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
    }
    if matches!(
        records.intent().admission_expectation(),
        CommandAdmissionExpectationV1::ExistingPending
    ) {
        let mut pending = transaction
            .open_table(IDEMPOTENCY_PENDING)
            .map_err(table_error)?;
        if pending
            .remove(encode_idempotency_key(&identity_key))
            .map_err(precommit_storage_error)?
            .is_none()
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
    }
    {
        let mut outcomes = transaction.open_table(IDEMPOTENCY).map_err(table_error)?;
        if outcomes
            .insert(
                encode_idempotency_key(&identity_key),
                encoded.outcome().as_bytes(),
            )
            .map_err(precommit_storage_error)?
            .is_some()
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
    }
    Ok(())
}

fn stage_application_allocator(
    transaction: &redb::WriteTransaction,
    allocator: ApplicationSequenceAllocator,
) -> Result<(), StorageError> {
    let encoded = riffdb_storage_api::encode_application_sequence_allocator_v1(allocator)
        .map_err(codec_error)?;
    let mut meta = transaction.open_table(META).map_err(table_error)?;
    meta.insert(META_APPLICATION_SEQUENCE, encoded.as_bytes())
        .map_err(precommit_storage_error)?;
    Ok(())
}

fn apply_entities(
    transaction: &redb::WriteTransaction,
    records: &AtomicCommandRecordSet,
    encoded: &riffdb_storage_api::EncodedAtomicCommandRecordSetV1,
) -> Result<(), StorageError> {
    let mut table = transaction.open_table(ENTITIES).map_err(table_error)?;
    for (mutation, bytes) in records.entities().iter().zip(encoded.entities()) {
        let key = encode_entity_key(mutation.post_image().target().key());
        let prior = table
            .insert(key, bytes.as_bytes())
            .map_err(precommit_storage_error)?;
        let expected_presence = matches!(mutation.expected(), ExpectedEntityState::Present(_));
        if prior.is_some() != expected_presence {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
    }
    Ok(())
}

fn apply_index_entries(
    transaction: &redb::WriteTransaction,
    records: &AtomicCommandRecordSet,
    encoded: &riffdb_storage_api::EncodedAtomicCommandRecordSetV1,
) -> Result<(), StorageError> {
    let mut table = transaction
        .open_table(SECONDARY_INDEXES)
        .map_err(table_error)?;
    for (mutation, bytes) in records.index_entries().iter().zip(encoded.index_entries()) {
        let key = encode_index_entry_key(mutation.key());
        match (mutation, bytes) {
            (IndexEntryMutationV1::Delete(_), None) => {
                if table
                    .remove(key)
                    .map_err(precommit_storage_error)?
                    .is_none()
                {
                    return Err(storage_error(StorageErrorKind::InvariantViolation));
                }
            }
            (IndexEntryMutationV1::Put(_), Some(bytes)) => {
                table
                    .insert(key, bytes.as_bytes())
                    .map_err(precommit_storage_error)?;
            }
            _ => return Err(storage_error(StorageErrorKind::InvariantViolation)),
        }
    }
    Ok(())
}

fn apply_index_epochs(
    transaction: &redb::WriteTransaction,
    records: &AtomicCommandRecordSet,
    encoded: &riffdb_storage_api::EncodedAtomicCommandRecordSetV1,
) -> Result<(), StorageError> {
    let mut table = transaction.open_table(INDEX_EPOCHS).map_err(table_error)?;
    for (advance, bytes) in records.index_epochs().iter().zip(encoded.index_epochs()) {
        let key = encode_partition_index_key(advance.post_image().target());
        let prior = table
            .insert(key.as_slice(), bytes.as_bytes())
            .map_err(precommit_storage_error)?;
        let expected_presence = matches!(advance.prior(), IndexEpochPosition::Value(_));
        if prior.is_some() != expected_presence {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
    }
    Ok(())
}

fn read_application_allocator(
    transaction: &redb::WriteTransaction,
) -> Result<ApplicationSequenceAllocator, StorageError> {
    let table = transaction.open_table(META).map_err(table_error)?;
    let value = table
        .get(META_APPLICATION_SEQUENCE)
        .map_err(precommit_storage_error)?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    decode_application_sequence_allocator_v1(value.value()).map(decoded_value)
}

fn read_admission(
    transaction: &redb::WriteTransaction,
    identity: &IdempotencyIdentity,
) -> Result<Option<StoredAdmissionStateV1>, StorageError> {
    read_admission_from_tables(
        transaction
            .open_table(IDEMPOTENCY_PENDING)
            .map_err(table_error)?,
        transaction.open_table(IDEMPOTENCY).map_err(table_error)?,
        identity,
    )
}

fn read_admission_readonly(
    transaction: &redb::ReadTransaction,
    identity: &IdempotencyIdentity,
) -> Result<Option<StoredAdmissionStateV1>, StorageError> {
    read_admission_from_tables(
        transaction
            .open_table(IDEMPOTENCY_PENDING)
            .map_err(table_error)?,
        transaction.open_table(IDEMPOTENCY).map_err(table_error)?,
        identity,
    )
}

fn read_admission_from_tables(
    pending: impl ReadableTable<&'static [u8], &'static [u8]>,
    terminal: impl ReadableTable<&'static [u8], &'static [u8]>,
    identity: &IdempotencyIdentity,
) -> Result<Option<StoredAdmissionStateV1>, StorageError> {
    let key = identity_key(identity)?;
    let pending = pending
        .get(encode_idempotency_key(&key))
        .map_err(precommit_storage_error)?
        .map(|value| decode_pending_admission_v1(value.value()).map(decoded_value))
        .transpose()?;
    let terminal = terminal
        .get(encode_idempotency_key(&key))
        .map_err(precommit_storage_error)?
        .map(|value| decode_idempotency_record_v1(value.value()).map(decoded_value))
        .transpose()?;
    match (pending, terminal) {
        (None, None) => Ok(None),
        (Some(value), None) if value.identity() == identity => {
            Ok(Some(StoredAdmissionStateV1::Pending(value)))
        }
        (None, Some(IdempotencyRecordV1::StoredOutcome(value))) if value.identity() == identity => {
            Ok(Some(StoredAdmissionStateV1::StoredOutcome(value)))
        }
        (None, Some(IdempotencyRecordV1::ExecutionFailed(value)))
            if value.pending().identity() == identity =>
        {
            Ok(Some(StoredAdmissionStateV1::ExecutionFailed(value)))
        }
        _ => Err(storage_error(StorageErrorKind::CorruptData)),
    }
}

fn matching_admissions(
    transaction: &impl AdmissionReadableTransaction,
    candidates: &IdempotencyLookupCandidatesV1,
) -> Result<Vec<StoredAdmissionStateV1>, StorageError> {
    let mut matches = Vec::new();
    for identity in candidates.as_slice() {
        if let Some(value) = transaction.read_one(identity)? {
            matches.push(value);
        }
    }
    Ok(matches)
}

trait AdmissionReadableTransaction {
    fn read_one(
        &self,
        identity: &IdempotencyIdentity,
    ) -> Result<Option<StoredAdmissionStateV1>, StorageError>;
}

impl AdmissionReadableTransaction for redb::WriteTransaction {
    fn read_one(
        &self,
        identity: &IdempotencyIdentity,
    ) -> Result<Option<StoredAdmissionStateV1>, StorageError> {
        read_admission(self, identity)
    }
}

impl AdmissionReadableTransaction for redb::ReadTransaction {
    fn read_one(
        &self,
        identity: &IdempotencyIdentity,
    ) -> Result<Option<StoredAdmissionStateV1>, StorageError> {
        read_admission_readonly(self, identity)
    }
}

fn admission_result(
    existing: &StoredAdmissionStateV1,
    proposed: &StoredPendingAdmissionV1,
) -> AdmissionResultV1 {
    let equal_input = match existing {
        StoredAdmissionStateV1::Pending(value) => {
            value.canonical_input_hash() == proposed.canonical_input_hash()
        }
        StoredAdmissionStateV1::StoredOutcome(value) => {
            value.canonical_input_hash() == proposed.canonical_input_hash()
        }
        StoredAdmissionStateV1::ExecutionFailed(value) => {
            value.pending().canonical_input_hash() == proposed.canonical_input_hash()
        }
    };
    if !equal_input {
        return AdmissionResultV1::InputMismatch;
    }
    match existing {
        StoredAdmissionStateV1::Pending(value) => AdmissionResultV1::Resumed(value.clone()),
        StoredAdmissionStateV1::StoredOutcome(value) => {
            AdmissionResultV1::StoredOutcome(value.clone())
        }
        StoredAdmissionStateV1::ExecutionFailed(value) => {
            AdmissionResultV1::ExecutionFailed(value.clone())
        }
    }
}

fn plan_bundle_exists(
    transaction: &redb::WriteTransaction,
    plan: &riffdb_storage_api::ExecutablePlanRef,
) -> Result<bool, StorageError> {
    let key = encode_contract_bundle_key(plan.contract_lineage(), plan.contract_version())
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    let table = transaction
        .open_table(CONTRACT_BUNDLES)
        .map_err(table_error)?;
    let Some(value) = table.get(key.as_slice()).map_err(precommit_storage_error)? else {
        return Ok(false);
    };
    let bundle = decoded_value(crate::codec::decode_contract_bundle_v1(value.value())?);
    Ok(bundle.lineage() == plan.contract_lineage()
        && bundle.contract_version() == plan.contract_version()
        && bundle.bundle_hash() == plan.contract_bundle_hash())
}

fn current_state(
    transaction: &redb::WriteTransaction,
    request: &ValidationReadRequest,
) -> Result<TransactionCurrentState, StorageError> {
    let mut builder = TransactionCurrentStateBuilder::new(request);
    for target in request.binding_targets() {
        builder
            .push_binding(entity_observation(transaction, target)?)
            .map_err(materialization_value)?;
    }
    for target in request.root_validation_targets() {
        builder
            .push_root_validation(entity_observation(transaction, target)?)
            .map_err(materialization_value)?;
    }
    for target in request.range_targets() {
        builder
            .push_range(CurrentRangeObservation::new(
                target.clone(),
                epoch_position(transaction, target.generation_target())?,
            ))
            .map_err(materialization_value)?;
    }
    builder.finish().map_err(materialization_value)
}

fn affected_current_state(
    transaction: &redb::WriteTransaction,
    targets: &AffectedIndexEpochTargets,
) -> Result<AffectedEpochCurrentState, StorageError> {
    let mut builder = AffectedEpochCurrentStateBuilder::new(targets);
    for target in targets.as_slice() {
        builder
            .push(CurrentIndexGenerationObservation::new(
                target.clone(),
                epoch_position(transaction, target)?,
            ))
            .map_err(materialization_value)?;
    }
    let table = transaction
        .open_table(SECONDARY_INDEXES)
        .map_err(table_error)?;
    for target in targets.unique_targets() {
        let prefix = target.prefix().prefix().as_bytes();
        let upper = exclusive_prefix_end(prefix)
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        let mut range = table
            .range::<&[u8]>((Included(prefix), Excluded(upper.as_slice())))
            .map_err(precommit_storage_error)?;
        let first = range
            .next()
            .transpose()
            .map_err(precommit_storage_error)?
            .map(|(key, _)| key.value().to_vec());
        let second = range.next().transpose().map_err(precommit_storage_error)?;
        let kind = match (first, second) {
            (None, None) => UniqueOccupancyKind::Vacant,
            (Some(key), None) if key.as_slice() == target.expected_entry().as_bytes() => {
                UniqueOccupancyKind::Owned
            }
            (Some(_), None) => UniqueOccupancyKind::Conflict,
            _ => return Err(storage_error(StorageErrorKind::CorruptData)),
        };
        builder
            .push_unique(UniqueIndexOccupancy::new(target.clone(), kind))
            .map_err(materialization_value)?;
    }
    builder.finish().map_err(materialization_value)
}

fn exclusive_prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    let position = end.iter().rposition(|byte| *byte != u8::MAX)?;
    end[position] = end[position].checked_add(1)?;
    end.truncate(position + 1);
    Some(end)
}

fn entity_observation(
    transaction: &redb::WriteTransaction,
    target: &EntityTarget,
) -> Result<EntityObservation, StorageError> {
    let table = transaction.open_table(ENTITIES).map_err(table_error)?;
    let Some(value) = table
        .get(encode_entity_key(target.key()))
        .map_err(precommit_storage_error)?
    else {
        return Ok(EntityObservation::Absent(target.clone()));
    };
    let record = decoded_value(decode_entity_record_v1(value.value())?);
    if record.target() != target {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok(EntityObservation::Present(record))
}

fn epoch_position(
    transaction: &redb::WriteTransaction,
    target: &PartitionIndexTarget,
) -> Result<IndexEpochPosition, StorageError> {
    let table = transaction.open_table(INDEX_EPOCHS).map_err(table_error)?;
    let key = encode_partition_index_key(target);
    let Some(value) = table.get(key.as_slice()).map_err(precommit_storage_error)? else {
        return Ok(IndexEpochPosition::BeforeFirst);
    };
    let record = decoded_value(decode_index_epoch_v1(value.value())?);
    if record.target() != target {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok(IndexEpochPosition::Value(record.epoch()))
}

fn dependencies_from_current(
    current: &TransactionCurrentState,
) -> Result<ReadDependencies, StorageError> {
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
    .map_err(invariant_value)
}

fn provenance_exists(
    transaction: &redb::WriteTransaction,
    provenance_id: ProvenanceId,
) -> Result<bool, StorageError> {
    let table = transaction.open_table(PROVENANCE).map_err(table_error)?;
    Ok(table
        .get(encode_provenance_key(provenance_id).as_slice())
        .map_err(precommit_storage_error)?
        .is_some())
}

fn metrics_after(
    current: Option<StagedBatchMetrics>,
    records: &AtomicCommandRecordSet,
) -> Result<StagedBatchMetrics, StorageError> {
    let charge = records.presequence_charge();
    let (count, semantic, encoded) = match current {
        None => (
            1,
            charge.semantic_bytes(),
            charge.encoded_upper_bound().total(),
        ),
        Some(current) => (
            current
                .command_count()
                .get()
                .checked_add(1)
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
            current
                .semantic_bytes()
                .checked_add(charge.semantic_bytes())
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?,
            current
                .reserved_encoded_bytes()
                .checked_add(charge.encoded_upper_bound().total())
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?,
        ),
    };
    StagedBatchMetrics::new(
        NonZeroU8::new(count).ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
        semantic,
        encoded,
    )
    .map_err(invariant_value)
}

fn identity_key(identity: &IdempotencyIdentity) -> Result<IdempotencyIdentityKey, StorageError> {
    identity
        .storage_key()
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))
}

fn sequence_error(error: riffdb_storage_api::SequenceAllocationError) -> StorageError {
    match error {
        riffdb_storage_api::SequenceAllocationError::Exhausted => {
            storage_error(StorageErrorKind::SequenceExhausted)
        }
        riffdb_storage_api::SequenceAllocationError::ZeroCount
        | riffdb_storage_api::SequenceAllocationError::TooMany => {
            storage_error(StorageErrorKind::InvariantViolation)
        }
    }
}

fn invariant_value(_: StorageValueError) -> StorageError {
    storage_error(StorageErrorKind::InvariantViolation)
}

fn materialization_value(error: StorageValueError) -> StorageError {
    match error {
        StorageValueError::LimitExceeded | StorageValueError::SizeOverflow => {
            storage_error(StorageErrorKind::LimitExceeded)
        }
        _ => storage_error(StorageErrorKind::CorruptData),
    }
}

fn storage_error(kind: StorageErrorKind) -> StorageError {
    StorageError::new(kind, None)
}

fn decoded_value<T>(item: riffdb_storage_api::EncodedPageItem<T>) -> T {
    item.into_parts().0
}
