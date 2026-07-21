//! One capability-owning deterministic command-evaluation attempt.

use std::{error::Error, fmt, time::Instant};

use riffdb_catalog::ResolvedExecutablePlan;
use riffdb_conflict::{CancellationToken, ConflictError, ConflictManager, MutationLease};
use riffdb_runtime::{ExecutionFault, ExecutionResult, TransactionContext, execute_command};
use riffdb_storage_api::{
    AdmissionLookupResultV1, AdmissionRepository, EvaluatedCommand, IdempotencyLookupCandidatesV1,
    PreEvaluationCommitContext, ReadSnapshot, SnapshotReader, SnapshotRequest, StorageError,
    StoredAdmissionStateV1, StoredExecutionFailedV1, StoredOutcomeV1,
};
use riffdb_types::{CanonicalRecord, ConflictKey, ExecutionFailureCode, RequestId};

use crate::command_admission::CommandExecutionCandidate;

/// Maximum complete owned-snapshot evaluations in one outer invocation.
pub(crate) const MAX_COMMAND_EVALUATION_ATTEMPTS_V1: usize = 3;

/// Move-only admitted state between complete command-evaluation attempts.
pub(crate) struct PendingCommandAttempts {
    resolved_plan: ResolvedExecutablePlan,
    normalized_input: CanonicalRecord,
    commit_context: PreEvaluationCommitContext,
    raw_conflict_keys: Vec<ConflictKey>,
    snapshot_request: SnapshotRequest,
    lookup_candidates: IdempotencyLookupCandidatesV1,
    invocation_request_id: RequestId,
    deadline: Instant,
    cancellation: CancellationToken,
    completed_attempts: usize,
}

impl PendingCommandAttempts {
    /// Lowers one exact admission candidate without reconstructing semantic facts.
    pub(crate) fn from_admission(
        candidate: Box<CommandExecutionCandidate>,
    ) -> Result<Self, CommandAttemptError> {
        let lookup_candidates = candidate
            .lookup_candidates_for_resolution()
            .map_err(|_| CommandAttemptError::Integrity)?;
        let (
            resolved_plan,
            normalized_input,
            commit_context,
            raw_conflict_keys,
            snapshot_request,
            invocation_request_id,
            deadline,
            cancellation,
        ) = (*candidate).into_acquisition_parts();
        Ok(Self {
            resolved_plan,
            normalized_input,
            commit_context,
            raw_conflict_keys,
            snapshot_request,
            lookup_candidates,
            invocation_request_id,
            deadline,
            cancellation,
            completed_attempts: 0,
        })
    }

    /// Borrows immutable capacity and durable-admission context for later commit work.
    pub(crate) const fn commit_context(&self) -> &PreEvaluationCommitContext {
        &self.commit_context
    }

    /// Returns the current transport invocation identity for internal correlation only.
    pub(crate) const fn invocation_request_id(&self) -> RequestId {
        self.invocation_request_id
    }

    /// Returns the number of complete runtime evaluations already begun.
    pub(crate) const fn completed_attempts(&self) -> usize {
        self.completed_attempts
    }
}

impl fmt::Debug for PendingCommandAttempts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PendingCommandAttempts([REDACTED])")
    }
}

/// Closed result of one exact capability-owning evaluation attempt.
#[allow(clippy::large_enum_variant)] // Keep move-only capability state inline at this private boundary.
pub(crate) enum CommandAttemptResolution {
    /// Evaluation produced a commit-required candidate while retaining its capability.
    Evaluated(EvaluatedCommandAttempt),
    /// Evaluation produced a dependency-sensitive deterministic failure.
    ExecutionFault(ExecutionFaultAttempt),
    /// A matching durable command outcome won before snapshot materialization.
    OutcomeReplay(StoredOutcomeV1),
    /// A matching durable deterministic failure won before snapshot materialization.
    ExecutionFailureReplay(StoredExecutionFailedV1),
}

impl fmt::Debug for CommandAttemptResolution {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Evaluated(_) => "CommandAttemptResolution::Evaluated([REDACTED])",
            Self::ExecutionFault(_) => "CommandAttemptResolution::ExecutionFault([REDACTED])",
            Self::OutcomeReplay(_) => "CommandAttemptResolution::OutcomeReplay([REDACTED])",
            Self::ExecutionFailureReplay(_) => {
                "CommandAttemptResolution::ExecutionFailureReplay([REDACTED])"
            }
        })
    }
}

/// Successful deterministic evaluation bundled with its exclusive capability.
pub(crate) struct EvaluatedCommandAttempt {
    state: PendingCommandAttempts,
    lease: MutationLease,
    snapshot: ReadSnapshot,
    evaluated: EvaluatedCommand,
}

impl EvaluatedCommandAttempt {
    /// Consumes the attempt into the exact inputs required by later commit orchestration.
    fn into_parts(
        self,
    ) -> (
        PendingCommandAttempts,
        MutationLease,
        ReadSnapshot,
        EvaluatedCommand,
    ) {
        (self.state, self.lease, self.snapshot, self.evaluated)
    }
}

impl fmt::Debug for EvaluatedCommandAttempt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EvaluatedCommandAttempt([REDACTED])")
    }
}

/// Dependency-sensitive deterministic failure bundled with its read evidence.
pub(crate) struct ExecutionFaultAttempt {
    state: PendingCommandAttempts,
    lease: MutationLease,
    snapshot: ReadSnapshot,
    code: ExecutionFailureCode,
}

impl ExecutionFaultAttempt {
    /// Consumes the attempt into the exact inputs required by terminalization.
    pub(crate) fn into_parts(
        self,
    ) -> (
        PendingCommandAttempts,
        MutationLease,
        ReadSnapshot,
        ExecutionFailureCode,
    ) {
        (self.state, self.lease, self.snapshot, self.code)
    }
}

impl fmt::Debug for ExecutionFaultAttempt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ExecutionFaultAttempt([REDACTED])")
    }
}

/// Failure before a valid evaluated or terminal-replay result can be released.
pub(crate) enum CommandAttemptError {
    /// Cancellation was observed at an attempt safe point.
    Cancelled,
    /// The request deadline was observed at an attempt safe point.
    DeadlineExceeded,
    /// Logical capability acquisition failed for another bounded reason.
    Conflict(ConflictError),
    /// The exact pending-admission recheck could not be read.
    PendingRecheck(StorageError),
    /// A complete owned snapshot could not be materialized.
    SnapshotRead(StorageError),
    /// Three complete evaluations were already begun by this invocation.
    RetryBudgetExhausted,
    /// Independently checked semantic state was inconsistent.
    Integrity,
}

impl fmt::Debug for CommandAttemptError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CommandAttemptError([REDACTED])")
    }
}

impl fmt::Display for CommandAttemptError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("command evaluation attempt could not be completed")
    }
}

impl Error for CommandAttemptError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Conflict(error) => Some(error),
            Self::PendingRecheck(error) | Self::SnapshotRead(error) => Some(error),
            Self::Cancelled
            | Self::DeadlineExceeded
            | Self::RetryBudgetExhausted
            | Self::Integrity => None,
        }
    }
}

/// Acquires, rechecks, snapshots, and synchronously evaluates one complete attempt.
///
/// The acquisition is deliberately the only await point. Once granted, the
/// move-only lease remains bundled with every value that may proceed toward a
/// durable transition and is released by drop on every replay or error path.
pub(crate) async fn evaluate_next_command_attempt(
    mut state: PendingCommandAttempts,
    admission: &dyn AdmissionRepository,
    snapshots: &dyn SnapshotReader,
    conflicts: &dyn ConflictManager,
) -> Result<CommandAttemptResolution, CommandAttemptError> {
    if state.completed_attempts >= MAX_COMMAND_EVALUATION_ATTEMPTS_V1 {
        return Err(CommandAttemptError::RetryBudgetExhausted);
    }
    check_request_control(state.deadline, &state.cancellation)?;

    let lease = conflicts
        .acquire_mut(
            state.raw_conflict_keys.clone(),
            state.deadline,
            state.cancellation.clone(),
        )
        .await
        .map_err(map_conflict_error)?;

    check_request_control(state.deadline, &state.cancellation)?;
    let durable = admission
        .lookup_admission(state.lookup_candidates.clone())
        .map_err(CommandAttemptError::PendingRecheck)?;
    match durable {
        AdmissionLookupResultV1::Found(found) => match *found {
            StoredAdmissionStateV1::Pending(pending)
                if pending == *state.commit_context.pending() => {}
            StoredAdmissionStateV1::StoredOutcome(outcome)
                if outcome_matches_context(&outcome, &state.commit_context) =>
            {
                return Ok(CommandAttemptResolution::OutcomeReplay(outcome));
            }
            StoredAdmissionStateV1::ExecutionFailed(failure)
                if failure.pending() == state.commit_context.pending() =>
            {
                return Ok(CommandAttemptResolution::ExecutionFailureReplay(failure));
            }
            _ => return Err(CommandAttemptError::Integrity),
        },
        AdmissionLookupResultV1::NotFound | AdmissionLookupResultV1::MultipleMatches => {
            return Err(CommandAttemptError::Integrity);
        }
    }

    let snapshot = snapshots
        .read_snapshot(state.snapshot_request.clone())
        .map_err(CommandAttemptError::SnapshotRead)?;
    check_request_control(state.deadline, &state.cancellation)?;

    let context = transaction_context(&state.commit_context);
    state.completed_attempts = state
        .completed_attempts
        .checked_add(1)
        .ok_or(CommandAttemptError::Integrity)?;
    let execution = execute_command(
        state.resolved_plan.bundle().bundle(),
        &state.normalized_input,
        &snapshot,
        &context,
        state.commit_context.evaluation_budget(),
    );

    let resolution = match execution {
        Ok(ExecutionResult::CommitRequired(evaluated)) => {
            check_request_control(state.deadline, &state.cancellation)?;
            CommandAttemptResolution::Evaluated(EvaluatedCommandAttempt {
                state,
                lease,
                snapshot,
                evaluated,
            })
        }
        Err(ExecutionFault::Arithmetic) => {
            check_request_control(state.deadline, &state.cancellation)?;
            CommandAttemptResolution::ExecutionFault(ExecutionFaultAttempt {
                state,
                lease,
                snapshot,
                code: ExecutionFailureCode::ArithmeticFault,
            })
        }
        Err(ExecutionFault::ResourceLimit) => {
            check_request_control(state.deadline, &state.cancellation)?;
            CommandAttemptResolution::ExecutionFault(ExecutionFaultAttempt {
                state,
                lease,
                snapshot,
                code: ExecutionFailureCode::ResourceLimit,
            })
        }
        Ok(ExecutionResult::ReadOnly(_)) | Err(ExecutionFault::Integrity) => {
            return Err(CommandAttemptError::Integrity);
        }
    };
    Ok(resolution)
}

fn transaction_context(context: &PreEvaluationCommitContext) -> TransactionContext {
    let pending = context.pending();
    TransactionContext::new(
        pending.admission_request_id(),
        pending.actor().clone(),
        pending.plan().clone(),
        pending.logical_time(),
        pending.partition_key().clone(),
    )
}

fn check_request_control(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), CommandAttemptError> {
    if cancellation.is_cancelled() {
        return Err(CommandAttemptError::Cancelled);
    }
    if Instant::now() >= deadline {
        return Err(CommandAttemptError::DeadlineExceeded);
    }
    Ok(())
}

fn map_conflict_error(error: ConflictError) -> CommandAttemptError {
    match error {
        ConflictError::Cancelled => CommandAttemptError::Cancelled,
        ConflictError::DeadlineExceeded => CommandAttemptError::DeadlineExceeded,
        other => CommandAttemptError::Conflict(other),
    }
}

fn outcome_matches_context(
    outcome: &StoredOutcomeV1,
    context: &PreEvaluationCommitContext,
) -> bool {
    let pending = context.pending();
    outcome.identity() == pending.identity()
        && outcome.admission_request_id() == pending.admission_request_id()
        && outcome.plan() == pending.plan()
        && outcome.canonical_input_hash() == pending.canonical_input_hash()
        && outcome.actor() == pending.actor()
        && outcome.logical_time() == pending.logical_time()
        && outcome.partition_key() == pending.partition_key()
        && outcome.partition_hash() == context.partition_hash()
        && outcome.conflict_hashes() == context.conflict_hashes()
        && outcome.admitted_claims() == pending.provenance_claims()
}

#[cfg(test)]
mod tests {
    use std::{
        cell::{Cell, RefCell},
        collections::BTreeMap,
        rc::Rc,
        task::{Context, Poll, Waker},
        time::Duration,
    };

    use riffdb_catalog::ValidatedContractBundle;
    use riffdb_conflict::{ConflictManagerConfig, ShardedConflictManager};
    use riffdb_contract_ir::{CommandPlan, RecordSchema};
    use riffdb_invariant::derive_input_command_facts;

    use riffdb_storage_api::{
        AdmissionRequestV1, AdmissionResultV1, DeclaredOutcome, DurabilityMode, EntityObservation,
        EntityTarget, ExecutablePlanRef, IdempotencyIdentity, IdempotencyKeyDigest,
        StoredAdmittedProvenanceClaimsV1, StoredPendingAdmissionV1,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdmittedActorContext, AggregateTypeId, CanonicalInputHash,
        CanonicalRecord, CanonicalValue, CommandId, CommitSequence, ConflictKeyHash,
        ContractBundleHash, ContractLineage, ContractVersion, DatabaseId, Decimal, DecimalSpec,
        DigestKeyId, Environment, LogicalTime, OutcomeId, PartitionKeyBuilder, PlanHash,
        ProvenanceId, TenantScope, Timestamp, hash_conflict_key, hash_partition_key,
    };

    use super::*;

    const PRINCIPAL: &str = "attempt-test-principal";
    const SENSITIVE_MARKER: &str = "attempt-test-sensitive";

    struct ScriptedRepository {
        expected: IdempotencyLookupCandidatesV1,
        result: AdmissionLookupResultV1,
        calls: Cell<usize>,
        order: Rc<RefCell<Vec<&'static str>>>,
    }

    impl ScriptedRepository {
        fn new(
            expected: IdempotencyLookupCandidatesV1,
            result: AdmissionLookupResultV1,
            order: Rc<RefCell<Vec<&'static str>>>,
        ) -> Self {
            Self {
                expected,
                result,
                calls: Cell::new(0),
                order,
            }
        }
    }

    impl AdmissionRepository for ScriptedRepository {
        fn admit_or_resolve(
            &self,
            _request: AdmissionRequestV1,
        ) -> Result<AdmissionResultV1, StorageError> {
            panic!("attempt recheck must never create admission")
        }

        fn lookup_admission(
            &self,
            candidates: IdempotencyLookupCandidatesV1,
        ) -> Result<AdmissionLookupResultV1, StorageError> {
            assert_eq!(candidates, self.expected);
            self.calls.set(self.calls.get() + 1);
            self.order.borrow_mut().push("pending-recheck");
            Ok(self.result.clone())
        }
    }

    struct ScriptedSnapshotReader {
        expected: SnapshotRequest,
        snapshot: ReadSnapshot,
        cancel_during_read: Option<CancellationToken>,
        calls: Cell<usize>,
        order: Rc<RefCell<Vec<&'static str>>>,
    }

    impl ScriptedSnapshotReader {
        fn new(
            expected: SnapshotRequest,
            snapshot: ReadSnapshot,
            order: Rc<RefCell<Vec<&'static str>>>,
        ) -> Self {
            Self {
                expected,
                snapshot,
                cancel_during_read: None,
                calls: Cell::new(0),
                order,
            }
        }

        fn cancelling(mut self, cancellation: CancellationToken) -> Self {
            self.cancel_during_read = Some(cancellation);
            self
        }
    }

    impl SnapshotReader for ScriptedSnapshotReader {
        fn read_snapshot(&self, request: SnapshotRequest) -> Result<ReadSnapshot, StorageError> {
            assert!(request == self.expected, "attempt changed snapshot request");
            self.calls.set(self.calls.get() + 1);
            self.order.borrow_mut().push("snapshot");
            if let Some(cancellation) = &self.cancel_during_read {
                cancellation.cancel();
            }
            Ok(self.snapshot.clone())
        }
    }

    fn manager() -> ShardedConflictManager {
        ShardedConflictManager::new(ConflictManagerConfig::default()).expect("conflict manager")
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("current-thread runtime")
    }

    fn future_deadline() -> Instant {
        Instant::now()
            .checked_add(Duration::from_secs(30))
            .expect("future deadline")
    }

    fn decimal(coefficient: i128) -> CanonicalValue {
        CanonicalValue::Decimal(
            Decimal::new(DecimalSpec::new(28, 2).expect("decimal spec"), coefficient)
                .expect("bounded decimal"),
        )
    }

    fn input_record<const N: usize>(
        schema: &RecordSchema,
        fields: [(&str, CanonicalValue); N],
    ) -> CanonicalRecord {
        let by_name = fields.into_iter().collect::<BTreeMap<_, _>>();
        CanonicalRecord::new(
            schema
                .fields()
                .iter()
                .map(|field| {
                    (
                        field.id(),
                        by_name
                            .get(field.name())
                            .unwrap_or_else(|| panic!("missing field {}", field.name()))
                            .clone(),
                    )
                })
                .collect(),
        )
        .expect("canonical input")
    }

    fn create_budget_input(plan: &CommandPlan) -> CanonicalRecord {
        input_record(
            plan.input().record(),
            [
                (
                    "idempotency_key",
                    CanonicalValue::string(SENSITIVE_MARKER).expect("bounded caller key"),
                ),
                ("organization_id", CanonicalValue::Uuid([0x41; 16])),
                ("fiscal_year", CanonicalValue::I64(2028)),
                ("approved_amount", decimal(25_000)),
            ],
        )
    }

    fn execution_fixture() -> (PendingCommandAttempts, ReadSnapshot) {
        let bundle = ValidatedContractBundle::decode(include_bytes!(
            "../../../fixtures/compiler/bundle.bin"
        ))
        .expect("checked compiler fixture");
        let plan = bundle
            .bundle()
            .commands()
            .iter()
            .find(|plan| plan.name() == "CreateBudget")
            .expect("CreateBudget plan");
        let reference = ExecutablePlanRef::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            plan.command_id(),
            plan.plan_hash(),
        );
        let normalized_input = create_budget_input(plan);
        let facts = derive_input_command_facts(plan, normalized_input.clone())
            .expect("input-derived command facts");

        let binding_targets = plan
            .bindings()
            .iter()
            .map(|binding| binding.entity_type())
            .zip(facts.binding_entity_keys().iter().cloned())
            .map(|(entity_type, key)| EntityTarget::new(entity_type, key))
            .collect::<Result<Vec<_>, _>>()
            .expect("binding targets");
        let root_targets = plan
            .root_validation_reads()
            .iter()
            .map(|read| read.entity_type())
            .zip(facts.root_validation_entity_keys().iter().cloned())
            .map(|(entity_type, key)| EntityTarget::new(entity_type, key))
            .collect::<Result<Vec<_>, _>>()
            .expect("root targets");
        let snapshot_request =
            SnapshotRequest::new(reference.clone(), binding_targets, root_targets, Vec::new())
                .expect("snapshot request");
        let snapshot = ReadSnapshot::new(
            &snapshot_request,
            None,
            snapshot_request
                .binding_targets()
                .iter()
                .cloned()
                .map(EntityObservation::Absent)
                .collect(),
            snapshot_request
                .root_validation_targets()
                .iter()
                .cloned()
                .map(EntityObservation::Absent)
                .collect(),
            Vec::new(),
        )
        .expect("complete owned snapshot");

        let actor = AdmittedActorContext::new(
            ActorId::new(PRINCIPAL).expect("principal"),
            ActorKind::Service,
            TenantScope::Global,
            None,
        );
        let identity = IdempotencyIdentity::new(
            database_id(),
            Environment::new("development").expect("environment"),
            TenantScope::Global,
            ActorId::new(PRINCIPAL).expect("principal"),
            reference.contract_lineage().clone(),
            reference.command_id(),
            IdempotencyKeyDigest::from_hmac_bytes(
                DigestKeyId::new(1).expect("digest key ID"),
                [0x51; 32],
            ),
        );
        let pending = StoredPendingAdmissionV1::new(
            identity,
            CanonicalInputHash::from_bytes([0x61; 32]),
            request_id(1),
            reference,
            LogicalTime::new(Timestamp::new(100, 17).expect("timestamp")),
            actor,
            facts.partition_key().clone(),
            StoredAdmittedProvenanceClaimsV1::default(),
        )
        .expect("pending admission");
        let mut raw_conflict_keys = facts.declared_conflict_keys().to_vec();
        raw_conflict_keys.sort_unstable();
        raw_conflict_keys.dedup();
        let mut conflict_hashes = raw_conflict_keys
            .iter()
            .map(|key| hash_conflict_key(key.as_bytes()))
            .collect::<Vec<_>>();
        conflict_hashes.sort_unstable();
        let commit_context = PreEvaluationCommitContext::new(
            pending,
            hash_partition_key(facts.partition_key().as_bytes()),
            conflict_hashes,
        )
        .expect("pre-evaluation context");
        let lookup_candidates =
            IdempotencyLookupCandidatesV1::new(vec![commit_context.pending().identity().clone()])
                .expect("singleton lookup");
        let resolved_plan =
            crate::test_support::resolve_genesis_plan(&bundle, commit_context.pending().plan())
                .expect("exact checked plan");

        (
            PendingCommandAttempts {
                resolved_plan,
                normalized_input,
                commit_context,
                raw_conflict_keys,
                snapshot_request,
                lookup_candidates,
                invocation_request_id: request_id(2),
                deadline: future_deadline(),
                cancellation: CancellationToken::new(),
                completed_attempts: 0,
            },
            snapshot,
        )
    }

    fn acquire_and_release(
        runtime: &tokio::runtime::Runtime,
        manager: &ShardedConflictManager,
        keys: Vec<ConflictKey>,
    ) {
        runtime
            .block_on(manager.acquire_mut(keys, future_deadline(), CancellationToken::new()))
            .expect("logical capability is available")
            .release();
    }

    fn request_id(seed: u8) -> RequestId {
        RequestId::from_unix_milliseconds_and_random(u64::from(seed) + 1, [seed; 10])
            .expect("UUIDv7 request ID")
    }

    fn database_id() -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(1, [0x21; 10]).expect("UUIDv7 database ID")
    }

    fn provenance_id() -> ProvenanceId {
        ProvenanceId::from_unix_milliseconds_and_random(1, [0x31; 10])
            .expect("UUIDv7 provenance ID")
    }

    fn commit_context() -> PreEvaluationCommitContext {
        let lineage = ContractLineage::new("attempt-test").expect("lineage");
        let command_id = CommandId::first();
        let plan = ExecutablePlanRef::new(
            lineage.clone(),
            ContractVersion::new(1).expect("contract version"),
            ContractBundleHash::from_bytes([0x41; 32]),
            command_id,
            PlanHash::from_bytes([0x42; 32]),
        );
        let actor = AdmittedActorContext::new(
            ActorId::new(PRINCIPAL).expect("principal"),
            ActorKind::Service,
            TenantScope::Global,
            None,
        );
        let identity = IdempotencyIdentity::new(
            database_id(),
            Environment::new("development").expect("environment"),
            TenantScope::Global,
            ActorId::new(PRINCIPAL).expect("principal"),
            lineage,
            command_id,
            IdempotencyKeyDigest::from_hmac_bytes(
                DigestKeyId::new(1).expect("digest key ID"),
                [0x51; 32],
            ),
        );
        let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
        partition.push_u64(7).expect("partition component");
        let partition = partition.finish().expect("partition key");
        let pending = StoredPendingAdmissionV1::new(
            identity,
            CanonicalInputHash::from_bytes([0x61; 32]),
            request_id(1),
            plan,
            LogicalTime::new(Timestamp::new(100, 17).expect("timestamp")),
            actor,
            partition.clone(),
            StoredAdmittedProvenanceClaimsV1::default(),
        )
        .expect("pending admission");
        PreEvaluationCommitContext::new(
            pending,
            hash_partition_key(partition.as_bytes()),
            vec![ConflictKeyHash::from_bytes([0x71; 32])],
        )
        .expect("pre-evaluation context")
    }

    fn outcome_from(
        context: &PreEvaluationCommitContext,
        admission_request_id: RequestId,
        conflict_hashes: Vec<ConflictKeyHash>,
    ) -> StoredOutcomeV1 {
        let pending = context.pending();
        StoredOutcomeV1::new(
            pending.identity().clone(),
            CommitSequence::new(1).expect("commit sequence"),
            admission_request_id,
            pending.plan().clone(),
            pending.canonical_input_hash(),
            pending.actor().clone(),
            pending.logical_time(),
            pending.partition_key().clone(),
            context.partition_hash(),
            conflict_hashes,
            DeclaredOutcome::new(
                OutcomeId::first(),
                CanonicalRecord::new(Vec::new()).expect("empty outcome"),
            )
            .expect("declared outcome"),
            pending.provenance_claims().clone(),
            provenance_id(),
            DurabilityMode::Memory,
        )
        .expect("stored outcome")
    }

    #[test]
    fn real_evaluation_orders_ports_and_retains_lease_until_drop() {
        let (state, snapshot) = execution_fixture();
        let keys = state.raw_conflict_keys.clone();
        let order = Rc::new(RefCell::new(Vec::new()));
        let repository = ScriptedRepository::new(
            state.lookup_candidates.clone(),
            AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::Pending(
                state.commit_context.pending().clone(),
            ))),
            Rc::clone(&order),
        );
        let snapshots = ScriptedSnapshotReader::new(
            state.snapshot_request.clone(),
            snapshot,
            Rc::clone(&order),
        );
        let manager = manager();
        let runtime = runtime();

        let resolution = runtime
            .block_on(evaluate_next_command_attempt(
                state,
                &repository,
                &snapshots,
                &manager,
            ))
            .expect("evaluation attempt");
        let CommandAttemptResolution::Evaluated(attempt) = resolution else {
            panic!("CreateBudget must require a commit")
        };
        let (next, lease, owned_snapshot, evaluated) = attempt.into_parts();
        assert_eq!(next.completed_attempts(), 1);
        assert_eq!(evaluated.mutations().len(), 1);
        assert_eq!(
            evaluated.read_dependencies(),
            owned_snapshot.read_dependencies()
        );
        assert_eq!(&*order.borrow(), &["pending-recheck", "snapshot"]);

        let mut competing = manager.acquire_mut(keys, future_deadline(), CancellationToken::new());
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            competing.as_mut().poll(&mut context),
            Poll::Pending
        ));
        drop(lease);
        runtime
            .block_on(competing)
            .expect("dropping retained lease grants competitor")
            .release();
    }

    #[test]
    fn terminal_replay_skips_snapshot_and_all_other_states_fail_closed() {
        let runtime = runtime();
        for replay_failure in [false, true] {
            let (state, snapshot) = execution_fixture();
            let keys = state.raw_conflict_keys.clone();
            let terminal = if replay_failure {
                StoredAdmissionStateV1::ExecutionFailed(StoredExecutionFailedV1::new(
                    state.commit_context.pending().clone(),
                    ExecutionFailureCode::ArithmeticFault,
                ))
            } else {
                StoredAdmissionStateV1::StoredOutcome(outcome_from(
                    &state.commit_context,
                    state.commit_context.pending().admission_request_id(),
                    state.commit_context.conflict_hashes().to_vec(),
                ))
            };
            let order = Rc::new(RefCell::new(Vec::new()));
            let repository = ScriptedRepository::new(
                state.lookup_candidates.clone(),
                AdmissionLookupResultV1::Found(Box::new(terminal)),
                Rc::clone(&order),
            );
            let snapshots = ScriptedSnapshotReader::new(
                state.snapshot_request.clone(),
                snapshot,
                Rc::clone(&order),
            );
            let manager = manager();

            let replay = runtime
                .block_on(evaluate_next_command_attempt(
                    state,
                    &repository,
                    &snapshots,
                    &manager,
                ))
                .expect("matching terminal replay");
            if replay_failure {
                assert!(matches!(
                    replay,
                    CommandAttemptResolution::ExecutionFailureReplay(_)
                ));
            } else {
                assert!(matches!(replay, CommandAttemptResolution::OutcomeReplay(_)));
            }
            assert_eq!(snapshots.calls.get(), 0);
            assert_eq!(&*order.borrow(), &["pending-recheck"]);
            acquire_and_release(&runtime, &manager, keys);
        }

        for terminal in [Some(AdmissionLookupResultV1::MultipleMatches), None] {
            let (state, snapshot) = execution_fixture();
            let keys = state.raw_conflict_keys.clone();
            let result = terminal.unwrap_or_else(|| {
                let mismatched = outcome_from(
                    &state.commit_context,
                    state.commit_context.pending().admission_request_id(),
                    Vec::new(),
                );
                AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::StoredOutcome(
                    mismatched,
                )))
            });
            let order = Rc::new(RefCell::new(Vec::new()));
            let repository =
                ScriptedRepository::new(state.lookup_candidates.clone(), result, Rc::clone(&order));
            let snapshots = ScriptedSnapshotReader::new(
                state.snapshot_request.clone(),
                snapshot,
                Rc::clone(&order),
            );
            let manager = manager();

            assert!(matches!(
                runtime.block_on(evaluate_next_command_attempt(
                    state,
                    &repository,
                    &snapshots,
                    &manager,
                )),
                Err(CommandAttemptError::Integrity)
            ));
            assert_eq!(snapshots.calls.get(), 0);
            assert_eq!(&*order.borrow(), &["pending-recheck"]);
            acquire_and_release(&runtime, &manager, keys);
        }
    }

    #[test]
    fn cancellation_before_acquire_or_after_snapshot_releases_all_capabilities() {
        let runtime = runtime();

        let (state, snapshot) = execution_fixture();
        let keys = state.raw_conflict_keys.clone();
        state.cancellation.cancel();
        let order = Rc::new(RefCell::new(Vec::new()));
        let repository = ScriptedRepository::new(
            state.lookup_candidates.clone(),
            AdmissionLookupResultV1::NotFound,
            Rc::clone(&order),
        );
        let snapshots = ScriptedSnapshotReader::new(
            state.snapshot_request.clone(),
            snapshot,
            Rc::clone(&order),
        );
        let first_manager = manager();
        assert!(matches!(
            runtime.block_on(evaluate_next_command_attempt(
                state,
                &repository,
                &snapshots,
                &first_manager,
            )),
            Err(CommandAttemptError::Cancelled)
        ));
        assert_eq!(repository.calls.get(), 0);
        assert_eq!(snapshots.calls.get(), 0);
        assert!(order.borrow().is_empty());
        acquire_and_release(&runtime, &first_manager, keys);

        let (state, snapshot) = execution_fixture();
        let keys = state.raw_conflict_keys.clone();
        let cancellation = state.cancellation.clone();
        let order = Rc::new(RefCell::new(Vec::new()));
        let repository = ScriptedRepository::new(
            state.lookup_candidates.clone(),
            AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::Pending(
                state.commit_context.pending().clone(),
            ))),
            Rc::clone(&order),
        );
        let snapshots = ScriptedSnapshotReader::new(
            state.snapshot_request.clone(),
            snapshot,
            Rc::clone(&order),
        )
        .cancelling(cancellation);
        let second_manager = manager();
        assert!(matches!(
            runtime.block_on(evaluate_next_command_attempt(
                state,
                &repository,
                &snapshots,
                &second_manager,
            )),
            Err(CommandAttemptError::Cancelled)
        ));
        assert_eq!(repository.calls.get(), 1);
        assert_eq!(snapshots.calls.get(), 1);
        assert_eq!(&*order.borrow(), &["pending-recheck", "snapshot"]);
        acquire_and_release(&runtime, &second_manager, keys);
    }

    #[test]
    fn three_real_evaluations_are_allowed_and_the_fourth_touches_no_port() {
        let (mut state, snapshot) = execution_fixture();
        let order = Rc::new(RefCell::new(Vec::new()));
        let repository = ScriptedRepository::new(
            state.lookup_candidates.clone(),
            AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::Pending(
                state.commit_context.pending().clone(),
            ))),
            Rc::clone(&order),
        );
        let snapshots = ScriptedSnapshotReader::new(
            state.snapshot_request.clone(),
            snapshot,
            Rc::clone(&order),
        );
        let manager = manager();
        let runtime = runtime();

        for expected in 1..=MAX_COMMAND_EVALUATION_ATTEMPTS_V1 {
            let resolution = runtime
                .block_on(evaluate_next_command_attempt(
                    state,
                    &repository,
                    &snapshots,
                    &manager,
                ))
                .expect("permitted evaluation");
            let CommandAttemptResolution::Evaluated(attempt) = resolution else {
                panic!("CreateBudget must evaluate")
            };
            let (next, lease, _, evaluated) = attempt.into_parts();
            assert_eq!(next.completed_attempts(), expected);
            assert_eq!(evaluated.mutations().len(), 1);
            drop(lease);
            state = next;
        }

        let calls_before_fourth = (repository.calls.get(), snapshots.calls.get());
        assert!(matches!(
            runtime.block_on(evaluate_next_command_attempt(
                state,
                &repository,
                &snapshots,
                &manager,
            )),
            Err(CommandAttemptError::RetryBudgetExhausted)
        ));
        assert_eq!(
            calls_before_fourth,
            (
                MAX_COMMAND_EVALUATION_ATTEMPTS_V1,
                MAX_COMMAND_EVALUATION_ATTEMPTS_V1
            )
        );
        assert_eq!(
            (repository.calls.get(), snapshots.calls.get()),
            calls_before_fourth
        );
        assert_eq!(order.borrow().len(), MAX_COMMAND_EVALUATION_ATTEMPTS_V1 * 2);
    }

    #[test]
    fn cancellation_precedes_deadline_at_request_control_safe_points() {
        let expired = Instant::now()
            .checked_sub(Duration::from_secs(1))
            .expect("expired deadline");
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(matches!(
            check_request_control(expired, &cancelled),
            Err(CommandAttemptError::Cancelled)
        ));

        assert!(matches!(
            check_request_control(expired, &CancellationToken::new()),
            Err(CommandAttemptError::DeadlineExceeded)
        ));
        let future = Instant::now()
            .checked_add(Duration::from_secs(30))
            .expect("future deadline");
        assert!(check_request_control(future, &CancellationToken::new()).is_ok());
    }

    #[test]
    fn runtime_context_uses_the_original_admission_identity_and_values() {
        let context = commit_context();
        let pending = context.pending();
        let later_invocation = request_id(2);
        assert_ne!(later_invocation, pending.admission_request_id());

        let runtime = transaction_context(&context);
        assert_eq!(runtime.request_id(), pending.admission_request_id());
        assert_eq!(runtime.actor(), pending.actor());
        assert_eq!(runtime.plan(), pending.plan());
        assert_eq!(runtime.tx_time(), pending.logical_time());
        assert_eq!(runtime.partition_key(), pending.partition_key());
    }

    #[test]
    fn terminal_outcome_match_requires_the_exact_admitted_commit_context() {
        let context = commit_context();
        let exact = outcome_from(
            &context,
            context.pending().admission_request_id(),
            context.conflict_hashes().to_vec(),
        );
        assert!(outcome_matches_context(&exact, &context));

        let wrong_request =
            outcome_from(&context, request_id(9), context.conflict_hashes().to_vec());
        assert!(!outcome_matches_context(&wrong_request, &context));
        let wrong_conflicts = outcome_from(
            &context,
            context.pending().admission_request_id(),
            Vec::new(),
        );
        assert!(!outcome_matches_context(&wrong_conflicts, &context));
    }

    #[test]
    fn attempt_diagnostics_are_static_and_redacted() {
        for error in [
            CommandAttemptError::Cancelled,
            CommandAttemptError::DeadlineExceeded,
            CommandAttemptError::RetryBudgetExhausted,
            CommandAttemptError::Integrity,
        ] {
            let debug = format!("{error:?}");
            assert_eq!(debug, "CommandAttemptError([REDACTED])");
            assert_eq!(
                error.to_string(),
                "command evaluation attempt could not be completed"
            );
            assert!(!debug.contains(PRINCIPAL));
            assert!(!debug.contains(SENSITIVE_MARKER));
        }

        let context = commit_context();
        let outcome = outcome_from(
            &context,
            context.pending().admission_request_id(),
            context.conflict_hashes().to_vec(),
        );
        let resolution = CommandAttemptResolution::OutcomeReplay(outcome);
        let debug = format!("{resolution:?}");
        assert_eq!(debug, "CommandAttemptResolution::OutcomeReplay([REDACTED])");
        assert!(!debug.contains(PRINCIPAL));
        assert!(!debug.contains(SENSITIVE_MARKER));
    }
}
