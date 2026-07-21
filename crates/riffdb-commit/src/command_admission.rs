//! Synchronous idempotency admission inside the sole-writer coordinator actor.

use std::{error::Error, fmt, time::Instant};

use riffdb_catalog::ResolvedExecutablePlan;
use riffdb_conflict::CancellationToken;
use riffdb_idempotency::{
    IdempotencyRecheckError, IdempotencyRecheckExecutor, IdempotencyRecheckResultV1,
    RecheckedPendingAdmissionV1, VacantIdempotencyAdmissionV1,
};
use riffdb_invariant::InputDerivedCommandFacts;
use riffdb_policy::AuthorizedCommandExecution;
use riffdb_storage_api::{
    AdmissionRepository, AdmissionRequestV1, AdmissionResultV1, EntityTarget,
    IdempotencyLookupCandidatesV1, PreEvaluationCommitContext, SnapshotRequest, StorageError,
    StoredAdmittedProvenanceClaimsV1, StoredExecutionFailedV1, StoredOutcomeV1,
    StoredPendingAdmissionV1,
};
use riffdb_types::{
    CanonicalRecord, ConflictKey, ConflictKeyHash, EntityKey, EntityTypeId, LogicalTime,
    MAX_COMMAND_CONFLICT_KEYS_V1, RequestId, hash_conflict_key, hash_partition_key,
};

use crate::{AdmissionClock, AdmissionClockError, CommandExecutionPreparation};

/// Closed result of the transaction-adjacent command admission reducer.
pub(crate) enum CommandAdmissionResult {
    /// One exact admitted or resumed command may proceed to capability acquisition.
    Execute(Box<CommandExecutionCandidate>),
    /// A committed equal-input outcome must be replayed unchanged.
    Outcome(StoredOutcomeV1),
    /// A terminal deterministic execution failure must be replayed unchanged.
    ExecutionFailed(StoredExecutionFailedV1),
    /// Durable state selected another immutable historical plan.
    PreparationChanged,
    /// The same plan and idempotency identity retained another canonical input.
    InputMismatch,
}

impl fmt::Debug for CommandAdmissionResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Execute(_) => "Execute([REDACTED])",
            Self::Outcome(_) => "Outcome([REDACTED])",
            Self::ExecutionFailed(_) => "ExecutionFailed([REDACTED])",
            Self::PreparationChanged => "PreparationChanged",
            Self::InputMismatch => "InputMismatch",
        })
    }
}

/// Failure before an execution candidate can enter capability acquisition.
pub(crate) enum CommandAdmissionError {
    /// The read-only transaction-adjacent idempotency recheck failed.
    Recheck(IdempotencyRecheckError),
    /// The one permitted clock sample for a new admission failed.
    Clock(AdmissionClockError),
    /// Independently checked state could not be lowered consistently.
    Integrity,
    /// The one mutating pending-admission transition failed.
    AdmissionWrite(StorageError),
}

impl fmt::Debug for CommandAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CommandAdmissionError([REDACTED])")
    }
}

impl fmt::Display for CommandAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("command admission could not be completed")
    }
}

impl Error for CommandAdmissionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Recheck(error) => Some(error),
            Self::Clock(error) => Some(error),
            Self::AdmissionWrite(error) => Some(error),
            Self::Integrity => None,
        }
    }
}

/// Private proof that snapshot targets came from one exact plan/facts join.
struct CommandSnapshotRequestProof {
    request: SnapshotRequest,
}

impl CommandSnapshotRequestProof {
    fn request_for_attempt(&self) -> SnapshotRequest {
        self.request.clone()
    }
}

impl fmt::Debug for CommandSnapshotRequestProof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CommandSnapshotRequestProof([REDACTED])")
    }
}

/// Move-only command state ready for later conflict acquisition and evaluation.
pub(crate) struct CommandExecutionCandidate {
    resolved_plan: ResolvedExecutablePlan,
    normalized_input: CanonicalRecord,
    commit_context: PreEvaluationCommitContext,
    raw_conflict_keys: Vec<ConflictKey>,
    snapshot: CommandSnapshotRequestProof,
    invocation_request_id: RequestId,
    deadline: Instant,
    cancellation: CancellationToken,
}

impl CommandExecutionCandidate {
    pub(crate) const fn resolved_plan(&self) -> &ResolvedExecutablePlan {
        &self.resolved_plan
    }

    pub(crate) const fn normalized_input(&self) -> &CanonicalRecord {
        &self.normalized_input
    }

    pub(crate) const fn commit_context(&self) -> &PreEvaluationCommitContext {
        &self.commit_context
    }

    pub(crate) fn raw_conflict_keys(&self) -> &[ConflictKey] {
        &self.raw_conflict_keys
    }

    pub(crate) fn snapshot_request_for_attempt(&self) -> SnapshotRequest {
        self.snapshot.request_for_attempt()
    }

    pub(crate) const fn invocation_request_id(&self) -> RequestId {
        self.invocation_request_id
    }

    pub(crate) fn into_acquisition_parts(
        self,
    ) -> (
        ResolvedExecutablePlan,
        CanonicalRecord,
        PreEvaluationCommitContext,
        Vec<ConflictKey>,
        SnapshotRequest,
        RequestId,
        Instant,
        CancellationToken,
    ) {
        (
            self.resolved_plan,
            self.normalized_input,
            self.commit_context,
            self.raw_conflict_keys,
            self.snapshot.request,
            self.invocation_request_id,
            self.deadline,
            self.cancellation,
        )
    }

    pub(crate) fn lookup_candidates_for_resolution(
        &self,
    ) -> Result<IdempotencyLookupCandidatesV1, CommandAdmissionError> {
        IdempotencyLookupCandidatesV1::new(vec![self.commit_context.pending().identity().clone()])
            .map_err(|_| CommandAdmissionError::Integrity)
    }
}

impl fmt::Debug for CommandExecutionCandidate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CommandExecutionCandidate([REDACTED])")
    }
}

struct LoweredPreparation {
    resolved_plan: ResolvedExecutablePlan,
    normalized_input: CanonicalRecord,
    raw_conflict_keys: Vec<ConflictKey>,
    conflict_hashes: Vec<ConflictKeyHash>,
    snapshot: CommandSnapshotRequestProof,
    invocation_request_id: RequestId,
    deadline: Instant,
    cancellation: CancellationToken,
}

struct AdmissionPreparationParts {
    resolved_plan: ResolvedExecutablePlan,
    normalized_input: CanonicalRecord,
    input_facts: InputDerivedCommandFacts,
    authorization: AuthorizedCommandExecution,
    request_id: RequestId,
    deadline: Instant,
    cancellation: CancellationToken,
}

/// Reduces one exact command preparation to replay, retry, or execution state.
///
/// The idempotency recheck is always the first external call. Only a stable
/// vacant observation samples the clock and invokes the mutating admission port.
pub(crate) fn reduce_command_admission(
    repository: &dyn AdmissionRepository,
    clock: &dyn AdmissionClock,
    preparation: CommandExecutionPreparation,
) -> Result<CommandAdmissionResult, CommandAdmissionError> {
    reduce_command_admission_with_hash(repository, clock, preparation, &hash_conflict_key)
}

fn reduce_command_admission_with_hash(
    repository: &dyn AdmissionRepository,
    clock: &dyn AdmissionClock,
    preparation: CommandExecutionPreparation,
    conflict_hasher: &dyn Fn(&[u8]) -> ConflictKeyHash,
) -> Result<CommandAdmissionResult, CommandAdmissionError> {
    let crate::command_preparation::CommandExecutionPreparationParts {
        resolved_plan,
        normalized_input,
        idempotency,
        input_facts,
        authorization,
        request_id,
        deadline,
        cancellation,
    } = preparation.into_parts();
    let parts = AdmissionPreparationParts {
        resolved_plan,
        normalized_input,
        input_facts,
        authorization,
        request_id,
        deadline,
        cancellation,
    };
    let rechecked = IdempotencyRecheckExecutor::new(repository)
        .recheck(idempotency)
        .map_err(CommandAdmissionError::Recheck)?;

    match rechecked {
        IdempotencyRecheckResultV1::Vacant(vacant) => {
            admit_vacant(repository, clock, parts, vacant, conflict_hasher)
        }
        IdempotencyRecheckResultV1::Pending(pending) => {
            resume_pending(parts, pending, conflict_hasher)
        }
        IdempotencyRecheckResultV1::Outcome(outcome) => {
            if outcome.partition_key() != parts.input_facts.partition_key() {
                return Err(CommandAdmissionError::Integrity);
            }
            Ok(CommandAdmissionResult::Outcome(outcome))
        }
        IdempotencyRecheckResultV1::ExecutionFailed(failure) => {
            if failure.pending().partition_key() != parts.input_facts.partition_key() {
                return Err(CommandAdmissionError::Integrity);
            }
            Ok(CommandAdmissionResult::ExecutionFailed(failure))
        }
        IdempotencyRecheckResultV1::PreparationChanged => {
            Ok(CommandAdmissionResult::PreparationChanged)
        }
        IdempotencyRecheckResultV1::InputMismatch => Ok(CommandAdmissionResult::InputMismatch),
    }
}

fn admit_vacant(
    repository: &dyn AdmissionRepository,
    clock: &dyn AdmissionClock,
    parts: AdmissionPreparationParts,
    vacant: VacantIdempotencyAdmissionV1,
    conflict_hasher: &dyn Fn(&[u8]) -> ConflictKeyHash,
) -> Result<CommandAdmissionResult, CommandAdmissionError> {
    let (selected_plan, normalized_input, prepared_idempotency) = vacant.into_parts();
    if &selected_plan != parts.resolved_plan.reference()
        || normalized_input != parts.normalized_input
    {
        return Err(CommandAdmissionError::Integrity);
    }

    let actor = parts.authorization.actor().clone();
    let provenance_claims = lower_provenance_claims(&parts.authorization)?;
    let partition_key = parts.input_facts.partition_key().clone();
    let mut lowered = lower_preparation(parts, normalized_input, conflict_hasher)?;
    let canonical_input_hash = prepared_idempotency.canonical_input_hash();
    let lookup_candidates = prepared_idempotency.lookup_candidates().clone();
    let identity = prepared_idempotency.current_identity().clone();

    let logical_time = LogicalTime::new(clock.now().map_err(CommandAdmissionError::Clock)?);
    let pending = StoredPendingAdmissionV1::new(
        identity,
        canonical_input_hash,
        lowered.invocation_request_id,
        selected_plan,
        logical_time,
        actor,
        partition_key,
        provenance_claims,
    )
    .map_err(|_| CommandAdmissionError::Integrity)?;
    let context = PreEvaluationCommitContext::new(
        pending.clone(),
        hash_partition_key(pending.partition_key().as_bytes()),
        std::mem::take(&mut lowered.conflict_hashes),
    )
    .map_err(|_| CommandAdmissionError::Integrity)?;
    let request = AdmissionRequestV1::new(lookup_candidates, &context)
        .map_err(|_| CommandAdmissionError::Integrity)?;

    match repository
        .admit_or_resolve(request)
        .map_err(CommandAdmissionError::AdmissionWrite)?
    {
        AdmissionResultV1::Created(created) if created == pending => Ok(
            CommandAdmissionResult::Execute(Box::new(candidate(lowered, context))),
        ),
        AdmissionResultV1::Created(_)
        | AdmissionResultV1::Resumed(_)
        | AdmissionResultV1::StoredOutcome(_)
        | AdmissionResultV1::ExecutionFailed(_)
        | AdmissionResultV1::InputMismatch
        | AdmissionResultV1::MultipleMatches => Err(CommandAdmissionError::Integrity),
    }
}

fn resume_pending(
    parts: AdmissionPreparationParts,
    rechecked: RecheckedPendingAdmissionV1,
    conflict_hasher: &dyn Fn(&[u8]) -> ConflictKeyHash,
) -> Result<CommandAdmissionResult, CommandAdmissionError> {
    let (pending, normalized_input) = rechecked.into_parts();
    if normalized_input != parts.normalized_input
        || pending.plan() != parts.resolved_plan.reference()
        || pending.partition_key() != parts.input_facts.partition_key()
    {
        return Err(CommandAdmissionError::Integrity);
    }
    let mut lowered = lower_preparation(parts, normalized_input, conflict_hasher)?;
    let context = PreEvaluationCommitContext::new(
        pending.clone(),
        hash_partition_key(pending.partition_key().as_bytes()),
        std::mem::take(&mut lowered.conflict_hashes),
    )
    .map_err(|_| CommandAdmissionError::Integrity)?;
    Ok(CommandAdmissionResult::Execute(Box::new(candidate(
        lowered, context,
    ))))
}

fn lower_preparation(
    parts: AdmissionPreparationParts,
    normalized_input: CanonicalRecord,
    conflict_hasher: &dyn Fn(&[u8]) -> ConflictKeyHash,
) -> Result<LoweredPreparation, CommandAdmissionError> {
    let (raw_conflict_keys, conflict_hashes) =
        lower_conflict_keys(parts.input_facts.declared_conflict_keys(), conflict_hasher)?;
    let snapshot = lower_snapshot(&parts.resolved_plan, &parts.input_facts)?;
    Ok(LoweredPreparation {
        resolved_plan: parts.resolved_plan,
        normalized_input,
        raw_conflict_keys,
        conflict_hashes,
        snapshot,
        invocation_request_id: parts.request_id,
        deadline: parts.deadline,
        cancellation: parts.cancellation,
    })
}

fn candidate(
    lowered: LoweredPreparation,
    commit_context: PreEvaluationCommitContext,
) -> CommandExecutionCandidate {
    CommandExecutionCandidate {
        resolved_plan: lowered.resolved_plan,
        normalized_input: lowered.normalized_input,
        commit_context,
        raw_conflict_keys: lowered.raw_conflict_keys,
        snapshot: lowered.snapshot,
        invocation_request_id: lowered.invocation_request_id,
        deadline: lowered.deadline,
        cancellation: lowered.cancellation,
    }
}

fn lower_provenance_claims(
    authorization: &AuthorizedCommandExecution,
) -> Result<StoredAdmittedProvenanceClaimsV1, CommandAdmissionError> {
    let claims = authorization.provenance();
    StoredAdmittedProvenanceClaimsV1::new(
        claims.source_repository().cloned(),
        claims.source_commit().cloned(),
        claims.reason().cloned(),
        claims.approval_id().cloned(),
    )
    .map_err(|_| CommandAdmissionError::Integrity)
}

fn lower_conflict_keys(
    declared: &[ConflictKey],
    hash: &dyn Fn(&[u8]) -> ConflictKeyHash,
) -> Result<(Vec<ConflictKey>, Vec<ConflictKeyHash>), CommandAdmissionError> {
    let mut raw = declared.to_vec();
    raw.sort_unstable();
    raw.dedup();
    if raw.is_empty() || raw.len() > MAX_COMMAND_CONFLICT_KEYS_V1 {
        return Err(CommandAdmissionError::Integrity);
    }

    let mut hashes = raw
        .iter()
        .map(|key| hash(key.as_bytes()))
        .collect::<Vec<_>>();
    hashes.sort_unstable();
    if hashes.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(CommandAdmissionError::Integrity);
    }
    Ok((raw, hashes))
}

fn lower_snapshot(
    resolved_plan: &ResolvedExecutablePlan,
    facts: &InputDerivedCommandFacts,
) -> Result<CommandSnapshotRequestProof, CommandAdmissionError> {
    let binding_types = resolved_plan
        .plan()
        .bindings()
        .iter()
        .map(|binding| binding.entity_type())
        .collect::<Vec<_>>();
    let root_types = resolved_plan
        .plan()
        .root_validation_reads()
        .iter()
        .map(|read| read.entity_type())
        .collect::<Vec<_>>();
    lower_snapshot_parts(
        resolved_plan.reference(),
        &binding_types,
        facts.binding_entity_keys(),
        &root_types,
        facts.root_validation_entity_keys(),
    )
}

fn lower_snapshot_parts(
    plan: &riffdb_storage_api::ExecutablePlanRef,
    binding_types: &[EntityTypeId],
    binding_keys: &[EntityKey],
    root_types: &[EntityTypeId],
    root_keys: &[EntityKey],
) -> Result<CommandSnapshotRequestProof, CommandAdmissionError> {
    if binding_types.len() != binding_keys.len() || root_types.len() != root_keys.len() {
        return Err(CommandAdmissionError::Integrity);
    }
    let binding_targets = binding_types
        .iter()
        .copied()
        .zip(binding_keys.iter().cloned())
        .map(|(entity_type, key)| EntityTarget::new(entity_type, key))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| CommandAdmissionError::Integrity)?;
    let root_validation_targets = root_types
        .iter()
        .copied()
        .zip(root_keys.iter().cloned())
        .map(|(entity_type, key)| EntityTarget::new(entity_type, key))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| CommandAdmissionError::Integrity)?;
    let request = SnapshotRequest::new(
        plan.clone(),
        binding_targets,
        root_validation_targets,
        Vec::new(),
    )
    .map_err(|_| CommandAdmissionError::Integrity)?;
    Ok(CommandSnapshotRequestProof { request })
}

#[cfg(test)]
mod tests {
    use std::{
        cell::{Cell, RefCell},
        collections::BTreeMap,
        num::NonZeroU16,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use riffdb_catalog::ValidatedContractBundle;
    use riffdb_contract_ir::{CommandPlan, RecordSchema};
    use riffdb_idempotency::{
        CommandIdempotencyScopeV1, IdempotencyDigestCandidatesV1, IdempotencyDigestError,
        IdempotencyDigestProvider, IdempotencyInspectionExecutor, prepare_command_idempotency,
        prepare_idempotency_lookup,
    };
    use riffdb_invariant::derive_input_command_facts;
    use riffdb_policy::{
        AgentSessionAdmissionPolicy, AuthorizationClock, AuthorizationClockError,
        CommandExecutionClass, CurrentAuthorizer, Decision, NoopAuthorizationTelemetry,
        OperationRequest, UntrustedInvocationClaims,
    };
    use riffdb_storage_api::{
        AdmissionLookupResultV1, DeclaredOutcome, DurabilityMode, ExecutablePlanRef,
        IdempotencyIdentity, IdempotencyKeyDigest, StoredAdmissionStateV1,
    };
    use riffdb_testkit::authorization::{
        AuthorizationFixture, AuthorizationFixtureConfig, AuthorizationFixtureTimes,
    };
    use riffdb_types::{
        ActorId, ActorKind, AggregateTypeId, Audience, CanonicalInputHash, CanonicalValue,
        CapabilityGrantV1, CapabilityPermissionV1, CapabilityPermissionsV1, CommitSequence,
        DatabaseId, Decimal, DecimalSpec, DigestKeyId, Environment, ExecutionFailureCode,
        IdempotencyKey, OutcomeId, PartitionScopeV1, ProvenanceId, RequestId, SourceCommit,
        SourceRepository, TenantScope, Timestamp,
    };

    use super::*;
    use crate::CommandRequestControl;

    const CALLER_KEY: &str = "admission-caller-secret-canary";
    const PRINCIPAL: &str = "admission-principal-secret-canary";

    struct CommandFixture {
        resolved: ResolvedExecutablePlan,
        reference: ExecutablePlanRef,
        normalized_input: CanonicalRecord,
        partition: riffdb_types::PartitionKey,
    }

    struct FixedAuthorizationClock(Timestamp);

    impl AuthorizationClock for FixedAuthorizationClock {
        fn now(&self) -> Result<Timestamp, AuthorizationClockError> {
            Ok(self.0)
        }
    }

    struct FixedDigestProvider;

    impl IdempotencyDigestProvider for FixedDigestProvider {
        fn digest_candidates(
            &self,
            _: &IdempotencyKey,
        ) -> Result<IdempotencyDigestCandidatesV1, IdempotencyDigestError> {
            IdempotencyDigestCandidatesV1::new(vec![IdempotencyKeyDigest::from_hmac_bytes(
                DigestKeyId::new(1).expect("digest key ID"),
                [0x51; 32],
            )])
        }
    }

    struct ObservationRepository(AdmissionLookupResultV1);

    impl AdmissionRepository for ObservationRepository {
        fn admit_or_resolve(
            &self,
            _: AdmissionRequestV1,
        ) -> Result<AdmissionResultV1, StorageError> {
            panic!("inspection must not mutate admission state")
        }

        fn lookup_admission(
            &self,
            _: IdempotencyLookupCandidatesV1,
        ) -> Result<AdmissionLookupResultV1, StorageError> {
            Ok(self.0.clone())
        }
    }

    #[derive(Clone, Copy)]
    enum AdmitBehavior {
        EchoCreated,
        DifferentCreated,
        EchoResumed,
        EchoOutcome,
        EchoExecutionFailed,
        InputMismatch,
        MultipleMatches,
        Error(riffdb_storage_api::StorageErrorKind),
    }

    struct ScriptedRepository {
        lookup: Result<AdmissionLookupResultV1, StorageError>,
        behavior: AdmitBehavior,
        lookup_calls: Cell<usize>,
        admission_calls: Cell<usize>,
        admitted_request: RefCell<Option<AdmissionRequestV1>>,
    }

    impl ScriptedRepository {
        fn new(lookup: AdmissionLookupResultV1, behavior: AdmitBehavior) -> Self {
            Self {
                lookup: Ok(lookup),
                behavior,
                lookup_calls: Cell::new(0),
                admission_calls: Cell::new(0),
                admitted_request: RefCell::new(None),
            }
        }

        fn read_error(kind: riffdb_storage_api::StorageErrorKind) -> Self {
            Self {
                lookup: Err(StorageError::new(kind, None)),
                behavior: AdmitBehavior::EchoCreated,
                lookup_calls: Cell::new(0),
                admission_calls: Cell::new(0),
                admitted_request: RefCell::new(None),
            }
        }
    }

    impl AdmissionRepository for ScriptedRepository {
        fn admit_or_resolve(
            &self,
            request: AdmissionRequestV1,
        ) -> Result<AdmissionResultV1, StorageError> {
            self.admission_calls.set(self.admission_calls.get() + 1);
            self.admitted_request.replace(Some(request.clone()));
            let proposed = request.proposed_pending();
            match self.behavior {
                AdmitBehavior::EchoCreated => Ok(AdmissionResultV1::Created(proposed.clone())),
                AdmitBehavior::DifferentCreated => Ok(AdmissionResultV1::Created(
                    pending_with_request(proposed, request_id(99)),
                )),
                AdmitBehavior::EchoResumed => Ok(AdmissionResultV1::Resumed(proposed.clone())),
                AdmitBehavior::EchoOutcome => {
                    Ok(AdmissionResultV1::StoredOutcome(outcome_from(proposed)))
                }
                AdmitBehavior::EchoExecutionFailed => Ok(AdmissionResultV1::ExecutionFailed(
                    StoredExecutionFailedV1::new(
                        proposed.clone(),
                        ExecutionFailureCode::ResourceLimit,
                    ),
                )),
                AdmitBehavior::InputMismatch => Ok(AdmissionResultV1::InputMismatch),
                AdmitBehavior::MultipleMatches => Ok(AdmissionResultV1::MultipleMatches),
                AdmitBehavior::Error(kind) => Err(StorageError::new(kind, None)),
            }
        }

        fn lookup_admission(
            &self,
            _: IdempotencyLookupCandidatesV1,
        ) -> Result<AdmissionLookupResultV1, StorageError> {
            self.lookup_calls.set(self.lookup_calls.get() + 1);
            self.lookup.clone()
        }
    }

    struct ScriptedClock {
        result: Result<Timestamp, AdmissionClockError>,
        calls: AtomicUsize,
    }

    impl ScriptedClock {
        fn fixed(value: Timestamp) -> Self {
            Self {
                result: Ok(value),
                calls: AtomicUsize::new(0),
            }
        }

        fn failing() -> Self {
            Self {
                result: Err(AdmissionClockError),
                calls: AtomicUsize::new(0),
            }
        }
    }

    impl AdmissionClock for ScriptedClock {
        fn now(&self) -> Result<Timestamp, AdmissionClockError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.result
        }
    }

    fn timestamp(seconds: i64) -> Timestamp {
        Timestamp::new(seconds, 17).expect("canonical timestamp")
    }

    fn database(seed: u8) -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(u64::from(seed) + 1, [seed; 10])
            .expect("UUIDv7 database ID")
    }

    fn request_id(seed: u8) -> RequestId {
        RequestId::from_unix_milliseconds_and_random(u64::from(seed) + 1, [seed; 10])
            .expect("UUIDv7 request ID")
    }

    fn provenance_id(seed: u8) -> ProvenanceId {
        ProvenanceId::from_unix_milliseconds_and_random(u64::from(seed) + 1, [seed; 10])
            .expect("UUIDv7 provenance ID")
    }

    fn environment() -> Environment {
        Environment::new("development").expect("bounded environment")
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

    fn fixture() -> CommandFixture {
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
        let normalized_input = normalized_input(plan);
        let facts = derive_input_command_facts(plan, normalized_input.clone())
            .expect("input-derived command facts");
        let partition = facts.partition_key().clone();
        let resolved = bundle
            .resolve_plan(&reference)
            .expect("exact checked plan resolves");
        CommandFixture {
            resolved,
            reference,
            normalized_input,
            partition,
        }
    }

    fn normalized_input(plan: &CommandPlan) -> CanonicalRecord {
        input_record(
            plan.input().record(),
            [
                (
                    "idempotency_key",
                    CanonicalValue::string(CALLER_KEY).expect("bounded caller key"),
                ),
                ("organization_id", CanonicalValue::Uuid([0x31; 16])),
                ("fiscal_year", CanonicalValue::I64(2026)),
                ("approved_amount", decimal(12_500)),
            ],
        )
    }

    fn scope(command: &CommandFixture) -> CommandIdempotencyScopeV1 {
        CommandIdempotencyScopeV1::new(
            database(1),
            environment(),
            TenantScope::Global,
            ActorId::new(PRINCIPAL).expect("bounded principal"),
            command.reference.contract_lineage().clone(),
            command.reference.command_id(),
        )
    }

    fn identity(command: &CommandFixture) -> IdempotencyIdentity {
        IdempotencyIdentity::new(
            database(1),
            environment(),
            TenantScope::Global,
            ActorId::new(PRINCIPAL).expect("bounded principal"),
            command.reference.contract_lineage().clone(),
            command.reference.command_id(),
            IdempotencyKeyDigest::from_hmac_bytes(
                DigestKeyId::new(1).expect("digest key ID"),
                [0x51; 32],
            ),
        )
    }

    fn input_hash(command: &CommandFixture) -> CanonicalInputHash {
        prepare_command_idempotency(
            &scope(command),
            &command.normalized_input,
            command
                .resolved
                .plan()
                .idempotency_input()
                .expect("mutation idempotency field"),
            &IdempotencyKey::new(CALLER_KEY).expect("bounded caller key"),
            &FixedDigestProvider,
        )
        .expect("idempotency preparation")
        .canonical_input_hash()
    }

    fn actor(kind: ActorKind) -> riffdb_types::AdmittedActorContext {
        riffdb_types::AdmittedActorContext::new(
            ActorId::new(PRINCIPAL).expect("bounded principal"),
            kind,
            TenantScope::Global,
            None,
        )
    }

    fn pending(
        command: &CommandFixture,
        plan: ExecutablePlanRef,
        canonical_input_hash: CanonicalInputHash,
        admission_request_id: RequestId,
        logical_time: LogicalTime,
        admitted_actor: riffdb_types::AdmittedActorContext,
        claims: StoredAdmittedProvenanceClaimsV1,
    ) -> StoredPendingAdmissionV1 {
        StoredPendingAdmissionV1::new(
            identity(command),
            canonical_input_hash,
            admission_request_id,
            plan,
            logical_time,
            admitted_actor,
            command.partition.clone(),
            claims,
        )
        .expect("valid pending admission")
    }

    fn pending_with_request(
        pending: &StoredPendingAdmissionV1,
        request: RequestId,
    ) -> StoredPendingAdmissionV1 {
        StoredPendingAdmissionV1::new(
            pending.identity().clone(),
            pending.canonical_input_hash(),
            request,
            pending.plan().clone(),
            pending.logical_time(),
            pending.actor().clone(),
            pending.partition_key().clone(),
            pending.provenance_claims().clone(),
        )
        .expect("changed pending request")
    }

    fn outcome_from(pending: &StoredPendingAdmissionV1) -> StoredOutcomeV1 {
        let partition_hash = hash_partition_key(pending.partition_key().as_bytes());
        StoredOutcomeV1::new(
            pending.identity().clone(),
            CommitSequence::new(3).expect("commit sequence"),
            pending.admission_request_id(),
            pending.plan().clone(),
            pending.canonical_input_hash(),
            pending.actor().clone(),
            pending.logical_time(),
            pending.partition_key().clone(),
            partition_hash,
            Vec::new(),
            DeclaredOutcome::new(
                OutcomeId::first(),
                CanonicalRecord::new(Vec::new()).expect("empty outcome record"),
            )
            .expect("declared outcome"),
            pending.provenance_claims().clone(),
            provenance_id(4),
            DurabilityMode::Memory,
        )
        .expect("stored outcome")
    }

    fn authorized(command: &CommandFixture) -> AuthorizedCommandExecution {
        let grant = CapabilityGrantV1::new(
            TenantScope::Global,
            PartitionScopeV1::All,
            CapabilityPermissionsV1::new(vec![CapabilityPermissionV1::InvokeCommand(
                command.reference.contract_lineage().clone(),
                command.reference.command_id(),
            )])
            .expect("permissions"),
            Vec::new(),
            NonZeroU16::new(10).expect("row bound"),
            Vec::new(),
        )
        .expect("grant");
        let fixture = AuthorizationFixture::new(AuthorizationFixtureConfig::new(
            database(1),
            environment(),
            ActorId::new(PRINCIPAL).expect("bounded principal"),
            ActorKind::Agent,
            Audience::new("riffdb-command-admission").expect("bounded audience"),
            AuthorizationFixtureTimes::new(timestamp(100), timestamp(300), timestamp(150)),
            grant,
        ))
        .expect("authorization fixture");
        let resolver = fixture.current_capability_resolver();
        let clock = FixedAuthorizationClock(timestamp(200));
        let decision = CurrentAuthorizer::new(
            &resolver,
            &clock,
            &NoopAuthorizationTelemetry,
            database(1),
            environment(),
        )
        .authorize(
            fixture.authenticated_principal(),
            OperationRequest::execute_command(
                command.reference.contract_lineage().clone(),
                command.reference.contract_version(),
                command.reference.command_id(),
                CommandExecutionClass::Mutation,
                command.partition.clone(),
            ),
        )
        .expect("policy decision");
        let Decision::Allow(proof) = decision else {
            panic!("fixture command must be allowed");
        };
        proof
            .into_command_execution(
                UntrustedInvocationClaims::new(None, None, None, None, None),
                AgentSessionAdmissionPolicy::Discard,
            )
            .expect("command authorization")
    }

    fn preparation(
        command: &CommandFixture,
        original: AdmissionLookupResultV1,
        invocation_request_id: RequestId,
    ) -> CommandExecutionPreparation {
        let caller_key = IdempotencyKey::new(CALLER_KEY).expect("bounded caller key");
        let lookup = prepare_idempotency_lookup(&scope(command), &caller_key, &FixedDigestProvider)
            .expect("idempotency lookup preparation");
        let inspection = IdempotencyInspectionExecutor::new(&ObservationRepository(original))
            .inspect(lookup)
            .expect("idempotency inspection");
        let idempotency = inspection
            .confirm_input(
                &command.normalized_input,
                command
                    .resolved
                    .plan()
                    .idempotency_input()
                    .expect("mutation idempotency field"),
                &caller_key,
            )
            .expect("input confirmation")
            .bind_selected_plan(command.reference.clone())
            .expect("selected plan binding");
        let facts =
            derive_input_command_facts(command.resolved.plan(), command.normalized_input.clone())
                .expect("input-derived facts");
        let (control, _) = CommandRequestControl::new(
            Instant::now()
                .checked_add(Duration::from_secs(30))
                .expect("future deadline"),
        );
        CommandExecutionPreparation::new(
            database(1),
            &environment(),
            command.resolved.clone(),
            command.normalized_input.clone(),
            idempotency,
            facts,
            authorized(command),
            invocation_request_id,
            control,
        )
        .expect("exact command preparation")
    }

    #[test]
    fn vacant_freezes_exact_pending_once_and_builds_bound_candidate() {
        let command = fixture();
        let invocation = request_id(7);
        let admitted_at = timestamp(777);
        let repository = ScriptedRepository::new(
            AdmissionLookupResultV1::NotFound,
            AdmitBehavior::EchoCreated,
        );
        let clock = ScriptedClock::fixed(admitted_at);

        let result = reduce_command_admission(
            &repository,
            &clock,
            preparation(&command, AdmissionLookupResultV1::NotFound, invocation),
        )
        .expect("new admission");
        let CommandAdmissionResult::Execute(candidate) = result else {
            panic!("vacant identity must produce an execution candidate");
        };

        assert_eq!(repository.lookup_calls.get(), 1);
        assert_eq!(repository.admission_calls.get(), 1);
        assert_eq!(clock.calls.load(Ordering::Relaxed), 1);
        let admitted = repository.admitted_request.borrow();
        let request = admitted.as_ref().expect("captured admission request");
        let proposed = request.proposed_pending();
        assert_eq!(proposed.admission_request_id(), invocation);
        assert_eq!(proposed.plan(), &command.reference);
        assert_eq!(proposed.logical_time(), LogicalTime::new(admitted_at));
        assert_eq!(proposed.actor(), &actor(ActorKind::Agent));
        assert_eq!(proposed.partition_key(), &command.partition);
        assert_eq!(proposed.provenance_claims(), &Default::default());
        assert_eq!(proposed.canonical_input_hash(), input_hash(&command));
        assert_eq!(candidate.commit_context().pending(), proposed);
        assert_eq!(candidate.invocation_request_id(), invocation);
        assert_eq!(candidate.resolved_plan().reference(), &command.reference);
        assert_eq!(candidate.normalized_input(), &command.normalized_input);
        assert_eq!(
            candidate
                .lookup_candidates_for_resolution()
                .expect("singleton resolution lookup")
                .as_slice(),
            &[proposed.identity().clone()]
        );

        let facts =
            derive_input_command_facts(command.resolved.plan(), command.normalized_input.clone())
                .expect("facts");
        let mut expected_raw = facts.declared_conflict_keys().to_vec();
        expected_raw.sort_unstable();
        expected_raw.dedup();
        assert_eq!(candidate.raw_conflict_keys(), expected_raw);
        let snapshot = candidate.snapshot_request_for_attempt();
        assert_eq!(snapshot.plan(), &command.reference);
        assert!(snapshot.range_targets().is_empty());
        assert_eq!(
            snapshot.binding_targets().len(),
            command.resolved.plan().bindings().len()
        );
        assert_eq!(
            snapshot.binding_targets()[0].key(),
            &facts.binding_entity_keys()[0]
        );
    }

    #[test]
    fn pending_resume_reuses_every_stored_admission_fact_without_clock_or_write() {
        let command = fixture();
        let claims = StoredAdmittedProvenanceClaimsV1::new(
            Some(SourceRepository::new("secret/repository").expect("repository")),
            Some(SourceCommit::new("abc123").expect("commit")),
            None,
            None,
        )
        .expect("stored claims");
        let stored = pending(
            &command,
            command.reference.clone(),
            input_hash(&command),
            request_id(8),
            LogicalTime::new(timestamp(888)),
            actor(ActorKind::Service),
            claims,
        );
        let state = StoredAdmissionStateV1::Pending(stored.clone());
        let repository = ScriptedRepository::new(
            AdmissionLookupResultV1::Found(Box::new(state.clone())),
            AdmitBehavior::EchoCreated,
        );
        let clock = ScriptedClock::fixed(timestamp(999));
        let current_invocation = request_id(9);

        let result = reduce_command_admission(
            &repository,
            &clock,
            preparation(
                &command,
                AdmissionLookupResultV1::Found(Box::new(state)),
                current_invocation,
            ),
        )
        .expect("pending resume");
        let CommandAdmissionResult::Execute(candidate) = result else {
            panic!("pending state must resume execution");
        };

        assert_eq!(repository.lookup_calls.get(), 1);
        assert_eq!(repository.admission_calls.get(), 0);
        assert_eq!(clock.calls.load(Ordering::Relaxed), 0);
        assert_eq!(candidate.commit_context().pending(), &stored);
        assert_eq!(
            candidate.commit_context().pending().admission_request_id(),
            request_id(8)
        );
        assert_eq!(candidate.invocation_request_id(), current_invocation);
        assert_eq!(
            candidate.commit_context().pending().actor(),
            &actor(ActorKind::Service)
        );
        assert_eq!(
            candidate.commit_context().pending().logical_time(),
            LogicalTime::new(timestamp(888))
        );
        assert_eq!(
            candidate.commit_context().pending().provenance_claims(),
            stored.provenance_claims()
        );
        assert_eq!(
            candidate
                .lookup_candidates_for_resolution()
                .expect("singleton resolution lookup")
                .as_slice(),
            &[stored.identity().clone()]
        );
    }

    #[test]
    fn terminal_and_mismatch_dispositions_never_sample_clock_or_write() {
        let command = fixture();
        let base = pending(
            &command,
            command.reference.clone(),
            input_hash(&command),
            request_id(10),
            LogicalTime::new(timestamp(10)),
            actor(ActorKind::Agent),
            StoredAdmittedProvenanceClaimsV1::default(),
        );
        let outcome = outcome_from(&base);
        let failure =
            StoredExecutionFailedV1::new(base.clone(), ExecutionFailureCode::ArithmeticFault);

        let cases = [
            StoredAdmissionStateV1::StoredOutcome(outcome.clone()),
            StoredAdmissionStateV1::ExecutionFailed(failure.clone()),
        ];
        for (index, state) in cases.into_iter().enumerate() {
            let repository = ScriptedRepository::new(
                AdmissionLookupResultV1::Found(Box::new(state)),
                AdmitBehavior::EchoCreated,
            );
            let clock = ScriptedClock::fixed(timestamp(20));
            let result = reduce_command_admission(
                &repository,
                &clock,
                preparation(
                    &command,
                    AdmissionLookupResultV1::NotFound,
                    request_id(20 + u8::try_from(index).expect("small index")),
                ),
            )
            .expect("terminal replay");
            match (index, result) {
                (0, CommandAdmissionResult::Outcome(actual)) => assert_eq!(actual, outcome),
                (1, CommandAdmissionResult::ExecutionFailed(actual)) => {
                    assert_eq!(actual, failure)
                }
                _ => panic!("wrong terminal disposition"),
            }
            assert_eq!(repository.lookup_calls.get(), 1);
            assert_eq!(repository.admission_calls.get(), 0);
            assert_eq!(clock.calls.load(Ordering::Relaxed), 0);
        }

        let changed_plan = ExecutablePlanRef::new(
            command.reference.contract_lineage().clone(),
            command.reference.contract_version(),
            command.reference.contract_bundle_hash(),
            command.reference.command_id(),
            riffdb_types::PlanHash::from_bytes([0xa1; 32]),
        );
        let changed = pending(
            &command,
            changed_plan,
            input_hash(&command),
            request_id(30),
            LogicalTime::new(timestamp(30)),
            actor(ActorKind::Agent),
            StoredAdmittedProvenanceClaimsV1::default(),
        );
        let mismatched = pending(
            &command,
            command.reference.clone(),
            CanonicalInputHash::from_bytes([0xb2; 32]),
            request_id(31),
            LogicalTime::new(timestamp(31)),
            actor(ActorKind::Agent),
            StoredAdmittedProvenanceClaimsV1::default(),
        );
        for (state, expect_changed) in [(changed, true), (mismatched, false)] {
            let repository = ScriptedRepository::new(
                AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::Pending(state))),
                AdmitBehavior::EchoCreated,
            );
            let clock = ScriptedClock::fixed(timestamp(40));
            let result = reduce_command_admission(
                &repository,
                &clock,
                preparation(&command, AdmissionLookupResultV1::NotFound, request_id(40)),
            )
            .expect("closed retry disposition");
            assert!(matches!(
                (expect_changed, result),
                (true, CommandAdmissionResult::PreparationChanged)
                    | (false, CommandAdmissionResult::InputMismatch)
            ));
            assert_eq!(repository.admission_calls.get(), 0);
            assert_eq!(clock.calls.load(Ordering::Relaxed), 0);
        }
    }

    #[test]
    fn terminal_replay_requires_the_exact_currently_authorized_partition() {
        let command = fixture();
        let mut wrong_partition = riffdb_types::PartitionKeyBuilder::new(AggregateTypeId::first());
        wrong_partition
            .push_u64(999)
            .expect("bounded partition component");
        let wrong_partition = wrong_partition.finish().expect("canonical partition");
        assert_ne!(wrong_partition, command.partition);
        let wrong_pending = StoredPendingAdmissionV1::new(
            identity(&command),
            input_hash(&command),
            request_id(41),
            command.reference.clone(),
            LogicalTime::new(timestamp(41)),
            actor(ActorKind::Agent),
            wrong_partition,
            StoredAdmittedProvenanceClaimsV1::default(),
        )
        .expect("structurally valid but plan-inconsistent partition");
        let cases = [
            StoredAdmissionStateV1::StoredOutcome(outcome_from(&wrong_pending)),
            StoredAdmissionStateV1::ExecutionFailed(StoredExecutionFailedV1::new(
                wrong_pending,
                ExecutionFailureCode::ArithmeticFault,
            )),
        ];

        for (index, state) in cases.into_iter().enumerate() {
            let repository = ScriptedRepository::new(
                AdmissionLookupResultV1::Found(Box::new(state)),
                AdmitBehavior::EchoCreated,
            );
            let clock = ScriptedClock::fixed(timestamp(42));
            let error = reduce_command_admission(
                &repository,
                &clock,
                preparation(
                    &command,
                    AdmissionLookupResultV1::NotFound,
                    request_id(42 + u8::try_from(index).expect("small index")),
                ),
            )
            .expect_err("terminal replay for another partition must fail closed");

            assert!(matches!(error, CommandAdmissionError::Integrity));
            assert_eq!(repository.lookup_calls.get(), 1);
            assert_eq!(repository.admission_calls.get(), 0);
            assert_eq!(clock.calls.load(Ordering::Relaxed), 0);
        }
    }

    #[test]
    fn recheck_clock_and_admission_write_failures_remain_distinct() {
        let command = fixture();
        let read_repository = ScriptedRepository::read_error(
            riffdb_storage_api::StorageErrorKind::CommitStatusUnknown,
        );
        let read_clock = ScriptedClock::fixed(timestamp(50));
        let read_error = reduce_command_admission(
            &read_repository,
            &read_clock,
            preparation(&command, AdmissionLookupResultV1::NotFound, request_id(50)),
        )
        .expect_err("recheck read must fail");
        assert!(matches!(read_error, CommandAdmissionError::Recheck(_)));
        assert_eq!(read_repository.admission_calls.get(), 0);
        assert_eq!(read_clock.calls.load(Ordering::Relaxed), 0);

        let clock_repository = ScriptedRepository::new(
            AdmissionLookupResultV1::NotFound,
            AdmitBehavior::EchoCreated,
        );
        let failed_clock = ScriptedClock::failing();
        let clock_error = reduce_command_admission(
            &clock_repository,
            &failed_clock,
            preparation(&command, AdmissionLookupResultV1::NotFound, request_id(51)),
        )
        .expect_err("clock failure must prevent admission");
        assert!(matches!(clock_error, CommandAdmissionError::Clock(_)));
        assert_eq!(failed_clock.calls.load(Ordering::Relaxed), 1);
        assert_eq!(clock_repository.admission_calls.get(), 0);

        let write_repository = ScriptedRepository::new(
            AdmissionLookupResultV1::NotFound,
            AdmitBehavior::Error(riffdb_storage_api::StorageErrorKind::CommitStatusUnknown),
        );
        let write_clock = ScriptedClock::fixed(timestamp(52));
        let write_error = reduce_command_admission(
            &write_repository,
            &write_clock,
            preparation(&command, AdmissionLookupResultV1::NotFound, request_id(52)),
        )
        .expect_err("admission write must preserve uncertain status");
        assert!(matches!(
            write_error,
            CommandAdmissionError::AdmissionWrite(error)
                if error.kind() == riffdb_storage_api::StorageErrorKind::CommitStatusUnknown
        ));
        assert_eq!(write_repository.lookup_calls.get(), 1);
        assert_eq!(write_repository.admission_calls.get(), 1);
        assert_eq!(write_clock.calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn every_nonidentical_post_vacant_atomic_result_is_integrity() {
        let command = fixture();
        let behaviors = [
            AdmitBehavior::DifferentCreated,
            AdmitBehavior::EchoResumed,
            AdmitBehavior::EchoOutcome,
            AdmitBehavior::EchoExecutionFailed,
            AdmitBehavior::InputMismatch,
            AdmitBehavior::MultipleMatches,
        ];
        for (index, behavior) in behaviors.into_iter().enumerate() {
            let repository = ScriptedRepository::new(AdmissionLookupResultV1::NotFound, behavior);
            let clock = ScriptedClock::fixed(timestamp(60));
            let error = reduce_command_admission(
                &repository,
                &clock,
                preparation(
                    &command,
                    AdmissionLookupResultV1::NotFound,
                    request_id(60 + u8::try_from(index).expect("small index")),
                ),
            )
            .expect_err("sole-writer Vacant transition cannot race");
            assert!(matches!(error, CommandAdmissionError::Integrity));
            assert_eq!(repository.lookup_calls.get(), 1);
            assert_eq!(repository.admission_calls.get(), 1);
            assert_eq!(clock.calls.load(Ordering::Relaxed), 1);
        }
    }

    #[test]
    fn raw_conflicts_are_bounded_sorted_deduplicated_and_collision_checked() {
        fn conflict(value: u64) -> ConflictKey {
            let mut builder = riffdb_types::ConflictKeyBuilder::new(AggregateTypeId::first());
            builder.push_u64(value).expect("bounded component");
            builder.finish().expect("conflict key")
        }

        let declared = vec![conflict(3), conflict(1), conflict(3), conflict(2)];
        let (raw, hashes) = lower_conflict_keys(&declared, &hash_conflict_key).expect("lowering");
        assert_eq!(raw, vec![conflict(1), conflict(2), conflict(3)]);
        assert!(hashes.windows(2).all(|pair| pair[0] < pair[1]));

        let collision = lower_conflict_keys(&[conflict(1), conflict(2)], &|_| {
            ConflictKeyHash::from_bytes([0x44; 32])
        })
        .expect_err("distinct raw keys with one hash must fail closed");
        assert!(matches!(collision, CommandAdmissionError::Integrity));
        assert!(matches!(
            lower_conflict_keys(&[], &hash_conflict_key),
            Err(CommandAdmissionError::Integrity)
        ));

        let over_limit = (0..=MAX_COMMAND_CONFLICT_KEYS_V1)
            .map(|value| conflict(u64::try_from(value).expect("usize fits u64")))
            .collect::<Vec<_>>();
        let hash_calls = Cell::new(0);
        let error = lower_conflict_keys(&over_limit, &|bytes| {
            hash_calls.set(hash_calls.get() + 1);
            hash_conflict_key(bytes)
        })
        .expect_err("conflict count must be checked before hashing");
        assert!(matches!(error, CommandAdmissionError::Integrity));
        assert_eq!(hash_calls.get(), 0);
    }

    #[test]
    fn snapshot_lowering_preserves_binding_and_root_orders_with_empty_ranges() {
        fn entity_key(entity_type: EntityTypeId, value: u64) -> EntityKey {
            let mut builder = riffdb_types::EntityKeyBuilder::new(entity_type);
            builder.push_u64(value).expect("bounded component");
            builder.finish().expect("entity key")
        }

        let command = fixture();
        let first = EntityTypeId::first();
        let second = EntityTypeId::new(2).expect("entity type");
        let third = EntityTypeId::new(3).expect("entity type");
        let binding_keys = vec![entity_key(second, 20), entity_key(first, 10)];
        let root_keys = vec![entity_key(third, 30), entity_key(second, 21)];
        let proof = lower_snapshot_parts(
            &command.reference,
            &[second, first],
            &binding_keys,
            &[third, second],
            &root_keys,
        )
        .expect("snapshot lowering");
        let request = proof.request_for_attempt();
        assert_eq!(
            request
                .binding_targets()
                .iter()
                .map(EntityTarget::entity_type_id)
                .collect::<Vec<_>>(),
            vec![second, first]
        );
        assert_eq!(
            request
                .root_validation_targets()
                .iter()
                .map(EntityTarget::entity_type_id)
                .collect::<Vec<_>>(),
            vec![third, second]
        );
        assert_eq!(request.binding_targets()[0].key(), &binding_keys[0]);
        assert_eq!(request.root_validation_targets()[1].key(), &root_keys[1]);
        assert!(request.range_targets().is_empty());
        assert!(matches!(
            lower_snapshot_parts(
                &command.reference,
                &[first],
                &binding_keys,
                &[third, second],
                &root_keys,
            ),
            Err(CommandAdmissionError::Integrity)
        ));
    }

    #[test]
    fn admission_diagnostics_and_private_proofs_are_redacted() {
        let errors = [
            CommandAdmissionError::Integrity,
            CommandAdmissionError::Clock(AdmissionClockError),
            CommandAdmissionError::AdmissionWrite(StorageError::new(
                riffdb_storage_api::StorageErrorKind::Unavailable,
                None,
            )),
        ];
        for error in errors {
            assert_eq!(format!("{error:?}"), "CommandAdmissionError([REDACTED])");
            assert_eq!(
                error.to_string(),
                "command admission could not be completed"
            );
        }

        let command = fixture();
        let repository = ScriptedRepository::new(
            AdmissionLookupResultV1::NotFound,
            AdmitBehavior::EchoCreated,
        );
        let result = reduce_command_admission(
            &repository,
            &ScriptedClock::fixed(timestamp(70)),
            preparation(&command, AdmissionLookupResultV1::NotFound, request_id(70)),
        )
        .expect("candidate");
        assert_eq!(format!("{result:?}"), "Execute([REDACTED])");
        let CommandAdmissionResult::Execute(candidate) = result else {
            unreachable!("checked above")
        };
        assert_eq!(
            format!("{candidate:?}"),
            "CommandExecutionCandidate([REDACTED])"
        );
        assert!(!format!("{candidate:?}").contains(CALLER_KEY));
        assert!(!format!("{candidate:?}").contains(PRINCIPAL));
    }
}
