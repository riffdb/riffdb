#![forbid(unsafe_code)]

//! Semantic matrix for retained-observation idempotency rechecks.

use std::{cell::Cell, cell::RefCell, collections::VecDeque};

use riffdb_idempotency::{
    CommandIdempotencyScopeV1, IdempotencyDigestCandidatesV1, IdempotencyDigestError,
    IdempotencyDigestProvider, IdempotencyInspectionExecutor, IdempotencyRecheckError,
    IdempotencyRecheckExecutor, IdempotencyRecheckIntegrityV1, IdempotencyRecheckResultV1,
    PreparedIdempotencyRecheckV1, prepare_command_idempotency, prepare_idempotency_lookup,
};
use riffdb_storage_api::{
    AdmissionLookupResultV1, AdmissionRepository, AdmissionRequestV1, AdmissionResultV1,
    DeclaredOutcome, DurabilityMode, ExecutablePlanRef, IdempotencyIdentity, IdempotencyKeyDigest,
    IdempotencyLookupCandidatesV1, StorageError, StorageErrorKind, StoredAdmissionStateV1,
    StoredAdmittedProvenanceClaimsV1, StoredExecutionFailedV1, StoredOutcomeV1,
    StoredPendingAdmissionV1,
};
use riffdb_types::{
    ActorId, ActorKind, AdmittedActorContext, AggregateTypeId, CanonicalInputHash, CanonicalRecord,
    CanonicalValue, CommandId, CommitSequence, ContractBundleHash, ContractLineage,
    ContractVersion, DatabaseId, DigestKeyId, Environment, ExecutionFailureCode, FieldId,
    IdempotencyKey, LogicalTime, OutcomeId, PartitionKeyBuilder, PlanHash, ProvenanceId, RequestId,
    TenantId, TenantScope, Timestamp,
};

const CALLER_KEY: &str = "caller-key-secret-canary";

struct FixedDigestProvider {
    key_ids: Vec<DigestKeyId>,
}

impl FixedDigestProvider {
    fn new(key_ids: &[u32]) -> Self {
        Self {
            key_ids: key_ids
                .iter()
                .copied()
                .map(|value| DigestKeyId::new(value).expect("nonzero key ID"))
                .collect(),
        }
    }
}

impl IdempotencyDigestProvider for FixedDigestProvider {
    fn digest_candidates(
        &self,
        _: &IdempotencyKey,
    ) -> Result<IdempotencyDigestCandidatesV1, IdempotencyDigestError> {
        IdempotencyDigestCandidatesV1::new(
            self.key_ids.iter().map(|key_id| digest(*key_id)).collect(),
        )
    }
}

struct SequencedRepository {
    observations: RefCell<VecDeque<Result<AdmissionLookupResultV1, StorageError>>>,
    candidates: RefCell<Vec<IdempotencyLookupCandidatesV1>>,
    mutation_calls: Cell<usize>,
}

impl SequencedRepository {
    fn new(observations: Vec<AdmissionLookupResultV1>) -> Self {
        Self::with_results(observations.into_iter().map(Ok).collect())
    }

    fn with_results(observations: Vec<Result<AdmissionLookupResultV1, StorageError>>) -> Self {
        Self {
            observations: RefCell::new(observations.into()),
            candidates: RefCell::new(Vec::new()),
            mutation_calls: Cell::new(0),
        }
    }

    fn assert_two_identical_reads_and_no_mutation(&self) {
        let candidates = self.candidates.borrow();
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0], candidates[1]);
        assert_eq!(self.mutation_calls.get(), 0);
        assert!(self.observations.borrow().is_empty());
    }
}

impl AdmissionRepository for SequencedRepository {
    fn admit_or_resolve(&self, _: AdmissionRequestV1) -> Result<AdmissionResultV1, StorageError> {
        self.mutation_calls.set(self.mutation_calls.get() + 1);
        panic!("read-only recheck must not mutate admission state")
    }

    fn lookup_admission(
        &self,
        candidates: IdempotencyLookupCandidatesV1,
    ) -> Result<AdmissionLookupResultV1, StorageError> {
        self.candidates.borrow_mut().push(candidates);
        self.observations
            .borrow_mut()
            .pop_front()
            .expect("one configured observation per lookup")
    }
}

#[derive(Clone, Copy)]
enum StateKind {
    Pending,
    Outcome,
    ExecutionFailed,
}

fn database(seed: u8) -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(u64::from(seed) + 1, [seed; 10])
        .expect("valid UUIDv7 inputs")
}

fn request(seed: u8) -> RequestId {
    RequestId::from_unix_milliseconds_and_random(u64::from(seed) + 1, [seed; 10])
        .expect("valid UUIDv7 inputs")
}

fn provenance(seed: u8) -> ProvenanceId {
    ProvenanceId::from_unix_milliseconds_and_random(u64::from(seed) + 1, [seed; 10])
        .expect("valid UUIDv7 inputs")
}

fn field(value: u32) -> FieldId {
    FieldId::new(value).expect("nonzero field ID")
}

fn command() -> CommandId {
    CommandId::new(7).expect("nonzero command ID")
}

fn digest(key_id: DigestKeyId) -> IdempotencyKeyDigest {
    let mut bytes = [0x91; 32];
    bytes[0] = u8::try_from(key_id.get()).expect("test key ID fits u8");
    IdempotencyKeyDigest::from_hmac_bytes(key_id, bytes)
}

fn identity(key_id: u32) -> IdempotencyIdentity {
    IdempotencyIdentity::new(
        database(1),
        Environment::new("development").expect("environment"),
        TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant")),
        ActorId::new("actor-secret-canary").expect("principal"),
        ContractLineage::new("budget").expect("lineage"),
        command(),
        digest(DigestKeyId::new(key_id).expect("nonzero key ID")),
    )
}

fn scope() -> CommandIdempotencyScopeV1 {
    CommandIdempotencyScopeV1::new(
        database(1),
        Environment::new("development").expect("environment"),
        TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant")),
        ActorId::new("actor-secret-canary").expect("principal"),
        ContractLineage::new("budget").expect("lineage"),
        command(),
    )
}

fn plan(seed: u8) -> ExecutablePlanRef {
    ExecutablePlanRef::new(
        ContractLineage::new("budget").expect("lineage"),
        ContractVersion::new(u64::from(seed) + 1).expect("version"),
        ContractBundleHash::from_bytes([seed; 32]),
        command(),
        PlanHash::from_bytes([seed.wrapping_add(1); 32]),
    )
}

fn normalized_input(amount: u64) -> CanonicalRecord {
    normalized_input_with_key(CALLER_KEY, amount)
}

fn normalized_input_with_key(caller_key: &str, amount: u64) -> CanonicalRecord {
    CanonicalRecord::new(vec![
        (
            field(7),
            CanonicalValue::string(caller_key).expect("bounded caller key"),
        ),
        (field(9), CanonicalValue::U64(amount)),
        (
            field(11),
            CanonicalValue::string("input-value-secret-canary").expect("bounded value"),
        ),
    ])
    .expect("canonical input")
}

fn input_hash(amount: u64, provider: &FixedDigestProvider) -> CanonicalInputHash {
    prepare_command_idempotency(
        &scope(),
        &normalized_input(amount),
        field(7),
        &IdempotencyKey::new(CALLER_KEY).expect("caller key"),
        provider,
    )
    .expect("idempotency preparation")
    .canonical_input_hash()
}

fn actor() -> AdmittedActorContext {
    AdmittedActorContext::new(
        ActorId::new("actor-secret-canary").expect("principal"),
        ActorKind::Service,
        TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant")),
        None,
    )
}

fn partition() -> riffdb_types::PartitionKey {
    let mut builder = PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate type"));
    builder
        .push_str("partition-secret-canary")
        .expect("partition component");
    builder.finish().expect("partition key")
}

fn pending_with(
    selected_plan: ExecutablePlanRef,
    canonical_input_hash: CanonicalInputHash,
    key_id: u32,
    request_seed: u8,
) -> StoredPendingAdmissionV1 {
    StoredPendingAdmissionV1::new(
        identity(key_id),
        canonical_input_hash,
        request(request_seed),
        selected_plan,
        LogicalTime::new(Timestamp::new(1_700_000_000, 12).expect("timestamp")),
        actor(),
        partition(),
        StoredAdmittedProvenanceClaimsV1::default(),
    )
    .expect("pending admission")
}

fn outcome_from(pending: &StoredPendingAdmissionV1) -> StoredOutcomeV1 {
    outcome_from_with(pending, 3, 5)
}

fn outcome_from_with(
    pending: &StoredPendingAdmissionV1,
    commit_sequence: u64,
    provenance_seed: u8,
) -> StoredOutcomeV1 {
    let partition_hash = riffdb_types::hash_partition_key(pending.partition_key().as_bytes());
    StoredOutcomeV1::new(
        pending.identity().clone(),
        CommitSequence::new(commit_sequence).expect("commit sequence"),
        pending.admission_request_id(),
        pending.plan().clone(),
        pending.canonical_input_hash(),
        pending.actor().clone(),
        pending.logical_time(),
        pending.partition_key().clone(),
        partition_hash,
        Vec::new(),
        DeclaredOutcome::new(
            OutcomeId::new(1).expect("outcome ID"),
            CanonicalRecord::new(vec![(
                field(1),
                CanonicalValue::string("outcome-secret-canary").expect("bounded outcome"),
            )])
            .expect("outcome record"),
        )
        .expect("declared outcome"),
        pending.provenance_claims().clone(),
        provenance(provenance_seed),
        DurabilityMode::Memory,
    )
    .expect("stored outcome")
}

fn state(
    kind: StateKind,
    selected_plan: ExecutablePlanRef,
    canonical_input_hash: CanonicalInputHash,
    key_id: u32,
    request_seed: u8,
) -> StoredAdmissionStateV1 {
    let pending = pending_with(selected_plan, canonical_input_hash, key_id, request_seed);
    match kind {
        StateKind::Pending => StoredAdmissionStateV1::Pending(pending),
        StateKind::Outcome => StoredAdmissionStateV1::StoredOutcome(outcome_from(&pending)),
        StateKind::ExecutionFailed => StoredAdmissionStateV1::ExecutionFailed(
            StoredExecutionFailedV1::new(pending, ExecutionFailureCode::ResourceLimit),
        ),
    }
}

fn found(state: StoredAdmissionStateV1) -> AdmissionLookupResultV1 {
    AdmissionLookupResultV1::Found(Box::new(state))
}

fn prepared_recheck(
    repository: &SequencedRepository,
    provider: &FixedDigestProvider,
    selected_plan: ExecutablePlanRef,
    amount: u64,
) -> Result<PreparedIdempotencyRecheckV1, IdempotencyRecheckIntegrityV1> {
    let caller_key = IdempotencyKey::new(CALLER_KEY).expect("caller key");
    let lookup =
        prepare_idempotency_lookup(&scope(), &caller_key, provider).expect("lookup preparation");
    IdempotencyInspectionExecutor::new(repository)
        .inspect(lookup)
        .expect("initial inspection")
        .confirm_input(&normalized_input(amount), field(7), &caller_key)
        .expect("input confirmation")
        .bind_selected_plan(selected_plan)
}

fn assert_exact_state_result(
    result: IdempotencyRecheckResultV1,
    expected: &StoredAdmissionStateV1,
    expected_input: &CanonicalRecord,
) {
    match (result, expected) {
        (
            IdempotencyRecheckResultV1::Pending(actual),
            StoredAdmissionStateV1::Pending(expected),
        ) => {
            assert_eq!(actual.pending(), expected);
            assert_eq!(actual.normalized_input(), expected_input);
        }
        (
            IdempotencyRecheckResultV1::Outcome(actual),
            StoredAdmissionStateV1::StoredOutcome(expected),
        ) => assert_eq!(&actual, expected),
        (
            IdempotencyRecheckResultV1::ExecutionFailed(actual),
            StoredAdmissionStateV1::ExecutionFailed(expected),
        ) => assert_eq!(&actual, expected),
        (actual, _) => panic!("unexpected exact-state result: {actual:?}"),
    }
}

#[test]
fn preparation_matcher_requires_every_plan_dimension_and_the_exact_input() {
    let provider = FixedDigestProvider::new(&[1]);
    let selected_plan = plan(1);
    let input = normalized_input(40);
    let repository = SequencedRepository::new(vec![AdmissionLookupResultV1::NotFound]);
    let prepared = prepared_recheck(&repository, &provider, selected_plan.clone(), 40)
        .expect("absent preparation");

    assert!(prepared.matches_preparation(&selected_plan, &input));

    let mismatched_plans = [
        ExecutablePlanRef::new(
            ContractLineage::new("different-lineage").expect("lineage"),
            selected_plan.contract_version(),
            selected_plan.contract_bundle_hash(),
            selected_plan.command_id(),
            selected_plan.command_plan_hash(),
        ),
        ExecutablePlanRef::new(
            selected_plan.contract_lineage().clone(),
            ContractVersion::new(selected_plan.contract_version().get() + 1).expect("version"),
            selected_plan.contract_bundle_hash(),
            selected_plan.command_id(),
            selected_plan.command_plan_hash(),
        ),
        ExecutablePlanRef::new(
            selected_plan.contract_lineage().clone(),
            selected_plan.contract_version(),
            ContractBundleHash::from_bytes([0x31; 32]),
            selected_plan.command_id(),
            selected_plan.command_plan_hash(),
        ),
        ExecutablePlanRef::new(
            selected_plan.contract_lineage().clone(),
            selected_plan.contract_version(),
            selected_plan.contract_bundle_hash(),
            CommandId::new(selected_plan.command_id().get() + 1).expect("command ID"),
            selected_plan.command_plan_hash(),
        ),
        ExecutablePlanRef::new(
            selected_plan.contract_lineage().clone(),
            selected_plan.contract_version(),
            selected_plan.contract_bundle_hash(),
            selected_plan.command_id(),
            PlanHash::from_bytes([0x32; 32]),
        ),
    ];

    for mismatched_plan in mismatched_plans {
        assert!(!prepared.matches_preparation(&mismatched_plan, &input));
    }

    assert!(!prepared.matches_preparation(&selected_plan, &normalized_input(41)));
    assert!(!prepared.matches_preparation(
        &selected_plan,
        &normalized_input_with_key("different-caller-key", 40),
    ));
    assert!(prepared.matches_preparation(&selected_plan, &input));
}

#[test]
fn scope_matcher_requires_every_non_digest_dimension_across_rotation_candidates() {
    let provider = FixedDigestProvider::new(&[3, 2, 1]);
    let repository = SequencedRepository::new(vec![AdmissionLookupResultV1::NotFound]);
    let prepared =
        prepared_recheck(&repository, &provider, plan(1), 40).expect("absent preparation");
    let expected_database = database(1);
    let expected_environment = Environment::new("development").expect("environment");
    let expected_tenant = TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant"));
    let expected_principal = ActorId::new("actor-secret-canary").expect("principal");
    let expected_lineage = ContractLineage::new("budget").expect("lineage");
    let expected_command = command();

    assert!(prepared.matches_scope(
        expected_database,
        &expected_environment,
        &expected_tenant,
        &expected_principal,
        &expected_lineage,
        expected_command,
    ));

    assert!(!prepared.matches_scope(
        database(2),
        &expected_environment,
        &expected_tenant,
        &expected_principal,
        &expected_lineage,
        expected_command,
    ));
    assert!(!prepared.matches_scope(
        expected_database,
        &Environment::new("different-environment").expect("environment"),
        &expected_tenant,
        &expected_principal,
        &expected_lineage,
        expected_command,
    ));
    assert!(!prepared.matches_scope(
        expected_database,
        &expected_environment,
        &TenantScope::Tenant(TenantId::new("tenant-b").expect("tenant")),
        &expected_principal,
        &expected_lineage,
        expected_command,
    ));
    assert!(!prepared.matches_scope(
        expected_database,
        &expected_environment,
        &expected_tenant,
        &ActorId::new("different-principal").expect("principal"),
        &expected_lineage,
        expected_command,
    ));
    assert!(!prepared.matches_scope(
        expected_database,
        &expected_environment,
        &expected_tenant,
        &expected_principal,
        &ContractLineage::new("different-lineage").expect("lineage"),
        expected_command,
    ));
    assert!(!prepared.matches_scope(
        expected_database,
        &expected_environment,
        &expected_tenant,
        &expected_principal,
        &expected_lineage,
        CommandId::new(expected_command.get() + 1).expect("command ID"),
    ));

    let observed = repository.candidates.borrow();
    let candidates = observed.first().expect("initial lookup candidates");
    assert_eq!(candidates.as_slice().len(), 3);
    assert_eq!(
        candidates
            .as_slice()
            .iter()
            .map(|candidate| candidate.caller_key_digest().key_id().get())
            .collect::<Vec<_>>(),
        [3, 2, 1],
        "digest rotation order is independent of common scope"
    );
}

#[test]
fn stable_absence_yields_one_move_only_token_with_exact_bound_values() {
    let provider = FixedDigestProvider::new(&[1, 2]);
    let selected_plan = plan(1);
    let input = normalized_input(40);
    let repository = SequencedRepository::new(vec![
        AdmissionLookupResultV1::NotFound,
        AdmissionLookupResultV1::NotFound,
    ]);
    let prepared = prepared_recheck(&repository, &provider, selected_plan.clone(), 40)
        .expect("absent preparation");
    let result = IdempotencyRecheckExecutor::new(&repository)
        .recheck(prepared)
        .expect("absence recheck");

    let IdempotencyRecheckResultV1::Vacant(vacant) = result else {
        panic!("stable absence must produce one vacant token");
    };
    assert_eq!(vacant.selected_plan(), &selected_plan);
    assert_eq!(vacant.normalized_input(), &input);
    assert_eq!(vacant.canonical_input_hash(), input_hash(40, &provider));
    assert_eq!(vacant.lookup_candidates().as_slice().len(), 2);
    let (returned_plan, returned_input, returned_preparation) = vacant.into_parts();
    assert_eq!(returned_plan, selected_plan);
    assert_eq!(returned_input, input);
    assert_eq!(
        returned_preparation.canonical_input_hash(),
        input_hash(40, &provider)
    );
    repository.assert_two_identical_reads_and_no_mutation();
}

#[test]
fn absent_to_present_matrix_compares_plan_before_hash_for_every_state() {
    let provider = FixedDigestProvider::new(&[1]);
    let selected_plan = plan(1);
    let matching_hash = input_hash(40, &provider);
    let different_hash = input_hash(41, &provider);

    for kind in [
        StateKind::Pending,
        StateKind::Outcome,
        StateKind::ExecutionFailed,
    ] {
        for plan_matches in [true, false] {
            for hash_matches in [true, false] {
                let current = state(
                    kind,
                    if plan_matches {
                        selected_plan.clone()
                    } else {
                        plan(2)
                    },
                    if hash_matches {
                        matching_hash
                    } else {
                        different_hash
                    },
                    1,
                    4,
                );
                let repository = SequencedRepository::new(vec![
                    AdmissionLookupResultV1::NotFound,
                    found(current.clone()),
                ]);
                let prepared = prepared_recheck(&repository, &provider, selected_plan.clone(), 40)
                    .expect("absent preparation");
                let result = IdempotencyRecheckExecutor::new(&repository)
                    .recheck(prepared)
                    .expect("bounded recheck");

                if !plan_matches {
                    assert!(matches!(
                        result,
                        IdempotencyRecheckResultV1::PreparationChanged
                    ));
                } else if !hash_matches {
                    assert!(matches!(result, IdempotencyRecheckResultV1::InputMismatch));
                } else {
                    assert_exact_state_result(result, &current, &normalized_input(40));
                }
                repository.assert_two_identical_reads_and_no_mutation();
            }
        }
    }
}

#[test]
fn pending_may_remain_exact_or_advance_to_either_exact_terminal_state() {
    let provider = FixedDigestProvider::new(&[1]);
    let selected_plan = plan(1);
    let hash = input_hash(40, &provider);
    let original_pending = pending_with(selected_plan.clone(), hash, 1, 4);
    let current_states = [
        StoredAdmissionStateV1::Pending(original_pending.clone()),
        StoredAdmissionStateV1::StoredOutcome(outcome_from(&original_pending)),
        StoredAdmissionStateV1::ExecutionFailed(StoredExecutionFailedV1::new(
            original_pending.clone(),
            ExecutionFailureCode::ResourceLimit,
        )),
    ];

    for current in current_states {
        let repository = SequencedRepository::new(vec![
            found(StoredAdmissionStateV1::Pending(original_pending.clone())),
            found(current.clone()),
        ]);
        let prepared = prepared_recheck(&repository, &provider, selected_plan.clone(), 40)
            .expect("historical preparation");
        let result = IdempotencyRecheckExecutor::new(&repository)
            .recheck(prepared)
            .expect("valid transition");
        assert_exact_state_result(result, &current, &normalized_input(40));
        repository.assert_two_identical_reads_and_no_mutation();
    }
}

#[test]
fn terminal_observations_may_only_remain_exact() {
    let provider = FixedDigestProvider::new(&[1]);
    let selected_plan = plan(1);
    let hash = input_hash(40, &provider);

    for kind in [StateKind::Outcome, StateKind::ExecutionFailed] {
        let terminal = state(kind, selected_plan.clone(), hash, 1, 4);
        let repository =
            SequencedRepository::new(vec![found(terminal.clone()), found(terminal.clone())]);
        let prepared = prepared_recheck(&repository, &provider, selected_plan.clone(), 40)
            .expect("historical preparation");
        let result = IdempotencyRecheckExecutor::new(&repository)
            .recheck(prepared)
            .expect("stable terminal");
        assert_exact_state_result(result, &terminal, &normalized_input(40));
        repository.assert_two_identical_reads_and_no_mutation();
    }
}

#[test]
fn stable_same_plan_state_with_different_requested_input_is_only_input_mismatch() {
    let provider = FixedDigestProvider::new(&[1]);
    let selected_plan = plan(1);
    let stored_hash = input_hash(40, &provider);

    for kind in [
        StateKind::Pending,
        StateKind::Outcome,
        StateKind::ExecutionFailed,
    ] {
        let durable = state(kind, selected_plan.clone(), stored_hash, 1, 4);
        let repository = SequencedRepository::new(vec![found(durable.clone()), found(durable)]);
        let prepared = prepared_recheck(&repository, &provider, selected_plan.clone(), 41)
            .expect("historical preparation");
        assert!(matches!(
            IdempotencyRecheckExecutor::new(&repository).recheck(prepared),
            Ok(IdempotencyRecheckResultV1::InputMismatch)
        ));
        repository.assert_two_identical_reads_and_no_mutation();
    }
}

#[test]
fn historical_plan_substitution_is_rejected_before_recheck() {
    let provider = FixedDigestProvider::new(&[1]);
    let historical = state(StateKind::Pending, plan(1), input_hash(40, &provider), 1, 4);
    let repository = SequencedRepository::new(vec![found(historical)]);

    assert!(matches!(
        prepared_recheck(&repository, &provider, plan(2), 40),
        Err(IdempotencyRecheckIntegrityV1::SelectedPlanMismatch)
    ));
    assert_eq!(repository.candidates.borrow().len(), 1);
    assert_eq!(repository.mutation_calls.get(), 0);
}

#[test]
fn present_to_missing_multiple_and_outside_candidate_fail_closed() {
    let provider = FixedDigestProvider::new(&[1]);
    let selected_plan = plan(1);
    let hash = input_hash(40, &provider);
    let original = state(StateKind::Pending, selected_plan.clone(), hash, 1, 4);
    let cases = [
        (
            AdmissionLookupResultV1::NotFound,
            IdempotencyRecheckIntegrityV1::PreviouslyPresentMissing,
        ),
        (
            AdmissionLookupResultV1::MultipleMatches,
            IdempotencyRecheckIntegrityV1::MultipleMatches,
        ),
        (
            found(state(StateKind::Pending, selected_plan.clone(), hash, 9, 4)),
            IdempotencyRecheckIntegrityV1::IdentityOutsideCandidates,
        ),
    ];

    for (current, reason) in cases {
        let repository = SequencedRepository::new(vec![found(original.clone()), current]);
        let prepared = prepared_recheck(&repository, &provider, selected_plan.clone(), 40)
            .expect("historical preparation");
        assert!(matches!(
            IdempotencyRecheckExecutor::new(&repository).recheck(prepared),
            Err(IdempotencyRecheckError::Integrity(actual)) if actual == reason
        ));
        repository.assert_two_identical_reads_and_no_mutation();
    }
}

#[test]
fn impossible_rewrites_regressions_and_candidate_switches_fail_closed() {
    let provider = FixedDigestProvider::new(&[1, 2]);
    let selected_plan = plan(1);
    let hash = input_hash(40, &provider);
    let pending = pending_with(selected_plan.clone(), hash, 1, 4);
    let outcome = outcome_from(&pending);
    let pending_with_changed_request = pending_with(selected_plan.clone(), hash, 1, 8);
    let cases = [
        (
            StoredAdmissionStateV1::Pending(pending.clone()),
            StoredAdmissionStateV1::Pending(pending_with(selected_plan.clone(), hash, 1, 8)),
        ),
        (
            StoredAdmissionStateV1::StoredOutcome(outcome.clone()),
            StoredAdmissionStateV1::Pending(pending.clone()),
        ),
        (
            StoredAdmissionStateV1::StoredOutcome(outcome.clone()),
            StoredAdmissionStateV1::StoredOutcome(outcome_from_with(&pending, 4, 5)),
        ),
        (
            StoredAdmissionStateV1::Pending(pending.clone()),
            StoredAdmissionStateV1::StoredOutcome(outcome_from(&pending_with_changed_request)),
        ),
        (
            StoredAdmissionStateV1::Pending(pending.clone()),
            state(StateKind::Pending, plan(2), hash, 1, 4),
        ),
        (
            StoredAdmissionStateV1::Pending(pending.clone()),
            state(StateKind::Pending, selected_plan.clone(), hash, 2, 4),
        ),
    ];

    for (original, current) in cases {
        let repository = SequencedRepository::new(vec![found(original), found(current)]);
        let prepared = prepared_recheck(&repository, &provider, selected_plan.clone(), 40)
            .expect("historical preparation");
        assert!(matches!(
            IdempotencyRecheckExecutor::new(&repository).recheck(prepared),
            Err(IdempotencyRecheckError::Integrity(
                IdempotencyRecheckIntegrityV1::ImpossibleTransition
            ))
        ));
        repository.assert_two_identical_reads_and_no_mutation();
    }
}

#[test]
fn recheck_storage_failure_is_typed_and_does_not_mutate() {
    let provider = FixedDigestProvider::new(&[1]);
    let failure = StorageError::new(StorageErrorKind::Unavailable, None);
    let repository = SequencedRepository::with_results(vec![
        Ok(AdmissionLookupResultV1::NotFound),
        Err(failure.clone()),
    ]);
    let prepared =
        prepared_recheck(&repository, &provider, plan(1), 40).expect("absent preparation");

    assert!(matches!(
        IdempotencyRecheckExecutor::new(&repository).recheck(prepared),
        Err(IdempotencyRecheckError::Storage(actual)) if actual == failure
    ));
    repository.assert_two_identical_reads_and_no_mutation();
}

#[test]
fn all_authority_and_result_diagnostics_are_redacted() {
    let provider = FixedDigestProvider::new(&[1]);
    let selected_plan = plan(1);
    let repository = SequencedRepository::new(vec![
        AdmissionLookupResultV1::NotFound,
        AdmissionLookupResultV1::NotFound,
    ]);
    let prepared =
        prepared_recheck(&repository, &provider, selected_plan, 40).expect("absent preparation");
    assert!(!prepared.matches_preparation(&plan(2), &normalized_input(40)));
    assert!(!prepared.matches_scope(
        database(1),
        &Environment::new("environment-secret-canary").expect("environment"),
        &TenantScope::Tenant(TenantId::new("tenant-secret-canary").expect("tenant")),
        &ActorId::new("principal-secret-canary").expect("principal"),
        &ContractLineage::new("lineage-secret-canary").expect("lineage"),
        CommandId::new(99).expect("command ID"),
    ));
    let prepared_debug = format!("{prepared:?}");
    let result = IdempotencyRecheckExecutor::new(&repository)
        .recheck(prepared)
        .expect("stable absence");
    let result_debug = format!("{result:?}");
    let executor_debug = format!("{:?}", IdempotencyRecheckExecutor::new(&repository));

    for diagnostic in [prepared_debug, result_debug, executor_debug] {
        assert!(diagnostic.contains("REDACTED"));
        assert!(!diagnostic.contains(CALLER_KEY));
        assert!(!diagnostic.contains("input-value-secret-canary"));
        assert!(!diagnostic.contains("actor-secret-canary"));
        assert!(!diagnostic.contains("partition-secret-canary"));
        assert!(!diagnostic.contains("environment-secret-canary"));
        assert!(!diagnostic.contains("tenant-secret-canary"));
        assert!(!diagnostic.contains("principal-secret-canary"));
        assert!(!diagnostic.contains("lineage-secret-canary"));
    }
}
