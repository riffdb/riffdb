#![forbid(unsafe_code)]

//! Semantic tests for bounded, read-only idempotency inspection.

use std::cell::Cell;

use riffdb_idempotency::{
    CommandIdempotencyScopeV1, IdempotencyDigestCandidatesV1, IdempotencyDigestError,
    IdempotencyDigestProvider, IdempotencyInspectionError, IdempotencyInspectionExecutor,
    IdempotencyPlanSelectionV1, prepare_idempotency_lookup,
};
use riffdb_storage_api::{
    AdmissionLookupResultV1, AdmissionRepository, AdmissionRequestV1, AdmissionResultV1,
    DeclaredOutcome, DurabilityMode, ExecutablePlanRef, IdempotencyKeyDigest, StorageError,
    StoredAdmissionStateV1, StoredAdmittedProvenanceClaimsV1, StoredExecutionFailedV1,
    StoredOutcomeV1, StoredPendingAdmissionV1,
};
use riffdb_types::{
    ActorId, ActorKind, AdmittedActorContext, AggregateTypeId, CanonicalInputHash, CanonicalRecord,
    CanonicalValue, CommandId, CommitSequence, ContractBundleHash, ContractLineage,
    ContractVersion, DatabaseId, DigestKeyId, Environment, ExecutionFailureCode, FieldId,
    IdempotencyKey, LogicalTime, OutcomeId, PartitionKeyBuilder, PlanHash, ProvenanceId, RequestId,
    TenantId, TenantScope, Timestamp,
};

struct FixedDigestProvider;

impl IdempotencyDigestProvider for FixedDigestProvider {
    fn digest_candidates(
        &self,
        _: &IdempotencyKey,
    ) -> Result<IdempotencyDigestCandidatesV1, IdempotencyDigestError> {
        IdempotencyDigestCandidatesV1::new(vec![IdempotencyKeyDigest::from_hmac_bytes(
            DigestKeyId::new(1).expect("digest key ID"),
            [0x31; 32],
        )])
    }
}

struct RecordingRepository {
    observation: AdmissionLookupResultV1,
    lookup_calls: Cell<usize>,
    mutation_calls: Cell<usize>,
}

impl RecordingRepository {
    fn new(observation: AdmissionLookupResultV1) -> Self {
        Self {
            observation,
            lookup_calls: Cell::new(0),
            mutation_calls: Cell::new(0),
        }
    }
}

impl AdmissionRepository for RecordingRepository {
    fn admit_or_resolve(&self, _: AdmissionRequestV1) -> Result<AdmissionResultV1, StorageError> {
        self.mutation_calls.set(self.mutation_calls.get() + 1);
        panic!("read-only inspection must not create an admission")
    }

    fn lookup_admission(
        &self,
        _: riffdb_storage_api::IdempotencyLookupCandidatesV1,
    ) -> Result<AdmissionLookupResultV1, StorageError> {
        self.lookup_calls.set(self.lookup_calls.get() + 1);
        Ok(self.observation.clone())
    }
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

fn command() -> CommandId {
    CommandId::new(7).expect("nonzero command ID")
}

fn identity() -> riffdb_storage_api::IdempotencyIdentity {
    riffdb_storage_api::IdempotencyIdentity::new(
        database(1),
        Environment::new("development").expect("environment"),
        TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant")),
        ActorId::new("actor-secret-canary").expect("principal"),
        ContractLineage::new("budget").expect("lineage"),
        command(),
        IdempotencyKeyDigest::from_hmac_bytes(
            DigestKeyId::new(1).expect("digest key ID"),
            [0x31; 32],
        ),
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

fn pending(historical_plan: ExecutablePlanRef) -> StoredPendingAdmissionV1 {
    StoredPendingAdmissionV1::new(
        identity(),
        CanonicalInputHash::from_bytes([0x42; 32]),
        request(4),
        historical_plan,
        LogicalTime::new(Timestamp::new(1_700_000_000, 12).expect("timestamp")),
        actor(),
        partition(),
        StoredAdmittedProvenanceClaimsV1::default(),
    )
    .expect("pending admission")
}

fn outcome(historical_plan: ExecutablePlanRef) -> StoredOutcomeV1 {
    let partition = partition();
    let partition_hash = riffdb_types::hash_partition_key(partition.as_bytes());
    StoredOutcomeV1::new(
        identity(),
        CommitSequence::new(3).expect("commit sequence"),
        request(4),
        historical_plan,
        CanonicalInputHash::from_bytes([0x42; 32]),
        actor(),
        LogicalTime::new(Timestamp::new(1_700_000_000, 12).expect("timestamp")),
        partition,
        partition_hash,
        Vec::new(),
        DeclaredOutcome::new(
            OutcomeId::new(1).expect("outcome ID"),
            CanonicalRecord::new(vec![(
                FieldId::new(1).expect("field ID"),
                CanonicalValue::string("outcome-secret-canary").expect("bounded string"),
            )])
            .expect("outcome record"),
        )
        .expect("declared outcome"),
        StoredAdmittedProvenanceClaimsV1::default(),
        provenance(5),
        DurabilityMode::Memory,
    )
    .expect("stored outcome")
}

fn prepared_lookup() -> riffdb_idempotency::PreparedIdempotencyLookupV1 {
    prepare_idempotency_lookup(
        &scope(),
        &IdempotencyKey::new("caller-key-secret-canary").expect("caller key"),
        &FixedDigestProvider,
    )
    .expect("lookup preparation")
}

fn found(state: StoredAdmissionStateV1) -> AdmissionLookupResultV1 {
    AdmissionLookupResultV1::Found(Box::new(state))
}

#[test]
fn absence_requires_exactly_one_read_and_performs_no_mutation() {
    let repository = RecordingRepository::new(AdmissionLookupResultV1::NotFound);
    let inspection = IdempotencyInspectionExecutor::new(&repository)
        .inspect(prepared_lookup())
        .expect("absence inspection");

    assert_eq!(
        inspection.plan_selection(),
        &IdempotencyPlanSelectionV1::Absent
    );
    assert_eq!(repository.lookup_calls.get(), 1);
    assert_eq!(repository.mutation_calls.get(), 0);
}

#[test]
fn every_durable_state_selects_its_exact_historical_plan_once() {
    let cases = [
        {
            let historical_plan = plan(2);
            (
                found(StoredAdmissionStateV1::Pending(pending(
                    historical_plan.clone(),
                ))),
                historical_plan,
            )
        },
        {
            let historical_plan = plan(3);
            (
                found(StoredAdmissionStateV1::StoredOutcome(outcome(
                    historical_plan.clone(),
                ))),
                historical_plan,
            )
        },
        {
            let historical_plan = plan(4);
            (
                found(StoredAdmissionStateV1::ExecutionFailed(
                    StoredExecutionFailedV1::new(
                        pending(historical_plan.clone()),
                        ExecutionFailureCode::ResourceLimit,
                    ),
                )),
                historical_plan,
            )
        },
    ];

    for (observation, historical_plan) in cases {
        let repository = RecordingRepository::new(observation);
        let inspection = IdempotencyInspectionExecutor::new(&repository)
            .inspect(prepared_lookup())
            .expect("stored-state inspection");
        assert_eq!(
            inspection.plan_selection(),
            &IdempotencyPlanSelectionV1::Historical(historical_plan)
        );
        assert_eq!(repository.lookup_calls.get(), 1);
        assert_eq!(repository.mutation_calls.get(), 0);
    }
}

#[test]
fn multiple_matches_fail_closed_after_one_read() {
    let repository = RecordingRepository::new(AdmissionLookupResultV1::MultipleMatches);
    let result = IdempotencyInspectionExecutor::new(&repository).inspect(prepared_lookup());

    assert!(matches!(
        result,
        Err(IdempotencyInspectionError::MultipleMatches)
    ));
    assert_eq!(repository.lookup_calls.get(), 1);
    assert_eq!(repository.mutation_calls.get(), 0);
}

#[test]
fn unrelated_storage_state_fails_closed_without_exposing_a_plan() {
    let mut unrelated = pending(plan(5));
    let unrelated_identity = riffdb_storage_api::IdempotencyIdentity::new(
        database(1),
        Environment::new("development").expect("environment"),
        TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant")),
        ActorId::new("actor-secret-canary").expect("principal"),
        ContractLineage::new("budget").expect("lineage"),
        command(),
        IdempotencyKeyDigest::from_hmac_bytes(
            DigestKeyId::new(2).expect("digest key ID"),
            [0x32; 32],
        ),
    );
    unrelated = StoredPendingAdmissionV1::new(
        unrelated_identity,
        unrelated.canonical_input_hash(),
        unrelated.admission_request_id(),
        unrelated.plan().clone(),
        unrelated.logical_time(),
        unrelated.actor().clone(),
        unrelated.partition_key().clone(),
        unrelated.provenance_claims().clone(),
    )
    .expect("unrelated pending admission");
    let repository = RecordingRepository::new(found(StoredAdmissionStateV1::Pending(unrelated)));

    assert!(matches!(
        IdempotencyInspectionExecutor::new(&repository).inspect(prepared_lookup()),
        Err(IdempotencyInspectionError::InvalidObservation)
    ));
    assert_eq!(repository.lookup_calls.get(), 1);
    assert_eq!(repository.mutation_calls.get(), 0);
}

#[test]
fn selection_and_opaque_diagnostics_disclose_no_stored_values() {
    let repository = RecordingRepository::new(found(StoredAdmissionStateV1::StoredOutcome(
        outcome(plan(6)),
    )));
    let inspection = IdempotencyInspectionExecutor::new(&repository)
        .inspect(prepared_lookup())
        .expect("stored outcome inspection");
    let selection_debug = format!("{:?}", inspection.plan_selection());
    let inspection_debug = format!("{inspection:?}");
    let confirmed = inspection
        .confirm_input(
            &CanonicalRecord::new(vec![(
                FieldId::new(7).expect("field ID"),
                CanonicalValue::string("caller-key-secret-canary").expect("bounded string"),
            )])
            .expect("normalized input"),
            FieldId::new(7).expect("field ID"),
            &IdempotencyKey::new("caller-key-secret-canary").expect("caller key"),
        )
        .expect("input confirmation");
    let confirmed_debug = format!("{confirmed:?}");

    for diagnostic in [selection_debug, inspection_debug, confirmed_debug] {
        assert!(diagnostic.contains("REDACTED"));
        assert!(!diagnostic.contains("outcome-secret-canary"));
        assert!(!diagnostic.contains("actor-secret-canary"));
        assert!(!diagnostic.contains("partition-secret-canary"));
        assert!(!diagnostic.contains("caller-key-secret-canary"));
    }
    assert_eq!(repository.lookup_calls.get(), 1);
    assert_eq!(repository.mutation_calls.get(), 0);
}
