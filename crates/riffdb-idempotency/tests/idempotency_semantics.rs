#![forbid(unsafe_code)]

//! Contract tests for canonical preparation, digest rotation, and replay lookup.

use riffdb_idempotency::{
    CommandIdempotencyScopeV1, IdempotencyDigestCandidatesV1, IdempotencyDigestError,
    IdempotencyDigestProvider, IdempotencyLookupClassificationV1, IdempotencyPreparationError,
    classify_idempotency_lookup, confirm_command_idempotency, prepare_command_idempotency,
    prepare_idempotency_lookup,
};
use riffdb_storage_api::{
    AdmissionLookupResultV1, DeclaredOutcome, DurabilityMode, ExecutablePlanRef,
    IdempotencyIdentity, IdempotencyKeyDigest, StoredAdmissionStateV1,
    StoredAdmittedProvenanceClaimsV1, StoredExecutionFailedV1, StoredOutcomeV1,
    StoredPendingAdmissionV1,
};
use riffdb_types::{
    ActorId, ActorKind, AdmittedActorContext, AggregateTypeId, CanonicalInputHash, CanonicalRecord,
    CanonicalValue, CommandId, CommitSequence, ContractBundleHash, ContractLineage,
    ContractVersion, DatabaseId, DigestKeyId, Environment, ExecutionFailureCode, FieldId,
    IdempotencyKey, LogicalTime, OutcomeId, PartitionKeyBuilder, PlanHash, ProvenanceId, RequestId,
    TenantId, TenantScope, Timestamp, encode_canonical_record, hash_command_input,
};

struct FixedProvider {
    key_ids: Vec<DigestKeyId>,
}

impl FixedProvider {
    fn new(key_ids: &[u32]) -> Self {
        Self {
            key_ids: key_ids
                .iter()
                .copied()
                .map(|value| DigestKeyId::new(value).expect("nonzero digest key ID"))
                .collect(),
        }
    }
}

impl IdempotencyDigestProvider for FixedProvider {
    fn digest_candidates(
        &self,
        caller_key: &IdempotencyKey,
    ) -> Result<IdempotencyDigestCandidatesV1, IdempotencyDigestError> {
        let key_byte = caller_key
            .expose_secret()
            .bytes()
            .fold(0u8, u8::wrapping_add);
        IdempotencyDigestCandidatesV1::new(
            self.key_ids
                .iter()
                .map(|key_id| {
                    let mut bytes = [key_byte; 32];
                    bytes[0] = u8::try_from(key_id.get()).unwrap_or(u8::MAX);
                    IdempotencyKeyDigest::from_hmac_bytes(*key_id, bytes)
                })
                .collect(),
        )
    }
}

struct FailingProvider;

impl IdempotencyDigestProvider for FailingProvider {
    fn digest_candidates(
        &self,
        _: &IdempotencyKey,
    ) -> Result<IdempotencyDigestCandidatesV1, IdempotencyDigestError> {
        Err(IdempotencyDigestError::Unavailable)
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

fn field(value: u32) -> FieldId {
    FieldId::new(value).expect("nonzero field ID")
}

fn command(value: u32) -> CommandId {
    CommandId::new(value).expect("nonzero command ID")
}

fn scope() -> CommandIdempotencyScopeV1 {
    CommandIdempotencyScopeV1::new(
        database(1),
        Environment::new("development").expect("environment"),
        TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant")),
        ActorId::new("principal-a").expect("principal"),
        ContractLineage::new("budget").expect("lineage"),
        command(7),
    )
}

fn input(key: &str, amount: u64) -> CanonicalRecord {
    CanonicalRecord::new(vec![
        (
            field(7),
            CanonicalValue::string(key).expect("bounded canonical string"),
        ),
        (field(9), CanonicalValue::Bool(true)),
        (field(2), CanonicalValue::U64(amount)),
    ])
    .expect("canonical input")
}

fn prepare(
    scope: &CommandIdempotencyScopeV1,
    key: &str,
    amount: u64,
    provider: &dyn IdempotencyDigestProvider,
) -> Result<riffdb_idempotency::PreparedCommandIdempotencyV1, IdempotencyPreparationError> {
    let typed_key = IdempotencyKey::new(key).expect("checked caller key");
    prepare_command_idempotency(scope, &input(key, amount), field(7), &typed_key, provider)
}

#[test]
fn digest_provider_port_is_object_safe_and_candidate_order_is_preserved() {
    fn accepts_shared_object_safe_port(_: &(dyn IdempotencyDigestProvider + Send + Sync)) {}

    let provider = FixedProvider::new(&[19, 2, 11]);
    accepts_shared_object_safe_port(&provider);
    let prepared = prepare(&scope(), "retry-a", 42, &provider).expect("preparation succeeds");
    let observed = prepared
        .lookup_candidates()
        .as_slice()
        .iter()
        .map(|identity| identity.caller_key_digest().key_id().get())
        .collect::<Vec<_>>();
    assert_eq!(observed, [19, 2, 11]);
    assert_eq!(
        prepared
            .current_identity()
            .caller_key_digest()
            .key_id()
            .get(),
        19
    );
}

#[test]
fn lookup_preparation_precedes_plan_dependent_input_confirmation() {
    let provider = FixedProvider::new(&[19, 2]);
    let checked_key = IdempotencyKey::new("retry-a").expect("checked caller key");
    let prepared_lookup = prepare_idempotency_lookup(&scope(), &checked_key, &provider)
        .expect("plan-independent lookup preparation");

    assert_eq!(
        prepared_lookup
            .lookup_candidates()
            .as_slice()
            .iter()
            .map(|candidate| candidate.caller_key_digest().key_id().get())
            .collect::<Vec<_>>(),
        [19, 2]
    );

    let confirmed = confirm_command_idempotency(
        prepared_lookup,
        &input("retry-a", 42),
        field(7),
        &checked_key,
    )
    .expect("historical-plan input confirmation");
    assert_eq!(
        confirmed
            .lookup_candidates()
            .as_slice()
            .iter()
            .map(|candidate| candidate.caller_key_digest().key_id().get())
            .collect::<Vec<_>>(),
        [19, 2]
    );
}

#[test]
fn candidate_bounds_and_uniqueness_fail_closed() {
    assert_eq!(
        IdempotencyDigestCandidatesV1::new(Vec::new()),
        Err(IdempotencyDigestError::Empty)
    );

    let too_many = (1..=9)
        .map(|value| {
            IdempotencyKeyDigest::from_hmac_bytes(
                DigestKeyId::new(value).expect("key ID"),
                [u8::try_from(value).expect("small value"); 32],
            )
        })
        .collect();
    assert_eq!(
        IdempotencyDigestCandidatesV1::new(too_many),
        Err(IdempotencyDigestError::TooMany)
    );

    let maximum = (1..=8)
        .map(|value| {
            IdempotencyKeyDigest::from_hmac_bytes(
                DigestKeyId::new(value).expect("key ID"),
                [u8::try_from(value).expect("small value"); 32],
            )
        })
        .collect();
    let maximum = IdempotencyDigestCandidatesV1::new(maximum).expect("eight keys are accepted");
    assert_eq!(maximum.len(), 8);
    assert!(!maximum.is_empty());

    let one = DigestKeyId::new(1).expect("key ID");
    assert_eq!(
        IdempotencyDigestCandidatesV1::new(vec![
            IdempotencyKeyDigest::from_hmac_bytes(one, [1; 32]),
            IdempotencyKeyDigest::from_hmac_bytes(one, [2; 32]),
        ]),
        Err(IdempotencyDigestError::DuplicateKeyId)
    );
    assert_eq!(
        IdempotencyDigestCandidatesV1::new(vec![
            IdempotencyKeyDigest::from_hmac_bytes(one, [3; 32]),
            IdempotencyKeyDigest::from_hmac_bytes(DigestKeyId::new(2).expect("key ID"), [3; 32],),
        ]),
        Err(IdempotencyDigestError::DuplicateDigest)
    );
}

#[test]
fn canonical_input_hash_omits_only_the_declared_key_field_and_matches_golden() {
    let provider = FixedProvider::new(&[1]);
    let prepared = prepare(&scope(), "retry-secret", 42, &provider).expect("preparation succeeds");
    let expected_record = CanonicalRecord::new(vec![
        (field(2), CanonicalValue::U64(42)),
        (field(9), CanonicalValue::Bool(true)),
    ])
    .expect("canonical record");
    let expected = hash_command_input(
        &encode_canonical_record(&expected_record).expect("canonical encoding succeeds"),
    );
    assert_eq!(prepared.canonical_input_hash(), expected);
    assert_eq!(
        prepared.canonical_input_hash().as_bytes(),
        &[
            0xb3, 0x2d, 0x41, 0x7a, 0x8d, 0x30, 0xf2, 0x2d, 0xba, 0x92, 0x61, 0xae, 0x47, 0xe3,
            0xc5, 0x0f, 0xcd, 0x7f, 0xbb, 0x4b, 0x32, 0x08, 0x13, 0xde, 0x25, 0xeb, 0x89, 0x8f,
            0x43, 0xaa, 0x82, 0xce,
        ]
    );
}

#[test]
fn key_only_changes_identity_while_other_input_changes_hash() {
    let provider = FixedProvider::new(&[3, 2]);
    let first = prepare(&scope(), "retry-a", 42, &provider).expect("first preparation");
    let changed_key = prepare(&scope(), "retry-b", 42, &provider).expect("changed key");
    let changed_input = prepare(&scope(), "retry-a", 43, &provider).expect("changed input");

    assert_eq!(
        first.canonical_input_hash(),
        changed_key.canonical_input_hash()
    );
    assert_ne!(first.current_identity(), changed_key.current_identity());
    assert_ne!(
        first.canonical_input_hash(),
        changed_input.canonical_input_hash()
    );
    assert_eq!(first.current_identity(), changed_input.current_identity());
}

#[test]
fn declared_key_field_shape_and_provider_failures_are_safe() {
    let provider = FixedProvider::new(&[1]);
    let checked_key = IdempotencyKey::new("retry-a").expect("caller key");
    let missing =
        CanonicalRecord::new(vec![(field(2), CanonicalValue::U64(1))]).expect("canonical input");
    assert!(matches!(
        prepare_command_idempotency(&scope(), &missing, field(7), &checked_key, &provider),
        Err(IdempotencyPreparationError::MissingIdempotencyField)
    ));

    let wrong_type =
        CanonicalRecord::new(vec![(field(7), CanonicalValue::U64(1))]).expect("canonical input");
    assert!(matches!(
        prepare_command_idempotency(&scope(), &wrong_type, field(7), &checked_key, &provider),
        Err(IdempotencyPreparationError::IdempotencyFieldNotString)
    ));

    assert!(matches!(
        prepare_command_idempotency(
            &scope(),
            &input("retry-b", 1),
            field(7),
            &checked_key,
            &provider,
        ),
        Err(IdempotencyPreparationError::IdempotencyKeyMismatch)
    ));
    assert!(matches!(
        prepare_command_idempotency(
            &scope(),
            &input("retry-a", 1),
            field(7),
            &checked_key,
            &FailingProvider,
        ),
        Err(IdempotencyPreparationError::DigestProvider(
            IdempotencyDigestError::Unavailable
        ))
    ));
}

#[test]
fn canonical_uuid_idempotency_field_matches_the_public_caller_key() {
    let provider = FixedProvider::new(&[1]);
    let bytes = [0xa1; 16];
    let canonical = "a1a1a1a1-a1a1-a1a1-a1a1-a1a1a1a1a1a1";
    let checked_key = IdempotencyKey::new(canonical).expect("canonical UUID caller key");
    let input = CanonicalRecord::new(vec![
        (field(2), CanonicalValue::U64(1)),
        (field(7), CanonicalValue::Uuid(bytes)),
    ])
    .expect("canonical UUID input");

    let prepared = prepare_command_idempotency(&scope(), &input, field(7), &checked_key, &provider)
        .expect("UUID idempotency input prepares");
    assert_eq!(prepared.lookup_candidates().as_slice().len(), 1);

    let noncanonical = IdempotencyKey::new("A1A1A1A1-A1A1-A1A1-A1A1-A1A1A1A1A1A1")
        .expect("bounded alternate UUID spelling");
    assert!(matches!(
        prepare_command_idempotency(&scope(), &input, field(7), &noncanonical, &provider),
        Err(IdempotencyPreparationError::IdempotencyKeyMismatch)
    ));
}

#[test]
fn every_identity_scope_component_separates_lookup() {
    let provider = FixedProvider::new(&[1]);
    let base = prepare(&scope(), "retry-a", 42, &provider).expect("base preparation");
    let variants = [
        CommandIdempotencyScopeV1::new(
            database(2),
            Environment::new("development").expect("environment"),
            TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant")),
            ActorId::new("principal-a").expect("principal"),
            ContractLineage::new("budget").expect("lineage"),
            command(7),
        ),
        CommandIdempotencyScopeV1::new(
            database(1),
            Environment::new("production").expect("environment"),
            TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant")),
            ActorId::new("principal-a").expect("principal"),
            ContractLineage::new("budget").expect("lineage"),
            command(7),
        ),
        CommandIdempotencyScopeV1::new(
            database(1),
            Environment::new("development").expect("environment"),
            TenantScope::Tenant(TenantId::new("tenant-b").expect("tenant")),
            ActorId::new("principal-a").expect("principal"),
            ContractLineage::new("budget").expect("lineage"),
            command(7),
        ),
        CommandIdempotencyScopeV1::new(
            database(1),
            Environment::new("development").expect("environment"),
            TenantScope::Global,
            ActorId::new("principal-a").expect("principal"),
            ContractLineage::new("budget").expect("lineage"),
            command(7),
        ),
        CommandIdempotencyScopeV1::new(
            database(1),
            Environment::new("development").expect("environment"),
            TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant")),
            ActorId::new("principal-b").expect("principal"),
            ContractLineage::new("budget").expect("lineage"),
            command(7),
        ),
        CommandIdempotencyScopeV1::new(
            database(1),
            Environment::new("development").expect("environment"),
            TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant")),
            ActorId::new("principal-a").expect("principal"),
            ContractLineage::new("ledger").expect("lineage"),
            command(7),
        ),
        CommandIdempotencyScopeV1::new(
            database(1),
            Environment::new("development").expect("environment"),
            TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant")),
            ActorId::new("principal-a").expect("principal"),
            ContractLineage::new("budget").expect("lineage"),
            command(8),
        ),
    ];

    let base_key = base.current_identity().storage_key().expect("storage key");
    for variant in variants {
        let prepared = prepare(&variant, "retry-a", 42, &provider).expect("variant preparation");
        assert_ne!(prepared.current_identity(), base.current_identity());
        assert_ne!(
            prepared
                .current_identity()
                .storage_key()
                .expect("storage key"),
            base_key
        );
    }
}

fn plan() -> ExecutablePlanRef {
    ExecutablePlanRef::new(
        ContractLineage::new("budget").expect("lineage"),
        ContractVersion::new(2).expect("version"),
        ContractBundleHash::from_bytes([0x21; 32]),
        command(7),
        PlanHash::from_bytes([0x22; 32]),
    )
}

fn identity() -> IdempotencyIdentity {
    IdempotencyIdentity::new(
        database(1),
        Environment::new("development").expect("environment"),
        TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant")),
        ActorId::new("principal-a").expect("principal"),
        ContractLineage::new("budget").expect("lineage"),
        command(7),
        IdempotencyKeyDigest::from_hmac_bytes(DigestKeyId::new(1).expect("digest key"), [0x31; 32]),
    )
}

fn actor() -> AdmittedActorContext {
    AdmittedActorContext::new(
        ActorId::new("principal-a").expect("principal"),
        ActorKind::Service,
        TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant")),
        None,
    )
}

fn partition() -> riffdb_types::PartitionKey {
    let mut builder = PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate type"));
    builder.push_str("tenant-a").expect("partition component");
    builder.finish().expect("partition key")
}

fn pending(hash: CanonicalInputHash) -> StoredPendingAdmissionV1 {
    StoredPendingAdmissionV1::new(
        identity(),
        hash,
        request(4),
        plan(),
        LogicalTime::new(Timestamp::new(1_700_000_000, 12).expect("timestamp")),
        actor(),
        partition(),
        StoredAdmittedProvenanceClaimsV1::default(),
    )
    .expect("pending admission")
}

fn outcome(hash: CanonicalInputHash) -> StoredOutcomeV1 {
    let partition = partition();
    let partition_hash = riffdb_types::hash_partition_key(partition.as_bytes());
    StoredOutcomeV1::new(
        identity(),
        CommitSequence::new(3).expect("commit sequence"),
        request(4),
        plan(),
        hash,
        actor(),
        LogicalTime::new(Timestamp::new(1_700_000_000, 12).expect("timestamp")),
        partition,
        partition_hash,
        Vec::new(),
        DeclaredOutcome::new(
            OutcomeId::new(1).expect("outcome ID"),
            CanonicalRecord::new(Vec::new()).expect("empty outcome"),
        )
        .expect("declared outcome"),
        StoredAdmittedProvenanceClaimsV1::default(),
        provenance(5),
        DurabilityMode::Memory,
    )
    .expect("stored outcome")
}

#[test]
fn durable_lookup_classification_is_closed_and_input_sensitive() {
    let hash = CanonicalInputHash::from_bytes([0x42; 32]);
    let mismatch = CanonicalInputHash::from_bytes([0x43; 32]);

    assert!(matches!(
        classify_idempotency_lookup(AdmissionLookupResultV1::NotFound, hash),
        IdempotencyLookupClassificationV1::Absent
    ));
    let expected_pending = pending(hash);
    match classify_idempotency_lookup(
        AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::Pending(
            expected_pending.clone(),
        ))),
        hash,
    ) {
        IdempotencyLookupClassificationV1::Pending(actual) => {
            assert!(actual == expected_pending);
        }
        _ => panic!("equal pending admission must be returned unchanged"),
    }

    let expected_outcome = outcome(hash);
    match classify_idempotency_lookup(
        AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::StoredOutcome(
            expected_outcome.clone(),
        ))),
        hash,
    ) {
        IdempotencyLookupClassificationV1::Outcome(actual) => {
            assert!(actual == expected_outcome);
        }
        _ => panic!("equal stored outcome must be returned unchanged"),
    }

    let expected_failure =
        StoredExecutionFailedV1::new(pending(hash), ExecutionFailureCode::ResourceLimit);
    match classify_idempotency_lookup(
        AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::ExecutionFailed(
            expected_failure.clone(),
        ))),
        hash,
    ) {
        IdempotencyLookupClassificationV1::ExecutionFailed(actual) => {
            assert!(actual == expected_failure);
        }
        _ => panic!("equal execution failure must be returned unchanged"),
    }
    assert!(matches!(
        classify_idempotency_lookup(
            AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::Pending(pending(
                hash
            )))),
            mismatch,
        ),
        IdempotencyLookupClassificationV1::InputMismatch
    ));
    assert!(matches!(
        classify_idempotency_lookup(AdmissionLookupResultV1::MultipleMatches, hash),
        IdempotencyLookupClassificationV1::MultipleMatches
    ));
}

#[test]
fn diagnostics_redact_keys_digests_scopes_preparations_and_stored_values() {
    let provider = FixedProvider::new(&[1, 2]);
    let prepared =
        prepare(&scope(), "raw-caller-key-canary", 42, &provider).expect("preparation succeeds");
    let digests = provider
        .digest_candidates(&IdempotencyKey::new("raw-caller-key-canary").expect("caller key"))
        .expect("candidate calculation");

    for diagnostic in [
        format!("{:?}", scope()),
        format!("{digests:?}"),
        format!("{prepared:?}"),
        format!(
            "{:?}",
            IdempotencyLookupClassificationV1::Pending(pending(prepared.canonical_input_hash()))
        ),
    ] {
        assert!(diagnostic.contains("REDACTED"));
        assert!(!diagnostic.contains("raw-caller-key-canary"));
        assert!(!diagnostic.contains("principal-a"));
        assert!(!diagnostic.contains("tenant-a"));
    }
}
