#![forbid(unsafe_code)]

//! Child-process crash and reopen evidence for the redb storage boundary.

use std::num::{NonZeroU16, NonZeroU64};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, TableHandle};
use riffdb_catalog::{
    CatalogHistoryOutcome, CatalogIndexMigrationContext, CatalogIndexMigrationDriveError,
    CatalogIndexMigrationDriver, ValidatedContractBundle, validate_catalog_history,
};
use riffdb_contract_compiler::compile_contract_source;
use riffdb_storage_api::{
    AdministrationAuditReader, AdministrationAuditScan, AdministrationAuditScanRequest,
    AdmissionLookupResultV1, AdmissionRepository, AffectedEntityV1, AffectedEpochCurrentState,
    AffectedIndexEpochTargets, ApplicationCommandTransactionPort, AssignedCommandSequence,
    AtomicCommandRecordSet, AuditPrincipalV1, AuthoritativeIndexScanPage,
    AuthoritativeIndexScanRequest, AuthoritativePointReader, AuthoritativeScanReader,
    CandidateAdmissionResult, CandidateCapacityResult, CandidateStartResult,
    CatalogActivationIntentV1, CatalogActivationResult, CatalogAdministrationRepository,
    CommandCandidateAdmission, CommandCandidateAffectedEpochRead, CommandCandidateAwaitingCapacity,
    CommandCandidateAwaitingValidation, CommandCandidateCapacityReserved,
    CommandCandidateSequenceAssigned, CommandCandidateStateRead, CommandWriteSetPlanV1,
    CurrentIndexGenerationObservation, DatabaseIdentityProbe, DatabaseIdentityProbePort,
    DatabaseInitializationPort, DeclaredOutcome, DeferredCommandEpoch, DeferredCommandEpochPort,
    DeferredCommandFence, DeferredNonEmptyCommandBatch, DurabilityMode, DurableKeySchemaBindingV1,
    EmptyCommandBatch, EncodedWriteSetUpperBoundResultV1, EntityMutation, EntityObservation,
    EntityPostImage, EntityTarget, EvaluationBudget, EventIntent, EventRoutePageLimit,
    EventRouteScanRequestV1, EventRouteScanV1, EventRouteUpperFenceV1, EvidencePageLimit,
    ExecutablePlanRef, ExpectedEntityState, IdempotencyIdentity, IdempotencyKeyDigest,
    IdempotencyLookupCandidatesV1, IndexEntryMutationV1, IndexEpochAdvanceV1, IndexEpochPosition,
    IndexRangeTarget, MAX_INDEX_MIGRATION_PAGE_BYTES, MAX_INDEX_MIGRATION_PAGE_ENTRIES,
    NonEmptyCommandBatch, OpenSessionId, OutboxPageLimit, OutboxRepository,
    OutboxStatusObservationV1, OutboxStatusReadResultV1, PartitionEventRouteReader,
    PartitionIndexTarget, PendingOutboxScanV1, PreEvaluationCommitContext, ReadSnapshot,
    ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
    ServiceAuditAppendIntentV1, SnapshotReader, SnapshotRequest, StartupValidationInputs,
    StorageScanLimit, StoredAdministrationAuditRecordV1, StoredAdmittedProvenanceClaimsV1,
    StoredContractBundleV1, StoredDurableEventV1, StoredEntityRecordV1, StoredIndexEntryV1,
    StoredIndexEntryV2, StoredOutcomeV1, StoredPendingAdmissionV1, StoredProvenanceRecordV1,
    StoredReadDependenciesV1, StructuralEvidenceCursor, StructuralEvidenceOpen,
    StructuralEvidencePage, StructuralEvidenceSession, StructuralFinding, StructuralFindingScope,
    StructuralOpenOutcome, StructurallyOpened, command_write_set_upper_bound_v1,
    decode_index_entry_v1, decode_index_entry_v2, decode_index_migration_row, derive_event_hash_v1,
    encode_index_entry_v1_fixture, encode_index_entry_v2, encode_record_registry_v2,
};
use riffdb_storage_redb::{
    RedbCommitProfile, RedbDormantPorts, RedbDurabilityEpoch, RedbOperationalPorts,
    RedbStartupIndexMigrationPort, RedbStore, RedbTestController, RedbTestOperation,
};
use riffdb_types::{
    ActorId, ActorKind, AggregateTypeId, CanonicalInputHash, CanonicalRecord, CanonicalValue,
    CapabilityId, CommitSequence, DatabaseId, DigestKeyId, EntityKeyBuilder, EntityTypeId,
    EntityVersion, Environment, EventId, EventTypeId, FieldId, IndexEntryKey, IndexEntryKeyBuilder,
    IndexId, LogicalTime, MAX_CANONICAL_DOCUMENT_BYTES, OutcomeId, PartitionKeyBuilder,
    ProvenanceId, RequestId, ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetsV1,
    ServiceIngressKindV1, ServiceOperationV1, TenantId, TenantScope, Timestamp, hash_partition_key,
};

const CHILD_MODE: &str = "RIFFDB_STORAGE_RECOVERY_CHILD_MODE";
const CHILD_PATH: &str = "RIFFDB_STORAGE_RECOVERY_CHILD_PATH";
const CHILD_COMMIT_PROFILE: &str = "RIFFDB_STORAGE_RECOVERY_CHILD_COMMIT_PROFILE";
const CHILD_PRUNE_TARGET: &str = "RIFFDB_STORAGE_RECOVERY_CHILD_PRUNE_TARGET";
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const SECONDARY_INDEXES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("secondary_indexes");
static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

const STORAGE_RECOVERY_CONTRACT: &str = r#"
contract StorageRecovery version 1 {
  entity Row {
    key (id: u64)
    field value: u64
    index ByValue(value)
  }

  event RowCreated {
    partition_by (id)
    id: u64
    value: u64
  }

  aggregate Rows {
    root Row
    partition_by id
    conflict_key (id)
  }

  command CreateRow {
    input idempotency_key: string<128>
    input id: u64
    input value: u64

    idempotency_key idempotency_key
    create Row(id) as row
      else RowAlreadyExists { id: id }

    set row.value = value

    emit RowCreated { id: id, value: value }
    return RowCreatedOutcome { row: row }
  }
}
"#;

struct TestDatabasePath(PathBuf);

impl TestDatabasePath {
    fn new(label: &str) -> Self {
        let ordinal = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!(
            "riffdb-storage-recovery-{label}-{}-{ordinal}.redb",
            std::process::id()
        )))
    }
}

impl Drop for TestDatabasePath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
        let mut journal = self.0.as_os_str().to_os_string();
        journal.push(".riffjournal");
        let _ = std::fs::remove_file(PathBuf::from(journal));
        let mut checkpoint = self.0.as_os_str().to_os_string();
        checkpoint.push(".riffjournal.checkpoint");
        let _ = std::fs::remove_file(PathBuf::from(checkpoint));
        let mut spare = self.0.as_os_str().to_os_string();
        spare.push(".riffjournal.next");
        let _ = std::fs::remove_file(PathBuf::from(spare));
    }
}

fn database_id() -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10])
        .expect("valid deterministic database ID")
}

fn uuid_bytes(fill: u8) -> [u8; 16] {
    let mut bytes = [fill; 16];
    bytes[6] = 0x70 | (fill & 0x0f);
    bytes[8] = 0x80 | (fill & 0x3f);
    bytes
}

fn run_crashing_child(mode: &str, path: &Path) {
    run_crashing_child_with_profile(mode, path, RedbCommitProfile::Standard);
}

/// Every armed child terminates through `std::process::abort()` (SIGABRT).
/// Discriminating on the signal keeps 'before' arms honest: a child that
/// panics without reaching its failpoint exits with a plain nonzero code and
/// must fail here instead of passing the parent's negative assertions vacuously.
fn assert_child_aborted(status: std::process::ExitStatus, label: &str) {
    assert!(
        !status.success(),
        "the armed {label} child must terminate abruptly"
    );
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        const SIGABRT: i32 = 6;
        assert_eq!(
            status.signal(),
            Some(SIGABRT),
            "the armed {label} child must die on SIGABRT at its failpoint, \
             not exit through an unrelated panic (status {status})"
        );
    }
}

fn run_crashing_child_prune(mode: &str, path: &Path, prune_target: u64) {
    let status = Command::new(std::env::current_exe().expect("current test executable"))
        .arg("--exact")
        .arg("process_recovery_child")
        .arg("--nocapture")
        .env(CHILD_MODE, mode)
        .env(CHILD_PATH, path)
        .env(CHILD_COMMIT_PROFILE, "standard")
        .env(CHILD_PRUNE_TARGET, prune_target.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("run recovery prune child");
    assert_child_aborted(status, "prune");
}

fn run_crashing_child_with_profile(mode: &str, path: &Path, profile: RedbCommitProfile) {
    let profile = match profile {
        RedbCommitProfile::Standard => "standard",
        RedbCommitProfile::Hardened => "hardened",
    };
    let status = Command::new(std::env::current_exe().expect("current test executable"))
        .arg("--exact")
        .arg("process_recovery_child")
        .arg("--nocapture")
        .env(CHILD_MODE, mode)
        .env(CHILD_PATH, path)
        .env(CHILD_COMMIT_PROFILE, profile)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("run recovery child");
    assert_child_aborted(status, "recovery");
}

fn complete_startup_pass(
    store: RedbStore,
) -> (
    OpenSessionId,
    CatalogHistoryOutcome,
    StructuralOpenOutcome<RedbDormantPorts, RedbStartupIndexMigrationPort>,
) {
    let digest_key = DigestKeyId::new(1).expect("digest key ID");
    let inputs = StartupValidationInputs::new(
        Timestamp::new(1_700_000_000, 0).expect("startup timestamp"),
        ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("capability digest inventory"),
        ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("idempotency digest inventory"),
    );
    let mut session = store
        .begin_structural_evidence(inputs)
        .expect("begin exclusive structural evidence");
    let database_id = session.database_id();
    let open_session_id = session.open_session_id();
    let limit = EvidencePageLimit::new(64).expect("page limit");

    let mut structural_cursor = StructuralEvidenceCursor::start(database_id, open_session_id);
    let structural_end = loop {
        match session
            .read_structural_evidence(structural_cursor, limit)
            .expect("read structural evidence")
        {
            StructuralEvidencePage::Page { findings, next, .. } => {
                assert!(
                    findings.is_empty(),
                    "a valid recovery fixture has no findings: {findings:?}"
                );
                structural_cursor = next;
            }
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };

    let (catalog_outcome, historical_end) = validate_catalog_history(&mut session)
        .expect("catalog validates the complete historical stream")
        .into_parts();
    let outcome = session
        .finish(structural_end, historical_end)
        .expect("finish structural evidence");
    (open_session_id, catalog_outcome, outcome)
}

fn complete_structural_open(store: RedbStore) -> StructurallyOpened<RedbDormantPorts> {
    let (_, catalog_outcome, outcome) = complete_startup_pass(store);
    assert!(matches!(catalog_outcome, CatalogHistoryOutcome::Ready(_)));
    let opened = match outcome {
        StructuralOpenOutcome::Clean(opened) => opened,
        StructuralOpenOutcome::MigrationRequired(_) => {
            panic!("V2 recovery fixture must not require index migration")
        }
    };
    assert_eq!(opened.database_id(), database_id());
    opened
}

fn collect_structural_findings(store: RedbStore) -> Vec<StructuralFinding> {
    let digest_key = DigestKeyId::new(1).expect("digest key ID");
    let inputs = StartupValidationInputs::new(
        Timestamp::new(1_700_000_000, 0).expect("startup timestamp"),
        ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("capability digest inventory"),
        ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("idempotency digest inventory"),
    );
    let mut session = store
        .begin_structural_evidence(inputs)
        .expect("begin structural evidence");
    let mut cursor =
        StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
    let mut findings = Vec::new();
    loop {
        match session
            .read_structural_evidence(
                cursor,
                EvidencePageLimit::new(64).expect("structural page limit"),
            )
            .expect("read structural evidence")
        {
            StructuralEvidencePage::Page {
                findings: page,
                next,
                ..
            } => {
                findings.extend(page);
                cursor = next;
            }
            StructuralEvidencePage::ExactEnd(_) => return findings,
        }
    }
}

fn open_operational(store: RedbStore) -> RedbOperationalPorts {
    let opened = complete_structural_open(store);
    let (_, _, _, dormant) = opened.into_parts();
    dormant
        .into_operational_after_catalog_validation()
        .expect("activate storage fixture; WP-130 owns catalog-proof composition")
}

#[derive(Clone)]
struct CommandFixture {
    candidates: IdempotencyLookupCandidatesV1,
    pending: StoredPendingAdmissionV1,
    intent: riffdb_storage_api::CommitIntent,
    affected_targets: AffectedIndexEpochTargets,
    write_plan: CommandWriteSetPlanV1,
    records: AtomicCommandRecordSet,
    target: EntityTarget,
    range: IndexRangeTarget,
    index_key: IndexEntryKey,
}

fn validated_contract_bundle() -> &'static ValidatedContractBundle {
    static BUNDLE: OnceLock<ValidatedContractBundle> = OnceLock::new();
    BUNDLE.get_or_init(|| {
        ValidatedContractBundle::from_compiler_bundle(
            compile_contract_source(STORAGE_RECOVERY_CONTRACT)
                .expect("compile indexed storage recovery contract"),
        )
        .expect("validate indexed storage recovery bundle")
    })
}

fn plan() -> ExecutablePlanRef {
    let bundle = validated_contract_bundle();
    let command = bundle
        .bundle()
        .commands()
        .first()
        .expect("storage recovery command");
    ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        command.command_id(),
        command.plan_hash(),
    )
}

fn contract_bundle() -> StoredContractBundleV1 {
    validated_contract_bundle()
        .to_stored()
        .expect("stored contract bundle")
}

fn record(value: u64) -> CanonicalRecord {
    CanonicalRecord::new(vec![(
        FieldId::new(1).expect("field ID"),
        CanonicalValue::U64(value),
    )])
    .expect("canonical record")
}

/// Ordinal-parameterized fixture target: ordinal 1 is the original fixture
/// shape (entity/partition value 7); later ordinals commit disjoint rows.
fn target_and_index_at(ordinal: u64) -> (EntityTarget, IndexEntryKey, IndexRangeTarget) {
    let entity_type_id = EntityTypeId::new(1).expect("entity type ID");
    let mut entity_key = EntityKeyBuilder::new(entity_type_id);
    entity_key
        .push_u64(6 + ordinal)
        .expect("entity key component");
    let entity_key = entity_key.finish().expect("entity key");
    let target = EntityTarget::new(entity_type_id, entity_key.clone()).expect("entity target");

    let index_id = IndexId::new(1).expect("index ID");
    let mut index_key = IndexEntryKeyBuilder::new(index_id);
    index_key.push_u64(10).expect("index component");
    let index_key = index_key.finish(entity_key).expect("index entry key");
    let mut prefix = riffdb_storage_api::IndexRangePrefixBuilder::new(index_id);
    prefix.push_u64(10).expect("range component");
    let mut partition = PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate"));
    partition
        .push_u64(6 + ordinal)
        .expect("partition component");
    let range = IndexRangeTarget::new(partition.finish().expect("partition key"), prefix.finish());
    (target, index_key, range)
}

fn command_fixture() -> CommandFixture {
    command_fixture_at(1)
}

/// Ordinal-parameterized committed command: ordinal 1 reproduces the original
/// fixture exactly; ordinal N commits sequence N over a disjoint entity.
fn command_fixture_at(ordinal: u64) -> CommandFixture {
    let plan = plan();
    let sequence = CommitSequence::new(ordinal).expect("fixture ordinal");
    let ordinal_u8 = u8::try_from(ordinal % 256).expect("bounded fixture ordinal");
    let (target, index_key, range) = target_and_index_at(ordinal);
    let tenant_scope = TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant"));
    let principal = ActorId::new("principal-a").expect("principal");
    let actor = riffdb_types::AdmittedActorContext::new(
        principal.clone(),
        ActorKind::Human,
        tenant_scope.clone(),
        None,
    );
    let identity = IdempotencyIdentity::new(
        database_id(),
        Environment::new("test").expect("environment"),
        tenant_scope,
        principal,
        plan.contract_lineage().clone(),
        plan.command_id(),
        IdempotencyKeyDigest::from_hmac_bytes(
            DigestKeyId::new(1).expect("digest key"),
            [0x40_u8.wrapping_add(ordinal_u8); 32],
        ),
    );
    let request_id =
        RequestId::from_bytes(uuid_bytes(0x30_u8.wrapping_add(ordinal_u8))).expect("request ID");
    let provenance_id = ProvenanceId::from_bytes(uuid_bytes(0x50_u8.wrapping_add(ordinal_u8)))
        .expect("provenance ID");
    let logical_time =
        LogicalTime::new(Timestamp::new(1_700_000_001, 0).expect("logical timestamp"));
    let mut partition = PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate"));
    partition
        .push_u64(6 + ordinal)
        .expect("partition component");
    let partition = partition.finish().expect("partition key");
    let pending = StoredPendingAdmissionV1::new(
        identity.clone(),
        CanonicalInputHash::from_bytes([0x42; 32]),
        request_id,
        plan.clone(),
        logical_time,
        actor.clone(),
        partition.clone(),
        StoredAdmittedProvenanceClaimsV1::default(),
    )
    .expect("pending admission");

    let snapshot_request =
        SnapshotRequest::new(plan.clone(), vec![target.clone()], Vec::new(), Vec::new())
            .expect("snapshot request");
    let snapshot = ReadSnapshot::new(
        &snapshot_request,
        None,
        vec![EntityObservation::Absent(target.clone())],
        Vec::new(),
        Vec::new(),
    )
    .expect("read snapshot");
    let post_image = EntityPostImage::new(target.clone(), plan.contract_version(), record(1))
        .expect("entity post-image");
    let event_intent = EventIntent::new(EventTypeId::new(1).expect("event type"), record(1))
        .expect("event intent");
    let declared_outcome = DeclaredOutcome::new(OutcomeId::new(1).expect("outcome ID"), record(1))
        .expect("declared outcome");
    let evaluated = riffdb_storage_api::EvaluatedCommand::new(
        &snapshot,
        vec![EntityMutation::Create(post_image.clone())],
        vec![event_intent],
        declared_outcome.clone(),
        EvaluationBudget::v1(),
    )
    .expect("evaluated command");
    let partition_hash = hash_partition_key(partition.as_bytes());
    let context = PreEvaluationCommitContext::new(pending.clone(), partition_hash, Vec::new())
        .expect("commit context");
    let candidates =
        IdempotencyLookupCandidatesV1::new(vec![identity.clone()]).expect("lookup candidates");
    let intent = riffdb_storage_api::CommitIntent::new_for_vacant_terminal_admission(
        context,
        candidates.clone(),
        evaluated,
        provenance_id,
    )
    .expect("fused terminal commit intent");

    let stored_entity = StoredEntityRecordV1::new(
        target.clone(),
        EntityVersion::first(),
        plan.contract_version(),
        DurableKeySchemaBindingV1::from_plan(&plan),
        record(1),
    )
    .expect("stored entity");
    let mutation = riffdb_storage_api::CommittedEntityMutationV1::new(
        ExpectedEntityState::Absent,
        stored_entity,
    )
    .expect("committed entity mutation");
    let index_record = StoredIndexEntryV2::new(
        index_key.clone(),
        DurableKeySchemaBindingV1::from_plan(&plan),
        record(1),
        pending.partition_key().clone(),
    )
    .expect("stored index entry");
    let index_mutation = IndexEntryMutationV1::Put(index_record);
    let generation = PartitionIndexTarget::new(partition.clone(), index_key.index_id());
    let affected_targets =
        AffectedIndexEpochTargets::new(vec![generation.clone()]).expect("affected targets");
    let affected_current = AffectedEpochCurrentState::new(
        &affected_targets,
        vec![CurrentIndexGenerationObservation::new(
            generation.clone(),
            IndexEpochPosition::BeforeFirst,
        )],
    )
    .expect("affected current state");
    let epoch_advance = IndexEpochAdvanceV1::new(
        generation,
        DurableKeySchemaBindingV1::from_plan(&plan),
        IndexEpochPosition::BeforeFirst,
    )
    .expect("epoch advance");
    let upper_bound = match command_write_set_upper_bound_v1(
        &intent,
        std::slice::from_ref(&index_mutation),
        std::slice::from_ref(&epoch_advance),
    )
    .expect("canonical encoded upper bound")
    {
        EncodedWriteSetUpperBoundResultV1::Fits(bound) => bound,
        EncodedWriteSetUpperBoundResultV1::ExceedsAcceptedAggregateCap(_) => {
            panic!("recovery fixture write set must fit the accepted aggregate cap")
        }
    };
    let write_plan = CommandWriteSetPlanV1::new(
        &intent,
        affected_targets.clone(),
        affected_current,
        vec![index_mutation],
        vec![epoch_advance],
        upper_bound,
    )
    .expect("write plan");

    let assignment = AssignedCommandSequence::from_assigned(sequence);
    let event_id = EventId::new(sequence, 0);
    let event_type_id = EventTypeId::new(1).expect("event type");
    let event_payload = record(1);
    let event = StoredDurableEventV1::new(
        event_id,
        event_type_id,
        event_payload.clone(),
        derive_event_hash_v1(event_id, event_type_id, &event_payload).expect("event hash"),
    )
    .expect("durable event");
    let stored_outcome = StoredOutcomeV1::new(
        identity.clone(),
        sequence,
        request_id,
        plan.clone(),
        pending.canonical_input_hash(),
        actor.clone(),
        logical_time,
        partition.clone(),
        partition_hash,
        Vec::new(),
        declared_outcome.clone(),
        StoredAdmittedProvenanceClaimsV1::default(),
        provenance_id,
        DurabilityMode::Sync,
    )
    .expect("stored outcome");
    let mutations = vec![mutation];
    let provenance = StoredProvenanceRecordV1::new(
        provenance_id,
        sequence,
        identity,
        request_id,
        plan.clone(),
        pending.canonical_input_hash(),
        actor.clone(),
        logical_time,
        partition_hash,
        Vec::new(),
        declared_outcome.outcome_id(),
        vec![AffectedEntityV1::from_record(mutations[0].post_image())],
        vec![event_id],
        StoredAdmittedProvenanceClaimsV1::default(),
    )
    .expect("provenance");
    let commit = riffdb_storage_api::StoredCommitRecordV1::new(
        sequence,
        request_id,
        plan,
        pending.canonical_input_hash(),
        actor,
        logical_time,
        partition_hash,
        Vec::new(),
        StoredReadDependenciesV1::from_live(snapshot.read_dependencies())
            .expect("stored dependencies"),
        mutations
            .iter()
            .map(riffdb_storage_api::CommittedEntityReferenceV2::from_mutation)
            .collect::<Result<Vec<_>, _>>()
            .expect("entity references"),
        vec![event.clone()],
        declared_outcome,
        provenance_id,
        vec![event_id],
        DurabilityMode::Sync,
    )
    .expect("commit record");
    let records = AtomicCommandRecordSet::new(
        assignment,
        mutations,
        write_plan.clone(),
        stored_outcome,
        provenance,
        commit,
    )
    .expect("atomic command record set");

    CommandFixture {
        candidates,
        pending,
        intent,
        affected_targets,
        write_plan,
        records,
        target,
        range,
        index_key,
    }
}

fn catalog_principal() -> AuditPrincipalV1 {
    AuditPrincipalV1::new(
        ActorId::new("storage-recovery-maintainer").expect("catalog principal"),
        ActorKind::Human,
        CapabilityId::from_bytes(uuid_bytes(0x61)).expect("catalog capability"),
        NonZeroU64::MIN,
    )
}

fn prepare_command_database(path: &Path) {
    let mut store = RedbStore::open(path).expect("open command recovery database");
    store
        .initialize_database(database_id())
        .expect("initialize command recovery database");
    let mut ports = open_operational(store);
    let bundle = contract_bundle();
    let result = ports
        .activate_catalog(&CatalogActivationIntentV1::new(
            None,
            bundle.clone(),
            RequestId::from_bytes(uuid_bytes(0x62)).expect("catalog request"),
            catalog_principal(),
            Timestamp::new(1_700_000_000, 0).expect("catalog timestamp"),
            None,
        ))
        .expect("activate command recovery catalog");
    assert!(
        matches!(
            &result,
            CatalogActivationResult::Activated { active, .. }
                if *active == riffdb_storage_api::ActiveCatalogPointerV1::from_bundle(&bundle)
        ),
        "unexpected catalog activation result: {result:?}"
    );
}

fn commit_command_fixture(ports: &RedbOperationalPorts, fixture: &CommandFixture) {
    let candidate = ports
        .begin_empty_batch()
        .expect("begin command batch")
        .begin_candidate(Box::new(fixture.intent.clone()))
        .expect("begin command candidate");
    let CandidateAdmissionResult::Proceed(candidate) = candidate
        .recheck_admission()
        .expect("recheck pending admission")
    else {
        panic!("fresh vacant terminal admission must proceed");
    };
    let (candidate, current) = candidate
        .read_transaction_current()
        .expect("read transaction-current state");
    assert_eq!(
        current.bindings()[0].expected_state(),
        ExpectedEntityState::Absent
    );
    let candidate = candidate
        .plan_validated(fixture.affected_targets.clone())
        .read_affected_epoch_current()
        .expect("read affected epoch state");
    let CandidateCapacityResult::Reserved(candidate) = candidate
        .reserve_capacity(fixture.write_plan.clone())
        .expect("reserve complete command graph")
    else {
        panic!("small recovery fixture must reserve");
    };
    let candidate = candidate.assign_sequence().expect("assign sequence");
    assert_eq!(
        candidate.assignment().assigned(),
        fixture.records.commit().commit_sequence()
    );
    candidate
        .stage(fixture.records.clone())
        .expect("stage complete command graph")
        .commit_with_service_audit_transitions(
            DurabilityMode::Sync,
            vec![command_audit_transition(fixture)],
        )
        .expect("commit complete command graph and audit lifecycle");
}

fn apply_unpublished_command_fixture(
    epoch: RedbDurabilityEpoch,
    fixture: &CommandFixture,
) -> RedbDurabilityEpoch {
    let candidate = DeferredCommandEpoch::begin_empty_batch(epoch)
        .expect("begin deferred command batch")
        .begin_candidate(Box::new(fixture.intent.clone()))
        .expect("begin deferred command candidate");
    let CandidateAdmissionResult::Proceed(candidate) = candidate
        .recheck_admission()
        .expect("recheck deferred pending admission")
    else {
        panic!("fresh vacant deferred admission must proceed");
    };
    let (candidate, current) = candidate
        .read_transaction_current()
        .expect("read deferred transaction-current state");
    assert_eq!(
        current.bindings()[0].expected_state(),
        ExpectedEntityState::Absent
    );
    let candidate = candidate
        .plan_validated(fixture.affected_targets.clone())
        .read_affected_epoch_current()
        .expect("read deferred affected epoch state");
    let CandidateCapacityResult::Reserved(candidate) = candidate
        .reserve_capacity(fixture.write_plan.clone())
        .expect("reserve deferred complete command graph")
    else {
        panic!("small deferred fixture must reserve");
    };
    let candidate = candidate
        .assign_sequence()
        .expect("assign deferred sequence");
    assert_eq!(
        candidate.assignment().assigned(),
        fixture.records.commit().commit_sequence()
    );
    candidate
        .stage(fixture.records.clone())
        .expect("stage deferred complete command graph")
        .apply_unpublished_with_service_audit_transitions(
            DurabilityMode::Sync,
            vec![command_audit_transition(fixture)],
        )
        .expect("apply complete command graph without publication")
}

fn commit_command_group(ports: &RedbOperationalPorts, fixtures: &[CommandFixture]) {
    try_commit_command_group(ports, fixtures, fixtures).expect("commit complete command group");
}

fn try_commit_command_group(
    ports: &RedbOperationalPorts,
    fixtures: &[CommandFixture],
    audit_fixtures: &[CommandFixture],
) -> Result<(), riffdb_storage_api::StorageError> {
    let (first, remaining) = fixtures.split_first().expect("non-empty recovery group");
    let candidate = ports
        .begin_empty_batch()
        .expect("begin serial recovery batch")
        .begin_candidate(Box::new(first.intent.clone()))
        .expect("begin first candidate");
    let CandidateAdmissionResult::Proceed(candidate) = candidate
        .recheck_admission()
        .expect("recheck first admission")
    else {
        panic!("first fresh admission proceeds");
    };
    let (candidate, _) = candidate
        .read_transaction_current()
        .expect("read first current state");
    let candidate = candidate
        .plan_validated(first.affected_targets.clone())
        .read_affected_epoch_current()
        .expect("read first affected state");
    let CandidateCapacityResult::Reserved(candidate) = candidate
        .reserve_capacity(first.write_plan.clone())
        .expect("reserve first graph")
    else {
        panic!("first graph fits");
    };
    let mut batch = candidate
        .assign_sequence()
        .expect("assign first sequence")
        .stage(first.records.clone())
        .expect("stage first graph");

    for fixture in remaining {
        let CandidateStartResult::Started(candidate) = batch
            .begin_candidate(Box::new(fixture.intent.clone()))
            .expect("begin next recovery candidate")
        else {
            panic!("bounded recovery group fits");
        };
        let CandidateAdmissionResult::Proceed(candidate) = candidate
            .recheck_admission()
            .expect("recheck next admission")
        else {
            panic!("fresh grouped admission proceeds");
        };
        let (candidate, _) = candidate
            .read_transaction_current()
            .expect("read next transaction-local state");
        let candidate = candidate
            .plan_validated(fixture.affected_targets.clone())
            .read_affected_epoch_current()
            .expect("read next affected state");
        let CandidateCapacityResult::Reserved(candidate) = candidate
            .reserve_capacity(fixture.write_plan.clone())
            .expect("reserve next graph")
        else {
            panic!("next graph fits");
        };
        batch = candidate
            .assign_sequence()
            .expect("assign next sequence")
            .stage(fixture.records.clone())
            .expect("stage next graph");
    }
    batch
        .commit_with_service_audit_transitions(
            DurabilityMode::Sync,
            audit_fixtures
                .iter()
                .map(command_audit_transition)
                .collect(),
        )
        .map(|_| ())
}

fn command_audit_transition(
    fixture: &CommandFixture,
) -> riffdb_storage_api::CommandServiceAuditTransitionV1 {
    let principal = catalog_principal();
    let started = ServiceAuditAppendIntentV1::new(
        fixture.pending.admission_request_id(),
        Timestamp::new(1_700_000_002, 0).expect("started timestamp"),
        ServiceOperationV1::ExecuteCommand,
        ServiceAuditPhaseV1::Started,
        principal.clone(),
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::empty(),
        None,
        ServiceAuditLinkV1::None,
    )
    .expect("started audit");
    let terminal = ServiceAuditAppendIntentV1::new(
        fixture.pending.admission_request_id(),
        Timestamp::new(1_700_000_003, 0).expect("terminal timestamp"),
        ServiceOperationV1::ExecuteCommand,
        ServiceAuditPhaseV1::Succeeded,
        principal,
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::empty(),
        None,
        ServiceAuditLinkV1::Command {
            commit_sequence: fixture.records.commit().commit_sequence(),
            provenance_id: fixture.records.provenance().provenance_id(),
        },
    )
    .expect("terminal audit");
    riffdb_storage_api::CommandServiceAuditTransitionV1::started_and_terminal(started, terminal)
        .expect("fused command audit lifecycle")
}

fn command_audit_phases(ports: &RedbOperationalPorts) -> Vec<ServiceAuditPhaseV1> {
    let scan = ports
        .scan_administration_audit(AdministrationAuditScanRequest::new(
            None,
            StorageScanLimit::new(64).expect("audit scan limit"),
        ))
        .expect("scan audit");
    let AdministrationAuditScan::ExactEnd { records } = scan else {
        panic!("small recovery audit stream must reach exact end");
    };
    records
        .into_iter()
        .filter_map(|record| match record.into_parts().0 {
            StoredAdministrationAuditRecordV1::Service(service)
                if service.operation() == ServiceOperationV1::ExecuteCommand =>
            {
                Some(service.phase())
            }
            _ => None,
        })
        .collect()
}

fn committed_index_entry(fixture: &CommandFixture) -> StoredIndexEntryV2 {
    let IndexEntryMutationV1::Put(entry) = &fixture.records.index_entries()[0] else {
        panic!("command fixture must install one index entry");
    };
    entry.clone()
}

fn prepare_committed_command_database(path: &Path) -> (CommandFixture, StoredIndexEntryV2) {
    prepare_command_database(path);
    let fixture = command_fixture();
    let ports = open_operational(RedbStore::open(path).expect("reopen command fixture database"));
    commit_command_fixture(&ports, &fixture);
    let index_entry = committed_index_entry(&fixture);
    drop(ports);
    (fixture, index_entry)
}

/// Two real committed commands (sequences 1 and 2, disjoint entities) — the
/// populated-history shape every retention prune test exercises.
fn prepare_two_command_database(path: &Path) -> (CommandFixture, CommandFixture) {
    prepare_command_database(path);
    let first = command_fixture_at(1);
    let second = command_fixture_at(2);
    let ports = open_operational(RedbStore::open(path).expect("reopen command fixture database"));
    commit_command_fixture(&ports, &first);
    commit_command_fixture(&ports, &second);
    drop(ports);
    (first, second)
}

fn migration_index_entry(entity_value: u64, partition_value: u64) -> StoredIndexEntryV2 {
    migration_index_entry_with_covered_values(entity_value, partition_value, record(entity_value))
}

fn migration_index_entry_with_covered_values(
    entity_value: u64,
    partition_value: u64,
    covered_values: CanonicalRecord,
) -> StoredIndexEntryV2 {
    let mut entity_key = EntityKeyBuilder::new(EntityTypeId::new(1).expect("entity type ID"));
    entity_key
        .push_u64(entity_value)
        .expect("entity key component");
    let entity_key = entity_key.finish().expect("entity key");
    let mut index_key = IndexEntryKeyBuilder::new(IndexId::new(1).expect("index ID"));
    index_key.push_u64(10).expect("index component");
    let index_key = index_key.finish(entity_key).expect("index entry key");
    let mut partition = PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate"));
    partition
        .push_u64(partition_value)
        .expect("partition component");
    StoredIndexEntryV2::new(
        index_key,
        DurableKeySchemaBindingV1::from_plan(&plan()),
        covered_values,
        partition.finish().expect("partition key"),
    )
    .expect("migration V2 row")
}

fn legacy_index_entry(current: &StoredIndexEntryV2) -> StoredIndexEntryV1 {
    StoredIndexEntryV1::new(
        current.key().clone(),
        current.schema_binding().clone(),
        current.covered_values().clone(),
    )
    .expect("legacy migration row")
}

fn write_raw_index_entries(
    path: &Path,
    entries: &[(IndexEntryKey, Vec<u8>)],
) -> Vec<Option<Vec<u8>>> {
    let database = Database::create(path).expect("open raw migration fixture");
    let transaction = database.begin_write().expect("begin raw migration write");
    let mut previous = Vec::with_capacity(entries.len());
    {
        let mut table = transaction
            .open_table(SECONDARY_INDEXES)
            .expect("open raw secondary-index table");
        for (key, envelope) in entries {
            previous.push(
                table
                    .insert(key.as_bytes(), envelope.as_slice())
                    .expect("write raw index envelope")
                    .map(|value| value.value().to_vec()),
            );
        }
    }
    transaction.commit().expect("commit raw migration fixture");
    previous
}

fn read_raw_index_entries(path: &Path) -> Vec<(Vec<u8>, Vec<u8>)> {
    let database = Database::create(path).expect("open raw migration result");
    let transaction = database.begin_read().expect("begin raw migration read");
    let table = transaction
        .open_table(SECONDARY_INDEXES)
        .expect("open raw secondary-index table");
    table
        .iter()
        .expect("iterate raw secondary-index table")
        .map(|entry| {
            let (key, value) = entry.expect("read raw secondary-index row");
            (key.value().to_vec(), value.value().to_vec())
        })
        .collect()
}

#[derive(Debug, Eq, PartialEq)]
struct MigrationControlState {
    tables: Vec<String>,
    metadata: Vec<(String, Vec<u8>)>,
}

fn read_migration_control_state(path: &Path) -> MigrationControlState {
    let database = Database::create(path).expect("open raw migration control state");
    let transaction = database.begin_read().expect("begin migration control read");
    let mut tables = transaction
        .list_tables()
        .expect("list migration fixture tables")
        .map(|table| table.name().to_owned())
        .collect::<Vec<_>>();
    tables.sort();
    let metadata = transaction
        .open_table(META)
        .expect("open migration fixture metadata")
        .iter()
        .expect("iterate migration fixture metadata")
        .filter_map(|entry| {
            let (key, value) = entry.expect("read migration fixture metadata");
            let key = key.value().to_owned();
            // Optional validated-prefix checkpoint is rewritten on every clean
            // finish and is not a migration control marker.
            if key == "validated_prefix_checkpoint/v1" {
                return None;
            }
            Some((key, value.value().to_vec()))
        })
        .collect();
    MigrationControlState { tables, metadata }
}

#[derive(Debug, Default, Eq, PartialEq)]
struct MigrationObservation {
    v1_rewrites: usize,
    v2_confirms: usize,
    pages: usize,
}

fn drive_index_migration(
    port: RedbStartupIndexMigrationPort,
    context: CatalogIndexMigrationContext,
    controller: &RedbTestController,
) -> (RedbStore, MigrationObservation) {
    let store = CatalogIndexMigrationDriver::new(context, port)
        .expect("bind same-session catalog migration")
        .run()
        .expect("drive catalog-owned migration");
    let (pages, v1_rewrites, v2_confirms) = controller.index_migration_observation();
    (
        store,
        MigrationObservation {
            v1_rewrites,
            v2_confirms,
            pages,
        },
    )
}

fn assert_precommit_command_state(ports: &RedbOperationalPorts, fixture: &CommandFixture) {
    assert_eq!(
        ports
            .lookup_admission(fixture.candidates.clone())
            .expect("lookup precommit admission"),
        AdmissionLookupResultV1::NotFound,
        "a crash before the fused terminal commit must leave no admission"
    );
    assert!(
        command_audit_phases(ports).is_empty(),
        "a precommit crash must leave no partial command audit lifecycle"
    );
    assert_eq!(
        ports.read_entity(&fixture.target).expect("read entity"),
        None
    );
    assert_eq!(
        ports
            .read_stored_outcome(fixture.pending.identity())
            .expect("read terminal outcome"),
        None
    );
    assert_eq!(
        ports
            .read_commit(CommitSequence::first())
            .expect("read commit"),
        None
    );
    assert_eq!(
        ports
            .read_provenance(fixture.records.provenance().provenance_id())
            .expect("read provenance"),
        None
    );
    let event_id = EventId::new(CommitSequence::first(), 0);
    assert_eq!(
        ports
            .read_durable_event(event_id)
            .expect("read durable event"),
        None
    );
    let route_scan = ports
        .scan_partition_event_routes(EventRouteScanRequestV1::initial(
            hash_partition_key(fixture.pending.partition_key().as_bytes()),
            None,
            EventRoutePageLimit::new(NonZeroU16::MIN).expect("event-route limit"),
        ))
        .expect("scan precommit event routes");
    assert!(matches!(
        route_scan,
        EventRouteScanV1::ExactEnd {
            items,
            inclusive_upper: EventRouteUpperFenceV1::BeforeFirst,
        } if items.is_empty()
    ));
    assert_eq!(
        ports
            .read_outbox_status(event_id)
            .expect("read outbox status"),
        OutboxStatusReadResultV1::AuthoritativeIntentMissing
    );

    let limit = StorageScanLimit::new(1).expect("scan limit");
    let index = ports
        .scan_index(
            AuthoritativeIndexScanRequest::new(fixture.range.clone(), None, limit)
                .expect("index request"),
        )
        .expect("scan index");
    assert!(matches!(
        index,
        AuthoritativeIndexScanPage::ExactEnd {
            entries,
            epoch: IndexEpochPosition::BeforeFirst,
        } if entries.is_empty()
    ));
    let outbox = ports
        .scan_pending_outbox(
            None,
            OutboxPageLimit::new(NonZeroU16::MIN).expect("outbox limit"),
        )
        .expect("scan pending outbox");
    assert!(matches!(
        outbox,
        PendingOutboxScanV1::ExactEnd { items } if items.is_empty()
    ));

    let snapshot = ports
        .read_snapshot(
            SnapshotRequest::new(
                plan(),
                vec![fixture.target.clone()],
                Vec::new(),
                vec![fixture.range.clone()],
            )
            .expect("snapshot request"),
        )
        .expect("precommit snapshot");
    assert_eq!(snapshot.observed_through(), None);
    assert_eq!(
        snapshot.bindings(),
        &[EntityObservation::Absent(fixture.target.clone())]
    );
    assert_eq!(
        snapshot.ranges()[0].epoch(),
        IndexEpochPosition::BeforeFirst
    );
    assert!(snapshot.ranges()[0].entries().is_empty());
}

fn assert_postcommit_command_state(ports: &RedbOperationalPorts, fixture: &CommandFixture) {
    let AdmissionLookupResultV1::Found(admission) = ports
        .lookup_admission(fixture.candidates.clone())
        .expect("lookup committed admission")
    else {
        panic!("the terminal admission must be present");
    };
    assert_eq!(
        *admission,
        riffdb_storage_api::StoredAdmissionStateV1::StoredOutcome(
            fixture.records.stored_outcome().clone()
        )
    );
    assert_eq!(
        ports.read_entity(&fixture.target).expect("read entity"),
        Some(fixture.records.entities()[0].post_image().clone())
    );
    let stored_outcome = ports
        .read_stored_outcome(fixture.pending.identity())
        .expect("read terminal outcome")
        .expect("committed outcome");
    assert_eq!(&stored_outcome, fixture.records.stored_outcome());
    assert_eq!(
        stored_outcome.partition_key(),
        fixture.pending.partition_key(),
        "the exact admitted partition key must survive commit and reopen"
    );
    assert_eq!(
        ports
            .read_commit(CommitSequence::first())
            .expect("read commit"),
        Some(fixture.records.commit().clone())
    );
    assert_eq!(
        ports
            .read_provenance(fixture.records.provenance().provenance_id())
            .expect("read provenance"),
        Some(fixture.records.provenance().clone())
    );
    assert_eq!(
        command_audit_phases(ports),
        [ServiceAuditPhaseV1::Started, ServiceAuditPhaseV1::Succeeded],
        "the complete command graph and audit lifecycle must recover together"
    );
    let event = &fixture.records.events()[0];
    assert_eq!(
        ports
            .read_durable_event(event.event_id())
            .expect("read durable event"),
        Some(event.clone())
    );
    let route_scan = ports
        .scan_partition_event_routes(EventRouteScanRequestV1::initial(
            hash_partition_key(fixture.pending.partition_key().as_bytes()),
            None,
            EventRoutePageLimit::new(NonZeroU16::MIN).expect("event-route limit"),
        ))
        .expect("scan committed event routes");
    let EventRouteScanV1::ExactEnd {
        items,
        inclusive_upper: EventRouteUpperFenceV1::Inclusive(upper),
    } = route_scan
    else {
        panic!("one committed event route must reach exact end");
    };
    assert_eq!(upper, event.event_id());
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].value().event_id(), event.event_id());
    assert_eq!(items[0].value().event_type_id(), event.event_type_id());
    assert_eq!(items[0].value().event_hash(), event.event_hash());
    assert_eq!(
        ports
            .read_outbox_status(event.event_id())
            .expect("read outbox status"),
        OutboxStatusReadResultV1::Status(OutboxStatusObservationV1::AbsentInitialPending)
    );

    let limit = StorageScanLimit::new(1).expect("scan limit");
    let index = ports
        .scan_index(
            AuthoritativeIndexScanRequest::new(fixture.range.clone(), None, limit)
                .expect("index request"),
        )
        .expect("scan index");
    let AuthoritativeIndexScanPage::ExactEnd { entries, epoch } = index else {
        panic!("one index row must reach exact end");
    };
    assert_eq!(
        epoch,
        IndexEpochPosition::Value(fixture.records.index_epochs()[0].next())
    );
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].value().key(), &fixture.index_key);
    assert_eq!(entries[0].value().covered_values(), &record(1));

    let outbox = ports
        .scan_pending_outbox(
            None,
            OutboxPageLimit::new(NonZeroU16::MIN).expect("outbox limit"),
        )
        .expect("scan pending outbox");
    let PendingOutboxScanV1::ExactEnd { items } = outbox else {
        panic!("one pending outbox tuple must reach exact end");
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].value().event(), event);
    assert_eq!(
        items[0].value().intent(),
        &fixture.records.outbox_intents()[0]
    );
    assert_eq!(
        items[0].value().status(),
        &OutboxStatusObservationV1::AbsentInitialPending
    );

    let snapshot = ports
        .read_snapshot(
            SnapshotRequest::new(
                plan(),
                vec![fixture.target.clone()],
                Vec::new(),
                vec![fixture.range.clone()],
            )
            .expect("snapshot request"),
        )
        .expect("postcommit snapshot");
    assert_eq!(snapshot.observed_through(), Some(CommitSequence::first()));
    assert_eq!(
        snapshot.bindings(),
        &[EntityObservation::Present(
            fixture.records.entities()[0].post_image().clone()
        )]
    );
    assert_eq!(
        snapshot.ranges()[0].epoch(),
        IndexEpochPosition::Value(fixture.records.index_epochs()[0].next())
    );
    assert_eq!(snapshot.ranges()[0].entries().len(), 1);
    assert_eq!(snapshot.ranges()[0].entries()[0].key(), &fixture.index_key);
}

#[test]
fn process_recovery_child() {
    let Ok(mode) = std::env::var(CHILD_MODE) else {
        return;
    };
    let path = PathBuf::from(std::env::var_os(CHILD_PATH).expect("child database path"));
    let profile = match std::env::var(CHILD_COMMIT_PROFILE).as_deref() {
        Ok("standard") => RedbCommitProfile::Standard,
        Ok("hardened") => RedbCommitProfile::Hardened,
        _ => panic!("unknown closed child commit profile"),
    };
    let controller = match mode.as_str() {
        "before-initialization-commit" => {
            RedbTestController::abort_before_commit(RedbTestOperation::Initialization)
        }
        "after-initialization-commit" => {
            RedbTestController::abort_after_commit(RedbTestOperation::Initialization)
        }
        "before-command-batch-commit" => {
            RedbTestController::abort_before_commit(RedbTestOperation::CommandBatch)
        }
        "after-command-batch-commit" => {
            RedbTestController::abort_after_commit(RedbTestOperation::CommandBatch)
        }
        "before-command-group-commit" => {
            RedbTestController::abort_before_commit(RedbTestOperation::CommandBatch)
        }
        "after-command-group-commit" => {
            RedbTestController::abort_after_commit(RedbTestOperation::CommandBatch)
        }
        "before-deferred-command-commit" => {
            RedbTestController::abort_before_commit(RedbTestOperation::DeferredCommandBatch)
        }
        "after-deferred-command-commit" => {
            RedbTestController::abort_after_commit(RedbTestOperation::DeferredCommandBatch)
        }
        "after-command-epoch-tail" => {
            RedbTestController::abort_after_commit(RedbTestOperation::CommandEpochTail)
        }
        "before-index-migration-batch-commit" => {
            RedbTestController::abort_before_commit(RedbTestOperation::IndexMigrationBatch)
        }
        "after-index-migration-batch-commit" => {
            RedbTestController::abort_after_commit(RedbTestOperation::IndexMigrationBatch)
        }
        "before-validated-prefix-checkpoint-commit" => {
            RedbTestController::abort_before_commit(RedbTestOperation::ValidatedPrefixCheckpoint)
        }
        "after-validated-prefix-checkpoint-commit" => {
            RedbTestController::abort_after_commit(RedbTestOperation::ValidatedPrefixCheckpoint)
        }
        // Shutdown write point (ADR-0019 A1 write point 2): first ValidatedPrefixCheckpoint
        // hit is the startup finish write (non-fatally rejected so clean=true and no
        // durable checkpoint), second hit is the public write_validated_prefix_checkpoint.
        "before-shutdown-validated-prefix-checkpoint-commit" => {
            RedbTestController::return_before_then_abort_before(
                RedbTestOperation::ValidatedPrefixCheckpoint,
            )
        }
        "after-shutdown-validated-prefix-checkpoint-commit" => {
            RedbTestController::return_before_then_abort_after(
                RedbTestOperation::ValidatedPrefixCheckpoint,
            )
        }
        "before-retention-prune-subrange-commit" => {
            RedbTestController::abort_before_commit(RedbTestOperation::RetentionPruneSubrange)
        }
        "after-retention-prune-subrange-commit" => {
            RedbTestController::abort_after_commit(RedbTestOperation::RetentionPruneSubrange)
        }
        _ => panic!("unknown closed child mode"),
    };
    // Offline prune uses RedbOfflineRetention's own controller, not RedbStore.
    if matches!(
        mode.as_str(),
        "before-retention-prune-subrange-commit" | "after-retention-prune-subrange-commit"
    ) {
        let target = std::env::var(CHILD_PRUNE_TARGET)
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(1);
        let maintenance =
            riffdb_storage_redb::RedbOfflineRetention::bind_with_test_controller(&path, controller);
        let _ = maintenance.prune_to(target);
        panic!("the armed prune failpoint did not terminate the child");
    }
    let store =
        RedbStore::open_with_test_controller_and_commit_profile(path, profile, controller.clone())
            .expect("open child database");
    match mode.as_str() {
        "before-initialization-commit" | "after-initialization-commit" => {
            let mut store = store;
            let _ = store.initialize_database(database_id());
        }
        "before-command-batch-commit" | "after-command-batch-commit" => {
            let ports = open_operational(store);
            commit_command_fixture(&ports, &command_fixture());
        }
        "before-command-group-commit" | "after-command-group-commit" => {
            let ports = open_operational(store);
            let fixtures = (1..=riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS)
                .map(|ordinal| {
                    command_fixture_at(u64::try_from(ordinal).expect("bounded fixture ordinal"))
                })
                .collect::<Vec<_>>();
            commit_command_group(&ports, &fixtures);
        }
        "before-deferred-command-commit"
        | "after-deferred-command-commit"
        | "after-command-epoch-tail" => {
            let ports = open_operational(store);
            let epoch = ports
                .begin_deferred_command_epoch()
                .expect("begin child durability epoch");
            let epoch = apply_unpublished_command_fixture(epoch, &command_fixture());
            let _ = DeferredCommandEpoch::fence(epoch);
        }
        "before-index-migration-batch-commit" | "after-index-migration-batch-commit" => {
            let (_, catalog_outcome, outcome) = complete_startup_pass(store);
            let CatalogHistoryOutcome::MigrationRequired(context) = catalog_outcome else {
                panic!("migration failpoint child requires catalog migration context");
            };
            let StructuralOpenOutcome::MigrationRequired(port) = outcome else {
                panic!("migration failpoint child requires one V1 index row");
            };
            let _ = drive_index_migration(port, context, &controller);
        }
        "before-validated-prefix-checkpoint-commit"
        | "after-validated-prefix-checkpoint-commit" => {
            // Seeded DB reopens and complete_startup_pass writes the checkpoint on finish.
            let _ = complete_startup_pass(store);
        }
        "before-shutdown-validated-prefix-checkpoint-commit"
        | "after-shutdown-validated-prefix-checkpoint-commit" => {
            // Drive the real public entry (write point 2), not the startup finish write.
            let ports = open_operational(store);
            let _ = ports.write_validated_prefix_checkpoint();
        }
        _ => unreachable!("controller match rejects unknown modes"),
    }
    panic!("the armed failpoint did not terminate the child");
}

#[test]
fn deferred_command_root_is_invisible_until_the_tail_fence_publishes_it() {
    let path = TestDatabasePath::new("deferred-frontier");
    prepare_command_database(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).expect("reopen command database"));
    let fixture = command_fixture();

    let epoch = ports
        .begin_deferred_command_epoch()
        .expect("begin standard durability epoch");
    let epoch = apply_unpublished_command_fixture(epoch, &fixture);

    assert_precommit_command_state(&ports, &fixture);
    let committed = DeferredCommandEpoch::fence(epoch).expect("fence deferred command epoch");
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].batch().outcomes().len(), 1);
    assert_postcommit_command_state(&ports, &fixture);
}

#[test]
fn multiple_deferred_subgroups_publish_together_in_sequence_order() {
    let path = TestDatabasePath::new("deferred-multiple-subgroups");
    prepare_command_database(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).expect("reopen command database"));
    let first = command_fixture_at(1);
    let second = command_fixture_at(2);

    let epoch = ports
        .begin_deferred_command_epoch()
        .expect("begin standard durability epoch");
    let epoch = apply_unpublished_command_fixture(epoch, &first);
    let epoch = apply_unpublished_command_fixture(epoch, &second);
    for fixture in [&first, &second] {
        assert_eq!(
            ports
                .read_entity(&fixture.target)
                .expect("read predecessor entity frontier"),
            None
        );
    }

    let committed = DeferredCommandEpoch::fence(epoch).expect("fence both deferred subgroups");
    assert_eq!(committed.len(), 2);
    assert_eq!(
        committed
            .iter()
            .map(|batch| batch.batch().outcomes()[0].commit_sequence())
            .collect::<Vec<_>>(),
        vec![
            CommitSequence::new(1).expect("sequence one"),
            CommitSequence::new(2).expect("sequence two"),
        ]
    );
    for fixture in [&first, &second] {
        let AdmissionLookupResultV1::Found(admission) = ports
            .lookup_admission(fixture.candidates.clone())
            .expect("lookup fenced subgroup identity")
        else {
            panic!("each fenced subgroup identity must be terminal");
        };
        assert_eq!(
            *admission,
            riffdb_storage_api::StoredAdmissionStateV1::StoredOutcome(
                fixture.records.stored_outcome().clone()
            )
        );
        assert_eq!(
            ports
                .read_entity(&fixture.target)
                .expect("read fenced entity"),
            Some(fixture.records.entities()[0].post_image().clone())
        );
        assert_eq!(
            ports
                .read_commit(fixture.records.commit().commit_sequence())
                .expect("read fenced commit"),
            Some(fixture.records.commit().clone())
        );
    }
    assert_eq!(
        command_audit_phases(&ports),
        vec![
            ServiceAuditPhaseV1::Started,
            ServiceAuditPhaseV1::Succeeded,
            ServiceAuditPhaseV1::Started,
            ServiceAuditPhaseV1::Succeeded,
        ]
    );
}

#[test]
fn multiple_sealed_epochs_publish_in_fifo_order_after_one_journal_flush() {
    let path = TestDatabasePath::new("deferred-multiple-epochs");
    prepare_command_database(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).expect("reopen command database"));
    let first = command_fixture_at(1);
    let second = command_fixture_at(2);

    let first_epoch = ports
        .begin_deferred_command_epoch()
        .expect("begin first standard durability epoch");
    let first_fence =
        DeferredCommandEpoch::seal(apply_unpublished_command_fixture(first_epoch, &first))
            .expect("seal first standard durability epoch");
    let second_epoch = ports
        .begin_deferred_command_epoch()
        .expect("begin second standard durability epoch");
    let second_fence =
        DeferredCommandEpoch::seal(apply_unpublished_command_fixture(second_epoch, &second))
            .expect("seal second standard durability epoch");

    for fixture in [&first, &second] {
        assert_eq!(
            ports
                .read_entity(&fixture.target)
                .expect("read predecessor entity frontier"),
            None
        );
    }
    let first_committed = first_fence.wait().expect("publish first journal frame");
    assert_eq!(first_committed.len(), 1);
    assert_postcommit_command_state(&ports, &first);
    assert_eq!(
        ports
            .read_entity(&second.target)
            .expect("read second unpublished entity"),
        None
    );
    let second_committed = second_fence.wait().expect("publish second journal frame");
    assert_eq!(second_committed.len(), 1);
    let AdmissionLookupResultV1::Found(admission) = ports
        .lookup_admission(second.candidates.clone())
        .expect("lookup second fenced identity")
    else {
        panic!("the second fenced identity must be terminal");
    };
    assert_eq!(
        *admission,
        riffdb_storage_api::StoredAdmissionStateV1::StoredOutcome(
            second.records.stored_outcome().clone()
        )
    );
    assert_eq!(
        ports
            .read_entity(&second.target)
            .expect("read second entity"),
        Some(second.records.entities()[0].post_image().clone())
    );
    assert_eq!(
        ports
            .read_commit(CommitSequence::new(2).expect("sequence two"))
            .expect("read second commit"),
        Some(second.records.commit().clone())
    );
}

#[test]
fn direct_commit_reanchors_a_drained_journal_before_the_next_deferred_epoch() {
    let path = TestDatabasePath::new("journal-direct-journal-frontiers");
    prepare_command_database(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).expect("reopen command database"));
    let first = command_fixture_at(1);
    let direct = command_fixture_at(2);
    let third = command_fixture_at(3);

    let first_epoch = ports
        .begin_deferred_command_epoch()
        .expect("begin first journal epoch");
    DeferredCommandEpoch::fence(apply_unpublished_command_fixture(first_epoch, &first))
        .expect("publish first journal epoch");

    commit_command_fixture(&ports, &direct);

    let third_epoch = ports
        .begin_deferred_command_epoch()
        .expect("begin journal epoch after direct commit");
    DeferredCommandEpoch::fence(apply_unpublished_command_fixture(third_epoch, &third))
        .expect("publish journal epoch after direct commit");

    for fixture in [&first, &direct, &third] {
        assert_eq!(
            ports
                .read_entity(&fixture.target)
                .expect("read committed entity"),
            Some(fixture.records.entities()[0].post_image().clone())
        );
        assert_eq!(
            ports
                .read_commit(fixture.records.commit().commit_sequence())
                .expect("read committed command"),
            Some(fixture.records.commit().clone())
        );
    }
    assert_eq!(
        command_audit_phases(&ports),
        vec![
            ServiceAuditPhaseV1::Started,
            ServiceAuditPhaseV1::Succeeded,
            ServiceAuditPhaseV1::Started,
            ServiceAuditPhaseV1::Succeeded,
            ServiceAuditPhaseV1::Started,
            ServiceAuditPhaseV1::Succeeded,
        ]
    );
}

#[test]
fn direct_commit_reanchors_an_unopened_empty_journal_before_a_deferred_epoch() {
    let path = TestDatabasePath::new("direct-empty-journal-frontier");
    prepare_command_database(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).expect("reopen command database"));
    let direct = command_fixture_at(1);
    let deferred = command_fixture_at(2);

    commit_command_fixture(&ports, &direct);

    let epoch = ports
        .begin_deferred_command_epoch()
        .expect("begin journal epoch after the first direct commit");
    DeferredCommandEpoch::fence(apply_unpublished_command_fixture(epoch, &deferred))
        .expect("publish journal epoch after the first direct commit");

    for fixture in [&direct, &deferred] {
        assert_eq!(
            ports
                .read_entity(&fixture.target)
                .expect("read committed entity"),
            Some(fixture.records.entities()[0].post_image().clone())
        );
        assert_eq!(
            ports
                .read_commit(fixture.records.commit().commit_sequence())
                .expect("read committed command"),
            Some(fixture.records.commit().clone())
        );
    }
    assert_eq!(
        command_audit_phases(&ports),
        vec![
            ServiceAuditPhaseV1::Started,
            ServiceAuditPhaseV1::Succeeded,
            ServiceAuditPhaseV1::Started,
            ServiceAuditPhaseV1::Succeeded,
        ]
    );
}

#[test]
fn unknown_tail_status_keeps_the_predecessor_frontier_and_fences_writes() {
    let path = TestDatabasePath::new("deferred-tail-unknown");
    prepare_command_database(&path.0);
    let controller =
        RedbTestController::return_unknown_after_commit(RedbTestOperation::CommandEpochTail);
    let ports = open_operational(
        RedbStore::open_with_test_controller(&path.0, controller)
            .expect("reopen controlled command database"),
    );
    let fixture = command_fixture();
    let epoch = ports
        .begin_deferred_command_epoch()
        .expect("begin controlled durability epoch");
    let epoch = apply_unpublished_command_fixture(epoch, &fixture);

    let error = DeferredCommandEpoch::fence(epoch).expect_err("tail status must be unknown");
    assert_eq!(
        error.kind(),
        riffdb_storage_api::StorageErrorKind::CommitStatusUnknown
    );
    assert_eq!(
        ports
            .read_entity(&fixture.target)
            .expect("fenced handle retains predecessor frontier"),
        None
    );
    let Err(write_error) = ports.begin_empty_batch() else {
        panic!("unknown tail status must fence authoritative writes");
    };
    assert_eq!(
        write_error.kind(),
        riffdb_storage_api::StorageErrorKind::Unavailable
    );
    drop(ports);

    let reopened =
        open_operational(RedbStore::open(&path.0).expect("reopen known-committed epoch"));
    assert_postcommit_command_state(&reopened, &fixture);
}

#[test]
fn hardened_profile_rejects_deferred_command_epochs() {
    let path = TestDatabasePath::new("deferred-hardened-rejected");
    prepare_command_database(&path.0);
    let ports = open_operational(
        RedbStore::open_with_commit_profile(&path.0, RedbCommitProfile::Hardened)
            .expect("reopen hardened command database"),
    );

    let Err(error) = ports.begin_deferred_command_epoch() else {
        panic!("hardened profile must keep independently Immediate groups");
    };
    assert_eq!(
        error.kind(),
        riffdb_storage_api::StorageErrorKind::Unavailable
    );
}

#[test]
fn all_v1_index_rows_migrate_exactly_and_require_a_fresh_clean_pass() {
    let path = TestDatabasePath::new("all-v1-index-migration");
    let (_, current) = prepare_committed_command_database(&path.0);
    let current_envelope = encode_index_entry_v2(&current).expect("encode current V2 row");
    let legacy = legacy_index_entry(&current);
    let legacy_envelope =
        encode_index_entry_v1_fixture(&legacy).expect("encode legacy V1 fixture row");
    let previous = write_raw_index_entries(
        &path.0,
        &[(current.key().clone(), legacy_envelope.as_bytes().to_vec())],
    );
    assert_eq!(
        previous,
        vec![Some(current_envelope.as_bytes().to_vec())],
        "the test must replace the normal V2 row with exact canonical V1 bytes"
    );

    let controller = RedbTestController::observe_index_migration();
    let (first_session, catalog_outcome, outcome) = complete_startup_pass(
        RedbStore::open_with_test_controller(&path.0, controller.clone())
            .expect("open all-V1 migration fixture"),
    );
    let CatalogHistoryOutcome::MigrationRequired(context) = catalog_outcome else {
        panic!("a complete all-V1 catalog pass must require migration");
    };
    let StructuralOpenOutcome::MigrationRequired(port) = outcome else {
        panic!("a complete all-V1 startup pass must require migration");
    };
    assert_eq!(context.database_id(), database_id());
    assert_eq!(context.open_session_id(), first_session);
    let (store, observation) = drive_index_migration(port, context, &controller);
    assert_eq!(
        observation,
        MigrationObservation {
            v1_rewrites: 1,
            v2_confirms: 0,
            pages: 1,
        }
    );

    let (fresh_session, catalog_outcome, outcome) = complete_startup_pass(store);
    assert!(matches!(catalog_outcome, CatalogHistoryOutcome::Ready(_)));
    assert_ne!(
        fresh_session, first_session,
        "post-migration validation must use a fresh OpenSessionId"
    );
    let StructuralOpenOutcome::Clean(opened) = outcome else {
        panic!("the complete post-migration pass must be V2-only and clean");
    };
    drop(opened);

    let rows = read_raw_index_entries(&path.0);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, current.key().as_bytes());
    assert_eq!(rows[0].1, current_envelope.as_bytes());
    assert_eq!(
        decode_index_entry_v2(&rows[0].1)
            .expect("migrated V2 row decodes")
            .value(),
        &current
    );
    assert!(
        decode_index_entry_v1(&rows[0].1).is_err(),
        "successful migration must leave no V1 row"
    );
}

#[test]
fn mixed_index_rows_rewrite_v1_and_confirm_v2_without_changing_its_bytes() {
    let path = TestDatabasePath::new("mixed-index-migration");
    let (_, first) = prepare_committed_command_database(&path.0);
    let second = migration_index_entry(8, 8);
    assert!(first.key().as_bytes() < second.key().as_bytes());

    let first_v2_envelope = encode_index_entry_v2(&first).expect("encode first V2 row");
    let first_v1_envelope = encode_index_entry_v1_fixture(&legacy_index_entry(&first))
        .expect("encode first legacy row");
    let second_v2_envelope = encode_index_entry_v2(&second).expect("encode second V2 row");
    let previous = write_raw_index_entries(
        &path.0,
        &[
            (first.key().clone(), first_v1_envelope.as_bytes().to_vec()),
            (second.key().clone(), second_v2_envelope.as_bytes().to_vec()),
        ],
    );
    assert_eq!(previous[0], Some(first_v2_envelope.as_bytes().to_vec()));
    assert_eq!(previous[1], None);
    let before = read_raw_index_entries(&path.0);
    assert_eq!(before.len(), 2);
    assert_eq!(before[1].1, second_v2_envelope.as_bytes());

    let controller = RedbTestController::observe_index_migration();
    let (first_session, catalog_outcome, outcome) = complete_startup_pass(
        RedbStore::open_with_test_controller(&path.0, controller.clone())
            .expect("open mixed migration fixture"),
    );
    let CatalogHistoryOutcome::MigrationRequired(context) = catalog_outcome else {
        panic!("a mixed V1/V2 catalog pass must require migration");
    };
    let StructuralOpenOutcome::MigrationRequired(port) = outcome else {
        panic!("a mixed V1/V2 startup pass must require migration");
    };
    let (store, observation) = drive_index_migration(port, context, &controller);
    assert_eq!(
        observation,
        MigrationObservation {
            v1_rewrites: 1,
            v2_confirms: 1,
            pages: 1,
        }
    );

    let (fresh_session, catalog_outcome, outcome) = complete_startup_pass(store);
    assert!(matches!(catalog_outcome, CatalogHistoryOutcome::Ready(_)));
    assert_ne!(fresh_session, first_session);
    let StructuralOpenOutcome::Clean(opened) = outcome else {
        panic!("mixed migration must be followed by one fresh clean pass");
    };
    drop(opened);

    let after = read_raw_index_entries(&path.0);
    assert_eq!(after.len(), 2);
    assert_eq!(after[0].0, first.key().as_bytes());
    assert_eq!(after[0].1, first_v2_envelope.as_bytes());
    assert_eq!(after[1].0, second.key().as_bytes());
    assert_eq!(
        after[1].1, before[1].1,
        "V2Confirm must perform no write and preserve the exact observed envelope"
    );
    assert_eq!(
        decode_index_entry_v2(&after[0].1)
            .expect("rewritten first row")
            .value(),
        &first
    );
    assert_eq!(
        decode_index_entry_v2(&after[1].1)
            .expect("confirmed second row")
            .value(),
        &second
    );
    assert!(
        after
            .iter()
            .all(|(_, envelope)| decode_index_entry_v1(envelope).is_err())
    );
}

#[test]
fn redb_migration_splits_501_rows_at_the_exact_backend_count_boundary() {
    let path = TestDatabasePath::new("migration-count-boundary");
    let (_, current) = prepare_committed_command_database(&path.0);
    let mut rows = Vec::with_capacity(MAX_INDEX_MIGRATION_PAGE_ENTRIES + 1);
    rows.push(current);
    rows.extend((8_u64..508).map(|value| migration_index_entry(value, value)));
    assert_eq!(rows.len(), MAX_INDEX_MIGRATION_PAGE_ENTRIES + 1);
    rows.sort_by(|left, right| left.key().as_bytes().cmp(right.key().as_bytes()));

    let raw_rows = rows
        .iter()
        .map(|row| {
            let envelope = encode_index_entry_v1_fixture(&legacy_index_entry(row))
                .expect("encode count-boundary V1 row");
            (row.key().clone(), envelope.as_bytes().to_vec())
        })
        .collect::<Vec<_>>();
    write_raw_index_entries(&path.0, &raw_rows);
    let controller = RedbTestController::observe_index_migration();
    let (_, catalog_outcome, outcome) = complete_startup_pass(
        RedbStore::open_with_test_controller(&path.0, controller.clone())
            .expect("open count-boundary migration fixture"),
    );
    let CatalogHistoryOutcome::MigrationRequired(context) = catalog_outcome else {
        panic!("501 V1 catalog rows must require migration");
    };
    let StructuralOpenOutcome::MigrationRequired(port) = outcome else {
        panic!("501 V1 rows must require migration");
    };
    let (store, observation) = drive_index_migration(port, context, &controller);
    assert_eq!(observation.pages, 2);
    assert_eq!(
        observation.v1_rewrites,
        MAX_INDEX_MIGRATION_PAGE_ENTRIES + 1
    );
    assert_eq!(observation.v2_confirms, 0);
    let (_, catalog_outcome, outcome) = complete_startup_pass(store);
    assert!(matches!(catalog_outcome, CatalogHistoryOutcome::Ready(_)));
    assert!(matches!(outcome, StructuralOpenOutcome::Clean(_)));
    drop(outcome);
    assert_eq!(read_raw_index_entries(&path.0).len(), rows.len());
}

#[test]
fn redb_migration_stops_on_instruction_bytes_with_evidence_capacity_remaining() {
    const CANONICAL_RECORD_OVERHEAD: usize = 16;
    let path = TestDatabasePath::new("migration-instruction-ledger");
    let (_, current) = prepare_committed_command_database(&path.0);
    let covered_values = CanonicalRecord::new(vec![(
        FieldId::first(),
        CanonicalValue::bytes(vec![
            0xa5;
            MAX_CANONICAL_DOCUMENT_BYTES - CANONICAL_RECORD_OVERHEAD
        ])
        .expect("maximum bytes value"),
    )])
    .expect("maximum covered values");
    let first = StoredIndexEntryV2::new(
        current.key().clone(),
        current.schema_binding().clone(),
        covered_values.clone(),
        current.partition_key().clone(),
    )
    .expect("first maximum migration row");
    let second = migration_index_entry_with_covered_values(8, 8, covered_values);
    let mut rows = [first, second];
    rows.sort_by(|left, right| left.key().as_bytes().cmp(right.key().as_bytes()));
    let raw_rows = rows
        .iter()
        .map(|row| {
            let envelope = encode_index_entry_v1_fixture(&legacy_index_entry(row))
                .expect("encode maximum V1 row");
            (row.key().clone(), envelope.as_bytes().to_vec())
        })
        .collect::<Vec<_>>();
    let evidence = raw_rows
        .iter()
        .map(|(key, envelope)| {
            decode_index_migration_row(key, envelope).expect("decode maximum migration evidence")
        })
        .collect::<Vec<_>>();
    assert!(
        evidence
            .iter()
            .map(|row| row.evidence_page_charge())
            .sum::<usize>()
            <= MAX_INDEX_MIGRATION_PAGE_BYTES
    );
    assert!(
        evidence
            .iter()
            .map(|row| row.instruction_page_charge())
            .sum::<usize>()
            > MAX_INDEX_MIGRATION_PAGE_BYTES
    );
    assert!(
        evidence
            .iter()
            .all(|row| row.instruction_page_charge() <= MAX_INDEX_MIGRATION_PAGE_BYTES),
        "each row must fit independently so migration can make progress"
    );
    write_raw_index_entries(&path.0, &raw_rows);
    let controller = RedbTestController::observe_index_migration();
    let (_, catalog_outcome, outcome) = complete_startup_pass(
        RedbStore::open_with_test_controller(&path.0, controller.clone())
            .expect("open instruction-ledger fixture"),
    );
    let CatalogHistoryOutcome::MigrationRequired(context) = catalog_outcome else {
        panic!("maximum V1 catalog rows must require migration");
    };
    let StructuralOpenOutcome::MigrationRequired(port) = outcome else {
        panic!("maximum V1 rows must require migration");
    };
    let (store, observation) = drive_index_migration(port, context, &controller);
    assert_eq!(observation.pages, 2);
    assert_eq!(observation.v1_rewrites, 2);
    assert_eq!(observation.v2_confirms, 0);
    let (_, catalog_outcome, outcome) = complete_startup_pass(store);
    assert!(matches!(catalog_outcome, CatalogHistoryOutcome::Ready(_)));
    assert!(matches!(outcome, StructuralOpenOutcome::Clean(_)));
}

#[test]
fn redb_driver_rejects_a_port_from_another_migration_session() {
    let first_path = TestDatabasePath::new("resume-first-session");
    let (_, first) = prepare_committed_command_database(&first_path.0);
    let first_legacy =
        encode_index_entry_v1_fixture(&legacy_index_entry(&first)).expect("encode first V1 row");
    write_raw_index_entries(
        &first_path.0,
        &[(first.key().clone(), first_legacy.as_bytes().to_vec())],
    );
    let (_, first_catalog_outcome, first_outcome) = complete_startup_pass(
        RedbStore::open(&first_path.0).expect("open first migration session"),
    );
    let CatalogHistoryOutcome::MigrationRequired(first_context) = first_catalog_outcome else {
        panic!("first catalog session must require migration");
    };
    let StructuralOpenOutcome::MigrationRequired(first_port) = first_outcome else {
        panic!("first session must require migration");
    };

    let second_path = TestDatabasePath::new("resume-second-session");
    let (_, second) = prepare_committed_command_database(&second_path.0);
    let second_legacy =
        encode_index_entry_v1_fixture(&legacy_index_entry(&second)).expect("encode second V1 row");
    write_raw_index_entries(
        &second_path.0,
        &[(second.key().clone(), second_legacy.as_bytes().to_vec())],
    );
    let (_, second_catalog_outcome, second_outcome) = complete_startup_pass(
        RedbStore::open(&second_path.0).expect("open second migration session"),
    );
    let CatalogHistoryOutcome::MigrationRequired(second_context) = second_catalog_outcome else {
        panic!("second catalog session must require migration");
    };
    let StructuralOpenOutcome::MigrationRequired(second_port) = second_outcome else {
        panic!("second session must require migration");
    };

    let error = match CatalogIndexMigrationDriver::new(first_context, second_port) {
        Ok(_) => panic!("catalog context must reject another session's backend port"),
        Err(error) => error,
    };
    assert!(matches!(error, CatalogIndexMigrationDriveError::Catalog(_)));
    drop((first_port, second_context));
    for path in [&first_path.0, &second_path.0] {
        assert_eq!(
            RedbStore::open(path)
                .expect("dropped migration capability permits reopen")
                .probe_database_identity()
                .expect("probe after dropped migration capability"),
            DatabaseIdentityProbe::Existing(database_id())
        );
    }
}

#[test]
fn crash_before_index_migration_batch_restarts_from_v1_without_a_marker() {
    let path = TestDatabasePath::new("before-index-migration-batch");
    let (_, current) = prepare_committed_command_database(&path.0);
    let current_envelope = encode_index_entry_v2(&current).expect("encode current V2 row");
    let legacy_envelope =
        encode_index_entry_v1_fixture(&legacy_index_entry(&current)).expect("encode legacy V1 row");
    assert_eq!(
        write_raw_index_entries(
            &path.0,
            &[(current.key().clone(), legacy_envelope.as_bytes().to_vec(),)],
        ),
        vec![Some(current_envelope.as_bytes().to_vec())]
    );
    let control_state = read_migration_control_state(&path.0);

    run_crashing_child("before-index-migration-batch-commit", &path.0);

    let rows = read_raw_index_entries(&path.0);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, current.key().as_bytes());
    assert_eq!(
        rows[0].1,
        legacy_envelope.as_bytes(),
        "a precommit crash must leave the exact V1 envelope"
    );
    assert_eq!(
        read_migration_control_state(&path.0),
        control_state,
        "a migration crash must not create a table or metadata marker"
    );

    let controller = RedbTestController::observe_index_migration();
    let (migration_session, catalog_outcome, outcome) = complete_startup_pass(
        RedbStore::open_with_test_controller(&path.0, controller.clone())
            .expect("restart migration from structural step one"),
    );
    let CatalogHistoryOutcome::MigrationRequired(context) = catalog_outcome else {
        panic!("the exact V1 catalog row must require migration after restart");
    };
    let StructuralOpenOutcome::MigrationRequired(port) = outcome else {
        panic!("the exact V1 row must require migration after restart");
    };
    let (store, observation) = drive_index_migration(port, context, &controller);
    assert_eq!(observation.v1_rewrites, 1);
    assert_eq!(observation.v2_confirms, 0);
    drop(store);

    let (clean_session, catalog_outcome, outcome) = complete_startup_pass(
        RedbStore::open(&path.0).expect("fresh reopen after completed migration"),
    );
    assert!(matches!(catalog_outcome, CatalogHistoryOutcome::Ready(_)));
    assert_ne!(clean_session, migration_session);
    let StructuralOpenOutcome::Clean(opened) = outcome else {
        panic!("the fresh post-migration session must be V2-only and clean");
    };
    drop(opened);
    assert_eq!(
        read_raw_index_entries(&path.0),
        vec![(
            current.key().as_bytes().to_vec(),
            current_envelope.as_bytes().to_vec(),
        )]
    );
    assert_eq!(read_migration_control_state(&path.0), control_state);

    let (repeat_session, catalog_outcome, outcome) = complete_startup_pass(
        RedbStore::open(&path.0).expect("repeat clean post-migration reopen"),
    );
    assert!(matches!(catalog_outcome, CatalogHistoryOutcome::Ready(_)));
    assert_ne!(repeat_session, clean_session);
    assert!(matches!(outcome, StructuralOpenOutcome::Clean(_)));
}

#[test]
fn crash_after_index_migration_batch_restarts_from_step_one_and_observes_exact_v2() {
    let path = TestDatabasePath::new("after-index-migration-batch");
    let (_, current) = prepare_committed_command_database(&path.0);
    let current_envelope = encode_index_entry_v2(&current).expect("encode current V2 row");
    let legacy_envelope =
        encode_index_entry_v1_fixture(&legacy_index_entry(&current)).expect("encode legacy V1 row");
    assert_eq!(
        write_raw_index_entries(
            &path.0,
            &[(current.key().clone(), legacy_envelope.as_bytes().to_vec(),)],
        ),
        vec![Some(current_envelope.as_bytes().to_vec())]
    );
    let control_state = read_migration_control_state(&path.0);

    run_crashing_child("after-index-migration-batch-commit", &path.0);

    assert_eq!(
        read_raw_index_entries(&path.0),
        vec![(
            current.key().as_bytes().to_vec(),
            current_envelope.as_bytes().to_vec(),
        )],
        "the after-commit crash must expose the complete exact V2 replacement"
    );
    assert_eq!(
        read_migration_control_state(&path.0),
        control_state,
        "the committed batch must not create a table or metadata marker"
    );

    let (first_session, catalog_outcome, outcome) = complete_startup_pass(
        RedbStore::open(&path.0).expect("restart structural validation from step one"),
    );
    assert!(matches!(catalog_outcome, CatalogHistoryOutcome::Ready(_)));
    let StructuralOpenOutcome::Clean(opened) = outcome else {
        panic!("step-one restart must observe the committed V2 batch as clean");
    };
    drop(opened);

    let (fresh_session, catalog_outcome, outcome) = complete_startup_pass(
        RedbStore::open(&path.0).expect("fresh repeat reopen after uncertain migration commit"),
    );
    assert!(matches!(catalog_outcome, CatalogHistoryOutcome::Ready(_)));
    assert_ne!(fresh_session, first_session);
    let StructuralOpenOutcome::Clean(opened) = outcome else {
        panic!("the repeat post-migration session must remain clean");
    };
    drop(opened);
    assert_eq!(read_migration_control_state(&path.0), control_state);
}

#[test]
fn crash_before_initialization_commit_leaves_a_proven_empty_store() {
    let path = TestDatabasePath::new("before-initialization");
    run_crashing_child("before-initialization-commit", &path.0);

    let store = RedbStore::open(&path.0).expect("recover precommit crash");
    assert_eq!(
        store.probe_database_identity().expect("probe recovery"),
        DatabaseIdentityProbe::NeedsInitialization
    );
}

#[test]
fn crash_after_initialization_commit_preserves_the_durable_database_identity() {
    let path = TestDatabasePath::new("after-initialization");
    run_crashing_child("after-initialization-commit", &path.0);

    let store = RedbStore::open(&path.0).expect("recover postcommit crash");
    assert_eq!(
        store.probe_database_identity().expect("probe recovery"),
        DatabaseIdentityProbe::Existing(database_id())
    );
    assert_eq!(complete_structural_open(store).database_id(), database_id());
}

#[test]
fn sealed_command_audit_links_reject_reordered_staging_evidence_atomically() {
    let path = TestDatabasePath::new("reordered-command-audit-links");
    prepare_command_database(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).expect("open command database"));
    let first = command_fixture_at(1);
    let second = command_fixture_at(2);
    let fixtures = [first.clone(), second.clone()];
    let reordered_audits = [second, first];

    let error = try_commit_command_group(&ports, &fixtures, &reordered_audits)
        .expect_err("audit links cannot be reordered relative to staged graphs");
    assert_eq!(
        error.kind(),
        riffdb_storage_api::StorageErrorKind::InvariantViolation
    );
    assert!(command_audit_phases(&ports).is_empty());
    for fixture in &fixtures {
        assert_precommit_command_state(&ports, fixture);
    }
}

#[test]
fn crash_before_fused_command_commit_preserves_complete_absence() {
    for (label, profile) in [
        ("standard", RedbCommitProfile::Standard),
        ("hardened", RedbCommitProfile::Hardened),
    ] {
        let path = TestDatabasePath::new(&format!("before-command-{label}"));
        prepare_command_database(&path.0);
        run_crashing_child_with_profile("before-command-batch-commit", &path.0, profile);

        let ports = open_operational(RedbStore::open(&path.0).expect("recover precommit crash"));
        assert_precommit_command_state(&ports, &command_fixture());
    }
}

#[test]
fn crash_after_command_commit_preserves_the_complete_reciprocal_graph() {
    for (label, profile) in [
        ("standard", RedbCommitProfile::Standard),
        ("hardened", RedbCommitProfile::Hardened),
    ] {
        let path = TestDatabasePath::new(&format!("after-command-{label}"));
        prepare_command_database(&path.0);
        run_crashing_child_with_profile("after-command-batch-commit", &path.0, profile);

        let ports = open_operational(RedbStore::open(&path.0).expect("recover postcommit crash"));
        assert_postcommit_command_state(&ports, &command_fixture());
        drop(ports);

        let ports = open_operational(RedbStore::open(&path.0).expect("repeat postcommit recovery"));
        assert_postcommit_command_state(&ports, &command_fixture());
    }
}

#[test]
fn crash_before_or_after_unpublished_subgroup_recovers_the_predecessor_frontier() {
    for mode in [
        "before-deferred-command-commit",
        "after-deferred-command-commit",
    ] {
        let path = TestDatabasePath::new(mode);
        prepare_command_database(&path.0);
        run_crashing_child(mode, &path.0);

        let ports = open_operational(
            RedbStore::open(&path.0).expect("recover unfenced deferred subgroup crash"),
        );
        assert_precommit_command_state(&ports, &command_fixture());
    }
}

#[test]
fn crash_after_epoch_tail_preserves_the_complete_reciprocal_graph() {
    let path = TestDatabasePath::new("after-command-epoch-tail");
    prepare_command_database(&path.0);
    run_crashing_child("after-command-epoch-tail", &path.0);

    let ports =
        open_operational(RedbStore::open(&path.0).expect("recover known-durable epoch tail crash"));
    assert_postcommit_command_state(&ports, &command_fixture());
}

#[test]
fn serial_group_crash_before_commit_leaves_every_command_absent() {
    let path = TestDatabasePath::new("before-command-group");
    prepare_command_database(&path.0);
    run_crashing_child_with_profile(
        "before-command-group-commit",
        &path.0,
        RedbCommitProfile::Standard,
    );

    let ports = open_operational(RedbStore::open(&path.0).expect("recover group precommit crash"));
    for ordinal in 1..=riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS {
        let fixture = command_fixture_at(u64::try_from(ordinal).expect("bounded fixture ordinal"));
        assert_eq!(
            ports
                .lookup_admission(fixture.candidates.clone())
                .expect("lookup absent grouped identity"),
            AdmissionLookupResultV1::NotFound
        );
        assert_eq!(
            ports.read_entity(&fixture.target).expect("read entity"),
            None
        );
        assert_eq!(
            ports
                .read_commit(fixture.records.commit().commit_sequence())
                .expect("read absent grouped commit"),
            None
        );
    }
    assert!(command_audit_phases(&ports).is_empty());
}

#[test]
fn segmented_group_commits_and_reopens_without_a_failpoint() {
    let path = TestDatabasePath::new("segmented-command-group");
    prepare_command_database(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).expect("open grouped database"));
    let fixtures = (1..=riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS)
        .map(|ordinal| command_fixture_at(u64::try_from(ordinal).expect("bounded ordinal")))
        .collect::<Vec<_>>();
    commit_command_group(&ports, &fixtures);
    drop(ports);
    let ports = open_operational(RedbStore::open(&path.0).expect("reopen grouped database"));
    for fixture in fixtures {
        assert_eq!(
            ports
                .read_commit(fixture.records.commit().commit_sequence())
                .expect("read grouped command"),
            Some(fixture.records.commit().clone())
        );
    }
}

#[test]
fn retention_splits_a_segment_and_rechains_the_retained_suffix() {
    let path = TestDatabasePath::new("retention-split-command-segment");
    prepare_command_database(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).expect("open grouped database"));
    let fixtures = (1..=riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS)
        .map(|ordinal| command_fixture_at(u64::try_from(ordinal).expect("bounded ordinal")))
        .collect::<Vec<_>>();
    commit_command_group(&ports, &fixtures);
    drop(ports);
    for ordinal in 1..=riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS {
        retention_deliver_outbox_status_raw(
            &path.0,
            u64::try_from(ordinal).expect("bounded ordinal"),
        );
    }
    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance
        .add_hold("split-cap", 64, "exercise partial segment retention")
        .expect("install split hold");
    maintenance
        .prune_to(32)
        .expect("split segment at watermark");

    let findings = collect_structural_findings(RedbStore::open(&path.0).expect("reopen split"));
    assert!(
        findings.is_empty(),
        "split segment must reopen clean: {findings:?}"
    );
    let ports = open_operational(RedbStore::open(&path.0).expect("open split database"));
    assert_eq!(
        ports
            .read_commit(CommitSequence::new(33).expect("retained sequence"))
            .expect("read retained command"),
        Some(fixtures[32].records.commit().clone())
    );
    assert_eq!(
        ports
            .read_commit(CommitSequence::new(32).expect("pruned sequence"))
            .expect_err("pruned command is typed")
            .kind(),
        riffdb_storage_api::StorageErrorKind::HistoryPruned
    );
}

#[test]
fn serial_group_crash_after_commit_preserves_every_complete_command() {
    let path = TestDatabasePath::new("after-command-group");
    prepare_command_database(&path.0);
    run_crashing_child_with_profile(
        "after-command-group-commit",
        &path.0,
        RedbCommitProfile::Standard,
    );

    let ports = open_operational(RedbStore::open(&path.0).expect("recover group postcommit crash"));
    for ordinal in 1..=riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS {
        let fixture = command_fixture_at(u64::try_from(ordinal).expect("bounded fixture ordinal"));
        let AdmissionLookupResultV1::Found(admission) = ports
            .lookup_admission(fixture.candidates.clone())
            .expect("lookup grouped identity")
        else {
            panic!("each grouped identity is terminal");
        };
        assert_eq!(
            *admission,
            riffdb_storage_api::StoredAdmissionStateV1::StoredOutcome(
                fixture.records.stored_outcome().clone()
            )
        );
        assert_eq!(
            ports
                .read_entity(&fixture.target)
                .expect("read grouped entity"),
            Some(fixture.records.entities()[0].post_image().clone())
        );
        assert_eq!(
            ports
                .read_commit(fixture.records.commit().commit_sequence())
                .expect("read grouped commit"),
            Some(fixture.records.commit().clone())
        );
    }
}

#[test]
fn segment_owned_event_route_requires_no_standalone_row() {
    let path = TestDatabasePath::new("segment-owned-event-route");
    let (fixture, _) = prepare_committed_command_database(&path.0);
    let event_id = fixture.records.events()[0].event_id();
    let partition_hash = hash_partition_key(fixture.pending.partition_key().as_bytes());
    let mut route_key = [0_u8; 44];
    route_key[..32].copy_from_slice(partition_hash.as_bytes());
    route_key[32..].copy_from_slice(&event_id.to_be_bytes());

    assert!(
        !retention_raw_row_present(&path.0, "event_routes", route_key.as_slice()),
        "the command segment, not a duplicate row, owns its event route"
    );
    let findings = collect_structural_findings(RedbStore::open(&path.0).expect("reopen database"));
    assert!(
        findings.is_empty(),
        "the segment manifest reconstructs the exact route: {findings:?}"
    );
}

fn complete_startup_observing_checkpoint(
    store: RedbStore,
) -> (
    OpenSessionId,
    CatalogHistoryOutcome,
    StructuralOpenOutcome<RedbDormantPorts, RedbStartupIndexMigrationPort>,
    bool,
    u64,
) {
    let digest_key = DigestKeyId::new(1).expect("digest key ID");
    let inputs = StartupValidationInputs::new(
        Timestamp::new(1_700_000_000, 0).expect("startup timestamp"),
        ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("capability digest inventory"),
        ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("idempotency digest inventory"),
    );
    let mut session = store
        .begin_structural_evidence(inputs)
        .expect("begin exclusive structural evidence");
    let checkpoint_verified = session.checkpoint_verified();
    let sampled = session.sampled_window_rows_inspected();
    let database_id = session.database_id();
    let open_session_id = session.open_session_id();
    let limit = EvidencePageLimit::new(64).expect("page limit");

    let mut structural_cursor = StructuralEvidenceCursor::start(database_id, open_session_id);
    let structural_end = loop {
        match session
            .read_structural_evidence(structural_cursor, limit)
            .expect("read structural evidence")
        {
            StructuralEvidencePage::Page { findings, next, .. } => {
                assert!(
                    findings.is_empty(),
                    "a valid recovery fixture has no findings"
                );
                structural_cursor = next;
            }
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };

    let (catalog_outcome, historical_end) = validate_catalog_history(&mut session)
        .expect("catalog validates the complete historical stream")
        .into_parts();
    let outcome = session
        .finish(structural_end, historical_end)
        .expect("finish structural evidence");
    (
        open_session_id,
        catalog_outcome,
        outcome,
        checkpoint_verified,
        sampled,
    )
}

#[test]
fn validated_prefix_checkpoint_roundtrip_second_open_uses_fast_path() {
    let path = TestDatabasePath::new("validated-prefix-roundtrip");
    let _ = prepare_committed_command_database(&path.0);
    // Write an S=head checkpoint after the committed command (prepare's open was pre-command).
    let _ = complete_startup_pass(RedbStore::open(&path.0).expect("seed S=head checkpoint"));

    // A subsequent open must verify the checkpoint and take the fast path.
    let (_, _, outcome, verified, sampled) =
        complete_startup_observing_checkpoint(RedbStore::open(&path.0).expect("checkpointed open"));
    assert!(
        matches!(outcome, StructuralOpenOutcome::Clean(_)),
        "checkpointed open must finish clean"
    );
    assert!(
        verified,
        "open after a clean finish must verify the written checkpoint"
    );
    assert!(
        sampled > 0,
        "verified checkpoint with S>0 must execute sampled windows (observed {sampled})"
    );
    drop(outcome);

    // Forced full path: remove checkpoint meta, open full, rewrite, reopen.
    {
        let database = Database::create(&path.0).expect("open");
        let txn = database.begin_write().expect("write");
        {
            let mut meta = txn.open_table(META).expect("meta");
            let _ = meta.remove("validated_prefix_checkpoint/v1");
        }
        txn.commit().expect("commit");
        drop(database);
    }
    let store = RedbStore::open(&path.0).expect("full open");
    let (_, _, outcome, verified_full, _) = complete_startup_observing_checkpoint(store);
    assert!(
        matches!(outcome, StructuralOpenOutcome::Clean(_)),
        "full validation open must finish clean"
    );
    assert!(
        !verified_full,
        "absent checkpoint must force full validation"
    );
    drop(outcome);
    let store = RedbStore::open(&path.0).expect("rewrite open");
    let (_, _, outcome, verified_again, _) = complete_startup_observing_checkpoint(store);
    assert!(matches!(outcome, StructuralOpenOutcome::Clean(_)));
    assert!(verified_again, "rewritten checkpoint must verify");
    drop(outcome);
}

#[test]
fn validated_prefix_checkpoint_binding_mismatch_falls_back_to_full_validation() {
    let path = TestDatabasePath::new("validated-prefix-binding-mismatch");
    let _ = prepare_committed_command_database(&path.0);
    let (_, _, seed_outcome) =
        complete_startup_pass(RedbStore::open(&path.0).expect("seed checkpoint"));
    drop(seed_outcome);

    // Corrupt the checkpoint self-hash by flipping a byte in the meta row.
    {
        let database = Database::create(&path.0).expect("open for doctor");
        let txn = database.begin_write().expect("write");
        {
            let mut meta = txn.open_table(META).expect("meta");
            let key = "validated_prefix_checkpoint/v1";
            let existing = meta
                .get(key)
                .expect("get")
                .expect("checkpoint present")
                .value()
                .to_vec();
            let mut doctored = existing;
            if let Some(last) = doctored.last_mut() {
                *last ^= 0xff;
            }
            meta.insert(key, doctored.as_slice()).expect("insert");
        }
        txn.commit().expect("commit doctor");
        drop(database);
    }

    let (_, _, outcome, verified, _) =
        complete_startup_observing_checkpoint(RedbStore::open(&path.0).expect("reopen"));
    assert!(
        !verified,
        "corrupted self-hash must ignore checkpoint (full validation)"
    );
    assert!(
        matches!(outcome, StructuralOpenOutcome::Clean(_)),
        "full validation must still open cleanly on otherwise-valid data"
    );
    drop(outcome);
}

#[test]
fn validated_prefix_checkpoint_prefix_count_mismatch_fails_closed() {
    use riffdb_storage_api::{
        StoredValidatedPrefixCheckpointV1,
        proto_codec::{
            decode_validated_prefix_checkpoint_v1, encode_validated_prefix_checkpoint_v1,
        },
    };

    let path = TestDatabasePath::new("validated-prefix-count-mismatch");
    let _ = prepare_committed_command_database(&path.0);
    let _ = complete_startup_pass(RedbStore::open(&path.0).expect("write checkpoint"));

    // Under-report commits_count while leaving rows in place. Load-time
    // CountImpossible does not fire (recorded ≤ full), but at ExactEnd
    // table_len − walked_suffix ≠ recorded_prefix → fail closed.
    {
        let database = Database::create(&path.0).expect("open");
        let txn = database.begin_write().expect("write");
        {
            let mut meta = txn.open_table(META).expect("meta");
            let key = "validated_prefix_checkpoint/v1";
            let existing = meta
                .get(key)
                .expect("get")
                .expect("checkpoint present")
                .value()
                .to_vec();
            let original = decode_validated_prefix_checkpoint_v1(&existing)
                .expect("decode")
                .into_parts()
                .0;
            assert!(
                original.counts().commits_count >= 1,
                "fixture must record a non-empty commits prefix"
            );
            let mut counts = original.counts();
            counts.commits_count = 0;
            let doctored = StoredValidatedPrefixCheckpointV1::new(
                original.database_id(),
                original.history_incarnation(),
                original.registry_digest(),
                original.checkpoint_commit_sequence(),
                original.audit_sequence_bound(),
                counts,
                original.entity_chain_fingerprint(),
                original.retained(),
                original.previous_checkpoint_hash(),
                0,
            )
            .expect("rehash under-reported counts");
            let encoded = encode_validated_prefix_checkpoint_v1(&doctored).expect("encode");
            meta.insert(key, encoded.as_bytes()).expect("insert");
        }
        txn.commit().expect("commit doctor");
    }

    let store = RedbStore::open(&path.0).expect("open doctored DB");
    let digest_key = DigestKeyId::new(1).expect("digest key ID");
    let inputs = StartupValidationInputs::new(
        Timestamp::new(1_700_000_000, 0).expect("startup timestamp"),
        ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("capability digest inventory"),
        ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("idempotency digest inventory"),
    );
    let mut session = store
        .begin_structural_evidence(inputs)
        .expect("begin accepts bindable checkpoint");
    assert!(
        session.checkpoint_verified(),
        "under-reported counts still bind at load; mismatch is walk-end verified"
    );
    let mut cursor =
        StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
    let limit = EvidencePageLimit::new(64).expect("page limit");
    let mut failed_closed = false;
    loop {
        match session.read_structural_evidence(cursor, limit) {
            Ok(StructuralEvidencePage::Page { findings, next, .. }) => {
                if !findings.is_empty() {
                    failed_closed = true;
                    break;
                }
                cursor = next;
            }
            Ok(StructuralEvidencePage::ExactEnd(_)) => break,
            Err(_) => {
                failed_closed = true;
                break;
            }
        }
    }
    assert!(
        failed_closed,
        "under-reported below-S commits_count must fail closed at walk end"
    );
}

#[test]
fn crash_before_validated_prefix_checkpoint_commit_leaves_no_meta() {
    let path = TestDatabasePath::new("before-validated-prefix-checkpoint");
    let _ = prepare_committed_command_database(&path.0);
    // Strip any checkpoint from prior opens so the crash targets a first write.
    {
        let database = Database::create(&path.0).expect("open");
        let txn = database.begin_write().expect("write");
        {
            let mut meta = txn.open_table(META).expect("meta");
            let _ = meta.remove("validated_prefix_checkpoint/v1");
        }
        txn.commit().expect("commit");
    }
    run_crashing_child("before-validated-prefix-checkpoint-commit", &path.0);

    // After abort-before-commit, meta must still be absent.
    {
        let database = Database::create(&path.0).expect("open");
        let txn = database.begin_read().expect("read");
        let meta = txn.open_table(META).expect("meta");
        assert!(
            meta.get("validated_prefix_checkpoint/v1")
                .expect("get")
                .is_none(),
            "abort before checkpoint commit must leave no durable checkpoint"
        );
    }

    // Reopen: full validation path (no verified checkpoint at begin).
    let store = RedbStore::open(&path.0).expect("reopen after before-checkpoint crash");
    let (_, _, outcome, verified, _) = complete_startup_observing_checkpoint(store);
    assert!(
        matches!(outcome, StructuralOpenOutcome::Clean(_)),
        "recovery after pre-checkpoint crash must open clean"
    );
    assert!(
        !verified,
        "first reopen after pre-checkpoint crash must not verify a checkpoint"
    );
}

#[test]
fn crash_after_validated_prefix_checkpoint_commit_reopens_fast_path() {
    let path = TestDatabasePath::new("after-validated-prefix-checkpoint");
    let _ = prepare_committed_command_database(&path.0);
    run_crashing_child("after-validated-prefix-checkpoint-commit", &path.0);

    let store = RedbStore::open(&path.0).expect("reopen after after-checkpoint crash");
    let (_, _, outcome, verified, _) = complete_startup_observing_checkpoint(store);
    assert!(
        matches!(outcome, StructuralOpenOutcome::Clean(_)),
        "recovery after post-checkpoint crash must open clean"
    );
    assert!(
        verified,
        "checkpoint committed before crash must be verified on reopen"
    );
}

#[test]
fn crash_before_shutdown_validated_prefix_checkpoint_leaves_no_fresh_checkpoint() {
    // ADR-0019 A1 write point (2): abort before the public shutdown-path write.
    // Child rejects the startup finish write non-fatally, then aborts before the
    // public write_validated_prefix_checkpoint commit — no durable checkpoint.
    let path = TestDatabasePath::new("before-shutdown-validated-prefix-checkpoint");
    let _ = prepare_committed_command_database(&path.0);
    strip_checkpoint_meta(&path.0);
    run_crashing_child(
        "before-shutdown-validated-prefix-checkpoint-commit",
        &path.0,
    );

    assert!(
        !checkpoint_meta_present(&path.0),
        "abort before the public shutdown checkpoint write must leave no durable checkpoint"
    );
    let store = RedbStore::open(&path.0).expect("reopen after before-shutdown-checkpoint crash");
    let (_, _, outcome, verified, _) = complete_startup_observing_checkpoint(store);
    assert!(
        matches!(outcome, StructuralOpenOutcome::Clean(_)),
        "recovery after pre-shutdown-checkpoint crash must open clean"
    );
    assert!(
        !verified,
        "first reopen after pre-shutdown-checkpoint crash must full-validate (no checkpoint)"
    );
}

#[test]
fn crash_after_shutdown_validated_prefix_checkpoint_reopens_fast_path() {
    // ADR-0019 A1 write point (2): public write commits, then process aborts.
    // Falsifiability (b): skipping the public write leaves no durable checkpoint
    // and this assertion fails.
    let path = TestDatabasePath::new("after-shutdown-validated-prefix-checkpoint");
    let _ = prepare_committed_command_database(&path.0);
    strip_checkpoint_meta(&path.0);
    run_crashing_child("after-shutdown-validated-prefix-checkpoint-commit", &path.0);

    assert!(
        checkpoint_meta_present(&path.0),
        "public shutdown checkpoint must be durable after the post-commit abort"
    );
    let store = RedbStore::open(&path.0).expect("reopen after after-shutdown-checkpoint crash");
    let (_, _, outcome, verified, _) = complete_startup_observing_checkpoint(store);
    assert!(
        matches!(outcome, StructuralOpenOutcome::Clean(_)),
        "recovery after post-shutdown-checkpoint crash must open clean"
    );
    assert!(
        verified,
        "checkpoint committed by the public entry before crash must verify on reopen"
    );
}

#[test]
fn perf_014_unclean_recovery_vs_clean_startup() {
    // Equivalent seeding: BOTH databases carry the identical committed state
    // (initialization + catalog activation, checkpoint from the preparation
    // pass). The unclean copy additionally suffers a real kill mid-CommandBatch
    // (child aborts before the engine commit), which is invisible to committed
    // state by engine atomicity — the two measured drains validate the same
    // rows, so the ratio isolates redb repair + reopen overhead.
    let unclean_path = TestDatabasePath::new("perf-014-unclean");
    prepare_command_database(&unclean_path.0);
    let clean_path = TestDatabasePath::new("perf-014-clean");
    prepare_command_database(&clean_path.0);
    run_crashing_child("before-command-batch-commit", &unclean_path.0);

    // Sentinel-reset the process-global repair observation so "changed" proves
    // the callback fired for THIS open (0% progress is distinguishable from
    // never-fired). Parallel tests in this process could also move it; the
    // sentinel eliminates staleness from earlier repairs, not concurrency.
    riffdb_storage_redb::reset_last_repair_progress_for_tests();
    let unclean_start = std::time::Instant::now();
    let unclean_store = RedbStore::open(&unclean_path.0).expect("open unclean");
    let repair_bps = riffdb_storage_redb::last_repair_progress_basis_points();
    assert_ne!(
        repair_bps,
        riffdb_storage_redb::REPAIR_PROGRESS_SENTINEL,
        "reopen after an aborted mid-batch write must invoke the redb repair callback"
    );
    let _ = complete_startup_pass(unclean_store);
    let unclean_ns = unclean_start.elapsed().as_nanos();

    let clean_start = std::time::Instant::now();
    let _ = complete_startup_pass(RedbStore::open(&clean_path.0).expect("clean reopen"));
    let clean_ns = clean_start.elapsed().as_nanos();

    let ratio = if clean_ns == 0 {
        0.0
    } else {
        unclean_ns as f64 / clean_ns as f64
    };
    println!(
        "{{\"schema\":\"riffdb.perf-014/v1\",\"repair_progress_bps\":{repair_bps},\"unclean_ns\":{unclean_ns},\"clean_ns\":{clean_ns},\"ratio\":{ratio}}}"
    );
    // Honest report only — do not reintroduce PERF-014 gate unless ratio ≤3× reliably.
    let _ = ratio;
}

#[test]
fn validated_prefix_checkpoint_entity_fingerprint_mismatch_falls_back() {
    use riffdb_storage_api::{
        EntityChainFingerprint, StoredValidatedPrefixCheckpointV1,
        proto_codec::{
            decode_validated_prefix_checkpoint_v1, encode_validated_prefix_checkpoint_v1,
        },
    };

    let path = TestDatabasePath::new("validated-prefix-entity-fp");
    let _ = prepare_committed_command_database(&path.0);
    let _ = complete_startup_pass(RedbStore::open(&path.0).expect("write checkpoint"));

    // Rewrite checkpoint with a wrong entity-chain fingerprint (self-hash recomputed so
    // the envelope is valid). Reconstruct-at-S must disagree → ignore → full validation.
    {
        let database = Database::create(&path.0).expect("open");
        let txn = database.begin_write().expect("write");
        {
            let mut meta = txn.open_table(META).expect("meta");
            let key = "validated_prefix_checkpoint/v1";
            let existing = meta
                .get(key)
                .expect("get")
                .expect("checkpoint present")
                .value()
                .to_vec();
            let original = decode_validated_prefix_checkpoint_v1(&existing)
                .expect("decode checkpoint")
                .into_parts()
                .0;
            let wrong_fp = EntityChainFingerprint::from_bytes([0xab; 32]);
            let doctored = StoredValidatedPrefixCheckpointV1::new(
                original.database_id(),
                original.history_incarnation(),
                original.registry_digest(),
                original.checkpoint_commit_sequence(),
                original.audit_sequence_bound(),
                original.counts(),
                wrong_fp,
                original.retained(),
                original.previous_checkpoint_hash(),
                0,
            )
            .expect("rehash doctored checkpoint");
            assert_ne!(
                doctored.entity_chain_fingerprint(),
                original.entity_chain_fingerprint()
            );
            let encoded =
                encode_validated_prefix_checkpoint_v1(&doctored).expect("encode doctored");
            meta.insert(key, encoded.as_bytes()).expect("insert");
        }
        txn.commit().expect("commit doctor");
    }

    let store = RedbStore::open(&path.0).expect("open");
    let (_, _, outcome, verified, _) = complete_startup_observing_checkpoint(store);
    assert!(
        !verified,
        "mismatched entity-chain fingerprint must ignore checkpoint (full validation)"
    );
    assert!(
        matches!(outcome, StructuralOpenOutcome::Clean(_)),
        "full validation of otherwise-valid data must open clean"
    );
}

#[test]
fn validated_prefix_checkpoint_incarnation_mismatch_falls_back() {
    let path = TestDatabasePath::new("validated-prefix-incarnation");
    let _ = prepare_committed_command_database(&path.0);
    let _ = complete_startup_pass(RedbStore::open(&path.0).expect("write checkpoint"));

    // Rewrite history_incarnation without rewriting the checkpoint binding.
    {
        let database = Database::create(&path.0).expect("open");
        let txn = database.begin_write().expect("write");
        {
            let mut meta = txn.open_table(META).expect("meta");
            let encoded = riffdb_storage_api::proto_codec::encode_history_incarnation_v1(2)
                .expect("encode incarnation 2");
            meta.insert("history_incarnation/v1", encoded.as_bytes())
                .expect("stamp incarnation");
        }
        txn.commit().expect("commit");
    }

    let store = RedbStore::open(&path.0).expect("open");
    let (_, _, outcome, verified, _) = complete_startup_observing_checkpoint(store);
    assert!(
        !verified,
        "incarnation mismatch must ignore checkpoint and full-validate"
    );
    assert!(
        matches!(outcome, StructuralOpenOutcome::Clean(_)),
        "full validation on incarnation-stamped-but-otherwise-valid DB must open clean"
    );
}

#[test]
fn pre_validated_prefix_registry_digest_migrates_on_open() {
    let path = TestDatabasePath::new("pre-validated-prefix-migrate");
    let mut store = RedbStore::open(&path.0).expect("open");
    store
        .initialize_database(database_id())
        .expect("initialize");
    drop(store);

    // Downgrade registry digest to PRE_VALIDATED (44-schema frozen digest).
    const PRE: [u8; 32] = [
        0xe1, 0x59, 0x2f, 0xba, 0x8c, 0x33, 0x8a, 0xee, 0x4e, 0xd7, 0x17, 0x8b, 0x6a, 0x09, 0xbb,
        0xcf, 0x88, 0x26, 0x7e, 0x5c, 0xd4, 0x33, 0xfa, 0x23, 0x31, 0x19, 0x9c, 0xaa, 0x3c, 0xd2,
        0xe8, 0xcd,
    ];
    {
        let database = Database::create(&path.0).expect("open");
        let txn = database.begin_write().expect("write");
        {
            let mut meta = txn.open_table(META).expect("meta");
            let encoded = encode_record_registry_v2(riffdb_types::SchemaHash::from_bytes(PRE))
                .expect("encode pre digest");
            meta.insert("record_registry/v2", encoded.as_bytes())
                .expect("insert");
        }
        txn.commit().expect("commit");
    }

    // Open must migrate PRE_VALIDATED → current without error.
    let store = RedbStore::open(&path.0).expect("migrate from PRE_VALIDATED");
    let _ = complete_startup_pass(store);
}

// ===== RT-A fix round: count-guard, write-gate, binding, and seeded-chain coverage =====

const ENTITIES_RAW: TableDefinition<&[u8], &[u8]> = TableDefinition::new("entities");
const OUTBOX_STATUS_RAW: TableDefinition<&[u8], &[u8]> = TableDefinition::new("outbox_status");

const CHECKPOINT_META_KEY: &str = "validated_prefix_checkpoint/v1";

/// Removes any stored validated-prefix checkpoint via a raw engine transaction.
fn strip_checkpoint_meta(path: &Path) {
    let database = Database::create(path).expect("open for checkpoint strip");
    let txn = database.begin_write().expect("begin checkpoint strip");
    {
        let mut meta = txn.open_table(META).expect("open meta");
        let _ = meta.remove(CHECKPOINT_META_KEY).expect("remove checkpoint");
    }
    txn.commit().expect("commit checkpoint strip");
}

/// Decodes the stored checkpoint, applies `mutate`, re-hashes, and stores it back.
fn rewrite_checkpoint(
    path: &Path,
    mutate: impl FnOnce(
        &riffdb_storage_api::StoredValidatedPrefixCheckpointV1,
    ) -> riffdb_storage_api::StoredValidatedPrefixCheckpointV1,
) {
    use riffdb_storage_api::proto_codec::{
        decode_validated_prefix_checkpoint_v1, encode_validated_prefix_checkpoint_v1,
    };
    let database = Database::create(path).expect("open for checkpoint rewrite");
    let txn = database.begin_write().expect("begin checkpoint rewrite");
    {
        let mut meta = txn.open_table(META).expect("open meta");
        let existing = meta
            .get(CHECKPOINT_META_KEY)
            .expect("get checkpoint")
            .expect("checkpoint present")
            .value()
            .to_vec();
        let original = decode_validated_prefix_checkpoint_v1(&existing)
            .expect("decode checkpoint")
            .into_parts()
            .0;
        let doctored = mutate(&original);
        let encoded = encode_validated_prefix_checkpoint_v1(&doctored).expect("encode doctored");
        meta.insert(CHECKPOINT_META_KEY, encoded.as_bytes())
            .expect("insert doctored");
    }
    txn.commit().expect("commit checkpoint rewrite");
}

fn checkpoint_meta_present(path: &Path) -> bool {
    let database = Database::create(path).expect("open for checkpoint probe");
    let txn = database.begin_read().expect("begin checkpoint probe");
    let meta = txn.open_table(META).expect("open meta");
    meta.get(CHECKPOINT_META_KEY).expect("get").is_some()
}

/// Deletes the first row of one raw byte-keyed table.
fn delete_first_raw_row(path: &Path, table: TableDefinition<&[u8], &[u8]>) {
    let database = Database::create(path).expect("open for row deletion");
    let txn = database.begin_write().expect("begin row deletion");
    {
        let mut open = txn.open_table(table).expect("open table");
        let victim = {
            let mut iter = open.iter().expect("iterate table");
            iter.next()
                .expect("table must have a row")
                .expect("read row")
                .0
                .value()
                .to_vec()
        };
        assert!(
            open.remove(victim.as_slice())
                .expect("remove row")
                .is_some(),
            "victim row must exist"
        );
    }
    txn.commit().expect("commit row deletion");
}

/// Full structural + historical drain that tolerates findings and errors.
struct TolerantDrain {
    verified: bool,
    ignored_reason: Option<&'static str>,
    findings: Vec<StructuralFinding>,
    structural_error: bool,
    finish_error: bool,
    outcome: Option<StructuralOpenOutcome<RedbDormantPorts, RedbStartupIndexMigrationPort>>,
}

impl TolerantDrain {
    fn refused(&self) -> bool {
        self.structural_error || self.finish_error
    }

    fn authoritative_finding(&self) -> bool {
        self.findings
            .iter()
            .any(|finding| finding.scope() == StructuralFindingScope::Authoritative)
    }
}

fn drain_tolerating_findings(store: RedbStore) -> TolerantDrain {
    let digest_key = DigestKeyId::new(1).expect("digest key ID");
    let inputs = StartupValidationInputs::new(
        Timestamp::new(1_700_000_000, 0).expect("startup timestamp"),
        ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("capability digest inventory"),
        ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("idempotency digest inventory"),
    );
    let mut session = store
        .begin_structural_evidence(inputs)
        .expect("begin structural evidence");
    let verified = session.checkpoint_verified();
    let ignored_reason = session.checkpoint_ignored_reason();
    let mut report = TolerantDrain {
        verified,
        ignored_reason,
        findings: Vec::new(),
        structural_error: false,
        finish_error: false,
        outcome: None,
    };
    let limit = EvidencePageLimit::new(64).expect("page limit");
    let mut cursor =
        StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
    let structural_end = loop {
        match session.read_structural_evidence(cursor, limit) {
            Ok(StructuralEvidencePage::Page { findings, next, .. }) => {
                report.findings.extend(findings);
                cursor = next;
            }
            Ok(StructuralEvidencePage::ExactEnd(end)) => break end,
            Err(_) => {
                report.structural_error = true;
                return report;
            }
        }
    };
    let Ok(historical) = validate_catalog_history(&mut session) else {
        report.finish_error = true;
        return report;
    };
    let (_, historical_end) = historical.into_parts();
    match session.finish(structural_end, historical_end) {
        Ok(outcome) => report.outcome = Some(outcome),
        Err(_) => report.finish_error = true,
    }
    report
}

/// Seeds one committed-command database with an S = head checkpoint (the
/// preparation passes leave a stale S = 0 checkpoint behind).
fn prepare_checkpointed_command_database(path: &Path) {
    let _ = prepare_committed_command_database(path);
    let _ = complete_startup_pass(RedbStore::open(path).expect("seed S=head checkpoint"));
}

#[test]
fn command_derived_checkpoint_tables_are_physically_empty() {
    let path = TestDatabasePath::new("derived-checkpoint-tables-empty");
    prepare_checkpointed_command_database(&path.0);
    assert!(!retention_raw_table_has_rows(&path.0, "audit_by_request"));
    assert!(!retention_raw_table_has_rows(&path.0, "event_routes"));
    assert!(!retention_raw_table_has_rows(&path.0, "idempotency"));
    let findings = collect_structural_findings(RedbStore::open(&path.0).expect("reopen database"));
    assert!(
        findings.is_empty(),
        "derived indexes rebuild from the segment: {findings:?}"
    );
}

#[test]
fn finding_vetoes_checkpoint_write_and_is_never_suppressed() {
    // Reviewer probe D: a garbage OUTBOX_STATUS row below S produces a
    // non-authoritative OutboxDelivery-scope finding on every full validation.
    // The finding must veto the checkpoint write so the next open re-validates
    // and reports it again — the fast path can never silence it.
    let path = TestDatabasePath::new("finding-vetoes-checkpoint");
    let _ = prepare_committed_command_database(&path.0);
    {
        let database = Database::create(&path.0).expect("open for garbage row");
        let txn = database.begin_write().expect("begin garbage row");
        {
            let key = retention_event_key(1);
            let mut statuses = txn.open_table(OUTBOX_STATUS_RAW).expect("open statuses");
            statuses
                .insert(key.as_slice(), &b"garbage-not-a-status"[..])
                .expect("insert garbage status");
        }
        txn.commit().expect("commit garbage row");
    }
    strip_checkpoint_meta(&path.0);

    // Open 1: full validation completes (non-authoritative finding does not
    // refuse the open) but the finding vetoes the checkpoint write.
    let first = drain_tolerating_findings(RedbStore::open(&path.0).expect("first open"));
    assert!(!first.verified, "no checkpoint may be verified after strip");
    assert!(
        !first.findings.is_empty(),
        "the garbage below-S row must produce a finding under full validation"
    );
    assert!(
        !first.authoritative_finding() && !first.refused(),
        "the outbox-scope finding must not refuse the open"
    );
    let outcome = first.outcome.expect("first open completes");
    drop(outcome);
    assert!(
        !checkpoint_meta_present(&path.0),
        "a finding of any scope must veto the checkpoint write"
    );

    // Open 2: no checkpoint exists, so full validation reports the finding
    // again — never suppressed by a range skip.
    let second = drain_tolerating_findings(RedbStore::open(&path.0).expect("second open"));
    assert!(!second.verified, "no checkpoint may have been written");
    assert!(
        !second.findings.is_empty(),
        "the finding must be reported again on the next open"
    );
}

#[test]
fn public_checkpoint_write_is_gated_on_clean_validation() {
    // Veto case: a validation session with findings (garbage OUTBOX_STATUS row)
    // must gate the public shutdown-side write exactly like the startup write.
    let path = TestDatabasePath::new("public-write-gate-veto");
    let _ = prepare_committed_command_database(&path.0);
    {
        let database = Database::create(&path.0).expect("open for garbage row");
        let txn = database.begin_write().expect("begin garbage row");
        {
            let key = retention_event_key(1);
            let mut statuses = txn.open_table(OUTBOX_STATUS_RAW).expect("open statuses");
            statuses
                .insert(key.as_slice(), &b"garbage-not-a-status"[..])
                .expect("insert garbage status");
        }
        txn.commit().expect("commit garbage row");
    }
    strip_checkpoint_meta(&path.0);

    let report = drain_tolerating_findings(RedbStore::open(&path.0).expect("open with finding"));
    assert!(
        !report.findings.is_empty(),
        "fixture must produce a finding"
    );
    let outcome = report.outcome.expect("outbox finding does not refuse open");
    let StructuralOpenOutcome::Clean(opened) = outcome else {
        panic!("fixture must not require migration");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let ports = dormant
        .into_operational_after_catalog_validation()
        .expect("activate operational ports");
    assert!(
        !ports
            .write_validated_prefix_checkpoint()
            .expect("gated write must not error"),
        "a session with findings must veto the public checkpoint write"
    );
    drop(ports);
    assert!(
        !checkpoint_meta_present(&path.0),
        "the vetoed public write must leave no checkpoint"
    );

    // Allowed case: a clean validation session permits the public write.
    let clean_path = TestDatabasePath::new("public-write-gate-clean");
    let _ = prepare_committed_command_database(&clean_path.0);
    let report = drain_tolerating_findings(RedbStore::open(&clean_path.0).expect("clean open"));
    assert!(report.findings.is_empty(), "clean fixture has no findings");
    let StructuralOpenOutcome::Clean(opened) = report.outcome.expect("clean open completes") else {
        panic!("clean fixture must not require migration");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let ports = dormant
        .into_operational_after_catalog_validation()
        .expect("activate operational ports");
    assert!(
        ports
            .write_validated_prefix_checkpoint()
            .expect("public write after clean validation"),
        "a clean validation session must permit the public checkpoint write"
    );
    drop(ports);
    assert!(checkpoint_meta_present(&clean_path.0));
}

#[test]
fn checkpoint_write_failure_after_clean_validation_is_nonfatal() {
    // ADR-0019 A1: a FAILED checkpoint write after clean validation is
    // non-fatal — the open completes and only the next fast path is lost.
    let path = TestDatabasePath::new("checkpoint-write-failure-nonfatal");
    let _ = prepare_committed_command_database(&path.0);
    strip_checkpoint_meta(&path.0);

    let controller =
        RedbTestController::return_before_commit(RedbTestOperation::ValidatedPrefixCheckpoint);
    let store = RedbStore::open_with_test_controller(&path.0, controller)
        .expect("open with armed checkpoint failpoint");
    let (_, catalog_outcome, outcome) = complete_startup_pass(store);
    assert!(matches!(catalog_outcome, CatalogHistoryOutcome::Ready(_)));
    let StructuralOpenOutcome::Clean(opened) = outcome else {
        panic!("clean validation with a failed checkpoint write must still open");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let ports = dormant
        .into_operational_after_catalog_validation()
        .expect("activate operational ports after failed checkpoint write");
    assert_eq!(
        ports.checkpoint_write_failures(),
        1,
        "the failed write must be counted for operator visibility"
    );
    drop(ports);
    assert!(
        !checkpoint_meta_present(&path.0),
        "the failed write must leave no durable checkpoint"
    );

    // The lost fast path is the only cost: the next open full-validates clean.
    let (_, _, outcome, verified, _) =
        complete_startup_observing_checkpoint(RedbStore::open(&path.0).expect("reopen"));
    assert!(matches!(outcome, StructuralOpenOutcome::Clean(_)));
    assert!(!verified, "no checkpoint may exist after the failed write");
}

/// Binding-mismatch coverage: doctors one binding field (self-hash kept valid),
/// asserts the checkpoint is IGNORED with the exact counted reason, and that
/// full validation still opens the otherwise-valid database cleanly.
fn assert_binding_mismatch_falls_back(
    label: &str,
    expected_reason: &'static str,
    mutate: impl FnOnce(
        &riffdb_storage_api::StoredValidatedPrefixCheckpointV1,
    ) -> riffdb_storage_api::StoredValidatedPrefixCheckpointV1,
) {
    let path = TestDatabasePath::new(label);
    prepare_checkpointed_command_database(&path.0);
    rewrite_checkpoint(&path.0, mutate);

    let report = drain_tolerating_findings(RedbStore::open(&path.0).expect("doctored open"));
    assert!(
        !report.verified,
        "{label}: binding mismatch must ignore the checkpoint"
    );
    assert_eq!(
        report.ignored_reason,
        Some(expected_reason),
        "{label}: the session must expose the exact ignore reason"
    );
    assert!(
        report.findings.is_empty() && !report.refused(),
        "{label}: full validation of otherwise-valid data must open cleanly"
    );
    let StructuralOpenOutcome::Clean(opened) = report.outcome.expect("open completes") else {
        panic!("{label}: fixture must not require migration");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let ports = dormant
        .into_operational_after_catalog_validation()
        .expect("activate operational ports");
    let counts = ports.checkpoint_ignore_counts();
    let observed = counts
        .iter()
        .find(|(reason, _)| *reason == expected_reason)
        .map(|(_, count)| *count)
        .expect("known reason");
    assert_eq!(
        observed, 1,
        "{label}: the ignore reason must be counted as {expected_reason} (counts: {counts:?})"
    );
}

#[test]
fn checkpoint_wrong_database_id_falls_back_to_full_validation() {
    assert_binding_mismatch_falls_back(
        "checkpoint-wrong-database-id",
        "database_id_mismatch",
        |original| {
            let other =
                DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_001, [0x27; 10])
                    .expect("distinct database ID");
            assert_ne!(other, original.database_id());
            riffdb_storage_api::StoredValidatedPrefixCheckpointV1::new(
                other,
                original.history_incarnation(),
                original.registry_digest(),
                original.checkpoint_commit_sequence(),
                original.audit_sequence_bound(),
                original.counts(),
                original.entity_chain_fingerprint(),
                original.retained(),
                original.previous_checkpoint_hash(),
                0,
            )
            .expect("rehash wrong database id")
        },
    );
}

#[test]
fn checkpoint_wrong_registry_digest_falls_back_to_full_validation() {
    assert_binding_mismatch_falls_back(
        "checkpoint-wrong-registry-digest",
        "registry_digest_mismatch",
        |original| {
            let wrong = riffdb_types::SchemaHash::from_bytes([0x5c; 32]);
            assert_ne!(wrong, original.registry_digest());
            riffdb_storage_api::StoredValidatedPrefixCheckpointV1::new(
                original.database_id(),
                original.history_incarnation(),
                wrong,
                original.checkpoint_commit_sequence(),
                original.audit_sequence_bound(),
                original.counts(),
                original.entity_chain_fingerprint(),
                original.retained(),
                original.previous_checkpoint_hash(),
                0,
            )
            .expect("rehash wrong digest")
        },
    );
}

#[test]
fn checkpoint_sequence_beyond_head_falls_back_to_full_validation() {
    assert_binding_mismatch_falls_back(
        "checkpoint-sequence-beyond-head",
        "sequence_beyond_head",
        |original| {
            riffdb_storage_api::StoredValidatedPrefixCheckpointV1::new(
                original.database_id(),
                original.history_incarnation(),
                original.registry_digest(),
                original.checkpoint_commit_sequence().saturating_add(999),
                original.audit_sequence_bound(),
                original.counts(),
                original.entity_chain_fingerprint(),
                original.retained(),
                original.previous_checkpoint_hash(),
                0,
            )
            .expect("rehash beyond-head sequence")
        },
    );
}

#[test]
fn doctored_entities_row_falls_back_and_full_pass_reports_the_finding() {
    // Doctor an ENTITIES row away: reconstruction at S disagrees with the
    // fingerprint → checkpoint ignored (counted) → the full pass reports the
    // real authoritative finding and the open refuses.
    let path = TestDatabasePath::new("doctored-entities-row");
    prepare_checkpointed_command_database(&path.0);
    delete_first_raw_row(&path.0, ENTITIES_RAW);

    let report = drain_tolerating_findings(RedbStore::open(&path.0).expect("doctored open"));
    assert!(
        !report.verified,
        "entity-chain fingerprint mismatch must ignore the checkpoint"
    );
    assert!(
        report.authoritative_finding(),
        "the post-fallback full pass must report the real authoritative \
         finding for the vanished ENTITIES row: {:?}",
        report.findings,
    );
    assert!(
        report.refused(),
        "authoritative corruption must refuse the open"
    );
}

#[test]
fn checkpointed_open_accepts_prefix_only_entities_via_seeded_chains() {
    // Prefix-only target under a verified checkpoint: the seeded chain carries
    // the fingerprint-verified version but no post-image hash. The version-only
    // acceptance arm must validate the ENTITIES row cleanly; without it every
    // prefix-only entity would report a false MissingCrossLink.
    let path = TestDatabasePath::new("seeded-chain-prefix-only");
    prepare_checkpointed_command_database(&path.0);

    let (_, _, outcome, verified, _) =
        complete_startup_observing_checkpoint(RedbStore::open(&path.0).expect("checkpointed open"));
    assert!(verified, "the S=head checkpoint must verify");
    assert!(
        matches!(outcome, StructuralOpenOutcome::Clean(_)),
        "prefix-only entities must validate via seeded chains"
    );
}

#[test]
fn audit_shaped_history_gets_nonzero_below_bound_sampling() {
    // The committed-command fixture carries audit rows below the bound; the
    // audit-class sample windows must inspect them (S2: audit histories get
    // nonzero below-S sampling; previously zero when only COMMITS/EVENTS were
    // sampled).
    let path = TestDatabasePath::new("audit-sample-windows");
    prepare_checkpointed_command_database(&path.0);

    let (_, _, outcome, verified, sampled) =
        complete_startup_observing_checkpoint(RedbStore::open(&path.0).expect("checkpointed open"));
    assert!(verified);
    assert!(matches!(outcome, StructuralOpenOutcome::Clean(_)));
    // Fixture: 1 commit + 1 event + 3 audit rows below bounds. Sampling must
    // now cover more than the commit/event classes alone.
    assert!(
        sampled > 2,
        "audit-class rows must be sampled below the bound (observed {sampled})"
    );
}

// ===================================================================
// RT-B retention acceptance spine (ADR-0085 Amendment 2): offline prune
// over REAL committed history. Grown from the independent review's
// runtime probes; every test asserts the spec-mandated behavior.
// ===================================================================

/// Marks the outbox intent at `(sequence, 0)` delivered, raw. The canonical
/// initial state (absent status row) is UNDELIVERED and fences prune.
fn retention_deliver_outbox_status_raw(path: &Path, sequence: u64) {
    let database = Database::open(path).expect("open raw for delivered status");
    let write = database.begin_write().expect("write");
    {
        let mut statuses = write
            .open_table(TableDefinition::<&[u8], &[u8]>::new("outbox_status"))
            .expect("outbox_status");
        let event_id = EventId::new(
            CommitSequence::new(sequence).expect("delivered sequence"),
            0,
        );
        let status = riffdb_storage_api::StoredOutboxStatusV1::delivered(
            event_id,
            std::num::NonZeroU32::MIN,
            riffdb_storage_api::OutboxDestinationIdV1::new("dest-1").expect("dest"),
            Timestamp::new(1_700_000_005, 0).expect("delivered at"),
        );
        let encoded = riffdb_storage_api::proto_codec::encode_outbox_status_v1(&status)
            .expect("encode delivered status");
        statuses
            .insert(retention_event_key(sequence).as_slice(), encoded.as_bytes())
            .expect("insert delivered status");
    }
    write.commit().expect("commit delivered status");
}

const fn retention_event_key(sequence: u64) -> [u8; 12] {
    let mut key = [0u8; 12];
    let bytes = sequence.to_be_bytes();
    let mut i = 0;
    while i < 8 {
        key[i] = bytes[i];
        i += 1;
    }
    key
}

fn retention_raw_row_present(path: &Path, table_name: &str, key: &[u8]) -> bool {
    let database = Database::open(path).expect("open raw");
    let read = database.begin_read().expect("read");
    let table = read
        .open_table(TableDefinition::<&[u8], &[u8]>::new(table_name))
        .expect("table");
    table.get(key).expect("get").is_some()
}

fn retention_raw_table_has_rows(path: &Path, table_name: &str) -> bool {
    let database = Database::open(path).expect("open raw");
    let read = database.begin_read().expect("read");
    let table = read
        .open_table(TableDefinition::<&[u8], &[u8]>::new(table_name))
        .expect("table");
    table.first().expect("first").is_some()
}

fn retention_meta_present(path: &Path, key: &str) -> bool {
    let database = Database::open(path).expect("open raw");
    let read = database.begin_read().expect("read");
    let table = read.open_table(META).expect("meta");
    table.get(key).expect("get").is_some()
}

fn retention_tombstones(path: &Path) -> Vec<riffdb_storage_api::StoredHistoryTombstoneV1> {
    let database = Database::open(path).expect("open raw");
    let read = database.begin_read().expect("read");
    let table = read
        .open_table(TableDefinition::<&[u8], &[u8]>::new("history_tombstones"))
        .expect("tombstones");
    table
        .iter()
        .expect("iter")
        .map(|entry| {
            let (_, value) = entry.expect("entry");
            riffdb_storage_api::proto_codec::decode_history_tombstone_v1(value.value())
                .expect("decode tombstone")
                .into_parts()
                .0
        })
        .collect()
}

fn retention_delete_raw_row(path: &Path, table_name: &str, key: &[u8]) {
    let database = Database::open(path).expect("open raw");
    let write = database.begin_write().expect("write");
    {
        let mut table = write
            .open_table(TableDefinition::<&[u8], &[u8]>::new(table_name))
            .expect("table");
        assert!(
            table.remove(key).expect("remove").is_some(),
            "doctored row must exist before deletion"
        );
    }
    write.commit().expect("commit doctored deletion");
}

#[test]
fn retention_prune_refuses_below_undelivered_outbox_intent() {
    // Commit 1 leaves outbox intent (1,0) in the canonical absent-status
    // (undelivered) initial state; the undelivered low-water mark fences the
    // watermark at 0. Prune REFUSES and deletes nothing.
    let path = TestDatabasePath::new("retention-undelivered-fence");
    let _ = prepare_committed_command_database(&path.0);
    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 5, "test cap").expect("hold");
    let outcome = maintenance.prune_to(1);
    assert!(
        outcome.is_err(),
        "prune below an undelivered outbox intent must refuse: {outcome:?}"
    );
    let status = maintenance.status().expect("status after refusal");
    assert_eq!(status.watermark_sequence, 0);
    assert_eq!(status.tombstone_count, 0);
    assert!(retention_raw_row_present(
        &path.0,
        "commits",
        &1u64.to_be_bytes()
    ));
    assert!(
        !retention_raw_table_has_rows(&path.0, "events"),
        "the refused prune leaves segment-owned events in their segment"
    );
}

#[test]
fn retention_prune_populated_roundtrip_reopens_clean_and_types_pruned_reads() {
    // The mandated populated-history roundtrip: prune → every pruned row gone,
    // tombstone counts exact → reopen validates CLEAN → fresh checkpoint
    // written → reads above the watermark unchanged → reads below typed
    // HistoryPruned (RDB-HISTORY-0102).
    let path = TestDatabasePath::new("retention-populated-roundtrip");
    let (_, second) = prepare_two_command_database(&path.0);
    retention_deliver_outbox_status_raw(&path.0, 1);
    retention_deliver_outbox_status_raw(&path.0, 2);

    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 5, "test cap").expect("hold");
    let status = maintenance.prune_to(1).expect("prune populated range");
    assert_eq!(status.watermark_sequence, 1);
    assert_eq!(status.tombstone_count, 1);

    assert!(!retention_raw_row_present(
        &path.0,
        "commits",
        &1u64.to_be_bytes()
    ));
    assert!(retention_raw_row_present(
        &path.0,
        "commits",
        &2u64.to_be_bytes()
    ));
    assert!(!retention_raw_table_has_rows(&path.0, "events"));
    assert!(!retention_raw_table_has_rows(&path.0, "outbox"));
    assert!(!retention_raw_row_present(
        &path.0,
        "outbox_status",
        &retention_event_key(1)
    ));
    assert!(retention_raw_row_present(
        &path.0,
        "outbox_status",
        &retention_event_key(2)
    ));
    let tombstones = retention_tombstones(&path.0);
    assert_eq!(tombstones.len(), 1);
    assert_eq!(
        (
            tombstones[0].first_sequence(),
            tombstones[0].last_sequence(),
            tombstones[0].commits_count(),
            tombstones[0].events_count(),
            tombstones[0].outbox_count(),
            tombstones[0].outbox_status_count(),
        ),
        (1, 1, 1, 1, 1, 1),
        "tombstone must record every pruned row"
    );

    // Reopen: the tombstone-tolerant walk must be CLEAN on a pruned database.
    let findings = collect_structural_findings(RedbStore::open(&path.0).expect("reopen pruned"));
    assert!(
        findings.is_empty(),
        "pruned reopen must validate clean: {findings:?}"
    );

    // A clean full pass writes a FRESH checkpoint bound to the new watermark.
    complete_startup_pass(RedbStore::open(&path.0).expect("checkpoint pass"));
    assert!(
        retention_meta_present(&path.0, "validated_prefix_checkpoint/v1"),
        "clean pruned validation must write a fresh checkpoint"
    );

    // Reads above the watermark are unchanged; reads below are typed pruned.
    let ports = open_operational(RedbStore::open(&path.0).expect("operational reopen"));
    let retained = ports
        .read_commit(second.records.commit().commit_sequence())
        .expect("read retained commit")
        .expect("retained commit body");
    assert_eq!(retained.commit_sequence().get(), 2);
    let retained_event = ports
        .read_durable_event(EventId::new(CommitSequence::new(2).expect("sequence"), 0))
        .expect("read retained event")
        .expect("retained event body");
    assert_eq!(retained_event.event_id().commit_sequence().get(), 2);
    let commit_err = ports
        .read_commit(CommitSequence::first())
        .expect_err("below-watermark commit read must be typed pruned");
    assert_eq!(
        commit_err.kind(),
        riffdb_storage_api::StorageErrorKind::HistoryPruned
    );
    let event_err = ports
        .read_durable_event(EventId::new(CommitSequence::first(), 0))
        .expect_err("below-watermark event read must be typed pruned");
    assert_eq!(
        event_err.kind(),
        riffdb_storage_api::StorageErrorKind::HistoryPruned
    );
}

#[test]
fn retention_prune_cannot_pass_durable_history_end() {
    // One committed command (durable head 1): the application-head fencing
    // input refuses any watermark past the end of durable history, even under
    // a permissive operator hold.
    let path = TestDatabasePath::new("retention-head-fence");
    let _ = prepare_committed_command_database(&path.0);
    retention_deliver_outbox_status_raw(&path.0, 1);
    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 100, "test cap").expect("hold");
    let outcome = maintenance.prune_to(50);
    assert!(
        outcome.is_err(),
        "watermark must not pass the durable history end: {outcome:?}"
    );
    let status = maintenance.status().expect("status after refusal");
    assert_eq!(status.watermark_sequence, 0);
    assert_eq!(
        status.max_permissible_watermark,
        Some(1),
        "durable application head must bind the fencing maximum"
    );
    assert_eq!(
        status.fence_binding,
        riffdb_storage_api::RetentionFenceBinding::DurableApplicationHead
    );
}

#[test]
fn retention_prune_refuses_on_empty_history() {
    // Nothing ever committed: the durable head is 0 and pruning anything is
    // impossible by construction, even under an operator hold.
    let path = TestDatabasePath::new("retention-empty-history");
    prepare_command_database(&path.0);
    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 5, "test cap").expect("hold");
    let outcome = maintenance.prune_to(1);
    assert!(
        outcome.is_err(),
        "prune over empty history must refuse: {outcome:?}"
    );
}

#[test]
fn retention_prune_deletes_live_checkpoint_first() {
    let path = TestDatabasePath::new("retention-checkpoint-delete");
    prepare_checkpointed_command_database(&path.0);
    assert!(
        retention_meta_present(&path.0, "validated_prefix_checkpoint/v1"),
        "fixture must start with a live checkpoint"
    );
    retention_deliver_outbox_status_raw(&path.0, 1);
    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 5, "test cap").expect("hold");
    let _ = maintenance
        .prune_to(1)
        .expect("prune checkpointed database");
    assert!(
        !retention_meta_present(&path.0, "validated_prefix_checkpoint/v1"),
        "prune must delete the validated-prefix checkpoint in its first transaction"
    );
}

#[test]
fn retention_stale_checkpoint_bound_to_wrong_watermark_is_ignored() {
    // A checkpoint written BEFORE the prune (watermark binding 0) restored
    // after the prune must be IGNORED via WatermarkMismatch and fall back to
    // full validation. The checkpoint is engineered so every other ignore
    // condition passes: removing the watermark ignore-arm would let it verify.
    let path = TestDatabasePath::new("retention-stale-checkpoint");
    prepare_checkpointed_command_database(&path.0);
    // Capture the S=1 checkpoint (bound to watermark 0, counts within the
    // post-prune totals).
    let stale = {
        let database = Database::open(&path.0).expect("open raw");
        let read = database.begin_read().expect("read");
        let meta = read.open_table(META).expect("meta");
        meta.get("validated_prefix_checkpoint/v1")
            .expect("get")
            .expect("live checkpoint")
            .value()
            .to_vec()
    };
    // Commit sequence 2 so the retained head stays above the pruned prefix.
    let second = command_fixture_at(2);
    let ports = open_operational(RedbStore::open(&path.0).expect("reopen for second commit"));
    commit_command_fixture(&ports, &second);
    drop(ports);
    retention_deliver_outbox_status_raw(&path.0, 1);
    retention_deliver_outbox_status_raw(&path.0, 2);
    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 5, "test cap").expect("hold");
    let _ = maintenance.prune_to(1).expect("prune");
    assert!(!retention_meta_present(
        &path.0,
        "validated_prefix_checkpoint/v1"
    ));
    {
        let database = Database::open(&path.0).expect("open raw");
        let write = database.begin_write().expect("write");
        {
            let mut meta = write.open_table(META).expect("meta");
            meta.insert("validated_prefix_checkpoint/v1", stale.as_slice())
                .expect("restore stale checkpoint");
        }
        write.commit().expect("commit stale checkpoint");
    }

    let digest_key = DigestKeyId::new(1).expect("digest key ID");
    let inputs = StartupValidationInputs::new(
        Timestamp::new(1_700_000_000, 0).expect("startup timestamp"),
        ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("capability digest inventory"),
        ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("idempotency digest inventory"),
    );
    let session = RedbStore::open(&path.0)
        .expect("open with stale checkpoint")
        .begin_structural_evidence(inputs)
        .expect("stale checkpoint must not block the open");
    assert!(
        !session.checkpoint_verified(),
        "a checkpoint bound to the wrong watermark must never verify"
    );
    assert_eq!(
        session.checkpoint_ignored_reason(),
        Some("watermark_mismatch"),
        "the stale checkpoint must be ignored by the watermark binding"
    );
    drop(session);
    let findings =
        collect_structural_findings(RedbStore::open(&path.0).expect("full validation reopen"));
    assert!(
        findings.is_empty(),
        "full validation after ignoring the stale checkpoint must be clean: {findings:?}"
    );
}

#[test]
fn retention_absence_above_watermark_remains_corruption() {
    // Deliverable: absence NOT covered by the verified tombstone chain is the
    // same corruption it is today. Doctor away rows ABOVE the watermark; the
    // walk must refuse or produce authoritative findings.
    let path = TestDatabasePath::new("retention-doctored-above");
    let _ = prepare_two_command_database(&path.0);
    retention_deliver_outbox_status_raw(&path.0, 1);
    retention_deliver_outbox_status_raw(&path.0, 2);
    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 5, "test cap").expect("hold");
    let _ = maintenance.prune_to(1).expect("prune");
    retention_delete_raw_row(&path.0, "commits", &2u64.to_be_bytes());
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        collect_structural_findings(RedbStore::open(&path.0).expect("reopen doctored database"))
    }));
    match outcome {
        Ok(findings) => assert!(
            !findings.is_empty(),
            "doctored commit above the watermark must stay corruption"
        ),
        Err(_) => {
            // A refused walk (structural error) is an equally valid
            // fail-closed outcome for uncovered absence.
        }
    }
}

#[test]
fn retention_multi_prune_resumes_tombstone_chain() {
    // The chain-resume path: a second prune (and a child-process kill before
    // the resumed sub-range commits) continues the tombstone chain —
    // contiguous, hash-linked, abutting the advanced watermark — and the
    // pruned database still validates clean.
    let path = TestDatabasePath::new("retention-chain-resume");
    let _ = prepare_two_command_database(&path.0);
    retention_deliver_outbox_status_raw(&path.0, 1);
    retention_deliver_outbox_status_raw(&path.0, 2);

    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 5, "test cap").expect("hold");
    let status = maintenance.prune_to(1).expect("first prune");
    assert_eq!(status.watermark_sequence, 1);
    drop(maintenance);

    // Child-process kill before the resumed sub-range commits: state stays
    // valid at watermark 1 and the database reopens.
    run_crashing_child_prune("before-retention-prune-subrange-commit", &path.0, 2);

    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    let status = maintenance.status().expect("status after aborted resume");
    assert_eq!(status.watermark_sequence, 1);
    assert_eq!(status.tombstone_count, 1);
    drop(RedbStore::open(&path.0).expect("reopen after aborted resume"));

    // Re-run: the chain RESUMES with a second tombstone linked to the first.
    let status = maintenance.prune_to(2).expect("resumed prune");
    assert_eq!(status.watermark_sequence, 2);
    assert_eq!(status.tombstone_count, 2);
    let tombstones = retention_tombstones(&path.0);
    assert_eq!(tombstones.len(), 2);
    assert_eq!(
        (
            tombstones[0].first_sequence(),
            tombstones[0].last_sequence()
        ),
        (1, 1)
    );
    assert_eq!(
        (
            tombstones[1].first_sequence(),
            tombstones[1].last_sequence()
        ),
        (2, 2)
    );
    assert_eq!(
        tombstones[1].previous_tombstone_hash(),
        Some(tombstones[0].tombstone_hash()),
        "the resumed tombstone must hash-link to its predecessor"
    );
    let findings =
        collect_structural_findings(RedbStore::open(&path.0).expect("reopen fully pruned"));
    assert!(
        findings.is_empty(),
        "fully pruned reopen must validate clean: {findings:?}"
    );
}

#[test]
fn retention_prune_refuses_when_commit_rows_missing_in_range() {
    // Pre-delete verification: a commit row already missing inside the
    // requested range is pre-existing corruption. Prune must REFUSE rather
    // than launder the absence into "verified pruned".
    let path = TestDatabasePath::new("retention-predelete-verification");
    let _ = prepare_committed_command_database(&path.0);
    retention_deliver_outbox_status_raw(&path.0, 1);
    retention_delete_raw_row(&path.0, "commits", &1u64.to_be_bytes());
    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 5, "test cap").expect("hold");
    let err = maintenance
        .prune_to(1)
        .expect_err("prune over a missing commit row must refuse");
    assert_eq!(
        err.kind(),
        riffdb_storage_api::StorageErrorKind::CorruptData
    );
    assert!(retention_raw_row_present(
        &path.0,
        "outbox_status",
        &retention_event_key(1)
    ));
    assert!(retention_tombstones(&path.0).is_empty());
}

#[test]
fn retention_semantic_chain_forgery_refuses_startup() {
    // A canonically re-encoded tombstone with a VALID self-hash but forged
    // semantics must refuse startup validation:
    // (a) forged per-table counts (count arithmetic),
    // (b) forged range end (abutment at the watermark).
    for (label, forge_last, forge_counts) in [("counts", 1u64, 7u64), ("gap", 1u64, 1u64)] {
        let path = TestDatabasePath::new("retention-chain-forgery");
        let _ = prepare_two_command_database(&path.0);
        retention_deliver_outbox_status_raw(&path.0, 1);
        retention_deliver_outbox_status_raw(&path.0, 2);
        let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
        maintenance.add_hold("cap", 5, "test cap").expect("hold");
        let target = if label == "gap" { 2 } else { 1 };
        let _ = maintenance.prune_to(target).expect("prune");
        {
            let database = Database::open(&path.0).expect("open raw");
            let write = database.begin_write().expect("write");
            {
                let mut table = write
                    .open_table(TableDefinition::<&[u8], &[u8]>::new("history_tombstones"))
                    .expect("tombstones");
                let (key_bytes, original) = {
                    let (key, value) = table
                        .iter()
                        .expect("iter")
                        .next()
                        .expect("row")
                        .expect("entry");
                    (key.value().to_vec(), value.value().to_vec())
                };
                let original =
                    riffdb_storage_api::proto_codec::decode_history_tombstone_v1(&original)
                        .expect("decode original")
                        .into_parts()
                        .0;
                let forged = riffdb_storage_api::StoredHistoryTombstoneV1::new(
                    original.first_sequence(),
                    forge_last,
                    forge_counts,
                    forge_counts,
                    forge_counts,
                    forge_counts,
                    original.content_digest(),
                    original.previous_tombstone_hash(),
                    original.history_incarnation(),
                )
                .expect("forge canonically valid tombstone");
                let encoded = riffdb_storage_api::proto_codec::encode_history_tombstone_v1(&forged)
                    .expect("encode forged tombstone");
                if label == "gap" {
                    // Drop any later tombstones so the forged [1,1] leaves a
                    // real gap below watermark 2.
                    let keys: Vec<Vec<u8>> = table
                        .iter()
                        .expect("iter")
                        .map(|entry| entry.expect("entry").0.value().to_vec())
                        .collect();
                    for key in keys {
                        let _ = table.remove(key.as_slice()).expect("remove");
                    }
                }
                table
                    .insert(key_bytes.as_slice(), encoded.as_bytes())
                    .expect("rewrite forged tombstone");
            }
            write.commit().expect("commit forgery");
        }
        let digest_key = DigestKeyId::new(1).expect("digest key ID");
        let inputs = StartupValidationInputs::new(
            Timestamp::new(1_700_000_000, 0).expect("startup timestamp"),
            ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
                .expect("capability digest inventory"),
            ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
                .expect("idempotency digest inventory"),
        );
        let outcome = RedbStore::open(&path.0)
            .expect("plain open (canonical bytes)")
            .begin_structural_evidence(inputs);
        assert!(
            outcome.is_err(),
            "semantic chain forgery ({label}) must refuse startup validation"
        );
    }
}

#[test]
fn retention_projection_frontier_fences_prune() {
    // A live projection whose durable frontier is BeforeFirst fences the
    // watermark at 0; detaching it (audited) releases the fence.
    use riffdb_types::{ContractLineage, ProjectionFrontierKey, ProjectionId, ProjectionPlanHash};

    let path = TestDatabasePath::new("retention-projection-fence");
    let _ = prepare_committed_command_database(&path.0);
    retention_deliver_outbox_status_raw(&path.0, 1);
    let identity = riffdb_types::ProjectionIdentity::new(
        ContractLineage::new("retention-fence").expect("lineage"),
        ProjectionId::new(9).expect("projection id"),
        ProjectionPlanHash::from_bytes([0x33; 32]),
    );
    let control = riffdb_storage_api::StoredProjectionControlV1::new(
        identity.clone(),
        riffdb_types::ProjectionGeneration::first(),
        Some(riffdb_storage_api::ProjectionGenerationPosition::new(
            riffdb_types::ProjectionGeneration::first(),
            riffdb_types::FrontierPosition::BeforeFirst,
        )),
        None,
        Some(riffdb_storage_api::PublishedApplyModeV1::Enabled),
        riffdb_storage_api::ProjectionLifecycleV1::Ready,
        None,
    )
    .expect("projection control");
    {
        let database = Database::open(&path.0).expect("open raw");
        let write = database.begin_write().expect("write");
        {
            let mut controls = write
                .open_table(TableDefinition::<&[u8], &[u8]>::new("projection_frontier"))
                .expect("projection_frontier");
            let key = ProjectionFrontierKey::new(identity.clone());
            let encoded = riffdb_storage_api::proto_codec::encode_projection_control_v1(&control)
                .expect("encode control");
            controls
                .insert(key.as_bytes(), encoded.as_bytes())
                .expect("insert control");
        }
        write.commit().expect("commit control");
    }

    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 5, "test cap").expect("hold");
    let err = maintenance
        .prune_to(1)
        .expect_err("projection frontier at BeforeFirst must fence the prune");
    assert_eq!(
        err.kind(),
        riffdb_storage_api::StorageErrorKind::InvariantViolation
    );
    let status = maintenance.status().expect("status");
    assert_eq!(status.max_permissible_watermark, Some(0));
    assert_eq!(
        status.fence_binding,
        riffdb_storage_api::RetentionFenceBinding::ProjectionDurableFrontier
    );

    // Audited detach releases the projection from the fencing minimum.
    maintenance
        .detach_projection(
            ProjectionId::new(9).expect("projection id"),
            "replay budget accepted",
            Timestamp::new(1_700_000_009, 0).expect("timestamp"),
        )
        .expect("detach projection");
    let status = maintenance.prune_to(1).expect("prune after detach");
    assert_eq!(status.watermark_sequence, 1);
}

#[test]
fn retention_pruned_route_resolution_yields_typed_pruned_via_event_replay() {
    // Real-path RDB-HISTORY-0102: routes are retained under prune, and a
    // route resolving below the watermark surfaces the typed pruned outcome
    // through the catalog event-replay join — never an integrity error.
    use riffdb_catalog::ActiveCatalogSnapshot;

    let path = TestDatabasePath::new("retention-replay-pruned");
    let _ = prepare_two_command_database(&path.0);
    retention_deliver_outbox_status_raw(&path.0, 1);
    retention_deliver_outbox_status_raw(&path.0, 2);
    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 5, "test cap").expect("hold");
    let _ = maintenance.prune_to(1).expect("prune");
    let route_key = {
        let mut key = [0u8; 44];
        key[..32].copy_from_slice(
            hash_partition_key(command_fixture_at(1).pending.partition_key().as_bytes()).as_bytes(),
        );
        key[32..].copy_from_slice(&retention_event_key(1));
        key
    };
    assert!(
        retention_raw_row_present(&path.0, "event_routes", &route_key),
        "event routes must be RETAINED under prune"
    );

    let ports = open_operational(RedbStore::open(&path.0).expect("operational reopen"));
    let active = ActiveCatalogSnapshot::read(&ports)
        .expect("catalog read")
        .expect("active catalog");
    let replay = active
        .resolve_event_replay(
            "RowCreated",
            [("id", CanonicalValue::U64(7))],
            ["id", "value"],
        )
        .expect("resolve event replay");
    let error = replay
        .replay_page(
            &ports,
            riffdb_catalog::EventReplayPosition::Initial { after: None },
            riffdb_storage_api::EventRoutePageLimit::new(
                std::num::NonZeroU16::new(4).expect("nonzero"),
            )
            .expect("limit"),
            1,
        )
        .expect_err("a route below the watermark must surface typed pruned");
    assert_eq!(
        error.kind(),
        riffdb_catalog::EventReplayErrorKind::Storage(
            riffdb_storage_api::StorageErrorKind::HistoryPruned
        ),
        "pruned replay must be the typed outcome, never integrity"
    );
}

#[test]
fn retention_operator_hold_fences_prune() {
    let path = TestDatabasePath::new("retention-hold-fence");
    let _ = prepare_two_command_database(&path.0);
    retention_deliver_outbox_status_raw(&path.0, 1);
    retention_deliver_outbox_status_raw(&path.0, 2);
    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance
        .add_hold("legal-hold", 1, "litigation")
        .expect("hold");
    let err = maintenance
        .prune_to(2)
        .expect_err("prune past the operator hold must refuse");
    assert_eq!(
        err.kind(),
        riffdb_storage_api::StorageErrorKind::InvariantViolation
    );
    let status = maintenance.status().expect("status");
    assert_eq!(status.max_permissible_watermark, Some(1));
    assert_eq!(
        status.fence_binding,
        riffdb_storage_api::RetentionFenceBinding::OperatorHold
    );
    let status = maintenance.prune_to(1).expect("prune at the hold fence");
    assert_eq!(status.watermark_sequence, 1);
}

#[test]
fn retention_staged_migration_frontier_fences_prune() {
    // An open staged contract migration's frozen application frontier fences
    // the watermark: history at or below the frozen frontier must survive
    // until the stage resolves.
    use riffdb_storage_api::{
        ContractMigrationArtifactsV1, ContractMigrationJournalStepV1, MigrationScanCursor,
        StoredContractMigrationJournalV1,
    };
    use riffdb_types::{
        ContractBundleHash, ContractMigrationInputHash, ContractMigrationOperationId,
        MigrationBundleHash,
    };

    let path = TestDatabasePath::new("retention-staged-fence");
    let _ = prepare_two_command_database(&path.0);
    retention_deliver_outbox_status_raw(&path.0, 1);
    retention_deliver_outbox_status_raw(&path.0, 2);
    let operation_id =
        ContractMigrationOperationId::from_unix_milliseconds_and_random(2, [0x22; 10])
            .expect("operation id");
    let journal = StoredContractMigrationJournalV1::new(
        database_id(),
        operation_id,
        ContractMigrationInputHash::from_bytes([0x55; 32]),
        ContractMigrationArtifactsV1::new(
            ContractBundleHash::from_bytes([0x66; 32]),
            ContractBundleHash::from_bytes([0x77; 32]),
            MigrationBundleHash::from_bytes([0x88; 32]),
        ),
        ContractMigrationJournalStepV1::Transforming,
        MigrationScanCursor::start(),
        0,
        0,
        1,
        Some(CommitSequence::first()),
        Vec::new(),
        None,
    )
    .expect("staged journal with frozen frontier");
    {
        let database = Database::open(&path.0).expect("open raw");
        let write = database.begin_write().expect("write");
        {
            let mut table = write
                .open_table(TableDefinition::<&[u8], &[u8]>::new(
                    "contract_migration_journal",
                ))
                .expect("journal table");
            let encoded =
                riffdb_storage_api::proto_codec::encode_contract_migration_journal_v1(&journal)
                    .expect("encode journal");
            table
                .insert(operation_id.into_bytes().as_slice(), encoded.as_bytes())
                .expect("insert journal");
        }
        write.commit().expect("commit journal");
    }

    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 5, "test cap").expect("hold");
    let err = maintenance
        .prune_to(2)
        .expect_err("prune past the frozen application frontier must refuse");
    assert_eq!(
        err.kind(),
        riffdb_storage_api::StorageErrorKind::InvariantViolation
    );
    let status = maintenance.status().expect("status");
    assert_eq!(status.max_permissible_watermark, Some(1));
    assert_eq!(
        status.fence_binding,
        riffdb_storage_api::RetentionFenceBinding::StagedMigrationFrozenFrontier
    );
}

#[test]
fn retention_after_commit_kill_leaves_durable_subrange() {
    // Child kill AFTER the sub-range transaction committed: the sub-range is
    // durable {deleted rows, tombstone, watermark} and the database reopens
    // valid (chain resumes / state consistent).
    let path = TestDatabasePath::new("retention-after-commit");
    let _ = prepare_committed_command_database(&path.0);
    retention_deliver_outbox_status_raw(&path.0, 1);
    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 5, "test cap").expect("hold");
    drop(maintenance);
    run_crashing_child_prune("after-retention-prune-subrange-commit", &path.0, 1);

    let status = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0)
        .status()
        .expect("status after post-commit kill");
    assert_eq!(status.watermark_sequence, 1);
    assert_eq!(status.tombstone_count, 1);
    let findings = collect_structural_findings(
        RedbStore::open(&path.0).expect("reopen after post-commit kill"),
    );
    assert!(findings.is_empty(), "durable sub-range must validate clean");
}

#[test]
fn retention_tombstone_byte_corruption_refuses_open() {
    // A byte-flipped tombstone row fails canonical decode at plain open.
    let path = TestDatabasePath::new("retention-tombstone-byteflip");
    let _ = prepare_committed_command_database(&path.0);
    retention_deliver_outbox_status_raw(&path.0, 1);
    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 5, "test cap").expect("hold");
    let _ = maintenance.prune_to(1).expect("prune");
    {
        let database = Database::open(&path.0).expect("open raw");
        let write = database.begin_write().expect("write");
        {
            let mut table = write
                .open_table(TableDefinition::<&[u8], &[u8]>::new("history_tombstones"))
                .expect("tombstones");
            let (key_bytes, mut bytes) = {
                let (key, value) = table
                    .iter()
                    .expect("iter")
                    .next()
                    .expect("row")
                    .expect("entry");
                (key.value().to_vec(), value.value().to_vec())
            };
            let last = bytes.len() - 1;
            bytes[last] ^= 0xff;
            table
                .insert(key_bytes.as_slice(), bytes.as_slice())
                .expect("rewrite");
        }
        write.commit().expect("commit corruption");
    }
    let err = RedbStore::open(&path.0).expect_err("byte-corrupt tombstone must refuse open");
    assert_eq!(
        err.kind(),
        riffdb_storage_api::StorageErrorKind::CorruptData
    );
}

#[test]
fn retention_backup_of_pruned_database_restores_and_validates() {
    use riffdb_storage_api::{
        BackupBuildMetadataV1, OfflineBackupPersistencePort, OfflineRestoreOverwritePolicyV1,
        OfflineRestorePersistencePort, OfflineRestoreResultV1,
    };

    let path = TestDatabasePath::new("retention-backup-pruned");
    let _ = prepare_two_command_database(&path.0);
    retention_deliver_outbox_status_raw(&path.0, 1);
    retention_deliver_outbox_status_raw(&path.0, 2);
    let maintenance = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    maintenance.add_hold("cap", 5, "test cap").expect("hold");
    let status = maintenance.prune_to(1).expect("prune");
    assert_eq!(status.watermark_sequence, 1);

    let root = std::env::temp_dir().join(format!(
        "riffdb-retention-backup-{}-{}",
        std::process::id(),
        NEXT_PATH.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("backup root");
    let backup_dir = root.join("backup");
    let build = BackupBuildMetadataV1::new(
        "0.1.0",
        "0123456789abcdef",
        "rustc-1.97.0",
        1,
        vec!["retention".to_owned()],
    )
    .expect("build metadata");
    let mut backup = riffdb_storage_redb::RedbOfflineBackup::bind(&path.0, &backup_dir);
    let manifest = backup.create_offline_backup(&build).expect("backup");
    assert_eq!(manifest.retention_watermark_sequence(), Some(1));

    let restore_dir = root.join("restored");
    std::fs::create_dir_all(&restore_dir).expect("restore dir");
    let mut restore = riffdb_storage_redb::RedbOfflineRestore::bind(&backup_dir, &restore_dir);
    let result = restore
        .restore_offline_backup(OfflineRestoreOverwritePolicyV1::RefuseNonEmpty)
        .expect("restore");
    assert!(matches!(result, OfflineRestoreResultV1::Restored { .. }));

    let restored = restore_dir.join("database.redb");
    let findings = collect_structural_findings(RedbStore::open(&restored).expect("open restored"));
    assert!(
        findings.is_empty(),
        "restored pruned database must validate clean: {findings:?}"
    );
    let restored_status = riffdb_storage_redb::RedbOfflineRetention::bind(&restored)
        .status()
        .expect("restored status");
    assert_eq!(restored_status.watermark_sequence, 1);
    assert_eq!(restored_status.tombstone_count, 1);
    let _ = std::fs::remove_dir_all(&root);
}
