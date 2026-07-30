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
    CandidateAdmissionResult, CandidateCapacityResult, CatalogActivationIntentV1,
    CatalogActivationResult, CatalogAdministrationRepository, CommandCandidateAdmission,
    CommandCandidateAffectedEpochRead, CommandCandidateAwaitingCapacity,
    CommandCandidateAwaitingValidation, CommandCandidateCapacityReserved,
    CommandCandidateSequenceAssigned, CommandCandidateStateRead, CommandWriteSetPlanV1,
    CurrentIndexGenerationObservation, DatabaseIdentityProbe, DatabaseIdentityProbePort,
    DatabaseInitializationPort, DeclaredOutcome, DurabilityMode, DurableKeySchemaBindingV1,
    EmptyCommandBatch, EncodedWriteSetUpperBoundResultV1, EntityMutation, EntityObservation,
    EntityPostImage, EntityTarget, EvaluationBudget, EventIntent, EvidencePageLimit,
    ExecutablePlanRef, ExpectedEntityState, IdempotencyIdentity, IdempotencyKeyDigest,
    IdempotencyLookupCandidatesV1, IndexEntryMutationV1, IndexEpochAdvanceV1, IndexEpochPosition,
    IndexRangeTarget, MAX_INDEX_MIGRATION_PAGE_BYTES, MAX_INDEX_MIGRATION_PAGE_ENTRIES,
    NonEmptyCommandBatch, OpenSessionId, OutboxPageLimit, OutboxRepository,
    OutboxStatusObservationV1, OutboxStatusReadResultV1, PartitionIndexTarget, PendingOutboxScanV1,
    PreEvaluationCommitContext, ReadSnapshot, ReadableCapabilityDigestInventory, ReadableDigestKey,
    ReadableIdempotencyDigestInventory, ServiceAuditAppendIntentV1, SnapshotReader,
    SnapshotRequest, StartupValidationInputs, StorageScanLimit, StoredAdministrationAuditRecordV1,
    StoredAdmittedProvenanceClaimsV1, StoredContractBundleV1, StoredDurableEventV1,
    StoredEntityRecordV1, StoredIndexEntryV1, StoredIndexEntryV2, StoredOutboxIntentV1,
    StoredOutcomeV1, StoredPendingAdmissionV1, StoredProvenanceRecordV1, StoredReadDependenciesV1,
    StructuralEvidenceCursor, StructuralEvidenceOpen, StructuralEvidencePage,
    StructuralEvidenceSession, StructuralOpenOutcome, StructurallyOpened,
    command_write_set_upper_bound_v1, decode_index_entry_v1, decode_index_entry_v2,
    decode_index_migration_row, derive_event_hash_v1, encode_index_entry_v1_fixture,
    encode_index_entry_v2,
};
use riffdb_storage_redb::{
    RedbCommitProfile, RedbDormantPorts, RedbOperationalPorts, RedbStartupIndexMigrationPort,
    RedbStore, RedbTestController, RedbTestOperation,
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
    assert!(!status.success(), "the armed child must terminate abruptly");
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

fn target_and_index() -> (EntityTarget, IndexEntryKey, IndexRangeTarget) {
    let entity_type_id = EntityTypeId::new(1).expect("entity type ID");
    let mut entity_key = EntityKeyBuilder::new(entity_type_id);
    entity_key.push_u64(7).expect("entity key component");
    let entity_key = entity_key.finish().expect("entity key");
    let target = EntityTarget::new(entity_type_id, entity_key.clone()).expect("entity target");

    let index_id = IndexId::new(1).expect("index ID");
    let mut index_key = IndexEntryKeyBuilder::new(index_id);
    index_key.push_u64(10).expect("index component");
    let index_key = index_key.finish(entity_key).expect("index entry key");
    let mut prefix = riffdb_storage_api::IndexRangePrefixBuilder::new(index_id);
    prefix.push_u64(10).expect("range component");
    let mut partition = PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate"));
    partition.push_u64(7).expect("partition component");
    let range = IndexRangeTarget::new(partition.finish().expect("partition key"), prefix.finish());
    (target, index_key, range)
}

fn command_fixture() -> CommandFixture {
    let plan = plan();
    let sequence = CommitSequence::first();
    let (target, index_key, range) = target_and_index();
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
        IdempotencyKeyDigest::from_hmac_bytes(DigestKeyId::new(1).expect("digest key"), [0x41; 32]),
    );
    let request_id = RequestId::from_bytes(uuid_bytes(0x31)).expect("request ID");
    let provenance_id = ProvenanceId::from_bytes(uuid_bytes(0x51)).expect("provenance ID");
    let logical_time =
        LogicalTime::new(Timestamp::new(1_700_000_001, 0).expect("logical timestamp"));
    let mut partition = PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate"));
    partition.push_u64(7).expect("partition component");
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
        mutations.clone(),
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
        vec![event.clone()],
        vec![StoredOutboxIntentV1::new(event)],
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
    assert!(matches!(
        result,
        CatalogActivationResult::Activated { active, .. }
            if active == riffdb_storage_api::ActiveCatalogPointerV1::from_bundle(&bundle)
    ));
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
    assert_eq!(candidate.assignment().assigned(), CommitSequence::first());
    candidate
        .stage(fixture.records.clone())
        .expect("stage complete command graph")
        .commit_with_service_audit_transitions(
            DurabilityMode::Sync,
            vec![command_audit_transition(fixture)],
        )
        .expect("commit complete command graph and audit lifecycle");
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
        .map(|entry| {
            let (key, value) = entry.expect("read migration fixture metadata");
            (key.value().to_owned(), value.value().to_vec())
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
        "before-index-migration-batch-commit" => {
            RedbTestController::abort_before_commit(RedbTestOperation::IndexMigrationBatch)
        }
        "after-index-migration-batch-commit" => {
            RedbTestController::abort_after_commit(RedbTestOperation::IndexMigrationBatch)
        }
        _ => panic!("unknown closed child mode"),
    };
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
        _ => unreachable!("controller match rejects unknown modes"),
    }
    panic!("the armed failpoint did not terminate the child");
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
