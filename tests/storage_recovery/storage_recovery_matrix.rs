#![forbid(unsafe_code)]

//! Child-process crash and reopen evidence for the redb storage boundary.

use std::num::{NonZeroU16, NonZeroU64};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

use riffdb_storage_api::{
    AdmissionLookupResultV1, AdmissionRepository, AdmissionRequestV1, AdmissionResultV1,
    AffectedEntityV1, AffectedEpochCurrentState, AffectedIndexEpochTargets,
    ApplicationCommandTransactionPort, AssignedCommandSequence, AtomicCommandRecordSet,
    AuditPrincipalV1, AuthoritativeIndexScanPage, AuthoritativeIndexScanRequest,
    AuthoritativePointReader, AuthoritativeScanReader, CandidateAdmissionResult,
    CandidateCapacityResult, CatalogActivationIntentV1, CatalogActivationResult,
    CatalogAdministrationRepository, CommandCandidateAdmission, CommandCandidateAffectedEpochRead,
    CommandCandidateAwaitingCapacity, CommandCandidateAwaitingValidation,
    CommandCandidateCapacityReserved, CommandCandidateSequenceAssigned, CommandCandidateStateRead,
    CommandWriteSetPlanV1, CurrentRangeObservation, DatabaseIdentityProbe,
    DatabaseIdentityProbePort, DatabaseInitializationPort, DeclaredOutcome, DurabilityMode,
    DurableKeySchemaBindingV1, EmptyCommandBatch, EntityMutation, EntityObservation,
    EntityPostImage, EntityTarget, EvaluationBudget, EventIntent, EvidencePageLimit,
    ExecutablePlanRef, ExpectedEntityState, HistoricalEvidenceCursor, HistoricalEvidencePage,
    IdempotencyIdentity, IdempotencyKeyDigest, IdempotencyLookupCandidatesV1, IndexEntryMutationV1,
    IndexEpochAdvanceV1, IndexEpochPosition, IndexRangeObservation, IndexRangeTarget,
    NonEmptyCommandBatch, OutboxPageLimit, OutboxRepository, OutboxStatusObservationV1,
    OutboxStatusReadResultV1, PendingOutboxScanV1, PreEvaluationCommitContext, ReadSnapshot,
    ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
    SnapshotReader, SnapshotRequest, StartupValidationInputs, StorageScanLimit,
    StoredAdmittedProvenanceClaimsV1, StoredContractBundleV1, StoredDurableEventV1,
    StoredEntityRecordV1, StoredIndexEntryV1, StoredOutboxIntentV1, StoredOutcomeV1,
    StoredPendingAdmissionV1, StoredProvenanceRecordV1, StoredReadDependenciesV1,
    StructuralEvidenceCursor, StructuralEvidenceOpen, StructuralEvidencePage,
    StructuralEvidenceSession, StructurallyOpened, command_write_set_upper_bound_v1,
    derive_event_hash_v1,
};
use riffdb_storage_redb::{
    RedbDormantPorts, RedbOperationalPorts, RedbStore, RedbTestController, RedbTestOperation,
};
use riffdb_types::{
    ActorId, ActorKind, AggregateTypeId, CanonicalInputHash, CanonicalRecord, CanonicalValue,
    CapabilityId, CommandId, CommitSequence, ContractLineage, ContractVersion, DatabaseId,
    DigestKeyId, EntityKeyBuilder, EntityTypeId, EntityVersion, Environment, EventId, EventTypeId,
    FieldId, IndexEntryKey, IndexEntryKeyBuilder, IndexId, LogicalTime, OutcomeId,
    PartitionKeyBuilder, PlanHash, ProvenanceId, RequestId, TenantId, TenantScope, Timestamp,
    hash_contract_bundle, hash_partition_key,
};

const CHILD_MODE: &str = "RIFFDB_STORAGE_RECOVERY_CHILD_MODE";
const CHILD_PATH: &str = "RIFFDB_STORAGE_RECOVERY_CHILD_PATH";
static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

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
    let status = Command::new(std::env::current_exe().expect("current test executable"))
        .arg("--exact")
        .arg("process_recovery_child")
        .arg("--nocapture")
        .env(CHILD_MODE, mode)
        .env(CHILD_PATH, path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("run recovery child");
    assert!(!status.success(), "the armed child must terminate abruptly");
}

fn complete_structural_open(store: RedbStore) -> StructurallyOpened<RedbDormantPorts> {
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

    let mut historical_cursor = HistoricalEvidenceCursor::start(database_id, open_session_id);
    let historical_end = loop {
        match session
            .read_historical_evidence(historical_cursor, limit)
            .expect("read historical evidence")
        {
            HistoricalEvidencePage::Page { next, .. } => historical_cursor = next,
            HistoricalEvidencePage::ExactEnd(end) => break end,
        }
    };

    let opened = session
        .finish(structural_end, historical_end)
        .expect("finish structural evidence");
    assert_eq!(opened.database_id(), database_id);
    opened
}

fn open_operational(store: RedbStore) -> RedbOperationalPorts {
    let opened = complete_structural_open(store);
    let (_, _, dormant) = opened.into_parts();
    dormant
        .into_operational_after_catalog_validation()
        .expect("activate storage fixture; WP-130 owns catalog-proof composition")
}

#[derive(Clone)]
struct CommandFixture {
    admission: AdmissionRequestV1,
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

fn contract_bundle_bytes() -> Vec<u8> {
    b"riffdb storage recovery command bundle v1".to_vec()
}

fn plan() -> ExecutablePlanRef {
    let bundle_bytes = contract_bundle_bytes();
    ExecutablePlanRef::new(
        ContractLineage::new("storage-recovery").expect("lineage"),
        ContractVersion::new(1).expect("version"),
        hash_contract_bundle(&bundle_bytes),
        CommandId::new(1).expect("command ID"),
        PlanHash::from_bytes([0x22; 32]),
    )
}

fn contract_bundle() -> StoredContractBundleV1 {
    let plan = plan();
    StoredContractBundleV1::new(
        plan.contract_lineage().clone(),
        plan.contract_version(),
        plan.contract_bundle_hash(),
        contract_bundle_bytes(),
    )
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
    let range = IndexRangeTarget::new(prefix.finish());
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

    let snapshot_request = SnapshotRequest::new(
        plan.clone(),
        vec![target.clone()],
        Vec::new(),
        vec![range.clone()],
    )
    .expect("snapshot request");
    let snapshot = ReadSnapshot::new(
        &snapshot_request,
        None,
        vec![EntityObservation::Absent(target.clone())],
        Vec::new(),
        vec![
            IndexRangeObservation::new(range.clone(), IndexEpochPosition::BeforeFirst, Vec::new())
                .expect("range observation"),
        ],
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
    let admission =
        AdmissionRequestV1::new(candidates.clone(), &context).expect("admission request");
    let intent = riffdb_storage_api::CommitIntent::new(context, evaluated, provenance_id)
        .expect("commit intent");

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
    let index_record = StoredIndexEntryV1::new(
        index_key.clone(),
        DurableKeySchemaBindingV1::from_plan(&plan),
        record(1),
    )
    .expect("stored index entry");
    let index_mutation = IndexEntryMutationV1::Put(index_record);
    let affected_targets =
        AffectedIndexEpochTargets::new(vec![range.clone()]).expect("affected targets");
    let affected_current = AffectedEpochCurrentState::new(
        &affected_targets,
        vec![CurrentRangeObservation::new(
            range.clone(),
            IndexEpochPosition::BeforeFirst,
        )],
    )
    .expect("affected current state");
    let epoch_advance = IndexEpochAdvanceV1::new(
        range.clone(),
        DurableKeySchemaBindingV1::from_plan(&plan),
        IndexEpochPosition::BeforeFirst,
    )
    .expect("epoch advance");
    let upper_bound = command_write_set_upper_bound_v1(
        &intent,
        std::slice::from_ref(&index_mutation),
        std::slice::from_ref(&epoch_advance),
    )
    .expect("canonical encoded upper bound");
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
        admission,
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
    assert_eq!(
        ports
            .admit_or_resolve(fixture.admission.clone())
            .expect("persist pending admission"),
        AdmissionResultV1::Created(fixture.pending.clone())
    );
    let candidate = ports
        .begin_empty_batch()
        .expect("begin command batch")
        .begin_candidate(Box::new(fixture.intent.clone()))
        .expect("begin command candidate");
    let CandidateAdmissionResult::Proceed(candidate) = candidate
        .recheck_admission()
        .expect("recheck pending admission")
    else {
        panic!("fresh pending admission must proceed");
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
        .commit(DurabilityMode::Sync)
        .expect("commit complete command graph");
}

fn assert_precommit_command_state(ports: &RedbOperationalPorts, fixture: &CommandFixture) {
    let AdmissionLookupResultV1::Found(admission) = ports
        .lookup_admission(fixture.candidates.clone())
        .expect("lookup precommit admission")
    else {
        panic!("the separately committed pending admission must survive");
    };
    assert_eq!(
        *admission,
        riffdb_storage_api::StoredAdmissionStateV1::Pending(fixture.pending.clone())
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
        AuthoritativeIndexScanPage::ExactEnd { entries } if entries.is_empty()
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
    let AuthoritativeIndexScanPage::ExactEnd { entries } = index else {
        panic!("one index row must reach exact end");
    };
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
        _ => panic!("unknown closed child mode"),
    };
    let store =
        RedbStore::open_with_test_controller(path, controller).expect("open child database");
    match mode.as_str() {
        "before-initialization-commit" | "after-initialization-commit" => {
            let mut store = store;
            let _ = store.initialize_database(database_id());
        }
        "before-command-batch-commit" | "after-command-batch-commit" => {
            let ports = open_operational(store);
            commit_command_fixture(&ports, &command_fixture());
        }
        _ => unreachable!("controller match rejects unknown modes"),
    }
    panic!("the armed failpoint did not terminate the child");
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
fn crash_before_command_commit_preserves_only_the_pending_admission() {
    let path = TestDatabasePath::new("before-command");
    prepare_command_database(&path.0);
    run_crashing_child("before-command-batch-commit", &path.0);

    let ports = open_operational(RedbStore::open(&path.0).expect("recover precommit crash"));
    assert_precommit_command_state(&ports, &command_fixture());
}

#[test]
fn crash_after_command_commit_preserves_the_complete_reciprocal_graph() {
    let path = TestDatabasePath::new("after-command");
    prepare_command_database(&path.0);
    run_crashing_child("after-command-batch-commit", &path.0);

    let ports = open_operational(RedbStore::open(&path.0).expect("recover postcommit crash"));
    assert_postcommit_command_state(&ports, &command_fixture());
    drop(ports);

    let ports = open_operational(RedbStore::open(&path.0).expect("repeat postcommit recovery"));
    assert_postcommit_command_state(&ports, &command_fixture());
}
