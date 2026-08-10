#![forbid(unsafe_code)]

//! D4 (SIM-B): the first fully simulated `RedbStore` open — a smoke test,
//! not a campaign. The store opens through the hidden media constructor with
//! `SimBackend` (engine) plus `SimJournalMedia` (journal/marker side files),
//! commits a small fixed workload through the storage API in the
//! `storage_recovery_matrix` fixture style (fixed timestamps, fixed
//! entropy), and must recover it across three distinct reopen paths: a
//! clean shutdown, a crash with an all-synced disk (redb's dirty-shutdown
//! repair path — empty torn-decision set, asserted as a precondition), and
//! a mid-commit crash whose torn-decision set is asserted NON-empty so
//! recovery provably resolves seeded keep/drop/prefix-truncate decisions.
//!
//! STORE-LEVEL DETERMINISM BLOCKER (deliberate scope limit): these tests
//! assert semantic outcomes only and MUST NOT assert trace-digest equality
//! across runs. RiffDB store commits touch many redb tables per transaction,
//! and redb 4.1.0 drains `pending_table_updates` from a
//! `std::collections::HashMap` during commit, so the backend write order —
//! and therefore the trace digest and the per-seed torn-write outcomes —
//! varies run to run. Byte-exact store-level replay is blocked until redb
//! orders that drain; the seeded store-level campaign and the
//! `AuthoritativeCommandModel` oracle wiring are SIM-C scope.

use std::num::NonZeroU64;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use riffdb_catalog::{CatalogHistoryOutcome, ValidatedContractBundle, validate_catalog_history};
use riffdb_contract_compiler::compile_contract_source;
use riffdb_sim::{FaultConfig, SimBackend, SimDisk, SimJournalMedia};
use riffdb_storage_api::{
    AffectedEntityV1, AffectedEpochCurrentState, AffectedIndexEpochTargets,
    ApplicationCommandTransactionPort, AssignedCommandSequence, AtomicCommandRecordSet,
    AuditPrincipalV1, AuthoritativePointReader, CandidateAdmissionResult, CandidateCapacityResult,
    CatalogActivationIntentV1, CatalogActivationResult, CatalogAdministrationRepository,
    CommandCandidateAdmission, CommandCandidateAffectedEpochRead, CommandCandidateAwaitingCapacity,
    CommandCandidateAwaitingValidation, CommandCandidateCapacityReserved,
    CommandCandidateSequenceAssigned, CommandCandidateStateRead, CommandWriteSetPlanV1,
    CurrentIndexGenerationObservation, DatabaseInitializationPort, DeclaredOutcome, DurabilityMode,
    DurableKeySchemaBindingV1, EmptyCommandBatch, EncodedWriteSetUpperBoundResultV1,
    EntityMutation, EntityObservation, EntityPostImage, EntityTarget, EvaluationBudget,
    EventIntent, EvidencePageLimit, ExecutablePlanRef, ExpectedEntityState, IdempotencyIdentity,
    IdempotencyKeyDigest, IdempotencyLookupCandidatesV1, IndexEntryMutationV1, IndexEpochAdvanceV1,
    IndexEpochPosition, NonEmptyCommandBatch, PartitionIndexTarget, PreEvaluationCommitContext,
    ReadSnapshot, ReadableCapabilityDigestInventory, ReadableDigestKey,
    ReadableIdempotencyDigestInventory, ServiceAuditAppendIntentV1, SnapshotRequest,
    StartupValidationInputs, StoredAdmittedProvenanceClaimsV1, StoredContractBundleV1,
    StoredDurableEventV1, StoredEntityRecordV1, StoredIndexEntryV2, StoredOutcomeV1,
    StoredPendingAdmissionV1, StoredProvenanceRecordV1, StoredReadDependenciesV1,
    StructuralEvidenceCursor, StructuralEvidenceOpen, StructuralEvidencePage,
    StructuralEvidenceSession, StructuralOpenOutcome, command_write_set_upper_bound_v1,
    derive_event_hash_v1,
};
use riffdb_storage_redb::{RedbOperationalPorts, RedbStorageMedia, RedbStore};
use riffdb_types::{
    ActorId, ActorKind, AggregateTypeId, CanonicalInputHash, CanonicalRecord, CanonicalValue,
    CapabilityId, CommitSequence, DatabaseId, DigestKeyId, EntityKeyBuilder, EntityTypeId,
    EntityVersion, Environment, EventId, EventTypeId, FieldId, IndexEntryKeyBuilder, IndexId,
    LogicalTime, OutcomeId, PartitionKeyBuilder, ProvenanceId, RequestId, ServiceAuditLinkV1,
    ServiceAuditPhaseV1, ServiceAuditTargetsV1, ServiceIngressKindV1, ServiceOperationV1, TenantId,
    TenantScope, Timestamp, hash_partition_key,
};

const SIM_DB_PATH: &str = "/sim/app.redb";

const SMOKE_CONTRACT: &str = r#"
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

fn open_simulated(disk: &SimDisk) -> RedbStore {
    RedbStore::open_with_storage_media(
        Path::new(SIM_DB_PATH),
        RedbStorageMedia::new(
            SimBackend::new(disk, SIM_DB_PATH),
            Arc::new(SimJournalMedia::new(disk)),
        ),
    )
    .expect("open the store over simulated media")
}

fn validated_contract_bundle() -> &'static ValidatedContractBundle {
    static BUNDLE: OnceLock<ValidatedContractBundle> = OnceLock::new();
    BUNDLE.get_or_init(|| {
        ValidatedContractBundle::from_compiler_bundle(
            compile_contract_source(SMOKE_CONTRACT).expect("compile smoke contract"),
        )
        .expect("validate smoke bundle")
    })
}

fn plan() -> ExecutablePlanRef {
    let bundle = validated_contract_bundle();
    let command = bundle.bundle().commands().first().expect("smoke command");
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

fn catalog_principal() -> AuditPrincipalV1 {
    AuditPrincipalV1::new(
        ActorId::new("sim-store-smoke-maintainer").expect("catalog principal"),
        ActorKind::Human,
        CapabilityId::from_bytes(uuid_bytes(0x61)).expect("catalog capability"),
        NonZeroU64::MIN,
    )
}

struct CommandFixture {
    pending: StoredPendingAdmissionV1,
    intent: riffdb_storage_api::CommitIntent,
    affected_targets: AffectedIndexEpochTargets,
    write_plan: CommandWriteSetPlanV1,
    records: AtomicCommandRecordSet,
    target: EntityTarget,
}

/// The `storage_recovery_matrix` command fixture, trimmed to the
/// vacant-terminal admission shape over disjoint entities: ordinal N commits
/// commit sequence N over entity id `6 + N` with value 1.
fn command_fixture_at(ordinal: u64) -> CommandFixture {
    let plan = plan();
    let sequence = CommitSequence::new(ordinal).expect("fixture ordinal");
    let ordinal_u8 = u8::try_from(ordinal % 256).expect("bounded fixture ordinal");
    // Distinct payload per fixture: a store that cross-swapped two entities'
    // payloads (or events, outcomes, or index rows) must fail the read-back
    // asserts.
    let payload = 100 + ordinal;
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
        CanonicalInputHash::from_bytes([0x42_u8.wrapping_add(ordinal_u8); 32]),
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
    let post_image = EntityPostImage::new(target.clone(), plan.contract_version(), record(payload))
        .expect("entity post-image");
    let event_intent = EventIntent::new(EventTypeId::new(1).expect("event type"), record(payload))
        .expect("event intent");
    let declared_outcome =
        DeclaredOutcome::new(OutcomeId::new(1).expect("outcome ID"), record(payload))
            .expect("declared outcome");
    let evaluated = riffdb_storage_api::EvaluatedCommand::new(
        &snapshot,
        vec![EntityMutation::Create(post_image)],
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
        candidates,
        evaluated,
        provenance_id,
    )
    .expect("fused terminal commit intent");

    let stored_entity = StoredEntityRecordV1::new(
        target.clone(),
        EntityVersion::first(),
        plan.contract_version(),
        DurableKeySchemaBindingV1::from_plan(&plan),
        record(payload),
    )
    .expect("stored entity");
    let mutation = riffdb_storage_api::CommittedEntityMutationV1::new(
        ExpectedEntityState::Absent,
        stored_entity,
    )
    .expect("committed entity mutation");
    let index_record = StoredIndexEntryV2::new(
        index_key,
        DurableKeySchemaBindingV1::from_plan(&plan),
        record(payload),
        pending.partition_key().clone(),
    )
    .expect("stored index entry");
    let index_mutation = IndexEntryMutationV1::Put(index_record);
    let generation = PartitionIndexTarget::new(partition.clone(), index_id);
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
            panic!("smoke fixture write set must fit the accepted aggregate cap")
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
    let event_payload = record(payload);
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
        pending,
        intent,
        affected_targets,
        write_plan,
        records,
        target,
    }
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

/// Outcome of one commit attempt under a possibly armed fault schedule.
enum CommitAttempt {
    Committed,
    Refused,
}

/// Fallible commit: any step failing (as it will when a scheduled crash
/// lands mid-attempt) yields `Refused` instead of panicking. Value
/// assertions still panic — under injected faults the store fails closed
/// with errors; a WRONG value would be a genuine defect worth a loud stop.
fn try_commit_command_fixture(
    ports: &RedbOperationalPorts,
    fixture: &CommandFixture,
) -> CommitAttempt {
    let Ok(batch) = ports.begin_empty_batch() else {
        return CommitAttempt::Refused;
    };
    let Ok(candidate) = batch.begin_candidate(Box::new(fixture.intent.clone())) else {
        return CommitAttempt::Refused;
    };
    let Ok(CandidateAdmissionResult::Proceed(candidate)) = candidate.recheck_admission() else {
        return CommitAttempt::Refused;
    };
    let Ok((candidate, current)) = candidate.read_transaction_current() else {
        return CommitAttempt::Refused;
    };
    assert_eq!(
        current.bindings()[0].expected_state(),
        fixture.records.entities()[0].expected(),
        "transaction-current state must match the fixture's committed expectation"
    );
    let Ok(candidate) = candidate
        .plan_validated(fixture.affected_targets.clone())
        .read_affected_epoch_current()
    else {
        return CommitAttempt::Refused;
    };
    let Ok(CandidateCapacityResult::Reserved(candidate)) =
        candidate.reserve_capacity(fixture.write_plan.clone())
    else {
        return CommitAttempt::Refused;
    };
    let Ok(candidate) = candidate.assign_sequence() else {
        return CommitAttempt::Refused;
    };
    assert_eq!(
        candidate.assignment().assigned(),
        fixture.records.commit().commit_sequence()
    );
    let Ok(staged) = candidate.stage(fixture.records.clone()) else {
        return CommitAttempt::Refused;
    };
    match staged.commit_with_service_audit_transitions(
        DurabilityMode::Sync,
        vec![command_audit_transition(fixture)],
    ) {
        Ok(_) => CommitAttempt::Committed,
        Err(_) => CommitAttempt::Refused,
    }
}

fn commit_command_fixture(ports: &RedbOperationalPorts, fixture: &CommandFixture) {
    match try_commit_command_fixture(ports, fixture) {
        CommitAttempt::Committed => {}
        CommitAttempt::Refused => panic!("a quiet-schedule commit must succeed"),
    }
}

fn open_operational(store: RedbStore) -> RedbOperationalPorts {
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
    let session_database_id = session.database_id();
    let open_session_id = session.open_session_id();
    let limit = EvidencePageLimit::new(64).expect("page limit");
    let mut cursor = StructuralEvidenceCursor::start(session_database_id, open_session_id);
    let mut pages = 0_u32;
    let structural_end = loop {
        match session
            .read_structural_evidence(cursor, limit)
            .expect("read structural evidence")
        {
            StructuralEvidencePage::Page { findings, next, .. } => {
                pages += 1;
                assert!(
                    findings.is_empty(),
                    "a recovered simulated store has no findings: {findings:?}"
                );
                cursor = next;
            }
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };
    // The zero-findings assertion must stand on its own reach: at least one
    // evidence page must actually have been produced and inspected.
    assert!(pages > 0, "structural evidence produced no pages");
    let (catalog_outcome, historical_end) = validate_catalog_history(&mut session)
        .expect("catalog validates the complete historical stream")
        .into_parts();
    assert!(matches!(catalog_outcome, CatalogHistoryOutcome::Ready(_)));
    // NOTE: `finish` also writes a validated-prefix checkpoint at the end
    // of every successful pass, so each open in these tests leaves a fresh
    // checkpoint for the next reopen. Keep that in mind before reordering
    // fixture opens.
    let outcome = session
        .finish(structural_end, historical_end)
        .expect("finish structural evidence");
    let opened = match outcome {
        StructuralOpenOutcome::Clean(opened) => opened,
        StructuralOpenOutcome::MigrationRequired(_) => {
            panic!("the smoke fixture must not require index migration")
        }
    };
    assert_eq!(opened.database_id(), database_id());
    let (_, _, _, dormant) = opened.into_parts();
    dormant
        .into_operational_after_catalog_validation()
        .expect("activate simulated storage fixture")
}

fn activate_catalog(ports: &mut RedbOperationalPorts) {
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
        .expect("activate smoke catalog");
    assert!(
        matches!(
            &result,
            CatalogActivationResult::Activated { active, .. }
                if *active == riffdb_storage_api::ActiveCatalogPointerV1::from_bundle(&bundle)
        ),
        "unexpected catalog activation result: {result:?}"
    );
}

/// Field-exact read-back (derived `Eq` on the decoded records — NOT an
/// encoded-byte comparison; an encode/decode normalization that round-trips
/// to the same struct is invisible here): entity, terminal outcome, commit
/// record, durable event, and provenance record against the values the
/// fixture constructed before writing.
fn assert_committed_row(ports: &RedbOperationalPorts, fixture: &CommandFixture) {
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
        ports
            .read_commit(fixture.records.commit().commit_sequence())
            .expect("read commit"),
        Some(fixture.records.commit().clone())
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
            .read_provenance(fixture.records.provenance().provenance_id())
            .expect("read provenance"),
        Some(fixture.records.provenance().clone())
    );
}

/// Prepares a fresh simulated store: initialize, structural open, catalog.
fn prepare_simulated(disk: &SimDisk) -> RedbOperationalPorts {
    let mut store = open_simulated(disk);
    store
        .initialize_database(database_id())
        .expect("initialize simulated database");
    let mut ports = open_operational(store);
    activate_catalog(&mut ports);
    ports
}

#[test]
fn simulated_store_survives_a_clean_shutdown_and_simulated_reopen() {
    let disk = SimDisk::new(FaultConfig::quiet(0x51B_0001));
    let ports = prepare_simulated(&disk);
    let one = command_fixture_at(1);
    let two = command_fixture_at(2);
    commit_command_fixture(&ports, &one);
    commit_command_fixture(&ports, &two);
    assert_committed_row(&ports, &one);
    assert_committed_row(&ports, &two);
    drop(ports);

    // Reopen simulated: startup validation runs inside `open_operational`
    // (zero structural findings over at least one inspected page) and both
    // rows read back field-exactly.
    let reopened = open_operational(open_simulated(&disk));
    assert_committed_row(&reopened, &one);
    assert_committed_row(&reopened, &two);
    drop(reopened);
}

#[test]
fn simulated_store_reopens_through_dirty_shutdown_repair_after_a_crash() {
    let disk = SimDisk::new(FaultConfig::quiet(0x51B_0002));
    let ports = prepare_simulated(&disk);
    let one = command_fixture_at(1);
    let two = command_fixture_at(2);
    commit_command_fixture(&ports, &one);
    commit_command_fixture(&ports, &two);
    // The rows are durably present BEFORE the crash, so "both acknowledged"
    // does not rest solely on commit() having returned Ok.
    assert_committed_row(&ports, &one);
    assert_committed_row(&ports, &two);

    // Crash after the acknowledged commits, before the store drops. Under
    // the quiet schedule every acknowledged mutation is already folded into
    // the durable image, so the torn-decision set at recovery is EMPTY —
    // asserted below as this test's stated precondition (review M1), not
    // implied coverage. What this test covers is the unclean-shutdown path:
    // dropping the crashed store makes redb's close() fail, leaving
    // recovery_required set in the durable header, so the reopen goes
    // through redb's dirty-shutdown repair rather than the clean path of
    // the test above. Torn-write resolution over a NON-empty decision set
    // is covered by
    // `simulated_store_resolves_torn_unsynced_state_from_a_mid_commit_crash`.
    assert_eq!(
        disk.unsynced_mutation_count(),
        0,
        "quiet-schedule Sync commits leave nothing unsynced; if this reds, \
         the dirty-shutdown claim of this test no longer holds"
    );
    disk.crash();
    drop(ports);
    disk.recover_after_crash();
    let counters = disk.counters();
    assert_eq!(
        counters.torn_kept + counters.torn_dropped + counters.torn_truncated,
        0,
        "this recovery must take zero torn decisions (all-synced crash)"
    );

    // The reopened store must pass the full startup validation after redb's
    // dirty-shutdown repair and land exactly on the acknowledged state.
    let recovered = open_operational(open_simulated(&disk));
    assert_committed_row(&recovered, &one);
    assert_committed_row(&recovered, &two);
    drop(recovered);
}

/// M1: torn-write resolution with a provably NON-empty decision set. A
/// scheduled crash is swept across EVERY countdown window (SIM-C1 removed
/// the first-tearing-window break; the full sweep is ~1.2s) so that each
/// window that crashes the store is recovered and checked — the engine's
/// page writes and the journal worker's frame writes both burst between
/// syncs. For every window that connects: unsynced torn-candidates existed at the crash, recovery
/// took exactly one seeded keep/drop/prefix-truncate decision per candidate,
/// and the reopened store passes startup validation on a consistent
/// acknowledged state — rows 1 and 2 exactly, the interrupted command 3
/// either fully present (its durability fence completed before the crash)
/// or fully absent, never partial.
///
/// A sweep is required because crash placement relative to the store's
/// internal operation stream is not seed-stable at store level (the journal
/// worker interleaves; see the module comment's determinism blocker), so no
/// single window can be pinned. If NO swept window can catch the store with
/// unsynced state, the final panic fires — per review M1 that outcome is a
/// reportable finding, never silence.
#[test]
fn simulated_store_resolves_torn_unsynced_state_from_a_mid_commit_crash() {
    let mut torn_window = None;
    for window in (1..=240_u64).step_by(3) {
        let disk = SimDisk::new(FaultConfig::quiet(0x51B_0003));
        let ports = prepare_simulated(&disk);
        let one = command_fixture_at(1);
        let two = command_fixture_at(2);
        commit_command_fixture(&ports, &one);
        commit_command_fixture(&ports, &two);
        assert_committed_row(&ports, &one);
        assert_committed_row(&ports, &two);

        // Arm the schedule and attempt a third command: the crash lands
        // `window` countable disk operations into the attempt (or later,
        // during the drop's shutdown writes, for large windows).
        disk.set_crash_after_operations(Some((window, window + 1)));
        let three = command_fixture_at(3);
        let attempt = try_commit_command_fixture(&ports, &three);
        drop(ports);
        if !disk.is_crashed() {
            // The window exceeded the whole attempt's operation count.
            continue;
        }
        let unsynced = disk.unsynced_mutation_count();
        disk.recover_after_crash();
        // Disarm before reopening: recovery redraws the countdown, and an
        // armed schedule would crash the reopen's own recovery reads.
        disk.set_crash_after_operations(None);
        let counters = disk.counters();
        let torn = counters.torn_kept + counters.torn_dropped + counters.torn_truncated;
        assert_eq!(
            torn, unsynced,
            "every unsynced mutation at the crash faces exactly one seeded \
             decision (window {window})"
        );

        let recovered = open_operational(open_simulated(&disk));
        assert_committed_row(&recovered, &one);
        assert_committed_row(&recovered, &two);
        match recovered
            .read_entity(&three.target)
            .expect("read the interrupted command's entity")
        {
            // The interrupted command became durable before the crash: its
            // complete graph must be present.
            Some(_) => assert_committed_row(&recovered, &three),
            None => assert!(
                recovered
                    .read_stored_outcome(three.pending.identity())
                    .expect("read the interrupted command's outcome")
                    .is_none(),
                "an unrecovered third command must be absent in full \
                 (window {window})"
            ),
        }
        drop(recovered);

        if torn > 0 && torn_window.is_none() {
            torn_window = Some((window, torn, matches!(attempt, CommitAttempt::Committed)));
        }
    }
    let Some((_window, torn, _acknowledged)) = torn_window else {
        panic!(
            "no crash window in the swept range caught the store with \
             unsynced state mid-commit; if the store truly cannot be caught \
             unsynced, that is a reportable finding (review M1) — do not \
             widen the sweep without understanding why"
        );
    };
    assert!(
        torn > 0,
        "the connecting window must have taken torn decisions"
    );
}
