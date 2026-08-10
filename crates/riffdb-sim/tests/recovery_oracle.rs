#![forbid(unsafe_code)]

//! SIM-C1 (ADR-0113 Phase 1 item 4, SPEC SIM-003): the recovery oracle.
//!
//! The real `RedbStore` runs over simulated media (`SimBackend` +
//! `SimJournalMedia` on one `SimDisk`), a driver applies every acknowledged
//! command to the independent `AuthoritativeCommandModel` in lockstep —
//! snapshotting a clone at every acknowledged durable commit, the driver-side
//! `Vec<(CommitSequence, model)>` idiom — and after EVERY simulated recovery
//! the reopened engine must (a) pass the full startup validation and
//! structural inspection pass (zero findings over at least one inspected
//! page), and (b) equal the model at the recovered durable frontier, compared
//! through the testkit's durable-inspection accessor surface
//! (`verify_model_against_inspection`: both directions, coverage-checked,
//! every representational normalization documented at the comparison).
//!
//! The workload mixes admission shapes: a two-phase admission (durable
//! `Pending` row consumed by a later commit), fused vacant-terminal commits,
//! and an ADR-0083 supersession chain (two commit sequences whose entity
//! post-images occupy one physical key, advancing the same index epoch).
//!
//! Crash-boundary acceptance (the SIM-A two-state discipline at store
//! level): the recovered frontier must be ≥ the last acknowledged commit's
//! sequence (acknowledged durability) and ≤ the last attempted; if the
//! interrupted command's effects are present they must equal the in-flight
//! model snapshot exactly — never a partial graph.
//!
//! STORE-LEVEL DETERMINISM BLOCKER (deliberate scope limit): these tests
//! assert semantic outcomes only and MUST NOT assert trace-digest equality
//! across runs, for the two reasons the smoke test pins: (1) redb 4.1.0
//! drains `pending_table_updates` from a `std::collections::HashMap` during
//! commit, so the backend write order varies run to run; (2) the journal
//! worker thread's operations interleave with the writer's under the shared
//! seeded-RNG lock, so fault-schedule draws are not seed-stable at store
//! level. Crash placement therefore cannot be pinned to one window — the
//! torn tests sweep every window instead (no first-tearing-window break).
//! The seeded workload GENERATOR and the open-ended campaign are SIM-C2
//! scope; this harness uses fixed fixtures over the swept windows.

use std::num::NonZeroU64;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use riffdb_catalog::{CatalogHistoryOutcome, ValidatedContractBundle, validate_catalog_history};
use riffdb_contract_compiler::compile_contract_source;
use riffdb_sim::{FaultConfig, SimBackend, SimDisk, SimJournalMedia};
use riffdb_storage_api::{
    AdmissionRequestV1, AdmissionResultV1, AffectedEntityV1, AffectedEpochCurrentState,
    AffectedIndexEpochTargets, ApplicationCommandTransactionPort, AssignedCommandSequence,
    AtomicCommandRecordSet, AuditPrincipalV1, AuditedAdmissionRepository,
    AuditedAdmissionRequestV1, CandidateAdmissionResult, CandidateCapacityResult,
    CatalogActivationIntentV1, CatalogActivationResult, CatalogAdministrationRepository,
    CommandCandidateAdmission, CommandCandidateAffectedEpochRead, CommandCandidateAwaitingCapacity,
    CommandCandidateAwaitingValidation, CommandCandidateCapacityReserved,
    CommandCandidateSequenceAssigned, CommandCandidateStateRead, CommandWriteSetPlanV1,
    CurrentIndexGenerationObservation, DatabaseInitializationPort, DeclaredOutcome, DurabilityMode,
    DurableKeySchemaBindingV1, EmptyCommandBatch, EncodedWriteSetUpperBoundResultV1,
    EntityMutation, EntityObservation, EntityPostImage, EntityTarget, EvaluationBudget,
    EventIntent, EvidencePageLimit, ExecutablePlanRef, ExpectedEntityState, IdempotencyIdentity,
    IdempotencyKeyDigest, IdempotencyLookupCandidatesV1, IndexEntryMutationV1, IndexEpochAdvanceV1,
    IndexEpochPosition, IndexRangePrefixBuilder, IndexRangeTarget, NonEmptyCommandBatch,
    PartitionIndexTarget, PreEvaluationCommitContext, ReadSnapshot,
    ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
    ServiceAuditAppendIntentV1, SnapshotRequest, StartupValidationInputs,
    StoredAdmittedProvenanceClaimsV1, StoredContractBundleV1, StoredDurableEventV1,
    StoredEntityRecordV1, StoredIndexEntryV2, StoredOutcomeV1, StoredPendingAdmissionV1,
    StoredProvenanceRecordV1, StoredReadDependenciesV1, StructuralEvidenceCursor,
    StructuralEvidenceOpen, StructuralEvidencePage, StructuralEvidenceSession,
    StructuralOpenOutcome, command_write_set_upper_bound_v1, derive_event_hash_v1,
};
use riffdb_storage_redb::{RedbOperationalPorts, RedbStorageMedia, RedbStore};
use riffdb_testkit::inspection::{
    DurableInspection, DurableInspectionRequest, inspect_opened_redb,
};
use riffdb_testkit::model::{AuthoritativeCommandModel, verify_model_against_inspection};
use riffdb_types::{
    ActorId, ActorKind, AggregateTypeId, CanonicalInputHash, CanonicalRecord, CanonicalValue,
    CapabilityId, CommitSequence, DatabaseId, DigestKeyId, EntityKeyBuilder, EntityTypeId,
    EntityVersion, Environment, EventId, EventTypeId, FieldId, FrontierPosition,
    IndexEntryKeyBuilder, IndexEpoch, IndexId, LogicalTime, OutcomeId, PartitionKeyBuilder,
    ProvenanceId, RequestId, ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetsV1,
    ServiceIngressKindV1, ServiceOperationV1, TenantId, TenantScope, Timestamp, hash_partition_key,
};

const SIM_DB_PATH: &str = "/sim/oracle.redb";

const ORACLE_CONTRACT: &str = r#"
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
            compile_contract_source(ORACLE_CONTRACT).expect("compile oracle contract"),
        )
        .expect("validate oracle bundle")
    })
}

fn plan() -> ExecutablePlanRef {
    let bundle = validated_contract_bundle();
    let command = bundle.bundle().commands().first().expect("oracle command");
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
        ActorId::new("recovery-oracle-maintainer").expect("catalog principal"),
        ActorKind::Human,
        CapabilityId::from_bytes(uuid_bytes(0x61)).expect("catalog capability"),
        NonZeroU64::MIN,
    )
}

/// Which durable admission state the committing writer expects to find (the
/// `storage_recovery_matrix` distinction: the fused shape never writes a
/// `Pending` row; the two-phase shape leaves one behind for the commit to
/// consume).
#[derive(Clone, Copy, Eq, PartialEq)]
enum AdmissionShape {
    VacantTerminal,
    ExistingPending,
}

struct CommandFixture {
    shape: AdmissionShape,
    candidates: IdempotencyLookupCandidatesV1,
    pending: StoredPendingAdmissionV1,
    context: PreEvaluationCommitContext,
    intent: riffdb_storage_api::CommitIntent,
    affected_targets: AffectedIndexEpochTargets,
    write_plan: CommandWriteSetPlanV1,
    records: AtomicCommandRecordSet,
    target: EntityTarget,
    range: IndexRangeTarget,
}

fn target_index_and_range(target_ordinal: u64) -> (EntityTarget, IndexRangeTarget) {
    let entity_type_id = EntityTypeId::new(1).expect("entity type ID");
    let mut entity_key = EntityKeyBuilder::new(entity_type_id);
    entity_key
        .push_u64(6 + target_ordinal)
        .expect("entity key component");
    let entity_key = entity_key.finish().expect("entity key");
    let target = EntityTarget::new(entity_type_id, entity_key).expect("entity target");
    let mut prefix = IndexRangePrefixBuilder::new(IndexId::new(1).expect("index ID"));
    prefix.push_u64(10).expect("range component");
    let mut partition = PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate"));
    partition
        .push_u64(6 + target_ordinal)
        .expect("partition component");
    let range = IndexRangeTarget::new(partition.finish().expect("partition key"), prefix.finish());
    (target, range)
}

/// The `storage_recovery_matrix` command fixture adapted for the oracle:
/// ordinal N commits sequence N; `prior` makes it the ADR-0083 supersession
/// shape over the prior's entity; the admission shape selects fused
/// vacant-terminal or two-phase existing-pending commit intents. Payloads are
/// distinct per (ordinal, supersession) so a cross-swapped row fails the
/// comparison.
fn build_command_fixture(
    ordinal: u64,
    target_ordinal: u64,
    prior: Option<&CommandFixture>,
    shape: AdmissionShape,
) -> CommandFixture {
    let plan = plan();
    let sequence = CommitSequence::new(ordinal).expect("fixture ordinal");
    let ordinal_u8 = u8::try_from(ordinal % 256).expect("bounded fixture ordinal");
    let payload = if prior.is_some() {
        150 + ordinal
    } else {
        100 + ordinal
    };
    let (target, range) = target_index_and_range(target_ordinal);
    let entity_key = target.key().clone();
    let index_id = IndexId::new(1).expect("index ID");
    let mut index_key = IndexEntryKeyBuilder::new(index_id);
    index_key.push_u64(10).expect("index component");
    let index_key = index_key.finish(entity_key).expect("index entry key");

    let prior_entity = prior.map(|fixture| fixture.records.entities()[0].post_image().clone());
    let prior_epoch = prior.map_or(IndexEpochPosition::BeforeFirst, |_| {
        IndexEpochPosition::Value(IndexEpoch::first())
    });

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
        .push_u64(6 + target_ordinal)
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
        vec![prior_entity.clone().map_or_else(
            || EntityObservation::Absent(target.clone()),
            EntityObservation::Present,
        )],
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
    let entity_mutation = prior_entity.as_ref().map_or_else(
        || EntityMutation::Create(post_image.clone()),
        |entity| EntityMutation::Replace {
            expected_version: entity.entity_version(),
            post_image: post_image.clone(),
        },
    );
    let evaluated = riffdb_storage_api::EvaluatedCommand::new(
        &snapshot,
        vec![entity_mutation],
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
    let intent = match shape {
        AdmissionShape::VacantTerminal => {
            riffdb_storage_api::CommitIntent::new_for_vacant_terminal_admission(
                context.clone(),
                candidates.clone(),
                evaluated,
                provenance_id,
            )
            .expect("fused terminal commit intent")
        }
        AdmissionShape::ExistingPending => {
            riffdb_storage_api::CommitIntent::new(context.clone(), evaluated, provenance_id)
                .expect("existing-pending commit intent")
        }
    };

    let stored_entity = StoredEntityRecordV1::new(
        target.clone(),
        prior_entity
            .as_ref()
            .map_or_else(EntityVersion::first, |entity| {
                entity
                    .entity_version()
                    .checked_next()
                    .expect("superseding entity version")
            }),
        plan.contract_version(),
        DurableKeySchemaBindingV1::from_plan(&plan),
        record(payload),
    )
    .expect("stored entity");
    let mutation = riffdb_storage_api::CommittedEntityMutationV1::new(
        prior_entity
            .as_ref()
            .map_or(ExpectedEntityState::Absent, |entity| {
                ExpectedEntityState::Present(entity.entity_version())
            }),
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
            prior_epoch,
        )],
    )
    .expect("affected current state");
    let epoch_advance = IndexEpochAdvanceV1::new(
        generation,
        DurableKeySchemaBindingV1::from_plan(&plan),
        prior_epoch,
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
            panic!("oracle fixture write set must fit the accepted aggregate cap")
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
        shape,
        candidates,
        pending,
        context,
        intent,
        affected_targets,
        write_plan,
        records,
        target,
        range,
    }
}

/// The one `Started`/`Succeeded` pair every fixture's command lifecycle uses;
/// both phases of a two-phase admission present the same common fields.
fn command_audit_intents(
    fixture: &CommandFixture,
) -> (ServiceAuditAppendIntentV1, ServiceAuditAppendIntentV1) {
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
    (started, terminal)
}

fn command_audit_transition(
    fixture: &CommandFixture,
) -> riffdb_storage_api::CommandServiceAuditTransitionV1 {
    let (started, terminal) = command_audit_intents(fixture);
    match fixture.shape {
        AdmissionShape::VacantTerminal => {
            riffdb_storage_api::CommandServiceAuditTransitionV1::started_and_terminal(
                started, terminal,
            )
            .expect("fused command audit lifecycle")
        }
        // The `Started` row is already durable from the audited admission.
        AdmissionShape::ExistingPending => {
            riffdb_storage_api::CommandServiceAuditTransitionV1::terminal_only(terminal)
                .expect("terminal-only command audit transition")
        }
    }
}

/// Phase one of a two-phase admission: one durable `Pending` row and its
/// physical `Started` audit row, written atomically by the audited-admission
/// port. Asserts the store created exactly the fixture's pending admission,
/// so the model's `admit_pending` mirrors the store field-exactly.
fn admit_audited_command(ports: &RedbOperationalPorts, fixture: &CommandFixture) {
    let (started, _) = command_audit_intents(fixture);
    let admission = AdmissionRequestV1::new(fixture.candidates.clone(), &fixture.context)
        .expect("audited admission request");
    let request =
        AuditedAdmissionRequestV1::new(admission, started).expect("audited admission request pair");
    let mut results = ports
        .admit_or_resolve_audited_group(vec![request])
        .expect("audited admission group");
    assert_eq!(results.len(), 1, "one request admits exactly one result");
    let result = results.pop().expect("one audited admission result");
    assert!(
        matches!(
            result.admission(),
            AdmissionResultV1::Created(created) if *created == fixture.pending
        ),
        "phase-one admission must durably create the fixture's exact pending row"
    );
}

/// Outcome of one commit attempt under a possibly armed fault schedule.
enum CommitAttempt {
    Committed,
    Refused,
}

/// Fallible commit: any step failing (as it will when a scheduled crash lands
/// mid-attempt) yields `Refused` instead of panicking. Value assertions still
/// panic — under injected faults the store fails closed with errors; a WRONG
/// value would be a genuine defect worth a loud stop.
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

fn startup_inputs() -> StartupValidationInputs {
    let digest_key = DigestKeyId::new(1).expect("digest key ID");
    StartupValidationInputs::new(
        Timestamp::new(1_700_000_000, 0).expect("startup timestamp"),
        ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("capability digest inventory"),
        ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("idempotency digest inventory"),
    )
}

fn open_operational(store: RedbStore) -> RedbOperationalPorts {
    let mut session = store
        .begin_structural_evidence(startup_inputs())
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
    assert!(pages > 0, "structural evidence produced no pages");
    let (catalog_outcome, historical_end) = validate_catalog_history(&mut session)
        .expect("catalog validates the complete historical stream")
        .into_parts();
    assert!(matches!(catalog_outcome, CatalogHistoryOutcome::Ready(_)));
    let outcome = session
        .finish(structural_end, historical_end)
        .expect("finish structural evidence");
    let opened = match outcome {
        StructuralOpenOutcome::Clean(opened) => opened,
        StructuralOpenOutcome::MigrationRequired(_) => {
            panic!("the oracle fixture must not require index migration")
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
        .expect("activate oracle catalog");
    assert!(
        matches!(
            &result,
            CatalogActivationResult::Activated { active, .. }
                if *active == riffdb_storage_api::ActiveCatalogPointerV1::from_bundle(&bundle)
        ),
        "unexpected catalog activation result: {result:?}"
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

/// The fixed mixed-shape workload: cmd1 is a two-phase admission, cmd2 a
/// fused vacant-terminal create, cmd3 a vacant-terminal ADR-0083 supersession
/// of cmd2's entity (same physical key, new bytes, epoch first→second).
fn oracle_fixtures() -> [CommandFixture; 3] {
    let one = build_command_fixture(1, 1, None, AdmissionShape::ExistingPending);
    let two = build_command_fixture(2, 2, None, AdmissionShape::VacantTerminal);
    let three = build_command_fixture(3, 2, Some(&two), AdmissionShape::VacantTerminal);
    [one, two, three]
}

/// Commits the fixed workload while driving the model in lockstep and
/// snapshotting a clone at every acknowledged durable commit (the driver-side
/// `Vec<(CommitSequence, model)>` idiom named by ADR-0113).
fn commit_workload_with_model(
    ports: &RedbOperationalPorts,
    fixtures: &[CommandFixture],
) -> (
    AuthoritativeCommandModel,
    Vec<(CommitSequence, AuthoritativeCommandModel)>,
) {
    let mut model = AuthoritativeCommandModel::new();
    let mut snapshots = Vec::new();
    for fixture in fixtures {
        if fixture.shape == AdmissionShape::ExistingPending {
            // Phase one is separately durable and acknowledged: mirror it in
            // the lockstep model at its own acknowledgement point.
            admit_audited_command(ports, fixture);
            model
                .admit_pending(fixture.pending.clone())
                .expect("model admits the two-phase pending");
        }
        commit_command_fixture(ports, fixture);
        if fixture.shape == AdmissionShape::VacantTerminal {
            // The fused shape admits and terminalizes in one durable
            // transition; the model's uniform two-step mirrors it at the
            // single acknowledgement point.
            model
                .admit_pending(fixture.pending.clone())
                .expect("model admits the fused pending");
        }
        model
            .apply_command(&fixture.records)
            .expect("model applies the acknowledged command");
        snapshots.push((fixture.records.commit().commit_sequence(), model.clone()));
    }
    // Non-vacuity: every acknowledged commit must have changed model state —
    // identical consecutive snapshots would make frontier selection
    // meaningless.
    for pair in snapshots.windows(2) {
        assert!(
            pair[0].1.first_divergence_from(&pair[1].1).is_some(),
            "consecutive model snapshots must diverge"
        );
    }
    (model, snapshots)
}

/// The full inspection request for the oracle workload: every entity target,
/// index range, and admission identity any compared command touches —
/// including the interrupted command's, so its absence is asserted rather
/// than unobserved.
fn oracle_request(fixtures: &[&CommandFixture]) -> DurableInspectionRequest {
    let mut entities: Vec<EntityTarget> = fixtures
        .iter()
        .map(|fixture| fixture.target.clone())
        .collect();
    entities.sort();
    entities.dedup();
    let mut ranges: Vec<IndexRangeTarget> = fixtures
        .iter()
        .map(|fixture| fixture.range.clone())
        .collect();
    ranges.sort();
    ranges.dedup();
    let mut identities: Vec<IdempotencyIdentity> = fixtures
        .iter()
        .map(|fixture| fixture.pending.identity().clone())
        .collect();
    identities.sort_by_key(|identity| identity.storage_key().expect("identity storage key"));
    identities.dedup_by_key(|identity| identity.storage_key().expect("identity storage key"));
    DurableInspectionRequest::new(entities, Vec::new())
        .expect("entity targets")
        .with_index_ranges(ranges)
        .expect("index ranges")
        .with_admissions(identities)
        .expect("admission identities")
}

/// One full recovery inspection: the SIM-003 precondition (startup validation
/// and structural inspection with zero findings over ≥1 page) plus the
/// recovered frontier.
fn inspect_recovered(disk: &SimDisk, request: &DurableInspectionRequest) -> DurableInspection {
    let inspection = inspect_opened_redb(open_simulated(disk), startup_inputs(), request)
        .expect("recovered store passes the full startup validation pass");
    assert!(
        inspection.structural_findings().is_empty(),
        "a recovered simulated store has no structural findings: {inspection:?}"
    );
    assert!(
        inspection.structural_pages() > 0,
        "structural evidence produced no pages"
    );
    inspection
}

fn frontier_value(frontier: FrontierPosition) -> u64 {
    match frontier {
        FrontierPosition::BeforeFirst => 0,
        FrontierPosition::AppliedThrough(sequence) => sequence.get(),
    }
}

/// Asserts the model↔store agreement covered the exact expected surface for
/// `commits` applied commands over the oracle workload (two entities before
/// the interrupted command, three after; one index row per live entity key;
/// one range epoch observation per declared range).
fn assert_agreement_counts(
    agreement: &riffdb_testkit::model::StoreAgreement,
    commits: u64,
    entities: usize,
    index_entries: usize,
    ranges: usize,
    admissions: usize,
    context: &str,
) {
    let commit_count = usize::try_from(commits).expect("bounded commit count");
    assert_eq!(agreement.commits(), commit_count, "commits ({context})");
    assert_eq!(agreement.outcomes(), commit_count, "outcomes ({context})");
    assert_eq!(agreement.events(), commit_count, "events ({context})");
    assert_eq!(
        agreement.provenance(),
        commit_count,
        "provenance ({context})"
    );
    assert_eq!(
        agreement.outbox_intents(),
        commit_count,
        "outbox intents ({context})"
    );
    assert_eq!(agreement.entities(), entities, "entities ({context})");
    assert_eq!(
        agreement.index_entries(),
        index_entries,
        "index entries ({context})"
    );
    assert_eq!(agreement.index_epochs(), ranges, "index epochs ({context})");
    assert_eq!(agreement.admissions(), admissions, "admissions ({context})");
}

/// SIM-003 base case: a clean shutdown is also a simulated recovery — the
/// reopened engine passes the full validation pass and equals the model at
/// the recovered frontier, selected from the per-commit snapshot vector.
#[test]
fn recovered_clean_shutdown_state_equals_the_model_at_the_frontier() {
    let disk = SimDisk::new(FaultConfig::quiet(0x51C_0001));
    let ports = prepare_simulated(&disk);
    let fixtures = oracle_fixtures();
    let (model, snapshots) = commit_workload_with_model(&ports, &fixtures);
    drop(ports);

    let request = oracle_request(&fixtures.iter().collect::<Vec<_>>());
    let inspection = inspect_recovered(&disk, &request);
    let frontier = inspection.application_frontier();
    assert_eq!(frontier_value(frontier), 3, "clean shutdown loses nothing");

    // Select the snapshot at the recovered frontier — for a clean shutdown
    // that is the final snapshot, equal to the live model.
    let (_, selected) = snapshots
        .iter()
        .find(|(sequence, _)| FrontierPosition::AppliedThrough(*sequence) == frontier)
        .expect("a snapshot exists at the recovered frontier");
    assert!(
        selected.first_divergence_from(&model).is_none(),
        "the frontier snapshot is the final model state"
    );
    let agreement = verify_model_against_inspection(selected, &inspection)
        .unwrap_or_else(|divergence| panic!("clean-shutdown oracle divergence: {divergence}"));
    assert_agreement_counts(&agreement, 3, 2, 2, 2, 3, "clean shutdown");
}

/// SIM-003 over redb's dirty-shutdown repair: a crash on an all-synced disk
/// (empty torn-decision set, asserted) recovers through repair and must equal
/// the model at the recovered frontier.
#[test]
fn recovered_dirty_shutdown_state_equals_the_model_at_the_frontier() {
    let disk = SimDisk::new(FaultConfig::quiet(0x51C_0002));
    let ports = prepare_simulated(&disk);
    let fixtures = oracle_fixtures();
    let (model, _snapshots) = commit_workload_with_model(&ports, &fixtures);

    assert_eq!(
        disk.unsynced_mutation_count(),
        0,
        "quiet-schedule Sync commits leave nothing unsynced"
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

    let request = oracle_request(&fixtures.iter().collect::<Vec<_>>());
    let inspection = inspect_recovered(&disk, &request);
    assert_eq!(frontier_value(inspection.application_frontier()), 3);
    let agreement = verify_model_against_inspection(&model, &inspection)
        .unwrap_or_else(|divergence| panic!("dirty-shutdown oracle divergence: {divergence}"));
    assert_agreement_counts(&agreement, 3, 2, 2, 2, 3, "dirty shutdown");
}

/// One swept mid-commit crash arm: outcome bookkeeping for the final
/// both-boundary assertion.
struct SweepOutcome {
    torn_windows: u32,
    crashed_windows: u32,
    saw_interrupted_absent: bool,
    saw_interrupted_present: bool,
}

/// Sweeps a scheduled crash across every countdown window (the SIM-B torn
/// mechanism WITHOUT the first-tearing-window break — every window that
/// crashes is recovered and oracle-checked). `armed_shape` selects whether
/// the interrupted command is fused vacant-terminal or the second phase of an
/// already-acknowledged two-phase admission.
fn sweep_crash_windows(seed: u64, armed_shape: AdmissionShape) -> SweepOutcome {
    let mut outcome = SweepOutcome {
        torn_windows: 0,
        crashed_windows: 0,
        saw_interrupted_absent: false,
        saw_interrupted_present: false,
    };
    for window in (1..=240_u64).step_by(3) {
        let disk = SimDisk::new(FaultConfig::quiet(seed));
        let ports = prepare_simulated(&disk);
        let fixtures = oracle_fixtures();
        let (mut baseline, _snapshots) = commit_workload_with_model(&ports, &fixtures);
        let four = build_command_fixture(4, 4, None, armed_shape);

        if armed_shape == AdmissionShape::ExistingPending {
            // Phase one is acknowledged durable BEFORE the crash arms: the
            // baseline the store must land on at frontier 3 includes the
            // pending admission row.
            admit_audited_command(&ports, &four);
            assert_eq!(
                disk.unsynced_mutation_count(),
                0,
                "an acknowledged audited admission is durably synced"
            );
            baseline
                .admit_pending(four.pending.clone())
                .expect("model admits the armed two-phase pending");
        }

        // The in-flight snapshot: the complete effects of the interrupted
        // command. The two-state acceptance at the crash boundary is
        // baseline XOR in-flight — never a partial graph.
        let in_flight = {
            let mut next = baseline.clone();
            if armed_shape == AdmissionShape::VacantTerminal {
                next.admit_pending(four.pending.clone())
                    .expect("model admits the armed fused pending");
            }
            next.apply_command(&four.records)
                .expect("model applies the armed command");
            next
        };
        assert!(
            baseline.first_divergence_from(&in_flight).is_some(),
            "the two acceptance states must actually differ"
        );

        disk.set_crash_after_operations(Some((window, window + 1)));
        let attempt = try_commit_command_fixture(&ports, &four);
        drop(ports);
        if !disk.is_crashed() {
            // The window exceeded the whole attempt's operation count.
            continue;
        }
        outcome.crashed_windows += 1;
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
        if torn > 0 {
            outcome.torn_windows += 1;
        }

        let all = [&fixtures[0], &fixtures[1], &fixtures[2], &four];
        let request = oracle_request(&all);
        let inspection = inspect_recovered(&disk, &request);
        let frontier = frontier_value(inspection.application_frontier());

        // Acknowledged durability: everything acknowledged must survive;
        // nothing past the attempt can exist.
        let acknowledged = match attempt {
            CommitAttempt::Committed => 4,
            CommitAttempt::Refused => 3,
        };
        assert!(
            (acknowledged..=4).contains(&frontier),
            "recovered frontier {frontier} outside [{acknowledged}, 4] \
             (window {window})"
        );

        if frontier == 4 {
            // The interrupted command's durability fence completed: its
            // COMPLETE effect graph must be present — the in-flight state.
            outcome.saw_interrupted_present = true;
            let agreement = verify_model_against_inspection(&in_flight, &inspection)
                .unwrap_or_else(|divergence| {
                    panic!(
                        "torn-crash oracle divergence at window {window} \
                         (seed {seed:#x}, interrupted command present): {divergence}"
                    )
                });
            assert_agreement_counts(&agreement, 4, 3, 3, 3, 4, "interrupted present");
        } else {
            // The interrupted command must be absent IN FULL — the baseline
            // state, which for the two-phase arm includes the acknowledged
            // pending admission row.
            outcome.saw_interrupted_absent = true;
            let agreement = verify_model_against_inspection(&baseline, &inspection).unwrap_or_else(
                |divergence| {
                    panic!(
                        "torn-crash oracle divergence at window {window} \
                         (seed {seed:#x}, interrupted command absent): {divergence}"
                    )
                },
            );
            let admissions = match armed_shape {
                AdmissionShape::VacantTerminal => 3,
                AdmissionShape::ExistingPending => 4,
            };
            assert_agreement_counts(&agreement, 3, 2, 2, 3, admissions, "interrupted absent");
        }
    }
    outcome
}

/// SIM-003 at the torn boundary, fused vacant-terminal interrupted command:
/// EVERY crash window in the sweep is recovered and oracle-checked (the
/// first-tearing-window break is deliberately absent), and at least one
/// window per seed must actually tear — a sweep that never connects is a
/// reportable finding, never silence.
#[test]
fn every_torn_crash_window_recovers_to_the_model_at_the_recovered_frontier() {
    let mut saw_absent = false;
    let mut saw_present = false;
    for seed in [0x51C_0003_u64, 0x51C_0004, 0x51C_0005] {
        let outcome = sweep_crash_windows(seed, AdmissionShape::VacantTerminal);
        assert!(
            outcome.crashed_windows > 0,
            "no swept window crashed at all (seed {seed:#x})"
        );
        assert!(
            outcome.torn_windows > 0,
            "no crash window caught the store with unsynced state mid-commit \
             (seed {seed:#x}); if the store truly cannot be caught unsynced, \
             that is a reportable finding — do not widen the sweep without \
             understanding why"
        );
        saw_absent |= outcome.saw_interrupted_absent;
        saw_present |= outcome.saw_interrupted_present;
    }
    // Both acceptance states must be exercised across the sweep, or the
    // two-state discipline was never actually tested from both sides.
    assert!(
        saw_absent,
        "no swept window recovered with the interrupted command absent"
    );
    assert!(
        saw_present,
        "no swept window recovered with the interrupted command present"
    );
}

/// SIM-003 at the torn boundary, two-phase interrupted command: phase one is
/// acknowledged durable before the crash arms, so frontier 3 must recover the
/// pending admission row exactly and frontier 4 the complete applied graph —
/// the admission family is exercised at the crash boundary itself.
#[test]
fn every_two_phase_crash_window_recovers_pending_or_applied_exactly() {
    let mut saw_absent = false;
    let mut saw_present = false;
    for seed in [0x51C_0006_u64, 0x51C_0007] {
        let outcome = sweep_crash_windows(seed, AdmissionShape::ExistingPending);
        assert!(
            outcome.crashed_windows > 0,
            "no swept window crashed at all (seed {seed:#x})"
        );
        assert!(
            outcome.torn_windows > 0,
            "no crash window caught the store with unsynced state mid-commit \
             (seed {seed:#x}); reportable finding — do not widen the sweep \
             without understanding why"
        );
        saw_absent |= outcome.saw_interrupted_absent;
        saw_present |= outcome.saw_interrupted_present;
    }
    assert!(
        saw_absent,
        "no swept window recovered onto the pending-only baseline"
    );
    assert!(
        saw_present,
        "no swept window recovered with the two-phase command applied"
    );
}
