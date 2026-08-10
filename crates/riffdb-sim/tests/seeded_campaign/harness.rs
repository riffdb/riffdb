//! Shared store harness for the seeded campaign: the fixture contract family,
//! plan-driven fixture construction, and fallible bring-up/commit primitives.
//!
//! This generalizes the SIM-C1 recovery-oracle harness
//! (`tests/recovery_oracle.rs`) from three fixed fixtures to fixtures built
//! from a generated [`WorkloadPlan`]: parameterized targets, payload values,
//! value sizes (the `note` field), supersession chains of arbitrary depth,
//! admission shapes, and multi-command batches. Every operation the campaign
//! performs under an armed fault schedule has a fallible (`try_*`) form that
//! reports interruption instead of panicking; assertions that would indicate a
//! genuine store defect (wrong values, structural findings, sequence
//! mismatches) still panic loudly.

use std::num::NonZeroU64;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use riffdb_catalog::{CatalogHistoryOutcome, ValidatedContractBundle, validate_catalog_history};
use riffdb_contract_compiler::compile_contract_source;
use riffdb_sim::{SimBackend, SimDisk, SimJournalMedia};
use riffdb_storage_api::{
    AdmissionRequestV1, AdmissionResultV1, AffectedEntityV1, AffectedEpochCurrentState,
    AffectedIndexEpochTargets, ApplicationCommandTransactionPort, AssignedCommandSequence,
    AtomicCommandRecordSet, AuditPrincipalV1, AuditedAdmissionRepository,
    AuditedAdmissionRequestV1, CandidateAdmissionResult, CandidateCapacityResult,
    CandidateStartResult, CatalogActivationIntentV1, CatalogActivationResult,
    CatalogAdministrationRepository, CommandCandidateAdmission, CommandCandidateAffectedEpochRead,
    CommandCandidateAwaitingCapacity, CommandCandidateAwaitingValidation,
    CommandCandidateCapacityReserved, CommandCandidateSequenceAssigned, CommandCandidateStateRead,
    CommandWriteSetPlanV1, CurrentIndexGenerationObservation, DatabaseInitializationPort,
    DatabaseInitializationResult, DeclaredOutcome, DurabilityMode, DurableKeySchemaBindingV1,
    EmptyCommandBatch, EncodedWriteSetUpperBoundResultV1, EntityMutation, EntityObservation,
    EntityPostImage, EntityTarget, EvaluationBudget, EventIntent, EvidencePageLimit,
    ExecutablePlanRef, ExpectedEntityState, IdempotencyIdentity, IdempotencyKeyDigest,
    IdempotencyLookupCandidatesV1, IndexEntryMutationV1, IndexEpochAdvanceV1, IndexEpochPosition,
    IndexRangePrefixBuilder, IndexRangeTarget, NonEmptyCommandBatch, PartitionIndexTarget,
    PreEvaluationCommitContext, ReadSnapshot, ReadableCapabilityDigestInventory, ReadableDigestKey,
    ReadableIdempotencyDigestInventory, ServiceAuditAppendIntentV1, SnapshotRequest,
    StartupValidationInputs, StoredAdmittedProvenanceClaimsV1, StoredContractBundleV1,
    StoredDurableEventV1, StoredEntityRecordV1, StoredIndexEntryV2, StoredOutcomeV1,
    StoredPendingAdmissionV1, StoredProvenanceRecordV1, StoredReadDependenciesV1,
    StructuralEvidenceCursor, StructuralEvidenceOpen, StructuralEvidencePage,
    StructuralEvidenceSession, StructuralOpenOutcome, command_write_set_upper_bound_v1,
    derive_event_hash_v1,
};
use riffdb_storage_redb::{RedbOperationalPorts, RedbStorageMedia, RedbStore};
use riffdb_testkit::inspection::{
    DurableInspection, DurableInspectionRequest, IndexRangeSelection, inspect_opened_redb,
};
use riffdb_types::{
    ActorId, ActorKind, AggregateTypeId, CanonicalInputHash, CanonicalRecord, CanonicalValue,
    CapabilityId, CommitSequence, DatabaseId, DigestKeyId, EntityKeyBuilder, EntityTypeId,
    EntityVersion, Environment, EventId, EventTypeId, FieldId, IndexEntryKeyBuilder, IndexEpoch,
    IndexId, LogicalTime, OutcomeId, PartitionKeyBuilder, ProvenanceId, RequestId,
    ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetsV1, ServiceIngressKindV1,
    ServiceOperationV1, TenantId, TenantScope, Timestamp, hash_partition_key,
};

use crate::generator::{AdmissionShape, PlannedCommand, WorkloadPlan};

const SIM_DB_PATH: &str = "/sim/campaign.redb";

/// The oracle fixture contract widened with a bounded `note` field so the
/// generator's value-size distribution has a real payload axis (a FIXTURE
/// contract extension — a test asset, never production code).
const CAMPAIGN_CONTRACT: &str = r#"
contract StorageRecovery version 1 {
  entity Row {
    key (id: u64)
    field value: u64
    field note: string<512>
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
    input note: string<512>

    idempotency_key idempotency_key
    create Row(id) as row
      else RowAlreadyExists { id: id }

    set row.value = value
    set row.note = note

    emit RowCreated { id: id, value: value }
    return RowCreatedOutcome { row: row }
  }
}
"#;

/// The one deterministic database identity every campaign store carries.
pub(crate) fn database_id() -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10])
        .expect("valid deterministic database ID")
}

fn uuid_bytes(fill: u8) -> [u8; 16] {
    let mut bytes = [fill; 16];
    bytes[6] = 0x70 | (fill & 0x0f);
    bytes[8] = 0x80 | (fill & 0x3f);
    bytes
}

/// Ten bytes of deterministic identity material from `(seed, ordinal, tag)` —
/// the `from_unix_milliseconds_and_random` random component (ADR-0113 item 5).
///
/// UUIDv7 assembly masks `random[0]` to its low nibble (version) and
/// `random[2]` to its low six bits (variant), so the ordinal — the value that
/// MUST distinguish identities within one campaign — occupies only the fully
/// preserved bytes (`random[1]`, `random[3..10]`). The tag separates the
/// request-id and provenance-id namespaces; the seed's low variant bits vary
/// identities across campaigns without carrying the uniqueness burden.
fn identity_material(seed: u64, ordinal: u64, tag: u8) -> [u8; 10] {
    let ordinal_bytes = ordinal.to_le_bytes();
    let mut bytes = [0_u8; 10];
    bytes[0] = tag & 0x0f;
    bytes[1] = ordinal_bytes[0];
    bytes[2] = u8::try_from(seed & 0x3f).expect("six masked bits");
    bytes[3..10].copy_from_slice(&ordinal_bytes[1..8]);
    bytes
}

/// Opens the store over simulated media. Fallible: an armed schedule can
/// crash the open itself.
pub(crate) fn try_open_simulated(disk: &SimDisk) -> Result<RedbStore, String> {
    RedbStore::open_with_storage_media(
        Path::new(SIM_DB_PATH),
        RedbStorageMedia::new(
            SimBackend::new(disk, SIM_DB_PATH),
            Arc::new(SimJournalMedia::new(disk)),
        ),
    )
    .map_err(|error| format!("open over simulated media: {error:?}"))
}

fn validated_contract_bundle() -> &'static ValidatedContractBundle {
    static BUNDLE: OnceLock<ValidatedContractBundle> = OnceLock::new();
    BUNDLE.get_or_init(|| {
        ValidatedContractBundle::from_compiler_bundle(
            compile_contract_source(CAMPAIGN_CONTRACT).expect("compile campaign contract"),
        )
        .expect("validate campaign bundle")
    })
}

fn plan_ref() -> ExecutablePlanRef {
    let bundle = validated_contract_bundle();
    let command = bundle
        .bundle()
        .commands()
        .first()
        .expect("campaign command");
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

/// Deterministic payload record: the drawn `value` plus, for nonzero lengths,
/// a `note` string of exactly `note_len` bytes derived from the value — the
/// value-size distribution made durable.
fn record(value: u64, note_len: u64) -> CanonicalRecord {
    let mut fields = vec![(
        FieldId::new(1).expect("field ID"),
        CanonicalValue::U64(value),
    )];
    if note_len > 0 {
        let length = usize::try_from(note_len).expect("bounded note length");
        let mut note = String::with_capacity(length);
        for index in 0..length {
            let offset =
                u8::try_from((value.wrapping_add(index as u64)) % 26).expect("bounded letter");
            note.push(char::from(b'a' + offset));
        }
        fields.push((
            FieldId::new(2).expect("field ID"),
            CanonicalValue::string(note).expect("bounded note string"),
        ));
    }
    CanonicalRecord::new(fields).expect("canonical record")
}

fn catalog_principal() -> AuditPrincipalV1 {
    AuditPrincipalV1::new(
        ActorId::new("seeded-campaign-maintainer").expect("catalog principal"),
        ActorKind::Human,
        CapabilityId::from_bytes(uuid_bytes(0x61)).expect("catalog capability"),
        NonZeroU64::MIN,
    )
}

/// One fully-resolved command fixture (the SIM-C1 shape, built from a
/// [`PlannedCommand`] instead of a hand-fixed ordinal).
pub(crate) struct CommandFixture {
    /// Admission shape (drives audit-transition selection and model admits).
    pub shape: AdmissionShape,
    /// Idempotency lookup candidates for the audited admission.
    pub candidates: IdempotencyLookupCandidatesV1,
    /// The durable `Pending` row phase one creates (also the model's admit).
    pub pending: StoredPendingAdmissionV1,
    /// Pre-evaluation commit context.
    pub context: PreEvaluationCommitContext,
    /// The commit intent (fused vacant-terminal or existing-pending).
    pub intent: riffdb_storage_api::CommitIntent,
    /// Affected index-epoch targets for plan validation.
    pub affected_targets: AffectedIndexEpochTargets,
    /// The write-set plan reserved before staging.
    pub write_plan: CommandWriteSetPlanV1,
    /// The atomic record set staged for commit (also the model's apply).
    pub records: AtomicCommandRecordSet,
    /// Entity target (inspection request surface).
    pub target: EntityTarget,
    /// Index range (inspection request surface).
    pub range: IndexRangeTarget,
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

/// The expected index-epoch position after `depth` prior commits on a target.
fn epoch_position(depth: u64) -> IndexEpochPosition {
    if depth == 0 {
        return IndexEpochPosition::BeforeFirst;
    }
    let mut epoch = IndexEpoch::first();
    for _ in 1..depth {
        epoch = epoch.checked_next().expect("bounded epoch chain");
    }
    IndexEpochPosition::Value(epoch)
}

/// Builds every fixture of the plan in order. A superseding command's fixture
/// references the superseded fixture's post-image and epoch depth, so chains
/// of any depth stay consistent with what the store must observe.
pub(crate) fn build_plan_fixtures(seed: u64, plan: &WorkloadPlan) -> Vec<CommandFixture> {
    let mut fixtures: Vec<CommandFixture> = Vec::with_capacity(plan.commands.len());
    for command in &plan.commands {
        let prior = command
            .supersedes
            .map(|index| &fixtures[usize::try_from(index).expect("bounded fixture index")]);
        fixtures.push(build_command_fixture(seed, command, prior));
    }
    fixtures
}

/// The SIM-C1 `build_command_fixture` generalized over a [`PlannedCommand`].
fn build_command_fixture(
    seed: u64,
    planned: &PlannedCommand,
    prior: Option<&CommandFixture>,
) -> CommandFixture {
    assert_eq!(
        planned.supersedes.is_some(),
        prior.is_some(),
        "supersession bookkeeping must hand the prior fixture through"
    );
    let plan = plan_ref();
    let ordinal = planned.ordinal;
    let sequence = CommitSequence::new(ordinal).expect("planned ordinal");
    let payload = planned.value;
    let (target, range) = target_index_and_range(planned.target);
    let entity_key = target.key().clone();
    let index_id = IndexId::new(1).expect("index ID");
    let mut index_key = IndexEntryKeyBuilder::new(index_id);
    index_key.push_u64(10).expect("index component");
    let index_key = index_key.finish(entity_key).expect("index entry key");

    let prior_entity = prior.map(|fixture| fixture.records.entities()[0].post_image().clone());
    let prior_epoch = epoch_position(planned.chain_depth);
    assert_eq!(
        planned.chain_depth > 0,
        prior.is_some(),
        "chain depth and prior fixture must agree"
    );

    let tenant_scope = TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant"));
    let principal = ActorId::new("principal-a").expect("principal");
    let actor = riffdb_types::AdmittedActorContext::new(
        principal.clone(),
        ActorKind::Human,
        tenant_scope.clone(),
        None,
    );
    let mut digest_bytes = [0x5A_u8; 32];
    digest_bytes[..8].copy_from_slice(&ordinal.to_le_bytes());
    digest_bytes[8..16].copy_from_slice(&seed.to_le_bytes());
    let identity = IdempotencyIdentity::new(
        database_id(),
        Environment::new("test").expect("environment"),
        tenant_scope,
        principal,
        plan.contract_lineage().clone(),
        plan.command_id(),
        IdempotencyKeyDigest::from_hmac_bytes(
            DigestKeyId::new(1).expect("digest key"),
            digest_bytes,
        ),
    );
    let request_id = RequestId::from_unix_milliseconds_and_random(
        1_700_000_000_000,
        identity_material(seed, ordinal, 0x01),
    )
    .expect("request ID");
    let provenance_id = ProvenanceId::from_unix_milliseconds_and_random(
        1_700_000_000_000,
        identity_material(seed, ordinal, 0x02),
    )
    .expect("provenance ID");
    let logical_time =
        LogicalTime::new(Timestamp::new(1_700_000_001, 0).expect("logical timestamp"));
    let mut partition = PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate"));
    partition
        .push_u64(6 + planned.target)
        .expect("partition component");
    let partition = partition.finish().expect("partition key");
    let mut input_hash_bytes = [0x42_u8; 32];
    input_hash_bytes[..8].copy_from_slice(&ordinal.to_le_bytes());
    let pending = StoredPendingAdmissionV1::new(
        identity.clone(),
        CanonicalInputHash::from_bytes(input_hash_bytes),
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
    let post_image = EntityPostImage::new(
        target.clone(),
        plan.contract_version(),
        record(payload, planned.note_len),
    )
    .expect("entity post-image");
    let event_intent = EventIntent::new(
        EventTypeId::new(1).expect("event type"),
        record(payload, planned.note_len),
    )
    .expect("event intent");
    let declared_outcome = DeclaredOutcome::new(
        OutcomeId::new(1).expect("outcome ID"),
        record(payload, planned.note_len),
    )
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
    let intent = match planned.shape {
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
        record(payload, planned.note_len),
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
        record(payload, planned.note_len),
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
            panic!("campaign fixture write set must fit the accepted aggregate cap")
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
    let event_payload = record(payload, planned.note_len);
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
        shape: planned.shape,
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

/// The one `Started`/`Succeeded` pair every fixture's command lifecycle uses.
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

/// Phase one of a two-phase admission under a possibly armed schedule.
/// `Ok(())` means the store durably created exactly the fixture's pending row
/// (asserted). A `Resumed` result panics: the campaign only retries an admit
/// after the recovery inspection proved the pending row absent, so resuming
/// an existing row would mean the two-state resolution lied.
pub(crate) fn try_admit_audited(
    ports: &RedbOperationalPorts,
    fixture: &CommandFixture,
) -> Result<(), String> {
    let (started, _) = command_audit_intents(fixture);
    let admission = AdmissionRequestV1::new(fixture.candidates.clone(), &fixture.context)
        .expect("audited admission request");
    let request =
        AuditedAdmissionRequestV1::new(admission, started).expect("audited admission request pair");
    let mut results = ports
        .admit_or_resolve_audited_group(vec![request])
        .map_err(|error| format!("audited admission group: {error:?}"))?;
    assert_eq!(results.len(), 1, "one request admits exactly one result");
    let result = results.pop().expect("one audited admission result");
    match result.admission() {
        AdmissionResultV1::Created(created) => {
            assert_eq!(
                created, &fixture.pending,
                "phase-one admission must durably create the fixture's exact pending row"
            );
            Ok(())
        }
        other => panic!(
            "admit of ordinal {} found unexpected durable admission state \
             (the recovery inspection resolved this step as not-durable): {other:?}",
            fixture.records.commit().commit_sequence().get()
        ),
    }
}

/// Outcome of one batch commit attempt under a possibly armed fault schedule.
pub(crate) enum CommitAttempt {
    /// The whole batch is durable and acknowledged.
    Committed,
    /// A step failed (a scheduled crash mid-attempt); nothing acknowledged.
    /// Carries the refusing stage and detail so a refusal that no crash
    /// explains is diagnosable from the failure output alone.
    Refused {
        /// The typestate stage that refused.
        stage: &'static str,
        /// Debug detail of the refusing result.
        detail: String,
    },
}

macro_rules! refuse {
    ($stage:expr, $detail:expr) => {
        return CommitAttempt::Refused {
            stage: $stage,
            detail: $detail,
        }
    };
}

/// Verifies the mid-chain hard invariants of one candidate: the
/// transaction-current expectation and the assigned sequence. Wrong values
/// under an armed schedule are genuine defects, never interruptions.
macro_rules! stage_candidate_chain {
    ($candidate:expr, $fixture:expr) => {{
        let candidate = match $candidate.recheck_admission() {
            Ok(CandidateAdmissionResult::Proceed(candidate)) => candidate,
            Ok(_) => refuse!(
                "recheck_admission",
                "non-Proceed admission result".to_owned()
            ),
            Err(error) => refuse!("recheck_admission", format!("{error:?}")),
        };
        let (candidate, current) = match candidate.read_transaction_current() {
            Ok(pair) => pair,
            Err(error) => refuse!("read_transaction_current", format!("{error:?}")),
        };
        assert_eq!(
            current.bindings()[0].expected_state(),
            $fixture.records.entities()[0].expected(),
            "transaction-current state must match the fixture's committed expectation"
        );
        let candidate = match candidate
            .plan_validated($fixture.affected_targets.clone())
            .read_affected_epoch_current()
        {
            Ok(candidate) => candidate,
            Err(error) => refuse!("read_affected_epoch_current", format!("{error:?}")),
        };
        let candidate = match candidate.reserve_capacity($fixture.write_plan.clone()) {
            Ok(CandidateCapacityResult::Reserved(candidate)) => candidate,
            Ok(CandidateCapacityResult::BatchFull(_)) => {
                refuse!("reserve_capacity", "BatchFull".to_owned())
            }
            Ok(CandidateCapacityResult::ProvenanceIdCollision(_)) => {
                refuse!("reserve_capacity", "ProvenanceIdCollision".to_owned())
            }
            Err(error) => refuse!("reserve_capacity", format!("{error:?}")),
        };
        let candidate = match candidate.assign_sequence() {
            Ok(candidate) => candidate,
            Err(error) => refuse!("assign_sequence", format!("{error:?}")),
        };
        assert_eq!(
            candidate.assignment().assigned(),
            $fixture.records.commit().commit_sequence(),
            "the store must assign exactly the planned commit sequence"
        );
        match candidate.stage($fixture.records.clone()) {
            Ok(staged) => staged,
            Err(error) => refuse!("stage", format!("{error:?}")),
        }
    }};
}

/// Stages every fixture into one batch and commits atomically (the
/// `storage_recovery_matrix` group idiom made fallible). Any step failing —
/// as it will when a scheduled crash lands mid-attempt — yields `Refused`;
/// value assertions still panic, because a WRONG value under faults is a
/// genuine defect worth a loud stop.
pub(crate) fn try_commit_group(
    ports: &RedbOperationalPorts,
    fixtures: &[&CommandFixture],
) -> CommitAttempt {
    let (first, remaining) = fixtures.split_first().expect("non-empty batch");
    let batch = match ports.begin_empty_batch() {
        Ok(batch) => batch,
        Err(error) => refuse!("begin_empty_batch", format!("{error:?}")),
    };
    let candidate = match batch.begin_candidate(Box::new(first.intent.clone())) {
        Ok(candidate) => candidate,
        Err(error) => refuse!("begin_candidate", format!("{error:?}")),
    };
    let mut staged = stage_candidate_chain!(candidate, first);
    for fixture in remaining {
        let candidate = match staged.begin_candidate(Box::new(fixture.intent.clone())) {
            Ok(CandidateStartResult::Started(candidate)) => candidate,
            Ok(_) => refuse!(
                "begin_candidate",
                "batch refused another candidate (count gate)".to_owned()
            ),
            Err(error) => refuse!("begin_candidate", format!("{error:?}")),
        };
        staged = stage_candidate_chain!(candidate, fixture);
    }
    match staged.commit_with_service_audit_transitions(
        DurabilityMode::Sync,
        fixtures
            .iter()
            .map(|fixture| command_audit_transition(fixture))
            .collect(),
    ) {
        Ok(_) => CommitAttempt::Committed,
        Err(error) => refuse!(
            "commit_with_service_audit_transitions",
            format!("{error:?}")
        ),
    }
}

/// Startup validation inputs shared by every open.
pub(crate) fn startup_inputs() -> StartupValidationInputs {
    let digest_key = DigestKeyId::new(1).expect("digest key ID");
    StartupValidationInputs::new(
        Timestamp::new(1_700_000_000, 0).expect("startup timestamp"),
        ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("capability digest inventory"),
        ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("idempotency digest inventory"),
    )
}

/// Re-proves emptiness or existing identity. Fallible under an armed schedule.
pub(crate) fn try_initialize(
    store: &mut RedbStore,
) -> Result<DatabaseInitializationResult, String> {
    store
        .initialize_database(database_id())
        .map_err(|error| format!("initialize simulated database: {error:?}"))
}

/// The full startup-validation and structural-inspection pass into operational
/// ports. Fallible under an armed schedule; structural findings and an empty
/// evidence stream still panic — a recovered store presenting findings is a
/// reportable defect, not an interruption.
pub(crate) fn try_open_operational(store: RedbStore) -> Result<RedbOperationalPorts, String> {
    let mut session = store
        .begin_structural_evidence(startup_inputs())
        .map_err(|error| format!("begin structural evidence: {error:?}"))?;
    let session_database_id = session.database_id();
    let open_session_id = session.open_session_id();
    let limit = EvidencePageLimit::new(64).expect("page limit");
    let mut cursor = StructuralEvidenceCursor::start(session_database_id, open_session_id);
    let mut pages = 0_u32;
    let structural_end = loop {
        match session
            .read_structural_evidence(cursor, limit)
            .map_err(|error| format!("read structural evidence: {error:?}"))?
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
        .map_err(|error| format!("validate catalog history: {error:?}"))?
        .into_parts();
    assert!(matches!(catalog_outcome, CatalogHistoryOutcome::Ready(_)));
    let outcome = session
        .finish(structural_end, historical_end)
        .map_err(|error| format!("finish structural evidence: {error:?}"))?;
    let opened = match outcome {
        StructuralOpenOutcome::Clean(opened) => opened,
        StructuralOpenOutcome::MigrationRequired(_) => {
            panic!("the campaign fixture must not require index migration")
        }
    };
    assert_eq!(opened.database_id(), database_id());
    let (_, _, _, dormant) = opened.into_parts();
    dormant
        .into_operational_after_catalog_validation()
        .map_err(|error| format!("activate simulated storage fixture: {error:?}"))
}

/// Catalog state resolved by [`try_activate_catalog`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CatalogOutcome {
    /// This call durably activated the campaign bundle.
    Activated,
    /// The exact campaign bundle pointer was already active (an interrupted
    /// earlier activation landed durably — the two-state "present" side).
    AlreadyActive,
}

/// Activates the campaign catalog, accepting both crash-boundary states.
/// Fallible under an armed schedule; any other result panics.
pub(crate) fn try_activate_catalog(
    ports: &mut RedbOperationalPorts,
) -> Result<CatalogOutcome, String> {
    let bundle = contract_bundle();
    let expected = riffdb_storage_api::ActiveCatalogPointerV1::from_bundle(&bundle);
    let result = ports
        .activate_catalog(&CatalogActivationIntentV1::new(
            None,
            bundle.clone(),
            RequestId::from_bytes(uuid_bytes(0x62)).expect("catalog request"),
            catalog_principal(),
            Timestamp::new(1_700_000_000, 0).expect("catalog timestamp"),
            None,
        ))
        .map_err(|error| format!("activate campaign catalog: {error:?}"))?;
    match result {
        CatalogActivationResult::Activated { active, .. } => {
            assert_eq!(
                active, expected,
                "activated pointer must be the campaign bundle"
            );
            Ok(CatalogOutcome::Activated)
        }
        CatalogActivationResult::AlreadyActive { active, .. } => {
            assert_eq!(
                active, expected,
                "surviving pointer must be the campaign bundle"
            );
            Ok(CatalogOutcome::AlreadyActive)
        }
        other => panic!("unexpected catalog activation result: {other:?}"),
    }
}

/// The full inspection request over `fixtures`: every entity target, index
/// range, and admission identity any compared command touches — including the
/// in-flight command's, so its absence is asserted rather than unobserved.
pub(crate) fn inspection_request(fixtures: &[&CommandFixture]) -> DurableInspectionRequest {
    let mut entities: Vec<EntityTarget> = fixtures
        .iter()
        .map(|fixture| fixture.target.clone())
        .collect();
    entities.sort();
    entities.dedup();
    let lineage = plan_ref().contract_lineage().clone();
    let mut ranges: Vec<IndexRangeSelection> = fixtures
        .iter()
        .map(|fixture| IndexRangeSelection::new(fixture.range.clone(), lineage.clone()))
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

/// One full recovery inspection over an already-opened store: the SIM-003
/// precondition (startup validation and structural inspection with zero
/// findings over at least one page) plus the recovered frontier. Fallible
/// under an armed schedule.
pub(crate) fn try_inspect(
    store: RedbStore,
    request: &DurableInspectionRequest,
) -> Result<DurableInspection, String> {
    let inspection = inspect_opened_redb(store, startup_inputs(), request)
        .map_err(|error| format!("recovered-store inspection: {error:?}"))?;
    assert!(
        inspection.structural_findings().is_empty(),
        "a recovered simulated store has no structural findings: {inspection:?}"
    );
    assert!(
        inspection.structural_pages() > 0,
        "structural evidence produced no pages"
    );
    Ok(inspection)
}
