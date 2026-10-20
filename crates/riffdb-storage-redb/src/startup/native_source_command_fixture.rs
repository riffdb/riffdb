//! Test-only extraction of the existing compiled storage-recovery fixture.
//! Application mutations still pass through the actual admitted coordinator.
use crate::RedbOperationalPorts;
use riffdb_catalog::ValidatedContractBundle;
use riffdb_contract_compiler::compile_contract_source;
use riffdb_storage_api::*;
use riffdb_types::*;
use std::num::NonZeroU64;
use std::sync::OnceLock;
#[path = "native_source_command_fixture_build.rs"]
mod build;
#[path = "native_source_projection_fixture.rs"]
mod projection_fixture;

pub(super) fn source_projection_bundle() -> &'static ValidatedContractBundle {
    projection_fixture::bundle()
}

pub(super) fn activate_source_projection(
    ports: &mut RedbOperationalPorts,
) -> Result<ActiveCatalogPointerV1, StorageError> {
    projection_fixture::activate(ports)
}

pub(super) fn append_source_projection_journaled(
    ports: &RedbOperationalPorts,
    ordinal: u64,
) -> Result<(), StorageError> {
    let fixture = build::build_runtime_command_fixture(projection_fixture::bundle(), ordinal);
    commit_journaled_fixture(ports, &fixture)
}

#[derive(Clone)]
struct CommandFixture {
    pending: StoredPendingAdmissionV1,
    intent: riffdb_storage_api::CommitIntent,
    affected_targets: AffectedIndexEpochTargets,
    write_plan: CommandWriteSetPlanV1,
    records: AtomicCommandRecordSet,
    index_key: IndexEntryKey,
}
pub(super) fn append(ports: &RedbOperationalPorts, ordinal: u64) -> Result<(), StorageError> {
    let fixture = command_fixture_at(ordinal);
    try_commit_command_fixture_with_audit(ports, &fixture, command_audit_transition(&fixture))
}

// Execute the genuine compiled contract through the existing runtime fixture,
// then use the same admitted Immediate audited coordinator. Returning the
// actual runtime's expected commit avoids warming any postcommit Current read.
pub(super) fn append_runtime_immediate(
    ports: &RedbOperationalPorts,
    ordinal: u64,
) -> Result<StoredCommitRecordV1, StorageError> {
    let fixture = build::build_runtime_command_fixture(projection_fixture::bundle(), ordinal);
    assert_eq!(fixture.records.events().len(), 1, "actual runtime event");
    let commit = fixture.records.commit().clone();
    try_commit_command_fixture_with_audit(ports, &fixture, command_audit_transition(&fixture))?;
    Ok(commit)
}

const STORAGE_RECOVERY_CONTRACT: &str = r#"
contract StorageRecovery version 1 {
  entity Row {
    key (id: u64)
    field value: u64
    index ByValue(value)
  }

  event RowCreated {
    partition_by (id)
    policy_anchor current Row(id: id)
    id: u64
    value: u64
  }

  aggregate Rows {
    root Row
    partition_by id
    conflict_key (id)
  }

  row policy RowAccess on Row {
    allow read when true
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

fn catalog_principal() -> AuditPrincipalV1 {
    AuditPrincipalV1::new(
        ActorId::new("storage-recovery-maintainer").expect("catalog principal"),
        ActorKind::Human,
        CapabilityId::from_bytes(uuid_bytes(0x61)).expect("catalog capability"),
        NonZeroU64::MIN,
    )
}

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
    riffdb_storage_api::CommandServiceAuditTransitionV1::started_and_terminal(started, terminal)
        .expect("fused command audit lifecycle")
}

fn command_fixture_at(ordinal: u64) -> CommandFixture {
    build::build_command_fixture(ordinal, ordinal, None, false)
}

fn database_id() -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10])
        .expect("valid deterministic database ID")
}

fn ordinal_uuid_bytes(fill: u8, ordinal: u64) -> [u8; 16] {
    if ordinal <= u64::from(u8::MAX) {
        return uuid_bytes(fill.wrapping_add(ordinal as u8));
    }
    let mut bytes = uuid_bytes(fill);
    bytes[9..].copy_from_slice(&ordinal.to_be_bytes()[1..]);
    bytes
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

fn record(value: u64) -> CanonicalRecord {
    CanonicalRecord::new(vec![(
        FieldId::new(1).expect("field ID"),
        CanonicalValue::U64(value),
    )])
    .expect("canonical record")
}

fn target_and_index_at(
    ordinal: u64,
    indexed_value: u64,
) -> (EntityTarget, IndexEntryKey, IndexRangeTarget) {
    let entity_type_id = EntityTypeId::new(1).expect("entity type ID");
    let mut entity_key = EntityKeyBuilder::new(entity_type_id);
    entity_key
        .push_u64(6 + ordinal)
        .expect("entity key component");
    let entity_key = entity_key.finish().expect("entity key");
    let target = EntityTarget::new(entity_type_id, entity_key.clone()).expect("entity target");

    let index_id = IndexId::new(1).expect("index ID");
    let mut index_key = IndexEntryKeyBuilder::new(index_id);
    index_key.push_u64(indexed_value).expect("index component");
    let index_key = index_key.finish(entity_key).expect("index entry key");
    let mut prefix = riffdb_storage_api::IndexRangePrefixBuilder::new(index_id);
    prefix.push_u64(indexed_value).expect("range component");
    let mut partition = PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate"));
    partition
        .push_u64(6 + ordinal)
        .expect("partition component");
    let range = IndexRangeTarget::new(partition.finish().expect("partition key"), prefix.finish());
    (target, index_key, range)
}

fn try_commit_command_fixture_with_audit(
    ports: &RedbOperationalPorts,
    fixture: &CommandFixture,
    transition: riffdb_storage_api::CommandServiceAuditTransitionV1,
) -> Result<(), riffdb_storage_api::StorageError> {
    let candidate = ports
        .begin_empty_batch()?
        .begin_candidate(Box::new(fixture.intent.clone()))?;
    let CandidateAdmissionResult::Proceed(candidate) = candidate.recheck_admission()? else {
        panic!("the fixture's expected admission state must proceed");
    };
    let (candidate, current) = candidate.read_transaction_current()?;
    assert_eq!(
        current.bindings()[0].expected_state(),
        fixture.records.entities()[0].expected(),
        "transaction-current state must match the fixture's committed expectation"
    );
    let candidate = candidate
        .plan_validated(fixture.affected_targets.clone())
        .read_affected_epoch_current()?;
    let CandidateCapacityResult::Reserved(candidate) =
        candidate.reserve_capacity(fixture.write_plan.clone())?
    else {
        panic!("small recovery fixture must reserve");
    };
    let candidate = candidate.assign_sequence()?;
    assert_eq!(
        candidate.assignment().assigned(),
        fixture.records.commit().commit_sequence()
    );
    candidate
        .stage(fixture.records.clone())?
        .commit_with_service_audit_transitions(DurabilityMode::Sync, vec![transition])
        .map(|_| ())
}

fn uuid_bytes(fill: u8) -> [u8; 16] {
    let mut bytes = [fill; 16];
    bytes[6] = 0x70 | (fill & 0x0f);
    bytes[8] = 0x80 | (fill & 0x3f);
    bytes
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

/// Real journaled equivalent of append: private audited command, then epoch fence.
pub(super) fn append_journaled(
    ports: &RedbOperationalPorts,
    ordinal: u64,
) -> Result<(), StorageError> {
    let fixture = command_fixture_at(ordinal);
    commit_journaled_fixture(ports, &fixture)
}

fn commit_journaled_fixture(
    ports: &RedbOperationalPorts,
    fixture: &CommandFixture,
) -> Result<(), StorageError> {
    let epoch = ports.begin_deferred_command_epoch()?;
    let candidate = DeferredCommandEpoch::begin_empty_batch(epoch)?
        .begin_candidate(Box::new(fixture.intent.clone()))?;
    let CandidateAdmissionResult::Proceed(candidate) = candidate.recheck_admission()? else {
        panic!("fresh vacant deferred admission must proceed");
    };
    let (candidate, current) = candidate.read_transaction_current()?;
    assert_eq!(
        current.bindings()[0].expected_state(),
        fixture.records.entities()[0].expected(),
        "transaction-current state must match the fixture's committed expectation"
    );
    let candidate = candidate
        .plan_validated(fixture.affected_targets.clone())
        .read_affected_epoch_current()?;
    let CandidateCapacityResult::Reserved(candidate) =
        candidate.reserve_capacity(fixture.write_plan.clone())?
    else {
        panic!("small deferred fixture must reserve");
    };
    let candidate = candidate.assign_sequence()?;
    assert_eq!(
        candidate.assignment().assigned(),
        fixture.records.commit().commit_sequence()
    );
    let epoch = candidate
        .stage(fixture.records.clone())?
        .apply_unpublished_with_service_audit_transitions(
            DurabilityMode::Sync,
            vec![command_audit_transition(fixture)],
        )?;
    DeferredCommandEpoch::fence(epoch).map(|_| ())
}

pub(super) fn activate(
    ports: &mut RedbOperationalPorts,
) -> Result<ActiveCatalogPointerV1, StorageError> {
    let bundle = validated_contract_bundle()
        .to_stored()
        .map_err(|_| crate::error::storage_error(StorageErrorKind::InvariantViolation))?;
    let result = ports.activate_catalog(&CatalogActivationIntentV1::new(
        None,
        bundle,
        RequestId::from_bytes(uuid_bytes(0x62)).unwrap(),
        catalog_principal(),
        Timestamp::new(1_700_000_000, 0).unwrap(),
        None,
    ))?;
    match result {
        CatalogActivationResult::Activated { active, .. } => Ok(active),
        _ => Err(crate::error::storage_error(
            StorageErrorKind::InvariantViolation,
        )),
    }
}
