use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

use riffdb_contract_compiler::compile_contract_source;
use riffdb_types::{
    ActorId, ActorKind, AdministrationSequence, AdmittedActorContext, AggregateTypeId, ApprovalId,
    Audience, CanonicalInputHash, CanonicalRecord, CanonicalValue, CapabilityId,
    CapabilityTokenDigest, CommandId, CommitSequence, ContractBundleHash, ContractLineage,
    ContractVersion, DatabaseId, DigestKeyId, EntityKeyBuilder, EntityTypeId, EntityVersion,
    Environment, EventId, EventTypeId, FieldId, IndexEntryKeyBuilder, IndexEpoch, IndexId,
    LogicalTime, OutcomeId, PartitionKey, PartitionKeyBuilder, PlanHash, ProjectionApplyHash,
    ProjectionApplyKey, ProjectionGeneration, ProjectionId, ProvenanceId, RequestId,
    ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetV1, ServiceAuditTargetsV1,
    ServiceIngressKindV1, ServiceOperationV1, TenantId, TenantScope, Timestamp, hash_partition_key,
};

use crate::{
    ActiveCatalogPointerV1, AffectedEntityV1, AffectedEpochCurrentState, AffectedIndexEpochTargets,
    AssignedCommandSequence, AtomicCommandRecordSet, AuditPrincipalV1,
    CapabilityAdministrationOperationV1, CapabilityBootstrapMarkerV1, CapabilityGrantV1,
    CapabilityPermissionKindV1, CapabilityPermissionV1, CapabilityPermissionsV1,
    CapabilityRequestedRecordV1, CapabilityTokenLookupV1, CheckedProjectionSchema,
    CommandWriteSetPlanV1, CommitIntent, CommittedEntityMutationV1, DeclaredOutcome,
    DurabilityMode, DurableKeySchemaBindingV1, EntityFieldVisibilityV1, EntityMutation,
    EntityObservation, EntityPostImage, EntityTarget, EvaluatedCommand, EvaluationBudget,
    ExecutablePlanRef, ExpectedEntityState, IdempotencyKeyDigest, IndexRangePrefixBuilder,
    OutboxDestinationIdV1, OutboxRetryMetadataV1, OutboxSafeErrorV1, PartitionScopeV1,
    PreEvaluationCommitContext, ReadSnapshot, ScopedPartitionV1, SnapshotRequest,
    StoredAdmittedProvenanceClaimsV1, StoredCapabilityAdministrationV1, StoredCapabilityRecordV1,
    StoredCatalogAdministrationV1, StoredCommitRecordV1, StoredContractBundleV1,
    StoredDurableEventV1, StoredEntityRecordV1, StoredIndexEntryV1, StoredIndexEpochV1,
    StoredOutboxIntentV1, StoredOutboxStatusV1, StoredOutcomeV1, StoredPendingAdmissionV1,
    StoredProjectionApplyV1, StoredProjectionControlV1, StoredProjectionStateV1,
    StoredProvenanceRecordV1, StoredReadDependenciesV1, StoredServiceAuditRecordV1,
};

use super::super::command_write_set_upper_bound_v1;

pub(super) fn uuid_v7(fill: u8) -> [u8; 16] {
    let mut bytes = [fill; 16];
    bytes[6] = 0x70 | (fill & 0x0f);
    bytes[8] = 0x80 | (fill & 0x3f);
    bytes
}

pub(super) fn database_id() -> DatabaseId {
    DatabaseId::from_bytes(uuid_v7(0x11)).expect("database ID")
}

pub(super) fn capability_id() -> CapabilityId {
    CapabilityId::from_bytes(uuid_v7(0x12)).expect("capability ID")
}

pub(super) fn request_id() -> RequestId {
    RequestId::from_bytes(uuid_v7(0x13)).expect("request ID")
}

pub(super) fn provenance_id() -> ProvenanceId {
    ProvenanceId::from_bytes(uuid_v7(0x14)).expect("provenance ID")
}

pub(super) fn lineage() -> ContractLineage {
    ContractLineage::new("codec-sample").expect("lineage")
}

pub(super) fn plan() -> ExecutablePlanRef {
    ExecutablePlanRef::new(
        lineage(),
        ContractVersion::new(1).expect("contract version"),
        ContractBundleHash::from_bytes([0x21; 32]),
        CommandId::first(),
        PlanHash::from_bytes([0x22; 32]),
    )
}

pub(super) fn actor() -> AdmittedActorContext {
    AdmittedActorContext::new(
        ActorId::new("codec-principal").expect("principal"),
        ActorKind::Human,
        TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant")),
        None,
    )
}

pub(super) fn audit_principal() -> AuditPrincipalV1 {
    AuditPrincipalV1::new(
        actor().principal_id().clone(),
        ActorKind::Human,
        capability_id(),
        NonZeroU64::MIN,
    )
}

pub(super) fn canonical_record(byte: u8) -> CanonicalRecord {
    CanonicalRecord::new(vec![
        (FieldId::first(), CanonicalValue::U64(u64::from(byte))),
        (
            FieldId::new(2).expect("field two"),
            CanonicalValue::bytes(vec![byte, byte.wrapping_add(1)]).expect("bytes"),
        ),
    ])
    .expect("canonical record")
}

pub(super) fn entity_target() -> EntityTarget {
    let entity_type = EntityTypeId::first();
    let mut key = EntityKeyBuilder::new(entity_type);
    key.push_u64(7).expect("entity key component");
    EntityTarget::new(entity_type, key.finish().expect("entity key")).expect("entity target")
}

pub(super) fn partition_key() -> PartitionKey {
    let mut key = PartitionKeyBuilder::new(AggregateTypeId::first());
    key.push_str("partition-a").expect("partition component");
    key.finish().expect("partition key")
}

pub(super) fn identity() -> crate::IdempotencyIdentity {
    let plan = plan();
    let actor = actor();
    crate::IdempotencyIdentity::new(
        database_id(),
        Environment::new("test").expect("environment"),
        actor.tenant_scope().clone(),
        actor.principal_id().clone(),
        plan.contract_lineage().clone(),
        plan.command_id(),
        IdempotencyKeyDigest::from_hmac_bytes(
            DigestKeyId::new(1).expect("digest key ID"),
            [0x31; 32],
        ),
    )
}

pub(super) fn pending() -> StoredPendingAdmissionV1 {
    StoredPendingAdmissionV1::new(
        identity(),
        CanonicalInputHash::from_bytes([0x32; 32]),
        request_id(),
        plan(),
        LogicalTime::new(Timestamp::new(42, 7).expect("logical time")),
        actor(),
        partition_key(),
        StoredAdmittedProvenanceClaimsV1::new(
            None,
            None,
            None,
            Some(ApprovalId::new("approval-a").expect("approval")),
        )
        .expect("claims"),
    )
    .expect("pending")
}

pub(super) fn commit_intent() -> CommitIntent {
    let plan = plan();
    let target = entity_target();
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
    .expect("snapshot");
    let outcome =
        DeclaredOutcome::new(OutcomeId::first(), canonical_record(0x41)).expect("declared outcome");
    let evaluated = EvaluatedCommand::new(
        &snapshot,
        vec![EntityMutation::Create(
            EntityPostImage::new(
                target.clone(),
                plan.contract_version(),
                canonical_record(0x42),
            )
            .expect("entity post image"),
        )],
        vec![
            crate::EventIntent::new(EventTypeId::first(), canonical_record(0x43))
                .expect("event intent"),
        ],
        outcome.clone(),
        EvaluationBudget::v1(),
    )
    .expect("evaluated command");
    let pending = pending();
    let partition_hash = hash_partition_key(pending.partition_key().as_bytes());
    let context = PreEvaluationCommitContext::new(pending.clone(), partition_hash, Vec::new())
        .expect("pre-evaluation context");
    CommitIntent::new(context, evaluated, provenance_id()).expect("commit intent")
}

pub(super) fn atomic_record_set() -> AtomicCommandRecordSet {
    let intent = commit_intent();
    let plan = intent.evaluated().plan().clone();
    let target = intent.evaluated().mutations()[0]
        .post_image()
        .target()
        .clone();
    let outcome = intent.evaluated().outcome().clone();
    let pending = intent.pending().clone();
    let partition_hash = intent.partition_hash();
    let entity = StoredEntityRecordV1::new(
        target,
        EntityVersion::first(),
        plan.contract_version(),
        DurableKeySchemaBindingV1::from_plan(&plan),
        canonical_record(0x42),
    )
    .expect("entity record");
    let mutations = vec![
        CommittedEntityMutationV1::new(ExpectedEntityState::Absent, entity)
            .expect("committed mutation"),
    ];
    let sequence = CommitSequence::first();
    let event_id = EventId::new(sequence, 0);
    let event_hash =
        crate::derive_event_hash_v1(event_id, EventTypeId::first(), &canonical_record(0x43))
            .expect("event hash");
    let events = vec![
        StoredDurableEventV1::new(
            event_id,
            EventTypeId::first(),
            canonical_record(0x43),
            event_hash,
        )
        .expect("durable event"),
    ];
    let event_ids = vec![event_id];
    let stored_outcome = StoredOutcomeV1::new(
        identity(),
        sequence,
        request_id(),
        plan.clone(),
        CanonicalInputHash::from_bytes([0x32; 32]),
        actor(),
        pending.logical_time(),
        partition_hash,
        Vec::new(),
        outcome.clone(),
        pending.provenance_claims().clone(),
        provenance_id(),
        DurabilityMode::Memory,
    )
    .expect("stored outcome");
    let affected = mutations
        .iter()
        .map(|value| AffectedEntityV1::from_record(value.post_image()))
        .collect();
    let provenance = StoredProvenanceRecordV1::new(
        provenance_id(),
        sequence,
        identity(),
        request_id(),
        plan.clone(),
        CanonicalInputHash::from_bytes([0x32; 32]),
        actor(),
        pending.logical_time(),
        partition_hash,
        Vec::new(),
        outcome.outcome_id(),
        affected,
        event_ids.clone(),
        pending.provenance_claims().clone(),
    )
    .expect("provenance");
    let read_dependencies =
        StoredReadDependenciesV1::from_live(intent.evaluated().read_dependencies())
            .expect("stored read dependencies");
    let commit = StoredCommitRecordV1::new(
        sequence,
        request_id(),
        plan,
        CanonicalInputHash::from_bytes([0x32; 32]),
        actor(),
        pending.logical_time(),
        partition_hash,
        Vec::new(),
        read_dependencies,
        mutations.clone(),
        events.clone(),
        outcome,
        provenance_id(),
        event_ids,
        DurabilityMode::Memory,
    )
    .expect("commit");
    let outbox_intents = events
        .iter()
        .cloned()
        .map(StoredOutboxIntentV1::new)
        .collect();
    let affected_targets = AffectedIndexEpochTargets::new(Vec::new()).expect("affected targets");
    let affected_current =
        AffectedEpochCurrentState::new(&affected_targets, Vec::new()).expect("affected state");
    let encoded_upper_bound =
        command_write_set_upper_bound_v1(&intent, &[], &[]).expect("encoded upper bound");
    let write_plan = CommandWriteSetPlanV1::new(
        &intent,
        affected_targets,
        affected_current,
        Vec::new(),
        Vec::new(),
        encoded_upper_bound,
    )
    .expect("write plan");
    AtomicCommandRecordSet::new(
        AssignedCommandSequence::from_assigned(sequence),
        mutations,
        write_plan,
        stored_outcome,
        events,
        outbox_intents,
        provenance,
        commit,
    )
    .expect("atomic record set")
}

pub(super) fn index_records() -> (StoredIndexEntryV1, StoredIndexEpochV1) {
    let plan = plan();
    let index_id = IndexId::first();
    let mut index_key = IndexEntryKeyBuilder::new(index_id);
    index_key.push_str("group-a").expect("index component");
    let index_key = index_key
        .finish(entity_target().key().clone())
        .expect("index key");
    let entry = StoredIndexEntryV1::new(
        index_key,
        DurableKeySchemaBindingV1::from_plan(&plan),
        canonical_record(0x44),
    )
    .expect("index entry");
    let mut prefix = IndexRangePrefixBuilder::new(index_id);
    prefix.push_str("group-a").expect("prefix component");
    let prefix = prefix.finish();
    let epoch = StoredIndexEpochV1::new(
        crate::StructurallyDecodedIndexRangePrefixV1::from_live(&prefix),
        DurableKeySchemaBindingV1::from_plan(&plan),
        IndexEpoch::first(),
    );
    (entry, epoch)
}

pub(super) fn catalog_records() -> (
    StoredContractBundleV1,
    ActiveCatalogPointerV1,
    StoredCatalogAdministrationV1,
) {
    let bundle = StoredContractBundleV1::new(
        lineage(),
        ContractVersion::new(1).expect("contract version"),
        ContractBundleHash::from_bytes([0x51; 32]),
        b"compiled-bundle-v1".to_vec(),
    )
    .expect("bundle");
    let active = ActiveCatalogPointerV1::from_bundle(&bundle);
    let administration = StoredCatalogAdministrationV1::from_stored_parts(
        AdministrationSequence::first(),
        request_id(),
        Timestamp::new(50, 5).expect("timestamp"),
        audit_principal(),
        None,
        active.clone(),
        Some(ApprovalId::new("catalog-approval").expect("approval")),
    );
    (bundle, active, administration)
}

pub(super) fn capability_records() -> (
    StoredCapabilityRecordV1,
    CapabilityTokenLookupV1,
    CapabilityBootstrapMarkerV1,
    StoredCapabilityAdministrationV1,
) {
    let mut permission_values = vec![
        CapabilityPermissionV1::ExplainCommand(lineage(), CommandId::first()),
        CapabilityPermissionV1::InvokeCommand(lineage(), CommandId::first()),
        CapabilityPermissionV1::ReadEntity(lineage(), EntityTypeId::first()),
        CapabilityPermissionV1::ScanIndex(lineage(), IndexId::first()),
        CapabilityPermissionV1::QueryProjection(lineage(), ProjectionId::first()),
        CapabilityPermissionV1::ReadProjectionStatus(lineage(), ProjectionId::first()),
    ];
    for kind in [
        CapabilityPermissionKindV1::ValidateContract,
        CapabilityPermissionKindV1::ReadContract,
        CapabilityPermissionKindV1::DeployContract,
        CapabilityPermissionKindV1::ReadCommit,
        CapabilityPermissionKindV1::ScanCommits,
        CapabilityPermissionKindV1::SubscribeCommits,
        CapabilityPermissionKindV1::ReadProvenance,
        CapabilityPermissionKindV1::InspectOutbox,
        CapabilityPermissionKindV1::ReadHealth,
        CapabilityPermissionKindV1::ReadStatistics,
        CapabilityPermissionKindV1::CreateCapability,
        CapabilityPermissionKindV1::RevokeCapability,
        CapabilityPermissionKindV1::AdministerCapabilities,
    ] {
        permission_values.push(
            CapabilityPermissionV1::unparameterized(kind).expect("unparameterized permission"),
        );
    }
    let permissions = CapabilityPermissionsV1::new(permission_values).expect("permission set");
    let partition_scope =
        PartitionScopeV1::explicit(vec![ScopedPartitionV1::new(lineage(), partition_key())])
            .expect("partition scope");
    let visibility = EntityFieldVisibilityV1::new(
        lineage(),
        EntityTypeId::first(),
        vec![FieldId::first(), FieldId::new(2).expect("field two")],
    )
    .expect("field visibility");
    let grant = CapabilityGrantV1::new(
        actor().tenant_scope().clone(),
        partition_scope,
        permissions,
        vec![visibility],
        NonZeroU16::new(100).expect("scan rows"),
        vec![CapabilityPermissionKindV1::InvokeCommand],
    )
    .expect("capability grant");
    let requested = CapabilityRequestedRecordV1::new(
        database_id(),
        Environment::new("test").expect("environment"),
        actor().principal_id().clone(),
        ActorKind::Human,
        NonZeroU32::new(60).expect("duration"),
        vec![Audience::new("riffdb-test").expect("audience")],
        grant,
    )
    .expect("requested capability");
    let digest = CapabilityTokenDigest::from_hmac_bytes(
        DigestKeyId::new(1).expect("digest key ID"),
        [0x61; 32],
    );
    let issued_at = Timestamp::new(1_000, 9).expect("issued timestamp");
    let expires_at = Timestamp::new(1_060, 9).expect("expiry timestamp");
    let record = StoredCapabilityRecordV1::active(
        capability_id(),
        digest,
        requested,
        issued_at,
        expires_at,
        AdministrationSequence::first(),
        request_id(),
    )
    .expect("active capability");
    let lookup = CapabilityTokenLookupV1::new(capability_id());
    let marker = CapabilityBootstrapMarkerV1::new(
        database_id(),
        capability_id(),
        AdministrationSequence::first(),
    );
    let administration = StoredCapabilityAdministrationV1::new(
        AdministrationSequence::first(),
        request_id(),
        CapabilityAdministrationOperationV1::Create,
        issued_at,
        Some(audit_principal()),
        capability_id(),
        NonZeroU64::MIN,
        Some(ApprovalId::new("capability-approval").expect("approval")),
        None,
    )
    .expect("capability administration");
    (record, lookup, marker, administration)
}

pub(super) fn service_audit_record() -> StoredServiceAuditRecordV1 {
    let targets = ServiceAuditTargetsV1::new([
        ServiceAuditTargetV1::ContractLineage(lineage()),
        ServiceAuditTargetV1::ContractVersion {
            lineage: lineage(),
            version: ContractVersion::new(1).expect("contract version"),
        },
        ServiceAuditTargetV1::EntityType {
            lineage: lineage(),
            entity_type_id: EntityTypeId::first(),
        },
        ServiceAuditTargetV1::Command {
            lineage: lineage(),
            command_id: CommandId::first(),
        },
        ServiceAuditTargetV1::Projection {
            lineage: lineage(),
            projection_id: ProjectionId::first(),
        },
        ServiceAuditTargetV1::Index {
            lineage: lineage(),
            index_id: IndexId::first(),
        },
        ServiceAuditTargetV1::Commit(CommitSequence::first()),
        ServiceAuditTargetV1::Provenance(provenance_id()),
        ServiceAuditTargetV1::Capability(capability_id()),
    ])
    .expect("all audit target variants");
    StoredServiceAuditRecordV1::from_stored_parts(
        AdministrationSequence::first(),
        request_id(),
        Timestamp::new(2_000, 3).expect("audit timestamp"),
        ServiceOperationV1::GetEntity,
        ServiceAuditPhaseV1::Started,
        Some(audit_principal()),
        ServiceIngressKindV1::Grpc,
        targets,
        None,
        ServiceAuditLinkV1::None,
    )
    .expect("service audit record")
}

pub(super) fn outbox_status() -> StoredOutboxStatusV1 {
    StoredOutboxStatusV1::pending(
        EventId::new(CommitSequence::first(), 0),
        OutboxRetryMetadataV1::new(
            NonZeroU32::MIN,
            Timestamp::new(3_000, 4).expect("last attempt"),
            Some(Timestamp::new(3_030, 4).expect("next attempt")),
            OutboxDestinationIdV1::new("destination-a").expect("destination"),
            Some(OutboxSafeErrorV1::new("retryable").expect("safe error")),
        ),
    )
}

pub(super) fn projection_records() -> (
    CheckedProjectionSchema,
    StoredProjectionStateV1,
    StoredProjectionApplyV1,
    StoredProjectionControlV1,
) {
    let source = r#"
contract CodecSample version 1 {
  event Source { group: string<32> }
  projection Totals {
    source event Source
    key (group)
    measure total = count()
    frontier transactionally_ordered
  }
}
"#;
    let bundle = compile_contract_source(source).expect("projection contract compiles");
    let schema = CheckedProjectionSchema::new(
        bundle
            .bound_projection_group_schema(ProjectionId::first())
            .expect("projection schema"),
    );
    let generation = ProjectionGeneration::first();
    let group = CanonicalValue::string("group-a").expect("group value");
    let key = schema
        .group_key(generation, &[group])
        .expect("projection group key");
    let measures = CanonicalRecord::new(vec![(FieldId::first(), CanonicalValue::U64(3))])
        .expect("projection measures");
    let state = StoredProjectionStateV1::new(&schema, key, measures, CommitSequence::first())
        .expect("projection state");
    let apply = StoredProjectionApplyV1::new(
        ProjectionApplyKey::new(
            schema.identity().clone(),
            generation,
            CommitSequence::first(),
        ),
        ProjectionApplyHash::from_bytes([0x71; 32]),
    );
    let control = StoredProjectionControlV1::initial(schema.identity().clone());
    (schema, state, apply, control)
}
