use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

use prost::Message;
use riffdb_proto::storage::v1 as wire;
use riffdb_types::{
    AdministrationSequence, ApprovalId, CanonicalRecord, CanonicalValue, CommitSequence, EventId,
    EventTypeId, ExecutionFailureCode, FieldId, FrontierPosition, ProjectionGeneration,
    RowPolicyName, ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetsV1,
    ServiceIngressKindV1, ServiceOperationV1, TenantScope, Timestamp,
};

use crate::{
    AdministrationSequenceAllocator, ApplicationSequenceAllocator,
    CapabilityAdministrationOperationV1, CapabilityGrantV1, CapabilityLifecycleV1,
    CapabilityPermissionsV1, DurabilityMode, OutboxDestinationIdV1, OutboxRetryMetadataV1,
    OutboxSafeErrorV1, PartitionScopeV1, ProjectionFailureCodeV1, ProjectionFailureV1,
    ProjectionGenerationPosition, ProjectionLifecycleV1, PublishedApplyModeV1,
    RevocationReasonCodeV1, StoredCapabilityAdministrationV1, StoredCapabilityRecordV1,
    StoredCatalogAdministrationV1, StoredCommitRecordV1, StoredDurableEventV2,
    StoredEventPolicyAnchorV1, StoredOutboxStatusV1, StoredOutcomeV1, StoredProjectionControlV1,
    StoredServiceAuditRecordV1, derive_event_hash_v2,
};

use super::super::*;
use super::{assert_round_trip, sample};

#[test]
fn anchored_event_successor_round_trips_and_binds_authority_into_its_hash() {
    let event_id = EventId::new(CommitSequence::first(), 0);
    let event_type = EventTypeId::first();
    let payload = sample::canonical_record(0x43);
    let anchor = StoredEventPolicyAnchorV1::new(
        crate::DurableKeySchemaBindingV1::from_plan(&sample::plan()),
        event_type,
        sample::entity_target(),
        RowPolicyName::new("TicketAccess").expect("policy name"),
    );
    let event = StoredDurableEventV2::new(
        event_id,
        event_type,
        payload.clone(),
        derive_event_hash_v2(event_id, event_type, &payload, &anchor).expect("event hash"),
        anchor,
    )
    .expect("anchored event");

    assert_eq!(
        riffdb_proto::durable::current_record_schema(EVENT_V2)
            .expect("event V2 writable schema")
            .record_type(),
        <wire::StoredDurableEventV2 as riffdb_proto::durable::ReadableRecordMessage>::record_schema(
        )
        .record_type(),
    );

    assert_round_trip(event, encode_durable_event_v2, decode_durable_event_v2);
}

#[test]
fn workflow_service_values_survive_pending_and_outcome_round_trips() {
    let service_values = CanonicalRecord::new(vec![
        (
            FieldId::new(9).expect("field"),
            CanonicalValue::Uuid([0x51; 16]),
        ),
        (
            FieldId::new(10).expect("field"),
            CanonicalValue::Timestamp(Timestamp::new(42, 7).expect("service time")),
        ),
    ])
    .expect("service values");
    let pending = sample::pending()
        .with_service_values(service_values.clone())
        .expect("service values attach");
    let encoded = encode_pending_admission_v1(&pending).expect("pending encodes");
    let decoded = decode_pending_admission_v1(encoded.as_bytes()).expect("pending decodes");
    assert_eq!(decoded.value().service_values(), pending.service_values());
    assert_eq!(decoded.value().causation(), pending.causation());
    assert_eq!(decoded.value().logical_time(), pending.logical_time());
    assert_eq!(decoded.value(), &pending);

    let records = sample::atomic_record_set();
    let outcome = records
        .stored_outcome()
        .clone()
        .with_service_values(service_values)
        .expect("service values attach");
    let encoded = encode_stored_outcome_v1(&outcome).expect("outcome encodes");
    let decoded = decode_stored_outcome_v1(encoded.as_bytes()).expect("outcome decodes");
    assert_eq!(decoded.value().service_values(), outcome.service_values());
    assert_eq!(decoded.value(), &outcome);
}

fn payload<M: Message + Default>(envelope: &CanonicalStoredEnvelopeV1) -> M {
    let decoded = riffdb_proto::durable::readable_record_registry()
        .decode(envelope.as_bytes())
        .expect("checked envelope");
    M::decode(decoded.payload()).expect("registered payload")
}

#[test]
fn migration_authority_uses_additive_capability_v2_only() {
    let (legacy, _, _, _) = sample::capability_records();
    let legacy = encode_capability_record_v1(&legacy).expect("legacy capability encodes");
    let legacy_envelope = riffdb_proto::durable::readable_record_registry()
        .decode(legacy.as_bytes())
        .expect("legacy capability envelope");
    assert_eq!(
        legacy_envelope.record_type(),
        "riffdb.storage.v1.CapabilityRecordV1"
    );

    let capability = sample::capability_record_with_migration_authority();
    let encoded = assert_round_trip(
        capability,
        encode_capability_record_v1,
        decode_capability_record_v1,
    );
    let envelope = riffdb_proto::durable::readable_record_registry()
        .decode(encoded.as_bytes())
        .expect("migration capability envelope");
    assert_eq!(
        envelope.record_type(),
        "riffdb.storage.v1.CapabilityRecordV2"
    );
    let record = wire::CapabilityRecordV2::decode(envelope.payload()).expect("capability V2");
    let base_grant = record.base.expect("V2 base").grant.expect("V2 base grant");
    assert!(
        base_grant
            .permissions
            .expect("V2 base permissions")
            .values
            .iter()
            .all(|permission| permission.kind != 26)
    );
    assert!(base_grant.approval_required.iter().all(|kind| *kind != 26));
    let migration = record.migration.expect("V2 migration extension");
    assert_eq!(
        migration.contract_lineages,
        ["accounts".to_owned(), "ticketdesk".to_owned()]
    );
    assert!(migration.approval_required);
}

#[test]
fn installation_authority_uses_additive_capability_v3_only() {
    let capability = sample::capability_record_with_installation_authority();
    let encoded = assert_round_trip(
        capability,
        encode_capability_record_v1,
        decode_capability_record_v1,
    );
    let envelope = riffdb_proto::durable::readable_record_registry()
        .decode(encoded.as_bytes())
        .expect("installation capability envelope");
    assert_eq!(
        envelope.record_type(),
        "riffdb.storage.v1.CapabilityRecordV3"
    );
    let record = wire::CapabilityRecordV3::decode(envelope.payload()).expect("capability V3");
    assert!(record.migration.is_none());
    let base_grant = record.base.expect("V3 base").grant.expect("V3 base grant");
    assert!(
        base_grant
            .permissions
            .expect("V3 base permissions")
            .values
            .iter()
            .all(|permission| permission.kind != 31)
    );
    assert!(base_grant.approval_required.iter().all(|kind| *kind != 31));
    let installation = record.installation.expect("V3 installation extension");
    assert_eq!(installation.contract_lineages, ["ticketdesk".to_owned()]);
    assert!(installation.approval_required);
}

#[test]
fn row_policy_authority_uses_additive_capability_v4_only() {
    let capability = sample::capability_record_with_row_policy_authority();
    let encoded = assert_round_trip(
        capability.clone(),
        encode_capability_record_v1,
        decode_capability_record_v1,
    );
    let envelope = riffdb_proto::durable::readable_record_registry()
        .decode(encoded.as_bytes())
        .expect("row-policy capability envelope");
    assert_eq!(
        envelope.record_type(),
        "riffdb.storage.v1.CapabilityRecordV4"
    );
    let record = wire::CapabilityRecordV4::decode(envelope.payload()).expect("capability V4");
    assert!(record.migration.is_none());
    assert!(record.installation.is_none());
    let extension = record.row_policy.expect("V4 row-policy extension");
    assert_eq!(extension.application_role_hash, vec![0x42; 32]);
    assert!(!extension.canonical_principal_facts.is_empty());
    assert_eq!(extension.policies.len(), 1);
    assert_eq!(extension.policies[0].contract_lineage, "ticketdesk");
    assert_eq!(extension.policies[0].policy_name, "TicketVisible");
    assert_eq!(extension.policies[0].entity_type_id, 1);
    assert_eq!(extension.policies[0].operations, [1, 3]);
    assert_eq!(
        capability
            .grant()
            .internal_row_policy()
            .expect("policy grant")
            .bindings()
            .len(),
        1
    );
}

#[test]
fn export_authority_uses_additive_capability_v5_only() {
    let capability = sample::capability_record_with_export_authority();
    assert_eq!(
        riffdb_proto::durable::current_record_schema("riffdb.storage.v1.CapabilityRecordV5")
            .expect("capability V5 writable schema")
            .record_type(),
        <wire::CapabilityRecordV5 as riffdb_proto::durable::ReadableRecordMessage>::record_schema()
            .record_type(),
    );
    let encoded = assert_round_trip(
        capability.clone(),
        encode_capability_record_v1,
        decode_capability_record_v1,
    );
    let envelope = riffdb_proto::durable::readable_record_registry()
        .decode(encoded.as_bytes())
        .expect("export capability envelope");
    assert_eq!(
        envelope.record_type(),
        "riffdb.storage.v1.CapabilityRecordV5"
    );
    let record = wire::CapabilityRecordV5::decode(envelope.payload()).expect("capability V5");
    assert!(record.migration.is_none());
    assert!(record.installation.is_none());
    assert!(record.row_policy.is_some());
    let export = record.export.expect("V5 export extension");
    assert_eq!(export.applications.len(), 1);
    assert_eq!(export.applications[0].contract_lineage, "ticketdesk");
    assert_eq!(export.applications[0].scope, 1);
    assert!(export.applications[0].entities);
    assert!(export.applications[0].events);
    assert!(export.applications[0].provenance);
    assert!(!export.applications[0].public_audit);
    assert_eq!(
        capability
            .grant()
            .internal_export()
            .expect("export grant")
            .applications()
            .len(),
        1
    );
}

#[test]
fn capability_v5_composes_all_predecessor_extensions_and_export_scopes() {
    let capability = sample::capability_record_with_complete_export_authority();
    let encoded = assert_round_trip(
        capability,
        encode_capability_record_v1,
        decode_capability_record_v1,
    );
    let envelope = riffdb_proto::durable::readable_record_registry()
        .decode(encoded.as_bytes())
        .expect("combined export capability envelope");
    assert_eq!(
        envelope.record_type(),
        "riffdb.storage.v1.CapabilityRecordV5"
    );
    let record = wire::CapabilityRecordV5::decode(envelope.payload()).expect("capability V5");
    assert!(record.migration.is_some());
    assert!(record.installation.is_some());
    assert!(record.row_policy.is_some());
    let applications = record.export.expect("export extension").applications;
    assert_eq!(applications.len(), 2);
    assert_eq!(applications[0].contract_lineage, "accounts");
    assert_eq!(applications[0].scope, 2);
    assert!(applications[0].events);
    assert!(applications[0].public_audit);
    assert_eq!(applications[1].contract_lineage, "ticketdesk");
    assert_eq!(applications[1].scope, 1);
    assert!(applications[1].entities);
    assert!(applications[1].events);
    assert!(applications[1].provenance);
}

#[test]
fn allocator_variants_include_maximum_and_exhausted_states() {
    let maximum_commit = CommitSequence::new(u64::MAX).expect("maximum commit sequence");
    let maximum_administration =
        AdministrationSequence::new(u64::MAX).expect("maximum administration sequence");
    for value in [
        ApplicationSequenceAllocator::Next(maximum_commit),
        ApplicationSequenceAllocator::Exhausted,
    ] {
        assert_round_trip(
            value,
            |value| encode_application_sequence_allocator_v1(*value),
            decode_application_sequence_allocator_v1,
        );
    }
    for value in [
        AdministrationSequenceAllocator::Next(maximum_administration),
        AdministrationSequenceAllocator::Exhausted,
    ] {
        assert_round_trip(
            value,
            |value| encode_administration_sequence_allocator_v1(*value),
            decode_administration_sequence_allocator_v1,
        );
    }
}

#[test]
fn unit_variants_are_encoded_as_present_oneofs() {
    use wire::capability_lifecycle_v1::State as CapabilityState;
    use wire::frontier_position_v1::Position as FrontierPosition;
    use wire::index_epoch_position_v1::Position as IndexEpochPosition;
    use wire::partition_scope_v1::Scope as PartitionScope;
    use wire::service_audit_link_v1::Link as AuditLink;
    use wire::stored_application_sequence_allocator_v1::State as AllocatorState;
    use wire::tenant_scope_v1::Scope as TenantScopeWire;

    let allocator =
        encode_application_sequence_allocator_v1(ApplicationSequenceAllocator::Exhausted)
            .expect("allocator encodes");
    let allocator: wire::StoredApplicationSequenceAllocatorV1 = payload(&allocator);
    assert!(matches!(
        allocator.state,
        Some(AllocatorState::Exhausted(_))
    ));

    let records = sample::atomic_record_set();
    let commit = encode_commit_record_v1(records.commit()).expect("commit encodes");
    let commit: wire::StoredCommitRecordV3 = payload(&commit);
    assert_eq!(commit.entity_references.len(), 1);
    assert_eq!(
        commit.entity_references[0].entity_version,
        riffdb_types::EntityVersion::first().get()
    );
    assert!(matches!(
        super::super::epoch_to_proto(crate::IndexEpochPosition::BeforeFirst).position,
        Some(IndexEpochPosition::BeforeFirst(_))
    ));

    let audit =
        encode_service_audit_record_v2(&sample::service_audit_record()).expect("audit encodes");
    let audit: wire::ServiceAuditRecordV2 = payload(&audit);
    assert!(matches!(
        audit.link.and_then(|link| link.link),
        Some(AuditLink::None(_))
    ));

    let (_, _, _, control) = sample::projection_records();
    let control = encode_projection_control_v1(&control).expect("control encodes");
    let control: wire::StoredProjectionControlV1 = payload(&control);
    assert!(matches!(
        control
            .candidate
            .and_then(|candidate| candidate.frontier)
            .and_then(|frontier| frontier.position),
        Some(FrontierPosition::BeforeFirst(_))
    ));

    let (active, _, _, _) = sample::capability_records();
    let grant = CapabilityGrantV1::new(
        TenantScope::Global,
        PartitionScopeV1::All,
        CapabilityPermissionsV1::new(Vec::new()).expect("empty permissions"),
        Vec::new(),
        NonZeroU16::MIN,
        Vec::new(),
    )
    .expect("least-authority global grant shape");
    let global = StoredCapabilityRecordV1::from_stored_parts(
        active.capability_id(),
        active.revision(),
        active.token_digest(),
        active.database_id(),
        active.environment().clone(),
        active.principal_id().clone(),
        active.actor_kind(),
        active.audiences().to_vec(),
        active.issued_at(),
        active.expires_at(),
        active.creation_sequence(),
        active.creation_request_id(),
        grant,
        CapabilityLifecycleV1::Active,
    )
    .expect("global active capability");
    let global = assert_round_trip(
        global,
        encode_capability_record_v1,
        decode_capability_record_v1,
    );
    let global: wire::CapabilityRecordV1 = payload(&global);
    let grant = global.grant.expect("grant wrapper present");
    assert!(matches!(
        grant.tenant_scope.and_then(|scope| scope.scope),
        Some(TenantScopeWire::Global(_))
    ));
    assert!(matches!(
        grant.partition_scope.and_then(|scope| scope.scope),
        Some(PartitionScope::All(_))
    ));
    assert!(
        grant
            .permissions
            .expect("empty permissions wrapper remains present")
            .values
            .is_empty()
    );
    assert!(matches!(
        global.lifecycle.and_then(|lifecycle| lifecycle.state),
        Some(CapabilityState::Active(_))
    ));
}

#[test]
fn every_execution_failure_code_round_trips() {
    for code in [
        ExecutionFailureCode::ArithmeticFault,
        ExecutionFailureCode::ResourceLimit,
        ExecutionFailureCode::UniqueConflict,
    ] {
        assert_round_trip(
            crate::StoredExecutionFailedV1::new(sample::pending(), code),
            encode_execution_failed_v1,
            decode_execution_failed_v1,
        );
    }
}

#[test]
fn catalog_administration_preserves_optional_previous_pointer() {
    let (_, active, administration) = sample::catalog_records();
    let with_previous = StoredCatalogAdministrationV1::from_stored_parts(
        administration.administration_sequence(),
        administration.request_id(),
        administration.timestamp(),
        administration.principal().clone(),
        Some(active.clone()),
        active,
        administration.approval_id().cloned(),
    );
    let envelope = assert_round_trip(
        with_previous,
        encode_catalog_administration_v1,
        decode_catalog_administration_v1,
    );
    assert!(
        decode_catalog_administration_v1(envelope.as_bytes())
            .expect("catalog administration")
            .value()
            .previous_active()
            .is_some()
    );
}

#[test]
fn every_durability_mode_survives_outcome_and_commit_round_trips() {
    let records = sample::atomic_record_set();
    for mode in [
        DurabilityMode::Sync,
        DurabilityMode::Group,
        DurabilityMode::Memory,
    ] {
        let outcome = outcome_with_mode(records.stored_outcome(), mode);
        let commit = commit_with_mode(records.commit(), mode);
        let outcome =
            assert_round_trip(outcome, encode_stored_outcome_v1, decode_stored_outcome_v1);
        assert_eq!(
            decode_stored_outcome_v1(outcome.as_bytes())
                .expect("outcome")
                .value()
                .durability_mode(),
            mode
        );
        let expected_events = commit.events().to_vec();
        let commit = assert_round_trip(commit, encode_commit_record_v1, |bytes| {
            decode_commit_record_v3(bytes, expected_events.clone())
        });
        assert_eq!(
            decode_commit_record_v3(commit.as_bytes(), expected_events)
                .expect("commit")
                .value()
                .durability_mode(),
            mode
        );
    }
}

fn outcome_with_mode(value: &StoredOutcomeV1, mode: DurabilityMode) -> StoredOutcomeV1 {
    StoredOutcomeV1::new(
        value.identity().clone(),
        value.commit_sequence(),
        value.admission_request_id(),
        value.plan().clone(),
        value.canonical_input_hash(),
        value.actor().clone(),
        value.logical_time(),
        value.partition_key().clone(),
        value.partition_hash(),
        value.conflict_hashes().to_vec(),
        value.declared_outcome().clone(),
        value.admitted_claims().clone(),
        value.provenance_id(),
        mode,
    )
    .expect("durability does not alter outcome identity")
}

fn commit_with_mode(value: &StoredCommitRecordV1, mode: DurabilityMode) -> StoredCommitRecordV1 {
    StoredCommitRecordV1::new(
        value.commit_sequence(),
        value.admission_request_id(),
        value.plan().clone(),
        value.canonical_input_hash(),
        value.actor().clone(),
        value.logical_time(),
        value.partition_hash(),
        value.conflict_hashes().to_vec(),
        value.read_dependencies().clone(),
        value.entity_references().to_vec(),
        value.events().to_vec(),
        value.declared_outcome().clone(),
        value.provenance_id(),
        value.outbox_event_ids().to_vec(),
        mode,
    )
    .expect("durability does not alter commit graph")
}

#[test]
fn capability_permission_and_lifecycle_registries_round_trip() {
    let (active, _, _, _) = sample::capability_records();
    let permission_tags = active
        .grant()
        .permissions()
        .as_slice()
        .iter()
        .map(|permission| permission.kind().tag())
        .collect::<Vec<_>>();
    assert_eq!(permission_tags, (1..=19).collect::<Vec<_>>());

    for reason in [
        RevocationReasonCodeV1::Requested,
        RevocationReasonCodeV1::Replaced,
        RevocationReasonCodeV1::SuspectedCompromise,
        RevocationReasonCodeV1::PolicyChange,
    ] {
        let (active, _, _, _) = sample::capability_records();
        let revoked = active
            .revoked(
                NonZeroU64::MIN,
                Timestamp::new(1_030, 9).expect("revocation timestamp"),
                AdministrationSequence::new(2).expect("revocation sequence"),
                reason,
            )
            .expect("revoked capability");
        let envelope = assert_round_trip(
            revoked,
            encode_capability_record_v1,
            decode_capability_record_v1,
        );
        assert!(matches!(
            decode_capability_record_v1(envelope.as_bytes())
                .expect("capability")
                .value()
                .lifecycle(),
            CapabilityLifecycleV1::Revoked {
                reason: decoded,
                ..
            } if *decoded == reason
        ));
    }
}

#[test]
fn every_capability_administration_shape_round_trips() {
    let timestamp = Timestamp::new(1_100, 1).expect("administration timestamp");
    let sequence = AdministrationSequence::new(2).expect("administration sequence");
    let mut records = vec![
        StoredCapabilityAdministrationV1::new(
            sequence,
            sample::request_id(),
            CapabilityAdministrationOperationV1::Bootstrap,
            timestamp,
            None,
            sample::capability_id(),
            NonZeroU64::MIN,
            None,
            None,
        )
        .expect("bootstrap audit"),
        StoredCapabilityAdministrationV1::new(
            sequence,
            sample::request_id(),
            CapabilityAdministrationOperationV1::Create,
            timestamp,
            Some(sample::audit_principal()),
            sample::capability_id(),
            NonZeroU64::MIN,
            Some(ApprovalId::new("create-approval").expect("approval")),
            None,
        )
        .expect("create audit"),
    ];
    for reason in [
        RevocationReasonCodeV1::Requested,
        RevocationReasonCodeV1::Replaced,
        RevocationReasonCodeV1::SuspectedCompromise,
        RevocationReasonCodeV1::PolicyChange,
    ] {
        records.push(
            StoredCapabilityAdministrationV1::new(
                sequence,
                sample::request_id(),
                CapabilityAdministrationOperationV1::Revoke,
                timestamp,
                Some(sample::audit_principal()),
                sample::capability_id(),
                NonZeroU64::new(2).expect("revision two"),
                None,
                Some(reason),
            )
            .expect("revoke audit"),
        );
    }
    for record in records {
        assert_round_trip(
            record,
            encode_capability_administration_v1,
            decode_capability_administration_v1,
        );
    }
}

#[test]
fn all_explicit_outbox_status_variants_round_trip() {
    let event_id = riffdb_types::EventId::new(CommitSequence::first(), 0);
    let destination = || OutboxDestinationIdV1::new("destination-a").expect("destination");
    let statuses = vec![
        sample::outbox_status(),
        StoredOutboxStatusV1::pending(
            event_id,
            OutboxRetryMetadataV1::new(
                NonZeroU32::MIN,
                Timestamp::new(5, 1).expect("last attempt"),
                None,
                destination(),
                None,
            ),
        ),
        StoredOutboxStatusV1::delivering(
            event_id,
            NonZeroU32::MIN,
            destination(),
            Timestamp::new(10, 1).expect("started"),
            Timestamp::new(20, 1).expect("lease deadline"),
        ),
        StoredOutboxStatusV1::delivered(
            event_id,
            NonZeroU32::new(2).expect("attempts"),
            destination(),
            Timestamp::new(30, 1).expect("delivered"),
        ),
        StoredOutboxStatusV1::dead_letter(
            event_id,
            3,
            destination(),
            Timestamp::new(40, 1).expect("failed"),
            Some(OutboxSafeErrorV1::new("terminal").expect("safe error")),
        ),
        StoredOutboxStatusV1::dead_letter(
            event_id,
            0,
            destination(),
            Timestamp::new(41, 1).expect("failed without attempt"),
            None,
        ),
    ];
    for status in statuses {
        assert_round_trip(status, encode_outbox_status_v1, decode_outbox_status_v1);
    }
}

fn service_audit_with(
    operation: ServiceOperationV1,
    phase: ServiceAuditPhaseV1,
    ingress: ServiceIngressKindV1,
) -> StoredServiceAuditRecordV1 {
    StoredServiceAuditRecordV1::from_stored_parts(
        AdministrationSequence::first(),
        sample::request_id(),
        Timestamp::new(60, 1).expect("audit timestamp"),
        operation,
        phase,
        Some(sample::audit_principal()),
        ingress,
        ServiceAuditTargetsV1::empty(),
        None,
        ServiceAuditLinkV1::None,
    )
    .expect("standalone audit shape")
}

#[test]
fn every_service_operation_round_trips() {
    for operation in ServiceOperationV1::ALL {
        assert_round_trip(
            service_audit_with(
                operation,
                ServiceAuditPhaseV1::Started,
                ServiceIngressKindV1::Grpc,
            ),
            encode_service_audit_record_v2,
            decode_service_audit_record_v2,
        );
    }
}

#[test]
fn every_service_phase_and_ingress_round_trips() {
    for phase in ServiceAuditPhaseV1::ALL {
        assert_round_trip(
            service_audit_with(
                ServiceOperationV1::GetEntity,
                phase,
                ServiceIngressKindV1::Grpc,
            ),
            encode_service_audit_record_v2,
            decode_service_audit_record_v2,
        );
    }
    for ingress in ServiceIngressKindV1::ALL {
        assert_round_trip(
            service_audit_with(
                ServiceOperationV1::GetEntity,
                ServiceAuditPhaseV1::Started,
                ingress,
            ),
            encode_service_audit_record_v2,
            decode_service_audit_record_v2,
        );
    }
}

#[test]
fn command_and_control_plane_audit_links_round_trip() {
    let principal = Some(sample::audit_principal());
    let command = StoredServiceAuditRecordV1::from_stored_parts(
        AdministrationSequence::first(),
        sample::request_id(),
        Timestamp::new(70, 1).expect("timestamp"),
        ServiceOperationV1::ExecuteCommand,
        ServiceAuditPhaseV1::Succeeded,
        principal.clone(),
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::empty(),
        None,
        ServiceAuditLinkV1::Command {
            commit_sequence: CommitSequence::first(),
            provenance_id: sample::provenance_id(),
        },
    )
    .expect("command-linked audit");
    let control = StoredServiceAuditRecordV1::from_stored_parts(
        AdministrationSequence::new(2).expect("sequence two"),
        sample::request_id(),
        Timestamp::new(71, 1).expect("timestamp"),
        ServiceOperationV1::DeployContract,
        ServiceAuditPhaseV1::Succeeded,
        principal,
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::empty(),
        None,
        ServiceAuditLinkV1::ControlPlane {
            administration_sequence: AdministrationSequence::first(),
        },
    )
    .expect("control-linked audit");
    for value in [command, control] {
        assert_round_trip(
            value,
            encode_service_audit_record_v2,
            decode_service_audit_record_v2,
        );
    }
}

#[test]
fn projection_structural_and_contextual_boundaries_are_distinct() {
    let (schema, state, _, _) = sample::projection_records();
    let encoded = encode_projection_state_v1(&state).expect("projection state encodes");
    let structural = decode_projection_state_structural_v1(encoded.as_bytes())
        .expect("structural state decodes");
    assert_eq!(structural.value().identity(), state.identity());
    assert_eq!(structural.value().generation(), state.generation());
    assert_eq!(
        decode_projection_state_v1(encoded.as_bytes(), &schema)
            .expect("contextual state decodes")
            .value(),
        &state
    );
}

#[test]
fn every_projection_lifecycle_shape_round_trips() {
    let (schema, _, _, _) = sample::projection_records();
    let generation = ProjectionGeneration::first();
    let replacement = ProjectionGeneration::new(2).expect("replacement generation");
    let frontier = FrontierPosition::AppliedThrough(CommitSequence::first());
    let failure_sequence = CommitSequence::new(2).expect("failure sequence");
    let building = StoredProjectionControlV1::initial(schema.identity().clone());
    let catching_up = StoredProjectionControlV1::new(
        schema.identity().clone(),
        generation,
        None,
        Some(ProjectionGenerationPosition::new(generation, frontier)),
        None,
        ProjectionLifecycleV1::CatchingUp,
        None,
    )
    .expect("catching-up control");
    let ready = StoredProjectionControlV1::new(
        schema.identity().clone(),
        generation,
        Some(ProjectionGenerationPosition::new(generation, frontier)),
        None,
        Some(PublishedApplyModeV1::Enabled),
        ProjectionLifecycleV1::Ready,
        None,
    )
    .expect("ready control");
    let rebuilding = StoredProjectionControlV1::new(
        schema.identity().clone(),
        replacement,
        Some(ProjectionGenerationPosition::new(generation, frontier)),
        Some(ProjectionGenerationPosition::new(
            replacement,
            FrontierPosition::BeforeFirst,
        )),
        Some(PublishedApplyModeV1::Enabled),
        ProjectionLifecycleV1::Rebuilding,
        None,
    )
    .expect("rebuilding control");
    let degraded = StoredProjectionControlV1::new(
        schema.identity().clone(),
        generation,
        Some(ProjectionGenerationPosition::new(generation, frontier)),
        None,
        Some(PublishedApplyModeV1::Suspended),
        ProjectionLifecycleV1::Degraded,
        Some(ProjectionFailureV1::new(
            generation,
            ProjectionFailureCodeV1::ProjectionStateIntegrity,
            Some(failure_sequence),
        )),
    )
    .expect("degraded control");
    let invalid = StoredProjectionControlV1::new(
        schema.identity().clone(),
        generation,
        None,
        Some(ProjectionGenerationPosition::new(generation, frontier)),
        None,
        ProjectionLifecycleV1::Invalid,
        Some(ProjectionFailureV1::new(
            generation,
            ProjectionFailureCodeV1::PlanOrSchemaUnavailable,
            Some(failure_sequence),
        )),
    )
    .expect("invalid control");
    for value in [building, catching_up, ready, rebuilding, degraded, invalid] {
        assert_round_trip(
            value,
            encode_projection_control_v1,
            decode_projection_control_v1,
        );
    }
}

#[test]
fn every_projection_failure_code_round_trips() {
    let (schema, _, _, _) = sample::projection_records();
    let generation = ProjectionGeneration::first();
    let sequence = CommitSequence::first();
    let failure_sequence = CommitSequence::new(2).expect("failure sequence");
    for code in [
        ProjectionFailureCodeV1::ArithmeticOverflow,
        ProjectionFailureCodeV1::MalformedDurableEvent,
        ProjectionFailureCodeV1::MissingCommit,
        ProjectionFailureCodeV1::PlanOrSchemaUnavailable,
        ProjectionFailureCodeV1::ProjectionStateIntegrity,
        ProjectionFailureCodeV1::HardLimitExceeded,
    ] {
        let control = StoredProjectionControlV1::new(
            schema.identity().clone(),
            generation,
            Some(ProjectionGenerationPosition::new(
                generation,
                FrontierPosition::AppliedThrough(sequence),
            )),
            None,
            Some(PublishedApplyModeV1::Suspended),
            ProjectionLifecycleV1::Degraded,
            Some(ProjectionFailureV1::new(
                generation,
                code,
                Some(failure_sequence),
            )),
        )
        .expect("degraded projection control");
        assert_round_trip(
            control,
            encode_projection_control_v1,
            decode_projection_control_v1,
        );
    }
}

/// ADR-0118 boundary: explicit secret naming rides the additive V6 record —
/// round-trips value-exact AND byte-exact, keeps the ordinary field list
/// free of secret ids on the wire, and one grant short (no naming) never
/// leaves the legacy record shape.
#[test]
fn secret_naming_uses_additive_capability_v6_only() {
    let capability = sample::capability_record_with_secret_naming();
    let encoded = assert_round_trip(
        capability.clone(),
        encode_capability_record_v1,
        decode_capability_record_v1,
    );
    let envelope = riffdb_proto::durable::readable_record_registry()
        .decode(encoded.as_bytes())
        .expect("secret capability envelope");
    assert_eq!(
        envelope.record_type(),
        "riffdb.storage.v1.CapabilityRecordV6"
    );
    let record = wire::CapabilityRecordV6::decode(envelope.payload()).expect("capability V6");
    assert!(record.migration.is_none());
    assert!(record.installation.is_none());
    assert!(record.row_policy.is_none());
    assert!(record.export.is_none());
    let base = record.base.expect("V6 base");
    let base_grant = base.grant.expect("base grant");
    assert_eq!(
        base_grant.field_visibility[0].field_ids,
        vec![1, 2],
        "the ordinary wire list must never carry secret ids"
    );
    let secret = record.secret.expect("V6 secret extension");
    assert_eq!(secret.entries.len(), 1);
    assert_eq!(secret.entries[0].entity_type_id, 1);
    assert_eq!(secret.entries[0].secret_field_ids, vec![7]);

    // Decoded-then-re-encoded is byte-exact.
    let decoded = decode_capability_record_v1(encoded.as_bytes()).expect("decodes");
    let reencoded = encode_capability_record_v1(decoded.value()).expect("re-encodes");
    assert_eq!(
        encoded.as_bytes(),
        reencoded.as_bytes(),
        "a decoded grant must re-encode byte-exactly, secret naming included"
    );

    // One grant short: the identical record without the naming stays on the
    // legacy record shape — adopting V6 is strictly additive.
    let (plain, _, _, _) = sample::capability_records();
    let plain_encoded = encode_capability_record_v1(&plain).expect("plain encodes");
    let plain_envelope = riffdb_proto::durable::readable_record_registry()
        .decode(plain_encoded.as_bytes())
        .expect("plain envelope");
    assert_eq!(
        plain_envelope.record_type(),
        "riffdb.storage.v1.CapabilityRecordV1"
    );
}

#[test]
fn exact_reimport_authority_uses_additive_capability_v7_only() {
    let capability = sample::capability_record_with_reimport_authority();
    let encoded = assert_round_trip(
        capability.clone(),
        encode_capability_record_v1,
        decode_capability_record_v1,
    );
    let envelope = riffdb_proto::durable::readable_record_registry()
        .decode(encoded.as_bytes())
        .expect("reimport capability envelope");
    assert_eq!(
        envelope.record_type(),
        "riffdb.storage.v1.CapabilityRecordV7"
    );
    let record = wire::CapabilityRecordV7::decode(envelope.payload()).expect("capability V7");
    let reimport = record.reimport.expect("reimport extension");
    assert_eq!(reimport.contract_lineage, "ticketdesk");
    assert_eq!(reimport.portability_manifest_hash, vec![0x53; 32]);
    assert_eq!(
        reimport.scope,
        i32::from(riffdb_types::CapabilityApplicationReimportScopeV1::WholeApplication.tag())
    );
    assert!(record.row_policy.is_some());
}
