//! Replication capabilities use the existing audited, durable administration path.
// req: REP-003

use super::*;
use riffdb_storage_api::{
    CapabilityAdministrationOperationV1, StoredCapabilityAdministrationV1, StoredCapabilityRecordV1,
};

fn replication_request() -> NormalizedCapabilityCreateRecord {
    NormalizedCapabilityCreateRecord::new(
        database_id(),
        environment(),
        ActorId::new("replication-peer").unwrap(),
        ActorKind::Service,
        NonZeroU32::new(300).unwrap(),
        vec![audience()],
        CapabilityGrantV1::new(
            TenantScope::Global,
            PartitionScopeV1::All,
            CapabilityPermissionsV1::new(vec![
                CapabilityPermissionV1::unparameterized(
                    CapabilityPermissionKindV1::ReplicateChangelog,
                )
                .unwrap(),
            ])
            .unwrap(),
            vec![],
            NonZeroU16::MIN,
            vec![],
        )
        .unwrap(),
    )
    .unwrap()
}

fn audits(
    ports: &RedbOperationalPorts,
    target: CapabilityId,
) -> Vec<StoredCapabilityAdministrationV1> {
    let AdministrationAuditScan::ExactEnd { records } = ports
        .scan_administration_audit(AdministrationAuditScanRequest::new(
            None,
            StorageScanLimit::new(64).unwrap(),
        ))
        .unwrap()
    else {
        panic!("small fixture must reach exact audit end");
    };
    records
        .into_iter()
        .filter_map(|item| match item.into_parts().0 {
            StoredAdministrationAuditRecordV1::Capability(record)
                if record.target_capability_id() == target =>
            {
                Some(record)
            }
            _ => None,
        })
        .collect()
}

fn target(record: &StoredCapabilityRecordV1) -> CapabilityRevokeTargetFacts {
    CapabilityRevokeTargetFacts::new(
        request_id(0x72),
        record.capability_id(),
        record.revision(),
        match record.lifecycle() {
            CapabilityLifecycleV1::Active => CapabilityActivity::Active,
            CapabilityLifecycleV1::Revoked { .. } => CapabilityActivity::Revoked,
        },
        record.database_id(),
        record.environment().clone(),
        record.principal_id().clone(),
        record.actor_kind(),
        record.audiences().to_vec(),
        record.issued_at(),
        record.expires_at(),
        record.grant().clone(),
    )
    .unwrap()
}

#[test]
fn replication_capability_create_revoke_and_retries_preserve_durable_audit_provenance() {
    let database = TestDatabase::create("replication-capability-audit");
    let fixture = authorization_fixture();
    let requested = replication_request();
    let id = capability_id(0x71);
    let clock = Arc::new(CountingAdministrationClock::working());
    let coordinator = start_coordinator(
        database.open(),
        clock.clone(),
        Arc::new(CountingAuthorizationClock::new()),
    );
    // This ordinary bootstrap administrator cannot itself carry replication.
    // Its accepted administration authority can issue a separate explicit grant.
    assert!(
        !administrator_grant()
            .permissions()
            .as_slice()
            .iter()
            .any(|permission| permission.kind() == CapabilityPermissionKindV1::ReplicateChangelog)
    );
    bootstrap_authorizer(
        &coordinator,
        &bootstrap_requested_record(),
        capability_digest(0x64),
        request_id(0x64),
    );
    let executor = coordinator.control_plane_executor();
    let create = |seed| {
        block_on(
            block_on(executor.reserve_capacity())
                .unwrap()
                .submit_capability_create(
                    CapabilityCreatePreparation::new(
                        authorize_create(&fixture, id, &requested, request_id(seed)),
                        capability_digest(seed),
                    )
                    .unwrap(),
                )
                .unwrap()
                .completion(),
        )
        .unwrap()
    };
    let created = create(0x71);
    let CapabilityCreateOutcome::Created(transition) = created.outcome() else {
        panic!("explicit administrative replication grant must be created");
    };
    let create_sequence = transition.administration_sequence();
    let replay = create(0x73);
    assert!(
        matches!(replay.outcome(), CapabilityCreateOutcome::AlreadyCreatedTokenUnavailable(identity)
        if identity.capability_id() == id)
    );
    assert_eq!(replay.terminal_audit(), created.terminal_audit());
    coordinator.shutdown().unwrap();

    let ports = database.open();
    let stored = ports.read_capability(id).unwrap().unwrap();
    assert_eq!(stored.grant(), requested.grant());
    assert_eq!(stored.lifecycle(), &CapabilityLifecycleV1::Active);
    let created_audits = audits(&ports, id);
    assert_eq!(
        created_audits.len(),
        1,
        "create retry writes no duplicate transition"
    );
    let audit = &created_audits[0];
    assert_eq!(
        audit.operation(),
        CapabilityAdministrationOperationV1::Create
    );
    assert_eq!(audit.request_id(), request_id(0x71));
    assert_eq!(audit.administration_sequence(), create_sequence);
    assert_eq!(audit.resulting_revision(), NonZeroU64::MIN);
    assert_eq!(
        audit.initiator().unwrap().principal_id(),
        fixture.authenticated_principal().principal_id()
    );

    let coordinator = start_coordinator(
        ports,
        clock.clone(),
        Arc::new(CountingAuthorizationClock::new()),
    );
    let executor = coordinator.control_plane_executor();
    let revoked = block_on(
        block_on(executor.reserve_capacity())
            .unwrap()
            .submit_capability_revoke(
                CapabilityRevokePreparation::new(authorize_revoke(
                    &fixture,
                    target(&stored),
                    request_id(0x72),
                ))
                .unwrap(),
            )
            .unwrap()
            .completion(),
    )
    .unwrap();
    let CapabilityRevokeOutcome::Revoked(transition) = revoked.outcome() else {
        panic!("explicit replication capability must revoke");
    };
    let revoke_sequence = transition.administration_sequence();
    assert!(revoke_sequence > create_sequence);
    coordinator.shutdown().unwrap();

    let ports = database.open();
    let stored = ports.read_capability(id).unwrap().unwrap();
    assert!(matches!(
        stored.lifecycle(),
        CapabilityLifecycleV1::Revoked { .. }
    ));
    assert_eq!(stored.grant(), requested.grant());
    let revoked_audits = audits(&ports, id);
    assert_eq!(revoked_audits.len(), 2);
    let audit = &revoked_audits[1];
    assert_eq!(
        audit.operation(),
        CapabilityAdministrationOperationV1::Revoke
    );
    assert_eq!(audit.request_id(), request_id(0x72));
    assert_eq!(audit.administration_sequence(), revoke_sequence);
    assert_eq!(audit.resulting_revision().get(), 2);
    assert_eq!(
        audit.revocation_reason(),
        Some(RevocationReasonCodeV1::Requested)
    );
    assert_eq!(audit.initiator(), revoked_audits[0].initiator());

    let coordinator = start_coordinator(ports, clock, Arc::new(CountingAuthorizationClock::new()));
    let executor = coordinator.control_plane_executor();
    let replay = block_on(
        block_on(executor.reserve_capacity())
            .unwrap()
            .submit_capability_revoke(
                CapabilityRevokePreparation::new(authorize_revoke(
                    &fixture,
                    target(&stored),
                    request_id(0x74),
                ))
                .unwrap(),
            )
            .unwrap()
            .completion(),
    )
    .unwrap();
    assert!(
        matches!(replay.outcome(), CapabilityRevokeOutcome::AlreadyRevoked(replayed)
        if replayed.administration_sequence() == revoke_sequence)
    );
    assert_eq!(replay.terminal_audit(), revoked.terminal_audit());
    coordinator.shutdown().unwrap();
    let ports = database.open();
    assert_eq!(ports.read_capability(id).unwrap().unwrap(), stored);
    assert_eq!(
        audits(&ports, id),
        revoked_audits,
        "revoke retry preserves the original durable evidence"
    );
}
