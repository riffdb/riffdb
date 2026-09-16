//! Restore policy evaluated against an immutable, fully validated follower pin.
// req: REP-007, AFC-007
use super::*;
use crate::maintenance_driver::{MaintenanceDriverDependencies, authorize_staged_restore};
use riffdb_auth::*;
use riffdb_policy::{NoopAuthorizationTelemetry, TrustedAudienceCatalog};
use riffdb_storage_api::*;
use riffdb_types::*;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::atomic::{AtomicI64, Ordering};

async fn catch_up(
    receiver: &mut FollowerReceiver,
    fixture: &Fixture,
    readers: &FollowerReadSnapshots,
) {
    let target = fixture.peer.history().tail().sequence();
    for _ in 0..16 {
        receiver.advance(&fixture.peer).await.unwrap();
        if readers.latest().unwrap().history().tail().sequence() >= target {
            return;
        }
    }
    panic!("bounded catch-up failed");
}

#[tokio::test]
async fn staged_restore_authentication_uses_replayed_capabilities_without_local_authority() {
    staged_authorization_case(false).await;
}

#[tokio::test]
async fn staged_restore_refuses_policy_required_approval_without_local_authority() {
    staged_authorization_case(true).await;
}

async fn staged_authorization_case(approval_required: bool) {
    let (mut fixture, build) = fixture().await;
    let mut receiver = build
        .publish_and_follow(fixture.path.clone())
        .await
        .unwrap();
    let (_, readers, _) = receiver.prepare_service().await.unwrap();
    let keys = Arc::new(CapabilityDigestKeyProvider::parse_document(
        b"riffdb-capability-digest-keys-v1\n1:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n",
    ).unwrap());
    let issued = issue_capability_token(&SystemEntropy, &keys).unwrap();
    let token = issued.text().expose_secret().to_owned();
    let id = CapabilityId::from_unix_milliseconds_and_random(9, [0x9c; 10]).unwrap();
    let request = RequestId::from_unix_milliseconds_and_random(9, [0x9d; 10]).unwrap();
    let operation =
        OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(9, [0x9e; 10]).unwrap();
    let environment = Environment::new("staged-archive").unwrap();
    let audience = Audience::new("riffdb-test").unwrap();
    let database = fixture.manifest.fence().history().lineage().database_id();
    let time = Arc::new(AtomicI64::new(1001));
    let clocks = crate::clocks::ProductionWallClocks::settable(time.clone());
    let recovery =
        crate::maintenance_recovery_controller::MaintenanceRecoveryController::disabled();
    let dependencies = MaintenanceDriverDependencies::new(
        inputs(),
        crate::identifiers::ProductionIdentifierSources::new().database_ids(),
        riffdb_storage_redb::RedbCommitProfile::Standard,
        keys,
        environment.clone(),
        audience.clone(),
        TrustedAudienceCatalog::new(vec![audience.clone()]).unwrap(),
        &clocks,
        Arc::new(NoopAuthenticationTelemetry),
        Arc::new(NoopAuthorizationTelemetry),
        &recovery,
        None,
        None,
    );
    let hash = archive_restore_input_hash(
        &BackupNameV1::new("backup").unwrap(),
        &ArchiveNameV1::new("archive").unwrap(),
        ArchiveRestoreStopV1::LastArchived,
        OfflineMaintenanceReplacementConfirmation::NotProvided,
    );
    let authorize = |snapshot, database| {
        authorize_staged_restore(
            snapshot,
            database,
            RetainedOpaqueCredential::new(&token).unwrap(),
            operation,
            hash,
            &dependencies,
        )
    };
    let before_grant = readers.open_owned_snapshot().unwrap();
    assert!(authorize(before_grant.clone(), database).is_err());
    let grant = CapabilityGrantV1::new(
        TenantScope::Global,
        PartitionScopeV1::All,
        CapabilityPermissionsV1::new(vec![
            CapabilityPermissionV1::unparameterized(
                CapabilityPermissionKindV1::AdministerCapabilities,
            )
            .unwrap(),
        ])
        .unwrap(),
        vec![],
        NonZeroU16::new(10).unwrap(),
        if approval_required {
            vec![CapabilityPermissionKindV1::AdministerCapabilities]
        } else {
            vec![]
        },
    )
    .unwrap();
    let intent = CapabilityBootstrapIntentV1::new(
        id,
        CapabilityRequestedRecordV1::new(
            database,
            environment,
            ActorId::new("restore-operator").unwrap(),
            ActorKind::Human,
            NonZeroU32::new(3600).unwrap(),
            vec![audience],
            grant,
        )
        .unwrap(),
        BootstrapDigestCandidatesV1::new(vec![issued.digest()], issued.digest()).unwrap(),
        Timestamp::new(1000, 0).unwrap(),
        Timestamp::new(4600, 0).unwrap(),
        BootstrapServiceAuditStartV1::new(
            request,
            Timestamp::new(1000, 0).unwrap(),
            ServiceIngressKindV1::Grpc,
            ServiceAuditTargetsV1::new([ServiceAuditTargetV1::Capability(id)]).unwrap(),
            None,
        )
        .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        Arc::get_mut(&mut fixture.peer.ports)
            .unwrap()
            .bootstrap_capability(&intent)
            .unwrap(),
        CapabilityBootstrapResult::BootstrapCreated { .. }
    ));
    assert!(authorize(readers.open_owned_snapshot().unwrap(), database).is_err());
    catch_up(&mut receiver, &fixture, &readers).await;
    assert!(authorize(before_grant, database).is_err());
    let granted_history = readers.latest().unwrap().history();
    let granted = readers.open_owned_snapshot().unwrap();
    if approval_required {
        assert_eq!(
            authorize(granted, database),
            Err(OfflineMaintenanceReceiptFailureV1::StagedAuthorizationFailed),
        );
        assert_eq!(readers.latest().unwrap().history(), granted_history);
        receiver.close().await.unwrap();
        let startup = crate::startup::open_redb_follower_startup(&fixture.path, inputs()).unwrap();
        let (history, snapshot) = startup.applier.capture_read_snapshot().unwrap();
        assert_eq!(history, granted_history);
        assert_eq!(
            authorize(snapshot, database),
            Err(OfflineMaintenanceReceiptFailureV1::StagedAuthorizationFailed),
        );
        startup.applier.close().unwrap();
        return;
    }
    let admission = authorize(granted.clone(), database).unwrap();
    assert_eq!(
        admission.principal_id(),
        &ActorId::new("restore-operator").unwrap()
    );
    assert_eq!(admission.capability_id(), id);
    assert_eq!(admission.actor_kind(), ActorKind::Human);
    let foreign = DatabaseId::from_unix_milliseconds_and_random(2, [0x71; 10]).unwrap();
    assert!(authorize(granted.clone(), foreign).is_err());
    time.store(4601, Ordering::SeqCst);
    assert!(authorize(granted.clone(), database).is_err());
    time.store(1001, Ordering::SeqCst);
    assert!(authorize(granted.clone(), database).is_ok());
    assert_eq!(readers.latest().unwrap().history(), granted_history);

    let actor = AuditPrincipalV1::new(
        ActorId::new("restore-operator").unwrap(),
        ActorKind::Human,
        id,
        NonZeroU64::MIN,
    );
    let (awaiting, current) = Arc::get_mut(&mut fixture.peer.ports)
        .unwrap()
        .begin_capability_revoke(CapabilityRevokeCandidateV1::new(
            id,
            request,
            actor.clone(),
            None,
            RevocationReasonCodeV1::Requested,
        ))
        .unwrap()
        .read_transaction_current()
        .unwrap();
    assert!(matches!(
        awaiting
            .commit_revoke(CapabilityRevokeIntentV1::new(
                id,
                current.target().unwrap().revision(),
                request,
                Timestamp::new(1001, 0).unwrap(),
                actor,
                None,
                RevocationReasonCodeV1::Requested,
            ))
            .unwrap(),
        CapabilityRevokeResult::Revoked { .. }
    ));
    catch_up(&mut receiver, &fixture, &readers).await;
    let revoked_history = readers.latest().unwrap().history();
    assert!(authorize(readers.open_owned_snapshot().unwrap(), database).is_err());
    // A selected earlier restore has its own authority. A newer source head
    // cannot silently replace that immutable candidate's policy state.
    assert!(authorize(granted.clone(), database).is_ok());
    assert_eq!(readers.latest().unwrap().history(), revoked_history);
    drop(granted);
    receiver.close().await.unwrap();
    let startup = crate::startup::open_redb_follower_startup(&fixture.path, inputs()).unwrap();
    assert_eq!(startup.applier.durable_history().unwrap(), revoked_history);
    let (_, snapshot) = startup.applier.capture_read_snapshot().unwrap();
    assert!(authorize(snapshot, database).is_err());
    startup.applier.close().unwrap();
}
