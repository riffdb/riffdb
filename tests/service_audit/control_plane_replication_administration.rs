//! Real coordinator queue, current authorization and atomic redb lifecycle writes.
// req: REP-006, STO-012
use super::*;
use riffdb_policy::{
    AuthorizedReplicationAdministrationPreparation, ReplicationAdministrationDecision,
};
use riffdb_storage_api::{
    FollowerHoldBudget, ReplicationAdministrationRequestV1 as Request,
    ReplicationAdministrationResultV1 as ResultV1,
};
use riffdb_types::{
    LeadershipEpochV1, ReplicationFollowerAuditTargetV1, ReplicationSourceHoldIdV1,
};

fn target() -> ReplicationFollowerAuditTargetV1 {
    ReplicationFollowerAuditTargetV1::new(
        database_id(),
        1,
        LeadershipEpochV1::initial(),
        ReplicationSourceHoldIdV1::new([0x72; 16]).unwrap(),
    )
    .unwrap()
}
fn register(seed: u8) -> Request {
    Request::register(
        request_id(seed),
        target(),
        FollowerHoldBudget::new(3).unwrap(),
        None,
    )
}
fn authorize(
    fixture: &AuthorizationFixture,
    request: Request,
) -> AuthorizedReplicationAdministrationPreparation {
    let resolver = fixture.current_capability_resolver();
    let decision = CurrentAuthorizer::new(
        &resolver,
        &FixedAuthorizationClock,
        &NoopAuthorizationTelemetry,
        database_id(),
        environment(),
    )
    .authorize_replication_administration(fixture.authenticated_principal(), request)
    .unwrap();
    let ReplicationAdministrationDecision::Allow(preparation) = decision else {
        panic!("administrator");
    };
    *preparation
}
fn execute(
    coordinator: &RunningCommandCoordinator,
    preparation: AuthorizedReplicationAdministrationPreparation,
) -> Result<
    riffdb_commit::ReplicationAdministrationExecutionResult,
    riffdb_commit::ControlPlaneExecutionError,
> {
    block_on(
        block_on(coordinator.control_plane_executor().reserve_capacity())
            .unwrap()
            .submit_replication_administration(preparation)
            .unwrap()
            .completion(),
    )
}
fn records(
    ports: &RedbOperationalPorts,
) -> Vec<riffdb_storage_api::StoredReplicationAdministrationV1> {
    let AdministrationAuditScan::ExactEnd { records } = ports
        .scan_administration_audit(AdministrationAuditScanRequest::new(
            None,
            StorageScanLimit::new(64).unwrap(),
        ))
        .unwrap()
    else {
        panic!("exact end");
    };
    records
        .into_iter()
        .filter_map(|item| match item.into_parts().0 {
            StoredAdministrationAuditRecordV1::Replication(record) => Some(*record),
            _ => None,
        })
        .collect()
}

#[test]
fn follower_lifecycle_coordinator_preserves_exact_receipts_and_audit_links_on_retry() {
    let database = TestDatabase::create("follower-coordinator");
    let clock = Arc::new(CountingAuthorizationClock::new());
    let coordinator = start_coordinator(
        database.open(),
        Arc::new(CountingAdministrationClock::working()),
        clock.clone(),
    );
    bootstrap_authorizer(
        &coordinator,
        &bootstrap_requested_record(),
        capability_digest(0x64),
        request_id(0x64),
    );
    let fixture = authorization_fixture();
    let before = clock.calls();
    let created = execute(&coordinator, authorize(&fixture, register(0x71))).unwrap();
    let ResultV1::Applied(original) = created.outcome() else {
        panic!("created");
    };
    assert_eq!(
        created.terminal_audit(),
        ControlPlaneTerminalAudit::Succeeded(ServiceAuditLinkV1::ControlPlane {
            administration_sequence: original.administration_sequence(),
        })
    );
    let retry = execute(&coordinator, authorize(&fixture, register(0x72))).unwrap();
    assert_eq!(retry.outcome(), &ResultV1::Replayed(original.clone()));
    let retire = Request::retire(request_id(0x73), target(), original.generation());
    let released = execute(&coordinator, authorize(&fixture, retire)).unwrap();
    assert!(matches!(released.outcome(), ResultV1::Applied(_)));
    let retry = execute(&coordinator, authorize(&fixture, retire)).unwrap();
    assert_eq!(retry.terminal_audit(), released.terminal_audit());
    let retry = execute(&coordinator, authorize(&fixture, register(0x74))).unwrap();
    assert_eq!(retry.terminal_audit(), created.terminal_audit());
    assert_eq!(
        clock.calls() - before,
        5,
        "every replay uses a fresh final sample"
    );
    coordinator.shutdown().unwrap();
    let records = records(&database.open());
    assert_eq!(records.len(), 2);
    assert!(records.iter().all(|r| r.target() == target()));
    assert_eq!(&records[0], original.as_ref());
}

#[test]
fn follower_lifecycle_queued_revocation_refuses_new_work_and_exact_replays() {
    let database = TestDatabase::create("follower-revoked-authorizer");
    let coordinator = start_coordinator(
        database.open(),
        Arc::new(CountingAdministrationClock::working()),
        Arc::new(CountingAuthorizationClock::new()),
    );
    let requested = bootstrap_requested_record();
    bootstrap_authorizer(
        &coordinator,
        &requested,
        capability_digest(0x64),
        request_id(0x64),
    );
    let fixture = authorization_fixture();
    let created = execute(&coordinator, authorize(&fixture, register(0x71))).unwrap();
    let ResultV1::Applied(original) = created.outcome() else {
        panic!("created");
    };
    let stale_retry = authorize(&fixture, register(0x72));
    let stale_retire = authorize(
        &fixture,
        Request::retire(request_id(0x73), target(), original.generation()),
    );
    let executor = coordinator.control_plane_executor();
    block_on(
        block_on(executor.reserve_capacity())
            .unwrap()
            .submit_capability_revoke(
                CapabilityRevokePreparation::new(authorize_revoke(
                    &fixture,
                    bootstrap_revoke_target(&requested),
                    request_id(0x74),
                ))
                .unwrap(),
            )
            .unwrap()
            .completion(),
    )
    .unwrap();
    for stale in [stale_retry, stale_retire] {
        assert_eq!(
            execute(&coordinator, stale).unwrap_err().kind(),
            ControlPlaneExecutionErrorKind::AuthorizationDenied
        );
    }
    assert_eq!(
        executor.lifecycle_state(),
        CoordinatorLifecycleState::Accepting
    );
    coordinator.shutdown().unwrap();
    assert_eq!(records(&database.open()), vec![*original.clone()]);
}

#[test]
fn follower_lifecycle_rechecks_expiry_and_clock_failure_after_opening_transaction() {
    for failed_clock in [false, true] {
        let database = TestDatabase::create("follower-current-clock");
        let coordinator = start_coordinator(
            database.open(),
            Arc::new(CountingAdministrationClock::working()),
            Arc::new(CountingAuthorizationClock::new()),
        );
        bootstrap_authorizer(
            &coordinator,
            &bootstrap_requested_record(),
            capability_digest(0x64),
            request_id(0x64),
        );
        let fixture = authorization_fixture();
        execute(&coordinator, authorize(&fixture, register(0x71))).unwrap();
        let stale_retry = authorize(&fixture, register(0x72));
        coordinator.shutdown().unwrap();
        let clock: Arc<dyn AuthorizationClock> = if failed_clock {
            Arc::new(FailingAuthorizationClock::new())
        } else {
            Arc::new(ValueAuthorizationClock(timestamp(BASE_SECONDS + 900)))
        };
        let coordinator = start_coordinator(
            database.open(),
            Arc::new(CountingAdministrationClock::working()),
            clock,
        );
        assert_eq!(
            execute(&coordinator, stale_retry).unwrap_err().kind(),
            if failed_clock {
                ControlPlaneExecutionErrorKind::StorageUnavailable
            } else {
                ControlPlaneExecutionErrorKind::AuthorizationDenied
            }
        );
        assert_eq!(
            coordinator.control_plane_executor().lifecycle_state(),
            CoordinatorLifecycleState::Accepting
        );
        coordinator.shutdown().unwrap();
        assert_eq!(records(&database.open()).len(), 1);
    }
}

#[test]
fn registration_maintenance_uses_fresh_administration_time_and_refuses_clock_outage() {
    for failing in [false, true] {
        let database = TestDatabase::create("registration-maintenance-clock");
        let clock = Arc::new(if failing {
            CountingAdministrationClock::failing()
        } else {
            CountingAdministrationClock::working()
        });
        let coordinator = start_coordinator(
            database.open(),
            clock.clone(),
            Arc::new(CountingAuthorizationClock::new()),
        );
        for call in 1..=2 {
            let result = block_on(
                block_on(coordinator.control_plane_executor().reserve_capacity())
                    .unwrap()
                    .submit_replication_maintenance()
                    .unwrap()
                    .completion(),
            );
            if failing {
                assert_eq!(
                    result.unwrap_err().kind(),
                    ControlPlaneExecutionErrorKind::StorageUnavailable
                );
            } else {
                assert_eq!(
                    result.unwrap(),
                    riffdb_storage_api::ReplicationRegistrationMaintenanceResultV1::Idle
                );
            }
            assert_eq!(clock.calls(), call);
        }
        coordinator.shutdown().unwrap();
        assert!(records(&database.open()).is_empty());
    }
}
