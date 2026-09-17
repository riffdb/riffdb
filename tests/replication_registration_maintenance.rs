//! Production command heads drive policy health before configured expiry.
// req: REP-006, REC-001, STO-012
use super::*;
use riffdb_storage_api::{
    ReplicationAdministrationAwaitingDecision, ReplicationAdministrationCandidateTransaction,
    ReplicationAdministrationCandidateV1, ReplicationAdministrationIntentV1,
    ReplicationAdministrationRequestV1, ReplicationAdministrationResultV1,
    ReplicationAdministrationTransactionPort, ReplicationRegistrationMaintenancePort,
    ReplicationRegistrationMaintenanceResultV1 as Maintenance,
};
use riffdb_types::ReplicationFollowerAuditTargetV1;

fn open_registered(store: RedbStore) -> RedbOperationalPorts {
    let key = |id| ReadableDigestKey::v1(DigestKeyId::new(id).unwrap());
    let inputs = StartupValidationInputs::new(
        Timestamp::new(1_700_000_000, 0).unwrap(),
        ReadableCapabilityDigestInventory::new(vec![key(1), key(7)]).unwrap(),
        ReadableIdempotencyDigestInventory::new(vec![key(1)]).unwrap(),
    );
    let mut session = store.begin_structural_evidence(inputs).unwrap();
    let mut cursor =
        StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
    let end = loop {
        match session
            .read_structural_evidence(cursor, EvidencePageLimit::new(64).unwrap())
            .unwrap()
        {
            StructuralEvidencePage::Page { findings, next, .. } => {
                assert!(findings.is_empty(), "{findings:?}");
                cursor = next;
            }
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };
    let (catalog, historical) = validate_catalog_history(&mut session).unwrap().into_parts();
    assert!(matches!(catalog, CatalogHistoryOutcome::Ready(_)));
    let StructuralOpenOutcome::Clean(opened) = session.finish(end, historical).unwrap() else {
        panic!("current fixture needs no migration");
    };
    opened
        .into_parts()
        .3
        .into_operational_after_catalog_validation()
        .unwrap()
}

fn register(ports: &RedbOperationalPorts, capability: &StoredCapabilityRecordV1, expiry: bool) {
    let history = ports
        .published_changelog_snapshot_v3()
        .unwrap()
        .replication_source_progress_v3()
        .unwrap()
        .history();
    let candidate = ReplicationAdministrationCandidateV1::new(
        ReplicationAdministrationRequestV1::register(
            RequestId::from_bytes(uuid_bytes(0x76)).unwrap(),
            ReplicationFollowerAuditTargetV1::new(
                database_id(),
                history.lineage().history_incarnation(),
                history.lineage().leadership_epoch(),
                ReplicationSourceHoldIdV1::new([0x76; 16]).unwrap(),
            )
            .unwrap(),
            FollowerHoldBudget::new(1).unwrap(),
            expiry.then(|| CommitSequence::new(1).unwrap()),
        ),
        AuditPrincipalV1::new(
            capability.principal_id().clone(),
            capability.actor_kind(),
            capability.capability_id(),
            capability.revision(),
        ),
    );
    let (awaiting, _) = ports
        .begin_replication_administration(candidate.clone())
        .unwrap()
        .read_transaction_current()
        .unwrap();
    assert!(matches!(
        awaiting
            .commit(ReplicationAdministrationIntentV1::new(
                candidate,
                Timestamp::new(1_700_000_000, 0).unwrap()
            ))
            .unwrap(),
        ReplicationAdministrationResultV1::Applied(_)
    ));
}

#[test]
fn configured_expiry_persists_health_first_and_budget_alone_never_releases_retention() {
    for expiry in [false, true] {
        let path = TestDatabasePath::new("registration-maintenance");
        let (mut ports, capability) = prepare_protected_command_database(
            &path.0,
            RedbTestController::observe_index_migration(),
        );
        register(&ports, &capability, expiry);
        let now = Timestamp::new(1_700_000_020, 0).unwrap();
        assert_eq!(
            ports.maintain_replication_registration(now).unwrap(),
            Maintenance::Idle
        );
        let fixture = command_fixture_at(1);
        commit_command_fixture(&ports, &fixture);
        deliver_command_event(&mut ports, &fixture);
        let progress = ports
            .published_changelog_snapshot_v3()
            .unwrap()
            .replication_source_progress_v3()
            .unwrap();
        assert_eq!(progress.degraded_followers(), 1);
        assert!(progress.registration_maintenance_pending());
        let Maintenance::HealthRecorded(policy) =
            ports.maintain_replication_registration(now).unwrap()
        else {
            panic!("health before any release");
        };
        assert_eq!(policy.degraded_at(), Some(progress.history().tail()));
        assert_ne!(
            policy.phase(),
            riffdb_storage_api::FollowerRegistrationPhaseV1::Retired
        );
        assert_eq!(
            ports
                .published_changelog_snapshot_v3()
                .unwrap()
                .replication_source_progress_v3()
                .unwrap()
                .registration_maintenance_pending(),
            expiry
        );

        drop(ports);
        let retention = RedbOfflineRetention::bind(&path.0);
        assert_eq!(
            retention.status().unwrap().max_permissible_watermark,
            Some(0)
        );
        assert!(retention.prune_to(1).is_err());
        let ports = open_registered(RedbStore::open(&path.0).unwrap());
        let outcome = ports.maintain_replication_registration(now).unwrap();
        if expiry {
            let Maintenance::Expired(record) = outcome else {
                panic!("configured expiry after durable degradation");
            };
            assert_eq!(
                record.action(),
                riffdb_storage_api::ReplicationAdministrationActionV1::ExpireFollower
            );
            assert_eq!(record.after().generation(), policy.generation());
            assert_eq!(record.after().degraded_at(), policy.degraded_at());
            assert!(matches!(
                record.origin(),
                riffdb_storage_api::ReplicationAdministrationOriginV1::ConfiguredExpiry { .. }
            ));
        } else {
            assert_eq!(outcome, Maintenance::Idle);
        }
        assert_eq!(
            ports.maintain_replication_registration(now).unwrap(),
            Maintenance::Idle
        );
        drop(ports);
        let status = RedbOfflineRetention::bind(&path.0).status().unwrap();
        assert_eq!(status.max_permissible_watermark, Some(u64::from(expiry)));
    }
}

#[test]
fn registered_maintenance_crash_child() {
    let Some(path) = std::env::var_os("RIFFDB_REGISTRATION_MAINTENANCE_PATH") else {
        return;
    };
    let controller = if std::env::var("RIFFDB_REGISTRATION_MAINTENANCE_AFTER").unwrap() == "1" {
        RedbTestController::abort_after_commit(RedbTestOperation::RetentionHold)
    } else {
        RedbTestController::abort_before_commit(RedbTestOperation::RetentionHold)
    };
    let ports = open_registered(RedbStore::open_with_test_controller(path, controller).unwrap());
    ports
        .maintain_replication_registration(Timestamp::new(1_700_000_020, 0).unwrap())
        .unwrap();
    panic!("maintenance crash edge not reached");
}

#[test]
fn policy_health_and_expiry_crashes_preserve_atomic_evidence_and_retention() {
    for expiry_step in [false, true] {
        for committed in [false, true] {
            let path = TestDatabasePath::new("registration-maintenance-crash");
            let (mut ports, capability) = prepare_protected_command_database(
                &path.0,
                RedbTestController::observe_index_migration(),
            );
            register(&ports, &capability, true);
            let fixture = command_fixture_at(1);
            commit_command_fixture(&ports, &fixture);
            deliver_command_event(&mut ports, &fixture);
            let now = Timestamp::new(1_700_000_020, 0).unwrap();
            if expiry_step {
                assert!(matches!(
                    ports.maintain_replication_registration(now).unwrap(),
                    Maintenance::HealthRecorded(_)
                ));
            }
            let before = ports
                .published_changelog_snapshot_v3()
                .unwrap()
                .replication_source_progress_v3()
                .unwrap()
                .history();
            drop(ports);
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "replication_promotion::registration_maintenance::registered_maintenance_crash_child"])
                .env("RIFFDB_REGISTRATION_MAINTENANCE_PATH", &path.0)
                .env("RIFFDB_REGISTRATION_MAINTENANCE_AFTER", if committed { "1" } else { "0" })
                .stdin(std::process::Stdio::null()).stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null())
                .status().unwrap();
            assert_child_aborted(status, "registration maintenance");
            for _ in 0..2 {
                let ports = open_registered(RedbStore::open(&path.0).unwrap());
                let progress = ports
                    .published_changelog_snapshot_v3()
                    .unwrap()
                    .replication_source_progress_v3()
                    .unwrap();
                // Startup recovery may append physical control receipts; it must not
                // invent application or administration progress.
                assert!(
                    progress.history().tail().sequence().get()
                        >= before.tail().sequence().get() + u64::from(committed)
                );
                assert_eq!(
                    progress.history().tail().frontier().application(),
                    before.tail().frontier().application()
                );
                assert_eq!(
                    progress
                        .history()
                        .tail()
                        .frontier()
                        .administration()
                        .unwrap()
                        .get(),
                    before.tail().frontier().administration().unwrap().get()
                        + u64::from(committed && expiry_step)
                );
                assert_eq!(
                    progress.follower_count(),
                    u32::from(!(committed && expiry_step))
                );
                drop(ports);
                assert_eq!(
                    RedbOfflineRetention::bind(&path.0)
                        .status()
                        .unwrap()
                        .max_permissible_watermark,
                    Some(u64::from(committed && expiry_step))
                );
            }
            let ports = open_registered(RedbStore::open(&path.0).unwrap());
            let result = ports.maintain_replication_registration(now).unwrap();
            match (expiry_step, committed) {
                (true, true) => assert_eq!(result, Maintenance::Idle),
                (true, false) | (false, true) => assert!(matches!(result, Maintenance::Expired(_))),
                (false, false) => assert!(matches!(result, Maintenance::HealthRecorded(_))),
            }
        }
    }
}

#[test]
fn caught_up_registration_remains_degraded_at_expiry_before_audited_release() {
    let path = TestDatabasePath::new("caught-up-registration-expiry");
    let (mut ports, capability) =
        prepare_protected_command_database(&path.0, RedbTestController::observe_index_migration());
    register(&ports, &capability, true);
    let attached = attach_follower(&ports, &path.0.with_extension("bootstrap"), 0x76);
    let fixture = command_fixture_at(1);
    commit_command_fixture(&ports, &fixture);
    deliver_command_event(&mut ports, &fixture);
    let head = ports
        .published_changelog_snapshot_v3()
        .unwrap()
        .replication_source_progress_v3()
        .unwrap()
        .history();
    ports
        .acknowledge_replication_follower_v3(
            attached.fence().hold_id(),
            head.lineage(),
            head.tail(),
        )
        .unwrap();
    let progress = ports
        .published_changelog_snapshot_v3()
        .unwrap()
        .replication_source_progress_v3()
        .unwrap();
    assert_eq!(
        progress.oldest_acknowledged().unwrap().frontier(),
        progress.history().tail().frontier()
    );
    assert_eq!(progress.follower_count(), 1);
    assert_eq!(progress.degraded_followers(), 1);
    let Maintenance::Expired(record) = ports
        .maintain_replication_registration(Timestamp::new(1_700_000_020, 0).unwrap())
        .unwrap()
    else {
        panic!("acknowledgement persisted expiry degradation before release");
    };
    assert!(record.after().degraded_at().is_some());
    assert!(
        record
            .after()
            .degraded_at()
            .unwrap()
            .precedes_or_equals(progress.history().tail())
    );
    assert_eq!(
        ports
            .published_changelog_snapshot_v3()
            .unwrap()
            .replication_source_progress_v3()
            .unwrap()
            .follower_count(),
        0
    );
}
