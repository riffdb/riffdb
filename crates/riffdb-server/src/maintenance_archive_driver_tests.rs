//! V3 archive receipts use the real exclusive maintenance lifecycle.
// req: REP-007, AFC-007
use super::tests::fixture_admission;
use super::*;
use riffdb_storage_api::{OfflineArchiveReceiptPersistencePort, OfflineMaintenanceReceiptV3};
use riffdb_types::*;

fn archive_receipt(seed: u8, source: Option<DatabaseId>) -> OfflineMaintenanceReceiptV3 {
    let backup = BackupNameV1::new("baseline").unwrap();
    let archive = ArchiveNameV1::new("daily").unwrap();
    let stop = ArchiveRestoreStopV1::LastArchived;
    let confirmation = OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget;
    OfflineMaintenanceReceiptV3::accepted_archive_restore(
        OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1000, [seed; 10]).unwrap(),
        backup.clone(),
        archive.clone(),
        stop,
        archive_restore_input_hash(&backup, &archive, stop, confirmation),
        confirmation,
        fixture_admission(seed),
        source,
    )
    .unwrap()
}

#[test]
fn archive_driver_persists_drain_and_offline_before_replay() {
    let root = tempfile::tempdir().unwrap();
    let (mut storage, _) =
        RedbMaintenanceStorage::open(root.path().join("target.redb"), root.path().join("backups"))
            .unwrap();
    let receipt = archive_receipt(
        87,
        Some(DatabaseId::from_unix_milliseconds_and_random(1000, [88; 10]).unwrap()),
    );
    storage.create_or_read_archive_receipt(&receipt).unwrap();
    let lifecycle = MaintenanceLifecycle::ready();
    lifecycle.begin(receipt.operation_id()).unwrap();
    mark_draining(&mut storage, &lifecycle, receipt.operation_id()).unwrap();
    assert_eq!(
        storage
            .read_archive_receipt(receipt.operation_id())
            .unwrap()
            .unwrap()
            .current_phase(),
        OfflineMaintenanceReceiptPhaseV1::Draining
    );
    mark_offline(&mut storage, &lifecycle, receipt.operation_id()).unwrap();
    assert_eq!(lifecycle.stage(), MaintenanceLifecycleStage::Offline);
    assert_eq!(
        storage
            .read_archive_receipt(receipt.operation_id())
            .unwrap()
            .unwrap()
            .current_phase(),
        OfflineMaintenanceReceiptPhaseV1::Offline
    );
    assert!(!storage.configured_database_file().exists());
}

struct ArchiveFixture {
    _root: tempfile::TempDir,
    storage: RedbMaintenanceStorage,
    receipt: OfflineMaintenanceReceiptV3,
    archives: Vec<(std::path::PathBuf, crate::config::ConfiguredArchive)>,
    token: [u8; 43],
}

fn restore_fixture(
    dependencies: &MaintenanceDriverDependencies<'_>,
    approval_required: bool,
) -> ArchiveFixture {
    use riffdb_auth::{SystemEntropy, issue_capability_token};
    use riffdb_storage_api::*;
    use std::num::{NonZeroU16, NonZeroU32};
    let root = tempfile::tempdir().unwrap();
    let target = root.path().join("target.redb");
    let backups = root.path().join("backups");
    let archive = root.path().join("archive");
    let startup = open_redb_startup_with_commit_profile(
        &target,
        dependencies.startup_inputs.clone(),
        &dependencies.database_ids,
        dependencies.application_commit_profile,
    )
    .unwrap();
    let database = startup.database_id();
    drop(startup);
    std::fs::create_dir(&backups).unwrap();
    riffdb_storage_redb::RedbOfflineBackup::bind(&target, backups.join("baseline"))
        .create_offline_backup(&production_backup_build_metadata().unwrap())
        .unwrap();
    let binding =
        riffdb_storage_redb::RedbVerifiedArchiveBackup::open(&backups.join("baseline")).unwrap();
    let history = binding.history();
    let repository = binding
        .open_archive(&archive, ArchiveEncryptionPostureV1::Unencrypted)
        .unwrap();
    drop(binding);
    let startup = open_redb_startup_with_commit_profile(
        &target,
        dependencies.startup_inputs.clone(),
        &dependencies.database_ids,
        dependencies.application_commit_profile,
    )
    .unwrap();
    let (_, _, _, _, _, mut ports) = startup.into_parts();
    let issued = issue_capability_token(&SystemEntropy, &dependencies.capability_keys).unwrap();
    let token = issued.text().expose_secret().to_owned();
    let capability = CapabilityId::from_unix_milliseconds_and_random(1000, [91; 10]).unwrap();
    let intent = CapabilityBootstrapIntentV1::new(
        capability,
        CapabilityRequestedRecordV1::new(
            database,
            dependencies.environment.clone(),
            ActorId::new("restore-operator").unwrap(),
            ActorKind::Human,
            NonZeroU32::new(3600).unwrap(),
            vec![dependencies.grpc_audience.clone()],
            CapabilityGrantV1::new(
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
            .unwrap(),
        )
        .unwrap(),
        BootstrapDigestCandidatesV1::new(vec![issued.digest()], issued.digest()).unwrap(),
        Timestamp::new(1_700_000_000, 0).unwrap(),
        Timestamp::new(1_700_003_600, 0).unwrap(),
        BootstrapServiceAuditStartV1::new(
            RequestId::from_unix_milliseconds_and_random(1000, [92; 10]).unwrap(),
            Timestamp::new(1_700_000_000, 0).unwrap(),
            ServiceIngressKindV1::Grpc,
            ServiceAuditTargetsV1::new([ServiceAuditTargetV1::Capability(capability)]).unwrap(),
            None,
        )
        .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        ports.bootstrap_capability(&intent).unwrap(),
        CapabilityBootstrapResult::BootstrapCreated { .. }
    ));
    // The backup predates the grant: successful staged authorization proves
    // the driver really replayed this original administrative suffix.
    let snapshot = ports.published_changelog_snapshot_v3().unwrap();
    let mut receipts = snapshot
        .changelog_receipts_v3(history.lineage(), history.tail())
        .unwrap();
    let mut consumer = ArchiveConsumerV1::new(repository, history.lineage(), history.tail());
    let mut before = history.tail();
    let mut count = 0;
    while let Some(receipt) = receipts.next_receipt().unwrap() {
        count += 1;
        assert!(count <= 4);
        let frame = ChangelogFrameV3::new(
            ChangelogFrameBindingV3::new(
                database,
                history.lineage().history_incarnation(),
                history.lineage().leadership_epoch().get(),
                history.lineage().catalog_digest(),
                before.history_hash(),
            )
            .unwrap(),
            vec![receipt],
        )
        .unwrap();
        consumer.begin_stream().unwrap();
        before = consumer.append(frame.encode().unwrap()).unwrap();
    }
    assert!(count >= 1);
    drop(consumer);
    drop(receipts);
    drop(snapshot);
    drop(ports);
    let document = root.path().join("config.toml");
    std::fs::write(&document, format!("[server]\ndatabase = '{}'\n[maintenance]\nbackup_root = '{}'\n[[maintenance.archives]]\nname = 'daily'\npath = '{}'\nencryption = 'unencrypted'\n", target.display(), backups.display(), archive.display())).unwrap();
    let config =
        crate::config::ServerConfig::parse(["--config".into(), document.into_os_string()]).unwrap();
    let (mut storage, _) = RedbMaintenanceStorage::open(&target, &backups).unwrap();
    let receipt = archive_receipt(93, Some(database));
    storage.create_or_read_archive_receipt(&receipt).unwrap();
    ArchiveFixture {
        _root: root,
        storage,
        receipt,
        archives: config.archive_bindings().unwrap(),
        token,
    }
}

fn offline(fixture: &mut ArchiveFixture) -> MaintenanceLifecycle {
    let lifecycle = MaintenanceLifecycle::ready();
    lifecycle.begin(fixture.receipt.operation_id()).unwrap();
    mark_draining(
        &mut fixture.storage,
        &lifecycle,
        fixture.receipt.operation_id(),
    )
    .unwrap();
    mark_offline(
        &mut fixture.storage,
        &lifecycle,
        fixture.receipt.operation_id(),
    )
    .unwrap();
    lifecycle
}

#[test]
fn archive_driver_authorizes_frozen_stage_and_validates_published_database() {
    let clocks =
        ProductionWallClocks::settable(Arc::new(std::sync::atomic::AtomicI64::new(1_700_000_001)));
    let recovery = MaintenanceRecoveryController::disabled();
    let mut dependencies = super::tests::fixture_dependencies(&clocks, &recovery, None);
    let mut fixture = restore_fixture(&dependencies, false);
    dependencies.archives = fixture.archives.clone();
    let lifecycle = offline(&mut fixture);
    let id = fixture.receipt.operation_id();
    let success = run_offline_maintenance(
        &mut fixture.storage,
        &lifecycle,
        &dependencies,
        MaintenanceDriverRequest::restore_backup(
            id,
            RetainedOpaqueCredential::new(&fixture.token).unwrap(),
        ),
    )
    .unwrap();
    let MaintenanceTerminalReceipt::V3(receipt) = success.receipt else {
        panic!("V3 receipt required");
    };
    assert_eq!(
        receipt.current_phase(),
        OfflineMaintenanceReceiptPhaseV1::Succeeded
    );
    assert!(receipt.selection().is_some());
    assert_eq!(receipt.published_history_incarnation(), Some(2));
    assert_eq!(
        success.startup.database_id(),
        receipt.staged_database_id().unwrap()
    );
    assert_eq!(success.startup.retained_metadata().history_incarnation(), 2);
    assert_eq!(
        fixture.storage.read_archive_receipt(id).unwrap().unwrap(),
        receipt
    );
}

#[test]
fn archive_driver_refuses_unknown_archive_missing_credential_and_required_approval() {
    for mode in [
        "unknown-archive",
        "missing-archive",
        "no-credential",
        "required-approval",
    ] {
        let clocks = ProductionWallClocks::settable(Arc::new(std::sync::atomic::AtomicI64::new(
            1_700_000_001,
        )));
        let recovery = MaintenanceRecoveryController::disabled();
        let mut dependencies = super::tests::fixture_dependencies(&clocks, &recovery, None);
        let mut fixture = restore_fixture(&dependencies, mode == "required-approval");
        if mode != "unknown-archive" {
            dependencies.archives = fixture.archives.clone();
        }
        if mode == "missing-archive" {
            std::fs::remove_dir_all(fixture.archives[0].1.path()).unwrap();
        }
        let before = std::fs::read(fixture.storage.configured_database_file()).unwrap();
        let lifecycle = offline(&mut fixture);
        let id = fixture.receipt.operation_id();
        let request = if mode == "no-credential" {
            MaintenanceDriverRequest::resume_published_restore(id)
        } else {
            MaintenanceDriverRequest::restore_backup(
                id,
                RetainedOpaqueCredential::new(&fixture.token).unwrap(),
            )
        };
        assert!(
            run_offline_maintenance(&mut fixture.storage, &lifecycle, &dependencies, request)
                .is_err(),
            "{mode}"
        );
        assert_eq!(
            std::fs::read(fixture.storage.configured_database_file()).unwrap(),
            before,
            "{mode}"
        );
        let receipt = fixture.storage.read_archive_receipt(id).unwrap().unwrap();
        assert_eq!(
            receipt.current_phase(),
            OfflineMaintenanceReceiptPhaseV1::FailedClosed
        );
        assert_eq!(receipt.selection().is_some(), mode == "required-approval");
        assert!(receipt.published_history_incarnation().is_none());
        if mode == "missing-archive" {
            assert!(!fixture.archives[0].1.path().exists());
        }
    }
}

fn publish_before_phase_record(
    fixture: &mut ArchiveFixture,
    dependencies: &MaintenanceDriverDependencies<'_>,
) {
    use riffdb_storage_api::*;
    let id = fixture.receipt.operation_id();
    let archive = &fixture.archives[0].1;
    let repository = fixture
        .storage
        .open_archive_restore_repository(id, archive.path(), archive.encryption())
        .unwrap();
    let stage = fixture
        .storage
        .stage_archive_restore(id, dependencies.startup_inputs.clone())
        .unwrap()
        .begin_archive_replay(repository, &std::sync::atomic::AtomicBool::new(false))
        .unwrap();
    let mut receipt = fixture.storage.read_archive_receipt(id).unwrap().unwrap();
    receipt.record_selection(stage.selection().clone()).unwrap();
    fixture.storage.replace_archive_receipt(&receipt).unwrap();
    let prepared = stage
        .prepare_restore(
            receipt.stop(),
            dependencies.startup_inputs.clone(),
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )
        .unwrap();
    let database = prepared.selection().lineage().database_id();
    authorize_staged_restore(
        prepared.authorization_snapshot().unwrap(),
        database,
        RetainedOpaqueCredential::new(&fixture.token).unwrap(),
        id,
        receipt.input_hash(),
        dependencies,
    )
    .unwrap();
    receipt
        .record_validated_restore(database, prepared.restored_frontier())
        .unwrap();
    receipt.record_published_incarnation(2).unwrap();
    fixture.storage.replace_archive_receipt(&receipt).unwrap();
    let sealed = prepared.seal_after_authorization(database).unwrap();
    fixture
        .storage
        .publish_sealed_archive_restore(sealed)
        .unwrap();
    assert!(fixture.storage.archive_restore_target_matches(id).unwrap());
}

#[test]
fn archive_driver_resumes_exact_publication_without_credential_or_archive_access() {
    for phase in [
        OfflineMaintenanceReceiptPhaseV1::Offline,
        OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
        OfflineMaintenanceReceiptPhaseV1::Validating,
    ] {
        let clocks = ProductionWallClocks::settable(Arc::new(std::sync::atomic::AtomicI64::new(
            1_700_000_001,
        )));
        let recovery = MaintenanceRecoveryController::disabled();
        let dependencies = super::tests::fixture_dependencies(&clocks, &recovery, None);
        let mut fixture = restore_fixture(&dependencies, false);
        let _previous_lifecycle = offline(&mut fixture);
        publish_before_phase_record(&mut fixture, &dependencies);
        let id = fixture.receipt.operation_id();
        let mut receipt = fixture.storage.read_archive_receipt(id).unwrap().unwrap();
        for next in [
            OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
            OfflineMaintenanceReceiptPhaseV1::Validating,
        ] {
            if receipt.current_phase() == phase {
                break;
            }
            receipt
                .advance(OfflineMaintenanceReceiptTransitionV1::phase(next))
                .unwrap();
            fixture.storage.replace_archive_receipt(&receipt).unwrap();
        }
        // Exact publication evidence survives archive loss; no credential-free
        // replay is possible because dependencies have no archive bindings.
        std::fs::remove_dir_all(fixture.archives[0].1.path()).unwrap();
        let lifecycle = offline(&mut fixture);
        let success = run_offline_maintenance(
            &mut fixture.storage,
            &lifecycle,
            &dependencies,
            MaintenanceDriverRequest::resume_published_restore(id),
        )
        .unwrap();
        assert_eq!(success.startup.retained_metadata().history_incarnation(), 2);
        assert_eq!(
            fixture
                .storage
                .read_archive_receipt(id)
                .unwrap()
                .unwrap()
                .current_phase(),
            OfflineMaintenanceReceiptPhaseV1::Succeeded
        );
    }
}

#[cfg(feature = "test-fixtures")]
#[test]
fn archive_driver_validation_crash_child() {
    let Ok(root) = std::env::var("RIFFDB_ARCHIVE_DRIVER_CRASH_ROOT") else {
        return;
    };
    let root = std::path::Path::new(&root);
    let clocks =
        ProductionWallClocks::settable(Arc::new(std::sync::atomic::AtomicI64::new(1_700_000_001)));
    let recovery =
        MaintenanceRecoveryController::armed(MaintenanceRecoveryBoundary::FreshValidationComplete);
    let dependencies = super::tests::fixture_dependencies(&clocks, &recovery, None);
    let (mut storage, _) =
        RedbMaintenanceStorage::open(root.join("target.redb"), root.join("backups")).unwrap();
    let id = archive_receipt(93, None).operation_id();
    let lifecycle = MaintenanceLifecycle::ready();
    lifecycle.begin(id).unwrap();
    mark_draining(&mut storage, &lifecycle, id).unwrap();
    mark_offline(&mut storage, &lifecycle, id).unwrap();
    let result = run_offline_maintenance(
        &mut storage,
        &lifecycle,
        &dependencies,
        MaintenanceDriverRequest::resume_published_restore(id),
    );
    panic!("expected process abort, got {result:?}");
}

#[cfg(feature = "test-fixtures")]
#[test]
fn archive_driver_recovers_after_process_crash_in_fresh_validation() {
    let clocks =
        ProductionWallClocks::settable(Arc::new(std::sync::atomic::AtomicI64::new(1_700_000_001)));
    let recovery = MaintenanceRecoveryController::disabled();
    let dependencies = super::tests::fixture_dependencies(&clocks, &recovery, None);
    let mut fixture = restore_fixture(&dependencies, false);
    let _previous_lifecycle = offline(&mut fixture);
    publish_before_phase_record(&mut fixture, &dependencies);
    let id = fixture.receipt.operation_id();
    let root = fixture._root.path().to_path_buf();
    drop(fixture.storage);
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "maintenance_driver::archive_tests::archive_driver_validation_crash_child",
            "--nocapture",
        ])
        .env("RIFFDB_ARCHIVE_DRIVER_CRASH_ROOT", &root)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(6));
    }
    #[cfg(not(unix))]
    assert!(!status.success());
    let (mut storage, _) =
        RedbMaintenanceStorage::open(root.join("target.redb"), root.join("backups")).unwrap();
    assert_eq!(
        storage
            .read_archive_receipt(id)
            .unwrap()
            .unwrap()
            .current_phase(),
        OfflineMaintenanceReceiptPhaseV1::Validating
    );
    let lifecycle = MaintenanceLifecycle::ready();
    lifecycle.begin(id).unwrap();
    mark_draining(&mut storage, &lifecycle, id).unwrap();
    mark_offline(&mut storage, &lifecycle, id).unwrap();
    let success = run_offline_maintenance(
        &mut storage,
        &lifecycle,
        &dependencies,
        MaintenanceDriverRequest::resume_published_restore(id),
    )
    .unwrap();
    assert_eq!(success.startup.retained_metadata().history_incarnation(), 2);
    assert_eq!(
        storage
            .read_archive_receipt(id)
            .unwrap()
            .unwrap()
            .current_phase(),
        OfflineMaintenanceReceiptPhaseV1::Succeeded
    );
}

#[test]
fn archive_driver_refuses_non_startup_receipts_after_publication() {
    let clocks =
        ProductionWallClocks::settable(Arc::new(std::sync::atomic::AtomicI64::new(1_700_000_001)));
    let recovery = MaintenanceRecoveryController::disabled();
    let dependencies = super::tests::fixture_dependencies(&clocks, &recovery, None);
    let mut fixture = restore_fixture(&dependencies, false);
    let _previous_lifecycle = offline(&mut fixture);
    publish_before_phase_record(&mut fixture, &dependencies);
    let id = fixture.receipt.operation_id();
    let mut receipt = fixture.storage.read_archive_receipt(id).unwrap().unwrap();
    receipt
        .advance(OfflineMaintenanceReceiptTransitionV1::phase(
            OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
        ))
        .unwrap();
    fixture.storage.replace_archive_receipt(&receipt).unwrap();
    let startup = open_redb_startup_with_commit_profile(
        fixture.storage.configured_database_file(),
        dependencies.startup_inputs.clone(),
        &dependencies.database_ids,
        dependencies.application_commit_profile,
    )
    .unwrap();
    let (_, _, _, _, _, ports) = startup.into_parts();
    ports.write_clean_close_lifecycle().unwrap();
    drop(ports);
    let lifecycle = offline(&mut fixture);
    assert!(
        run_offline_maintenance(
            &mut fixture.storage,
            &lifecycle,
            &dependencies,
            MaintenanceDriverRequest::resume_published_restore(id)
        )
        .is_err()
    );
    assert_eq!(
        fixture
            .storage
            .read_archive_receipt(id)
            .unwrap()
            .unwrap()
            .current_phase(),
        OfflineMaintenanceReceiptPhaseV1::FailedClosed
    );
}
