//! Real native ordinary publication through the receipt-owning storage boundary.
// req: REC-001, REC-004, REC-005, REC-006, REC-008, END-001
use super::*;
use riffdb_storage_api::*;
use riffdb_types::{
    ActorId, ActorKind, CapabilityId, DigestKeyId, OfflineMaintenanceOperationKind,
    OfflineMaintenanceReplacementConfirmation, Timestamp, offline_maintenance_input_hash,
};
use std::os::unix::fs::MetadataExt;

pub(in crate::maintenance) struct NativeTransferFixture {
    pub(in crate::maintenance) storage: crate::RedbMaintenanceStorage,
    pub(in crate::maintenance) sealed: RedbSealedStagedRestore,
    pub(in crate::maintenance) immutable:
        (OfflineBackupManifestV1, OfflineBackupManifestIdentityV1),
    pub(in crate::maintenance) configured: PathBuf,
    pub(in crate::maintenance) named: PathBuf,
    pub(in crate::maintenance) operation: OfflineMaintenanceOperationId,
}

pub(in crate::maintenance) fn native_transfer_fixture(scope: &Path) -> NativeTransferFixture {
    native_transfer_fixture_with_history(scope, false)
}

pub(in crate::maintenance) fn native_transfer_fixture_with_history(
    scope: &Path,
    sealed_history: bool,
) -> NativeTransferFixture {
    native_transfer_fixture_inner(scope, sealed_history, false, drop, None)
}

pub(in crate::maintenance) fn native_transfer_fixture_with_prepared_source(
    scope: &Path,
    sealed_history: bool,
    prepare: fn(crate::RedbStore),
    observe_materialization: Option<fn(usize)>,
) -> NativeTransferFixture {
    native_transfer_fixture_inner(
        scope,
        sealed_history,
        true,
        prepare,
        observe_materialization,
    )
}

fn native_transfer_fixture_inner(
    scope: &Path,
    sealed_history: bool,
    prepared_source: bool,
    prepare: fn(crate::RedbStore),
    observe_materialization: Option<fn(usize)>,
) -> NativeTransferFixture {
    let source = scope.join("source.redb");
    let mut store = crate::RedbStore::open(&source).unwrap();
    store
        .initialize_database(
            DatabaseId::from_unix_milliseconds_and_random(1234, [0x78; 10]).unwrap(),
        )
        .unwrap();
    prepare(store);
    if prepared_source {
        let database = redb::Database::open(&source).unwrap();
        assert!(
            crate::startup::private_restore_image_supports_bounded_readiness(&database).unwrap(),
            "retained-owner fixture must complete the actual ordinary source startup"
        );
        drop(database);
        let journal = crate::journal::journal_path(&source);
        if !journal.exists() {
            // Immediate-only sources have no active journal. Use the genuine
            // public backup's bound empty extent, as the readiness fixtures do.
            let initial = scope.join("prepared-source-backup");
            crate::RedbOfflineBackup::bind(&source, &initial)
                .create_offline_backup(
                    &BackupBuildMetadataV1::new("0.1.0", "abc", "rustc", 1, vec![]).unwrap(),
                )
                .unwrap();
            fs::copy(
                initial.join(crate::backup::JOURNAL_ARTIFACT_FILE_NAME),
                &journal,
            )
            .unwrap();
            fs::remove_dir_all(initial).unwrap();
        }
        assert!(
            crate::journal::extent_uses_configured_capacity_with_media(
                &crate::media::RealJournalMedia,
                &crate::journal::journal_path(&source),
            )
            .unwrap()
        );
    }
    if sealed_history {
        let id = DatabaseId::from_unix_milliseconds_and_random(1234, [0x78; 10]).unwrap();
        let journal = crate::journal::journal_path(&source);
        if !prepared_source {
            crate::journal::reset_journal(
                &journal,
                &crate::journal::JournalFileHeader::new(id, None, [0; 32]),
            )
            .unwrap();
        }
        fs::rename(&journal, crate::journal::sealing_journal_path(&source)).unwrap();
        crate::journal::seal_extent_with_media(&crate::media::RealJournalMedia, &source, id)
            .unwrap()
            .unwrap();
        crate::journal::recreate_missing_active_with_media(
            &crate::media::RealJournalMedia,
            &source,
        )
        .unwrap();
    }
    let backup = scope.join("backup");
    crate::RedbOfflineBackup::bind(&source, &backup)
        .create_offline_backup(
            &BackupBuildMetadataV1::new("0.1.0", "abc", "rustc", 1, vec![]).unwrap(),
        )
        .unwrap();
    let backups = scope.join("backups");
    fs::create_dir(&backups).unwrap();
    let backup_name = BackupNameV1::new("ordinary-transfer").unwrap();
    let named = backups.join(backup_name.as_str());
    fs::rename(&backup, &named).unwrap();
    let immutable = validate_immutable_backup(&named).unwrap();
    let configured = scope.join("configured.redb");
    let (mut storage, _) = crate::RedbMaintenanceStorage::open(&configured, &backups).unwrap();
    let operation =
        OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1234, [0x69; 10]).unwrap();
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
    let inputs = StartupValidationInputs::new(
        Timestamp::new(1, 0).unwrap(),
        ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
        ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
    );
    let before_materialization = crate::backup::BACKUP_CONTENT_PASSES.with(std::cell::Cell::get);
    let mut stage = storage
        .stage_owned_recovery_restore_candidate(operation, &backup_name, inputs)
        .unwrap();
    if let Some(observe) = observe_materialization {
        observe(
            crate::backup::BACKUP_CONTENT_PASSES.with(std::cell::Cell::get)
                - before_materialization,
        );
    }
    drop(stage.take_owned_authorization_evidence().unwrap());
    let database = stage.manifest_identity().database_id();
    let manifest = stage.manifest_identity().clone();
    let mut sealed = stage.prepare_receipt_authorized_stamp(database).unwrap();
    let confirmation = OfflineMaintenanceReplacementConfirmation::NotProvided;
    let kind = OfflineMaintenanceOperationKind::RestoreBackup;
    let mut receipt = OfflineMaintenanceReceiptV1::accepted(
        operation,
        kind,
        backup_name.clone(),
        offline_maintenance_input_hash(kind, &backup_name, confirmation),
        confirmation,
        OfflineMaintenanceAdmissionV1::new(
            ActorId::new("operator").unwrap(),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(1234, [0x6a; 10]).unwrap(),
            None,
        ),
    )
    .unwrap();
    storage.create_or_read_receipt(&receipt).unwrap();
    for phase in [
        OfflineMaintenanceReceiptPhaseV1::Draining,
        OfflineMaintenanceReceiptPhaseV1::Offline,
    ] {
        receipt
            .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
            .unwrap();
        storage.replace_receipt(&receipt).unwrap();
    }
    receipt.record_staged_database_id(database).unwrap();
    receipt.record_manifest_identity(manifest).unwrap();
    receipt.record_published_incarnation(2).unwrap();
    storage.replace_receipt(&receipt).unwrap();
    sealed.apply_published_history_incarnation(2).unwrap();
    NativeTransferFixture {
        storage,
        sealed,
        immutable,
        configured,
        named,
        operation,
    }
}

#[test]
fn native_empty_restore_publication_transfers_original_sealed_artifacts() {
    let scope = crate::test_path::ScopedDirectory::new("native-publication-transfer");
    let root = scope.join("fixture");
    fs::create_dir(&root).unwrap();
    let NativeTransferFixture {
        mut storage,
        sealed,
        immutable,
        configured,
        named,
        operation,
    } = native_transfer_fixture(&root);
    let backups = root.join("backups");
    let originals = [
        sealed.staged_database_file.clone(),
        sealed.staged_journal_file.clone(),
        sealed.staged_format_marker_file.clone(),
    ];
    let target_paths = [
        configured.clone(),
        crate::journal::journal_path(&configured),
        crate::durable_format_marker_path(&configured),
    ];
    let expected: Vec<_> = originals
        .iter()
        .map(|path| {
            let metadata = fs::metadata(path).unwrap();
            (
                (metadata.dev(), metadata.ino(), metadata.len()),
                sha256_file(path).unwrap(),
            )
        })
        .collect();
    assert!(matches!(
        storage
            .publish_sealed_restore(sealed, OfflineRestoreOverwritePolicyV1::RefuseNonEmpty)
            .unwrap(),
        OfflineRestoreResultV1::Restored { .. }
    ));
    assert_eq!(validate_immutable_backup(&named).unwrap(), immutable);
    let intent_path = backups
        .join(".maintenance/receipts")
        .join(format!("{operation}.publication-intent-v1"));
    let intent =
        crate::maintenance::PublicationIntent::decode(&fs::read(&intent_path).unwrap()).unwrap();
    assert_eq!(
        intent.phase,
        crate::maintenance::codec::publication_intent::PublicationPhase::Published
    );
    assert_eq!(intent.operation_id, operation);
    assert_eq!(intent.manifest, immutable.1);
    assert_eq!(intent.incarnation, 2);
    assert_eq!(intent.history.lineage().history_incarnation(), 2);
    let inventory = storage.validate_receipt_inventory().unwrap();
    assert_eq!(
        inventory.receipts()[0].current_phase(),
        OfflineMaintenanceReceiptPhaseV1::Offline,
        "Published intent never advances the ordinary receipt or grants readiness"
    );
    for (((original, target), (identity, seal)), durable_seal) in originals
        .iter()
        .zip(&target_paths)
        .zip(expected)
        .zip(&intent.artifacts)
    {
        let metadata = fs::metadata(target).unwrap();
        assert_eq!(
            (metadata.dev(), metadata.ino(), metadata.len()),
            identity,
            "native publication must transfer the original sealed artifact without copying"
        );
        assert_eq!(sha256_file(target).unwrap(), seal);
        assert_eq!(durable_seal.checksum, seal);
        assert_eq!(durable_seal.length, metadata.len());
        assert!(
            !original.exists(),
            "each transferred artifact has one owned location"
        );
        assert_eq!(
            metadata.nlink(),
            1,
            "transfer cannot leave a writable hard-link alias"
        );
    }
    assert!(
        backups
            .join(".maintenance/receipts")
            .join(format!("{operation}.publication-intent-v1"))
            .is_file()
    );
}

#[test]
fn native_transfer_reconciliation_resumes_owned_partial_transfer_without_copy_proof() {
    let scope = crate::test_path::ScopedDirectory::new("native-transfer-reconciliation");
    let root = scope.join("fixture");
    fs::create_dir(&root).unwrap();
    let NativeTransferFixture {
        mut storage,
        mut sealed,
        configured,
        operation,
        ..
    } = native_transfer_fixture(&root);
    let original = sealed.staged_database_file.clone();
    sealed.test_controller = Some(RedbMaintenanceTestController::return_at(
        RedbMaintenanceFailpoint::BeforeTargetPublication,
    ));
    assert_eq!(
        storage
            .publish_native_transfer_for_qualification(sealed)
            .unwrap_err()
            .kind(),
        StorageErrorKind::CommitStatusUnknown
    );
    // This is the exact allowed position after the first durable transfer move.
    let temporary = configured.parent().unwrap().join(target_temporary_name(
        configured.file_name().unwrap(),
        operation,
    ));
    fs::rename(&original, &temporary).unwrap();
    let bytes = sha256_file(&temporary).unwrap();
    let before = fs::metadata(&temporary).unwrap();
    let result = storage.reconcile().unwrap();
    assert!(!temporary.exists());
    assert_eq!(sha256_file(&configured).unwrap(), bytes);
    let after = fs::metadata(&configured).unwrap();
    assert_eq!((before.dev(), before.ino()), (after.dev(), after.ino()));
    let evidence = result
        .operations()
        .iter()
        .find(|r| r.operation_id() == operation)
        .unwrap();
    assert!(evidence.native_publication_target_owned());
    assert!(
        !evidence.configured_target_matches(),
        "native ownership cannot certify copied-stage validation or readiness"
    );
}

pub(in crate::maintenance) fn prepared_transfer_fixture(
    root: &Path,
) -> (
    crate::RedbMaintenanceStorage,
    PathBuf,
    PathBuf,
    OfflineMaintenanceOperationId,
) {
    let NativeTransferFixture {
        storage,
        mut sealed,
        configured,
        named,
        operation,
        ..
    } = native_transfer_fixture(root);
    sealed.test_controller = Some(RedbMaintenanceTestController::return_at(
        RedbMaintenanceFailpoint::BeforeTargetPublication,
    ));
    assert_eq!(
        storage
            .publish_native_transfer_for_qualification(sealed)
            .unwrap_err()
            .kind(),
        StorageErrorKind::CommitStatusUnknown
    );
    (storage, configured, named, operation)
}

pub(in crate::maintenance) fn arm_native_transfer_fixture(
    fixture: &mut NativeTransferFixture,
    controller: RedbMaintenanceTestController,
) {
    fixture.sealed.test_controller = Some(controller);
}
