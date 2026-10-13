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
    let source = scope.join("source.redb");
    let mut store = crate::RedbStore::open(&source).unwrap();
    store
        .initialize_database(
            DatabaseId::from_unix_milliseconds_and_random(1234, [0x78; 10]).unwrap(),
        )
        .unwrap();
    drop(store);
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
    let mut stage = storage
        .stage_owned_recovery_restore_candidate(operation, &backup_name, inputs)
        .unwrap();
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
fn qualified_native_transfer_publication_preserves_original_sealed_artifacts() {
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
            .publish_native_transfer_for_qualification(sealed)
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
