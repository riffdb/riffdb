//! Ordinary maintenance needs exact successful reconciliation under this owner.
// req: REP-005, REC-001, STO-012
use super::*;

#[test]
fn reconciled_promotion_owner_allows_maintenance_without_pinning_the_engine() {
    let scope = crate::test_path::ScopedDirectory::new("promotion-maintenance-owner");
    let (path, mut owner, record, _) = fixture(&scope);
    owner.apply_promotion_cutover(&record).unwrap();
    assert!(owner.reconcile().is_err());
    let store = reconcile(&mut owner, &record).unwrap();
    assert!(
        owner.reconcile().is_ok(),
        "exact successful reconciliation must release the ordinary maintenance fence"
    );
    assert!(owner.reconcile_for_startup().is_ok());
    drop(store);
    assert!(!owner.configured_target_requires_recovery().unwrap());
    assert!(
        RedbMaintenanceStorage::open_for_promotion_recovery(&path, scope.join("backups")).is_err()
    );
    drop(owner);
    let mut owner =
        RedbMaintenanceStorage::open_for_promotion_recovery(&path, scope.join("backups")).unwrap();
    assert!(
        owner.reconcile().is_err(),
        "a fresh owner must independently reconcile success"
    );
    let store = reconcile(&mut owner, &record).unwrap();
    assert!(owner.reconcile().is_ok());
    assert!(owner.reconcile_for_startup().is_ok());
    drop(store);
    assert!(!owner.configured_target_requires_recovery().unwrap());
}

#[test]
fn reconciled_promotion_owner_refuses_missing_or_changed_success_evidence() {
    for remove in [false, true] {
        let scope = crate::test_path::ScopedDirectory::new("promotion-maintenance-ledger");
        let (_, mut owner, record, _) = fixture(&scope);
        owner.apply_promotion_cutover(&record).unwrap();
        drop(reconcile(&mut owner, &record).unwrap());
        assert!(owner.reconcile().is_ok());
        let receipts = owner.promotion_receipts().unwrap();
        let receipt = &receipts.receipts()[0];
        let path = scope
            .join("backups/.maintenance/replication_promotion")
            .join(format!("{}.receipt-v1", receipt.request_id()));
        if remove {
            std::fs::remove_file(path).unwrap();
        } else {
            let substituted = ReplicationPromotionReceiptV1::from_canonical_parts(
                receipt.request(),
                receipt.request_id(),
                receipt.principal().clone(),
                receipt.approval_id().cloned(),
                Timestamp::new(1235, 0).unwrap(),
                receipt.selection().cloned(),
                receipt.steps().to_vec(),
            )
            .unwrap();
            std::fs::write(
                path,
                crate::maintenance::codec::encode_promotion_receipt(&substituted).unwrap(),
            )
            .unwrap();
        }
        assert!(owner.reconcile().is_err());
        assert!(owner.configured_target_requires_recovery().is_err());
    }
}

#[test]
fn failed_promotion_revalidation_revokes_the_prior_maintenance_join() {
    let scope = crate::test_path::ScopedDirectory::new("promotion-maintenance-revalidate");
    let (path, mut owner, record, _) = fixture(&scope);
    owner.apply_promotion_cutover(&record).unwrap();
    drop(reconcile(&mut owner, &record).unwrap());
    assert!(owner.reconcile().is_ok());
    let database = redb::Database::open(&path).unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(crate::layout::AUDIT)
        .unwrap()
        .remove(crate::keys::encode_audit_key(record.administration_sequence()).as_slice())
        .unwrap();
    write.commit().unwrap();
    drop(database);
    assert!(reconcile(&mut owner, &record).is_err());
    assert!(owner.reconcile().is_err());
}

#[test]
fn promoted_backup_private_authorization_never_grants_source_open() {
    let scope = crate::test_path::ScopedDirectory::new("promotion-private-backup");
    let stage = promoted_backup_stage(&scope);
    let snapshot = stage.promoted_authorization_snapshot().unwrap().unwrap();
    drop(snapshot);
    let path = stage.staged_database_file().to_path_buf();
    let id = stage.manifest_identity().database_id();
    let sealed = stage.seal_after_validation(id).unwrap();
    // A refused writable engine open may still change backend bookkeeping.
    // Check that refusal after sealing, without presenting those changed bytes
    // as the unchanged private authorization candidate.
    assert!(crate::RedbStore::open(&path).is_err());
    drop(sealed);
}

#[test]
fn promoted_backup_authorization_and_sealing_refuse_changed_validated_bytes() {
    let scope = crate::test_path::ScopedDirectory::new("promotion-private-backup-tamper");
    let stage = promoted_backup_stage(&scope);
    drop(stage.promoted_authorization_snapshot().unwrap().unwrap());
    let database = redb::Database::open(stage.staged_database_file()).unwrap();
    let transaction = database.begin_write().unwrap();
    transaction
        .open_table(crate::layout::META)
        .unwrap()
        .remove(crate::primary_admission_roots::key().unwrap())
        .unwrap();
    transaction.commit().unwrap();
    drop(database);
    assert!(stage.promoted_authorization_snapshot().is_err());
    let id = stage.manifest_identity().database_id();
    assert!(stage.seal_after_validation(id).is_err());
}

fn promoted_backup_stage(
    scope: &crate::test_path::ScopedDirectory,
) -> crate::maintenance::staged::RedbStagedRestore {
    use riffdb_storage_api::{BackupBuildMetadataV1, OfflineBackupPersistencePort};
    let (path, mut owner, record, _) = fixture(scope);
    owner.apply_promotion_cutover(&record).unwrap();
    drop(reconcile(&mut owner, &record).unwrap());
    let backup = scope.join("independent-backup");
    crate::RedbOfflineBackup::bind(&path, &backup)
        .create_offline_backup(
            &BackupBuildMetadataV1::new("0.1.0", "fixture", "rustc", 1, vec![]).unwrap(),
        )
        .unwrap();
    drop(owner);
    crate::maintenance::staged::RedbStagedRestore::materialize(
        OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1234, [0x57; 10]).unwrap(),
        riffdb_types::BackupNameV1::new("promoted-copy").unwrap(),
        backup,
        scope.join("private-stage"),
        scope.join("other-target.redb"),
        inputs(),
        None,
    )
    .unwrap()
}

#[test]
fn restored_promotion_rejoins_current_authority_and_retains_exact_restore_evidence() {
    let scope = crate::test_path::ScopedDirectory::new("promotion-restored-owner");
    let (path, mut owner, attempt, receipt) = restored_promotion_fixture(&scope);
    assert!(owner.discover_committed_promotion().unwrap().is_none());
    assert!(
        owner.reconcile().is_err(),
        "decoded success alone is insufficient"
    );
    drop(reconcile_restored(&mut owner, &attempt).unwrap());
    assert!(owner.reconcile().is_ok());
    let receipt_path = scope
        .join("backups/.maintenance/receipts")
        .join(format!("{}.receipt-v1", receipt.operation_id()));
    std::fs::remove_file(receipt_path).unwrap();
    assert!(
        owner.reconcile().is_err(),
        "joined restore evidence must remain present"
    );
    drop(owner);
    let mut owner =
        RedbMaintenanceStorage::open_for_promotion_recovery(&path, scope.join("backups")).unwrap();
    assert!(reconcile_restored(&mut owner, &attempt).is_err());
}

#[test]
fn restored_promotion_refuses_foreign_restore_origin_and_revokes_prior_join() {
    let scope = crate::test_path::ScopedDirectory::new("promotion-restored-foreign");
    let (_, mut owner, attempt, receipt) = restored_promotion_fixture(&scope);
    drop(reconcile_restored(&mut owner, &attempt).unwrap());
    let foreign = DatabaseId::from_unix_milliseconds_and_random(1234, [0x66; 10]).unwrap();
    let changed = OfflineMaintenanceReceiptV1::from_canonical_parts(
        receipt.operation_id(),
        receipt.operation_kind(),
        receipt.backup_name().clone(),
        receipt.input_hash(),
        receipt.replacement_confirmation(),
        receipt.admission().clone(),
        Some(foreign),
        receipt.staged_database_id(),
        receipt.manifest_identity().cloned(),
        receipt.published_history_incarnation(),
        receipt.transitions().to_vec(),
    )
    .unwrap();
    let receipt_path = scope
        .join("backups/.maintenance/receipts")
        .join(format!("{}.receipt-v1", receipt.operation_id()));
    std::fs::write(
        receipt_path,
        crate::maintenance::codec::encode_receipt(&changed).unwrap(),
    )
    .unwrap();
    assert!(owner.reconcile().is_err());
    assert!(reconcile_restored(&mut owner, &attempt).is_err());
    assert!(owner.reconcile().is_err());
}

#[test]
fn restored_promotion_rechecks_current_source_integrity_before_granting_a_join() {
    let scope = crate::test_path::ScopedDirectory::new("promotion-restored-integrity");
    let (path, mut owner, attempt, _) = restored_promotion_fixture(&scope);
    drop(reconcile_restored(&mut owner, &attempt).unwrap());
    let database = redb::Database::open(path).unwrap();
    let transaction = database.begin_write().unwrap();
    transaction
        .open_table(crate::layout::META)
        .unwrap()
        .remove(crate::primary_admission_roots::key().unwrap())
        .unwrap();
    transaction.commit().unwrap();
    drop(database);
    assert!(reconcile_restored(&mut owner, &attempt).is_err());
    assert!(owner.reconcile().is_err());
}

fn reconcile_restored(
    owner: &mut RedbMaintenanceStorage,
    attempt: &ReplicationPromotionReceiptV1,
) -> Result<crate::RedbStore, StorageError> {
    owner.reconcile_restored_promotion(
        attempt,
        inputs(),
        Arc::new(AtomicBool::new(false)),
        crate::RedbCommitProfile::Standard,
        Arc::new(NoChangelogPublicationPort),
    )
}

fn restored_promotion_fixture(
    scope: &crate::test_path::ScopedDirectory,
) -> (
    std::path::PathBuf,
    RedbMaintenanceStorage,
    ReplicationPromotionReceiptV1,
    OfflineMaintenanceReceiptV1,
) {
    use riffdb_storage_api::*;
    use riffdb_types::*;
    let (path, mut owner, record, _) = fixture(scope);
    owner.apply_promotion_cutover(&record).unwrap();
    drop(reconcile(&mut owner, &record).unwrap());
    let attempt = owner.promotion_receipts().unwrap().receipts()[0].clone();
    let lineage = attempt.selection().unwrap().published_lineage();
    let name = BackupNameV1::new("promoted-copy").unwrap();
    let backup = scope.join("backups/promoted-copy");
    crate::RedbOfflineBackup::bind(&path, &backup)
        .create_offline_backup(
            &BackupBuildMetadataV1::new("0.1.0", "fixture", "rustc", 1, vec![]).unwrap(),
        )
        .unwrap();
    let id =
        OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1234, [0x59; 10]).unwrap();
    let stage = crate::maintenance::staged::RedbStagedRestore::materialize(
        id,
        name.clone(),
        backup,
        scope
            .join("backups/.maintenance/staged")
            .join(id.to_string()),
        path.clone(),
        inputs(),
        None,
    )
    .unwrap();
    let manifest = stage.manifest_identity().clone();
    let mut sealed = stage.seal_after_validation(lineage.database_id()).unwrap();
    let confirmation = OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget;
    let mut receipt = OfflineMaintenanceReceiptV1::accepted(
        id,
        OfflineMaintenanceOperationKind::RestoreBackup,
        name.clone(),
        offline_maintenance_input_hash(
            OfflineMaintenanceOperationKind::RestoreBackup,
            &name,
            confirmation,
        ),
        confirmation,
        OfflineMaintenanceAdmissionV1::new(
            attempt.principal().principal_id().clone(),
            ActorKind::Human,
            attempt.principal().capability_id(),
            None,
        ),
    )
    .unwrap();
    receipt
        .record_source_database_id(lineage.database_id())
        .unwrap();
    for phase in [
        OfflineMaintenanceReceiptPhaseV1::Draining,
        OfflineMaintenanceReceiptPhaseV1::Offline,
    ] {
        receipt
            .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
            .unwrap();
    }
    receipt.record_manifest_identity(manifest).unwrap();
    receipt
        .record_staged_database_id(lineage.database_id())
        .unwrap();
    receipt
        .record_published_incarnation(lineage.history_incarnation() + 1)
        .unwrap();
    owner.create_or_read_receipt(&receipt).unwrap();
    sealed
        .apply_published_history_incarnation(lineage.history_incarnation() + 1)
        .unwrap();
    owner
        .publish_sealed_restore(
            sealed,
            OfflineRestoreOverwritePolicyV1::ExplicitlyAllowDestructive,
        )
        .unwrap();
    for phase in [
        OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
        OfflineMaintenanceReceiptPhaseV1::Validating,
        OfflineMaintenanceReceiptPhaseV1::Succeeded,
    ] {
        receipt
            .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
            .unwrap();
    }
    owner.replace_receipt(&receipt).unwrap();
    drop(owner);
    let owner =
        RedbMaintenanceStorage::open_for_promotion_recovery(&path, scope.join("backups")).unwrap();
    (path, owner, attempt, receipt)
}

#[test]
fn restored_promotion_ignores_failed_unpublished_incarnation_reservations() {
    use riffdb_storage_api::OfflineMaintenanceReceiptFailureV1;
    let scope = crate::test_path::ScopedDirectory::new("promotion-restored-failed-reservation");
    let (_, mut owner, attempt, receipt) = restored_promotion_fixture(&scope);
    let failed_id =
        OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1234, [0x58; 10]).unwrap();
    let mut transitions = receipt.transitions()[..3].to_vec();
    transitions.push(OfflineMaintenanceReceiptTransitionV1::failed(
        OfflineMaintenanceReceiptFailureV1::ArtifactUnavailable,
    ));
    let failed = OfflineMaintenanceReceiptV1::from_canonical_parts(
        failed_id,
        receipt.operation_kind(),
        receipt.backup_name().clone(),
        receipt.input_hash(),
        receipt.replacement_confirmation(),
        receipt.admission().clone(),
        receipt.source_database_id(),
        receipt.staged_database_id(),
        receipt.manifest_identity().cloned(),
        receipt.published_history_incarnation(),
        transitions,
    )
    .unwrap();
    let failed_path = scope
        .join("backups/.maintenance/receipts")
        .join(format!("{failed_id}.receipt-v1"));
    let bytes = crate::maintenance::codec::encode_receipt(&failed).unwrap();
    std::fs::write(&failed_path, &bytes).unwrap();
    drop(reconcile_restored(&mut owner, &attempt).unwrap());
    assert!(owner.reconcile().is_ok());
    assert_eq!(std::fs::read(failed_path).unwrap(), bytes);
    // A failed attempt cannot substitute for the successful publication.
    std::fs::remove_file(
        scope
            .join("backups/.maintenance/receipts")
            .join(format!("{}.receipt-v1", receipt.operation_id())),
    )
    .unwrap();
    assert!(reconcile_restored(&mut owner, &attempt).is_err());
    assert!(owner.reconcile().is_err());
}

#[test]
fn restored_promotion_cleans_only_recognized_unpublished_receipt_temps() {
    let scope = crate::test_path::ScopedDirectory::new("promotion-restored-receipt-temp");
    let (_, mut owner, attempt, receipt) = restored_promotion_fixture(&scope);
    let directory = scope.join("backups/.maintenance/receipts");
    let published = directory.join(format!("{}.receipt-v1", receipt.operation_id()));
    let bytes = std::fs::read(&published).unwrap();
    let temporary = directory.join(format!(".{}.receipt-v1.tmp", receipt.operation_id()));
    std::fs::write(&temporary, b"interrupted receipt replacement").unwrap();
    drop(reconcile_restored(&mut owner, &attempt).unwrap());
    assert!(owner.reconcile().is_ok());
    assert!(!temporary.exists());
    assert_eq!(std::fs::read(&published).unwrap(), bytes);

    // A malformed inventory must refuse before deleting any crash evidence.
    std::fs::write(&temporary, b"interrupted receipt replacement").unwrap();
    let unknown = directory.join("unknown-receipt-version");
    std::fs::write(&unknown, b"unrecognized durable record").unwrap();
    assert!(reconcile_restored(&mut owner, &attempt).is_err());
    assert!(owner.reconcile().is_err());
    assert!(temporary.exists());
    assert!(unknown.exists());
    assert_eq!(std::fs::read(published).unwrap(), bytes);
}

#[test]
fn exact_success_reconciles_failed_selection_only_under_the_current_owner() {
    let scope = crate::test_path::ScopedDirectory::new("promotion-failed-selection-retry");
    let (path, mut owner, record, _) = fixture(&scope);
    let failed = retain_failed_selection(&mut owner, &record, false);
    owner.apply_promotion_cutover(&record).unwrap();
    drop(reconcile(&mut owner, &record).unwrap());
    assert!(owner.reconcile_for_startup().is_ok());
    assert!(
        owner
            .promotion_receipts()
            .unwrap()
            .receipts()
            .contains(&failed)
    );
    drop(owner);
    assert!(RedbMaintenanceStorage::open(&path, scope.join("backups")).is_err());
    let mut owner =
        RedbMaintenanceStorage::open_for_promotion_recovery(&path, scope.join("backups")).unwrap();
    assert!(owner.reconcile_for_startup().is_err());
    drop(reconcile(&mut owner, &record).unwrap());
    assert!(owner.reconcile_for_startup().is_ok());
    let success_path = scope
        .join("backups/.maintenance/replication_promotion")
        .join(format!("{}.receipt-v1", record.attempt().request_id()));
    std::fs::remove_file(success_path).unwrap();
    assert!(owner.reconcile_for_startup().is_err());
    assert!(
        owner
            .promotion_receipts()
            .unwrap()
            .receipts()
            .contains(&failed)
    );
}

#[test]
fn exact_success_does_not_release_another_operations_failed_selection() {
    let scope = crate::test_path::ScopedDirectory::new("promotion-foreign-failed-selection");
    let (_, mut owner, record, _) = fixture(&scope);
    owner.apply_promotion_cutover(&record).unwrap();
    drop(reconcile(&mut owner, &record).unwrap());
    assert!(owner.reconcile_for_startup().is_ok());
    retain_failed_selection(&mut owner, &record, true);
    assert!(owner.reconcile_for_startup().is_err());
}

#[test]
fn exact_success_resolves_interrupted_attempts_without_rewriting_their_audit() {
    for prefix_len in 1..=5 {
        let scope = crate::test_path::ScopedDirectory::new("promotion-interrupted-retry");
        let (path, mut owner, record, _) = fixture(&scope);
        let original = record.attempt();
        let initial = ReplicationPromotionReceiptV1::attempted(
            original.request(),
            RequestId::from_unix_milliseconds_and_random(1234, [0x66; 10]).unwrap(),
            original.principal().clone(),
            original.approval_id().cloned(),
            original.timestamp(),
        );
        owner.persist_promotion_receipt(&initial).unwrap();
        let mut interrupted = ReplicationPromotionReceiptV1::from_canonical_parts(
            initial.request(),
            initial.request_id(),
            initial.principal().clone(),
            initial.approval_id().cloned(),
            initial.timestamp(),
            (prefix_len >= 4).then(|| original.selection().unwrap().clone()),
            original.steps()[..prefix_len].to_vec(),
        )
        .unwrap();
        if prefix_len == 5 {
            interrupted
                .advance(ReplicationPromotionStepV1::Uncertain(
                    riffdb_storage_api::ReplicationPromotionFailureV1::StorageUnavailable,
                ))
                .unwrap();
        }
        owner.persist_promotion_receipt(&interrupted).unwrap();
        assert!(owner.reconcile_for_startup().is_err());
        owner.apply_promotion_cutover(&record).unwrap();
        assert!(owner.reconcile_for_startup().is_err());
        drop(reconcile(&mut owner, &record).unwrap());
        assert!(
            owner.reconcile_for_startup().is_ok(),
            "validated exact success resolves the operation, preserving attempt {prefix_len}"
        );
        assert!(
            owner
                .promotion_receipts()
                .unwrap()
                .receipts()
                .contains(&interrupted)
        );
        assert!(!interrupted.is_terminal());
        drop(owner);
        let mut owner =
            RedbMaintenanceStorage::open_for_promotion_recovery(&path, scope.join("backups"))
                .unwrap();
        assert!(owner.reconcile_for_startup().is_err());
        drop(reconcile(&mut owner, &record).unwrap());
        assert!(owner.reconcile_for_startup().is_ok());
        assert!(
            owner
                .promotion_receipts()
                .unwrap()
                .receipts()
                .contains(&interrupted)
        );
    }
}

#[test]
fn exact_success_never_resolves_a_second_claimed_cutover() {
    for phase in [
        ReplicationPromotionPhaseV1::CutoverCommitted,
        ReplicationPromotionPhaseV1::Validated,
    ] {
        let scope = crate::test_path::ScopedDirectory::new("promotion-conflicting-cutover");
        let (_, mut owner, record, _) = fixture(&scope);
        owner.apply_promotion_cutover(&record).unwrap();
        drop(reconcile(&mut owner, &record).unwrap());
        assert!(owner.reconcile_for_startup().is_ok());
        let original = record.attempt();
        let initial = ReplicationPromotionReceiptV1::attempted(
            original.request(),
            RequestId::from_unix_milliseconds_and_random(1234, [0x67; 10]).unwrap(),
            original.principal().clone(),
            original.approval_id().cloned(),
            original.timestamp(),
        );
        owner.persist_promotion_receipt(&initial).unwrap();
        let mut contradictory = ReplicationPromotionReceiptV1::from_canonical_parts(
            initial.request(),
            initial.request_id(),
            initial.principal().clone(),
            initial.approval_id().cloned(),
            initial.timestamp(),
            original.selection().cloned(),
            original.steps().to_vec(),
        )
        .unwrap();
        contradictory
            .advance(ReplicationPromotionStepV1::Phase(
                ReplicationPromotionPhaseV1::CutoverCommitted,
            ))
            .unwrap();
        if phase == ReplicationPromotionPhaseV1::Validated {
            contradictory
                .advance(ReplicationPromotionStepV1::Phase(phase))
                .unwrap();
        }
        owner.persist_promotion_receipt(&contradictory).unwrap();

        assert!(owner.reconcile_for_startup().is_err());
        assert!(
            owner
                .promotion_receipts()
                .unwrap()
                .receipts()
                .contains(&contradictory)
        );
    }
}

#[test]
fn pending_promotion_checks_ordinary_inventory_without_cleaning_it_up() {
    for directory in ["receipts", "staged", "retired", "migrations"] {
        let scope = crate::test_path::ScopedDirectory::new("promotion-unknown-maintenance");
        let (_, mut owner, record, _) = fixture(&scope);
        assert_eq!(
            owner.pending_promotion_request().unwrap(),
            Some(record.attempt().request())
        );
        let unexpected = scope
            .join("backups/.maintenance")
            .join(directory)
            .join("unrecognized-entry");
        std::fs::write(&unexpected, b"retained-unknown-custody").unwrap();
        let before = owner.promotion_receipts().unwrap();
        assert!(owner.pending_promotion_request().is_err());
        assert!(owner.reconcile_for_startup().is_err());
        assert_eq!(
            std::fs::read(&unexpected).unwrap(),
            b"retained-unknown-custody"
        );
        assert_eq!(owner.promotion_receipts().unwrap(), before);
    }
}

#[test]
fn pending_promotion_refuses_another_operations_selected_failure() {
    let scope = crate::test_path::ScopedDirectory::new("promotion-multiple-pending");
    let (_, mut owner, record, _) = fixture(&scope);
    let failed = retain_failed_selection(&mut owner, &record, true);
    assert!(owner.pending_promotion_request().is_err());
    assert!(
        owner
            .promotion_receipts()
            .unwrap()
            .receipts()
            .contains(&failed)
    );
}

fn retain_failed_selection(
    owner: &mut RedbMaintenanceStorage,
    record: &StoredPromotionAdministrationV1,
    foreign: bool,
) -> ReplicationPromotionReceiptV1 {
    let original = record.attempt();
    let request = original.request();
    let request = if foreign {
        ReplicationPromotionRequestV1::new(
            ReplicationPromotionOperationId::from_unix_milliseconds_and_random(1234, [0x65; 10])
                .unwrap(),
            request.fence_operation_id(),
            request.target(),
            request.generation(),
        )
    } else {
        request
    };
    let mut failed = ReplicationPromotionReceiptV1::attempted(
        request,
        RequestId::from_unix_milliseconds_and_random(1234, [6; 10]).unwrap(),
        original.principal().clone(),
        original.approval_id().cloned(),
        original.timestamp(),
    );
    owner.persist_promotion_receipt(&failed).unwrap();
    for phase in [
        ReplicationPromotionPhaseV1::Draining,
        ReplicationPromotionPhaseV1::Offline,
    ] {
        failed
            .advance(ReplicationPromotionStepV1::Phase(phase))
            .unwrap();
    }
    let selection = original.selection().unwrap();
    failed
        .record_selection(
            ReplicationPromotionSelectionV1::from_canonical_parts(
                request,
                selection.evidence().clone(),
                selection.published_lineage(),
                selection.application_rpo(),
            )
            .unwrap(),
        )
        .unwrap();
    failed
        .advance(ReplicationPromotionStepV1::FailedClosed(
            riffdb_storage_api::ReplicationPromotionFailureV1::StorageUnavailable,
        ))
        .unwrap();
    owner.persist_promotion_receipt(&failed).unwrap();
    failed
}
