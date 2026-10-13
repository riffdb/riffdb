//! Whole-owner native reconciliation, never a serialized readiness certificate.
// req: REC-001, REC-004, REC-005, REC-006, REC-008
use super::*;
use crate::maintenance::codec::publication_intent::{PublicationIntent, PublicationPhase};
use crate::maintenance::staged::transfer_tests::prepared_transfer_fixture;
use crate::test_path::ScopedDirectory;
use std::os::unix::fs::MetadataExt;

fn current(
    storage: &RedbMaintenanceStorage,
    operation: OfflineMaintenanceOperationId,
) -> PublicationIntent {
    PublicationIntent::decode(
        &fs::read(
            storage
                .receipts_directory
                .join(format!("{operation}.publication-intent-v1")),
        )
        .unwrap(),
    )
    .unwrap()
}

#[test]
fn native_publication_cold_open_resumes_partial_transfer_without_copy_or_readiness() {
    let scope = ScopedDirectory::new("native-cold-open-transfer");
    let root = scope.join("fixture");
    fs::create_dir(&root).unwrap();
    let (storage, configured, named, operation) = prepared_transfer_fixture(&root);
    let stage_primary = storage
        .staged_directory
        .join(operation.to_string())
        .join(crate::backup::DATABASE_ARTIFACT_FILE_NAME);
    let stage_paths = [
        stage_primary.clone(),
        crate::journal::journal_path(&stage_primary),
        crate::durable_format_marker_path(&stage_primary),
    ];
    let finals = [
        configured.clone(),
        crate::journal::journal_path(&configured),
        crate::durable_format_marker_path(&configured),
    ];
    let original: Vec<_> = stage_paths
        .iter()
        .map(|p| {
            let m = fs::metadata(p).unwrap();
            (m.dev(), m.ino(), crate::backup::sha256_file(p).unwrap())
        })
        .collect();
    let temporary = configured.with_file_name(super::super::staged::target_temporary_name(
        configured.file_name().unwrap(),
        operation,
    ));
    fs::rename(&stage_primary, &temporary).unwrap();
    let backup = validate_immutable_backup(&named).unwrap();
    drop(storage);
    let (storage, reconciliation) = RedbMaintenanceStorage::open(&configured, root.join("backups"))
        .expect("cold native reconciliation must resume the recorded transfer instead of refusing or copying it");
    assert_eq!(
        current(&storage, operation).phase,
        PublicationPhase::Published
    );
    for (path, (dev, inode, checksum)) in finals.iter().zip(original) {
        let m = fs::metadata(path).unwrap();
        assert_eq!((m.dev(), m.ino()), (dev, inode));
        assert_eq!(crate::backup::sha256_file(path).unwrap(), checksum);
    }
    assert!(!temporary.exists());
    assert_eq!(validate_immutable_backup(&named).unwrap(), backup);
    let evidence = reconciliation
        .operations()
        .iter()
        .find(|evidence| evidence.operation_id() == operation)
        .unwrap();
    assert!(evidence.native_publication_target_owned());
    assert!(
        !evidence.configured_target_matches(),
        "native ownership must not masquerade as old copied-stage validation"
    );
    assert_eq!(
        read_receipt_file(&storage.receipt_path(operation))
            .unwrap()
            .current_phase(),
        OfflineMaintenanceReceiptPhaseV1::Offline
    );
}

#[test]
fn native_publication_cold_open_refuses_unknown_owner_inventory_before_any_cleanup_or_move() {
    for migration in [false, true] {
        let scope = ScopedDirectory::new("native-cold-open-whole-inventory");
        let root = scope.join("fixture");
        fs::create_dir(&root).unwrap();
        let (mut storage, configured, _, operation) = prepared_transfer_fixture(&root);
        let before = current(&storage, operation).encode().unwrap();
        let stage_primary = storage
            .staged_directory
            .join(operation.to_string())
            .join(crate::backup::DATABASE_ARTIFACT_FILE_NAME);
        let temporary = configured.with_file_name(super::super::staged::target_temporary_name(
            configured.file_name().unwrap(),
            operation,
        ));
        fs::rename(&stage_primary, &temporary).unwrap();
        let ordinary = storage
            .receipts_directory
            .join(format!(".{operation}.receipt-v1.tmp"));
        fs::write(&ordinary, b"preserved unpublished receipt").unwrap();
        let foreign = if migration {
            storage.migrations_directory.join("foreign")
        } else {
            storage.staged_directory.join("foreign")
        };
        fs::create_dir(&foreign).unwrap();
        fs::write(foreign.join("evidence"), b"preserved foreign ownership").unwrap();
        assert!(storage.reconcile_for_startup().is_err());
        assert_eq!(
            fs::read(&ordinary).unwrap(),
            b"preserved unpublished receipt"
        );
        assert_eq!(
            fs::read(foreign.join("evidence")).unwrap(),
            b"preserved foreign ownership"
        );
        assert_eq!(current(&storage, operation).encode().unwrap(), before);
        assert!(temporary.exists());
        assert!(!configured.exists());
    }
}
