//! Durable archive receipt retries, inventory exclusion and recovery boundaries.
// req: REP-007, AFC-007
use riffdb_storage_api::*;
use riffdb_storage_redb::{
    RedbMaintenanceFailpoint, RedbMaintenanceStorage, RedbMaintenanceTestController,
};
use riffdb_types::*;
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Root(PathBuf);
impl Root {
    fn new() -> Self {
        let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "archive-receipt-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn open(
        &self,
    ) -> (
        RedbMaintenanceStorage,
        riffdb_storage_redb::RedbMaintenanceReconciliation,
    ) {
        RedbMaintenanceStorage::open(self.0.join("db.redb"), self.0.join("backups")).unwrap()
    }
    fn receipt_path(&self, id: OfflineMaintenanceOperationId) -> PathBuf {
        self.0
            .join("backups/.maintenance/receipts")
            .join(format!("{id}.receipt-v3"))
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn accepted(seed: u8) -> OfflineMaintenanceReceiptV3 {
    let backup = BackupNameV1::new("before").unwrap();
    let archive = ArchiveNameV1::new("daily").unwrap();
    let stop = ArchiveRestoreStopV1::AtApplicationSequence(CommitSequence::new(2).unwrap());
    let confirmation = OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget;
    OfflineMaintenanceReceiptV3::accepted_archive_restore(
        OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1000, [seed; 10]).unwrap(),
        backup.clone(),
        archive.clone(),
        stop,
        archive_restore_input_hash(&backup, &archive, stop, confirmation),
        confirmation,
        OfflineMaintenanceAdmissionV1::new(
            ActorId::new("operator").unwrap(),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(1000, [7; 10]).unwrap(),
            None,
        ),
        None,
    )
    .unwrap()
}
fn selection(last: u64) -> ArchiveRestoreSelectionV3 {
    let text = if last == 3 {
        include_str!("../../../fixtures/replication/archive-manifest-v1-unencrypted-3.hex")
    } else {
        include_str!("../../../fixtures/replication/archive-manifest-v1-unencrypted-5.hex")
    };
    let bytes = text
        .trim()
        .as_bytes()
        .chunks_exact(2)
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect::<Vec<_>>();
    let m = ArchiveManifestV1::decode(&bytes).unwrap();
    ArchiveRestoreSelectionV3::new(
        OfflineBackupManifestIdentityV1::new(
            BackupIntegrityChecksumV1::new(m.full_backup_manifest_digest().to_vec()).unwrap(),
            m.lineage().database_id(),
            None,
        ),
        m.lineage(),
        m.backup_fence(),
        ArchiveRestoreSuffixV3::Terminal(Box::new(m)),
    )
    .unwrap()
}
#[test]
fn archive_receipt_persists_exact_selection_across_reopen_and_preserves_owned_stage() {
    let root = Root::new();
    let (mut store, _) = root.open();
    let mut receipt = accepted(1);
    assert!(matches!(
        store.create_or_read_archive_receipt(&receipt).unwrap(),
        OfflineMaintenanceReceiptCreateResultV3::Created
    ));
    receipt.record_selection(selection(3)).unwrap();
    store.replace_archive_receipt(&receipt).unwrap();
    let bytes = fs::read(root.receipt_path(receipt.operation_id())).unwrap();
    assert!(bytes.len() <= 4096);
    let stage = root
        .0
        .join("backups/.maintenance/staged")
        .join(receipt.operation_id().to_string());
    fs::create_dir(&stage).unwrap();
    fs::write(stage.join("private-candidate"), b"retained private bytes").unwrap();
    drop(store);
    let (mut store, reconciliation) = root.open();
    assert_eq!(
        reconciliation.archive_receipts().receipts(),
        &[receipt.clone()]
    );
    assert_eq!(
        store.read_archive_receipt(receipt.operation_id()).unwrap(),
        Some(receipt.clone())
    );
    assert_eq!(
        fs::read(stage.join("private-candidate")).unwrap(),
        b"retained private bytes"
    );
    assert!(receipt.record_selection(selection(5)).is_err());
    assert!(matches!(
        store.replace_archive_receipt(&receipt).unwrap(),
        OfflineMaintenanceReceiptReplaceResultV3::AlreadyCurrent
    ));
    assert_eq!(
        fs::read(root.receipt_path(receipt.operation_id())).unwrap(),
        bytes
    );
}
#[test]
fn archive_receipt_excludes_other_maintenance_and_cross_version_operation_ids() {
    let root = Root::new();
    let (mut store, _) = root.open();
    let mut archive = accepted(2);
    store.create_or_read_archive_receipt(&archive).unwrap();
    assert!(store.create_or_read_archive_receipt(&accepted(3)).is_err());
    let plain = |id| {
        let name = BackupNameV1::new("plain").unwrap();
        let confirmation = OfflineMaintenanceReplacementConfirmation::NotProvided;
        OfflineMaintenanceReceiptV1::accepted(
            id,
            OfflineMaintenanceOperationKind::CreateBackup,
            name.clone(),
            offline_maintenance_input_hash(
                OfflineMaintenanceOperationKind::CreateBackup,
                &name,
                confirmation,
            ),
            confirmation,
            archive.admission().clone(),
        )
        .unwrap()
    };
    let other = plain(accepted(4).operation_id());
    let same = plain(archive.operation_id());
    assert!(store.create_or_read_receipt(&other).is_err());
    archive
        .advance(OfflineMaintenanceReceiptTransitionV1::failed(
            OfflineMaintenanceReceiptFailureV1::ArtifactInvalid,
        ))
        .unwrap();
    store.replace_archive_receipt(&archive).unwrap();
    assert!(store.create_or_read_receipt(&same).is_err());
    store.create_or_read_receipt(&other).unwrap();
    assert!(store.create_or_read_archive_receipt(&accepted(5)).is_err());
}

fn boundaries() -> [(RedbMaintenanceFailpoint, bool); 5] {
    use RedbMaintenanceFailpoint::*;
    [
        (BeforeReceiptFileSync, false),
        (AfterReceiptFileSync, false),
        (BeforeReceiptRename, false),
        (AfterReceiptRename, true),
        (AfterReceiptParentSync, true),
    ]
}

#[test]
fn archive_receipt_uncertainty_retries_sync_parent_and_keep_frozen_selection() {
    for (boundary, published) in boundaries() {
        let root = Root::new();
        let original = accepted(10);
        let (mut store, _) = root.open();
        store.create_or_read_archive_receipt(&original).unwrap();
        drop(store);
        let controller = RedbMaintenanceTestController::return_at(boundary);
        let (mut store, _) = RedbMaintenanceStorage::open_with_test_controller(
            root.0.join("db.redb"),
            root.0.join("backups"),
            controller.clone(),
        )
        .unwrap();
        let mut selected = original.clone();
        selected.record_selection(selection(3)).unwrap();
        let error = store.replace_archive_receipt(&selected).unwrap_err();
        assert_eq!(
            error.kind(),
            if published {
                StorageErrorKind::CommitStatusUnknown
            } else {
                StorageErrorKind::Unavailable
            }
        );
        assert_eq!(
            store
                .read_archive_receipt(original.operation_id())
                .unwrap()
                .as_ref(),
            Some(if published { &selected } else { &original })
        );
        let before = controller.events().len();
        store.replace_archive_receipt(&selected).unwrap();
        assert!(
            controller.events()[before..]
                .iter()
                .any(|e| e.failpoint() == RedbMaintenanceFailpoint::AfterReceiptParentSync)
        );
        drop(store);
        let (mut store, reconciliation) = root.open();
        assert_eq!(
            reconciliation.archive_receipts().receipts(),
            std::slice::from_ref(&selected)
        );
        assert_eq!(
            store.read_archive_receipt(original.operation_id()).unwrap(),
            Some(selected)
        );
    }
}

#[test]
fn archive_receipt_crash_child() {
    let Some(path) = std::env::var_os("RIFFDB_ARCHIVE_RECEIPT_CRASH_ROOT") else {
        return;
    };
    let index: usize = std::env::var("RIFFDB_ARCHIVE_RECEIPT_CRASH_BOUNDARY")
        .unwrap()
        .parse()
        .unwrap();
    let root = PathBuf::from(path);
    let controller = RedbMaintenanceTestController::abort_at(boundaries()[index].0);
    let (mut store, _) = RedbMaintenanceStorage::open_with_test_controller(
        root.join("db.redb"),
        root.join("backups"),
        controller,
    )
    .unwrap();
    let mut selected = accepted(11);
    if std::env::var("RIFFDB_ARCHIVE_RECEIPT_CRASH_CREATE").as_deref() == Ok("1") {
        store.create_or_read_archive_receipt(&selected).unwrap();
    } else {
        selected.record_selection(selection(3)).unwrap();
        store.replace_archive_receipt(&selected).unwrap();
    }
    panic!("crash boundary was not reached");
}

#[test]
fn archive_receipt_process_crashes_recover_old_or_exact_selected_receipt() {
    #[cfg(unix)]
    use std::os::unix::process::ExitStatusExt;
    use std::process::{Command, Stdio};
    for creating in [false, true] {
        for (index, (_, published)) in boundaries().into_iter().enumerate() {
            let root = Root::new();
            let original = accepted(11);
            let (mut store, _) = root.open();
            if !creating {
                store.create_or_read_archive_receipt(&original).unwrap();
            }
            drop(store);
            let status = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "archive_receipt_crash_child", "--nocapture"])
                .env("RIFFDB_ARCHIVE_RECEIPT_CRASH_ROOT", &root.0)
                .env(
                    "RIFFDB_ARCHIVE_RECEIPT_CRASH_CREATE",
                    if creating { "1" } else { "0" },
                )
                .env("RIFFDB_ARCHIVE_RECEIPT_CRASH_BOUNDARY", index.to_string())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .unwrap();
            #[cfg(unix)]
            assert_eq!(status.signal(), Some(6));
            #[cfg(not(unix))]
            assert!(!status.success());
            let (mut store, reconciliation) = root.open();
            let mut selected = original.clone();
            selected.record_selection(selection(3)).unwrap();
            let expected = if creating {
                published.then_some(&original)
            } else {
                Some(if published { &selected } else { &original })
            };
            assert_eq!(
                reconciliation.archive_receipts().receipts(),
                expected.into_iter().cloned().collect::<Vec<_>>()
            );
            assert_eq!(
                store
                    .read_archive_receipt(original.operation_id())
                    .unwrap()
                    .as_ref(),
                expected
            );
            store.create_or_read_archive_receipt(&original).unwrap();
            store.replace_archive_receipt(&selected).unwrap();
            assert_eq!(
                store.read_archive_receipt(original.operation_id()).unwrap(),
                Some(selected)
            );
        }
    }
}

#[test]
fn archive_receipt_corrupt_inventory_preserves_unpublished_temps() {
    for oversized in [false, true] {
        let root = Root::new();
        let receipt = accepted(12);
        let (mut store, _) = root.open();
        store.create_or_read_archive_receipt(&receipt).unwrap();
        drop(store);
        let path = root.receipt_path(receipt.operation_id());
        let temporary = path
            .parent()
            .unwrap()
            .join(format!(".{}.receipt-v3.tmp", receipt.operation_id()));
        fs::write(&temporary, b"unpublished candidate").unwrap();
        let mut bytes = fs::read(&path).unwrap();
        if oversized {
            bytes.resize(4097, 0);
        } else {
            bytes[0] ^= 1;
        }
        fs::write(&path, &bytes).unwrap();
        assert!(
            RedbMaintenanceStorage::open(root.0.join("db.redb"), root.0.join("backups")).is_err()
        );
        assert_eq!(fs::read(&temporary).unwrap(), b"unpublished candidate");
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }
}

#[test]
fn archive_receipt_v3_goldens_preserve_exact_bytes_and_refuse_unknown_encodings() {
    use sha2::{Digest, Sha256};
    let root = Root::new();
    let (mut store, _) = root.open();
    let mut receipt = accepted(11);
    store.create_or_read_archive_receipt(&receipt).unwrap();
    let path = root.receipt_path(receipt.operation_id());
    let accepted_bytes = fs::read(&path).unwrap();
    let hex = |bytes: &[u8]| bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
    assert_eq!(
        hex(&accepted_bytes),
        include_str!(
            "../../../fixtures/compatibility/offline-maintenance-archive-accepted-receipt-v3.hex"
        )
        .trim()
    );
    receipt.record_selection(selection(3)).unwrap();
    store.replace_archive_receipt(&receipt).unwrap();
    let selected = fs::read(&path).unwrap();
    assert_eq!(
        hex(&selected),
        include_str!(
            "../../../fixtures/compatibility/offline-maintenance-archive-selected-receipt-v3.hex"
        )
        .trim()
    );
    drop(store);
    let body_end = accepted_bytes.len() - 32;
    let mut variants = Vec::new();
    let mut unknown = accepted_bytes[..body_end].to_vec();
    let magic_len = b"RIFFDB-MAINT-RECEIPT\0".len();
    unknown[magic_len..magic_len + 4].copy_from_slice(&4u32.to_be_bytes());
    variants.push(unknown);
    let mut trailing = accepted_bytes[..body_end].to_vec();
    trailing.push(0);
    variants.push(trailing);
    let mut empty_history = accepted_bytes[..body_end].to_vec();
    empty_history[body_end - 3] = 0;
    variants.push(empty_history);
    let mut unknown_stop = accepted_bytes[..body_end].to_vec();
    unknown_stop[magic_len + 4 + 16 + 2 + "before".len() + 2 + "daily".len()] = 2;
    variants.push(unknown_stop);
    for mut body in variants {
        let checksum = Sha256::digest(&body);
        body.extend_from_slice(&checksum);
        fs::write(&path, &body).unwrap();
        assert!(
            RedbMaintenanceStorage::open(root.0.join("db.redb"), root.0.join("backups")).is_err()
        );
        assert_eq!(fs::read(&path).unwrap(), body);
    }
    fs::write(&path, &selected).unwrap();
    let (mut store, _) = root.open();
    assert_eq!(
        store.read_archive_receipt(receipt.operation_id()).unwrap(),
        Some(receipt)
    );
}

#[test]
fn archive_receipt_round_trips_source_route_empty_suffix_and_terminal_evidence() {
    use OfflineMaintenanceReceiptPhaseV1::*;
    for empty in [false, true] {
        let root = Root::new();
        let (mut store, _) = root.open();
        let base = accepted(14);
        let original = selection(3);
        let selected = if empty {
            ArchiveRestoreSelectionV3::new(
                original.backup().clone(),
                original.lineage(),
                original.backup_fence(),
                ArchiveRestoreSuffixV3::Empty,
            )
            .unwrap()
        } else {
            original
        };
        let stop = ArchiveRestoreStopV1::LastArchived;
        let source = Some(DatabaseId::from_unix_milliseconds_and_random(1000, [15; 10]).unwrap());
        let mut receipt = OfflineMaintenanceReceiptV3::accepted_archive_restore(
            base.operation_id(),
            base.backup_name().clone(),
            base.archive_name().clone(),
            stop,
            archive_restore_input_hash(
                base.backup_name(),
                base.archive_name(),
                stop,
                base.replacement_confirmation(),
            ),
            base.replacement_confirmation(),
            OfflineMaintenanceAdmissionV1::new(
                base.admission().principal_id().clone(),
                base.admission().actor_kind(),
                base.admission().capability_id(),
                Some(ApprovalId::new("reviewed-restore").unwrap()),
            ),
            source,
        )
        .unwrap();
        store.create_or_read_archive_receipt(&receipt).unwrap();
        for phase in [Draining, Offline] {
            receipt
                .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
                .unwrap();
            store.replace_archive_receipt(&receipt).unwrap();
        }
        receipt.record_selection(selected.clone()).unwrap();
        receipt
            .record_validated_restore(
                selected.lineage().database_id(),
                selected.terminal_frontier(),
            )
            .unwrap();
        receipt
            .record_published_incarnation(selected.lineage().history_incarnation() + 1)
            .unwrap();
        for phase in [ArtifactPublished, Validating, Succeeded] {
            receipt
                .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
                .unwrap();
            store.replace_archive_receipt(&receipt).unwrap();
            assert_eq!(
                store.read_archive_receipt(receipt.operation_id()).unwrap(),
                Some(receipt.clone())
            );
        }
        let bytes = fs::read(root.receipt_path(receipt.operation_id())).unwrap();
        assert!(bytes.len() <= 4096);
        drop(store);
        let (mut store, reconciliation) = root.open();
        assert_eq!(
            reconciliation.archive_receipts().receipts(),
            std::slice::from_ref(&receipt)
        );
        assert_eq!(
            store.read_archive_receipt(receipt.operation_id()).unwrap(),
            Some(receipt)
        );
    }
}
