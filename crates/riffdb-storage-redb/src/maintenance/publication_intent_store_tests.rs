//! Intent durability and inventory only; these tests grant no restore readiness.
// req: REC-001, REC-004, REC-005, REC-006, REC-008
use super::*;
use crate::test_path::ScopedDirectory;
use riffdb_storage_api::{
    BackupIntegrityChecksumV1, OfflineMaintenanceAdmissionV1, OfflineMaintenanceReceiptTransitionV1,
};
use riffdb_types::{ActorId, ActorKind, CapabilityId, offline_maintenance_input_hash};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn intent() -> PublicationIntent {
    let hex = include_str!(
        "../../../../fixtures/compatibility/native-publication-intent-v1/prepared.hex"
    )
    .trim();
    let bytes: Vec<u8> = hex
        .as_bytes()
        .chunks_exact(2)
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect();
    PublicationIntent::decode(&bytes).unwrap()
}

fn fixture(
    root: &Path,
) -> (
    RedbMaintenanceStorage,
    OfflineMaintenanceReceiptV1,
    PublicationIntent,
) {
    fs::create_dir(root).unwrap();
    let (mut storage, _) =
        RedbMaintenanceStorage::open(root.join("configured.redb"), root.join("backups")).unwrap();
    let intent = intent();
    let backup = BackupNameV1::new("synthetic-intent-codec-binding").unwrap();
    let kind = OfflineMaintenanceOperationKind::RestoreBackup;
    let confirmation = OfflineMaintenanceReplacementConfirmation::NotProvided;
    let mut receipt = OfflineMaintenanceReceiptV1::accepted(
        intent.operation_id,
        kind,
        backup.clone(),
        offline_maintenance_input_hash(kind, &backup, confirmation),
        confirmation,
        OfflineMaintenanceAdmissionV1::new(
            ActorId::new("operator").unwrap(),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(1234, [0x72; 10]).unwrap(),
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
    receipt
        .record_staged_database_id(intent.manifest.database_id())
        .unwrap();
    receipt
        .record_manifest_identity(intent.manifest.clone())
        .unwrap();
    receipt
        .record_published_incarnation(intent.incarnation)
        .unwrap();
    storage.replace_receipt(&receipt).unwrap();
    (storage, receipt, intent)
}

#[test]
fn native_publication_intent_store_advances_only_the_same_receipt_and_original_seals() {
    let root = ScopedDirectory::new("intent-store-phases");
    let (mut storage, mut receipt, prepared) = fixture(&root.join("operation"));
    storage
        .write_native_publication_intent(None, &prepared)
        .unwrap();
    let mut published = prepared.clone();
    published.advance(PublicationPhase::Published).unwrap();
    storage
        .write_native_publication_intent(Some(&prepared), &published)
        .unwrap();
    for phase in [
        OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
        OfflineMaintenanceReceiptPhaseV1::Validating,
    ] {
        receipt
            .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
            .unwrap();
        storage.replace_receipt(&receipt).unwrap();
    }
    let mut consumed = published.clone();
    consumed.advance(PublicationPhase::StartupConsumed).unwrap();
    storage
        .write_native_publication_intent(Some(&published), &consumed)
        .unwrap();
    let observed = IntentInventory::read(&storage.receipts_directory_guard)
        .unwrap()
        .current
        .unwrap();
    assert!(observed == consumed);
    assert!(observed.artifacts == prepared.artifacts);
    assert!(
        storage
            .write_native_publication_intent(Some(&published), &consumed)
            .is_err()
    );
    assert!(
        storage
            .write_native_publication_intent(Some(&consumed), &published)
            .is_err()
    );
    receipt
        .advance(OfflineMaintenanceReceiptTransitionV1::phase(
            OfflineMaintenanceReceiptPhaseV1::Succeeded,
        ))
        .unwrap();
    storage.replace_receipt(&receipt).unwrap();
    storage.read_complete_inventory(false).unwrap();
    assert!(
        !storage.database_file.exists(),
        "intent facts never create a database or application writer"
    );
}

#[test]
fn native_publication_intent_store_refuses_a_changed_seal_before_replacing_the_original_record() {
    let root = ScopedDirectory::new("intent-store-changed-seal");
    let (storage, _, prepared) = fixture(&root.join("operation"));
    storage
        .write_native_publication_intent(None, &prepared)
        .unwrap();
    let mut changed = prepared.clone();
    changed.advance(PublicationPhase::Published).unwrap();
    changed.artifacts[0].checksum = BackupIntegrityChecksumV1::new(vec![0x79; 32]).unwrap();
    assert!(
        storage
            .write_native_publication_intent(Some(&prepared), &changed)
            .is_err()
    );
    let inventory = IntentInventory::read(&storage.receipts_directory_guard).unwrap();
    assert!(inventory.current == Some(prepared));
    assert!(inventory.temporary.is_none());
}

#[test]
fn native_publication_intent_store_validates_all_bindings_before_cleaning_any_temporary() {
    let root = ScopedDirectory::new("intent-store-inventory-refusal");
    let (storage, _, prepared) = fixture(&root.join("operation"));
    storage
        .write_native_publication_intent(None, &prepared)
        .unwrap();
    let record = storage
        .receipts_directory
        .join(format!("{}{SUFFIX}", prepared.operation_id));
    let ordinary = storage
        .receipts_directory
        .join(format!(".{}.receipt-v1.tmp", prepared.operation_id));
    fs::write(&ordinary, b"ordinary temporary evidence").unwrap();
    let mut changed = prepared.clone();
    changed.manifest = OfflineBackupManifestIdentityV1::new(
        BackupIntegrityChecksumV1::new(vec![0x78; 32]).unwrap(),
        changed.manifest.database_id(),
        changed.manifest.included_application_frontier(),
    );
    let bytes = changed.encode().unwrap();
    fs::write(&record, &bytes).unwrap();
    assert!(storage.read_complete_inventory(true).is_err());
    assert_eq!(fs::read(&ordinary).unwrap(), b"ordinary temporary evidence");
    assert_eq!(fs::read(&record).unwrap(), bytes);
    assert!(!storage.database_file.exists());
}

#[test]
fn native_publication_intent_store_rejects_multiple_operations_and_unknown_inventory_before_cleanup()
 {
    let root = ScopedDirectory::new("intent-store-foreign-operation");
    let (storage, _, prepared) = fixture(&root.join("operation"));
    storage
        .write_native_publication_intent(None, &prepared)
        .unwrap();
    let ordinary = storage
        .receipts_directory
        .join(format!(".{}.receipt-v1.tmp", prepared.operation_id));
    fs::write(&ordinary, b"ordinary evidence").unwrap();
    for name in [
        format!(
            ".{}{TEMP_SUFFIX}",
            OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1234, [0x73; 10])
                .unwrap()
        ),
        "unknown-intent-v2".to_string(),
    ] {
        let foreign = storage.receipts_directory.join(name);
        fs::write(&foreign, b"foreign evidence").unwrap();
        assert!(storage.read_complete_inventory(true).is_err());
        assert_eq!(fs::read(&ordinary).unwrap(), b"ordinary evidence");
        assert_eq!(fs::read(&foreign).unwrap(), b"foreign evidence");
        fs::remove_file(foreign).unwrap();
    }
}

const CUTS: [RedbMaintenanceFailpoint; 5] = [
    RedbMaintenanceFailpoint::BeforeReceiptFileSync,
    RedbMaintenanceFailpoint::AfterReceiptFileSync,
    RedbMaintenanceFailpoint::BeforeReceiptRename,
    RedbMaintenanceFailpoint::AfterReceiptRename,
    RedbMaintenanceFailpoint::AfterReceiptParentSync,
];

#[test]
fn native_publication_intent_store_process_child() {
    let Some(root) = std::env::var_os("RIFFDB_TEST_INTENT_STORE_ROOT") else {
        return;
    };
    let selection: usize = std::env::var("RIFFDB_TEST_INTENT_STORE_CUT")
        .unwrap()
        .parse()
        .unwrap();
    assert!(selection < 15);
    let phase = selection / 5;
    let cut = CUTS[selection % 5];
    let (mut storage, mut receipt, prepared) = fixture(Path::new(&root));
    let mut prior = None;
    let mut next = prepared;
    for step in 0..phase {
        storage
            .write_native_publication_intent(prior.as_ref(), &next)
            .unwrap();
        prior = Some(next.clone());
        next.advance(if step == 0 {
            PublicationPhase::Published
        } else {
            PublicationPhase::StartupConsumed
        })
        .unwrap();
    }
    if phase == 2 {
        for state in [
            OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
            OfflineMaintenanceReceiptPhaseV1::Validating,
        ] {
            receipt
                .advance(OfflineMaintenanceReceiptTransitionV1::phase(state))
                .unwrap();
            storage.replace_receipt(&receipt).unwrap();
        }
    }
    storage.test_controller = Some(RedbMaintenanceTestController::abort_at(cut));
    storage
        .write_native_publication_intent(prior.as_ref(), &next)
        .unwrap();
    panic!("intent store process must stop at the selected durable boundary");
}

#[test]
fn native_publication_intent_store_process_cuts_preserve_prior_or_next_phase_without_creating_readiness()
 {
    for selection in 0..15 {
        let scope = ScopedDirectory::new("intent-store-process-cut");
        let root = scope.join("child");
        let mut child=Command::new(std::env::current_exe().unwrap())
            .args(["--exact","maintenance::store::native_publication::tests::native_publication_intent_store_process_child","--nocapture"])
            .env("RIFFDB_TEST_INTENT_STORE_ROOT",&root).env("RIFFDB_TEST_INTENT_STORE_CUT",selection.to_string())
            .stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::inherit()).spawn().unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("bounded intent-store child exceeded its deadline");
            }
            std::thread::yield_now();
        };
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(
                status.signal(),
                Some(6),
                "must observe the actual abort boundary"
            );
        }
        let directory = PinnedDirectory::open(&root.join("backups/.maintenance/receipts")).unwrap();
        let inventory = IntentInventory::read(&directory).unwrap();
        let expected_phase = match selection / 5 {
            0 => PublicationPhase::Prepared,
            1 => PublicationPhase::Published,
            _ => PublicationPhase::StartupConsumed,
        };
        if selection % 5 >= 3 {
            assert_eq!(inventory.current.unwrap().phase, expected_phase);
            assert!(inventory.temporary.is_none());
        } else {
            let expected_prior = match expected_phase {
                PublicationPhase::Prepared => None,
                PublicationPhase::Published => Some(PublicationPhase::Prepared),
                PublicationPhase::StartupConsumed => Some(PublicationPhase::Published),
            };
            assert_eq!(inventory.current.map(|v| v.phase), expected_prior);
            assert!(inventory.temporary.is_some());
        }
        assert!(
            !root.join("configured.redb").exists(),
            "no intent phase grants engine or application readiness"
        );
        let ordinary = read_receipt_file(
            &root
                .join("backups/.maintenance/receipts")
                .join(receipt_file_name(intent().operation_id)),
        )
        .unwrap();
        assert_ne!(
            ordinary.current_phase(),
            OfflineMaintenanceReceiptPhaseV1::Succeeded
        );
    }
}
