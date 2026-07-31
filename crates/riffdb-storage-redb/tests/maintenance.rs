//! External maintenance receipt, reconciliation, and staged-publication evidence.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use riffdb_storage_api::{
    BackupBuildMetadataV1, DatabaseIdentityProbe, DatabaseIdentityProbePort,
    DatabaseInitializationPort, DatabaseInitializationResult, EvidencePageLimit,
    HistoricalEvidenceCursor, HistoricalEvidencePage, OfflineMaintenanceAdmissionV1,
    OfflineMaintenanceReceiptCreateResultV1, OfflineMaintenanceReceiptFailureV1,
    OfflineMaintenanceReceiptPersistencePort, OfflineMaintenanceReceiptPhaseV1,
    OfflineMaintenanceReceiptTransitionV1, OfflineMaintenanceReceiptV1,
    OfflineRestoreOverwritePolicyV1, ReadableCapabilityDigestInventory, ReadableDigestKey,
    ReadableIdempotencyDigestInventory, StartupValidationInputs, StorageErrorKind,
    StructuralEvidenceCursor, StructuralEvidenceOpen, StructuralEvidencePage,
    StructuralEvidenceSession, StructuralOpenOutcome,
};
use riffdb_storage_redb::{
    RedbMaintenanceFailpoint, RedbMaintenanceStorage, RedbMaintenanceTestController, RedbStore,
};
use riffdb_types::{
    ActorId, ActorKind, ApprovalId, BackupNameV1, CapabilityId, DatabaseId, DigestKeyId,
    OfflineMaintenanceOperationId, OfflineMaintenanceOperationKind,
    OfflineMaintenanceReplacementConfirmation, Timestamp, offline_maintenance_input_hash,
};
use sha2::{Digest, Sha256};

static NEXT_TEST_PATH: AtomicU64 = AtomicU64::new(1);

struct TestRoot(PathBuf);

impl TestRoot {
    fn new(label: &str) -> Self {
        let ordinal = NEXT_TEST_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "riffdb-redb-maintenance-{label}-{}-{ordinal}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir(&path).expect("create test root");
        Self(path)
    }

    fn join(&self, value: &str) -> PathBuf {
        self.0.join(value)
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn operation_id(seed: u8) -> OfflineMaintenanceOperationId {
    OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1, [seed; 10])
        .expect("operation ID")
}

fn database_id() -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(2, [0x22; 10]).expect("database ID")
}

fn backup_name() -> BackupNameV1 {
    BackupNameV1::new("before-upgrade").expect("backup name")
}

fn build_metadata() -> BackupBuildMetadataV1 {
    BackupBuildMetadataV1::new(
        "0.1.0",
        "0123456789abcdef",
        "rustc-1.97.0",
        1,
        vec!["maintenance".to_owned()],
    )
    .expect("build metadata")
}

fn admission() -> OfflineMaintenanceAdmissionV1 {
    OfflineMaintenanceAdmissionV1::new(
        ActorId::new("operator-1").expect("actor ID"),
        ActorKind::Human,
        CapabilityId::from_unix_milliseconds_and_random(3, [0x33; 10]).expect("capability ID"),
        Some(ApprovalId::new("approval-1").expect("approval ID")),
    )
}

fn receipt(seed: u8, kind: OfflineMaintenanceOperationKind) -> OfflineMaintenanceReceiptV1 {
    let confirmation = match kind {
        OfflineMaintenanceOperationKind::CreateBackup => {
            OfflineMaintenanceReplacementConfirmation::NotProvided
        }
        OfflineMaintenanceOperationKind::RestoreBackup => {
            OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget
        }
    };
    receipt_with_confirmation(seed, kind, confirmation)
}

fn receipt_with_confirmation(
    seed: u8,
    kind: OfflineMaintenanceOperationKind,
    confirmation: OfflineMaintenanceReplacementConfirmation,
) -> OfflineMaintenanceReceiptV1 {
    let name = backup_name();
    OfflineMaintenanceReceiptV1::accepted(
        operation_id(seed),
        kind,
        name.clone(),
        offline_maintenance_input_hash(kind, &name, confirmation),
        confirmation,
        admission(),
    )
    .expect("accepted receipt")
}

fn create_completed_named_backup(
    storage: &mut RedbMaintenanceStorage,
    seed: u8,
) -> (
    riffdb_storage_api::OfflineBackupManifestV1,
    riffdb_storage_api::OfflineBackupManifestIdentityV1,
) {
    let mut create = receipt(seed, OfflineMaintenanceOperationKind::CreateBackup);
    create
        .record_source_database_id(database_id())
        .expect("source identity");
    advance_offline(&mut create);
    storage
        .create_or_read_receipt(&create)
        .expect("create backup receipt");
    let (manifest, identity) = storage
        .create_named_backup(
            create.operation_id(),
            create.backup_name(),
            &build_metadata(),
        )
        .expect("create named backup");
    create
        .record_manifest_identity(identity.clone())
        .expect("manifest identity");
    for phase in [
        OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
        OfflineMaintenanceReceiptPhaseV1::Validating,
        OfflineMaintenanceReceiptPhaseV1::Succeeded,
    ] {
        create
            .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
            .expect("advance create");
    }
    storage
        .replace_receipt(&create)
        .expect("persist create completion");
    (manifest, identity)
}

fn advance_offline(receipt: &mut OfflineMaintenanceReceiptV1) {
    for phase in [
        OfflineMaintenanceReceiptPhaseV1::Draining,
        OfflineMaintenanceReceiptPhaseV1::Offline,
    ] {
        receipt
            .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
            .expect("advance receipt");
    }
}

fn initialize(path: &Path) {
    let mut store = RedbStore::open(path).expect("open database");
    assert_eq!(
        store.probe_database_identity().expect("probe database"),
        DatabaseIdentityProbe::NeedsInitialization
    );
    assert_eq!(
        store
            .initialize_database(database_id())
            .expect("initialize database"),
        DatabaseInitializationResult::Installed(database_id())
    );
}

fn complete_structural_validation(path: &Path) -> DatabaseId {
    let store = RedbStore::open(path).expect("open staged database");
    let digest_key = DigestKeyId::new(1).expect("digest key ID");
    let inputs = StartupValidationInputs::new(
        Timestamp::new(1_700_000_000, 0).expect("startup timestamp"),
        ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("capability inventory"),
        ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("idempotency inventory"),
    );
    let mut session = store
        .begin_structural_evidence(inputs)
        .expect("begin structural validation");
    let database_id = session.database_id();
    let session_id = session.open_session_id();
    let limit = EvidencePageLimit::new(64).expect("page limit");

    let mut structural = StructuralEvidenceCursor::start(database_id, session_id);
    let structural_end = loop {
        match session
            .read_structural_evidence(structural, limit)
            .expect("structural evidence")
        {
            StructuralEvidencePage::Page { next, .. } => structural = next,
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };
    let mut historical = HistoricalEvidenceCursor::start(database_id, session_id);
    let historical_end = loop {
        match session
            .read_historical_evidence(historical, limit)
            .expect("historical evidence")
        {
            HistoricalEvidencePage::Page { next, .. } => historical = next,
            HistoricalEvidencePage::ExactEnd(end) => break end,
        }
    };
    assert!(matches!(
        session
            .finish(structural_end, historical_end)
            .expect("finish validation"),
        StructuralOpenOutcome::Clean(_)
    ));
    database_id
}

#[test]
fn recovery_target_assessment_distinguishes_healthy_from_absent_empty_or_corrupt() {
    let root = TestRoot::new("target-assessment");
    let database = root.join("database.redb");
    let backup_root = root.join("backups");
    let (storage, _) =
        RedbMaintenanceStorage::open(&database, &backup_root).expect("open maintenance");

    assert!(
        storage
            .configured_target_requires_recovery()
            .expect("assess absent target")
    );
    fs::write(&database, []).expect("create empty target");
    assert!(
        storage
            .configured_target_requires_recovery()
            .expect("assess empty target")
    );
    fs::write(&database, b"not-a-redb-database").expect("create corrupt target");
    assert!(
        storage
            .configured_target_requires_recovery()
            .expect("assess corrupt target")
    );

    fs::remove_file(&database).expect("remove corrupt target");
    initialize(&database);
    assert!(
        !storage
            .configured_target_requires_recovery()
            .expect("assess healthy target")
    );
}

#[test]
fn maintenance_ownership_is_exclusive_across_processes_for_the_storage_lifetime() {
    const CHILD_DATABASE: &str = "RIFFDB_WP155_LOCK_CHILD_DATABASE";
    const CHILD_BACKUP_ROOT: &str = "RIFFDB_WP155_LOCK_CHILD_BACKUP_ROOT";

    if let (Ok(database), Ok(backup_root)) = (
        std::env::var(CHILD_DATABASE),
        std::env::var(CHILD_BACKUP_ROOT),
    ) {
        assert_eq!(
            RedbMaintenanceStorage::open(database, backup_root)
                .expect_err("a second process must not acquire maintenance ownership")
                .kind(),
            StorageErrorKind::Unavailable
        );
        return;
    }

    let root = TestRoot::new("exclusive-owner");
    let database = root.join("database.redb");
    let backup_root = root.join("backups");
    let (storage, _) =
        RedbMaintenanceStorage::open(&database, &backup_root).expect("open maintenance owner");

    let output = Command::new(std::env::current_exe().expect("current test executable"))
        .arg("--exact")
        .arg("maintenance_ownership_is_exclusive_across_processes_for_the_storage_lifetime")
        .arg("--nocapture")
        .env(CHILD_DATABASE, &database)
        .env(CHILD_BACKUP_ROOT, &backup_root)
        .output()
        .expect("run competing maintenance process");
    assert!(
        output.status.success(),
        "competing process failed unexpectedly: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    drop(storage);
    RedbMaintenanceStorage::open(&database, &backup_root)
        .expect("ownership lock must release when storage is dropped");
}

#[test]
fn recovery_stage_failures_and_retries_leave_no_pre_receipt_inventory() {
    const REPEATED_ATTEMPTS: usize = 32;

    let root = TestRoot::new("recovery-stage-cleanup");
    let database = root.join("database.redb");
    let backup_root = root.join("backups");
    initialize(&database);
    let controller = RedbMaintenanceTestController::return_at(
        RedbMaintenanceFailpoint::AfterStagedMaterialization,
    );
    let (mut storage, _) =
        RedbMaintenanceStorage::open_with_test_controller(&database, &backup_root, controller)
            .expect("open maintenance");
    create_completed_named_backup(&mut storage, 0x61);

    let recovery_operation_id = operation_id(0x62);
    let stage_directory = backup_root.join(format!(".maintenance/staged/{recovery_operation_id}"));
    assert_eq!(
        storage
            .stage_recovery_restore_candidate(recovery_operation_id, &backup_name())
            .expect_err("materialization failpoint must return a closed error")
            .kind(),
        StorageErrorKind::Unavailable
    );
    assert!(
        !stage_directory.exists(),
        "a returned materialization failure must synchronously clean its stage"
    );
    for _ in 0..REPEATED_ATTEMPTS {
        let candidate = storage
            .stage_recovery_restore_candidate(recovery_operation_id, &backup_name())
            .expect("materialize recovery candidate");
        assert!(stage_directory.is_dir());
        drop(candidate);
        assert!(
            !stage_directory.exists(),
            "a failed validation or authorization attempt must clean its stage"
        );
        storage
            .discard_pre_receipt_recovery_stage(recovery_operation_id)
            .expect("pre-receipt cleanup is idempotent");
    }

    fs::create_dir(&stage_directory).expect("create abandoned pre-receipt stage");
    fs::write(stage_directory.join("partial"), b"partial").expect("write abandoned stage");
    storage
        .discard_pre_receipt_recovery_stage(recovery_operation_id)
        .expect("explicit server cleanup removes an abandoned attempt");
    assert!(!stage_directory.exists());

    fs::write(
        backup_root
            .join(backup_name().as_str())
            .join("database.redb"),
        b"corrupt-source",
    )
    .expect("corrupt immutable source");
    assert_eq!(
        storage
            .stage_recovery_restore_candidate(recovery_operation_id, &backup_name())
            .expect_err("source validation failure must not leave a stage")
            .kind(),
        StorageErrorKind::CorruptData
    );
    assert!(!stage_directory.exists());
}

#[test]
fn zero_byte_target_needs_no_confirmation_but_every_nonempty_target_does() {
    let root = TestRoot::new("target-confirmation");
    let database = root.join("database.redb");
    let backup_root = root.join("backups");
    initialize(&database);
    let (mut storage, _) =
        RedbMaintenanceStorage::open(&database, &backup_root).expect("open maintenance");
    let (_manifest, manifest_identity) = create_completed_named_backup(&mut storage, 0x63);

    let mut restore = receipt_with_confirmation(
        0x64,
        OfflineMaintenanceOperationKind::RestoreBackup,
        OfflineMaintenanceReplacementConfirmation::NotProvided,
    );
    advance_offline(&mut restore);
    storage
        .create_or_read_receipt(&restore)
        .expect("create restore receipt");
    restore
        .record_staged_database_id(manifest_identity.database_id())
        .expect("staged database identity");
    restore
        .record_manifest_identity(manifest_identity)
        .expect("manifest identity");
    storage
        .replace_receipt(&restore)
        .expect("persist staged evidence");

    let seal = |storage: &RedbMaintenanceStorage| {
        let stage = storage
            .stage_restore(restore.operation_id(), restore.backup_name())
            .expect("materialize stage");
        let staged_database_id = complete_structural_validation(stage.staged_database_file());
        stage
            .seal_after_validation(staged_database_id)
            .expect("seal stage")
    };

    fs::write(&database, []).expect("install zero-byte target");
    assert!(matches!(
        storage
            .publish_sealed_restore(
                seal(&storage),
                OfflineRestoreOverwritePolicyV1::RefuseNonEmpty,
            )
            .expect("zero-byte target publication"),
        riffdb_storage_api::OfflineRestoreResultV1::Restored { .. }
    ));

    let corrupt_bytes = b"nonempty-corrupt-target";
    fs::write(&database, corrupt_bytes).expect("install nonempty corrupt target");
    assert_eq!(
        storage
            .publish_sealed_restore(
                seal(&storage),
                OfflineRestoreOverwritePolicyV1::RefuseNonEmpty,
            )
            .expect("nonempty target is a closed refusal"),
        riffdb_storage_api::OfflineRestoreResultV1::TargetNotEmpty
    );
    assert_eq!(
        fs::read(&database).expect("read refused target"),
        corrupt_bytes
    );

    fs::remove_file(&database).expect("remove corrupt target");
    initialize(&database);
    assert_eq!(
        storage
            .publish_sealed_restore(
                seal(&storage),
                OfflineRestoreOverwritePolicyV1::RefuseNonEmpty,
            )
            .expect("healthy nonempty target is a closed refusal"),
        riffdb_storage_api::OfflineRestoreResultV1::TargetNotEmpty
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        fs::write(&database, b"nonempty-unreadable-target").expect("install unreadable target");
        fs::set_permissions(&database, fs::Permissions::from_mode(0o000))
            .expect("make target unreadable");
        let result = storage
            .publish_sealed_restore(
                seal(&storage),
                OfflineRestoreOverwritePolicyV1::RefuseNonEmpty,
            )
            .expect("unreadable nonempty target is refused before opening");
        fs::set_permissions(&database, fs::Permissions::from_mode(0o600))
            .expect("restore target permissions");
        assert_eq!(
            result,
            riffdb_storage_api::OfflineRestoreResultV1::TargetNotEmpty
        );
    }
}

#[test]
fn receipt_codec_is_canonical_checksummed_and_version_closed() {
    let root = TestRoot::new("receipt-golden");
    let database = root.join("database.redb");
    let backup_root = root.join("backups");
    let (mut storage, startup) =
        RedbMaintenanceStorage::open(&database, &backup_root).expect("open maintenance");
    assert!(startup.receipts().receipts().is_empty());

    let mut receipt = receipt(0x11, OfflineMaintenanceOperationKind::CreateBackup);
    assert_eq!(
        storage
            .create_or_read_receipt(&receipt)
            .expect("create receipt"),
        OfflineMaintenanceReceiptCreateResultV1::Created
    );
    receipt
        .advance(OfflineMaintenanceReceiptTransitionV1::failed(
            OfflineMaintenanceReceiptFailureV1::QuiescenceFailed,
        ))
        .expect("failed closed");
    storage.replace_receipt(&receipt).expect("replace receipt");
    drop(storage);

    let receipt_path = backup_root.join(format!(
        ".maintenance/receipts/{}.receipt-v1",
        receipt.operation_id()
    ));
    let encoded = fs::read(&receipt_path).expect("read receipt");
    assert!(encoded.len() < 4 * 1024);
    let golden: [u8; 32] = Sha256::digest(&encoded).into();
    assert_eq!(
        golden,
        [
            5, 10, 194, 95, 12, 51, 83, 164, 41, 176, 31, 99, 133, 174, 71, 163, 61, 62, 32, 125,
            47, 105, 14, 207, 22, 190, 87, 11, 174, 41, 232, 189,
        ],
        "receipt-v1 exact bytes are a compatibility boundary"
    );

    let mut corrupt = encoded.clone();
    *corrupt.last_mut().expect("checksum byte") ^= 0xff;
    fs::write(&receipt_path, &corrupt).expect("write corruption");
    assert_eq!(
        RedbMaintenanceStorage::open(&database, &backup_root)
            .expect_err("bad checksum must fail closed")
            .kind(),
        StorageErrorKind::CorruptData
    );

    let mut unsupported = encoded;
    unsupported[24] = 2;
    let body_length = unsupported.len() - 32;
    let replacement: [u8; 32] = Sha256::digest(&unsupported[..body_length]).into();
    unsupported[body_length..].copy_from_slice(&replacement);
    fs::write(&receipt_path, unsupported).expect("write unsupported receipt");
    assert_eq!(
        RedbMaintenanceStorage::open(&database, &backup_root)
            .expect_err("unknown version must fail closed")
            .kind(),
        StorageErrorKind::IncompatibleFormat
    );
}

#[test]
fn receipt_failpoints_distinguish_absence_from_uncertain_publication() {
    let root = TestRoot::new("receipt-failpoints");
    let database = root.join("database.redb");

    let before_root = root.join("before");
    let before =
        RedbMaintenanceTestController::return_at(RedbMaintenanceFailpoint::BeforeReceiptRename);
    let (mut before_storage, _) =
        RedbMaintenanceStorage::open_with_test_controller(&database, &before_root, before)
            .expect("open before store");
    let before_receipt = receipt(0x21, OfflineMaintenanceOperationKind::CreateBackup);
    let error = before_storage
        .create_or_read_receipt(&before_receipt)
        .expect_err("before rename must fail");
    assert_eq!(error.kind(), StorageErrorKind::Unavailable);
    assert!(
        before_storage
            .read_receipt(before_receipt.operation_id())
            .expect("read receipt")
            .is_none()
    );

    let after_root = root.join("after");
    let after =
        RedbMaintenanceTestController::return_at(RedbMaintenanceFailpoint::AfterReceiptRename);
    let (mut after_storage, _) =
        RedbMaintenanceStorage::open_with_test_controller(&database, &after_root, after)
            .expect("open after store");
    let after_receipt = receipt(0x22, OfflineMaintenanceOperationKind::CreateBackup);
    let error = after_storage
        .create_or_read_receipt(&after_receipt)
        .expect_err("after rename is uncertain");
    assert_eq!(error.kind(), StorageErrorKind::CommitStatusUnknown);
    assert_eq!(
        after_storage
            .read_receipt(after_receipt.operation_id())
            .expect("read uncertain receipt"),
        Some(after_receipt)
    );
    assert_eq!(
        after_storage
            .create_or_read_receipt(&receipt(
                0x23,
                OfflineMaintenanceOperationKind::CreateBackup,
            ))
            .expect_err("one incomplete operation is exclusive")
            .kind(),
        StorageErrorKind::InvariantViolation
    );
}

#[test]
fn startup_discards_only_recognized_unpublished_temps_and_rejects_unknown_inventory() {
    let root = TestRoot::new("inventory");
    let database = root.join("database.redb");
    let backup_root = root.join("backups");
    let (storage, _) =
        RedbMaintenanceStorage::open(&database, &backup_root).expect("open maintenance");
    drop(storage);
    let temporary = backup_root.join(format!(
        ".maintenance/receipts/.{}.receipt-v1.tmp",
        operation_id(0x31)
    ));
    fs::write(&temporary, b"partial unpublished bytes").expect("write temp");
    let (storage, evidence) =
        RedbMaintenanceStorage::open(&database, &backup_root).expect("reconcile temp");
    assert_eq!(evidence.removed_unpublished_receipt_temps(), 1);
    assert!(!temporary.exists());
    drop(storage);

    let unknown = backup_root.join(".maintenance/receipts/.not-an-operation.receipt-v1.tmp");
    fs::write(&unknown, b"unknown").expect("write unknown file");
    assert_eq!(
        RedbMaintenanceStorage::open(&database, &backup_root)
            .expect_err("unknown reserved inventory")
            .kind(),
        StorageErrorKind::CorruptData
    );
    assert!(unknown.exists(), "unknown files are never silently deleted");
}

#[test]
fn named_backup_publication_retry_resolves_only_the_reserved_operation_artifact() {
    let root = TestRoot::new("backup-publication-retry");
    let database = root.join("database.redb");
    let backup_root = root.join("backups");
    initialize(&database);
    let controller = RedbMaintenanceTestController::return_at(
        RedbMaintenanceFailpoint::AfterNamedBackupPublication,
    );
    let observed = controller.clone();
    let (mut storage, _) =
        RedbMaintenanceStorage::open_with_test_controller(&database, &backup_root, controller)
            .expect("open maintenance");

    let mut create = receipt(0x40, OfflineMaintenanceOperationKind::CreateBackup);
    create
        .record_source_database_id(database_id())
        .expect("source database identity");
    advance_offline(&mut create);
    storage
        .create_or_read_receipt(&create)
        .expect("reserve named backup");

    let error = storage
        .create_named_backup(
            create.operation_id(),
            create.backup_name(),
            &build_metadata(),
        )
        .expect_err("lost response after publication is uncertain");
    assert_eq!(error.kind(), StorageErrorKind::CommitStatusUnknown);
    let backup_directory = backup_root.join(backup_name().as_str());
    assert_eq!(
        fs::read_dir(&backup_directory)
            .expect("published backup")
            .count(),
        2
    );

    let (manifest, identity) = storage
        .create_named_backup(
            create.operation_id(),
            create.backup_name(),
            &build_metadata(),
        )
        .expect("same operation resolves exact published artifact");
    assert_eq!(manifest.database_id(), database_id());
    assert_eq!(
        observed
            .events()
            .iter()
            .filter(|event| {
                event.failpoint() == RedbMaintenanceFailpoint::AfterNamedBackupPublication
            })
            .count(),
        1,
        "retry validates rather than republishing"
    );
    let reconciliation = storage.reconcile().expect("reconcile published backup");
    assert!(
        reconciliation
            .operations()
            .iter()
            .any(|evidence| evidence.operation_id() == create.operation_id()
                && evidence.named_backup_matches())
    );

    create
        .record_manifest_identity(identity)
        .expect("manifest identity");
    for phase in [
        OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
        OfflineMaintenanceReceiptPhaseV1::Validating,
        OfflineMaintenanceReceiptPhaseV1::Succeeded,
    ] {
        create
            .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
            .expect("advance create");
    }
    storage
        .replace_receipt(&create)
        .expect("persist terminal receipt");

    let mut different_operation = receipt(0x44, OfflineMaintenanceOperationKind::CreateBackup);
    different_operation
        .record_source_database_id(database_id())
        .expect("source database identity");
    assert_eq!(
        storage
            .create_or_read_receipt(&different_operation)
            .expect_err("another operation cannot adopt the existing name")
            .kind(),
        StorageErrorKind::Unavailable
    );
    assert!(
        storage
            .read_receipt(different_operation.operation_id())
            .expect("read absent different operation")
            .is_none()
    );
}

#[test]
fn restore_stamps_staged_before_publish_so_checksum_reconcile_holds() {
    // C2 option (b): stamp staged + reseal before rename; ArtifactPublished
    // reconcile compares target to staged seal and must remain valid.
    let root = TestRoot::new("stamp-before-publish");
    let database = root.join("database.redb");
    let backup_root = root.join("backups");
    initialize(&database);
    let (mut storage, _) =
        RedbMaintenanceStorage::open(&database, &backup_root).expect("open maintenance");
    let (manifest, manifest_identity) = create_completed_named_backup(&mut storage, 0x51);

    // Advance target incarnation so bump is max(1,1)+1 = 2 after a second open.
    drop(storage);
    riffdb_storage_redb::stamp_history_incarnation(&database, 5).expect("bump target");
    let (mut storage, _) =
        RedbMaintenanceStorage::open(&database, &backup_root).expect("reopen maintenance");

    let mut restore = receipt(0x52, OfflineMaintenanceOperationKind::RestoreBackup);
    advance_offline(&mut restore);
    storage
        .create_or_read_receipt(&restore)
        .expect("create restore receipt");
    let staged = storage
        .stage_restore(restore.operation_id(), restore.backup_name())
        .expect("stage");
    let staged_database_id = complete_structural_validation(staged.staged_database_file());
    let mut sealed = staged
        .seal_after_validation(staged_database_id)
        .expect("seal");
    let staged_before = sealed.staged_history_incarnation();
    assert!(staged_before <= 5);
    let published = 5u64.max(staged_before) + 1;
    sealed
        .apply_published_history_incarnation(published)
        .expect("stamp staged");
    assert_eq!(sealed.staged_history_incarnation(), published);

    restore
        .record_staged_database_id(staged_database_id)
        .expect("staged id");
    restore
        .record_manifest_identity(manifest_identity)
        .expect("manifest");
    restore
        .record_published_incarnation(published)
        .expect("receipt incarnation");
    storage
        .replace_receipt(&restore)
        .expect("persist receipt evidence");

    let result = storage
        .publish_sealed_restore(
            sealed,
            OfflineRestoreOverwritePolicyV1::ExplicitlyAllowDestructive,
        )
        .expect("publish");
    assert!(matches!(
        result,
        riffdb_storage_api::OfflineRestoreResultV1::Restored { .. }
    ));
    assert_eq!(
        riffdb_storage_redb::read_history_incarnation(&database).expect("read target"),
        Some(published)
    );

    restore
        .advance(OfflineMaintenanceReceiptTransitionV1::phase(
            OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
        ))
        .expect("published phase");
    storage
        .replace_receipt(&restore)
        .expect("persist published");

    // Reconcile must accept the stamped target (checksum seal intact).
    let reconciliation = storage.reconcile().expect("reconcile after stamp+publish");
    let evidence = reconciliation
        .operations()
        .iter()
        .find(|evidence| evidence.operation_id() == restore.operation_id())
        .expect("restore evidence");
    assert!(
        evidence.configured_target_matches(),
        "stamped target must match staged seal after option-b publication"
    );
    assert_eq!(manifest.database_id(), database_id());
}

#[test]
fn pre_fence_receipt_decode_round_trips_without_published_incarnation() {
    // Dual-path receipt decode: pre-fence bytes (no field) load as None.
    let mut restore = receipt(0x61, OfflineMaintenanceOperationKind::RestoreBackup);
    advance_offline(&mut restore);
    assert!(restore.published_history_incarnation().is_none());
    // Encoding goes through maintenance storage write path; validate via fixture
    // API that pre-fence receipts remain structurally accepted.
    let root = TestRoot::new("pre-fence-receipt");
    let database = root.join("database.redb");
    let backup_root = root.join("backups");
    initialize(&database);
    let (mut storage, _) = RedbMaintenanceStorage::open(&database, &backup_root).expect("open");
    storage
        .create_or_read_receipt(&restore)
        .expect("persist pre-fence-shaped receipt");
    let loaded = storage
        .read_receipt(restore.operation_id())
        .expect("read")
        .expect("present");
    assert!(loaded.published_history_incarnation().is_none());
    assert_eq!(loaded.operation_id(), restore.operation_id());
}

#[test]
fn named_backup_staged_validation_and_exact_file_publication_reuse_wp070() {
    let root = TestRoot::new("staged-roundtrip");
    let database = root.join("database.redb");
    let backup_root = root.join("backups");
    initialize(&database);
    let publication_failure =
        RedbMaintenanceTestController::return_at(RedbMaintenanceFailpoint::AfterTargetPublication);
    let (mut storage, _) = RedbMaintenanceStorage::open_with_test_controller(
        &database,
        &backup_root,
        publication_failure,
    )
    .expect("open maintenance");

    let mut create = receipt(0x41, OfflineMaintenanceOperationKind::CreateBackup);
    create
        .record_source_database_id(database_id())
        .expect("source identity");
    advance_offline(&mut create);
    storage
        .create_or_read_receipt(&create)
        .expect("create backup receipt");
    let (manifest, manifest_identity) = storage
        .create_named_backup(
            create.operation_id(),
            create.backup_name(),
            &build_metadata(),
        )
        .expect("create named backup");
    create
        .record_manifest_identity(manifest_identity.clone())
        .expect("manifest identity");
    for phase in [
        OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
        OfflineMaintenanceReceiptPhaseV1::Validating,
        OfflineMaintenanceReceiptPhaseV1::Succeeded,
    ] {
        create
            .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
            .expect("advance create");
    }
    storage
        .replace_receipt(&create)
        .expect("persist create completion");

    let recovery_operation_id = operation_id(0x43);
    assert_eq!(
        storage
            .stage_restore(recovery_operation_id, &backup_name())
            .expect_err("normal staging requires an offline receipt")
            .kind(),
        StorageErrorKind::Unavailable
    );
    let target_before_recovery = Sha256::digest(fs::read(&database).expect("read target"));
    let recovery_candidate = storage
        .stage_recovery_restore_candidate(recovery_operation_id, &backup_name())
        .expect("materialize recovery candidate");
    assert!(recovery_candidate.staged_database_file().is_file());
    assert!(
        storage
            .read_receipt(recovery_operation_id)
            .expect("read absent recovery receipt")
            .is_none()
    );
    assert_eq!(
        Sha256::digest(fs::read(&database).expect("read unchanged target")),
        target_before_recovery,
        "recovery staging must neither publish the target nor create a receipt"
    );
    drop(recovery_candidate);
    assert!(
        !backup_root
            .join(format!(".maintenance/staged/{}", recovery_operation_id))
            .exists(),
        "dropping a pre-receipt attempt cleans it immediately"
    );
    let recovery_cleanup = storage
        .reconcile()
        .expect("confirm no deferred recovery cleanup");
    assert_eq!(recovery_cleanup.removed_incomplete_stages(), 0);

    let mut restore = receipt(0x42, OfflineMaintenanceOperationKind::RestoreBackup);
    advance_offline(&mut restore);
    storage
        .create_or_read_receipt(&restore)
        .expect("create restore receipt");
    let incomplete_stage =
        backup_root.join(format!(".maintenance/staged/{}", restore.operation_id()));
    fs::create_dir(&incomplete_stage).expect("create incomplete stage");
    fs::write(incomplete_stage.join("partial"), b"not published").expect("write partial stage");
    assert_eq!(
        storage
            .discard_pre_receipt_recovery_stage(restore.operation_id())
            .expect_err("pre-receipt cleanup cannot delete receipt-backed evidence")
            .kind(),
        StorageErrorKind::InvariantViolation
    );
    assert!(incomplete_stage.exists());
    let target_temp_name = format!(
        ".database.redb.riffdb-maintenance-publish-{}.tmp",
        restore.operation_id()
    );
    let target_temp = root.join(&target_temp_name);
    fs::write(&target_temp, b"not renamed").expect("write target temp");
    let cleanup = storage.reconcile().expect("reconcile unpublished files");
    assert_eq!(cleanup.removed_incomplete_stages(), 1);
    assert_eq!(cleanup.removed_unpublished_target_temps(), 1);
    assert!(!incomplete_stage.exists());
    assert!(!target_temp.exists());

    let staged = storage
        .stage_restore(restore.operation_id(), restore.backup_name())
        .expect("materialize stage");
    assert_eq!(staged.manifest(), &manifest);
    assert_eq!(staged.manifest_identity(), &manifest_identity);
    let staged_database_id = complete_structural_validation(staged.staged_database_file());
    let premature_sealed = staged
        .seal_after_validation(staged_database_id)
        .expect("seal validated stage");
    assert_eq!(
        storage
            .publish_sealed_restore(
                premature_sealed,
                OfflineRestoreOverwritePolicyV1::ExplicitlyAllowDestructive,
            )
            .expect_err("publication requires durable receipt evidence")
            .kind(),
        StorageErrorKind::InvariantViolation
    );
    let staged = storage
        .stage_restore(restore.operation_id(), restore.backup_name())
        .expect("rematerialize after refused publication");
    let staged_database_id = complete_structural_validation(staged.staged_database_file());
    let sealed = staged
        .seal_after_validation(staged_database_id)
        .expect("reseal validated stage");

    restore
        .record_staged_database_id(staged_database_id)
        .expect("staged database");
    restore
        .record_manifest_identity(manifest_identity)
        .expect("restore manifest");
    storage
        .replace_receipt(&restore)
        .expect("persist staged evidence");
    let publication_error = storage
        .publish_sealed_restore(
            sealed,
            OfflineRestoreOverwritePolicyV1::ExplicitlyAllowDestructive,
        )
        .expect_err("post-publication response is uncertain");
    assert_eq!(
        publication_error.kind(),
        StorageErrorKind::CommitStatusUnknown
    );
    let uncertain = storage
        .reconcile()
        .expect("reconcile uncertain publication");
    let uncertain_evidence = uncertain
        .operations()
        .iter()
        .find(|evidence| evidence.operation_id() == restore.operation_id())
        .expect("uncertain restore evidence");
    assert!(uncertain_evidence.configured_target_matches());
    restore
        .advance(OfflineMaintenanceReceiptTransitionV1::phase(
            OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
        ))
        .expect("record publication");
    storage
        .replace_receipt(&restore)
        .expect("persist publication");
    drop(storage);

    let (storage, recovered) =
        RedbMaintenanceStorage::open(&database, &backup_root).expect("reconcile publication");
    let evidence = recovered
        .operations()
        .iter()
        .find(|evidence| evidence.operation_id() == restore.operation_id())
        .expect("restore evidence");
    assert!(evidence.named_backup_matches());
    assert!(evidence.staged_restore_matches());
    assert!(evidence.configured_target_matches());
    assert_eq!(
        complete_structural_validation(storage.configured_database_file()),
        database_id()
    );
    drop(storage);
    fs::write(&database, b"not-the-sealed-stage").expect("replace target with mismatch");
    assert_eq!(
        RedbMaintenanceStorage::open(&database, &backup_root)
            .expect_err("published target mismatch must fail closed")
            .kind(),
        StorageErrorKind::CorruptData
    );
}

#[test]
fn overlapping_or_symlinked_maintenance_roots_fail_closed_without_path_text() {
    let root = TestRoot::new("paths");
    let backup_root = root.join("backups");
    fs::create_dir(&backup_root).expect("create backup root");
    let database_inside_root = backup_root.join("database.redb");
    let error = RedbMaintenanceStorage::open(&database_inside_root, &backup_root)
        .expect_err("overlap must reject");
    assert_eq!(error.kind(), StorageErrorKind::InvariantViolation);
    assert!(!format!("{error:?} {error}").contains("backups"));

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let real = root.join("real");
        fs::create_dir(&real).expect("create real root");
        let linked = root.join("linked");
        symlink(&real, &linked).expect("create symlink");
        assert_eq!(
            RedbMaintenanceStorage::open(root.join("database.redb"), &linked)
                .expect_err("symlink root must reject")
                .kind(),
            StorageErrorKind::CorruptData
        );
    }
}

#[test]
fn maintenance_detects_backup_root_and_database_parent_replacement_after_open() {
    let backup_case = TestRoot::new("backup-root-swap");
    let database = backup_case.join("database.redb");
    let backup_root = backup_case.join("backups");
    let moved_backup_root = backup_case.join("backups-original");
    let (mut storage, _) =
        RedbMaintenanceStorage::open(&database, &backup_root).expect("open maintenance");
    fs::rename(&backup_root, &moved_backup_root).expect("move pinned backup root");
    fs::create_dir(&backup_root).expect("replace backup root");
    assert_eq!(
        storage
            .reconcile()
            .expect_err("replacement backup root must fail closed")
            .kind(),
        StorageErrorKind::CorruptData
    );
    drop(storage);

    let database_case = TestRoot::new("database-parent-swap");
    let database_parent = database_case.join("data");
    fs::create_dir(&database_parent).expect("create database parent");
    let database = database_parent.join("database.redb");
    let backup_root = database_case.join("backups");
    let moved_database_parent = database_case.join("data-original");
    let (storage, _) =
        RedbMaintenanceStorage::open(&database, &backup_root).expect("open maintenance");
    fs::rename(&database_parent, &moved_database_parent).expect("move pinned database parent");
    fs::create_dir(&database_parent).expect("replace database parent");
    assert_eq!(
        storage
            .configured_target_requires_recovery()
            .expect_err("replacement database parent must fail closed")
            .kind(),
        StorageErrorKind::CorruptData
    );
}
