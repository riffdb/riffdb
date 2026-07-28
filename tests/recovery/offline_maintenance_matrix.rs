#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]

//! Integrated WP-190 external-maintenance process recovery matrix.

use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use riffdb_storage_api::{
    BackupBuildMetadataV1, DatabaseIdentityProbe, DatabaseIdentityProbePort,
    DatabaseInitializationPort, DatabaseInitializationResult, EvidencePageLimit,
    HistoricalEvidenceCursor, HistoricalEvidencePage, OfflineMaintenanceAdmissionV1,
    OfflineMaintenanceReceiptCreateResultV1, OfflineMaintenanceReceiptPersistencePort,
    OfflineMaintenanceReceiptPhaseV1, OfflineMaintenanceReceiptTransitionV1,
    OfflineMaintenanceReceiptV1, OfflineRestoreOverwritePolicyV1,
    ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
    StartupValidationInputs, StructuralEvidenceCursor, StructuralEvidenceOpen,
    StructuralEvidencePage, StructuralEvidenceSession, StructuralOpenOutcome,
};
use riffdb_storage_redb::{
    RedbMaintenanceFailpoint, RedbMaintenanceReconciliation, RedbMaintenanceStorage, RedbStore,
};
use riffdb_testkit::failpoint::verify_wp190_recovery_report_v1;
use riffdb_testkit::process::{ChildProcessController, ChildProcessSpec};
use riffdb_types::{
    ActorId, ActorKind, ApprovalId, BackupNameV1, CapabilityId, DatabaseId, DigestKeyId,
    OfflineMaintenanceOperationId, OfflineMaintenanceOperationKind,
    OfflineMaintenanceReplacementConfirmation, Timestamp, offline_maintenance_input_hash,
};

const CHILD_MODE: &str = "RIFFDB_WP190_MAINTENANCE_CHILD";
const CHILD_ACTION: &str = "RIFFDB_WP190_MAINTENANCE_ACTION";
const CHILD_FAILPOINT: &str = "RIFFDB_WP190_MAINTENANCE_FAILPOINT";
const CHILD_DATABASE: &str = "RIFFDB_WP190_MAINTENANCE_DATABASE";
const CHILD_BACKUP_ROOT: &str = "RIFFDB_WP190_MAINTENANCE_BACKUP_ROOT";
const CHILD_DEDICATED: &str = "dedicated-crash-child-v1";
const CHILD_TIMEOUT: Duration = Duration::from_secs(20);
const BACKUP_NAME: &str = "wp190-baseline";

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CrashAction {
    CreateReceipt,
    CreateBackup,
    StageRestore,
    PublishRestore,
}

impl CrashAction {
    const fn as_str(self) -> &'static str {
        match self {
            Self::CreateReceipt => "create-receipt",
            Self::CreateBackup => "create-backup",
            Self::StageRestore => "stage-restore",
            Self::PublishRestore => "publish-restore",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "create-receipt" => Some(Self::CreateReceipt),
            "create-backup" => Some(Self::CreateBackup),
            "stage-restore" => Some(Self::StageRestore),
            "publish-restore" => Some(Self::PublishRestore),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CrashCase {
    id: &'static str,
    action: CrashAction,
    failpoint: RedbMaintenanceFailpoint,
}

const RECEIPT_CASES: &[CrashCase] = &[
    CrashCase {
        id: "maintenance.receipt.before-file-sync",
        action: CrashAction::CreateReceipt,
        failpoint: RedbMaintenanceFailpoint::BeforeReceiptFileSync,
    },
    CrashCase {
        id: "maintenance.receipt.after-file-sync",
        action: CrashAction::CreateReceipt,
        failpoint: RedbMaintenanceFailpoint::AfterReceiptFileSync,
    },
    CrashCase {
        id: "maintenance.receipt.before-rename",
        action: CrashAction::CreateReceipt,
        failpoint: RedbMaintenanceFailpoint::BeforeReceiptRename,
    },
    CrashCase {
        id: "maintenance.receipt.after-rename",
        action: CrashAction::CreateReceipt,
        failpoint: RedbMaintenanceFailpoint::AfterReceiptRename,
    },
    CrashCase {
        id: "maintenance.receipt.after-parent-sync",
        action: CrashAction::CreateReceipt,
        failpoint: RedbMaintenanceFailpoint::AfterReceiptParentSync,
    },
];

const ARTIFACT_CASES: &[CrashCase] = &[
    CrashCase {
        id: "maintenance.backup.after-publication",
        action: CrashAction::CreateBackup,
        failpoint: RedbMaintenanceFailpoint::AfterNamedBackupPublication,
    },
    CrashCase {
        id: "maintenance.restore.after-stage",
        action: CrashAction::StageRestore,
        failpoint: RedbMaintenanceFailpoint::AfterStagedMaterialization,
    },
    CrashCase {
        id: "maintenance.restore.before-target-publication",
        action: CrashAction::PublishRestore,
        failpoint: RedbMaintenanceFailpoint::BeforeTargetPublication,
    },
    CrashCase {
        id: "maintenance.restore.after-target-publication",
        action: CrashAction::PublishRestore,
        failpoint: RedbMaintenanceFailpoint::AfterTargetPublication,
    },
    CrashCase {
        id: "maintenance.restore.after-target-parent-sync",
        action: CrashAction::PublishRestore,
        failpoint: RedbMaintenanceFailpoint::AfterTargetParentSync,
    },
];

fn main() -> ExitCode {
    if std::env::var(CHILD_MODE).as_deref() == Ok(CHILD_DEDICATED) {
        return child_main();
    }
    if !std::env::args().any(|argument| argument == "--ignored") {
        println!("offline_maintenance_matrix: 10 ignored");
        return ExitCode::SUCCESS;
    }
    match run_matrix() {
        Ok(()) => {
            println!("offline_maintenance_matrix: 10 passed");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("offline_maintenance_matrix failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn child_main() -> ExitCode {
    match run_child_from_environment() {
        Ok(()) => {
            eprintln!("maintenance child passed its armed abort boundary");
            ExitCode::FAILURE
        }
        Err(error) => {
            eprintln!("maintenance child failed before its armed abort boundary: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run_matrix() -> TestResult<()> {
    verify_wp190_recovery_report_v1()
        .map_err(|_| test_failure("checked WP-190 recovery report is invalid"))?;
    for case in RECEIPT_CASES {
        run_receipt_case(*case)?;
    }
    run_backup_case(ARTIFACT_CASES[0])?;
    run_stage_case(ARTIFACT_CASES[1])?;
    for case in &ARTIFACT_CASES[2..] {
        run_publication_case(*case)?;
    }
    Ok(())
}

fn run_receipt_case(case: CrashCase) -> TestResult<()> {
    let fixture = MaintenanceFixture::new(case.id)?;
    spawn_expected_abort(case, &fixture)?;

    let (mut storage, first) =
        RedbMaintenanceStorage::open(&fixture.database, &fixture.backup_root)?;
    let receipt = storage.read_receipt(operation_id(0x50))?;
    let published = matches!(
        case.failpoint,
        RedbMaintenanceFailpoint::AfterReceiptRename
            | RedbMaintenanceFailpoint::AfterReceiptParentSync
    );
    if published != receipt.is_some() || first.receipts().receipts().len() != usize::from(published)
    {
        return Err(test_failure(format!(
            "{} recovered the wrong receipt publication state",
            case.id
        )));
    }
    drop(storage);
    assert_reconciliation_stable(&fixture, usize::from(published))?;
    Ok(())
}

fn run_backup_case(case: CrashCase) -> TestResult<()> {
    let fixture = MaintenanceFixture::new(case.id)?;
    initialize(&fixture.database, baseline_database_id())?;
    prepare_create_receipt(&fixture)?;
    spawn_expected_abort(case, &fixture)?;

    let (mut storage, first) =
        RedbMaintenanceStorage::open(&fixture.database, &fixture.backup_root)?;
    let evidence = operation_evidence(&first, operation_id(0x60))?;
    if !evidence.named_backup_matches()
        || evidence.staged_restore_matches()
        || evidence.configured_target_matches()
    {
        return Err(test_failure(
            "published backup did not reconcile to its reserved operation",
        ));
    }
    let (manifest, identity) =
        storage.create_named_backup(operation_id(0x60), &backup_name()?, &build_metadata()?)?;
    if manifest.database_id() != baseline_database_id()
        || identity.database_id() != baseline_database_id()
    {
        return Err(test_failure(
            "same-operation backup retry resolved the wrong database",
        ));
    }
    let second = storage.reconcile()?;
    let third = storage.reconcile()?;
    if second != third {
        return Err(test_failure(
            "published backup reconciliation was not idempotent",
        ));
    }
    Ok(())
}

fn run_stage_case(case: CrashCase) -> TestResult<()> {
    let fixture = MaintenanceFixture::new(case.id)?;
    prepare_restore_fixture(&fixture)?;
    spawn_expected_abort(case, &fixture)?;

    let (mut storage, first) =
        RedbMaintenanceStorage::open(&fixture.database, &fixture.backup_root)?;
    let evidence = operation_evidence(&first, operation_id(0x61))?;
    if !evidence.named_backup_matches()
        || !evidence.staged_restore_matches()
        || evidence.configured_target_matches()
    {
        return Err(test_failure(
            "staged restore recovery confused a private stage with publication",
        ));
    }
    if complete_structural_validation(storage.configured_database_file())? != diverged_database_id()
    {
        return Err(test_failure(
            "stage-only crash changed the configured database",
        ));
    }
    let second = storage.reconcile()?;
    let third = storage.reconcile()?;
    if second != third {
        return Err(test_failure(
            "staged restore reconciliation was not idempotent",
        ));
    }
    Ok(())
}

fn run_publication_case(case: CrashCase) -> TestResult<()> {
    let fixture = MaintenanceFixture::new(case.id)?;
    prepare_restore_fixture(&fixture)?;
    spawn_expected_abort(case, &fixture)?;

    let (mut storage, first) =
        RedbMaintenanceStorage::open(&fixture.database, &fixture.backup_root)?;
    let evidence = operation_evidence(&first, operation_id(0x61))?;
    let published = matches!(
        case.failpoint,
        RedbMaintenanceFailpoint::AfterTargetPublication
            | RedbMaintenanceFailpoint::AfterTargetParentSync
    );
    if !evidence.named_backup_matches()
        || !evidence.staged_restore_matches()
        || evidence.configured_target_matches() != published
    {
        return Err(test_failure(format!(
            "{} recovered the wrong restore publication state",
            case.id
        )));
    }
    let second = storage.reconcile()?;
    let third = storage.reconcile()?;
    if second != third {
        return Err(test_failure(format!(
            "{} reconciliation was not idempotent: second={second:?}, third={third:?}",
            case.id,
        )));
    }
    let expected_database_id = if published {
        baseline_database_id()
    } else {
        diverged_database_id()
    };
    if complete_structural_validation(storage.configured_database_file())? != expected_database_id {
        return Err(test_failure(format!(
            "{} retained the wrong configured database identity",
            case.id
        )));
    }
    Ok(())
}

fn prepare_create_receipt(fixture: &MaintenanceFixture) -> TestResult<()> {
    let (mut storage, startup) =
        RedbMaintenanceStorage::open(&fixture.database, &fixture.backup_root)?;
    if !startup.receipts().receipts().is_empty() {
        return Err(test_failure("fresh backup fixture contained a receipt"));
    }
    let mut create = receipt(0x60, OfflineMaintenanceOperationKind::CreateBackup)?;
    create.record_source_database_id(baseline_database_id())?;
    advance_offline(&mut create)?;
    if storage.create_or_read_receipt(&create)? != OfflineMaintenanceReceiptCreateResultV1::Created
    {
        return Err(test_failure("backup receipt was not newly reserved"));
    }
    Ok(())
}

fn prepare_restore_fixture(fixture: &MaintenanceFixture) -> TestResult<()> {
    initialize(&fixture.database, baseline_database_id())?;
    let (mut storage, startup) =
        RedbMaintenanceStorage::open(&fixture.database, &fixture.backup_root)?;
    if !startup.receipts().receipts().is_empty() {
        return Err(test_failure("fresh restore fixture contained a receipt"));
    }

    let mut create = receipt(0x60, OfflineMaintenanceOperationKind::CreateBackup)?;
    create.record_source_database_id(baseline_database_id())?;
    advance_offline(&mut create)?;
    storage.create_or_read_receipt(&create)?;
    let (_, identity) = storage.create_named_backup(
        create.operation_id(),
        create.backup_name(),
        &build_metadata()?,
    )?;
    create.record_manifest_identity(identity)?;
    advance_to_success(&mut create)?;
    storage.replace_receipt(&create)?;
    drop(storage);

    replace_with_database(&fixture.database, diverged_database_id())?;
    let (mut storage, _) = RedbMaintenanceStorage::open(&fixture.database, &fixture.backup_root)?;
    let mut restore = receipt(0x61, OfflineMaintenanceOperationKind::RestoreBackup)?;
    advance_offline(&mut restore)?;
    if storage.create_or_read_receipt(&restore)? != OfflineMaintenanceReceiptCreateResultV1::Created
    {
        return Err(test_failure("restore receipt was not newly reserved"));
    }
    Ok(())
}

fn spawn_expected_abort(case: CrashCase, fixture: &MaintenanceFixture) -> TestResult<()> {
    let specification = ChildProcessSpec::new(std::env::current_exe()?)?
        .env(CHILD_MODE, CHILD_DEDICATED)?
        .env(CHILD_ACTION, case.action.as_str())?
        .env(CHILD_FAILPOINT, failpoint_name(case.failpoint))?
        .env(CHILD_DATABASE, fixture.database.as_os_str())?
        .env(CHILD_BACKUP_ROOT, fixture.backup_root.as_os_str())?;
    let mut child = ChildProcessController::spawn(&specification)?;
    let exit = child.wait_for_exit(CHILD_TIMEOUT)?;
    if exit.status.success() {
        return Err(test_failure(format!(
            "{} child did not abort at its closed failpoint",
            case.id
        )));
    }
    Ok(())
}

fn run_child_from_environment() -> TestResult<()> {
    let action = std::env::var(CHILD_ACTION)
        .ok()
        .as_deref()
        .and_then(CrashAction::parse)
        .ok_or_else(|| test_failure("invalid maintenance child action"))?;
    let failpoint = std::env::var(CHILD_FAILPOINT)
        .ok()
        .as_deref()
        .and_then(parse_failpoint)
        .ok_or_else(|| test_failure("invalid maintenance child failpoint"))?;
    if !action_accepts_failpoint(action, failpoint) {
        return Err(test_failure(
            "maintenance child action and failpoint disagree",
        ));
    }
    let database = bounded_environment_path(CHILD_DATABASE)?;
    let backup_root = bounded_environment_path(CHILD_BACKUP_ROOT)?;
    let controller = riffdb_storage_redb::RedbMaintenanceTestController::abort_at(failpoint);
    let (mut storage, _) =
        RedbMaintenanceStorage::open_with_test_controller(database, backup_root, controller)?;

    match action {
        CrashAction::CreateReceipt => {
            let receipt = receipt(0x50, OfflineMaintenanceOperationKind::CreateBackup)?;
            storage.create_or_read_receipt(&receipt)?;
        }
        CrashAction::CreateBackup => {
            storage.create_named_backup(operation_id(0x60), &backup_name()?, &build_metadata()?)?;
        }
        CrashAction::StageRestore => {
            storage.stage_restore(operation_id(0x61), &backup_name()?)?;
        }
        CrashAction::PublishRestore => {
            let staged = storage.stage_restore(operation_id(0x61), &backup_name()?)?;
            let manifest_identity = staged.manifest_identity().clone();
            let staged_database_id = complete_structural_validation(staged.staged_database_file())?;
            let sealed = staged.seal_after_validation(staged_database_id)?;
            let mut restore = storage
                .read_receipt(operation_id(0x61))?
                .ok_or_else(|| test_failure("restore child receipt disappeared"))?;
            restore.record_staged_database_id(staged_database_id)?;
            restore.record_manifest_identity(manifest_identity)?;
            storage.replace_receipt(&restore)?;
            storage.publish_sealed_restore(
                sealed,
                OfflineRestoreOverwritePolicyV1::ExplicitlyAllowDestructive,
            )?;
        }
    }
    Err(test_failure(
        "maintenance child passed its armed failpoint without aborting",
    ))
}

fn assert_reconciliation_stable(
    fixture: &MaintenanceFixture,
    expected_receipts: usize,
) -> TestResult<()> {
    let (storage, second) = RedbMaintenanceStorage::open(&fixture.database, &fixture.backup_root)?;
    drop(storage);
    let (storage, third) = RedbMaintenanceStorage::open(&fixture.database, &fixture.backup_root)?;
    drop(storage);
    if second != third || second.receipts().receipts().len() != expected_receipts {
        return Err(test_failure(
            "receipt reconciliation was not stable after cleanup",
        ));
    }
    Ok(())
}

fn operation_evidence(
    reconciliation: &RedbMaintenanceReconciliation,
    operation_id: OfflineMaintenanceOperationId,
) -> TestResult<riffdb_storage_redb::RedbMaintenanceOperationEvidence> {
    reconciliation
        .operations()
        .iter()
        .copied()
        .find(|evidence| evidence.operation_id() == operation_id)
        .ok_or_else(|| test_failure("reconciliation omitted operation evidence"))
}

fn initialize(path: &Path, database_id: DatabaseId) -> TestResult<()> {
    let mut store = RedbStore::open(path)?;
    if store.probe_database_identity()? != DatabaseIdentityProbe::NeedsInitialization
        || store.initialize_database(database_id)?
            != DatabaseInitializationResult::Installed(database_id)
    {
        return Err(test_failure("database initialization was not exact"));
    }
    Ok(())
}

fn replace_with_database(path: &Path, database_id: DatabaseId) -> TestResult<()> {
    let replacement = path.with_extension("replacement.redb");
    initialize(&replacement, database_id)?;
    fs::rename(&replacement, path)?;
    fs::File::open(
        path.parent()
            .ok_or_else(|| test_failure("database path had no parent"))?,
    )?
    .sync_all()?;
    Ok(())
}

fn complete_structural_validation(path: &Path) -> TestResult<DatabaseId> {
    let store = RedbStore::open(path)?;
    let digest_key =
        DigestKeyId::new(1).ok_or_else(|| test_failure("static digest key is invalid"))?;
    let inputs = StartupValidationInputs::new(
        Timestamp::new(1_700_000_000, 0)?,
        ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])?,
        ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])?,
    );
    let mut session = store.begin_structural_evidence(inputs)?;
    let database_id = session.database_id();
    let session_id = session.open_session_id();
    let limit =
        EvidencePageLimit::new(64).ok_or_else(|| test_failure("static page limit is invalid"))?;
    let mut structural = StructuralEvidenceCursor::start(database_id, session_id);
    let structural_end = loop {
        match session.read_structural_evidence(structural, limit)? {
            StructuralEvidencePage::Page { next, .. } => structural = next,
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };
    let mut historical = HistoricalEvidenceCursor::start(database_id, session_id);
    let historical_end = loop {
        match session.read_historical_evidence(historical, limit)? {
            HistoricalEvidencePage::Page { next, .. } => historical = next,
            HistoricalEvidencePage::ExactEnd(end) => break end,
        }
    };
    match session.finish(structural_end, historical_end)? {
        StructuralOpenOutcome::Clean(_) => Ok(database_id),
        StructuralOpenOutcome::MigrationRequired(_) => Err(test_failure(
            "WP-190 fixture unexpectedly required migration",
        )),
    }
}

fn receipt(
    seed: u8,
    kind: OfflineMaintenanceOperationKind,
) -> TestResult<OfflineMaintenanceReceiptV1> {
    let name = backup_name()?;
    let confirmation = match kind {
        OfflineMaintenanceOperationKind::CreateBackup => {
            OfflineMaintenanceReplacementConfirmation::NotProvided
        }
        OfflineMaintenanceOperationKind::RestoreBackup => {
            OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget
        }
    };
    Ok(OfflineMaintenanceReceiptV1::accepted(
        operation_id(seed),
        kind,
        name.clone(),
        offline_maintenance_input_hash(kind, &name, confirmation),
        confirmation,
        admission()?,
    )?)
}

fn admission() -> TestResult<OfflineMaintenanceAdmissionV1> {
    Ok(OfflineMaintenanceAdmissionV1::new(
        ActorId::new("wp190-operator")?,
        ActorKind::Human,
        CapabilityId::from_unix_milliseconds_and_random(3, [0x33; 10])?,
        Some(ApprovalId::new("wp190-approval")?),
    ))
}

fn advance_offline(receipt: &mut OfflineMaintenanceReceiptV1) -> TestResult<()> {
    for phase in [
        OfflineMaintenanceReceiptPhaseV1::Draining,
        OfflineMaintenanceReceiptPhaseV1::Offline,
    ] {
        receipt.advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))?;
    }
    Ok(())
}

fn advance_to_success(receipt: &mut OfflineMaintenanceReceiptV1) -> TestResult<()> {
    for phase in [
        OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
        OfflineMaintenanceReceiptPhaseV1::Validating,
        OfflineMaintenanceReceiptPhaseV1::Succeeded,
    ] {
        receipt.advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))?;
    }
    Ok(())
}

fn build_metadata() -> TestResult<BackupBuildMetadataV1> {
    Ok(BackupBuildMetadataV1::new(
        "0.1.0",
        "0123456789abcdef",
        "rustc-1.97.0",
        1,
        vec!["wp190-maintenance".to_owned()],
    )?)
}

fn operation_id(seed: u8) -> OfflineMaintenanceOperationId {
    OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1, [seed; 10])
        .expect("static operation identifier is valid")
}

fn baseline_database_id() -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(2, [0x22; 10])
        .expect("static baseline database identifier is valid")
}

fn diverged_database_id() -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(2, [0x44; 10])
        .expect("static diverged database identifier is valid")
}

fn backup_name() -> TestResult<BackupNameV1> {
    Ok(BackupNameV1::new(BACKUP_NAME)?)
}

fn bounded_environment_path(key: &str) -> TestResult<PathBuf> {
    let value =
        std::env::var_os(key).ok_or_else(|| test_failure("maintenance child path was absent"))?;
    if value.is_empty() || value.as_encoded_bytes().len() > 4_096 {
        return Err(test_failure("maintenance child path was invalid"));
    }
    Ok(PathBuf::from(value))
}

const fn failpoint_name(failpoint: RedbMaintenanceFailpoint) -> &'static str {
    match failpoint {
        RedbMaintenanceFailpoint::BeforeReceiptFileSync => "before-receipt-file-sync",
        RedbMaintenanceFailpoint::AfterReceiptFileSync => "after-receipt-file-sync",
        RedbMaintenanceFailpoint::BeforeReceiptRename => "before-receipt-rename",
        RedbMaintenanceFailpoint::AfterReceiptRename => "after-receipt-rename",
        RedbMaintenanceFailpoint::AfterReceiptParentSync => "after-receipt-parent-sync",
        RedbMaintenanceFailpoint::AfterNamedBackupPublication => "after-named-backup-publication",
        RedbMaintenanceFailpoint::AfterStagedMaterialization => "after-staged-materialization",
        RedbMaintenanceFailpoint::BeforeTargetPublication => "before-target-publication",
        RedbMaintenanceFailpoint::AfterTargetPublication => "after-target-publication",
        RedbMaintenanceFailpoint::AfterTargetParentSync => "after-target-parent-sync",
    }
}

fn parse_failpoint(value: &str) -> Option<RedbMaintenanceFailpoint> {
    Some(match value {
        "before-receipt-file-sync" => RedbMaintenanceFailpoint::BeforeReceiptFileSync,
        "after-receipt-file-sync" => RedbMaintenanceFailpoint::AfterReceiptFileSync,
        "before-receipt-rename" => RedbMaintenanceFailpoint::BeforeReceiptRename,
        "after-receipt-rename" => RedbMaintenanceFailpoint::AfterReceiptRename,
        "after-receipt-parent-sync" => RedbMaintenanceFailpoint::AfterReceiptParentSync,
        "after-named-backup-publication" => RedbMaintenanceFailpoint::AfterNamedBackupPublication,
        "after-staged-materialization" => RedbMaintenanceFailpoint::AfterStagedMaterialization,
        "before-target-publication" => RedbMaintenanceFailpoint::BeforeTargetPublication,
        "after-target-publication" => RedbMaintenanceFailpoint::AfterTargetPublication,
        "after-target-parent-sync" => RedbMaintenanceFailpoint::AfterTargetParentSync,
        _ => return None,
    })
}

const fn action_accepts_failpoint(
    action: CrashAction,
    failpoint: RedbMaintenanceFailpoint,
) -> bool {
    match action {
        CrashAction::CreateReceipt => matches!(
            failpoint,
            RedbMaintenanceFailpoint::BeforeReceiptFileSync
                | RedbMaintenanceFailpoint::AfterReceiptFileSync
                | RedbMaintenanceFailpoint::BeforeReceiptRename
                | RedbMaintenanceFailpoint::AfterReceiptRename
                | RedbMaintenanceFailpoint::AfterReceiptParentSync
        ),
        CrashAction::CreateBackup => {
            matches!(
                failpoint,
                RedbMaintenanceFailpoint::AfterNamedBackupPublication
            )
        }
        CrashAction::StageRestore => {
            matches!(
                failpoint,
                RedbMaintenanceFailpoint::AfterStagedMaterialization
            )
        }
        CrashAction::PublishRestore => matches!(
            failpoint,
            RedbMaintenanceFailpoint::BeforeTargetPublication
                | RedbMaintenanceFailpoint::AfterTargetPublication
                | RedbMaintenanceFailpoint::AfterTargetParentSync
        ),
    }
}

struct MaintenanceFixture {
    _directory: TemporaryDirectory,
    database: PathBuf,
    backup_root: PathBuf,
}

impl MaintenanceFixture {
    fn new(label: &str) -> TestResult<Self> {
        let directory = TemporaryDirectory::new(label)?;
        Ok(Self {
            database: directory.path().join("database.redb"),
            backup_root: directory.path().join("backups"),
            _directory: directory,
        })
    }
}

struct TemporaryDirectory {
    path: PathBuf,
}

impl TemporaryDirectory {
    fn new(label: &str) -> TestResult<Self> {
        static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);
        for _ in 0..1_024 {
            let ordinal = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "riffdb-wp190-maintenance-{label}-{}-{ordinal}",
                std::process::id()
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(Box::new(error)),
            }
        }
        Err(test_failure(
            "temporary maintenance directory bound exhausted",
        ))
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn test_failure(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(TestFailure(message.into()))
}

#[derive(Debug)]
struct TestFailure(String);

impl std::fmt::Display for TestFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for TestFailure {}
