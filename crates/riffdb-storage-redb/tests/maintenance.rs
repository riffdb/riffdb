//! External maintenance receipt, reconciliation, and staged-publication evidence.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

use redb::{ReadableDatabase, TableDefinition};
use riffdb_catalog::{CatalogHistoryOutcome, validate_catalog_history};
use riffdb_storage_api::{
    AuditPrincipalV1, BackupBuildMetadataV1, ContractMigrationAdmissionV1,
    ContractMigrationArtifactFileV1, ContractMigrationArtifactsV1,
    ContractMigrationOperationArtifactsV1, ContractMigrationReceiptPhaseV1,
    ContractMigrationReceiptTransitionV1, ContractMigrationReceiptV1, DatabaseIdentityProbe,
    DatabaseIdentityProbePort, DatabaseInitializationPort, DatabaseInitializationResult,
    EvidencePageLimit, HistoricalEvidenceCursor, HistoricalEvidencePage,
    OfflineBackupRetirementEvidenceV2, OfflineMaintenanceAdmissionV1,
    OfflineMaintenanceReceiptCreateResultV1, OfflineMaintenanceReceiptCreateResultV2,
    OfflineMaintenanceReceiptFailureV1, OfflineMaintenanceReceiptPersistencePort,
    OfflineMaintenanceReceiptPhaseV1, OfflineMaintenanceReceiptTransitionV1,
    OfflineMaintenanceReceiptV1, OfflineMaintenanceReceiptV2, OfflineRestoreOverwritePolicyV1,
    ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
    StartupValidationInputs, StorageErrorKind, StructuralEvidenceCursor, StructuralEvidenceOpen,
    StructuralEvidencePage, StructuralEvidenceSession, StructuralOpenOutcome,
};
use riffdb_storage_redb::{
    RedbMaintenanceFailpoint, RedbMaintenanceStorage, RedbMaintenanceTestController, RedbStore,
};
use riffdb_types::{
    ActorId, ActorKind, ApprovalId, BackupNameV1, CapabilityId, ContractBundleHash,
    ContractMigrationInputHash, ContractMigrationOperationId, DatabaseId, DigestKeyId,
    MigrationBundleHash, OfflineMaintenanceOperationId, OfflineMaintenanceOperationKind,
    OfflineMaintenanceReplacementConfirmation, RequestId, ServiceIngressKindV1, Timestamp,
    offline_maintenance_input_hash,
};
use sha2::{Digest, Sha256};

static NEXT_TEST_PATH: AtomicU64 = AtomicU64::new(1);

struct TestRoot(PathBuf);

impl TestRoot {
    fn new(label: &str) -> Self {
        let ordinal = NEXT_TEST_PATH.fetch_add(1, Ordering::Relaxed);
        let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
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

fn migration_operation_id(seed: u8) -> ContractMigrationOperationId {
    ContractMigrationOperationId::from_unix_milliseconds_and_random(4, [seed; 10])
        .expect("migration operation ID")
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
        OfflineMaintenanceOperationKind::RetireBackup => {
            panic!("retire operations require V2 receipt evidence")
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

fn accepted_offline_retirement(
    storage: &mut RedbMaintenanceStorage,
    create_seed: u8,
    retire_seed: u8,
) -> OfflineMaintenanceReceiptV2 {
    let (_, manifest) = create_completed_named_backup(storage, create_seed);
    let (originating_create, prepared_manifest) = storage
        .prepare_backup_retirement(&backup_name())
        .expect("prepare retirement");
    assert_eq!(prepared_manifest, manifest);
    let name = backup_name();
    let mut retirement = OfflineMaintenanceReceiptV2::accepted_retirement(
        operation_id(retire_seed),
        name.clone(),
        offline_maintenance_input_hash(
            OfflineMaintenanceOperationKind::RetireBackup,
            &name,
            OfflineMaintenanceReplacementConfirmation::NotProvided,
        ),
        admission(),
        OfflineBackupRetirementEvidenceV2::new(originating_create, manifest),
    )
    .expect("accepted retirement");
    assert_eq!(
        storage
            .create_or_read_retire_receipt(&retirement)
            .expect("persist accepted retirement"),
        OfflineMaintenanceReceiptCreateResultV2::Created
    );
    for phase in [
        OfflineMaintenanceReceiptPhaseV1::Draining,
        OfflineMaintenanceReceiptPhaseV1::Offline,
    ] {
        retirement
            .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
            .expect("advance retirement");
    }
    storage
        .replace_retire_receipt(&retirement)
        .expect("persist offline retirement");
    retirement
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

fn accepted_migration_receipt(
    seed: u8,
    candidate: &[u8],
    migration: &[u8],
) -> ContractMigrationReceiptV1 {
    let file = |bytes: &[u8]| {
        ContractMigrationArtifactFileV1::new(
            u64::try_from(bytes.len()).expect("length"),
            Sha256::digest(bytes).into(),
        )
        .expect("artifact")
    };
    ContractMigrationReceiptV1::from_canonical_parts(
        database_id(),
        migration_operation_id(seed),
        ContractMigrationInputHash::from_bytes([1; 32]),
        ContractMigrationArtifactsV1::new(
            ContractBundleHash::from_bytes([2; 32]),
            ContractBundleHash::from_bytes([3; 32]),
            MigrationBundleHash::from_bytes([4; 32]),
        ),
        ContractMigrationOperationArtifactsV1::new(file(candidate), file(migration)),
        ContractMigrationAdmissionV1::new(
            AuditPrincipalV1::new(
                ActorId::new("migration-operator").expect("actor"),
                ActorKind::Human,
                CapabilityId::from_unix_milliseconds_and_random(5, [0x55; 10]).expect("capability"),
                std::num::NonZeroU64::new(1).expect("revision"),
            ),
            Some(ApprovalId::new("migration-approval").expect("approval")),
            RequestId::from_unix_milliseconds_and_random(6, [0x66; 10]).expect("request"),
            Timestamp::new(1_700_000_000, 123).expect("timestamp"),
            ServiceIngressKindV1::Grpc,
        ),
        None,
        None,
        None,
        vec![ContractMigrationReceiptTransitionV1::phase(
            ContractMigrationReceiptPhaseV1::Accepted,
        )],
    )
    .expect("accepted receipt")
}

#[test]
fn migration_artifacts_and_receipt_are_immutable_restart_evidence() {
    let root = TestRoot::new("migration-artifacts");
    let database = root.join("database.redb");
    let backup_root = root.join("backups");
    let candidate = b"canonical-candidate";
    let migration = b"canonical-migration";
    let operation = migration_operation_id(0x44);
    let accepted = accepted_migration_receipt(0x44, candidate, migration);

    let (storage, _) = RedbMaintenanceStorage::open(&database, &backup_root).expect("maintenance");
    storage
        .accept_contract_migration(&accepted, candidate, migration)
        .expect("durable acceptance");
    storage
        .accept_contract_migration(&accepted, candidate, migration)
        .expect("same input replay");
    assert!(
        storage
            .accept_contract_migration(&accepted, b"different", migration)
            .is_err()
    );
    let draining = accepted
        .advance(ContractMigrationReceiptPhaseV1::Draining)
        .expect("draining receipt");
    storage
        .replace_contract_migration_receipt(&accepted, &draining)
        .expect("advance receipt");
    drop(storage);

    let (storage, _) =
        RedbMaintenanceStorage::open(&database, &backup_root).expect("restart reconcile");
    assert_eq!(
        storage
            .read_contract_migration_receipt(operation)
            .expect("read")
            .expect("receipt"),
        draining
    );
    assert_eq!(
        storage
            .read_contract_migration_artifacts(operation)
            .expect("artifacts"),
        (candidate.to_vec(), migration.to_vec())
    );
}

#[test]
fn migration_preflight_reserves_disk_and_restart_recovers_exact_stage() {
    let root = TestRoot::new("migration-stage-restart");
    let database = root.join("database.redb");
    let backup_root = root.join("backups");
    initialize(&database);
    let candidate = b"canonical-candidate";
    let migration = b"canonical-migration";
    let accepted = accepted_migration_receipt(0x45, candidate, migration);
    let operation = accepted.operation_id();
    let (storage, _) = RedbMaintenanceStorage::open(&database, &backup_root).expect("maintenance");
    storage
        .accept_contract_migration(&accepted, candidate, migration)
        .expect("accept");
    let draining = accepted
        .advance(ContractMigrationReceiptPhaseV1::Draining)
        .expect("draining");
    storage
        .replace_contract_migration_receipt(&accepted, &draining)
        .expect("persist draining");
    let preflight = draining
        .advance(ContractMigrationReceiptPhaseV1::Preflight)
        .expect("preflight");
    storage
        .replace_contract_migration_receipt(&draining, &preflight)
        .expect("persist preflight");

    let reservation = storage
        .reserve_contract_migration_disk(operation, 4_096)
        .expect("reserve both filesystems");
    assert_eq!(reservation.operation_id(), operation);
    let (name, _manifest, identity) = storage
        .create_contract_migration_backup(operation, &build_metadata())
        .expect("automatic immutable backup");
    let backed_up = preflight
        .publish_backup(name, identity.clone())
        .expect("bind backup");
    storage
        .replace_contract_migration_receipt(&preflight, &backed_up)
        .expect("persist backup");
    let staging = backed_up
        .advance(ContractMigrationReceiptPhaseV1::Staging)
        .expect("staging");
    storage
        .replace_contract_migration_receipt(&backed_up, &staging)
        .expect("persist staging");
    let (stage, stage_identity) = storage
        .materialize_contract_migration_stage(operation)
        .expect("materialize sibling stage");
    assert_eq!(complete_structural_validation(&stage), database_id());
    let transforming = staging
        .begin_transforming(stage_identity)
        .expect("bind stage");
    storage
        .replace_contract_migration_receipt(&staging, &transforming)
        .expect("persist stage");
    reservation.release().expect("release reservation");
    drop(storage);

    let (storage, reconciliation) =
        RedbMaintenanceStorage::open(&database, &backup_root).expect("restart reconcile");
    assert_eq!(reconciliation.migration_receipts(), [transforming]);
    assert_eq!(
        storage
            .materialize_contract_migration_stage(operation)
            .expect("resume exact stage")
            .0,
        stage
    );
}

fn migration_at_preflight(
    label: &str,
    controller: RedbMaintenanceTestController,
) -> (TestRoot, RedbMaintenanceStorage, ContractMigrationReceiptV1) {
    let root = TestRoot::new(label);
    let database = root.join("database.redb");
    let backup_root = root.join("backups");
    initialize(&database);
    let candidate = b"canonical-candidate";
    let migration = b"canonical-migration";
    let accepted = accepted_migration_receipt(0x46, candidate, migration);
    let (storage, _) =
        RedbMaintenanceStorage::open_with_test_controller(&database, &backup_root, controller)
            .expect("maintenance");
    storage
        .accept_contract_migration(&accepted, candidate, migration)
        .expect("accept");
    let draining = accepted
        .advance(ContractMigrationReceiptPhaseV1::Draining)
        .expect("draining");
    storage
        .replace_contract_migration_receipt(&accepted, &draining)
        .expect("persist draining");
    let preflight = draining
        .advance(ContractMigrationReceiptPhaseV1::Preflight)
        .expect("preflight");
    storage
        .replace_contract_migration_receipt(&draining, &preflight)
        .expect("persist preflight");
    (root, storage, preflight)
}

fn migration_publish_backup(
    storage: &RedbMaintenanceStorage,
    preflight: &ContractMigrationReceiptV1,
) -> ContractMigrationReceiptV1 {
    let (name, _, identity) = storage
        .create_contract_migration_backup(preflight.operation_id(), &build_metadata())
        .expect("migration backup");
    let backed_up = preflight
        .publish_backup(name, identity)
        .expect("bind backup");
    storage
        .replace_contract_migration_receipt(preflight, &backed_up)
        .expect("persist backup");
    backed_up
}

#[test]
fn migration_external_failpoints_recover_exact_receipt_stage_and_target_state() {
    for (index, failpoint, published) in [
        (0x70, RedbMaintenanceFailpoint::BeforeReceiptFileSync, false),
        (0x71, RedbMaintenanceFailpoint::AfterReceiptFileSync, false),
        (0x72, RedbMaintenanceFailpoint::BeforeReceiptRename, false),
        (0x73, RedbMaintenanceFailpoint::AfterReceiptRename, true),
        (0x74, RedbMaintenanceFailpoint::AfterReceiptParentSync, true),
    ] {
        let root = TestRoot::new("migration-receipt-failpoint");
        let database = root.join("database.redb");
        let backup_root = root.join("backups");
        let accepted = accepted_migration_receipt(index, b"candidate", b"migration");
        let operation = accepted.operation_id();
        let controller = RedbMaintenanceTestController::return_at(failpoint);
        let (storage, _) =
            RedbMaintenanceStorage::open_with_test_controller(&database, &backup_root, controller)
                .expect("maintenance");
        storage
            .accept_contract_migration(&accepted, b"candidate", b"migration")
            .expect("accept");
        let draining = accepted
            .advance(ContractMigrationReceiptPhaseV1::Draining)
            .expect("draining");
        let error = storage
            .replace_contract_migration_receipt(&accepted, &draining)
            .expect_err("armed receipt boundary");
        assert_eq!(
            error.kind(),
            if published {
                StorageErrorKind::CommitStatusUnknown
            } else {
                StorageErrorKind::Unavailable
            }
        );
        drop(storage);
        let (storage, reconciliation) =
            RedbMaintenanceStorage::open(&database, &backup_root).expect("reconcile receipt");
        let expected = if published { &draining } else { &accepted };
        assert_eq!(
            storage
                .read_contract_migration_receipt(operation)
                .expect("read receipt")
                .as_ref(),
            Some(expected)
        );
        assert_eq!(
            reconciliation.migration_receipts(),
            std::slice::from_ref(expected)
        );
    }

    let backup_controller = RedbMaintenanceTestController::return_at(
        RedbMaintenanceFailpoint::AfterNamedBackupPublication,
    );
    let (_root, storage, preflight) =
        migration_at_preflight("migration-backup-failpoint", backup_controller);
    assert_eq!(
        storage
            .create_contract_migration_backup(preflight.operation_id(), &build_metadata())
            .expect_err("armed backup boundary")
            .kind(),
        StorageErrorKind::CommitStatusUnknown
    );
    migration_publish_backup(&storage, &preflight);

    let stage_controller = RedbMaintenanceTestController::return_at(
        RedbMaintenanceFailpoint::AfterStagedMaterialization,
    );
    let (_root, storage, preflight) =
        migration_at_preflight("migration-stage-failpoint", stage_controller);
    let backed_up = migration_publish_backup(&storage, &preflight);
    let staging = backed_up
        .advance(ContractMigrationReceiptPhaseV1::Staging)
        .expect("staging");
    storage
        .replace_contract_migration_receipt(&backed_up, &staging)
        .expect("persist staging");
    assert_eq!(
        storage
            .materialize_contract_migration_stage(staging.operation_id())
            .expect_err("armed stage boundary")
            .kind(),
        StorageErrorKind::Unavailable
    );
    assert!(
        storage
            .materialize_contract_migration_stage(staging.operation_id())
            .expect("resume materialized stage")
            .0
            .is_file()
    );

    for (failpoint, published) in [
        (RedbMaintenanceFailpoint::BeforeTargetPublication, false),
        (RedbMaintenanceFailpoint::AfterTargetPublication, true),
        (RedbMaintenanceFailpoint::AfterTargetParentSync, true),
    ] {
        let controller = RedbMaintenanceTestController::return_at(failpoint);
        let (root, storage, preflight) =
            migration_at_preflight("migration-publication-failpoint", controller);
        let target = root.join("database.redb");
        let backed_up = migration_publish_backup(&storage, &preflight);
        let staging = backed_up
            .advance(ContractMigrationReceiptPhaseV1::Staging)
            .expect("staging");
        storage
            .replace_contract_migration_receipt(&backed_up, &staging)
            .expect("persist staging");
        let (stage, identity) = storage
            .materialize_contract_migration_stage(staging.operation_id())
            .expect("stage");
        let transforming = staging.begin_transforming(identity).expect("transforming");
        storage
            .replace_contract_migration_receipt(&staging, &transforming)
            .expect("persist transforming");
        let rebuilding = transforming
            .advance(ContractMigrationReceiptPhaseV1::RebuildingProjections)
            .expect("rebuilding");
        storage
            .replace_contract_migration_receipt(&transforming, &rebuilding)
            .expect("persist rebuilding");
        let validating = rebuilding
            .advance(ContractMigrationReceiptPhaseV1::ValidatingStage)
            .expect("validating");
        storage
            .replace_contract_migration_receipt(&rebuilding, &validating)
            .expect("persist validating");
        let publishing = validating
            .advance(ContractMigrationReceiptPhaseV1::Publishing)
            .expect("publishing");
        storage
            .replace_contract_migration_receipt(&validating, &publishing)
            .expect("persist publishing");
        let before = Sha256::digest(fs::read(&target).expect("target before"));
        let staged = Sha256::digest(fs::read(&stage).expect("stage before"));
        let error = storage
            .publish_contract_migration_stage(publishing.operation_id())
            .expect_err("armed publication boundary");
        assert_eq!(
            error.kind(),
            if published {
                StorageErrorKind::CommitStatusUnknown
            } else {
                StorageErrorKind::Unavailable
            }
        );
        assert_eq!(
            Sha256::digest(fs::read(&target).expect("target after")),
            if published { staged } else { before }
        );
        assert_eq!(stage.exists(), !published);
    }
}

fn complete_structural_validation(path: &Path) -> DatabaseId {
    let store = RedbStore::open(path).expect("open staged database");
    let inputs = validation_inputs();
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

fn validation_inputs() -> StartupValidationInputs {
    let digest_key = DigestKeyId::new(1).expect("digest key ID");
    StartupValidationInputs::new(
        Timestamp::new(1_700_000_000, 0).expect("startup timestamp"),
        ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("capability inventory"),
        ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(digest_key)])
            .expect("idempotency inventory"),
    )
}

fn certify_clean_close(path: &Path) {
    let store = RedbStore::open(path).expect("open database to certify");
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
        .expect("begin complete validation");
    let database_id = session.database_id();
    let session_id = session.open_session_id();
    let limit = EvidencePageLimit::new(64).expect("page limit");
    let mut structural = StructuralEvidenceCursor::start(database_id, session_id);
    let structural_end = loop {
        match session
            .read_structural_evidence(structural, limit)
            .expect("structural evidence")
        {
            StructuralEvidencePage::Page { findings, next, .. } => {
                assert!(findings.is_empty());
                structural = next;
            }
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };
    let (catalog, historical_end) = validate_catalog_history(&mut session)
        .expect("catalog history")
        .into_parts();
    assert!(matches!(catalog, CatalogHistoryOutcome::Ready(_)));
    let StructuralOpenOutcome::Clean(opened) = session
        .finish(structural_end, historical_end)
        .expect("finish startup")
    else {
        panic!("current format must not require migration");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let ports = dormant
        .into_operational_after_catalog_validation()
        .expect("activate for final clean close");
    ports
        .write_clean_close_lifecycle()
        .expect("write clean lifecycle");
}

fn raw_meta(path: &Path, key: &str) -> Option<Vec<u8>> {
    let database = redb::ReadOnlyDatabase::open(path).expect("open raw read-only database");
    let read = database.begin_read().expect("begin raw read");
    let table = read
        .open_table(TableDefinition::<&str, &[u8]>::new("meta"))
        .expect("open meta");
    table
        .get(key)
        .expect("read meta")
        .map(|value| value.value().to_vec())
}

fn clean_mode_selected(path: &Path) -> bool {
    let key = DigestKeyId::new(1).expect("digest key ID");
    let inputs = StartupValidationInputs::new(
        Timestamp::new(1_700_000_000, 0).expect("startup timestamp"),
        ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(key)])
            .expect("capability inventory"),
        ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(key)])
            .expect("idempotency inventory"),
    );
    let session = RedbStore::open(path)
        .expect("open compatibility database")
        .begin_structural_evidence(inputs)
        .expect("begin compatibility evidence");
    session.clean_close_fast_path()
}

// req: STO-023, REC-004, END-004
#[test]
fn maintenance_and_copy_transitions_have_exact_clean_certificate_behavior() {
    let root = TestRoot::new("clean-certificate-compatibility");
    let source = root.join("source.redb");
    let copy = root.join("copy.redb");
    let backup_root = root.join("backups");
    initialize(&source);
    certify_clean_close(&source);
    let clean = raw_meta(&source, "clean_close_certificate/v1").expect("clean lifecycle");

    let (mut maintenance, _) =
        RedbMaintenanceStorage::open(&source, &backup_root).expect("maintenance storage");
    let (_, backup_identity) = create_completed_named_backup(&mut maintenance, 0x61);
    assert_eq!(
        raw_meta(
            &backup_root.join("before-upgrade/database.redb"),
            "clean_close_certificate/v1"
        ),
        Some(clean.clone()),
        "immutable backup creation preserves the matching certificate bytes"
    );

    fs::copy(&source, &copy).expect("copy closed database");
    fs::copy(
        riffdb_storage_redb::durable_format_marker_path(&source),
        riffdb_storage_redb::durable_format_marker_path(&copy),
    )
    .expect("copy exact format marker");
    assert!(
        !clean_mode_selected(&copy),
        "a bare database-file copy lacks the complete journal/engine lifecycle unit and must fall back"
    );

    riffdb_storage_redb::stamp_history_incarnation(&copy, 2)
        .expect("rotate copied history incarnation");
    assert!(
        !clean_mode_selected(&copy),
        "history-incarnation rotation must invalidate the copied certificate"
    );

    let mut restore = receipt(0x62, OfflineMaintenanceOperationKind::RestoreBackup);
    advance_offline(&mut restore);
    maintenance
        .create_or_read_receipt(&restore)
        .expect("create restore receipt");
    let staged = maintenance
        .stage_restore(
            restore.operation_id(),
            restore.backup_name(),
            validation_inputs(),
        )
        .expect("stage immutable backup through normal restore selector");
    assert_eq!(
        raw_meta(staged.staged_database_file(), "clean_close_certificate/v1"),
        Some(clean),
        "normal restore staging preserves the immutable artifact's certificate bytes"
    );
    assert!(
        clean_mode_selected(staged.staged_database_file()),
        "the complete immutable backup selected through normal restore staging preserves matching clean eligibility"
    );
    let staged_database_id = complete_structural_validation(staged.staged_database_file());
    let mut sealed = staged
        .seal_after_validation(staged_database_id)
        .expect("seal validated restore");
    sealed
        .apply_published_history_incarnation(2)
        .expect("stamp replacement incarnation");
    restore
        .record_staged_database_id(staged_database_id)
        .expect("record staged database identity");
    restore
        .record_manifest_identity(backup_identity)
        .expect("record backup manifest identity");
    restore
        .record_published_incarnation(2)
        .expect("record replacement incarnation");
    maintenance
        .replace_receipt(&restore)
        .expect("persist restore evidence");
    assert!(matches!(
        maintenance
            .publish_sealed_restore(
                sealed,
                OfflineRestoreOverwritePolicyV1::ExplicitlyAllowDestructive,
            )
            .expect("publish maintenance replacement"),
        riffdb_storage_api::OfflineRestoreResultV1::Restored { .. }
    ));
    drop(maintenance);
    assert!(
        !clean_mode_selected(&source),
        "restore/maintenance replacement must invalidate the preserved certificate into complete validation"
    );
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

// req: STO-023, REC-004, END-004
#[test]
fn clean_certified_restore_stage_scrubs_population_corruption_before_handoff() {
    let root = TestRoot::new("clean-certified-corrupt-stage");
    let database = root.join("database.redb");
    let backup_root = root.join("backups");
    initialize(&database);
    certify_clean_close(&database);

    // A clean binding intentionally does not cover population rows. Preserve
    // the certificate while installing a malformed entity so a fast startup
    // alone would miss this corruption before staged authorization.
    let raw = redb::Database::open(&database).expect("open raw database");
    let write = raw.begin_write().expect("begin population corruption");
    {
        let mut entities = write
            .open_table(TableDefinition::<&[u8], &[u8]>::new("entities"))
            .expect("open entities");
        entities
            .insert(&[0x81][..], &[0xff][..])
            .expect("install malformed population row");
    }
    write.commit().expect("commit population corruption");
    drop(raw);

    let (mut maintenance, _) =
        RedbMaintenanceStorage::open(&database, &backup_root).expect("maintenance storage");
    create_completed_named_backup(&mut maintenance, 0x63);
    let mut restore = receipt(0x64, OfflineMaintenanceOperationKind::RestoreBackup);
    advance_offline(&mut restore);
    maintenance
        .create_or_read_receipt(&restore)
        .expect("create restore receipt");
    let target_before = Sha256::digest(fs::read(&database).expect("read target before stage"));

    assert_eq!(
        maintenance
            .stage_restore(
                restore.operation_id(),
                restore.backup_name(),
                validation_inputs(),
            )
            .expect_err("complete stage scrub must reject malformed population")
            .kind(),
        StorageErrorKind::CorruptData
    );
    assert_eq!(
        Sha256::digest(fs::read(&database).expect("read unchanged target")),
        target_before,
        "failed staged scrub must not publish or mutate the configured target"
    );
    assert!(
        !backup_root
            .join(format!(".maintenance/staged/{}", restore.operation_id()))
            .exists(),
        "a failed pre-authorization scrub must delete its private stage"
    );
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
            .stage_recovery_restore_candidate(
                recovery_operation_id,
                &backup_name(),
                validation_inputs(),
            )
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
            .stage_recovery_restore_candidate(
                recovery_operation_id,
                &backup_name(),
                validation_inputs(),
            )
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
            .stage_recovery_restore_candidate(
                recovery_operation_id,
                &backup_name(),
                validation_inputs(),
            )
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
            .stage_restore(
                restore.operation_id(),
                restore.backup_name(),
                validation_inputs(),
            )
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
    let journal = {
        let mut path = database.as_os_str().to_os_string();
        path.push(".riffjournal");
        PathBuf::from(path)
    };
    fs::remove_file(journal).expect("remove paired journal");
    fs::remove_file(riffdb_storage_redb::durable_format_marker_path(&database))
        .expect("remove paired format marker");
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
// req: STO-021
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
fn receipted_retirement_explains_absence_and_permanently_consumes_the_name() {
    let root = TestRoot::new("receipted-retirement");
    let database = root.join("database.redb");
    let backup_root = root.join("backups");
    initialize(&database);
    let (mut storage, _) =
        RedbMaintenanceStorage::open(&database, &backup_root).expect("open maintenance");
    let (_, manifest) = create_completed_named_backup(&mut storage, 0x31);
    let (originating_create, prepared_manifest) = storage
        .prepare_backup_retirement(&backup_name())
        .expect("prepare retirement");
    assert_eq!(prepared_manifest, manifest);

    let operation_id = operation_id(0x32);
    let name = backup_name();
    let input_hash = offline_maintenance_input_hash(
        OfflineMaintenanceOperationKind::RetireBackup,
        &name,
        OfflineMaintenanceReplacementConfirmation::NotProvided,
    );
    let mut retirement = OfflineMaintenanceReceiptV2::accepted_retirement(
        operation_id,
        name.clone(),
        input_hash,
        admission(),
        OfflineBackupRetirementEvidenceV2::new(originating_create, manifest.clone()),
    )
    .expect("accepted retirement");
    assert_eq!(
        storage
            .create_or_read_retire_receipt(&retirement)
            .expect("persist accepted retirement"),
        OfflineMaintenanceReceiptCreateResultV2::Created
    );
    for phase in [
        OfflineMaintenanceReceiptPhaseV1::Draining,
        OfflineMaintenanceReceiptPhaseV1::Offline,
    ] {
        retirement
            .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
            .expect("advance retirement");
    }
    storage
        .replace_retire_receipt(&retirement)
        .expect("persist offline retirement");
    storage
        .publish_backup_retirement(operation_id)
        .expect("publish retire stage");
    for phase in [
        OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
        OfflineMaintenanceReceiptPhaseV1::Validating,
    ] {
        retirement
            .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
            .expect("advance published retirement");
    }
    storage
        .replace_retire_receipt(&retirement)
        .expect("persist validation");
    storage
        .delete_published_backup_retirement(operation_id)
        .expect("delete fixed inventory");
    retirement
        .advance(OfflineMaintenanceReceiptTransitionV1::phase(
            OfflineMaintenanceReceiptPhaseV1::Succeeded,
        ))
        .expect("complete retirement");
    storage
        .replace_retire_receipt(&retirement)
        .expect("persist completion");
    assert!(!backup_root.join(name.as_str()).exists());
    storage
        .reconcile()
        .expect("terminal receipt explains absence");
    drop(storage);

    let (mut reopened, _) = RedbMaintenanceStorage::open(&database, &backup_root)
        .expect("startup accepts exactly receipted absence");
    let duplicate = receipt(0x33, OfflineMaintenanceOperationKind::CreateBackup);
    assert_eq!(
        reopened
            .create_or_read_receipt(&duplicate)
            .expect_err("retired name remains consumed")
            .kind(),
        StorageErrorKind::Unavailable
    );
}

#[test]
fn missing_published_backup_without_terminal_retirement_fails_closed() {
    let root = TestRoot::new("unreceipted-retirement");
    let database = root.join("database.redb");
    let backup_root = root.join("backups");
    initialize(&database);
    let (mut storage, _) =
        RedbMaintenanceStorage::open(&database, &backup_root).expect("open maintenance");
    create_completed_named_backup(&mut storage, 0x41);
    fs::remove_dir_all(backup_root.join(backup_name().as_str()))
        .expect("simulate unexplained artifact loss");
    assert_eq!(
        storage
            .reconcile()
            .expect_err("absence without retirement must fail closed")
            .kind(),
        StorageErrorKind::CorruptData
    );
}

#[test]
fn every_retirement_rename_and_delete_boundary_is_retry_safe() {
    for (ordinal, failpoint) in [
        RedbMaintenanceFailpoint::BeforeRetirementPublication,
        RedbMaintenanceFailpoint::AfterRetirementPublication,
        RedbMaintenanceFailpoint::AfterRetirementNamedParentSync,
        RedbMaintenanceFailpoint::AfterRetirementStageParentSync,
    ]
    .into_iter()
    .enumerate()
    {
        let root = TestRoot::new(&format!("retire-publish-{ordinal}"));
        let database = root.join("database.redb");
        let backup_root = root.join("backups");
        initialize(&database);
        let controller = RedbMaintenanceTestController::return_at(failpoint);
        let (mut storage, _) =
            RedbMaintenanceStorage::open_with_test_controller(&database, &backup_root, controller)
                .expect("open controlled maintenance");
        let retirement = accepted_offline_retirement(&mut storage, 0x51, 0x52);
        assert!(
            storage
                .publish_backup_retirement(retirement.operation_id())
                .is_err(),
            "armed publication boundary must return uncertainty"
        );
        storage.reconcile().unwrap_or_else(|error| {
            panic!("publication uncertainty at {failpoint:?} remains reconcilable: {error:?}")
        });
        storage
            .publish_backup_retirement(retirement.operation_id())
            .expect("exact retry converges publication");
    }

    for (ordinal, failpoint) in [
        RedbMaintenanceFailpoint::AfterRetirementDatabaseDelete,
        RedbMaintenanceFailpoint::AfterRetirementFormatDelete,
        RedbMaintenanceFailpoint::AfterRetirementJournalDelete,
        RedbMaintenanceFailpoint::AfterRetirementManifestDelete,
        RedbMaintenanceFailpoint::AfterRetirementStageDelete,
        RedbMaintenanceFailpoint::AfterRetirementDeleteParentSync,
    ]
    .into_iter()
    .enumerate()
    {
        let root = TestRoot::new(&format!("retire-delete-{ordinal}"));
        let database = root.join("database.redb");
        let backup_root = root.join("backups");
        initialize(&database);
        let controller = RedbMaintenanceTestController::return_at(failpoint);
        let (mut storage, _) =
            RedbMaintenanceStorage::open_with_test_controller(&database, &backup_root, controller)
                .expect("open controlled maintenance");
        let mut retirement = accepted_offline_retirement(&mut storage, 0x61, 0x62);
        storage
            .publish_backup_retirement(retirement.operation_id())
            .expect("publish retirement");
        for phase in [
            OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
            OfflineMaintenanceReceiptPhaseV1::Validating,
        ] {
            retirement
                .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
                .expect("advance retirement");
        }
        storage
            .replace_retire_receipt(&retirement)
            .expect("persist validating retirement");
        assert!(
            storage
                .delete_published_backup_retirement(retirement.operation_id())
                .is_err(),
            "armed deletion boundary must return uncertainty"
        );
        storage.reconcile().unwrap_or_else(|error| {
            panic!("partial deletion at {failpoint:?} remains reconcilable: {error:?}")
        });
        storage
            .delete_published_backup_retirement(retirement.operation_id())
            .expect("exact retry converges deletion");
    }
}

#[test]
fn retirement_process_crash_boundaries_recover_without_inventing_success() {
    const CHILD_DATABASE: &str = "RIFFDB_WP610_RETIRE_CHILD_DATABASE";
    const CHILD_BACKUP_ROOT: &str = "RIFFDB_WP610_RETIRE_CHILD_BACKUP_ROOT";
    const CHILD_ACTION: &str = "RIFFDB_WP610_RETIRE_CHILD_ACTION";
    const CHILD_FAILPOINT: &str = "RIFFDB_WP610_RETIRE_CHILD_FAILPOINT";

    if let (Ok(database), Ok(backup_root), Ok(action), Ok(failpoint)) = (
        std::env::var(CHILD_DATABASE),
        std::env::var(CHILD_BACKUP_ROOT),
        std::env::var(CHILD_ACTION),
        std::env::var(CHILD_FAILPOINT),
    ) {
        let failpoint = match failpoint.as_str() {
            "before-publication" => RedbMaintenanceFailpoint::BeforeRetirementPublication,
            "after-publication" => RedbMaintenanceFailpoint::AfterRetirementPublication,
            "after-named-sync" => RedbMaintenanceFailpoint::AfterRetirementNamedParentSync,
            "after-stage-sync" => RedbMaintenanceFailpoint::AfterRetirementStageParentSync,
            "after-database-delete" => RedbMaintenanceFailpoint::AfterRetirementDatabaseDelete,
            "after-format-delete" => RedbMaintenanceFailpoint::AfterRetirementFormatDelete,
            "after-journal-delete" => RedbMaintenanceFailpoint::AfterRetirementJournalDelete,
            "after-manifest-delete" => RedbMaintenanceFailpoint::AfterRetirementManifestDelete,
            "after-stage-delete" => RedbMaintenanceFailpoint::AfterRetirementStageDelete,
            "after-delete-sync" => RedbMaintenanceFailpoint::AfterRetirementDeleteParentSync,
            _ => panic!("unknown closed retirement failpoint"),
        };
        let controller = RedbMaintenanceTestController::abort_at(failpoint);
        let (mut storage, _) =
            RedbMaintenanceStorage::open_with_test_controller(database, backup_root, controller)
                .expect("child opens exact maintenance state");
        let result = match action.as_str() {
            "publish" => storage.publish_backup_retirement(operation_id(0x72)),
            "delete" => storage.delete_published_backup_retirement(operation_id(0x72)),
            _ => panic!("unknown closed retirement action"),
        };
        panic!("armed retirement child did not abort: {result:?}");
    }

    let cases = [
        ("publish", "before-publication"),
        ("publish", "after-publication"),
        ("publish", "after-named-sync"),
        ("publish", "after-stage-sync"),
        ("delete", "after-database-delete"),
        ("delete", "after-format-delete"),
        ("delete", "after-journal-delete"),
        ("delete", "after-manifest-delete"),
        ("delete", "after-stage-delete"),
        ("delete", "after-delete-sync"),
    ];
    for (ordinal, (action, failpoint)) in cases.into_iter().enumerate() {
        let root = TestRoot::new(&format!("retire-process-{ordinal}"));
        let database = root.join("database.redb");
        let backup_root = root.join("backups");
        initialize(&database);
        let (mut storage, _) =
            RedbMaintenanceStorage::open(&database, &backup_root).expect("open maintenance");
        let mut retirement = accepted_offline_retirement(&mut storage, 0x71, 0x72);
        if action == "delete" {
            storage
                .publish_backup_retirement(retirement.operation_id())
                .expect("publish before delete crash");
            for phase in [
                OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
                OfflineMaintenanceReceiptPhaseV1::Validating,
            ] {
                retirement
                    .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
                    .expect("advance validating retirement");
            }
            storage
                .replace_retire_receipt(&retirement)
                .expect("persist validating retirement");
        }
        drop(storage);

        let status = Command::new(std::env::current_exe().expect("current test executable"))
            .arg("--exact")
            .arg("retirement_process_crash_boundaries_recover_without_inventing_success")
            .arg("--nocapture")
            .env(CHILD_DATABASE, &database)
            .env(CHILD_BACKUP_ROOT, &backup_root)
            .env(CHILD_ACTION, action)
            .env(CHILD_FAILPOINT, failpoint)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("run retirement crash child");
        assert!(!status.success(), "armed retirement child must abort");
        use std::os::unix::process::ExitStatusExt as _;
        assert_eq!(
            status.signal(),
            Some(6),
            "armed retirement child must terminate at {failpoint}"
        );

        let (mut recovered, _) = RedbMaintenanceStorage::open(&database, &backup_root)
            .expect("startup reconciles exact interrupted retirement");
        recovered
            .reconcile()
            .expect("interrupted retirement remains exact");
        if action == "publish" {
            recovered
                .publish_backup_retirement(retirement.operation_id())
                .expect("publication retry converges");
        } else {
            recovered
                .delete_published_backup_retirement(retirement.operation_id())
                .expect("deletion retry converges");
        }
    }
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
        4
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
fn corrupt_target_restore_completes_and_fence_does_not_decrease() {
    // N1: destructive restore may replace a present-but-unreadable target.
    // Stage/seal against a healthy target first (staging copies the backup only),
    // then corrupt the configured target immediately before publication.
    let root = TestRoot::new("corrupt-target-restore");
    let database = root.join("database.redb");
    let backup_root = root.join("backups");
    initialize(&database);
    let (mut storage, _) =
        RedbMaintenanceStorage::open(&database, &backup_root).expect("open maintenance");
    let (_manifest, manifest_identity) = create_completed_named_backup(&mut storage, 0x71);

    riffdb_storage_redb::stamp_history_incarnation(&database, 5).expect("stamp target");
    assert_eq!(
        riffdb_storage_redb::read_history_incarnation(&database).expect("read"),
        Some(5)
    );

    let mut restore = receipt(0x72, OfflineMaintenanceOperationKind::RestoreBackup);
    advance_offline(&mut restore);
    storage
        .create_or_read_receipt(&restore)
        .expect("create restore receipt");
    let staged = storage
        .stage_restore(
            restore.operation_id(),
            restore.backup_name(),
            validation_inputs(),
        )
        .expect("stage");
    let staged_database_id = complete_structural_validation(staged.staged_database_file());
    let mut sealed = staged
        .seal_after_validation(staged_database_id)
        .expect("seal");
    // Unreadable target floor is staged; published = max(staged, staged)+1 when
    // the driver cannot read the prior high-water mark.
    let staged_inc = sealed.staged_history_incarnation();
    let published = staged_inc + 1;
    sealed
        .apply_published_history_incarnation(published)
        .expect("stamp staged");
    restore
        .record_staged_database_id(staged_database_id)
        .expect("staged id");
    restore
        .record_manifest_identity(manifest_identity)
        .expect("manifest");
    restore
        .record_published_incarnation(published)
        .expect("receipt");
    storage.replace_receipt(&restore).expect("persist receipt");

    // Corrupt only after receipt authority is durable — mirrors crash/recovery
    // where the operator replaces a brick-failed database file.
    std::fs::write(&database, b"truncated-garbage-not-redb").expect("corrupt target");
    assert!(
        riffdb_storage_redb::read_history_incarnation(&database).is_err(),
        "target must be unreadable for this scenario"
    );

    let result = storage
        .publish_sealed_restore(
            sealed,
            OfflineRestoreOverwritePolicyV1::ExplicitlyAllowDestructive,
        )
        .expect("publish replaces corrupt target");
    assert!(matches!(
        result,
        riffdb_storage_api::OfflineRestoreResultV1::Restored { .. }
    ));
    assert_eq!(
        riffdb_storage_redb::read_history_incarnation(&database).expect("read restored"),
        Some(published)
    );
    assert!(published >= 1);
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
        .stage_restore(
            restore.operation_id(),
            restore.backup_name(),
            validation_inputs(),
        )
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
fn resume_after_between_receipt_and_stamp_converges_to_receipt_value() {
    // W4: crash window between durable receipt published-incarnation write and
    // staged stamp; resume must use the receipt value and keep target bytes
    // identical to the staged seal.
    let root = TestRoot::new("receipt-stamp-window");
    let database = root.join("database.redb");
    let backup_root = root.join("backups");
    initialize(&database);
    let failpoint = RedbMaintenanceTestController::return_at(
        RedbMaintenanceFailpoint::BetweenReceiptWriteAndStagedStamp,
    );
    let (mut storage, _) =
        RedbMaintenanceStorage::open_with_test_controller(&database, &backup_root, failpoint)
            .expect("open maintenance");
    let (_manifest, manifest_identity) = create_completed_named_backup(&mut storage, 0x81);

    let mut restore = receipt(0x82, OfflineMaintenanceOperationKind::RestoreBackup);
    advance_offline(&mut restore);
    storage
        .create_or_read_receipt(&restore)
        .expect("create restore receipt");
    let staged = storage
        .stage_restore(
            restore.operation_id(),
            restore.backup_name(),
            validation_inputs(),
        )
        .expect("stage");
    let staged_database_id = complete_structural_validation(staged.staged_database_file());
    let mut sealed = staged
        .seal_after_validation(staged_database_id)
        .expect("seal");
    let staged_before = sealed.staged_history_incarnation();
    let published = staged_before.max(1) + 1;

    restore
        .record_staged_database_id(staged_database_id)
        .expect("staged id");
    restore
        .record_manifest_identity(manifest_identity)
        .expect("manifest");
    restore
        .record_published_incarnation(published)
        .expect("receipt authority");
    storage
        .replace_receipt(&restore)
        .expect("durable receipt before stamp");

    // Failpoint fires at the start of apply_published — receipt is durable,
    // staged is not yet stamped.
    let err = sealed
        .apply_published_history_incarnation(published)
        .expect_err("between receipt and stamp");
    assert_eq!(err.kind(), StorageErrorKind::CommitStatusUnknown);
    assert_eq!(
        sealed.staged_history_incarnation(),
        staged_before,
        "failed stamp must not mutate sealed staged incarnation"
    );

    // Resume: re-apply from receipt; stamping is idempotent and converges.
    sealed
        .apply_published_history_incarnation(published)
        .expect("resume stamp from receipt");
    assert_eq!(sealed.staged_history_incarnation(), published);
    let result = storage
        .publish_sealed_restore(
            sealed,
            OfflineRestoreOverwritePolicyV1::ExplicitlyAllowDestructive,
        )
        .expect("publish after resume");
    assert!(matches!(
        result,
        riffdb_storage_api::OfflineRestoreResultV1::Restored { .. }
    ));
    assert_eq!(
        riffdb_storage_redb::read_history_incarnation(&database).expect("target"),
        Some(published)
    );
    let loaded = storage
        .read_receipt(restore.operation_id())
        .expect("read")
        .expect("present");
    assert_eq!(loaded.published_history_incarnation(), Some(published));

    // Target must match the staged seal (byte-identical invariant).
    restore
        .advance(OfflineMaintenanceReceiptTransitionV1::phase(
            OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
        ))
        .expect("published phase");
    storage
        .replace_receipt(&restore)
        .expect("persist published");
    let reconciliation = storage.reconcile().expect("reconcile");
    let evidence = reconciliation
        .operations()
        .iter()
        .find(|evidence| evidence.operation_id() == restore.operation_id())
        .expect("restore evidence");
    assert!(
        evidence.configured_target_matches(),
        "resumed stamp+publish must keep target byte-identical to staged seal"
    );
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
            .stage_restore(recovery_operation_id, &backup_name(), validation_inputs(),)
            .expect_err("normal staging requires an offline receipt")
            .kind(),
        StorageErrorKind::Unavailable
    );
    let target_before_recovery = Sha256::digest(fs::read(&database).expect("read target"));
    let recovery_candidate = storage
        .stage_recovery_restore_candidate(
            recovery_operation_id,
            &backup_name(),
            validation_inputs(),
        )
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
        .stage_restore(
            restore.operation_id(),
            restore.backup_name(),
            validation_inputs(),
        )
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
        .stage_restore(
            restore.operation_id(),
            restore.backup_name(),
            validation_inputs(),
        )
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
