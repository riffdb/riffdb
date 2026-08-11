#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]

//! Real-process WP-408 staged contract-migration recovery evidence.

use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use riffdb_catalog::{
    CatalogHistoryOutcome, ValidatedContractBundle, ValidatedMigrationPlan,
    validate_catalog_history,
};
use riffdb_commit::MigrationCoordinator;
use riffdb_contract_ir::MigrationBundleV1;
use riffdb_storage_api::{
    AuditPrincipalV1, BootstrapDigestCandidatesV1, BootstrapServiceAuditStartV1,
    CapabilityBootstrapAdministrationRepository, CapabilityBootstrapIntentV1,
    CapabilityBootstrapResult, CapabilityGrantV1, CapabilityPermissionKindV1,
    CapabilityPermissionV1, CapabilityPermissionsV1, CapabilityRequestedRecordV1,
    CatalogActivationIntentV1, CatalogActivationResult, CatalogAdministrationRepository,
    ContractMigrationAdmissionV1, ContractMigrationArtifactFileV1, ContractMigrationArtifactsV1,
    ContractMigrationOperationArtifactsV1, ContractMigrationReceiptFailureV1,
    ContractMigrationReceiptPhaseV1, ContractMigrationReceiptTransitionV1,
    ContractMigrationReceiptV1, DatabaseIdentityProbe, DatabaseIdentityProbePort,
    DatabaseInitializationPort, DatabaseInitializationResult, EvidencePageLimit,
    HistoricalEvidenceCursor, HistoricalEvidencePage, PartitionScopeV1,
    ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
    StartupValidationInputs, StructuralEvidenceCursor, StructuralEvidenceOpen,
    StructuralEvidencePage, StructuralEvidenceSession, StructuralOpenOutcome,
};
use riffdb_storage_redb::{
    RedbContractMigrationContext, RedbContractMigrationStage, RedbMaintenanceStorage,
    RedbOperationalPorts, RedbStore,
};
use riffdb_testkit::process::{ChildProcessController, ChildProcessSpec};
use riffdb_types::{
    ActorId, ActorKind, ApprovalId, Audience, CapabilityId, CapabilityTokenDigest,
    ContractBundleHash, DatabaseId, DigestKeyId, Environment, RequestId, ServiceAuditTargetV1,
    ServiceAuditTargetsV1, ServiceIngressKindV1, TenantScope, Timestamp,
    hash_contract_migration_input,
};

const PARENT: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/bundle/v1/parent.contract.bundle"
));
const CANDIDATE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/bundle/v1/candidate.contract.bundle"
));
const MIGRATION: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/bundle/v1/required-field.migration.bundle"
));
const CANDIDATE_SHA256: &str = "d3ef04f8a5639e1aa48b2aea0a12176bda81e03dd5bc659f79e2452a4a57cd6e";
const MIGRATION_SHA256: &str = "a38067913625a08eefd5bb3c4d029c19c3f4b51c63ccae8377a1e1c22801fecc";
const CAPABILITY_KEYS: &[u8] = b"riffdb-capability-digest-keys-v1\n7:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";
const IDEMPOTENCY_KEYS: &[u8] = b"riffdb-idempotency-digest-keys-v1\n9:202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f\n";
const ENVIRONMENT: &str = "wp408-migration-recovery";
const AUDIENCE: &str = "riffdb-grpc-loopback";
const READY_PREFIX: &str = "riffdbd-ready-v1\t";
const START_TIMEOUT: Duration = Duration::from_secs(60);
const STOP_TIMEOUT: Duration = Duration::from_secs(15);

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[test]
#[ignore = "spawns the real riffdbd process and atomically replaces a staged database"]
fn accepted_migration_recovers_to_one_fully_validated_successor() -> TestResult<()> {
    let temporary = TemporaryDirectory::new()?;
    let database = temporary.path().join("riffdb.redb");
    let backup_root = temporary.path().join("backups");
    let sibling_database = temporary.path().join("sibling.redb");
    let sibling_backup_root = temporary.path().join("sibling-backups");
    let capability_keys = temporary.path().join("capability.keys");
    let idempotency_keys = temporary.path().join("idempotency.keys");
    write_protected_file(&capability_keys, CAPABILITY_KEYS)?;
    write_protected_file(&idempotency_keys, IDEMPOTENCY_KEYS)?;

    let database_id = deterministic_database_id()?;
    seed_predecessor(&database, database_id)?;
    seed_predecessor(&sibling_database, deterministic_sibling_database_id()?)?;
    fs::create_dir_all(&sibling_backup_root)?;
    let parent = ValidatedContractBundle::decode(PARENT)?;
    let candidate = ValidatedContractBundle::decode(CANDIDATE)?;
    let migration = MigrationBundleV1::decode(MIGRATION)?;
    let operation_id =
        riffdb_types::ContractMigrationOperationId::from_unix_milliseconds_and_random(
            1_785_000_000_000,
            [0x48; 10],
        )?;
    let receipt = accepted_receipt(database_id, operation_id, &parent, &candidate, &migration)?;
    let (maintenance, reconciliation) = RedbMaintenanceStorage::open(&database, &backup_root)?;
    if !reconciliation.migration_receipts().is_empty() {
        return Err(test_failure(
            "fresh fixture already contained migration evidence",
        ));
    }
    maintenance.accept_contract_migration(&receipt, CANDIDATE, MIGRATION)?;
    drop(maintenance);

    let config = write_multi_database_config(
        temporary.path(),
        &database,
        &backup_root,
        &sibling_database,
        &sibling_backup_root,
        &capability_keys,
        &idempotency_keys,
    )?;
    let specification = ChildProcessSpec::new(env!("CARGO_BIN_EXE_riffdbd"))?
        .arg("--config")?
        .arg(config.as_os_str())?;
    let mut process = ChildProcessController::spawn(&specification)?;
    let readiness = match process.wait_for_readiness(READY_PREFIX, START_TIMEOUT) {
        Ok(readiness) => readiness,
        Err(error) => {
            let retained = RedbMaintenanceStorage::open(&database, &backup_root)
                .ok()
                .and_then(|(storage, _)| {
                    storage
                        .read_contract_migration_receipt(operation_id)
                        .ok()
                        .flatten()
                });
            let phase = retained
                .as_ref()
                .map(ContractMigrationReceiptV1::current_phase);
            let stage_path = database
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(format!(
                    ".{}.migration-{operation_id}.stage",
                    database
                        .file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or("riffdb.redb")
                ));
            let stage_validation = validated_history(&stage_path)
                .map(|history| history.active().map(|active| active.bundle_hash()))
                .map_err(|error| error.to_string());
            let target_validation = validated_history(&database)
                .map(|history| history.active().map(|active| active.bundle_hash()))
                .map_err(|error| error.to_string());
            return Err(test_failure(format!(
                "riffdbd failed before readiness at migration phase {phase:?}, target {target_validation:?}, stage {stage_validation:?}: {error}"
            )));
        }
    };
    if readiness
        .trim_start_matches(READY_PREFIX)
        .parse::<std::net::SocketAddr>()
        .is_err()
    {
        return Err(test_failure(
            "riffdbd emitted a malformed readiness address",
        ));
    }
    process.shutdown_cleanly(b"shutdown\n", STOP_TIMEOUT)?;
    let sibling_history = validated_history(&sibling_database)?;
    let (sibling_maintenance, sibling_reconciliation) =
        RedbMaintenanceStorage::open(&sibling_database, &sibling_backup_root)?;
    let sibling_has_operation_backup = fs::read_dir(&sibling_backup_root)?.any(|entry| {
        entry
            .ok()
            .is_some_and(|entry| entry.file_name() != ".maintenance")
    });
    if sibling_history
        .active()
        .is_none_or(|active| active.bundle_hash() != parent.bundle_hash())
        || !sibling_reconciliation.migration_receipts().is_empty()
        || sibling_maintenance
            .read_contract_migration_receipt(operation_id)?
            .is_some()
        || sibling_has_operation_backup
    {
        return Err(test_failure(
            "selected migration changed the configured sibling authority or backup inventory",
        ));
    }

    let (maintenance, reconciliation) = RedbMaintenanceStorage::open(&database, &backup_root)?;
    let terminal = maintenance
        .read_contract_migration_receipt(operation_id)?
        .ok_or_else(|| test_failure("terminal migration receipt disappeared"))?;
    if terminal.current_phase() != ContractMigrationReceiptPhaseV1::Succeeded
        || reconciliation.migration_receipts() != [terminal.clone()]
    {
        return Err(test_failure(
            "migration did not converge to one terminal success",
        ));
    }
    let backup_name = terminal
        .backup_name()
        .ok_or_else(|| test_failure("success omitted immutable backup identity"))?;
    if !backup_root.join(backup_name.as_str()).is_dir() {
        return Err(test_failure(
            "successful migration did not retain its normal backup",
        ));
    }
    drop(maintenance);

    let history = validated_history(&database)?;
    let active = history
        .active()
        .ok_or_else(|| test_failure("successor catalog was absent after readiness"))?;
    if active.bundle_hash() != candidate.bundle_hash()
        || history
            .active_lineage_bundles()
            .is_none_or(|lineage| lineage.len() != 2)
    {
        return Err(test_failure(
            "fresh startup did not validate the predecessor-to-successor migration edge",
        ));
    }
    Ok(())
}

#[test]
#[ignore = "spawns the real riffdbd process and proves preflight failure reopens the predecessor"]
fn artifact_mismatch_fails_closed_and_restores_predecessor_readiness() -> TestResult<()> {
    let temporary = TemporaryDirectory::new()?;
    let database = temporary.path().join("riffdb.redb");
    let backup_root = temporary.path().join("backups");
    let capability_keys = temporary.path().join("capability.keys");
    let idempotency_keys = temporary.path().join("idempotency.keys");
    write_protected_file(&capability_keys, CAPABILITY_KEYS)?;
    write_protected_file(&idempotency_keys, IDEMPOTENCY_KEYS)?;

    let database_id = deterministic_database_id()?;
    seed_predecessor(&database, database_id)?;
    let parent = ValidatedContractBundle::decode(PARENT)?;
    let candidate = ValidatedContractBundle::decode(CANDIDATE)?;
    let migration = MigrationBundleV1::decode(MIGRATION)?;
    let operation_id =
        riffdb_types::ContractMigrationOperationId::from_unix_milliseconds_and_random(
            1_785_000_000_000,
            [0x4a; 10],
        )?;
    let accepted = accepted_receipt(database_id, operation_id, &parent, &candidate, &migration)?;
    let mismatched = ContractMigrationReceiptV1::from_canonical_parts(
        accepted.database_id(),
        accepted.operation_id(),
        accepted.input_hash(),
        ContractMigrationArtifactsV1::new(
            accepted.artifacts().parent(),
            ContractBundleHash::from_bytes([0xd1; 32]),
            accepted.artifacts().migration(),
        ),
        accepted.operation_artifacts(),
        accepted.admission().clone(),
        None,
        None,
        None,
        accepted.transitions().to_vec(),
    )?;
    let (maintenance, _) = RedbMaintenanceStorage::open(&database, &backup_root)?;
    maintenance.accept_contract_migration(&mismatched, CANDIDATE, MIGRATION)?;
    drop(maintenance);

    let specification = ChildProcessSpec::new(env!("CARGO_BIN_EXE_riffdbd"))?
        .arg("--database")?
        .arg(database.as_os_str())?
        .arg("--listen")?
        .arg("127.0.0.1:0")?
        .arg("--environment")?
        .arg(ENVIRONMENT)?
        .arg("--audience")?
        .arg(AUDIENCE)?
        .arg("--backup-root")?
        .arg(backup_root.as_os_str())?
        .arg("--capability-keys")?
        .arg(capability_keys.as_os_str())?
        .arg("--idempotency-keys")?
        .arg(idempotency_keys.as_os_str())?;
    let mut process = ChildProcessController::spawn(&specification)?;
    process.wait_for_readiness(READY_PREFIX, START_TIMEOUT)?;
    process.shutdown_cleanly(b"shutdown\n", STOP_TIMEOUT)?;

    let (maintenance, _) = RedbMaintenanceStorage::open(&database, &backup_root)?;
    let terminal = maintenance
        .read_contract_migration_receipt(operation_id)?
        .ok_or_else(|| test_failure("failed-closed receipt disappeared"))?;
    if terminal.current_phase() != ContractMigrationReceiptPhaseV1::FailedClosed
        || terminal
            .transitions()
            .last()
            .and_then(|transition| transition.failure())
            != Some(ContractMigrationReceiptFailureV1::ArtifactMismatch)
        || terminal.backup_name().is_some()
    {
        return Err(test_failure(
            "artifact mismatch did not fail before backup with its closed reason",
        ));
    }
    drop(maintenance);
    let history = validated_history(&database)?;
    if history
        .active()
        .is_none_or(|active| active.bundle_hash() != parent.bundle_hash())
    {
        return Err(test_failure(
            "failed preflight did not reopen the exact predecessor",
        ));
    }
    Ok(())
}

#[test]
#[ignore = "spawns the real riffdbd process and automatically restores a failed publication"]
fn invalid_published_successor_rolls_back_before_readiness() -> TestResult<()> {
    let temporary = TemporaryDirectory::new()?;
    let database = temporary.path().join("riffdb.redb");
    let backup_root = temporary.path().join("backups");
    let capability_keys = temporary.path().join("capability.keys");
    let idempotency_keys = temporary.path().join("idempotency.keys");
    write_protected_file(&capability_keys, CAPABILITY_KEYS)?;
    write_protected_file(&idempotency_keys, IDEMPOTENCY_KEYS)?;

    let database_id = deterministic_database_id()?;
    seed_predecessor(&database, database_id)?;
    let parent = ValidatedContractBundle::decode(PARENT)?;
    let candidate = ValidatedContractBundle::decode(CANDIDATE)?;
    let migration = MigrationBundleV1::decode(MIGRATION)?;
    let plan = ValidatedMigrationPlan::from_lineage_artifacts(
        vec![parent.clone()],
        candidate,
        migration.clone(),
    )
    .map_err(|finding| test_failure(format!("migration plan failed: {}", finding.code())))?;
    let operation_id =
        riffdb_types::ContractMigrationOperationId::from_unix_milliseconds_and_random(
            1_785_000_000_000,
            [0x49; 10],
        )?;
    let mut receipt = accepted_receipt(
        database_id,
        operation_id,
        &parent,
        plan.candidate(),
        &migration,
    )?;
    let (maintenance, _) = RedbMaintenanceStorage::open(&database, &backup_root)?;
    maintenance.accept_contract_migration(&receipt, CANDIDATE, MIGRATION)?;
    for phase in [
        ContractMigrationReceiptPhaseV1::Draining,
        ContractMigrationReceiptPhaseV1::Preflight,
    ] {
        let next = receipt.advance(phase)?;
        maintenance.replace_contract_migration_receipt(&receipt, &next)?;
        receipt = next;
    }
    let (backup_name, _, backup_identity) =
        maintenance.create_contract_migration_backup(operation_id, &backup_build_metadata()?)?;
    let next = receipt.publish_backup(backup_name, backup_identity)?;
    maintenance.replace_contract_migration_receipt(&receipt, &next)?;
    receipt = next;
    let next = receipt.advance(ContractMigrationReceiptPhaseV1::Staging)?;
    maintenance.replace_contract_migration_receipt(&receipt, &next)?;
    receipt = next;
    let (stage_path, stage_identity) =
        maintenance.materialize_contract_migration_stage(operation_id)?;
    let next = receipt.begin_transforming(stage_identity)?;
    maintenance.replace_contract_migration_receipt(&receipt, &next)?;
    receipt = next;

    let context = RedbContractMigrationContext::from_receipt(&receipt)?;
    let ports = open_operational(RedbStore::open(&stage_path)?)?;
    let mut stage = RedbContractMigrationStage::new(ports, context)?;
    MigrationCoordinator::apply(&plan, &mut stage)
        .map_err(|finding| test_failure(format!("migration apply failed: {}", finding.code())))?;
    drop(stage.into_ports());
    for phase in [
        ContractMigrationReceiptPhaseV1::RebuildingProjections,
        ContractMigrationReceiptPhaseV1::ValidatingStage,
        ContractMigrationReceiptPhaseV1::Publishing,
    ] {
        let next = receipt.advance(phase)?;
        maintenance.replace_contract_migration_receipt(&receipt, &next)?;
        receipt = next;
    }
    maintenance.publish_contract_migration_stage(operation_id)?;
    let next = receipt.advance(ContractMigrationReceiptPhaseV1::ValidatingPublished)?;
    maintenance.replace_contract_migration_receipt(&receipt, &next)?;
    drop(maintenance);

    OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&database)?
        .sync_all()?;

    let specification = ChildProcessSpec::new(env!("CARGO_BIN_EXE_riffdbd"))?
        .arg("--database")?
        .arg(database.as_os_str())?
        .arg("--listen")?
        .arg("127.0.0.1:0")?
        .arg("--environment")?
        .arg(ENVIRONMENT)?
        .arg("--audience")?
        .arg(AUDIENCE)?
        .arg("--backup-root")?
        .arg(backup_root.as_os_str())?
        .arg("--capability-keys")?
        .arg(capability_keys.as_os_str())?
        .arg("--idempotency-keys")?
        .arg(idempotency_keys.as_os_str())?;
    let mut process = ChildProcessController::spawn(&specification)?;
    process.wait_for_readiness(READY_PREFIX, START_TIMEOUT)?;
    process.shutdown_cleanly(b"shutdown\n", STOP_TIMEOUT)?;

    let (maintenance, reconciliation) = RedbMaintenanceStorage::open(&database, &backup_root)?;
    let terminal = maintenance
        .read_contract_migration_receipt(operation_id)?
        .ok_or_else(|| test_failure("rollback receipt disappeared"))?;
    if terminal.current_phase() != ContractMigrationReceiptPhaseV1::FailedRolledBack
        || reconciliation.migration_receipts() != [terminal]
    {
        return Err(test_failure(
            "published validation failure did not converge to rollback",
        ));
    }
    drop(maintenance);
    let history = validated_history(&database)?;
    if history
        .active()
        .is_none_or(|active| active.bundle_hash() != parent.bundle_hash())
    {
        return Err(test_failure(
            "rollback did not restore the exact predecessor",
        ));
    }
    Ok(())
}

fn backup_build_metadata() -> TestResult<riffdb_storage_api::BackupBuildMetadataV1> {
    Ok(riffdb_storage_api::BackupBuildMetadataV1::new(
        env!("CARGO_PKG_VERSION"),
        "wp408-recovery-fixture",
        "rustc-1.97.0",
        1,
        vec!["contract-migration".to_owned()],
    )?)
}

fn seed_predecessor(path: &Path, database_id: DatabaseId) -> TestResult<()> {
    let mut store = RedbStore::open(path)?;
    if store.probe_database_identity()? != DatabaseIdentityProbe::NeedsInitialization
        || store.initialize_database(database_id)?
            != DatabaseInitializationResult::Installed(database_id)
    {
        return Err(test_failure("database initialization was not exact"));
    }
    let mut ports = open_operational(store)?;
    let bootstrap = bootstrap_intent(database_id)?;
    if !matches!(
        ports.bootstrap_capability(&bootstrap)?,
        CapabilityBootstrapResult::BootstrapCreated { .. }
    ) {
        return Err(test_failure("bootstrap fixture was not newly installed"));
    }
    let parent = ValidatedContractBundle::decode(PARENT)?;
    let activation = CatalogActivationIntentV1::new(
        None,
        parent.to_stored()?,
        request_id(0x31)?,
        principal()?,
        fixture_time()?,
        Some(ApprovalId::new("wp408-parent-approval")?),
    );
    if !matches!(
        ports.activate_catalog(&activation)?,
        CatalogActivationResult::Activated { .. }
    ) {
        return Err(test_failure("predecessor catalog was not newly activated"));
    }
    Ok(())
}

fn accepted_receipt(
    database_id: DatabaseId,
    operation_id: riffdb_types::ContractMigrationOperationId,
    parent: &ValidatedContractBundle,
    candidate: &ValidatedContractBundle,
    migration: &MigrationBundleV1,
) -> TestResult<ContractMigrationReceiptV1> {
    let mut semantic_input = Vec::with_capacity(96);
    semantic_input.extend_from_slice(parent.bundle_hash().as_bytes());
    semantic_input.extend_from_slice(candidate.bundle_hash().as_bytes());
    semantic_input.extend_from_slice(migration.bundle_hash().as_bytes());
    Ok(ContractMigrationReceiptV1::from_canonical_parts(
        database_id,
        operation_id,
        hash_contract_migration_input(&semantic_input),
        ContractMigrationArtifactsV1::new(
            parent.bundle_hash(),
            candidate.bundle_hash(),
            migration.bundle_hash(),
        ),
        ContractMigrationOperationArtifactsV1::new(
            ContractMigrationArtifactFileV1::new(
                u64::try_from(CANDIDATE.len())?,
                hex_32(CANDIDATE_SHA256)?,
            )?,
            ContractMigrationArtifactFileV1::new(
                u64::try_from(MIGRATION.len())?,
                hex_32(MIGRATION_SHA256)?,
            )?,
        ),
        ContractMigrationAdmissionV1::new(
            principal()?,
            Some(ApprovalId::new("wp408-migration-approval")?),
            request_id(0x32)?,
            fixture_time()?,
            ServiceIngressKindV1::Grpc,
        ),
        None,
        None,
        None,
        vec![ContractMigrationReceiptTransitionV1::phase(
            ContractMigrationReceiptPhaseV1::Accepted,
        )],
    )?)
}

fn bootstrap_intent(database_id: DatabaseId) -> TestResult<CapabilityBootstrapIntentV1> {
    let capability_id = capability_id()?;
    let permissions = CapabilityPermissionsV1::new(vec![CapabilityPermissionV1::unparameterized(
        CapabilityPermissionKindV1::AdministerCapabilities,
    )?])?;
    let grant = CapabilityGrantV1::new(
        TenantScope::Global,
        PartitionScopeV1::All,
        permissions,
        Vec::new(),
        NonZeroU16::MIN,
        Vec::new(),
    )?;
    let issued_at = fixture_time()?;
    let requested = CapabilityRequestedRecordV1::new(
        database_id,
        Environment::new(ENVIRONMENT)?,
        ActorId::new("wp408-operator")?,
        ActorKind::Human,
        NonZeroU32::new(3_600).ok_or_else(|| test_failure("duration is zero"))?,
        vec![Audience::new(AUDIENCE)?],
        grant,
    )?;
    let digest = CapabilityTokenDigest::from_hmac_bytes(
        DigestKeyId::new(7).ok_or_else(|| test_failure("digest key is zero"))?,
        [0x71; 32],
    );
    let start = BootstrapServiceAuditStartV1::new(
        request_id(0x30)?,
        issued_at,
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::new([ServiceAuditTargetV1::Capability(capability_id)])?,
        None,
    )?;
    Ok(CapabilityBootstrapIntentV1::new(
        capability_id,
        requested,
        BootstrapDigestCandidatesV1::new(vec![digest], digest)?,
        issued_at,
        Timestamp::new(issued_at.seconds() + 3_600, 0)?,
        start,
    )?)
}

fn open_operational(store: RedbStore) -> TestResult<RedbOperationalPorts> {
    let mut session = store.begin_structural_evidence(startup_inputs()?)?;
    let database_id = session.database_id();
    let open_session_id = session.open_session_id();
    let limit = EvidencePageLimit::new(64).ok_or_else(|| test_failure("page limit is zero"))?;
    let mut structural = StructuralEvidenceCursor::start(database_id, open_session_id);
    let structural_end = loop {
        match session.read_structural_evidence(structural, limit)? {
            StructuralEvidencePage::Page { findings, next, .. } => {
                if !findings.is_empty() {
                    return Err(test_failure("seed database had a structural finding"));
                }
                structural = next;
            }
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };
    let mut historical = HistoricalEvidenceCursor::start(database_id, open_session_id);
    let historical_end = loop {
        match session.read_historical_evidence(historical, limit)? {
            HistoricalEvidencePage::Page { next, .. } => historical = next,
            HistoricalEvidencePage::ExactEnd(end) => break end,
        }
    };
    let StructuralOpenOutcome::Clean(opened) = session.finish(structural_end, historical_end)?
    else {
        return Err(test_failure(
            "seed unexpectedly required a format migration",
        ));
    };
    let (_, _, _, dormant) = opened.into_parts();
    Ok(dormant.into_operational_after_catalog_validation()?)
}

fn validated_history(path: &Path) -> TestResult<riffdb_catalog::ValidatedCatalogHistory> {
    let store =
        RedbStore::open(path).map_err(|error| test_failure(format!("open stage: {error:?}")))?;
    let mut session = store
        .begin_structural_evidence(startup_inputs()?)
        .map_err(|error| test_failure(format!("begin stage evidence: {error:?}")))?;
    let database_id = session.database_id();
    let open_session_id = session.open_session_id();
    let limit = EvidencePageLimit::new(64).ok_or_else(|| test_failure("page limit is zero"))?;
    let mut structural = StructuralEvidenceCursor::start(database_id, open_session_id);
    let structural_end = loop {
        match session
            .read_structural_evidence(structural, limit)
            .map_err(|error| {
                test_failure(format!(
                    "read stage structure at position {}: {error:?}",
                    structural.position()
                ))
            })? {
            StructuralEvidencePage::Page { findings, next, .. } => {
                if !findings.is_empty() {
                    return Err(test_failure(format!(
                        "published successor had structural findings at {}: {findings:?}",
                        structural.position()
                    )));
                }
                structural = next;
            }
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };
    let validation = validate_catalog_history(&mut session)
        .map_err(|error| test_failure(format!("validate stage catalog: {error:?}")))?;
    let (outcome, historical_end) = validation.into_parts();
    let CatalogHistoryOutcome::Ready(history) = outcome else {
        return Err(test_failure("published successor catalog was not ready"));
    };
    if !matches!(
        session
            .finish(structural_end, historical_end)
            .map_err(|error| test_failure(format!("finish stage evidence: {error:?}")))?,
        StructuralOpenOutcome::Clean(_)
    ) {
        return Err(test_failure(
            "published successor required a format migration",
        ));
    }
    Ok(history)
}

fn startup_inputs() -> TestResult<StartupValidationInputs> {
    let capability = ReadableDigestKey::v1(
        DigestKeyId::new(7).ok_or_else(|| test_failure("capability key is zero"))?,
    );
    let idempotency = ReadableDigestKey::v1(
        DigestKeyId::new(9).ok_or_else(|| test_failure("idempotency key is zero"))?,
    );
    Ok(StartupValidationInputs::new(
        fixture_time()?,
        ReadableCapabilityDigestInventory::new(vec![capability])?,
        ReadableIdempotencyDigestInventory::new(vec![idempotency])?,
    ))
}

fn principal() -> TestResult<AuditPrincipalV1> {
    Ok(AuditPrincipalV1::new(
        ActorId::new("wp408-operator")?,
        ActorKind::Human,
        capability_id()?,
        NonZeroU64::MIN,
    ))
}

fn deterministic_database_id() -> TestResult<DatabaseId> {
    Ok(DatabaseId::from_unix_milliseconds_and_random(
        1_785_000_000_000,
        [0x41; 10],
    )?)
}

fn deterministic_sibling_database_id() -> TestResult<DatabaseId> {
    Ok(DatabaseId::from_unix_milliseconds_and_random(
        1_785_000_000_000,
        [0x51; 10],
    )?)
}

#[allow(clippy::too_many_arguments)]
fn write_multi_database_config(
    root: &Path,
    selected: &Path,
    selected_backup: &Path,
    sibling: &Path,
    sibling_backup: &Path,
    capability_keys: &Path,
    idempotency_keys: &Path,
) -> TestResult<PathBuf> {
    let document = format!(
        "[server]\n\
         grpc_listen = \"127.0.0.1:0\"\n\
         audience = {audience:?}\n\
         capability_keys = {capability_keys:?}\n\
         idempotency_keys = {idempotency_keys:?}\n\
         \n\
         [databases.selected]\n\
         path = {selected:?}\n\
         backup_root = {selected_backup:?}\n\
         environment = {environment:?}\n\
         \n\
         [databases.sibling]\n\
         path = {sibling:?}\n\
         backup_root = {sibling_backup:?}\n\
         environment = {environment:?}\n",
        audience = AUDIENCE,
        capability_keys = capability_keys
            .to_str()
            .ok_or_else(|| test_failure("capability key path is not UTF-8"))?,
        idempotency_keys = idempotency_keys
            .to_str()
            .ok_or_else(|| test_failure("idempotency key path is not UTF-8"))?,
        selected = selected
            .to_str()
            .ok_or_else(|| test_failure("selected database path is not UTF-8"))?,
        selected_backup = selected_backup
            .to_str()
            .ok_or_else(|| test_failure("selected backup path is not UTF-8"))?,
        sibling = sibling
            .to_str()
            .ok_or_else(|| test_failure("sibling database path is not UTF-8"))?,
        sibling_backup = sibling_backup
            .to_str()
            .ok_or_else(|| test_failure("sibling backup path is not UTF-8"))?,
        environment = ENVIRONMENT,
    );
    let path = root.join("multi-database.toml");
    fs::write(&path, document)?;
    Ok(path)
}

fn capability_id() -> TestResult<CapabilityId> {
    Ok(CapabilityId::from_unix_milliseconds_and_random(
        1_785_000_000_001,
        [0x42; 10],
    )?)
}

fn request_id(seed: u8) -> TestResult<RequestId> {
    Ok(RequestId::from_unix_milliseconds_and_random(
        1_785_000_000_000 + u64::from(seed),
        [seed; 10],
    )?)
}

fn fixture_time() -> TestResult<Timestamp> {
    Ok(Timestamp::new(1_785_000_000, 0)?)
}

fn hex_32(value: &str) -> TestResult<[u8; 32]> {
    if value.len() != 64 {
        return Err(test_failure("fixture checksum length is not SHA-256"));
    }
    let mut decoded = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(pair)?;
        decoded[index] = u8::from_str_radix(text, 16)?;
    }
    Ok(decoded)
}

fn write_protected_file(path: &Path, contents: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(contents)?;
    file.sync_all()?;
    drop(file);
    File::open(
        path.parent()
            .ok_or_else(|| io::Error::other("protected file has no parent"))?,
    )?
    .sync_all()
}

fn test_failure(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}

/// Whole-directory scope backed by the canonical testkit guard: removed on
/// `Drop` — pass, fail, or panic — with a dead-pid sweep for directories
/// orphaned by a killed harness. `RIFFDB_WP408_FIXTURE_ROOT` still selects
/// the root and `RIFFDB_WP408_KEEP_FIXTURE` still retains the directory.
struct TemporaryDirectory(riffdb_testkit::scratch::ScratchDir);

impl TemporaryDirectory {
    fn new() -> io::Result<Self> {
        let root = std::env::var_os("RIFFDB_WP408_FIXTURE_ROOT")
            .map_or_else(std::env::temp_dir, PathBuf::from);
        riffdb_testkit::scratch::ScratchDir::new_in(root, "wp408-recovery").map(Self)
    }

    fn path(&self) -> &Path {
        self.0.path()
    }
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        if std::env::var_os("RIFFDB_WP408_KEEP_FIXTURE").is_some() {
            // Disarm the inner guard so the fixture survives; keep() renames
            // the directory out of sweep scope, so log the path afterwards.
            self.0.keep();
            eprintln!("retained WP-408 fixture at {}", self.0.path().display());
        }
    }
}
