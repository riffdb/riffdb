#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]

//! Integrated WP-190 public command uncertainty and durable recovery matrix.

use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::str;
use std::time::Duration;

use riffdb_auth::bootstrap_secret::{
    BootstrapCredential as RetainedBootstrapCredential, SystemEntropy,
    generate_bootstrap_credential, load_bootstrap_credential_file,
};
use riffdb_client_rust::generated::GeneratedCommand;
use riffdb_client_rust::generated::legal_spend::{
    AllocateBudget, AllocateBudgetOutcome, Amount, CONTRACT_LINEAGE, CONTRACT_VERSION,
    CreateBudget, CreateBudgetOutcome,
};
use riffdb_client_rust::{
    AttemptBudget, BackupNameV1, BearerCredential, BootstrapCallMetadata,
    BootstrapCredential as TransportBootstrapCredential, CallMetadata, ClientError,
    CreateOfflineBackup, OfflineMaintenanceOperationId, OfflineMaintenanceReplacementConfirmation,
    RestoreOfflineBackup, RiffDbClient, generate_offline_maintenance_operation_id,
    generate_request_id, v1,
};
use riffdb_storage_api::{
    ApplicationSequenceAllocator, EntityTarget, OutboxStatusReadResultV1, ProjectionLifecycleV1,
    ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
    StartupValidationInputs,
};
use riffdb_testkit::failpoint::{render_wp190_recovery_report_v1, verify_wp190_recovery_report_v1};
use riffdb_testkit::http2_gate::Http2ResponseGate;
use riffdb_testkit::inspection::{DurableInspection, DurableInspectionRequest, inspect_redb};
use riffdb_testkit::model::budget_projection_schema;
use riffdb_testkit::process::{ChildProcessController, ChildProcessSpec};
use riffdb_testkit::scratch::ScratchDir;
use riffdb_types::{
    CommitSequence, DatabaseAlias, DigestKeyId, EntityKey, EntityKeyBuilder, EntityTypeId, EventId,
    FrontierPosition, RequestId, Timestamp,
};
use tokio::time::timeout;
use tonic::transport::Endpoint;

const CHILD_MODE: &str = "RIFFDB_WP190_CHILD_MODE";
const CHILD_RIFFDBD: &str = "riffdbd";
const CHILD_CLI_CREDENTIAL: &str = "RIFFDB_WP190_CLI_CREDENTIAL";
const CHILD_CLI_RPC_MARKER: &str = "RIFFDB_WP190_CLI_RPC_MARKER";
const AUDIENCE: &str = "riffdb-grpc-loopback";
const ENVIRONMENT: &str = "wp190-command-recovery";
const READY_PREFIX: &str = "riffdbd-ready-v1\t";
const SHUTDOWN_COMMAND: &[u8] = b"shutdown\n";
const PROCESS_START_TIMEOUT: Duration = Duration::from_secs(30);
const PROCESS_STOP_TIMEOUT: Duration = Duration::from_secs(20);
const PROCESS_KILL_TIMEOUT: Duration = Duration::from_secs(10);
const GATE_TIMEOUT: Duration = Duration::from_secs(20);
const RPC_TIMEOUT: Duration = Duration::from_secs(20);
const PROJECTION_WAIT_NANOS: u64 = 10_000_000_000;
const ABORT_SIGNAL: i32 = 6;
const BOOTSTRAP_UNIX_MILLISECONDS: u64 = 1_700_000_000_000;
const CAPABILITY_LIFETIME_SECONDS: u32 = 3_600;
const FISCAL_YEAR: i64 = 2027;
const ORGANIZATION_ID: [u8; 16] = [0x51; 16];
const MATTER_ID: [u8; 16] = [0x62; 16];
const APPROVED_MINOR_UNITS: i128 = 10_000;
const ALLOCATED_MINOR_UNITS: i128 = 2_500;

const CAPABILITY_KEY_DOCUMENT: &[u8] = b"riffdb-capability-digest-keys-v1\n7:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";
const IDEMPOTENCY_KEY_DOCUMENT: &[u8] = b"riffdb-idempotency-digest-keys-v1\n9:202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f\n";
const BUDGET_CONTRACT: &str = include_str!("../../contracts/examples/budget.riff");

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone, Copy, Debug)]
enum ResponseLoss {
    IncompleteFrame,
    KillRiffdbd,
}

#[derive(Clone, Copy, Debug)]
enum BootstrapRetentionCrash {
    CredentialFileSynced,
    ParentDirectorySynced,
}

impl BootstrapRetentionCrash {
    const fn mode(self) -> &'static str {
        match self {
            Self::CredentialFileSynced => "cli-after-credential-file-sync",
            Self::ParentDirectorySynced => "cli-after-parent-directory-sync",
        }
    }

    const fn point(self) -> riffdb_cli::test_fixtures::BootstrapRetentionTestPoint {
        match self {
            Self::CredentialFileSynced => {
                riffdb_cli::test_fixtures::BootstrapRetentionTestPoint::CredentialFileSynced
            }
            Self::ParentDirectorySynced => {
                riffdb_cli::test_fixtures::BootstrapRetentionTestPoint::ParentDirectorySynced
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum MaintenanceDaemonCrash {
    DrainComplete,
    DatabaseClosed,
    StagedAuthorizationComplete,
    FreshValidationComplete,
}

impl MaintenanceDaemonCrash {
    const fn mode(self) -> &'static str {
        match self {
            Self::DrainComplete => "maintenance-after-drain",
            Self::DatabaseClosed => "maintenance-after-database-close",
            Self::StagedAuthorizationComplete => "maintenance-after-staged-authorization",
            Self::FreshValidationComplete => "maintenance-after-fresh-validation",
        }
    }

    const fn point(self) -> riffdb_server::test_fixtures::MaintenanceRecoveryTestPoint {
        match self {
            Self::DrainComplete => {
                riffdb_server::test_fixtures::MaintenanceRecoveryTestPoint::DrainComplete
            }
            Self::DatabaseClosed => {
                riffdb_server::test_fixtures::MaintenanceRecoveryTestPoint::DatabaseClosed
            }
            Self::StagedAuthorizationComplete => {
                riffdb_server::test_fixtures::MaintenanceRecoveryTestPoint::StagedAuthorizationComplete
            }
            Self::FreshValidationComplete => {
                riffdb_server::test_fixtures::MaintenanceRecoveryTestPoint::FreshValidationComplete
            }
        }
    }
}

fn main() -> ExitCode {
    if let Ok(mode) = std::env::var(CHILD_MODE) {
        if mode == CHILD_RIFFDBD {
            return riffdb_server::riffdbd_main();
        }
        for crash in [
            BootstrapRetentionCrash::CredentialFileSynced,
            BootstrapRetentionCrash::ParentDirectorySynced,
        ] {
            if mode == crash.mode() {
                return bootstrap_retention_child(crash);
            }
        }
        for crash in [
            MaintenanceDaemonCrash::DrainComplete,
            MaintenanceDaemonCrash::DatabaseClosed,
            MaintenanceDaemonCrash::StagedAuthorizationComplete,
            MaintenanceDaemonCrash::FreshValidationComplete,
        ] {
            if mode == crash.mode() {
                return riffdb_server::test_fixtures::riffdbd_main_with_maintenance_abort(
                    crash.point(),
                );
            }
        }
    }
    if std::env::args().any(|argument| argument == "--write-report-fixture") {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/recovery/fixtures/wp190-report-v1.json");
        return match fs::write(path, render_wp190_recovery_report_v1()) {
            Ok(()) => ExitCode::SUCCESS,
            Err(_) => ExitCode::FAILURE,
        };
    }
    let multi_database_only =
        std::env::args().any(|argument| argument == "--multi-database-recovery-only");
    if !multi_database_only && !std::env::args().any(|argument| argument == "--ignored") {
        println!("full_recovery_matrix: 9 ignored");
        return ExitCode::SUCCESS;
    }

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => {
            eprintln!("full_recovery_matrix: runtime construction failed");
            return ExitCode::FAILURE;
        }
    };
    let result = if multi_database_only {
        runtime.block_on(run_multi_database_staged_authorization_case())
    } else {
        runtime.block_on(run_matrix())
    };
    match result {
        Ok(()) => {
            if multi_database_only {
                println!("full_recovery_matrix: multi-database recovery passed");
            } else {
                println!("full_recovery_matrix: 9 passed");
            }
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("full_recovery_matrix failed: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run_matrix() -> TestResult<()> {
    verify_wp190_recovery_report_v1()
        .map_err(|_| test_failure("checked WP-190 recovery report is invalid"))?;
    run_response_loss_case(ResponseLoss::IncompleteFrame).await?;
    run_response_loss_case(ResponseLoss::KillRiffdbd).await?;
    run_bootstrap_retention_case(BootstrapRetentionCrash::CredentialFileSynced).await?;
    run_bootstrap_retention_case(BootstrapRetentionCrash::ParentDirectorySynced).await?;
    run_maintenance_daemon_case(MaintenanceDaemonCrash::DrainComplete).await?;
    run_maintenance_daemon_case(MaintenanceDaemonCrash::DatabaseClosed).await?;
    run_maintenance_daemon_case(MaintenanceDaemonCrash::StagedAuthorizationComplete).await?;
    run_maintenance_daemon_case(MaintenanceDaemonCrash::FreshValidationComplete).await?;
    run_multi_database_staged_authorization_case().await
}

fn bootstrap_retention_child(crash: BootstrapRetentionCrash) -> ExitCode {
    let Some(credential) = std::env::var_os(CHILD_CLI_CREDENTIAL).map(PathBuf::from) else {
        return ExitCode::FAILURE;
    };
    let Some(rpc_marker) = std::env::var_os(CHILD_CLI_RPC_MARKER).map(PathBuf::from) else {
        return ExitCode::FAILURE;
    };
    if credential.as_os_str().is_empty()
        || credential.as_os_str().as_encoded_bytes().len() > 4_096
        || rpc_marker.as_os_str().is_empty()
        || rpc_marker.as_os_str().as_encoded_bytes().len() > 4_096
    {
        return ExitCode::FAILURE;
    }
    if riffdb_cli::test_fixtures::run_bootstrap_retention_fixture(&credential, crash.point())
        .is_err()
    {
        return ExitCode::FAILURE;
    }

    // Reaching this write means retention returned and a caller could proceed
    // toward transport. An armed fixture must abort before this point.
    let _ = fs::write(rpc_marker, b"rpc-became-reachable");
    ExitCode::FAILURE
}

async fn run_bootstrap_retention_case(crash: BootstrapRetentionCrash) -> TestResult<()> {
    let fixture = ProcessFixture::new(crash.mode())?;
    fs::remove_file(&fixture.bootstrap)?;
    let rpc_marker = fixture.bootstrap.with_extension("rpc-marker");
    let specification = ChildProcessSpec::new(std::env::current_exe()?)?
        .env(CHILD_MODE, crash.mode())?
        .env(CHILD_CLI_CREDENTIAL, fixture.bootstrap.as_os_str())?
        .env(CHILD_CLI_RPC_MARKER, rpc_marker.as_os_str())?;
    let mut child = ChildProcessController::spawn(&specification)?;
    let exit = child.wait_for_exit(PROCESS_STOP_TIMEOUT)?;
    if exit.status.signal() != Some(ABORT_SIGNAL) {
        return Err(test_failure(format!(
            "bootstrap retention child exited with {:?}, expected SIGABRT",
            exit.status
        )));
    }
    if rpc_marker.exists() {
        return Err(test_failure(
            "bootstrap RPC became reachable before crash-durable credential retention",
        ));
    }

    let retained = fixture.retained_bootstrap()?;
    let authenticated = CallMetadata::authenticated(bearer_credential(&retained)?);
    let mut process = fixture.spawn_riffdbd()?;
    let address =
        parse_ready_address(&process.wait_for_readiness(READY_PREFIX, PROCESS_START_TIMEOUT)?)?;
    let mut client = connect(address).await?;
    bootstrap_and_deploy(&mut client, &retained, &authenticated).await?;
    fs::write(&rpc_marker, b"public-bootstrap-completed-after-restart")?;
    if !rpc_marker.exists() {
        return Err(test_failure(
            "restart did not reach the public bootstrap boundary",
        ));
    }
    drop(client);
    process.shutdown_cleanly(SHUTDOWN_COMMAND, PROCESS_STOP_TIMEOUT)?;
    Ok(())
}

async fn run_maintenance_daemon_case(crash: MaintenanceDaemonCrash) -> TestResult<()> {
    let fixture = ProcessFixture::new(crash.mode())?;
    let retained = fixture.retained_bootstrap()?;
    let authenticated = CallMetadata::authenticated(bearer_credential(&retained)?);

    let mut seed_process = fixture.spawn_riffdbd()?;
    let seed_address = parse_ready_address(
        &seed_process.wait_for_readiness(READY_PREFIX, PROCESS_START_TIMEOUT)?,
    )?;
    let mut seed_client = connect(seed_address).await?;
    bootstrap_and_deploy(&mut seed_client, &retained, &authenticated).await?;

    let restore_source = BackupNameV1::new("wp190-daemon-source")?;
    if matches!(crash, MaintenanceDaemonCrash::StagedAuthorizationComplete) {
        seed_client = create_backup_to_terminal(
            &mut seed_process,
            seed_client,
            restore_source.clone(),
            &authenticated,
        )
        .await?;
    }
    drop(seed_client);
    seed_process.shutdown_cleanly(SHUTDOWN_COMMAND, PROCESS_STOP_TIMEOUT)?;

    let mut armed_process = fixture.spawn_riffdbd_with_mode(crash.mode(), "127.0.0.1:0")?;
    let armed_address = parse_ready_address(
        &armed_process.wait_for_readiness(READY_PREFIX, PROCESS_START_TIMEOUT)?,
    )?;
    let mut armed_client = connect(armed_address).await?;
    let operation_id = generate_offline_maintenance_operation_id()?;

    if matches!(crash, MaintenanceDaemonCrash::StagedAuthorizationComplete) {
        let restore = RestoreOfflineBackup::new(
            operation_id,
            restore_source.clone(),
            OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget,
        );
        let started = bounded_rpc(
            "armed public RestoreOfflineBackup",
            armed_client.restore_offline_backup_with_retry(&restore, one_attempt(), &authenticated),
        )
        .await?;
        assert_restore_maintenance_accepted(&started, operation_id)?;
    } else {
        let name = BackupNameV1::new(match crash {
            MaintenanceDaemonCrash::DrainComplete => "wp190-after-drain",
            MaintenanceDaemonCrash::DatabaseClosed => "wp190-after-close",
            MaintenanceDaemonCrash::FreshValidationComplete => "wp190-after-fresh-validation",
            MaintenanceDaemonCrash::StagedAuthorizationComplete => unreachable!(),
        })?;
        let create = CreateOfflineBackup::new(operation_id, name);
        let started = bounded_rpc(
            "armed public CreateOfflineBackup",
            armed_client.create_offline_backup_with_retry(&create, one_attempt(), &authenticated),
        )
        .await?;
        assert_maintenance_accepted(&started, operation_id)?;
    }
    drop(armed_client);
    let aborted = armed_process.wait_for_exit(PROCESS_START_TIMEOUT)?;
    if aborted.status.signal() != Some(ABORT_SIGNAL) {
        return Err(test_failure(format!(
            "maintenance daemon child exited with {:?}, expected SIGABRT",
            aborted.status
        )));
    }

    if matches!(crash, MaintenanceDaemonCrash::StagedAuthorizationComplete) {
        let mut restarted =
            fixture.spawn_riffdbd_with_mode(CHILD_RIFFDBD, &armed_address.to_string())?;
        let mut retry_client = connect_eventually(armed_address).await?;
        let terminal = restore_eventually(
            &mut retry_client,
            operation_id,
            &restore_source,
            &authenticated,
        )
        .await?;
        let Some(retry_operation) = terminal.operation.as_ref() else {
            return Err(test_failure(
                "staged-authorization restart omitted the same-operation receipt",
            ));
        };
        if retry_operation.operation_id != operation_id.into_bytes()
            || !matches!(
                v1::OfflineMaintenanceStartDisposition::try_from(terminal.disposition),
                Ok(v1::OfflineMaintenanceStartDisposition::AlreadyAccepted
                    | v1::OfflineMaintenanceStartDisposition::Terminal)
            )
        {
            return Err(test_failure(
                "staged-authorization restart did not resolve the same operation",
            ));
        }
        let rebound = parse_ready_address(
            &restarted.wait_for_readiness(READY_PREFIX, PROCESS_START_TIMEOUT)?,
        )?;
        if rebound != armed_address {
            return Err(test_failure(
                "staged-authorization recovery changed the listener address",
            ));
        }
        let mut ready_client = connect(rebound).await?;
        poll_terminal_maintenance(&mut ready_client, operation_id, &authenticated).await?;
        drop(ready_client);
        drop(retry_client);
        restarted.shutdown_cleanly(SHUTDOWN_COMMAND, PROCESS_STOP_TIMEOUT)?;
    } else {
        let mut restarted = fixture.spawn_riffdbd()?;
        let address = parse_ready_address(
            &restarted.wait_for_readiness(READY_PREFIX, PROCESS_START_TIMEOUT)?,
        )?;
        let mut ready_client = connect(address).await?;
        poll_terminal_maintenance(&mut ready_client, operation_id, &authenticated).await?;
        drop(ready_client);
        restarted.shutdown_cleanly(SHUTDOWN_COMMAND, PROCESS_STOP_TIMEOUT)?;
    }
    Ok(())
}

async fn run_multi_database_staged_authorization_case() -> TestResult<()> {
    let fixture = ProcessFixture::new("multi-staged-authorization")?;
    let retained = fixture.retained_bootstrap()?;
    let selected = DatabaseAlias::new("default")?;
    let authenticated =
        CallMetadata::authenticated(bearer_credential(&retained)?).with_database(selected.clone());

    let mut seed_process = fixture.spawn_multi_riffdbd_with_mode(CHILD_RIFFDBD, "127.0.0.1:0")?;
    let address = parse_ready_address(
        &seed_process
            .wait_for_readiness(READY_PREFIX, PROCESS_START_TIMEOUT)
            .map_err(|error| test_failure(format!("multi-database seed readiness: {error}")))?,
    )?;
    let mut seed_client = connect(address).await?;
    bootstrap_and_deploy_selected(
        &mut seed_client,
        &retained,
        &authenticated,
        selected.clone(),
    )
    .await?;

    let backup_name = BackupNameV1::new("wp385-multi-restore-source")?;
    let backup = CreateOfflineBackup::new(
        generate_offline_maintenance_operation_id()?,
        backup_name.clone(),
    );
    let started = bounded_rpc(
        "multi-database source backup",
        seed_client.create_offline_backup_with_retry(&backup, one_attempt(), &authenticated),
    )
    .await?;
    assert_maintenance_accepted(&started, backup.operation_id())?;
    drop(seed_client);
    let mut seed_client = connect_eventually(address).await?;
    poll_terminal_maintenance_eventually(&mut seed_client, backup.operation_id(), &authenticated)
        .await?;
    drop(seed_client);
    seed_process.shutdown_cleanly(SHUTDOWN_COMMAND, PROCESS_STOP_TIMEOUT)?;

    let mut armed = fixture.spawn_multi_riffdbd_with_mode(
        MaintenanceDaemonCrash::StagedAuthorizationComplete.mode(),
        &address.to_string(),
    )?;
    let rebound = parse_ready_address(
        &armed
            .wait_for_readiness(READY_PREFIX, PROCESS_START_TIMEOUT)
            .map_err(|error| test_failure(format!("multi-database armed readiness: {error}")))?,
    )?;
    if rebound != address {
        return Err(test_failure(
            "multi-database armed restart changed the listener address",
        ));
    }
    let mut armed_client = connect(address).await?;
    let operation_id = generate_offline_maintenance_operation_id()?;
    let restore = RestoreOfflineBackup::new(
        operation_id,
        backup_name.clone(),
        OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget,
    );
    let started = bounded_rpc(
        "multi-database armed restore",
        armed_client.restore_offline_backup_with_retry(&restore, one_attempt(), &authenticated),
    )
    .await?;
    assert_restore_maintenance_accepted(&started, operation_id)?;
    drop(armed_client);
    let aborted = armed.wait_for_exit(PROCESS_START_TIMEOUT)?;
    if aborted.status.signal() != Some(ABORT_SIGNAL) {
        return Err(test_failure(format!(
            "multi-database maintenance child exited with {:?}, expected SIGABRT",
            aborted.status
        )));
    }

    let mut restarted =
        fixture.spawn_multi_riffdbd_with_mode(CHILD_RIFFDBD, &address.to_string())?;
    let mut retry_client = connect_eventually(address).await?;
    let staged = start_restore_eventually(
        &mut retry_client,
        operation_id,
        &backup_name,
        &authenticated,
    )
    .await?;
    let Some(operation) = staged.operation.as_ref() else {
        return Err(test_failure(
            "multi-database recovery omitted the same-operation receipt",
        ));
    };
    if operation.operation_id != operation_id.into_bytes()
        || staged.disposition != v1::OfflineMaintenanceStartDisposition::AlreadyAccepted as i32
        || operation.phase != v1::OfflineMaintenancePhase::Offline as i32
    {
        return Err(test_failure(format!(
            "multi-database recovery did not resume the exact restore: disposition={} phase={} failure={}",
            staged.disposition, operation.phase, operation.failure
        )));
    }
    let ready = parse_ready_address(
        &restarted
            .wait_for_readiness(READY_PREFIX, PROCESS_START_TIMEOUT)
            .map_err(|error| {
                test_failure(format!("multi-database recovered readiness: {error}"))
            })?,
    )?;
    if ready != address {
        return Err(test_failure(
            "multi-database recovery changed the listener address",
        ));
    }
    let terminal = restore_eventually(
        &mut retry_client,
        operation_id,
        &backup_name,
        &authenticated,
    )
    .await?;
    let Some(operation) = terminal.operation.as_ref() else {
        return Err(test_failure(
            "multi-database recovery omitted its terminal receipt",
        ));
    };
    if operation.operation_id != operation_id.into_bytes()
        || terminal.disposition != v1::OfflineMaintenanceStartDisposition::Terminal as i32
        || operation.phase != v1::OfflineMaintenancePhase::Succeeded as i32
        || operation.failure != v1::OfflineMaintenanceFailureClass::Unspecified as i32
    {
        return Err(test_failure(
            "multi-database recovery did not resolve terminal success",
        ));
    }
    poll_terminal_maintenance(&mut retry_client, operation_id, &authenticated).await?;
    drop(retry_client);
    restarted.shutdown_cleanly(SHUTDOWN_COMMAND, PROCESS_STOP_TIMEOUT)?;
    Ok(())
}

async fn create_backup_to_terminal(
    process: &mut ChildProcessController,
    mut client: RiffDbClient,
    backup_name: BackupNameV1,
    metadata: &CallMetadata,
) -> TestResult<RiffDbClient> {
    let operation =
        CreateOfflineBackup::new(generate_offline_maintenance_operation_id()?, backup_name);
    let started = bounded_rpc(
        "maintenance crash source backup",
        client.create_offline_backup_with_retry(&operation, one_attempt(), metadata),
    )
    .await?;
    assert_maintenance_accepted(&started, operation.operation_id())?;
    drop(client);
    let rebound =
        parse_ready_address(&process.wait_for_readiness(READY_PREFIX, PROCESS_START_TIMEOUT)?)?;
    let mut client = connect(rebound).await?;
    poll_terminal_maintenance(&mut client, operation.operation_id(), metadata).await?;
    Ok(client)
}

fn assert_maintenance_accepted(
    response: &v1::CreateOfflineBackupResponse,
    operation_id: OfflineMaintenanceOperationId,
) -> TestResult<()> {
    let operation = response
        .operation
        .as_ref()
        .ok_or_else(|| test_failure("maintenance start omitted its receipt"))?;
    if response.disposition != v1::OfflineMaintenanceStartDisposition::Accepted as i32
        || operation.operation_id != operation_id.into_bytes()
        || operation.phase != v1::OfflineMaintenancePhase::Draining as i32
    {
        return Err(test_failure(
            "maintenance start did not durably enter the draining phase",
        ));
    }
    Ok(())
}

fn assert_restore_maintenance_accepted(
    response: &v1::RestoreOfflineBackupResponse,
    operation_id: OfflineMaintenanceOperationId,
) -> TestResult<()> {
    let operation = response
        .operation
        .as_ref()
        .ok_or_else(|| test_failure("restore start omitted its receipt"))?;
    if response.disposition != v1::OfflineMaintenanceStartDisposition::Accepted as i32
        || operation.operation_id != operation_id.into_bytes()
        || operation.phase != v1::OfflineMaintenancePhase::Draining as i32
    {
        return Err(test_failure(
            "restore start did not durably enter the draining phase",
        ));
    }
    Ok(())
}

async fn poll_terminal_maintenance(
    client: &mut RiffDbClient,
    operation_id: OfflineMaintenanceOperationId,
    metadata: &CallMetadata,
) -> TestResult<()> {
    let response = bounded_rpc(
        "terminal maintenance observation",
        client.get_offline_maintenance_operation(
            v1::GetOfflineMaintenanceOperationRequest {
                request_id: fresh_request_id_bytes()?,
                operation_id: operation_id.into_bytes().to_vec(),
            },
            metadata,
        ),
    )
    .await?;
    let Some(v1::get_offline_maintenance_operation_response::Result::Found(operation)) =
        response.result
    else {
        return Err(test_failure(
            "restarted daemon did not expose the reconciled maintenance receipt",
        ));
    };
    if operation.phase != v1::OfflineMaintenancePhase::Succeeded as i32
        || operation.failure != v1::OfflineMaintenanceFailureClass::Unspecified as i32
    {
        return Err(test_failure(
            "restarted daemon invented or failed to complete maintenance success",
        ));
    }
    Ok(())
}

async fn poll_terminal_maintenance_eventually(
    client: &mut RiffDbClient,
    operation_id: OfflineMaintenanceOperationId,
    metadata: &CallMetadata,
) -> TestResult<()> {
    timeout(PROCESS_START_TIMEOUT, async {
        loop {
            let request = v1::GetOfflineMaintenanceOperationRequest {
                request_id: fresh_request_id_bytes()?,
                operation_id: operation_id.into_bytes().to_vec(),
            };
            if let Ok(response) = client
                .get_offline_maintenance_operation(request, metadata)
                .await
            {
                let Some(v1::get_offline_maintenance_operation_response::Result::Found(operation)) =
                    response.result
                else {
                    return Err(test_failure(
                        "multi-database maintenance receipt disappeared",
                    ));
                };
                if operation.phase == v1::OfflineMaintenancePhase::Succeeded as i32
                    && operation.failure == v1::OfflineMaintenanceFailureClass::Unspecified as i32
                {
                    return Ok(());
                }
                if matches!(
                    v1::OfflineMaintenancePhase::try_from(operation.phase),
                    Ok(v1::OfflineMaintenancePhase::FailedClosed)
                ) {
                    return Err(test_failure(
                        "multi-database maintenance reached terminal failure",
                    ));
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| test_failure("multi-database maintenance did not become observable"))?
}

async fn run_response_loss_case(loss: ResponseLoss) -> TestResult<()> {
    let fixture = ProcessFixture::new(match loss {
        ResponseLoss::IncompleteFrame => "partial-frame",
        ResponseLoss::KillRiffdbd => "kill-riffdbd",
    })?;
    let retained_bootstrap = fixture.retained_bootstrap()?;
    let authenticated = CallMetadata::authenticated(bearer_credential(&retained_bootstrap)?);
    let entity_key = budget_entity_key()?;
    let entity_target = budget_entity_target(&entity_key)?;
    let projection_identity = budget_projection_schema().identity().clone();
    let create = CreateBudget {
        idempotency_key: format!("wp190-create-{loss:?}"),
        organization_id: ORGANIZATION_ID,
        fiscal_year: FISCAL_YEAR,
        approved_amount: amount(APPROVED_MINOR_UNITS)?,
    };
    let allocate = AllocateBudget {
        idempotency_key: format!("wp190-allocate-{loss:?}"),
        organization_id: ORGANIZATION_ID,
        fiscal_year: FISCAL_YEAR,
        matter_id: MATTER_ID,
        amount: amount(ALLOCATED_MINOR_UNITS)?,
    };
    let immutable_command = allocate.idempotent_command()?;

    let mut process = fixture.spawn_riffdbd()?;
    let server_address =
        parse_ready_address(&process.wait_for_readiness(READY_PREFIX, PROCESS_START_TIMEOUT)?)?;
    let mut direct = connect(server_address).await?;
    bootstrap_and_deploy(&mut direct, &retained_bootstrap, &authenticated).await?;
    seed_create_budget(&mut direct, &create, &authenticated).await?;
    drop(direct);

    let mut gate = Http2ResponseGate::bind(server_address)?;
    let mut gated_client = connect(gate.address()).await?;
    gate.arm()?;
    let submitted_command = immutable_command.clone();
    let submitted_metadata = authenticated.clone();
    let first_attempt = tokio::spawn(async move {
        gated_client
            .execute_with_retry(&submitted_command, one_attempt(), &submitted_metadata)
            .await
    });
    let held = gate.wait_for_held_frame(GATE_TIMEOUT)?;
    if held.stream_id == 0 || !matches!(held.frame_type, 0 | 1) {
        return Err(test_failure(
            "the uncertainty gate did not hold an application response frame",
        ));
    }

    match loss {
        ResponseLoss::IncompleteFrame => {
            gate.release_incomplete_prefix(8)?;
        }
        ResponseLoss::KillRiffdbd => {
            let killed = process.kill(PROCESS_KILL_TIMEOUT)?;
            if killed.status.success() {
                return Err(test_failure(
                    "the synchronized riffdbd kill unexpectedly exited successfully",
                ));
            }
            gate.drop_complete_frame()?;
        }
    }
    gate.finish(GATE_TIMEOUT)
        .map_err(|error| test_failure(format!("HTTP/2 gate completion failed: {error:?}")))?;
    match first_attempt
        .await
        .map_err(|_| test_failure("gated public Execute task panicked"))?
    {
        Err(ClientError::OutcomeUnknown(_)) => {}
        Err(error) => {
            return Err(test_failure(format!(
                "lost public Execute returned the wrong checked failure: {error:?}"
            )));
        }
        Ok(_) => {
            return Err(test_failure(
                "lost public Execute unexpectedly received a complete response",
            ));
        }
    }

    let recovered_address = if matches!(loss, ResponseLoss::KillRiffdbd) {
        process = fixture.spawn_riffdbd()?;
        parse_ready_address(&process.wait_for_readiness(READY_PREFIX, PROCESS_START_TIMEOUT)?)?
    } else {
        server_address
    };
    let mut recovered = connect(recovered_address).await?;

    let persisted = found_outcome(
        bounded_rpc(
            "persisted outcome resolution",
            recovered.get_outcome(
                allocate.outcome_request(generate_request_id()?)?,
                &authenticated,
            ),
        )
        .await?,
    )?;
    assert_replayed_allocate(&persisted, &allocate)?;

    let replay = bounded_rpc(
        "same-command SDK replay",
        recovered.execute_with_retry(&immutable_command, one_attempt(), &authenticated),
    )
    .await?;
    assert_replayed_allocate(&replay, &allocate)?;
    if replay != persisted {
        return Err(test_failure(
            "same-command retry did not return the exact persisted public outcome",
        ));
    }

    let entity = found_entity(
        bounded_rpc(
            "entity after uncertain replay",
            recovered.get_entity(entity_request(&entity_key)?, &authenticated),
        )
        .await?,
    )?;
    if entity.entity_version != 2 {
        return Err(test_failure(
            "uncertain replay created zero or multiple authoritative mutations",
        ));
    }

    let commits = bounded_rpc(
        "commit scan after uncertain replay",
        recovered.scan_commits(commit_scan_request()?, &authenticated),
    )
    .await?;
    let commit_page = commits
        .page
        .as_ref()
        .ok_or_else(|| test_failure("commit scan omitted its page"))?;
    if commit_page.items.len() != 2
        || commit_page.next_cursor.is_some()
        || commit_page.items[0].commit_sequence != 1
        || commit_page.items[1].commit_sequence != 2
        || frontier_sequence(commit_page.observed_fence.as_ref()) != Some(2)
    {
        return Err(test_failure(
            "uncertain replay did not retain exactly two contiguous commits",
        ));
    }
    let allocation_commit = &commit_page.items[1];
    if allocation_commit.events.len() != 1
        || allocation_commit.provenance_uri != replay.provenance_uri
        || allocation_commit.plan_hash != replay.plan_hash
        || RequestId::from_bytes(
            allocation_commit
                .admission_request_id
                .clone()
                .try_into()
                .map_err(|_| test_failure("commit request ID width was invalid"))?,
        )
        .is_err()
    {
        return Err(test_failure(
            "allocation commit omitted stable request, event, plan, or provenance evidence",
        ));
    }

    let provenance = bounded_rpc(
        "provenance after uncertain replay",
        recovered.trace_provenance(provenance_request()?, &authenticated),
    )
    .await?;
    let Some(v1::trace_provenance_response::Result::Found(provenance)) = provenance.result else {
        return Err(test_failure(
            "uncertain replay provenance was not durably resolvable",
        ));
    };
    if provenance.commit_sequence != 2
        || provenance.admission_request_id != allocation_commit.admission_request_id
        || provenance.plan_hash != replay.plan_hash
        || provenance.event_ids
            != [v1::EventId {
                commit_sequence: 2,
                event_ordinal: 0,
            }]
    {
        return Err(test_failure(
            "uncertain replay changed admitted provenance or event identity",
        ));
    }

    let projection = bounded_rpc(
        "projection frontier after uncertain replay",
        recovered.query_projection(projection_request()?, &authenticated),
    )
    .await?;
    let Some(v1::query_projection_response::Result::Ready(ready)) = projection.result else {
        return Err(test_failure(
            "projection did not reach the uncertain commit frontier",
        ));
    };
    if frontier_sequence(ready.frontier.as_ref()) != Some(2) {
        return Err(test_failure(
            "projection frontier did not account for both commits",
        ));
    }

    let projection_status = bounded_rpc(
        "projection status after uncertain replay",
        recovered.get_projection_status(projection_status_request()?, &authenticated),
    )
    .await?;
    let Some(v1::get_projection_status_response::Result::Found(status)) = projection_status.result
    else {
        return Err(test_failure("projection status was not found"));
    };
    let published_frontier = status
        .published
        .as_ref()
        .and_then(|published| published.frontier.as_ref());
    if status.lifecycle != v1::ProjectionLifecycle::Ready as i32
        || frontier_sequence(published_frontier) != Some(2)
        || frontier_sequence(status.authoritative_head.as_ref()) != Some(2)
    {
        return Err(test_failure(
            "projection status did not publish the exact authoritative frontier",
        ));
    }

    let outbox = bounded_rpc(
        "outbox observation after uncertain replay",
        recovered.list_pending_outbox_deliveries(outbox_request()?, &authenticated),
    )
    .await?;
    let outbox_page = outbox
        .page
        .as_ref()
        .ok_or_else(|| test_failure("outbox observation omitted its page"))?;
    if outbox_page.next_cursor.is_some()
        || outbox_page.items.len() > 1
        || outbox_page.items.iter().any(|item| {
            item.event_id
                != Some(v1::EventId {
                    commit_sequence: 2,
                    event_ordinal: 0,
                })
        })
    {
        return Err(test_failure(
            "outbox observation contained a duplicate or foreign event",
        ));
    }

    drop(recovered);
    process.shutdown_cleanly(SHUTDOWN_COMMAND, PROCESS_STOP_TIMEOUT)?;

    let request = DurableInspectionRequest::new(vec![entity_target], vec![projection_identity])?;
    let first = inspect_redb(fixture.database_path(), startup_inputs()?, &request)?;
    let second = inspect_redb(fixture.database_path(), startup_inputs()?, &request)?;
    if first != second {
        return Err(test_failure(
            "repeated complete startup validation changed durable state",
        ));
    }
    assert_durable_graph(&first)?;
    Ok(())
}

async fn bootstrap_and_deploy(
    client: &mut RiffDbClient,
    credential: &RetainedBootstrapCredential,
    authenticated: &CallMetadata,
) -> TestResult<()> {
    bootstrap_and_deploy_with_metadata(
        client,
        credential,
        authenticated,
        bootstrap_metadata(credential)?,
    )
    .await
}

async fn bootstrap_and_deploy_selected(
    client: &mut RiffDbClient,
    credential: &RetainedBootstrapCredential,
    authenticated: &CallMetadata,
    database: DatabaseAlias,
) -> TestResult<()> {
    bootstrap_and_deploy_with_metadata(
        client,
        credential,
        authenticated,
        bootstrap_metadata(credential)?.with_database(database),
    )
    .await
}

async fn bootstrap_and_deploy_with_metadata(
    client: &mut RiffDbClient,
    credential: &RetainedBootstrapCredential,
    authenticated: &CallMetadata,
    bootstrap: BootstrapCallMetadata,
) -> TestResult<()> {
    let request = bootstrap_request(credential)?;
    riffdb_proto::validate_public_message(&request).map_err(|error| {
        test_failure(format!(
            "bootstrap request violated its public shape before transport: {error:?}"
        ))
    })?;
    let response = bounded_rpc(
        "bootstrap capability creation",
        client.create_bootstrap_capability(request, &bootstrap),
    )
    .await?;
    let Some(v1::create_capability_response::Result::Bootstrap(result)) = response.result else {
        return Err(test_failure("bootstrap response used the wrong family"));
    };
    if !matches!(
        result.result,
        Some(v1::bootstrap_create_capability_result::Result::Created(_))
    ) {
        return Err(test_failure("bootstrap capability was not created"));
    }

    let deployment = bounded_rpc(
        "budget contract deployment",
        client.deploy_contract(
            v1::DeployContractRequest {
                request_id: fresh_request_id_bytes()?,
                source: BUDGET_CONTRACT.to_owned(),
                expected_active_version: None,
                expected_active_bundle_hash: Vec::new(),
                expected_candidate_bundle_hash: Vec::new(),
            },
            authenticated,
        ),
    )
    .await?;
    let Some(v1::deploy_contract_response::Result::Activated(contract)) = deployment.result else {
        return Err(test_failure("budget contract was not activated"));
    };
    if contract.contract_lineage != CONTRACT_LINEAGE
        || contract.contract_version != CONTRACT_VERSION
    {
        return Err(test_failure("activated contract identity was unexpected"));
    }
    Ok(())
}

async fn seed_create_budget(
    client: &mut RiffDbClient,
    create: &CreateBudget,
    authenticated: &CallMetadata,
) -> TestResult<()> {
    let created = bounded_rpc(
        "CreateBudget seed",
        client.execute_generated(create, one_attempt(), authenticated),
    )
    .await?;
    if created.response().status != v1::execute_command_response::CompletionStatus::Committed as i32
        || created.response().commit_sequence != 1
    {
        return Err(test_failure("CreateBudget did not commit at sequence one"));
    }
    let CreateBudgetOutcome::BudgetCreated { budget } = created.outcome() else {
        return Err(test_failure("CreateBudget returned the wrong outcome"));
    };
    if budget.allocated_amount.minor_units() != 0 {
        return Err(test_failure("CreateBudget seed contained an allocation"));
    }
    Ok(())
}

fn assert_replayed_allocate(
    response: &v1::ExecuteCommandResponse,
    command: &AllocateBudget,
) -> TestResult<()> {
    if response.status != v1::execute_command_response::CompletionStatus::Replayed as i32
        || response.commit_sequence != 2
        || response.contract_version != CONTRACT_VERSION
        || response.durability_mode != "sync"
        || response.provenance_uri.is_empty()
        || response.outcome_uri.is_none()
    {
        return Err(test_failure(format!(
            "uncertain AllocateBudget did not resolve as the original replay: status={} sequence={} version={} durability={} provenance={} outcome_uri={}",
            response.status,
            response.commit_sequence,
            response.contract_version,
            response.durability_mode,
            !response.provenance_uri.is_empty(),
            response.outcome_uri.is_some(),
        )));
    }
    let AllocateBudgetOutcome::Allocated { budget, remaining } =
        command.decode_outcome(response)?
    else {
        return Err(test_failure(
            "uncertain AllocateBudget replay returned the wrong outcome",
        ));
    };
    if budget.allocated_amount.minor_units() != ALLOCATED_MINOR_UNITS
        || remaining.minor_units() != APPROVED_MINOR_UNITS - ALLOCATED_MINOR_UNITS
    {
        return Err(test_failure(
            "uncertain AllocateBudget replay changed business state",
        ));
    }
    Ok(())
}

fn assert_durable_graph(inspection: &DurableInspection) -> TestResult<()> {
    if !inspection.structural_findings().is_empty()
        || inspection.commits().len() != 2
        || inspection.provenance().len() != 2
        || inspection.events().len() != 1
        || inspection.entities().len() != 1
        || inspection.projections().len() != 1
    {
        return Err(test_failure(
            "offline inspection did not recover one complete authoritative graph",
        ));
    }
    if inspection.metadata().application_sequence()
        != ApplicationSequenceAllocator::next(
            CommitSequence::new(3).expect("sequence three is nonzero"),
        )
    {
        return Err(test_failure(
            "uncertain replay consumed an additional application sequence",
        ));
    }
    if inspection.commits()[0].commit_sequence() != CommitSequence::first()
        || inspection.commits()[1].commit_sequence()
            != CommitSequence::new(2).expect("sequence two")
        || !inspection.commits()[0].events().is_empty()
        || inspection.commits()[1].event_ids()
            != [EventId::new(
                CommitSequence::new(2).expect("sequence two"),
                0,
            )]
    {
        return Err(test_failure(
            "offline commit graph was duplicated or noncontiguous",
        ));
    }
    let entity = inspection.entities()[0]
        .record()
        .ok_or_else(|| test_failure("offline entity was absent"))?;
    if entity.entity_version().get() != 2 {
        return Err(test_failure(
            "offline entity did not retain exactly two versions",
        ));
    }
    let event = inspection.events()[0].event();
    if event.event_id() != EventId::new(CommitSequence::new(2).expect("sequence two"), 0)
        || matches!(
            inspection.events()[0].outbox(),
            OutboxStatusReadResultV1::AuthoritativeIntentMissing
        )
    {
        return Err(test_failure(
            "offline event/outbox reciprocity was not preserved",
        ));
    }
    let projection = &inspection.projections()[0];
    if projection.lifecycle() != ProjectionLifecycleV1::Ready
        || projection.authoritative_head()
            != FrontierPosition::AppliedThrough(CommitSequence::new(2).expect("sequence two"))
        || projection.published().map(|value| value.frontier())
            != Some(FrontierPosition::AppliedThrough(
                CommitSequence::new(2).expect("sequence two"),
            ))
    {
        return Err(test_failure(
            "offline projection frontier did not match the authoritative prefix",
        ));
    }
    if inspection.administration().is_empty() {
        return Err(test_failure(
            "public command recovery emitted no durable service audit",
        ));
    }
    Ok(())
}

fn bootstrap_request(
    credential: &RetainedBootstrapCredential,
) -> TestResult<v1::CreateCapabilityRequest> {
    use v1::capability_permission::Permission;

    let scoped = |stable_id| v1::LineageScopedStableId {
        contract_lineage: CONTRACT_LINEAGE.to_owned(),
        stable_id,
    };
    Ok(v1::CreateCapabilityRequest {
        request_id: fresh_request_id_bytes()?,
        mode: v1::CapabilityCreateMode::Bootstrap as i32,
        capability_id: credential.capability_id().into_bytes().to_vec(),
        principal_id: "wp190-maintainer".to_owned(),
        actor_kind: v1::ActorKind::Human as i32,
        requested_lifetime_seconds: CAPABILITY_LIFETIME_SECONDS,
        audiences: vec![AUDIENCE.to_owned()],
        grant: Some(v1::CapabilityGrant {
            tenant_scope: Some(v1::TenantScope {
                scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
            }),
            partition_scope: Some(v1::PartitionScope {
                scope: Some(v1::partition_scope::Scope::All(v1::Unit {})),
            }),
            permissions: vec![
                permission(Permission::DeployContract(v1::Unit {})),
                permission(Permission::InvokeCommand(scoped(1))),
                permission(Permission::InvokeCommand(scoped(2))),
                permission(Permission::ReadEntity(scoped(1))),
                permission(Permission::QueryProjection(scoped(1))),
                permission(Permission::ReadProjectionStatus(scoped(1))),
                permission(Permission::ReadCommit(v1::Unit {})),
                permission(Permission::ScanCommits(v1::Unit {})),
                permission(Permission::ReadProvenance(v1::Unit {})),
                permission(Permission::InspectOutbox(v1::Unit {})),
                permission(Permission::ReadHealth(v1::Unit {})),
                permission(Permission::AdministerCapabilities(v1::Unit {})),
            ],
            field_visibility: vec![v1::EntityFieldVisibility {
                contract_lineage: CONTRACT_LINEAGE.to_owned(),
                entity_type_id: 1,
                field_ids: vec![1, 2, 3, 4, 5],
                secret_field_ids: Vec::new(),
            }],
            max_scan_rows: 100,
            approval_required: Vec::new(),
            row_policy: None,
            export: None,
        }),
    })
}

fn permission(permission: v1::capability_permission::Permission) -> v1::CapabilityPermission {
    v1::CapabilityPermission {
        permission: Some(permission),
    }
}

fn entity_request(entity_key: &[u8]) -> TestResult<v1::GetEntityRequest> {
    Ok(v1::GetEntityRequest {
        request_id: fresh_request_id_bytes()?,
        contract: Some(active_contract()),
        entity_type_id: 1,
        entity_key: entity_key.to_vec(),
        fields: Some(v1::FieldSelection {
            field_ids: vec![1, 3, 5],
        }),
    })
}

fn commit_scan_request() -> TestResult<v1::ScanCommitsRequest> {
    Ok(v1::ScanCommitsRequest {
        request_id: fresh_request_id_bytes()?,
        page: Some(v1::PageRequest {
            limit: Some(100),
            cursor: None,
        }),

        observed_history_incarnation: None,
    })
}

fn provenance_request() -> TestResult<v1::TraceProvenanceRequest> {
    Ok(v1::TraceProvenanceRequest {
        request_id: fresh_request_id_bytes()?,
        selector: Some(v1::ProvenanceSelection {
            selection: Some(v1::provenance_selection::Selection::CommitSequence(2)),
        }),
    })
}

fn projection_request() -> TestResult<v1::QueryProjectionRequest> {
    Ok(v1::QueryProjectionRequest {
        request_id: fresh_request_id_bytes()?,
        contract: Some(active_contract()),
        projection_id: 1,
        leading_components: Vec::new(),
        required_sequence: Some(2),
        wait_nanos: PROJECTION_WAIT_NANOS,
        page: Some(v1::PageRequest {
            limit: Some(100),
            cursor: None,
        }),
    })
}

fn projection_status_request() -> TestResult<v1::GetProjectionStatusRequest> {
    Ok(v1::GetProjectionStatusRequest {
        request_id: fresh_request_id_bytes()?,
        contract: Some(active_contract()),
        projection_id: 1,
    })
}

fn outbox_request() -> TestResult<v1::ListPendingOutboxDeliveriesRequest> {
    Ok(v1::ListPendingOutboxDeliveriesRequest {
        request_id: fresh_request_id_bytes()?,
        page: Some(v1::PageRequest {
            limit: Some(100),
            cursor: None,
        }),
    })
}

fn active_contract() -> v1::ContractSelection {
    v1::ContractSelection {
        selection: Some(v1::contract_selection::Selection::Active(v1::Unit {})),
    }
}

fn found_outcome(response: v1::GetOutcomeResponse) -> TestResult<v1::ExecuteCommandResponse> {
    match response.result {
        Some(v1::get_outcome_response::Result::Found(outcome)) => Ok(outcome),
        _ => Err(test_failure("durable outcome was not found")),
    }
}

fn found_entity(response: v1::GetEntityResponse) -> TestResult<v1::Entity> {
    match response.result {
        Some(v1::get_entity_response::Result::Found(entity)) => Ok(entity),
        _ => Err(test_failure("budget entity was not found")),
    }
}

fn frontier_sequence(frontier: Option<&v1::FrontierPosition>) -> Option<u64> {
    match frontier.and_then(|frontier| frontier.position.as_ref()) {
        Some(v1::frontier_position::Position::AppliedThrough(sequence)) => Some(*sequence),
        _ => None,
    }
}

fn budget_entity_key() -> TestResult<Vec<u8>> {
    let entity_type = EntityTypeId::new(1).expect("Budget entity ID");
    let mut key = EntityKeyBuilder::new(entity_type);
    key.push_uuid(&ORGANIZATION_ID)?;
    key.push_i64(FISCAL_YEAR)?;
    Ok(key.finish()?.into_bytes())
}

fn budget_entity_target(encoded: &[u8]) -> TestResult<EntityTarget> {
    let entity_type = EntityTypeId::new(1).expect("Budget entity ID");
    let key = EntityKey::from_bytes(encoded.to_vec())?;
    Ok(EntityTarget::new(entity_type, key)?)
}

fn amount(minor_units: i128) -> TestResult<Amount> {
    Amount::from_minor_units(minor_units)
        .ok_or_else(|| test_failure("test amount exceeded generated decimal bounds"))
}

fn one_attempt() -> AttemptBudget {
    AttemptBudget::new(1).expect("one is nonzero")
}

fn fresh_request_id_bytes() -> TestResult<Vec<u8>> {
    Ok(generate_request_id()?.into_bytes().to_vec())
}

fn startup_inputs() -> TestResult<StartupValidationInputs> {
    let capability = DigestKeyId::new(7).expect("capability key ID");
    let idempotency = DigestKeyId::new(9).expect("idempotency key ID");
    Ok(StartupValidationInputs::new(
        Timestamp::new(1_700_000_100, 0)?,
        ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(capability)])?,
        ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(idempotency)])?,
    ))
}

async fn connect(address: SocketAddr) -> TestResult<RiffDbClient> {
    let endpoint = Endpoint::from_shared(format!("http://{address}"))?
        .connect_timeout(RPC_TIMEOUT)
        .timeout(RPC_TIMEOUT);
    bounded_rpc("gRPC connection", RiffDbClient::connect(endpoint)).await
}

async fn connect_eventually(address: SocketAddr) -> TestResult<RiffDbClient> {
    timeout(PROCESS_START_TIMEOUT, async move {
        loop {
            match connect(address).await {
                Ok(client) => return Ok(client),
                Err(_) => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .map_err(|_| test_failure("maintenance recovery listener did not accept a connection"))?
}

async fn restore_eventually(
    client: &mut RiffDbClient,
    operation_id: OfflineMaintenanceOperationId,
    backup_name: &BackupNameV1,
    metadata: &CallMetadata,
) -> TestResult<v1::RestoreOfflineBackupResponse> {
    timeout(PROCESS_START_TIMEOUT, async {
        loop {
            let request = v1::RestoreOfflineBackupRequest {
                request_id: fresh_request_id_bytes()?,
                operation_id: operation_id.into_bytes().to_vec(),
                backup_name: backup_name.as_str().to_owned(),
                replacement_confirmation:
                    v1::OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget
                        as i32,
            };
            match client.restore_offline_backup(request, metadata).await {
                Ok(response)
                    if response.operation.as_ref().is_some_and(|operation| {
                        matches!(
                            v1::OfflineMaintenancePhase::try_from(operation.phase),
                            Ok(
                                v1::OfflineMaintenancePhase::Succeeded
                                    | v1::OfflineMaintenancePhase::FailedClosed
                            )
                        )
                    }) =>
                {
                    return Ok(response);
                }
                Ok(_) => tokio::task::yield_now().await,
                Err(_) => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .map_err(|_| test_failure("same-operation restore retry did not become terminal"))?
}

async fn start_restore_eventually(
    client: &mut RiffDbClient,
    operation_id: OfflineMaintenanceOperationId,
    backup_name: &BackupNameV1,
    metadata: &CallMetadata,
) -> TestResult<v1::RestoreOfflineBackupResponse> {
    timeout(PROCESS_START_TIMEOUT, async {
        loop {
            let request = v1::RestoreOfflineBackupRequest {
                request_id: fresh_request_id_bytes()?,
                operation_id: operation_id.into_bytes().to_vec(),
                backup_name: backup_name.as_str().to_owned(),
                replacement_confirmation:
                    v1::OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget
                        as i32,
            };
            match client.restore_offline_backup(request, metadata).await {
                Ok(response) => return Ok(response),
                Err(_) => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .map_err(|_| test_failure("same-operation restore retry was not admitted"))?
}

async fn bounded_rpc<T, E>(
    label: &'static str,
    future: impl Future<Output = Result<T, E>>,
) -> TestResult<T>
where
    E: std::fmt::Display,
{
    match timeout(RPC_TIMEOUT, future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(test_failure(format!("{label} failed: {error}"))),
        Err(_) => Err(test_failure(format!("{label} exceeded its deadline"))),
    }
}

fn bootstrap_metadata(
    credential: &RetainedBootstrapCredential,
) -> TestResult<BootstrapCallMetadata> {
    let transport = TransportBootstrapCredential::new(bootstrap_token_text(credential)?)?;
    Ok(BootstrapCallMetadata::new(transport))
}

fn bearer_credential(credential: &RetainedBootstrapCredential) -> TestResult<BearerCredential> {
    Ok(BearerCredential::new(bootstrap_token_text(credential)?)?)
}

fn bootstrap_token_text(credential: &RetainedBootstrapCredential) -> TestResult<&str> {
    Ok(str::from_utf8(credential.token().expose_secret())?)
}

fn parse_ready_address(line: &str) -> TestResult<SocketAddr> {
    let address = line
        .strip_prefix(READY_PREFIX)
        .ok_or_else(|| test_failure("riffdbd readiness prefix changed"))?
        .parse::<SocketAddr>()?;
    if !address.ip().is_loopback() || address.port() == 0 {
        return Err(test_failure(
            "riffdbd readiness address was not bound loopback",
        ));
    }
    Ok(address)
}

struct ProcessFixture {
    _directory: TemporaryDirectory,
    database: PathBuf,
    backup_root: PathBuf,
    second_database: PathBuf,
    second_backup_root: PathBuf,
    multi_config: PathBuf,
    capability_keys: PathBuf,
    idempotency_keys: PathBuf,
    bootstrap: PathBuf,
}

impl ProcessFixture {
    fn new(label: &str) -> TestResult<Self> {
        let directory = TemporaryDirectory::new(label)?;
        let database = directory.path().join("riffdb.redb");
        let backup_root = directory.path().join("backups");
        let second_database = directory.path().join("other.redb");
        let second_backup_root = directory.path().join("other-backups");
        let multi_config = directory.path().join("riffdb-multi.toml");
        let capability_keys = directory.path().join("capability.keys");
        let idempotency_keys = directory.path().join("idempotency.keys");
        let bootstrap = directory.path().join("bootstrap.credential");
        fs::create_dir(&backup_root)?;
        fs::create_dir(&second_backup_root)?;
        write_protected_file(&capability_keys, CAPABILITY_KEY_DOCUMENT)?;
        write_protected_file(&idempotency_keys, IDEMPOTENCY_KEY_DOCUMENT)?;
        let generated = generate_bootstrap_credential(BOOTSTRAP_UNIX_MILLISECONDS, &SystemEntropy)?;
        write_protected_file(&bootstrap, generated.render_document().expose_secret())?;
        drop(generated);
        Ok(Self {
            _directory: directory,
            database,
            backup_root,
            second_database,
            second_backup_root,
            multi_config,
            capability_keys,
            idempotency_keys,
            bootstrap,
        })
    }

    fn retained_bootstrap(&self) -> TestResult<RetainedBootstrapCredential> {
        Ok(load_bootstrap_credential_file(&self.bootstrap)?)
    }

    fn database_path(&self) -> &Path {
        &self.database
    }

    fn spawn_riffdbd(&self) -> TestResult<ChildProcessController> {
        self.spawn_riffdbd_with_mode(CHILD_RIFFDBD, "127.0.0.1:0")
    }

    fn spawn_riffdbd_with_mode(
        &self,
        mode: &str,
        listen_address: &str,
    ) -> TestResult<ChildProcessController> {
        let executable = std::env::current_exe()?;
        let specification = ChildProcessSpec::new(executable)?
            .env(CHILD_MODE, mode)?
            .arg("--database")?
            .arg(self.database.as_os_str())?
            .arg("--listen")?
            .arg(listen_address)?
            .arg("--environment")?
            .arg(ENVIRONMENT)?
            .arg("--audience")?
            .arg(AUDIENCE)?
            .arg("--backup-root")?
            .arg(self.backup_root.as_os_str())?
            .arg("--capability-keys")?
            .arg(self.capability_keys.as_os_str())?
            .arg("--idempotency-keys")?
            .arg(self.idempotency_keys.as_os_str())?;
        Ok(ChildProcessController::spawn(&specification)?)
    }

    fn spawn_multi_riffdbd_with_mode(
        &self,
        mode: &str,
        listen_address: &str,
    ) -> TestResult<ChildProcessController> {
        let document = format!(
            "[server]\n\
             grpc_listen = {listen_address:?}\n\
             audience = {audience:?}\n\
             capability_keys = {capability_keys:?}\n\
             idempotency_keys = {idempotency_keys:?}\n\
             \n\
             [databases.default]\n\
             path = {database:?}\n\
             backup_root = {backup_root:?}\n\
             environment = {environment:?}\n\
             \n\
             [databases.other]\n\
             path = {second_database:?}\n\
             backup_root = {second_backup_root:?}\n\
             environment = {environment:?}\n",
            audience = AUDIENCE,
            capability_keys = self.capability_keys,
            idempotency_keys = self.idempotency_keys,
            database = self.database,
            backup_root = self.backup_root,
            environment = ENVIRONMENT,
            second_database = self.second_database,
            second_backup_root = self.second_backup_root,
        );
        fs::write(&self.multi_config, document)?;
        let specification = ChildProcessSpec::new(std::env::current_exe()?)?
            .env(CHILD_MODE, mode)?
            .arg("--config")?
            .arg(self.multi_config.as_os_str())?;
        Ok(ChildProcessController::spawn(&specification)?)
    }
}

fn write_protected_file(path: &Path, document: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(document)?;
    file.sync_all()?;
    drop(file);
    File::open(
        path.parent()
            .ok_or_else(|| io::Error::other("protected path has no parent"))?,
    )?
    .sync_all()
}

struct TemporaryDirectory {
    scratch: ScratchDir,
}

impl TemporaryDirectory {
    fn new(label: &str) -> io::Result<Self> {
        Ok(Self {
            scratch: ScratchDir::new(&format!("wp190-{label}"))?,
        })
    }

    fn path(&self) -> &Path {
        self.scratch.path()
    }
}

fn test_failure(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}
