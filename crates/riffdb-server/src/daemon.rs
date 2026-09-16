#![expect(
    clippy::expect_used,
    reason = "validated daemon startup retains its selected listener and refusal evidence"
)]

//! Hosted `riffdbd` process lifecycle for the runnable P1 checkpoint.

#[path = "daemon_follower.rs"]
mod follower;

#[path = "replication_peer.rs"]
pub(crate) mod replication_peer;

use std::error::Error;
use std::fmt;
use std::fs;
use std::future::Future;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::num::NonZeroU32;
#[cfg(unix)]
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use futures_util::stream::FuturesUnordered;
use riffdb_api_grpc::{GrpcApplication, GrpcDatabaseRoutes, GrpcLifecycleRoute, GrpcRequestLimits};
use riffdb_api_mcp::MCP_PROTOCOL_VERSION;
use riffdb_auth::{NoopAuthenticationTelemetry, load_digest_key_providers};
use riffdb_config::{
    ApplicationListenerConfig, DirectTlsListenerConfig, ListenerBounds, LocalSocketListenerConfig,
    LoopbackCleartextListener, TlsServerIdentity,
};
use riffdb_contract_ir::EXECUTABLE_IR_VERSION_V1;
use riffdb_policy::{
    AuthorizationClock, AuthorizationClockError, NoopAuthorizationTelemetry, TrustedAudienceCatalog,
};
use riffdb_service::{
    BuildInfo, OfflineMaintenanceStartDisposition, RecoveryOfflineMaintenancePortError,
    RestoreOfflineBackupRequest, RiffDbService,
};
use riffdb_storage_api::{
    ContractMigrationReceiptV1, OfflineMaintenanceReceiptFailureV1,
    OfflineMaintenanceReceiptPersistencePort, OfflineMaintenanceReceiptPhaseV1,
    OfflineMaintenanceReceiptV1, ReadableCapabilityDigestInventory, ReadableDigestKey,
    ReadableIdempotencyDigestInventory, StartupValidationInputs, StorageError, StorageErrorKind,
    StorageValueError,
};
use riffdb_storage_redb::{
    RedbMaintenanceOperationEvidence, RedbMaintenanceReconciliation, RedbMaintenanceStorage,
};
use riffdb_types::{DatabaseAlias, OfflineMaintenanceOperationKind};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject as _};
use tokio::sync::futures::OwnedNotified;
use tokio::sync::oneshot;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::{JoinError, JoinHandle as TokioJoinHandle};
use tokio::{io::AsyncRead, io::AsyncWrite, io::ReadBuf};
use tokio_rustls::TlsAcceptor;
use tokio_stream::Stream;
#[cfg(unix)]
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::{
    Server,
    server::{Connected, TcpIncoming},
};
use zeroize::Zeroizing;

use crate::clocks::{ProductionWallClocks, ServerProcessClockError};
use crate::config::{DatabaseConfig, ServerConfig, ServerConfigError};
use crate::hosted_mcp::{HostedMcp, HostedMcpStartError, HostedMcpStopError};
use crate::identifiers::ProductionIdentifierSources;
use crate::lifecycle::ProductionLifecycleRoute;
use crate::maintenance_adapter::{
    MaintenanceController, MaintenanceTrigger, MigrationProcessExclusion,
    maintenance_trigger_channel, shared_maintenance_storage, start_result,
};
use crate::maintenance_driver::{
    MaintenanceDriverDependencies, MaintenanceDriverFailure, MaintenanceDriverRequest,
    MaintenanceDriverSuccess, RecoveryMaintenanceDriverRequest,
    contract_migration_backup_build_metadata, mark_draining, mark_offline,
    receipt_matches_restore_request, run_offline_maintenance, run_recovery_restore,
};
use crate::maintenance_lifecycle::MaintenanceLifecycle;
use crate::maintenance_migration::{MigrationDriverInputs, drive_contract_migration};
use crate::maintenance_recovery_controller::{
    MaintenanceRecoveryBoundary, MaintenanceRecoveryController,
};
use crate::process_graph::{
    ProductionDigestKeys, ProductionGraphBuildError, ProductionGraphBuilder,
    ProductionGraphShutdownError, ProductionGraphShutdownStageEvidence, RunningProductionGraph,
};
use crate::recovery_host::{
    RecoveryHostShutdownError, RecoveryHostStartError, RunningRecoveryHost,
};
use crate::restore_retry_host::{
    RestoreRetryHostShutdownError, RestoreRetryHostStartError, RunningRestoreRetryHost,
};
use crate::runtime_support::{RuntimeRoutingState, RuntimeStopReason};
use crate::startup::{RedbStartupError, open_redb_startup_with_commit_profile};

/// Bounded public-service scheduler width. Blocking storage ports retain their
/// separate eight-thread admission bound.
const RUNTIME_WORKER_THREADS: usize = 8;
const REQUEST_DURATION_LIMIT: Duration = Duration::from_secs(30);
const TRANSPORT_DRAIN_LIMIT: Duration = Duration::from_secs(35);
const RUNTIME_DRAIN_LIMIT: Duration = Duration::from_secs(5);
const SHUTDOWN_COMMAND: &[u8] = b"shutdown\n";
const READY_PROTOCOL: &str = "riffdbd-ready-v1";
const MAX_CERTIFICATE_CHAIN_BYTES: u64 = 256 * 1_024;
const MAX_PRIVATE_KEY_BYTES: u64 = 64 * 1_024;

type ShutdownReceiver = oneshot::Receiver<Result<ShutdownInput, ShutdownCommandError>>;

enum ReadyProcessTrigger {
    Command,
    ShutdownInputFailure,
    Signal(Result<(), io::Error>),
    Runtime(RuntimeStopReason),
    Transport(Result<Result<(), tonic::transport::Error>, JoinError>),
    McpTransport(Result<(), HostedMcpStopError>),
    Maintenance(MaintenanceTrigger),
}

enum ReadyProcessCompletion {
    Stopped,
    Maintenance(MaintenanceTrigger),
}

enum RecoveryProcessTrigger {
    Signal(Result<(), io::Error>),
    Runtime,
    Transport(Result<Result<(), tonic::transport::Error>, JoinError>),
    Maintenance(Option<MaintenanceTrigger>),
}

enum RestoreRetryProcessTrigger {
    Signal(Result<(), io::Error>),
    Runtime,
    Transport(Result<Result<(), tonic::transport::Error>, JoinError>),
    Maintenance(Option<MaintenanceTrigger>),
}

enum RecoveryAttempt {
    Succeeded(Box<MaintenanceDriverSuccess>),
    Retry(RecoveryOfflineMaintenancePortError),
    Terminal(riffdb_service::OfflineMaintenanceStartResult),
    Stop(RecoveryOfflineMaintenancePortError),
}

struct ReadyGeneration {
    graph: RunningProductionGraph,
    routing: RuntimeRoutingState,
    transport: HostedGrpc,
    hosted_mcp: Option<HostedMcp>,
}

struct GenerationInputs {
    digest_keys: ProductionDigestKeys,
    startup_inputs: StartupValidationInputs,
    identifiers: ProductionIdentifierSources,
    clocks: ProductionWallClocks,
}

#[derive(Clone)]
struct ReconciledMaintenanceOperation {
    receipt: OfflineMaintenanceReceiptV1,
    evidence: RedbMaintenanceOperationEvidence,
}

enum InitialDatabaseAction {
    OpenCurrent,
    ResumeCurrent {
        receipt: OfflineMaintenanceReceiptV1,
        request: MaintenanceDriverRequest,
        validate_current_source: bool,
    },
    ResumeRecovery(RestoreOfflineBackupRequest),
    AwaitRestoreCredential(OfflineMaintenanceReceiptV1),
    AwaitRecoveryCredential(OfflineMaintenanceReceiptV1),
    RecoveryOnly,
    FailClosed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IncompleteMaintenanceDecision {
    ResumeCreate,
    ResumePublishedCurrentRestore,
    ResumePublishedRecoveryRestore,
    AwaitCurrentCredential,
    AwaitRecoveryCredential,
    FailClosed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct IncompleteMaintenanceShape {
    kind: OfflineMaintenanceOperationKind,
    has_source_database: bool,
    phase: OfflineMaintenanceReceiptPhaseV1,
    named: bool,
    staged: bool,
    target: bool,
    has_staged_database_id: bool,
    has_manifest_identity: bool,
}

impl GenerationInputs {
    fn load(config: &ServerConfig) -> Result<Self, DaemonError> {
        let clocks = ProductionWallClocks::new();
        let digest_keys = load_production_digest_keys(config)?;
        let authorization_time = clocks
            .authorization()
            .now()
            .map_err(DaemonError::StartupClock)?;
        let startup_inputs = startup_validation_inputs(&digest_keys, authorization_time)?;
        Ok(Self {
            digest_keys,
            startup_inputs,
            identifiers: ProductionIdentifierSources::new(),
            clocks,
        })
    }
}

fn incomplete_maintenance_operation(
    reconciliation: &RedbMaintenanceReconciliation,
) -> Result<Option<ReconciledMaintenanceOperation>, DaemonError> {
    let mut incomplete = reconciliation
        .receipts()
        .receipts()
        .iter()
        .filter(|receipt| !receipt.current_phase().is_terminal());
    let Some(receipt) = incomplete.next() else {
        return Ok(None);
    };
    if incomplete.next().is_some() {
        return Err(DaemonError::MaintenanceDriver);
    }
    let evidence = reconciliation
        .operations()
        .iter()
        .copied()
        .find(|evidence| evidence.operation_id() == receipt.operation_id())
        .ok_or(DaemonError::MaintenanceDriver)?;
    Ok(Some(ReconciledMaintenanceOperation {
        receipt: receipt.clone(),
        evidence,
    }))
}

fn incomplete_contract_migration(
    reconciliation: &RedbMaintenanceReconciliation,
) -> Result<Option<ContractMigrationReceiptV1>, DaemonError> {
    let mut incomplete = reconciliation
        .migration_receipts()
        .iter()
        .filter(|receipt| !receipt.current_phase().is_terminal());
    let receipt = incomplete.next().cloned();
    if incomplete.next().is_some() {
        return Err(DaemonError::MaintenanceDriver);
    }
    Ok(receipt)
}

fn has_recovery_backup(reconciliation: &RedbMaintenanceReconciliation) -> bool {
    reconciliation
        .operations()
        .iter()
        .any(|evidence| evidence.named_backup_matches())
}

fn initial_database_action(
    reconciliation: &RedbMaintenanceReconciliation,
    target_requires_recovery: bool,
) -> Result<InitialDatabaseAction, DaemonError> {
    // Archive replay candidates need their own validated recovery driver. They
    // cannot be opened by the ordinary V1 restore or current-database route.
    if reconciliation
        .archive_receipts()
        .receipts()
        .iter()
        .any(|receipt| !receipt.current_phase().is_terminal())
    {
        return Err(DaemonError::MaintenanceDriver);
    }
    let mut retirements = reconciliation
        .retire_receipts()
        .receipts()
        .iter()
        .filter(|receipt| !receipt.current_phase().is_terminal());
    if let Some(retirement) = retirements.next() {
        if retirements.next().is_some()
            || incomplete_maintenance_operation(reconciliation)?.is_some()
        {
            return Err(DaemonError::MaintenanceDriver);
        }
        let create = reconciliation
            .receipts()
            .receipts()
            .iter()
            .find(|receipt| {
                receipt.operation_id() == retirement.retirement().originating_create_operation_id()
            })
            .cloned()
            .ok_or(DaemonError::MaintenanceDriver)?;
        return Ok(InitialDatabaseAction::ResumeCurrent {
            receipt: create,
            request: MaintenanceDriverRequest::retire_backup(retirement.operation_id()),
            validate_current_source: true,
        });
    }
    let Some(operation) = incomplete_maintenance_operation(reconciliation)? else {
        return if target_requires_recovery && has_recovery_backup(reconciliation) {
            Ok(InitialDatabaseAction::RecoveryOnly)
        } else {
            Ok(InitialDatabaseAction::OpenCurrent)
        };
    };
    let receipt = operation.receipt;
    match decide_incomplete_maintenance(&receipt, operation.evidence) {
        IncompleteMaintenanceDecision::ResumeCreate => {
            let request = MaintenanceDriverRequest::create_backup(receipt.operation_id());
            Ok(InitialDatabaseAction::ResumeCurrent {
                receipt,
                request,
                validate_current_source: true,
            })
        }
        IncompleteMaintenanceDecision::ResumePublishedCurrentRestore => {
            let request =
                MaintenanceDriverRequest::resume_published_restore(receipt.operation_id());
            Ok(InitialDatabaseAction::ResumeCurrent {
                receipt,
                request,
                validate_current_source: false,
            })
        }
        IncompleteMaintenanceDecision::ResumePublishedRecoveryRestore => Ok(
            InitialDatabaseAction::ResumeRecovery(restore_request_from_receipt(&receipt)?),
        ),
        IncompleteMaintenanceDecision::AwaitCurrentCredential => {
            Ok(InitialDatabaseAction::AwaitRestoreCredential(receipt))
        }
        IncompleteMaintenanceDecision::AwaitRecoveryCredential => {
            Ok(InitialDatabaseAction::AwaitRecoveryCredential(receipt))
        }
        IncompleteMaintenanceDecision::FailClosed => Ok(InitialDatabaseAction::FailClosed),
    }
}

fn decide_incomplete_maintenance(
    receipt: &OfflineMaintenanceReceiptV1,
    evidence: RedbMaintenanceOperationEvidence,
) -> IncompleteMaintenanceDecision {
    decide_incomplete_shape(IncompleteMaintenanceShape {
        kind: receipt.operation_kind(),
        has_source_database: receipt.source_database_id().is_some(),
        phase: receipt.current_phase(),
        named: evidence.named_backup_matches(),
        staged: evidence.staged_restore_matches(),
        target: evidence.configured_target_matches(),
        has_staged_database_id: receipt.staged_database_id().is_some(),
        has_manifest_identity: receipt.manifest_identity().is_some(),
    })
}

fn decide_incomplete_shape(shape: IncompleteMaintenanceShape) -> IncompleteMaintenanceDecision {
    let IncompleteMaintenanceShape {
        kind,
        has_source_database,
        phase,
        named,
        staged,
        target,
        has_staged_database_id,
        has_manifest_identity,
    } = shape;
    let has_complete_restore_identity = has_staged_database_id && has_manifest_identity;
    let has_consistent_restore_identity = has_staged_database_id == has_manifest_identity;
    match (kind, has_source_database, phase) {
        (
            OfflineMaintenanceOperationKind::CreateBackup,
            true,
            OfflineMaintenanceReceiptPhaseV1::Accepted | OfflineMaintenanceReceiptPhaseV1::Draining,
        ) if !named && !staged && !target && !has_staged_database_id && !has_manifest_identity => {
            IncompleteMaintenanceDecision::ResumeCreate
        }
        (
            OfflineMaintenanceOperationKind::CreateBackup,
            true,
            OfflineMaintenanceReceiptPhaseV1::Offline,
        ) if !staged && !target && !has_staged_database_id && (!has_manifest_identity || named) => {
            IncompleteMaintenanceDecision::ResumeCreate
        }
        (
            OfflineMaintenanceOperationKind::CreateBackup,
            true,
            OfflineMaintenanceReceiptPhaseV1::ArtifactPublished
            | OfflineMaintenanceReceiptPhaseV1::Validating,
        ) if named && !staged && !target && !has_staged_database_id && has_manifest_identity => {
            IncompleteMaintenanceDecision::ResumeCreate
        }
        (
            OfflineMaintenanceOperationKind::RestoreBackup,
            true,
            OfflineMaintenanceReceiptPhaseV1::Accepted | OfflineMaintenanceReceiptPhaseV1::Draining,
        ) if named && !staged && !target && !has_staged_database_id && !has_manifest_identity => {
            IncompleteMaintenanceDecision::AwaitCurrentCredential
        }
        (
            OfflineMaintenanceOperationKind::RestoreBackup,
            true,
            OfflineMaintenanceReceiptPhaseV1::Offline,
        ) if named && staged && target && has_complete_restore_identity => {
            IncompleteMaintenanceDecision::ResumePublishedCurrentRestore
        }
        (
            OfflineMaintenanceOperationKind::RestoreBackup,
            true,
            OfflineMaintenanceReceiptPhaseV1::Offline,
        ) if named && !target && has_consistent_restore_identity => {
            IncompleteMaintenanceDecision::AwaitCurrentCredential
        }
        (
            OfflineMaintenanceOperationKind::RestoreBackup,
            true,
            OfflineMaintenanceReceiptPhaseV1::ArtifactPublished
            | OfflineMaintenanceReceiptPhaseV1::Validating,
        ) if named && staged && target && has_complete_restore_identity => {
            IncompleteMaintenanceDecision::ResumePublishedCurrentRestore
        }
        (
            OfflineMaintenanceOperationKind::RestoreBackup,
            false,
            OfflineMaintenanceReceiptPhaseV1::Accepted,
        ) if named && staged && !target && !has_staged_database_id && !has_manifest_identity => {
            IncompleteMaintenanceDecision::AwaitRecoveryCredential
        }
        (
            OfflineMaintenanceOperationKind::RestoreBackup,
            false,
            OfflineMaintenanceReceiptPhaseV1::Offline,
        ) if named && staged && target && has_complete_restore_identity => {
            IncompleteMaintenanceDecision::ResumePublishedRecoveryRestore
        }
        (
            OfflineMaintenanceOperationKind::RestoreBackup,
            false,
            OfflineMaintenanceReceiptPhaseV1::Offline,
        ) if named && staged && !target && has_consistent_restore_identity => {
            IncompleteMaintenanceDecision::AwaitRecoveryCredential
        }
        (
            OfflineMaintenanceOperationKind::RestoreBackup,
            false,
            OfflineMaintenanceReceiptPhaseV1::ArtifactPublished
            | OfflineMaintenanceReceiptPhaseV1::Validating,
        ) if named && staged && target && has_complete_restore_identity => {
            IncompleteMaintenanceDecision::ResumePublishedRecoveryRestore
        }
        _ => IncompleteMaintenanceDecision::FailClosed,
    }
}

fn restore_request_from_receipt(
    receipt: &OfflineMaintenanceReceiptV1,
) -> Result<RestoreOfflineBackupRequest, DaemonError> {
    let request = RestoreOfflineBackupRequest::new(
        receipt.operation_id(),
        receipt.backup_name().clone(),
        receipt.replacement_confirmation(),
    )
    .map_err(|_| DaemonError::MaintenanceDriver)?;
    if request.input_hash() != receipt.input_hash() {
        return Err(DaemonError::MaintenanceDriver);
    }
    Ok(request)
}

/// Runs the production `riffdbd` process and returns a conventional exit status.
///
/// Configuration failures are rendered through their bounded, value-redacted
/// diagnostics. All later process failures retain one static nonsecret message.
#[must_use]
pub fn riffdbd_main() -> ExitCode {
    match run_from_process(MaintenanceRecoveryController::disabled()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(DaemonError::Config(error)) => {
            eprintln!("RDB-CONFIG-0001: {error}");
            ExitCode::FAILURE
        }
        Err(DaemonError::Startup(error @ RedbStartupError::Format(_))) => {
            // This source contains only manifest identities, bounded action
            // metadata, and static guidance. Paths and stored values remain
            // redacted by construction.
            eprintln!("RDB-FORMAT-0101: {error}");
            ExitCode::FAILURE
        }
        Err(error) => {
            // Stable, non-secret lifecycle kind only — never dump raw sources.
            // App-baseline and operators need the discriminant to diagnose mid-run
            // process death (e.g. transport_ended vs runtime_stopped).
            //
            // A startup refusal also names the closed storage class that caused
            // it. `kind=startup` alone is not diagnosable: a database that has
            // outgrown a startup validation bound and one that is genuinely
            // corrupt both print the same line and exit silently, which cost a
            // full instrumentation cycle to tell apart. `StorageErrorKind` is a
            // closed enum carrying no path, key, value, or identity, so naming
            // it leaks nothing the lifecycle kind does not already.
            if let DaemonError::Startup(ref startup) = error {
                match startup {
                    RedbStartupError::Storage(storage) => eprintln!(
                        "riffdbd startup validation refused the database class={:?}",
                        storage.kind()
                    ),
                    RedbStartupError::Integrity(reason) => eprintln!(
                        "riffdbd startup validation refused the database integrity={reason:?}"
                    ),
                    RedbStartupError::Catalog(catalog) => {
                        eprintln!(
                            "riffdbd startup validation refused the database catalog={:?}",
                            catalog.kind()
                        );
                    }
                    RedbStartupError::Identifier(_) => {
                        eprintln!(
                            "riffdbd startup validation refused the database class=identifier"
                        );
                    }
                    RedbStartupError::Format(_) => {}
                }
            }
            if let DaemonError::GraphBuild(source) = &error {
                // The kind alone is true of fifteen different owners. The class
                // is a fixed string naming which one refused, with no path,
                // identity, or stored value in it.
                eprintln!(
                    "riffdbd process graph construction refused class={}",
                    source.class()
                );
            }
            eprintln!(
                "riffdbd terminated without reaching a clean process boundary kind={}",
                error.kind()
            );
            ExitCode::FAILURE
        }
    }
}

#[cfg(feature = "test-fixtures")]
pub(crate) fn riffdbd_test_fixture_main(
    point: crate::maintenance_recovery_controller::MaintenanceRecoveryTestPoint,
) -> ExitCode {
    let recovery = MaintenanceRecoveryController::armed(point.into());
    match run_from_process(recovery) {
        Ok(()) => ExitCode::SUCCESS,
        Err(_error) => {
            eprintln!("riffdbd recovery fixture terminated before its armed boundary");
            ExitCode::FAILURE
        }
    }
}

fn run_from_process(recovery: MaintenanceRecoveryController) -> Result<(), DaemonError> {
    crate::startup_census::mark_process_start();
    let config = ServerConfig::from_process_args().map_err(DaemonError::Config)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(RUNTIME_WORKER_THREADS)
        .enable_all()
        .build()
        .map_err(DaemonError::Runtime)?;
    let result = runtime.block_on(run_server(config, recovery));
    runtime.shutdown_timeout(RUNTIME_DRAIN_LIMIT);
    // Every storage clone that outlived the graph has now been dropped,
    // `redb::Database::drop` included. Reprint the release census with the
    // remainder filled in: this is the only receipt that can cover it.
    crate::shutdown_census::record_post_graph_release();
    eprintln!("{}", crate::shutdown_census::format_v1_line());
    result
}

async fn run_server(
    config: ServerConfig,
    recovery: MaintenanceRecoveryController,
) -> Result<(), DaemonError> {
    // Register before any blocking startup work so a service-manager signal
    // cannot be lost while structural and catalog validation are running.
    let mut process_signal =
        ProductionShutdownSignal::register().map_err(DaemonError::ShutdownSignal)?;
    // Internal benchmark synchronization only: the runtime and signal owner
    // now exist, while no database or application population has been opened.
    eprintln!("riffdb-process-memory-baseline-v1");
    if config.mode() == crate::config::ServerMode::Follower {
        return follower::run(config, &mut process_signal).await;
    }
    if config.databases().len() > 1 {
        return run_multi_database_server(config, &mut process_signal, &recovery).await;
    }
    let clocks = ProductionWallClocks::new();
    let started_at = clocks.process_time().map_err(DaemonError::ProcessClock)?;
    let digest_keys = load_production_digest_keys(&config)?;
    let authorization_time = clocks
        .authorization()
        .now()
        .map_err(DaemonError::StartupClock)?;
    let startup_inputs = startup_validation_inputs(&digest_keys, authorization_time)?;
    let identifiers = ProductionIdentifierSources::new();
    let database_ids = identifiers.database_ids();

    let (initializing, activator, issuer) = RiffDbService::begin_initialization();
    let routing = RuntimeRoutingState::new();
    let maintenance_lifecycle = Arc::new(MaintenanceLifecycle::ready());
    let lifecycle = Arc::new(ProductionLifecycleRoute::new_with_maintenance(
        initializing,
        issuer,
        routing.clone(),
        Arc::clone(&maintenance_lifecycle),
    ));
    let lifecycle_for_grpc: Arc<dyn GrpcLifecycleRoute> = lifecycle.clone();
    let limits = GrpcRequestLimits::new(REQUEST_DURATION_LIMIT)
        .map_err(|_| DaemonError::GrpcConfiguration)?;
    let application =
        GrpcApplication::new_with_audience(lifecycle_for_grpc, limits, config.audience().clone());
    let mut transport = HostedGrpc::bind(config.application_listener().clone(), &application)?;
    drop(application);
    let (maintenance_storage, reconciliation) =
        RedbMaintenanceStorage::open(config.database_path(), config.backup_root())
            .map_err(DaemonError::MaintenanceStorage)?;
    let migration_startup = match incomplete_contract_migration(&reconciliation)? {
        None => None,
        Some(receipt) => {
            let build = contract_migration_backup_build_metadata()
                .map_err(|_| DaemonError::MaintenanceDriver)?;
            let inputs = MigrationDriverInputs {
                startup: startup_inputs.clone(),
                database_ids: &database_ids,
                commit_profile: config.redb_commit_profile(),
                build: &build,
            };
            let (_, startup) = drive_contract_migration(&maintenance_storage, receipt, &inputs)
                .map_err(|_| DaemonError::MaintenanceDriver)?;
            Some(startup)
        }
    };
    let target_requires_recovery = if migration_startup.is_some() {
        false
    } else {
        maintenance_storage
            .configured_target_requires_recovery()
            .map_err(DaemonError::MaintenanceStorage)?
    };
    let recovery_backup_available = has_recovery_backup(&reconciliation);
    let initial_action = initial_database_action(&reconciliation, target_requires_recovery)?;
    let (maintenance_triggers, mut maintenance_receiver) = maintenance_trigger_channel();
    let maintenance = MaintenanceController::new(
        shared_maintenance_storage(maintenance_storage),
        Arc::clone(&maintenance_lifecycle),
        maintenance_triggers,
    );

    // ADR-0034 requires the real listener to exist before every blocking proof.
    let mut completed_initial_operation = None;
    let startup = if let Some(startup) = migration_startup {
        startup
    } else {
        match initial_action {
            InitialDatabaseAction::OpenCurrent => match open_redb_startup_with_commit_profile(
                config.database_path(),
                startup_inputs.clone(),
                &database_ids,
                config.redb_commit_profile(),
            ) {
                Ok(startup) => startup,
                Err(source)
                    if recovery_backup_available && startup_failure_allows_recovery(&source) =>
                {
                    maintenance_lifecycle
                        .enter_recovery_mode()
                        .map_err(|_| DaemonError::MaintenanceDriver)?;
                    return run_recovery_until_ready(
                        &config,
                        started_at,
                        transport,
                        lifecycle,
                        Arc::clone(&maintenance_lifecycle),
                        maintenance,
                        &mut maintenance_receiver,
                        &mut process_signal,
                        &identifiers,
                        &recovery,
                    )
                    .await;
                }
                Err(source) => {
                    lifecycle.stop();
                    transport.drain_after_signal().await?;
                    return Err(DaemonError::Startup(source));
                }
            },
            InitialDatabaseAction::ResumeCurrent {
                receipt,
                request,
                validate_current_source,
            } => {
                let operation_id = request.operation_id();
                let mut retained_target_history_incarnation = None;
                if validate_current_source {
                    let current = open_redb_startup_with_commit_profile(
                        config.database_path(),
                        startup_inputs.clone(),
                        &database_ids,
                        config.redb_commit_profile(),
                    )
                    .map_err(DaemonError::Startup)?;
                    if receipt.source_database_id() != Some(current.database_id()) {
                        drop(current);
                        lifecycle.stop();
                        transport.drain_after_signal().await?;
                        return Err(DaemonError::MaintenanceDriver);
                    }
                    retained_target_history_incarnation =
                        Some(current.retained_metadata().history_incarnation());
                    drop(current);
                }
                let dependencies = maintenance_driver_dependencies(
                    &config,
                    config.environment(),
                    &startup_inputs,
                    &digest_keys,
                    &identifiers,
                    &clocks,
                    &recovery,
                    retained_target_history_incarnation,
                    None,
                )?;
                let storage = maintenance.storage();
                let mut storage = storage.lock().map_err(|_| DaemonError::MaintenanceDriver)?;
                mark_draining(&mut storage, &maintenance_lifecycle, operation_id)
                    .map_err(|_| DaemonError::MaintenanceDriver)?;
                mark_offline(&mut storage, &maintenance_lifecycle, operation_id)
                    .map_err(|_| DaemonError::MaintenanceDriver)?;
                let success = run_offline_maintenance(
                    &mut storage,
                    &maintenance_lifecycle,
                    &dependencies,
                    request,
                )
                .map_err(|_| DaemonError::MaintenanceDriver)?;
                completed_initial_operation = Some(operation_id);
                success.into_parts().1
            }
            InitialDatabaseAction::ResumeRecovery(request) => {
                let operation_id = request.operation_id();
                maintenance_lifecycle
                    .await_recovery_retry(operation_id, request.input_hash())
                    .and_then(|()| maintenance_lifecycle.begin_recovery_restore(operation_id))
                    .map_err(|_| DaemonError::MaintenanceDriver)?;
                let dependencies = maintenance_driver_dependencies(
                    &config,
                    config.environment(),
                    &startup_inputs,
                    &digest_keys,
                    &identifiers,
                    &clocks,
                    &recovery,
                    None,
                    None,
                )?;
                let storage = maintenance.storage();
                let mut storage = storage.lock().map_err(|_| DaemonError::MaintenanceDriver)?;
                let success = run_recovery_restore(
                    &mut storage,
                    &maintenance_lifecycle,
                    &dependencies,
                    RecoveryMaintenanceDriverRequest::resume_published(request),
                )
                .map_err(|_| DaemonError::MaintenanceDriver)?;
                completed_initial_operation = Some(operation_id);
                success.into_parts().1
            }
            InitialDatabaseAction::AwaitRestoreCredential(receipt) => {
                let startup = open_redb_startup_with_commit_profile(
                    config.database_path(),
                    startup_inputs.clone(),
                    &database_ids,
                    config.redb_commit_profile(),
                )
                .map_err(DaemonError::Startup)?;
                if receipt.source_database_id() != Some(startup.database_id()) {
                    lifecycle.stop();
                    transport.drain_after_signal().await?;
                    return Err(DaemonError::MaintenanceDriver);
                }
                maintenance_lifecycle
                    .await_restore_retry(receipt.operation_id(), receipt.input_hash())
                    .map_err(|_| DaemonError::MaintenanceDriver)?;
                drop(activator);
                return run_restore_retry_until_ready(
                    &config,
                    started_at,
                    startup,
                    receipt,
                    transport,
                    lifecycle,
                    Arc::clone(&maintenance_lifecycle),
                    maintenance,
                    &mut maintenance_receiver,
                    &mut process_signal,
                    &digest_keys,
                    &clocks,
                    &identifiers,
                    &recovery,
                )
                .await;
            }
            InitialDatabaseAction::AwaitRecoveryCredential(receipt) => {
                maintenance_lifecycle
                    .await_recovery_retry(receipt.operation_id(), receipt.input_hash())
                    .map_err(|_| DaemonError::MaintenanceDriver)?;
                return run_recovery_until_ready(
                    &config,
                    started_at,
                    transport,
                    lifecycle,
                    Arc::clone(&maintenance_lifecycle),
                    maintenance,
                    &mut maintenance_receiver,
                    &mut process_signal,
                    &identifiers,
                    &recovery,
                )
                .await;
            }
            InitialDatabaseAction::RecoveryOnly => {
                maintenance_lifecycle
                    .enter_recovery_mode()
                    .map_err(|_| DaemonError::MaintenanceDriver)?;
                return run_recovery_until_ready(
                    &config,
                    started_at,
                    transport,
                    lifecycle,
                    Arc::clone(&maintenance_lifecycle),
                    maintenance,
                    &mut maintenance_receiver,
                    &mut process_signal,
                    &identifiers,
                    &recovery,
                )
                .await;
            }
            InitialDatabaseAction::FailClosed => {
                lifecycle.stop();
                transport.drain_after_signal().await?;
                return Err(DaemonError::MaintenanceDriver);
            }
        }
    };
    let build = match build_info(&startup) {
        Ok(build) => build,
        Err(source) => {
            lifecycle.stop();
            transport.drain_after_signal().await?;
            return Err(source);
        }
    };
    let graph = match ProductionGraphBuilder::new(
        startup,
        activator,
        digest_keys,
        &config,
        &config.databases()[0],
        started_at,
        build,
        identifiers,
        clocks,
        lifecycle.clone(),
        maintenance.clone(),
    )
    .build()
    {
        Ok(graph) => graph,
        Err(source) => {
            lifecycle.stop();
            transport.drain_after_signal().await?;
            return Err(DaemonError::GraphBuild(source));
        }
    };
    let mut hosted_mcp = if maintenance_lifecycle.credential_retry_restore_available() {
        None
    } else {
        match config.mcp_listen_address() {
            Some(address) => {
                let Some(dependencies) = graph.hosted_mcp_dependencies() else {
                    let mut no_hosted_mcp = None;
                    shutdown_before_ready(graph, &mut transport, &mut no_hosted_mcp).await?;
                    return Err(DaemonError::McpDependencies);
                };
                match HostedMcp::bind(address, config.mcp_origins(), dependencies).await {
                    Ok(hosted) => Some(hosted),
                    Err(source) => {
                        let mut no_hosted_mcp = None;
                        shutdown_before_ready(graph, &mut transport, &mut no_hosted_mcp).await?;
                        return Err(DaemonError::McpStart(source));
                    }
                }
            }
            None => None,
        }
    };

    if transport.is_finished() || hosted_mcp.as_ref().is_some_and(HostedMcp::is_finished) {
        let grpc_finished = transport.is_finished();
        let mcp_finished = hosted_mcp.as_ref().is_some_and(HostedMcp::is_finished);
        if let Some(hosted_mcp) = hosted_mcp.as_mut() {
            hosted_mcp.begin_shutdown();
        }
        let notification_stop_failed = graph.begin_transport_shutdown().is_err();
        let completion = if grpc_finished {
            transport.completed().await
        } else {
            transport.drain_after_signal().await
        };
        let mcp_completion = match hosted_mcp.as_mut() {
            Some(hosted_mcp) if mcp_finished => hosted_mcp.completed().await,
            Some(hosted_mcp) => hosted_mcp.drain_after_signal().await,
            None => Ok(()),
        };
        let graph_result = graph.shutdown().await;
        if notification_stop_failed {
            return Err(DaemonError::NotificationShutdown);
        }
        graph_result.map_err(DaemonError::GraphShutdown)?;
        completion?;
        mcp_completion.map_err(DaemonError::McpStop)?;
        return Err(if grpc_finished {
            DaemonError::TransportEnded
        } else {
            DaemonError::McpTransportEnded
        });
    }
    // The transport and graph now own every route clone. Retaining this
    // composition temporary would retain the activated service and its redb
    // handles across the offline boundary.
    drop(lifecycle);

    if let Some(operation_id) = completed_initial_operation {
        maintenance_lifecycle
            .finish_ready(operation_id)
            .map_err(|_| DaemonError::MaintenanceDriver)?;
    }
    let publish_initial = maintenance_lifecycle.ordinary_admission_available();
    run_ready_generations(
        &config,
        started_at,
        ReadyGeneration {
            graph,
            routing,
            transport,
            hosted_mcp,
        },
        publish_initial,
        maintenance_lifecycle,
        maintenance,
        &mut maintenance_receiver,
        &mut process_signal,
        &recovery,
    )
    .await
}

struct MultiDatabaseGraph {
    alias: DatabaseAlias,
    graph: Option<RunningProductionGraph>,
    lifecycle: Arc<ProductionLifecycleRoute>,
    routing: RuntimeRoutingState,
    maintenance_lifecycle: Arc<MaintenanceLifecycle>,
    maintenance: MaintenanceController,
    maintenance_receiver: Option<tokio::sync::mpsc::Receiver<MaintenanceTrigger>>,
    generation: u64,
}

struct MultiInitialRecovery {
    operation_id: riffdb_types::OfflineMaintenanceOperationId,
    startup: crate::startup::CheckedRedbStartup,
}

enum MultiDatabaseEvent {
    Maintenance {
        database_index: usize,
        trigger: MaintenanceTrigger,
    },
    RuntimeStopped {
        database_index: usize,
        generation: u64,
    },
    MaintenanceClosed {
        database_index: usize,
    },
}

enum MultiDatabaseStop {
    Clean,
    TransportEnded,
    McpEnded,
}

async fn run_multi_database_server(
    config: ServerConfig,
    process_signal: &mut ProductionShutdownSignal,
    recovery: &MaintenanceRecoveryController,
) -> Result<(), DaemonError> {
    let process_clocks = ProductionWallClocks::new();
    let started_at = process_clocks
        .process_time()
        .map_err(DaemonError::ProcessClock)?;
    let digest_keys = load_production_digest_keys(&config)?;
    let authorization_time = process_clocks
        .authorization()
        .now()
        .map_err(DaemonError::StartupClock)?;
    let startup_inputs = startup_validation_inputs(&digest_keys, authorization_time)?;
    let migration_exclusion = Arc::new(MigrationProcessExclusion::new());

    struct PendingDatabase {
        alias: riffdb_types::DatabaseAlias,
        activator: riffdb_service::RiffDbServiceActivator,
        lifecycle: Arc<ProductionLifecycleRoute>,
        routing: RuntimeRoutingState,
        maintenance_lifecycle: Arc<MaintenanceLifecycle>,
        identifiers: ProductionIdentifierSources,
    }

    let mut pending = Vec::with_capacity(config.databases().len());
    let mut routes = Vec::with_capacity(config.databases().len());
    for database in config.databases() {
        let (public_initializing, _public_activator, public_issuer) =
            RiffDbService::begin_initialization();
        let public_route = Arc::new(ProductionLifecycleRoute::new_with_maintenance(
            public_initializing,
            public_issuer,
            RuntimeRoutingState::new(),
            Arc::new(MaintenanceLifecycle::ready()),
        ));
        let route: Arc<dyn GrpcLifecycleRoute> = public_route;
        routes.push((database.alias().clone(), route));

        let (initializing, activator, issuer) = RiffDbService::begin_initialization();
        let routing = RuntimeRoutingState::new();
        let maintenance_lifecycle = Arc::new(MaintenanceLifecycle::ready());
        let lifecycle = Arc::new(ProductionLifecycleRoute::new_with_maintenance(
            initializing,
            issuer,
            routing.clone(),
            Arc::clone(&maintenance_lifecycle),
        ));
        pending.push(PendingDatabase {
            alias: database.alias().clone(),
            activator,
            lifecycle,
            routing,
            maintenance_lifecycle,
            identifiers: ProductionIdentifierSources::new(),
        });
    }
    let routes =
        Arc::new(GrpcDatabaseRoutes::new(routes).map_err(|_| DaemonError::GrpcConfiguration)?);
    let limits = GrpcRequestLimits::new(REQUEST_DURATION_LIMIT)
        .map_err(|_| DaemonError::GrpcConfiguration)?;
    let application = GrpcApplication::with_database_routes_and_audience(
        Arc::clone(&routes),
        limits,
        config.audience().clone(),
    );
    let mut transport = HostedGrpc::bind(config.application_listener().clone(), &application)?;
    drop(application);

    let mut graphs = Vec::with_capacity(pending.len());
    for (database, mut pending) in config.databases().iter().zip(pending) {
        let (maintenance_storage, reconciliation) =
            match RedbMaintenanceStorage::open(database.database_path(), database.backup_root()) {
                Ok(value) => value,
                Err(source) => {
                    shutdown_multi_before_ready(&mut transport, graphs).await?;
                    return Err(DaemonError::MaintenanceStorage(source));
                }
            };
        let migration_startup = match incomplete_contract_migration(&reconciliation)? {
            None => None,
            Some(receipt) => {
                let build = contract_migration_backup_build_metadata()
                    .map_err(|_| DaemonError::MaintenanceDriver)?;
                let database_ids = pending.identifiers.database_ids();
                let inputs = MigrationDriverInputs {
                    startup: startup_inputs.clone(),
                    database_ids: &database_ids,
                    commit_profile: config.redb_commit_profile(),
                    build: &build,
                };
                match drive_contract_migration(&maintenance_storage, receipt, &inputs) {
                    Ok((_, startup)) => Some(startup),
                    Err(_) => {
                        shutdown_multi_before_ready(&mut transport, graphs).await?;
                        return Err(DaemonError::MaintenanceDriver);
                    }
                }
            }
        };
        let target_requires_recovery = if migration_startup.is_some() {
            false
        } else {
            maintenance_storage
                .configured_target_requires_recovery()
                .map_err(DaemonError::MaintenanceStorage)?
        };
        let initial_action = initial_database_action(&reconciliation, target_requires_recovery)?;
        let (maintenance_triggers, mut maintenance_receiver) = maintenance_trigger_channel();
        let maintenance = MaintenanceController::new_with_migration_exclusion(
            shared_maintenance_storage(maintenance_storage),
            Arc::clone(&pending.maintenance_lifecycle),
            maintenance_triggers,
            Arc::clone(&migration_exclusion),
        );
        let mut completed_initial_operation = None;
        let startup = if let Some(startup) = migration_startup {
            startup
        } else {
            match initial_action {
                InitialDatabaseAction::OpenCurrent => {
                    match open_redb_startup_with_commit_profile(
                        database.database_path(),
                        startup_inputs.clone(),
                        &pending.identifiers.database_ids(),
                        config.redb_commit_profile(),
                    ) {
                        Ok(startup) => startup,
                        Err(source) => {
                            shutdown_multi_before_ready(&mut transport, graphs).await?;
                            return Err(DaemonError::Startup(source));
                        }
                    }
                }
                InitialDatabaseAction::ResumeCurrent {
                    receipt,
                    request,
                    validate_current_source,
                } => {
                    let operation_id = request.operation_id();
                    let mut retained_target_history_incarnation = None;
                    if validate_current_source {
                        let current = open_redb_startup_with_commit_profile(
                            database.database_path(),
                            startup_inputs.clone(),
                            &pending.identifiers.database_ids(),
                            config.redb_commit_profile(),
                        )
                        .map_err(DaemonError::Startup)?;
                        if receipt.source_database_id() != Some(current.database_id()) {
                            shutdown_multi_before_ready(&mut transport, graphs).await?;
                            return Err(DaemonError::MaintenanceDriver);
                        }
                        retained_target_history_incarnation =
                            Some(current.retained_metadata().history_incarnation());
                        drop(current);
                    }
                    let dependencies = maintenance_driver_dependencies(
                        &config,
                        database.environment(),
                        &startup_inputs,
                        &digest_keys,
                        &pending.identifiers,
                        &process_clocks,
                        recovery,
                        retained_target_history_incarnation,
                        None,
                    )?;
                    let storage = maintenance.storage();
                    let mut storage = storage.lock().map_err(|_| DaemonError::MaintenanceDriver)?;
                    mark_draining(&mut storage, &pending.maintenance_lifecycle, operation_id)
                        .map_err(|_| DaemonError::MaintenanceDriver)?;
                    mark_offline(&mut storage, &pending.maintenance_lifecycle, operation_id)
                        .map_err(|_| DaemonError::MaintenanceDriver)?;
                    let success = run_offline_maintenance(
                        &mut storage,
                        &pending.maintenance_lifecycle,
                        &dependencies,
                        request,
                    )
                    .map_err(|_| DaemonError::MaintenanceDriver)?;
                    completed_initial_operation = Some(operation_id);
                    success.into_parts().1
                }
                InitialDatabaseAction::ResumeRecovery(request) => {
                    let operation_id = request.operation_id();
                    pending
                        .maintenance_lifecycle
                        .await_recovery_retry(operation_id, request.input_hash())
                        .and_then(|()| {
                            pending
                                .maintenance_lifecycle
                                .begin_recovery_restore(operation_id)
                        })
                        .map_err(|_| DaemonError::MaintenanceDriver)?;
                    let dependencies = maintenance_driver_dependencies(
                        &config,
                        database.environment(),
                        &startup_inputs,
                        &digest_keys,
                        &pending.identifiers,
                        &process_clocks,
                        recovery,
                        None,
                        None,
                    )?;
                    let storage = maintenance.storage();
                    let mut storage = storage.lock().map_err(|_| DaemonError::MaintenanceDriver)?;
                    let success = run_recovery_restore(
                        &mut storage,
                        &pending.maintenance_lifecycle,
                        &dependencies,
                        RecoveryMaintenanceDriverRequest::resume_published(request),
                    )
                    .map_err(|_| DaemonError::MaintenanceDriver)?;
                    completed_initial_operation = Some(operation_id);
                    success.into_parts().1
                }
                InitialDatabaseAction::AwaitRestoreCredential(receipt) => {
                    let current = open_redb_startup_with_commit_profile(
                        database.database_path(),
                        startup_inputs.clone(),
                        &pending.identifiers.database_ids(),
                        config.redb_commit_profile(),
                    )
                    .map_err(DaemonError::Startup)?;
                    if receipt.source_database_id() != Some(current.database_id()) {
                        drop(current);
                        shutdown_multi_before_ready(&mut transport, graphs).await?;
                        return Err(DaemonError::MaintenanceDriver);
                    }
                    pending
                        .maintenance_lifecycle
                        .await_restore_retry(receipt.operation_id(), receipt.input_hash())
                        .map_err(|_| DaemonError::MaintenanceDriver)?;
                    let (offline_service, _offline_activator, offline_issuer) =
                        RiffDbService::begin_initialization();
                    let offline_lifecycle =
                        Arc::new(ProductionLifecycleRoute::new_with_maintenance(
                            offline_service,
                            offline_issuer,
                            RuntimeRoutingState::new(),
                            Arc::clone(&pending.maintenance_lifecycle),
                        ));
                    let retry_lifecycle =
                        std::mem::replace(&mut pending.lifecycle, offline_lifecycle);
                    let route: Arc<dyn GrpcLifecycleRoute> = retry_lifecycle.clone();
                    routes
                        .replace(&pending.alias, route)
                        .map_err(|_| DaemonError::GrpcConfiguration)?;
                    match await_multi_restore_retry(
                        &config,
                        database,
                        current,
                        receipt,
                        &digest_keys,
                        &process_clocks,
                        &pending.identifiers,
                        retry_lifecycle,
                        pending.maintenance_lifecycle.clone(),
                        maintenance.clone(),
                        &mut maintenance_receiver,
                        &routes,
                        process_signal,
                        &mut transport,
                        recovery,
                    )
                    .await
                    {
                        Ok(Some(recovered)) => {
                            completed_initial_operation = Some(recovered.operation_id);
                            let (initializing, activator, issuer) =
                                RiffDbService::begin_initialization();
                            let routing = RuntimeRoutingState::new();
                            pending.lifecycle =
                                Arc::new(ProductionLifecycleRoute::new_with_maintenance(
                                    initializing,
                                    issuer,
                                    routing.clone(),
                                    pending.maintenance_lifecycle.clone(),
                                ));
                            pending.activator = activator;
                            pending.routing = routing;
                            recovered.startup
                        }
                        Ok(None) => {
                            shutdown_multi_before_ready(&mut transport, graphs).await?;
                            return Ok(());
                        }
                        Err(source) => {
                            shutdown_multi_before_ready(&mut transport, graphs).await?;
                            return Err(source);
                        }
                    }
                }
                InitialDatabaseAction::AwaitRecoveryCredential(receipt) => {
                    pending
                        .maintenance_lifecycle
                        .await_recovery_retry(receipt.operation_id(), receipt.input_hash())
                        .map_err(|_| DaemonError::MaintenanceDriver)?;
                    let route: Arc<dyn GrpcLifecycleRoute> = pending.lifecycle.clone();
                    routes
                        .replace(&pending.alias, route)
                        .map_err(|_| DaemonError::GrpcConfiguration)?;
                    match await_multi_recovery(
                        &config,
                        database,
                        &pending.identifiers,
                        pending.lifecycle.clone(),
                        pending.maintenance_lifecycle.clone(),
                        maintenance.clone(),
                        &mut maintenance_receiver,
                        process_signal,
                        &mut transport,
                        recovery,
                    )
                    .await
                    {
                        Ok(Some(recovered)) => {
                            completed_initial_operation = Some(recovered.operation_id);
                            let (initializing, activator, issuer) =
                                RiffDbService::begin_initialization();
                            let routing = RuntimeRoutingState::new();
                            pending.lifecycle =
                                Arc::new(ProductionLifecycleRoute::new_with_maintenance(
                                    initializing,
                                    issuer,
                                    routing.clone(),
                                    pending.maintenance_lifecycle.clone(),
                                ));
                            pending.activator = activator;
                            pending.routing = routing;
                            recovered.startup
                        }
                        Ok(None) => {
                            shutdown_multi_before_ready(&mut transport, graphs).await?;
                            return Ok(());
                        }
                        Err(source) => {
                            shutdown_multi_before_ready(&mut transport, graphs).await?;
                            return Err(source);
                        }
                    }
                }
                InitialDatabaseAction::RecoveryOnly => {
                    pending
                        .maintenance_lifecycle
                        .enter_recovery_mode()
                        .map_err(|_| DaemonError::MaintenanceDriver)?;
                    let route: Arc<dyn GrpcLifecycleRoute> = pending.lifecycle.clone();
                    routes
                        .replace(&pending.alias, route)
                        .map_err(|_| DaemonError::GrpcConfiguration)?;
                    match await_multi_recovery(
                        &config,
                        database,
                        &pending.identifiers,
                        pending.lifecycle.clone(),
                        pending.maintenance_lifecycle.clone(),
                        maintenance.clone(),
                        &mut maintenance_receiver,
                        process_signal,
                        &mut transport,
                        recovery,
                    )
                    .await
                    {
                        Ok(Some(recovered)) => {
                            completed_initial_operation = Some(recovered.operation_id);
                            let (initializing, activator, issuer) =
                                RiffDbService::begin_initialization();
                            let routing = RuntimeRoutingState::new();
                            pending.lifecycle =
                                Arc::new(ProductionLifecycleRoute::new_with_maintenance(
                                    initializing,
                                    issuer,
                                    routing.clone(),
                                    pending.maintenance_lifecycle.clone(),
                                ));
                            pending.activator = activator;
                            pending.routing = routing;
                            recovered.startup
                        }
                        Ok(None) => {
                            shutdown_multi_before_ready(&mut transport, graphs).await?;
                            return Ok(());
                        }
                        Err(source) => {
                            shutdown_multi_before_ready(&mut transport, graphs).await?;
                            return Err(source);
                        }
                    }
                }
                InitialDatabaseAction::FailClosed => {
                    shutdown_multi_before_ready(&mut transport, graphs).await?;
                    return Err(DaemonError::MaintenanceDriver);
                }
            }
        };
        let build = build_info(&startup)?;
        let graph = match ProductionGraphBuilder::new(
            startup,
            pending.activator,
            digest_keys.clone(),
            &config,
            database,
            started_at,
            build,
            pending.identifiers,
            ProductionWallClocks::new(),
            pending.lifecycle.clone(),
            maintenance.clone(),
        )
        .build()
        {
            Ok(graph) => graph,
            Err(source) => {
                shutdown_multi_before_ready(&mut transport, graphs).await?;
                return Err(DaemonError::GraphBuild(source));
            }
        };
        if let Some(operation_id) = completed_initial_operation {
            pending
                .maintenance_lifecycle
                .finish_ready(operation_id)
                .map_err(|_| DaemonError::MaintenanceDriver)?;
        }
        let _ = pending.routing;
        graphs.push(MultiDatabaseGraph {
            alias: pending.alias,
            graph: Some(graph),
            lifecycle: pending.lifecycle,
            routing: pending.routing,
            maintenance_lifecycle: pending.maintenance_lifecycle,
            maintenance,
            maintenance_receiver: Some(maintenance_receiver),
            generation: 0,
        });
    }

    for graph in &graphs {
        let route: Arc<dyn GrpcLifecycleRoute> = graph.lifecycle.clone();
        routes
            .replace(&graph.alias, route)
            .map_err(|_| DaemonError::GrpcConfiguration)?;
    }

    let mut hosted_mcp = match config.mcp_listen_address() {
        Some(address) => {
            let mut dependencies = Vec::with_capacity(graphs.len());
            for (database, graph) in config.databases().iter().zip(&graphs) {
                let Some(hosted) = graph
                    .graph
                    .as_ref()
                    .and_then(RunningProductionGraph::hosted_mcp_dependencies)
                else {
                    shutdown_multi_before_ready(&mut transport, graphs).await?;
                    return Err(DaemonError::McpDependencies);
                };
                dependencies.push((database.alias().clone(), hosted));
            }
            match HostedMcp::bind_routes(address, config.mcp_origins(), dependencies).await {
                Ok(hosted) => Some(hosted),
                Err(source) => {
                    shutdown_multi_before_ready(&mut transport, graphs).await?;
                    return Err(DaemonError::McpStart(source));
                }
            }
        }
        None => None,
    };

    publish_readiness(transport.endpoint())?;
    let (shutdown_input, shutdown_thread) = spawn_shutdown_reader()?;
    let mut shutdown_input = Some(shutdown_input);
    let (event_sender, mut event_receiver) =
        tokio::sync::mpsc::channel(config.databases().len().saturating_mul(2));
    let mut monitors = Vec::with_capacity(graphs.len().saturating_mul(2));
    for (database_index, graph) in graphs.iter_mut().enumerate() {
        let mut receiver = graph
            .maintenance_receiver
            .take()
            .expect("each database owns one maintenance receiver");
        let sender = event_sender.clone();
        monitors.push(tokio::spawn(async move {
            while let Some(trigger) = receiver.recv().await {
                if sender
                    .send(MultiDatabaseEvent::Maintenance {
                        database_index,
                        trigger,
                    })
                    .await
                    .is_err()
                {
                    return;
                }
            }
            let _ = sender
                .send(MultiDatabaseEvent::MaintenanceClosed { database_index })
                .await;
        }));
        monitors.push(spawn_multi_runtime_monitor(
            database_index,
            graph.generation,
            graph.routing.clone(),
            event_sender.clone(),
        ));
    }

    let stop = loop {
        let selected = tokio::select! {
            signal = process_signal.received() => {
                signal.map_err(DaemonError::ShutdownSignal)?;
                MultiDatabaseStop::Clean
            }
            input = wait_for_shutdown_input(&mut shutdown_input) => {
                match input {
                    Some(ReadyProcessTrigger::Command) => MultiDatabaseStop::Clean,
                    Some(ReadyProcessTrigger::ShutdownInputFailure) => {
                        return Err(DaemonError::ShutdownInput);
                    }
                    None => continue,
                    Some(_) => return Err(DaemonError::ShutdownInput),
                }
            }
            completion = transport.completed() => {
                completion?;
                MultiDatabaseStop::TransportEnded
            }
            completion = wait_for_hosted_mcp(&mut hosted_mcp) => {
                completion.map_err(DaemonError::McpStop)?;
                MultiDatabaseStop::McpEnded
            }
            event = event_receiver.recv() => {
                let Some(event) = event else {
                    return Err(DaemonError::RuntimeStopped);
                };
                match event {
                    MultiDatabaseEvent::RuntimeStopped {
                        database_index,
                        generation,
                    } => {
                        if graphs
                            .get(database_index)
                            .is_some_and(|graph| graph.generation == generation)
                        {
                            quarantine_multi_database_generation(
                                database_index,
                                &routes,
                                &hosted_mcp,
                                &mut graphs,
                            )
                            .await?;
                        }
                        continue;
                    }
                    MultiDatabaseEvent::MaintenanceClosed { database_index } => {
                        quarantine_multi_database_generation(
                            database_index,
                            &routes,
                            &hosted_mcp,
                            &mut graphs,
                        )
                        .await?;
                        continue;
                    }
                    MultiDatabaseEvent::Maintenance {
                        database_index,
                        trigger,
                    } => {
                        let replacement = replace_multi_database_generation(
                            &config,
                            started_at,
                            database_index,
                            trigger,
                            &routes,
                            &mut hosted_mcp,
                            &mut graphs,
                            recovery,
                        )
                        .await;
                        if replacement.is_ok() {
                            monitors.push(spawn_multi_runtime_monitor(
                                database_index,
                                graphs[database_index].generation,
                                graphs[database_index].routing.clone(),
                                event_sender.clone(),
                            ));
                        } else {
                            quarantine_multi_database_generation(
                                database_index,
                                &routes,
                                &hosted_mcp,
                                &mut graphs,
                            )
                            .await?;
                        }
                        continue;
                    }
                }
            }
        };
        break selected;
    };
    drop(shutdown_thread);
    for monitor in monitors {
        monitor.abort();
    }
    if let Some(hosted) = hosted_mcp.as_mut() {
        hosted.begin_shutdown();
    }
    for graph in &graphs {
        graph.lifecycle.stop();
        if let Some(graph) = graph.graph.as_ref() {
            graph
                .begin_transport_shutdown()
                .map_err(|_| DaemonError::NotificationShutdown)?;
        }
    }
    let writer_evidence = graphs
        .iter()
        .filter_map(|graph| graph.graph.as_ref())
        .map(RunningProductionGraph::writer_evidence_snapshot)
        .collect::<Vec<_>>();
    let completion_lane_evidence = graphs
        .iter()
        .filter_map(|graph| graph.graph.as_ref())
        .map(RunningProductionGraph::completion_lane_evidence_snapshot)
        .collect::<Vec<_>>();
    if !matches!(stop, MultiDatabaseStop::TransportEnded) {
        transport.drain_after_signal().await?;
    }
    if let Some(hosted) = hosted_mcp.as_mut() {
        hosted
            .drain_after_signal()
            .await
            .map_err(DaemonError::McpStop)?;
    }
    for graph in graphs {
        if let Some(graph) = graph.graph {
            graph.shutdown().await.map_err(DaemonError::GraphShutdown)?;
        }
    }
    if matches!(stop, MultiDatabaseStop::Clean) {
        let stdout = io::stdout();
        let mut stdout = stdout.lock();
        for snapshot in &writer_evidence {
            let line = riffdb_observability::format_writer_evidence_v1_line(snapshot);
            let _ = writeln!(stdout, "{line}");
        }
        for snapshot in &completion_lane_evidence {
            let line = riffdb_observability::format_completion_lane_evidence_v1_line(snapshot);
            let _ = writeln!(stdout, "{line}");
        }
        let _ = stdout.flush();
    }
    match stop {
        MultiDatabaseStop::Clean => Ok(()),
        MultiDatabaseStop::TransportEnded => Err(DaemonError::TransportEnded),
        MultiDatabaseStop::McpEnded => Err(DaemonError::McpTransportEnded),
    }
}

fn spawn_multi_runtime_monitor(
    database_index: usize,
    generation: u64,
    routing: RuntimeRoutingState,
    events: tokio::sync::mpsc::Sender<MultiDatabaseEvent>,
) -> TokioJoinHandle<()> {
    tokio::spawn(async move {
        let _reason = routing.stopped().await;
        let _ = events
            .send(MultiDatabaseEvent::RuntimeStopped {
                database_index,
                generation,
            })
            .await;
    })
}

async fn quarantine_multi_database_generation(
    database_index: usize,
    routes: &GrpcDatabaseRoutes,
    hosted_mcp: &Option<HostedMcp>,
    graphs: &mut [MultiDatabaseGraph],
) -> Result<(), DaemonError> {
    let generation = graphs
        .get_mut(database_index)
        .ok_or(DaemonError::RuntimeStopped)?;
    generation.generation = generation
        .generation
        .checked_add(1)
        .ok_or(DaemonError::RuntimeStopped)?;
    generation.lifecycle.stop();
    let routing = RuntimeRoutingState::new();
    let (offline_service, _offline_activator, offline_issuer) =
        RiffDbService::begin_initialization();
    let offline_lifecycle = Arc::new(ProductionLifecycleRoute::new_with_maintenance(
        offline_service,
        offline_issuer,
        routing.clone(),
        Arc::clone(&generation.maintenance_lifecycle),
    ));
    let offline_route: Arc<dyn GrpcLifecycleRoute> = offline_lifecycle.clone();
    routes
        .replace(&generation.alias, offline_route)
        .map_err(|_| DaemonError::GrpcConfiguration)?;
    if let Some(hosted_mcp) = hosted_mcp {
        hosted_mcp
            .suspend(&generation.alias)
            .map_err(DaemonError::McpStart)?;
    }
    if let Some(graph) = generation.graph.take() {
        let _ = graph.begin_transport_shutdown();
        let _ = graph.shutdown().await;
    }
    generation.lifecycle = offline_lifecycle;
    generation.routing = routing;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn await_multi_restore_retry(
    config: &ServerConfig,
    database: &DatabaseConfig,
    startup: crate::startup::CheckedRedbStartup,
    receipt: OfflineMaintenanceReceiptV1,
    digest_keys: &ProductionDigestKeys,
    clocks: &ProductionWallClocks,
    identifiers: &ProductionIdentifierSources,
    lifecycle: Arc<ProductionLifecycleRoute>,
    maintenance_lifecycle: Arc<MaintenanceLifecycle>,
    maintenance: MaintenanceController,
    maintenance_receiver: &mut tokio::sync::mpsc::Receiver<MaintenanceTrigger>,
    routes: &GrpcDatabaseRoutes,
    process_signal: &mut ProductionShutdownSignal,
    transport: &mut HostedGrpc,
    recovery: &MaintenanceRecoveryController,
) -> Result<Option<MultiInitialRecovery>, DaemonError> {
    let operation_id = receipt.operation_id();
    let input_hash = receipt.input_hash();
    let routing = lifecycle.runtime_routing();
    // The narrow retry host discards the validated startup, so the target's
    // durable fence must be captured before the handoff. Without it the restore
    // driver has no monotonicity evidence if the target becomes unreadable.
    let retained_target_history_incarnation =
        Some(startup.retained_metadata().history_incarnation());
    let retry_host = RunningRestoreRetryHost::start(
        startup,
        operation_id,
        input_hash,
        digest_keys,
        config,
        database.environment(),
        clocks,
        maintenance.clone(),
        lifecycle,
        identifiers,
    )
    .map_err(DaemonError::RestoreRetryHostStart)?;

    let trigger = tokio::select! {
        signal = process_signal.received() => RestoreRetryProcessTrigger::Signal(signal),
        _reason = routing.stopped() => RestoreRetryProcessTrigger::Runtime,
        completion = &mut transport.task => RestoreRetryProcessTrigger::Transport(completion),
        maintenance = maintenance_receiver.recv() => {
            RestoreRetryProcessTrigger::Maintenance(maintenance)
        },
    };
    let RestoreRetryProcessTrigger::Maintenance(Some(
        mut trigger @ MaintenanceTrigger::RestoreBackup { .. },
    )) = trigger
    else {
        retry_host.begin_transport_shutdown();
        let host_result = retry_host.shutdown().await;
        host_result.map_err(DaemonError::RestoreRetryHostShutdown)?;
        return match trigger {
            RestoreRetryProcessTrigger::Signal(signal) => {
                signal.map_err(DaemonError::ShutdownSignal)?;
                Ok(None)
            }
            RestoreRetryProcessTrigger::Transport(completion) => {
                classify_transport_completion(&completion)?;
                Err(DaemonError::TransportEnded)
            }
            RestoreRetryProcessTrigger::Runtime
            | RestoreRetryProcessTrigger::Maintenance(None)
            | RestoreRetryProcessTrigger::Maintenance(Some(_)) => Err(DaemonError::RuntimeStopped),
        };
    };

    trigger
        .wait_for_start_ready()
        .await
        .map_err(|()| DaemonError::MaintenanceDriver)?;
    retry_host.begin_transport_shutdown();
    retry_host
        .shutdown()
        .await
        .map_err(DaemonError::RestoreRetryHostShutdown)?;
    let (offline_service, _offline_activator, offline_issuer) =
        RiffDbService::begin_initialization();
    let offline_lifecycle = Arc::new(ProductionLifecycleRoute::new_with_maintenance(
        offline_service,
        offline_issuer,
        RuntimeRoutingState::new(),
        Arc::clone(&maintenance_lifecycle),
    ));
    let offline_route: Arc<dyn GrpcLifecycleRoute> = offline_lifecycle;
    routes
        .replace(database.alias(), offline_route)
        .map_err(|_| DaemonError::GrpcConfiguration)?;
    if trigger.offline_operation_id() != Some(operation_id) {
        maintenance_lifecycle.fail_closed(operation_id);
        return Err(DaemonError::MaintenanceDriver);
    }

    let prepared = GenerationInputs::load(config)?;
    let request = normal_driver_request(trigger)?;
    let success = {
        let storage = maintenance.storage();
        let mut storage = storage.lock().map_err(|_| DaemonError::MaintenanceDriver)?;
        mark_offline(&mut storage, &maintenance_lifecycle, operation_id)
            .map_err(|_| DaemonError::MaintenanceDriver)?;
        let dependencies = maintenance_driver_dependencies(
            config,
            database.environment(),
            &prepared.startup_inputs,
            &prepared.digest_keys,
            &prepared.identifiers,
            &prepared.clocks,
            recovery,
            retained_target_history_incarnation,
            None,
        )?;
        run_offline_maintenance(&mut storage, &maintenance_lifecycle, &dependencies, request)
            .map_err(|_| DaemonError::MaintenanceDriver)?
    };
    let (_receipt, startup) = success.into_parts();
    Ok(Some(MultiInitialRecovery {
        operation_id,
        startup,
    }))
}

#[allow(clippy::too_many_arguments)]
async fn await_multi_recovery(
    config: &ServerConfig,
    database: &DatabaseConfig,
    identifiers: &ProductionIdentifierSources,
    lifecycle: Arc<ProductionLifecycleRoute>,
    maintenance_lifecycle: Arc<MaintenanceLifecycle>,
    maintenance: MaintenanceController,
    maintenance_receiver: &mut tokio::sync::mpsc::Receiver<MaintenanceTrigger>,
    process_signal: &mut ProductionShutdownSignal,
    transport: &mut HostedGrpc,
    recovery: &MaintenanceRecoveryController,
) -> Result<Option<MultiInitialRecovery>, DaemonError> {
    let routing = lifecycle.runtime_routing();
    let recovery_host = RunningRecoveryHost::start(maintenance.clone(), lifecycle, identifiers)
        .map_err(DaemonError::RecoveryHostStart)?;

    loop {
        let trigger = tokio::select! {
            signal = process_signal.received() => RecoveryProcessTrigger::Signal(signal),
            _reason = routing.stopped() => RecoveryProcessTrigger::Runtime,
            completion = &mut transport.task => RecoveryProcessTrigger::Transport(completion),
            maintenance = maintenance_receiver.recv() => {
                RecoveryProcessTrigger::Maintenance(maintenance)
            },
        };
        let RecoveryProcessTrigger::Maintenance(Some(MaintenanceTrigger::RecoveryRestore {
            restore,
            completion,
        })) = trigger
        else {
            recovery_host.begin_transport_shutdown();
            let host_result = recovery_host.shutdown().await;
            host_result.map_err(DaemonError::RecoveryHostShutdown)?;
            return match trigger {
                RecoveryProcessTrigger::Signal(signal) => {
                    signal.map_err(DaemonError::ShutdownSignal)?;
                    Ok(None)
                }
                RecoveryProcessTrigger::Transport(completion) => {
                    classify_transport_completion(&completion)?;
                    Err(DaemonError::TransportEnded)
                }
                RecoveryProcessTrigger::Runtime
                | RecoveryProcessTrigger::Maintenance(None)
                | RecoveryProcessTrigger::Maintenance(Some(_)) => Err(DaemonError::RuntimeStopped),
            };
        };

        let operation_id = restore.request().operation_id();
        let prepared = match GenerationInputs::load(config) {
            Ok(prepared) => prepared,
            Err(_) => {
                if maintenance_lifecycle
                    .release_recovery_restore(operation_id)
                    .is_err()
                {
                    let _ =
                        completion.complete(Err(RecoveryOfflineMaintenancePortError::Integrity));
                    recovery_host.begin_transport_shutdown();
                    recovery_host
                        .shutdown()
                        .await
                        .map_err(DaemonError::RecoveryHostShutdown)?;
                    return Err(DaemonError::MaintenanceDriver);
                }
                let _ = completion.complete(Err(RecoveryOfflineMaintenancePortError::Unavailable));
                continue;
            }
        };
        let (_request_id, request, credential) = restore.into_parts();
        let attempt = {
            let storage = maintenance.storage();
            let mut storage = match storage.lock() {
                Ok(storage) => storage,
                Err(poisoned) => {
                    drop(poisoned.into_inner());
                    maintenance_lifecycle.fail_closed(operation_id);
                    let _ =
                        completion.complete(Err(RecoveryOfflineMaintenancePortError::Integrity));
                    recovery_host.begin_transport_shutdown();
                    recovery_host
                        .shutdown()
                        .await
                        .map_err(DaemonError::RecoveryHostShutdown)?;
                    return Err(DaemonError::MaintenanceDriver);
                }
            };
            match storage.read_receipt(operation_id) {
                Ok(Some(receipt))
                    if !receipt_matches_restore_request(&receipt, &request)
                        || receipt.source_database_id().is_some() =>
                {
                    if maintenance_lifecycle
                        .release_recovery_restore(operation_id)
                        .is_err()
                    {
                        RecoveryAttempt::Stop(RecoveryOfflineMaintenancePortError::Integrity)
                    } else {
                        RecoveryAttempt::Retry(RecoveryOfflineMaintenancePortError::InputMismatch)
                    }
                }
                Err(_) => {
                    maintenance_lifecycle.fail_closed(operation_id);
                    RecoveryAttempt::Stop(RecoveryOfflineMaintenancePortError::OutcomeUnknown)
                }
                Ok(_) => match maintenance_driver_dependencies(
                    config,
                    database.environment(),
                    &prepared.startup_inputs,
                    &prepared.digest_keys,
                    &prepared.identifiers,
                    &prepared.clocks,
                    recovery,
                    None,
                    None,
                ) {
                    Ok(dependencies) => match run_recovery_restore(
                        &mut storage,
                        &maintenance_lifecycle,
                        &dependencies,
                        RecoveryMaintenanceDriverRequest::new(request, credential),
                    ) {
                        Ok(success) => RecoveryAttempt::Succeeded(Box::new(success)),
                        Err(failure) => classify_recovery_driver_failure(
                            &maintenance_lifecycle,
                            operation_id,
                            failure,
                        ),
                    },
                    Err(_) => {
                        maintenance_lifecycle.fail_closed(operation_id);
                        RecoveryAttempt::Stop(RecoveryOfflineMaintenancePortError::Integrity)
                    }
                },
            }
        };

        match attempt {
            RecoveryAttempt::Retry(error) => {
                let _ = completion.complete(Err(error));
            }
            RecoveryAttempt::Stop(error) => {
                let _ = completion.complete(Err(error));
                recovery_host.begin_transport_shutdown();
                recovery_host
                    .shutdown()
                    .await
                    .map_err(DaemonError::RecoveryHostShutdown)?;
                return Err(DaemonError::MaintenanceDriver);
            }
            RecoveryAttempt::Terminal(result) => {
                let _ = completion.complete(Ok(result));
                recovery_host.begin_transport_shutdown();
                recovery_host
                    .shutdown()
                    .await
                    .map_err(DaemonError::RecoveryHostShutdown)?;
                return Err(DaemonError::MaintenanceDriver);
            }
            RecoveryAttempt::Succeeded(success) => {
                let (receipt, startup) = (*success).into_parts();
                let crate::maintenance_driver::MaintenanceTerminalReceipt::V1(receipt) = receipt
                else {
                    return Err(DaemonError::MaintenanceDriver);
                };
                let result = start_result(OfflineMaintenanceStartDisposition::Terminal, &receipt)
                    .map_err(|_| DaemonError::MaintenanceDriver)?;
                let _ = completion.complete(Ok(result));
                recovery_host.begin_transport_shutdown();
                recovery_host
                    .shutdown()
                    .await
                    .map_err(DaemonError::RecoveryHostShutdown)?;
                return Ok(Some(MultiInitialRecovery {
                    operation_id,
                    startup,
                }));
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn replace_multi_database_generation(
    config: &ServerConfig,
    started_at: riffdb_types::Timestamp,
    database_index: usize,
    mut trigger: MaintenanceTrigger,
    routes: &GrpcDatabaseRoutes,
    hosted_mcp: &mut Option<HostedMcp>,
    graphs: &mut [MultiDatabaseGraph],
    recovery: &MaintenanceRecoveryController,
) -> Result<(), DaemonError> {
    let database = config
        .databases()
        .get(database_index)
        .ok_or(DaemonError::MaintenanceDriver)?;
    let generation = graphs
        .get_mut(database_index)
        .ok_or(DaemonError::MaintenanceDriver)?;
    trigger
        .wait_for_start_ready()
        .await
        .map_err(|()| DaemonError::MaintenanceDriver)?;
    let offline_operation_id = trigger.offline_operation_id();
    let migration_operation_id = trigger.migration_operation_id();
    if offline_operation_id.is_some() == migration_operation_id.is_some() {
        return Err(DaemonError::MaintenanceDriver);
    }
    generation.generation = generation
        .generation
        .checked_add(1)
        .ok_or(DaemonError::MaintenanceDriver)?;
    let retained_target_history_incarnation = generation.lifecycle.retained_history_incarnation();
    let retained_metrics = generation
        .graph
        .as_ref()
        .map(RunningProductionGraph::metrics);
    generation.lifecycle.stop();
    let (offline_service, _offline_activator, offline_issuer) =
        RiffDbService::begin_initialization();
    let offline_lifecycle = Arc::new(ProductionLifecycleRoute::new_with_maintenance(
        offline_service,
        offline_issuer,
        RuntimeRoutingState::new(),
        Arc::clone(&generation.maintenance_lifecycle),
    ));
    let offline_route: Arc<dyn GrpcLifecycleRoute> = offline_lifecycle.clone();
    routes
        .replace(&generation.alias, offline_route)
        .map_err(|_| DaemonError::GrpcConfiguration)?;
    generation.lifecycle = offline_lifecycle;
    if let Some(hosted_mcp) = hosted_mcp.as_ref() {
        hosted_mcp
            .suspend(&generation.alias)
            .map_err(DaemonError::McpStart)?;
    }
    let graph = generation
        .graph
        .take()
        .ok_or(DaemonError::MaintenanceDriver)?;
    graph
        .begin_transport_shutdown()
        .map_err(|_| DaemonError::NotificationShutdown)?;
    graph
        .shutdown_for_maintenance(recovery)
        .await
        .map_err(DaemonError::GraphShutdown)?;
    recovery.reached(MaintenanceRecoveryBoundary::DatabaseClosed);

    let prepared = GenerationInputs::load(config)?;
    let startup = {
        let storage = generation.maintenance.storage();
        let mut storage = storage.lock().map_err(|_| DaemonError::MaintenanceDriver)?;
        if let Some(operation_id) = migration_operation_id {
            generation
                .maintenance_lifecycle
                .mark_migration_offline(operation_id)
                .map_err(|_| DaemonError::MaintenanceDriver)?;
            let receipt = storage
                .read_contract_migration_receipt(operation_id)
                .map_err(DaemonError::MaintenanceStorage)?
                .ok_or(DaemonError::MaintenanceDriver)?;
            let build = contract_migration_backup_build_metadata()
                .map_err(|_| DaemonError::MaintenanceDriver)?;
            let database_ids = prepared.identifiers.database_ids();
            let inputs = MigrationDriverInputs {
                startup: prepared.startup_inputs.clone(),
                database_ids: &database_ids,
                commit_profile: config.redb_commit_profile(),
                build: &build,
            };
            let (_, startup) = drive_contract_migration(&storage, receipt, &inputs)
                .map_err(|_| DaemonError::MaintenanceDriver)?;
            generation
                .maintenance_lifecycle
                .mark_migration_validating(operation_id)
                .map_err(|_| DaemonError::MaintenanceDriver)?;
            startup
        } else {
            let operation_id = offline_operation_id.ok_or(DaemonError::MaintenanceDriver)?;
            let request = normal_driver_request(trigger)?;
            mark_offline(
                &mut storage,
                &generation.maintenance_lifecycle,
                operation_id,
            )
            .map_err(|_| DaemonError::MaintenanceDriver)?;
            let dependencies = maintenance_driver_dependencies(
                config,
                database.environment(),
                &prepared.startup_inputs,
                &prepared.digest_keys,
                &prepared.identifiers,
                &prepared.clocks,
                recovery,
                retained_target_history_incarnation,
                retained_metrics,
            )?;
            run_offline_maintenance(
                &mut storage,
                &generation.maintenance_lifecycle,
                &dependencies,
                request,
            )
            .map_err(|_| DaemonError::MaintenanceDriver)?
            .into_parts()
            .1
        }
    };

    let (initializing, activator, issuer) = RiffDbService::begin_initialization();
    let routing = RuntimeRoutingState::new();
    let lifecycle = Arc::new(ProductionLifecycleRoute::new_with_maintenance(
        initializing,
        issuer,
        routing.clone(),
        Arc::clone(&generation.maintenance_lifecycle),
    ));
    let build = build_info(&startup)?;
    let graph = ProductionGraphBuilder::new(
        startup,
        activator,
        prepared.digest_keys,
        config,
        database,
        started_at,
        build,
        prepared.identifiers,
        prepared.clocks,
        lifecycle.clone(),
        generation.maintenance.clone(),
    )
    .build()
    .map_err(DaemonError::GraphBuild)?;
    if let Some(hosted_mcp) = hosted_mcp.as_ref() {
        let dependencies = graph
            .hosted_mcp_dependencies()
            .ok_or(DaemonError::McpDependencies)?;
        hosted_mcp
            .replace(&generation.alias, dependencies)
            .map_err(DaemonError::McpStart)?;
    }
    let route: Arc<dyn GrpcLifecycleRoute> = lifecycle.clone();
    routes
        .replace(&generation.alias, route)
        .map_err(|_| DaemonError::GrpcConfiguration)?;
    if let Some(operation_id) = migration_operation_id {
        generation
            .maintenance_lifecycle
            .finish_migration_ready(operation_id)
            .map_err(|_| DaemonError::MaintenanceDriver)?;
        generation
            .maintenance
            .finish_migration(operation_id)
            .map_err(|()| DaemonError::MaintenanceDriver)?;
    } else {
        generation
            .maintenance_lifecycle
            .finish_ready(offline_operation_id.ok_or(DaemonError::MaintenanceDriver)?)
            .map_err(|_| DaemonError::MaintenanceDriver)?;
    }
    generation.graph = Some(graph);
    generation.lifecycle = lifecycle;
    generation.routing = routing;
    Ok(())
}

async fn shutdown_multi_before_ready(
    transport: &mut HostedGrpc,
    graphs: Vec<MultiDatabaseGraph>,
) -> Result<(), DaemonError> {
    for graph in &graphs {
        graph.lifecycle.stop();
        if let Some(graph) = graph.graph.as_ref() {
            let _ = graph.begin_transport_shutdown();
        }
    }
    transport.drain_after_signal().await?;
    for graph in graphs {
        if let Some(graph) = graph.graph {
            graph.shutdown().await.map_err(DaemonError::GraphShutdown)?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_ready_generations(
    config: &ServerConfig,
    started_at: riffdb_types::Timestamp,
    mut generation: ReadyGeneration,
    publish_initial: bool,
    maintenance_lifecycle: Arc<MaintenanceLifecycle>,
    maintenance: MaintenanceController,
    maintenance_receiver: &mut tokio::sync::mpsc::Receiver<MaintenanceTrigger>,
    process_signal: &mut ProductionShutdownSignal,
    recovery: &MaintenanceRecoveryController,
) -> Result<(), DaemonError> {
    if process_signal
        .take_pending()
        .await
        .map_err(DaemonError::ShutdownSignal)?
    {
        shutdown_before_ready(
            generation.graph,
            &mut generation.transport,
            &mut generation.hosted_mcp,
        )
        .await?;
        return Ok(());
    }
    if publish_initial && let Err(source) = publish_readiness(generation.transport.endpoint()) {
        shutdown_before_ready(
            generation.graph,
            &mut generation.transport,
            &mut generation.hosted_mcp,
        )
        .await?;
        return Err(source);
    }
    let (shutdown, stdin_thread) = match spawn_shutdown_reader() {
        Ok(reader) => reader,
        Err(source) => {
            shutdown_before_ready(
                generation.graph,
                &mut generation.transport,
                &mut generation.hosted_mcp,
            )
            .await?;
            return Err(source);
        }
    };
    let listener_config = generation.transport.rebind_config();
    let mut shutdown = Some(shutdown);
    let mut stdin_thread = Some(stdin_thread);

    loop {
        let retained_target_history_incarnation = generation.graph.retained_history_incarnation();
        let retained_metrics = Some(generation.graph.metrics());
        let completion = supervise_ready_process(
            generation.graph,
            generation.routing,
            generation.transport,
            generation.hosted_mcp,
            &mut shutdown,
            &mut stdin_thread,
            process_signal,
            maintenance_receiver,
            recovery,
        )
        .await?;
        let ReadyProcessCompletion::Maintenance(trigger) = completion else {
            return Ok(());
        };
        let offline_operation_id = trigger.offline_operation_id();
        let migration_operation_id = trigger.migration_operation_id();
        if offline_operation_id.is_some() == migration_operation_id.is_some() {
            return Err(DaemonError::MaintenanceDriver);
        }
        let prepared = GenerationInputs::load(config)?;
        let startup = {
            let storage = maintenance.storage();
            let mut storage = storage.lock().map_err(|_| DaemonError::MaintenanceDriver)?;
            if let Some(operation_id) = migration_operation_id {
                maintenance_lifecycle
                    .mark_migration_offline(operation_id)
                    .map_err(|_| DaemonError::MaintenanceDriver)?;
                let receipt = storage
                    .read_contract_migration_receipt(operation_id)
                    .map_err(DaemonError::MaintenanceStorage)?
                    .ok_or(DaemonError::MaintenanceDriver)?;
                let build = contract_migration_backup_build_metadata()
                    .map_err(|_| DaemonError::MaintenanceDriver)?;
                let database_ids = prepared.identifiers.database_ids();
                let inputs = MigrationDriverInputs {
                    startup: prepared.startup_inputs.clone(),
                    database_ids: &database_ids,
                    commit_profile: config.redb_commit_profile(),
                    build: &build,
                };
                let (_, startup) = drive_contract_migration(&storage, receipt, &inputs)
                    .map_err(|_| DaemonError::MaintenanceDriver)?;
                maintenance_lifecycle
                    .mark_migration_validating(operation_id)
                    .map_err(|_| DaemonError::MaintenanceDriver)?;
                startup
            } else {
                let operation_id = offline_operation_id.ok_or(DaemonError::MaintenanceDriver)?;
                let request = normal_driver_request(trigger)?;
                mark_offline(&mut storage, &maintenance_lifecycle, operation_id)
                    .map_err(|_| DaemonError::MaintenanceDriver)?;
                let dependencies = maintenance_driver_dependencies(
                    config,
                    config.environment(),
                    &prepared.startup_inputs,
                    &prepared.digest_keys,
                    &prepared.identifiers,
                    &prepared.clocks,
                    recovery,
                    retained_target_history_incarnation,
                    retained_metrics,
                )?;
                run_offline_maintenance(
                    &mut storage,
                    &maintenance_lifecycle,
                    &dependencies,
                    request,
                )
                .map_err(|_| DaemonError::MaintenanceDriver)?
                .into_parts()
                .1
            }
        };
        generation = start_generation(
            config,
            listener_config.clone(),
            started_at,
            startup,
            prepared,
            Arc::clone(&maintenance_lifecycle),
            maintenance.clone(),
        )
        .await?;
        if process_signal
            .take_pending()
            .await
            .map_err(DaemonError::ShutdownSignal)?
        {
            shutdown_before_ready(
                generation.graph,
                &mut generation.transport,
                &mut generation.hosted_mcp,
            )
            .await?;
            return Ok(());
        }
        if let Some(operation_id) = migration_operation_id {
            maintenance_lifecycle
                .finish_migration_ready(operation_id)
                .map_err(|_| DaemonError::MaintenanceDriver)?;
            maintenance
                .finish_migration(operation_id)
                .map_err(|()| DaemonError::MaintenanceDriver)?;
        } else {
            maintenance_lifecycle
                .finish_ready(offline_operation_id.ok_or(DaemonError::MaintenanceDriver)?)
                .map_err(|_| DaemonError::MaintenanceDriver)?;
        }
        if let Err(source) = publish_readiness(generation.transport.endpoint()) {
            shutdown_before_ready(
                generation.graph,
                &mut generation.transport,
                &mut generation.hosted_mcp,
            )
            .await?;
            return Err(source);
        }
    }
}

fn normal_driver_request(
    trigger: MaintenanceTrigger,
) -> Result<MaintenanceDriverRequest, DaemonError> {
    match trigger {
        MaintenanceTrigger::CreateBackup { request, .. } => Ok(
            MaintenanceDriverRequest::create_backup(request.operation_id()),
        ),
        MaintenanceTrigger::RestoreBackup {
            request,
            credential,
            ..
        } => Ok(MaintenanceDriverRequest::restore_backup(
            request.operation_id(),
            credential,
        )),
        MaintenanceTrigger::RetireBackup { request, .. } => Ok(
            MaintenanceDriverRequest::retire_backup(request.operation_id()),
        ),
        MaintenanceTrigger::RecoveryRestore { .. }
        | MaintenanceTrigger::ContractMigrationApply { .. } => Err(DaemonError::MaintenanceDriver),
    }
}

fn trusted_audiences(config: &ServerConfig) -> Result<TrustedAudienceCatalog, DaemonError> {
    let mut audiences = vec![config.audience().clone()];
    audiences.extend(config.mcp_audience().cloned());
    TrustedAudienceCatalog::new(audiences).map_err(|_| DaemonError::MaintenanceDriver)
}

#[allow(clippy::too_many_arguments)]
fn maintenance_driver_dependencies<'a>(
    config: &ServerConfig,
    environment: &riffdb_types::Environment,
    startup_inputs: &StartupValidationInputs,
    digest_keys: &ProductionDigestKeys,
    identifiers: &ProductionIdentifierSources,
    clocks: &'a ProductionWallClocks,
    recovery: &'a MaintenanceRecoveryController,
    retained_target_history_incarnation: Option<u64>,
    metrics: Option<riffdb_observability::MetricRegistry>,
) -> Result<MaintenanceDriverDependencies<'a>, DaemonError> {
    Ok(MaintenanceDriverDependencies::new(
        startup_inputs.clone(),
        identifiers.database_ids(),
        config.redb_commit_profile(),
        digest_keys.shared_capability(),
        environment.clone(),
        config.audience().clone(),
        trusted_audiences(config)?,
        clocks,
        Arc::new(NoopAuthenticationTelemetry),
        Arc::new(NoopAuthorizationTelemetry),
        recovery,
        retained_target_history_incarnation,
        metrics,
    )
    .with_archives(
        config
            .archive_bindings()
            .map_err(|_| DaemonError::MaintenanceDriver)?,
    ))
}

fn startup_failure_allows_recovery(error: &RedbStartupError) -> bool {
    match error {
        RedbStartupError::Format(_) => false,
        RedbStartupError::Storage(error) => matches!(
            error.kind(),
            StorageErrorKind::CorruptData | StorageErrorKind::IncompatibleFormat
        ),
        RedbStartupError::Catalog(error) => error.storage_kind().is_some_and(|kind| {
            matches!(
                kind,
                StorageErrorKind::CorruptData | StorageErrorKind::IncompatibleFormat
            )
        }),
        RedbStartupError::Integrity(_) => true,
        RedbStartupError::Identifier(_) => false,
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_restore_retry_until_ready(
    config: &ServerConfig,
    started_at: riffdb_types::Timestamp,
    startup: crate::startup::CheckedRedbStartup,
    receipt: OfflineMaintenanceReceiptV1,
    mut transport: HostedGrpc,
    lifecycle: Arc<ProductionLifecycleRoute>,
    maintenance_lifecycle: Arc<MaintenanceLifecycle>,
    maintenance: MaintenanceController,
    maintenance_receiver: &mut tokio::sync::mpsc::Receiver<MaintenanceTrigger>,
    process_signal: &mut ProductionShutdownSignal,
    digest_keys: &ProductionDigestKeys,
    clocks: &ProductionWallClocks,
    identifiers: &ProductionIdentifierSources,
    recovery: &MaintenanceRecoveryController,
) -> Result<(), DaemonError> {
    let operation_id = receipt.operation_id();
    let input_hash = receipt.input_hash();
    let listener_config = transport.rebind_config();
    let routing = lifecycle.runtime_routing();
    // The narrow retry host discards the validated startup, so the target's
    // durable fence must be captured before the handoff. Without it the restore
    // driver has no monotonicity evidence if the target becomes unreadable.
    let retained_target_history_incarnation =
        Some(startup.retained_metadata().history_incarnation());
    let retry_host = match RunningRestoreRetryHost::start(
        startup,
        operation_id,
        input_hash,
        digest_keys,
        config,
        config.environment(),
        clocks,
        maintenance.clone(),
        Arc::clone(&lifecycle),
        identifiers,
    ) {
        Ok(host) => host,
        Err(source) => {
            lifecycle.stop();
            transport.drain_after_signal().await?;
            drop(lifecycle);
            return Err(DaemonError::RestoreRetryHostStart(source));
        }
    };

    let trigger = tokio::select! {
        signal = process_signal.received() => RestoreRetryProcessTrigger::Signal(signal),
        _reason = routing.stopped() => RestoreRetryProcessTrigger::Runtime,
        completion = &mut transport.task => RestoreRetryProcessTrigger::Transport(completion),
        maintenance = maintenance_receiver.recv() => {
            RestoreRetryProcessTrigger::Maintenance(maintenance)
        },
    };

    let RestoreRetryProcessTrigger::Maintenance(Some(
        trigger @ MaintenanceTrigger::RestoreBackup { .. },
    )) = trigger
    else {
        retry_host.begin_transport_shutdown();
        return match trigger {
            RestoreRetryProcessTrigger::Signal(signal) => {
                let transport_result = transport.drain_after_signal().await;
                let host_result = retry_host.shutdown().await;
                drop(lifecycle);
                transport_result?;
                host_result.map_err(DaemonError::RestoreRetryHostShutdown)?;
                signal.map_err(DaemonError::ShutdownSignal)
            }
            RestoreRetryProcessTrigger::Transport(completion) => {
                let transport_result = classify_transport_completion(&completion);
                let host_result = retry_host.shutdown().await;
                drop(lifecycle);
                transport_result?;
                host_result.map_err(DaemonError::RestoreRetryHostShutdown)?;
                Err(DaemonError::TransportEnded)
            }
            RestoreRetryProcessTrigger::Runtime
            | RestoreRetryProcessTrigger::Maintenance(None)
            | RestoreRetryProcessTrigger::Maintenance(Some(_)) => {
                let transport_result = transport.drain_after_signal().await;
                let host_result = retry_host.shutdown().await;
                drop(lifecycle);
                transport_result?;
                host_result.map_err(DaemonError::RestoreRetryHostShutdown)?;
                Err(DaemonError::RuntimeStopped)
            }
        };
    };

    if trigger.offline_operation_id() != Some(operation_id) {
        retry_host.begin_transport_shutdown();
        transport.drain_after_signal().await?;
        retry_host
            .shutdown()
            .await
            .map_err(DaemonError::RestoreRetryHostShutdown)?;
        drop(lifecycle);
        maintenance_lifecycle.fail_closed(operation_id);
        return Err(DaemonError::MaintenanceDriver);
    }

    retry_host.begin_transport_shutdown();
    transport.drain_after_signal().await?;
    retry_host
        .shutdown()
        .await
        .map_err(DaemonError::RestoreRetryHostShutdown)?;
    drop(lifecycle);

    let prepared = GenerationInputs::load(config)?;
    let request = normal_driver_request(trigger)?;
    let driver_result = {
        let storage = maintenance.storage();
        let mut storage = storage.lock().map_err(|_| DaemonError::MaintenanceDriver)?;
        mark_offline(&mut storage, &maintenance_lifecycle, operation_id)
            .map_err(|_| DaemonError::MaintenanceDriver)?;
        let dependencies = maintenance_driver_dependencies(
            config,
            config.environment(),
            &prepared.startup_inputs,
            &prepared.digest_keys,
            &prepared.identifiers,
            &prepared.clocks,
            recovery,
            retained_target_history_incarnation,
            None,
        )?;
        run_offline_maintenance(&mut storage, &maintenance_lifecycle, &dependencies, request)
            .map_err(|_| DaemonError::MaintenanceDriver)?
    };
    let (_terminal_receipt, startup) = driver_result.into_parts();
    let mut generation = start_generation(
        config,
        listener_config,
        started_at,
        startup,
        prepared,
        Arc::clone(&maintenance_lifecycle),
        maintenance.clone(),
    )
    .await?;
    if process_signal
        .take_pending()
        .await
        .map_err(DaemonError::ShutdownSignal)?
    {
        shutdown_before_ready(
            generation.graph,
            &mut generation.transport,
            &mut generation.hosted_mcp,
        )
        .await?;
        return Ok(());
    }
    maintenance_lifecycle
        .finish_ready(operation_id)
        .map_err(|_| DaemonError::MaintenanceDriver)?;
    run_ready_generations(
        config,
        started_at,
        generation,
        true,
        maintenance_lifecycle,
        maintenance,
        maintenance_receiver,
        process_signal,
        recovery,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_recovery_until_ready(
    config: &ServerConfig,
    started_at: riffdb_types::Timestamp,
    mut transport: HostedGrpc,
    lifecycle: Arc<ProductionLifecycleRoute>,
    maintenance_lifecycle: Arc<MaintenanceLifecycle>,
    maintenance: MaintenanceController,
    maintenance_receiver: &mut tokio::sync::mpsc::Receiver<MaintenanceTrigger>,
    process_signal: &mut ProductionShutdownSignal,
    host_identifiers: &ProductionIdentifierSources,
    recovery: &MaintenanceRecoveryController,
) -> Result<(), DaemonError> {
    let listener_config = transport.rebind_config();
    let routing = lifecycle.runtime_routing();
    let recovery_host = RunningRecoveryHost::start(
        maintenance.clone(),
        Arc::clone(&lifecycle),
        host_identifiers,
    )
    .map_err(DaemonError::RecoveryHostStart)?;

    loop {
        let trigger = tokio::select! {
            signal = process_signal.received() => RecoveryProcessTrigger::Signal(signal),
            _reason = routing.stopped() => RecoveryProcessTrigger::Runtime,
            completion = &mut transport.task => RecoveryProcessTrigger::Transport(completion),
            maintenance = maintenance_receiver.recv() => {
                RecoveryProcessTrigger::Maintenance(maintenance)
            },
        };
        let RecoveryProcessTrigger::Maintenance(Some(MaintenanceTrigger::RecoveryRestore {
            restore,
            completion,
        })) = trigger
        else {
            recovery_host.begin_transport_shutdown();
            return match trigger {
                RecoveryProcessTrigger::Signal(signal) => {
                    let transport_result = transport.drain_after_signal().await;
                    let host_result = recovery_host.shutdown().await;
                    transport_result?;
                    host_result.map_err(DaemonError::RecoveryHostShutdown)?;
                    signal.map_err(DaemonError::ShutdownSignal)
                }
                RecoveryProcessTrigger::Transport(completion) => {
                    let transport_result = classify_transport_completion(&completion);
                    let host_result = recovery_host.shutdown().await;
                    transport_result?;
                    host_result.map_err(DaemonError::RecoveryHostShutdown)?;
                    Err(DaemonError::TransportEnded)
                }
                RecoveryProcessTrigger::Runtime
                | RecoveryProcessTrigger::Maintenance(None)
                | RecoveryProcessTrigger::Maintenance(Some(_)) => {
                    let transport_result = transport.drain_after_signal().await;
                    let host_result = recovery_host.shutdown().await;
                    transport_result?;
                    host_result.map_err(DaemonError::RecoveryHostShutdown)?;
                    Err(DaemonError::RuntimeStopped)
                }
            };
        };

        let operation_id = restore.request().operation_id();
        let prepared = match GenerationInputs::load(config) {
            Ok(prepared) => prepared,
            Err(_) => {
                if maintenance_lifecycle
                    .release_recovery_restore(operation_id)
                    .is_err()
                {
                    let _ =
                        completion.complete(Err(RecoveryOfflineMaintenancePortError::Integrity));
                    recovery_host.begin_transport_shutdown();
                    transport.drain_after_signal().await?;
                    recovery_host
                        .shutdown()
                        .await
                        .map_err(DaemonError::RecoveryHostShutdown)?;
                    return Err(DaemonError::MaintenanceDriver);
                }
                let _ = completion.complete(Err(RecoveryOfflineMaintenancePortError::Unavailable));
                continue;
            }
        };
        let (_request_id, request, credential) = restore.into_parts();
        let attempt = {
            let storage = maintenance.storage();
            let mut storage = match storage.lock() {
                Ok(storage) => storage,
                Err(poisoned) => {
                    drop(poisoned.into_inner());
                    maintenance_lifecycle.fail_closed(operation_id);
                    let _ =
                        completion.complete(Err(RecoveryOfflineMaintenancePortError::Integrity));
                    recovery_host.begin_transport_shutdown();
                    transport.drain_after_signal().await?;
                    recovery_host
                        .shutdown()
                        .await
                        .map_err(DaemonError::RecoveryHostShutdown)?;
                    return Err(DaemonError::MaintenanceDriver);
                }
            };
            match storage.read_receipt(operation_id) {
                Ok(Some(receipt))
                    if !receipt_matches_restore_request(&receipt, &request)
                        || receipt.source_database_id().is_some() =>
                {
                    if maintenance_lifecycle
                        .release_recovery_restore(operation_id)
                        .is_err()
                    {
                        RecoveryAttempt::Stop(RecoveryOfflineMaintenancePortError::Integrity)
                    } else {
                        RecoveryAttempt::Retry(RecoveryOfflineMaintenancePortError::InputMismatch)
                    }
                }
                Err(_) => {
                    maintenance_lifecycle.fail_closed(operation_id);
                    RecoveryAttempt::Stop(RecoveryOfflineMaintenancePortError::OutcomeUnknown)
                }
                Ok(_) => match maintenance_driver_dependencies(
                    config,
                    config.environment(),
                    &prepared.startup_inputs,
                    &prepared.digest_keys,
                    &prepared.identifiers,
                    &prepared.clocks,
                    recovery,
                    None,
                    None,
                ) {
                    Ok(dependencies) => match run_recovery_restore(
                        &mut storage,
                        &maintenance_lifecycle,
                        &dependencies,
                        RecoveryMaintenanceDriverRequest::new(request, credential),
                    ) {
                        Ok(success) => RecoveryAttempt::Succeeded(Box::new(success)),
                        Err(failure) => classify_recovery_driver_failure(
                            &maintenance_lifecycle,
                            operation_id,
                            failure,
                        ),
                    },
                    Err(_) => {
                        maintenance_lifecycle.fail_closed(operation_id);
                        RecoveryAttempt::Stop(RecoveryOfflineMaintenancePortError::Integrity)
                    }
                },
            }
        };

        match attempt {
            RecoveryAttempt::Retry(error) => {
                let _ = completion.complete(Err(error));
            }
            RecoveryAttempt::Stop(error) => {
                let _ = completion.complete(Err(error));
                recovery_host.begin_transport_shutdown();
                transport.drain_after_signal().await?;
                recovery_host
                    .shutdown()
                    .await
                    .map_err(DaemonError::RecoveryHostShutdown)?;
                return Err(DaemonError::MaintenanceDriver);
            }
            RecoveryAttempt::Terminal(result) => {
                let _ = completion.complete(Ok(result));
                recovery_host.begin_transport_shutdown();
                transport.drain_after_signal().await?;
                recovery_host
                    .shutdown()
                    .await
                    .map_err(DaemonError::RecoveryHostShutdown)?;
                return Err(DaemonError::MaintenanceDriver);
            }
            RecoveryAttempt::Succeeded(success) => {
                let (receipt, startup) = (*success).into_parts();
                let crate::maintenance_driver::MaintenanceTerminalReceipt::V1(receipt) = receipt
                else {
                    return Err(DaemonError::MaintenanceDriver);
                };
                let result = start_result(OfflineMaintenanceStartDisposition::Terminal, &receipt)
                    .map_err(|_| DaemonError::MaintenanceDriver)?;
                let _ = completion.complete(Ok(result));
                recovery_host.begin_transport_shutdown();
                transport.drain_after_signal().await?;
                recovery_host
                    .shutdown()
                    .await
                    .map_err(DaemonError::RecoveryHostShutdown)?;
                drop(lifecycle);

                let generation = start_generation(
                    config,
                    listener_config.clone(),
                    started_at,
                    startup,
                    prepared,
                    Arc::clone(&maintenance_lifecycle),
                    maintenance.clone(),
                )
                .await?;
                maintenance_lifecycle
                    .finish_ready(operation_id)
                    .map_err(|_| DaemonError::MaintenanceDriver)?;
                return run_ready_generations(
                    config,
                    started_at,
                    generation,
                    true,
                    maintenance_lifecycle,
                    maintenance,
                    maintenance_receiver,
                    process_signal,
                    recovery,
                )
                .await;
            }
        }
    }
}

fn classify_recovery_driver_failure(
    lifecycle: &MaintenanceLifecycle,
    operation_id: riffdb_types::OfflineMaintenanceOperationId,
    failure: MaintenanceDriverFailure,
) -> RecoveryAttempt {
    if failure.operation_id() != operation_id {
        lifecycle.fail_closed(operation_id);
        return RecoveryAttempt::Stop(RecoveryOfflineMaintenancePortError::Integrity);
    }
    if let Some(receipt) = failure.terminal_receipt() {
        return match start_result(OfflineMaintenanceStartDisposition::Terminal, receipt) {
            Ok(result) => RecoveryAttempt::Terminal(result),
            Err(_) => RecoveryAttempt::Stop(RecoveryOfflineMaintenancePortError::Integrity),
        };
    }
    let (error, may_retry) = match failure.failure() {
        OfflineMaintenanceReceiptFailureV1::StagedAuthorizationFailed => (
            RecoveryOfflineMaintenancePortError::AuthorizationDenied,
            true,
        ),
        OfflineMaintenanceReceiptFailureV1::ArtifactUnavailable
        | OfflineMaintenanceReceiptFailureV1::StorageUnavailable => {
            (RecoveryOfflineMaintenancePortError::Unavailable, true)
        }
        OfflineMaintenanceReceiptFailureV1::ArtifactInvalid
        | OfflineMaintenanceReceiptFailureV1::ValidationFailed => {
            (RecoveryOfflineMaintenancePortError::Integrity, true)
        }
        OfflineMaintenanceReceiptFailureV1::ReceiptUnavailable => {
            (RecoveryOfflineMaintenancePortError::OutcomeUnknown, false)
        }
        OfflineMaintenanceReceiptFailureV1::QuiescenceFailed
        | OfflineMaintenanceReceiptFailureV1::InternalFailure => {
            (RecoveryOfflineMaintenancePortError::Integrity, false)
        }
    };
    if may_retry && lifecycle.release_recovery_restore(operation_id).is_ok() {
        RecoveryAttempt::Retry(error)
    } else {
        lifecycle.fail_closed(operation_id);
        RecoveryAttempt::Stop(error)
    }
}

#[allow(clippy::too_many_arguments)]
async fn start_generation(
    config: &ServerConfig,
    listener_config: ApplicationListenerConfig,
    started_at: riffdb_types::Timestamp,
    startup: crate::startup::CheckedRedbStartup,
    inputs: GenerationInputs,
    maintenance_lifecycle: Arc<MaintenanceLifecycle>,
    maintenance: MaintenanceController,
) -> Result<ReadyGeneration, DaemonError> {
    let GenerationInputs {
        digest_keys,
        startup_inputs: _,
        identifiers,
        clocks,
    } = inputs;
    let build = build_info(&startup)?;
    let (initializing, activator, issuer) = RiffDbService::begin_initialization();
    let routing = RuntimeRoutingState::new();
    let lifecycle = Arc::new(ProductionLifecycleRoute::new_with_maintenance(
        initializing,
        issuer,
        routing.clone(),
        maintenance_lifecycle,
    ));
    let lifecycle_for_grpc: Arc<dyn GrpcLifecycleRoute> = lifecycle.clone();
    let limits = GrpcRequestLimits::new(REQUEST_DURATION_LIMIT)
        .map_err(|_| DaemonError::GrpcConfiguration)?;
    let application =
        GrpcApplication::new_with_audience(lifecycle_for_grpc, limits, config.audience().clone());
    let mut transport = HostedGrpc::bind(listener_config, &application)?;
    let graph = match ProductionGraphBuilder::new(
        startup,
        activator,
        digest_keys,
        config,
        &config.databases()[0],
        started_at,
        build,
        identifiers,
        clocks,
        lifecycle.clone(),
        maintenance,
    )
    .build()
    {
        Ok(graph) => graph,
        Err(source) => {
            lifecycle.stop();
            transport.drain_after_signal().await?;
            return Err(DaemonError::GraphBuild(source));
        }
    };
    let mut hosted_mcp = match config.mcp_listen_address() {
        Some(address) => {
            let Some(dependencies) = graph.hosted_mcp_dependencies() else {
                let mut no_hosted_mcp = None;
                shutdown_before_ready(graph, &mut transport, &mut no_hosted_mcp).await?;
                return Err(DaemonError::McpDependencies);
            };
            match HostedMcp::bind(address, config.mcp_origins(), dependencies).await {
                Ok(hosted) => Some(hosted),
                Err(source) => {
                    let mut no_hosted_mcp = None;
                    shutdown_before_ready(graph, &mut transport, &mut no_hosted_mcp).await?;
                    return Err(DaemonError::McpStart(source));
                }
            }
        }
        None => None,
    };
    if transport.is_finished() || hosted_mcp.as_ref().is_some_and(HostedMcp::is_finished) {
        let grpc_finished = transport.is_finished();
        let mcp_finished = hosted_mcp.as_ref().is_some_and(HostedMcp::is_finished);
        if let Some(hosted_mcp) = hosted_mcp.as_mut() {
            hosted_mcp.begin_shutdown();
        }
        let notification_stop_failed = graph.begin_transport_shutdown().is_err();
        let transport_result = if grpc_finished {
            transport.completed().await
        } else {
            transport.drain_after_signal().await
        };
        let mcp_result = match hosted_mcp.as_mut() {
            Some(hosted_mcp) if mcp_finished => hosted_mcp.completed().await,
            Some(hosted_mcp) => hosted_mcp.drain_after_signal().await,
            None => Ok(()),
        };
        let graph_result = graph.shutdown().await;
        if notification_stop_failed {
            return Err(DaemonError::NotificationShutdown);
        }
        graph_result.map_err(DaemonError::GraphShutdown)?;
        transport_result?;
        mcp_result.map_err(DaemonError::McpStop)?;
        return Err(if grpc_finished {
            DaemonError::TransportEnded
        } else {
            DaemonError::McpTransportEnded
        });
    }
    Ok(ReadyGeneration {
        graph,
        routing,
        transport,
        hosted_mcp,
    })
}

async fn shutdown_before_ready(
    graph: RunningProductionGraph,
    transport: &mut HostedGrpc,
    hosted_mcp: &mut Option<HostedMcp>,
) -> Result<(), DaemonError> {
    if let Some(hosted_mcp) = hosted_mcp.as_mut() {
        hosted_mcp.begin_shutdown();
    }
    let notification_stop_failed = graph.begin_transport_shutdown().is_err();
    let transport_result = transport.drain_after_signal().await;
    let mcp_result = match hosted_mcp.as_mut() {
        Some(hosted_mcp) => hosted_mcp.drain_after_signal().await,
        None => Ok(()),
    };
    transport_result?;
    mcp_result.map_err(DaemonError::McpStop)?;
    graph.shutdown().await.map_err(DaemonError::GraphShutdown)?;
    if notification_stop_failed {
        Err(DaemonError::NotificationShutdown)
    } else {
        Ok(())
    }
}

fn load_production_digest_keys(config: &ServerConfig) -> Result<ProductionDigestKeys, DaemonError> {
    load_digest_key_providers(
        None,
        Some(config.capability_key_path()),
        None,
        Some(config.idempotency_key_path()),
    )
    .map(ProductionDigestKeys::new)
    .map_err(|_| DaemonError::DigestKeys)
}

fn startup_validation_inputs(
    digest_keys: &ProductionDigestKeys,
    authorization_time: riffdb_types::Timestamp,
) -> Result<StartupValidationInputs, DaemonError> {
    let capability = ReadableCapabilityDigestInventory::new(
        digest_keys
            .capability()
            .readable_key_ids()
            .map(ReadableDigestKey::v1)
            .collect(),
    )
    .map_err(DaemonError::StartupInventory)?;
    let idempotency = ReadableIdempotencyDigestInventory::new(
        digest_keys
            .idempotency()
            .readable_key_ids()
            .map(ReadableDigestKey::v1)
            .collect(),
    )
    .map_err(DaemonError::StartupInventory)?;
    Ok(StartupValidationInputs::new(
        authorization_time,
        capability,
        idempotency,
    ))
}

fn build_info(startup: &crate::startup::CheckedRedbStartup) -> Result<BuildInfo, DaemonError> {
    build_info_for_format(startup.retained_metadata().storage_format_version().get())
}
fn build_info_for_format(format_version: u32) -> Result<BuildInfo, DaemonError> {
    // Release packaging supplies RIFFDB_GIT_REVISION; the fallback is truthful for local builds.
    BuildInfo::new(
        env!("CARGO_PKG_VERSION"),
        option_env!("RIFFDB_GIT_REVISION").unwrap_or("development-unversioned"),
        "rustc-1.97.0",
        Vec::new(),
        format_version,
        EXECUTABLE_IR_VERSION_V1,
        MCP_PROTOCOL_VERSION,
    )
    .map_err(|_| DaemonError::BuildInfo)
}

#[allow(clippy::too_many_arguments)]
async fn supervise_ready_process(
    graph: RunningProductionGraph,
    routing: RuntimeRoutingState,
    mut transport: HostedGrpc,
    mut hosted_mcp: Option<HostedMcp>,
    shutdown: &mut Option<ShutdownReceiver>,
    stdin_thread: &mut Option<JoinHandle<()>>,
    process_signal: &mut ProductionShutdownSignal,
    maintenance_receiver: &mut tokio::sync::mpsc::Receiver<MaintenanceTrigger>,
    recovery: &MaintenanceRecoveryController,
) -> Result<ReadyProcessCompletion, DaemonError> {
    let trigger = loop {
        let selected = tokio::select! {
            command = wait_for_shutdown_input(shutdown) => command,
            signal = process_signal.received() => Some(ReadyProcessTrigger::Signal(signal)),
            reason = routing.stopped() => Some(ReadyProcessTrigger::Runtime(reason)),
            completion = &mut transport.task => Some(ReadyProcessTrigger::Transport(completion)),
            completion = wait_for_hosted_mcp(&mut hosted_mcp) => {
                Some(ReadyProcessTrigger::McpTransport(completion))
            },
            trigger = maintenance_receiver.recv() => {
                Some(trigger.map_or(
                    ReadyProcessTrigger::Runtime(RuntimeStopReason::SupervisionStateCorrupted),
                    ReadyProcessTrigger::Maintenance,
                ))
            },
        };
        if let Some(trigger) = selected {
            break trigger;
        }
    };

    if let Some(hosted_mcp) = hosted_mcp.as_mut() {
        hosted_mcp.begin_shutdown();
    }
    let write_completion_groups = graph.write_completion_group_snapshot();
    let dispatch_reasons = graph.command_group_dispatch_snapshot();
    let read_stages = graph.read_stage_snapshot();
    let write_service_stages = graph.write_service_stage_snapshot();
    let command_stages = graph.command_stage_snapshot();
    let writer_evidence = graph.writer_evidence_snapshot();
    let completion_lane_evidence = graph.completion_lane_evidence_snapshot();
    let writer_frame_census = riffdb_storage_redb::writer_command_frame_census_v1();
    let writer_flush_census = riffdb_storage_redb::writer_command_flush_census_v1();
    let writer_journal_stage_census = riffdb_storage_redb::writer_journal_stage_census_v1();
    let writer_publication_stage_census = riffdb_storage_redb::writer_publication_stage_census_v1();
    let query_execute_census = riffdb_storage_redb::query_execute_census_v1();
    let writer_batch_census = riffdb_commit::writer_batch_stage_census_v1();
    let notification_stop_failed = graph.begin_transport_shutdown().is_err();
    let transport_result = match &trigger {
        ReadyProcessTrigger::Transport(completion) => classify_transport_completion(completion),
        ReadyProcessTrigger::Command
        | ReadyProcessTrigger::ShutdownInputFailure
        | ReadyProcessTrigger::Signal(_)
        | ReadyProcessTrigger::Runtime(_)
        | ReadyProcessTrigger::McpTransport(_)
        | ReadyProcessTrigger::Maintenance(_) => transport.drain_after_signal().await,
    };
    let mcp_result = match (&trigger, hosted_mcp.as_mut()) {
        (ReadyProcessTrigger::McpTransport(completion), Some(_)) => *completion,
        (_, Some(hosted_mcp)) => hosted_mcp.drain_after_signal().await,
        (_, None) => Ok(()),
    };
    if !transport_is_terminal(&transport_result) || !mcp_transport_is_terminal(&mcp_result) {
        if !transport_is_terminal(&transport_result) {
            return transport_result.map(|()| ReadyProcessCompletion::Stopped);
        }
        return Err(DaemonError::McpStop(
            mcp_result.expect_err("a nonterminal MCP result is an error"),
        ));
    }
    let maintenance_shutdown = matches!(trigger, ReadyProcessTrigger::Maintenance(_));
    let graph_result = if maintenance_shutdown {
        graph
            .shutdown_for_maintenance_with_stage_evidence(recovery)
            .await
    } else {
        graph.shutdown_with_stage_evidence().await
    };
    if let Err(error) = &graph_result
        && let Some(checkpoint_close) = error.checkpoint_close_receipt()
    {
        eprintln!(
            "{}",
            ProductionGraphShutdownStageEvidence::format_labels_v1_line()
        );
        eprintln!("{}", checkpoint_close.format_v1_line());
    }
    if let Ok(shutdown_stages) = &graph_result {
        let counts = write_completion_groups
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let reasons = dispatch_reasons
            .0
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let stdout = io::stdout();
        let mut stdout = stdout.lock();
        // BYTE-IDENTICAL with prior releases: Tier 2 harness depends on this line.
        let _ = writeln!(stdout, "riffdb-write-completion-groups-v1\t{counts}");
        let _ = writeln!(stdout, "riffdb-dispatch-reasons-v1\t{reasons}");
        let _ = writeln!(stdout, "{}", shutdown_stages.format_v1_line());
        // Stderr for both: the shutdown evidence block on stdout is an exact
        // ordered protocol, and an additional line in it fails a lifecycle
        // transition. Stamp the receipt moment so the remainder to the process
        // boundary is attributable too.
        eprintln!(
            "{}",
            ProductionGraphShutdownStageEvidence::format_labels_v1_line()
        );
        eprintln!("{}", shutdown_stages.format_checkpoint_close_v1_line());
        eprintln!("{}", crate::shutdown_census::format_v1_line());
        crate::shutdown_census::mark_graph_receipt();
        let read_stages_line = riffdb_observability::format_read_stages_v1_line(&read_stages);
        let _ = writeln!(stdout, "{read_stages_line}");
        let write_service_stages_line =
            riffdb_observability::format_write_service_stages_v1_line(&write_service_stages);
        let _ = writeln!(stdout, "{write_service_stages_line}");
        let command_stages_line =
            riffdb_observability::format_command_stages_v1_line(&command_stages);
        let _ = writeln!(stdout, "{command_stages_line}");
        let writer_evidence_line =
            riffdb_observability::format_writer_evidence_v1_line(&writer_evidence);
        let _ = writeln!(stdout, "{writer_evidence_line}");
        let completion_lane_evidence_line =
            riffdb_observability::format_completion_lane_evidence_v1_line(
                &completion_lane_evidence,
            );
        let _ = writeln!(stdout, "{completion_lane_evidence_line}");
        let writer_frame_census = writer_frame_census
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let _ = writeln!(
            stdout,
            "riffdb-writer-frame-census-v1\t{writer_frame_census}"
        );
        let writer_flush_census = writer_flush_census
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let _ = writeln!(
            stdout,
            "riffdb-writer-flush-census-v1\t{writer_flush_census}"
        );
        let writer_journal_stage_census = writer_journal_stage_census
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let _ = writeln!(
            stdout,
            "riffdb-writer-journal-stages-v1\t{writer_journal_stage_census}"
        );
        let writer_publication_stage_census = writer_publication_stage_census
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let _ = writeln!(
            stdout,
            "riffdb-writer-publication-stages-v1\t{writer_publication_stage_census}"
        );
        // Diagnostic-only, and zero unless RIFFDB_COMMAND_SERVICE_DIAGNOSTICS=1
        // populated it. The writer censuses start where the writer picks a
        // command up; this one covers the service path around it.
        let command_service_stage_census = riffdb_service::command_service_stage_census_v1();
        if command_service_stage_census[riffdb_service::COMMAND_SERVICE_STAGE_LABELS_V1.len()] > 0 {
            let labels = riffdb_service::COMMAND_SERVICE_STAGE_LABELS_V1.join(",");
            let values = command_service_stage_census
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(",");
            let _ = writeln!(
                stdout,
                "riffdb-command-service-stages-v1\tlabels={labels}\tvalues={values}"
            );
        }
        if query_execute_census.total_count > 0 {
            let stage_names = riffdb_storage_redb::QUERY_EXECUTE_STAGE_LABELS_V1.join(",");
            let windows = query_execute_census
                .windows
                .iter()
                .map(|window| {
                    let mut values = Vec::with_capacity(
                        riffdb_storage_redb::QUERY_EXECUTE_STAGE_LABELS_V1.len()
                            + riffdb_storage_redb::QUERY_EXECUTE_TRAILING_VALUES_V1
                            + 1,
                    );
                    values.push(window.count.to_string());
                    values.extend(window.stage_ns.iter().map(u64::to_string));
                    values.push(window.overlay_transitions_sum.to_string());
                    values.push(window.overlay_transitions_max.to_string());
                    values.push(window.overlay_bytes_sum.to_string());
                    values.push(window.overlay_bytes_max.to_string());
                    values.push(window.authority_tail_bytes_sum.to_string());
                    values.push(window.authority_tail_bytes_max.to_string());
                    values.push(window.authority_tail_commands_sum.to_string());
                    values.push(window.authority_tail_commands_max.to_string());
                    values.push(window.entity_point_reads_sum.to_string());
                    values.push(window.entity_point_reads_max.to_string());
                    values.push(window.index_rows_sum.to_string());
                    values.push(window.index_rows_max.to_string());
                    values.push(window.index_range_reads_sum.to_string());
                    values.push(window.index_range_reads_max.to_string());
                    values.push(window.program_steps_sum.to_string());
                    values.push(window.program_steps_max.to_string());
                    values.join(",")
                })
                .collect::<Vec<_>>()
                .join(";");
            let _ = writeln!(
                stdout,
                "riffdb-query-execute-windows-v1\t{}\t{}\t{stage_names}\t{}\t{windows}",
                riffdb_storage_redb::QUERY_EXECUTE_WINDOW_WIDTH_V1,
                riffdb_storage_redb::QUERY_EXECUTE_WINDOW_COUNT_V1,
                query_execute_census.total_count,
            );
        }
        // A debug build inflates CPU and leaves I/O alone, so it does not scale
        // a stage census uniformly -- it reorders it. Measuring one and reading
        // the ordering as real produced a complete, confidently wrong write-path
        // decomposition once; say so where the numbers are handed over. Stderr,
        // because the stdout evidence block is an exact ordered protocol.
        if cfg!(debug_assertions)
            && (writer_batch_census.total_count > 0 || query_execute_census.total_count > 0)
        {
            eprintln!(
                "riffdb-census-profile-warning-v1\tdebug build: stage costs are inflated                  unevenly against I/O and their ordering is not the release ordering;                  rebuild with --release before drawing a conclusion"
            );
        }
        // Diagnostic-only, and absent unless RIFFDB_WRITER_BATCH_DIAGNOSTICS=1
        // populated it, so the ordered evidence block is unchanged by default.
        if writer_batch_census.total_count > 0 {
            let _ = writeln!(
                stdout,
                "{}",
                riffdb_commit::format_writer_batch_stages_v1_line(&writer_batch_census)
            );
        }
        let _ = stdout.flush();
    }
    if maintenance_shutdown {
        recovery.reached(MaintenanceRecoveryBoundary::DatabaseClosed);
    }
    let trigger_result = match trigger {
        ReadyProcessTrigger::Command => stdin_thread
            .take()
            .ok_or(DaemonError::ShutdownReaderPanicked)?
            .join()
            .map(|()| ReadyProcessCompletion::Stopped)
            .map_err(|_| DaemonError::ShutdownReaderPanicked),
        ReadyProcessTrigger::ShutdownInputFailure => {
            if let Some(stdin_thread) = stdin_thread.take() {
                let _ = stdin_thread.join();
            }
            Err(DaemonError::ShutdownInput)
        }
        ReadyProcessTrigger::Signal(Ok(())) => Ok(ReadyProcessCompletion::Stopped),
        ReadyProcessTrigger::Signal(Err(source)) => Err(DaemonError::ShutdownSignal(source)),
        ReadyProcessTrigger::Runtime(reason) => {
            // Non-secret enum discriminant only; kind=runtime_stopped alone is
            // not enough to distinguish Integrity vs SupervisionStateCorrupted.
            eprintln!("[riffdbd-diag] trigger=runtime reason={reason:?}");
            Err(DaemonError::RuntimeStopped)
        }
        ReadyProcessTrigger::Transport(Ok(Ok(()))) => Err(DaemonError::TransportEnded),
        ReadyProcessTrigger::Transport(Ok(Err(_))) => Err(DaemonError::Transport),
        ReadyProcessTrigger::Transport(Err(_)) => Err(DaemonError::TransportTask),
        ReadyProcessTrigger::McpTransport(Ok(())) => Err(DaemonError::McpTransportEnded),
        ReadyProcessTrigger::McpTransport(Err(source)) => Err(DaemonError::McpStop(source)),
        ReadyProcessTrigger::Maintenance(trigger) => {
            Ok(ReadyProcessCompletion::Maintenance(trigger))
        }
    };

    if notification_stop_failed {
        return Err(DaemonError::NotificationShutdown);
    }
    graph_result
        .map(|_| ())
        .map_err(DaemonError::GraphShutdown)?;
    transport_result?;
    mcp_result.map_err(DaemonError::McpStop)?;
    trigger_result
}

async fn wait_for_hosted_mcp(hosted_mcp: &mut Option<HostedMcp>) -> Result<(), HostedMcpStopError> {
    match hosted_mcp.as_mut() {
        Some(hosted_mcp) => hosted_mcp.completed().await,
        None => std::future::pending().await,
    }
}

async fn wait_for_shutdown_input(
    shutdown: &mut Option<ShutdownReceiver>,
) -> Option<ReadyProcessTrigger> {
    let Some(receiver) = shutdown.as_mut() else {
        return std::future::pending().await;
    };
    match receiver.await {
        Ok(Ok(ShutdownInput::Command)) => Some(ReadyProcessTrigger::Command),
        Ok(Ok(ShutdownInput::Eof)) => {
            *shutdown = None;
            None
        }
        Ok(Err(_)) | Err(_) => Some(ReadyProcessTrigger::ShutdownInputFailure),
    }
}

fn publish_readiness(endpoint: &HostedGrpcEndpoint) -> Result<(), DaemonError> {
    // Stderr, not stdout: the readiness stream is a checked lifecycle protocol
    // whose first line must be the ready line within a fixed byte bound, so an
    // extra stdout line is a protocol violation rather than a log entry.
    // Stderr is already this process's diagnostic channel and carries the
    // startup-selection line beside this one.
    eprintln!("{}", crate::startup_census::format_v1_line());
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    writeln!(stdout, "{READY_PROTOCOL}\t{endpoint}").map_err(DaemonError::Readiness)?;
    stdout.flush().map_err(DaemonError::Readiness)
}

fn spawn_shutdown_reader() -> Result<(ShutdownReceiver, JoinHandle<()>), DaemonError> {
    let (sender, receiver) = oneshot::channel();
    let handle = thread::Builder::new()
        .name("riffdb-shutdown-input".to_owned())
        .stack_size(crate::PRODUCTION_THREAD_STACK_BYTES)
        .spawn(move || {
            let stdin = io::stdin();
            let result = read_shutdown_command(&mut stdin.lock());
            let _ = sender.send(result);
        })
        .map_err(DaemonError::ShutdownReader)?;
    Ok((receiver, handle))
}

fn read_shutdown_command(reader: &mut impl Read) -> Result<ShutdownInput, ShutdownCommandError> {
    let mut command = [0_u8; SHUTDOWN_COMMAND.len()];
    let mut received = 0;
    while received < command.len() {
        let count = reader
            .read(&mut command[received..])
            .map_err(|_| ShutdownCommandError)?;
        if count == 0 {
            return if received == 0 {
                Ok(ShutdownInput::Eof)
            } else {
                Err(ShutdownCommandError)
            };
        }
        received += count;
    }
    if command == SHUTDOWN_COMMAND {
        Ok(ShutdownInput::Command)
    } else {
        Err(ShutdownCommandError)
    }
}

#[cfg(unix)]
struct ProductionShutdownSignal {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl ProductionShutdownSignal {
    fn register() -> Result<Self, io::Error> {
        use tokio::signal::unix::{SignalKind, signal};

        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }

    async fn received(&mut self) -> Result<(), io::Error> {
        tokio::select! {
            value = self.interrupt.recv() => value.ok_or_else(signal_stream_closed),
            value = self.terminate.recv() => value.ok_or_else(signal_stream_closed),
        }
    }

    async fn take_pending(&mut self) -> Result<bool, io::Error> {
        tokio::select! {
            biased;
            result = self.received() => result.map(|()| true),
            () = tokio::task::yield_now() => Ok(false),
        }
    }
}

#[cfg(not(unix))]
struct ProductionShutdownSignal;

#[cfg(not(unix))]
impl ProductionShutdownSignal {
    fn register() -> Result<Self, io::Error> {
        Ok(Self)
    }

    async fn received(&mut self) -> Result<(), io::Error> {
        tokio::signal::ctrl_c().await
    }

    async fn take_pending(&mut self) -> Result<bool, io::Error> {
        tokio::select! {
            biased;
            result = self.received() => result.map(|()| true),
            () = tokio::task::yield_now() => Ok(false),
        }
    }
}

#[cfg(unix)]
fn signal_stream_closed() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "shutdown signal stream closed")
}

#[derive(Clone, Debug)]
enum HostedGrpcEndpoint {
    Tcp(SocketAddr),
    LocalSocket,
}

impl fmt::Display for HostedGrpcEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tcp(address) => address.fmt(formatter),
            Self::LocalSocket => formatter.write_str("local-socket"),
        }
    }
}

#[cfg(unix)]
struct BoundLocalSocket {
    path: PathBuf,
    device: u64,
    inode: u64,
}

#[cfg(unix)]
impl Drop for BoundLocalSocket {
    fn drop(&mut self) {
        let Ok(metadata) = fs::symlink_metadata(&self.path) else {
            return;
        };
        if metadata.file_type().is_socket()
            && metadata.dev() == self.device
            && metadata.ino() == self.inode
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

struct HostedGrpc {
    endpoint: HostedGrpcEndpoint,
    rebind_config: ApplicationListenerConfig,
    drain_limit: Duration,
    #[cfg(unix)]
    _local_socket: Option<BoundLocalSocket>,
    shutdown: Option<oneshot::Sender<()>>,
    task: TokioJoinHandle<Result<(), tonic::transport::Error>>,
}

impl HostedGrpc {
    fn bind(
        config: ApplicationListenerConfig,
        application: &GrpcApplication,
    ) -> Result<Self, DaemonError> {
        let (shutdown, stopped) = oneshot::channel();
        match config {
            ApplicationListenerConfig::LoopbackCleartext(listener) => {
                let address = listener.listen_address();
                // HTTP/2 emits small control and data frames independently. Leaving
                // Nagle enabled on the accepted side couples those frames to the peer's
                // delayed-ACK timer and adds a repeatable ~40 ms to otherwise local
                // unary calls. The public client already enables TCP_NODELAY.
                let incoming = TcpIncoming::bind(address)
                    .map_err(DaemonError::Listener)?
                    .with_nodelay(Some(true));
                let local_address = incoming.local_addr().map_err(DaemonError::Listener)?;
                let rebind_config = ApplicationListenerConfig::LoopbackCleartext(
                    LoopbackCleartextListener::new(local_address)
                        .map_err(|_| DaemonError::GrpcConfiguration)?,
                );
                let router = application_router(Server::builder(), application, None);
                let task = tokio::spawn(
                    router.serve_with_incoming_shutdown(incoming, shutdown_signal(stopped)),
                );
                Ok(Self {
                    endpoint: HostedGrpcEndpoint::Tcp(local_address),
                    rebind_config,
                    drain_limit: TRANSPORT_DRAIN_LIMIT,
                    #[cfg(unix)]
                    _local_socket: None,
                    shutdown: Some(shutdown),
                    task,
                })
            }
            ApplicationListenerConfig::DirectTls(listener) => {
                let identity = ReloadableTlsIdentity::load(&listener)?;
                let incoming = TcpIncoming::bind(listener.listen_address())
                    .map_err(DaemonError::Listener)?
                    .with_nodelay(Some(true));
                let local_address = incoming.local_addr().map_err(DaemonError::Listener)?;
                let bounds = listener.bounds();
                let incoming = ReloadingTlsIncoming::new(
                    BoundedIncoming::new(incoming, bounds.max_connections(), bounds.idle_timeout()),
                    identity,
                    bounds,
                );
                let router = application_router(
                    Server::builder(),
                    &application.for_confidential_listener(),
                    Some(bounds),
                );
                let rebind_config = ApplicationListenerConfig::DirectTls(listener);
                let task = tokio::spawn(
                    router.serve_with_incoming_shutdown(incoming, shutdown_signal(stopped)),
                );
                Ok(Self {
                    endpoint: HostedGrpcEndpoint::Tcp(local_address),
                    rebind_config,
                    drain_limit: bounds.drain_timeout(),
                    #[cfg(unix)]
                    _local_socket: None,
                    shutdown: Some(shutdown),
                    task,
                })
            }
            ApplicationListenerConfig::LocalSocket(listener) => {
                #[cfg(unix)]
                {
                    let (incoming, guard) = bind_local_socket(&listener)?;
                    let bounds = listener.bounds();
                    let router = application_router(
                        Server::builder(),
                        &application.for_confidential_listener(),
                        Some(bounds),
                    );
                    let rebind_config = ApplicationListenerConfig::LocalSocket(listener);
                    let task = tokio::spawn(router.serve_with_incoming_shutdown(
                        BoundedIncoming::new(
                            incoming,
                            bounds.max_connections(),
                            bounds.idle_timeout(),
                        ),
                        shutdown_signal(stopped),
                    ));
                    Ok(Self {
                        endpoint: HostedGrpcEndpoint::LocalSocket,
                        rebind_config,
                        drain_limit: bounds.drain_timeout(),
                        _local_socket: Some(guard),
                        shutdown: Some(shutdown),
                        task,
                    })
                }
                #[cfg(not(unix))]
                {
                    let _ = listener;
                    let _ = stopped;
                    Err(DaemonError::LocalSocketUnsupported)
                }
            }
        }
    }

    const fn endpoint(&self) -> &HostedGrpcEndpoint {
        &self.endpoint
    }

    fn rebind_config(&self) -> ApplicationListenerConfig {
        self.rebind_config.clone()
    }

    #[cfg(test)]
    const fn tcp_address(&self) -> Option<SocketAddr> {
        match self.endpoint {
            HostedGrpcEndpoint::Tcp(address) => Some(address),
            HostedGrpcEndpoint::LocalSocket => None,
        }
    }

    fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    async fn drain_after_signal(&mut self) -> Result<(), DaemonError> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let completion = tokio::time::timeout(self.drain_limit, &mut self.task)
            .await
            .map_err(|_| DaemonError::TransportDrainTimeout)?;
        classify_transport_completion(&completion)
    }

    async fn completed(&mut self) -> Result<(), DaemonError> {
        let completion = (&mut self.task).await;
        classify_transport_completion(&completion)
    }
}

fn application_router(
    mut builder: Server,
    application: &GrpcApplication,
    bounds: Option<ListenerBounds>,
) -> tonic::transport::server::Router {
    if let Some(bounds) = bounds {
        builder = builder
            .max_concurrent_streams(bounds.max_streams_per_connection().get())
            .http2_keepalive_interval(Some(bounds.keepalive_interval()))
            .http2_keepalive_timeout(Some(bounds.keepalive_interval()))
            .timeout(REQUEST_DURATION_LIMIT);
    }
    builder
        .add_service(application.contract_server())
        .add_service(application.command_server())
        .add_service(application.application_session_server())
        .add_service(application.query_server())
        .add_service(application.application_query_server())
        .add_service(application.commit_server())
        .add_service(application.event_server())
        .add_service(application.admin_server())
        .add_service(application.replication_server())
}

async fn shutdown_signal(stopped: oneshot::Receiver<()>) {
    let _ = stopped.await;
}

struct BoundedIncoming<S> {
    incoming: Pin<Box<S>>,
    permits: Arc<Semaphore>,
    idle_timeout: Duration,
}

impl<S> BoundedIncoming<S> {
    fn new(incoming: S, maximum_connections: NonZeroU32, idle_timeout: Duration) -> Self {
        Self {
            incoming: Box::pin(incoming),
            permits: Arc::new(Semaphore::new(maximum_connections.get() as usize)),
            idle_timeout,
        }
    }
}

struct AdmittedConnection<IO> {
    io: IO,
    _permit: OwnedSemaphorePermit,
    idle_timeout: Duration,
    idle: Pin<Box<tokio::time::Sleep>>,
}

impl<S, IO, E> Stream for BoundedIncoming<S>
where
    S: Stream<Item = Result<IO, E>>,
{
    type Item = Result<AdmittedConnection<IO>, E>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let item = match self.incoming.as_mut().poll_next(context) {
                Poll::Ready(Some(item)) => item,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            };
            match item {
                Ok(io) => match Arc::clone(&self.permits).try_acquire_owned() {
                    Ok(permit) => {
                        return Poll::Ready(Some(Ok(AdmittedConnection {
                            io,
                            _permit: permit,
                            idle_timeout: self.idle_timeout,
                            idle: Box::pin(tokio::time::sleep(self.idle_timeout)),
                        })));
                    }
                    Err(_) => drop(io),
                },
                Err(error) => return Poll::Ready(Some(Err(error))),
            }
        }
    }
}

impl<IO: AsyncRead + Unpin> AsyncRead for AdmittedConnection<IO> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.idle.as_mut().poll(context).is_ready() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "connection idle limit elapsed",
            )));
        }
        let before = buffer.filled().len();
        let result = Pin::new(&mut self.io).poll_read(context, buffer);
        if matches!(result, Poll::Ready(Ok(()))) && buffer.filled().len() > before {
            let deadline = tokio::time::Instant::now() + self.idle_timeout;
            self.idle.as_mut().reset(deadline);
        }
        result
    }
}

impl<IO: AsyncWrite + Unpin> AsyncWrite for AdmittedConnection<IO> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        if self.idle.as_mut().poll(context).is_ready() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "connection idle limit elapsed",
            )));
        }
        let result = Pin::new(&mut self.io).poll_write(context, buffer);
        if matches!(result, Poll::Ready(Ok(written)) if written > 0) {
            let deadline = tokio::time::Instant::now() + self.idle_timeout;
            self.idle.as_mut().reset(deadline);
        }
        result
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.io).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.io).poll_shutdown(context)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        if self.idle.as_mut().poll(context).is_ready() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "connection idle limit elapsed",
            )));
        }
        let result = Pin::new(&mut self.io).poll_write_vectored(context, buffers);
        if matches!(result, Poll::Ready(Ok(written)) if written > 0) {
            let deadline = tokio::time::Instant::now() + self.idle_timeout;
            self.idle.as_mut().reset(deadline);
        }
        result
    }
}

impl<IO: Connected> Connected for AdmittedConnection<IO> {
    type ConnectInfo = IO::ConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.io.connect_info()
    }
}

type RawTlsConnection = tokio_rustls::server::TlsStream<AdmittedConnection<tokio::net::TcpStream>>;
type TlsConnection = GenerationDrainedConnection<RawTlsConnection>;
type TlsHandshake =
    Pin<Box<dyn Future<Output = Option<Result<TlsConnection, io::Error>>> + Send + 'static>>;

struct TlsReloadSignal {
    generation: AtomicU64,
    changed: Arc<Notify>,
}

impl TlsReloadSignal {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            generation: AtomicU64::new(0),
            changed: Arc::new(Notify::new()),
        })
    }

    fn current_generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    fn publish_successor(&self) -> u64 {
        let generation = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
        self.changed.notify_waiters();
        generation
    }
}

struct TlsHandshakeSnapshot {
    config: Arc<tokio_rustls::rustls::ServerConfig>,
    generation: u64,
    signal: Arc<TlsReloadSignal>,
}

struct GenerationDrainedConnection<IO> {
    io: IO,
    accepted_generation: u64,
    signal: Arc<TlsReloadSignal>,
    changed: Pin<Box<OwnedNotified>>,
    drain_timeout: Duration,
    drain: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl<IO> GenerationDrainedConnection<IO> {
    fn new(
        io: IO,
        accepted_generation: u64,
        signal: Arc<TlsReloadSignal>,
        drain_timeout: Duration,
    ) -> Self {
        let changed = Box::pin(Arc::clone(&signal.changed).notified_owned());
        Self {
            io,
            accepted_generation,
            signal,
            changed,
            drain_timeout,
            drain: None,
        }
    }

    fn check_rotation(&mut self, context: &mut Context<'_>) -> io::Result<()> {
        if self.drain.is_none() {
            // Register the notification before the second generation read. This
            // closes the race where publication occurs between observing the
            // generation and parking the connection task.
            if self.changed.as_mut().poll(context).is_ready() {
                self.changed = Box::pin(Arc::clone(&self.signal.changed).notified_owned());
            }
            if self.signal.current_generation() != self.accepted_generation {
                self.drain = Some(Box::pin(tokio::time::sleep(self.drain_timeout)));
            }
        }
        if self
            .drain
            .as_mut()
            .is_some_and(|drain| drain.as_mut().poll(context).is_ready())
        {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "TLS identity rotation drain limit elapsed",
            ));
        }
        Ok(())
    }
}

impl<IO: AsyncRead + Unpin> AsyncRead for GenerationDrainedConnection<IO> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Err(error) = self.check_rotation(context) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.io).poll_read(context, buffer)
    }
}

impl<IO: AsyncWrite + Unpin> AsyncWrite for GenerationDrainedConnection<IO> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        if let Err(error) = self.check_rotation(context) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.io).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        if let Err(error) = self.check_rotation(context) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.io).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        if let Err(error) = self.check_rotation(context) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.io).poll_shutdown(context)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        if let Err(error) = self.check_rotation(context) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.io).poll_write_vectored(context, buffers)
    }
}

impl<IO: Connected> Connected for GenerationDrainedConnection<IO> {
    type ConnectInfo = IO::ConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.io.connect_info()
    }
}

struct ReloadingTlsIncoming {
    incoming: BoundedIncoming<TcpIncoming>,
    incoming_ended: bool,
    handshakes: FuturesUnordered<TlsHandshake>,
    identity: ReloadableTlsIdentity,
    handshake_timeout: Duration,
    maximum_handshakes: usize,
}

impl ReloadingTlsIncoming {
    fn new(
        incoming: BoundedIncoming<TcpIncoming>,
        identity: ReloadableTlsIdentity,
        bounds: ListenerBounds,
    ) -> Self {
        Self {
            incoming,
            incoming_ended: false,
            handshakes: FuturesUnordered::new(),
            identity,
            handshake_timeout: bounds.handshake_timeout(),
            maximum_handshakes: bounds.max_connections().get() as usize,
        }
    }
}

impl Stream for ReloadingTlsIncoming {
    type Item = Result<TlsConnection, io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        'poll: loop {
            while !self.incoming_ended && self.handshakes.len() < self.maximum_handshakes {
                match Pin::new(&mut self.incoming).poll_next(context) {
                    Poll::Ready(Some(Ok(connection))) => {
                        let snapshot = self.identity.snapshot_for_new_handshake();
                        let timeout = self.handshake_timeout;
                        let drain_timeout = self.identity.drain_timeout;
                        self.handshakes.push(Box::pin(async move {
                            match tokio::time::timeout(
                                timeout,
                                TlsAcceptor::from(snapshot.config).accept(connection),
                            )
                            .await
                            {
                                Ok(Ok(connection)) => Some(Ok(GenerationDrainedConnection::new(
                                    connection,
                                    snapshot.generation,
                                    snapshot.signal,
                                    drain_timeout,
                                ))),
                                Ok(Err(_)) | Err(_) => None,
                            }
                        }));
                    }
                    Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                    Poll::Ready(None) => {
                        self.incoming_ended = true;
                        break;
                    }
                    Poll::Pending => break,
                }
            }
            match Pin::new(&mut self.handshakes).poll_next(context) {
                Poll::Ready(Some(Some(result))) => return Poll::Ready(Some(result)),
                Poll::Ready(Some(None)) => continue 'poll,
                Poll::Ready(None) if self.incoming_ended => return Poll::Ready(None),
                Poll::Ready(None) | Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TransportFileIdentity {
    bytes: u64,
    modified: Option<std::time::SystemTime>,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TlsFilePairIdentity {
    certificate: TransportFileIdentity,
    private_key: TransportFileIdentity,
}

struct ReloadableTlsIdentity {
    certificate_path: PathBuf,
    private_key_path: PathBuf,
    expected_identity: TlsServerIdentity,
    current_files: TlsFilePairIdentity,
    current: Arc<tokio_rustls::rustls::ServerConfig>,
    signal: Arc<TlsReloadSignal>,
    drain_timeout: Duration,
    rejected_files: Option<TlsFilePairIdentity>,
    unavailable_reported: bool,
}

impl ReloadableTlsIdentity {
    fn load(listener: &DirectTlsListenerConfig) -> Result<Self, DaemonError> {
        let certificate_path = listener.tls().certificate_chain().as_path().to_path_buf();
        let private_key_path = listener.tls().private_key().as_path().to_path_buf();
        let expected_identity = listener.public_endpoint().identity().clone();
        let (current, current_files) =
            load_tls_snapshot(&certificate_path, &private_key_path, &expected_identity)?;
        Ok(Self {
            certificate_path,
            private_key_path,
            expected_identity,
            current_files,
            current,
            signal: TlsReloadSignal::new(),
            drain_timeout: listener.bounds().drain_timeout(),
            rejected_files: None,
            unavailable_reported: false,
        })
    }

    fn snapshot_for_new_handshake(&mut self) -> TlsHandshakeSnapshot {
        let observed = tls_file_pair_identity(&self.certificate_path, &self.private_key_path);
        let Ok(observed) = observed else {
            if !self.unavailable_reported {
                eprintln!(
                    "riffdbd transport reload rejected kind=tls_material retained_previous=true"
                );
                self.unavailable_reported = true;
            }
            return self.current_snapshot();
        };
        self.unavailable_reported = false;
        if observed == self.current_files || self.rejected_files.as_ref() == Some(&observed) {
            return self.current_snapshot();
        }
        match load_tls_snapshot(
            &self.certificate_path,
            &self.private_key_path,
            &self.expected_identity,
        ) {
            Ok((snapshot, files)) => {
                self.current = snapshot;
                self.current_files = files;
                self.rejected_files = None;
                self.signal.publish_successor();
            }
            Err(_) => {
                eprintln!(
                    "riffdbd transport reload rejected kind=tls_configuration retained_previous=true"
                );
                self.rejected_files = Some(observed);
            }
        }
        self.current_snapshot()
    }

    fn current_snapshot(&self) -> TlsHandshakeSnapshot {
        TlsHandshakeSnapshot {
            config: Arc::clone(&self.current),
            generation: self.signal.current_generation(),
            signal: Arc::clone(&self.signal),
        }
    }
}

fn load_tls_snapshot(
    certificate_path: &Path,
    private_key_path: &Path,
    expected_identity: &TlsServerIdentity,
) -> Result<(Arc<tokio_rustls::rustls::ServerConfig>, TlsFilePairIdentity), DaemonError> {
    let (certificate_chain, certificate_identity) =
        read_transport_file(certificate_path, MAX_CERTIFICATE_CHAIN_BYTES, false)?;
    let (private_key, private_key_identity) =
        read_transport_file(private_key_path, MAX_PRIVATE_KEY_BYTES, true)?;
    verify_certificate_identity(&certificate_chain, expected_identity)?;
    let mut certificate_reader = io::Cursor::new(&certificate_chain);
    let certificates = CertificateDer::pem_reader_iter(&mut certificate_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| DaemonError::TlsConfiguration)?;
    if certificates.is_empty() {
        return Err(DaemonError::TlsConfiguration);
    }
    let private_key = Zeroizing::new(private_key);
    let private_key = PrivateKeyDer::from_pem_reader(io::Cursor::new(private_key.as_slice()))
        .map_err(|_| DaemonError::TlsConfiguration)?;
    let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
    let mut config = tokio_rustls::rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|_| DaemonError::TlsConfiguration)?
        .with_no_client_auth()
        .with_single_cert(certificates, private_key)
        .map_err(|_| DaemonError::TlsConfiguration)?;
    config.alpn_protocols.push(b"h2".to_vec());
    Ok((
        Arc::new(config),
        TlsFilePairIdentity {
            certificate: certificate_identity,
            private_key: private_key_identity,
        },
    ))
}

fn read_transport_file(
    path: &Path,
    maximum_bytes: u64,
    private: bool,
) -> Result<(Vec<u8>, TransportFileIdentity), DaemonError> {
    let before = transport_file_identity(path, private)?;
    let file = fs::File::open(path).map_err(|_| DaemonError::TlsMaterial)?;
    let opened_metadata = file.metadata().map_err(|_| DaemonError::TlsMaterial)?;
    if !opened_metadata.is_file()
        || opened_metadata.len() == 0
        || opened_metadata.len() > maximum_bytes
        || !opened_file_matches_identity(&before, &opened_metadata)
    {
        return Err(DaemonError::TlsMaterial);
    }
    let mut contents = Vec::with_capacity(opened_metadata.len() as usize);
    file.take(maximum_bytes.saturating_add(1))
        .read_to_end(&mut contents)
        .map_err(|_| DaemonError::TlsMaterial)?;
    if contents.is_empty() || contents.len() as u64 > maximum_bytes {
        return Err(DaemonError::TlsMaterial);
    }
    let after = transport_file_identity(path, private)?;
    if after != before {
        return Err(DaemonError::TlsMaterial);
    }
    Ok((contents, after))
}

fn tls_file_pair_identity(
    certificate_path: &Path,
    private_key_path: &Path,
) -> Result<TlsFilePairIdentity, DaemonError> {
    Ok(TlsFilePairIdentity {
        certificate: transport_file_identity(certificate_path, false)?,
        private_key: transport_file_identity(private_key_path, true)?,
    })
}

fn transport_file_identity(
    path: &Path,
    private: bool,
) -> Result<TransportFileIdentity, DaemonError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| DaemonError::TlsMaterial)?;
    if !metadata.file_type().is_file() || metadata.len() == 0 {
        return Err(DaemonError::TlsMaterial);
    }
    #[cfg(unix)]
    {
        let forbidden = if private { 0o077 } else { 0o022 };
        if metadata.permissions().mode() & forbidden != 0 {
            return Err(DaemonError::TlsMaterialPermissions);
        }
    }
    Ok(TransportFileIdentity {
        bytes: metadata.len(),
        modified: metadata.modified().ok(),
        #[cfg(unix)]
        device: metadata.dev(),
        #[cfg(unix)]
        inode: metadata.ino(),
    })
}

fn verify_certificate_identity(
    certificate_chain: &[u8],
    expected: &TlsServerIdentity,
) -> Result<(), DaemonError> {
    let mut reader = io::Cursor::new(certificate_chain);
    let certificate = CertificateDer::pem_reader_iter(&mut reader)
        .next()
        .ok_or(DaemonError::TlsConfiguration)?
        .map_err(|_| DaemonError::TlsConfiguration)?;
    let certificate =
        webpki::EndEntityCert::try_from(&certificate).map_err(|_| DaemonError::TlsConfiguration)?;
    let expected =
        ServerName::try_from(expected.as_str()).map_err(|_| DaemonError::TlsConfiguration)?;
    certificate
        .verify_is_valid_for_subject_name(&expected)
        .map_err(|_| DaemonError::TlsConfiguration)
}

#[cfg(unix)]
fn opened_file_matches_identity(identity: &TransportFileIdentity, opened: &fs::Metadata) -> bool {
    identity.device == opened.dev() && identity.inode == opened.ino()
}

#[cfg(not(unix))]
fn opened_file_matches_identity(identity: &TransportFileIdentity, opened: &fs::Metadata) -> bool {
    identity.bytes == opened.len() && identity.modified == opened.modified().ok()
}

#[cfg(unix)]
fn bind_local_socket(
    listener: &LocalSocketListenerConfig,
) -> Result<(UnixListenerStream, BoundLocalSocket), DaemonError> {
    let path = listener.path();
    let parent = path.parent().ok_or(DaemonError::LocalSocketConfiguration)?;
    let parent_metadata =
        fs::symlink_metadata(parent).map_err(|_| DaemonError::LocalSocketConfiguration)?;
    if !parent_metadata.file_type().is_dir() || parent_metadata.permissions().mode() & 0o022 != 0 {
        return Err(DaemonError::LocalSocketPermissions);
    }
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Ok(_) | Err(_) => return Err(DaemonError::LocalSocketOccupied),
    }
    let socket = tokio::net::UnixListener::bind(path).map_err(DaemonError::Listener)?;
    fs::set_permissions(path, fs::Permissions::from_mode(listener.access().mode()))
        .map_err(|_| DaemonError::LocalSocketPermissions)?;
    let metadata = fs::symlink_metadata(path).map_err(|_| DaemonError::LocalSocketPermissions)?;
    if !metadata.file_type().is_socket()
        || metadata.permissions().mode() & 0o777 != listener.access().mode()
    {
        return Err(DaemonError::LocalSocketPermissions);
    }
    let guard = BoundLocalSocket {
        path: path.to_path_buf(),
        device: metadata.dev(),
        inode: metadata.ino(),
    };
    Ok((UnixListenerStream::new(socket), guard))
}

fn classify_transport_completion(
    completion: &Result<Result<(), tonic::transport::Error>, JoinError>,
) -> Result<(), DaemonError> {
    match completion {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(DaemonError::Transport),
        Err(_) => Err(DaemonError::TransportTask),
    }
}

fn transport_is_terminal(result: &Result<(), DaemonError>) -> bool {
    !matches!(result, Err(DaemonError::TransportDrainTimeout))
}

fn mcp_transport_is_terminal(result: &Result<(), HostedMcpStopError>) -> bool {
    !matches!(result, Err(HostedMcpStopError::DrainTimeout))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ShutdownCommandError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ShutdownInput {
    Command,
    Eof,
}

enum DaemonError {
    Replication(riffdb_service::ReplicationFailure),
    Config(ServerConfigError),
    Runtime(io::Error),
    ProcessClock(ServerProcessClockError),
    DigestKeys,
    StartupClock(AuthorizationClockError),
    StartupInventory(StorageValueError),
    GrpcConfiguration,
    Listener(io::Error),
    TlsConfiguration,
    TlsMaterial,
    TlsMaterialPermissions,
    LocalSocketConfiguration,
    LocalSocketPermissions,
    LocalSocketOccupied,
    #[cfg(not(unix))]
    LocalSocketUnsupported,
    McpDependencies,
    McpStart(HostedMcpStartError),
    McpStop(HostedMcpStopError),
    Startup(RedbStartupError),
    MaintenanceStorage(StorageError),
    MaintenanceDriver,
    RecoveryHostStart(RecoveryHostStartError),
    RecoveryHostShutdown(RecoveryHostShutdownError),
    RestoreRetryHostStart(RestoreRetryHostStartError),
    RestoreRetryHostShutdown(RestoreRetryHostShutdownError),
    BuildInfo,
    GraphBuild(ProductionGraphBuildError),
    Readiness(io::Error),
    ShutdownReader(io::Error),
    ShutdownSignal(io::Error),
    ShutdownReaderPanicked,
    ShutdownInput,
    NotificationShutdown,
    RuntimeStopped,
    TransportEnded,
    McpTransportEnded,
    Transport,
    TransportTask,
    TransportDrainTimeout,
    GraphShutdown(ProductionGraphShutdownError),
}

impl DaemonError {
    /// Stable, non-secret lifecycle failure kind for process-boundary diagnostics.
    #[must_use]
    const fn kind(&self) -> &'static str {
        match self {
            Self::Replication(riffdb_service::ReplicationFailure::AuthorizationDenied) => {
                "replication_authorization"
            }
            Self::Replication(riffdb_service::ReplicationFailure::Source(_)) => {
                "replication_source"
            }
            Self::Replication(riffdb_service::ReplicationFailure::Unavailable) => {
                "replication_unavailable"
            }
            Self::Config(_) => "config",
            Self::Runtime(_) => "runtime",
            Self::ProcessClock(_) => "process_clock",
            Self::DigestKeys => "digest_keys",
            Self::StartupClock(_) => "startup_clock",
            Self::StartupInventory(_) => "startup_inventory",
            Self::GrpcConfiguration => "grpc_configuration",
            Self::Listener(_) => "listener",
            Self::TlsConfiguration => "tls_configuration",
            Self::TlsMaterial => "tls_material",
            Self::TlsMaterialPermissions => "tls_material_permissions",
            Self::LocalSocketConfiguration => "local_socket_configuration",
            Self::LocalSocketPermissions => "local_socket_permissions",
            Self::LocalSocketOccupied => "local_socket_occupied",
            #[cfg(not(unix))]
            Self::LocalSocketUnsupported => "local_socket_unsupported",
            Self::McpDependencies => "mcp_dependencies",
            Self::McpStart(_) => "mcp_start",
            Self::McpStop(_) => "mcp_stop",
            Self::Startup(_) => "startup",
            Self::MaintenanceStorage(_) => "maintenance_storage",
            Self::MaintenanceDriver => "maintenance_driver",
            Self::RecoveryHostStart(_) => "recovery_host_start",
            Self::RecoveryHostShutdown(_) => "recovery_host_shutdown",
            Self::RestoreRetryHostStart(_) => "restore_retry_host_start",
            Self::RestoreRetryHostShutdown(_) => "restore_retry_host_shutdown",
            Self::BuildInfo => "build_info",
            Self::GraphBuild(_) => "graph_build",
            Self::Readiness(_) => "readiness",
            Self::ShutdownReader(_) => "shutdown_reader",
            Self::ShutdownSignal(_) => "shutdown_signal",
            Self::ShutdownReaderPanicked => "shutdown_reader_panicked",
            Self::ShutdownInput => "shutdown_input",
            Self::NotificationShutdown => "notification_shutdown",
            Self::RuntimeStopped => "runtime_stopped",
            Self::TransportEnded => "transport_ended",
            Self::McpTransportEnded => "mcp_transport_ended",
            Self::Transport => "transport",
            Self::TransportTask => "transport_task",
            Self::TransportDrainTimeout => "transport_drain_timeout",
            Self::GraphShutdown(_) => "graph_shutdown",
        }
    }
}

impl fmt::Debug for DaemonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "DaemonError(kind={})", self.kind())
    }
}

impl fmt::Display for DaemonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "riffdbd process lifecycle failed kind={}",
            self.kind()
        )
    }
}

impl Error for DaemonError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Replication(_) => None,
            Self::Config(source) => Some(source),
            Self::Runtime(source)
            | Self::Listener(source)
            | Self::Readiness(source)
            | Self::ShutdownReader(source)
            | Self::ShutdownSignal(source) => Some(source),
            Self::ProcessClock(source) => Some(source),
            Self::McpStart(source) => Some(source),
            Self::McpStop(source) => Some(source),
            Self::StartupClock(source) => Some(source),
            Self::StartupInventory(source) => Some(source),
            Self::Startup(source) => Some(source),
            Self::MaintenanceStorage(source) => Some(source),
            Self::RecoveryHostStart(source) => Some(source),
            Self::RecoveryHostShutdown(source) => Some(source),
            Self::RestoreRetryHostStart(source) => Some(source),
            Self::RestoreRetryHostShutdown(source) => Some(source),
            Self::GraphBuild(source) => Some(source),
            Self::GraphShutdown(source) => Some(source),
            #[cfg(not(unix))]
            Self::LocalSocketUnsupported => None,
            Self::DigestKeys
            | Self::GrpcConfiguration
            | Self::TlsConfiguration
            | Self::TlsMaterial
            | Self::TlsMaterialPermissions
            | Self::LocalSocketConfiguration
            | Self::LocalSocketPermissions
            | Self::LocalSocketOccupied
            | Self::McpDependencies
            | Self::BuildInfo
            | Self::MaintenanceDriver
            | Self::ShutdownReaderPanicked
            | Self::ShutdownInput
            | Self::NotificationShutdown
            | Self::RuntimeStopped
            | Self::TransportEnded
            | Self::McpTransportEnded
            | Self::Transport
            | Self::TransportTask
            | Self::TransportDrainTimeout => None,
        }
    }
}

#[cfg(test)]
#[cfg(unix)]
#[path = "replication_transport_tests.rs"]
mod replication_transport_tests;

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use riffdb_client_rust::{CallMetadata, RiffDbClient};
    use riffdb_proto::v1;
    use tonic::transport::Endpoint;

    use super::*;

    #[test]
    // req: REP-007, AFC-007
    fn incomplete_archive_restore_refuses_ordinary_startup() {
        use riffdb_storage_api::{
            OfflineArchiveReceiptPersistencePort, OfflineMaintenanceAdmissionV1,
            OfflineMaintenanceReceiptV3,
        };
        use riffdb_types::{
            ActorId, ActorKind, ArchiveNameV1, ArchiveRestoreStopV1, BackupNameV1, CapabilityId,
            OfflineMaintenanceOperationId, OfflineMaintenanceReplacementConfirmation,
            archive_restore_input_hash,
        };
        let root = tempfile::TempDir::new().unwrap();
        let target = root.path().join("db.redb");
        let backups = root.path().join("backups");
        let (mut store, _) = RedbMaintenanceStorage::open(&target, &backups).unwrap();
        let backup = BackupNameV1::new("before").unwrap();
        let archive = ArchiveNameV1::new("daily").unwrap();
        let stop = ArchiveRestoreStopV1::LastArchived;
        let confirmation = OfflineMaintenanceReplacementConfirmation::NotProvided;
        let receipt = OfflineMaintenanceReceiptV3::accepted_archive_restore(
            OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1000, [3; 10])
                .unwrap(),
            backup.clone(),
            archive.clone(),
            stop,
            archive_restore_input_hash(&backup, &archive, stop, confirmation),
            confirmation,
            OfflineMaintenanceAdmissionV1::new(
                ActorId::new("operator").unwrap(),
                ActorKind::Human,
                CapabilityId::from_unix_milliseconds_and_random(1000, [4; 10]).unwrap(),
                None,
            ),
            None,
        )
        .unwrap();
        store.create_or_read_archive_receipt(&receipt).unwrap();
        drop(store);
        let (_store, reconciliation) = RedbMaintenanceStorage::open(&target, &backups).unwrap();
        for target_requires_recovery in [false, true] {
            assert!(matches!(
                initial_database_action(&reconciliation, target_requires_recovery),
                Err(DaemonError::MaintenanceDriver)
            ));
        }
    }

    #[test]
    fn shutdown_command_is_exact_and_bounded() {
        assert_eq!(
            read_shutdown_command(&mut Cursor::new(b"shutdown\n")),
            Ok(ShutdownInput::Command)
        );
        assert_eq!(
            read_shutdown_command(&mut Cursor::new(b"")),
            Ok(ShutdownInput::Eof)
        );
        for invalid in [
            b"shutdown".as_slice(),
            b"shutdown\r".as_slice(),
            b"stop-now\n".as_slice(),
        ] {
            assert_eq!(
                read_shutdown_command(&mut Cursor::new(invalid)),
                Err(ShutdownCommandError)
            );
        }
    }

    #[test]
    fn write_completion_and_dispatch_shutdown_lines_remain_byte_identical() {
        let groups = [0_u64; riffdb_observability::MAX_WRITE_GROUP_SIZE];
        let counts = groups
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let write_line = format!("riffdb-write-completion-groups-v1\t{counts}");
        assert_eq!(
            write_line,
            format!(
                "riffdb-write-completion-groups-v1\t{}",
                vec!["0"; riffdb_observability::MAX_WRITE_GROUP_SIZE].join(",")
            )
        );

        let reasons = [0_u64; riffdb_observability::COMMAND_GROUP_DISPATCH_REASON_COUNT];
        let reasons = reasons
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let dispatch_line = format!("riffdb-dispatch-reasons-v1\t{reasons}");
        assert_eq!(dispatch_line, "riffdb-dispatch-reasons-v1\t0,0,0,0");

        let source = include_str!("daemon.rs");
        assert!(
            source.contains(
                "let _ = writeln!(stdout, \"riffdb-write-completion-groups-v1\\t{counts}\");"
            ),
            "write-completion groups line format is frozen"
        );
        assert!(
            source
                .contains("let _ = writeln!(stdout, \"riffdb-dispatch-reasons-v1\\t{reasons}\");"),
            "dispatch-reasons line format is frozen"
        );
        assert!(
            source.contains("riffdb_observability::format_read_stages_v1_line(&read_stages)"),
            "read stages line is emitted beside the frozen write/dispatch lines"
        );
        assert!(
            source.contains("riffdb_observability::format_command_stages_v1_line(&command_stages)"),
            "command stages line is emitted beside the frozen write/dispatch lines"
        );
    }

    #[test]
    fn incomplete_maintenance_selector_is_phase_and_evidence_exact() {
        use IncompleteMaintenanceDecision::{
            AwaitCurrentCredential, AwaitRecoveryCredential, FailClosed, ResumeCreate,
            ResumePublishedCurrentRestore, ResumePublishedRecoveryRestore,
        };
        use OfflineMaintenanceOperationKind::{CreateBackup, RestoreBackup};
        use OfflineMaintenanceReceiptPhaseV1::{
            Accepted, ArtifactPublished, Draining, FailedClosed, Offline, Succeeded, Validating,
        };

        type Shape = (
            OfflineMaintenanceOperationKind,
            bool,
            OfflineMaintenanceReceiptPhaseV1,
            bool,
            bool,
            bool,
            bool,
            bool,
        );
        let mut allowed: Vec<(Shape, IncompleteMaintenanceDecision)> = Vec::new();
        for phase in [Accepted, Draining] {
            allowed.push((
                (CreateBackup, true, phase, false, false, false, false, false),
                ResumeCreate,
            ));
            allowed.push((
                (RestoreBackup, true, phase, true, false, false, false, false),
                AwaitCurrentCredential,
            ));
        }
        for (named, manifest) in [(false, false), (true, false), (true, true)] {
            allowed.push((
                (
                    CreateBackup,
                    true,
                    Offline,
                    named,
                    false,
                    false,
                    false,
                    manifest,
                ),
                ResumeCreate,
            ));
        }
        for phase in [ArtifactPublished, Validating] {
            allowed.push((
                (CreateBackup, true, phase, true, false, false, false, true),
                ResumeCreate,
            ));
            allowed.push((
                (RestoreBackup, true, phase, true, true, true, true, true),
                ResumePublishedCurrentRestore,
            ));
            allowed.push((
                (RestoreBackup, false, phase, true, true, true, true, true),
                ResumePublishedRecoveryRestore,
            ));
        }
        for staged in [false, true] {
            for has_identity in [false, true] {
                allowed.push((
                    (
                        RestoreBackup,
                        true,
                        Offline,
                        true,
                        staged,
                        false,
                        has_identity,
                        has_identity,
                    ),
                    AwaitCurrentCredential,
                ));
            }
        }
        allowed.push((
            (RestoreBackup, true, Offline, true, true, true, true, true),
            ResumePublishedCurrentRestore,
        ));
        allowed.push((
            (
                RestoreBackup,
                false,
                Accepted,
                true,
                true,
                false,
                false,
                false,
            ),
            AwaitRecoveryCredential,
        ));
        for has_identity in [false, true] {
            allowed.push((
                (
                    RestoreBackup,
                    false,
                    Offline,
                    true,
                    true,
                    false,
                    has_identity,
                    has_identity,
                ),
                AwaitRecoveryCredential,
            ));
        }
        allowed.push((
            (RestoreBackup, false, Offline, true, true, true, true, true),
            ResumePublishedRecoveryRestore,
        ));

        for kind in [CreateBackup, RestoreBackup] {
            for source in [false, true] {
                for phase in [
                    Accepted,
                    Draining,
                    Offline,
                    ArtifactPublished,
                    Validating,
                    Succeeded,
                    FailedClosed,
                ] {
                    for named in [false, true] {
                        for staged in [false, true] {
                            for target in [false, true] {
                                for staged_id in [false, true] {
                                    for manifest in [false, true] {
                                        let shape = (
                                            kind, source, phase, named, staged, target, staged_id,
                                            manifest,
                                        );
                                        let expected = allowed
                                            .iter()
                                            .find_map(|(candidate, decision)| {
                                                (*candidate == shape).then_some(*decision)
                                            })
                                            .unwrap_or(FailClosed);
                                        assert_eq!(
                                            decide_incomplete_shape(IncompleteMaintenanceShape {
                                                kind,
                                                has_source_database: source,
                                                phase,
                                                named,
                                                staged,
                                                target,
                                                has_staged_database_id: staged_id,
                                                has_manifest_identity: manifest,
                                            }),
                                            expected,
                                            "unexpected decision for {shape:?}"
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn production_listener_precedes_the_blocking_startup_proof() {
        let source = include_str!("daemon.rs");
        let production = source
            .split_once("#[cfg(test)]")
            .expect("daemon architecture boundary")
            .0;
        let listener = production.find("HostedGrpc::bind(").expect("listener bind");
        let startup = production
            .find("open_redb_startup_with_commit_profile(")
            .expect("startup proof");
        let readiness = production.find("publish_readiness(").expect("readiness");
        assert!(listener < startup);
        assert!(startup < readiness);
    }

    #[test]
    fn multi_database_failures_quarantine_only_the_selected_generation() {
        let source = include_str!("daemon.rs");
        let production = source
            .split_once("#[cfg(test)]")
            .expect("daemon architecture boundary")
            .0;
        let supervisor = production
            .split_once("let stop = loop {")
            .and_then(|(_, tail)| tail.split_once("drop(shutdown_thread);"))
            .map(|(body, _)| body)
            .expect("multi-database ready supervisor");

        assert_eq!(
            supervisor
                .matches("quarantine_multi_database_generation(")
                .count(),
            3,
            "runtime, maintenance-channel, and replacement failures remain database-local"
        );
        assert!(
            supervisor.contains("let replacement = replace_multi_database_generation("),
            "maintenance replacement must be classified before the process supervisor decides"
        );

        let quarantine = production
            .split_once("async fn quarantine_multi_database_generation(")
            .and_then(|(_, tail)| tail.split_once("async fn await_multi_restore_retry("))
            .map(|(body, _)| body)
            .expect("database-local quarantine");
        for required in [
            "generation.lifecycle.stop();",
            "routes\n        .replace(&generation.alias, offline_route)",
            ".suspend(&generation.alias)",
            "generation.graph.take()",
        ] {
            assert!(
                quarantine.contains(required),
                "quarantine omitted required isolation step: {required}"
            );
        }
    }

    #[test]
    fn production_grpc_accepts_connections_with_nagle_disabled() {
        let source = include_str!("daemon.rs");
        let production = source
            .split_once("#[cfg(test)]")
            .expect("daemon architecture boundary")
            .0;
        let hosted = production
            .split_once("impl HostedGrpc {")
            .and_then(|(_, tail)| tail.split_once("const fn endpoint"))
            .map(|(body, _)| body)
            .expect("hosted gRPC construction");

        assert!(hosted.contains(".with_nodelay(Some(true))"));
    }

    #[test]
    fn interrupted_current_restore_uses_the_narrow_host_until_database_close() {
        let source = include_str!("daemon.rs");
        let production = source
            .split_once("#[cfg(test)]")
            .expect("daemon architecture boundary")
            .0;
        let startup_arm = production
            .split_once("InitialDatabaseAction::AwaitRestoreCredential(receipt) =>")
            .and_then(|(_, tail)| {
                tail.split_once("InitialDatabaseAction::AwaitRecoveryCredential(receipt)")
            })
            .map(|(body, _)| body)
            .expect("current restore-retry startup arm");
        assert!(startup_arm.contains("run_restore_retry_until_ready("));
        for forbidden in [
            "ProductionGraphBuilder",
            "RunningCommandCoordinator",
            "RunningProjectionWorker",
            "HostedMcp::bind",
            "recover_outbox",
        ] {
            assert!(
                !startup_arm.contains(forbidden),
                "startup retry arm constructed forbidden authority {forbidden}"
            );
        }

        let retry = production
            .split_once("async fn run_restore_retry_until_ready(")
            .and_then(|(_, tail)| tail.split_once("async fn run_recovery_until_ready("))
            .map(|(body, _)| body)
            .expect("restore-retry supervisor");
        let host = retry
            .find("RunningRestoreRetryHost::start(")
            .expect("narrow host start");
        let trigger = retry
            .find("MaintenanceTrigger::RestoreBackup")
            .expect("exact restore trigger");
        let transport = retry
            .rfind("transport.drain_after_signal().await?")
            .expect("transport drain before database close");
        let host_shutdown = retry
            .rfind(".shutdown()")
            .expect("retry host and database close");
        let driver = retry
            .find("run_offline_maintenance(")
            .expect("offline restore driver");
        let fresh_generation = retry
            .find("start_generation(")
            .expect("fresh complete graph after restore");
        assert!(host < trigger);
        assert!(trigger < transport);
        assert!(transport < host_shutdown);
        assert!(host_shutdown < driver);
        assert!(driver < fresh_generation);
        assert!(!retry.contains("ProductionGraphBuilder"));
        assert!(!retry.contains("HostedMcp::bind"));
    }

    #[test]
    fn restore_retry_resumption_carries_the_target_fence_past_the_narrow_host() {
        // The narrow host consumes the validated startup and drops its retained
        // metadata, so both retry supervisors must read the target's history
        // incarnation before the handoff and hand it to the restore driver.
        let source = include_str!("daemon.rs");
        let production = source
            .split_once("#[cfg(test)]")
            .expect("daemon architecture boundary")
            .0;
        for (supervisor, next) in [
            (
                "async fn await_multi_restore_retry(",
                "async fn await_multi_recovery(",
            ),
            (
                "async fn run_restore_retry_until_ready(",
                "async fn run_recovery_until_ready(",
            ),
        ] {
            let body = production
                .split_once(supervisor)
                .and_then(|(_, tail)| tail.split_once(next))
                .map(|(body, _)| body)
                .unwrap_or_else(|| panic!("{supervisor} body"));
            let capture = body
                .find("startup.retained_metadata().history_incarnation()")
                .unwrap_or_else(|| panic!("{supervisor} must capture the target fence"));
            let handoff = body
                .find("RunningRestoreRetryHost::start(")
                .unwrap_or_else(|| panic!("{supervisor} narrow host start"));
            let dependencies = body
                .find("maintenance_driver_dependencies(")
                .unwrap_or_else(|| panic!("{supervisor} driver dependencies"));
            assert!(
                capture < handoff,
                "{supervisor} must read the fence before surrendering the startup"
            );
            assert!(handoff < dependencies);
            let arguments = body[dependencies..]
                .split_once(")?;")
                .map(|(head, _)| head)
                .unwrap_or_else(|| panic!("{supervisor} dependency arguments"));
            assert!(
                arguments.contains("retained_target_history_incarnation,"),
                "{supervisor} must pass the captured fence to the restore driver"
            );
        }
    }

    #[test]
    fn only_a_transport_drain_timeout_withholds_lower_graph_teardown() {
        assert!(!transport_is_terminal(&Err(
            DaemonError::TransportDrainTimeout
        )));
        assert!(transport_is_terminal(&Ok(())));
        assert!(transport_is_terminal(&Err(DaemonError::Transport)));
        assert!(transport_is_terminal(&Err(DaemonError::TransportTask)));

        let source = include_str!("daemon.rs");
        let production = source
            .split_once("#[cfg(test)]")
            .expect("daemon architecture boundary")
            .0;
        let early = production
            .split_once("if transport.is_finished()")
            .and_then(|(_, tail)| tail.split_once("if let Err(source) = publish_readiness"))
            .map(|(body, _)| body)
            .expect("early terminal transport branch");
        assert!(
            early
                .find("transport.completed().await")
                .expect("terminal wait")
                < early.find("graph.shutdown().await").expect("graph drain")
        );
        assert!(
            early.find("graph.shutdown().await").expect("graph drain")
                < early.rfind("return Err(").expect("terminal result")
        );

        let ready = production
            .split_once("async fn supervise_ready_process(")
            .and_then(|(_, tail)| tail.split_once("fn publish_readiness("))
            .map(|(body, _)| body)
            .expect("ready process supervisor");
        assert!(
            ready
                .find("transport_is_terminal(&transport_result)")
                .expect("terminal classification")
                < ready
                    .find("graph.shutdown_with_stage_evidence().await")
                    .expect("graph drain")
        );
        assert!(
            ready
                .find("graph.shutdown_with_stage_evidence().await")
                .expect("graph drain")
                < ready.rfind("transport_result?;").expect("transport result")
        );
        assert!(
            ready.rfind("transport_result?;").expect("transport result")
                < ready.rfind("\n    trigger_result").expect("trigger result")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bound_listener_serves_only_initializing_health_before_validation() {
        let (initializing, _activator, issuer) = RiffDbService::begin_initialization();
        let lifecycle = Arc::new(ProductionLifecycleRoute::new(
            initializing,
            issuer,
            RuntimeRoutingState::new(),
        ));
        let route: Arc<dyn GrpcLifecycleRoute> = lifecycle.clone();
        let application = GrpcApplication::new(
            route,
            GrpcRequestLimits::new(REQUEST_DURATION_LIMIT).expect("fixed request limit"),
        );
        let mut transport = HostedGrpc::bind(
            ApplicationListenerConfig::LoopbackCleartext(
                LoopbackCleartextListener::new("127.0.0.1:0".parse().expect("loopback address"))
                    .expect("loopback listener"),
            ),
            &application,
        )
        .expect("bound initializing listener");
        let endpoint = Endpoint::from_shared(format!(
            "http://{}",
            transport.tcp_address().expect("TCP listener")
        ))
        .expect("loopback endpoint");
        let mut client = RiffDbClient::connect(endpoint)
            .await
            .expect("connect to initializing listener");

        let response = client
            .health(
                v1::HealthRequest { request_id: None },
                &CallMetadata::default(),
            )
            .await
            .expect("restricted initializing Health");
        let Some(v1::health_response::Result::PreBootstrap(report)) = response.result else {
            panic!("initializing listener returned authenticated Health");
        };
        assert_eq!(
            report.lifecycle,
            v1::PreBootstrapLifecycle::Unspecified as i32
        );
        assert!(report.liveness);
        assert!(!report.readiness);
        assert!(response.database_alias.is_empty());
        assert!(response.authentication_audience.is_empty());

        lifecycle.stop();
        drop(client);
        transport
            .drain_after_signal()
            .await
            .expect("initializing transport drains");
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn protected_local_socket_is_published_exactly_and_removed_by_its_owner() {
        // MAX_LOCAL_SOCKET_PATH_BYTES (100) bounds the whole socket path, so
        // the scope keeps a short basename by construction (`riffdb-sock-`
        // plus tempfile's suffix); an unusually long TMPDIR can still exceed
        // the limit, which is the config contract under test, not an
        // artifact of this scope. The scope removes the directory on drop —
        // pass, fail, or panic — while the trailing `fs::remove_dir`
        // assertion below still proves the listener left it empty.
        let scope =
            tempfile::TempDir::with_prefix("riffdb-sock-").expect("create socket directory");
        let root = scope.path().to_path_buf();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
            .expect("protect socket directory");
        let socket_path = root.join("s");

        let (initializing, _activator, issuer) = RiffDbService::begin_initialization();
        let lifecycle = Arc::new(ProductionLifecycleRoute::new(
            initializing,
            issuer,
            RuntimeRoutingState::new(),
        ));
        let route: Arc<dyn GrpcLifecycleRoute> = lifecycle;
        let application = GrpcApplication::new(
            route,
            GrpcRequestLimits::new(REQUEST_DURATION_LIMIT).expect("fixed request limit"),
        );
        let listener = LocalSocketListenerConfig::new(
            socket_path.clone(),
            riffdb_config::LocalSocketAccess::OwnerOnly,
            ListenerBounds::alpha_default(),
        )
        .expect("local socket config");
        let mut transport = HostedGrpc::bind(
            ApplicationListenerConfig::LocalSocket(listener),
            &application,
        )
        .expect("bind protected local socket");

        let metadata = fs::symlink_metadata(&socket_path).expect("published socket");
        assert!(metadata.file_type().is_socket());
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        assert!(matches!(
            transport.endpoint(),
            HostedGrpcEndpoint::LocalSocket
        ));

        transport
            .drain_after_signal()
            .await
            .expect("drain local socket");
        drop(transport);
        assert!(!socket_path.exists());
        fs::remove_dir(&root).expect("remove empty socket directory");
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn direct_tls_accepts_verified_peer_and_rejects_cleartext_and_wrong_name() {
        use std::num::NonZeroU32;

        use riffdb_config::{
            CanonicalHttpsEndpoint, DirectTlsListenerConfig, ProtectedFilePath, ServerTlsFiles,
            TlsClientConfig, TlsServerIdentity,
        };

        // Whole-directory scope removed on drop — pass, fail, or panic; the
        // trailing remove_file/remove_dir assertions still prove the test
        // created exactly the files it removed.
        let scope = tempfile::TempDir::with_prefix("riffdb-direct-tls-test-")
            .expect("create TLS test directory");
        let root = scope.path().to_path_buf();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
            .expect("protect TLS test directory");
        let certificate = root.join("server.pem");
        let private_key = root.join("server.key");
        let trust_root = root.join("ca.pem");
        fs::write(
            &certificate,
            include_bytes!("../tests/fixtures/localhost-cert.pem"),
        )
        .expect("write test certificate");
        fs::write(
            &private_key,
            include_bytes!("../tests/fixtures/localhost-key.pem"),
        )
        .expect("write test private key");
        fs::write(&trust_root, include_bytes!("../tests/fixtures/test-ca.pem"))
            .expect("write test trust root");
        fs::set_permissions(&certificate, fs::Permissions::from_mode(0o444))
            .expect("protect test certificate");
        fs::set_permissions(&private_key, fs::Permissions::from_mode(0o600))
            .expect("protect test private key");
        fs::set_permissions(&trust_root, fs::Permissions::from_mode(0o444))
            .expect("protect test trust root");

        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve test port");
        let port = probe.local_addr().expect("test address").port();
        drop(probe);
        let public_endpoint = CanonicalHttpsEndpoint::parse(&format!("https://127.0.0.1:{port}"))
            .expect("public endpoint");
        let server_files = ServerTlsFiles::new(
            ProtectedFilePath::new(certificate.clone()).expect("certificate path"),
            ProtectedFilePath::new(private_key.clone()).expect("private-key path"),
        )
        .expect("server files");
        let listener = DirectTlsListenerConfig::new(
            format!("0.0.0.0:{port}").parse().expect("bind address"),
            public_endpoint.clone(),
            server_files,
            ListenerBounds::alpha_default(),
        )
        .expect("direct TLS listener");

        let (initializing, _activator, issuer) = RiffDbService::begin_initialization();
        let lifecycle = Arc::new(ProductionLifecycleRoute::new(
            initializing,
            issuer,
            RuntimeRoutingState::new(),
        ));
        let route: Arc<dyn GrpcLifecycleRoute> = lifecycle.clone();
        let application = GrpcApplication::new(
            route,
            GrpcRequestLimits::new(REQUEST_DURATION_LIMIT).expect("fixed request limit"),
        );
        let mismatched_listener = DirectTlsListenerConfig::new(
            format!("0.0.0.0:{port}").parse().expect("bind address"),
            CanonicalHttpsEndpoint::parse(&format!("https://127.0.0.2:{port}"))
                .expect("mismatched endpoint"),
            ServerTlsFiles::new(
                ProtectedFilePath::new(certificate.clone()).expect("certificate path"),
                ProtectedFilePath::new(private_key.clone()).expect("private-key path"),
            )
            .expect("mismatched server files"),
            ListenerBounds::alpha_default(),
        )
        .expect("mismatched listener shape");
        assert!(matches!(
            HostedGrpc::bind(
                ApplicationListenerConfig::DirectTls(mismatched_listener),
                &application,
            ),
            Err(DaemonError::TlsConfiguration)
        ));
        let pre_bind_probe = std::net::TcpListener::bind(format!("0.0.0.0:{port}"))
            .expect("identity mismatch fails before bind");
        drop(pre_bind_probe);
        let mut transport =
            HostedGrpc::bind(ApplicationListenerConfig::DirectTls(listener), &application)
                .expect("bind direct TLS listener");

        let verified = TlsClientConfig::new(
            public_endpoint,
            ProtectedFilePath::new(trust_root.clone()).expect("trust root path"),
            TlsServerIdentity::parse("127.0.0.1").expect("server identity"),
            Duration::from_secs(5),
            Duration::from_secs(30),
            NonZeroU32::new(1).expect("pool"),
            NonZeroU32::new(64).expect("streams"),
        )
        .expect("verified client config");
        let mut client = RiffDbClient::connect_verified_tls(&verified)
            .await
            .expect("verified TLS connection");
        let health = client
            .health(
                v1::HealthRequest { request_id: None },
                &CallMetadata::default(),
            )
            .await
            .expect("health over verified TLS");
        assert!(matches!(
            health.result,
            Some(v1::health_response::Result::PreBootstrap(_))
        ));

        let wrong_endpoint = CanonicalHttpsEndpoint::parse(&format!("https://127.0.0.2:{port}"))
            .expect("wrong-name endpoint");
        let wrong_name = TlsClientConfig::new(
            wrong_endpoint,
            ProtectedFilePath::new(trust_root.clone()).expect("trust root path"),
            TlsServerIdentity::parse("127.0.0.2").expect("wrong identity"),
            Duration::from_secs(5),
            Duration::from_secs(30),
            NonZeroU32::new(1).expect("pool"),
            NonZeroU32::new(64).expect("streams"),
        )
        .expect("wrong-name client shape remains structurally valid");
        assert!(matches!(
            RiffDbClient::connect_verified_tls(&wrong_name).await,
            Err(riffdb_client_rust::ClientError::Tls(
                riffdb_client_rust::TlsClientFailure::ConnectionOrPeerVerification
            ))
        ));

        let cleartext =
            Endpoint::from_shared(format!("http://127.0.0.1:{port}")).expect("cleartext endpoint");
        match RiffDbClient::connect(cleartext).await {
            Err(_) => {}
            Ok(mut cleartext_client) => assert!(
                cleartext_client
                    .health(
                        v1::HealthRequest { request_id: None },
                        &CallMetadata::default(),
                    )
                    .await
                    .is_err()
            ),
        }

        lifecycle.stop();
        drop(client);
        transport
            .drain_after_signal()
            .await
            .expect("drain direct TLS listener");
        fs::remove_file(certificate).expect("remove certificate");
        fs::remove_file(private_key).expect("remove private key");
        fs::remove_file(trust_root).expect("remove trust root");
        fs::remove_dir(root).expect("remove TLS test directory");
    }

    #[cfg(unix)]
    #[test]
    fn tls_identity_reload_swaps_complete_valid_snapshot_and_retains_last_good_on_failure() {
        use riffdb_config::{
            CanonicalHttpsEndpoint, DirectTlsListenerConfig, ProtectedFilePath, ServerTlsFiles,
        };

        // Whole-directory scope removed on drop — pass, fail, or panic; the
        // trailing remove_file/remove_dir assertions still prove the test
        // created exactly the files it removed.
        let scope = tempfile::TempDir::with_prefix("riffdb-tls-reload-test-")
            .expect("create TLS reload root");
        let root = scope.path().to_path_buf();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).expect("protect root");
        let certificate = root.join("server.pem");
        let private_key = root.join("server.key");
        fs::write(
            &certificate,
            include_bytes!("../tests/fixtures/localhost-cert.pem"),
        )
        .expect("write certificate");
        fs::write(
            &private_key,
            include_bytes!("../tests/fixtures/localhost-key.pem"),
        )
        .expect("write private key");
        fs::set_permissions(&certificate, fs::Permissions::from_mode(0o444))
            .expect("protect certificate");
        fs::set_permissions(&private_key, fs::Permissions::from_mode(0o600))
            .expect("protect private key");
        let listener = DirectTlsListenerConfig::new(
            "127.0.0.1:7443".parse().expect("address"),
            CanonicalHttpsEndpoint::parse("https://127.0.0.1:7443").expect("endpoint"),
            ServerTlsFiles::new(
                ProtectedFilePath::new(certificate.clone()).expect("certificate path"),
                ProtectedFilePath::new(private_key.clone()).expect("private-key path"),
            )
            .expect("TLS files"),
            ListenerBounds::alpha_default(),
        )
        .expect("listener");
        let mut identity = ReloadableTlsIdentity::load(&listener).expect("initial snapshot");
        let initial = identity.snapshot_for_new_handshake();
        assert_eq!(initial.generation, 0);

        let replacement = root.join("server.next.pem");
        fs::write(
            &replacement,
            include_bytes!("../tests/fixtures/localhost-cert.pem"),
        )
        .expect("write replacement");
        fs::set_permissions(&replacement, fs::Permissions::from_mode(0o444))
            .expect("protect replacement");
        fs::rename(&replacement, &certificate).expect("publish replacement atomically");
        let reloaded = identity.snapshot_for_new_handshake();
        assert!(!Arc::ptr_eq(&initial.config, &reloaded.config));
        assert_eq!(reloaded.generation, 1);

        let invalid = root.join("server.invalid.pem");
        fs::write(&invalid, b"not a certificate").expect("write invalid replacement");
        fs::set_permissions(&invalid, fs::Permissions::from_mode(0o444))
            .expect("protect invalid replacement");
        fs::rename(&invalid, &certificate).expect("publish invalid replacement atomically");
        let retained = identity.snapshot_for_new_handshake();
        assert!(Arc::ptr_eq(&reloaded.config, &retained.config));
        assert_eq!(retained.generation, 1);

        fs::remove_file(certificate).expect("remove invalid certificate");
        fs::remove_file(private_key).expect("remove private key");
        fs::remove_dir(root).expect("remove reload root");
    }

    #[tokio::test(start_paused = true)]
    async fn tls_identity_rotation_drains_the_predecessor_on_the_configured_bound() {
        use tokio::io::AsyncReadExt as _;

        let signal = TlsReloadSignal::new();
        let (connection, _peer) = tokio::io::duplex(64);
        let mut connection = GenerationDrainedConnection::new(
            connection,
            signal.current_generation(),
            Arc::clone(&signal),
            Duration::from_secs(3),
        );

        signal.publish_successor();
        let read = tokio::spawn(async move {
            let mut byte = [0_u8; 1];
            connection.read_exact(&mut byte).await
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(!read.is_finished());
        tokio::time::advance(Duration::from_secs(1)).await;
        let error = read
            .await
            .expect("drained read task")
            .expect_err("predecessor connection must be retired");
        assert_eq!(error.kind(), io::ErrorKind::ConnectionAborted);
    }
}
