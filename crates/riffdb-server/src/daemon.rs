//! Hosted `riffdbd` process lifecycle for the runnable P1 checkpoint.

use std::error::Error;
use std::fmt;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use riffdb_api_grpc::{GrpcApplication, GrpcLifecycleRoute, GrpcRequestLimits};
use riffdb_api_mcp::MCP_PROTOCOL_VERSION;
use riffdb_auth::{NoopAuthenticationTelemetry, load_digest_key_providers};
use riffdb_contract_ir::EXECUTABLE_IR_VERSION_V1;
use riffdb_policy::{
    AuthorizationClock, AuthorizationClockError, NoopAuthorizationTelemetry, TrustedAudienceCatalog,
};
use riffdb_service::{
    BuildInfo, OfflineMaintenanceStartDisposition, RecoveryOfflineMaintenancePortError,
    RestoreOfflineBackupRequest, RiffDbService,
};
use riffdb_storage_api::{
    OfflineMaintenanceReceiptFailureV1, OfflineMaintenanceReceiptPersistencePort,
    OfflineMaintenanceReceiptPhaseV1, OfflineMaintenanceReceiptV1,
    ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
    StartupValidationInputs, StorageError, StorageErrorKind, StorageValueError,
};
use riffdb_storage_redb::{
    RedbMaintenanceOperationEvidence, RedbMaintenanceReconciliation, RedbMaintenanceStorage,
};
use riffdb_types::OfflineMaintenanceOperationKind;
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinHandle as TokioJoinHandle};
use tonic::transport::{Server, server::TcpIncoming};

use crate::clocks::{ProductionWallClocks, ServerProcessClockError};
use crate::config::{ServerConfig, ServerConfigError};
use crate::hosted_mcp::{HostedMcp, HostedMcpStartError, HostedMcpStopError};
use crate::identifiers::ProductionIdentifierSources;
use crate::lifecycle::ProductionLifecycleRoute;
use crate::maintenance_adapter::{
    MaintenanceController, MaintenanceTrigger, maintenance_trigger_channel,
    shared_maintenance_storage, start_result,
};
use crate::maintenance_driver::{
    MaintenanceDriverDependencies, MaintenanceDriverFailure, MaintenanceDriverRequest,
    MaintenanceDriverSuccess, RecoveryMaintenanceDriverRequest, mark_draining, mark_offline,
    receipt_matches_restore_request, run_offline_maintenance, run_recovery_restore,
};
use crate::maintenance_lifecycle::MaintenanceLifecycle;
use crate::maintenance_recovery_controller::{
    MaintenanceRecoveryBoundary, MaintenanceRecoveryController,
};
use crate::process_graph::{
    ProductionDigestKeys, ProductionGraphBuildError, ProductionGraphBuilder,
    ProductionGraphShutdownError, RunningProductionGraph,
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
/// Process errors are intentionally rendered as one static, nonsecret message.
#[must_use]
pub fn riffdbd_main() -> ExitCode {
    match run_from_process(MaintenanceRecoveryController::disabled()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(_error) => {
            eprintln!("riffdbd terminated without reaching a clean process boundary");
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
    let config = ServerConfig::from_process_args().map_err(DaemonError::Config)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(RUNTIME_WORKER_THREADS)
        .enable_all()
        .build()
        .map_err(DaemonError::Runtime)?;
    let result = runtime.block_on(run_server(config, recovery));
    runtime.shutdown_timeout(RUNTIME_DRAIN_LIMIT);
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
    let application = GrpcApplication::new(lifecycle_for_grpc, limits);
    let mut transport = HostedGrpc::bind(config.listen_address(), &application)?;
    drop(application);
    let (maintenance_storage, reconciliation) =
        RedbMaintenanceStorage::open(config.database_path(), config.backup_root())
            .map_err(DaemonError::MaintenanceStorage)?;
    let target_requires_recovery = maintenance_storage
        .configured_target_requires_recovery()
        .map_err(DaemonError::MaintenanceStorage)?;
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
    let startup = match initial_action {
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
                drop(current);
            }
            let dependencies = maintenance_driver_dependencies(
                &config,
                &startup_inputs,
                &digest_keys,
                &identifiers,
                &clocks,
                &recovery,
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
                &startup_inputs,
                &digest_keys,
                &identifiers,
                &clocks,
                &recovery,
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
    if publish_initial && let Err(source) = publish_readiness(generation.transport.local_address())
    {
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
    let listen_address = generation.transport.local_address();
    let mut shutdown = Some(shutdown);
    let mut stdin_thread = Some(stdin_thread);

    loop {
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
        let operation_id = trigger.operation_id();
        let prepared = GenerationInputs::load(config)?;
        let request = normal_driver_request(trigger)?;
        let driver_result = {
            let storage = maintenance.storage();
            let mut storage = storage.lock().map_err(|_| DaemonError::MaintenanceDriver)?;
            mark_offline(&mut storage, &maintenance_lifecycle, operation_id)
                .map_err(|_| DaemonError::MaintenanceDriver)?;
            let dependencies = maintenance_driver_dependencies(
                config,
                &prepared.startup_inputs,
                &prepared.digest_keys,
                &prepared.identifiers,
                &prepared.clocks,
                recovery,
            )?;
            run_offline_maintenance(&mut storage, &maintenance_lifecycle, &dependencies, request)
                .map_err(|_| DaemonError::MaintenanceDriver)?
        };
        let (_receipt, startup) = driver_result.into_parts();
        generation = start_generation(
            config,
            listen_address,
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
        if let Err(source) = publish_readiness(generation.transport.local_address()) {
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
        MaintenanceTrigger::CreateBackup { request } => Ok(
            MaintenanceDriverRequest::create_backup(request.operation_id()),
        ),
        MaintenanceTrigger::RestoreBackup {
            request,
            credential,
        } => Ok(MaintenanceDriverRequest::restore_backup(
            request.operation_id(),
            credential,
        )),
        MaintenanceTrigger::RecoveryRestore { .. } => Err(DaemonError::MaintenanceDriver),
    }
}

fn trusted_audiences(config: &ServerConfig) -> Result<TrustedAudienceCatalog, DaemonError> {
    let mut audiences = vec![config.audience().clone()];
    audiences.extend(config.mcp_audience().cloned());
    TrustedAudienceCatalog::new(audiences).map_err(|_| DaemonError::MaintenanceDriver)
}

fn maintenance_driver_dependencies<'a>(
    config: &ServerConfig,
    startup_inputs: &StartupValidationInputs,
    digest_keys: &ProductionDigestKeys,
    identifiers: &ProductionIdentifierSources,
    clocks: &'a ProductionWallClocks,
    recovery: &'a MaintenanceRecoveryController,
) -> Result<MaintenanceDriverDependencies<'a>, DaemonError> {
    Ok(MaintenanceDriverDependencies::new(
        startup_inputs.clone(),
        identifiers.database_ids(),
        config.redb_commit_profile(),
        digest_keys.shared_capability(),
        config.environment().clone(),
        config.audience().clone(),
        trusted_audiences(config)?,
        clocks,
        Arc::new(NoopAuthenticationTelemetry),
        Arc::new(NoopAuthorizationTelemetry),
        recovery,
    ))
}

fn startup_failure_allows_recovery(error: &RedbStartupError) -> bool {
    match error {
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
    let listen_address = transport.local_address();
    let routing = lifecycle.runtime_routing();
    let retry_host = match RunningRestoreRetryHost::start(
        startup,
        operation_id,
        input_hash,
        digest_keys,
        config,
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

    if trigger.operation_id() != operation_id {
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
            &prepared.startup_inputs,
            &prepared.digest_keys,
            &prepared.identifiers,
            &prepared.clocks,
            recovery,
        )?;
        run_offline_maintenance(&mut storage, &maintenance_lifecycle, &dependencies, request)
            .map_err(|_| DaemonError::MaintenanceDriver)?
    };
    let (_terminal_receipt, startup) = driver_result.into_parts();
    let mut generation = start_generation(
        config,
        listen_address,
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
    let listen_address = transport.local_address();
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
                    &prepared.startup_inputs,
                    &prepared.digest_keys,
                    &prepared.identifiers,
                    &prepared.clocks,
                    recovery,
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
                    listen_address,
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
    listen_address: SocketAddr,
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
    let application = GrpcApplication::new(lifecycle_for_grpc, limits);
    let mut transport = HostedGrpc::bind(listen_address, &application)?;
    let graph = match ProductionGraphBuilder::new(
        startup,
        activator,
        digest_keys,
        config,
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
    // Release packaging supplies RIFFDB_GIT_REVISION; the fallback is truthful for local builds.
    BuildInfo::new(
        env!("CARGO_PKG_VERSION"),
        option_env!("RIFFDB_GIT_REVISION").unwrap_or("development-unversioned"),
        "rustc-1.97.0",
        Vec::new(),
        startup.retained_metadata().storage_format_version().get(),
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
        graph.shutdown_for_maintenance(recovery).await
    } else {
        graph.shutdown().await
    };
    if graph_result.is_ok() {
        let counts = write_completion_groups
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let stdout = io::stdout();
        let mut stdout = stdout.lock();
        let _ = writeln!(stdout, "riffdb-write-completion-groups-v1\t{counts}");
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
        ReadyProcessTrigger::Runtime(_reason) => Err(DaemonError::RuntimeStopped),
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
    graph_result.map_err(DaemonError::GraphShutdown)?;
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

fn publish_readiness(address: SocketAddr) -> Result<(), DaemonError> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    writeln!(stdout, "{READY_PROTOCOL}\t{address}").map_err(DaemonError::Readiness)?;
    stdout.flush().map_err(DaemonError::Readiness)
}

fn spawn_shutdown_reader() -> Result<(ShutdownReceiver, JoinHandle<()>), DaemonError> {
    let (sender, receiver) = oneshot::channel();
    let handle = thread::Builder::new()
        .name("riffdb-shutdown-input".to_owned())
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

struct HostedGrpc {
    local_address: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: TokioJoinHandle<Result<(), tonic::transport::Error>>,
}

impl HostedGrpc {
    fn bind(address: SocketAddr, application: &GrpcApplication) -> Result<Self, DaemonError> {
        // HTTP/2 emits small control and data frames independently. Leaving
        // Nagle enabled on the accepted side couples those frames to the peer's
        // delayed-ACK timer and adds a repeatable ~40 ms to otherwise local
        // unary calls. The public client already enables TCP_NODELAY.
        let incoming = TcpIncoming::bind(address)
            .map_err(DaemonError::Listener)?
            .with_nodelay(Some(true));
        let local_address = incoming.local_addr().map_err(DaemonError::Listener)?;
        let (shutdown, stopped) = oneshot::channel();
        let router = Server::builder()
            .add_service(application.contract_server())
            .add_service(application.command_server())
            .add_service(application.query_server())
            .add_service(application.application_query_server())
            .add_service(application.commit_server())
            .add_service(application.admin_server());
        let task = tokio::spawn(router.serve_with_incoming_shutdown(incoming, async move {
            let _ = stopped.await;
        }));
        Ok(Self {
            local_address,
            shutdown: Some(shutdown),
            task,
        })
    }

    const fn local_address(&self) -> SocketAddr {
        self.local_address
    }

    fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    async fn drain_after_signal(&mut self) -> Result<(), DaemonError> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let completion = tokio::time::timeout(TRANSPORT_DRAIN_LIMIT, &mut self.task)
            .await
            .map_err(|_| DaemonError::TransportDrainTimeout)?;
        classify_transport_completion(&completion)
    }

    async fn completed(&mut self) -> Result<(), DaemonError> {
        let completion = (&mut self.task).await;
        classify_transport_completion(&completion)
    }
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
    Config(ServerConfigError),
    Runtime(io::Error),
    ProcessClock(ServerProcessClockError),
    DigestKeys,
    StartupClock(AuthorizationClockError),
    StartupInventory(StorageValueError),
    GrpcConfiguration,
    Listener(io::Error),
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

impl fmt::Debug for DaemonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DaemonError([REDACTED])")
    }
}

impl fmt::Display for DaemonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("riffdbd process lifecycle failed")
    }
}

impl Error for DaemonError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
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
            Self::DigestKeys
            | Self::GrpcConfiguration
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
mod tests {
    use std::io::Cursor;

    use riffdb_client_rust::{CallMetadata, RiffDbClient};
    use riffdb_proto::v1;
    use tonic::transport::Endpoint;

    use super::*;

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
    fn production_grpc_accepts_connections_with_nagle_disabled() {
        let source = include_str!("daemon.rs");
        let production = source
            .split_once("#[cfg(test)]")
            .expect("daemon architecture boundary")
            .0;
        let hosted = production
            .split_once("impl HostedGrpc {")
            .and_then(|(_, tail)| tail.split_once("const fn local_address"))
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
                < ready.find("graph.shutdown().await").expect("graph drain")
        );
        assert!(
            ready.find("graph.shutdown().await").expect("graph drain")
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
            "127.0.0.1:0".parse().expect("loopback address"),
            &application,
        )
        .expect("bound initializing listener");
        let endpoint = Endpoint::from_shared(format!("http://{}", transport.local_address()))
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
            v1::PreBootstrapLifecycle::InitializingValidation as i32
        );
        assert!(report.liveness);
        assert!(!report.readiness);

        lifecycle.stop();
        drop(client);
        transport
            .drain_after_signal()
            .await
            .expect("initializing transport drains");
    }
}
