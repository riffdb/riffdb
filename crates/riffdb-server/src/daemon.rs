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
use riffdb_auth::{DigestKeyProviders, load_digest_key_providers};
use riffdb_contract_ir::EXECUTABLE_IR_VERSION_V1;
use riffdb_policy::{AuthorizationClock, AuthorizationClockError};
use riffdb_service::{BuildInfo, RiffDbService};
use riffdb_storage_api::{
    ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
    StartupValidationInputs, StorageValueError,
};
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinHandle as TokioJoinHandle};
use tonic::transport::{Server, server::TcpIncoming};

use crate::clocks::{ProductionWallClocks, ServerProcessClockError};
use crate::config::{ServerConfig, ServerConfigError};
use crate::identifiers::ProductionIdentifierSources;
use crate::lifecycle::ProductionLifecycleRoute;
use crate::process_graph::{
    ProductionGraphBuildError, ProductionGraphBuilder, ProductionGraphShutdownError,
    RunningProductionGraph,
};
use crate::runtime_support::{RuntimeRoutingState, RuntimeStopReason};
use crate::startup::{RedbStartupError, open_redb_startup};

const RUNTIME_WORKER_THREADS: usize = 2;
const REQUEST_DURATION_LIMIT: Duration = Duration::from_secs(30);
const TRANSPORT_DRAIN_LIMIT: Duration = Duration::from_secs(35);
const RUNTIME_DRAIN_LIMIT: Duration = Duration::from_secs(5);
const SHUTDOWN_COMMAND: &[u8] = b"shutdown\n";
const READY_PROTOCOL: &str = "riffdbd-ready-v1";

type ShutdownCommandResult = Result<(), ShutdownCommandError>;
type ShutdownReceiver = oneshot::Receiver<ShutdownCommandResult>;

/// Runs the production `riffdbd` process and returns a conventional exit status.
///
/// Process errors are intentionally rendered as one static, nonsecret message.
#[must_use]
pub fn riffdbd_main() -> ExitCode {
    match run_from_process() {
        Ok(()) => ExitCode::SUCCESS,
        Err(_error) => {
            eprintln!("riffdbd terminated without reaching a clean process boundary");
            ExitCode::FAILURE
        }
    }
}

fn run_from_process() -> Result<(), DaemonError> {
    let config = ServerConfig::from_process_args().map_err(DaemonError::Config)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(RUNTIME_WORKER_THREADS)
        .enable_all()
        .build()
        .map_err(DaemonError::Runtime)?;
    let result = runtime.block_on(run_server(config));
    runtime.shutdown_timeout(RUNTIME_DRAIN_LIMIT);
    result
}

async fn run_server(config: ServerConfig) -> Result<(), DaemonError> {
    let clocks = ProductionWallClocks::new();
    let started_at = clocks.process_time().map_err(DaemonError::ProcessClock)?;
    let digest_keys = load_digest_key_providers(
        None,
        Some(config.capability_key_path()),
        None,
        Some(config.idempotency_key_path()),
    )
    .map_err(|_| DaemonError::DigestKeys)?;
    let authorization_time = clocks
        .authorization()
        .now()
        .map_err(DaemonError::StartupClock)?;
    let startup_inputs = startup_validation_inputs(&digest_keys, authorization_time)?;
    let identifiers = ProductionIdentifierSources::new();
    let database_ids = identifiers.database_ids();

    let (initializing, activator, issuer) = RiffDbService::begin_initialization();
    let routing = RuntimeRoutingState::new();
    let lifecycle = Arc::new(ProductionLifecycleRoute::new(
        initializing,
        issuer,
        routing.clone(),
    ));
    let lifecycle_for_grpc: Arc<dyn GrpcLifecycleRoute> = lifecycle.clone();
    let limits = GrpcRequestLimits::new(REQUEST_DURATION_LIMIT)
        .map_err(|_| DaemonError::GrpcConfiguration)?;
    let application = GrpcApplication::new(lifecycle_for_grpc, limits);
    let mut transport = HostedGrpc::bind(config.listen_address(), &application)?;

    // ADR-0034 requires the real listener to exist before this blocking proof.
    let startup = match open_redb_startup(config.database_path(), startup_inputs, &database_ids) {
        Ok(startup) => startup,
        Err(source) => {
            lifecycle.stop();
            transport.drain_after_signal().await?;
            return Err(DaemonError::Startup(source));
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

    if transport.is_finished() {
        let notification_stop_failed = graph.begin_transport_shutdown().is_err();
        let completion = transport.completed().await;
        let graph_result = graph.shutdown().await;
        if notification_stop_failed {
            return Err(DaemonError::NotificationShutdown);
        }
        graph_result.map_err(DaemonError::GraphShutdown)?;
        return Err(completion.err().unwrap_or(DaemonError::TransportEnded));
    }

    if let Err(source) = publish_readiness(transport.local_address()) {
        shutdown_before_ready(graph, &mut transport).await?;
        return Err(source);
    }
    let (shutdown, stdin_thread) = match spawn_shutdown_reader() {
        Ok(reader) => reader,
        Err(source) => {
            shutdown_before_ready(graph, &mut transport).await?;
            return Err(source);
        }
    };
    supervise_ready_process(graph, routing, transport, shutdown, stdin_thread).await
}

async fn shutdown_before_ready(
    graph: RunningProductionGraph,
    transport: &mut HostedGrpc,
) -> Result<(), DaemonError> {
    let notification_stop_failed = graph.begin_transport_shutdown().is_err();
    transport.drain_after_signal().await?;
    graph.shutdown().await.map_err(DaemonError::GraphShutdown)?;
    if notification_stop_failed {
        Err(DaemonError::NotificationShutdown)
    } else {
        Ok(())
    }
}

fn startup_validation_inputs(
    digest_keys: &DigestKeyProviders,
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
        "not-hosted-p1",
    )
    .map_err(|_| DaemonError::BuildInfo)
}

async fn supervise_ready_process(
    graph: RunningProductionGraph,
    routing: RuntimeRoutingState,
    mut transport: HostedGrpc,
    mut shutdown: ShutdownReceiver,
    stdin_thread: JoinHandle<()>,
) -> Result<(), DaemonError> {
    enum Trigger {
        Command(Result<Result<(), ShutdownCommandError>, oneshot::error::RecvError>),
        Runtime(RuntimeStopReason),
        Transport(Result<Result<(), tonic::transport::Error>, JoinError>),
    }

    let trigger = tokio::select! {
        command = &mut shutdown => Trigger::Command(command),
        reason = routing.stopped() => Trigger::Runtime(reason),
        completion = &mut transport.task => Trigger::Transport(completion),
    };

    let notification_stop_failed = graph.begin_transport_shutdown().is_err();
    let transport_result = match &trigger {
        Trigger::Transport(completion) => classify_transport_completion(completion),
        Trigger::Command(_) | Trigger::Runtime(_) => transport.drain_after_signal().await,
    };
    if !transport_is_terminal(&transport_result) {
        return transport_result;
    }

    let graph_result = graph.shutdown().await;
    let trigger_result = match trigger {
        Trigger::Command(Ok(Ok(()))) => stdin_thread
            .join()
            .map_err(|_| DaemonError::ShutdownReaderPanicked),
        Trigger::Command(Ok(Err(_))) | Trigger::Command(Err(_)) => {
            let _ = stdin_thread.join();
            Err(DaemonError::ShutdownInput)
        }
        Trigger::Runtime(_reason) => Err(DaemonError::RuntimeStopped),
        Trigger::Transport(Ok(Ok(()))) => Err(DaemonError::TransportEnded),
        Trigger::Transport(Ok(Err(_))) => Err(DaemonError::Transport),
        Trigger::Transport(Err(_)) => Err(DaemonError::TransportTask),
    };

    if notification_stop_failed {
        return Err(DaemonError::NotificationShutdown);
    }
    graph_result.map_err(DaemonError::GraphShutdown)?;
    transport_result?;
    trigger_result
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

fn read_shutdown_command(reader: &mut impl Read) -> Result<(), ShutdownCommandError> {
    let mut command = [0_u8; SHUTDOWN_COMMAND.len()];
    let mut received = 0;
    while received < command.len() {
        let count = reader
            .read(&mut command[received..])
            .map_err(|_| ShutdownCommandError)?;
        if count == 0 {
            return Err(ShutdownCommandError);
        }
        received += count;
    }
    if command == SHUTDOWN_COMMAND {
        Ok(())
    } else {
        Err(ShutdownCommandError)
    }
}

struct HostedGrpc {
    local_address: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: TokioJoinHandle<Result<(), tonic::transport::Error>>,
}

impl HostedGrpc {
    fn bind(address: SocketAddr, application: &GrpcApplication) -> Result<Self, DaemonError> {
        let incoming = TcpIncoming::bind(address).map_err(DaemonError::Listener)?;
        let local_address = incoming.local_addr().map_err(DaemonError::Listener)?;
        let (shutdown, stopped) = oneshot::channel();
        let router = Server::builder()
            .add_service(application.contract_server())
            .add_service(application.command_server())
            .add_service(application.query_server())
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ShutdownCommandError;

enum DaemonError {
    Config(ServerConfigError),
    Runtime(io::Error),
    ProcessClock(ServerProcessClockError),
    DigestKeys,
    StartupClock(AuthorizationClockError),
    StartupInventory(StorageValueError),
    GrpcConfiguration,
    Listener(io::Error),
    Startup(RedbStartupError),
    BuildInfo,
    GraphBuild(ProductionGraphBuildError),
    Readiness(io::Error),
    ShutdownReader(io::Error),
    ShutdownReaderPanicked,
    ShutdownInput,
    NotificationShutdown,
    RuntimeStopped,
    TransportEnded,
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
            | Self::ShutdownReader(source) => Some(source),
            Self::ProcessClock(source) => Some(source),
            Self::StartupClock(source) => Some(source),
            Self::StartupInventory(source) => Some(source),
            Self::Startup(source) => Some(source),
            Self::GraphBuild(source) => Some(source),
            Self::GraphShutdown(source) => Some(source),
            Self::DigestKeys
            | Self::GrpcConfiguration
            | Self::BuildInfo
            | Self::ShutdownReaderPanicked
            | Self::ShutdownInput
            | Self::NotificationShutdown
            | Self::RuntimeStopped
            | Self::TransportEnded
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
            Ok(())
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
    fn production_listener_precedes_the_blocking_startup_proof() {
        let source = include_str!("daemon.rs");
        let production = source
            .split_once("#[cfg(test)]")
            .expect("daemon architecture boundary")
            .0;
        let listener = production.find("HostedGrpc::bind(").expect("listener bind");
        let startup = production
            .find("open_redb_startup(")
            .expect("startup proof");
        let readiness = production.find("publish_readiness(").expect("readiness");
        assert!(listener < startup);
        assert!(startup < readiness);
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
