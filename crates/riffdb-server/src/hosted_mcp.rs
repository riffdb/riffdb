#![expect(
    clippy::expect_used,
    reason = "the fixed loopback MCP authority and URI satisfy the parser's validated grammar"
)]

//! Owned loopback HTTP transport for the native MCP service.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::{Future, IntoFuture};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::task::Poll;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{ConnectInfo, State};
use axum::http::{Request, Response, StatusCode};
use axum::routing::any;
use futures_util::stream::FuturesUnordered;
use riffdb_api_grpc::DATABASE_METADATA_KEY;
use riffdb_api_mcp::{
    HostedMcpHttpConfiguration, HostedMcpHttpConfigurationError, HostedMcpHttpRegistration,
    HostedMcpSessionMaintenanceError, HostedServiceMcpBackend, McpAdmissionSessionKey,
    McpInflightLimiter, McpMonotonicClock, McpRateLimitConfig, McpRateLimiter, McpTransportKind,
    RequestIdSource, RiffDbMcpServer, SystemMcpMonotonicClock, SystemMcpRateClock,
    register_hosted_service_mcp_http,
};
use riffdb_types::{DatabaseAlias, MAX_DATABASES_PER_PROCESS};
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinHandle};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_stream::Stream as _;
use tower_service::Service;

use crate::process_graph::HostedMcpDependencies;

const SESSION_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(5);
const HOSTED_MCP_DRAIN_LIMIT: Duration = Duration::from_secs(35);
const SESSION_KEY_PREFIX: &str = "riffdb-http-";

type HostedMcpService = RiffDbMcpServer<HostedServiceMcpBackend>;
type HostedMcpRegistration = HostedMcpHttpRegistration<HostedMcpService>;

/// One bound MCP listener plus its session-maintenance owner.
pub(crate) struct HostedMcp {
    routes: HostedMcpRoutes,
    factory: HostedMcpRegistrationFactory,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), HostedMcpTaskError>>,
}

#[derive(Clone)]
struct HostedMcpRegistrationFactory {
    listener: crate::config::McpListenerConfig,
    allowed_origins: Arc<[String]>,
    rate_limiter: Arc<McpRateLimiter<SystemMcpRateClock>>,
    monotonic_clock: Arc<dyn McpMonotonicClock>,
    inflight: Arc<McpInflightLimiter>,
    session_sequence: Arc<AtomicU64>,
}

impl HostedMcpRegistrationFactory {
    fn build_config(
        listener: &crate::config::McpListenerConfig,
        authentication: riffdb_auth::AuthenticationContext,
        allowed_origins: &[String],
    ) -> Result<HostedMcpHttpConfiguration, HostedMcpStartError> {
        match listener {
            crate::config::McpListenerConfig::LoopbackCleartext(address) => {
                HostedMcpHttpConfiguration::new(
                    *address,
                    authentication,
                    allowed_origins.iter().cloned(),
                )
                .map_err(HostedMcpStartError::Configuration)
            }
            crate::config::McpListenerConfig::DirectTls(tls_listener) => {
                HostedMcpHttpConfiguration::new_direct_tls(
                    tls_listener.listen_address(),
                    authentication,
                    allowed_origins.iter().cloned(),
                )
                .map_err(HostedMcpStartError::Configuration)
            }
        }
    }

    fn build(
        &self,
        database_alias: DatabaseAlias,
        dependencies: HostedMcpDependencies,
    ) -> Result<HostedMcpRegistration, HostedMcpStartError> {
        let authentication_audience = dependencies.authentication.audience().clone();
        let configuration = Self::build_config(
            &self.listener,
            dependencies.authentication,
            &self.allowed_origins,
        )?;
        let observer_request_ids: Arc<dyn RequestIdSource> =
            Arc::new(dependencies.request_ids.clone());
        let observer_service = Arc::new(HostedServiceMcpBackend::new(
            Arc::clone(&dependencies.service),
            observer_request_ids,
            database_alias.clone(),
            authentication_audience.clone(),
        ));
        let factory_service = Arc::clone(&dependencies.service);
        let factory_request_ids = dependencies.request_ids;
        let factory_telemetry = dependencies.telemetry;
        let session_sequence = Arc::clone(&self.session_sequence);
        let inflight = Arc::clone(&self.inflight);
        let service_factory = move || {
            let sequence = session_sequence
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    current.checked_add(1)
                })
                .map_err(|_| io::Error::other("hosted MCP session capacity exhausted"))?
                .checked_add(1)
                .ok_or_else(|| io::Error::other("hosted MCP session capacity exhausted"))?;
            let session = McpAdmissionSessionKey::new(format!("{SESSION_KEY_PREFIX}{sequence}"))
                .map_err(|_| io::Error::other("hosted MCP session identity is unavailable"))?;
            let request_ids: Arc<dyn RequestIdSource> = Arc::new(factory_request_ids.clone());
            let backend = HostedServiceMcpBackend::new(
                Arc::clone(&factory_service),
                request_ids,
                database_alias.clone(),
                authentication_audience.clone(),
            );
            Ok(RiffDbMcpServer::with_shared_admission_and_telemetry(
                backend,
                McpTransportKind::StreamableHttp,
                Arc::clone(&inflight),
                session,
                Arc::clone(&factory_telemetry),
            ))
        };
        Ok(register_hosted_service_mcp_http(
            configuration,
            dependencies.authenticator,
            Arc::clone(&self.monotonic_clock),
            Arc::clone(&self.rate_limiter),
            observer_service,
            service_factory,
        ))
    }
}

type DirectTlsMcpHandshake =
    Pin<Box<dyn Future<Output = Option<(PermittedTlsStream, SocketAddr)>> + Send + 'static>>;

struct DirectTlsMcpListener {
    tcp: tokio::net::TcpListener,
    acceptor: tokio_rustls::TlsAcceptor,
    handshake_timeout: Duration,
    idle_timeout: Duration,
    permits: Arc<tokio::sync::Semaphore>,
    maximum_handshakes: usize,
    handshakes: FuturesUnordered<DirectTlsMcpHandshake>,
    accept_backoff: Option<Pin<Box<tokio::time::Sleep>>>,
    local_addr: SocketAddr,
}

impl DirectTlsMcpListener {
    fn new(
        tcp: tokio::net::TcpListener,
        acceptor: tokio_rustls::TlsAcceptor,
        handshake_timeout: Duration,
        idle_timeout: Duration,
        maximum_connections: usize,
        local_addr: SocketAddr,
    ) -> Self {
        Self {
            tcp,
            acceptor,
            handshake_timeout,
            idle_timeout,
            permits: Arc::new(tokio::sync::Semaphore::new(maximum_connections)),
            maximum_handshakes: maximum_connections,
            handshakes: FuturesUnordered::new(),
            accept_backoff: None,
            local_addr,
        }
    }
}

struct PermittedTlsStream {
    inner: tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
    _permit: tokio::sync::OwnedSemaphorePermit,
    idle_timeout: Duration,
    read_idle: Pin<Box<tokio::time::Sleep>>,
    write_idle: Pin<Box<tokio::time::Sleep>>,
}

impl tokio::io::AsyncRead for PermittedTlsStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        if self.read_idle.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "connection idle limit elapsed",
            )));
        }
        let before = buf.filled().len();
        let result = std::pin::Pin::new(&mut self.inner).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(()))) && buf.filled().len() > before {
            let deadline = tokio::time::Instant::now() + self.idle_timeout;
            self.read_idle.as_mut().reset(deadline);
            self.write_idle.as_mut().reset(deadline);
        }
        result
    }
}

impl tokio::io::AsyncWrite for PermittedTlsStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        if self.write_idle.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "connection idle limit elapsed",
            )));
        }
        let result = std::pin::Pin::new(&mut self.inner).poll_write(cx, buf);
        if matches!(result, Poll::Ready(Ok(written)) if written > 0) {
            let deadline = tokio::time::Instant::now() + self.idle_timeout;
            self.read_idle.as_mut().reset(deadline);
            self.write_idle.as_mut().reset(deadline);
        }
        result
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> std::task::Poll<io::Result<usize>> {
        if self.write_idle.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "connection idle limit elapsed",
            )));
        }
        let result = std::pin::Pin::new(&mut self.inner).poll_write_vectored(cx, buffers);
        if matches!(result, Poll::Ready(Ok(written)) if written > 0) {
            let deadline = tokio::time::Instant::now() + self.idle_timeout;
            self.read_idle.as_mut().reset(deadline);
            self.write_idle.as_mut().reset(deadline);
        }
        result
    }
}

impl axum::serve::Listener for DirectTlsMcpListener {
    type Io = PermittedTlsStream;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        std::future::poll_fn(|context| {
            loop {
                while self.handshakes.len() < self.maximum_handshakes {
                    if let Some(backoff) = self.accept_backoff.as_mut() {
                        if backoff.as_mut().poll(context).is_pending() {
                            break;
                        }
                        self.accept_backoff = None;
                    }
                    match self.tcp.poll_accept(context) {
                        Poll::Ready(Ok((tcp_stream, peer_addr))) => {
                            let _ = tcp_stream.set_nodelay(true);
                            let Ok(permit) = Arc::clone(&self.permits).try_acquire_owned() else {
                                drop(tcp_stream);
                                continue;
                            };
                            let acceptor = self.acceptor.clone();
                            let timeout = self.handshake_timeout;
                            let idle_timeout = self.idle_timeout;
                            self.handshakes.push(Box::pin(async move {
                                match tokio::time::timeout(timeout, acceptor.accept(tcp_stream))
                                    .await
                                {
                                    Ok(Ok(tls_stream)) => Some((
                                        PermittedTlsStream {
                                            inner: tls_stream,
                                            _permit: permit,
                                            idle_timeout,
                                            read_idle: Box::pin(tokio::time::sleep(idle_timeout)),
                                            write_idle: Box::pin(tokio::time::sleep(idle_timeout)),
                                        },
                                        peer_addr,
                                    )),
                                    Ok(Err(_)) | Err(_) => None,
                                }
                            }));
                        }
                        Poll::Ready(Err(_)) => {
                            self.accept_backoff =
                                Some(Box::pin(tokio::time::sleep(Duration::from_millis(10))));
                        }
                        Poll::Pending => break,
                    }
                }
                match Pin::new(&mut self.handshakes).poll_next(context) {
                    Poll::Ready(Some(Some(accepted))) => return Poll::Ready(accepted),
                    Poll::Ready(Some(None)) => continue,
                    Poll::Ready(None) | Poll::Pending => return Poll::Pending,
                }
            }
        })
        .await
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        Ok(self.local_addr)
    }
}

#[derive(Clone, Copy)]
struct McpPeerAddr(SocketAddr);

impl
    axum::extract::connect_info::Connected<axum::serve::IncomingStream<'_, tokio::net::TcpListener>>
    for McpPeerAddr
{
    fn connect_info(stream: axum::serve::IncomingStream<'_, tokio::net::TcpListener>) -> Self {
        Self(*stream.remote_addr())
    }
}

impl axum::extract::connect_info::Connected<axum::serve::IncomingStream<'_, DirectTlsMcpListener>>
    for McpPeerAddr
{
    fn connect_info(stream: axum::serve::IncomingStream<'_, DirectTlsMcpListener>) -> Self {
        Self(*stream.remote_addr())
    }
}

impl HostedMcp {
    #[allow(dead_code)]
    pub(crate) async fn bind(
        address: SocketAddr,
        allowed_origins: &[String],
        dependencies: HostedMcpDependencies,
    ) -> Result<Self, HostedMcpStartError> {
        Self::bind_routes(
            address,
            allowed_origins,
            [(DatabaseAlias::default_alias(), dependencies)],
        )
        .await
    }

    pub(crate) async fn bind_listener(
        listener: &crate::config::McpListenerConfig,
        allowed_origins: &[String],
        dependencies: HostedMcpDependencies,
    ) -> Result<Self, HostedMcpStartError> {
        Self::bind_listener_routes(
            listener,
            allowed_origins,
            [(DatabaseAlias::default_alias(), dependencies)],
        )
        .await
    }

    #[allow(dead_code)]
    pub(crate) async fn bind_routes(
        address: SocketAddr,
        allowed_origins: &[String],
        dependencies: impl IntoIterator<Item = (DatabaseAlias, HostedMcpDependencies)>,
    ) -> Result<Self, HostedMcpStartError> {
        Self::bind_listener_routes(
            &crate::config::McpListenerConfig::LoopbackCleartext(address),
            allowed_origins,
            dependencies,
        )
        .await
    }

    pub(crate) async fn bind_listener_routes(
        listener_config: &crate::config::McpListenerConfig,
        allowed_origins: &[String],
        dependencies: impl IntoIterator<Item = (DatabaseAlias, HostedMcpDependencies)>,
    ) -> Result<Self, HostedMcpStartError> {
        let dependencies = dependencies.into_iter().collect::<Vec<_>>();
        if dependencies.is_empty() || dependencies.len() > MAX_DATABASES_PER_PROCESS {
            return Err(HostedMcpStartError::Routes);
        }
        let configuration = HostedMcpRegistrationFactory::build_config(
            listener_config,
            dependencies[0].1.authentication.clone(),
            allowed_origins,
        )?;
        let tls_acceptor = match listener_config {
            crate::config::McpListenerConfig::LoopbackCleartext(_) => None,
            crate::config::McpListenerConfig::DirectTls(tls_listener) => {
                let acceptor = crate::daemon::load_http1_tls_acceptor(tls_listener)?;
                Some((acceptor, tls_listener.bounds()))
            }
        };
        let listener = tokio::net::TcpListener::bind(configuration.bind_address())
            .await
            .map_err(HostedMcpStartError::Listener)?;
        let local_addr = listener
            .local_addr()
            .map_err(HostedMcpStartError::Listener)?;
        let factory = HostedMcpRegistrationFactory {
            listener: listener_config.clone(),
            allowed_origins: Arc::from(allowed_origins.to_vec()),
            rate_limiter: Arc::new(McpRateLimiter::new(
                SystemMcpRateClock::new(),
                McpRateLimitConfig::poc_default(),
            )),
            monotonic_clock: Arc::new(SystemMcpMonotonicClock::new()),
            inflight: Arc::new(McpInflightLimiter::new()),
            session_sequence: Arc::new(AtomicU64::new(0)),
        };

        let mut routes = BTreeMap::new();
        for (alias, dependencies) in dependencies {
            let registration = factory.build(alias.clone(), dependencies)?;
            if routes.insert(alias, registration).is_some() {
                return Err(HostedMcpStartError::Routes);
            }
        }
        let configured = Arc::new(RwLock::new(routes.keys().cloned().collect()));
        let routes = HostedMcpRoutes {
            active: Arc::new(RwLock::new(routes)),
            configured,
        };
        let application = Router::new()
            .fallback(any(proxy))
            .layer(axum::middleware::from_fn(crate::telemetry_ingress::mcp))
            .with_state(routes.clone());
        let (shutdown, stopped) = oneshot::channel();
        let routes_for_maintenance = routes.clone();
        let task = tokio::spawn(async move {
            let server_future: std::pin::Pin<
                Box<dyn std::future::Future<Output = io::Result<()>> + Send>,
            > = match tls_acceptor {
                None => Box::pin(
                    axum::serve(
                        listener,
                        application.into_make_service_with_connect_info::<McpPeerAddr>(),
                    )
                    .with_graceful_shutdown(async move {
                        let _ = stopped.await;
                    })
                    .into_future(),
                ),
                Some((acceptor, bounds)) => {
                    let tls_listener = DirectTlsMcpListener::new(
                        listener,
                        acceptor,
                        bounds.handshake_timeout(),
                        bounds.idle_timeout(),
                        bounds.max_connections().get() as usize,
                        local_addr,
                    );
                    Box::pin(
                        axum::serve(
                            tls_listener,
                            application.into_make_service_with_connect_info::<McpPeerAddr>(),
                        )
                        .with_graceful_shutdown(async move {
                            let _ = stopped.await;
                        })
                        .into_future(),
                    )
                }
            };
            tokio::pin!(server_future);
            let mut interval = tokio::time::interval_at(
                Instant::now() + SESSION_MAINTENANCE_INTERVAL,
                SESSION_MAINTENANCE_INTERVAL,
            );
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    result = &mut server_future => {
                        return result.map_err(|_| HostedMcpTaskError::Server);
                    }
                    _ = interval.tick() => {
                        let registrations = routes_for_maintenance
                            .snapshot()
                            .map_err(HostedMcpTaskError::Maintenance)?;
                        for registration in &registrations {
                            registration
                                .expire_due_sessions()
                                .await
                                .map_err(HostedMcpTaskError::Maintenance)?;
                        }
                    }
                }
            }
        });

        Ok(Self {
            routes,
            factory,
            shutdown: Some(shutdown),
            task,
        })
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    pub(crate) fn begin_shutdown(&mut self) {
        for registration in self.routes.snapshot().unwrap_or_default() {
            registration.shutdown();
        }
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }

    pub(crate) async fn drain_after_signal(&mut self) -> Result<(), HostedMcpStopError> {
        self.begin_shutdown();
        let completion = tokio::time::timeout(HOSTED_MCP_DRAIN_LIMIT, &mut self.task)
            .await
            .map_err(|_| HostedMcpStopError::DrainTimeout)?;
        classify_completion(&completion)
    }

    pub(crate) async fn completed(&mut self) -> Result<(), HostedMcpStopError> {
        let completion = (&mut self.task).await;
        classify_completion(&completion)
    }

    pub(crate) fn replace(
        &self,
        alias: &DatabaseAlias,
        dependencies: HostedMcpDependencies,
    ) -> Result<(), HostedMcpStartError> {
        let registration = self.factory.build(alias.clone(), dependencies)?;
        let configured = self
            .routes
            .configured
            .read()
            .map_err(|_| HostedMcpStartError::Routes)?;
        if !configured.contains(alias) {
            return Err(HostedMcpStartError::Routes);
        }
        let mut routes = self
            .routes
            .active
            .write()
            .map_err(|_| HostedMcpStartError::Routes)?;
        if let Some(existing) = routes.insert(alias.clone(), registration) {
            existing.shutdown();
        }
        Ok(())
    }

    pub(crate) fn insert(
        &self,
        alias: DatabaseAlias,
        dependencies: HostedMcpDependencies,
    ) -> Result<(), HostedMcpStartError> {
        let registration = self.factory.build(alias.clone(), dependencies)?;
        let mut configured = self
            .routes
            .configured
            .write()
            .map_err(|_| HostedMcpStartError::Routes)?;
        if configured.len() >= MAX_DATABASES_PER_PROCESS || configured.contains(&alias) {
            return Err(HostedMcpStartError::Routes);
        }
        let mut active = self
            .routes
            .active
            .write()
            .map_err(|_| HostedMcpStartError::Routes)?;
        configured.insert(alias.clone());
        active.insert(alias, registration);
        Ok(())
    }

    pub(crate) fn remove(&self, alias: &DatabaseAlias) -> Result<(), HostedMcpStartError> {
        let mut configured = self
            .routes
            .configured
            .write()
            .map_err(|_| HostedMcpStartError::Routes)?;
        if configured.len() <= 1 || !configured.contains(alias) {
            return Err(HostedMcpStartError::Routes);
        }
        let mut active = self
            .routes
            .active
            .write()
            .map_err(|_| HostedMcpStartError::Routes)?;
        configured.remove(alias);
        let registration = active.remove(alias);
        drop(active);
        drop(configured);
        if let Some(registration) = registration {
            registration.shutdown();
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, alias: &DatabaseAlias) -> bool {
        self.routes
            .configured
            .read()
            .is_ok_and(|configured| configured.contains(alias))
    }

    pub(crate) fn suspend(&self, alias: &DatabaseAlias) -> Result<(), HostedMcpStartError> {
        let configured = self
            .routes
            .configured
            .read()
            .map_err(|_| HostedMcpStartError::Routes)?;
        if !configured.contains(alias) {
            return Err(HostedMcpStartError::Routes);
        }
        let registration = self
            .routes
            .active
            .write()
            .map_err(|_| HostedMcpStartError::Routes)?
            .remove(alias);
        drop(configured);
        if let Some(registration) = registration {
            registration.shutdown();
        }
        Ok(())
    }
}

#[derive(Clone)]
struct HostedMcpRoutes {
    active: Arc<RwLock<BTreeMap<DatabaseAlias, HostedMcpRegistration>>>,
    configured: Arc<RwLock<BTreeSet<DatabaseAlias>>>,
}

impl HostedMcpRoutes {
    fn snapshot(&self) -> Result<Vec<HostedMcpRegistration>, HostedMcpSessionMaintenanceError> {
        self.active
            .read()
            .map(|routes| routes.values().cloned().collect())
            .map_err(|_| HostedMcpSessionMaintenanceError)
    }
}

impl fmt::Debug for HostedMcp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HostedMcp([CAPABILITIES])")
    }
}

async fn proxy(
    State(routes): State<HostedMcpRoutes>,
    ConnectInfo(McpPeerAddr(peer)): ConnectInfo<McpPeerAddr>,
    request: Request<Body>,
) -> Response<Body> {
    let mut values = request.headers().get_all(DATABASE_METADATA_KEY).iter();
    let selected = values.next();
    if values.next().is_some() {
        return database_route_rejection();
    }
    let singleton = routes
        .configured
        .read()
        .is_ok_and(|configured| configured.len() == 1);
    let registration = match routes.active.read() {
        Ok(routes) => match selected {
            Some(value) => value
                .to_str()
                .ok()
                .and_then(|value| DatabaseAlias::new(value).ok())
                .and_then(|alias| routes.get(&alias).cloned()),
            None if singleton => routes.values().next().cloned(),
            None => None,
        },
        Err(_) => None,
    };
    let Some(registration) = registration else {
        return database_route_rejection();
    };
    let Ok(mut connection) = registration.service_for_peer(peer) else {
        return Response::builder()
            .status(StatusCode::FORBIDDEN)
            .body(Body::empty())
            .expect("the static MCP peer rejection response is valid");
    };
    match connection.call(request).await {
        Ok(response) => response.map(Body::new),
        Err(never) => match never {},
    }
}

fn database_route_rejection() -> Response<Body> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::empty())
        .expect("the static MCP route rejection response is valid")
}

fn classify_completion(
    completion: &Result<Result<(), HostedMcpTaskError>, JoinError>,
) -> Result<(), HostedMcpStopError> {
    match completion {
        Ok(Ok(())) => Ok(()),
        Ok(Err(HostedMcpTaskError::Server)) => Err(HostedMcpStopError::Server),
        Ok(Err(HostedMcpTaskError::Maintenance(_))) => Err(HostedMcpStopError::Maintenance),
        Err(_) => Err(HostedMcpStopError::Task),
    }
}

enum HostedMcpTaskError {
    Server,
    Maintenance(HostedMcpSessionMaintenanceError),
}

/// Closed startup failure for the optional hosted endpoint.
pub(crate) enum HostedMcpStartError {
    Configuration(HostedMcpHttpConfigurationError),
    Listener(io::Error),
    Tls,
    Routes,
}

impl fmt::Debug for HostedMcpStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HostedMcpStartError([REDACTED])")
    }
}

impl fmt::Display for HostedMcpStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("hosted MCP could not start")
    }
}

impl std::error::Error for HostedMcpStartError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Configuration(source) => Some(source),
            Self::Listener(source) => Some(source),
            Self::Tls | Self::Routes => None,
        }
    }
}

/// Closed completion failure for the optional hosted endpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HostedMcpStopError {
    Server,
    Maintenance,
    Task,
    DrainTimeout,
}

impl fmt::Display for HostedMcpStopError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("hosted MCP did not reach a clean process boundary")
    }
}

impl std::error::Error for HostedMcpStopError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_identity_is_bounded_for_every_sequence() {
        let maximum = format!("{SESSION_KEY_PREFIX}{}", u64::MAX);
        assert!(McpAdmissionSessionKey::new(maximum).is_ok());
    }

    #[test]
    fn errors_are_closed_and_redacted() {
        assert_eq!(
            HostedMcpStopError::Maintenance.to_string(),
            "hosted MCP did not reach a clean process boundary"
        );
        assert_eq!(
            format!(
                "{:?}",
                HostedMcpStartError::Configuration(HostedMcpHttpConfigurationError)
            ),
            "HostedMcpStartError([REDACTED])"
        );
    }

    // req: MCP-021, NET-001, SEC-001
    #[tokio::test]
    async fn direct_tls_mcp_listener_runs_handshakes_concurrently_when_peer_stalls() {
        use axum::serve::Listener as _;
        use riffdb_config::{
            CanonicalHttpsEndpoint, DirectTlsListenerConfig, ListenerBounds, ProtectedFilePath,
            ServerTlsFiles, issue_tls_bundle, write_protected_pem_file,
        };
        use rustls_pki_types::{CertificateDer, ServerName, pem::PemObject as _};

        let temp = tempfile::TempDir::with_prefix("riffdb-mcp-tls-").expect("temp dir");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700))
                .expect("chmod 0700 temp dir");
        }

        let tcp_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral tcp listener");
        let listen_addr = tcp_listener.local_addr().expect("listener local addr");

        let ca_cert_path = temp.path().join("ca.pem");
        let server_cert_path = temp.path().join("server.pem");
        let server_key_path = temp.path().join("server.key");
        let endpoint = CanonicalHttpsEndpoint::parse(&format!("https://{listen_addr}"))
            .expect("canonical https endpoint");
        let bundle = issue_tls_bundle(&endpoint, 30, false).expect("issue tls bundle");
        write_protected_pem_file(&ca_cert_path, bundle.ca().certificate_pem(), false)
            .expect("write ca.pem");
        write_protected_pem_file(&server_cert_path, bundle.server().certificate_pem(), false)
            .expect("write server.pem");
        write_protected_pem_file(&server_key_path, bundle.server().private_key_pem(), true)
            .expect("write server.key");

        let tls_files = ServerTlsFiles::new(
            ProtectedFilePath::new(&server_cert_path).expect("server cert"),
            ProtectedFilePath::new(&server_key_path).expect("server key"),
        )
        .expect("server tls files");
        let listener_cfg = DirectTlsListenerConfig::new(
            listen_addr,
            endpoint,
            tls_files,
            ListenerBounds::alpha_default(),
        )
        .expect("direct tls listener config");
        let acceptor =
            crate::daemon::load_http1_tls_acceptor(&listener_cfg).expect("load tls acceptor");

        let mut tls_listener = DirectTlsMcpListener::new(
            tcp_listener,
            acceptor,
            Duration::from_secs(30),
            Duration::from_millis(50),
            1,
            listen_addr,
        );

        let _stalled_peer = tokio::net::TcpStream::connect(listen_addr)
            .await
            .expect("connect stalled peer");

        let mut roots = tokio_rustls::rustls::RootCertStore::empty();
        for cert in CertificateDer::pem_slice_iter(bundle.ca().certificate_pem().as_bytes()) {
            roots
                .add(cert.expect("parse ca cert"))
                .expect("add ca cert");
        }
        let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
        let client_config = tokio_rustls::rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("protocol versions")
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));

        // Permit limit is 1; revise to 2 so stalled + active can coexist, then verify idle timeout.
        tls_listener.permits = Arc::new(tokio::sync::Semaphore::new(2));
        tls_listener.maximum_handshakes = 2;

        let client_task = tokio::spawn(async move {
            let tcp = tokio::net::TcpStream::connect(listen_addr)
                .await
                .expect("connect valid tls peer");
            let server_name = ServerName::from(listen_addr.ip());
            connector
                .connect(server_name, tcp)
                .await
                .expect("complete tls handshake")
        });

        let (mut accepted_stream, accepted_addr) =
            tokio::time::timeout(Duration::from_secs(2), tls_listener.accept())
                .await
                .expect("stalled TLS handshake must not block subsequent connection");
        let _client_stream = client_task.await.expect("join client task");
        assert!(accepted_addr.ip().is_loopback());

        let mut buf = [0u8; 8];
        let idle_err = tokio::io::AsyncReadExt::read(&mut accepted_stream, &mut buf)
            .await
            .expect_err("idle stream must time out");
        assert_eq!(idle_err.kind(), io::ErrorKind::TimedOut);
    }
}
