#![expect(
    clippy::expect_used,
    reason = "static HTTP responses use validated status codes and headers"
)]

//! Direct HTTP/JSON application ingress for `riffdbd` (`ADR-0331`).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::IntoFuture;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::extract::{ConnectInfo, DefaultBodyLimit, Path, State};
use axum::http::{
    HeaderMap, HeaderValue, Response, StatusCode, Uri,
    header::{AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE, WWW_AUTHENTICATE},
};
use axum::routing::{get, post};
use riffdb_api_grpc::DATABASE_METADATA_KEY;
use riffdb_api_mcp::format_provenance_locator;
use riffdb_auth::RetainedOpaqueCredential;
use riffdb_config::ListenerBounds;
use riffdb_errors::{PublicError, PublicErrorKind};
use riffdb_service::{
    CommandToolDiscoveryItem, CursorToken, DiscoverCommandToolsRequest,
    DiscoverCommandToolsResultRef, DiscoveryRepresentation, ExecuteCommandRequest,
    ExecuteCommandResult, ExecuteSymbolicQueryResult, GetActiveContractRequest,
    GetActiveContractResult, HealthContext, HealthRequest, HealthResult, NamedSymbolicQueryRequest,
    OutcomeResourceLocator, PageLimit, PageRequest, RequestContext, RequestControl,
    ResolveCommandOutcomeRequest, ResolveCommandOutcomeResult, SchemaBoundOutcomeRecord,
    SchemaBoundOutcomeValue, ServiceFailure, SourceName, SubmittedDecimal, SubmittedField,
    SubmittedFieldIdentity, SubmittedList, SubmittedMoney, SubmittedRecord, SubmittedValue,
    SymbolicContractSelector, SymbolicQueryParameters, SymbolicResultField, SymbolicResultRecord,
};
use riffdb_types::{
    CanonicalValue, ContractLineage, ContractVersion, CurrencyCode, DatabaseAlias, Date,
    IdempotencyKey, MAX_DATABASES_PER_PROCESS, Timestamp,
};
use tokio::sync::{Semaphore, oneshot};
use tokio::task::{JoinError, JoinHandle};

use crate::config::HttpListenerConfig;
use crate::process_graph::HostedHttpDependencies;

const HOSTED_HTTP_DRAIN_LIMIT: Duration = Duration::from_secs(35);
const HOSTED_HTTP_CALL_TIMEOUT: Duration = Duration::from_secs(30);
const JSON_CONTENT_TYPE: &str = "application/json";
const AUTHORIZATION_PREFIX: &[u8] = b"Bearer ";
const CAPABILITY_PRESENTATION_BYTES: usize = 43;
const AUTHORIZATION_VALUE_BYTES: usize = AUTHORIZATION_PREFIX.len() + CAPABILITY_PRESENTATION_BYTES;
const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";
const MAX_OUTCOME_DISCOVERY_PAGES: usize = 3;

/// One bound direct HTTP/JSON application ingress listener.
pub(crate) struct HostedHttp {
    #[allow(dead_code)]
    routes: HostedHttpRoutes,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), HostedHttpTaskError>>,
    local_addr: SocketAddr,
}

#[derive(Clone)]
struct HostedHttpRoutes {
    active: Arc<RwLock<BTreeMap<DatabaseAlias, HostedHttpDependencies>>>,
    configured: Arc<RwLock<BTreeSet<DatabaseAlias>>>,
    allow_remote_peers: bool,
    inflight_permits: Arc<Semaphore>,
}

struct DirectTlsHttpListener {
    tcp: tokio::net::TcpListener,
    acceptor: tokio_rustls::TlsAcceptor,
    handshake_timeout: Duration,
    permits: Arc<Semaphore>,
    local_addr: SocketAddr,
}

struct PermittedTlsStream {
    inner: tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl tokio::io::AsyncRead for PermittedTlsStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for PermittedTlsStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
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
}

impl axum::serve::Listener for DirectTlsHttpListener {
    type Io = PermittedTlsStream;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let Ok((tcp_stream, peer_addr)) = self.tcp.accept().await else {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            };
            let _ = tcp_stream.set_nodelay(true);
            let Ok(permit) = Arc::clone(&self.permits).try_acquire_owned() else {
                drop(tcp_stream);
                continue;
            };
            let acceptor = self.acceptor.clone();
            let timeout = self.handshake_timeout;
            if let Ok(Ok(tls_stream)) =
                tokio::time::timeout(timeout, acceptor.accept(tcp_stream)).await
            {
                return (
                    PermittedTlsStream {
                        inner: tls_stream,
                        _permit: permit,
                    },
                    peer_addr,
                );
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        Ok(self.local_addr)
    }
}

#[derive(Clone, Copy)]
struct HttpPeerAddr(SocketAddr);

impl
    axum::extract::connect_info::Connected<axum::serve::IncomingStream<'_, tokio::net::TcpListener>>
    for HttpPeerAddr
{
    fn connect_info(stream: axum::serve::IncomingStream<'_, tokio::net::TcpListener>) -> Self {
        Self(*stream.remote_addr())
    }
}

impl axum::extract::connect_info::Connected<axum::serve::IncomingStream<'_, DirectTlsHttpListener>>
    for HttpPeerAddr
{
    fn connect_info(stream: axum::serve::IncomingStream<'_, DirectTlsHttpListener>) -> Self {
        Self(*stream.remote_addr())
    }
}

impl HostedHttp {
    pub(crate) async fn bind_listener(
        listener: &HttpListenerConfig,
        dependencies: HostedHttpDependencies,
    ) -> Result<Self, HostedHttpStartError> {
        Self::bind_listener_routes(listener, [(DatabaseAlias::default_alias(), dependencies)]).await
    }

    pub(crate) async fn bind_listener_routes(
        listener_config: &HttpListenerConfig,
        dependencies: impl IntoIterator<Item = (DatabaseAlias, HostedHttpDependencies)>,
    ) -> Result<Self, HostedHttpStartError> {
        let dependencies = dependencies.into_iter().collect::<Vec<_>>();
        if dependencies.is_empty() || dependencies.len() > MAX_DATABASES_PER_PROCESS {
            return Err(HostedHttpStartError::Routes);
        }

        let (bind_addr, allow_remote_peers, bounds, tls_acceptor) = match listener_config {
            HttpListenerConfig::LoopbackCleartext(addr) => {
                if !addr.ip().is_loopback() || addr.port() == 0 {
                    return Err(HostedHttpStartError::Configuration);
                }
                (*addr, false, ListenerBounds::alpha_default(), None)
            }
            HttpListenerConfig::DirectTls(tls_listener) => {
                if tls_listener.listen_address().port() == 0 {
                    return Err(HostedHttpStartError::Configuration);
                }
                let acceptor = crate::daemon::load_http1_tls_acceptor(tls_listener)
                    .map_err(|_| HostedHttpStartError::Tls)?;
                (
                    tls_listener.listen_address(),
                    true,
                    tls_listener.bounds(),
                    Some(acceptor),
                )
            }
        };

        let listener = tokio::net::TcpListener::bind(bind_addr)
            .await
            .map_err(HostedHttpStartError::Listener)?;
        let local_addr = listener
            .local_addr()
            .map_err(HostedHttpStartError::Listener)?;

        let mut routes_map = BTreeMap::new();
        for (alias, deps) in dependencies {
            if routes_map.insert(alias, deps).is_some() {
                return Err(HostedHttpStartError::Routes);
            }
        }
        let configured = Arc::new(RwLock::new(routes_map.keys().cloned().collect()));
        let routes = HostedHttpRoutes {
            active: Arc::new(RwLock::new(routes_map)),
            configured,
            allow_remote_peers,
            inflight_permits: Arc::new(Semaphore::new(bounds.max_connections().get() as usize)),
        };

        let application = Router::new()
            .route("/v1/health", get(handle_health))
            .route(
                "/v1/databases/{database}/commands/{command}",
                post(handle_execute_command),
            )
            .route(
                "/v1/databases/{database}/queries/{module}/{query}",
                post(handle_execute_query),
            )
            .route(
                "/v1/databases/{database}/outcomes/{idempotency_key}",
                get(handle_resolve_outcome),
            )
            .layer(DefaultBodyLimit::max(
                riffdb_service::MAX_SERVICE_COMMAND_REQUEST_BYTES,
            ))
            .with_state(routes.clone());

        let (shutdown, stopped) = oneshot::channel();
        let max_connections = bounds.max_connections().get() as usize;
        let handshake_timeout = bounds.handshake_timeout();
        let task = tokio::spawn(async move {
            let server_future: std::pin::Pin<
                Box<dyn std::future::Future<Output = io::Result<()>> + Send>,
            > = match tls_acceptor {
                None => Box::pin(
                    axum::serve(
                        listener,
                        application.into_make_service_with_connect_info::<HttpPeerAddr>(),
                    )
                    .with_graceful_shutdown(async move {
                        let _ = stopped.await;
                    })
                    .into_future(),
                ),
                Some(acceptor) => {
                    let tls_listener = DirectTlsHttpListener {
                        tcp: listener,
                        acceptor,
                        handshake_timeout,
                        permits: Arc::new(Semaphore::new(max_connections)),
                        local_addr,
                    };
                    Box::pin(
                        axum::serve(
                            tls_listener,
                            application.into_make_service_with_connect_info::<HttpPeerAddr>(),
                        )
                        .with_graceful_shutdown(async move {
                            let _ = stopped.await;
                        })
                        .into_future(),
                    )
                }
            };
            server_future.await.map_err(|_| HostedHttpTaskError::Server)
        });

        Ok(Self {
            routes,
            shutdown: Some(shutdown),
            task,
            local_addr,
        })
    }

    #[allow(dead_code)]
    #[must_use]
    pub(crate) const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    pub(crate) fn begin_shutdown(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }

    pub(crate) async fn drain_after_signal(&mut self) -> Result<(), HostedHttpStopError> {
        self.begin_shutdown();
        let completion = tokio::time::timeout(HOSTED_HTTP_DRAIN_LIMIT, &mut self.task)
            .await
            .map_err(|_| HostedHttpStopError::DrainTimeout)?;
        classify_completion(&completion)
    }

    pub(crate) async fn completed(&mut self) -> Result<(), HostedHttpStopError> {
        let completion = (&mut self.task).await;
        classify_completion(&completion)
    }

    #[allow(dead_code)]
    pub(crate) fn replace(
        &self,
        alias: &DatabaseAlias,
        dependencies: HostedHttpDependencies,
    ) -> Result<(), HostedHttpStartError> {
        let configured = self
            .routes
            .configured
            .read()
            .map_err(|_| HostedHttpStartError::Routes)?;
        if !configured.contains(alias) {
            return Err(HostedHttpStartError::Routes);
        }
        let mut active = self
            .routes
            .active
            .write()
            .map_err(|_| HostedHttpStartError::Routes)?;
        active.insert(alias.clone(), dependencies);
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn insert(
        &self,
        alias: DatabaseAlias,
        dependencies: HostedHttpDependencies,
    ) -> Result<(), HostedHttpStartError> {
        let mut configured = self
            .routes
            .configured
            .write()
            .map_err(|_| HostedHttpStartError::Routes)?;
        if configured.len() >= MAX_DATABASES_PER_PROCESS || configured.contains(&alias) {
            return Err(HostedHttpStartError::Routes);
        }
        let mut active = self
            .routes
            .active
            .write()
            .map_err(|_| HostedHttpStartError::Routes)?;
        configured.insert(alias.clone());
        active.insert(alias, dependencies);
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn remove(&self, alias: &DatabaseAlias) -> Result<(), HostedHttpStartError> {
        let mut configured = self
            .routes
            .configured
            .write()
            .map_err(|_| HostedHttpStartError::Routes)?;
        if configured.len() <= 1 || !configured.contains(alias) {
            return Err(HostedHttpStartError::Routes);
        }
        let mut active = self
            .routes
            .active
            .write()
            .map_err(|_| HostedHttpStartError::Routes)?;
        configured.remove(alias);
        active.remove(alias);
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn suspend(&self, alias: &DatabaseAlias) -> Result<(), HostedHttpStartError> {
        let configured = self
            .routes
            .configured
            .read()
            .map_err(|_| HostedHttpStartError::Routes)?;
        if !configured.contains(alias) {
            return Err(HostedHttpStartError::Routes);
        }
        self.routes
            .active
            .write()
            .map_err(|_| HostedHttpStartError::Routes)?
            .remove(alias);
        Ok(())
    }
}

impl fmt::Debug for HostedHttp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HostedHttp([CAPABILITIES])")
    }
}

enum HostedHttpTaskError {
    Server,
}

/// Closed startup failure for the optional direct HTTP/JSON endpoint.
pub(crate) enum HostedHttpStartError {
    Configuration,
    Listener(io::Error),
    Tls,
    Routes,
}

impl fmt::Debug for HostedHttpStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HostedHttpStartError([REDACTED])")
    }
}

impl fmt::Display for HostedHttpStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("hosted HTTP/JSON ingress could not start")
    }
}

impl std::error::Error for HostedHttpStartError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Listener(source) => Some(source),
            Self::Configuration | Self::Tls | Self::Routes => None,
        }
    }
}

/// Closed completion failure for the optional direct HTTP/JSON endpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HostedHttpStopError {
    Server,
    Task,
    DrainTimeout,
}

impl fmt::Display for HostedHttpStopError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("hosted HTTP/JSON ingress did not reach a clean process boundary")
    }
}

impl std::error::Error for HostedHttpStopError {}

fn classify_completion(
    completion: &Result<Result<(), HostedHttpTaskError>, JoinError>,
) -> Result<(), HostedHttpStopError> {
    match completion {
        Ok(Ok(())) => Ok(()),
        Ok(Err(HostedHttpTaskError::Server)) => Err(HostedHttpStopError::Server),
        Err(_) => Err(HostedHttpStopError::Task),
    }
}

#[derive(Default)]
struct HealthQueryParams {
    database: Option<String>,
}

#[derive(Default)]
struct OutcomeQueryParams {
    command: Option<String>,
    lineage: Option<String>,
    outcome_uri: Option<String>,
}

fn parse_query_params(uri: &Uri) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    let Some(query) = uri.query() else {
        return map;
    };
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (raw_key, raw_val) = pair.split_once('=').unwrap_or((pair, ""));
        if let (Ok(key), Ok(val)) = (percent_decode_str(raw_key), percent_decode_str(raw_val)) {
            map.insert(key, val);
        }
    }
    map
}

fn percent_decode_str(input: &str) -> Result<String, ()> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut idx = 0;
    while idx < bytes.len() {
        match bytes[idx] {
            b'+' => {
                out.push(b' ');
                idx += 1;
            }
            b'%' => {
                if idx + 2 >= bytes.len() {
                    return Err(());
                }
                let hi = decode_hex_nibble(bytes[idx + 1]).ok_or(())?;
                let lo = decode_hex_nibble(bytes[idx + 2]).ok_or(())?;
                out.push((hi << 4) | lo);
                idx += 3;
            }
            byte => {
                out.push(byte);
                idx += 1;
            }
        }
    }
    String::from_utf8(out).map_err(|_| ())
}

const fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

async fn handle_health(
    State(routes): State<HostedHttpRoutes>,
    ConnectInfo(HttpPeerAddr(peer)): ConnectInfo<HttpPeerAddr>,
    uri: Uri,
    mut headers: HeaderMap,
) -> Response<Body> {
    if !routes.allow_remote_peers && !peer.ip().is_loopback() {
        return json_error(StatusCode::FORBIDDEN, "forbidden", "peer is not permitted");
    }
    let Ok(_permit) = Arc::clone(&routes.inflight_permits).try_acquire_owned() else {
        return json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "overloaded",
            "service is over capacity",
        );
    };

    if !headers.contains_key(AUTHORIZATION) {
        return json_response(StatusCode::OK, &serde_json::json!({ "status": "alive" }));
    }

    let Ok(retained) = take_authorization(&mut headers) else {
        return unauthorized_response();
    };

    let mut params = parse_query_params(&uri);
    let query = HealthQueryParams {
        database: params.remove("database"),
    };
    let selected_db = query.database.or_else(|| {
        headers
            .get(DATABASE_METADATA_KEY)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    });
    let Some((alias, deps)) = resolve_database_route(&routes, selected_db.as_deref()) else {
        return json_error(
            StatusCode::NOT_FOUND,
            "not_found",
            "database route not found",
        );
    };

    let Ok(principal) = deps
        .authenticator
        .authenticate(retained.borrow(), &deps.authentication)
    else {
        return unauthorized_response();
    };

    let Ok((ctx, _cancel)) = build_request_context(&deps, principal) else {
        return json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_defect",
            "internal service failure",
        );
    };

    match deps
        .service
        .health(HealthContext::authenticated(ctx), HealthRequest)
        .await
    {
        Ok(HealthResult::Authenticated(report)) => {
            let status_str = match report.status() {
                riffdb_service::HealthStatus::Ready => "ready",
                riffdb_service::HealthStatus::Degraded => "degraded",
                riffdb_service::HealthStatus::NotReady => "not_ready",
            };
            json_response(
                StatusCode::OK,
                &serde_json::json!({
                    "status": status_str,
                    "database": alias.as_str(),
                    "contract_ready": report.active_contract_version().is_some(),
                    "active_contract_version": report.active_contract_version().map(|v| v.get()),
                    "last_commit_sequence": report.last_commit_sequence().map(|s| s.get().to_string()),
                    "version": report.build().semantic_version(),
                    "executable_ir_version": report.build().contract_ir_version(),
                }),
            )
        }
        Ok(HealthResult::PreBootstrap(pre)) => json_response(
            StatusCode::OK,
            &serde_json::json!({
                "status": "pre_bootstrap",
                "database": alias.as_str(),
                "liveness": pre.liveness(),
                "readiness": pre.readiness(),
            }),
        ),
        Err(failure) => map_service_failure_response(&failure),
    }
}

async fn handle_execute_command(
    State(routes): State<HostedHttpRoutes>,
    ConnectInfo(HttpPeerAddr(peer)): ConnectInfo<HttpPeerAddr>,
    Path((database, command)): Path<(String, String)>,
    mut headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response<Body> {
    if !routes.allow_remote_peers && !peer.ip().is_loopback() {
        return json_error(StatusCode::FORBIDDEN, "forbidden", "peer is not permitted");
    }
    let Ok(_permit) = Arc::clone(&routes.inflight_permits).try_acquire_owned() else {
        return json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "overloaded",
            "service is over capacity",
        );
    };

    let Ok(retained) = take_authorization(&mut headers) else {
        return unauthorized_response();
    };
    let Some((_alias, deps)) = resolve_database_route(&routes, Some(database.as_str())) else {
        return json_error(
            StatusCode::NOT_FOUND,
            "not_found",
            "database route not found",
        );
    };
    let Ok(principal) = deps
        .authenticator
        .authenticate(retained.borrow(), &deps.authentication)
    else {
        return unauthorized_response();
    };

    let header_idempotency = match extract_idempotency_header(&headers) {
        Ok(key) => key,
        Err(()) => {
            return json_error(
                StatusCode::BAD_REQUEST,
                "input_invalid",
                "Idempotency-Key header is malformed",
            );
        }
    };

    let payload: serde_json::Value = if body.is_empty() {
        serde_json::Value::Object(serde_json::Map::new())
    } else {
        match serde_json::from_slice(&body) {
            Ok(value) => value,
            Err(_) => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_request",
                    "request body must be valid JSON",
                );
            }
        }
    };
    let Some(mut root_obj) = payload.as_object().cloned() else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "command payload must be a JSON object",
        );
    };

    let expected_version = match extract_expected_contract_version(&mut root_obj) {
        Ok(version) => version,
        Err(()) => {
            return json_error(
                StatusCode::BAD_REQUEST,
                "input_invalid",
                "expected_contract_version is invalid",
            );
        }
    };

    let mut arguments = if let Some(args_val) = root_obj
        .remove("arguments")
        .or_else(|| root_obj.remove("input"))
    {
        let Some(mut args_obj) = args_val.as_object().cloned() else {
            return json_error(
                StatusCode::BAD_REQUEST,
                "input_invalid",
                "arguments must be a JSON object",
            );
        };
        if let Some(top_key) = root_obj.remove("idempotency_key") {
            let Some(top_str) = top_key.as_str() else {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "input_invalid",
                    "idempotency_key must be a string",
                );
            };
            if let Some(existing) = args_obj.get("idempotency_key").and_then(|v| v.as_str())
                && existing != top_str
            {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "input_invalid",
                    "conflicting idempotency_key values",
                );
            }
            args_obj.insert(
                "idempotency_key".to_owned(),
                serde_json::Value::String(top_str.to_owned()),
            );
        }
        if !root_obj.is_empty() {
            return json_error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "unexpected top-level fields alongside arguments",
            );
        }
        args_obj
    } else {
        root_obj
    };

    if let Some(header_key) = header_idempotency {
        if let Some(existing) = arguments.get("idempotency_key") {
            let Some(existing_str) = existing.as_str() else {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "input_invalid",
                    "idempotency_key must be a string",
                );
            };
            if existing_str != header_key.expose_secret() {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "input_invalid",
                    "Idempotency-Key header does not match body idempotency_key",
                );
            }
        } else {
            arguments.insert(
                "idempotency_key".to_owned(),
                serde_json::Value::String(header_key.expose_secret().to_owned()),
            );
        }
    }

    let Ok(command_name) = SourceName::new(command) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "input_invalid",
            "invalid command name",
        );
    };

    let Ok(fields) = json_map_to_submitted_fields(arguments) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "input_invalid",
            "invalid command input fields",
        );
    };
    let Ok(submitted_record) = SubmittedRecord::new_command_input(fields) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "input_invalid",
            "invalid command input record",
        );
    };
    let Ok(exec_request) =
        ExecuteCommandRequest::new(command_name, expected_version, submitted_record)
    else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "input_invalid",
            "invalid command execution request",
        );
    };

    let Ok((ctx, _cancel)) = build_request_context(&deps, principal) else {
        return json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_defect",
            "internal service failure",
        );
    };

    match deps.service.execute_command(ctx, exec_request).await {
        Ok(result) => match render_command_result(&result) {
            Ok(json_body) => json_response(StatusCode::OK, &json_body),
            Err(()) => json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_defect",
                "failed to encode command outcome",
            ),
        },
        Err(failure) => map_service_failure_response(&failure),
    }
}

async fn handle_execute_query(
    State(routes): State<HostedHttpRoutes>,
    ConnectInfo(HttpPeerAddr(peer)): ConnectInfo<HttpPeerAddr>,
    Path((database, module_segment, query_name)): Path<(String, String, String)>,
    mut headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response<Body> {
    if !routes.allow_remote_peers && !peer.ip().is_loopback() {
        return json_error(StatusCode::FORBIDDEN, "forbidden", "peer is not permitted");
    }
    let Ok(_permit) = Arc::clone(&routes.inflight_permits).try_acquire_owned() else {
        return json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "overloaded",
            "service is over capacity",
        );
    };

    let Ok(retained) = take_authorization(&mut headers) else {
        return unauthorized_response();
    };
    let Some((_alias, deps)) = resolve_database_route(&routes, Some(database.as_str())) else {
        return json_error(
            StatusCode::NOT_FOUND,
            "not_found",
            "database route not found",
        );
    };
    let Ok(principal) = deps
        .authenticator
        .authenticate(retained.borrow(), &deps.authentication)
    else {
        return unauthorized_response();
    };

    let payload: serde_json::Value = if body.is_empty() {
        serde_json::Value::Object(serde_json::Map::new())
    } else {
        match serde_json::from_slice(&body) {
            Ok(value) => value,
            Err(_) => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_request",
                    "request body must be valid JSON",
                );
            }
        }
    };
    let Some(mut root_obj) = payload.as_object().cloned() else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "query payload must be a JSON object",
        );
    };

    let cursor = match extract_query_cursor(&mut root_obj) {
        Ok(cursor) => cursor,
        Err(()) => {
            return json_error(
                StatusCode::BAD_REQUEST,
                "cursor_invalid",
                "query continuation cursor is invalid",
            );
        }
    };
    let freshness = match extract_query_freshness(&mut root_obj) {
        Ok(freshness) => freshness,
        Err(()) => {
            return json_error(
                StatusCode::BAD_REQUEST,
                "input_invalid",
                "query freshness specification is invalid",
            );
        }
    };

    let parameters_map = if let Some(params_val) = root_obj.remove("parameters") {
        let Some(params_obj) = params_val.as_object().cloned() else {
            return json_error(
                StatusCode::BAD_REQUEST,
                "input_invalid",
                "parameters must be a JSON object",
            );
        };
        if !root_obj.is_empty() {
            return json_error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "unexpected top-level fields alongside parameters",
            );
        }
        params_obj
    } else {
        root_obj
    };

    let Ok(values) = json_map_to_query_parameters(parameters_map) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "input_invalid",
            "invalid query parameter values",
        );
    };
    let Ok(parameters) = SymbolicQueryParameters::new(values) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "input_invalid",
            "query parameters exceed bounds",
        );
    };

    let module_hash = parse_optional_module_hash(&module_segment);
    let Ok(mut named_request) = NamedSymbolicQueryRequest::new(
        SymbolicContractSelector::active(),
        query_name,
        module_hash,
        parameters,
    ) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "input_invalid",
            "invalid query name",
        );
    };

    if let Some(cursor) = cursor {
        named_request = named_request.with_cursor(cursor);
    }
    match freshness {
        ParsedQueryFreshness::Default => {}
        ParsedQueryFreshness::Latest => {
            named_request = named_request.with_admission_head_consistency();
        }
        ParsedQueryFreshness::AtLeast(seq) => {
            named_request = named_request.with_minimum_application_head(seq);
        }
    }

    let Ok((ctx, _cancel)) = build_request_context(&deps, principal) else {
        return json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_defect",
            "internal service failure",
        );
    };

    match deps
        .service
        .execute_named_symbolic_query(ctx, named_request)
        .await
    {
        Ok(result) => match render_query_result(&result) {
            Ok(json_body) => json_response(StatusCode::OK, &json_body),
            Err(()) => json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_defect",
                "failed to encode query result",
            ),
        },
        Err(failure) => map_service_failure_response(&failure),
    }
}

async fn handle_resolve_outcome(
    State(routes): State<HostedHttpRoutes>,
    ConnectInfo(HttpPeerAddr(peer)): ConnectInfo<HttpPeerAddr>,
    Path((database, idempotency_key_raw)): Path<(String, String)>,
    uri: Uri,
    mut headers: HeaderMap,
) -> Response<Body> {
    if !routes.allow_remote_peers && !peer.ip().is_loopback() {
        return json_error(StatusCode::FORBIDDEN, "forbidden", "peer is not permitted");
    }
    let Ok(_permit) = Arc::clone(&routes.inflight_permits).try_acquire_owned() else {
        return json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "overloaded",
            "service is over capacity",
        );
    };

    let Ok(retained) = take_authorization(&mut headers) else {
        return unauthorized_response();
    };
    let Some((_alias, deps)) = resolve_database_route(&routes, Some(database.as_str())) else {
        return json_error(
            StatusCode::NOT_FOUND,
            "not_found",
            "database route not found",
        );
    };
    let Ok(principal) = deps
        .authenticator
        .authenticate(retained.borrow(), &deps.authentication)
    else {
        return unauthorized_response();
    };

    let mut params = parse_query_params(&uri);
    let query = OutcomeQueryParams {
        command: params.remove("command"),
        lineage: params.remove("lineage"),
        outcome_uri: params.remove("outcome_uri"),
    };

    if let Some(outcome_uri) = query.outcome_uri {
        let Ok(locator) = OutcomeResourceLocator::parse(outcome_uri) else {
            return json_error(
                StatusCode::BAD_REQUEST,
                "input_invalid",
                "invalid outcome_uri",
            );
        };
        let Ok((ctx, _cancel)) = build_request_context(&deps, principal) else {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_defect",
                "internal service failure",
            );
        };
        return match deps
            .service
            .resolve_command_outcome(ctx, ResolveCommandOutcomeRequest::locator(locator))
            .await
        {
            Ok(ResolveCommandOutcomeResult::Found(found)) => {
                match render_recovered_outcome(found.journaled()) {
                    Ok(json_body) => json_response(StatusCode::OK, &json_body),
                    Err(()) => json_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal_defect",
                        "failed to encode command outcome",
                    ),
                }
            }
            Ok(ResolveCommandOutcomeResult::NotFound) => json_error(
                StatusCode::NOT_FOUND,
                "not_found",
                "command outcome not found",
            ),
            Err(failure) => map_service_failure_response(&failure),
        };
    }

    let Ok(idempotency_key) = IdempotencyKey::new(idempotency_key_raw) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "input_invalid",
            "invalid idempotency_key",
        );
    };

    let candidates = match resolve_outcome_candidates(
        &deps,
        &principal,
        query.lineage.as_deref(),
        query.command.as_deref(),
    )
    .await
    {
        Ok(candidates) => candidates,
        Err(response) => return response,
    };

    if candidates.is_empty() {
        return json_error(
            StatusCode::NOT_FOUND,
            "not_found",
            "command outcome not found",
        );
    }

    let mut last_failure: Option<ServiceFailure> = None;
    for (lineage, command_name) in candidates {
        let Ok((ctx, _cancel)) = build_request_context(&deps, principal.clone()) else {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_defect",
                "internal service failure",
            );
        };
        let request =
            ResolveCommandOutcomeRequest::new(lineage, command_name, idempotency_key.clone());
        match deps.service.resolve_command_outcome(ctx, request).await {
            Ok(ResolveCommandOutcomeResult::Found(found)) => {
                return match render_recovered_outcome(found.journaled()) {
                    Ok(json_body) => json_response(StatusCode::OK, &json_body),
                    Err(()) => json_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal_defect",
                        "failed to encode command outcome",
                    ),
                };
            }
            Ok(ResolveCommandOutcomeResult::NotFound) => {}
            Err(failure) => {
                last_failure = Some(failure);
            }
        }
    }

    if let Some(failure) = last_failure {
        return map_service_failure_response(&failure);
    }

    json_error(
        StatusCode::NOT_FOUND,
        "not_found",
        "command outcome not found",
    )
}

#[allow(clippy::result_large_err)]
async fn resolve_outcome_candidates(
    deps: &HostedHttpDependencies,
    principal: &riffdb_auth::AuthenticatedPrincipal,
    lineage_filter: Option<&str>,
    command_filter: Option<&str>,
) -> Result<Vec<(ContractLineage, SourceName)>, Response<Body>> {
    if let (Some(lineage_raw), Some(command_raw)) = (lineage_filter, command_filter) {
        let lineage = ContractLineage::new(lineage_raw).map_err(|_| {
            json_error(
                StatusCode::BAD_REQUEST,
                "input_invalid",
                "invalid contract lineage",
            )
        })?;
        let command = SourceName::new(command_raw).map_err(|_| {
            json_error(
                StatusCode::BAD_REQUEST,
                "input_invalid",
                "invalid command name",
            )
        })?;
        return Ok(vec![(lineage, command)]);
    }

    let explicit_lineage = match lineage_filter {
        Some(raw) => Some(ContractLineage::new(raw).map_err(|_| {
            json_error(
                StatusCode::BAD_REQUEST,
                "input_invalid",
                "invalid contract lineage",
            )
        })?),
        None => None,
    };
    let explicit_command = match command_filter {
        Some(raw) => Some(SourceName::new(raw).map_err(|_| {
            json_error(
                StatusCode::BAD_REQUEST,
                "input_invalid",
                "invalid command name",
            )
        })?),
        None => None,
    };

    if let Some(command_name) = explicit_command.clone()
        && explicit_lineage.is_none()
    {
        let Ok((ctx, _cancel)) = build_request_context(deps, principal.clone()) else {
            return Err(json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_defect",
                "internal service failure",
            ));
        };
        if let Ok(GetActiveContractResult::Present(contract)) = deps
            .service
            .get_active_contract(ctx, GetActiveContractRequest)
            .await
        {
            return Ok(vec![(contract.lineage().clone(), command_name)]);
        }
    }

    let mut candidates = Vec::new();
    let mut cursor = None;
    for _ in 0..MAX_OUTCOME_DISCOVERY_PAGES {
        let Ok(limit) = PageLimit::new(500) else {
            break;
        };
        let page_req = PageRequest::new(limit, cursor);
        let Ok(discover_req) = DiscoverCommandToolsRequest::with_options(
            page_req,
            DiscoveryRepresentation::Full,
            None,
        ) else {
            break;
        };
        let Ok((ctx, _cancel)) = build_request_context(deps, principal.clone()) else {
            return Err(json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_defect",
                "internal service failure",
            ));
        };
        let result = deps
            .service
            .discover_command_tools(ctx, discover_req)
            .await
            .map_err(|failure| map_service_failure_response(&failure))?;
        let DiscoverCommandToolsResultRef::Page { page, .. } = result.result() else {
            break;
        };
        for item in page.items() {
            if let CommandToolDiscoveryItem::Command(desc) = item {
                if explicit_lineage
                    .as_ref()
                    .is_some_and(|expected| expected != desc.lineage())
                {
                    continue;
                }
                if explicit_command
                    .as_ref()
                    .is_some_and(|expected| expected != desc.source_command())
                {
                    continue;
                }
                candidates.push((desc.lineage().clone(), desc.source_command().clone()));
            }
        }
        cursor = page.next_cursor();
        if cursor.is_none() {
            break;
        }
    }
    Ok(candidates)
}

fn resolve_database_route(
    routes: &HostedHttpRoutes,
    requested: Option<&str>,
) -> Option<(DatabaseAlias, HostedHttpDependencies)> {
    let singleton = routes
        .configured
        .read()
        .is_ok_and(|configured| configured.len() == 1);
    let active = routes.active.read().ok()?;
    match requested {
        Some(raw) => {
            let alias = DatabaseAlias::new(raw).ok()?;
            let deps = active.get(&alias)?.clone();
            Some((alias, deps))
        }
        None if singleton => active
            .iter()
            .next()
            .map(|(alias, deps)| (alias.clone(), deps.clone())),
        None => None,
    }
}

fn take_authorization(headers: &mut HeaderMap) -> Result<RetainedOpaqueCredential, ()> {
    let retained = {
        let values: Vec<_> = headers.get_all(AUTHORIZATION).iter().collect();
        let [value] = values.as_slice() else {
            return Err(());
        };
        if value.as_bytes().len() != AUTHORIZATION_VALUE_BYTES
            || !value.as_bytes().starts_with(AUTHORIZATION_PREFIX)
        {
            return Err(());
        }
        RetainedOpaqueCredential::new(&value.as_bytes()[AUTHORIZATION_PREFIX.len()..])
            .map_err(|_| ())?
    };
    headers.remove(AUTHORIZATION);
    Ok(retained)
}

fn extract_idempotency_header(headers: &HeaderMap) -> Result<Option<IdempotencyKey>, ()> {
    let values: Vec<_> = headers.get_all(IDEMPOTENCY_KEY_HEADER).iter().collect();
    match values.as_slice() {
        [] => Ok(None),
        [value] => {
            let text = value.to_str().map_err(|_| ())?;
            let key = IdempotencyKey::new(text).map_err(|_| ())?;
            Ok(Some(key))
        }
        _ => Err(()),
    }
}

fn extract_expected_contract_version(
    root: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<Option<ContractVersion>, ()> {
    let value = root
        .remove("expected_contract_version")
        .or_else(|| root.remove("expected_active_version"));
    match value {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Number(num)) => {
            let raw = num.as_u64().ok_or(())?;
            ContractVersion::new(raw).map(Some).ok_or(())
        }
        Some(serde_json::Value::String(text)) => {
            let raw = text.parse::<u64>().map_err(|_| ())?;
            ContractVersion::new(raw).map(Some).ok_or(())
        }
        Some(_) => Err(()),
    }
}

enum ParsedQueryFreshness {
    Default,
    Latest,
    AtLeast(u64),
}

fn extract_query_cursor(
    root: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<Option<CursorToken>, ()> {
    match root.remove("cursor") {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(text)) => {
            if let Ok(bytes) = riffdb_api_mcp::decode_application_query_cursor(&text) {
                return Ok(Some(CursorToken::from_bytes(bytes)));
            }
            if text.len() == 32
                && let Some(bytes) = decode_hex_16(&text)
            {
                return Ok(Some(CursorToken::from_bytes(bytes)));
            }
            Err(())
        }
        Some(_) => Err(()),
    }
}

fn extract_query_freshness(
    root: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<ParsedQueryFreshness, ()> {
    if let Some(admission_head) = root.remove("admission_head") {
        let flag = admission_head.as_bool().ok_or(())?;
        if flag {
            return Ok(ParsedQueryFreshness::Latest);
        }
    }
    if let Some(min_head) = root.remove("minimum_application_head") {
        let seq = parse_u64_json(&min_head).ok_or(())?;
        return Ok(ParsedQueryFreshness::AtLeast(seq));
    }
    let Some(freshness) = root.remove("freshness") else {
        return Ok(ParsedQueryFreshness::Default);
    };
    match freshness {
        serde_json::Value::Null => Ok(ParsedQueryFreshness::Default),
        serde_json::Value::String(mode) => match mode.as_str() {
            "latest" | "admission_head" => Ok(ParsedQueryFreshness::Latest),
            "bounded_staleness" | "available" => Ok(ParsedQueryFreshness::Default),
            _ => Err(()),
        },
        serde_json::Value::Object(mut obj) => {
            let mode = obj
                .remove("mode")
                .or_else(|| obj.remove("kind"))
                .and_then(|v| v.as_str().map(str::to_owned))
                .ok_or(())?;
            match mode.as_str() {
                "latest" | "admission_head" => Ok(ParsedQueryFreshness::Latest),
                "bounded_staleness" | "available" => Ok(ParsedQueryFreshness::Default),
                "at_least" | "causal" => {
                    let seq_val = obj
                        .remove("commit_sequence")
                        .or_else(|| obj.remove("minimum_application_head"))
                        .or_else(|| obj.remove("sequence"))
                        .ok_or(())?;
                    let seq = parse_u64_json(&seq_val).ok_or(())?;
                    Ok(ParsedQueryFreshness::AtLeast(seq))
                }
                _ => Err(()),
            }
        }
        _ => Err(()),
    }
}

fn parse_u64_json(value: &serde_json::Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|s| s.parse::<u64>().ok()))
}

fn parse_optional_module_hash(segment: &str) -> Option<riffdb_types::QueryModuleHash> {
    if segment.len() == 64 {
        let mut bytes = [0_u8; 32];
        for (idx, chunk) in segment.as_bytes().chunks_exact(2).enumerate() {
            let text = std::str::from_utf8(chunk).ok()?;
            bytes[idx] = u8::from_str_radix(text, 16).ok()?;
        }
        Some(riffdb_types::QueryModuleHash::from_bytes(bytes))
    } else {
        None
    }
}

fn decode_hex_16(text: &str) -> Option<[u8; 16]> {
    if text.len() != 32 {
        return None;
    }
    let mut bytes = [0_u8; 16];
    for (idx, chunk) in text.as_bytes().chunks_exact(2).enumerate() {
        let hex_pair = std::str::from_utf8(chunk).ok()?;
        bytes[idx] = u8::from_str_radix(hex_pair, 16).ok()?;
    }
    Some(bytes)
}

fn build_request_context(
    deps: &HostedHttpDependencies,
    principal: riffdb_auth::AuthenticatedPrincipal,
) -> Result<(RequestContext, riffdb_service::RequestCancellationHandle), ()> {
    let request_id = deps.request_ids.next_request_id().map_err(|_| ())?;
    let deadline = Instant::now()
        .checked_add(HOSTED_HTTP_CALL_TIMEOUT)
        .ok_or(())?;
    let (control, cancellation) = RequestControl::new(deadline);
    let context = RequestContext::from_authenticated_mcp_http(request_id, principal, control, None);
    Ok((context, cancellation))
}

fn json_map_to_submitted_fields(
    map: serde_json::Map<String, serde_json::Value>,
) -> Result<Vec<SubmittedField>, ()> {
    let mut fields = Vec::with_capacity(map.len());
    for (name, value) in map {
        let source_name = SourceName::new(name).map_err(|_| ())?;
        let submitted_value = json_to_submitted_value(value)?;
        fields.push(SubmittedField::new(
            SubmittedFieldIdentity::Name(source_name),
            submitted_value,
        ));
    }
    Ok(fields)
}

fn json_map_to_query_parameters(
    map: serde_json::Map<String, serde_json::Value>,
) -> Result<BTreeMap<String, SubmittedValue>, ()> {
    let mut values = BTreeMap::new();
    for (name, value) in map {
        values.insert(name, json_to_submitted_value(value)?);
    }
    Ok(values)
}

fn json_to_submitted_value(value: serde_json::Value) -> Result<SubmittedValue, ()> {
    match value {
        serde_json::Value::Null => Ok(SubmittedValue::Null),
        serde_json::Value::Bool(v) => Ok(SubmittedValue::Bool(v)),
        serde_json::Value::Number(num) => {
            if let Some(u) = num.as_u64() {
                Ok(SubmittedValue::U64(u))
            } else if let Some(i) = num.as_i64() {
                Ok(SubmittedValue::I64(i))
            } else {
                Err(())
            }
        }
        serde_json::Value::String(s) => SubmittedValue::string(s).map_err(|_| ()),
        serde_json::Value::Array(items) => {
            let mut list = Vec::with_capacity(items.len());
            for item in items {
                list.push(json_to_submitted_value(item)?);
            }
            SubmittedList::new(list)
                .map(SubmittedValue::List)
                .map_err(|_| ())
        }
        serde_json::Value::Object(obj) => {
            if let Some(tagged) = try_parse_tagged_submitted_object(&obj)? {
                return Ok(tagged);
            }
            let fields = json_map_to_submitted_fields(obj)?;
            SubmittedRecord::new(fields)
                .map(SubmittedValue::Record)
                .map_err(|_| ())
        }
    }
}

fn try_parse_tagged_submitted_object(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Result<Option<SubmittedValue>, ()> {
    if obj.len() == 1
        && let Some(b64) = obj.get("$bytes").and_then(|v| v.as_str())
    {
        let decoded = decode_base64_standard(b64)?;
        return SubmittedValue::bytes(decoded).map(Some).map_err(|_| ());
    }
    if obj.len() == 1
        && let Some(days) = obj.get("days_since_unix_epoch").and_then(|v| v.as_i64())
    {
        let days = i32::try_from(days).map_err(|_| ())?;
        return Ok(Some(SubmittedValue::Date(Date::new(days))));
    }
    if obj.len() == 2
        && let (Some(seconds_val), Some(nanos_val)) = (obj.get("seconds"), obj.get("nanos"))
    {
        let seconds = seconds_val
            .as_i64()
            .or_else(|| seconds_val.as_str().and_then(|s| s.parse::<i64>().ok()))
            .ok_or(())?;
        let nanos = nanos_val
            .as_u64()
            .and_then(|n| u32::try_from(n).ok())
            .ok_or(())?;
        let ts = Timestamp::new(seconds, nanos).map_err(|_| ())?;
        return Ok(Some(SubmittedValue::Timestamp(ts)));
    }
    if obj.len() == 3
        && let (Some(coeff_val), Some(prec_val), Some(scale_val)) = (
            obj.get("coefficient"),
            obj.get("precision"),
            obj.get("scale"),
        )
    {
        let coeff = coeff_val
            .as_i64()
            .map(i128::from)
            .or_else(|| coeff_val.as_str().and_then(|s| s.parse::<i128>().ok()))
            .ok_or(())?;
        let prec = prec_val
            .as_u64()
            .and_then(|n| u8::try_from(n).ok())
            .ok_or(())?;
        let scale = scale_val
            .as_u64()
            .and_then(|n| u8::try_from(n).ok())
            .ok_or(())?;
        let dec = SubmittedDecimal::with_precision(coeff, scale, Some(prec)).map_err(|_| ())?;
        return Ok(Some(SubmittedValue::Decimal(dec)));
    }
    if obj.len() == 4
        && let (Some(curr_val), Some(coeff_val), Some(prec_val), Some(scale_val)) = (
            obj.get("currency"),
            obj.get("coefficient"),
            obj.get("precision"),
            obj.get("scale"),
        )
    {
        let currency_str = curr_val.as_str().ok_or(())?;
        let currency = CurrencyCode::new(currency_str.as_bytes()).map_err(|_| ())?;
        let coeff = coeff_val
            .as_i64()
            .map(i128::from)
            .or_else(|| coeff_val.as_str().and_then(|s| s.parse::<i128>().ok()))
            .ok_or(())?;
        let prec = prec_val
            .as_u64()
            .and_then(|n| u8::try_from(n).ok())
            .ok_or(())?;
        let scale = scale_val
            .as_u64()
            .and_then(|n| u8::try_from(n).ok())
            .ok_or(())?;
        let dec = SubmittedDecimal::with_precision(coeff, scale, Some(prec)).map_err(|_| ())?;
        return Ok(Some(SubmittedValue::Money(SubmittedMoney::new(
            currency, dec,
        ))));
    }
    Ok(None)
}

fn render_command_result(result: &ExecuteCommandResult) -> Result<serde_json::Value, ()> {
    match result {
        ExecuteCommandResult::Journaled(journaled) => {
            let (status, replayed) = match journaled.completion() {
                riffdb_service::JournaledCompletion::Committed => ("committed", false),
                riffdb_service::JournaledCompletion::Replayed => ("replayed", true),
            };
            let durability = match journaled.durability() {
                riffdb_service::CommandDurability::Synchronous => "synchronous",
                riffdb_service::CommandDurability::Group => "group",
            };
            let output = render_outcome_record(journaled.outcome().schema_bound_value())?;
            Ok(serde_json::json!({
                "status": status,
                "replayed": replayed,
                "commit_sequence": journaled.commit_sequence().get(),
                "contract_version": journaled.contract_version().get(),
                "plan_hash": lower_hex(journaled.plan_hash().as_bytes()),
                "durability": durability,
                "outcome": {
                    "type": journaled.outcome().outcome_name().as_str(),
                    "value": output.clone(),
                },
                "output": output,
                "provenance_uri": format_provenance_locator(journaled.provenance_id()),
                "outcome_uri": journaled.outcome_locator().canonical_uri(),
            }))
        }
        ExecuteCommandResult::ReadOnlyExecuted(readonly) => {
            let output = render_outcome_record(readonly.outcome().schema_bound_value())?;
            Ok(serde_json::json!({
                "status": "executed_read_only",
                "replayed": false,
                "contract_version": readonly.contract_version().get(),
                "plan_hash": lower_hex(readonly.plan_hash().as_bytes()),
                "outcome": {
                    "type": readonly.outcome().outcome_name().as_str(),
                    "value": output.clone(),
                },
                "output": output,
            }))
        }
    }
}

fn render_recovered_outcome(
    journaled: &riffdb_service::JournaledCommandResult,
) -> Result<serde_json::Value, ()> {
    let durability = match journaled.durability() {
        riffdb_service::CommandDurability::Synchronous => "synchronous",
        riffdb_service::CommandDurability::Group => "group",
    };
    let output = render_outcome_record(journaled.outcome().schema_bound_value())?;
    Ok(serde_json::json!({
        "status": "found",
        "replayed": true,
        "commit_sequence": journaled.commit_sequence().get(),
        "contract_version": journaled.contract_version().get(),
        "plan_hash": lower_hex(journaled.plan_hash().as_bytes()),
        "durability": durability,
        "outcome": {
            "type": journaled.outcome().outcome_name().as_str(),
            "value": output.clone(),
        },
        "output": output,
        "provenance_uri": format_provenance_locator(journaled.provenance_id()),
        "outcome_uri": journaled.outcome_locator().canonical_uri(),
    }))
}

fn render_outcome_record(record: SchemaBoundOutcomeRecord<'_>) -> Result<serde_json::Value, ()> {
    let mut fields = serde_json::Map::new();
    for index in 0..record.len() {
        let field = record.field(index).ok_or(())?;
        let value = field.value().ok_or(())?;
        if fields
            .insert(
                field.field_name().as_str().to_owned(),
                render_outcome_value(value)?,
            )
            .is_some()
        {
            return Err(());
        }
    }
    Ok(serde_json::Value::Object(fields))
}

fn render_outcome_value(value: SchemaBoundOutcomeValue<'_>) -> Result<serde_json::Value, ()> {
    match value {
        SchemaBoundOutcomeValue::Null => Ok(serde_json::Value::Null),
        SchemaBoundOutcomeValue::Scalar(scalar) => render_canonical_scalar(scalar),
        SchemaBoundOutcomeValue::Enum { variant_name, .. } => {
            Ok(serde_json::Value::String(variant_name.as_str().to_owned()))
        }
        SchemaBoundOutcomeValue::List(values) => {
            let mut items = Vec::with_capacity(values.len());
            for index in 0..values.len() {
                let item = values.value(index).ok_or(())?;
                items.push(render_outcome_value(item)?);
            }
            Ok(serde_json::Value::Array(items))
        }
        SchemaBoundOutcomeValue::Record(record) => render_outcome_record(record),
    }
}

fn render_query_result(result: &ExecuteSymbolicQueryResult) -> Result<serde_json::Value, ()> {
    let mut fields_map = serde_json::Map::new();
    fields_map.insert(
        "outcome".to_owned(),
        serde_json::Value::String(result.outcome().to_owned()),
    );
    for (name, field) in result.fields() {
        let rendered = match field {
            SymbolicResultField::One(record) => render_symbolic_record(result, record)?,
            SymbolicResultField::Maybe(record) => match record {
                Some(record) => render_symbolic_record(result, record)?,
                None => serde_json::Value::Null,
            },
            SymbolicResultField::Many(records) => {
                let mut rows = Vec::with_capacity(records.len());
                for record in records {
                    rows.push(render_symbolic_record(result, record)?);
                }
                serde_json::Value::Array(rows)
            }
        };
        fields_map.insert(name.clone(), rendered);
    }
    let next_cursor = result
        .next_cursor()
        .map(|cursor| riffdb_api_mcp::encode_application_query_cursor(*cursor.as_bytes()));
    let result_obj = serde_json::Value::Object(fields_map.clone());
    fields_map.insert("result".to_owned(), result_obj);
    fields_map.insert(
        "application_head".to_owned(),
        serde_json::Value::Number(serde_json::Number::from(result.application_head())),
    );
    fields_map.insert(
        "next_cursor".to_owned(),
        next_cursor.map_or(serde_json::Value::Null, serde_json::Value::String),
    );
    Ok(serde_json::Value::Object(fields_map))
}

fn render_symbolic_record(
    result: &ExecuteSymbolicQueryResult,
    record: &SymbolicResultRecord,
) -> Result<serde_json::Value, ()> {
    let mut fields = serde_json::Map::new();
    for (name, value) in record.fields() {
        fields.insert(name.to_string(), render_symbolic_value(result, value)?);
    }
    for (name, decimal) in record.exact_decimals() {
        fields.insert(
            name.to_string(),
            serde_json::Value::String(format!("{}e-{}", decimal.coefficient(), decimal.scale())),
        );
    }
    for (name, nested_rows) in record.nested() {
        let mut rows = Vec::with_capacity(nested_rows.len());
        for row in nested_rows {
            rows.push(render_symbolic_record(result, row)?);
        }
        fields.insert(name.to_string(), serde_json::Value::Array(rows));
    }
    Ok(serde_json::Value::Object(fields))
}

fn render_symbolic_value(
    result: &ExecuteSymbolicQueryResult,
    value: &CanonicalValue,
) -> Result<serde_json::Value, ()> {
    match value {
        CanonicalValue::Enum {
            type_id,
            variant_id,
        } => result
            .enum_variant_name(type_id.get(), variant_id.get())
            .map(|name| serde_json::Value::String(name.to_owned()))
            .ok_or(()),
        CanonicalValue::List(values) => {
            let mut items = Vec::with_capacity(values.len());
            for item in values.values() {
                items.push(render_symbolic_value(result, item)?);
            }
            Ok(serde_json::Value::Array(items))
        }
        CanonicalValue::Record(_) => Err(()),
        other => render_canonical_scalar(other),
    }
}

fn render_canonical_scalar(value: &CanonicalValue) -> Result<serde_json::Value, ()> {
    match value {
        CanonicalValue::Null => Ok(serde_json::Value::Null),
        CanonicalValue::Bool(v) => Ok(serde_json::Value::Bool(*v)),
        CanonicalValue::I64(v) => Ok(serde_json::Value::Number(serde_json::Number::from(*v))),
        CanonicalValue::U64(v) => Ok(serde_json::Value::Number(serde_json::Number::from(*v))),
        CanonicalValue::Decimal(v) => Ok(serde_json::json!({
            "coefficient": v.coefficient().to_string(),
            "precision": v.spec().precision(),
            "scale": v.spec().scale(),
        })),
        CanonicalValue::Money(v) => Ok(serde_json::json!({
            "currency": v.currency().to_string(),
            "coefficient": v.amount().coefficient().to_string(),
            "precision": v.amount().spec().precision(),
            "scale": v.amount().spec().scale(),
        })),
        CanonicalValue::String(v) => Ok(serde_json::Value::String(v.as_str().to_owned())),
        CanonicalValue::Bytes(v) => Ok(serde_json::Value::String(encode_base64_standard(
            v.as_bytes(),
        ))),
        CanonicalValue::Timestamp(v) => Ok(serde_json::json!({
            "seconds": v.seconds(),
            "nanos": v.nanos(),
        })),
        CanonicalValue::Date(v) => Ok(serde_json::json!({
            "days_since_unix_epoch": v.days_since_unix_epoch(),
        })),
        CanonicalValue::Uuid(v) => Ok(serde_json::Value::String(format_uuid(*v))),
        CanonicalValue::Vector(vec) => {
            let mut items = Vec::with_capacity(vec.components().len());
            for component in vec.components() {
                let num = serde_json::Number::from_f64(f64::from(*component)).ok_or(())?;
                items.push(serde_json::Value::Number(num));
            }
            Ok(serde_json::Value::Array(items))
        }
        CanonicalValue::Enum { .. } | CanonicalValue::List(_) | CanonicalValue::Record(_) => {
            Err(())
        }
    }
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn format_uuid(bytes: [u8; 16]) -> String {
    let hex = lower_hex(&bytes);
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

fn encode_base64_standard(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = if chunk.len() > 1 { chunk[1] } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] } else { 0 };
        out.push(char::from(TABLE[usize::from(b0 >> 2)]));
        out.push(char::from(
            TABLE[usize::from(((b0 & 0x03) << 4) | (b1 >> 4))],
        ));
        if chunk.len() > 1 {
            out.push(char::from(
                TABLE[usize::from(((b1 & 0x0f) << 2) | (b2 >> 6))],
            ));
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(char::from(TABLE[usize::from(b2 & 0x3f)]));
        } else {
            out.push('=');
        }
    }
    out
}

fn decode_base64_standard(input: &str) -> Result<Vec<u8>, ()> {
    let bytes = input.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return Err(());
    }
    let mut out = Vec::with_capacity((bytes.len() / 4) * 3);
    for chunk in bytes.chunks_exact(4) {
        let c0 = decode_base64_val(chunk[0]).ok_or(())?;
        let c1 = decode_base64_val(chunk[1]).ok_or(())?;
        out.push((c0 << 2) | (c1 >> 4));
        if chunk[2] != b'=' {
            let c2 = decode_base64_val(chunk[2]).ok_or(())?;
            out.push(((c1 & 0x0f) << 4) | (c2 >> 2));
            if chunk[3] != b'=' {
                let c3 = decode_base64_val(chunk[3]).ok_or(())?;
                out.push(((c2 & 0x03) << 6) | c3);
            }
        } else if chunk[3] != b'=' {
            return Err(());
        }
    }
    Ok(out)
}

const fn decode_base64_val(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

fn map_service_failure_response(failure: &ServiceFailure) -> Response<Body> {
    match failure {
        ServiceFailure::Public(error) => map_public_error_response(error),
        ServiceFailure::Cancelled => json_error(
            StatusCode::REQUEST_TIMEOUT,
            "operation_cancelled",
            "request was cancelled",
        ),
        ServiceFailure::DeadlineExceeded => json_error(
            StatusCode::GATEWAY_TIMEOUT,
            "deadline_exceeded",
            "request deadline elapsed",
        ),
        ServiceFailure::ResponseTooLarge => json_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "response_too_large",
            "response exceeds the service limit",
        ),
        ServiceFailure::EmergencyInternal(error) => json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_defect",
            error.safe_message(),
        ),
    }
}

fn map_public_error_response(error: &PublicError) -> Response<Body> {
    let (status, code) = match error.kind() {
        PublicErrorKind::Validation => (StatusCode::BAD_REQUEST, "input_invalid"),
        PublicErrorKind::IdempotencyKeyReuse => (StatusCode::CONFLICT, "idempotency_key_reuse"),
        PublicErrorKind::AuthorizationDenied => (StatusCode::FORBIDDEN, "authorization_denied"),
        PublicErrorKind::ConcurrencyDeadlineExceeded => {
            (StatusCode::GATEWAY_TIMEOUT, "deadline_exceeded")
        }
        PublicErrorKind::ContractMismatch => (StatusCode::CONFLICT, "contract_mismatch"),
        PublicErrorKind::StorageUnavailable => {
            (StatusCode::SERVICE_UNAVAILABLE, "storage_unavailable")
        }
        PublicErrorKind::OutcomeUnknown => (StatusCode::SERVICE_UNAVAILABLE, "outcome_unknown"),
        PublicErrorKind::InternalDefect => (StatusCode::INTERNAL_SERVER_ERROR, "internal_defect"),
        PublicErrorKind::CommandExecutionFailed => {
            (StatusCode::UNPROCESSABLE_ENTITY, "command_execution_failed")
        }
        PublicErrorKind::HistoryIncarnationMismatch => {
            (StatusCode::CONFLICT, "history_incarnation_mismatch")
        }
        PublicErrorKind::HistoryPruned => (StatusCode::GONE, "history_pruned"),
        PublicErrorKind::Overloaded => (StatusCode::SERVICE_UNAVAILABLE, "overloaded"),
        PublicErrorKind::FollowerMode => (StatusCode::MISDIRECTED_REQUEST, "follower_mode"),
        PublicErrorKind::PrimaryFenced => (StatusCode::SERVICE_UNAVAILABLE, "primary_fenced"),
    };
    json_error(status, code, error.safe_message())
}

fn unauthorized_response() -> Response<Body> {
    let body = serde_json::json!({
        "error": {
            "code": "unauthenticated",
            "message": "authentication required",
        }
    });
    let encoded = serde_json::to_vec(&body).expect("static error JSON serializes");
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header(CONTENT_TYPE, HeaderValue::from_static(JSON_CONTENT_TYPE))
        .header(CACHE_CONTROL, HeaderValue::from_static("no-store"))
        .header(WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"))
        .body(Body::from(encoded))
        .expect("static unauthorized response is valid")
}

fn json_error(status: StatusCode, code: &'static str, message: &str) -> Response<Body> {
    let body = serde_json::json!({
        "error": {
            "code": code,
            "message": message,
        }
    });
    json_response(status, &body)
}

fn json_response(status: StatusCode, body: &serde_json::Value) -> Response<Body> {
    let encoded = serde_json::to_vec(body).expect("checked JSON response serializes");
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, HeaderValue::from_static(JSON_CONTENT_TYPE))
        .header(CACHE_CONTROL, HeaderValue::from_static("no-store"))
        .body(Body::from(encoded))
        .expect("static JSON response headers are valid")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;

    use riffdb_api_grpc::{GrpcApplication, GrpcLifecycleRoute, GrpcRequestLimits};
    use riffdb_auth::bootstrap_secret::{
        SystemEntropy, generate_bootstrap_credential, load_bootstrap_credential_file,
    };
    use riffdb_client_rust::{
        BearerCredential, BootstrapCallMetadata,
        BootstrapCredential as TransportBootstrapCredential, CallMetadata, RiffDbClient,
        generate_capability_id, generate_request_id,
    };
    use riffdb_proto::{app::v1 as app_v1, v1};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tonic::transport::Endpoint;

    use crate::config::ServerConfig;
    use crate::inventory_reload::build_provisioned_database_graph;
    use crate::maintenance_adapter::MigrationProcessExclusion;
    use crate::real_storage_support::temporary_database_scope;

    const HTTP_INGRESS_CONTRACT: &str = r#"contract HttpIngressItems version 1 {
  entity Item {
    key (org_id: string<64>, item_id: string<64>)
    field title: string<128>
    field quantity: u64
  }

  aggregate Items {
    root Item
    partition_by org_id
    conflict_key (org_id, item_id)
  }

  command CreateItem {
    input idempotency_key: string<128>
    input org_id: string<64>
    input item_id: string<64>
    input title: string<128>
    input quantity: u64

    idempotency_key idempotency_key
    create Item(org_id, item_id) as item
      else AlreadyExists { item_id: item_id }
    set item.title = title
    set item.quantity = quantity
    return Created { item: item }
  }
}
"#;

    const HTTP_INGRESS_QUERY: &str = r#"query GetItem(
  $org_id: Item.org_id,
  $item_id: Item.item_id,
) {
  maybe item from Item
    where org_id == $org_id && item_id == $item_id

  return Found {
    item: item {
      org_id
      item_id
      title
      quantity
    }
  }

  outcomes Found
}
"#;

    fn write_protected_file(path: &Path, content: &[u8]) {
        fs::write(path, content).expect("write protected file");
        #[cfg(unix)]
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("set 0600 permissions");
    }

    fn ensure_protected_dir(path: &Path) {
        fs::create_dir_all(path).expect("create protected dir");
        #[cfg(unix)]
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).expect("set 0700 permissions");
    }

    async fn send_http_json(
        addr: SocketAddr,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> (u16, String, serde_json::Value) {
        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("connect to HTTP ingress");
        let mut request = format!(
            "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Length: {}\r\n",
            body.len()
        );
        if !body.is_empty() {
            request.push_str("Content-Type: application/json\r\n");
        }
        for (name, value) in headers {
            request.push_str(&format!("{name}: {value}\r\n"));
        }
        request.push_str("\r\n");
        stream
            .write_all(request.as_bytes())
            .await
            .expect("write request headers");
        if !body.is_empty() {
            stream.write_all(body).await.expect("write request body");
        }
        let mut raw = Vec::new();
        stream
            .read_to_end(&mut raw)
            .await
            .expect("read HTTP response");
        let text = String::from_utf8(raw).expect("UTF-8 HTTP response");
        let (head, body_str) = text
            .split_once("\r\n\r\n")
            .expect("HTTP response header/body split");
        let status_line = head.lines().next().expect("HTTP status line");
        let status_code: u16 = status_line
            .split_whitespace()
            .nth(1)
            .expect("status code token")
            .parse()
            .expect("numeric status code");
        let json: serde_json::Value = serde_json::from_str(body_str)
            .unwrap_or_else(|e| panic!("invalid JSON body ({e}): {body_str}"));
        (status_code, head.to_owned(), json)
    }

    // req: API-001, NET-001, NET-006, NET-008, SEC-001, OUT-001
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn direct_http_json_ingress_executes_commands_queries_and_outcome_recovery_with_auth_parity()
     {
        let (scope, db_path) = temporary_database_scope("http-ingress-parity");
        let cap_key = scope.path().join("cap.key");
        let idem_key = scope.path().join("idem.key");
        let bootstrap_path = scope.path().join("bootstrap.credential");
        write_protected_file(
            &cap_key,
            b"riffdb-capability-digest-keys-v1\n1:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n",
        );
        write_protected_file(
            &idem_key,
            b"riffdb-idempotency-digest-keys-v1\n1:1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100\n",
        );
        let generated_bootstrap = generate_bootstrap_credential(1_700_000_000_000, &SystemEntropy)
            .expect("generate bootstrap credential");
        write_protected_file(
            &bootstrap_path,
            generated_bootstrap.render_document().expose_secret(),
        );
        drop(generated_bootstrap);
        let retained_bootstrap =
            load_bootstrap_credential_file(&bootstrap_path).expect("load bootstrap credential");

        let http_probe =
            std::net::TcpListener::bind("127.0.0.1:0").expect("allocate ephemeral HTTP port");
        let http_addr = http_probe.local_addr().expect("http local_addr");
        drop(http_probe);

        let backup_root = scope.path().join("backups");
        let projections_root = scope.path().join("projections");
        ensure_protected_dir(&backup_root);
        ensure_protected_dir(&projections_root);

        let audience = "riffdb-grpc-loopback";
        let config_toml = format!(
            r#"[server]
grpc_listen = "127.0.0.1:7443"
http_listen = "{http_addr}"
audience = "{audience}"
capability_keys = "{}"
idempotency_keys = "{}"

[databases.default]
path = "{}"
backup_root = "{}"
projections_root = "{}"
environment = "local"

[databases.default.limits]
extent_profile = "compact"
"#,
            cap_key.display(),
            idem_key.display(),
            db_path.display(),
            backup_root.display(),
            projections_root.display(),
        );
        let config_path = scope.path().join("riffdbd.toml");
        fs::write(&config_path, config_toml).expect("write riffdbd.toml");

        let config = ServerConfig::parse_config_file(&config_path, scope.path())
            .expect("parse ServerConfig");
        let started_at = Timestamp::new(1_700_000_000, 0).expect("started_at");
        let migration_exclusion = Arc::new(MigrationProcessExclusion::new());

        let provisioned = build_provisioned_database_graph(
            &config,
            &config.databases()[0],
            started_at,
            &migration_exclusion,
        )
        .expect("build provisioned database graph");

        let running_graph = provisioned
            .graph
            .as_ref()
            .expect("running production graph");
        let mut hosted_http = HostedHttp::bind_listener(
            config.http_listener().expect("configured http_listener"),
            running_graph.hosted_http_dependencies(),
        )
        .await
        .expect("bind HostedHttp listener");

        let route: Arc<dyn GrpcLifecycleRoute> = provisioned.lifecycle.clone();
        let limits = GrpcRequestLimits::new(Duration::from_secs(30)).expect("grpc limits");
        let grpc_app = GrpcApplication::new_with_audience(route, limits, config.audience().clone());
        let grpc_probe =
            std::net::TcpListener::bind("127.0.0.1:0").expect("allocate ephemeral gRPC port");
        let grpc_addr = grpc_probe.local_addr().expect("grpc local_addr");
        drop(grpc_probe);
        let grpc_listener = tonic::transport::server::TcpIncoming::bind(grpc_addr)
            .expect("bind gRPC listener")
            .with_nodelay(Some(true));
        let (grpc_stop_tx, grpc_stop_rx) = oneshot::channel::<()>();
        let grpc_router = tonic::transport::Server::builder()
            .add_service(grpc_app.contract_server())
            .add_service(grpc_app.command_server())
            .add_service(grpc_app.application_session_server())
            .add_service(grpc_app.query_server())
            .add_service(grpc_app.application_query_server())
            .add_service(grpc_app.commit_server())
            .add_service(grpc_app.event_server())
            .add_service(grpc_app.admin_server());
        let grpc_task = tokio::spawn(async move {
            let _ = grpc_router
                .serve_with_incoming_shutdown(grpc_listener, async {
                    let _ = grpc_stop_rx.await;
                })
                .await;
        });

        let endpoint = Endpoint::from_shared(format!("http://{grpc_addr}")).expect("gRPC endpoint");
        let mut admin_client = RiffDbClient::connect(endpoint)
            .await
            .expect("connect gRPC admin client");

        let bootstrap_token_str = std::str::from_utf8(retained_bootstrap.token().expose_secret())
            .expect("UTF-8 bootstrap token");
        let bootstrap_meta = BootstrapCallMetadata::new(
            TransportBootstrapCredential::new(bootstrap_token_str).expect("transport bootstrap"),
        );
        let bootstrap_req = v1::CreateCapabilityRequest {
            request_id: generate_request_id().expect("req id").into_bytes().to_vec(),
            mode: v1::CapabilityCreateMode::Bootstrap as i32,
            capability_id: retained_bootstrap.capability_id().into_bytes().to_vec(),
            principal_id: "http-ingress-admin".to_owned(),
            actor_kind: v1::ActorKind::Human as i32,
            requested_lifetime_seconds: 3600,
            audiences: vec![audience.to_owned()],
            grant: Some(v1::CapabilityGrant {
                tenant_scope: Some(v1::TenantScope {
                    scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
                }),
                partition_scope: Some(v1::PartitionScope {
                    scope: Some(v1::partition_scope::Scope::All(v1::Unit {})),
                }),
                permissions: vec![
                    v1::CapabilityPermission {
                        permission: Some(v1::capability_permission::Permission::ReadContract(
                            v1::Unit {},
                        )),
                    },
                    v1::CapabilityPermission {
                        permission: Some(v1::capability_permission::Permission::DeployContract(
                            v1::Unit {},
                        )),
                    },
                    v1::CapabilityPermission {
                        permission: Some(v1::capability_permission::Permission::ReadHealth(
                            v1::Unit {},
                        )),
                    },
                    v1::CapabilityPermission {
                        permission: Some(
                            v1::capability_permission::Permission::AdministerCapabilities(
                                v1::Unit {},
                            ),
                        ),
                    },
                ],
                field_visibility: Vec::new(),
                max_scan_rows: 100,
                approval_required: Vec::new(),
                row_policy: None,
                export: None,
                reimport: None,
                vector_inspection: None,
            }),
        };
        admin_client
            .create_bootstrap_capability(bootstrap_req, &bootstrap_meta)
            .await
            .expect("create bootstrap capability");

        let admin_auth = CallMetadata::authenticated(
            BearerCredential::new(bootstrap_token_str).expect("admin bearer"),
        );
        let deployed = admin_client
            .deploy_contract(
                v1::DeployContractRequest {
                    request_id: generate_request_id().expect("req id").into_bytes().to_vec(),
                    source: HTTP_INGRESS_CONTRACT.to_owned(),
                    expected_active_version: None,
                    expected_active_bundle_hash: Vec::new(),
                    expected_candidate_bundle_hash: Vec::new(),
                },
                &admin_auth,
            )
            .await
            .expect("deploy contract");
        let Some(v1::deploy_contract_response::Result::Activated(activated_contract)) =
            deployed.result
        else {
            panic!("contract was not activated");
        };

        let deployed_module = admin_client
            .deploy_query_module(
                app_v1::DeployQueryModuleRequest {
                    contract: Some(app_v1::ContractSelector {
                        lineage: activated_contract.contract_lineage.clone(),
                        version: activated_contract.contract_version,
                        bundle_hash: activated_contract.bundle_hash.clone(),
                    }),
                    module_name: "items_queries".to_owned(),
                    module_version: 1,
                    queries: vec![app_v1::NamedQuerySource {
                        name: "GetItem".to_owned(),
                        source: HTTP_INGRESS_QUERY.to_owned(),
                    }],
                    request_id: generate_request_id().expect("req id").into_bytes().to_vec(),
                    expected_active: Some(
                        app_v1::deploy_query_module_request::ExpectedActive::AbsentActive(true),
                    ),
                },
                &admin_auth,
            )
            .await
            .expect("deploy query module");
        let module_hash = deployed_module
            .module
            .expect("deployed query module descriptor")
            .module_hash;

        let created_cap = admin_client
            .create_capability(
                v1::CreateCapabilityRequest {
                    request_id: generate_request_id().expect("req id").into_bytes().to_vec(),
                    mode: v1::CapabilityCreateMode::Normal as i32,
                    capability_id: generate_capability_id()
                        .expect("cap id")
                        .into_bytes()
                        .to_vec(),
                    principal_id: "http-ingress-app".to_owned(),
                    actor_kind: v1::ActorKind::Service as i32,
                    requested_lifetime_seconds: 1800,
                    audiences: vec![audience.to_owned()],
                    grant: Some(v1::CapabilityGrant {
                        tenant_scope: Some(v1::TenantScope {
                            scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
                        }),
                        partition_scope: Some(v1::PartitionScope {
                            scope: Some(v1::partition_scope::Scope::All(v1::Unit {})),
                        }),
                        permissions: vec![
                            v1::CapabilityPermission {
                                permission: Some(
                                    v1::capability_permission::Permission::ReadContract(
                                        v1::Unit {},
                                    ),
                                ),
                            },
                            v1::CapabilityPermission {
                                permission: Some(
                                    v1::capability_permission::Permission::InvokeCommand(
                                        v1::LineageScopedStableId {
                                            contract_lineage: "HttpIngressItems".to_owned(),
                                            stable_id: 1,
                                        },
                                    ),
                                ),
                            },
                            v1::CapabilityPermission {
                                permission: Some(
                                    v1::capability_permission::Permission::ReadEntity(
                                        v1::LineageScopedStableId {
                                            contract_lineage: "HttpIngressItems".to_owned(),
                                            stable_id: 1,
                                        },
                                    ),
                                ),
                            },
                            v1::CapabilityPermission {
                                permission: Some(
                                    v1::capability_permission::Permission::ReadHealth(v1::Unit {}),
                                ),
                            },
                            v1::CapabilityPermission {
                                permission: Some(
                                    v1::capability_permission::Permission::ExecuteNamedQuery(
                                        v1::NamedQueryPermission {
                                            contract_lineage: "HttpIngressItems".to_owned(),
                                            query_module_hash: module_hash,
                                            query_name: "GetItem".to_owned(),
                                        },
                                    ),
                                ),
                            },
                        ],
                        field_visibility: vec![v1::EntityFieldVisibility {
                            contract_lineage: "HttpIngressItems".to_owned(),
                            entity_type_id: 1,
                            field_ids: vec![1, 2, 3, 4],
                            secret_field_ids: Vec::new(),
                        }],
                        max_scan_rows: 100,
                        approval_required: Vec::new(),
                        row_policy: None,
                        export: None,
                        reimport: None,
                        vector_inspection: None,
                    }),
                },
                &admin_auth,
            )
            .await
            .expect("create app capability");
        let Some(v1::create_capability_response::Result::Normal(normal_res)) = created_cap.result
        else {
            panic!("expected normal capability result");
        };
        let Some(v1::normal_create_capability_result::Result::Created(created_token)) =
            normal_res.result
        else {
            panic!("expected created capability token");
        };
        let bearer_header = format!("Bearer {}", created_token.token);

        // 1. Unauthenticated and authenticated GET /v1/health
        let (status, _, unauth_health) =
            send_http_json(http_addr, "GET", "/v1/health", &[], b"").await;
        assert_eq!(status, 200);
        assert_eq!(unauth_health["status"], "alive");

        let (status, _, auth_health) = send_http_json(
            http_addr,
            "GET",
            "/v1/health",
            &[("Authorization", &bearer_header)],
            b"",
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(auth_health["status"], "ready");
        assert_eq!(auth_health["active_contract_version"], 1);

        // 2. Unauthenticated command execution fails closed with 401 Unauthorized + WWW-Authenticate: Bearer
        let cmd_body =
            br#"{"org_id":"org-alpha","item_id":"item-1","title":"Widget","quantity":42}"#;
        let (status, head, unauth_cmd) = send_http_json(
            http_addr,
            "POST",
            "/v1/databases/default/commands/CreateItem",
            &[("Idempotency-Key", "idem-http-001")],
            cmd_body,
        )
        .await;
        assert_eq!(status, 401);
        assert!(
            head.to_ascii_lowercase()
                .contains("www-authenticate: bearer")
        );
        assert_eq!(unauth_cmd["error"]["code"], "unauthenticated");

        // 3. Authenticated command execution commits and returns typed output
        let (status, _, committed_cmd) = send_http_json(
            http_addr,
            "POST",
            "/v1/databases/default/commands/CreateItem",
            &[
                ("Authorization", &bearer_header),
                ("Idempotency-Key", "idem-http-001"),
            ],
            cmd_body,
        )
        .await;
        assert_eq!(status, 200, "unexpected command error: {committed_cmd}");
        assert_eq!(committed_cmd["status"], "committed");
        assert_eq!(committed_cmd["replayed"], false);
        assert_eq!(committed_cmd["commit_sequence"], 1);
        assert_eq!(committed_cmd["output"]["item"]["org_id"], "org-alpha");
        assert_eq!(committed_cmd["output"]["item"]["item_id"], "item-1");
        assert_eq!(committed_cmd["output"]["item"]["title"], "Widget");
        assert_eq!(committed_cmd["output"]["item"]["quantity"], 42);

        // 4. Replaying the same command with the same Idempotency-Key returns replayed=true
        let (status, _, replayed_cmd) = send_http_json(
            http_addr,
            "POST",
            "/v1/databases/default/commands/CreateItem",
            &[
                ("Authorization", &bearer_header),
                ("Idempotency-Key", "idem-http-001"),
            ],
            cmd_body,
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(replayed_cmd["status"], "replayed");
        assert_eq!(replayed_cmd["replayed"], true);
        assert_eq!(replayed_cmd["commit_sequence"], 1);

        // 5. Replaying with a mutated payload and the same Idempotency-Key returns 409 Conflict
        let mutated_body =
            br#"{"org_id":"org-alpha","item_id":"item-1","title":"Widget","quantity":99}"#;
        let (status, _, conflict_cmd) = send_http_json(
            http_addr,
            "POST",
            "/v1/databases/default/commands/CreateItem",
            &[
                ("Authorization", &bearer_header),
                ("Idempotency-Key", "idem-http-001"),
            ],
            mutated_body,
        )
        .await;
        assert_eq!(status, 409);
        assert_eq!(conflict_cmd["error"]["code"], "idempotency_key_reuse");

        // 6. Named symbolic query execution over HTTP/JSON returns the committed entity
        let query_body = br#"{"org_id":"org-alpha","item_id":"item-1"}"#;
        let (status, _, query_res) = send_http_json(
            http_addr,
            "POST",
            "/v1/databases/default/queries/active/GetItem",
            &[("Authorization", &bearer_header)],
            query_body,
        )
        .await;
        assert_eq!(status, 200, "unexpected query error: {query_res}");
        assert_eq!(query_res["outcome"], "Found");
        assert_eq!(query_res["application_head"], 1);
        assert_eq!(query_res["item"]["org_id"], "org-alpha");
        assert_eq!(query_res["item"]["item_id"], "item-1");
        assert_eq!(query_res["item"]["title"], "Widget");
        assert_eq!(query_res["item"]["quantity"], 42);

        // 7. Outcome uncertainty recovery via GET /v1/databases/{db}/outcomes/{idempotency_key}
        let (status, _, recovered) = send_http_json(
            http_addr,
            "GET",
            "/v1/databases/default/outcomes/idem-http-001?command=CreateItem",
            &[("Authorization", &bearer_header)],
            b"",
        )
        .await;
        assert_eq!(
            status, 200,
            "unexpected outcome recovery error: {recovered}"
        );
        assert_eq!(recovered["status"], "found");
        assert_eq!(recovered["replayed"], true);
        assert_eq!(recovered["commit_sequence"], 1);
        assert_eq!(recovered["output"]["item"]["title"], "Widget");

        drop(admin_client);
        let _ = grpc_stop_tx.send(());
        let _ = grpc_task.await;
        hosted_http
            .drain_after_signal()
            .await
            .expect("drain HostedHttp listener");
        provisioned.lifecycle.stop();
        if let Some(running) = provisioned.graph.as_ref() {
            let _ = running.begin_transport_shutdown();
        }
        provisioned.shutdown().await.expect("shutdown graph");
    }
}
