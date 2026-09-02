#![expect(
    clippy::expect_used,
    reason = "the fixed loopback MCP authority and URI satisfy the parser's validated grammar"
)]

//! Owned loopback HTTP transport for the native MCP service.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::IntoFuture;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{ConnectInfo, State};
use axum::http::{Request, Response, StatusCode};
use axum::routing::any;
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
    address: SocketAddr,
    allowed_origins: Arc<[String]>,
    rate_limiter: Arc<McpRateLimiter<SystemMcpRateClock>>,
    monotonic_clock: Arc<dyn McpMonotonicClock>,
    inflight: Arc<McpInflightLimiter>,
    session_sequence: Arc<AtomicU64>,
}

impl HostedMcpRegistrationFactory {
    fn build(
        &self,
        database_alias: DatabaseAlias,
        dependencies: HostedMcpDependencies,
    ) -> Result<HostedMcpRegistration, HostedMcpStartError> {
        let authentication_audience = dependencies.authentication.audience().clone();
        let configuration = HostedMcpHttpConfiguration::new(
            self.address,
            dependencies.authentication,
            self.allowed_origins.iter().cloned(),
        )
        .map_err(HostedMcpStartError::Configuration)?;
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

impl HostedMcp {
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

    pub(crate) async fn bind_routes(
        address: SocketAddr,
        allowed_origins: &[String],
        dependencies: impl IntoIterator<Item = (DatabaseAlias, HostedMcpDependencies)>,
    ) -> Result<Self, HostedMcpStartError> {
        let dependencies = dependencies.into_iter().collect::<Vec<_>>();
        if dependencies.is_empty() || dependencies.len() > MAX_DATABASES_PER_PROCESS {
            return Err(HostedMcpStartError::Routes);
        }
        let configuration = HostedMcpHttpConfiguration::new(
            address,
            dependencies[0].1.authentication.clone(),
            allowed_origins.iter().cloned(),
        )
        .map_err(HostedMcpStartError::Configuration)?;
        let listener = tokio::net::TcpListener::bind(configuration.bind_address())
            .await
            .map_err(HostedMcpStartError::Listener)?;
        let factory = HostedMcpRegistrationFactory {
            address,
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
        let configured = Arc::new(routes.keys().cloned().collect());
        let routes = HostedMcpRoutes {
            active: Arc::new(RwLock::new(routes)),
            configured,
        };
        let application = Router::new()
            .fallback(any(proxy))
            .with_state(routes.clone());
        let (shutdown, stopped) = oneshot::channel();
        let routes_for_maintenance = routes.clone();
        let task = tokio::spawn(async move {
            let server = axum::serve(
                listener,
                application.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async move {
                let _ = stopped.await;
            })
            .into_future();
            tokio::pin!(server);
            let mut interval = tokio::time::interval_at(
                Instant::now() + SESSION_MAINTENANCE_INTERVAL,
                SESSION_MAINTENANCE_INTERVAL,
            );
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    result = &mut server => {
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
        if !self.routes.configured.contains(alias) {
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

    pub(crate) fn suspend(&self, alias: &DatabaseAlias) -> Result<(), HostedMcpStartError> {
        if !self.routes.configured.contains(alias) {
            return Err(HostedMcpStartError::Routes);
        }
        let registration = self
            .routes
            .active
            .write()
            .map_err(|_| HostedMcpStartError::Routes)?
            .remove(alias);
        if let Some(registration) = registration {
            registration.shutdown();
        }
        Ok(())
    }
}

#[derive(Clone)]
struct HostedMcpRoutes {
    active: Arc<RwLock<BTreeMap<DatabaseAlias, HostedMcpRegistration>>>,
    configured: Arc<BTreeSet<DatabaseAlias>>,
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
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request<Body>,
) -> Response<Body> {
    let mut values = request.headers().get_all(DATABASE_METADATA_KEY).iter();
    let selected = values.next();
    if values.next().is_some() {
        return database_route_rejection();
    }
    let singleton = routes.configured.len() == 1;
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
            Self::Routes => None,
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
}
