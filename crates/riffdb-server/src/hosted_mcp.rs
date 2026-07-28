//! Owned loopback HTTP transport for the native MCP service.

use std::fmt;
use std::future::IntoFuture;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{ConnectInfo, State};
use axum::http::{Request, Response, StatusCode};
use axum::routing::any;
use riffdb_api_mcp::{
    HostedMcpHttpConfiguration, HostedMcpHttpConfigurationError, HostedMcpHttpRegistration,
    HostedMcpSessionMaintenanceError, HostedServiceMcpBackend, McpAdmissionSessionKey,
    McpInflightLimiter, McpRateLimitConfig, McpRateLimiter, McpTransportKind, RequestIdSource,
    RiffDbMcpServer, SystemMcpMonotonicClock, SystemMcpRateClock, register_hosted_service_mcp_http,
};
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
    registration: HostedMcpRegistration,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), HostedMcpTaskError>>,
}

impl HostedMcp {
    pub(crate) async fn bind(
        address: SocketAddr,
        allowed_origins: &[String],
        dependencies: HostedMcpDependencies,
    ) -> Result<Self, HostedMcpStartError> {
        let configuration = HostedMcpHttpConfiguration::new(
            address,
            dependencies.authentication,
            allowed_origins.iter().cloned(),
        )
        .map_err(HostedMcpStartError::Configuration)?;
        let listener = tokio::net::TcpListener::bind(configuration.bind_address())
            .await
            .map_err(HostedMcpStartError::Listener)?;
        let rate_limiter = Arc::new(McpRateLimiter::new(
            SystemMcpRateClock::new(),
            McpRateLimitConfig::poc_default(),
        ));
        let monotonic_clock = Arc::new(SystemMcpMonotonicClock::new());
        let inflight = Arc::new(McpInflightLimiter::new());
        let session_sequence = Arc::new(AtomicU64::new(0));

        let observer_request_ids: Arc<dyn RequestIdSource> =
            Arc::new(dependencies.request_ids.clone());
        let observer_service = Arc::new(HostedServiceMcpBackend::new(
            Arc::clone(&dependencies.service),
            observer_request_ids,
        ));

        let factory_service = Arc::clone(&dependencies.service);
        let factory_request_ids = dependencies.request_ids;
        let factory_telemetry = dependencies.telemetry;
        let factory = move || {
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
            let backend = HostedServiceMcpBackend::new(Arc::clone(&factory_service), request_ids);
            Ok(RiffDbMcpServer::with_shared_admission_and_telemetry(
                backend,
                McpTransportKind::StreamableHttp,
                Arc::clone(&inflight),
                session,
                Arc::clone(&factory_telemetry),
            ))
        };
        let registration = register_hosted_service_mcp_http(
            configuration,
            dependencies.authenticator,
            monotonic_clock,
            rate_limiter,
            observer_service,
            factory,
        );
        let application = Router::new()
            .fallback(any(proxy))
            .with_state(registration.clone());
        let (shutdown, stopped) = oneshot::channel();
        let registration_for_maintenance = registration.clone();
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
                        registration_for_maintenance
                            .expire_due_sessions()
                            .await
                            .map_err(HostedMcpTaskError::Maintenance)?;
                    }
                }
            }
        });

        Ok(Self {
            registration,
            shutdown: Some(shutdown),
            task,
        })
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    pub(crate) fn begin_shutdown(&mut self) {
        self.registration.shutdown();
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
}

impl fmt::Debug for HostedMcp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HostedMcp([CAPABILITIES])")
    }
}

async fn proxy(
    State(registration): State<HostedMcpRegistration>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request<Body>,
) -> Response<Body> {
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
