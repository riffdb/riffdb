use std::{
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use riffdb_api_mcp::{
    HostedMcpHttpConfiguration, HostedMcpHttpConnection, HostedMcpHttpRegistration,
    McpAdmissionSessionKey, McpCompactObservationRequest, McpCompactObservationResult,
    McpInflightLimiter, McpObservedInventory, McpObserverBackend, McpObserverBackendError,
    McpObserverBackendFuture, McpRateLimitConfig, McpRateLimiter, McpSubscribedResourceObservation,
    McpTransportKind, RiffDbMcpServer, SystemMcpMonotonicClock, SystemMcpRateClock,
    register_hosted_mcp_http,
};
use riffdb_auth::CredentialAuthenticator;
use riffdb_service::DiscoveryCatalogFence;

use super::{
    auth::ConformanceAuthenticator,
    backend::{ConformanceBackend, INTERNAL_CANARY},
};

pub(crate) type HostedConformanceService = RiffDbMcpServer<ConformanceBackend>;
pub(crate) type HostedConformanceRegistration = HostedMcpHttpRegistration<HostedConformanceService>;
pub(crate) type HostedConformanceConnection = HostedMcpHttpConnection<HostedConformanceService>;

struct HostedConformanceObserver;

impl McpObserverBackend for HostedConformanceObserver {
    type Fence = DiscoveryCatalogFence;

    fn discover_compact<'a>(
        &'a self,
        _inventory: McpObservedInventory,
        _request: McpCompactObservationRequest<'a, Self::Fence>,
    ) -> McpObserverBackendFuture<'a, McpCompactObservationResult<Self::Fence>> {
        Box::pin(async { Err(McpObserverBackendError::RetryNextTick) })
    }

    fn observe_subscribed_resource<'a>(
        &'a self,
        _uri: &'a str,
    ) -> McpObserverBackendFuture<'a, McpSubscribedResourceObservation> {
        Box::pin(async { Err(McpObserverBackendError::RetryNextTick) })
    }
}

pub(crate) fn hosted_registration(
    address: SocketAddr,
    backend: ConformanceBackend,
) -> (HostedConformanceRegistration, Arc<ConformanceAuthenticator>) {
    let protected_resource = format!("http://{address}/mcp");
    let authenticator = Arc::new(ConformanceAuthenticator::new(protected_resource));
    let configuration = HostedMcpHttpConfiguration::new(
        address,
        authenticator.context(),
        ["http://127.0.0.1:3000".to_owned()],
    )
    .expect("conformance hosted HTTP configuration");
    let rate_limiter = Arc::new(McpRateLimiter::new(
        SystemMcpRateClock::new(),
        McpRateLimitConfig::poc_default(),
    ));
    let monotonic_clock = Arc::new(SystemMcpMonotonicClock::new());
    let inflight = Arc::new(McpInflightLimiter::new());
    let session_sequence = Arc::new(AtomicU64::new(0));
    let factory_backend = backend;
    let factory = move || {
        let sequence = session_sequence
            .fetch_add(1, Ordering::AcqRel)
            .checked_add(1)
            .ok_or_else(|| io::Error::other(INTERNAL_CANARY))?;
        let session = McpAdmissionSessionKey::new(format!("conformance-http-{sequence}"))
            .map_err(|_| io::Error::other(INTERNAL_CANARY))?;
        Ok(RiffDbMcpServer::with_shared_admission(
            factory_backend.clone(),
            McpTransportKind::StreamableHttp,
            Arc::clone(&inflight),
            session,
        ))
    };
    let authentication_boundary: Arc<dyn CredentialAuthenticator> = authenticator.clone();
    let registration = register_hosted_mcp_http(
        configuration,
        authentication_boundary,
        monotonic_clock,
        rate_limiter,
        Arc::new(HostedConformanceObserver),
        factory,
    );
    (registration, authenticator)
}
