//! Strict loopback Streamable HTTP admission and authentication wrapper.

use std::{
    convert::Infallible,
    error::Error,
    fmt, io,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    str::FromStr,
    sync::Arc,
    task::{Context, Poll},
};

use bytes::{Buf, Bytes};
use futures::future::BoxFuture;
use http::{
    HeaderMap, HeaderValue, Method, Request, Response, StatusCode, Uri, Version,
    header::{
        ACCESS_CONTROL_ALLOW_ORIGIN, ALLOW, AUTHORIZATION, CACHE_CONTROL, CONTENT_LENGTH,
        CONTENT_TYPE, HOST, ORIGIN, TRANSFER_ENCODING, VARY, WWW_AUTHENTICATE,
    },
};
use http_body::{Body, Frame, SizeHint};
use http_body_util::{BodyExt, Full, Limited, combinators::BoxBody};
use riffdb_auth::{
    AuthenticatedPrincipal, AuthenticationContext, CredentialAuthenticator,
    RetainedOpaqueCredential,
};
use riffdb_service::{DiscoveryCatalogFence, RequestContext, RequestControl};
use riffdb_types::{CapabilityId, RequestId};
use rmcp::{
    RoleServer,
    model::{
        ClientJsonRpcMessage, ClientRequest, EmptyResult, Extensions, ProtocolVersion,
        ServerJsonRpcMessage, ServerResult,
    },
    transport::streamable_http_server::{
        SessionId, StreamableHttpServerConfig, StreamableHttpService,
    },
};
use serde::{
    Deserialize, Deserializer,
    de::{MapAccess, SeqAccess, Visitor},
};
use serde_json::{Map, Number, Value};
use tokio_util::sync::CancellationToken;
use tower_service::Service;

use crate::{
    MCP_INBOUND_MESSAGE_MAX_BYTES, MCP_OUTBOUND_MESSAGE_MAX_BYTES, MCP_PROTOCOL_VERSION, MCP_ROUTE,
    McpBackendRequest, McpObserverBackend, McpPostAuthenticationAdmission,
    McpPostAuthenticationLimiter, McpRateClock, McpRateLimiter, McpTransportKind,
    hosted_observer::{
        HostedGenericObserverBackend, HostedInitializationRendezvous, HostedObserverBodyAction,
        HostedObserverLifecycleError, HostedObserverRegistry, HostedObserverResponseState,
        emit_hosted_notification,
    },
    hosted_session::{
        HostedSessionManager, MAX_HOSTED_MCP_SESSION_ID_BYTES, McpMonotonicClock,
        RmcpSessionIdSource,
    },
    service_backend::{HostedServiceInvocation, HostedServiceMcpBackend},
};

const MCP_PROTOCOL_HEADER: &str = "mcp-protocol-version";
const MCP_SESSION_HEADER: &str = "mcp-session-id";
const EVENT_STREAM_CONTENT_TYPE: &str = "text/event-stream";
const JSON_CONTENT_TYPE: &str = "application/json";
const UNSUPPORTED_PROTOCOL_SENTINEL: &str = "riffdb-unsupported";
const AUTHORIZATION_PREFIX: &[u8] = b"Bearer ";
const CAPABILITY_PRESENTATION_BYTES: usize = 43;
const AUTHORIZATION_VALUE_BYTES: usize = AUTHORIZATION_PREFIX.len() + CAPABILITY_PRESENTATION_BYTES;
const MAX_ALLOWED_ORIGINS: usize = 16;
const MAX_ORIGIN_BYTES: usize = 512;
const MAX_ALLOWED_ORIGIN_AGGREGATE_BYTES: usize = 8_192;

/// Validated registration configuration for the POC loopback endpoint.
pub struct HostedMcpHttpConfiguration {
    bind_address: SocketAddr,
    authority: String,
    allowed_origins: Vec<HeaderValue>,
    authentication_context: AuthenticationContext,
}

impl HostedMcpHttpConfiguration {
    /// Validates the bind address, protected resource, and exact origin allowlist.
    pub fn new(
        bind_address: SocketAddr,
        authentication_context: AuthenticationContext,
        allowed_origins: impl IntoIterator<Item = String>,
    ) -> Result<Self, HostedMcpHttpConfigurationError> {
        if !bind_address.ip().is_loopback() || bind_address.port() == 0 {
            return Err(HostedMcpHttpConfigurationError);
        }
        let authority = validate_protected_resource(authentication_context.audience().as_str())?;
        if authority != canonical_authority(bind_address) {
            return Err(HostedMcpHttpConfigurationError);
        }

        let mut total_bytes = 0_usize;
        let mut origins = Vec::new();
        for origin in allowed_origins {
            if origins.len() >= MAX_ALLOWED_ORIGINS
                || origin.len() > MAX_ORIGIN_BYTES
                || !valid_origin(&origin)
            {
                return Err(HostedMcpHttpConfigurationError);
            }
            total_bytes = total_bytes
                .checked_add(origin.len())
                .ok_or(HostedMcpHttpConfigurationError)?;
            if total_bytes > MAX_ALLOWED_ORIGIN_AGGREGATE_BYTES {
                return Err(HostedMcpHttpConfigurationError);
            }
            let header =
                HeaderValue::from_str(&origin).map_err(|_| HostedMcpHttpConfigurationError)?;
            if origins
                .iter()
                .any(|existing: &HeaderValue| existing.as_bytes() == header.as_bytes())
            {
                return Err(HostedMcpHttpConfigurationError);
            }
            origins.push(header);
        }

        Ok(Self {
            bind_address,
            authority,
            allowed_origins: origins,
            authentication_context,
        })
    }

    /// Returns the exact loopback socket the server composition must bind.
    #[must_use]
    pub const fn bind_address(&self) -> SocketAddr {
        self.bind_address
    }
}

impl fmt::Debug for HostedMcpHttpConfiguration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HostedMcpHttpConfiguration([REDACTED])")
    }
}

/// Closed configuration rejection without echoing protected-resource text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HostedMcpHttpConfigurationError;

impl fmt::Display for HostedMcpHttpConfigurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("hosted MCP HTTP configuration is invalid")
    }
}

impl Error for HostedMcpHttpConfigurationError {}

/// A registration hook that creates one strict Tower service per accepted peer.
pub struct HostedMcpHttpRegistration<S> {
    inner: StreamableHttpService<S, HostedSessionManager>,
    shared: Arc<HostedSharedState>,
    cancellation: CancellationToken,
}

impl<S> Clone for HostedMcpHttpRegistration<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            shared: Arc::clone(&self.shared),
            cancellation: self.cancellation.clone(),
        }
    }
}

impl<S> HostedMcpHttpRegistration<S> {
    /// Creates a peer-bound Tower service using only the trusted socket address.
    pub fn service_for_peer(
        &self,
        peer: SocketAddr,
    ) -> Result<HostedMcpHttpConnection<S>, HostedMcpHttpPeerError> {
        if !peer.ip().is_loopback() {
            return Err(HostedMcpHttpPeerError);
        }
        Ok(HostedMcpHttpConnection {
            inner: self.inner.clone(),
            shared: Arc::clone(&self.shared),
            peer_ip: peer.ip(),
        })
    }

    /// Cancels the hosted SDK service during server shutdown.
    pub fn shutdown(&self) {
        self.cancellation.cancel();
    }

    /// Runs one deterministic session-lifecycle maintenance pass.
    ///
    /// Server composition schedules this on the shared five-second observer
    /// cadence. The hook itself owns no timer and samples only the injected
    /// monotonic clock.
    pub async fn expire_due_sessions(&self) -> Result<usize, HostedMcpSessionMaintenanceError> {
        let expired = self
            .shared
            .manager
            .expire_due()
            .await
            .map_err(|_| HostedMcpSessionMaintenanceError)?;
        self.shared
            .observer_registry
            .retain(|session_id| self.shared.manager.is_active(session_id))
            .map_err(|_| HostedMcpSessionMaintenanceError)?;
        Ok(expired)
    }
}

impl<S> fmt::Debug for HostedMcpHttpRegistration<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HostedMcpHttpRegistration([REDACTED])")
    }
}

/// Rejection of a non-loopback peer at registration time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HostedMcpHttpPeerError;

impl fmt::Display for HostedMcpHttpPeerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("hosted MCP HTTP peer is unavailable")
    }
}

impl Error for HostedMcpHttpPeerError {}

/// Closed failure from deterministic hosted-session lifecycle maintenance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HostedMcpSessionMaintenanceError;

impl fmt::Display for HostedMcpSessionMaintenanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("hosted MCP session maintenance is unavailable")
    }
}

impl Error for HostedMcpSessionMaintenanceError {}

/// Builds transport/conformance plumbing with an injected deterministic observer.
///
/// Production server composition must use [`register_hosted_service_mcp_http`],
/// whose body-owned bridge freshly authenticates every underlying service call.
pub fn register_hosted_mcp_http<S, C, O>(
    configuration: HostedMcpHttpConfiguration,
    authenticator: Arc<dyn CredentialAuthenticator>,
    clock: Arc<dyn McpMonotonicClock>,
    rate_limiter: Arc<McpRateLimiter<C>>,
    observer: Arc<O>,
    service_factory: impl Fn() -> Result<S, io::Error> + Send + Sync + 'static,
) -> HostedMcpHttpRegistration<S>
where
    S: rmcp::Service<RoleServer> + Send + 'static,
    C: McpRateClock + 'static,
    O: McpObserverBackend<Fence = DiscoveryCatalogFence>,
{
    let observer: Arc<HostedGenericObserverBackend> = observer;
    register_hosted_mcp_http_with_observer(
        configuration,
        authenticator,
        clock,
        rate_limiter,
        HostedObserverComposition::Generic(observer),
        service_factory,
    )
}

/// Builds the production hosted adapter over the shared API-neutral service.
///
/// This is the only production composition path. Its response body owns the
/// retained credential and gates each individual observer service call.
pub fn register_hosted_service_mcp_http<S, C>(
    configuration: HostedMcpHttpConfiguration,
    authenticator: Arc<dyn CredentialAuthenticator>,
    clock: Arc<dyn McpMonotonicClock>,
    rate_limiter: Arc<McpRateLimiter<C>>,
    observer_service: Arc<HostedServiceMcpBackend>,
    service_factory: impl Fn() -> Result<S, io::Error> + Send + Sync + 'static,
) -> HostedMcpHttpRegistration<S>
where
    S: rmcp::Service<RoleServer> + Send + 'static,
    C: McpRateClock + 'static,
{
    register_hosted_mcp_http_with_observer(
        configuration,
        authenticator,
        clock,
        rate_limiter,
        HostedObserverComposition::Service(observer_service),
        service_factory,
    )
}

fn register_hosted_mcp_http_with_observer<S, C>(
    configuration: HostedMcpHttpConfiguration,
    authenticator: Arc<dyn CredentialAuthenticator>,
    clock: Arc<dyn McpMonotonicClock>,
    rate_limiter: Arc<McpRateLimiter<C>>,
    observer: HostedObserverComposition,
    service_factory: impl Fn() -> Result<S, io::Error> + Send + Sync + 'static,
) -> HostedMcpHttpRegistration<S>
where
    S: rmcp::Service<RoleServer> + Send + 'static,
    C: McpRateClock + 'static,
{
    let cancellation = CancellationToken::new();
    let manager = Arc::new(HostedSessionManager::new(
        Arc::clone(&clock),
        Arc::new(RmcpSessionIdSource),
    ));
    let mut sdk_config = StreamableHttpServerConfig::default();
    sdk_config.sse_keep_alive = None;
    sdk_config.sse_retry = None;
    sdk_config.stateful_mode = true;
    sdk_config.json_response = false;
    sdk_config.cancellation_token = cancellation.clone();
    sdk_config.allowed_hosts = vec![configuration.authority.clone()];
    sdk_config.allowed_origins = Vec::new();
    sdk_config.session_store = None;
    let inner = StreamableHttpService::new(service_factory, Arc::clone(&manager), sdk_config);
    let pre_authentication_admission: Arc<dyn PreAuthenticationAdmission> = rate_limiter.clone();
    let post_authentication_limiter: Arc<dyn McpPostAuthenticationLimiter> = rate_limiter;
    let shared = HostedSharedState {
        authority: configuration.authority,
        allowed_origins: configuration.allowed_origins,
        authentication_context: configuration.authentication_context,
        authenticator,
        pre_authentication_admission,
        post_authentication_limiter,
        manager,
        clock,
        observer,
        observer_registry: Arc::new(HostedObserverRegistry::new()),
    };
    HostedMcpHttpRegistration {
        inner,
        shared: Arc::new(shared),
        cancellation,
    }
}

#[derive(Clone)]
enum HostedObserverComposition {
    Generic(Arc<HostedGenericObserverBackend>),
    Service(Arc<HostedServiceMcpBackend>),
}

struct HostedSharedState {
    authority: String,
    allowed_origins: Vec<HeaderValue>,
    authentication_context: AuthenticationContext,
    authenticator: Arc<dyn CredentialAuthenticator>,
    pre_authentication_admission: Arc<dyn PreAuthenticationAdmission>,
    post_authentication_limiter: Arc<dyn McpPostAuthenticationLimiter>,
    manager: Arc<HostedSessionManager>,
    clock: Arc<dyn McpMonotonicClock>,
    observer: HostedObserverComposition,
    observer_registry: Arc<HostedObserverRegistry>,
}

trait PreAuthenticationAdmission: Send + Sync {
    fn admit(&self, peer: IpAddr) -> Result<(), ()>;
}

impl<C> PreAuthenticationAdmission for McpRateLimiter<C>
where
    C: McpRateClock + 'static,
{
    fn admit(&self, peer: IpAddr) -> Result<(), ()> {
        self.admit_pre_authentication(peer).map_err(|_| ())
    }
}

/// One per-peer Tower service; request headers cannot replace its source IP.
pub struct HostedMcpHttpConnection<S> {
    inner: StreamableHttpService<S, HostedSessionManager>,
    shared: Arc<HostedSharedState>,
    peer_ip: IpAddr,
}

impl<S> Clone for HostedMcpHttpConnection<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            shared: Arc::clone(&self.shared),
            peer_ip: self.peer_ip,
        }
    }
}

impl<S> fmt::Debug for HostedMcpHttpConnection<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HostedMcpHttpConnection([REDACTED])")
    }
}

impl<B, S> Service<Request<B>> for HostedMcpHttpConnection<S>
where
    B: Body + Send + 'static,
    B::Data: Buf + Send + 'static,
    B::Error: Error + Send + Sync + 'static,
    S: rmcp::Service<RoleServer> + Send + 'static,
{
    type Response = Response<HostedMcpHttpResponseBody>;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        let connection = self.clone();
        Box::pin(async move { Ok(connection.handle(request).await) })
    }
}

impl<S> HostedMcpHttpConnection<S>
where
    S: rmcp::Service<RoleServer> + Send + 'static,
{
    async fn handle<B>(&self, request: Request<B>) -> Response<HostedMcpHttpResponseBody>
    where
        B: Body + Send + 'static,
        B::Data: Buf + Send + 'static,
        B::Error: Error + Send + Sync + 'static,
    {
        if request.uri().path() != MCP_ROUTE || request.uri().query().is_some() {
            return rejection(StatusCode::NOT_FOUND, None, None);
        }
        let supported_method = matches!(
            request.method(),
            &Method::POST | &Method::GET | &Method::DELETE
        );
        let allowed_origin = match validate_request_origin(
            request.headers(),
            self.shared.allowed_origins.as_slice(),
        ) {
            Ok(origin) => origin,
            Err(()) => return rejection(StatusCode::FORBIDDEN, None, None),
        };
        if !supported_method {
            return rejection(
                StatusCode::METHOD_NOT_ALLOWED,
                allowed_origin,
                Some((ALLOW, HeaderValue::from_static("GET, POST, DELETE"))),
            );
        }
        if !valid_effective_authority(&request, &self.shared.authority) {
            return rejection(StatusCode::MISDIRECTED_REQUEST, allowed_origin, None);
        }
        if self
            .shared
            .pre_authentication_admission
            .admit(self.peer_ip)
            .is_err()
        {
            return rejection(StatusCode::TOO_MANY_REQUESTS, allowed_origin, None);
        }

        let (mut parts, body) = request.into_parts();
        let retained = match take_authorization(&mut parts.headers) {
            Ok(retained) => retained,
            Err(()) => return unauthenticated(allowed_origin),
        };
        let principal = match self
            .shared
            .authenticator
            .authenticate(retained.borrow(), &self.shared.authentication_context)
        {
            Ok(principal) => principal,
            Err(_) => return unauthenticated(allowed_origin),
        };
        let capability_id = principal.capability_id();
        let post_authentication_admission = McpPostAuthenticationAdmission::new_hosted(
            Arc::clone(&self.shared.post_authentication_limiter),
            principal.principal_id().clone(),
            principal.tenant_scope().clone(),
        );

        let session_id = match parse_session_header(&parts.headers) {
            Ok(session_id) => session_id,
            Err(()) => return rejection(StatusCode::NOT_FOUND, allowed_origin, None),
        };
        if let Some(active_session_id) = session_id.as_ref() {
            if !has_exact_protocol_header(&parts.headers) {
                return rejection(StatusCode::BAD_REQUEST, allowed_origin, None);
            }
            if self
                .shared
                .manager
                .authorize_and_touch(active_session_id, capability_id)
                .await
                .is_err()
            {
                return rejection(StatusCode::NOT_FOUND, allowed_origin, None);
            }
        } else if parts.method != Method::POST {
            return rejection(StatusCode::NOT_FOUND, allowed_origin, None);
        } else if protocol_header_count(&parts.headers) > 1 {
            return rejection(StatusCode::BAD_REQUEST, allowed_origin, None);
        }

        match validate_declared_body_length(&parts.headers) {
            Ok(()) => {}
            Err(DeclaredBodyLengthError::TooLarge) => {
                return rejection(StatusCode::PAYLOAD_TOO_LARGE, allowed_origin, None);
            }
            Err(DeclaredBodyLengthError::Invalid) => {
                return rejection(StatusCode::BAD_REQUEST, allowed_origin, None);
            }
        }
        let body = match collect_bounded(body).await {
            Ok(body) => body,
            Err(BodyCollectionFailure::TooLarge) => {
                return rejection(StatusCode::PAYLOAD_TOO_LARGE, allowed_origin, None);
            }
            Err(BodyCollectionFailure::Unreadable) => {
                return rejection(StatusCode::BAD_REQUEST, allowed_origin, None);
            }
        };

        if parts.method != Method::POST && !body.is_empty() {
            return rejection(StatusCode::BAD_REQUEST, allowed_origin, None);
        }
        let body = if session_id.is_none() {
            match prepare_pre_initialization(&body, &mut parts.headers) {
                Ok(PreInitialization::Ping(response)) => {
                    return ping_response(*response, allowed_origin);
                }
                Ok(PreInitialization::Initialize(body)) => body,
                Err(()) => return rejection(StatusCode::BAD_REQUEST, allowed_origin, None),
            }
        } else {
            body
        };
        let initialization_rendezvous = session_id
            .is_none()
            .then(HostedInitializationRendezvous::new);
        let observer_initialization = if parts.method == Method::GET {
            let Some(active_session_id) = session_id.as_ref() else {
                return rejection(StatusCode::NOT_FOUND, allowed_origin, None);
            };
            match self.shared.observer_registry.claim(active_session_id) {
                Ok(initialization) => Some(initialization),
                Err(_) => return rejection(StatusCode::NOT_FOUND, allowed_origin, None),
            }
        } else {
            if parts.method == Method::DELETE
                && let Some(active_session_id) = session_id.as_ref()
            {
                self.shared.observer_registry.cancel(active_session_id);
            }
            None
        };

        parts.headers.remove(CONTENT_LENGTH);
        parts.headers.remove(TRANSFER_ENCODING);
        if let Ok(length) = HeaderValue::from_str(&body.len().to_string()) {
            parts.headers.insert(CONTENT_LENGTH, length);
        } else {
            return rejection(StatusCode::BAD_REQUEST, allowed_origin, None);
        }
        parts
            .extensions
            .insert(HostedAuthenticatedPrincipal(principal));
        parts.extensions.insert(post_authentication_admission);
        if let Some(rendezvous) = initialization_rendezvous.as_ref() {
            parts.extensions.insert(rendezvous.clone());
        }
        let request = Request::from_parts(parts, Full::new(body));
        let response = self.inner.handle(request).await;
        wrap_sdk_response(HostedSdkResponse {
            response,
            retained,
            capability_id,
            request_session_id: session_id,
            initialization_rendezvous,
            observer_initialization,
            shared: Arc::clone(&self.shared),
            allowed_origin,
        })
    }
}

/// Private authenticated request state copied by `http::request::Parts`.
#[derive(Clone)]
pub(crate) struct HostedAuthenticatedPrincipal(AuthenticatedPrincipal);

impl HostedAuthenticatedPrincipal {
    pub(crate) const fn capability_id(&self) -> CapabilityId {
        self.0.capability_id()
    }

    /// Joins only checked inputs through the service-owned closed constructor.
    pub(crate) fn request_context(
        &self,
        request_id: RequestId,
        control: RequestControl,
    ) -> RequestContext {
        RequestContext::from_authenticated_mcp_http(request_id, self.0.clone(), control, None)
    }
}

/// Constructs one hosted API-neutral service context from private request state.
///
/// The backend supplies only process-local control. Request identity comes from
/// the common handler's checked source, while the authenticated principal comes
/// only from the private extension inserted by this HTTP wrapper.
pub fn hosted_mcp_request_context(
    request: &McpBackendRequest<'_>,
    control: RequestControl,
) -> Result<RequestContext, HostedMcpInvocationError> {
    if request.source() != McpTransportKind::StreamableHttp {
        return Err(HostedMcpInvocationError);
    }
    let parts = request
        .extension::<http::request::Parts>()
        .ok_or(HostedMcpInvocationError)?;
    let principal = parts
        .extensions
        .get::<HostedAuthenticatedPrincipal>()
        .ok_or(HostedMcpInvocationError)?;
    Ok(principal.request_context(request.request_id().into_riffdb(), control))
}

pub(crate) fn hosted_mcp_post_authentication_admission(
    extensions: &Extensions,
) -> Option<McpPostAuthenticationAdmission> {
    extensions
        .get::<http::request::Parts>()?
        .extensions
        .get::<McpPostAuthenticationAdmission>()
        .cloned()
}

/// Closed failure when the hosted authenticated carrier is absent or mismatched.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HostedMcpInvocationError;

impl fmt::Display for HostedMcpInvocationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("hosted MCP invocation context is unavailable")
    }
}

impl Error for HostedMcpInvocationError {}

impl fmt::Debug for HostedAuthenticatedPrincipal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HostedAuthenticatedPrincipal([REDACTED])")
    }
}

/// Body-level network-write gate for complete SDK response frames.
pub struct HostedMcpHttpResponseBody {
    inner: BoxBody<Bytes, Infallible>,
    sse_authentication: Option<SseAuthentication>,
    observer: Option<HostedObserverResponseState>,
    observer_action: Option<BoxFuture<'static, Result<(), HostedObserverLifecycleError>>>,
    terminal: bool,
}

impl fmt::Debug for HostedMcpHttpResponseBody {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HostedMcpHttpResponseBody([REDACTED])")
    }
}

impl Body for HostedMcpHttpResponseBody {
    type Data = Bytes;
    type Error = HostedMcpHttpResponseBodyError;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        if this.terminal {
            return Poll::Ready(None);
        }
        if let Err(error) = this.poll_observer(context) {
            this.fail_sse();
            return Poll::Ready(Some(Err(error)));
        }
        match Pin::new(&mut this.inner).poll_frame(context) {
            Poll::Ready(Some(Ok(frame))) => {
                if frame
                    .data_ref()
                    .is_some_and(|data| data.len() > MCP_OUTBOUND_MESSAGE_MAX_BYTES)
                {
                    this.fail_sse();
                    return Poll::Ready(Some(Err(HostedMcpHttpResponseBodyError::OutputBoundary)));
                }
                if frame.data_ref().is_some()
                    && this
                        .sse_authentication
                        .as_ref()
                        .is_some_and(|authentication| !authentication.reauthenticate())
                {
                    this.fail_sse();
                    return Poll::Ready(Some(Err(HostedMcpHttpResponseBodyError::Authentication)));
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(never))) => match never {},
            Poll::Ready(None) => {
                this.terminal = true;
                this.finish_observer();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.terminal || self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        let mut hint = self.inner.size_hint();
        if hint.lower() > MCP_OUTBOUND_MESSAGE_MAX_BYTES as u64 {
            return SizeHint::new();
        }
        if hint
            .upper()
            .is_none_or(|upper| upper > MCP_OUTBOUND_MESSAGE_MAX_BYTES as u64)
        {
            hint.set_upper(MCP_OUTBOUND_MESSAGE_MAX_BYTES as u64);
        }
        hint
    }
}

impl HostedMcpHttpResponseBody {
    fn plain(inner: BoxBody<Bytes, Infallible>) -> Self {
        Self {
            inner,
            sse_authentication: None,
            observer: None,
            observer_action: None,
            terminal: false,
        }
    }

    fn poll_observer(
        &mut self,
        context: &mut Context<'_>,
    ) -> Result<(), HostedMcpHttpResponseBodyError> {
        if let Some(action) = self.observer_action.as_mut() {
            match action.as_mut().poll(context) {
                Poll::Ready(Ok(())) => self.observer_action = None,
                Poll::Ready(Err(_)) => {
                    self.observer_action = None;
                    return Err(HostedMcpHttpResponseBodyError::Observer);
                }
                Poll::Pending => return Ok(()),
            }
        }

        let Some(observer) = self.observer.as_mut() else {
            return Ok(());
        };
        let action = match observer.poll_action(context) {
            Poll::Ready(Some(action)) => action,
            Poll::Ready(None) => return Err(HostedMcpHttpResponseBodyError::Observer),
            Poll::Pending => return Ok(()),
        };
        self.start_observer_action(action)?;
        if let Some(action) = self.observer_action.as_mut() {
            match action.as_mut().poll(context) {
                Poll::Ready(Ok(())) => {
                    self.observer_action = None;
                    context.waker().wake_by_ref();
                }
                Poll::Ready(Err(_)) => {
                    self.observer_action = None;
                    return Err(HostedMcpHttpResponseBodyError::Observer);
                }
                Poll::Pending => {}
            }
        }
        Ok(())
    }

    fn start_observer_action(
        &mut self,
        action: HostedObserverBodyAction,
    ) -> Result<(), HostedMcpHttpResponseBodyError> {
        let Some(authentication) = self.sse_authentication.as_ref() else {
            return Err(HostedMcpHttpResponseBodyError::Authentication);
        };
        let principal = match authentication.authenticate_principal() {
            Ok(principal) => principal,
            Err(()) => {
                fail_observer_action_authentication(action);
                return Err(HostedMcpHttpResponseBodyError::Authentication);
            }
        };
        self.observer_action = Some(match action {
            HostedObserverBodyAction::Service {
                request,
                completion,
            } => {
                let admission = McpPostAuthenticationAdmission::new_hosted(
                    Arc::clone(&authentication.post_authentication_limiter),
                    principal.principal_id().clone(),
                    principal.tenant_scope().clone(),
                );
                let invocation = HostedServiceInvocation::fresh_observer(
                    HostedAuthenticatedPrincipal(principal),
                    admission,
                );
                let Some(service) = authentication.observer_service.as_ref().map(Arc::clone) else {
                    let _ = completion.send(Err(crate::McpBackendError::InvalidResponse));
                    return Err(HostedMcpHttpResponseBodyError::Observer);
                };
                Box::pin(async move {
                    let result = service.invoke_observer_service(&invocation, request).await;
                    let _ = completion.send(result);
                    Ok(())
                })
            }
            HostedObserverBodyAction::GenericDiscovery {
                backend,
                inventory,
                cursor,
                prior_fence,
                completion,
            } => Box::pin(async move {
                let request = crate::McpCompactObservationRequest::from_owned_parts(
                    cursor,
                    prior_fence.as_ref(),
                );
                let result = backend.discover_compact(inventory, request).await;
                let _ = completion.send(result);
                Ok(())
            }),
            HostedObserverBodyAction::GenericResource {
                backend,
                uri,
                completion,
            } => Box::pin(async move {
                let result = backend.observe_subscribed_resource(&uri).await;
                let _ = completion.send(result);
                Ok(())
            }),
            HostedObserverBodyAction::Notification { peer, notification } => {
                // `principal` is fresh authentication and exact binding evidence
                // for this delayed SDK send. Current content/policy evidence was
                // obtained immediately before the common loop enqueued this
                // authority-free marker; notification delivery adds no service
                // call outside the fixed observer budget.
                drop(principal);
                Box::pin(emit_hosted_notification(peer, notification))
            }
        });
        Ok(())
    }

    fn finish_observer(&mut self) {
        self.observer_action = None;
        let Some(mut observer) = self.observer.take() else {
            return;
        };
        observer.shutdown();
        if let Some(authentication) = self.sse_authentication.take()
            && let Some(session_id) = authentication.session_id
        {
            authentication.observer_registry.cancel(&session_id);
            authentication.manager.close_in_background(&session_id);
        }
    }

    fn fail_sse(&mut self) {
        self.terminal = true;
        self.observer_action = None;
        if let Some(mut observer) = self.observer.take() {
            observer.shutdown();
        }
        if let Some(authentication) = self.sse_authentication.take()
            && let Some(session_id) = authentication.session_id
        {
            authentication.observer_registry.cancel(&session_id);
            authentication.manager.close_in_background(&session_id);
        }
    }
}

impl Drop for HostedMcpHttpResponseBody {
    fn drop(&mut self) {
        self.finish_observer();
    }
}

fn fail_observer_action_authentication(action: HostedObserverBodyAction) {
    match action {
        HostedObserverBodyAction::Service { completion, .. } => {
            let _ = completion.send(Err(crate::McpBackendError::AuthenticationLost));
        }
        HostedObserverBodyAction::GenericDiscovery { completion, .. } => {
            let _ = completion.send(Err(crate::McpObserverBackendError::AuthenticationLost));
        }
        HostedObserverBodyAction::GenericResource { completion, .. } => {
            let _ = completion.send(Err(crate::McpObserverBackendError::AuthenticationLost));
        }
        HostedObserverBodyAction::Notification { .. } => {}
    }
}

/// Closed body failure suitable for transport logging without peer data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HostedMcpHttpResponseBodyError {
    /// The pinned SDK produced a frame larger than the accepted network bound.
    OutputBoundary,
    /// Fresh SSE credential authentication no longer matches the session.
    Authentication,
    /// The bounded hosted observer or notification channel terminated.
    Observer,
}

impl fmt::Display for HostedMcpHttpResponseBodyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("hosted MCP HTTP response was terminated")
    }
}

impl Error for HostedMcpHttpResponseBodyError {}

struct SseAuthentication {
    retained: RetainedOpaqueCredential,
    authenticator: Arc<dyn CredentialAuthenticator>,
    context: AuthenticationContext,
    manager: Arc<HostedSessionManager>,
    observer_registry: Arc<HostedObserverRegistry>,
    post_authentication_limiter: Arc<dyn McpPostAuthenticationLimiter>,
    observer_service: Option<Arc<HostedServiceMcpBackend>>,
    session_id: Option<SessionId>,
}

impl SseAuthentication {
    fn authenticate_principal(&self) -> Result<AuthenticatedPrincipal, ()> {
        let Some(session_id) = self.session_id.as_ref() else {
            return Err(());
        };
        let principal = self
            .authenticator
            .authenticate(self.retained.borrow(), &self.context)
            .map_err(|_| ())?;
        self.manager
            .validate_sse_binding(session_id, principal.capability_id())
            .map_err(|_| ())?;
        Ok(principal)
    }

    fn reauthenticate(&self) -> bool {
        self.authenticate_principal().is_ok()
    }
}

struct HostedSdkResponse {
    response: Response<BoxBody<Bytes, Infallible>>,
    retained: RetainedOpaqueCredential,
    capability_id: CapabilityId,
    request_session_id: Option<SessionId>,
    initialization_rendezvous: Option<HostedInitializationRendezvous>,
    observer_initialization: Option<crate::hosted_observer::HostedObserverInitialization>,
    shared: Arc<HostedSharedState>,
    allowed_origin: Option<HeaderValue>,
}

fn wrap_sdk_response(input: HostedSdkResponse) -> Response<HostedMcpHttpResponseBody> {
    let HostedSdkResponse {
        response,
        retained,
        capability_id,
        request_session_id,
        initialization_rendezvous,
        observer_initialization,
        shared,
        allowed_origin,
    } = input;
    let (mut parts, body) = response.into_parts();
    let is_sse = parts
        .headers
        .get(CONTENT_TYPE)
        .is_some_and(|value| value.as_bytes() == EVENT_STREAM_CONTENT_TYPE.as_bytes());
    let response_session_id = request_session_id.clone().or_else(|| {
        single_header(&parts.headers, MCP_SESSION_HEADER)
            .and_then(|value| value.to_str().ok())
            .filter(|value| valid_session_text(value))
            .map(SessionId::from)
    });

    if let Some(rendezvous) = initialization_rendezvous {
        let Some(session_id) = response_session_id.as_ref() else {
            return rejection(StatusCode::INTERNAL_SERVER_ERROR, allowed_origin, None);
        };
        if !parts.status.is_success()
            || !is_sse
            || shared
                .manager
                .validate_sse_binding(session_id, capability_id)
                .is_err()
        {
            shared.manager.close_in_background(session_id);
            return rejection(StatusCode::INTERNAL_SERVER_ERROR, allowed_origin, None);
        }
        let initialization = match rendezvous.take() {
            Ok(initialization) => initialization,
            Err(_) => {
                shared.manager.close_in_background(session_id);
                return rejection(StatusCode::INTERNAL_SERVER_ERROR, allowed_origin, None);
            }
        };
        if shared
            .observer_registry
            .register(session_id.clone(), initialization)
            .is_err()
        {
            shared.manager.close_in_background(session_id);
            return rejection(StatusCode::INTERNAL_SERVER_ERROR, allowed_origin, None);
        }
    }

    let observer = if let Some(initialization) = observer_initialization {
        let session_id = match response_session_id.as_ref() {
            Some(session_id) => session_id,
            None => {
                initialization.cancel();
                return rejection(StatusCode::INTERNAL_SERVER_ERROR, allowed_origin, None);
            }
        };
        if !parts.status.is_success() || !is_sse || request_session_id.as_ref() != Some(session_id)
        {
            initialization.cancel();
            shared.manager.close_in_background(session_id);
            return rejection(StatusCode::INTERNAL_SERVER_ERROR, allowed_origin, None);
        }
        let semaphore = shared.observer_registry.semaphore();
        let started = match &shared.observer {
            HostedObserverComposition::Generic(backend) => {
                HostedObserverResponseState::start_generic(
                    initialization,
                    Arc::clone(backend),
                    Arc::clone(&shared.clock),
                    semaphore,
                )
            }
            HostedObserverComposition::Service(service) => {
                HostedObserverResponseState::start_service(
                    initialization,
                    Arc::clone(service),
                    Arc::clone(&shared.clock),
                    semaphore,
                )
            }
        };
        match started {
            Ok(observer) => Some(observer),
            Err(_) => {
                shared.manager.close_in_background(session_id);
                return rejection(StatusCode::INTERNAL_SERVER_ERROR, allowed_origin, None);
            }
        }
    } else {
        None
    };

    apply_allowed_origin(&mut parts.headers, allowed_origin);
    let sse_authentication = is_sse.then(|| SseAuthentication {
        retained,
        authenticator: Arc::clone(&shared.authenticator),
        context: shared.authentication_context.clone(),
        manager: Arc::clone(&shared.manager),
        observer_registry: Arc::clone(&shared.observer_registry),
        post_authentication_limiter: Arc::clone(&shared.post_authentication_limiter),
        observer_service: match &shared.observer {
            HostedObserverComposition::Generic(_) => None,
            HostedObserverComposition::Service(service) => Some(Arc::clone(service)),
        },
        session_id: response_session_id,
    });
    Response::from_parts(
        parts,
        HostedMcpHttpResponseBody {
            inner: body,
            sse_authentication,
            observer,
            observer_action: None,
            terminal: false,
        },
    )
}

fn rejection(
    status: StatusCode,
    allowed_origin: Option<HeaderValue>,
    extra_header: Option<(http::header::HeaderName, HeaderValue)>,
) -> Response<HostedMcpHttpResponseBody> {
    let mut response = Response::new(HostedMcpHttpResponseBody::plain(
        Full::new(Bytes::new()).boxed(),
    ));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if let Some((name, value)) = extra_header {
        response.headers_mut().insert(name, value);
    }
    apply_allowed_origin(response.headers_mut(), allowed_origin);
    response
}

fn unauthenticated(allowed_origin: Option<HeaderValue>) -> Response<HostedMcpHttpResponseBody> {
    rejection(
        StatusCode::UNAUTHORIZED,
        allowed_origin,
        Some((WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"))),
    )
}

fn ping_response(
    message: ServerJsonRpcMessage,
    allowed_origin: Option<HeaderValue>,
) -> Response<HostedMcpHttpResponseBody> {
    let body = match serde_json::to_vec(&message) {
        Ok(body) if body.len() <= MCP_OUTBOUND_MESSAGE_MAX_BYTES => Bytes::from(body),
        _ => return rejection(StatusCode::INTERNAL_SERVER_ERROR, allowed_origin, None),
    };
    let mut response = Response::new(HostedMcpHttpResponseBody::plain(Full::new(body).boxed()));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(JSON_CONTENT_TYPE));
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    apply_allowed_origin(response.headers_mut(), allowed_origin);
    response
}

fn apply_allowed_origin(headers: &mut HeaderMap, origin: Option<HeaderValue>) {
    if let Some(origin) = origin {
        headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, origin);
        headers.insert(VARY, HeaderValue::from_static("Origin"));
    }
}

fn validate_protected_resource(
    protected_resource: &str,
) -> Result<String, HostedMcpHttpConfigurationError> {
    let uri = Uri::from_str(protected_resource).map_err(|_| HostedMcpHttpConfigurationError)?;
    if uri.scheme_str() != Some("http")
        || uri.path() != MCP_ROUTE
        || uri.query().is_some()
        || protected_resource.contains('#')
    {
        return Err(HostedMcpHttpConfigurationError);
    }
    let authority = uri
        .authority()
        .ok_or(HostedMcpHttpConfigurationError)?
        .as_str();
    let canonical = canonical_uri_authority(authority)?;
    if protected_resource != format!("http://{canonical}{MCP_ROUTE}") {
        return Err(HostedMcpHttpConfigurationError);
    }
    Ok(canonical)
}

fn valid_origin(origin: &str) -> bool {
    if origin.is_empty()
        || origin.len() > MAX_ORIGIN_BYTES
        || !origin.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
    {
        return false;
    }
    let Ok(uri) = Uri::from_str(origin) else {
        return false;
    };
    if uri.scheme_str() != Some("http") || uri.query().is_some() || origin.contains('#') {
        return false;
    }
    let Some(authority) = uri.authority() else {
        return false;
    };
    canonical_uri_authority(authority.as_str())
        .is_ok_and(|canonical| origin == format!("http://{canonical}"))
}

fn canonical_uri_authority(authority: &str) -> Result<String, HostedMcpHttpConfigurationError> {
    if authority.is_empty()
        || authority.contains('@')
        || authority.bytes().any(|byte| !(0x21..=0x7e).contains(&byte))
    {
        return Err(HostedMcpHttpConfigurationError);
    }
    let parsed =
        http::uri::Authority::from_str(authority).map_err(|_| HostedMcpHttpConfigurationError)?;
    let port = parsed
        .port_u16()
        .filter(|port| *port != 0)
        .ok_or(HostedMcpHttpConfigurationError)?;
    let host = parsed.host();
    let ip_text = host
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(host);
    let ip = IpAddr::from_str(ip_text).map_err(|_| HostedMcpHttpConfigurationError)?;
    if !ip.is_loopback() {
        return Err(HostedMcpHttpConfigurationError);
    }
    let canonical = canonical_authority(SocketAddr::new(ip, port));
    if authority != canonical {
        return Err(HostedMcpHttpConfigurationError);
    }
    Ok(canonical)
}

fn canonical_authority(address: SocketAddr) -> String {
    match address.ip() {
        IpAddr::V4(ip) => format!("{ip}:{}", address.port()),
        IpAddr::V6(ip) => format!("[{ip}]:{}", address.port()),
    }
}

fn validate_request_origin(
    headers: &HeaderMap,
    allowed_origins: &[HeaderValue],
) -> Result<Option<HeaderValue>, ()> {
    let values: Vec<_> = headers.get_all(ORIGIN).iter().collect();
    match values.as_slice() {
        [] => Ok(None),
        [origin]
            if origin.as_bytes().len() <= MAX_ORIGIN_BYTES
                && origin
                    .as_bytes()
                    .iter()
                    .all(|byte| (0x21..=0x7e).contains(byte))
                && allowed_origins
                    .iter()
                    .any(|allowed| allowed.as_bytes() == origin.as_bytes()) =>
        {
            Ok(Some((*origin).clone()))
        }
        _ => Err(()),
    }
}

fn valid_effective_authority<B>(request: &Request<B>, expected: &str) -> bool {
    let host_values: Vec<_> = request.headers().get_all(HOST).iter().collect();
    match request.version() {
        Version::HTTP_11 => {
            request.uri().authority().is_none()
                && matches!(
                    host_values.as_slice(),
                    [host] if host.as_bytes() == expected.as_bytes()
                )
        }
        Version::HTTP_2 | Version::HTTP_3 => {
            host_values.is_empty()
                && request
                    .uri()
                    .authority()
                    .is_some_and(|authority| authority.as_str() == expected)
        }
        _ => false,
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

fn parse_session_header(headers: &HeaderMap) -> Result<Option<SessionId>, ()> {
    let values: Vec<_> = headers.get_all(MCP_SESSION_HEADER).iter().collect();
    match values.as_slice() {
        [] => Ok(None),
        [value] => {
            let value = value.to_str().map_err(|_| ())?;
            if !valid_session_text(value) {
                return Err(());
            }
            Ok(Some(SessionId::from(value)))
        }
        _ => Err(()),
    }
}

fn valid_session_text(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_HOSTED_MCP_SESSION_ID_BYTES
        && !value.contains(',')
        && value.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
}

fn protocol_header_count(headers: &HeaderMap) -> usize {
    headers.get_all(MCP_PROTOCOL_HEADER).iter().count()
}

fn has_exact_protocol_header(headers: &HeaderMap) -> bool {
    matches!(
        headers
            .get_all(MCP_PROTOCOL_HEADER)
            .iter()
            .collect::<Vec<_>>()
            .as_slice(),
        [value] if value.as_bytes() == MCP_PROTOCOL_VERSION.as_bytes()
    )
}

enum DeclaredBodyLengthError {
    Invalid,
    TooLarge,
}

fn validate_declared_body_length(headers: &HeaderMap) -> Result<(), DeclaredBodyLengthError> {
    let values: Vec<_> = headers.get_all(CONTENT_LENGTH).iter().collect();
    match values.as_slice() {
        [] => Ok(()),
        [value] => {
            let text = value
                .to_str()
                .map_err(|_| DeclaredBodyLengthError::Invalid)?;
            let length = text
                .parse::<u64>()
                .map_err(|_| DeclaredBodyLengthError::Invalid)?;
            if text != length.to_string() {
                return Err(DeclaredBodyLengthError::Invalid);
            }
            if length > MCP_INBOUND_MESSAGE_MAX_BYTES as u64 {
                Err(DeclaredBodyLengthError::TooLarge)
            } else {
                Ok(())
            }
        }
        _ => Err(DeclaredBodyLengthError::Invalid),
    }
}

enum BodyCollectionFailure {
    TooLarge,
    Unreadable,
}

async fn collect_bounded<B>(body: B) -> Result<Bytes, BodyCollectionFailure>
where
    B: Body + Send + 'static,
    B::Data: Buf,
    B::Error: Error + Send + Sync + 'static,
{
    match Limited::new(body, MCP_INBOUND_MESSAGE_MAX_BYTES)
        .collect()
        .await
    {
        Ok(collected) => Ok(collected.to_bytes()),
        Err(error) if error.is::<http_body_util::LengthLimitError>() => {
            Err(BodyCollectionFailure::TooLarge)
        }
        Err(_) => Err(BodyCollectionFailure::Unreadable),
    }
}

enum PreInitialization {
    Ping(Box<ServerJsonRpcMessage>),
    Initialize(Bytes),
}

fn prepare_pre_initialization(
    body: &[u8],
    headers: &mut HeaderMap,
) -> Result<PreInitialization, ()> {
    let strict: StrictJsonValue = serde_json::from_slice(body).map_err(|_| ())?;
    let root = strict.0.as_object().ok_or(())?;
    let mut message: ClientJsonRpcMessage =
        serde_json::from_value(strict.0.clone()).map_err(|_| ())?;
    let ClientJsonRpcMessage::Request(request) = &mut message else {
        return Err(());
    };
    match &mut request.request {
        ClientRequest::PingRequest(_) => {
            if root.len() != 3
                || !root.contains_key("jsonrpc")
                || !root.contains_key("id")
                || root.get("method") != Some(&Value::String("ping".to_owned()))
                || protocol_header_count(headers) != 0
            {
                return Err(());
            }
            Ok(PreInitialization::Ping(Box::new(
                ServerJsonRpcMessage::response(
                    ServerResult::EmptyResult(EmptyResult {}),
                    request.id.clone(),
                ),
            )))
        }
        ClientRequest::InitializeRequest(initialize) => {
            if root.len() != 4
                || !root.contains_key("jsonrpc")
                || !root.contains_key("id")
                || !root.contains_key("params")
                || root.get("method") != Some(&Value::String("initialize".to_owned()))
            {
                return Err(());
            }
            let original = initialize.params.protocol_version.as_str().to_owned();
            if !valid_protocol_offer(&original) {
                return Err(());
            }
            let header = single_header(headers, MCP_PROTOCOL_HEADER);
            if protocol_header_count(headers) > 1
                || header.is_some_and(|value| value.as_bytes() != original.as_bytes())
            {
                return Err(());
            }

            if original != MCP_PROTOCOL_VERSION {
                let sentinel: ProtocolVersion =
                    serde_json::from_value(Value::String(UNSUPPORTED_PROTOCOL_SENTINEL.to_owned()))
                        .map_err(|_| ())?;
                initialize.params.protocol_version = sentinel;
                if header.is_some() {
                    headers.insert(
                        MCP_PROTOCOL_HEADER,
                        HeaderValue::from_static(UNSUPPORTED_PROTOCOL_SENTINEL),
                    );
                }
            }
            let encoded = serde_json::to_vec(&message).map_err(|_| ())?;
            if encoded.len() > MCP_INBOUND_MESSAGE_MAX_BYTES {
                return Err(());
            }
            Ok(PreInitialization::Initialize(Bytes::from(encoded)))
        }
        _ => Err(()),
    }
}

fn valid_protocol_offer(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 10
        && bytes[0..4].iter().all(u8::is_ascii_digit)
        && bytes[4] == b'-'
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[7] == b'-'
        && bytes[8..10].iter().all(u8::is_ascii_digit)
}

fn single_header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a HeaderValue> {
    let mut values = headers.get_all(name).iter();
    let first = values.next()?;
    values.next().is_none().then_some(first)
}

struct StrictJsonValue(Value);

impl<'de> Deserialize<'de> for StrictJsonValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(StrictJsonVisitor).map(Self)
    }
}

struct StrictJsonVisitor;

impl<'de> Visitor<'de> for StrictJsonVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("JSON without duplicate object members")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(Value::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(Value::Number(Number::from(value)))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(Value::Number(Number::from(value)))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| E::custom("non-finite JSON number"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(Value::String(value.to_owned()))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(Value::String(value))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(Value::Null)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(Value::Null)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element::<StrictJsonValue>()? {
            values.push(value.0);
        }
        Ok(Value::Array(values))
    }

    fn visit_map<A>(self, mut object: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = Map::new();
        while let Some(key) = object.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(serde::de::Error::custom("duplicate JSON object member"));
            }
            let value = object.next_value::<StrictJsonValue>()?;
            values.insert(key, value.0);
        }
        Ok(Value::Object(values))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicU64, Ordering},
        time::Duration,
    };

    use riffdb_types::{
        ActorId, Audience, DatabaseId, Environment, ServiceOperationV1, TenantScope,
    };

    use super::*;
    use crate::{McpRateLimitConfig, McpRateTarget};

    #[derive(Clone)]
    struct TestClock(Arc<AtomicU64>);

    impl TestClock {
        fn new(nanos: u64) -> Self {
            Self(Arc::new(AtomicU64::new(nanos)))
        }

        fn set(&self, nanos: u64) {
            self.0.store(nanos, Ordering::SeqCst);
        }
    }

    impl McpRateClock for TestClock {
        fn now(&self) -> Result<Duration, crate::McpRateClockError> {
            Ok(Duration::from_nanos(self.0.load(Ordering::SeqCst)))
        }
    }

    fn uuid_bytes(marker: u8) -> [u8; 16] {
        let mut bytes = [marker; 16];
        bytes[0..6].copy_from_slice(&[0, 0, 0, 0, 0, marker]);
        bytes[6] = 0x70;
        bytes[8] = 0x80;
        bytes
    }

    fn authentication_context(audience: &str) -> AuthenticationContext {
        AuthenticationContext::new(
            DatabaseId::from_bytes(uuid_bytes(1)).expect("database"),
            Environment::new("test").expect("environment"),
            Audience::new(audience).expect("audience"),
        )
    }

    #[test]
    fn protected_resource_and_origins_are_exact_loopback_spellings() {
        let config = HostedMcpHttpConfiguration::new(
            "127.0.0.1:7444".parse().expect("socket"),
            authentication_context("http://127.0.0.1:7444/mcp"),
            ["http://127.0.0.1:3000".to_owned()],
        )
        .expect("valid configuration");
        assert_eq!(
            config.bind_address(),
            "127.0.0.1:7444".parse().expect("socket")
        );
        assert_eq!(
            format!("{config:?}"),
            "HostedMcpHttpConfiguration([REDACTED])"
        );

        for audience in [
            "https://127.0.0.1:7444/mcp",
            "http://localhost:7444/mcp",
            "http://127.0.0.1/mcp",
            "http://127.0.0.1:7444/mcp/",
            "http://127.0.0.1:7444/mcp?x=1",
        ] {
            assert!(
                HostedMcpHttpConfiguration::new(
                    "127.0.0.1:7444".parse().expect("socket"),
                    authentication_context(audience),
                    Vec::<String>::new(),
                )
                .is_err(),
                "{audience}"
            );
        }
    }

    #[test]
    fn origin_header_is_absent_or_one_exact_allowlisted_value() {
        let allowed = vec![HeaderValue::from_static("http://127.0.0.1:3000")];
        let mut headers = HeaderMap::new();
        assert_eq!(
            validate_request_origin(&headers, &allowed).expect("absent"),
            None
        );
        headers.insert(ORIGIN, HeaderValue::from_static("http://127.0.0.1:3000"));
        assert_eq!(
            validate_request_origin(&headers, &allowed)
                .expect("allowed")
                .expect("present")
                .as_bytes(),
            b"http://127.0.0.1:3000"
        );
        headers.insert(ORIGIN, HeaderValue::from_static("http://127.0.0.1:3001"));
        assert!(validate_request_origin(&headers, &allowed).is_err());

        headers.insert(
            ORIGIN,
            HeaderValue::from_bytes(b"http://127.0.0.1:3000 ")
                .expect("test-only visible whitespace"),
        );
        assert!(validate_request_origin(&headers, &allowed).is_err());
    }

    #[test]
    fn effective_authority_is_version_specific_and_byte_exact() {
        let http_11 = Request::builder()
            .version(Version::HTTP_11)
            .uri(MCP_ROUTE)
            .header(HOST, "127.0.0.1:7444")
            .body(())
            .expect("request");
        assert!(valid_effective_authority(&http_11, "127.0.0.1:7444"));

        let http_2 = Request::builder()
            .version(Version::HTTP_2)
            .uri("http://127.0.0.1:7444/mcp")
            .body(())
            .expect("request");
        assert!(valid_effective_authority(&http_2, "127.0.0.1:7444"));

        let both = Request::builder()
            .version(Version::HTTP_2)
            .uri("http://127.0.0.1:7444/mcp")
            .header(HOST, "127.0.0.1:7444")
            .body(())
            .expect("request");
        assert!(!valid_effective_authority(&both, "127.0.0.1:7444"));

        let whitespace = Request::builder()
            .version(Version::HTTP_11)
            .uri(MCP_ROUTE)
            .header(
                HOST,
                HeaderValue::from_bytes(b"127.0.0.1:7444 ").expect("test-only visible whitespace"),
            )
            .body(())
            .expect("request");
        assert!(!valid_effective_authority(&whitespace, "127.0.0.1:7444"));
    }

    #[test]
    fn authorization_is_one_exact_field_and_is_removed_before_dispatch() {
        let token = "A".repeat(CAPABILITY_PRESENTATION_BYTES);
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).expect("header"),
        );
        let retained = take_authorization(&mut headers).expect("exact bearer field");
        assert!(!headers.contains_key(AUTHORIZATION));
        assert!(!format!("{retained:?}").contains(&token));

        for malformed in [
            format!("bearer {token}"),
            format!("Bearer  {token}"),
            format!("Bearer {token}="),
            format!("Bearer {token} "),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&malformed).expect("test header"),
            );
            assert!(take_authorization(&mut headers).is_err());
        }
    }

    #[test]
    fn content_length_is_canonical_and_distinguishes_excess() {
        let mut headers = HeaderMap::new();
        assert!(validate_declared_body_length(&headers).is_ok());
        headers.insert(CONTENT_LENGTH, HeaderValue::from_static("1048576"));
        assert!(validate_declared_body_length(&headers).is_ok());
        headers.insert(CONTENT_LENGTH, HeaderValue::from_static("1048577"));
        assert!(matches!(
            validate_declared_body_length(&headers),
            Err(DeclaredBodyLengthError::TooLarge)
        ));
        headers.insert(CONTENT_LENGTH, HeaderValue::from_static("01"));
        assert!(matches!(
            validate_declared_body_length(&headers),
            Err(DeclaredBodyLengthError::Invalid)
        ));
    }

    #[test]
    fn pre_authentication_bucket_has_exact_burst_and_refill() {
        let clock = TestClock::new(0);
        let limiter = McpRateLimiter::new(clock.clone(), McpRateLimitConfig::poc_default());
        let peer = IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
        for _ in 0..32 {
            limiter.admit_pre_authentication(peer).expect("burst token");
        }
        assert!(limiter.admit_pre_authentication(peer).is_err());
        clock.set(124_999_999);
        assert!(limiter.admit_pre_authentication(peer).is_err());
        clock.set(125_000_000);
        limiter
            .admit_pre_authentication(peer)
            .expect("one refilled token");
        assert!(limiter.admit_pre_authentication(peer).is_err());
    }

    #[test]
    fn authenticated_admission_survives_the_exact_nested_extension_handoff() {
        let limiter = Arc::new(McpRateLimiter::new(
            TestClock::new(0),
            McpRateLimitConfig::new(1, 1, 1, 1).expect("lowered configuration"),
        ));
        let erased: Arc<dyn McpPostAuthenticationLimiter> = limiter;
        let admission = McpPostAuthenticationAdmission::new_hosted(
            erased,
            ActorId::new("operator").expect("bounded actor"),
            TenantScope::Global,
        );
        let request = Request::builder().body(()).expect("request");
        let (mut parts, _) = request.into_parts();
        parts.extensions.insert(admission);
        let mut extensions = Extensions::new();
        extensions.insert(parts);

        let recovered = hosted_mcp_post_authentication_admission(&extensions)
            .expect("nested authenticated admission");
        let target = McpRateTarget::Service(ServiceOperationV1::GetEntity);
        assert_eq!(recovered.admit(target.clone()), Ok(()));
        assert_eq!(
            recovered.admit(target),
            Err(crate::McpRateLimitError::RateLimited)
        );
    }

    #[test]
    fn strict_json_rejects_duplicate_fields_and_ping_parameters() {
        assert!(
            serde_json::from_slice::<StrictJsonValue>(
                br#"{"jsonrpc":"2.0","id":1,"id":2,"method":"ping"}"#
            )
            .is_err()
        );

        let mut headers = HeaderMap::new();
        assert!(matches!(
            prepare_pre_initialization(
                br#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#,
                &mut headers
            ),
            Ok(PreInitialization::Ping(_))
        ));
        assert!(
            prepare_pre_initialization(
                br#"{"jsonrpc":"2.0","id":1,"method":"ping","params":{}}"#,
                &mut headers
            )
            .is_err()
        );
    }

    #[test]
    fn unsupported_initialize_offer_is_rewritten_as_typed_sentinel() {
        let mut headers = HeaderMap::new();
        headers.insert(MCP_PROTOCOL_HEADER, HeaderValue::from_static("2099-01-01"));
        let body = br#"{"jsonrpc":"2.0","id":"init","method":"initialize","params":{"protocolVersion":"2099-01-01","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#;
        let PreInitialization::Initialize(rewritten) =
            prepare_pre_initialization(body, &mut headers).expect("valid initialization")
        else {
            panic!("expected initialize");
        };
        let message: ClientJsonRpcMessage =
            serde_json::from_slice(&rewritten).expect("rewritten typed message");
        let ClientJsonRpcMessage::Request(request) = message else {
            panic!("request");
        };
        let ClientRequest::InitializeRequest(initialize) = request.request else {
            panic!("initialize");
        };
        assert_eq!(
            initialize.params.protocol_version.as_str(),
            UNSUPPORTED_PROTOCOL_SENTINEL
        );
        assert_eq!(
            headers
                .get(MCP_PROTOCOL_HEADER)
                .expect("rewritten header")
                .as_bytes(),
            UNSUPPORTED_PROTOCOL_SENTINEL.as_bytes()
        );
    }

    #[tokio::test]
    async fn outer_response_gate_accepts_exact_limit_and_rejects_one_excess_byte() {
        let exact = HostedMcpHttpResponseBody::plain(
            Full::new(Bytes::from(vec![0_u8; MCP_OUTBOUND_MESSAGE_MAX_BYTES])).boxed(),
        )
        .collect()
        .await
        .expect("exact output limit")
        .to_bytes();
        assert_eq!(exact.len(), MCP_OUTBOUND_MESSAGE_MAX_BYTES);

        let error = HostedMcpHttpResponseBody::plain(
            Full::new(Bytes::from(vec![0_u8; MCP_OUTBOUND_MESSAGE_MAX_BYTES + 1])).boxed(),
        )
        .collect()
        .await
        .expect_err("one excess byte");
        assert_eq!(error.to_string(), "hosted MCP HTTP response was terminated");
    }

    #[test]
    fn credentials_never_enter_session_source() {
        let source = include_str!("hosted_session.rs");
        assert!(!source.contains("RetainedOpaqueCredential"));
        assert!(!source.contains("OpaqueCredential"));
        assert!(!source.contains("Authorization"));
    }
}
