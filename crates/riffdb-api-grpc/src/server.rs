//! Tonic service implementations over the API-neutral application service.

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use riffdb_auth::{AuthenticationContext, CapabilityDigestKeyProvider, CredentialAuthenticator};
use riffdb_proto::{MAX_PUBLIC_REQUEST_BYTES, MAX_PUBLIC_RESPONSE_BYTES, v1};
use riffdb_service::{
    ApplicationService, BootstrapRequestContext, CommitSubscription, CommitSubscriptionEvent,
    CreateCapabilityInvocation, HealthContext, HealthRequest, HealthResult,
    MAX_COMMIT_SUBSCRIPTION_LIFETIME, RequestCancellationHandle, RequestContext, RequestControl,
    ServiceFuture, ServiceResult,
};
use riffdb_types::RequestId;
use tonic::codegen::tokio_stream::Stream;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

use crate::authentication::{
    AUTHORIZATION_METADATA_KEY, BOOTSTRAP_TOKEN_METADATA_KEY, UNAUTHENTICATED_MESSAGE,
    authenticate_normal_request, prepare_loopback_bootstrap_token,
};
use crate::conversion::*;
use crate::error::status_from_service_failure;
use crate::generated::admin_service_server::{AdminService, AdminServiceServer};
use crate::generated::command_service_server::{CommandService, CommandServiceServer};
use crate::generated::commit_service_server::{CommitService, CommitServiceServer};
use crate::generated::contract_service_server::{ContractService, ContractServiceServer};
use crate::generated::query_service_server::{QueryService, QueryServiceServer};

const GRPC_TIMEOUT_METADATA_KEY: &str = "grpc-timeout";

/// Server-owned atomic route across initializing and activated service stages.
pub trait GrpcLifecycleRoute: Send + Sync {
    /// Returns the complete application service only after startup proofs join.
    fn active_service(&self) -> Option<Arc<dyn ApplicationService>>;

    /// Routes restricted Health through the current API-neutral service stage.
    fn restricted_health(&self, request: HealthRequest) -> Option<ServiceFuture<'_, HealthResult>>;

    /// Irreversibly closes principal-less Health before bootstrap submission.
    fn close_pre_bootstrap_admission(&self);
}

/// Explicit hard cap used to derive one finite service request deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GrpcRequestLimits {
    maximum_request_duration: Duration,
}

impl GrpcRequestLimits {
    /// Accepts a nonzero cap no larger than the POC's longest public operation.
    pub fn new(maximum_request_duration: Duration) -> Result<Self, GrpcConfigurationError> {
        if maximum_request_duration.is_zero()
            || maximum_request_duration > MAX_COMMIT_SUBSCRIPTION_LIFETIME
        {
            return Err(GrpcConfigurationError);
        }
        Ok(Self {
            maximum_request_duration,
        })
    }

    fn deadline(self, metadata: &MetadataMap) -> Result<Instant, Status> {
        let requested = grpc_timeout(metadata)?.unwrap_or(self.maximum_request_duration);
        let duration = requested.min(self.maximum_request_duration);
        Instant::now()
            .checked_add(duration)
            .ok_or_else(|| Status::internal(crate::EMERGENCY_INTERNAL_MESSAGE))
    }
}

/// Invalid server request-limit configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GrpcConfigurationError;

impl fmt::Display for GrpcConfigurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("gRPC request duration is outside the POC hard bound")
    }
}

impl Error for GrpcConfigurationError {}

/// One transport adapter shared by all five generated gRPC services.
#[derive(Clone)]
pub struct GrpcApplication {
    authenticator: Arc<dyn CredentialAuthenticator>,
    authentication: AuthenticationContext,
    bootstrap_keys: Option<Arc<CapabilityDigestKeyProvider>>,
    lifecycle: Arc<dyn GrpcLifecycleRoute>,
    limits: GrpcRequestLimits,
}

impl GrpcApplication {
    /// Wires transport-only dependencies around the shared application service.
    #[must_use]
    pub fn new(
        authenticator: Arc<dyn CredentialAuthenticator>,
        authentication: AuthenticationContext,
        bootstrap_keys: Option<Arc<CapabilityDigestKeyProvider>>,
        lifecycle: Arc<dyn GrpcLifecycleRoute>,
        limits: GrpcRequestLimits,
    ) -> Self {
        Self {
            authenticator,
            authentication,
            bootstrap_keys,
            lifecycle,
            limits,
        }
    }

    /// Builds the bounded contract service without enabling compression.
    #[must_use]
    pub fn contract_server(&self) -> ContractServiceServer<Self> {
        ContractServiceServer::new(self.clone())
            .max_decoding_message_size(MAX_PUBLIC_REQUEST_BYTES)
            .max_encoding_message_size(MAX_PUBLIC_RESPONSE_BYTES)
    }

    /// Builds the bounded command service without enabling compression.
    #[must_use]
    pub fn command_server(&self) -> CommandServiceServer<Self> {
        CommandServiceServer::new(self.clone())
            .max_decoding_message_size(MAX_PUBLIC_REQUEST_BYTES)
            .max_encoding_message_size(MAX_PUBLIC_RESPONSE_BYTES)
    }

    /// Builds the bounded query service without enabling compression.
    #[must_use]
    pub fn query_server(&self) -> QueryServiceServer<Self> {
        QueryServiceServer::new(self.clone())
            .max_decoding_message_size(MAX_PUBLIC_REQUEST_BYTES)
            .max_encoding_message_size(MAX_PUBLIC_RESPONSE_BYTES)
    }

    /// Builds the bounded commit service and stream-item ceiling.
    #[must_use]
    pub fn commit_server(&self) -> CommitServiceServer<Self> {
        CommitServiceServer::new(self.clone())
            .max_decoding_message_size(MAX_PUBLIC_REQUEST_BYTES)
            .max_encoding_message_size(MAX_PUBLIC_RESPONSE_BYTES)
    }

    /// Builds the bounded administration service without a privileged path.
    #[must_use]
    pub fn admin_server(&self) -> AdminServiceServer<Self> {
        AdminServiceServer::new(self.clone())
            .max_decoding_message_size(MAX_PUBLIC_REQUEST_BYTES)
            .max_encoding_message_size(MAX_PUBLIC_RESPONSE_BYTES)
    }

    fn normal_context(
        &self,
        metadata: &MetadataMap,
        request_id: RequestId,
    ) -> Result<(RequestContext, CancellationGuard), Status> {
        let deadline = self.limits.deadline(metadata)?;
        let principal = authenticate_normal_request(
            metadata,
            self.authenticator.as_ref(),
            &self.authentication,
        )?;
        let (control, cancellation) = RequestControl::new(deadline);
        Ok((
            RequestContext::from_authenticated_grpc(request_id, principal, control, None),
            CancellationGuard(cancellation),
        ))
    }

    fn active_service(&self) -> Result<Arc<dyn ApplicationService>, Status> {
        self.lifecycle
            .active_service()
            .ok_or_else(|| Status::unavailable("service is not ready"))
    }

    fn bootstrap_context(
        &self,
        metadata: &MetadataMap,
        peer: Option<SocketAddr>,
        request_id: RequestId,
    ) -> Result<(BootstrapRequestContext, CancellationGuard), Status> {
        let deadline = self.limits.deadline(metadata)?;
        let keys = self.bootstrap_keys.as_deref().ok_or_else(unauthenticated)?;
        let digests = prepare_loopback_bootstrap_token(metadata, peer, keys)?;
        let (control, cancellation) = RequestControl::new(deadline);
        Ok((
            BootstrapRequestContext::from_loopback_grpc(request_id, control, digests),
            CancellationGuard(cancellation),
        ))
    }
}

impl fmt::Debug for GrpcApplication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("GrpcApplication([CAPABILITY])")
    }
}

struct CancellationGuard(RequestCancellationHandle);

impl Drop for CancellationGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

fn split_request<T>(request: Request<T>) -> (MetadataMap, Option<SocketAddr>, T) {
    let peer = request.remote_addr();
    let (metadata, _extensions, message) = request.into_parts();
    (metadata, peer, message)
}

fn unauthenticated() -> Status {
    Status::unauthenticated(UNAUTHENTICATED_MESSAGE)
}

fn map_service<T>(result: ServiceResult<T>) -> Result<T, Status> {
    result.map_err(|failure| status_from_service_failure(&failure))
}

fn grpc_timeout(metadata: &MetadataMap) -> Result<Option<Duration>, Status> {
    let mut values = metadata.get_all(GRPC_TIMEOUT_METADATA_KEY).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(invalid_request());
    }
    let value = value.to_str().map_err(|_| invalid_request())?;
    if value.len() < 2 || value.len() > 9 {
        return Err(invalid_request());
    }
    let (digits, unit) = value.split_at(value.len() - 1);
    if digits.len() > 8 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid_request());
    }
    let value = digits.parse::<u64>().map_err(|_| invalid_request())?;
    let duration = match unit {
        "H" => Duration::from_secs(value.saturating_mul(60 * 60)),
        "M" => Duration::from_secs(value.saturating_mul(60)),
        "S" => Duration::from_secs(value),
        "m" => Duration::from_millis(value),
        "u" => Duration::from_micros(value),
        "n" => Duration::from_nanos(value),
        _ => return Err(invalid_request()),
    };
    Ok(Some(duration))
}

#[tonic::async_trait]
impl ContractService for GrpcApplication {
    async fn validate_contract(
        &self,
        request: Request<v1::ValidateContractRequest>,
    ) -> Result<Response<v1::ValidateContractResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = validate_contract_request_from_proto(message)?;
        let (context, _cancellation) = self.normal_context(&metadata, request_id)?;
        let service = self.active_service()?;
        let result = map_service(service.validate_contract(context, request).await)?;
        Ok(Response::new(contract_validation_result_to_proto(&result)?))
    }

    async fn explain_command(
        &self,
        request: Request<v1::ExplainCommandRequest>,
    ) -> Result<Response<v1::ExplainCommandResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = explain_command_request_from_proto(message)?;
        let (context, _cancellation) = self.normal_context(&metadata, request_id)?;
        let service = self.active_service()?;
        let result = map_service(service.explain_command(context, request).await)?;
        Ok(Response::new(explain_command_result_to_proto(&result)?))
    }

    async fn deploy_contract(
        &self,
        request: Request<v1::DeployContractRequest>,
    ) -> Result<Response<v1::DeployContractResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = deploy_contract_request_from_proto(message)?;
        let (context, _cancellation) = self.normal_context(&metadata, request_id)?;
        let service = self.active_service()?;
        let result = map_service(service.deploy_contract(context, request).await)?;
        Ok(Response::new(deploy_contract_result_to_proto(&result)))
    }

    async fn get_active_contract(
        &self,
        request: Request<v1::GetActiveContractRequest>,
    ) -> Result<Response<v1::GetActiveContractResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = get_active_contract_request_from_proto(message)?;
        let (context, _cancellation) = self.normal_context(&metadata, request_id)?;
        let service = self.active_service()?;
        let result = map_service(service.get_active_contract(context, request).await)?;
        Ok(Response::new(get_active_contract_result_to_proto(&result)))
    }
}

#[tonic::async_trait]
impl CommandService for GrpcApplication {
    async fn execute(
        &self,
        request: Request<v1::ExecuteCommandRequest>,
    ) -> Result<Response<v1::ExecuteCommandResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = execute_command_request_from_proto(message)?;
        let (context, _cancellation) = self.normal_context(&metadata, request_id)?;
        let service = self.active_service()?;
        let result = map_service(service.execute_command(context, request).await)?;
        Ok(Response::new(execute_command_result_to_proto(&result)?))
    }

    async fn get_outcome(
        &self,
        request: Request<v1::GetOutcomeRequest>,
    ) -> Result<Response<v1::GetOutcomeResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = resolve_outcome_request_from_proto(message)?;
        let (context, _cancellation) = self.normal_context(&metadata, request_id)?;
        let service = self.active_service()?;
        let result = map_service(service.resolve_command_outcome(context, request).await)?;
        Ok(Response::new(resolve_outcome_result_to_proto(&result)?))
    }
}

#[tonic::async_trait]
impl QueryService for GrpcApplication {
    async fn get_entity(
        &self,
        request: Request<v1::GetEntityRequest>,
    ) -> Result<Response<v1::GetEntityResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = get_entity_request_from_proto(message)?;
        let (context, _cancellation) = self.normal_context(&metadata, request_id)?;
        let service = self.active_service()?;
        let result = map_service(service.get_entity(context, request).await)?;
        Ok(Response::new(get_entity_result_to_proto(&result)?))
    }

    async fn scan_index(
        &self,
        request: Request<v1::ScanIndexRequest>,
    ) -> Result<Response<v1::ScanIndexResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = scan_index_request_from_proto(message)?;
        let (context, _cancellation) = self.normal_context(&metadata, request_id)?;
        let service = self.active_service()?;
        let result = map_service(service.scan_index(context, request).await)?;
        Ok(Response::new(scan_index_result_to_proto(&result)?))
    }

    async fn query_projection(
        &self,
        request: Request<v1::QueryProjectionRequest>,
    ) -> Result<Response<v1::QueryProjectionResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = query_projection_request_from_proto(message)?;
        let (context, _cancellation) = self.normal_context(&metadata, request_id)?;
        let service = self.active_service()?;
        let result = map_service(service.query_projection(context, request).await)?;
        Ok(Response::new(query_projection_result_to_proto(&result)?))
    }
}

#[tonic::async_trait]
impl CommitService for GrpcApplication {
    type SubscribeCommitsStream = CommitNotificationStream;

    async fn get_commit(
        &self,
        request: Request<v1::GetCommitRequest>,
    ) -> Result<Response<v1::GetCommitResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = get_commit_request_from_proto(message)?;
        let (context, _cancellation) = self.normal_context(&metadata, request_id)?;
        let service = self.active_service()?;
        let result = map_service(service.get_commit(context, request).await)?;
        Ok(Response::new(get_commit_result_to_proto(&result)?))
    }

    async fn scan_commits(
        &self,
        request: Request<v1::ScanCommitsRequest>,
    ) -> Result<Response<v1::ScanCommitsResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = scan_commits_request_from_proto(message)?;
        let (context, _cancellation) = self.normal_context(&metadata, request_id)?;
        let service = self.active_service()?;
        let result = map_service(service.scan_commits(context, request).await)?;
        Ok(Response::new(scan_commits_result_to_proto(&result)?))
    }

    async fn subscribe_commits(
        &self,
        request: Request<v1::SubscribeCommitsRequest>,
    ) -> Result<Response<Self::SubscribeCommitsStream>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = subscribe_commits_request_from_proto(message)?;
        let (context, cancellation) = self.normal_context(&metadata, request_id)?;
        let service = self.active_service()?;
        let result = map_service(service.subscribe_to_commits(context, request).await)?;
        Ok(Response::new(CommitNotificationStream::new(
            result.into_subscription(),
            cancellation,
        )))
    }
}

#[tonic::async_trait]
impl AdminService for GrpcApplication {
    async fn health(
        &self,
        request: Request<v1::HealthRequest>,
    ) -> Result<Response<v1::HealthResponse>, Status> {
        let (metadata, peer, message) = split_request(request);
        let (request_id, request) = health_request_from_proto(message)?;
        let result = match request_id {
            Some(request_id) => {
                let (context, _cancellation) = self.normal_context(&metadata, request_id)?;
                let service = self.active_service()?;
                map_service(
                    service
                        .health(HealthContext::authenticated(context), request)
                        .await,
                )?
            }
            None => {
                if !peer.is_some_and(|address| address.ip().is_loopback())
                    || has_normal_or_bootstrap_credentials(&metadata)
                {
                    return Err(unauthenticated());
                }
                let health = self
                    .lifecycle
                    .restricted_health(request)
                    .ok_or_else(unauthenticated)?;
                map_service(health.await)?
            }
        };
        Ok(Response::new(health_result_to_proto(&result)))
    }

    async fn stats(
        &self,
        request: Request<v1::StatsRequest>,
    ) -> Result<Response<v1::StatsResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = statistics_request_from_proto(message)?;
        let (context, _cancellation) = self.normal_context(&metadata, request_id)?;
        let service = self.active_service()?;
        let result = map_service(service.statistics(context, request).await)?;
        Ok(Response::new(statistics_result_to_proto(result)))
    }

    async fn create_capability(
        &self,
        request: Request<v1::CreateCapabilityRequest>,
    ) -> Result<Response<v1::CreateCapabilityResponse>, Status> {
        let (metadata, peer, message) = split_request(request);
        let service = self.active_service()?;
        let (invocation, _cancellation) = match v1::CapabilityCreateMode::try_from(message.mode)
            .map_err(|_| invalid_request())?
        {
            v1::CapabilityCreateMode::Normal => {
                let (request_id, request) =
                    normal_create_capability_request_from_proto(message, &self.authentication)?;
                let (context, cancellation) = self.normal_context(&metadata, request_id)?;
                (
                    CreateCapabilityInvocation::Normal { context, request },
                    cancellation,
                )
            }
            v1::CapabilityCreateMode::Bootstrap => {
                let (request_id, request) =
                    bootstrap_capability_request_from_proto(message, &self.authentication)?;
                let (context, cancellation) =
                    self.bootstrap_context(&metadata, peer, request_id)?;
                self.lifecycle.close_pre_bootstrap_admission();
                (
                    CreateCapabilityInvocation::Bootstrap { context, request },
                    cancellation,
                )
            }
            v1::CapabilityCreateMode::Unspecified => return Err(invalid_request()),
        };
        let result = map_service(service.create_capability(invocation).await)?;
        Ok(Response::new(create_capability_result_to_proto(&result)?))
    }

    async fn revoke_capability(
        &self,
        request: Request<v1::RevokeCapabilityRequest>,
    ) -> Result<Response<v1::RevokeCapabilityResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = revoke_capability_request_from_proto(message)?;
        let (context, _cancellation) = self.normal_context(&metadata, request_id)?;
        let service = self.active_service()?;
        let result = map_service(service.revoke_capability(context, request).await)?;
        Ok(Response::new(revoke_capability_result_to_proto(result)))
    }
}

fn has_normal_or_bootstrap_credentials(metadata: &MetadataMap) -> bool {
    metadata
        .get_all(AUTHORIZATION_METADATA_KEY)
        .iter()
        .next()
        .is_some()
        || metadata
            .get_all_bin(BOOTSTRAP_TOKEN_METADATA_KEY)
            .iter()
            .next()
            .is_some()
}

type NextCommitFuture = Pin<
    Box<
        dyn Future<
                Output = (
                    Box<dyn CommitSubscription>,
                    ServiceResult<CommitSubscriptionEvent>,
                ),
            > + Send,
    >,
>;

/// Bounded Tonic stream over the service-owned move-only subscription.
pub struct CommitNotificationStream {
    next: Option<NextCommitFuture>,
    _cancellation: CancellationGuard,
    done: bool,
}

impl CommitNotificationStream {
    fn new(subscription: Box<dyn CommitSubscription>, cancellation: CancellationGuard) -> Self {
        Self {
            next: Some(next_commit(subscription)),
            _cancellation: cancellation,
            done: false,
        }
    }
}

impl fmt::Debug for CommitNotificationStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CommitNotificationStream([CAPABILITY])")
    }
}

impl Stream for CommitNotificationStream {
    type Item = Result<v1::CommitNotification, Status>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        let Some(next) = self.next.as_mut() else {
            self.done = true;
            return Poll::Ready(None);
        };
        let (subscription, result) = match next.as_mut().poll(context) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result,
        };
        self.next = None;
        match result {
            Ok(event) => {
                let terminal = matches!(event, CommitSubscriptionEvent::Terminal(_));
                let event = commit_subscription_event_to_proto(&event);
                if terminal || event.is_err() {
                    self.done = true;
                } else {
                    self.next = Some(next_commit(subscription));
                }
                Poll::Ready(Some(event))
            }
            Err(failure) => {
                self.done = true;
                Poll::Ready(Some(Err(status_from_service_failure(&failure))))
            }
        }
    }
}

fn next_commit(mut subscription: Box<dyn CommitSubscription>) -> NextCommitFuture {
    Box::pin(async move {
        let event = subscription.next().await;
        (subscription, event)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_auth::{AuthenticatedPrincipal, AuthenticationFailure, OpaqueCredential};
    use riffdb_types::{Audience, DatabaseId, Environment};
    use tonic::metadata::MetadataValue;

    struct RejectingAuthenticator;

    impl CredentialAuthenticator for RejectingAuthenticator {
        fn authenticate(
            &self,
            _credential: OpaqueCredential<'_>,
            _context: &AuthenticationContext,
        ) -> Result<AuthenticatedPrincipal, AuthenticationFailure> {
            Err(AuthenticationFailure::Unauthenticated)
        }
    }

    struct InitializingRoute;

    impl GrpcLifecycleRoute for InitializingRoute {
        fn active_service(&self) -> Option<Arc<dyn ApplicationService>> {
            None
        }

        fn restricted_health(
            &self,
            _request: HealthRequest,
        ) -> Option<ServiceFuture<'_, HealthResult>> {
            None
        }

        fn close_pre_bootstrap_admission(&self) {}
    }

    #[test]
    fn request_limit_rejects_zero_and_values_above_the_public_operation_bound() {
        assert!(GrpcRequestLimits::new(Duration::ZERO).is_err());
        assert!(GrpcRequestLimits::new(MAX_COMMIT_SUBSCRIPTION_LIFETIME).is_ok());
        assert!(
            GrpcRequestLimits::new(MAX_COMMIT_SUBSCRIPTION_LIFETIME + Duration::from_nanos(1))
                .is_err()
        );
    }

    #[test]
    fn grpc_timeout_parser_is_exact_and_bounded() {
        let mut metadata = MetadataMap::new();
        metadata.insert(
            GRPC_TIMEOUT_METADATA_KEY,
            MetadataValue::from_static("30000000u"),
        );
        assert_eq!(
            grpc_timeout(&metadata).expect("valid timeout"),
            Some(Duration::from_secs(30))
        );

        metadata.insert(
            GRPC_TIMEOUT_METADATA_KEY,
            MetadataValue::from_static("123456789n"),
        );
        assert!(grpc_timeout(&metadata).is_err());
    }

    #[test]
    fn initializing_adapter_requires_no_full_application_service() {
        let adapter = GrpcApplication::new(
            Arc::new(RejectingAuthenticator),
            AuthenticationContext::new(
                DatabaseId::from_unix_milliseconds_and_random(1, [0; 10])
                    .expect("valid database ID"),
                Environment::new("test").expect("valid environment"),
                Audience::new("grpc").expect("valid audience"),
            ),
            None,
            Arc::new(InitializingRoute),
            GrpcRequestLimits::new(Duration::from_secs(30)).expect("valid request limit"),
        );
        let status = match adapter.active_service() {
            Ok(_) => panic!("initializing route must not expose a full service"),
            Err(status) => status,
        };
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert!(status.details().is_empty());
    }
}
