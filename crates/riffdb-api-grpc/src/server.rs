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
use riffdb_errors::{ApplicationOperation, PublicErrorKind};
use riffdb_proto::{MAX_PUBLIC_REQUEST_BYTES, MAX_PUBLIC_RESPONSE_BYTES, app::v1 as app_v1, v1};
use riffdb_service::{
    ApplicationErrorContextBuilder, ApplicationService, BootstrapCapabilityResult,
    BootstrapRequestContext, CommitSubscription, CommitSubscriptionEvent,
    CreateCapabilityInvocation, CreateCapabilityResult, DeployContractResult, HealthContext,
    HealthRequest, HealthResult, MAX_COMMIT_SUBSCRIPTION_LIFETIME,
    RecoveryOfflineMaintenanceApplication, RecoveryRestoreOfflineBackupInvocation,
    RequestCancellationHandle, RequestContext, RequestControl, RestoreOfflineBackupInvocation,
    RestoreRetryOfflineMaintenanceApplication, ServiceFuture, ServiceResult,
};
use riffdb_types::{
    ContractLineage, ContractVersion, OfflineMaintenanceInputHash, OfflineMaintenanceOperationId,
    RequestId, ServiceOperationV1,
};
use tonic::codegen::tokio_stream::Stream;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

use crate::authentication::{
    AUTHORIZATION_METADATA_KEY, BOOTSTRAP_TOKEN_METADATA_KEY, UNAUTHENTICATED_MESSAGE,
    authenticate_and_retain_normal_request, authenticate_normal_request,
    prepare_loopback_bootstrap_token, retain_normal_request_credential,
};
use crate::conversion::*;
use crate::error::{
    status_from_application_boundary, status_from_application_failure, status_from_service_failure,
};
use crate::generated::admin_service_server::{AdminService, AdminServiceServer};
use crate::generated::command_service_server::{CommandService, CommandServiceServer};
use crate::generated::commit_service_server::{CommitService, CommitServiceServer};
use crate::generated::contract_service_server::{ContractService, ContractServiceServer};
use crate::generated::query_service_server::{QueryService, QueryServiceServer};
use crate::generated_app::application_query_service_server::{
    ApplicationQueryService, ApplicationQueryServiceServer,
};

const GRPC_TIMEOUT_METADATA_KEY: &str = "grpc-timeout";

/// Server-owned atomic route across initializing and activated service stages.
pub trait GrpcLifecycleRoute: Send + Sync {
    /// Atomically admits one authenticated operation in the current lifecycle.
    ///
    /// Implementations must reject before authentication when the operation is
    /// unavailable, and must never return a service for a broader lifecycle
    /// surface than the supplied exact operation permits.
    fn admit_authenticated(
        &self,
        operation: ServiceOperationV1,
    ) -> Option<Arc<dyn ApplicationService>>;

    /// Atomically admits one current-database maintenance action.
    ///
    /// This separate process-local registry must never be converted into a
    /// durable [`ServiceOperationV1`] value. Restore admission carries the
    /// structurally checked operation identity so retry-only lifecycle stages
    /// reject unrelated requests before credential authentication.
    fn admit_offline_maintenance(
        &self,
        operation: GrpcOfflineMaintenanceOperation,
    ) -> Option<Arc<dyn ApplicationService>>;

    /// Returns the sole current-database restore retry service for exact input.
    ///
    /// This capability is disjoint from the ordinary application service and
    /// cannot create a backup, observe a receipt, or invoke another operation.
    fn admit_restore_retry(
        &self,
        operation_id: OfflineMaintenanceOperationId,
        input_hash: OfflineMaintenanceInputHash,
    ) -> Option<Arc<dyn RestoreRetryOfflineMaintenanceApplication>>;

    /// Returns the sole restricted staged-only restore service for exact input.
    ///
    /// Recovery mode exposes no normal application service, backup creation,
    /// receipt polling, or current-database security context.
    fn admit_recovery_restore(
        &self,
        operation_id: OfflineMaintenanceOperationId,
        input_hash: OfflineMaintenanceInputHash,
    ) -> Option<Arc<dyn RecoveryOfflineMaintenanceApplication>>;

    /// Returns the checked transport security installed after startup validation.
    ///
    /// Callers must first establish that the exact lifecycle operation is
    /// admissible. Initializing and stopped routes return no context.
    fn security_context(&self) -> Option<CheckedGrpcSecurityContext>;

    /// Returns current-database authentication scoped only to an admitted retry.
    fn restore_retry_security_context(&self) -> Option<CheckedGrpcRestoreRetrySecurityContext>;

    /// Returns the one opaque process generation installed with the activated route.
    ///
    /// The bytes are presentation-only and carry no semantic or authorization
    /// meaning. Initializing and stopped routes must not expose them.
    fn server_generation(&self) -> Option<[u8; 16]>;

    /// Routes restricted Health through the current API-neutral service stage.
    fn restricted_health(&self, request: HealthRequest) -> Option<ServiceFuture<'_, HealthResult>>;

    /// Checks whether bootstrap token preparation is legal without changing state.
    fn bootstrap_available(&self) -> bool;

    /// Atomically admits one legal bootstrap attempt and returns its exact service.
    ///
    /// At most one caller may hold bootstrap admission. For initial bootstrap,
    /// the transition must close the shared pre-bootstrap Health issuer before
    /// returning, so submission has no close-versus-submit race.
    fn begin_bootstrap(&self) -> Option<Arc<dyn ApplicationService>>;

    /// Applies the terminal lifecycle disposition for the admitted bootstrap attempt.
    ///
    /// `Created` and `Replayed` transition to authenticated deployment-required
    /// routing. `OutcomeUnknown`, `Failed`, and `Abandoned` stop routing. The
    /// server-owned route applies the separately reviewed conflict disposition.
    fn finish_bootstrap(&self, completion: GrpcBootstrapCompletion);

    /// Applies the terminal lifecycle disposition for an admitted first deployment.
    fn finish_deployment(&self, completion: GrpcDeploymentCompletion);
}

/// Closed transport-local offline-maintenance admission registry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GrpcOfflineMaintenanceOperation {
    /// Start or resolve immutable backup creation.
    CreateBackup,
    /// Start or resolve staged restore with its checked receipt and semantic-input identity.
    RestoreBackup {
        /// Caller-stable receipt identity.
        operation_id: OfflineMaintenanceOperationId,
        /// Canonical operation kind, name, and replacement-confirmation identity.
        input_hash: OfflineMaintenanceInputHash,
    },
    /// Observe one protected external receipt.
    GetOperation,
}

/// Transport-observed terminal classification for one admitted bootstrap attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GrpcBootstrapCompletion {
    /// The first durable capability and marker were created.
    Created,
    /// The retained credential recovered the original durable transition.
    Replayed,
    /// Storage returned the closed bootstrap-conflict result.
    Conflict,
    /// The application service reported that durable completion is uncertain.
    OutcomeUnknown,
    /// The admitted attempt ended with another definite failure.
    Failed,
    /// The transport future disappeared before observing a terminal result.
    Abandoned,
}

/// Transport-observed terminal classification for deployment-required activation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GrpcDeploymentCompletion {
    /// A new active catalog was committed.
    Activated,
    /// The exact candidate was already active.
    AlreadyActive,
    /// A definite non-activating result or failure leaves deployment required.
    NotActivated,
    /// The application service reported uncertain durable completion.
    OutcomeUnknown,
    /// The transport future disappeared before observing a terminal result.
    Abandoned,
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

/// Cloneable transport security installed only after checked startup activation.
#[derive(Clone)]
pub struct CheckedGrpcSecurityContext {
    authenticator: Arc<dyn CredentialAuthenticator>,
    authentication: AuthenticationContext,
    bootstrap_keys: Arc<CapabilityDigestKeyProvider>,
}

impl CheckedGrpcSecurityContext {
    /// Packages the exact startup-validated authentication scope and key custody.
    #[must_use]
    pub fn new(
        authenticator: Arc<dyn CredentialAuthenticator>,
        authentication: AuthenticationContext,
        bootstrap_keys: Arc<CapabilityDigestKeyProvider>,
    ) -> Self {
        Self {
            authenticator,
            authentication,
            bootstrap_keys,
        }
    }
}

impl fmt::Debug for CheckedGrpcSecurityContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CheckedGrpcSecurityContext([CAPABILITY])")
    }
}

/// Cloneable current-database authentication scoped to exact restore retry.
///
/// Unlike [`CheckedGrpcSecurityContext`], this capability carries no bootstrap
/// digest-key custody and is never used by an ordinary application operation.
#[derive(Clone)]
pub struct CheckedGrpcRestoreRetrySecurityContext {
    authenticator: Arc<dyn CredentialAuthenticator>,
    authentication: AuthenticationContext,
}

impl CheckedGrpcRestoreRetrySecurityContext {
    /// Packages the validated current-database authentication boundary.
    #[must_use]
    pub fn new(
        authenticator: Arc<dyn CredentialAuthenticator>,
        authentication: AuthenticationContext,
    ) -> Self {
        Self {
            authenticator,
            authentication,
        }
    }
}

impl fmt::Debug for CheckedGrpcRestoreRetrySecurityContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CheckedGrpcRestoreRetrySecurityContext([CAPABILITY])")
    }
}

/// One transport adapter shared by all five generated gRPC services.
#[derive(Clone)]
pub struct GrpcApplication {
    lifecycle: Arc<dyn GrpcLifecycleRoute>,
    limits: GrpcRequestLimits,
}

impl GrpcApplication {
    /// Wires transport-only dependencies around the shared application service.
    #[must_use]
    pub fn new(lifecycle: Arc<dyn GrpcLifecycleRoute>, limits: GrpcRequestLimits) -> Self {
        Self { lifecycle, limits }
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

    /// Builds the bounded symbolic application query service.
    #[must_use]
    pub fn application_query_server(&self) -> ApplicationQueryServiceServer<Self> {
        ApplicationQueryServiceServer::new(self.clone())
            .max_decoding_message_size(MAX_PUBLIC_REQUEST_BYTES)
            .max_encoding_message_size(MAX_PUBLIC_RESPONSE_BYTES)
    }

    fn normal_context(
        &self,
        metadata: &MetadataMap,
        request_id: RequestId,
        security: &CheckedGrpcSecurityContext,
    ) -> Result<(RequestContext, CancellationGuard), Status> {
        let deadline = self.limits.deadline(metadata)?;
        let principal = authenticate_normal_request(
            metadata,
            security.authenticator.as_ref(),
            &security.authentication,
        )?;
        let (control, cancellation) = RequestControl::new(deadline);
        Ok((
            RequestContext::from_authenticated_grpc(request_id, principal, control, None),
            CancellationGuard(cancellation),
        ))
    }

    fn normal_invocation(
        &self,
        operation: ServiceOperationV1,
        metadata: &MetadataMap,
        request_id: RequestId,
    ) -> Result<
        (
            Arc<dyn ApplicationService>,
            RequestContext,
            CancellationGuard,
        ),
        Status,
    > {
        let (service, security) = self.normal_admission(operation)?;
        let (context, cancellation) = self.normal_context(metadata, request_id, &security)?;
        Ok((service, context, cancellation))
    }

    fn normal_admission(
        &self,
        operation: ServiceOperationV1,
    ) -> Result<(Arc<dyn ApplicationService>, CheckedGrpcSecurityContext), Status> {
        let service = self
            .lifecycle
            .admit_authenticated(operation)
            .ok_or_else(service_not_ready)?;
        let security = self
            .lifecycle
            .security_context()
            .ok_or_else(service_not_ready)?;
        Ok((service, security))
    }

    fn ready_maintenance_admission(
        &self,
        operation: GrpcOfflineMaintenanceOperation,
    ) -> Result<(Arc<dyn ApplicationService>, CheckedGrpcSecurityContext), Status> {
        let service = self
            .lifecycle
            .admit_offline_maintenance(operation)
            .ok_or_else(service_not_ready)?;
        let security = self
            .lifecycle
            .security_context()
            .ok_or_else(service_not_ready)?;
        Ok((service, security))
    }

    fn restore_context(
        &self,
        metadata: &MetadataMap,
        request_id: RequestId,
        security: &CheckedGrpcSecurityContext,
    ) -> Result<
        (
            RequestContext,
            riffdb_auth::RetainedOpaqueCredential,
            CancellationGuard,
        ),
        Status,
    > {
        let deadline = self.limits.deadline(metadata)?;
        let (principal, credential) = authenticate_and_retain_normal_request(
            metadata,
            security.authenticator.as_ref(),
            &security.authentication,
        )?;
        let (control, cancellation) = RequestControl::new(deadline);
        Ok((
            RequestContext::from_authenticated_grpc(request_id, principal, control, None),
            credential,
            CancellationGuard(cancellation),
        ))
    }

    fn restore_retry_context(
        &self,
        metadata: &MetadataMap,
        request_id: RequestId,
        security: &CheckedGrpcRestoreRetrySecurityContext,
    ) -> Result<
        (
            RequestContext,
            riffdb_auth::RetainedOpaqueCredential,
            CancellationGuard,
        ),
        Status,
    > {
        let deadline = self.limits.deadline(metadata)?;
        let (principal, credential) = authenticate_and_retain_normal_request(
            metadata,
            security.authenticator.as_ref(),
            &security.authentication,
        )?;
        let (control, cancellation) = RequestControl::new(deadline);
        Ok((
            RequestContext::from_authenticated_grpc(request_id, principal, control, None),
            credential,
            CancellationGuard(cancellation),
        ))
    }

    fn recovery_restore_context(
        &self,
        metadata: &MetadataMap,
        request_id: RequestId,
        request: riffdb_service::RestoreOfflineBackupRequest,
    ) -> Result<(RecoveryRestoreOfflineBackupInvocation, CancellationGuard), Status> {
        let deadline = self.limits.deadline(metadata)?;
        let credential = retain_normal_request_credential(metadata)?;
        let (control, cancellation) = RequestControl::new(deadline);
        Ok((
            RecoveryRestoreOfflineBackupInvocation::from_restricted_grpc(
                request_id, control, request, credential,
            ),
            CancellationGuard(cancellation),
        ))
    }

    fn bootstrap_context(
        &self,
        metadata: &MetadataMap,
        peer: Option<SocketAddr>,
        request_id: RequestId,
        security: &CheckedGrpcSecurityContext,
    ) -> Result<(BootstrapRequestContext, CancellationGuard), Status> {
        let deadline = self.limits.deadline(metadata)?;
        let digests = prepare_loopback_bootstrap_token(metadata, peer, &security.bootstrap_keys)?;
        let (control, cancellation) = RequestControl::new(deadline);
        Ok((
            BootstrapRequestContext::from_loopback_grpc(request_id, control, digests),
            CancellationGuard(cancellation),
        ))
    }

    fn bootstrap_security(&self) -> Result<CheckedGrpcSecurityContext, Status> {
        if !self.lifecycle.bootstrap_available() {
            return Err(service_not_ready());
        }
        self.lifecycle
            .security_context()
            .ok_or_else(service_not_ready)
    }
}

struct BootstrapLifecycleGuard<'a> {
    lifecycle: &'a dyn GrpcLifecycleRoute,
    completed: bool,
}

impl<'a> BootstrapLifecycleGuard<'a> {
    fn new(lifecycle: &'a dyn GrpcLifecycleRoute) -> Self {
        Self {
            lifecycle,
            completed: false,
        }
    }

    fn complete(mut self, completion: GrpcBootstrapCompletion) {
        self.completed = true;
        self.lifecycle.finish_bootstrap(completion);
    }
}

impl Drop for BootstrapLifecycleGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.lifecycle
                .finish_bootstrap(GrpcBootstrapCompletion::Abandoned);
        }
    }
}

struct DeploymentLifecycleGuard<'a> {
    lifecycle: &'a dyn GrpcLifecycleRoute,
    completed: bool,
}

impl<'a> DeploymentLifecycleGuard<'a> {
    fn new(lifecycle: &'a dyn GrpcLifecycleRoute) -> Self {
        Self {
            lifecycle,
            completed: false,
        }
    }

    fn complete(mut self, completion: GrpcDeploymentCompletion) {
        self.completed = true;
        self.lifecycle.finish_deployment(completion);
    }
}

impl Drop for DeploymentLifecycleGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.lifecycle
                .finish_deployment(GrpcDeploymentCompletion::Abandoned);
        }
    }
}

fn classify_bootstrap_completion(
    result: &ServiceResult<CreateCapabilityResult>,
) -> GrpcBootstrapCompletion {
    match result {
        Ok(CreateCapabilityResult::Bootstrap(BootstrapCapabilityResult::Created(_))) => {
            GrpcBootstrapCompletion::Created
        }
        Ok(CreateCapabilityResult::Bootstrap(BootstrapCapabilityResult::Replayed(_))) => {
            GrpcBootstrapCompletion::Replayed
        }
        Ok(CreateCapabilityResult::Bootstrap(BootstrapCapabilityResult::BootstrapConflict)) => {
            GrpcBootstrapCompletion::Conflict
        }
        Err(failure)
            if failure
                .public_error()
                .is_some_and(|error| error.kind() == PublicErrorKind::OutcomeUnknown) =>
        {
            GrpcBootstrapCompletion::OutcomeUnknown
        }
        Ok(CreateCapabilityResult::Normal(_)) | Err(_) => GrpcBootstrapCompletion::Failed,
    }
}

/// Classifies one API-neutral deployment result for the shared server lifecycle.
///
/// Every in-process transport must use this exact classifier so activation,
/// definite non-activation, and uncertain completion cannot diverge by adapter.
#[must_use]
pub fn classify_grpc_deployment_completion(
    result: &ServiceResult<DeployContractResult>,
) -> GrpcDeploymentCompletion {
    match result {
        Ok(DeployContractResult::Activated(_)) => GrpcDeploymentCompletion::Activated,
        Ok(DeployContractResult::AlreadyActive(_)) => GrpcDeploymentCompletion::AlreadyActive,
        Err(failure)
            if failure
                .public_error()
                .is_some_and(|error| error.kind() == PublicErrorKind::OutcomeUnknown) =>
        {
            GrpcDeploymentCompletion::OutcomeUnknown
        }
        Ok(DeployContractResult::ExpectedActiveVersionMismatch { .. })
        | Ok(DeployContractResult::BundleConflict)
        | Err(_) => GrpcDeploymentCompletion::NotActivated,
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

fn service_not_ready() -> Status {
    Status::unavailable("service is not ready")
}

fn map_service<T>(result: ServiceResult<T>) -> Result<T, Status> {
    result.map_err(|failure| status_from_service_failure(&failure))
}

fn map_application_service<T>(
    result: ServiceResult<T>,
    context: &ApplicationErrorContextBuilder,
) -> Result<T, Status> {
    result.map_err(|failure| status_from_application_failure(&failure, context))
}

fn application_context(
    operation: ApplicationOperation,
    request_id: RequestId,
    contract: Option<&app_v1::ContractSelector>,
    operation_symbol: Option<&str>,
) -> ApplicationErrorContextBuilder {
    let mut context = ApplicationErrorContextBuilder::new(operation, request_id);
    if let Some(contract) = contract
        && let (Ok(lineage), Some(version)) = (
            ContractLineage::new(contract.lineage.clone()),
            ContractVersion::new(contract.version),
        )
    {
        context = context.with_contract(lineage, version);
    }
    if let Some(symbol) = operation_symbol {
        context = context.with_operation_symbol(symbol.to_owned());
    }
    context
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
        let (service, context, _cancellation) =
            self.normal_invocation(ServiceOperationV1::ValidateContract, &metadata, request_id)?;
        let result = map_service(service.validate_contract(context, request).await)?;
        Ok(Response::new(contract_validation_result_to_proto(&result)?))
    }

    async fn explain_command(
        &self,
        request: Request<v1::ExplainCommandRequest>,
    ) -> Result<Response<v1::ExplainCommandResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = explain_command_request_from_proto(message)?;
        let (service, context, _cancellation) =
            self.normal_invocation(ServiceOperationV1::ExplainCommand, &metadata, request_id)?;
        let result = map_service(service.explain_command(context, request).await)?;
        Ok(Response::new(explain_command_result_to_proto(&result)?))
    }

    async fn deploy_contract(
        &self,
        request: Request<v1::DeployContractRequest>,
    ) -> Result<Response<v1::DeployContractResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = deploy_contract_request_from_proto(message)?;
        let (service, context, _cancellation) =
            self.normal_invocation(ServiceOperationV1::DeployContract, &metadata, request_id)?;
        let lifecycle = DeploymentLifecycleGuard::new(self.lifecycle.as_ref());
        let result = service.deploy_contract(context, request).await;
        let completion = classify_grpc_deployment_completion(&result);
        lifecycle.complete(completion);
        let result = map_service(result)?;
        Ok(Response::new(deploy_contract_result_to_proto(&result)))
    }

    async fn get_active_contract(
        &self,
        request: Request<v1::GetActiveContractRequest>,
    ) -> Result<Response<v1::GetActiveContractResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = get_active_contract_request_from_proto(message)?;
        let (service, context, _cancellation) =
            self.normal_invocation(ServiceOperationV1::GetActiveContract, &metadata, request_id)?;
        let result = map_service(service.get_active_contract(context, request).await)?;
        Ok(Response::new(get_active_contract_result_to_proto(&result)))
    }

    async fn get_contract_version(
        &self,
        request: Request<v1::GetContractVersionRequest>,
    ) -> Result<Response<v1::GetContractVersionResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = get_contract_version_request_from_proto(message)?;
        let (service, context, _cancellation) = self.normal_invocation(
            ServiceOperationV1::GetContractVersion,
            &metadata,
            request_id,
        )?;
        let result = map_service(service.get_contract_version(context, request).await)?;
        Ok(Response::new(get_contract_version_result_to_proto(&result)))
    }

    async fn discover_command_tools(
        &self,
        request: Request<v1::DiscoverCommandToolsRequest>,
    ) -> Result<Response<v1::DiscoverCommandToolsResponse>, Status> {
        let generation = self
            .lifecycle
            .server_generation()
            .ok_or_else(service_not_ready)?;
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = discover_command_tools_request_from_proto(message, generation)?;
        let (service, context, _cancellation) = self.normal_invocation(
            ServiceOperationV1::DiscoverCommandTools,
            &metadata,
            request_id,
        )?;
        let result = map_service(service.discover_command_tools(context, request).await)?;
        Ok(Response::new(discover_command_tools_result_to_proto(
            &result, generation,
        )?))
    }

    async fn discover_resources(
        &self,
        request: Request<v1::DiscoverResourcesRequest>,
    ) -> Result<Response<v1::DiscoverResourcesResponse>, Status> {
        let generation = self
            .lifecycle
            .server_generation()
            .ok_or_else(service_not_ready)?;
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = discover_resources_request_from_proto(message, generation)?;
        let (service, context, _cancellation) =
            self.normal_invocation(ServiceOperationV1::DiscoverResources, &metadata, request_id)?;
        let result = map_service(service.discover_resources(context, request).await)?;
        Ok(Response::new(discover_resources_result_to_proto(
            &result, generation,
        )?))
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
        let (service, context, _cancellation) =
            self.normal_invocation(ServiceOperationV1::ExecuteCommand, &metadata, request_id)?;
        let result = map_service(service.execute_command(context, request).await)?;
        Ok(Response::new(execute_command_result_to_proto(&result)?))
    }

    async fn get_outcome(
        &self,
        request: Request<v1::GetOutcomeRequest>,
    ) -> Result<Response<v1::GetOutcomeResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = resolve_outcome_request_from_proto(message)?;
        let (service, context, _cancellation) = self.normal_invocation(
            ServiceOperationV1::ResolveCommandOutcome,
            &metadata,
            request_id,
        )?;
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
        let (service, context, _cancellation) =
            self.normal_invocation(ServiceOperationV1::GetEntity, &metadata, request_id)?;
        let result = map_service(service.get_entity(context, request).await)?;
        Ok(Response::new(get_entity_result_to_proto(&result)?))
    }

    async fn scan_index(
        &self,
        request: Request<v1::ScanIndexRequest>,
    ) -> Result<Response<v1::ScanIndexResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = scan_index_request_from_proto(message)?;
        let (service, context, _cancellation) =
            self.normal_invocation(ServiceOperationV1::ScanIndex, &metadata, request_id)?;
        let result = map_service(service.scan_index(context, request).await)?;
        Ok(Response::new(scan_index_result_to_proto(&result)?))
    }

    async fn query_projection(
        &self,
        request: Request<v1::QueryProjectionRequest>,
    ) -> Result<Response<v1::QueryProjectionResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = query_projection_request_from_proto(message)?;
        let (service, context, _cancellation) =
            self.normal_invocation(ServiceOperationV1::QueryProjection, &metadata, request_id)?;
        let result = map_service(service.query_projection(context, request).await)?;
        Ok(Response::new(query_projection_result_to_proto(&result)?))
    }

    async fn get_projection_status(
        &self,
        request: Request<v1::GetProjectionStatusRequest>,
    ) -> Result<Response<v1::GetProjectionStatusResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = get_projection_status_request_from_proto(message)?;
        let (service, context, _cancellation) = self.normal_invocation(
            ServiceOperationV1::GetProjectionStatus,
            &metadata,
            request_id,
        )?;
        let result = map_service(service.get_projection_status(context, request).await)?;
        Ok(Response::new(get_projection_status_result_to_proto(
            &result,
        )))
    }
}

#[tonic::async_trait]
impl ApplicationQueryService for GrpcApplication {
    async fn describe_contract(
        &self,
        request: Request<app_v1::DescribeContractRequest>,
    ) -> Result<Response<app_v1::DescribeContractResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let boundary =
            ApplicationErrorContextBuilder::without_trace(ApplicationOperation::DescribeContract);
        let original = message.clone();
        let (request_id, selector) = describe_symbolic_contract_request_from_proto(message)
            .map_err(|status| status_from_application_boundary(status, &boundary))?;
        let application = application_context(
            ApplicationOperation::DescribeContract,
            request_id,
            original.contract.as_ref(),
            None,
        );
        let (service, context, _cancellation) = self
            .normal_invocation(ServiceOperationV1::DescribeContract, &metadata, request_id)
            .map_err(|status| status_from_application_boundary(status, &application))?;
        let result = map_application_service(
            service.describe_symbolic_contract(context, selector).await,
            &application,
        )?;
        Ok(Response::new(describe_symbolic_contract_result_to_proto(
            &result,
        )))
    }

    async fn check_query(
        &self,
        request: Request<app_v1::CheckQueryRequest>,
    ) -> Result<Response<app_v1::CheckQueryResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let boundary =
            ApplicationErrorContextBuilder::without_trace(ApplicationOperation::CheckQuery);
        let original = message.clone();
        let (request_id, request) = check_symbolic_query_request_from_proto(message)
            .map_err(|status| status_from_application_boundary(status, &boundary))?;
        let application = application_context(
            ApplicationOperation::CheckQuery,
            request_id,
            original.contract.as_ref(),
            None,
        );
        let (service, context, _cancellation) = self
            .normal_invocation(ServiceOperationV1::CheckQuery, &metadata, request_id)
            .map_err(|status| status_from_application_boundary(status, &application))?;
        let result = map_application_service(
            service.check_symbolic_query(context, request).await,
            &application,
        )?;
        Ok(Response::new(check_symbolic_query_result_to_proto(
            &result,
        )?))
    }

    async fn explain_query(
        &self,
        request: Request<app_v1::ExplainQueryRequest>,
    ) -> Result<Response<app_v1::ExplainQueryResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let boundary =
            ApplicationErrorContextBuilder::without_trace(ApplicationOperation::ExplainQuery);
        let original = message.clone();
        let operation_symbol = match original.query.as_ref() {
            Some(app_v1::explain_query_request::Query::QueryName(name)) => Some(name.as_str()),
            Some(app_v1::explain_query_request::Query::Source(_)) | None => None,
        };
        let (request_id, request) = explain_symbolic_query_request_from_proto(message)
            .map_err(|status| status_from_application_boundary(status, &boundary))?;
        let application = application_context(
            ApplicationOperation::ExplainQuery,
            request_id,
            original.contract.as_ref(),
            operation_symbol,
        );
        let (service, context, _cancellation) = self
            .normal_invocation(ServiceOperationV1::ExplainQuery, &metadata, request_id)
            .map_err(|status| status_from_application_boundary(status, &application))?;
        let result = match request {
            ExplainSymbolicQueryInvocation::AdHoc(request) => map_application_service(
                service.explain_symbolic_query(context, request).await,
                &application,
            )?,
            ExplainSymbolicQueryInvocation::Named(request) => map_application_service(
                service.explain_named_symbolic_query(context, request).await,
                &application,
            )?,
        };
        Ok(Response::new(explain_symbolic_query_result_to_proto(
            &result,
        )?))
    }

    async fn execute_query(
        &self,
        request: Request<app_v1::ExecuteQueryRequest>,
    ) -> Result<Response<app_v1::ExecuteQueryResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let boundary =
            ApplicationErrorContextBuilder::without_trace(ApplicationOperation::ExecuteQuery);
        let original = message.clone();
        let operation_symbol = match original.query.as_ref() {
            Some(app_v1::execute_query_request::Query::QueryName(name)) => Some(name.as_str()),
            Some(app_v1::execute_query_request::Query::Source(_)) | None => None,
        };
        let (request_id, request) = execute_symbolic_query_request_from_proto(message)
            .map_err(|status| status_from_application_boundary(status, &boundary))?;
        let application = application_context(
            ApplicationOperation::ExecuteQuery,
            request_id,
            original.contract.as_ref(),
            operation_symbol,
        );
        let (service, context, _cancellation) = self
            .normal_invocation(ServiceOperationV1::ExecuteQuery, &metadata, request_id)
            .map_err(|status| status_from_application_boundary(status, &application))?;
        let result = match request {
            ExecuteSymbolicQueryInvocation::AdHoc(request) => map_application_service(
                service.execute_symbolic_query(context, request).await,
                &application,
            )?,
            ExecuteSymbolicQueryInvocation::Named(request) => map_application_service(
                service.execute_named_symbolic_query(context, request).await,
                &application,
            )?,
        };
        Ok(Response::new(execute_symbolic_query_result_to_proto(
            &result,
        )?))
    }

    async fn deploy_query_module(
        &self,
        request: Request<app_v1::DeployQueryModuleRequest>,
    ) -> Result<Response<app_v1::DeployQueryModuleResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let boundary =
            ApplicationErrorContextBuilder::without_trace(ApplicationOperation::DeployQueryModule);
        let original = message.clone();
        let (request_id, request) = deploy_query_module_request_from_proto(message)
            .map_err(|status| status_from_application_boundary(status, &boundary))?;
        let application = application_context(
            ApplicationOperation::DeployQueryModule,
            request_id,
            original.contract.as_ref(),
            Some(original.module_name.as_str()),
        );
        let (service, context, _cancellation) = self
            .normal_invocation(ServiceOperationV1::DeployQueryModule, &metadata, request_id)
            .map_err(|status| status_from_application_boundary(status, &application))?;
        let result = map_application_service(
            service.deploy_query_module(context, request).await,
            &application,
        )?;
        Ok(Response::new(deploy_query_module_result_to_proto(&result)))
    }

    async fn get_query_module(
        &self,
        request: Request<app_v1::GetQueryModuleRequest>,
    ) -> Result<Response<app_v1::GetQueryModuleResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let boundary =
            ApplicationErrorContextBuilder::without_trace(ApplicationOperation::GetQueryModule);
        let original = message.clone();
        let (request_id, request) = get_query_module_request_from_proto(message)
            .map_err(|status| status_from_application_boundary(status, &boundary))?;
        let application = application_context(
            ApplicationOperation::GetQueryModule,
            request_id,
            original.contract.as_ref(),
            None,
        );
        let (service, context, _cancellation) = self
            .normal_invocation(ServiceOperationV1::ExplainQuery, &metadata, request_id)
            .map_err(|status| status_from_application_boundary(status, &application))?;
        let result = map_application_service(
            service.get_query_module(context, request).await,
            &application,
        )?;
        Ok(Response::new(get_query_module_result_to_proto(
            result.as_ref(),
        )))
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
        let (service, context, _cancellation) =
            self.normal_invocation(ServiceOperationV1::GetCommit, &metadata, request_id)?;
        let result = map_service(service.get_commit(context, request).await)?;
        Ok(Response::new(get_commit_result_to_proto(&result)?))
    }

    async fn scan_commits(
        &self,
        request: Request<v1::ScanCommitsRequest>,
    ) -> Result<Response<v1::ScanCommitsResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = scan_commits_request_from_proto(message)?;
        let (service, context, _cancellation) =
            self.normal_invocation(ServiceOperationV1::ScanCommits, &metadata, request_id)?;
        let result = map_service(service.scan_commits(context, request).await)?;
        Ok(Response::new(scan_commits_result_to_proto(&result)?))
    }

    async fn subscribe_commits(
        &self,
        request: Request<v1::SubscribeCommitsRequest>,
    ) -> Result<Response<Self::SubscribeCommitsStream>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = subscribe_commits_request_from_proto(message)?;
        let (service, context, cancellation) = self.normal_invocation(
            ServiceOperationV1::SubscribeToCommits,
            &metadata,
            request_id,
        )?;
        let result = map_service(service.subscribe_to_commits(context, request).await)?;
        Ok(Response::new(CommitNotificationStream::new(
            result.into_subscription(),
            cancellation,
        )))
    }

    async fn trace_provenance(
        &self,
        request: Request<v1::TraceProvenanceRequest>,
    ) -> Result<Response<v1::TraceProvenanceResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = trace_provenance_request_from_proto(message)?;
        let (service, context, _cancellation) =
            self.normal_invocation(ServiceOperationV1::TraceProvenance, &metadata, request_id)?;
        let result = map_service(service.trace_provenance(context, request).await)?;
        Ok(Response::new(trace_provenance_result_to_proto(&result)?))
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
                let (service, context, _cancellation) =
                    self.normal_invocation(ServiceOperationV1::GetHealth, &metadata, request_id)?;
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
        let (service, context, _cancellation) =
            self.normal_invocation(ServiceOperationV1::GetStatistics, &metadata, request_id)?;
        let result = map_service(service.statistics(context, request).await)?;
        Ok(Response::new(statistics_result_to_proto(result)))
    }

    async fn create_capability(
        &self,
        request: Request<v1::CreateCapabilityRequest>,
    ) -> Result<Response<v1::CreateCapabilityResponse>, Status> {
        let (metadata, peer, message) = split_request(request);
        match v1::CapabilityCreateMode::try_from(message.mode).map_err(|_| invalid_request())? {
            v1::CapabilityCreateMode::Normal => {
                let (service, security) =
                    self.normal_admission(ServiceOperationV1::CreateCapability)?;
                let (request_id, request) =
                    normal_create_capability_request_from_proto(message, &security.authentication)?;
                let (context, _cancellation) =
                    self.normal_context(&metadata, request_id, &security)?;
                let invocation = CreateCapabilityInvocation::Normal { context, request };
                let result = map_service(service.create_capability(invocation).await)?;
                Ok(Response::new(create_capability_result_to_proto(&result)?))
            }
            v1::CapabilityCreateMode::Bootstrap => {
                let security = self.bootstrap_security()?;
                let (request_id, request) =
                    bootstrap_capability_request_from_proto(message, &security.authentication)?;
                let (context, _cancellation) =
                    self.bootstrap_context(&metadata, peer, request_id, &security)?;
                let service = self
                    .lifecycle
                    .begin_bootstrap()
                    .ok_or_else(service_not_ready)?;
                let lifecycle = BootstrapLifecycleGuard::new(self.lifecycle.as_ref());
                let invocation = CreateCapabilityInvocation::Bootstrap { context, request };
                let result = service.create_capability(invocation).await;
                let completion = classify_bootstrap_completion(&result);
                lifecycle.complete(completion);
                let result = map_service(result)?;
                Ok(Response::new(create_capability_result_to_proto(&result)?))
            }
            v1::CapabilityCreateMode::Unspecified => return Err(invalid_request()),
        }
    }

    async fn revoke_capability(
        &self,
        request: Request<v1::RevokeCapabilityRequest>,
    ) -> Result<Response<v1::RevokeCapabilityResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = revoke_capability_request_from_proto(message)?;
        let (service, context, _cancellation) =
            self.normal_invocation(ServiceOperationV1::RevokeCapability, &metadata, request_id)?;
        let result = map_service(service.revoke_capability(context, request).await)?;
        Ok(Response::new(revoke_capability_result_to_proto(result)))
    }

    async fn list_pending_outbox_deliveries(
        &self,
        request: Request<v1::ListPendingOutboxDeliveriesRequest>,
    ) -> Result<Response<v1::ListPendingOutboxDeliveriesResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = list_pending_outbox_deliveries_request_from_proto(message)?;
        let (service, context, _cancellation) = self.normal_invocation(
            ServiceOperationV1::ListPendingOutboxDeliveries,
            &metadata,
            request_id,
        )?;
        let result = map_service(
            service
                .list_pending_outbox_deliveries(context, request)
                .await,
        )?;
        Ok(Response::new(
            list_pending_outbox_deliveries_result_to_proto(&result),
        ))
    }

    async fn create_offline_backup(
        &self,
        request: Request<v1::CreateOfflineBackupRequest>,
    ) -> Result<Response<v1::CreateOfflineBackupResponse>, Status> {
        let (service, security) =
            self.ready_maintenance_admission(GrpcOfflineMaintenanceOperation::CreateBackup)?;
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = create_offline_backup_request_from_proto(message)?;
        let (context, _cancellation) = self.normal_context(&metadata, request_id, &security)?;
        let result = map_service(service.create_offline_backup(context, request).await)?;
        let (disposition, operation) = offline_maintenance_start_result_to_proto(&result);
        Ok(Response::new(v1::CreateOfflineBackupResponse {
            disposition,
            operation,
        }))
    }

    async fn restore_offline_backup(
        &self,
        request: Request<v1::RestoreOfflineBackupRequest>,
    ) -> Result<Response<v1::RestoreOfflineBackupResponse>, Status> {
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = restore_offline_backup_request_from_proto(message)?;
        let operation_id = request.operation_id();
        let input_hash = request.input_hash();
        let ready = self.lifecycle.admit_offline_maintenance(
            GrpcOfflineMaintenanceOperation::RestoreBackup {
                operation_id,
                input_hash,
            },
        );
        let retry = ready
            .is_none()
            .then(|| self.lifecycle.admit_restore_retry(operation_id, input_hash))
            .flatten();
        let recovery = (ready.is_none() && retry.is_none())
            .then(|| {
                self.lifecycle
                    .admit_recovery_restore(operation_id, input_hash)
            })
            .flatten();
        let result = match (ready, retry, recovery) {
            (Some(service), None, None) => {
                let security = self
                    .lifecycle
                    .security_context()
                    .ok_or_else(service_not_ready)?;
                let (context, credential, _cancellation) =
                    self.restore_context(&metadata, request_id, &security)?;
                let invocation = RestoreOfflineBackupInvocation::new(context, request, credential);
                map_service(service.restore_offline_backup(invocation).await)?
            }
            (None, Some(service), None) => {
                let security = self
                    .lifecycle
                    .restore_retry_security_context()
                    .ok_or_else(service_not_ready)?;
                let (context, credential, _cancellation) =
                    self.restore_retry_context(&metadata, request_id, &security)?;
                let invocation = RestoreOfflineBackupInvocation::new(context, request, credential);
                map_service(service.restore_offline_backup(invocation).await)?
            }
            (None, None, Some(service)) => {
                let (invocation, _cancellation) =
                    self.recovery_restore_context(&metadata, request_id, request)?;
                map_service(service.restore_offline_backup(invocation).await)?
            }
            _ => return Err(service_not_ready()),
        };
        let (disposition, operation) = offline_maintenance_start_result_to_proto(&result);
        Ok(Response::new(v1::RestoreOfflineBackupResponse {
            disposition,
            operation,
        }))
    }

    async fn get_offline_maintenance_operation(
        &self,
        request: Request<v1::GetOfflineMaintenanceOperationRequest>,
    ) -> Result<Response<v1::GetOfflineMaintenanceOperationResponse>, Status> {
        let (service, security) =
            self.ready_maintenance_admission(GrpcOfflineMaintenanceOperation::GetOperation)?;
        let (metadata, _peer, message) = split_request(request);
        let (request_id, request) = get_offline_maintenance_operation_request_from_proto(message)?;
        let (context, _cancellation) = self.normal_context(&metadata, request_id, &security)?;
        let result = map_service(
            service
                .get_offline_maintenance_operation(context, request)
                .await,
        )?;
        Ok(Response::new(
            get_offline_maintenance_operation_result_to_proto(&result),
        ))
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
    use riffdb_errors::PublicError;
    use riffdb_service::{CapabilityIdentityView, CapabilityTransitionView};
    use riffdb_types::{
        AdministrationSequence, Audience, BackupNameV1, CapabilityId, DatabaseId, Environment,
        OfflineMaintenanceOperationKind, OfflineMaintenanceReplacementConfirmation, RequestId,
        offline_maintenance_input_hash,
    };
    use std::num::NonZeroU64;
    use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
    use std::sync::{Barrier, Mutex};
    use std::thread;
    use tonic::metadata::MetadataValue;

    struct CountingAuthenticator(AtomicUsize);

    impl CredentialAuthenticator for CountingAuthenticator {
        fn authenticate(
            &self,
            _credential: OpaqueCredential<'_>,
            _context: &AuthenticationContext,
        ) -> Result<AuthenticatedPrincipal, AuthenticationFailure> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(AuthenticationFailure::Unauthenticated)
        }
    }

    struct InitializingRoute {
        security_fetches: AtomicUsize,
        security: Option<CheckedGrpcSecurityContext>,
    }

    impl InitializingRoute {
        fn without_security() -> Self {
            Self {
                security_fetches: AtomicUsize::new(0),
                security: None,
            }
        }

        fn with_security(security: CheckedGrpcSecurityContext) -> Self {
            Self {
                security_fetches: AtomicUsize::new(0),
                security: Some(security),
            }
        }
    }

    impl GrpcLifecycleRoute for InitializingRoute {
        fn admit_authenticated(
            &self,
            _operation: ServiceOperationV1,
        ) -> Option<Arc<dyn ApplicationService>> {
            None
        }

        fn admit_offline_maintenance(
            &self,
            _operation: GrpcOfflineMaintenanceOperation,
        ) -> Option<Arc<dyn ApplicationService>> {
            None
        }

        fn admit_restore_retry(
            &self,
            _operation_id: OfflineMaintenanceOperationId,
            _input_hash: OfflineMaintenanceInputHash,
        ) -> Option<Arc<dyn RestoreRetryOfflineMaintenanceApplication>> {
            None
        }

        fn admit_recovery_restore(
            &self,
            _operation_id: OfflineMaintenanceOperationId,
            _input_hash: OfflineMaintenanceInputHash,
        ) -> Option<Arc<dyn RecoveryOfflineMaintenanceApplication>> {
            None
        }

        fn security_context(&self) -> Option<CheckedGrpcSecurityContext> {
            self.security_fetches.fetch_add(1, Ordering::SeqCst);
            self.security.clone()
        }

        fn restore_retry_security_context(&self) -> Option<CheckedGrpcRestoreRetrySecurityContext> {
            None
        }

        fn server_generation(&self) -> Option<[u8; 16]> {
            None
        }

        fn restricted_health(
            &self,
            _request: HealthRequest,
        ) -> Option<ServiceFuture<'_, HealthResult>> {
            None
        }

        fn bootstrap_available(&self) -> bool {
            false
        }

        fn begin_bootstrap(&self) -> Option<Arc<dyn ApplicationService>> {
            None
        }

        fn finish_bootstrap(&self, _completion: GrpcBootstrapCompletion) {}

        fn finish_deployment(&self, _completion: GrpcDeploymentCompletion) {}
    }

    struct AtomicBootstrapRoute {
        admitted: AtomicBool,
        phase: AtomicU8,
        completions: Mutex<Vec<GrpcBootstrapCompletion>>,
    }

    impl AtomicBootstrapRoute {
        fn new() -> Self {
            Self {
                admitted: AtomicBool::new(false),
                phase: AtomicU8::new(0),
                completions: Mutex::new(Vec::new()),
            }
        }

        fn claim_bootstrap(&self) -> bool {
            let admitted = self
                .admitted
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok();
            if admitted {
                self.phase.store(1, Ordering::Release);
            }
            admitted
        }
    }

    impl GrpcLifecycleRoute for AtomicBootstrapRoute {
        fn admit_authenticated(
            &self,
            _operation: ServiceOperationV1,
        ) -> Option<Arc<dyn ApplicationService>> {
            None
        }

        fn admit_offline_maintenance(
            &self,
            _operation: GrpcOfflineMaintenanceOperation,
        ) -> Option<Arc<dyn ApplicationService>> {
            None
        }

        fn admit_restore_retry(
            &self,
            _operation_id: OfflineMaintenanceOperationId,
            _input_hash: OfflineMaintenanceInputHash,
        ) -> Option<Arc<dyn RestoreRetryOfflineMaintenanceApplication>> {
            None
        }

        fn admit_recovery_restore(
            &self,
            _operation_id: OfflineMaintenanceOperationId,
            _input_hash: OfflineMaintenanceInputHash,
        ) -> Option<Arc<dyn RecoveryOfflineMaintenanceApplication>> {
            None
        }

        fn security_context(&self) -> Option<CheckedGrpcSecurityContext> {
            None
        }

        fn restore_retry_security_context(&self) -> Option<CheckedGrpcRestoreRetrySecurityContext> {
            None
        }

        fn server_generation(&self) -> Option<[u8; 16]> {
            None
        }

        fn restricted_health(
            &self,
            _request: HealthRequest,
        ) -> Option<ServiceFuture<'_, HealthResult>> {
            None
        }

        fn bootstrap_available(&self) -> bool {
            !self.admitted.load(Ordering::Acquire)
        }

        fn begin_bootstrap(&self) -> Option<Arc<dyn ApplicationService>> {
            None
        }

        fn finish_bootstrap(&self, completion: GrpcBootstrapCompletion) {
            self.completions
                .lock()
                .expect("completion lock")
                .push(completion);
            let phase = match completion {
                GrpcBootstrapCompletion::Created
                | GrpcBootstrapCompletion::Replayed
                | GrpcBootstrapCompletion::Conflict => 2,
                GrpcBootstrapCompletion::OutcomeUnknown
                | GrpcBootstrapCompletion::Failed
                | GrpcBootstrapCompletion::Abandoned => 4,
            };
            self.phase.store(phase, Ordering::Release);
        }

        fn finish_deployment(&self, _completion: GrpcDeploymentCompletion) {}
    }

    struct RejectingMaintenanceRoute {
        ready_admissions: Mutex<Vec<GrpcOfflineMaintenanceOperation>>,
        retry_admissions: Mutex<Vec<(OfflineMaintenanceOperationId, OfflineMaintenanceInputHash)>>,
        recovery_admissions:
            Mutex<Vec<(OfflineMaintenanceOperationId, OfflineMaintenanceInputHash)>>,
        security_fetches: AtomicUsize,
        security: CheckedGrpcSecurityContext,
        retry_security: CheckedGrpcRestoreRetrySecurityContext,
    }

    impl RejectingMaintenanceRoute {
        fn new(security: CheckedGrpcSecurityContext) -> Self {
            let retry_security = CheckedGrpcRestoreRetrySecurityContext::new(
                Arc::clone(&security.authenticator),
                security.authentication.clone(),
            );
            Self {
                ready_admissions: Mutex::new(Vec::new()),
                retry_admissions: Mutex::new(Vec::new()),
                recovery_admissions: Mutex::new(Vec::new()),
                security_fetches: AtomicUsize::new(0),
                security,
                retry_security,
            }
        }
    }

    impl GrpcLifecycleRoute for RejectingMaintenanceRoute {
        fn admit_authenticated(
            &self,
            _operation: ServiceOperationV1,
        ) -> Option<Arc<dyn ApplicationService>> {
            None
        }

        fn admit_restore_retry(
            &self,
            operation_id: OfflineMaintenanceOperationId,
            input_hash: OfflineMaintenanceInputHash,
        ) -> Option<Arc<dyn RestoreRetryOfflineMaintenanceApplication>> {
            self.retry_admissions
                .lock()
                .expect("retry admission recorder")
                .push((operation_id, input_hash));
            None
        }

        fn admit_offline_maintenance(
            &self,
            operation: GrpcOfflineMaintenanceOperation,
        ) -> Option<Arc<dyn ApplicationService>> {
            self.ready_admissions
                .lock()
                .expect("ready admission recorder")
                .push(operation);
            None
        }

        fn admit_recovery_restore(
            &self,
            operation_id: OfflineMaintenanceOperationId,
            input_hash: OfflineMaintenanceInputHash,
        ) -> Option<Arc<dyn RecoveryOfflineMaintenanceApplication>> {
            self.recovery_admissions
                .lock()
                .expect("recovery admission recorder")
                .push((operation_id, input_hash));
            None
        }

        fn security_context(&self) -> Option<CheckedGrpcSecurityContext> {
            self.security_fetches.fetch_add(1, Ordering::SeqCst);
            Some(self.security.clone())
        }

        fn restore_retry_security_context(&self) -> Option<CheckedGrpcRestoreRetrySecurityContext> {
            self.security_fetches.fetch_add(1, Ordering::SeqCst);
            Some(self.retry_security.clone())
        }

        fn server_generation(&self) -> Option<[u8; 16]> {
            None
        }

        fn restricted_health(
            &self,
            _request: HealthRequest,
        ) -> Option<ServiceFuture<'_, HealthResult>> {
            None
        }

        fn bootstrap_available(&self) -> bool {
            false
        }

        fn begin_bootstrap(&self) -> Option<Arc<dyn ApplicationService>> {
            None
        }

        fn finish_bootstrap(&self, _completion: GrpcBootstrapCompletion) {}

        fn finish_deployment(&self, _completion: GrpcDeploymentCompletion) {}
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
        let route = Arc::new(InitializingRoute::without_security());
        let adapter = GrpcApplication::new(
            route.clone(),
            GrpcRequestLimits::new(Duration::from_secs(30)).expect("valid request limit"),
        );
        let status = match adapter
            .lifecycle
            .admit_authenticated(ServiceOperationV1::GetEntity)
        {
            Some(_) => panic!("initializing route must not expose a full service"),
            None => service_not_ready(),
        };
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert!(status.details().is_empty());
        assert_eq!(route.security_fetches.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn initializing_route_rejects_before_fetching_security_or_authenticating() {
        let authenticator = Arc::new(CountingAuthenticator(AtomicUsize::new(0)));
        let security = CheckedGrpcSecurityContext::new(
            authenticator.clone(),
            test_authentication_context(),
            Arc::new(test_bootstrap_keys()),
        );
        let route = Arc::new(InitializingRoute::with_security(security));
        let adapter = GrpcApplication::new(
            route.clone(),
            GrpcRequestLimits::new(Duration::from_secs(30)).expect("valid request limit"),
        );
        let mut metadata = MetadataMap::new();
        metadata.insert(
            AUTHORIZATION_METADATA_KEY,
            MetadataValue::try_from(format!("Bearer {}", "A".repeat(43)))
                .expect("bounded bearer metadata"),
        );

        let status =
            match adapter.normal_invocation(ServiceOperationV1::GetEntity, &metadata, request_id())
            {
                Ok(_) => panic!("initializing route must reject normal invocation"),
                Err(status) => status,
            };

        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(route.security_fetches.load(Ordering::SeqCst), 0);
        assert_eq!(authenticator.0.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn restore_decodes_before_all_three_exact_admission_routes() {
        let authenticator = Arc::new(CountingAuthenticator(AtomicUsize::new(0)));
        let security = CheckedGrpcSecurityContext::new(
            authenticator.clone(),
            test_authentication_context(),
            Arc::new(test_bootstrap_keys()),
        );
        let route = Arc::new(RejectingMaintenanceRoute::new(security));
        let adapter = GrpcApplication::new(
            route.clone(),
            GrpcRequestLimits::new(Duration::from_secs(30)).expect("valid request limit"),
        );
        let operation_id =
            OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(3, [9; 10])
                .expect("valid maintenance operation ID");
        let input_hash = offline_maintenance_input_hash(
            OfflineMaintenanceOperationKind::RestoreBackup,
            &BackupNameV1::new("nightly").expect("valid backup name"),
            OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget,
        );
        let mut request = Request::new(v1::RestoreOfflineBackupRequest {
            request_id: request_id().into_bytes().to_vec(),
            operation_id: operation_id.into_bytes().to_vec(),
            backup_name: "nightly".to_owned(),
            replacement_confirmation:
                v1::OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget as i32,
        });
        request.metadata_mut().insert(
            AUTHORIZATION_METADATA_KEY,
            MetadataValue::try_from(format!("Bearer {}", "A".repeat(43)))
                .expect("bounded bearer metadata"),
        );

        let status = AdminService::restore_offline_backup(&adapter, request)
            .await
            .expect_err("closed exact routes reject the restore");

        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(
            route
                .ready_admissions
                .lock()
                .expect("ready admission recorder")
                .as_slice(),
            &[GrpcOfflineMaintenanceOperation::RestoreBackup {
                operation_id,
                input_hash,
            }]
        );
        assert_eq!(
            route
                .retry_admissions
                .lock()
                .expect("retry admission recorder")
                .as_slice(),
            &[(operation_id, input_hash)]
        );
        assert_eq!(
            route
                .recovery_admissions
                .lock()
                .expect("recovery admission recorder")
                .as_slice(),
            &[(operation_id, input_hash)]
        );
        assert_eq!(route.security_fetches.load(Ordering::SeqCst), 0);
        assert_eq!(authenticator.0.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn malformed_restore_input_reaches_neither_admission_nor_authentication() {
        let authenticator = Arc::new(CountingAuthenticator(AtomicUsize::new(0)));
        let security = CheckedGrpcSecurityContext::new(
            authenticator.clone(),
            test_authentication_context(),
            Arc::new(test_bootstrap_keys()),
        );
        let route = Arc::new(RejectingMaintenanceRoute::new(security));
        let adapter = GrpcApplication::new(
            route.clone(),
            GrpcRequestLimits::new(Duration::from_secs(30)).expect("valid request limit"),
        );
        let operation_id =
            OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(3, [10; 10])
                .expect("valid maintenance operation ID");
        let mut request = Request::new(v1::RestoreOfflineBackupRequest {
            request_id: request_id().into_bytes().to_vec(),
            operation_id: operation_id.into_bytes().to_vec(),
            backup_name: "../nightly".to_owned(),
            replacement_confirmation:
                v1::OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget as i32,
        });
        request.metadata_mut().insert(
            AUTHORIZATION_METADATA_KEY,
            MetadataValue::try_from(format!("Bearer {}", "A".repeat(43)))
                .expect("bounded bearer metadata"),
        );

        let status = AdminService::restore_offline_backup(&adapter, request)
            .await
            .expect_err("malformed input is rejected structurally");

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(
            route
                .ready_admissions
                .lock()
                .expect("ready admission recorder")
                .is_empty()
        );
        assert!(
            route
                .retry_admissions
                .lock()
                .expect("retry admission recorder")
                .is_empty()
        );
        assert!(
            route
                .recovery_admissions
                .lock()
                .expect("recovery admission recorder")
                .is_empty()
        );
        assert_eq!(route.security_fetches.load(Ordering::SeqCst), 0);
        assert_eq!(authenticator.0.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn unavailable_bootstrap_rejects_before_fetching_security() {
        let authenticator = Arc::new(CountingAuthenticator(AtomicUsize::new(0)));
        let security = CheckedGrpcSecurityContext::new(
            authenticator.clone(),
            test_authentication_context(),
            Arc::new(test_bootstrap_keys()),
        );
        let route = Arc::new(InitializingRoute::with_security(security));
        let adapter = GrpcApplication::new(
            route.clone(),
            GrpcRequestLimits::new(Duration::from_secs(30)).expect("valid request limit"),
        );

        let status = adapter
            .bootstrap_security()
            .expect_err("initializing bootstrap must not fetch security");

        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(route.security_fetches.load(Ordering::SeqCst), 0);
        assert_eq!(authenticator.0.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn bootstrap_begin_is_atomic_under_a_barrier_race() {
        const CALLERS: usize = 8;
        let route = Arc::new(AtomicBootstrapRoute::new());
        let barrier = Arc::new(Barrier::new(CALLERS + 1));
        let mut callers = Vec::with_capacity(CALLERS);
        for _ in 0..CALLERS {
            let route = Arc::clone(&route);
            let barrier = Arc::clone(&barrier);
            callers.push(thread::spawn(move || {
                barrier.wait();
                route.claim_bootstrap()
            }));
        }

        barrier.wait();
        let admitted = callers
            .into_iter()
            .map(|caller| caller.join().expect("bootstrap caller completed"))
            .filter(|admitted| *admitted)
            .count();

        assert_eq!(admitted, 1);
        assert_eq!(route.phase.load(Ordering::Acquire), 1);
    }

    #[test]
    fn bootstrap_results_have_closed_lifecycle_classifications() {
        let transition = capability_transition();
        let cases = [
            (
                Ok(CreateCapabilityResult::Bootstrap(
                    BootstrapCapabilityResult::Created(transition),
                )),
                GrpcBootstrapCompletion::Created,
            ),
            (
                Ok(CreateCapabilityResult::Bootstrap(
                    BootstrapCapabilityResult::Replayed(transition),
                )),
                GrpcBootstrapCompletion::Replayed,
            ),
            (
                Ok(CreateCapabilityResult::Bootstrap(
                    BootstrapCapabilityResult::BootstrapConflict,
                )),
                GrpcBootstrapCompletion::Conflict,
            ),
            (
                Err(PublicError::outcome_unknown().into()),
                GrpcBootstrapCompletion::OutcomeUnknown,
            ),
            (
                Err(PublicError::storage_unavailable().into()),
                GrpcBootstrapCompletion::Failed,
            ),
        ];

        for (result, expected) in cases {
            assert_eq!(classify_bootstrap_completion(&result), expected);
        }
    }

    #[test]
    fn bootstrap_completion_and_abandonment_are_fail_closed() {
        let success = AtomicBootstrapRoute::new();
        assert!(success.claim_bootstrap());
        BootstrapLifecycleGuard::new(&success).complete(GrpcBootstrapCompletion::Created);
        assert_eq!(success.phase.load(Ordering::Acquire), 2);

        let uncertain = AtomicBootstrapRoute::new();
        assert!(uncertain.claim_bootstrap());
        BootstrapLifecycleGuard::new(&uncertain).complete(GrpcBootstrapCompletion::OutcomeUnknown);
        assert_eq!(uncertain.phase.load(Ordering::Acquire), 4);

        let abandoned = AtomicBootstrapRoute::new();
        assert!(abandoned.claim_bootstrap());
        drop(BootstrapLifecycleGuard::new(&abandoned));
        assert_eq!(abandoned.phase.load(Ordering::Acquire), 4);
        assert_eq!(
            abandoned
                .completions
                .lock()
                .expect("completion lock")
                .as_slice(),
            &[GrpcBootstrapCompletion::Abandoned]
        );
    }

    fn test_authentication_context() -> AuthenticationContext {
        AuthenticationContext::new(
            DatabaseId::from_unix_milliseconds_and_random(1, [0; 10]).expect("valid database ID"),
            Environment::new("test").expect("valid environment"),
            Audience::new("grpc").expect("valid audience"),
        )
    }

    fn test_bootstrap_keys() -> CapabilityDigestKeyProvider {
        CapabilityDigestKeyProvider::parse_document(
            b"riffdb-capability-digest-keys-v1\n7:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n",
        )
        .expect("valid capability digest-key document")
    }

    fn request_id() -> RequestId {
        RequestId::from_unix_milliseconds_and_random(2, [1; 10]).expect("valid request ID")
    }

    fn capability_transition() -> CapabilityTransitionView {
        CapabilityTransitionView::new(
            CapabilityIdentityView::new(
                CapabilityId::from_unix_milliseconds_and_random(3, [2; 10])
                    .expect("valid capability ID"),
                NonZeroU64::MIN,
            ),
            AdministrationSequence::first(),
        )
    }
}
