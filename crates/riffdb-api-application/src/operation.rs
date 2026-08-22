//! API-neutral admission, authentication, invocation, and response release.

use std::sync::Arc;
use std::time::{Duration, Instant};

use riffdb_auth::{
    AuthenticationContext, AuthenticationFailure, CredentialAuthenticator, OpaqueCredential,
};
use riffdb_errors::{
    ApplicationError, ApplicationErrorCode, ApplicationErrorContext, ApplicationOperation,
};
use riffdb_proto::{app::v1 as app_v1, v1};
use riffdb_service::{
    ApplicationErrorContextBuilder, ApplicationService, ReadPipelineStage, RequestContext,
    RequestControl, ServiceFailure, ServiceTelemetry, ServiceTelemetryEvent, WriteServiceStage,
};
use riffdb_types::{ContractLineage, ContractVersion, ServiceOperationV1};

use crate::conversion::{
    ConversionError, ExecuteSymbolicQueryInvocation,
    application_session_catalog_request_from_proto, execute_command_request_from_proto,
    execute_command_result_to_proto, execute_symbolic_query_request_from_proto,
    execute_symbolic_query_result_to_proto,
};

/// Version of the closed framed/generated application-session identity.
pub const APPLICATION_SESSION_PROTOCOL_V1: u32 = 1;

/// Cloneable current security material returned only after lifecycle admission.
#[derive(Clone)]
pub struct ApplicationOperationSecurity {
    authenticator: Arc<dyn CredentialAuthenticator>,
    authentication: AuthenticationContext,
}

impl ApplicationOperationSecurity {
    /// Packages startup-validated authentication dependencies.
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

    fn authenticate(
        &self,
        presentation: OpaqueCredential<'_>,
    ) -> Result<riffdb_auth::AuthenticatedPrincipal, AuthenticationFailure> {
        self.authenticator
            .authenticate(presentation, &self.authentication)
    }
}

impl std::fmt::Debug for ApplicationOperationSecurity {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ApplicationOperationSecurity([CAPABILITY])")
    }
}

/// Current lifecycle route required by the generated-operation adapter.
///
/// The route grants no storage or policy authority. It returns only the same
/// API-neutral service and authentication boundary used by every transport.
pub trait ApplicationOperationRoute: Send + Sync {
    /// Atomically admits one exact operation in the current lifecycle.
    fn admit_authenticated(
        &self,
        operation: ServiceOperationV1,
    ) -> Option<Arc<dyn ApplicationService>>;

    /// Returns current authentication only for an admitted operation.
    fn security_context(&self) -> Option<ApplicationOperationSecurity>;

    /// Returns the current durable history incarnation.
    fn history_incarnation(&self) -> Option<u64>;

    /// Returns fixed-cardinality stage telemetry when configured.
    fn read_stage_telemetry(&self) -> Option<Arc<dyn ServiceTelemetry>> {
        None
    }
}

/// Borrowed transport presentation for one independently authorized operation.
pub struct ApplicationOperationPresentation<'a> {
    credential: Result<OpaqueCredential<'a>, ApplicationPresentationError>,
    deadline: Result<Instant, ApplicationPresentationError>,
}

impl<'a> ApplicationOperationPresentation<'a> {
    /// Binds one opaque credential presentation and finite process-local deadline.
    #[must_use]
    pub const fn new(credential: &'a [u8], deadline: Instant) -> Self {
        Self {
            credential: Ok(OpaqueCredential::new(credential)),
            deadline: Ok(deadline),
        }
    }

    /// Retains transport framing failures until the operation message has
    /// passed structural conversion.
    #[must_use]
    pub const fn from_checked_parts(
        credential: Result<&'a [u8], ApplicationPresentationError>,
        deadline: Result<Instant, ApplicationPresentationError>,
    ) -> Self {
        Self {
            credential: match credential {
                Ok(credential) => Ok(OpaqueCredential::new(credential)),
                Err(error) => Err(error),
            },
            deadline,
        }
    }

    /// Binds one auth-owned retained presentation without exposing its bytes.
    #[must_use]
    pub const fn from_opaque(credential: OpaqueCredential<'a>, deadline: Instant) -> Self {
        Self {
            credential: Ok(credential),
            deadline: Ok(deadline),
        }
    }

    /// Retains a checked auth-owned presentation failure until after message
    /// conversion, without exposing credential bytes to the caller.
    #[must_use]
    pub const fn from_checked_opaque_parts(
        credential: Result<OpaqueCredential<'a>, ApplicationPresentationError>,
        deadline: Result<Instant, ApplicationPresentationError>,
    ) -> Self {
        Self {
            credential,
            deadline,
        }
    }

    fn validate(
        &self,
        application: Option<&ApplicationErrorContextBuilder>,
    ) -> Result<(), ApplicationOperationFailure> {
        if self.credential.is_err() {
            return Err(ApplicationOperationFailure::Unauthenticated(
                application.cloned(),
            ));
        }
        if self.deadline.is_err() {
            return Err(ApplicationOperationFailure::InvalidRequest(
                application.cloned(),
            ));
        }
        Ok(())
    }
}

/// Closed transport-presentation defect deferred until after conversion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationPresentationError {
    /// Missing, duplicate, malformed, or wrong-sized credential carriage.
    InvalidCredential,
    /// Malformed, zero, or overflowing deadline carriage.
    InvalidDeadline,
}

impl std::fmt::Debug for ApplicationOperationPresentation<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ApplicationOperationPresentation([REDACTED])")
    }
}

/// Closed failure before a transport maps it to gRPC status or framed close.
pub enum ApplicationOperationFailure {
    /// Structurally invalid public operation, with application context for queries.
    InvalidRequest(Option<ApplicationErrorContextBuilder>),
    /// Missing, malformed, expired, or otherwise unauthenticated presentation.
    Unauthenticated(Option<ApplicationErrorContextBuilder>),
    /// Reciprocally matched revoked capability.
    CapabilityRevoked(Option<ApplicationErrorContextBuilder>),
    /// Current lifecycle, database, or security context is unavailable.
    Unavailable(Option<ApplicationErrorContextBuilder>),
    /// Failure returned by the shared application service.
    Service {
        /// Closed service failure.
        failure: ServiceFailure,
        /// Application context for named reads; commands retain public-error carriage.
        application: Option<ApplicationErrorContextBuilder>,
    },
    /// Exact application-identity failure constructed by the shared adapter.
    Application(ApplicationError),
    /// Service output could not be represented by the strict public response.
    InvalidResponse,
}

impl ApplicationOperationFailure {
    /// Builds the symbolic application error when this is a named-query failure.
    #[must_use]
    pub fn application_error(&self) -> Option<ApplicationError> {
        match self {
            Self::Application(error) => Some(error.clone()),
            Self::InvalidRequest(Some(context)) => Some(context.invalid_request()),
            Self::Unauthenticated(Some(context)) => Some(context.authorization_denied()),
            Self::CapabilityRevoked(Some(context)) => Some(context.capability_revoked()),
            Self::Unavailable(Some(context)) => Some(context.unavailable()),
            Self::Service {
                failure,
                application: Some(context),
            } => context.build(failure),
            Self::InvalidRequest(None)
            | Self::Unauthenticated(None)
            | Self::CapabilityRevoked(None)
            | Self::Unavailable(None)
            | Self::Service {
                application: None, ..
            }
            | Self::InvalidResponse => None,
        }
    }
}

impl std::fmt::Debug for ApplicationOperationFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InvalidRequest(_) => "ApplicationOperationFailure::InvalidRequest",
            Self::Unauthenticated(_) => "ApplicationOperationFailure::Unauthenticated",
            Self::CapabilityRevoked(_) => "ApplicationOperationFailure::CapabilityRevoked",
            Self::Unavailable(_) => "ApplicationOperationFailure::Unavailable",
            Self::Service { .. } => "ApplicationOperationFailure::Service([REDACTED])",
            Self::Application(_) => "ApplicationOperationFailure::Application([REDACTED])",
            Self::InvalidResponse => "ApplicationOperationFailure::InvalidResponse",
        })
    }
}

/// Authenticates and proves one exact application-session identity without
/// caching its allow decision for any later operation.
pub async fn open_application_session(
    route: &dyn ApplicationOperationRoute,
    presentation: ApplicationOperationPresentation<'_>,
    open: v1::ApplicationSessionOpen,
) -> Result<v1::ApplicationSessionOpened, ApplicationOperationFailure> {
    let selected_contract = open
        .contract
        .clone()
        .ok_or(ApplicationOperationFailure::InvalidRequest(None))?;
    let (request_id, request) = application_session_catalog_request_from_proto(&open)
        .map_err(|_| ApplicationOperationFailure::InvalidRequest(None))?;
    let application = application_context(request_id, Some(&selected_contract), None);
    presentation.validate(Some(&application))?;
    let (service, context, _cancellation, _) = admit(
        route,
        ServiceOperationV1::DescribeContract,
        request_id,
        presentation,
        Some(&application),
    )?;
    let result = service
        .get_application_catalog(context, request)
        .await
        .map_err(|failure| ApplicationOperationFailure::Service {
            failure,
            application: Some(application),
        })?;
    let identity = result.page().identity();
    if identity.lineage().as_str() != selected_contract.lineage
        || identity.version().get() != selected_contract.version
        || identity.contract_hash().as_bytes() != selected_contract.bundle_hash.as_slice()
    {
        return Err(ApplicationOperationFailure::Application(
            ApplicationError::new(
                ApplicationErrorCode::ContractMismatch,
                ApplicationOperation::DescribeContract,
                ApplicationErrorContext::empty(),
                None,
            ),
        ));
    }
    let modules = identity.module_hashes();
    if open.query_module_hashes.iter().any(|selected| {
        !modules
            .iter()
            .any(|active| active.as_bytes() == selected.as_slice())
    }) {
        return Err(ApplicationOperationFailure::Application(
            ApplicationError::new(
                ApplicationErrorCode::ModuleUnavailable,
                ApplicationOperation::DescribeContract,
                ApplicationErrorContext::empty(),
                None,
            ),
        ));
    }
    Ok(v1::ApplicationSessionOpened {
        protocol_version: APPLICATION_SESSION_PROTOCOL_V1,
        contract: Some(selected_contract),
        query_module_hashes: open.query_module_hashes,
        application_lock_hash: open.application_lock_hash,
        maximum_in_flight: open.requested_max_in_flight,
    })
}

/// Runs one generated command through conversion, lifecycle admission,
/// authentication, the shared service, and strict response conversion.
pub async fn execute_generated_command(
    route: &dyn ApplicationOperationRoute,
    presentation: ApplicationOperationPresentation<'_>,
    message: v1::ExecuteCommandRequest,
) -> Result<v1::ExecuteCommandResponse, ApplicationOperationFailure> {
    let transport_started = Instant::now();
    let (request_id, request) = execute_command_request_from_proto(message)
        .map_err(|_| ApplicationOperationFailure::InvalidRequest(None))?;
    presentation.validate(None)?;
    let history_incarnation = route
        .history_incarnation()
        .ok_or(ApplicationOperationFailure::Unavailable(None))?;
    let admission_started = Instant::now();
    let (service, context, _cancellation, authn_elapsed) = admit(
        route,
        ServiceOperationV1::ExecuteCommand,
        request_id,
        presentation,
        None,
    )?;
    let telemetry = route.read_stage_telemetry();
    if let Some(telemetry) = &telemetry {
        telemetry.record(ServiceTelemetryEvent::WriteServiceStageCompleted {
            stage: WriteServiceStage::TransportAdapt,
            elapsed: transport_started.elapsed(),
        });
        record_read_admission(
            telemetry.as_ref(),
            authn_elapsed,
            admission_started.elapsed().saturating_sub(authn_elapsed),
        );
    }
    let result = service
        .execute_command(context, request)
        .await
        .map_err(|failure| ApplicationOperationFailure::Service {
            failure,
            application: None,
        })?;
    let encode_started = Instant::now();
    let response = execute_command_result_to_proto(&result, history_incarnation)
        .map_err(|_| ApplicationOperationFailure::InvalidResponse)?;
    if let Some(telemetry) = &telemetry {
        telemetry.record(ServiceTelemetryEvent::WriteServiceStageCompleted {
            stage: WriteServiceStage::EncodeConvert,
            elapsed: encode_started.elapsed(),
        });
    }
    Ok(response)
}

/// Runs one named query through the same per-operation safe points.
pub async fn execute_generated_query(
    route: &dyn ApplicationOperationRoute,
    presentation: ApplicationOperationPresentation<'_>,
    message: app_v1::ExecuteQueryRequest,
    named_only: bool,
) -> Result<app_v1::ExecuteQueryResponse, ApplicationOperationFailure> {
    let transport_started = Instant::now();
    let boundary =
        ApplicationErrorContextBuilder::without_trace(ApplicationOperation::ExecuteQuery);
    let original = message.clone();
    let operation_symbol = match original.query.as_ref() {
        Some(app_v1::execute_query_request::Query::QueryName(name)) => Some(name.as_str()),
        Some(app_v1::execute_query_request::Query::Source(_)) | None => None,
    };
    let (request_id, request) = execute_symbolic_query_request_from_proto(message)
        .map_err(|_| ApplicationOperationFailure::InvalidRequest(Some(boundary)))?;
    if named_only && !matches!(request, ExecuteSymbolicQueryInvocation::Named(_)) {
        return Err(ApplicationOperationFailure::InvalidRequest(Some(
            ApplicationErrorContextBuilder::new(ApplicationOperation::ExecuteQuery, request_id),
        )));
    }
    let application = application_context(request_id, original.contract.as_ref(), operation_symbol);
    presentation.validate(Some(&application))?;
    let telemetry = route.read_stage_telemetry();
    if let Some(telemetry) = &telemetry {
        telemetry.record(ServiceTelemetryEvent::ReadPipelineStageCompleted {
            stage: ReadPipelineStage::TransportAdapt,
            elapsed: transport_started.elapsed(),
        });
    }
    let admission_started = Instant::now();
    let (service, context, _cancellation, authn_elapsed) = admit(
        route,
        ServiceOperationV1::ExecuteQuery,
        request_id,
        presentation,
        Some(&application),
    )?;
    if let Some(telemetry) = &telemetry {
        record_read_admission(
            telemetry.as_ref(),
            authn_elapsed,
            admission_started.elapsed().saturating_sub(authn_elapsed),
        );
    }
    let result = match request {
        ExecuteSymbolicQueryInvocation::AdHoc(request) => {
            service.execute_symbolic_query(context, request).await
        }
        ExecuteSymbolicQueryInvocation::Named(request) => {
            service.execute_named_symbolic_query(context, request).await
        }
    }
    .map_err(|failure| ApplicationOperationFailure::Service {
        failure,
        application: Some(application.clone()),
    })?;
    let encode_started = Instant::now();
    let response = execute_symbolic_query_result_to_proto(result)
        .map_err(|_| ApplicationOperationFailure::InvalidResponse)?;
    if let Some(telemetry) = &telemetry {
        telemetry.record(ServiceTelemetryEvent::ReadPipelineStageCompleted {
            stage: ReadPipelineStage::EncodeConvert,
            elapsed: encode_started.elapsed(),
        });
    }
    Ok(response)
}

fn admit(
    route: &dyn ApplicationOperationRoute,
    operation: ServiceOperationV1,
    request_id: riffdb_types::RequestId,
    presentation: ApplicationOperationPresentation<'_>,
    application: Option<&ApplicationErrorContextBuilder>,
) -> Result<
    (
        Arc<dyn ApplicationService>,
        RequestContext,
        CancellationGuard,
        Duration,
    ),
    ApplicationOperationFailure,
> {
    let service = route
        .admit_authenticated(operation)
        .ok_or_else(|| unavailable(application))?;
    let security = route
        .security_context()
        .ok_or_else(|| unavailable(application))?;
    let credential = presentation
        .credential
        .map_err(|_| ApplicationOperationFailure::Unauthenticated(application.cloned()))?;
    let deadline = presentation
        .deadline
        .map_err(|_| ApplicationOperationFailure::InvalidRequest(application.cloned()))?;
    let authn_started = Instant::now();
    let principal = security
        .authenticate(credential)
        .map_err(|failure| authentication_failure(failure, application))?;
    let authn_elapsed = authn_started.elapsed();
    let (control, cancellation) = RequestControl::new(deadline);
    let context = RequestContext::from_authenticated_grpc(request_id, principal, control, None);
    Ok((
        service,
        context,
        CancellationGuard(cancellation),
        authn_elapsed,
    ))
}

fn authentication_failure(
    failure: AuthenticationFailure,
    application: Option<&ApplicationErrorContextBuilder>,
) -> ApplicationOperationFailure {
    let context = application.cloned();
    match failure {
        AuthenticationFailure::Unauthenticated => {
            ApplicationOperationFailure::Unauthenticated(context)
        }
        AuthenticationFailure::CapabilityRevoked => {
            ApplicationOperationFailure::CapabilityRevoked(context)
        }
        AuthenticationFailure::Internal => ApplicationOperationFailure::InvalidResponse,
    }
}

fn unavailable(
    application: Option<&ApplicationErrorContextBuilder>,
) -> ApplicationOperationFailure {
    ApplicationOperationFailure::Unavailable(application.cloned())
}

fn application_context(
    request_id: riffdb_types::RequestId,
    contract: Option<&app_v1::ContractSelector>,
    operation_symbol: Option<&str>,
) -> ApplicationErrorContextBuilder {
    let mut context =
        ApplicationErrorContextBuilder::new(ApplicationOperation::ExecuteQuery, request_id);
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

fn record_read_admission(
    telemetry: &dyn ServiceTelemetry,
    authn_elapsed: Duration,
    admission_elapsed: Duration,
) {
    telemetry.record(ServiceTelemetryEvent::ReadPipelineStageCompleted {
        stage: ReadPipelineStage::Authn,
        elapsed: authn_elapsed,
    });
    telemetry.record(ServiceTelemetryEvent::ReadPipelineStageCompleted {
        stage: ReadPipelineStage::AdmissionContext,
        elapsed: admission_elapsed,
    });
}

struct CancellationGuard(riffdb_service::RequestCancellationHandle);

impl Drop for CancellationGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

impl From<ConversionError> for ApplicationOperationFailure {
    fn from(error: ConversionError) -> Self {
        match error {
            ConversionError::InvalidRequest => Self::InvalidRequest(None),
            ConversionError::InvalidResponse => Self::InvalidResponse,
        }
    }
}
