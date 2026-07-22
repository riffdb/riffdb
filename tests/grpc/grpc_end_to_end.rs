//! Loopback gRPC conformance over an injected API-neutral application service.

#![cfg(all(feature = "server", feature = "client"))]
#![forbid(unsafe_code)]

use std::num::NonZeroU16;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use riffdb_api_grpc::generated::query_service_client::QueryServiceClient;
use riffdb_api_grpc::{
    CheckedGrpcSecurityContext, GrpcApplication, GrpcBootstrapCompletion, GrpcDeploymentCompletion,
    GrpcLifecycleRoute, GrpcRequestLimits,
};
use riffdb_auth::{
    AuthenticatedPrincipal, AuthenticationContext, AuthenticationFailure,
    CapabilityDigestKeyProvider, CredentialAuthenticator, OpaqueCredential,
};
use riffdb_client_rust::{BearerCredential, CallMetadata, RiffDbClient};
use riffdb_errors::PublicError;
use riffdb_proto::v1;
use riffdb_proto::{MAX_PUBLIC_ERROR_BYTES, decode_public_error};
use riffdb_service::{
    AdministrationApplication, ApplicationService, CommitApplication, ContractApplication,
    CreateCapabilityInvocation, CreateCapabilityResult, DiscoveryApplication, HealthContext,
    HealthRequest, HealthResult, Page, PageLimit, ProjectionFailureCode, ProjectionPageFence,
    ProjectionRow, ProjectionUnavailableReason, QueryApplication, QueryProjectionReady,
    QueryProjectionRequest, QueryProjectionResult, RequestContext, ServiceFailure, ServiceFuture,
};
use riffdb_testkit::authorization::{
    AuthorizationFixture, AuthorizationFixtureConfig, AuthorizationFixtureTimes,
};
use riffdb_types::{
    ActorId, ActorKind, Audience, CapabilityGrantV1, CapabilityPermissionsV1, CommitSequence,
    ContractLineage, DatabaseId, Environment, FrontierPosition, PartitionScopeV1,
    ProjectionGeneration, ProjectionId, ProjectionIdentity, ProjectionPlanHash, RequestId,
    ServiceIngressKindV1, ServiceOperationV1, TenantScope, Timestamp,
};
use tokio::sync::oneshot;
use tonic::metadata::MetadataValue;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Endpoint, Server};
use tonic::{Code, Request};

const CAPABILITY_TOKEN: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
const CAPABILITY_KEYS: &[u8] = b"riffdb-capability-digest-keys-v1\n1:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";
const LINEAGE: &str = "grpc-projection";

#[derive(Clone, Debug, Eq, PartialEq)]
struct ObservedInvocation {
    request_id: RequestId,
    capability_id: riffdb_types::CapabilityId,
    principal_id: ActorId,
    ingress: ServiceIngressKindV1,
}

struct ProjectionService {
    observed: Mutex<Vec<ObservedInvocation>>,
}

impl ProjectionService {
    fn new() -> Self {
        Self {
            observed: Mutex::new(Vec::new()),
        }
    }

    fn observed(&self) -> Vec<ObservedInvocation> {
        self.observed
            .lock()
            .expect("observation lock remains available")
            .clone()
    }
}

fn denied<T>() -> ServiceFuture<'static, T> {
    Box::pin(async { Err(ServiceFailure::from(PublicError::authorization_denied())) })
}

macro_rules! denied_operation {
    ($name:ident, $context:ty, $request:ty, $result:ty) => {
        fn $name(&self, _context: $context, _request: $request) -> ServiceFuture<'_, $result> {
            denied()
        }
    };
}

impl ContractApplication for ProjectionService {
    denied_operation!(
        validate_contract,
        RequestContext,
        riffdb_service::ValidateContractRequest,
        riffdb_service::ContractValidationResult
    );
    denied_operation!(
        explain_command,
        RequestContext,
        riffdb_service::ExplainCommandRequest,
        riffdb_service::ExplainCommandResult
    );
    denied_operation!(
        deploy_contract,
        RequestContext,
        riffdb_service::DeployContractRequest,
        riffdb_service::DeployContractResult
    );
    denied_operation!(
        get_active_contract,
        RequestContext,
        riffdb_service::GetActiveContractRequest,
        riffdb_service::GetActiveContractResult
    );
    denied_operation!(
        get_contract_version,
        RequestContext,
        riffdb_service::GetContractVersionRequest,
        riffdb_service::GetContractVersionResult
    );
}

impl riffdb_service::CommandApplication for ProjectionService {
    denied_operation!(
        execute_command,
        RequestContext,
        riffdb_service::ExecuteCommandRequest,
        riffdb_service::ExecuteCommandResult
    );
    denied_operation!(
        resolve_command_outcome,
        RequestContext,
        riffdb_service::ResolveCommandOutcomeRequest,
        riffdb_service::ResolveCommandOutcomeResult
    );
}

impl QueryApplication for ProjectionService {
    denied_operation!(
        get_entity,
        RequestContext,
        riffdb_service::GetEntityRequest,
        riffdb_service::GetEntityResult
    );
    denied_operation!(
        scan_index,
        RequestContext,
        riffdb_service::ScanIndexRequest,
        riffdb_service::ScanIndexResult
    );

    fn query_projection(
        &self,
        context: RequestContext,
        request: QueryProjectionRequest,
    ) -> ServiceFuture<'_, QueryProjectionResult> {
        self.observed
            .lock()
            .expect("observation lock remains available")
            .push(ObservedInvocation {
                request_id: context.request_id(),
                capability_id: context.principal().capability_id(),
                principal_id: context.principal().principal_id().clone(),
                ingress: context.ingress(),
            });

        let result = match request.projection_id().get() {
            1 => Ok(ready_projection()),
            2 => Ok(QueryProjectionResult::WaitTimedOut {
                required: CommitSequence::first(),
                current: FrontierPosition::BeforeFirst,
            }),
            3 => Ok(QueryProjectionResult::Degraded {
                current: FrontierPosition::AppliedThrough(CommitSequence::first()),
                reason: ProjectionUnavailableReason::Rebuilding,
            }),
            4 => Ok(QueryProjectionResult::Invalid {
                reason: ProjectionFailureCode::HardLimitExceeded,
            }),
            _ => Err(ServiceFailure::from(PublicError::authorization_denied())),
        };
        Box::pin(async move { result })
    }

    denied_operation!(
        get_projection_status,
        RequestContext,
        riffdb_service::GetProjectionStatusRequest,
        riffdb_service::GetProjectionStatusResult
    );
}

impl CommitApplication for ProjectionService {
    denied_operation!(
        get_commit,
        RequestContext,
        riffdb_service::GetCommitRequest,
        riffdb_service::GetCommitResult
    );
    denied_operation!(
        scan_commits,
        RequestContext,
        riffdb_service::ScanCommitsRequest,
        riffdb_service::ScanCommitsResult
    );
    denied_operation!(
        subscribe_to_commits,
        RequestContext,
        riffdb_service::SubscribeToCommitsRequest,
        riffdb_service::SubscribeToCommitsResult
    );
    denied_operation!(
        trace_provenance,
        RequestContext,
        riffdb_service::TraceProvenanceRequest,
        riffdb_service::TraceProvenanceResult
    );
}

impl AdministrationApplication for ProjectionService {
    denied_operation!(health, HealthContext, HealthRequest, HealthResult);
    denied_operation!(
        statistics,
        RequestContext,
        riffdb_service::StatisticsRequest,
        riffdb_service::StatisticsResult
    );

    fn create_capability(
        &self,
        _invocation: CreateCapabilityInvocation,
    ) -> ServiceFuture<'_, CreateCapabilityResult> {
        denied()
    }

    denied_operation!(
        revoke_capability,
        RequestContext,
        riffdb_service::RevokeCapabilityRequest,
        riffdb_service::RevokeCapabilityResult
    );
    denied_operation!(
        list_pending_outbox_deliveries,
        RequestContext,
        riffdb_service::ListPendingOutboxDeliveriesRequest,
        riffdb_service::ListPendingOutboxDeliveriesResult
    );
}

impl DiscoveryApplication for ProjectionService {
    denied_operation!(
        discover_command_tools,
        RequestContext,
        riffdb_service::DiscoverCommandToolsRequest,
        riffdb_service::DiscoverCommandToolsResult
    );
    denied_operation!(
        discover_resources,
        RequestContext,
        riffdb_service::DiscoverResourcesRequest,
        riffdb_service::DiscoverResourcesResult
    );
}

#[derive(Clone)]
struct AcceptingAuthenticator {
    principal: AuthenticatedPrincipal,
}

impl CredentialAuthenticator for AcceptingAuthenticator {
    fn authenticate(
        &self,
        _credential: OpaqueCredential<'_>,
        _context: &AuthenticationContext,
    ) -> Result<AuthenticatedPrincipal, AuthenticationFailure> {
        Ok(self.principal.clone())
    }
}

struct ActiveRoute {
    service: Arc<dyn ApplicationService>,
    security: CheckedGrpcSecurityContext,
}

impl GrpcLifecycleRoute for ActiveRoute {
    fn admit_authenticated(
        &self,
        operation: ServiceOperationV1,
    ) -> Option<Arc<dyn ApplicationService>> {
        (operation == ServiceOperationV1::QueryProjection).then(|| Arc::clone(&self.service))
    }

    fn security_context(&self) -> Option<CheckedGrpcSecurityContext> {
        Some(self.security.clone())
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn projection_variants_and_public_error_cross_real_grpc() {
    let database_id =
        DatabaseId::from_unix_milliseconds_and_random(1, [1; 10]).expect("valid database ID");
    let environment = Environment::new("grpc-test").expect("valid environment");
    let audience = Audience::new("grpc-loopback").expect("valid audience");
    let principal = authenticated_principal(database_id, environment.clone(), audience.clone());
    let expected_capability = principal.capability_id();
    let expected_actor = principal.principal_id().clone();

    let service = Arc::new(ProjectionService::new());
    let application_service: Arc<dyn ApplicationService> = service.clone();
    let authenticator: Arc<dyn CredentialAuthenticator> =
        Arc::new(AcceptingAuthenticator { principal });
    let capability_keys = Arc::new(
        CapabilityDigestKeyProvider::parse_document(CAPABILITY_KEYS)
            .expect("valid capability key fixture"),
    );
    let security = CheckedGrpcSecurityContext::new(
        authenticator,
        AuthenticationContext::new(database_id, environment, audience),
        capability_keys,
    );
    let route: Arc<dyn GrpcLifecycleRoute> = Arc::new(ActiveRoute {
        service: application_service,
        security,
    });
    let application = GrpcApplication::new(
        route,
        GrpcRequestLimits::new(Duration::from_secs(30)).expect("bounded request duration"),
    );

    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().expect("loopback address"))
        .expect("bind loopback listener");
    let address = incoming.local_addr().expect("bound loopback address");
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let server = tokio::spawn(
        Server::builder()
            .add_service(application.query_server())
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_receiver.await;
            }),
    );

    let endpoint =
        Endpoint::from_shared(format!("http://{address}")).expect("valid loopback endpoint");
    let channel = endpoint.connect().await.expect("connect loopback client");
    let mut client = RiffDbClient::from_channel(channel.clone());
    let metadata = CallMetadata::authenticated(
        BearerCredential::new(CAPABILITY_TOKEN).expect("valid credential presentation"),
    );
    let request_ids = (1_u8..=5).map(request_id).collect::<Vec<_>>();

    let ready = client
        .query_projection(projection_request(request_ids[0], 1, false), &metadata)
        .await
        .expect("ready projection response");
    assert_ready(ready);

    let timed_out = client
        .query_projection(projection_request(request_ids[1], 2, true), &metadata)
        .await
        .expect("wait timeout response");
    assert_wait_timed_out(timed_out);

    let degraded = client
        .query_projection(projection_request(request_ids[2], 3, false), &metadata)
        .await
        .expect("degraded projection response");
    assert_degraded(degraded);

    let invalid = client
        .query_projection(projection_request(request_ids[3], 4, false), &metadata)
        .await
        .expect("invalid projection response");
    assert_invalid(invalid);

    let mut raw_client = QueryServiceClient::new(channel);
    let mut request = Request::new(projection_request(request_ids[4], 5, false));
    request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {CAPABILITY_TOKEN}"))
            .expect("valid authorization metadata"),
    );
    let status = raw_client
        .query_projection(request)
        .await
        .expect_err("public error must cross gRPC directly");
    let expected_error = PublicError::authorization_denied();
    assert_eq!(status.code(), Code::PermissionDenied);
    assert_eq!(status.message(), expected_error.safe_message());
    assert!(!status.details().is_empty());
    assert!(status.details().len() <= MAX_PUBLIC_ERROR_BYTES);
    assert_eq!(decode_public_error(status.details()), Ok(expected_error));

    let observed = service.observed();
    assert_eq!(observed.len(), request_ids.len());
    for (observation, expected_request) in observed.iter().zip(request_ids) {
        assert_eq!(observation.request_id, expected_request);
        assert_eq!(observation.capability_id, expected_capability);
        assert_eq!(observation.principal_id, expected_actor);
        assert_eq!(observation.ingress, ServiceIngressKindV1::Grpc);
    }

    drop(raw_client);
    drop(client);
    shutdown_sender.send(()).expect("server still running");
    server
        .await
        .expect("server task did not panic")
        .expect("server shut down cleanly");
}

fn authenticated_principal(
    database_id: DatabaseId,
    environment: Environment,
    audience: Audience,
) -> AuthenticatedPrincipal {
    let grant = CapabilityGrantV1::new(
        TenantScope::Global,
        PartitionScopeV1::All,
        CapabilityPermissionsV1::new(Vec::new()).expect("empty permission set is valid"),
        Vec::new(),
        NonZeroU16::MIN,
        Vec::new(),
    )
    .expect("valid test grant");
    let fixture = AuthorizationFixture::new(AuthorizationFixtureConfig::new(
        database_id,
        environment,
        ActorId::new("grpc-principal").expect("valid principal"),
        ActorKind::Human,
        audience,
        AuthorizationFixtureTimes::new(timestamp(100), timestamp(200), timestamp(150)),
        grant,
    ))
    .expect("valid authorization fixture");
    fixture.authenticated_principal().clone()
}

fn timestamp(seconds: i64) -> Timestamp {
    Timestamp::new(seconds, 0).expect("canonical timestamp")
}

fn request_id(ordinal: u8) -> RequestId {
    RequestId::from_unix_milliseconds_and_random(u64::from(ordinal), [ordinal; 10])
        .expect("valid fresh request ID")
}

fn projection_request(
    request_id: RequestId,
    projection_id: u32,
    waits: bool,
) -> v1::QueryProjectionRequest {
    v1::QueryProjectionRequest {
        request_id: request_id.into_bytes().to_vec(),
        contract: Some(v1::ContractSelection {
            selection: Some(v1::contract_selection::Selection::Active(v1::Unit {})),
        }),
        projection_id,
        leading_components: Vec::new(),
        required_sequence: waits.then_some(CommitSequence::first().get()),
        wait_nanos: u64::from(waits),
        page: Some(v1::PageRequest {
            limit: Some(1),
            cursor: None,
        }),
    }
}

fn ready_projection() -> QueryProjectionResult {
    let frontier = FrontierPosition::AppliedThrough(CommitSequence::first());
    let identity = ProjectionIdentity::new(
        ContractLineage::new(LINEAGE).expect("valid projection lineage"),
        ProjectionId::first(),
        ProjectionPlanHash::from_bytes([7; 32]),
    );
    let fence = ProjectionPageFence::new(identity, ProjectionGeneration::first(), frontier);
    let page = Page::new(
        PageLimit::new(1).expect("nonzero page limit"),
        Vec::<ProjectionRow>::new(),
        None,
        fence,
    )
    .expect("valid empty terminal page");
    QueryProjectionResult::Ready(
        QueryProjectionReady::new(page, frontier).expect("matching projection frontiers"),
    )
}

fn assert_ready(response: v1::QueryProjectionResponse) {
    let Some(v1::query_projection_response::Result::Ready(ready)) = response.result else {
        panic!("expected ready projection response");
    };
    let data = ready.data.expect("ready data");
    assert!(data.items.is_empty());
    let fence = data.observed_fence.expect("ready fence");
    assert_eq!(fence.generation, 1);
    assert_eq!(
        fence.identity.expect("projection identity").projection_id,
        1
    );
    assert_applied_through(fence.frontier, 1);
    assert_applied_through(ready.frontier, 1);
}

fn assert_wait_timed_out(response: v1::QueryProjectionResponse) {
    let Some(v1::query_projection_response::Result::WaitTimedOut(result)) = response.result else {
        panic!("expected projection wait timeout");
    };
    assert_eq!(result.required_sequence, 1);
    assert!(matches!(
        result.current.and_then(|frontier| frontier.position),
        Some(v1::frontier_position::Position::BeforeFirst(_))
    ));
}

fn assert_degraded(response: v1::QueryProjectionResponse) {
    let Some(v1::query_projection_response::Result::Degraded(result)) = response.result else {
        panic!("expected degraded projection response");
    };
    assert_applied_through(result.current, 1);
    assert!(matches!(
        result.reason.and_then(|reason| reason.reason),
        Some(v1::projection_unavailable_reason::Reason::Rebuilding(_))
    ));
}

fn assert_invalid(response: v1::QueryProjectionResponse) {
    let Some(v1::query_projection_response::Result::Invalid(result)) = response.result else {
        panic!("expected invalid projection response");
    };
    assert_eq!(
        result.reason,
        v1::ProjectionFailureCode::HardLimitExceeded as i32
    );
}

fn assert_applied_through(frontier: Option<v1::FrontierPosition>, expected: u64) {
    assert!(matches!(
        frontier.and_then(|frontier| frontier.position),
        Some(v1::frontier_position::Position::AppliedThrough(sequence)) if sequence == expected
    ));
}
