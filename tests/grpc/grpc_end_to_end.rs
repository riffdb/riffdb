//! Loopback gRPC conformance over an injected API-neutral application service.

#![cfg(all(feature = "server", feature = "client"))]
#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::num::NonZeroU16;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use riffdb_api_grpc::generated::admin_service_client::AdminServiceClient;
use riffdb_api_grpc::generated::contract_service_client::ContractServiceClient;
use riffdb_api_grpc::generated::query_service_client::QueryServiceClient;
use riffdb_api_grpc::{
    CheckedGrpcRestoreRetrySecurityContext, CheckedGrpcSecurityContext, DEADLINE_EXCEEDED_MESSAGE,
    GrpcApplication, GrpcBootstrapCompletion, GrpcDeploymentCompletion, GrpcLifecycleRoute,
    GrpcOfflineMaintenanceOperation, GrpcRequestLimits, UNAUTHENTICATED_MESSAGE,
};
use riffdb_auth::{
    AuthenticatedPrincipal, AuthenticationContext, AuthenticationFailure,
    CapabilityDigestKeyProvider, CredentialAuthenticator, CurrentCapabilityActivity,
    CurrentCapabilityResolver, OpaqueCredential,
};
use riffdb_client_rust::{BearerCredential, CallMetadata, RiffDbClient};
use riffdb_errors::{
    PublicError, ValidationCode, ValidationIssue, ValidationIssues, ValidationPath,
};
use riffdb_proto::v1;
use riffdb_proto::{MAX_PUBLIC_ERROR_BYTES, decode_public_error};
use riffdb_service::{
    AdministrationApplication, ApplicationService, CommandToolDiscoveryItem, CommitApplication,
    CompactCommandToolDiscoveryItem, CompactResourceDescriptor, ContractApplication,
    ContractDescriptor, CreateCapabilityInvocation, CreateCapabilityResult, CursorToken,
    DiscoverCommandToolsRequest, DiscoverCommandToolsResult, DiscoverResourcesRequest,
    DiscoverResourcesResult, DiscoveryApplication, DiscoveryCatalogFence, DiscoveryRepresentation,
    FixedToolKind, GetContractVersionResult, GetOfflineMaintenanceOperationResult,
    GetProjectionStatusResult, HealthContext, HealthRequest, HealthResult,
    ListPendingOutboxDeliveriesResult, OfflineMaintenanceApplication,
    OfflineMaintenanceObservationPhase, OfflineMaintenanceOperationObservation,
    OfflineMaintenanceStartDisposition, OfflineMaintenanceStartResult, OperationSchemaCatalog,
    Page, PageLimit, ProjectionFailureCode, ProjectionPageFence, ProjectionRow,
    ProjectionStatusSnapshot, ProjectionUnavailableReason, QueryApplication, QueryProjectionReady,
    QueryProjectionRequest, QueryProjectionResult, RecoveryOfflineMaintenanceApplication,
    RecoveryRestoreOfflineBackupInvocation, RequestContext, ResolveCommandOutcomeResult,
    ResolveCommandOutcomeSelectorRef, ResourceDescriptor, RestoreOfflineBackupInvocation,
    RestoreRetryOfflineMaintenanceApplication, ServiceFailure, ServiceFuture,
    SymbolicQueryApplication, TraceProvenanceResult,
};
use riffdb_testkit::authorization::{
    AuthorizationFixture, AuthorizationFixtureConfig, AuthorizationFixtureTimes,
};
use riffdb_types::{
    ActorId, ActorKind, Audience, BackupNameV1, CapabilityGrantV1, CapabilityPermissionsV1,
    CommitSequence, ContractBundleHash, ContractLineage, ContractPlanRootHash, ContractVersion,
    DatabaseId, Environment, FrontierPosition, OfflineMaintenanceInputHash,
    OfflineMaintenanceOperationId, OfflineMaintenanceOperationKind,
    OfflineMaintenanceReplacementConfirmation, PartitionScopeV1, PlanHash, ProjectionGeneration,
    ProjectionId, ProjectionIdentity, ProjectionPlanHash, RequestId, ServiceIngressKindV1,
    ServiceOperationV1, SourceHash, TenantScope, Timestamp, offline_maintenance_input_hash,
};
use tokio::sync::oneshot;
use tonic::metadata::MetadataValue;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Endpoint, Server};
use tonic::{Code, Request};

const CAPABILITY_TOKEN: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
const CAPABILITY_KEYS: &[u8] = b"riffdb-capability-digest-keys-v1\n1:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";
const LINEAGE: &str = "grpc-projection";
const SERVER_GENERATION: [u8; 16] = [0xa5; 16];
const OUTCOME_LOCATOR: &str = "riffdb://outcome/agent_01/legalspend/2/riffdb_cmd_legalspend_allocatebudget/AQAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

#[derive(Clone, Debug, Eq, PartialEq)]
struct ObservedInvocation {
    request_id: RequestId,
    capability_id: riffdb_types::CapabilityId,
    principal_id: ActorId,
    ingress: ServiceIngressKindV1,
    operation: ServiceOperationV1,
}

struct ProjectionService {
    observed: Mutex<Vec<ObservedInvocation>>,
    command_discovery_prior: Mutex<Vec<bool>>,
    outcome_locators: Mutex<Vec<String>>,
    shortened_deadline_observed: Mutex<bool>,
    pending_probe: Mutex<Option<PendingProbe>>,
    current_authority: Option<Arc<AuthorizationFixture>>,
    resource_cursors: Mutex<ResourceCursorState>,
    maintenance_invocations: Mutex<Vec<ObservedMaintenanceInvocation>>,
    /// Optional residual-stage sink for SpawnDispatch in the gRPC harness.
    read_stage_telemetry: Option<Arc<dyn riffdb_service::ServiceTelemetry>>,
}

struct PendingProbe {
    started: oneshot::Sender<()>,
    cancelled: oneshot::Sender<()>,
}

#[derive(Default)]
struct ResourceCursorState {
    next_ordinal: u8,
    valid: BTreeSet<CursorToken>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ObservedMaintenanceInvocation {
    Create {
        request_id: RequestId,
        operation_id: OfflineMaintenanceOperationId,
        backup_name: String,
    },
    Restore {
        redacted_invocation: String,
    },
    Get {
        request_id: RequestId,
        operation_id: OfflineMaintenanceOperationId,
    },
}

impl ProjectionService {
    fn new() -> Self {
        Self {
            observed: Mutex::new(Vec::new()),
            command_discovery_prior: Mutex::new(Vec::new()),
            outcome_locators: Mutex::new(Vec::new()),
            shortened_deadline_observed: Mutex::new(false),
            pending_probe: Mutex::new(None),
            read_stage_telemetry: None,
            current_authority: None,
            resource_cursors: Mutex::new(ResourceCursorState::default()),
            maintenance_invocations: Mutex::new(Vec::new()),
        }
    }

    fn with_current_authority(current_authority: Arc<AuthorizationFixture>) -> Self {
        Self {
            current_authority: Some(current_authority),
            ..Self::new()
        }
    }

    fn observed(&self) -> Vec<ObservedInvocation> {
        self.observed
            .lock()
            .expect("observation lock remains available")
            .clone()
    }

    fn maintenance_invocations(&self) -> Vec<ObservedMaintenanceInvocation> {
        self.maintenance_invocations
            .lock()
            .expect("maintenance observation lock remains available")
            .clone()
    }

    fn observe(&self, context: &RequestContext, operation: ServiceOperationV1) {
        self.observed
            .lock()
            .expect("observation lock remains available")
            .push(ObservedInvocation {
                request_id: context.request_id(),
                capability_id: context.principal().capability_id(),
                principal_id: context.principal().principal_id().clone(),
                ingress: context.ingress(),
                operation,
            });
    }

    fn command_discovery_prior(&self) -> Vec<bool> {
        self.command_discovery_prior
            .lock()
            .expect("discovery observation lock remains available")
            .clone()
    }

    fn outcome_locators(&self) -> Vec<String> {
        self.outcome_locators
            .lock()
            .expect("outcome observation lock remains available")
            .clone()
    }

    fn shortened_deadline_observed(&self) -> bool {
        *self
            .shortened_deadline_observed
            .lock()
            .expect("deadline observation lock remains available")
    }

    fn prepare_pending_probe(&self) -> (oneshot::Receiver<()>, oneshot::Receiver<()>) {
        let (started_sender, started_receiver) = oneshot::channel();
        let (cancelled_sender, cancelled_receiver) = oneshot::channel();
        let previous = self
            .pending_probe
            .lock()
            .expect("pending-probe lock remains available")
            .replace(PendingProbe {
                started: started_sender,
                cancelled: cancelled_sender,
            });
        assert!(previous.is_none(), "only one pending probe may be armed");
        (started_receiver, cancelled_receiver)
    }

    fn take_pending_probe(&self) -> PendingProbe {
        self.pending_probe
            .lock()
            .expect("pending-probe lock remains available")
            .take()
            .expect("pending request must have an armed probe")
    }

    fn current_authority_allows(&self, context: &RequestContext) -> bool {
        let Some(fixture) = &self.current_authority else {
            return true;
        };
        fixture
            .current_capability_resolver()
            .resolve_current(context.principal())
            .is_ok_and(|current| current.activity() == CurrentCapabilityActivity::Active)
    }

    fn issue_resource_cursor(&self) -> CursorToken {
        let mut state = self
            .resource_cursors
            .lock()
            .expect("resource-cursor lock remains available");
        state.next_ordinal = state
            .next_ordinal
            .checked_add(1)
            .expect("the conformance test issues only bounded cursor tokens");
        let token = CursorToken::from_bytes([state.next_ordinal; 16]);
        assert!(
            state.valid.insert(token),
            "issued cursor token must be unique"
        );
        token
    }

    fn resource_cursor_is_valid(&self, token: CursorToken) -> bool {
        self.resource_cursors
            .lock()
            .expect("resource-cursor lock remains available")
            .valid
            .contains(&token)
    }

    fn invalidate_resource_cursor(&self, bytes: &[u8]) {
        let token = CursorToken::from_bytes(
            bytes
                .try_into()
                .expect("public cursor has the exact 16-byte representation"),
        );
        assert!(
            self.resource_cursors
                .lock()
                .expect("resource-cursor lock remains available")
                .valid
                .remove(&token),
            "only an issued cursor may be invalidated"
        );
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
    fn get_contract_version(
        &self,
        context: RequestContext,
        _request: riffdb_service::GetContractVersionRequest,
    ) -> ServiceFuture<'_, GetContractVersionResult> {
        self.observe(&context, ServiceOperationV1::GetContractVersion);
        Box::pin(async { Ok(GetContractVersionResult::Found(contract_descriptor())) })
    }
}

impl riffdb_service::CommandApplication for ProjectionService {
    fn execute_command(
        &self,
        context: RequestContext,
        request: riffdb_service::ExecuteCommandRequest,
    ) -> ServiceFuture<'_, riffdb_service::ExecuteCommandResult> {
        self.observe(&context, ServiceOperationV1::ExecuteCommand);
        let name = request.command().as_str().to_owned();
        Box::pin(async move {
            match name.as_str() {
                "Overloaded" => Err(ServiceFailure::from(PublicError::overloaded())),
                "InputInvalid" => Err(ServiceFailure::from(PublicError::validation(
                    ValidationIssues::one(ValidationIssue::new(
                        ValidationCode::InvalidValue,
                        ValidationPath::root(),
                    )),
                ))),
                // Distinct plan hashes so mirroring/order assertions discriminate.
                "OkCommandA" => {
                    let result = riffdb_service::ReadOnlyCommandResult::integration_fixture(
                        ContractLineage::new(LINEAGE).expect("lineage"),
                        ContractVersion::new(1).expect("version"),
                        PlanHash::from_bytes([0xA1; 32]),
                        "CompletedA",
                    )
                    .expect("fixture read-only result");
                    Ok(riffdb_service::ExecuteCommandResult::ReadOnlyExecuted(
                        result,
                    ))
                }
                "OkCommandB" => {
                    let result = riffdb_service::ReadOnlyCommandResult::integration_fixture(
                        ContractLineage::new(LINEAGE).expect("lineage"),
                        ContractVersion::new(1).expect("version"),
                        PlanHash::from_bytes([0xB2; 32]),
                        "CompletedB",
                    )
                    .expect("fixture read-only result");
                    Ok(riffdb_service::ExecuteCommandResult::ReadOnlyExecuted(
                        result,
                    ))
                }
                "OkCommandC" => {
                    let result = riffdb_service::ReadOnlyCommandResult::integration_fixture(
                        ContractLineage::new(LINEAGE).expect("lineage"),
                        ContractVersion::new(1).expect("version"),
                        PlanHash::from_bytes([0xC3; 32]),
                        "CompletedC",
                    )
                    .expect("fixture read-only result");
                    Ok(riffdb_service::ExecuteCommandResult::ReadOnlyExecuted(
                        result,
                    ))
                }
                _ => {
                    let result = riffdb_service::ReadOnlyCommandResult::integration_fixture(
                        ContractLineage::new(LINEAGE).expect("lineage"),
                        ContractVersion::new(1).expect("version"),
                        PlanHash::from_bytes([0x44; 32]),
                        "Completed",
                    )
                    .expect("fixture read-only result");
                    Ok(riffdb_service::ExecuteCommandResult::ReadOnlyExecuted(
                        result,
                    ))
                }
            }
        })
    }
    fn resolve_command_outcome(
        &self,
        context: RequestContext,
        request: riffdb_service::ResolveCommandOutcomeRequest,
    ) -> ServiceFuture<'_, ResolveCommandOutcomeResult> {
        self.observe(&context, ServiceOperationV1::ResolveCommandOutcome);
        if let ResolveCommandOutcomeSelectorRef::Locator(locator) = request.selector() {
            self.outcome_locators
                .lock()
                .expect("outcome observation lock remains available")
                .push(locator.canonical_uri().to_owned());
        }
        Box::pin(async { Ok(ResolveCommandOutcomeResult::NotFound) })
    }
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
        self.observe(&context, ServiceOperationV1::QueryProjection);

        if matches!(request.projection_id().get(), 6 | 7) {
            let probe = self.take_pending_probe();
            if request.projection_id().get() == 6 {
                *self
                    .shortened_deadline_observed
                    .lock()
                    .expect("deadline observation lock remains available") = context
                    .control()
                    .deadline()
                    .saturating_duration_since(Instant::now())
                    <= Duration::from_secs(1);
                return Box::pin(async move {
                    let _ = probe.started.send(());
                    tokio::time::sleep_until(tokio::time::Instant::from_std(
                        context.control().deadline(),
                    ))
                    .await;
                    assert!(context.control().is_deadline_exceeded());
                    let _ = probe.cancelled.send(());
                    Err(ServiceFailure::DeadlineExceeded)
                });
            }
            drop(tokio::spawn(async move {
                let _ = probe.started.send(());
                context.control().cancelled().await;
                let _ = probe.cancelled.send(());
            }));
            return Box::pin(std::future::pending());
        }

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

    fn get_projection_status(
        &self,
        context: RequestContext,
        _request: riffdb_service::GetProjectionStatusRequest,
    ) -> ServiceFuture<'_, GetProjectionStatusResult> {
        self.observe(&context, ServiceOperationV1::GetProjectionStatus);
        Box::pin(async {
            Ok(GetProjectionStatusResult::Found(
                ProjectionStatusSnapshot::uninitialized(
                    projection_identity(),
                    FrontierPosition::AppliedThrough(CommitSequence::first()),
                ),
            ))
        })
    }
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
    fn trace_provenance(
        &self,
        context: RequestContext,
        _request: riffdb_service::TraceProvenanceRequest,
    ) -> ServiceFuture<'_, TraceProvenanceResult> {
        self.observe(&context, ServiceOperationV1::TraceProvenance);
        Box::pin(async { Ok(TraceProvenanceResult::NotFound) })
    }
}

impl riffdb_service::EventServiceApplication for ProjectionService {
    fn describe_event(
        &self,
        context: RequestContext,
        _request: riffdb_service::DescribeEventRequest,
    ) -> ServiceFuture<'_, riffdb_service::DescribeEventResult> {
        self.observe(&context, ServiceOperationV1::DescribeEvent);
        Box::pin(async { Ok(riffdb_service::DescribeEventResult::NotFound) })
    }
    denied_operation!(
        replay_events,
        RequestContext,
        riffdb_service::ReplayEventsRequest,
        riffdb_service::ReplayEventsResult
    );
    denied_operation!(
        tail_events,
        RequestContext,
        riffdb_service::TailEventsRequest,
        riffdb_service::TailEventsResult
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
    fn list_pending_outbox_deliveries(
        &self,
        context: RequestContext,
        request: riffdb_service::ListPendingOutboxDeliveriesRequest,
    ) -> ServiceFuture<'_, ListPendingOutboxDeliveriesResult> {
        self.observe(&context, ServiceOperationV1::ListPendingOutboxDeliveries);
        let page = Page::new(request.page().limit(), Vec::new(), None, ())
            .expect("valid empty outbox page");
        Box::pin(async move { Ok(ListPendingOutboxDeliveriesResult::new(page)) })
    }
}

impl OfflineMaintenanceApplication for ProjectionService {
    fn create_offline_backup(
        &self,
        context: RequestContext,
        request: riffdb_service::CreateOfflineBackupRequest,
    ) -> ServiceFuture<'_, OfflineMaintenanceStartResult> {
        self.maintenance_invocations
            .lock()
            .expect("maintenance observation lock remains available")
            .push(ObservedMaintenanceInvocation::Create {
                request_id: context.request_id(),
                operation_id: request.operation_id(),
                backup_name: request.backup_name().as_str().to_owned(),
            });
        let observation = OfflineMaintenanceOperationObservation::new(
            request.operation_id(),
            OfflineMaintenanceOperationKind::CreateBackup,
            request.backup_name().clone(),
            request.input_hash(),
            OfflineMaintenanceObservationPhase::Accepted,
            None,
        )
        .expect("matching backup-create observation");
        let result = OfflineMaintenanceStartResult::new(
            OfflineMaintenanceStartDisposition::Accepted,
            observation,
        )
        .expect("matching accepted start result");
        Box::pin(async move { Ok(result) })
    }

    fn restore_offline_backup(
        &self,
        invocation: RestoreOfflineBackupInvocation,
    ) -> ServiceFuture<'_, OfflineMaintenanceStartResult> {
        self.maintenance_invocations
            .lock()
            .expect("maintenance observation lock remains available")
            .push(ObservedMaintenanceInvocation::Restore {
                redacted_invocation: format!("{invocation:?}"),
            });
        Box::pin(async { Ok(maintenance_restore_start_result()) })
    }

    fn get_offline_maintenance_operation(
        &self,
        context: RequestContext,
        request: riffdb_service::GetOfflineMaintenanceOperationRequest,
    ) -> ServiceFuture<'_, GetOfflineMaintenanceOperationResult> {
        self.maintenance_invocations
            .lock()
            .expect("maintenance observation lock remains available")
            .push(ObservedMaintenanceInvocation::Get {
                request_id: context.request_id(),
                operation_id: request.operation_id(),
            });
        let result = GetOfflineMaintenanceOperationResult::Found(maintenance_create_observation(
            request.operation_id(),
        ));
        Box::pin(async move { Ok(result) })
    }
}

impl DiscoveryApplication for ProjectionService {
    fn discover_command_tools(
        &self,
        context: RequestContext,
        request: DiscoverCommandToolsRequest,
    ) -> ServiceFuture<'_, DiscoverCommandToolsResult> {
        self.observe(&context, ServiceOperationV1::DiscoverCommandTools);
        self.command_discovery_prior
            .lock()
            .expect("discovery observation lock remains available")
            .push(request.prior_fence().is_some());

        let operation_schemas = OperationSchemaCatalog::accepted().expect("accepted test schemas");
        let fence = DiscoveryCatalogFence::no_active_contract(operation_schemas.identity());
        let result = match (request.representation(), request.prior_fence()) {
            (DiscoveryRepresentation::Full, None) => {
                let page = Page::new(
                    request.page().limit(),
                    vec![CommandToolDiscoveryItem::Fixed(FixedToolKind::GetHealth)],
                    None,
                    fence,
                )
                .expect("valid full command discovery page");
                DiscoverCommandToolsResult::page(&request, page, operation_schemas)
                    .expect("matching full command discovery result")
            }
            (DiscoveryRepresentation::CompactObservation, Some(prior)) => {
                DiscoverCommandToolsResult::catalog_unchanged(&request, prior.clone())
                    .expect("matching conditional discovery result")
            }
            (DiscoveryRepresentation::CompactObservation, None) => {
                let page = Page::<CompactCommandToolDiscoveryItem, _>::new(
                    request.page().limit(),
                    Vec::new(),
                    None,
                    fence,
                )
                .expect("valid compact command discovery page");
                DiscoverCommandToolsResult::compact_page(&request, page)
                    .expect("matching compact command discovery result")
            }
            (DiscoveryRepresentation::Full, Some(_)) => {
                unreachable!("service request DTO rejects a prior fence for full discovery")
            }
        };
        Box::pin(async move { Ok(result) })
    }

    fn discover_resources(
        &self,
        context: RequestContext,
        request: DiscoverResourcesRequest,
    ) -> ServiceFuture<'_, DiscoverResourcesResult> {
        self.observe(&context, ServiceOperationV1::DiscoverResources);
        if !self.current_authority_allows(&context) {
            return Box::pin(async {
                Err(ServiceFailure::from(PublicError::authorization_denied()))
            });
        }
        let continuation = request.page().cursor();
        if continuation.is_some_and(|cursor| !self.resource_cursor_is_valid(cursor)) {
            let failure = PublicError::validation(ValidationIssues::one(ValidationIssue::new(
                ValidationCode::InvalidValue,
                ValidationPath::root(),
            )));
            return Box::pin(async move { Err(ServiceFailure::from(failure)) });
        }
        let next_cursor = (self.current_authority.is_some()
            && request.representation() == DiscoveryRepresentation::Full
            && continuation.is_none())
        .then(|| self.issue_resource_cursor());
        let fence = discovery_fence();
        let result = match (request.representation(), request.prior_fence()) {
            (DiscoveryRepresentation::Full, None) => {
                let page = Page::new(
                    request.page().limit(),
                    vec![ResourceDescriptor::server_health()],
                    next_cursor,
                    fence,
                )
                .expect("valid full resource discovery page");
                DiscoverResourcesResult::page(&request, page)
                    .expect("matching full resource discovery result")
            }
            (DiscoveryRepresentation::CompactObservation, Some(prior)) => {
                DiscoverResourcesResult::catalog_unchanged(&request, prior.clone())
                    .expect("matching conditional resource discovery result")
            }
            (DiscoveryRepresentation::CompactObservation, None) => {
                let page = Page::<CompactResourceDescriptor, _>::new(
                    request.page().limit(),
                    Vec::new(),
                    None,
                    fence,
                )
                .expect("valid compact resource discovery page");
                DiscoverResourcesResult::compact_page(&request, page)
                    .expect("matching compact resource discovery result")
            }
            (DiscoveryRepresentation::Full, Some(_)) => {
                unreachable!("service request DTO rejects a prior fence for full discovery")
            }
        };
        Box::pin(async move { Ok(result) })
    }
}

impl ProjectionService {
    /// Denies exactly like every other unimplemented operation unless this
    /// harness was explicitly configured for residual-stage instrumentation.
    ///
    /// The default is denial so no other test's expectations change; only the
    /// residual-stage test opts into the synthetic success needed to reach the
    /// response-encoding stage.
    fn residual_symbolic_query(
        &self,
    ) -> ServiceFuture<'_, riffdb_service::ExecuteSymbolicQueryResult> {
        let Some(telemetry) = self.read_stage_telemetry.clone() else {
            return denied();
        };
        // Production records SpawnDispatch as the first statement of the
        // spawned task; this harness has no spawner, so it records the same
        // stage at service-body entry.
        let submitted = Instant::now();
        Box::pin(async move {
            telemetry.record(
                riffdb_service::ServiceTelemetryEvent::ReadPipelineStageCompleted {
                    stage: riffdb_service::ReadPipelineStage::SpawnDispatch,
                    elapsed: submitted.elapsed(),
                },
            );
            Ok(riffdb_service::ExecuteSymbolicQueryResult::transport_residual_fixture())
        })
    }
}

impl SymbolicQueryApplication for ProjectionService {
    denied_operation!(
        describe_symbolic_contract,
        RequestContext,
        riffdb_service::SymbolicContractSelector,
        riffdb_service::DescribeSymbolicContractResult
    );
    denied_operation!(
        check_symbolic_query,
        RequestContext,
        riffdb_service::CompileSymbolicQueryRequest,
        riffdb_service::CheckSymbolicQueryResult
    );
    denied_operation!(
        explain_symbolic_query,
        RequestContext,
        riffdb_service::CompileSymbolicQueryRequest,
        riffdb_service::ExplainSymbolicQueryResult
    );
    fn execute_symbolic_query(
        &self,
        _context: RequestContext,
        _request: riffdb_service::ExecuteSymbolicQueryRequest,
    ) -> ServiceFuture<'_, riffdb_service::ExecuteSymbolicQueryResult> {
        self.residual_symbolic_query()
    }
    denied_operation!(
        deploy_query_module,
        RequestContext,
        riffdb_service::DeployQueryModuleRequest,
        riffdb_service::DeployQueryModuleResult
    );
    denied_operation!(
        get_query_module,
        RequestContext,
        riffdb_service::GetQueryModuleRequest,
        Option<riffdb_service::QueryModuleInspection>
    );
    denied_operation!(
        explain_named_symbolic_query,
        RequestContext,
        riffdb_service::NamedSymbolicQueryRequest,
        riffdb_service::ExplainSymbolicQueryResult
    );
    fn execute_named_symbolic_query(
        &self,
        _context: RequestContext,
        _request: riffdb_service::NamedSymbolicQueryRequest,
    ) -> ServiceFuture<'_, riffdb_service::ExecuteSymbolicQueryResult> {
        self.residual_symbolic_query()
    }
}

impl riffdb_service::ProjectedQueryApplication for ProjectionService {
    denied_operation!(
        execute_projected_query,
        RequestContext,
        riffdb_service::ExecuteProjectedQueryRequest,
        riffdb_service::ExecuteProjectedQueryResult
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

struct CountingAcceptingAuthenticator {
    principal: AuthenticatedPrincipal,
    calls: AtomicUsize,
}

impl CountingAcceptingAuthenticator {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl CredentialAuthenticator for CountingAcceptingAuthenticator {
    fn authenticate(
        &self,
        _credential: OpaqueCredential<'_>,
        _context: &AuthenticationContext,
    ) -> Result<AuthenticatedPrincipal, AuthenticationFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.principal.clone())
    }
}

struct RecordingRecoveryService {
    invocations: Mutex<Vec<String>>,
}

impl RecordingRecoveryService {
    fn new() -> Self {
        Self {
            invocations: Mutex::new(Vec::new()),
        }
    }

    fn invocations(&self) -> Vec<String> {
        self.invocations
            .lock()
            .expect("recovery invocation lock remains available")
            .clone()
    }
}

impl RecoveryOfflineMaintenanceApplication for RecordingRecoveryService {
    fn restore_offline_backup(
        &self,
        invocation: RecoveryRestoreOfflineBackupInvocation,
    ) -> ServiceFuture<'_, OfflineMaintenanceStartResult> {
        self.invocations
            .lock()
            .expect("recovery invocation lock remains available")
            .push(format!("{invocation:?}"));
        Box::pin(async { Ok(maintenance_restore_start_result()) })
    }
}

struct MaintenanceRoute {
    ready: Option<Arc<dyn ApplicationService>>,
    recovery: Option<Arc<dyn RecoveryOfflineMaintenanceApplication>>,
    security: Option<CheckedGrpcSecurityContext>,
    ready_admissions: Mutex<Vec<GrpcOfflineMaintenanceOperation>>,
    recovery_admissions: Mutex<Vec<(OfflineMaintenanceOperationId, OfflineMaintenanceInputHash)>>,
    security_fetches: AtomicUsize,
}

impl MaintenanceRoute {
    fn ready_admissions(&self) -> Vec<GrpcOfflineMaintenanceOperation> {
        self.ready_admissions
            .lock()
            .expect("maintenance admission lock remains available")
            .clone()
    }

    fn recovery_admissions(
        &self,
    ) -> Vec<(OfflineMaintenanceOperationId, OfflineMaintenanceInputHash)> {
        self.recovery_admissions
            .lock()
            .expect("recovery admission lock remains available")
            .clone()
    }

    fn security_fetches(&self) -> usize {
        self.security_fetches.load(Ordering::SeqCst)
    }
}

impl GrpcLifecycleRoute for MaintenanceRoute {
    fn admit_authenticated(
        &self,
        _operation: ServiceOperationV1,
    ) -> Option<Arc<dyn ApplicationService>> {
        None
    }

    fn admit_offline_maintenance(
        &self,
        operation: GrpcOfflineMaintenanceOperation,
    ) -> Option<Arc<dyn ApplicationService>> {
        self.ready_admissions
            .lock()
            .expect("maintenance admission lock remains available")
            .push(operation);
        self.ready.clone()
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
        operation_id: OfflineMaintenanceOperationId,
        input_hash: OfflineMaintenanceInputHash,
    ) -> Option<Arc<dyn RecoveryOfflineMaintenanceApplication>> {
        self.recovery_admissions
            .lock()
            .expect("recovery admission lock remains available")
            .push((operation_id, input_hash));
        self.recovery.clone()
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

    fn history_incarnation(&self) -> Option<u64> {
        Some(1)
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

struct ActiveRoute {
    service: Arc<dyn ApplicationService>,
    security: CheckedGrpcSecurityContext,
    read_stage_telemetry: Option<Arc<dyn riffdb_service::ServiceTelemetry>>,
}

impl GrpcLifecycleRoute for ActiveRoute {
    fn admit_authenticated(
        &self,
        operation: ServiceOperationV1,
    ) -> Option<Arc<dyn ApplicationService>> {
        matches!(
            operation,
            ServiceOperationV1::QueryProjection
                | ServiceOperationV1::GetContractVersion
                | ServiceOperationV1::ExecuteCommand
                | ServiceOperationV1::ResolveCommandOutcome
                | ServiceOperationV1::GetProjectionStatus
                | ServiceOperationV1::TraceProvenance
                | ServiceOperationV1::ListPendingOutboxDeliveries
                | ServiceOperationV1::DiscoverCommandTools
                | ServiceOperationV1::DiscoverResources
                | ServiceOperationV1::DescribeEvent
                | ServiceOperationV1::ExecuteQuery
        )
        .then(|| Arc::clone(&self.service))
    }

    fn admit_offline_maintenance(
        &self,
        _operation: riffdb_api_grpc::GrpcOfflineMaintenanceOperation,
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
    ) -> Option<Arc<dyn riffdb_service::RecoveryOfflineMaintenanceApplication>> {
        None
    }

    fn security_context(&self) -> Option<CheckedGrpcSecurityContext> {
        Some(self.security.clone())
    }

    fn restore_retry_security_context(&self) -> Option<CheckedGrpcRestoreRetrySecurityContext> {
        None
    }

    fn server_generation(&self) -> Option<[u8; 16]> {
        Some(SERVER_GENERATION)
    }

    fn history_incarnation(&self) -> Option<u64> {
        Some(1)
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

    fn read_stage_telemetry(&self) -> Option<Arc<dyn riffdb_service::ServiceTelemetry>> {
        self.read_stage_telemetry.clone()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ready_offline_maintenance_uses_one_shared_service_and_exact_current_authentication() {
    let database_id =
        DatabaseId::from_unix_milliseconds_and_random(21, [0x21; 10]).expect("valid database ID");
    let environment = Environment::new("grpc-maintenance").expect("valid environment");
    let audience = Audience::new("grpc-loopback").expect("valid audience");
    let principal = authenticated_principal(database_id, environment.clone(), audience.clone());
    let authenticator = Arc::new(CountingAcceptingAuthenticator {
        principal,
        calls: AtomicUsize::new(0),
    });
    let security = CheckedGrpcSecurityContext::new(
        authenticator.clone(),
        AuthenticationContext::new(database_id, environment, audience),
        Arc::new(
            CapabilityDigestKeyProvider::parse_document(CAPABILITY_KEYS)
                .expect("valid capability key fixture"),
        ),
    );
    let service = Arc::new(ProjectionService::new());
    let shared_service: Arc<dyn ApplicationService> = service.clone();
    let route = Arc::new(MaintenanceRoute {
        ready: Some(shared_service),
        recovery: None,
        security: Some(security),
        ready_admissions: Mutex::new(Vec::new()),
        recovery_admissions: Mutex::new(Vec::new()),
        security_fetches: AtomicUsize::new(0),
    });
    let application = GrpcApplication::new(
        route.clone(),
        GrpcRequestLimits::new(Duration::from_secs(30)).expect("bounded request duration"),
    );

    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().expect("loopback address"))
        .expect("bind loopback listener");
    let address = incoming.local_addr().expect("bound loopback address");
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let server = tokio::spawn(
        Server::builder()
            .add_service(application.admin_server())
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_receiver.await;
            }),
    );
    let endpoint =
        Endpoint::from_shared(format!("http://{address}")).expect("valid loopback endpoint");
    let channel = endpoint.connect().await.expect("connect loopback client");
    let mut client = AdminServiceClient::new(channel);

    let create_operation_id = maintenance_operation_id(1);
    let mut create = Request::new(v1::CreateOfflineBackupRequest {
        request_id: request_id(21).into_bytes().to_vec(),
        operation_id: create_operation_id.into_bytes().to_vec(),
        backup_name: "nightly".to_owned(),
    });
    authorize(&mut create);
    let create = client
        .create_offline_backup(create)
        .await
        .expect("ready create reaches shared service")
        .into_inner();
    assert_eq!(
        create.disposition,
        v1::OfflineMaintenanceStartDisposition::Accepted as i32
    );
    assert_eq!(
        create.operation.expect("create observation").operation_id,
        create_operation_id.as_bytes()
    );
    assert_eq!(authenticator.calls(), 1);

    let restore_operation_id = maintenance_operation_id(2);
    let mut restore = Request::new(v1::RestoreOfflineBackupRequest {
        request_id: request_id(22).into_bytes().to_vec(),
        operation_id: restore_operation_id.into_bytes().to_vec(),
        backup_name: "restore".to_owned(),
        replacement_confirmation:
            v1::OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget as i32,
    });
    authorize(&mut restore);
    let restore = client
        .restore_offline_backup(restore)
        .await
        .expect("ready restore reaches shared service")
        .into_inner();
    assert_eq!(
        restore.disposition,
        v1::OfflineMaintenanceStartDisposition::Accepted as i32
    );
    assert_eq!(
        restore.operation.expect("restore observation").operation_id,
        restore_operation_id.as_bytes()
    );
    assert_eq!(
        authenticator.calls(),
        2,
        "ready restore authenticates the current database exactly once"
    );

    let mut poll = Request::new(v1::GetOfflineMaintenanceOperationRequest {
        request_id: request_id(23).into_bytes().to_vec(),
        operation_id: create_operation_id.into_bytes().to_vec(),
    });
    authorize(&mut poll);
    let poll = client
        .get_offline_maintenance_operation(poll)
        .await
        .expect("ready poll reaches shared service")
        .into_inner();
    assert!(matches!(
        poll.result,
        Some(v1::get_offline_maintenance_operation_response::Result::Found(_))
    ));
    assert_eq!(authenticator.calls(), 3);

    assert_eq!(
        route.ready_admissions(),
        vec![
            GrpcOfflineMaintenanceOperation::CreateBackup,
            GrpcOfflineMaintenanceOperation::RestoreBackup {
                operation_id: restore_operation_id,
                input_hash: restore_input_hash("restore"),
            },
            GrpcOfflineMaintenanceOperation::GetOperation,
        ]
    );
    assert!(route.recovery_admissions().is_empty());
    assert_eq!(route.security_fetches(), 3);
    assert_eq!(
        service.maintenance_invocations(),
        vec![
            ObservedMaintenanceInvocation::Create {
                request_id: request_id(21),
                operation_id: create_operation_id,
                backup_name: "nightly".to_owned(),
            },
            ObservedMaintenanceInvocation::Restore {
                redacted_invocation: "RestoreOfflineBackupInvocation([REDACTED])".to_owned(),
            },
            ObservedMaintenanceInvocation::Get {
                request_id: request_id(23),
                operation_id: create_operation_id,
            },
        ]
    );

    drop(client);
    shutdown_sender.send(()).expect("server still running");
    server
        .await
        .expect("server task did not panic")
        .expect("server shut down cleanly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_mode_exposes_only_restore_and_performs_no_current_authentication() {
    let database_id =
        DatabaseId::from_unix_milliseconds_and_random(31, [0x31; 10]).expect("valid database ID");
    let environment = Environment::new("grpc-recovery").expect("valid environment");
    let audience = Audience::new("grpc-loopback").expect("valid audience");
    let principal = authenticated_principal(database_id, environment.clone(), audience.clone());
    let authenticator = Arc::new(CountingAcceptingAuthenticator {
        principal,
        calls: AtomicUsize::new(0),
    });
    let security = CheckedGrpcSecurityContext::new(
        authenticator.clone(),
        AuthenticationContext::new(database_id, environment, audience),
        Arc::new(
            CapabilityDigestKeyProvider::parse_document(CAPABILITY_KEYS)
                .expect("valid capability key fixture"),
        ),
    );
    let recovery = Arc::new(RecordingRecoveryService::new());
    let recovery_service: Arc<dyn RecoveryOfflineMaintenanceApplication> = recovery.clone();
    let route = Arc::new(MaintenanceRoute {
        ready: None,
        recovery: Some(recovery_service),
        security: Some(security),
        ready_admissions: Mutex::new(Vec::new()),
        recovery_admissions: Mutex::new(Vec::new()),
        security_fetches: AtomicUsize::new(0),
    });
    let application = GrpcApplication::new(
        route.clone(),
        GrpcRequestLimits::new(Duration::from_secs(30)).expect("bounded request duration"),
    );

    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().expect("loopback address"))
        .expect("bind loopback listener");
    let address = incoming.local_addr().expect("bound loopback address");
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let server = tokio::spawn(
        Server::builder()
            .add_service(application.admin_server())
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_receiver.await;
            }),
    );
    let endpoint =
        Endpoint::from_shared(format!("http://{address}")).expect("valid loopback endpoint");
    let channel = endpoint.connect().await.expect("connect loopback client");
    let mut client = AdminServiceClient::new(channel);

    let operation_id = maintenance_operation_id(2);
    let mut create = Request::new(v1::CreateOfflineBackupRequest {
        request_id: request_id(31).into_bytes().to_vec(),
        operation_id: operation_id.into_bytes().to_vec(),
        backup_name: "restore".to_owned(),
    });
    authorize(&mut create);
    assert_eq!(
        client
            .create_offline_backup(create)
            .await
            .expect_err("recovery mode cannot create a backup")
            .code(),
        Code::Unavailable
    );

    let mut poll = Request::new(v1::GetOfflineMaintenanceOperationRequest {
        request_id: request_id(32).into_bytes().to_vec(),
        operation_id: operation_id.into_bytes().to_vec(),
    });
    authorize(&mut poll);
    assert_eq!(
        client
            .get_offline_maintenance_operation(poll)
            .await
            .expect_err("recovery mode cannot poll receipts")
            .code(),
        Code::Unavailable
    );

    let mut restore = Request::new(v1::RestoreOfflineBackupRequest {
        request_id: request_id(33).into_bytes().to_vec(),
        operation_id: operation_id.into_bytes().to_vec(),
        backup_name: "restore".to_owned(),
        replacement_confirmation:
            v1::OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget as i32,
    });
    authorize(&mut restore);
    let restore = client
        .restore_offline_backup(restore)
        .await
        .expect("restricted recovery restore reaches recovery service")
        .into_inner();
    assert_eq!(
        restore.disposition,
        v1::OfflineMaintenanceStartDisposition::Accepted as i32
    );
    assert_eq!(
        authenticator.calls(),
        0,
        "recovery restore must not authenticate against current state"
    );
    assert_eq!(route.security_fetches(), 0);
    assert_eq!(
        route.recovery_admissions(),
        vec![(operation_id, restore_input_hash("restore"))],
        "recovery admission receives the structurally checked receipt identity"
    );
    assert_eq!(
        route.ready_admissions(),
        vec![
            GrpcOfflineMaintenanceOperation::CreateBackup,
            GrpcOfflineMaintenanceOperation::GetOperation,
            GrpcOfflineMaintenanceOperation::RestoreBackup {
                operation_id,
                input_hash: restore_input_hash("restore"),
            },
        ]
    );
    assert_eq!(
        recovery.invocations(),
        vec!["RecoveryRestoreOfflineBackupInvocation([REDACTED])"]
    );

    drop(client);
    shutdown_sender.send(()).expect("server still running");
    server
        .await
        .expect("server task did not panic")
        .expect("server shut down cleanly");
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
        read_stage_telemetry: None,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_failure_boundaries_remain_fail_closed_and_exact() {
    let database_id =
        DatabaseId::from_unix_milliseconds_and_random(3, [3; 10]).expect("valid database ID");
    let environment = Environment::new("grpc-failures").expect("valid environment");
    let audience = Audience::new("grpc-loopback").expect("valid audience");
    let current_authority = Arc::new(authorization_fixture(
        database_id,
        environment.clone(),
        audience.clone(),
    ));
    let principal = current_authority.authenticated_principal().clone();

    let service = Arc::new(ProjectionService::with_current_authority(Arc::clone(
        &current_authority,
    )));
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
        read_stage_telemetry: None,
    });
    let application = GrpcApplication::new(
        route,
        GrpcRequestLimits::new(Duration::from_millis(100)).expect("bounded request duration"),
    );

    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().expect("loopback address"))
        .expect("bind loopback listener");
    let address = incoming.local_addr().expect("bound loopback address");
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let server = tokio::spawn(
        Server::builder()
            .add_service(application.contract_server())
            .add_service(application.query_server())
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_receiver.await;
            }),
    );

    let endpoint =
        Endpoint::from_shared(format!("http://{address}")).expect("valid loopback endpoint");
    let channel = endpoint.connect().await.expect("connect loopback client");
    let mut query_client = QueryServiceClient::new(channel.clone());

    let status = query_client
        .query_projection(Request::new(projection_request(request_id(30), 1, false)))
        .await
        .expect_err("missing authentication must fail before service admission");
    assert_eq!(status.code(), Code::Unauthenticated);
    assert_eq!(status.message(), UNAUTHENTICATED_MESSAGE);
    assert!(status.details().is_empty());
    assert!(service.observed().is_empty());

    let (deadline_started, deadline_cancelled) = service.prepare_pending_probe();
    let mut deadline_request = Request::new(projection_request(request_id(31), 6, false));
    authorize(&mut deadline_request);
    deadline_request
        .metadata_mut()
        .insert("grpc-timeout", MetadataValue::from_static("10S"));
    let mut deadline_client = query_client.clone();
    let deadline_call =
        tokio::spawn(async move { deadline_client.query_projection(deadline_request).await });
    await_probe(deadline_started, "deadline request reached the service").await;
    let status = deadline_call
        .await
        .expect("deadline client task did not panic")
        .expect_err("in-flight request must observe its gRPC deadline");
    assert_eq!(status.code(), Code::DeadlineExceeded);
    assert_eq!(status.message(), DEADLINE_EXCEEDED_MESSAGE);
    assert!(status.details().is_empty());
    assert!(service.shortened_deadline_observed());
    await_probe(
        deadline_cancelled,
        "deadline dropped the server future and cancelled RequestControl",
    )
    .await;

    let (cancellation_started, cancellation_observed) = service.prepare_pending_probe();
    let mut cancellation_request = Request::new(projection_request(request_id(32), 7, false));
    authorize(&mut cancellation_request);
    let mut cancellation_client = query_client.clone();
    let cancellation_call = tokio::spawn(async move {
        cancellation_client
            .query_projection(cancellation_request)
            .await
    });
    await_probe(
        cancellation_started,
        "cancellable request reached the service",
    )
    .await;
    cancellation_call.abort();
    assert!(
        cancellation_call
            .await
            .expect_err("aborted client call must not complete")
            .is_cancelled()
    );
    await_probe(
        cancellation_observed,
        "dropping the client RPC cancelled RequestControl",
    )
    .await;

    let mut contract_client = ContractServiceClient::new(channel);
    // Full policy/cursor orchestration remains covered in tests/service. This
    // adapter schedule uses the production current-capability resolver and a
    // stateful issue/invalidate lifecycle so transport errors are not fabricated.
    let mut issued_cursors = Vec::new();
    for ordinal in [33, 34] {
        let mut first_page = Request::new(resource_first_page_request(ordinal));
        authorize(&mut first_page);
        let response = contract_client
            .discover_resources(first_page)
            .await
            .expect("active current authority may discover one resource")
            .into_inner();
        let Some(v1::discover_resources_response::Result::Page(page)) = response.result else {
            panic!("full resource discovery must return a page");
        };
        issued_cursors.push(
            page.next_cursor
                .expect("nonterminal conformance page cursor"),
        );
    }

    service.invalidate_resource_cursor(&issued_cursors[0]);
    let mut invalid_cursor_request =
        Request::new(resource_continuation_request(35, issued_cursors[0].clone()));
    authorize(&mut invalid_cursor_request);
    let status = contract_client
        .discover_resources(invalid_cursor_request)
        .await
        .expect_err("an invalidated issued cursor must fail closed");
    let invalid_cursor = PublicError::validation(ValidationIssues::one(ValidationIssue::new(
        ValidationCode::InvalidValue,
        ValidationPath::root(),
    )));
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(status.message(), invalid_cursor.safe_message());
    assert_eq!(decode_public_error(status.details()), Ok(invalid_cursor));

    current_authority
        .revoke_current(timestamp(160))
        .expect("current test capability revokes exactly once");
    let mut stale_policy_request =
        Request::new(resource_continuation_request(36, issued_cursors[1].clone()));
    authorize(&mut stale_policy_request);
    let status = contract_client
        .discover_resources(stale_policy_request)
        .await
        .expect_err("fresh current-authority denial must discard the valid continuation");
    let denied = PublicError::authorization_denied();
    assert_eq!(status.code(), Code::PermissionDenied);
    assert_eq!(status.message(), denied.safe_message());
    assert_eq!(decode_public_error(status.details()), Ok(denied));

    let operations = service
        .observed()
        .into_iter()
        .map(|observation| observation.operation)
        .collect::<Vec<_>>();
    assert_eq!(
        operations,
        vec![
            ServiceOperationV1::QueryProjection,
            ServiceOperationV1::QueryProjection,
            ServiceOperationV1::DiscoverResources,
            ServiceOperationV1::DiscoverResources,
            ServiceOperationV1::DiscoverResources,
            ServiceOperationV1::DiscoverResources,
        ]
    );

    drop(contract_client);
    drop(query_client);
    shutdown_sender.send(()).expect("server still running");
    server
        .await
        .expect("server task did not panic")
        .expect("server shut down cleanly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wp137_unary_surface_crosses_authenticated_loopback_grpc() {
    let database_id =
        DatabaseId::from_unix_milliseconds_and_random(2, [2; 10]).expect("valid database ID");
    let environment = Environment::new("grpc-wp137").expect("valid environment");
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
        read_stage_telemetry: None,
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
            .add_service(application.contract_server())
            .add_service(application.command_server())
            .add_service(application.query_server())
            .add_service(application.commit_server())
            .add_service(application.event_server())
            .add_service(application.admin_server())
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_receiver.await;
            }),
    );

    let endpoint =
        Endpoint::from_shared(format!("http://{address}")).expect("valid loopback endpoint");
    let channel = endpoint.connect().await.expect("connect loopback client");
    let mut client = RiffDbClient::from_channel(channel);
    let metadata = CallMetadata::authenticated(
        BearerCredential::new(CAPABILITY_TOKEN).expect("valid credential presentation"),
    );
    let request_ids = (10_u8..=19).map(request_id).collect::<Vec<_>>();

    let contract = client
        .get_contract_version(
            v1::GetContractVersionRequest {
                request_id: request_ids[0].into_bytes().to_vec(),
                contract_lineage: LINEAGE.to_owned(),
                contract_version: 1,
            },
            &metadata,
        )
        .await
        .expect("historical contract response");
    let Some(v1::get_contract_version_response::Result::Found(contract)) = contract.result else {
        panic!("expected historical contract");
    };
    assert_eq!(contract.contract_lineage, LINEAGE);
    assert_eq!(contract.contract_version, 1);
    assert_eq!(contract.bundle_hash, vec![0x11; 32]);

    let projection = client
        .get_projection_status(
            v1::GetProjectionStatusRequest {
                request_id: request_ids[1].into_bytes().to_vec(),
                contract: Some(exact_contract_selection()),
                projection_id: 1,
            },
            &metadata,
        )
        .await
        .expect("projection status response");
    let Some(v1::get_projection_status_response::Result::Found(projection)) = projection.result
    else {
        panic!("expected projection status");
    };
    assert_eq!(
        projection.lifecycle,
        v1::ProjectionLifecycle::Building as i32
    );
    assert_eq!(
        projection
            .identity
            .expect("projection identity")
            .contract_lineage,
        LINEAGE
    );
    assert_applied_through(projection.authoritative_head, 1);

    let provenance = client
        .trace_provenance(
            v1::TraceProvenanceRequest {
                request_id: request_ids[2].into_bytes().to_vec(),
                selector: Some(v1::ProvenanceSelection {
                    selection: Some(v1::provenance_selection::Selection::CommitSequence(1)),
                }),
            },
            &metadata,
        )
        .await
        .expect("provenance response");
    assert!(matches!(
        provenance.result,
        Some(v1::trace_provenance_response::Result::NotFound(_))
    ));

    let outbox = client
        .list_pending_outbox_deliveries(
            v1::ListPendingOutboxDeliveriesRequest {
                request_id: request_ids[3].into_bytes().to_vec(),
                page: Some(wire_page(2)),
            },
            &metadata,
        )
        .await
        .expect("outbox response");
    let outbox_page = outbox.page.expect("outbox page");
    assert!(outbox_page.items.is_empty());
    assert!(outbox_page.next_cursor.is_none());

    let resources = client
        .discover_resources(
            v1::DiscoverResourcesRequest {
                request_id: request_ids[4].into_bytes().to_vec(),
                page: Some(wire_page(2)),
                prior_fence: None,
                representation: v1::DiscoveryRepresentation::Full as i32,
                kind: v1::ResourceDiscoveryKind::Concrete as i32,
            },
            &metadata,
        )
        .await
        .expect("resource discovery response");
    let Some(v1::discover_resources_response::Result::Page(resources)) = resources.result else {
        panic!("expected full resource page");
    };
    assert!(matches!(
        resources.items.as_slice(),
        [v1::ResourceDescriptor {
            resource: Some(v1::resource_descriptor::Resource::ServerHealth(_))
        }]
    ));
    assert_eq!(
        resources
            .observed_fence
            .expect("resource discovery fence")
            .server_generation,
        SERVER_GENERATION
    );

    let full_tools = client
        .discover_command_tools(
            v1::DiscoverCommandToolsRequest {
                request_id: request_ids[5].into_bytes().to_vec(),
                page: Some(wire_page(2)),
                prior_fence: None,
                representation: v1::DiscoveryRepresentation::Full as i32,
            },
            &metadata,
        )
        .await
        .expect("full command discovery response");
    let Some(v1::discover_command_tools_response::Result::Page(full_tools)) = full_tools.result
    else {
        panic!("expected full command-tool page");
    };
    assert!(full_tools.operation_schemas.is_some());
    assert!(matches!(
        full_tools.items.as_slice(),
        [v1::CommandToolDiscoveryItem {
            item: Some(v1::command_tool_discovery_item::Item::FixedTool(kind))
        }] if *kind == v1::FixedToolKind::GetHealth as i32
    ));
    let current_fence = full_tools.observed_fence.expect("command discovery fence");
    assert_eq!(current_fence.server_generation, SERVER_GENERATION);

    let unchanged = client
        .discover_command_tools(
            v1::DiscoverCommandToolsRequest {
                request_id: request_ids[6].into_bytes().to_vec(),
                page: Some(wire_page(2)),
                prior_fence: Some(current_fence.clone()),
                representation: v1::DiscoveryRepresentation::CompactObservation as i32,
            },
            &metadata,
        )
        .await
        .expect("unchanged command discovery response");
    let Some(v1::discover_command_tools_response::Result::CatalogUnchanged(unchanged)) =
        unchanged.result
    else {
        panic!("expected unchanged command catalog");
    };
    assert_eq!(unchanged.server_generation, SERVER_GENERATION);

    let mut stale_fence = current_fence;
    stale_fence.server_generation = vec![0x5a; 16];
    let refreshed = client
        .discover_command_tools(
            v1::DiscoverCommandToolsRequest {
                request_id: request_ids[7].into_bytes().to_vec(),
                page: Some(wire_page(2)),
                prior_fence: Some(stale_fence),
                representation: v1::DiscoveryRepresentation::CompactObservation as i32,
            },
            &metadata,
        )
        .await
        .expect("refreshed command discovery response");
    let Some(v1::discover_command_tools_response::Result::CompactPage(refreshed)) =
        refreshed.result
    else {
        panic!("stale generation must force a normal compact page");
    };
    assert_eq!(
        refreshed
            .observed_fence
            .expect("refreshed command discovery fence")
            .server_generation,
        SERVER_GENERATION
    );

    let outcome = client
        .get_outcome(
            v1::GetOutcomeRequest {
                request_id: request_ids[8].into_bytes().to_vec(),
                contract_lineage: String::new(),
                command_name: String::new(),
                idempotency_key: String::new(),
                outcome_uri: Some(OUTCOME_LOCATOR.to_owned()),
            },
            &metadata,
        )
        .await
        .expect("locator-form outcome response");
    assert!(matches!(
        outcome.result,
        Some(v1::get_outcome_response::Result::NotFound(_))
    ));

    let event = client
        .describe_event(
            v1::DescribeEventRequest {
                request_id: request_ids[9].into_bytes().to_vec(),
                event_name: "BudgetAllocated".to_owned(),
            },
            &metadata,
        )
        .await
        .expect("event description response");
    assert!(matches!(
        event.result,
        Some(v1::describe_event_response::Result::NotFound(_))
    ));

    assert_eq!(service.command_discovery_prior(), vec![false, true, false]);
    assert_eq!(service.outcome_locators(), vec![OUTCOME_LOCATOR]);
    let expected_operations = [
        ServiceOperationV1::GetContractVersion,
        ServiceOperationV1::GetProjectionStatus,
        ServiceOperationV1::TraceProvenance,
        ServiceOperationV1::ListPendingOutboxDeliveries,
        ServiceOperationV1::DiscoverResources,
        ServiceOperationV1::DiscoverCommandTools,
        ServiceOperationV1::DiscoverCommandTools,
        ServiceOperationV1::DiscoverCommandTools,
        ServiceOperationV1::ResolveCommandOutcome,
        ServiceOperationV1::DescribeEvent,
    ];
    let observed = service.observed();
    assert_eq!(observed.len(), expected_operations.len());
    for ((observation, expected_operation), expected_request_id) in
        observed.iter().zip(expected_operations).zip(request_ids)
    {
        assert_eq!(observation.request_id, expected_request_id);
        assert_eq!(observation.operation, expected_operation);
        assert_eq!(observation.capability_id, expected_capability);
        assert_eq!(observation.principal_id, expected_actor);
        assert_eq!(observation.ingress, ServiceIngressKindV1::Grpc);
    }

    drop(client);
    shutdown_sender.send(()).expect("server still running");
    server
        .await
        .expect("server task did not panic")
        .expect("server shut down cleanly");
}

fn contract_descriptor() -> ContractDescriptor {
    ContractDescriptor::genesis(
        ContractLineage::new(LINEAGE).expect("valid contract lineage"),
        ContractVersion::new(1).expect("nonzero contract version"),
        ContractBundleHash::from_bytes([0x11; 32]),
        SourceHash::from_bytes([0x22; 32]),
        ContractPlanRootHash::from_bytes([0x33; 32]),
    )
}

fn projection_identity() -> ProjectionIdentity {
    ProjectionIdentity::new(
        ContractLineage::new(LINEAGE).expect("valid projection lineage"),
        ProjectionId::first(),
        ProjectionPlanHash::from_bytes([7; 32]),
    )
}

fn discovery_fence() -> DiscoveryCatalogFence {
    let operation_schemas = OperationSchemaCatalog::accepted().expect("accepted test schemas");
    DiscoveryCatalogFence::no_active_contract(operation_schemas.identity())
}

fn exact_contract_selection() -> v1::ContractSelection {
    v1::ContractSelection {
        selection: Some(v1::contract_selection::Selection::Exact(
            v1::ExactContractSelection {
                contract_lineage: LINEAGE.to_owned(),
                contract_version: 1,
            },
        )),
    }
}

fn wire_page(limit: u32) -> v1::PageRequest {
    v1::PageRequest {
        limit: Some(limit),
        cursor: None,
    }
}

fn resource_first_page_request(ordinal: u8) -> v1::DiscoverResourcesRequest {
    v1::DiscoverResourcesRequest {
        request_id: request_id(ordinal).into_bytes().to_vec(),
        page: Some(v1::PageRequest {
            limit: Some(1),
            cursor: None,
        }),
        prior_fence: None,
        representation: v1::DiscoveryRepresentation::Full as i32,
        kind: v1::ResourceDiscoveryKind::All as i32,
    }
}

fn resource_continuation_request(ordinal: u8, cursor: Vec<u8>) -> v1::DiscoverResourcesRequest {
    v1::DiscoverResourcesRequest {
        request_id: request_id(ordinal).into_bytes().to_vec(),
        page: Some(v1::PageRequest {
            limit: Some(1),
            cursor: Some(cursor),
        }),
        prior_fence: None,
        representation: v1::DiscoveryRepresentation::Full as i32,
        kind: v1::ResourceDiscoveryKind::All as i32,
    }
}

async fn await_probe(receiver: oneshot::Receiver<()>, expectation: &'static str) {
    tokio::time::timeout(Duration::from_secs(2), receiver)
        .await
        .unwrap_or_else(|_| panic!("timed out: {expectation}"))
        .unwrap_or_else(|_| panic!("probe sender dropped: {expectation}"));
}

fn authorize<T>(request: &mut Request<T>) {
    request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {CAPABILITY_TOKEN}"))
            .expect("valid authorization metadata"),
    );
}

fn authenticated_principal(
    database_id: DatabaseId,
    environment: Environment,
    audience: Audience,
) -> AuthenticatedPrincipal {
    authorization_fixture(database_id, environment, audience)
        .authenticated_principal()
        .clone()
}

fn authorization_fixture(
    database_id: DatabaseId,
    environment: Environment,
    audience: Audience,
) -> AuthorizationFixture {
    let grant = CapabilityGrantV1::new(
        TenantScope::Global,
        PartitionScopeV1::All,
        CapabilityPermissionsV1::new(Vec::new()).expect("empty permission set is valid"),
        Vec::new(),
        NonZeroU16::MIN,
        Vec::new(),
    )
    .expect("valid test grant");
    AuthorizationFixture::new(AuthorizationFixtureConfig::new(
        database_id,
        environment,
        ActorId::new("grpc-principal").expect("valid principal"),
        ActorKind::Human,
        audience,
        AuthorizationFixtureTimes::new(timestamp(100), timestamp(200), timestamp(150)),
        grant,
    ))
    .expect("valid authorization fixture")
}

fn timestamp(seconds: i64) -> Timestamp {
    Timestamp::new(seconds, 0).expect("canonical timestamp")
}

fn maintenance_operation_id(ordinal: u8) -> OfflineMaintenanceOperationId {
    OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(
        u64::from(ordinal),
        [ordinal; 10],
    )
    .expect("valid maintenance operation ID")
}

fn restore_input_hash(name: &str) -> OfflineMaintenanceInputHash {
    offline_maintenance_input_hash(
        OfflineMaintenanceOperationKind::RestoreBackup,
        &BackupNameV1::new(name).expect("valid backup name"),
        OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget,
    )
}

fn maintenance_create_observation(
    operation_id: OfflineMaintenanceOperationId,
) -> OfflineMaintenanceOperationObservation {
    let backup_name = BackupNameV1::new("nightly").expect("valid backup name");
    let request =
        riffdb_service::CreateOfflineBackupRequest::new(operation_id, backup_name.clone())
            .expect("valid create request");
    OfflineMaintenanceOperationObservation::new(
        operation_id,
        OfflineMaintenanceOperationKind::CreateBackup,
        backup_name,
        request.input_hash(),
        OfflineMaintenanceObservationPhase::Accepted,
        None,
    )
    .expect("valid create observation")
}

fn maintenance_restore_start_result() -> OfflineMaintenanceStartResult {
    let operation_id = maintenance_operation_id(2);
    let backup_name = BackupNameV1::new("restore").expect("valid backup name");
    let request = riffdb_service::RestoreOfflineBackupRequest::new(
        operation_id,
        backup_name.clone(),
        OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget,
    )
    .expect("valid restore request");
    let observation = OfflineMaintenanceOperationObservation::new(
        operation_id,
        OfflineMaintenanceOperationKind::RestoreBackup,
        backup_name,
        request.input_hash(),
        OfflineMaintenanceObservationPhase::Accepted,
        None,
    )
    .expect("valid restore observation");
    OfflineMaintenanceStartResult::new(OfflineMaintenanceStartDisposition::Accepted, observation)
        .expect("valid restore start result")
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

fn empty_command_input() -> v1::Value {
    v1::Value {
        kind: Some(v1::value::Kind::RecordValue(v1::ValueRecord {
            fields: Vec::new(),
        })),
    }
}

fn batch_command(ordinal: u8, name: &str) -> v1::ExecuteCommandRequest {
    v1::ExecuteCommandRequest {
        request_id: request_id(ordinal).into_bytes().to_vec(),
        command_name: name.to_owned(),
        expected_contract_version: None,
        input: Some(empty_command_input()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn execute_batch_carries_per_item_results_over_authenticated_loopback() {
    let database_id =
        DatabaseId::from_unix_milliseconds_and_random(77, [0x77; 10]).expect("valid database ID");
    let environment = Environment::new("grpc-batch").expect("valid environment");
    let audience = Audience::new("grpc-loopback").expect("valid audience");
    let principal = authenticated_principal(database_id, environment.clone(), audience.clone());
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
        read_stage_telemetry: None,
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
            .add_service(application.command_server())
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_receiver.await;
            }),
    );

    let endpoint =
        Endpoint::from_shared(format!("http://{address}")).expect("valid loopback endpoint");
    let channel = endpoint.connect().await.expect("connect loopback client");
    let mut client = RiffDbClient::from_channel(channel);
    let metadata = CallMetadata::authenticated(
        BearerCredential::new(CAPABILITY_TOKEN).expect("valid credential presentation"),
    );

    // Mixed batch: distinct successes + capacity rejection — input order preserved
    // by plan-hash discrimination, legacy field 1 empty, typed error names batch op.
    let mixed = client
        .execute_batch(
            v1::ExecuteCommandBatchRequest {
                commands: vec![
                    batch_command(10, "OkCommandA"),
                    batch_command(11, "Overloaded"),
                    batch_command(12, "OkCommandC"),
                ],
            },
            &metadata,
        )
        .await
        .expect("mixed batch is a normal response");
    assert!(
        mixed.responses.is_empty(),
        "field 1 must be empty when any item fails"
    );
    assert_eq!(mixed.items.len(), 3);
    match mixed.items[0].result.as_ref().expect("item 0 set") {
        v1::execute_command_batch_item::Result::Response(response) => {
            assert_eq!(response.plan_hash, vec![0xA1; 32]);
            assert_eq!(response.outcome_type, "CompletedA");
        }
        v1::execute_command_batch_item::Result::Error(_) => panic!("expected success arm"),
    }
    match mixed.items[1].result.as_ref().expect("item 1 set") {
        v1::execute_command_batch_item::Result::Error(error) => {
            assert_eq!(
                error.code,
                riffdb_proto::app::v1::ApplicationErrorCode::Overloaded as i32
            );
            assert_eq!(
                error.operation,
                riffdb_proto::app::v1::ApplicationOperation::BatchCommand as i32
            );
        }
        v1::execute_command_batch_item::Result::Response(_) => {
            panic!("expected capacity error arm")
        }
    }
    match mixed.items[2].result.as_ref().expect("item 2 set") {
        v1::execute_command_batch_item::Result::Response(response) => {
            assert_eq!(response.plan_hash, vec![0xC3; 32]);
            assert_eq!(response.outcome_type, "CompletedC");
        }
        v1::execute_command_batch_item::Result::Error(_) => panic!("expected success arm"),
    }

    // Validation rejection is carried per-item (exercises InputInvalid arm).
    let invalid = client
        .execute_batch(
            v1::ExecuteCommandBatchRequest {
                commands: vec![
                    batch_command(13, "OkCommandA"),
                    batch_command(14, "InputInvalid"),
                ],
            },
            &metadata,
        )
        .await
        .expect("validation mixed batch");
    assert!(invalid.responses.is_empty());
    assert_eq!(invalid.items.len(), 2);
    match invalid.items[1].result.as_ref().expect("invalid item") {
        v1::execute_command_batch_item::Result::Error(error) => {
            assert_eq!(
                error.code,
                riffdb_proto::app::v1::ApplicationErrorCode::InputInvalid as i32
            );
        }
        v1::execute_command_batch_item::Result::Response(_) => {
            panic!("expected validation error arm")
        }
    }

    // All-success: both fields populated and positionally mirrored with distinct rows.
    let all_success = client
        .execute_batch(
            v1::ExecuteCommandBatchRequest {
                commands: vec![
                    batch_command(20, "OkCommandA"),
                    batch_command(21, "OkCommandB"),
                ],
            },
            &metadata,
        )
        .await
        .expect("all-success batch");
    assert_eq!(all_success.items.len(), 2);
    assert_eq!(all_success.responses.len(), 2);
    assert_ne!(
        all_success.responses[0].plan_hash, all_success.responses[1].plan_hash,
        "fixture responses must be distinct so reverse-mirror would fail"
    );
    for (index, item) in all_success.items.iter().enumerate() {
        let v1::execute_command_batch_item::Result::Response(response) =
            item.result.as_ref().expect("success arm")
        else {
            panic!("expected response arm at {index}");
        };
        assert_eq!(&all_success.responses[index], response);
    }
    assert_eq!(all_success.responses[0].plan_hash, vec![0xA1; 32]);
    assert_eq!(all_success.responses[1].plan_hash, vec![0xB2; 32]);

    let observed = service.observed();
    assert!(
        observed
            .iter()
            .filter(|item| item.operation == ServiceOperationV1::ExecuteCommand)
            .count()
            >= 7
    );

    shutdown_sender.send(()).expect("server still running");
    server
        .await
        .expect("server task did not panic")
        .expect("server shut down cleanly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn execute_query_records_five_residual_read_pipeline_stages() {
    use riffdb_api_grpc::generated_app::application_query_service_client::ApplicationQueryServiceClient;
    use riffdb_errors::IncidentIdSource;
    use riffdb_observability::Observability;
    use riffdb_proto::app::v1 as app_v1;
    use riffdb_service::{ReadPipelineStage, ServiceTelemetry};
    use riffdb_types::IncidentId;

    struct FixedIncidents;
    impl IncidentIdSource for FixedIncidents {
        fn next_incident_id(&self) -> Result<IncidentId, riffdb_errors::IncidentIdSourceError> {
            IncidentId::from_bytes([0xee; 16]).map_err(|_| riffdb_errors::IncidentIdSourceError)
        }
    }

    let database_id =
        DatabaseId::from_unix_milliseconds_and_random(77, [0x77; 10]).expect("valid database ID");
    let environment = Environment::new("grpc-residual").expect("valid environment");
    let audience = Audience::new("grpc-loopback").expect("valid audience");
    let principal = authenticated_principal(database_id, environment.clone(), audience.clone());

    let observability =
        Arc::new(Observability::new(Arc::new(FixedIncidents), 32).expect("bounded observability"));
    let telemetry: Arc<dyn ServiceTelemetry> = observability.clone();

    let mut service = ProjectionService::new();
    service.read_stage_telemetry = Some(Arc::clone(&telemetry));
    let application_service: Arc<dyn ApplicationService> = Arc::new(service);
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
        read_stage_telemetry: Some(telemetry),
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
            .add_service(application.application_query_server())
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_receiver.await;
            }),
    );

    let endpoint =
        Endpoint::from_shared(format!("http://{address}")).expect("valid loopback endpoint");
    let channel = endpoint.connect().await.expect("connect loopback client");
    let mut client = ApplicationQueryServiceClient::new(channel);

    let mut request = tonic::Request::new(app_v1::ExecuteQueryRequest {
        contract: None,
        module_hash: None,
        parameters: Vec::new(),
        cursor: None,
        minimum_application_head: None,
        request_id: request_id(1).into_bytes().to_vec(),
        query: Some(app_v1::execute_query_request::Query::Source(
            "query Q { return Ok { x: 1 } outcomes Ok }".to_owned(),
        )),
    });
    request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {CAPABILITY_TOKEN}"))
            .expect("valid authorization metadata"),
    );
    let response = client
        .execute_query(request)
        .await
        .expect("execute_query residual path succeeds");
    assert_eq!(response.get_ref().outcome, "Ok");

    for stage in [
        ReadPipelineStage::TransportAdapt,
        ReadPipelineStage::Authn,
        ReadPipelineStage::AdmissionContext,
        ReadPipelineStage::SpawnDispatch,
        ReadPipelineStage::EncodeConvert,
    ] {
        let snapshot = observability.metrics().read_stage_duration(stage);
        assert!(
            snapshot.count >= 1,
            "expected residual stage {stage:?} count >= 1, got {}",
            snapshot.count
        );
    }

    // Shutdown line inventory is 12 stages.
    let line =
        riffdb_observability::format_read_stages_v1_line(&observability.read_stage_snapshot());
    assert!(line.starts_with("riffdb-read-stages-v1\t"));
    let payload = line
        .strip_prefix("riffdb-read-stages-v1\t")
        .expect("prefix");
    let parsed = riffdb_observability::parse_read_stages_v1_payload(payload).expect("parse");
    assert_eq!(parsed.len(), 12);

    shutdown_sender.send(()).expect("server still running");
    server
        .await
        .expect("server task did not panic")
        .expect("server shut down cleanly");
}
