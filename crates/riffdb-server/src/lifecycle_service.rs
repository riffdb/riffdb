//! API-neutral transport proxy over the one production lifecycle route.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use riffdb_api_grpc::{
    GrpcDeploymentCompletion, GrpcLifecycleRoute, GrpcOfflineMaintenanceOperation,
    classify_grpc_deployment_completion,
};
use riffdb_catalog::ValidatedContractBundle;
use riffdb_contract_ir::{CommandPlan, ExecutionClass};
use riffdb_errors::PublicError;
use riffdb_idempotency::{
    CommandIdempotencyScopeV1, IdempotencyDigestProvider, prepare_idempotency_lookup,
};
use riffdb_service::{
    AdministrationApplication, ApplicationCatalogRequest, ApplicationCatalogResult,
    ApplicationInstallationApplication, ApplicationInstallationOperationResult,
    CheckSymbolicQueryResult, CommandApplication, CommitApplication, CompileSymbolicQueryRequest,
    ConsumeContextualSubscriptionRequest, ConsumeContextualSubscriptionResult,
    ConsumeEventStreamRequest, ConsumeEventStreamResult, ContextualSubscriptionApplication,
    ContractApplication, ContractValidationResult, CreateCapabilityInvocation,
    CreateCapabilityResult, CreateOfflineBackupRequest, DeployContractRequest,
    DeployContractResult, DeployQueryModuleRequest, DeployQueryModuleResult,
    DeployReactiveModuleRequest, DeployReactiveModuleResult, DescribeEventRequest,
    DescribeEventResult, DescribeSymbolicContractResult, DiscoverCommandToolsRequest,
    DiscoverCommandToolsResult, DiscoverResourcesRequest, DiscoverResourcesResult,
    DiscoveryApplication, EventConsumerLeaseSelection, EventConsumerMutationResult,
    EventConsumerPublicStatus, EventConsumerSelection, EventConsumerServiceApplication,
    EventServiceApplication, ExecuteCommandRequest, ExecuteCommandResult,
    ExecuteContextualReactionRequest, ExecuteProjectedQueryRequest, ExecuteProjectedQueryResult,
    ExecuteSymbolicQueryRequest, ExecuteSymbolicQueryResult, ExplainCommandRequest,
    ExplainCommandResult, ExplainSymbolicQueryResult, GetActiveContractRequest,
    GetActiveContractResult, GetApplicationInstallationRequest, GetApplicationInstallationResult,
    GetCommitRequest, GetCommitResult, GetContractVersionRequest, GetContractVersionResult,
    GetEntityRequest, GetEntityResult, GetOfflineMaintenanceOperationRequest,
    GetOfflineMaintenanceOperationResult, GetProjectionStatusRequest, GetProjectionStatusResult,
    GetQueryModuleRequest, GetReactiveWakeupResult, HealthContext, HealthRequest, HealthResult,
    InspectVectorStateRequest, InspectVectorStateResult, ListPendingOutboxDeliveriesRequest,
    ListPendingOutboxDeliveriesResult, LiveNamedQueryApplication, NamedSymbolicQueryRequest,
    NegativeAcknowledgeEventStreamRequest, OfflineMaintenanceApplication,
    OfflineMaintenanceStartResult, ProjectedQueryApplication, QueryApplication,
    QueryModuleInspection, QueryProjectionRequest, QueryProjectionResult, ReplayEventsRequest,
    ReplayEventsResult, RequestContext, ResolveCommandOutcomeRequest, ResolveCommandOutcomeResult,
    RestoreOfflineBackupInvocation, RevokeCapabilityRequest, RevokeCapabilityResult,
    ScanCommitsRequest, ScanCommitsResult, ScanIndexRequest, ScanIndexResult,
    SeekEventStreamConsumerRequest, ServiceFuture, StartApplicationInstallationRequest,
    StatisticsRequest, StatisticsResult, SubmittedFieldIdentity, SubmittedRecord, SubmittedValue,
    SubscribeToCommitsRequest, SubscribeToCommitsResult, SymbolicContractSelector,
    SymbolicQueryApplication, TailEventsRequest, TailEventsResult, TraceProvenanceRequest,
    TraceProvenanceResult, ValidateContractRequest, WatchLiveNamedQueryRequest,
    WatchLiveNamedQueryResult,
};
use riffdb_storage_api::{
    AdmissionLookupResultV1, AdmissionRepository, CatalogRepository, StoredAdmissionStateV1,
};
use riffdb_types::{DatabaseId, Environment, IdempotencyKey, ServiceOperationV1, TenantScope};

use crate::storage::SharedRedbOperationalPorts;

/// Server-private proxy that applies the production lifecycle to non-gRPC callers.
pub(crate) struct LifecycleApplicationService {
    route: Arc<dyn GrpcLifecycleRoute>,
}

impl LifecycleApplicationService {
    pub(crate) fn new(route: Arc<dyn GrpcLifecycleRoute>) -> Self {
        Self { route }
    }

    fn admit(
        &self,
        operation: ServiceOperationV1,
    ) -> Option<Arc<dyn riffdb_service::ApplicationService>> {
        self.route.admit_authenticated(operation)
    }
}

impl fmt::Debug for LifecycleApplicationService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LifecycleApplicationService([CAPABILITY])")
    }
}

fn unavailable<T>() -> ServiceFuture<'static, T> {
    Box::pin(async { Err(PublicError::storage_unavailable().into()) })
}

macro_rules! delegate_operation {
    (
        $trait_name:ident {
            $(
                $method:ident(
                    $context:ident: $context_type:ty,
                    $request:ident: $request_type:ty
                ) -> $result_type:ty => $operation:ident;
            )*
        }
    ) => {
        impl $trait_name for LifecycleApplicationService {
            $(
                fn $method(
                    &self,
                    $context: $context_type,
                    $request: $request_type,
                ) -> ServiceFuture<'_, $result_type> {
                    let Some(service) = self.admit(ServiceOperationV1::$operation) else {
                        return unavailable();
                    };
                    Box::pin(async move { service.$method($context, $request).await })
                }
            )*
        }
    };
}

impl ContractApplication for LifecycleApplicationService {
    fn validate_contract(
        &self,
        context: RequestContext,
        request: ValidateContractRequest,
    ) -> ServiceFuture<'_, ContractValidationResult> {
        let Some(service) = self.admit(ServiceOperationV1::ValidateContract) else {
            return unavailable();
        };
        Box::pin(async move { service.validate_contract(context, request).await })
    }

    fn explain_command(
        &self,
        context: RequestContext,
        request: ExplainCommandRequest,
    ) -> ServiceFuture<'_, ExplainCommandResult> {
        let Some(service) = self.admit(ServiceOperationV1::ExplainCommand) else {
            return unavailable();
        };
        Box::pin(async move { service.explain_command(context, request).await })
    }

    fn deploy_contract(
        &self,
        context: RequestContext,
        request: DeployContractRequest,
    ) -> ServiceFuture<'_, DeployContractResult> {
        let Some(service) = self.admit(ServiceOperationV1::DeployContract) else {
            return unavailable();
        };
        let route = Arc::clone(&self.route);
        Box::pin(async move {
            let lifecycle = OwnedDeploymentLifecycleGuard::new(route);
            let result = service.deploy_contract(context, request).await;
            lifecycle.complete(classify_grpc_deployment_completion(&result));
            result
        })
    }

    fn get_active_contract(
        &self,
        context: RequestContext,
        request: GetActiveContractRequest,
    ) -> ServiceFuture<'_, GetActiveContractResult> {
        let Some(service) = self.admit(ServiceOperationV1::GetActiveContract) else {
            return unavailable();
        };
        Box::pin(async move { service.get_active_contract(context, request).await })
    }

    fn get_contract_version(
        &self,
        context: RequestContext,
        request: GetContractVersionRequest,
    ) -> ServiceFuture<'_, GetContractVersionResult> {
        let Some(service) = self.admit(ServiceOperationV1::GetContractVersion) else {
            return unavailable();
        };
        Box::pin(async move { service.get_contract_version(context, request).await })
    }
}

delegate_operation! {
    ApplicationInstallationApplication {
        start_application_installation(
            context: RequestContext,
            request: StartApplicationInstallationRequest
        ) -> ApplicationInstallationOperationResult => StartApplicationInstallation;
        get_application_installation(
            context: RequestContext,
            request: GetApplicationInstallationRequest
        ) -> GetApplicationInstallationResult => GetApplicationInstallation;
    }
}

delegate_operation! {
    CommandApplication {
        execute_command(
            context: RequestContext,
            request: ExecuteCommandRequest
        ) -> ExecuteCommandResult => ExecuteCommand;
        resolve_command_outcome(
            context: RequestContext,
            request: ResolveCommandOutcomeRequest
        ) -> ResolveCommandOutcomeResult => ResolveCommandOutcome;
    }
}

delegate_operation! {
    QueryApplication {
        get_entity(
            context: RequestContext,
            request: GetEntityRequest
        ) -> GetEntityResult => GetEntity;
        scan_index(
            context: RequestContext,
            request: ScanIndexRequest
        ) -> ScanIndexResult => ScanIndex;
        query_projection(
            context: RequestContext,
            request: QueryProjectionRequest
        ) -> QueryProjectionResult => QueryProjection;
        get_projection_status(
            context: RequestContext,
            request: GetProjectionStatusRequest
        ) -> GetProjectionStatusResult => GetProjectionStatus;
        inspect_vector_state(
            context: RequestContext,
            request: InspectVectorStateRequest
        ) -> InspectVectorStateResult => InspectVectorState;
    }
}

delegate_operation! {
    CommitApplication {
        get_commit(
            context: RequestContext,
            request: GetCommitRequest
        ) -> GetCommitResult => GetCommit;
        scan_commits(
            context: RequestContext,
            request: ScanCommitsRequest
        ) -> ScanCommitsResult => ScanCommits;
        subscribe_to_commits(
            context: RequestContext,
            request: SubscribeToCommitsRequest
        ) -> SubscribeToCommitsResult => SubscribeToCommits;
        trace_provenance(
            context: RequestContext,
            request: TraceProvenanceRequest
        ) -> TraceProvenanceResult => TraceProvenance;
    }
}

delegate_operation! {
    EventServiceApplication {
        describe_event(
            context: RequestContext,
            request: DescribeEventRequest
        ) -> DescribeEventResult => DescribeEvent;
        replay_events(
            context: RequestContext,
            request: ReplayEventsRequest
        ) -> ReplayEventsResult => ReplayEvents;
        tail_events(
            context: RequestContext,
            request: TailEventsRequest
        ) -> TailEventsResult => TailEvents;
    }
}

delegate_operation! {
    EventConsumerServiceApplication {
        consume_event_stream(
            context: RequestContext,
            request: ConsumeEventStreamRequest
        ) -> ConsumeEventStreamResult => ConsumeEventStream;
        acknowledge_event_stream(
            context: RequestContext,
            request: EventConsumerLeaseSelection
        ) -> EventConsumerMutationResult => AcknowledgeEventStream;
        negative_acknowledge_event_stream(
            context: RequestContext,
            request: NegativeAcknowledgeEventStreamRequest
        ) -> EventConsumerMutationResult => NegativeAcknowledgeEventStream;
        seek_event_stream_consumer(
            context: RequestContext,
            request: SeekEventStreamConsumerRequest
        ) -> EventConsumerMutationResult => SeekEventStreamConsumer;
        retire_event_stream_consumer(
            context: RequestContext,
            request: EventConsumerSelection
        ) -> EventConsumerMutationResult => RetireEventStreamConsumer;
        get_event_stream_consumer_status(
            context: RequestContext,
            request: EventConsumerSelection
        ) -> Option<EventConsumerPublicStatus> => GetEventStreamConsumerStatus;
    }
}

impl ContextualSubscriptionApplication for LifecycleApplicationService {
    fn consume_contextual_subscription(
        &self,
        context: RequestContext,
        request: ConsumeContextualSubscriptionRequest,
    ) -> ServiceFuture<'_, ConsumeContextualSubscriptionResult> {
        let Some(service) = self.admit(ServiceOperationV1::ConsumeContextualSubscription) else {
            return unavailable();
        };
        Box::pin(async move {
            service
                .consume_contextual_subscription(context, request)
                .await
        })
    }

    fn acknowledge_contextual_subscription(
        &self,
        context: RequestContext,
        lease: EventConsumerLeaseSelection,
    ) -> ServiceFuture<'_, EventConsumerMutationResult> {
        let Some(service) = self.admit(ServiceOperationV1::AcknowledgeContextualSubscription)
        else {
            return unavailable();
        };
        Box::pin(async move {
            service
                .acknowledge_contextual_subscription(context, lease)
                .await
        })
    }

    fn negative_acknowledge_contextual_subscription(
        &self,
        context: RequestContext,
        lease: EventConsumerLeaseSelection,
        retry_delay: Duration,
    ) -> ServiceFuture<'_, EventConsumerMutationResult> {
        let Some(service) =
            self.admit(ServiceOperationV1::NegativeAcknowledgeContextualSubscription)
        else {
            return unavailable();
        };
        Box::pin(async move {
            service
                .negative_acknowledge_contextual_subscription(context, lease, retry_delay)
                .await
        })
    }

    fn get_contextual_subscription_status(
        &self,
        context: RequestContext,
        selection: EventConsumerSelection,
    ) -> ServiceFuture<'_, Option<EventConsumerPublicStatus>> {
        let Some(service) = self.admit(ServiceOperationV1::GetContextualSubscriptionStatus) else {
            return unavailable();
        };
        Box::pin(async move {
            service
                .get_contextual_subscription_status(context, selection)
                .await
        })
    }

    fn execute_contextual_reaction(
        &self,
        context: RequestContext,
        request: ExecuteContextualReactionRequest,
    ) -> ServiceFuture<'_, ExecuteCommandResult> {
        let Some(service) = self.admit(ServiceOperationV1::ExecuteContextualReaction) else {
            return unavailable();
        };
        Box::pin(async move { service.execute_contextual_reaction(context, request).await })
    }
}

impl AdministrationApplication for LifecycleApplicationService {
    fn health(
        &self,
        context: HealthContext,
        request: HealthRequest,
    ) -> ServiceFuture<'_, HealthResult> {
        let Some(service) = self.admit(ServiceOperationV1::GetHealth) else {
            return unavailable();
        };
        Box::pin(async move { service.health(context, request).await })
    }

    fn statistics(
        &self,
        context: RequestContext,
        request: StatisticsRequest,
    ) -> ServiceFuture<'_, StatisticsResult> {
        let Some(service) = self.admit(ServiceOperationV1::GetStatistics) else {
            return unavailable();
        };
        Box::pin(async move { service.statistics(context, request).await })
    }

    fn create_capability(
        &self,
        invocation: CreateCapabilityInvocation,
    ) -> ServiceFuture<'_, CreateCapabilityResult> {
        if matches!(&invocation, CreateCapabilityInvocation::Bootstrap { .. }) {
            return unavailable();
        }
        let Some(service) = self.admit(ServiceOperationV1::CreateCapability) else {
            return unavailable();
        };
        Box::pin(async move { service.create_capability(invocation).await })
    }

    fn fence_replication_primary(
        &self,
        context: RequestContext,
        request: riffdb_service::FenceReplicationPrimaryRequest,
    ) -> ServiceFuture<'_, riffdb_service::FenceReplicationPrimaryResult> {
        let Some(service) = self.admit(ServiceOperationV1::FenceReplicationPrimary) else {
            return unavailable();
        };
        Box::pin(async move { service.fence_replication_primary(context, request).await })
    }

    fn register_follower(
        &self,
        context: RequestContext,
        request: riffdb_service::RegisterFollowerRequest,
    ) -> ServiceFuture<'_, riffdb_service::RegisterFollowerResult> {
        let Some(service) = self.admit(ServiceOperationV1::RegisterFollower) else {
            return unavailable();
        };
        Box::pin(async move { service.register_follower(context, request).await })
    }

    fn retire_follower(
        &self,
        context: RequestContext,
        request: riffdb_service::RetireFollowerRequest,
    ) -> ServiceFuture<'_, riffdb_service::RetireFollowerResult> {
        let Some(service) = self.admit(ServiceOperationV1::RetireFollower) else {
            return unavailable();
        };
        Box::pin(async move { service.retire_follower(context, request).await })
    }

    fn revoke_capability(
        &self,
        context: RequestContext,
        request: RevokeCapabilityRequest,
    ) -> ServiceFuture<'_, RevokeCapabilityResult> {
        let Some(service) = self.admit(ServiceOperationV1::RevokeCapability) else {
            return unavailable();
        };
        Box::pin(async move { service.revoke_capability(context, request).await })
    }

    fn list_pending_outbox_deliveries(
        &self,
        context: RequestContext,
        request: ListPendingOutboxDeliveriesRequest,
    ) -> ServiceFuture<'_, ListPendingOutboxDeliveriesResult> {
        let Some(service) = self.admit(ServiceOperationV1::ListPendingOutboxDeliveries) else {
            return unavailable();
        };
        Box::pin(async move {
            service
                .list_pending_outbox_deliveries(context, request)
                .await
        })
    }
}

impl OfflineMaintenanceApplication for LifecycleApplicationService {
    fn create_offline_backup(
        &self,
        context: RequestContext,
        request: CreateOfflineBackupRequest,
    ) -> ServiceFuture<'_, OfflineMaintenanceStartResult> {
        let Some(service) = self
            .route
            .admit_offline_maintenance(GrpcOfflineMaintenanceOperation::CreateBackup)
        else {
            return unavailable();
        };
        Box::pin(async move { service.create_offline_backup(context, request).await })
    }

    fn restore_offline_backup(
        &self,
        invocation: RestoreOfflineBackupInvocation,
    ) -> ServiceFuture<'_, OfflineMaintenanceStartResult> {
        let operation_id = invocation.operation_id();
        let input_hash = invocation.input_hash();
        let Some(service) =
            self.route
                .admit_offline_maintenance(GrpcOfflineMaintenanceOperation::RestoreBackup {
                    operation_id,
                    input_hash,
                })
        else {
            return unavailable();
        };
        Box::pin(async move { service.restore_offline_backup(invocation).await })
    }

    fn restore_archived_backup(
        &self,
        invocation: riffdb_service::RestoreArchivedBackupInvocation,
    ) -> ServiceFuture<'_, OfflineMaintenanceStartResult> {
        let Some(service) =
            self.route
                .admit_offline_maintenance(GrpcOfflineMaintenanceOperation::RestoreBackup {
                    operation_id: invocation.operation_id(),
                    input_hash: invocation.input_hash(),
                })
        else {
            return unavailable();
        };
        Box::pin(async move { service.restore_archived_backup(invocation).await })
    }

    fn retire_offline_backup(
        &self,
        context: RequestContext,
        request: riffdb_service::RetireOfflineBackupRequest,
    ) -> ServiceFuture<'_, OfflineMaintenanceStartResult> {
        let Some(service) = self
            .route
            .admit_offline_maintenance(GrpcOfflineMaintenanceOperation::RetireBackup)
        else {
            return unavailable();
        };
        Box::pin(async move { service.retire_offline_backup(context, request).await })
    }

    fn start_offline_storage_scrub(
        &self,
        context: RequestContext,
        request: riffdb_service::StartOfflineStorageScrubRequest,
    ) -> ServiceFuture<'_, OfflineMaintenanceStartResult> {
        let Some(service) = self
            .route
            .admit_offline_maintenance(GrpcOfflineMaintenanceOperation::StorageScrub)
        else {
            return unavailable();
        };
        Box::pin(async move { service.start_offline_storage_scrub(context, request).await })
    }

    fn get_offline_maintenance_operation(
        &self,
        context: RequestContext,
        request: GetOfflineMaintenanceOperationRequest,
    ) -> ServiceFuture<'_, GetOfflineMaintenanceOperationResult> {
        let Some(service) = self
            .route
            .admit_offline_maintenance(GrpcOfflineMaintenanceOperation::GetOperation)
        else {
            return unavailable();
        };
        Box::pin(async move {
            service
                .get_offline_maintenance_operation(context, request)
                .await
        })
    }
}

impl DiscoveryApplication for LifecycleApplicationService {
    fn discover_command_tools(
        &self,
        context: RequestContext,
        request: DiscoverCommandToolsRequest,
    ) -> ServiceFuture<'_, DiscoverCommandToolsResult> {
        let Some(service) = self.admit(ServiceOperationV1::DiscoverCommandTools) else {
            return unavailable();
        };
        Box::pin(async move { service.discover_command_tools(context, request).await })
    }

    fn discover_resources(
        &self,
        context: RequestContext,
        request: DiscoverResourcesRequest,
    ) -> ServiceFuture<'_, DiscoverResourcesResult> {
        let Some(service) = self.admit(ServiceOperationV1::DiscoverResources) else {
            return unavailable();
        };
        Box::pin(async move { service.discover_resources(context, request).await })
    }

    fn get_reactive_wakeup(
        &self,
        context: RequestContext,
    ) -> ServiceFuture<'_, GetReactiveWakeupResult> {
        let Some(service) = self.admit(ServiceOperationV1::GetReactiveWakeup) else {
            return unavailable();
        };
        Box::pin(async move { service.get_reactive_wakeup(context).await })
    }
}

delegate_operation! {
    SymbolicQueryApplication {
        describe_symbolic_contract(
            context: RequestContext,
            request: SymbolicContractSelector
        ) -> DescribeSymbolicContractResult => DescribeContract;
        get_application_catalog(
            context: RequestContext,
            request: ApplicationCatalogRequest
        ) -> ApplicationCatalogResult => DescribeContract;
        check_symbolic_query(
            context: RequestContext,
            request: CompileSymbolicQueryRequest
        ) -> CheckSymbolicQueryResult => CheckQuery;
        explain_symbolic_query(
            context: RequestContext,
            request: CompileSymbolicQueryRequest
        ) -> ExplainSymbolicQueryResult => ExplainQuery;
        execute_symbolic_query(
            context: RequestContext,
            request: ExecuteSymbolicQueryRequest
        ) -> ExecuteSymbolicQueryResult => ExecuteQuery;
        deploy_query_module(
            context: RequestContext,
            request: DeployQueryModuleRequest
        ) -> DeployQueryModuleResult => DeployQueryModule;
        deploy_reactive_module(
            context: RequestContext,
            request: DeployReactiveModuleRequest
        ) -> DeployReactiveModuleResult => DeployReactiveModule;
        get_query_module(
            context: RequestContext,
            request: GetQueryModuleRequest
        ) -> Option<QueryModuleInspection> => ExplainQuery;
        explain_named_symbolic_query(
            context: RequestContext,
            request: NamedSymbolicQueryRequest
        ) -> ExplainSymbolicQueryResult => ExplainQuery;
        execute_named_symbolic_query(
            context: RequestContext,
            request: NamedSymbolicQueryRequest
        ) -> ExecuteSymbolicQueryResult => ExecuteQuery;
    }
}

delegate_operation! {
    ProjectedQueryApplication {
        execute_projected_query(
            context: RequestContext,
            request: ExecuteProjectedQueryRequest
        ) -> ExecuteProjectedQueryResult => ExecuteProjectedQuery;
    }
}

delegate_operation! {
    LiveNamedQueryApplication {
        watch_live_named_query(
            context: RequestContext,
            request: WatchLiveNamedQueryRequest
        ) -> WatchLiveNamedQueryResult => WatchNamedQuery;
    }
}

/// Activated application service wrapper that preserves historical idempotent
/// command replay when an explicit `--expected-version` matches the durable
/// admission record's committed contract version after an additive deploy, and
/// enforces deployment-configured semi-synchronous quorum commit durability.
pub(crate) struct ProductionApplicationService {
    service: Arc<dyn riffdb_service::ApplicationService>,
    storage: SharedRedbOperationalPorts,
    idempotency_digests: Arc<dyn IdempotencyDigestProvider>,
    database_id: DatabaseId,
    environment: Environment,
    commit_acknowledgement: crate::config::CommitAcknowledgementMode,
    follower_ack_receiver: Option<tokio::sync::watch::Receiver<u64>>,
    primary_lease: Option<Arc<crate::replication_source::PrimaryQuorumLeaseTracker>>,
}

impl ProductionApplicationService {
    pub(crate) fn new(
        service: Arc<dyn riffdb_service::ApplicationService>,
        storage: SharedRedbOperationalPorts,
        idempotency_digests: Arc<dyn IdempotencyDigestProvider>,
        database_id: DatabaseId,
        environment: Environment,
    ) -> Self {
        Self {
            service,
            storage,
            idempotency_digests,
            database_id,
            environment,
            commit_acknowledgement: crate::config::CommitAcknowledgementMode::Local,
            follower_ack_receiver: None,
            primary_lease: None,
        }
    }

    pub(crate) fn with_commit_quorum(
        mut self,
        commit_acknowledgement: crate::config::CommitAcknowledgementMode,
        follower_ack_receiver: Option<tokio::sync::watch::Receiver<u64>>,
    ) -> Self {
        self.commit_acknowledgement = commit_acknowledgement;
        self.follower_ack_receiver = follower_ack_receiver;
        self
    }

    pub(crate) fn with_primary_lease(
        mut self,
        primary_lease: Option<Arc<crate::replication_source::PrimaryQuorumLeaseTracker>>,
    ) -> Self {
        self.primary_lease = primary_lease;
        self
    }

    fn ensure_primary_lease_valid(&self) -> bool {
        self.primary_lease
            .as_ref()
            .is_none_or(|lease| lease.check_or_fence(std::time::Instant::now()))
    }

    async fn wait_for_commit_quorum(
        &self,
        commit_sequence: riffdb_types::CommitSequence,
    ) -> Result<(), PublicError> {
        if !self.ensure_primary_lease_valid() {
            return Err(PublicError::outcome_unknown());
        }
        let crate::config::CommitAcknowledgementMode::Quorum {
            required_followers,
            timeout,
        } = self.commit_acknowledgement
        else {
            return Ok(());
        };
        let Some(ref ack_receiver) = self.follower_ack_receiver else {
            return Err(PublicError::outcome_unknown());
        };
        let mut rx = ack_receiver.clone();
        let shared = self.storage.shared_read_ports();
        let primary_lease = self.primary_lease.clone();
        tokio::time::timeout(timeout, async move {
            loop {
                if primary_lease
                    .as_ref()
                    .is_some_and(|lease| !lease.check_or_fence(std::time::Instant::now()))
                {
                    return Err(PublicError::outcome_unknown());
                }
                let _ = rx.borrow_and_update();
                let progress = shared
                    .observe_replication_source_progress_v3()
                    .map_err(|_| PublicError::outcome_unknown())?;
                if progress
                    .has_quorum_acknowledged_application_commit(commit_sequence, required_followers)
                {
                    return Ok(());
                }
                rx.changed()
                    .await
                    .map_err(|_| PublicError::outcome_unknown())?;
            }
        })
        .await
        .map_err(|_| PublicError::outcome_unknown())?
    }

    fn normalize_historical_replay_request(
        &self,
        context: &RequestContext,
        request: ExecuteCommandRequest,
    ) -> ExecuteCommandRequest {
        normalize_historical_replay_request_for_principal(
            &self.storage,
            self.idempotency_digests.as_ref(),
            self.database_id,
            &self.environment,
            context.principal().principal_id(),
            request,
        )
    }
}

fn normalize_historical_replay_request_for_principal(
    storage: &SharedRedbOperationalPorts,
    idempotency_digests: &dyn IdempotencyDigestProvider,
    database_id: DatabaseId,
    environment: &Environment,
    principal_id: &riffdb_types::ActorId,
    request: ExecuteCommandRequest,
) -> ExecuteCommandRequest {
    let Some(expected_version) = request.expected_contract_version() else {
        return request;
    };
    let Ok(Some(pointer)) = CatalogRepository::read_active_catalog(storage) else {
        return request;
    };
    if pointer.contract_version() == expected_version {
        return request;
    }
    let Ok(Some(stored_bundle)) =
        CatalogRepository::read_contract_bundle(storage, pointer.lineage(), expected_version)
    else {
        return request;
    };
    let Ok(bundle) = ValidatedContractBundle::from_stored(&stored_bundle) else {
        return request;
    };
    let Some(plan) = bundle
        .bundle()
        .commands()
        .iter()
        .find(|plan| !plan.is_reimport() && plan.name() == request.command().as_str())
        .filter(|plan| plan.execution_class() == ExecutionClass::IdempotentMutation)
    else {
        return request;
    };
    let Some(caller_key) = extract_historical_replay_idempotency_key(plan, request.input()) else {
        return request;
    };
    let scope = CommandIdempotencyScopeV1::new(
        database_id,
        environment.clone(),
        TenantScope::Global,
        principal_id.clone(),
        pointer.lineage().clone(),
        plan.command_id(),
    );
    let Ok(prepared_lookup) = prepare_idempotency_lookup(&scope, &caller_key, idempotency_digests)
    else {
        return request;
    };
    let candidates = prepared_lookup.lookup_candidates().clone();
    let Ok(AdmissionLookupResultV1::Found(state)) =
        AdmissionRepository::lookup_admission(storage, candidates.clone())
    else {
        return request;
    };
    if !candidates.contains(state.identity()) {
        return request;
    }
    let admitted_version = match state.as_ref() {
        StoredAdmissionStateV1::Pending(pending) => pending.plan().contract_version(),
        StoredAdmissionStateV1::StoredOutcome(outcome) => outcome.plan().contract_version(),
        StoredAdmissionStateV1::ExecutionFailed(failure) => {
            failure.pending().plan().contract_version()
        }
    };
    if admitted_version != expected_version {
        return request;
    }
    ExecuteCommandRequest::new(
        request.command().clone(),
        Some(pointer.contract_version()),
        request.input().clone(),
    )
    .unwrap_or(request)
}

impl fmt::Debug for ProductionApplicationService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProductionApplicationService([CAPABILITY])")
    }
}

fn extract_historical_replay_idempotency_key(
    plan: &CommandPlan,
    submitted: &SubmittedRecord,
) -> Option<IdempotencyKey> {
    let target = plan.idempotency_input()?;
    let declared = plan.input().record().field(target)?;
    let mut candidate = None;
    for field in submitted.fields() {
        let matches = match field.identity() {
            SubmittedFieldIdentity::Id(id) => *id == target,
            SubmittedFieldIdentity::Name(name) => name.as_str() == declared.name(),
            SubmittedFieldIdentity::IdAndName { id, name } => {
                *id == target && name.as_str() == declared.name()
            }
        };
        if matches {
            if candidate.is_some() {
                return None;
            }
            candidate = Some(field.value());
        }
    }
    let raw = match candidate? {
        SubmittedValue::String(value) => value.as_str().to_owned(),
        SubmittedValue::Uuid(bytes) => format!(
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            bytes[0],
            bytes[1],
            bytes[2],
            bytes[3],
            bytes[4],
            bytes[5],
            bytes[6],
            bytes[7],
            bytes[8],
            bytes[9],
            bytes[10],
            bytes[11],
            bytes[12],
            bytes[13],
            bytes[14],
            bytes[15],
        ),
        _ => return None,
    };
    IdempotencyKey::new(raw).ok()
}

macro_rules! delegate_direct_operation {
    (
        $trait_name:ident {
            $(
                $method:ident(
                    $($arg_name:ident: $arg_type:ty),* $(,)?
                ) -> $result_type:ty;
            )*
        }
    ) => {
        impl $trait_name for ProductionApplicationService {
            $(
                fn $method(
                    &self,
                    $($arg_name: $arg_type),*
                ) -> ServiceFuture<'_, $result_type> {
                    if !self.ensure_primary_lease_valid() {
                        return unavailable();
                    }
                    self.service.$method($($arg_name),*)
                }
            )*
        }
    };
}

delegate_direct_operation! {
    ContractApplication {
        validate_contract(
            context: RequestContext,
            request: ValidateContractRequest
        ) -> ContractValidationResult;
        explain_command(
            context: RequestContext,
            request: ExplainCommandRequest
        ) -> ExplainCommandResult;
        deploy_contract(
            context: RequestContext,
            request: DeployContractRequest
        ) -> DeployContractResult;
        get_active_contract(
            context: RequestContext,
            request: GetActiveContractRequest
        ) -> GetActiveContractResult;
        get_contract_version(
            context: RequestContext,
            request: GetContractVersionRequest
        ) -> GetContractVersionResult;
    }
}

delegate_direct_operation! {
    ApplicationInstallationApplication {
        start_application_installation(
            context: RequestContext,
            request: StartApplicationInstallationRequest
        ) -> ApplicationInstallationOperationResult;
        get_application_installation(
            context: RequestContext,
            request: GetApplicationInstallationRequest
        ) -> GetApplicationInstallationResult;
    }
}

impl CommandApplication for ProductionApplicationService {
    fn execute_command(
        &self,
        context: RequestContext,
        request: ExecuteCommandRequest,
    ) -> ServiceFuture<'_, ExecuteCommandResult> {
        if !self.ensure_primary_lease_valid() {
            return unavailable();
        }
        let request = self.normalize_historical_replay_request(&context, request);
        Box::pin(async move {
            let result = self.service.execute_command(context, request).await?;
            if let ExecuteCommandResult::Journaled(ref journaled) = result {
                self.wait_for_commit_quorum(journaled.commit_sequence())
                    .await?;
            }
            Ok(result)
        })
    }

    fn resolve_command_outcome(
        &self,
        context: RequestContext,
        request: ResolveCommandOutcomeRequest,
    ) -> ServiceFuture<'_, ResolveCommandOutcomeResult> {
        if !self.ensure_primary_lease_valid() {
            return unavailable();
        }
        Box::pin(async move {
            let result = self
                .service
                .resolve_command_outcome(context, request)
                .await?;
            if let ResolveCommandOutcomeResult::Found(ref recovered) = result {
                self.wait_for_commit_quorum(recovered.commit_sequence())
                    .await?;
            }
            Ok(result)
        })
    }
}

delegate_direct_operation! {
    QueryApplication {
        get_entity(
            context: RequestContext,
            request: GetEntityRequest
        ) -> GetEntityResult;
        scan_index(
            context: RequestContext,
            request: ScanIndexRequest
        ) -> ScanIndexResult;
        query_projection(
            context: RequestContext,
            request: QueryProjectionRequest
        ) -> QueryProjectionResult;
        get_projection_status(
            context: RequestContext,
            request: GetProjectionStatusRequest
        ) -> GetProjectionStatusResult;
        inspect_vector_state(
            context: RequestContext,
            request: InspectVectorStateRequest
        ) -> InspectVectorStateResult;
    }
}

delegate_direct_operation! {
    CommitApplication {
        get_commit(
            context: RequestContext,
            request: GetCommitRequest
        ) -> GetCommitResult;
        scan_commits(
            context: RequestContext,
            request: ScanCommitsRequest
        ) -> ScanCommitsResult;
        subscribe_to_commits(
            context: RequestContext,
            request: SubscribeToCommitsRequest
        ) -> SubscribeToCommitsResult;
        trace_provenance(
            context: RequestContext,
            request: TraceProvenanceRequest
        ) -> TraceProvenanceResult;
    }
}

delegate_direct_operation! {
    EventServiceApplication {
        describe_event(
            context: RequestContext,
            request: DescribeEventRequest
        ) -> DescribeEventResult;
        replay_events(
            context: RequestContext,
            request: ReplayEventsRequest
        ) -> ReplayEventsResult;
        tail_events(
            context: RequestContext,
            request: TailEventsRequest
        ) -> TailEventsResult;
    }
}

delegate_direct_operation! {
    EventConsumerServiceApplication {
        consume_event_stream(
            context: RequestContext,
            request: ConsumeEventStreamRequest
        ) -> ConsumeEventStreamResult;
        acknowledge_event_stream(
            context: RequestContext,
            request: EventConsumerLeaseSelection
        ) -> EventConsumerMutationResult;
        negative_acknowledge_event_stream(
            context: RequestContext,
            request: NegativeAcknowledgeEventStreamRequest
        ) -> EventConsumerMutationResult;
        seek_event_stream_consumer(
            context: RequestContext,
            request: SeekEventStreamConsumerRequest
        ) -> EventConsumerMutationResult;
        retire_event_stream_consumer(
            context: RequestContext,
            request: EventConsumerSelection
        ) -> EventConsumerMutationResult;
        get_event_stream_consumer_status(
            context: RequestContext,
            request: EventConsumerSelection
        ) -> Option<EventConsumerPublicStatus>;
    }
}

impl ContextualSubscriptionApplication for ProductionApplicationService {
    fn consume_contextual_subscription(
        &self,
        context: RequestContext,
        request: ConsumeContextualSubscriptionRequest,
    ) -> ServiceFuture<'_, ConsumeContextualSubscriptionResult> {
        self.service
            .consume_contextual_subscription(context, request)
    }

    fn acknowledge_contextual_subscription(
        &self,
        context: RequestContext,
        lease: EventConsumerLeaseSelection,
    ) -> ServiceFuture<'_, EventConsumerMutationResult> {
        self.service
            .acknowledge_contextual_subscription(context, lease)
    }

    fn negative_acknowledge_contextual_subscription(
        &self,
        context: RequestContext,
        lease: EventConsumerLeaseSelection,
        retry_delay: Duration,
    ) -> ServiceFuture<'_, EventConsumerMutationResult> {
        self.service
            .negative_acknowledge_contextual_subscription(context, lease, retry_delay)
    }

    fn get_contextual_subscription_status(
        &self,
        context: RequestContext,
        selection: EventConsumerSelection,
    ) -> ServiceFuture<'_, Option<EventConsumerPublicStatus>> {
        self.service
            .get_contextual_subscription_status(context, selection)
    }

    fn execute_contextual_reaction(
        &self,
        context: RequestContext,
        request: ExecuteContextualReactionRequest,
    ) -> ServiceFuture<'_, ExecuteCommandResult> {
        if !self.ensure_primary_lease_valid() {
            return unavailable();
        }
        Box::pin(async move {
            let result = self
                .service
                .execute_contextual_reaction(context, request)
                .await?;
            if let ExecuteCommandResult::Journaled(ref journaled) = result {
                self.wait_for_commit_quorum(journaled.commit_sequence())
                    .await?;
            }
            Ok(result)
        })
    }
}

delegate_direct_operation! {
    AdministrationApplication {
        fence_replication_primary(
            context: RequestContext,
            request: riffdb_service::FenceReplicationPrimaryRequest
        ) -> riffdb_service::FenceReplicationPrimaryResult;
        health(
            context: HealthContext,
            request: HealthRequest
        ) -> HealthResult;
        statistics(
            context: RequestContext,
            request: StatisticsRequest
        ) -> StatisticsResult;
        create_capability(
            invocation: CreateCapabilityInvocation
        ) -> CreateCapabilityResult;
        register_follower(
            context: RequestContext,
            request: riffdb_service::RegisterFollowerRequest
        ) -> riffdb_service::RegisterFollowerResult;
        retire_follower(
            context: RequestContext,
            request: riffdb_service::RetireFollowerRequest
        ) -> riffdb_service::RetireFollowerResult;
        revoke_capability(
            context: RequestContext,
            request: RevokeCapabilityRequest
        ) -> RevokeCapabilityResult;
        list_pending_outbox_deliveries(
            context: RequestContext,
            request: ListPendingOutboxDeliveriesRequest
        ) -> ListPendingOutboxDeliveriesResult;
    }
}

delegate_direct_operation! {
    OfflineMaintenanceApplication {
        create_offline_backup(
            context: RequestContext,
            request: CreateOfflineBackupRequest
        ) -> OfflineMaintenanceStartResult;
        restore_offline_backup(
            invocation: RestoreOfflineBackupInvocation
        ) -> OfflineMaintenanceStartResult;
        restore_archived_backup(
            invocation: riffdb_service::RestoreArchivedBackupInvocation
        ) -> OfflineMaintenanceStartResult;
        retire_offline_backup(
            context: RequestContext,
            request: riffdb_service::RetireOfflineBackupRequest
        ) -> OfflineMaintenanceStartResult;
        start_offline_storage_scrub(
            context: RequestContext,
            request: riffdb_service::StartOfflineStorageScrubRequest
        ) -> OfflineMaintenanceStartResult;
        get_offline_maintenance_operation(
            context: RequestContext,
            request: GetOfflineMaintenanceOperationRequest
        ) -> GetOfflineMaintenanceOperationResult;
    }
}

delegate_direct_operation! {
    DiscoveryApplication {
        discover_command_tools(
            context: RequestContext,
            request: DiscoverCommandToolsRequest
        ) -> DiscoverCommandToolsResult;
        discover_resources(
            context: RequestContext,
            request: DiscoverResourcesRequest
        ) -> DiscoverResourcesResult;
        get_reactive_wakeup(
            context: RequestContext
        ) -> GetReactiveWakeupResult;
    }
}

delegate_direct_operation! {
    SymbolicQueryApplication {
        describe_symbolic_contract(
            context: RequestContext,
            request: SymbolicContractSelector
        ) -> DescribeSymbolicContractResult;
        get_application_catalog(
            context: RequestContext,
            request: ApplicationCatalogRequest
        ) -> ApplicationCatalogResult;
        check_symbolic_query(
            context: RequestContext,
            request: CompileSymbolicQueryRequest
        ) -> CheckSymbolicQueryResult;
        explain_symbolic_query(
            context: RequestContext,
            request: CompileSymbolicQueryRequest
        ) -> ExplainSymbolicQueryResult;
        execute_symbolic_query(
            context: RequestContext,
            request: ExecuteSymbolicQueryRequest
        ) -> ExecuteSymbolicQueryResult;
        deploy_query_module(
            context: RequestContext,
            request: DeployQueryModuleRequest
        ) -> DeployQueryModuleResult;
        deploy_reactive_module(
            context: RequestContext,
            request: DeployReactiveModuleRequest
        ) -> DeployReactiveModuleResult;
        get_query_module(
            context: RequestContext,
            request: GetQueryModuleRequest
        ) -> Option<QueryModuleInspection>;
        explain_named_symbolic_query(
            context: RequestContext,
            request: NamedSymbolicQueryRequest
        ) -> ExplainSymbolicQueryResult;
        execute_named_symbolic_query(
            context: RequestContext,
            request: NamedSymbolicQueryRequest
        ) -> ExecuteSymbolicQueryResult;
    }
}

delegate_direct_operation! {
    ProjectedQueryApplication {
        execute_projected_query(
            context: RequestContext,
            request: ExecuteProjectedQueryRequest
        ) -> ExecuteProjectedQueryResult;
    }
}

delegate_direct_operation! {
    LiveNamedQueryApplication {
        watch_live_named_query(
            context: RequestContext,
            request: WatchLiveNamedQueryRequest
        ) -> WatchLiveNamedQueryResult;
    }
}

struct OwnedDeploymentLifecycleGuard {
    route: Arc<dyn GrpcLifecycleRoute>,
    completed: bool,
}

impl OwnedDeploymentLifecycleGuard {
    fn new(route: Arc<dyn GrpcLifecycleRoute>) -> Self {
        Self {
            route,
            completed: false,
        }
    }

    fn complete(mut self, completion: GrpcDeploymentCompletion) {
        self.completed = true;
        self.route.finish_deployment(completion);
    }
}

impl Drop for OwnedDeploymentLifecycleGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.route
                .finish_deployment(GrpcDeploymentCompletion::Abandoned);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use riffdb_api_grpc::{
        CheckedGrpcSecurityContext, GrpcBootstrapCompletion, GrpcDeploymentCompletion,
    };
    use riffdb_service::{HealthRequest, HealthResult};

    use super::*;

    #[derive(Default)]
    struct RecordingRoute {
        deployments: Mutex<Vec<GrpcDeploymentCompletion>>,
    }

    impl GrpcLifecycleRoute for RecordingRoute {
        fn admit_authenticated(
            &self,
            _operation: ServiceOperationV1,
        ) -> Option<Arc<dyn riffdb_service::ApplicationService>> {
            None
        }

        fn admit_offline_maintenance(
            &self,
            _operation: riffdb_api_grpc::GrpcOfflineMaintenanceOperation,
        ) -> Option<Arc<dyn riffdb_service::ApplicationService>> {
            None
        }

        fn admit_restore_retry(
            &self,
            _operation_id: riffdb_types::OfflineMaintenanceOperationId,
            _input_hash: riffdb_types::OfflineMaintenanceInputHash,
        ) -> Option<Arc<dyn riffdb_service::RestoreRetryOfflineMaintenanceApplication>> {
            None
        }

        fn admit_recovery_restore(
            &self,
            _operation_id: riffdb_types::OfflineMaintenanceOperationId,
            _input_hash: riffdb_types::OfflineMaintenanceInputHash,
        ) -> Option<Arc<dyn riffdb_service::RecoveryOfflineMaintenanceApplication>> {
            None
        }

        fn security_context(&self) -> Option<CheckedGrpcSecurityContext> {
            None
        }

        fn restore_retry_security_context(
            &self,
        ) -> Option<riffdb_api_grpc::CheckedGrpcRestoreRetrySecurityContext> {
            None
        }

        fn server_generation(&self) -> Option<[u8; 16]> {
            None
        }

        fn history_incarnation(&self) -> Option<u64> {
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

        fn begin_bootstrap(&self) -> Option<Arc<dyn riffdb_service::ApplicationService>> {
            None
        }

        fn finish_bootstrap(&self, _completion: GrpcBootstrapCompletion) {}

        fn finish_deployment(&self, completion: GrpcDeploymentCompletion) {
            self.deployments
                .lock()
                .expect("deployment observation lock")
                .push(completion);
        }
    }

    #[test]
    fn deployment_guard_reports_exact_completion_or_abandonment_once() {
        let route = Arc::new(RecordingRoute::default());
        let lifecycle: Arc<dyn GrpcLifecycleRoute> = route.clone();

        drop(OwnedDeploymentLifecycleGuard::new(Arc::clone(&lifecycle)));
        OwnedDeploymentLifecycleGuard::new(lifecycle).complete(GrpcDeploymentCompletion::Activated);

        assert_eq!(
            *route.deployments.lock().expect("deployment observations"),
            [
                GrpcDeploymentCompletion::Abandoned,
                GrpcDeploymentCompletion::Activated,
            ]
        );
    }

    #[test]
    fn proxy_is_one_application_service_over_the_lifecycle_trait() {
        fn assert_application_service<T: riffdb_service::ApplicationService>() {}
        assert_application_service::<LifecycleApplicationService>();
    }

    struct DeterministicDigestProvider;

    impl IdempotencyDigestProvider for DeterministicDigestProvider {
        fn digest_candidates(
            &self,
            caller_key: &IdempotencyKey,
        ) -> Result<
            riffdb_idempotency::IdempotencyDigestCandidatesV1,
            riffdb_idempotency::IdempotencyDigestError,
        > {
            let mut bytes = [0_u8; 32];
            for (index, byte) in caller_key.expose_secret().bytes().take(32).enumerate() {
                bytes[index] = byte;
            }
            let digest = riffdb_storage_api::IdempotencyKeyDigest::from_hmac_bytes(
                riffdb_types::DigestKeyId::new(1).expect("digest key ID"),
                bytes,
            );
            riffdb_idempotency::IdempotencyDigestCandidatesV1::new(vec![digest])
        }
    }

    #[test]
    fn historical_idempotent_replay_normalizes_expected_version_after_additive_deploy_and_fails_closed_for_uncommitted_keys()
     {
        use std::num::NonZeroU64;

        use riffdb_contract_compiler::compile_contract_source;
        use riffdb_service::{SourceName, SubmittedField};
        use riffdb_storage_api::{
            AdmissionRequestV1, AdmissionResultV1, AuditPrincipalV1, CatalogActivationIntentV1,
            CatalogActivationResult, CatalogAdministrationRepository, ExecutablePlanRef,
            PreEvaluationCommitContext, StoredAdmittedProvenanceClaimsV1, StoredPendingAdmissionV1,
        };
        use riffdb_types::{
            ActorId, ActorKind, AdmittedActorContext, AggregateTypeId, CanonicalInputHash,
            CapabilityId, ContractVersion, LogicalTime, PartitionKeyBuilder, RequestId, Timestamp,
            hash_partition_key,
        };

        let real = crate::real_storage_support::RealStorage::open("historical-replay-normalize");
        let mut storage = real.storage.clone();
        let v1 = ValidatedContractBundle::from_compiler_bundle(
            compile_contract_source(include_str!(
                "../../../examples/ticketdesk/lifecycle/riffdb/contract.riff"
            ))
            .expect("compile v1 contract"),
        )
        .expect("validate v1 contract");
        let v2 = ValidatedContractBundle::from_compiler_bundle(
            compile_contract_source(include_str!(
                "../../../examples/ticketdesk/lifecycle-v2/riffdb/contract.riff"
            ))
            .expect("compile v2 contract"),
        )
        .expect("validate v2 contract");
        let v1_stored = v1.to_stored().expect("encode v1 contract");
        let audit_principal = AuditPrincipalV1::new(
            ActorId::new("lifecycle-operator").expect("operator actor"),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(7, [0x41; 10])
                .expect("capability identity"),
            NonZeroU64::MIN,
        );
        assert!(matches!(
            storage
                .activate_catalog(&CatalogActivationIntentV1::new(
                    None,
                    v1_stored.clone(),
                    RequestId::from_unix_milliseconds_and_random(1, [0x11; 10])
                        .expect("request id"),
                    audit_principal.clone(),
                    Timestamp::new(1_000, 0).expect("timestamp"),
                    None,
                ))
                .expect("activate v1 contract"),
            CatalogActivationResult::Activated { .. }
        ));
        let v1_pointer = CatalogRepository::read_active_catalog(&storage)
            .expect("read v1 pointer")
            .expect("v1 pointer present");
        assert!(matches!(
            storage
                .activate_catalog(&CatalogActivationIntentV1::new(
                    Some(v1_pointer.contract_version()),
                    v2.to_stored().expect("encode v2 contract"),
                    RequestId::from_unix_milliseconds_and_random(2, [0x22; 10])
                        .expect("request id"),
                    audit_principal,
                    Timestamp::new(2_000, 0).expect("timestamp"),
                    None,
                ))
                .expect("activate v2 contract"),
            CatalogActivationResult::Activated { .. }
        ));

        let v1_command = v1
            .bundle()
            .commands()
            .iter()
            .find(|plan| !plan.is_reimport() && plan.name() == "UpdateTicket")
            .expect("find v1 UpdateTicket");
        let plan_ref = ExecutablePlanRef::new(
            v1_stored.lineage().clone(),
            v1_stored.contract_version(),
            v1_stored.bundle_hash(),
            v1_command.command_id(),
            v1_command.plan_hash(),
        );
        let environment = Environment::new("production").expect("environment");
        let principal_id = ActorId::new("ticketdesk-runtime").expect("runtime actor");
        let caller_key =
            IdempotencyKey::new("ticketdesk-lifecycle-update-0").expect("idempotency key");
        let digests = DeterministicDigestProvider;
        let scope = CommandIdempotencyScopeV1::new(
            real.database_id,
            environment.clone(),
            TenantScope::Global,
            principal_id.clone(),
            v1.bundle().lineage().clone(),
            v1_command.command_id(),
        );
        let prepared =
            prepare_idempotency_lookup(&scope, &caller_key, &digests).expect("prepare lookup");
        let mut partition =
            PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate type"));
        partition.push_u64(1).expect("partition key");
        let partition = partition.finish().expect("finish partition");
        let pending = StoredPendingAdmissionV1::new(
            prepared.lookup_candidates().as_slice()[0].clone(),
            CanonicalInputHash::from_bytes([0x55; 32]),
            RequestId::from_unix_milliseconds_and_random(3, [0x33; 10]).expect("request id"),
            plan_ref,
            LogicalTime::new(Timestamp::new(1_500, 0).expect("logical time")),
            AdmittedActorContext::new(
                principal_id.clone(),
                ActorKind::Service,
                TenantScope::Global,
                None,
            ),
            partition.clone(),
            StoredAdmittedProvenanceClaimsV1::default(),
        )
        .expect("pending admission");
        let pre_eval = PreEvaluationCommitContext::new(
            pending,
            hash_partition_key(partition.as_bytes()),
            Vec::new(),
        )
        .expect("pre-evaluation context");
        let admission_request =
            AdmissionRequestV1::new(prepared.lookup_candidates().clone(), &pre_eval)
                .expect("admission request");
        assert!(matches!(
            AdmissionRepository::admit_or_resolve(&storage, admission_request)
                .expect("admit v1 command"),
            AdmissionResultV1::Created(_)
        ));

        let build_request = |key: &str| {
            let input = SubmittedRecord::new_command_input(vec![SubmittedField::new(
                SubmittedFieldIdentity::Name(
                    SourceName::new("idempotency_key").expect("field name"),
                ),
                SubmittedValue::string(key).expect("key value"),
            )])
            .expect("submitted record");
            ExecuteCommandRequest::new(
                SourceName::new("UpdateTicket").expect("command name"),
                Some(ContractVersion::new(1).expect("v1")),
                input,
            )
            .expect("command request")
        };

        let normalized = normalize_historical_replay_request_for_principal(
            &storage,
            &digests,
            real.database_id,
            &environment,
            &principal_id,
            build_request("ticketdesk-lifecycle-update-0"),
        );
        assert_eq!(
            normalized.expected_contract_version(),
            Some(ContractVersion::new(2).expect("v2"))
        );

        let uncommitted = normalize_historical_replay_request_for_principal(
            &storage,
            &digests,
            real.database_id,
            &environment,
            &principal_id,
            build_request("ticketdesk-uncommitted"),
        );
        assert_eq!(
            uncommitted.expected_contract_version(),
            Some(ContractVersion::new(1).expect("v1"))
        );

        let other_principal = ActorId::new("other-runtime").expect("other actor");
        let wrong_principal = normalize_historical_replay_request_for_principal(
            &storage,
            &digests,
            real.database_id,
            &environment,
            &other_principal,
            build_request("ticketdesk-lifecycle-update-0"),
        );
        assert_eq!(
            wrong_principal.expected_contract_version(),
            Some(ContractVersion::new(1).expect("v1"))
        );
    }

    // req: REP-002, REP-003, REP-006, ID-001, ID-002
    #[tokio::test]
    async fn quorum_commit_waits_for_k_follower_acks_and_returns_unknown_on_timeout() {
        use std::num::NonZeroU32;
        use std::time::Duration;

        use riffdb_errors::PublicErrorKind;
        use riffdb_storage_api::{
            ReplicationSourceHoldIdV1, ReplicationSourceHoldKindV1, ReplicationSourceHoldV1,
        };
        use riffdb_types::CommitSequence;

        let real = crate::real_storage_support::RealStorage::open("quorum-commit-barrier");
        let shared = real.storage.shared_read_ports();
        let initial_progress = shared
            .observe_replication_source_progress_v3()
            .expect("observe initial source progress");
        let initial_history = initial_progress.history();
        let lineage = initial_history.lineage();

        let follower_a_id = ReplicationSourceHoldIdV1::new([0xA1; 16]).expect("follower A hold id");
        let follower_b_id = ReplicationSourceHoldIdV1::new([0xB2; 16]).expect("follower B hold id");
        assert!(
            shared
                .register_replication_source_hold_v3(ReplicationSourceHoldV1::new(
                    follower_a_id,
                    ReplicationSourceHoldKindV1::FollowerAcknowledgement,
                    lineage,
                    initial_history.tail(),
                ))
                .expect("register follower A hold")
        );
        assert!(
            shared
                .register_replication_source_hold_v3(ReplicationSourceHoldV1::new(
                    follower_b_id,
                    ReplicationSourceHoldKindV1::FollowerAcknowledgement,
                    lineage,
                    initial_history.tail(),
                ))
                .expect("register follower B hold")
        );

        let commit_1 = CommitSequence::new(1).expect("commit sequence 1");
        let commit_1_history = shared
            .record_application_commit_sequence_for_test(commit_1)
            .expect("advance application commit 1");

        // Advance only follower A; with required_followers = 2, quorum is not yet met.
        assert!(
            shared
                .advance_replication_source_hold_v3(ReplicationSourceHoldV1::new(
                    follower_a_id,
                    ReplicationSourceHoldKindV1::FollowerAcknowledgement,
                    lineage,
                    commit_1_history.tail(),
                ))
                .expect("advance follower A to commit 1")
        );

        let (ack_tx, ack_rx) = tokio::sync::watch::channel(1_u64);
        let dummy_inner: Arc<dyn riffdb_service::ApplicationService> = Arc::new(
            LifecycleApplicationService::new(Arc::new(RecordingRoute::default())),
        );
        let timeout_service = ProductionApplicationService::new(
            Arc::clone(&dummy_inner),
            real.storage.clone(),
            Arc::new(DeterministicDigestProvider),
            real.database_id,
            Environment::new("production").expect("environment"),
        )
        .with_commit_quorum(
            crate::config::CommitAcknowledgementMode::Quorum {
                required_followers: NonZeroU32::new(2).expect("2 followers"),
                timeout: Duration::from_millis(50),
            },
            Some(ack_rx.clone()),
        );

        let err = timeout_service
            .wait_for_commit_quorum(commit_1)
            .await
            .expect_err("1 of 2 follower acks must time out with OUTCOME_UNKNOWN");
        assert_eq!(err.kind(), PublicErrorKind::OutcomeUnknown);

        // Now test that advancing follower B and notifying wakes the quorum barrier immediately.
        let ready_service = ProductionApplicationService::new(
            dummy_inner,
            real.storage.clone(),
            Arc::new(DeterministicDigestProvider),
            real.database_id,
            Environment::new("production").expect("environment"),
        )
        .with_commit_quorum(
            crate::config::CommitAcknowledgementMode::Quorum {
                required_followers: NonZeroU32::new(2).expect("2 followers"),
                timeout: Duration::from_secs(5),
            },
            Some(ack_rx),
        );

        let shared_for_ack = shared.clone();
        let ack_task = tokio::spawn(async move {
            tokio::task::yield_now().await;
            assert!(
                shared_for_ack
                    .advance_replication_source_hold_v3(ReplicationSourceHoldV1::new(
                        follower_b_id,
                        ReplicationSourceHoldKindV1::FollowerAcknowledgement,
                        lineage,
                        commit_1_history.tail(),
                    ))
                    .expect("advance follower B to commit 1")
            );
            let _ = ack_tx.send(2);
        });

        ready_service
            .wait_for_commit_quorum(commit_1)
            .await
            .expect("quorum commit succeeds once both followers acknowledge commit 1");
        ack_task.await.expect("follower B ack task");
    }
}
