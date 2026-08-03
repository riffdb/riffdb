//! API-neutral transport proxy over the one production lifecycle route.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use riffdb_api_grpc::{
    GrpcDeploymentCompletion, GrpcLifecycleRoute, GrpcOfflineMaintenanceOperation,
    classify_grpc_deployment_completion,
};
use riffdb_errors::PublicError;
use riffdb_service::{
    AdministrationApplication, CheckSymbolicQueryResult, CommandApplication, CommitApplication,
    CompileSymbolicQueryRequest, ConsumeContextualSubscriptionRequest,
    ConsumeContextualSubscriptionResult, ConsumeEventStreamRequest, ConsumeEventStreamResult,
    ContextualSubscriptionApplication, ContractApplication, ContractValidationResult,
    CreateCapabilityInvocation, CreateCapabilityResult, CreateOfflineBackupRequest,
    DeployContractRequest, DeployContractResult, DeployQueryModuleRequest, DeployQueryModuleResult,
    DeployReactiveModuleRequest, DeployReactiveModuleResult, DescribeEventRequest,
    DescribeEventResult, DescribeSymbolicContractResult, DiscoverCommandToolsRequest,
    DiscoverCommandToolsResult, DiscoverResourcesRequest, DiscoverResourcesResult,
    DiscoveryApplication, EventConsumerLeaseSelection, EventConsumerMutationResult,
    EventConsumerSelection, EventConsumerServiceApplication, EventConsumerStatus,
    EventServiceApplication, ExecuteCommandRequest, ExecuteCommandResult,
    ExecuteContextualReactionRequest, ExecuteProjectedQueryRequest, ExecuteProjectedQueryResult,
    ExecuteSymbolicQueryRequest, ExecuteSymbolicQueryResult, ExplainCommandRequest,
    ExplainCommandResult, ExplainSymbolicQueryResult, GetActiveContractRequest,
    GetActiveContractResult, GetCommitRequest, GetCommitResult, GetContractVersionRequest,
    GetContractVersionResult, GetEntityRequest, GetEntityResult,
    GetOfflineMaintenanceOperationRequest, GetOfflineMaintenanceOperationResult,
    GetProjectionStatusRequest, GetProjectionStatusResult, GetQueryModuleRequest, HealthContext,
    HealthRequest, HealthResult, ListPendingOutboxDeliveriesRequest,
    ListPendingOutboxDeliveriesResult, LiveNamedQueryApplication, NamedSymbolicQueryRequest,
    NegativeAcknowledgeEventStreamRequest, OfflineMaintenanceApplication,
    OfflineMaintenanceStartResult, ProjectedQueryApplication, QueryApplication,
    QueryModuleInspection, QueryProjectionRequest, QueryProjectionResult, ReplayEventsRequest,
    ReplayEventsResult, RequestContext, ResolveCommandOutcomeRequest, ResolveCommandOutcomeResult,
    RestoreOfflineBackupInvocation, RevokeCapabilityRequest, RevokeCapabilityResult,
    ScanCommitsRequest, ScanCommitsResult, ScanIndexRequest, ScanIndexResult,
    SeekEventStreamConsumerRequest, ServiceFuture, StatisticsRequest, StatisticsResult,
    SubscribeToCommitsRequest, SubscribeToCommitsResult, SymbolicContractSelector,
    SymbolicQueryApplication, TailEventsRequest, TailEventsResult, TraceProvenanceRequest,
    TraceProvenanceResult, ValidateContractRequest, WatchLiveNamedQueryRequest,
    WatchLiveNamedQueryResult,
};
use riffdb_types::ServiceOperationV1;

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
        ) -> Option<EventConsumerStatus> => GetEventStreamConsumerStatus;
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
    ) -> ServiceFuture<'_, Option<EventConsumerStatus>> {
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

delegate_operation! {
    DiscoveryApplication {
        discover_command_tools(
            context: RequestContext,
            request: DiscoverCommandToolsRequest
        ) -> DiscoverCommandToolsResult => DiscoverCommandTools;
        discover_resources(
            context: RequestContext,
            request: DiscoverResourcesRequest
        ) -> DiscoverResourcesResult => DiscoverResources;
    }
}

delegate_operation! {
    SymbolicQueryApplication {
        describe_symbolic_contract(
            context: RequestContext,
            request: SymbolicContractSelector
        ) -> DescribeSymbolicContractResult => DescribeContract;
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
}
