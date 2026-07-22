//! Object-safe operation-specific application-service traits.

use std::future::Future;
use std::pin::Pin;

use crate::{
    ContractValidationResult, CreateCapabilityInvocation, CreateCapabilityResult,
    DeployContractRequest, DeployContractResult, DiscoverCommandToolsRequest,
    DiscoverCommandToolsResult, DiscoverResourcesRequest, DiscoverResourcesResult,
    ExecuteCommandRequest, ExecuteCommandResult, ExplainCommandRequest, ExplainCommandResult,
    GetActiveContractRequest, GetActiveContractResult, GetCommitRequest, GetCommitResult,
    GetContractVersionRequest, GetContractVersionResult, GetEntityRequest, GetEntityResult,
    GetProjectionStatusRequest, GetProjectionStatusResult, HealthContext, HealthRequest,
    HealthResult, ListPendingOutboxDeliveriesRequest, ListPendingOutboxDeliveriesResult,
    QueryProjectionRequest, QueryProjectionResult, RequestContext, ResolveCommandOutcomeRequest,
    ResolveCommandOutcomeResult, RevokeCapabilityRequest, RevokeCapabilityResult,
    ScanCommitsRequest, ScanCommitsResult, ScanIndexRequest, ScanIndexResult, ServiceResult,
    StatisticsRequest, StatisticsResult, SubscribeToCommitsRequest, SubscribeToCommitsResult,
    TraceProvenanceRequest, TraceProvenanceResult, ValidateContractRequest,
};

/// One boxed, sendable operation future used to keep service traits object-safe.
pub type ServiceFuture<'a, T> = Pin<Box<dyn Future<Output = ServiceResult<T>> + Send + 'a>>;

/// Contract validation, explanation, deployment, and catalog reads.
pub trait ContractApplication: Send + Sync {
    /// Validates bounded contract source without deploying it.
    fn validate_contract(
        &self,
        context: RequestContext,
        request: ValidateContractRequest,
    ) -> ServiceFuture<'_, ContractValidationResult>;

    /// Explains one exact checked command plan.
    fn explain_command(
        &self,
        context: RequestContext,
        request: ExplainCommandRequest,
    ) -> ServiceFuture<'_, ExplainCommandResult>;

    /// Compiles and attempts one typed catalog deployment.
    fn deploy_contract(
        &self,
        context: RequestContext,
        request: DeployContractRequest,
    ) -> ServiceFuture<'_, DeployContractResult>;

    /// Reads the active checked contract descriptor.
    fn get_active_contract(
        &self,
        context: RequestContext,
        request: GetActiveContractRequest,
    ) -> ServiceFuture<'_, GetActiveContractResult>;

    /// Reads one exact historical checked contract descriptor.
    fn get_contract_version(
        &self,
        context: RequestContext,
        request: GetContractVersionRequest,
    ) -> ServiceFuture<'_, GetContractVersionResult>;
}

/// Checked command invocation and outcome recovery.
pub trait CommandApplication: Send + Sync {
    /// Executes one classified command through the sole-writer coordinator.
    fn execute_command(
        &self,
        context: RequestContext,
        request: ExecuteCommandRequest,
    ) -> ServiceFuture<'_, ExecuteCommandResult>;

    /// Resolves one durable outcome under current disclosure policy.
    fn resolve_command_outcome(
        &self,
        context: RequestContext,
        request: ResolveCommandOutcomeRequest,
    ) -> ServiceFuture<'_, ResolveCommandOutcomeResult>;
}

/// Authoritative and derived data queries.
pub trait QueryApplication: Send + Sync {
    /// Reads one exact entity with policy-selected fields.
    fn get_entity(
        &self,
        context: RequestContext,
        request: GetEntityRequest,
    ) -> ServiceFuture<'_, GetEntityResult>;

    /// Scans one checked index with bounded server-side pagination.
    fn scan_index(
        &self,
        context: RequestContext,
        request: ScanIndexRequest,
    ) -> ServiceFuture<'_, ScanIndexResult>;

    /// Queries one exact projection generation/frontier boundary.
    fn query_projection(
        &self,
        context: RequestContext,
        request: QueryProjectionRequest,
    ) -> ServiceFuture<'_, QueryProjectionResult>;

    /// Reads one exact projection lifecycle status.
    fn get_projection_status(
        &self,
        context: RequestContext,
        request: GetProjectionStatusRequest,
    ) -> ServiceFuture<'_, GetProjectionStatusResult>;
}

/// Commit-log reads, subscription establishment, and provenance tracing.
pub trait CommitApplication: Send + Sync {
    /// Reads one exact application commit.
    fn get_commit(
        &self,
        context: RequestContext,
        request: GetCommitRequest,
    ) -> ServiceFuture<'_, GetCommitResult>;

    /// Scans an upper-fenced bounded commit page.
    fn scan_commits(
        &self,
        context: RequestContext,
        request: ScanCommitsRequest,
    ) -> ServiceFuture<'_, ScanCommitsResult>;

    /// Establishes one bounded, policy-filtered commit subscription.
    fn subscribe_to_commits(
        &self,
        context: RequestContext,
        request: SubscribeToCommitsRequest,
    ) -> ServiceFuture<'_, SubscribeToCommitsResult>;

    /// Traces from one exact commit or provenance selector.
    fn trace_provenance(
        &self,
        context: RequestContext,
        request: TraceProvenanceRequest,
    ) -> ServiceFuture<'_, TraceProvenanceResult>;
}

/// Health, statistics, capabilities, and outbox administration.
pub trait AdministrationApplication: Send + Sync {
    /// Returns either restricted pre-bootstrap or authenticated health.
    fn health(
        &self,
        context: HealthContext,
        request: HealthRequest,
    ) -> ServiceFuture<'_, HealthResult>;

    /// Returns bounded authenticated operational statistics.
    fn statistics(
        &self,
        context: RequestContext,
        request: StatisticsRequest,
    ) -> ServiceFuture<'_, StatisticsResult>;

    /// Performs exactly one normal or bootstrap capability-create invocation.
    fn create_capability(
        &self,
        invocation: CreateCapabilityInvocation,
    ) -> ServiceFuture<'_, CreateCapabilityResult>;

    /// Revokes one capability through current policy and the coordinator.
    fn revoke_capability(
        &self,
        context: RequestContext,
        request: RevokeCapabilityRequest,
    ) -> ServiceFuture<'_, RevokeCapabilityResult>;

    /// Lists bounded payload-free pending outbox status.
    fn list_pending_outbox_deliveries(
        &self,
        context: RequestContext,
        request: ListPendingOutboxDeliveriesRequest,
    ) -> ServiceFuture<'_, ListPendingOutboxDeliveriesResult>;
}

/// Current-policy-filtered command-tool and resource discovery.
pub trait DiscoveryApplication: Send + Sync {
    /// Lists visible command descriptors with compiler-owned names verbatim.
    fn discover_command_tools(
        &self,
        context: RequestContext,
        request: DiscoverCommandToolsRequest,
    ) -> ServiceFuture<'_, DiscoverCommandToolsResult>;

    /// Lists visible semantic resource descriptors.
    fn discover_resources(
        &self,
        context: RequestContext,
        request: DiscoverResourcesRequest,
    ) -> ServiceFuture<'_, DiscoverResourcesResult>;
}

/// Marker trait grouping the six coherent object-safe surfaces.
pub trait ApplicationService:
    ContractApplication
    + CommandApplication
    + QueryApplication
    + CommitApplication
    + AdministrationApplication
    + DiscoveryApplication
{
}

impl<T> ApplicationService for T where
    T: ContractApplication
        + CommandApplication
        + QueryApplication
        + CommitApplication
        + AdministrationApplication
        + DiscoveryApplication
{
}
