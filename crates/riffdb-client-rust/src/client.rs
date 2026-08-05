//! Typed Tonic facade over the frozen public RiffDB protocol.

use std::error::Error;
use std::fmt;
use std::time::Duration;

use riffdb_api_grpc::generated::{
    admin_service_client::AdminServiceClient, command_service_client::CommandServiceClient,
    commit_service_client::CommitServiceClient, contract_service_client::ContractServiceClient,
    event_service_client::EventServiceClient, query_service_client::QueryServiceClient,
};
use riffdb_api_grpc::generated_app::application_query_service_client::ApplicationQueryServiceClient;
use riffdb_proto::{
    PublicMessage, validate_apply_contract_migration_exchange,
    validate_check_contract_migration_exchange, validate_contract_validation_exchange,
    validate_create_capability_exchange, validate_create_offline_backup_exchange,
    validate_discover_command_tools_exchange, validate_discover_resources_exchange,
    validate_explain_command_exchange, validate_get_contract_migration_operation_exchange,
    validate_get_contract_version_exchange, validate_get_offline_maintenance_operation_exchange,
    validate_get_outcome_exchange, validate_get_projection_status_exchange,
    validate_list_pending_outbox_deliveries_exchange, validate_public_message,
    validate_query_projection_exchange, validate_restore_offline_backup_exchange,
    validate_scan_commits_exchange, validate_scan_index_exchange,
    validate_trace_provenance_exchange,
};
use riffdb_proto::{app::v1 as app_v1, v1};
use riffdb_types::{CommitSequence, RequestId};
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Streaming};

use crate::application::contextualize_command_client_error;
use crate::command::{RetryDecision, RetryState, apply_overloaded_backoff};
use crate::generated::{GeneratedCommand, GeneratedCommandError};
use crate::status::{
    ClientError, ProtocolFailure, ProtocolFailureKind, checked_application_status, checked_status,
};
use crate::{
    ApplyContractMigration, AttemptBudget, BootstrapCallMetadata,
    BootstrapCapabilityCreateTemplate, CallMetadata, CheckContractMigration, CreateOfflineBackup,
    IdempotentCommand, NormalCapabilityCreateTemplate, RestoreOfflineBackup, SystemIdSource,
};

pub(crate) trait RetryRequestIdSource {
    fn next_request_id(&mut self) -> Result<RequestId, crate::IdentifierGenerationError>;
}

impl RetryRequestIdSource for SystemIdSource {
    fn next_request_id(&mut self) -> Result<RequestId, crate::IdentifierGenerationError> {
        (*self).request_id()
    }
}

pub(crate) trait ExecuteRetryAttempt {
    async fn submit_execute_attempt(
        &mut self,
        request: v1::ExecuteCommandRequest,
        metadata: &CallMetadata,
    ) -> Result<v1::ExecuteCommandResponse, ClientError>;
}

trait NormalCapabilityRetryAttempt {
    async fn submit_normal_capability_attempt(
        &mut self,
        request: v1::CreateCapabilityRequest,
        metadata: &CallMetadata,
    ) -> Result<v1::CreateCapabilityResponse, ClientError>;
}

trait BootstrapCapabilityRetryAttempt {
    async fn submit_bootstrap_capability_attempt(
        &mut self,
        request: v1::CreateCapabilityRequest,
        metadata: &BootstrapCallMetadata,
    ) -> Result<v1::CreateCapabilityResponse, ClientError>;
}

macro_rules! unary {
    ($name:ident, $client:ident, $rpc:ident, $request:ty, $response:ty) => {
        #[doc = concat!("Performs the checked `", stringify!($rpc), "` unary RPC.")]
        pub async fn $name(
            &mut self,
            message: $request,
            metadata: &CallMetadata,
        ) -> Result<$response, ClientError> {
            // Walk inventory (five structural validations per unary round-trip):
            // (1) client validate_outbound — certain local fail-fast classification
            // (2) client StrictProstEncoder — wire-bound encode
            // (3) server StrictProstDecoder — wire contract on requests
            // (4) server StrictProstEncoder — server fail-closed outbound
            // (5) client StrictProstDecoder — canonical wire on responses
            // Inbound re-walk after decode is intentionally absent (codec owns it).
            validate_outbound(&message)?;
            let mut request = Request::new(message);
            metadata.apply(&mut request);
            let response = self
                .$client
                .$rpc(request)
                .await
                .map_err(checked_status)?
                .into_inner();
            Ok(response)
        }
    };
}

macro_rules! unary_exchange {
    ($name:ident, $client:ident, $rpc:ident, $request:ty, $response:ty, $exchange:path) => {
        #[doc = concat!("Performs the checked `", stringify!($rpc), "` unary RPC.")]
        pub async fn $name(
            &mut self,
            message: $request,
            metadata: &CallMetadata,
        ) -> Result<$response, ClientError> {
            validate_outbound(&message)?;
            let exchange_request = message.clone();
            let mut request = Request::new(message);
            metadata.apply(&mut request);
            let response = self
                .$client
                .$rpc(request)
                .await
                .map_err(checked_status)?
                .into_inner();
            // Structural re-walk of the response is omitted: the decoder already
            // ran decode_public_message. Exchange checks assert request↔response
            // relations only (they may re-validate structure internally — that
            // is riffdb-proto ownership, not this client layer).
            $exchange(&exchange_request, &response).map_err(|_| invalid_inbound())?;
            Ok(response)
        }
    };
}

macro_rules! unary_application {
    ($name:ident, $rpc:ident, $request:ty, $response:ty) => {
        #[doc = concat!(
                                                            "Performs the checked application `",
                                                            stringify!($rpc),
                                                            "` unary RPC."
                                                        )]
        pub async fn $name(
            &mut self,
            message: $request,
            metadata: &CallMetadata,
        ) -> Result<$response, ClientError> {
            validate_outbound(&message)?;
            let mut request = Request::new(message);
            metadata.apply(&mut request);
            let response = self
                .application_query
                .$rpc(request)
                .await
                .map_err(checked_application_status)?
                .into_inner();
            Ok(response)
        }
    };
}

/// Explicit per-request observed incarnation wins; otherwise use remembered.
#[must_use]
pub(crate) const fn merge_observed_history_incarnation(
    explicit: Option<u64>,
    remembered: Option<u64>,
) -> Option<u64> {
    if explicit.is_some() {
        explicit
    } else {
        remembered
    }
}

/// Typed clients for every service in the public `riffdb.v1` API.
///
/// Clones share one Tonic channel while retaining independent generated client
/// readiness state.
#[derive(Clone)]
pub struct RiffDbClient {
    contract: ContractServiceClient<Channel>,
    command: CommandServiceClient<Channel>,
    query: QueryServiceClient<Channel>,
    commit: CommitServiceClient<Channel>,
    event: EventServiceClient<Channel>,
    admin: AdminServiceClient<Channel>,
    application_query: ApplicationQueryServiceClient<Channel>,
    /// Optional client-remembered history incarnation for commit read surfaces.
    ///
    /// When set, [`Self::apply_observed_history_incarnation`] fills request
    /// fields that are still unset so callers can fence stale history after
    /// a destructive restore (ADR-0072).
    observed_history_incarnation: Option<u64>,
}

impl RiffDbClient {
    /// Connects all typed service clients over one lazily cloned channel.
    pub async fn connect(endpoint: Endpoint) -> Result<Self, ClientError> {
        let channel = endpoint
            .connect()
            .await
            .map_err(|_| ClientError::ConnectionFailure)?;
        Ok(Self::from_channel(channel))
    }

    /// Constructs all typed service clients over an existing channel.
    #[must_use]
    pub fn from_channel(channel: Channel) -> Self {
        Self {
            contract: ContractServiceClient::new(channel.clone()),
            command: CommandServiceClient::new(channel.clone()),
            query: QueryServiceClient::new(channel.clone()),
            commit: CommitServiceClient::new(channel.clone()),
            event: EventServiceClient::new(channel.clone()),
            admin: AdminServiceClient::new(channel.clone()),
            application_query: ApplicationQueryServiceClient::new(channel),
            observed_history_incarnation: None,
        }
    }

    /// Remembers an observed history incarnation for subsequent commit reads.
    ///
    /// Pass `None` to clear. Explicit per-request values always win.
    pub fn set_observed_history_incarnation(&mut self, observed: Option<u64>) {
        self.observed_history_incarnation = observed;
    }

    /// Returns the client-remembered observed history incarnation, if any.
    #[must_use]
    pub const fn observed_history_incarnation(&self) -> Option<u64> {
        self.observed_history_incarnation
    }

    /// Fills `observed_history_incarnation` on a get-commit request when unset.
    #[must_use]
    pub fn apply_observed_history_incarnation_get(
        &self,
        mut request: v1::GetCommitRequest,
    ) -> v1::GetCommitRequest {
        request.observed_history_incarnation = merge_observed_history_incarnation(
            request.observed_history_incarnation,
            self.observed_history_incarnation,
        );
        request
    }

    /// Fills `observed_history_incarnation` on a scan-commits request when unset.
    #[must_use]
    pub fn apply_observed_history_incarnation_scan(
        &self,
        mut request: v1::ScanCommitsRequest,
    ) -> v1::ScanCommitsRequest {
        request.observed_history_incarnation = merge_observed_history_incarnation(
            request.observed_history_incarnation,
            self.observed_history_incarnation,
        );
        request
    }

    /// Fills `observed_history_incarnation` on a subscribe request when unset.
    #[must_use]
    pub fn apply_observed_history_incarnation_subscribe(
        &self,
        mut request: v1::SubscribeCommitsRequest,
    ) -> v1::SubscribeCommitsRequest {
        request.observed_history_incarnation = merge_observed_history_incarnation(
            request.observed_history_incarnation,
            self.observed_history_incarnation,
        );
        request
    }

    /// Remembers the history incarnation reported by a get-commit response.
    pub fn remember_history_incarnation_from_get(&mut self, response: &v1::GetCommitResponse) {
        if response.history_incarnation >= 1 {
            self.observed_history_incarnation = Some(response.history_incarnation);
        }
    }

    /// Remembers the history incarnation reported by a commit page.
    pub fn remember_history_incarnation_from_scan(&mut self, response: &v1::ScanCommitsResponse) {
        if let Some(page) = response.page.as_ref()
            && page.history_incarnation >= 1
        {
            self.observed_history_incarnation = Some(page.history_incarnation);
        }
    }

    unary_application!(
        describe_contract,
        describe_contract,
        app_v1::DescribeContractRequest,
        app_v1::DescribeContractResponse
    );
    unary_application!(
        check_query,
        check_query,
        app_v1::CheckQueryRequest,
        app_v1::CheckQueryResponse
    );
    unary_application!(
        explain_query,
        explain_query,
        app_v1::ExplainQueryRequest,
        app_v1::ExplainQueryResponse
    );
    unary_application!(
        execute_query,
        execute_query,
        app_v1::ExecuteQueryRequest,
        app_v1::ExecuteQueryResponse
    );
    unary_application!(
        deploy_query_module,
        deploy_query_module,
        app_v1::DeployQueryModuleRequest,
        app_v1::DeployQueryModuleResponse
    );
    unary_application!(
        deploy_reactive_module,
        deploy_reactive_module,
        app_v1::DeployReactiveModuleRequest,
        app_v1::DeployReactiveModuleResponse
    );
    unary_application!(
        get_query_module,
        get_query_module,
        app_v1::GetQueryModuleRequest,
        app_v1::GetQueryModuleResponse
    );
    unary_application!(
        execute_projected_query_raw,
        execute_projected_query,
        app_v1::ExecuteProjectedQueryRequest,
        app_v1::ExecuteProjectedQueryResponse
    );

    unary_exchange!(
        validate_contract,
        contract,
        validate_contract,
        v1::ValidateContractRequest,
        v1::ValidateContractResponse,
        validate_contract_validation_exchange
    );
    unary_exchange!(
        explain_command,
        contract,
        explain_command,
        v1::ExplainCommandRequest,
        v1::ExplainCommandResponse,
        validate_explain_command_exchange
    );
    unary!(
        deploy_contract,
        contract,
        deploy_contract,
        v1::DeployContractRequest,
        v1::DeployContractResponse
    );
    unary!(
        get_active_contract,
        contract,
        get_active_contract,
        v1::GetActiveContractRequest,
        v1::GetActiveContractResponse
    );
    unary_exchange!(
        get_contract_version,
        contract,
        get_contract_version,
        v1::GetContractVersionRequest,
        v1::GetContractVersionResponse,
        validate_get_contract_version_exchange
    );
    unary_exchange!(
        discover_command_tools,
        contract,
        discover_command_tools,
        v1::DiscoverCommandToolsRequest,
        v1::DiscoverCommandToolsResponse,
        validate_discover_command_tools_exchange
    );
    unary_exchange!(
        discover_resources,
        contract,
        discover_resources,
        v1::DiscoverResourcesRequest,
        v1::DiscoverResourcesResponse,
        validate_discover_resources_exchange
    );
    unary!(
        get_reactive_wakeup,
        contract,
        get_reactive_wakeup,
        v1::GetReactiveWakeupRequest,
        v1::GetReactiveWakeupResponse
    );
    unary!(
        execute,
        command,
        execute,
        v1::ExecuteCommandRequest,
        v1::ExecuteCommandResponse
    );
    unary_exchange!(
        execute_batch,
        command,
        execute_batch,
        v1::ExecuteCommandBatchRequest,
        v1::ExecuteCommandBatchResponse,
        riffdb_proto::validate_execute_command_batch_exchange
    );
    unary_exchange!(
        get_outcome,
        command,
        get_outcome,
        v1::GetOutcomeRequest,
        v1::GetOutcomeResponse,
        validate_get_outcome_exchange
    );
    unary!(
        get_entity,
        query,
        get_entity,
        v1::GetEntityRequest,
        v1::GetEntityResponse
    );
    unary_exchange!(
        scan_index,
        query,
        scan_index,
        v1::ScanIndexRequest,
        v1::ScanIndexResponse,
        validate_scan_index_exchange
    );
    unary_exchange!(
        query_projection,
        query,
        query_projection,
        v1::QueryProjectionRequest,
        v1::QueryProjectionResponse,
        validate_query_projection_exchange
    );
    unary_exchange!(
        get_projection_status,
        query,
        get_projection_status,
        v1::GetProjectionStatusRequest,
        v1::GetProjectionStatusResponse,
        validate_get_projection_status_exchange
    );

    /// Queries a projection while asking the server to wait for one sequence.
    ///
    /// The server owns waiting and frontier correctness. This helper only maps
    /// the exact `Duration` into the protocol's total-nanosecond field and
    /// retains every caller-selected query, page, and fresh request ID field.
    pub async fn wait_for_projection(
        &mut self,
        mut message: v1::QueryProjectionRequest,
        required_sequence: CommitSequence,
        maximum_wait: Duration,
        metadata: &CallMetadata,
    ) -> Result<v1::QueryProjectionResponse, ClientError> {
        configure_projection_wait(&mut message, required_sequence, maximum_wait)?;
        self.query_projection(message, metadata).await
    }
    unary!(
        get_commit,
        commit,
        get_commit,
        v1::GetCommitRequest,
        v1::GetCommitResponse
    );
    unary_exchange!(
        scan_commits,
        commit,
        scan_commits,
        v1::ScanCommitsRequest,
        v1::ScanCommitsResponse,
        validate_scan_commits_exchange
    );
    unary_exchange!(
        trace_provenance,
        commit,
        trace_provenance,
        v1::TraceProvenanceRequest,
        v1::TraceProvenanceResponse,
        validate_trace_provenance_exchange
    );
    unary!(
        describe_event,
        event,
        describe_event,
        v1::DescribeEventRequest,
        v1::DescribeEventResponse
    );
    unary!(
        replay_events,
        event,
        replay_events,
        v1::ReplayEventsRequest,
        v1::ReplayEventsResponse
    );
    unary!(
        tail_events,
        event,
        tail_events,
        v1::TailEventsRequest,
        v1::TailEventsResponse
    );
    unary!(
        consume_event_stream,
        event,
        consume_event_stream,
        v1::ConsumeEventStreamRequest,
        v1::ConsumeEventStreamResponse
    );
    unary!(
        acknowledge_event_stream,
        event,
        acknowledge_event_stream,
        v1::AcknowledgeEventStreamRequest,
        v1::EventConsumerMutationResponse
    );
    unary!(
        negative_acknowledge_event_stream,
        event,
        negative_acknowledge_event_stream,
        v1::NegativeAcknowledgeEventStreamRequest,
        v1::EventConsumerMutationResponse
    );
    unary!(
        seek_event_stream_consumer,
        event,
        seek_event_stream_consumer,
        v1::SeekEventStreamConsumerRequest,
        v1::EventConsumerMutationResponse
    );
    unary!(
        retire_event_stream_consumer,
        event,
        retire_event_stream_consumer,
        v1::RetireEventStreamConsumerRequest,
        v1::EventConsumerMutationResponse
    );
    unary!(
        get_event_stream_consumer_status,
        event,
        get_event_stream_consumer_status,
        v1::GetEventStreamConsumerStatusRequest,
        v1::GetEventStreamConsumerStatusResponse
    );
    unary!(
        consume_contextual_subscription,
        event,
        consume_contextual_subscription,
        v1::ConsumeContextualSubscriptionRequest,
        v1::ConsumeContextualSubscriptionResponse
    );
    unary!(
        acknowledge_contextual_subscription,
        event,
        acknowledge_contextual_subscription,
        v1::AcknowledgeContextualSubscriptionRequest,
        v1::EventConsumerMutationResponse
    );
    unary!(
        negative_acknowledge_contextual_subscription,
        event,
        negative_acknowledge_contextual_subscription,
        v1::NegativeAcknowledgeContextualSubscriptionRequest,
        v1::EventConsumerMutationResponse
    );
    unary!(
        get_contextual_subscription_status,
        event,
        get_contextual_subscription_status,
        v1::GetContextualSubscriptionStatusRequest,
        v1::GetEventStreamConsumerStatusResponse
    );
    unary!(
        execute_contextual_reaction,
        event,
        execute_contextual_reaction,
        v1::ExecuteContextualReactionRequest,
        v1::ExecuteCommandResponse
    );
    unary!(health, admin, health, v1::HealthRequest, v1::HealthResponse);
    unary!(stats, admin, stats, v1::StatsRequest, v1::StatsResponse);
    unary!(
        revoke_capability,
        admin,
        revoke_capability,
        v1::RevokeCapabilityRequest,
        v1::RevokeCapabilityResponse
    );
    unary_exchange!(
        list_pending_outbox_deliveries,
        admin,
        list_pending_outbox_deliveries,
        v1::ListPendingOutboxDeliveriesRequest,
        v1::ListPendingOutboxDeliveriesResponse,
        validate_list_pending_outbox_deliveries_exchange
    );
    unary_exchange!(
        create_offline_backup,
        admin,
        create_offline_backup,
        v1::CreateOfflineBackupRequest,
        v1::CreateOfflineBackupResponse,
        validate_create_offline_backup_exchange
    );
    unary_exchange!(
        restore_offline_backup,
        admin,
        restore_offline_backup,
        v1::RestoreOfflineBackupRequest,
        v1::RestoreOfflineBackupResponse,
        validate_restore_offline_backup_exchange
    );
    unary_exchange!(
        get_offline_maintenance_operation,
        admin,
        get_offline_maintenance_operation,
        v1::GetOfflineMaintenanceOperationRequest,
        v1::GetOfflineMaintenanceOperationResponse,
        validate_get_offline_maintenance_operation_exchange
    );
    unary_exchange!(
        check_contract_migration,
        admin,
        check_contract_migration,
        v1::CheckContractMigrationRequest,
        v1::CheckContractMigrationResponse,
        validate_check_contract_migration_exchange
    );
    unary_exchange!(
        apply_contract_migration,
        admin,
        apply_contract_migration,
        v1::ApplyContractMigrationRequest,
        v1::ApplyContractMigrationResponse,
        validate_apply_contract_migration_exchange
    );
    unary_exchange!(
        get_contract_migration_operation,
        admin,
        get_contract_migration_operation,
        v1::GetContractMigrationOperationRequest,
        v1::GetContractMigrationOperationResponse,
        validate_get_contract_migration_operation_exchange
    );

    /// Performs normal authenticated capability creation.
    pub async fn create_capability(
        &mut self,
        message: v1::CreateCapabilityRequest,
        metadata: &CallMetadata,
    ) -> Result<v1::CreateCapabilityResponse, ClientError> {
        if message.mode != v1::CapabilityCreateMode::Normal as i32 {
            return Err(invalid_outbound());
        }
        self.create_capability_with(message, metadata, CallMetadata::apply)
            .await
    }

    /// Performs the loopback-only bootstrap capability creation call.
    pub async fn create_bootstrap_capability(
        &mut self,
        message: v1::CreateCapabilityRequest,
        metadata: &BootstrapCallMetadata,
    ) -> Result<v1::CreateCapabilityResponse, ClientError> {
        if message.mode != v1::CapabilityCreateMode::Bootstrap as i32 {
            return Err(invalid_outbound());
        }
        self.create_capability_with(message, metadata, BootstrapCallMetadata::apply)
            .await
    }

    async fn create_capability_with<M>(
        &mut self,
        message: v1::CreateCapabilityRequest,
        metadata: &M,
        apply: fn(&M, &mut Request<v1::CreateCapabilityRequest>),
    ) -> Result<v1::CreateCapabilityResponse, ClientError> {
        validate_outbound(&message)?;
        let exchange_request = message.clone();
        let mut request = Request::new(message);
        apply(metadata, &mut request);
        let response = self
            .admin
            .create_capability(request)
            .await
            .map_err(checked_status)?
            .into_inner();
        validate_create_capability_exchange(&exchange_request, &response)
            .map_err(|_| invalid_inbound())?;
        Ok(response)
    }

    /// Starts a checked commit stream.
    ///
    /// Items are decoded by the strict codec (`decode_public_message`). This
    /// stream does not re-walk structure after decode; exchange-style relations
    /// do not apply to commit notifications.
    pub async fn subscribe_commits(
        &mut self,
        message: v1::SubscribeCommitsRequest,
        metadata: &CallMetadata,
    ) -> Result<CommitNotificationStream, ClientError> {
        validate_outbound(&message)?;
        let mut request = Request::new(message);
        metadata.apply(&mut request);
        let inner = self
            .commit
            .subscribe_commits(request)
            .await
            .map_err(checked_status)?
            .into_inner();
        Ok(CommitNotificationStream { inner })
    }

    /// Starts a checked durable event-consumer response stream.
    ///
    /// Items are decoded by the strict public codec. Consumer identity,
    /// authorization, leasing, and checkpoint semantics remain server-owned.
    pub async fn stream_event_consumer(
        &mut self,
        message: v1::ConsumeEventStreamRequest,
        metadata: &CallMetadata,
    ) -> Result<EventConsumerResponseStream, ClientError> {
        validate_outbound(&message)?;
        let mut request = Request::new(message);
        metadata.apply(&mut request);
        let inner = self
            .event
            .stream_event_consumer(request)
            .await
            .map_err(checked_status)?
            .into_inner();
        Ok(EventConsumerResponseStream { inner })
    }

    /// Starts a checked exact named-query live stream.
    ///
    /// The cursor is consistency evidence only; every reconnect is freshly
    /// authenticated and authorized by the server.
    pub async fn watch_named_query(
        &mut self,
        message: v1::WatchNamedQueryRequest,
        metadata: &CallMetadata,
    ) -> Result<LiveQueryUpdateStream, ClientError> {
        validate_outbound(&message)?;
        let mut request = Request::new(message);
        metadata.apply(&mut request);
        let inner = self
            .query
            .watch_named_query(request)
            .await
            .map_err(checked_status)?
            .into_inner();
        Ok(LiveQueryUpdateStream { inner })
    }

    /// Executes one immutable command with an explicit total submission bound.
    ///
    /// Retryable checked failures are resubmitted immediately with a fresh
    /// outer request ID. Overloaded retries apply bounded backoff (command
    /// module); other retryables resubmit immediately without inventing a
    /// generic retry policy. This method never mutates
    /// the command name, selected version, or opaque input.
    pub async fn execute_with_retry(
        &mut self,
        command: &IdempotentCommand,
        attempt_budget: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<v1::ExecuteCommandResponse, ClientError> {
        let mut request_ids = SystemIdSource::new();
        execute_retry_attempts(self, command, attempt_budget, metadata, &mut request_ids).await
    }

    /// Creates one normal capability with immutable identity and bounded retry.
    pub async fn create_capability_with_retry(
        &mut self,
        create: &NormalCapabilityCreateTemplate,
        attempt_budget: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<v1::CreateCapabilityResponse, ClientError> {
        let mut request_ids = SystemIdSource::new();
        normal_capability_retry_attempts(self, create, attempt_budget, metadata, &mut request_ids)
            .await
    }

    /// Creates one bootstrap capability with immutable identity and bounded retry.
    pub async fn create_bootstrap_capability_with_retry(
        &mut self,
        create: &BootstrapCapabilityCreateTemplate,
        attempt_budget: AttemptBudget,
        metadata: &BootstrapCallMetadata,
    ) -> Result<v1::CreateCapabilityResponse, ClientError> {
        let mut request_ids = SystemIdSource::new();
        bootstrap_capability_retry_attempts(
            self,
            create,
            attempt_budget,
            metadata,
            &mut request_ids,
        )
        .await
    }

    /// Starts or resolves one immutable backup-create operation with bounded retry.
    pub async fn create_offline_backup_with_retry(
        &mut self,
        create: &CreateOfflineBackup,
        attempt_budget: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<v1::CreateOfflineBackupResponse, ClientError> {
        let mut request_ids = SystemIdSource::new();
        let mut retry = RetryState::new(attempt_budget);
        loop {
            if !retry.begin_submission() {
                return Err(ClientError::OutcomeUnknown(crate::OutcomeUnknown));
            }
            let request_id = request_ids
                .next_request_id()
                .map_err(|error| retry.request_id_failure(error))?;
            retry.note_request_id(&request_id);
            match self
                .create_offline_backup(create.request(request_id), metadata)
                .await
            {
                Ok(response) => return Ok(response),
                Err(error) => match retry.handle_failure(error) {
                    RetryDecision::Retry => {}
                    RetryDecision::RetryAfter(delay) => {
                        apply_overloaded_backoff(delay).await;
                    }
                    RetryDecision::Return(error) => return Err(error),
                },
            }
        }
    }

    /// Starts or resolves one immutable restore operation with bounded retry.
    pub async fn restore_offline_backup_with_retry(
        &mut self,
        restore: &RestoreOfflineBackup,
        attempt_budget: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<v1::RestoreOfflineBackupResponse, ClientError> {
        let mut request_ids = SystemIdSource::new();
        let mut retry = RetryState::new(attempt_budget);
        loop {
            if !retry.begin_submission() {
                return Err(ClientError::OutcomeUnknown(crate::OutcomeUnknown));
            }
            let request_id = request_ids
                .next_request_id()
                .map_err(|error| retry.request_id_failure(error))?;
            retry.note_request_id(&request_id);
            match self
                .restore_offline_backup(restore.request(request_id), metadata)
                .await
            {
                Ok(response) => return Ok(response),
                Err(error) => match retry.handle_failure(error) {
                    RetryDecision::Retry => {}
                    RetryDecision::RetryAfter(delay) => {
                        apply_overloaded_backoff(delay).await;
                    }
                    RetryDecision::Return(error) => return Err(error),
                },
            }
        }
    }

    /// Starts or resolves one immutable migration check with bounded retry.
    pub async fn check_contract_migration_with_retry(
        &mut self,
        check: &CheckContractMigration,
        attempt_budget: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<v1::CheckContractMigrationResponse, ClientError> {
        let mut request_ids = SystemIdSource::new();
        let mut retry = RetryState::new(attempt_budget);
        loop {
            if !retry.begin_submission() {
                return Err(ClientError::OutcomeUnknown(crate::OutcomeUnknown));
            }
            let request_id = request_ids
                .next_request_id()
                .map_err(|error| retry.request_id_failure(error))?;
            retry.note_request_id(&request_id);
            match self
                .check_contract_migration(check.request(request_id), metadata)
                .await
            {
                Ok(response) => return Ok(response),
                Err(error) => match retry.handle_failure(error) {
                    RetryDecision::Retry => {}
                    RetryDecision::RetryAfter(delay) => apply_overloaded_backoff(delay).await,
                    RetryDecision::Return(error) => return Err(error),
                },
            }
        }
    }

    /// Starts or resolves one immutable confirmed migration apply with bounded retry.
    pub async fn apply_contract_migration_with_retry(
        &mut self,
        apply: &ApplyContractMigration,
        attempt_budget: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<v1::ApplyContractMigrationResponse, ClientError> {
        let mut request_ids = SystemIdSource::new();
        let mut retry = RetryState::new(attempt_budget);
        loop {
            if !retry.begin_submission() {
                return Err(ClientError::OutcomeUnknown(crate::OutcomeUnknown));
            }
            let request_id = request_ids
                .next_request_id()
                .map_err(|error| retry.request_id_failure(error))?;
            retry.note_request_id(&request_id);
            match self
                .apply_contract_migration(apply.request(request_id), metadata)
                .await
            {
                Ok(response) => return Ok(response),
                Err(error) => match retry.handle_failure(error) {
                    RetryDecision::Retry => {}
                    RetryDecision::RetryAfter(delay) => apply_overloaded_backoff(delay).await,
                    RetryDecision::Return(error) => return Err(error),
                },
            }
        }
    }

    /// Builds, submits, and decodes one generated command shape.
    pub async fn execute_generated<C: GeneratedCommand>(
        &mut self,
        command: &C,
        attempt_budget: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<GeneratedExecution<C::Outcome>, GeneratedExecutionError> {
        let generic = command
            .idempotent_command()
            .map_err(GeneratedExecutionError::CommandShape)?;
        let command_name = generic.command_name().to_owned();
        let response = self
            .execute_with_retry(&generic, attempt_budget, metadata)
            .await
            .map_err(|error| contextualize_command_client_error(error, &command_name))
            .map_err(GeneratedExecutionError::Client)?;
        let outcome = command
            .decode_outcome(&response)
            .map_err(GeneratedExecutionError::CommandShape)?;
        Ok(GeneratedExecution { response, outcome })
    }

    /// Executes a generated command and performs one exact same-key outcome
    /// lookup when the bounded submission loop ends in uncertainty.
    pub async fn execute_generated_with_recovery<C: GeneratedCommand>(
        &mut self,
        command: &C,
        attempt_budget: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<GeneratedExecution<C::Outcome>, GeneratedExecutionError> {
        let generic = command
            .idempotent_command()
            .map_err(GeneratedExecutionError::CommandShape)?;
        let command_name = generic.command_name().to_owned();
        let response = match self
            .execute_with_retry(&generic, attempt_budget, metadata)
            .await
        {
            Ok(response) => response,
            Err(ClientError::OutcomeUnknown(_)) => {
                let request_id = crate::generate_request_id().map_err(|_| {
                    GeneratedExecutionError::Client(ClientError::OutcomeUnknown(
                        crate::OutcomeUnknown,
                    ))
                })?;
                let request = command
                    .outcome_request(request_id)
                    .map_err(GeneratedExecutionError::CommandShape)?;
                let resolved = self
                    .get_outcome(request, metadata)
                    .await
                    .map_err(GeneratedExecutionError::Client)?;
                match resolved.result {
                    Some(v1::get_outcome_response::Result::Found(response)) => response,
                    Some(v1::get_outcome_response::Result::NotFound(_)) | None => {
                        return Err(GeneratedExecutionError::Client(
                            ClientError::OutcomeUnknown(crate::OutcomeUnknown),
                        ));
                    }
                }
            }
            Err(error) => {
                return Err(GeneratedExecutionError::Client(
                    contextualize_command_client_error(error, &command_name),
                ));
            }
        };
        let outcome = command
            .decode_outcome(&response)
            .map_err(GeneratedExecutionError::CommandShape)?;
        Ok(GeneratedExecution { response, outcome })
    }
}

impl ExecuteRetryAttempt for RiffDbClient {
    async fn submit_execute_attempt(
        &mut self,
        request: v1::ExecuteCommandRequest,
        metadata: &CallMetadata,
    ) -> Result<v1::ExecuteCommandResponse, ClientError> {
        self.execute(request, metadata).await
    }
}

impl NormalCapabilityRetryAttempt for RiffDbClient {
    async fn submit_normal_capability_attempt(
        &mut self,
        request: v1::CreateCapabilityRequest,
        metadata: &CallMetadata,
    ) -> Result<v1::CreateCapabilityResponse, ClientError> {
        self.create_capability(request, metadata).await
    }
}

impl BootstrapCapabilityRetryAttempt for RiffDbClient {
    async fn submit_bootstrap_capability_attempt(
        &mut self,
        request: v1::CreateCapabilityRequest,
        metadata: &BootstrapCallMetadata,
    ) -> Result<v1::CreateCapabilityResponse, ClientError> {
        self.create_bootstrap_capability(request, metadata).await
    }
}

pub(crate) async fn execute_retry_attempts<T: ExecuteRetryAttempt, S: RetryRequestIdSource>(
    attempts: &mut T,
    command: &IdempotentCommand,
    attempt_budget: AttemptBudget,
    metadata: &CallMetadata,
    request_ids: &mut S,
) -> Result<v1::ExecuteCommandResponse, ClientError> {
    let mut retry = RetryState::new(attempt_budget);
    loop {
        let Some(request) = next_retry_request(command, &mut retry, request_ids)? else {
            return Err(ClientError::OutcomeUnknown(crate::OutcomeUnknown));
        };
        match attempts.submit_execute_attempt(request, metadata).await {
            Ok(response) => return Ok(response),
            Err(error) => match retry.handle_failure(error) {
                RetryDecision::Retry => {}
                RetryDecision::RetryAfter(delay) => {
                    apply_overloaded_backoff(delay).await;
                }
                RetryDecision::Return(error) => return Err(error),
            },
        }
    }
}

async fn normal_capability_retry_attempts<
    T: NormalCapabilityRetryAttempt,
    S: RetryRequestIdSource,
>(
    attempts: &mut T,
    create: &NormalCapabilityCreateTemplate,
    attempt_budget: AttemptBudget,
    metadata: &CallMetadata,
    request_ids: &mut S,
) -> Result<v1::CreateCapabilityResponse, ClientError> {
    let mut retry = RetryState::new(attempt_budget);
    loop {
        let Some(request) = next_normal_create_request(create, &mut retry, request_ids)? else {
            return Err(ClientError::OutcomeUnknown(crate::OutcomeUnknown));
        };
        match attempts
            .submit_normal_capability_attempt(request, metadata)
            .await
        {
            Ok(response) => return Ok(response),
            Err(error) => match retry.handle_failure(error) {
                RetryDecision::Retry => {}
                RetryDecision::RetryAfter(delay) => {
                    apply_overloaded_backoff(delay).await;
                }
                RetryDecision::Return(error) => return Err(error),
            },
        }
    }
}

async fn bootstrap_capability_retry_attempts<
    T: BootstrapCapabilityRetryAttempt,
    S: RetryRequestIdSource,
>(
    attempts: &mut T,
    create: &BootstrapCapabilityCreateTemplate,
    attempt_budget: AttemptBudget,
    metadata: &BootstrapCallMetadata,
    request_ids: &mut S,
) -> Result<v1::CreateCapabilityResponse, ClientError> {
    let mut retry = RetryState::new(attempt_budget);
    loop {
        let Some(request) = next_bootstrap_create_request(create, &mut retry, request_ids)? else {
            return Err(ClientError::OutcomeUnknown(crate::OutcomeUnknown));
        };
        match attempts
            .submit_bootstrap_capability_attempt(request, metadata)
            .await
        {
            Ok(response) => return Ok(response),
            Err(error) => match retry.handle_failure(error) {
                RetryDecision::Retry => {}
                RetryDecision::RetryAfter(delay) => {
                    apply_overloaded_backoff(delay).await;
                }
                RetryDecision::Return(error) => return Err(error),
            },
        }
    }
}

fn next_retry_request<S: RetryRequestIdSource>(
    command: &IdempotentCommand,
    retry: &mut RetryState,
    request_ids: &mut S,
) -> Result<Option<v1::ExecuteCommandRequest>, ClientError> {
    if !retry.begin_submission() {
        return Ok(None);
    }
    request_ids
        .next_request_id()
        .map(|request_id| {
            retry.note_request_id(&request_id);
            Some(command.request(request_id))
        })
        .map_err(|error| retry.request_id_failure(error))
}

fn next_normal_create_request<S: RetryRequestIdSource>(
    create: &NormalCapabilityCreateTemplate,
    retry: &mut RetryState,
    request_ids: &mut S,
) -> Result<Option<v1::CreateCapabilityRequest>, ClientError> {
    if !retry.begin_submission() {
        return Ok(None);
    }
    request_ids
        .next_request_id()
        .map(|request_id| {
            retry.note_request_id(&request_id);
            Some(create.request(request_id))
        })
        .map_err(|error| retry.request_id_failure(error))
}

fn next_bootstrap_create_request<S: RetryRequestIdSource>(
    create: &BootstrapCapabilityCreateTemplate,
    retry: &mut RetryState,
    request_ids: &mut S,
) -> Result<Option<v1::CreateCapabilityRequest>, ClientError> {
    if !retry.begin_submission() {
        return Ok(None);
    }
    request_ids
        .next_request_id()
        .map(|request_id| {
            retry.note_request_id(&request_id);
            Some(create.request(request_id))
        })
        .map_err(|error| retry.request_id_failure(error))
}

/// A commit-notification stream decoded by the strict client codec.
pub struct CommitNotificationStream {
    inner: Streaming<v1::CommitNotification>,
}

/// A durable event-consumer stream decoded by the strict client codec.
pub struct EventConsumerResponseStream {
    inner: Streaming<v1::ConsumeEventStreamResponse>,
}

/// A live named-query stream decoded by the strict client codec.
pub struct LiveQueryUpdateStream {
    inner: Streaming<v1::LiveQueryUpdate>,
}

impl LiveQueryUpdateStream {
    /// Receives the next closed live update (structure enforced at decode).
    pub async fn message(&mut self) -> Result<Option<v1::LiveQueryUpdate>, ClientError> {
        self.inner.message().await.map_err(checked_status)
    }
}

impl EventConsumerResponseStream {
    /// Receives the next leased-event response (structure enforced at decode).
    pub async fn message(&mut self) -> Result<Option<v1::ConsumeEventStreamResponse>, ClientError> {
        self.inner.message().await.map_err(checked_status)
    }
}

impl CommitNotificationStream {
    /// Receives the next commit notification (structure enforced at decode).
    pub async fn message(&mut self) -> Result<Option<v1::CommitNotification>, ClientError> {
        self.inner.message().await.map_err(checked_status)
    }
}

/// A generated outcome together with its complete checked transport response.
pub struct GeneratedExecution<T> {
    response: v1::ExecuteCommandResponse,
    outcome: T,
}

impl<T> GeneratedExecution<T> {
    /// Borrows the generated declared outcome.
    #[must_use]
    pub const fn outcome(&self) -> &T {
        &self.outcome
    }

    /// Borrows commit, provenance, contract, and durability metadata.
    #[must_use]
    pub const fn response(&self) -> &v1::ExecuteCommandResponse {
        &self.response
    }

    /// Splits the generated outcome from its checked transport response.
    #[must_use]
    pub fn into_parts(self) -> (T, v1::ExecuteCommandResponse) {
        (self.outcome, self.response)
    }
}

/// A closed failure while executing one generated command shape.
#[derive(Debug)]
pub enum GeneratedExecutionError {
    /// The generic client or peer protocol failed.
    Client(ClientError),
    /// Generated input or output did not match its frozen contract shape.
    CommandShape(GeneratedCommandError),
}

impl fmt::Display for GeneratedExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Client(error) => error.fmt(formatter),
            Self::CommandShape(error) => error.fmt(formatter),
        }
    }
}

impl Error for GeneratedExecutionError {}

/// Client-side outbound structural validation (walk 1 of 5 per unary trip).
///
/// Load-bearing classification, not repeated representation: invalid
/// client-built messages become certain, non-retryable
/// [`ProtocolFailureKind::InvalidOutboundMessage`]. The encoder (walk 2) still
/// validates before wire write, but encoder failures surface through Tonic as
/// details-free transport errors and can be misclassified as retryable
/// uncertainty. Inbound has no client re-walk after decode (codec owns that).
fn validate_outbound<M: PublicMessage>(message: &M) -> Result<(), ClientError> {
    validate_public_message(message).map_err(|_| {
        ClientError::Protocol(ProtocolFailure::new(
            ProtocolFailureKind::InvalidOutboundMessage,
        ))
    })
}

fn configure_projection_wait(
    message: &mut v1::QueryProjectionRequest,
    required_sequence: CommitSequence,
    maximum_wait: Duration,
) -> Result<(), ClientError> {
    message.required_sequence = Some(required_sequence.get());
    message.wait_nanos = u64::try_from(maximum_wait.as_nanos()).map_err(|_| invalid_outbound())?;
    validate_outbound(message)
}

const fn invalid_inbound() -> ClientError {
    ClientError::Protocol(ProtocolFailure::new(
        ProtocolFailureKind::InvalidInboundMessage,
    ))
}

const fn invalid_outbound() -> ClientError {
    ClientError::Protocol(ProtocolFailure::new(
        ProtocolFailureKind::InvalidOutboundMessage,
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Waker};

    use super::*;
    use crate::BootstrapCredential;
    use crate::status::DetailsFreeStatus;
    use riffdb_errors::{PublicError, PublicErrorKind};
    use tonic_prost::prost::Message;

    const BOOTSTRAP_TOKEN: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";

    struct FixedRequestIds {
        values: VecDeque<RequestId>,
    }

    impl RetryRequestIdSource for FixedRequestIds {
        fn next_request_id(&mut self) -> Result<RequestId, crate::IdentifierGenerationError> {
            self.values
                .pop_front()
                .ok_or(crate::IdentifierGenerationError::EntropyUnavailable)
        }
    }

    struct ScriptedRequestIds {
        results: VecDeque<Result<RequestId, crate::IdentifierGenerationError>>,
        calls: usize,
    }

    impl RetryRequestIdSource for ScriptedRequestIds {
        fn next_request_id(&mut self) -> Result<RequestId, crate::IdentifierGenerationError> {
            self.calls += 1;
            self.results
                .pop_front()
                .unwrap_or(Err(crate::IdentifierGenerationError::EntropyUnavailable))
        }
    }

    struct ExecuteAttempts {
        results: VecDeque<Result<v1::ExecuteCommandResponse, ClientError>>,
        requests: Vec<v1::ExecuteCommandRequest>,
        pending_once: bool,
    }

    impl ExecuteAttempts {
        fn new(
            results: impl IntoIterator<Item = Result<v1::ExecuteCommandResponse, ClientError>>,
        ) -> Self {
            Self {
                results: results.into_iter().collect(),
                requests: Vec::new(),
                pending_once: false,
            }
        }

        fn with_pending_once(mut self) -> Self {
            self.pending_once = true;
            self
        }
    }

    impl ExecuteRetryAttempt for ExecuteAttempts {
        async fn submit_execute_attempt(
            &mut self,
            request: v1::ExecuteCommandRequest,
            _metadata: &CallMetadata,
        ) -> Result<v1::ExecuteCommandResponse, ClientError> {
            self.requests.push(request);
            if self.pending_once {
                let mut first_poll = true;
                std::future::poll_fn(|context| {
                    if std::mem::take(&mut first_poll) {
                        context.waker().wake_by_ref();
                        Poll::Pending
                    } else {
                        Poll::Ready(())
                    }
                })
                .await;
                self.pending_once = false;
            }
            self.results.pop_front().expect("scripted execute result")
        }
    }

    struct NormalCapabilityAttempts {
        results: VecDeque<Result<v1::CreateCapabilityResponse, ClientError>>,
        requests: Vec<v1::CreateCapabilityRequest>,
    }

    impl NormalCapabilityAttempts {
        fn new(
            results: impl IntoIterator<Item = Result<v1::CreateCapabilityResponse, ClientError>>,
        ) -> Self {
            Self {
                results: results.into_iter().collect(),
                requests: Vec::new(),
            }
        }
    }

    impl NormalCapabilityRetryAttempt for NormalCapabilityAttempts {
        async fn submit_normal_capability_attempt(
            &mut self,
            request: v1::CreateCapabilityRequest,
            _metadata: &CallMetadata,
        ) -> Result<v1::CreateCapabilityResponse, ClientError> {
            self.requests.push(request);
            self.results
                .pop_front()
                .expect("scripted normal-create result")
        }
    }

    struct BootstrapCapabilityAttempts {
        results: VecDeque<Result<v1::CreateCapabilityResponse, ClientError>>,
        requests: Vec<v1::CreateCapabilityRequest>,
        credential_presentations: Vec<Vec<u8>>,
    }

    impl BootstrapCapabilityAttempts {
        fn new(
            results: impl IntoIterator<Item = Result<v1::CreateCapabilityResponse, ClientError>>,
        ) -> Self {
            Self {
                results: results.into_iter().collect(),
                requests: Vec::new(),
                credential_presentations: Vec::new(),
            }
        }
    }

    impl BootstrapCapabilityRetryAttempt for BootstrapCapabilityAttempts {
        async fn submit_bootstrap_capability_attempt(
            &mut self,
            request: v1::CreateCapabilityRequest,
            metadata: &BootstrapCallMetadata,
        ) -> Result<v1::CreateCapabilityResponse, ClientError> {
            let mut transport_request = Request::new(());
            metadata.apply(&mut transport_request);
            let presentation = transport_request
                .metadata()
                .get_bin("riffdb-bootstrap-token-bin")
                .expect("bootstrap credential metadata")
                .to_bytes()
                .expect("valid binary metadata")
                .to_vec();
            self.credential_presentations.push(presentation);
            self.requests.push(request);
            self.results
                .pop_front()
                .expect("scripted bootstrap-create result")
        }
    }

    fn block_on_ready<T>(future: impl Future<Output = T>) -> T {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        let mut future = std::pin::pin!(future);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("deterministic retry schedule unexpectedly yielded"),
        }
    }

    #[derive(Default)]
    struct WakeCounter {
        wakes: AtomicUsize,
    }

    impl std::task::Wake for WakeCounter {
        fn wake(self: Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn block_on_deterministic_wakes<T>(future: impl Future<Output = T>) -> (T, usize) {
        let wake_counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(Arc::clone(&wake_counter));
        let mut context = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);
        let mut pending_polls = 0;
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(output) => return (output, pending_polls),
                Poll::Pending => {
                    pending_polls += 1;
                    assert!(
                        pending_polls <= 1,
                        "deterministic schedule exceeded its one pending poll"
                    );
                    assert_eq!(
                        wake_counter.wakes.swap(0, Ordering::SeqCst),
                        1,
                        "each deterministic pending poll must arrange exactly one wake"
                    );
                }
            }
        }
    }

    fn scripted_ids(
        results: impl IntoIterator<Item = Result<RequestId, crate::IdentifierGenerationError>>,
    ) -> ScriptedRequestIds {
        ScriptedRequestIds {
            results: results.into_iter().collect(),
            calls: 0,
        }
    }

    fn request_id(ordinal: u8) -> RequestId {
        RequestId::from_unix_milliseconds_and_random(u64::from(ordinal), [ordinal; 10])
            .expect("request ID")
    }

    fn successful_execute_response() -> v1::ExecuteCommandResponse {
        v1::ExecuteCommandResponse {
            status: v1::execute_command_response::CompletionStatus::ExecutedReadOnly as i32,
            commit_sequence: 0,
            contract_version: 1,
            plan_hash: vec![7; 32],
            outcome_type: "Observed".to_owned(),
            outcome: Some(v1::Value {
                kind: Some(v1::value::Kind::RecordValue(v1::ValueRecord {
                    fields: Vec::new(),
                })),
            }),
            provenance_uri: String::new(),
            durability_mode: String::new(),
            outcome_uri: None,
            history_incarnation: 1,
        }
    }

    fn normal_token_unavailable_response(capability_id: &[u8]) -> v1::CreateCapabilityResponse {
        v1::CreateCapabilityResponse {
            result: Some(v1::create_capability_response::Result::Normal(
                v1::NormalCreateCapabilityResult {
                    result: Some(
                        v1::normal_create_capability_result::Result::AlreadyCreatedTokenUnavailable(
                            v1::CapabilityIdentity {
                                capability_id: capability_id.to_vec(),
                                revision: 1,
                            },
                        ),
                    ),
                },
            )),
        }
    }

    fn bootstrap_replayed_response(capability_id: &[u8]) -> v1::CreateCapabilityResponse {
        v1::CreateCapabilityResponse {
            result: Some(v1::create_capability_response::Result::Bootstrap(
                v1::BootstrapCreateCapabilityResult {
                    result: Some(v1::bootstrap_create_capability_result::Result::Replayed(
                        v1::CapabilityTransition {
                            identity: Some(v1::CapabilityIdentity {
                                capability_id: capability_id.to_vec(),
                                revision: 1,
                            }),
                            administration_sequence: 1,
                        },
                    )),
                },
            )),
        }
    }

    fn transport_unavailable() -> ClientError {
        ClientError::DetailsFree(DetailsFreeStatus::TransportUnavailable)
    }

    fn retryable_checked_error() -> ClientError {
        ClientError::Public(PublicError::concurrency_deadline_exceeded())
    }

    fn public_outcome_unknown() -> ClientError {
        ClientError::Public(PublicError::outcome_unknown())
    }

    fn terminal_checked_error() -> ClientError {
        ClientError::Public(PublicError::authorization_denied())
    }

    fn assert_only_execute_request_id_changes(
        requests: &[v1::ExecuteCommandRequest],
        expected_ids: &[RequestId],
    ) {
        assert_eq!(requests.len(), expected_ids.len());
        let mut semantic_bodies = Vec::new();
        for (request, expected_id) in requests.iter().zip(expected_ids) {
            assert_eq!(request.request_id, expected_id.into_bytes());
            let mut semantic_body = request.clone();
            semantic_body.request_id.clear();
            semantic_bodies.push(semantic_body.encode_to_vec());
        }
        assert!(semantic_bodies.windows(2).all(|pair| pair[0] == pair[1]));
    }

    fn assert_only_create_request_id_changes(
        requests: &[v1::CreateCapabilityRequest],
        expected_ids: &[RequestId],
    ) {
        assert_eq!(requests.len(), expected_ids.len());
        let mut semantic_bodies = Vec::new();
        for (request, expected_id) in requests.iter().zip(expected_ids) {
            assert_eq!(request.request_id, expected_id.into_bytes());
            let mut semantic_body = request.clone();
            semantic_body.request_id.clear();
            semantic_bodies.push(semantic_body.encode_to_vec());
        }
        assert!(semantic_bodies.windows(2).all(|pair| pair[0] == pair[1]));
    }

    fn assert_authorization_denied<T>(result: Result<T, ClientError>) {
        assert!(matches!(
            result,
            Err(ClientError::Public(error))
                if error.kind() == PublicErrorKind::AuthorizationDenied
        ));
    }

    fn assert_concurrency_deadline<T>(result: Result<T, ClientError>) {
        assert!(matches!(
            result,
            Err(ClientError::Public(error))
                if error.kind() == PublicErrorKind::ConcurrencyDeadlineExceeded
        ));
    }

    fn assert_outcome_unknown<T>(result: Result<T, ClientError>) {
        assert!(matches!(result, Err(ClientError::OutcomeUnknown(_))));
    }

    fn retry_input() -> v1::Value {
        v1::Value {
            kind: Some(v1::value::Kind::RecordValue(v1::ValueRecord {
                fields: vec![v1::ValueField {
                    field_id: Some(1),
                    name: String::new(),
                    value: Some(v1::Value {
                        kind: Some(v1::value::Kind::StringValue("same-key".to_owned())),
                    }),
                }],
            })),
        }
    }

    fn create_template_request(mode: v1::CapabilityCreateMode) -> v1::CreateCapabilityRequest {
        let permissions = if mode == v1::CapabilityCreateMode::Bootstrap {
            vec![v1::CapabilityPermission {
                permission: Some(
                    v1::capability_permission::Permission::AdministerCapabilities(v1::Unit {}),
                ),
            }]
        } else {
            Vec::new()
        };
        v1::CreateCapabilityRequest {
            request_id: Vec::new(),
            mode: mode as i32,
            capability_id: RequestId::from_unix_milliseconds_and_random(1, [4; 10])
                .expect("capability ID")
                .into_bytes()
                .to_vec(),
            principal_id: "operator".to_owned(),
            actor_kind: v1::ActorKind::Human as i32,
            requested_lifetime_seconds: 60,
            audiences: vec!["riffdb-cli".to_owned()],
            grant: Some(v1::CapabilityGrant {
                tenant_scope: Some(v1::TenantScope {
                    scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
                }),
                partition_scope: Some(v1::PartitionScope {
                    scope: Some(v1::partition_scope::Scope::All(v1::Unit {})),
                }),
                permissions,
                field_visibility: Vec::new(),
                max_scan_rows: 1,
                approval_required: Vec::new(),
            }),
        }
    }

    #[test]
    fn execute_helper_retries_checked_failure_with_fresh_id_and_stable_body() {
        let first_id = request_id(1);
        let second_id = request_id(2);
        let mut request_ids = scripted_ids([Ok(first_id), Ok(second_id)]);
        let command = IdempotentCommand::new("CreateBudget", Some(1), retry_input())
            .expect("immutable command");
        let expected = successful_execute_response();
        let mut attempts =
            ExecuteAttempts::new([Err(retryable_checked_error()), Ok(expected.clone())]);

        let result = block_on_ready(execute_retry_attempts(
            &mut attempts,
            &command,
            AttemptBudget::new(2).expect("budget"),
            &CallMetadata::default(),
            &mut request_ids,
        ));

        assert_eq!(result.expect("second attempt succeeds"), expected);
        assert_eq!(request_ids.calls, 2);
        assert_only_execute_request_id_changes(&attempts.requests, &[first_id, second_id]);
        validate_public_message(&attempts.requests[0]).expect("first checked request");
        validate_public_message(&attempts.requests[1]).expect("second checked request");
    }

    #[test]
    fn bootstrap_helper_retains_identity_body_and_token_across_retry() {
        let first_id = request_id(3);
        let second_id = request_id(4);
        let mut request_ids = scripted_ids([Ok(first_id), Ok(second_id)]);
        let template_request = create_template_request(v1::CapabilityCreateMode::Bootstrap);
        let capability_id = template_request.capability_id.clone();
        let create =
            BootstrapCapabilityCreateTemplate::new(template_request).expect("bootstrap template");
        let expected = bootstrap_replayed_response(&capability_id);
        let mut attempts = BootstrapCapabilityAttempts::new([
            Err(retryable_checked_error()),
            Ok(expected.clone()),
        ]);
        let metadata = BootstrapCallMetadata::new(
            BootstrapCredential::new(BOOTSTRAP_TOKEN).expect("bootstrap credential"),
        );

        let result = block_on_ready(bootstrap_capability_retry_attempts(
            &mut attempts,
            &create,
            AttemptBudget::new(2).expect("budget"),
            &metadata,
            &mut request_ids,
        ));

        assert_eq!(result.expect("replayed bootstrap result"), expected);
        assert_eq!(request_ids.calls, 2);
        assert_only_create_request_id_changes(&attempts.requests, &[first_id, second_id]);
        assert!(
            attempts
                .requests
                .iter()
                .all(|request| request.capability_id == capability_id)
        );
        assert_eq!(
            attempts.credential_presentations,
            vec![BOOTSTRAP_TOKEN.as_bytes().to_vec(); 2]
        );
        validate_create_capability_exchange(&attempts.requests[1], &expected)
            .expect("checked bootstrap exchange");
    }

    #[test]
    fn normal_helper_returns_token_unavailable_after_uncertainty_without_replacement() {
        let first_id = request_id(5);
        let second_id = request_id(6);
        let unused_third_id = request_id(7);
        let mut request_ids = scripted_ids([Ok(first_id), Ok(second_id), Ok(unused_third_id)]);
        let template_request = create_template_request(v1::CapabilityCreateMode::Normal);
        let capability_id = template_request.capability_id.clone();
        let create =
            NormalCapabilityCreateTemplate::new(template_request).expect("normal template");
        let expected = normal_token_unavailable_response(&capability_id);
        let mut attempts =
            NormalCapabilityAttempts::new([Err(transport_unavailable()), Ok(expected.clone())]);

        let result = block_on_ready(normal_capability_retry_attempts(
            &mut attempts,
            &create,
            AttemptBudget::new(3).expect("budget"),
            &CallMetadata::default(),
            &mut request_ids,
        ));

        assert_eq!(
            result.expect("terminal token-unavailable response"),
            expected
        );
        assert_eq!(request_ids.calls, 2);
        assert_eq!(request_ids.results.len(), 1);
        assert!(attempts.results.is_empty());
        assert_only_create_request_id_changes(&attempts.requests, &[first_id, second_id]);
        validate_create_capability_exchange(&attempts.requests[1], &expected)
            .expect("checked normal-create exchange");
        let Some(v1::create_capability_response::Result::Normal(normal)) = expected.result else {
            panic!("normal result family")
        };
        assert!(matches!(
            normal.result,
            Some(
                v1::normal_create_capability_result::Result::AlreadyCreatedTokenUnavailable(
                    v1::CapabilityIdentity {
                        capability_id: returned_id,
                        revision: 1,
                    }
                )
            ) if returned_id == capability_id
        ));
    }

    #[test]
    fn every_helper_resolves_public_outcome_unknown_with_a_valid_terminal_result() {
        let command = IdempotentCommand::new("CreateBudget", Some(1), retry_input())
            .expect("immutable command");
        let normal_request = create_template_request(v1::CapabilityCreateMode::Normal);
        let normal_capability_id = normal_request.capability_id.clone();
        let normal = NormalCapabilityCreateTemplate::new(normal_request).expect("normal template");
        let bootstrap_request = create_template_request(v1::CapabilityCreateMode::Bootstrap);
        let bootstrap_capability_id = bootstrap_request.capability_id.clone();
        let bootstrap =
            BootstrapCapabilityCreateTemplate::new(bootstrap_request).expect("bootstrap template");
        let bootstrap_metadata = BootstrapCallMetadata::new(
            BootstrapCredential::new(BOOTSTRAP_TOKEN).expect("bootstrap credential"),
        );
        let budget = AttemptBudget::new(2).expect("budget");

        let execute_request_ids = [request_id(35), request_id(36)];
        let mut execute_ids = scripted_ids(execute_request_ids.map(Ok));
        let execute_response = successful_execute_response();
        let mut execute =
            ExecuteAttempts::new([Err(public_outcome_unknown()), Ok(execute_response.clone())]);
        assert_eq!(
            block_on_ready(execute_retry_attempts(
                &mut execute,
                &command,
                budget,
                &CallMetadata::default(),
                &mut execute_ids,
            ))
            .expect("same-key command resolution"),
            execute_response
        );
        assert_eq!(execute_ids.calls, 2);
        assert_only_execute_request_id_changes(&execute.requests, &execute_request_ids);

        let normal_request_ids = [request_id(37), request_id(38)];
        let mut normal_ids = scripted_ids(normal_request_ids.map(Ok));
        let normal_response = normal_token_unavailable_response(&normal_capability_id);
        let mut normal_attempts = NormalCapabilityAttempts::new([
            Err(public_outcome_unknown()),
            Ok(normal_response.clone()),
        ]);
        assert_eq!(
            block_on_ready(normal_capability_retry_attempts(
                &mut normal_attempts,
                &normal,
                budget,
                &CallMetadata::default(),
                &mut normal_ids,
            ))
            .expect("normal-create identity resolution"),
            normal_response
        );
        assert_eq!(normal_ids.calls, 2);
        assert_only_create_request_id_changes(&normal_attempts.requests, &normal_request_ids);

        let bootstrap_request_ids = [request_id(39), request_id(40)];
        let mut bootstrap_ids = scripted_ids(bootstrap_request_ids.map(Ok));
        let bootstrap_response = bootstrap_replayed_response(&bootstrap_capability_id);
        let mut bootstrap_attempts = BootstrapCapabilityAttempts::new([
            Err(public_outcome_unknown()),
            Ok(bootstrap_response.clone()),
        ]);
        assert_eq!(
            block_on_ready(bootstrap_capability_retry_attempts(
                &mut bootstrap_attempts,
                &bootstrap,
                budget,
                &bootstrap_metadata,
                &mut bootstrap_ids,
            ))
            .expect("bootstrap identity resolution"),
            bootstrap_response
        );
        assert_eq!(bootstrap_ids.calls, 2);
        assert_only_create_request_id_changes(&bootstrap_attempts.requests, &bootstrap_request_ids);
        assert_eq!(
            bootstrap_attempts.credential_presentations,
            vec![BOOTSTRAP_TOKEN.as_bytes().to_vec(); 2]
        );
    }

    #[test]
    fn execute_retry_state_survives_a_deterministic_pending_and_wake() {
        let command = IdempotentCommand::new("CreateBudget", Some(1), retry_input())
            .expect("immutable command");
        let request_ids_expected = [request_id(41), request_id(42)];
        let mut request_ids = scripted_ids(request_ids_expected.map(Ok));
        let expected = successful_execute_response();
        let mut attempts =
            ExecuteAttempts::new([Err(public_outcome_unknown()), Ok(expected.clone())])
                .with_pending_once();

        let (result, pending_polls) = block_on_deterministic_wakes(execute_retry_attempts(
            &mut attempts,
            &command,
            AttemptBudget::new(2).expect("budget"),
            &CallMetadata::default(),
            &mut request_ids,
        ));

        assert_eq!(pending_polls, 1);
        assert_eq!(result.expect("retry after deterministic wake"), expected);
        assert_eq!(request_ids.calls, 2);
        assert_only_execute_request_id_changes(&attempts.requests, &request_ids_expected);
    }

    #[test]
    fn every_helper_returns_terminal_checked_error_without_retry() {
        let command = IdempotentCommand::new("CreateBudget", Some(1), retry_input())
            .expect("immutable command");
        let normal = NormalCapabilityCreateTemplate::new(create_template_request(
            v1::CapabilityCreateMode::Normal,
        ))
        .expect("normal template");
        let bootstrap = BootstrapCapabilityCreateTemplate::new(create_template_request(
            v1::CapabilityCreateMode::Bootstrap,
        ))
        .expect("bootstrap template");
        let bootstrap_metadata = BootstrapCallMetadata::new(
            BootstrapCredential::new(BOOTSTRAP_TOKEN).expect("bootstrap credential"),
        );
        let budget = AttemptBudget::new(3).expect("budget");

        let mut execute_ids = scripted_ids([Ok(request_id(8)), Ok(request_id(9))]);
        let mut execute = ExecuteAttempts::new([Err(terminal_checked_error())]);
        assert_authorization_denied(block_on_ready(execute_retry_attempts(
            &mut execute,
            &command,
            budget,
            &CallMetadata::default(),
            &mut execute_ids,
        )));
        assert_eq!((execute.requests.len(), execute_ids.calls), (1, 1));

        let mut normal_ids = scripted_ids([Ok(request_id(10)), Ok(request_id(11))]);
        let mut normal_attempts = NormalCapabilityAttempts::new([Err(terminal_checked_error())]);
        assert_authorization_denied(block_on_ready(normal_capability_retry_attempts(
            &mut normal_attempts,
            &normal,
            budget,
            &CallMetadata::default(),
            &mut normal_ids,
        )));
        assert_eq!((normal_attempts.requests.len(), normal_ids.calls), (1, 1));

        let mut bootstrap_ids = scripted_ids([Ok(request_id(12)), Ok(request_id(13))]);
        let mut bootstrap_attempts =
            BootstrapCapabilityAttempts::new([Err(terminal_checked_error())]);
        assert_authorization_denied(block_on_ready(bootstrap_capability_retry_attempts(
            &mut bootstrap_attempts,
            &bootstrap,
            budget,
            &bootstrap_metadata,
            &mut bootstrap_ids,
        )));
        assert_eq!(
            (bootstrap_attempts.requests.len(), bootstrap_ids.calls),
            (1, 1)
        );
    }

    #[test]
    fn every_helper_keeps_uncertainty_when_a_later_error_is_terminal() {
        let command = IdempotentCommand::new("CreateBudget", Some(1), retry_input())
            .expect("immutable command");
        let normal = NormalCapabilityCreateTemplate::new(create_template_request(
            v1::CapabilityCreateMode::Normal,
        ))
        .expect("normal template");
        let bootstrap = BootstrapCapabilityCreateTemplate::new(create_template_request(
            v1::CapabilityCreateMode::Bootstrap,
        ))
        .expect("bootstrap template");
        let bootstrap_metadata = BootstrapCallMetadata::new(
            BootstrapCredential::new(BOOTSTRAP_TOKEN).expect("bootstrap credential"),
        );
        let budget = AttemptBudget::new(3).expect("budget");

        let execute_request_ids = [request_id(29), request_id(30)];
        let mut execute_ids = scripted_ids(execute_request_ids.map(Ok));
        let mut execute =
            ExecuteAttempts::new([Err(public_outcome_unknown()), Err(terminal_checked_error())]);
        assert_outcome_unknown(block_on_ready(execute_retry_attempts(
            &mut execute,
            &command,
            budget,
            &CallMetadata::default(),
            &mut execute_ids,
        )));
        assert_eq!((execute.requests.len(), execute_ids.calls), (2, 2));
        assert_only_execute_request_id_changes(&execute.requests, &execute_request_ids);

        let normal_request_ids = [request_id(31), request_id(32)];
        let mut normal_ids = scripted_ids(normal_request_ids.map(Ok));
        let mut normal_attempts = NormalCapabilityAttempts::new([
            Err(public_outcome_unknown()),
            Err(terminal_checked_error()),
        ]);
        assert_outcome_unknown(block_on_ready(normal_capability_retry_attempts(
            &mut normal_attempts,
            &normal,
            budget,
            &CallMetadata::default(),
            &mut normal_ids,
        )));
        assert_eq!((normal_attempts.requests.len(), normal_ids.calls), (2, 2));
        assert_only_create_request_id_changes(&normal_attempts.requests, &normal_request_ids);

        let bootstrap_request_ids = [request_id(33), request_id(34)];
        let mut bootstrap_ids = scripted_ids(bootstrap_request_ids.map(Ok));
        let mut bootstrap_attempts = BootstrapCapabilityAttempts::new([
            Err(public_outcome_unknown()),
            Err(terminal_checked_error()),
        ]);
        assert_outcome_unknown(block_on_ready(bootstrap_capability_retry_attempts(
            &mut bootstrap_attempts,
            &bootstrap,
            budget,
            &bootstrap_metadata,
            &mut bootstrap_ids,
        )));
        assert_eq!(
            (bootstrap_attempts.requests.len(), bootstrap_ids.calls),
            (2, 2)
        );
        assert_only_create_request_id_changes(&bootstrap_attempts.requests, &bootstrap_request_ids);
    }

    #[test]
    fn every_helper_preserves_checked_retry_error_on_known_exhaustion() {
        let command = IdempotentCommand::new("CreateBudget", Some(1), retry_input())
            .expect("immutable command");
        let normal = NormalCapabilityCreateTemplate::new(create_template_request(
            v1::CapabilityCreateMode::Normal,
        ))
        .expect("normal template");
        let bootstrap = BootstrapCapabilityCreateTemplate::new(create_template_request(
            v1::CapabilityCreateMode::Bootstrap,
        ))
        .expect("bootstrap template");
        let bootstrap_metadata = BootstrapCallMetadata::new(
            BootstrapCredential::new(BOOTSTRAP_TOKEN).expect("bootstrap credential"),
        );
        let budget = AttemptBudget::new(2).expect("budget");

        let execute_request_ids = [request_id(14), request_id(15)];
        let mut execute_ids = scripted_ids(execute_request_ids.map(Ok));
        let mut execute = ExecuteAttempts::new([
            Err(retryable_checked_error()),
            Err(retryable_checked_error()),
        ]);
        assert_concurrency_deadline(block_on_ready(execute_retry_attempts(
            &mut execute,
            &command,
            budget,
            &CallMetadata::default(),
            &mut execute_ids,
        )));
        assert_eq!(execute_ids.calls, 2);
        assert_only_execute_request_id_changes(&execute.requests, &execute_request_ids);

        let normal_request_ids = [request_id(16), request_id(17)];
        let mut normal_ids = scripted_ids(normal_request_ids.map(Ok));
        let mut normal_attempts = NormalCapabilityAttempts::new([
            Err(retryable_checked_error()),
            Err(retryable_checked_error()),
        ]);
        assert_concurrency_deadline(block_on_ready(normal_capability_retry_attempts(
            &mut normal_attempts,
            &normal,
            budget,
            &CallMetadata::default(),
            &mut normal_ids,
        )));
        assert_eq!(normal_ids.calls, 2);
        assert_only_create_request_id_changes(&normal_attempts.requests, &normal_request_ids);

        let bootstrap_request_ids = [request_id(18), request_id(19)];
        let mut bootstrap_ids = scripted_ids(bootstrap_request_ids.map(Ok));
        let mut bootstrap_attempts = BootstrapCapabilityAttempts::new([
            Err(retryable_checked_error()),
            Err(retryable_checked_error()),
        ]);
        assert_concurrency_deadline(block_on_ready(bootstrap_capability_retry_attempts(
            &mut bootstrap_attempts,
            &bootstrap,
            budget,
            &bootstrap_metadata,
            &mut bootstrap_ids,
        )));
        assert_eq!(bootstrap_ids.calls, 2);
        assert_only_create_request_id_changes(&bootstrap_attempts.requests, &bootstrap_request_ids);
    }

    #[test]
    fn every_helper_reports_unknown_outcome_on_transport_exhaustion() {
        let command = IdempotentCommand::new("CreateBudget", Some(1), retry_input())
            .expect("immutable command");
        let normal = NormalCapabilityCreateTemplate::new(create_template_request(
            v1::CapabilityCreateMode::Normal,
        ))
        .expect("normal template");
        let bootstrap = BootstrapCapabilityCreateTemplate::new(create_template_request(
            v1::CapabilityCreateMode::Bootstrap,
        ))
        .expect("bootstrap template");
        let bootstrap_metadata = BootstrapCallMetadata::new(
            BootstrapCredential::new(BOOTSTRAP_TOKEN).expect("bootstrap credential"),
        );
        let budget = AttemptBudget::new(2).expect("budget");

        let execute_request_ids = [request_id(20), request_id(21)];
        let mut execute_ids = scripted_ids(execute_request_ids.map(Ok));
        let mut execute =
            ExecuteAttempts::new([Err(transport_unavailable()), Err(transport_unavailable())]);
        assert_outcome_unknown(block_on_ready(execute_retry_attempts(
            &mut execute,
            &command,
            budget,
            &CallMetadata::default(),
            &mut execute_ids,
        )));
        assert_eq!(execute_ids.calls, 2);
        assert_only_execute_request_id_changes(&execute.requests, &execute_request_ids);

        let normal_request_ids = [request_id(22), request_id(23)];
        let mut normal_ids = scripted_ids(normal_request_ids.map(Ok));
        let mut normal_attempts = NormalCapabilityAttempts::new([
            Err(transport_unavailable()),
            Err(transport_unavailable()),
        ]);
        assert_outcome_unknown(block_on_ready(normal_capability_retry_attempts(
            &mut normal_attempts,
            &normal,
            budget,
            &CallMetadata::default(),
            &mut normal_ids,
        )));
        assert_eq!(normal_ids.calls, 2);
        assert_only_create_request_id_changes(&normal_attempts.requests, &normal_request_ids);

        let bootstrap_request_ids = [request_id(24), request_id(25)];
        let mut bootstrap_ids = scripted_ids(bootstrap_request_ids.map(Ok));
        let mut bootstrap_attempts = BootstrapCapabilityAttempts::new([
            Err(transport_unavailable()),
            Err(transport_unavailable()),
        ]);
        assert_outcome_unknown(block_on_ready(bootstrap_capability_retry_attempts(
            &mut bootstrap_attempts,
            &bootstrap,
            budget,
            &bootstrap_metadata,
            &mut bootstrap_ids,
        )));
        assert_eq!(bootstrap_ids.calls, 2);
        assert_only_create_request_id_changes(&bootstrap_attempts.requests, &bootstrap_request_ids);
    }

    #[test]
    fn every_helper_closes_request_id_failure_before_and_after_uncertainty() {
        let command = IdempotentCommand::new("CreateBudget", Some(1), retry_input())
            .expect("immutable command");
        let normal = NormalCapabilityCreateTemplate::new(create_template_request(
            v1::CapabilityCreateMode::Normal,
        ))
        .expect("normal template");
        let bootstrap = BootstrapCapabilityCreateTemplate::new(create_template_request(
            v1::CapabilityCreateMode::Bootstrap,
        ))
        .expect("bootstrap template");
        let bootstrap_metadata = BootstrapCallMetadata::new(
            BootstrapCredential::new(BOOTSTRAP_TOKEN).expect("bootstrap credential"),
        );
        let budget = AttemptBudget::new(2).expect("budget");
        let id_failure = crate::IdentifierGenerationError::EntropyUnavailable;

        let mut execute_ids = scripted_ids([Err(id_failure)]);
        let mut execute = ExecuteAttempts::new([]);
        assert!(matches!(
            block_on_ready(execute_retry_attempts(
                &mut execute,
                &command,
                budget,
                &CallMetadata::default(),
                &mut execute_ids,
            )),
            Err(ClientError::IdentifierGeneration(
                crate::IdentifierGenerationError::EntropyUnavailable
            ))
        ));
        assert!(execute.requests.is_empty());
        assert_eq!(execute_ids.calls, 1);

        let mut normal_ids = scripted_ids([Err(id_failure)]);
        let mut normal_attempts = NormalCapabilityAttempts::new([]);
        assert!(matches!(
            block_on_ready(normal_capability_retry_attempts(
                &mut normal_attempts,
                &normal,
                budget,
                &CallMetadata::default(),
                &mut normal_ids,
            )),
            Err(ClientError::IdentifierGeneration(
                crate::IdentifierGenerationError::EntropyUnavailable
            ))
        ));
        assert!(normal_attempts.requests.is_empty());
        assert_eq!(normal_ids.calls, 1);

        let mut bootstrap_ids = scripted_ids([Err(id_failure)]);
        let mut bootstrap_attempts = BootstrapCapabilityAttempts::new([]);
        assert!(matches!(
            block_on_ready(bootstrap_capability_retry_attempts(
                &mut bootstrap_attempts,
                &bootstrap,
                budget,
                &bootstrap_metadata,
                &mut bootstrap_ids,
            )),
            Err(ClientError::IdentifierGeneration(
                crate::IdentifierGenerationError::EntropyUnavailable
            ))
        ));
        assert!(bootstrap_attempts.requests.is_empty());
        assert_eq!(bootstrap_ids.calls, 1);

        let mut execute_ids = scripted_ids([Ok(request_id(26)), Err(id_failure)]);
        let mut execute = ExecuteAttempts::new([Err(transport_unavailable())]);
        assert_outcome_unknown(block_on_ready(execute_retry_attempts(
            &mut execute,
            &command,
            budget,
            &CallMetadata::default(),
            &mut execute_ids,
        )));
        assert_eq!((execute.requests.len(), execute_ids.calls), (1, 2));

        let mut normal_ids = scripted_ids([Ok(request_id(27)), Err(id_failure)]);
        let mut normal_attempts = NormalCapabilityAttempts::new([Err(transport_unavailable())]);
        assert_outcome_unknown(block_on_ready(normal_capability_retry_attempts(
            &mut normal_attempts,
            &normal,
            budget,
            &CallMetadata::default(),
            &mut normal_ids,
        )));
        assert_eq!((normal_attempts.requests.len(), normal_ids.calls), (1, 2));

        let mut bootstrap_ids = scripted_ids([Ok(request_id(28)), Err(id_failure)]);
        let mut bootstrap_attempts =
            BootstrapCapabilityAttempts::new([Err(transport_unavailable())]);
        assert_outcome_unknown(block_on_ready(bootstrap_capability_retry_attempts(
            &mut bootstrap_attempts,
            &bootstrap,
            budget,
            &bootstrap_metadata,
            &mut bootstrap_ids,
        )));
        assert_eq!(
            (bootstrap_attempts.requests.len(), bootstrap_ids.calls),
            (1, 2)
        );
    }

    #[test]
    fn bounded_retry_uses_fresh_request_ids_and_retains_identical_input() {
        let first_id = RequestId::from_unix_milliseconds_and_random(1, [1; 10]).expect("first ID");
        let second_id =
            RequestId::from_unix_milliseconds_and_random(2, [2; 10]).expect("second ID");
        let mut request_ids = FixedRequestIds {
            values: VecDeque::from([first_id, second_id]),
        };
        let command = IdempotentCommand::new("CreateBudget", Some(1), retry_input())
            .expect("immutable command");
        let mut retry = RetryState::new(AttemptBudget::new(2).expect("budget"));

        let first = next_retry_request(&command, &mut retry, &mut request_ids)
            .expect("first request")
            .expect("first submission");
        assert!(matches!(
            retry.handle_failure(ClientError::DetailsFree(
                DetailsFreeStatus::TransportUnavailable
            )),
            RetryDecision::Retry
        ));
        let second = next_retry_request(&command, &mut retry, &mut request_ids)
            .expect("second request")
            .expect("second submission");

        assert_eq!(first.request_id, first_id.into_bytes());
        assert_eq!(second.request_id, second_id.into_bytes());
        assert_ne!(first.request_id, second.request_id);
        assert_eq!(first.input, second.input);
        assert_eq!(
            first.input.as_ref().expect("first input").encode_to_vec(),
            second.input.as_ref().expect("second input").encode_to_vec()
        );
        assert_eq!(first.command_name, second.command_name);
        assert_eq!(
            first.expected_contract_version,
            second.expected_contract_version
        );
        assert!(request_ids.values.is_empty());
        assert!(matches!(
            retry.handle_failure(ClientError::DetailsFree(
                DetailsFreeStatus::TransportUnavailable
            )),
            RetryDecision::Return(ClientError::OutcomeUnknown(_))
        ));
    }

    #[test]
    fn capability_retry_builders_preserve_body_and_apply_fresh_request_ids() {
        let first_id = RequestId::from_unix_milliseconds_and_random(4, [4; 10]).expect("first ID");
        let second_id =
            RequestId::from_unix_milliseconds_and_random(5, [5; 10]).expect("second ID");
        let mut request_ids = FixedRequestIds {
            values: VecDeque::from([first_id, second_id]),
        };
        let create = NormalCapabilityCreateTemplate::new(create_template_request(
            v1::CapabilityCreateMode::Normal,
        ))
        .expect("normal template");
        let mut retry = RetryState::new(AttemptBudget::new(2).expect("budget"));

        let first = next_normal_create_request(&create, &mut retry, &mut request_ids)
            .expect("first request")
            .expect("first attempt");
        assert!(matches!(
            retry.handle_failure(ClientError::DetailsFree(
                DetailsFreeStatus::TransportUnavailable
            )),
            RetryDecision::Retry
        ));
        let second = next_normal_create_request(&create, &mut retry, &mut request_ids)
            .expect("second request")
            .expect("second attempt");
        assert_eq!(first.request_id, first_id.into_bytes());
        assert_eq!(second.request_id, second_id.into_bytes());
        let mut first_without_id = first;
        let mut second_without_id = second;
        first_without_id.request_id.clear();
        second_without_id.request_id.clear();
        assert_eq!(first_without_id, second_without_id);
        assert!(matches!(
            retry.handle_failure(ClientError::DetailsFree(
                DetailsFreeStatus::TransportUnavailable
            )),
            RetryDecision::Return(ClientError::OutcomeUnknown(_))
        ));

        let bootstrap = BootstrapCapabilityCreateTemplate::new(create_template_request(
            v1::CapabilityCreateMode::Bootstrap,
        ))
        .expect("bootstrap template");
        let mut bootstrap_ids = FixedRequestIds {
            values: VecDeque::from([
                RequestId::from_unix_milliseconds_and_random(6, [6; 10]).expect("bootstrap ID")
            ]),
        };
        let mut bootstrap_retry = RetryState::new(AttemptBudget::new(1).expect("budget"));
        let request =
            next_bootstrap_create_request(&bootstrap, &mut bootstrap_retry, &mut bootstrap_ids)
                .expect("bootstrap request")
                .expect("bootstrap attempt");
        assert_eq!(request.mode, v1::CapabilityCreateMode::Bootstrap as i32);
        assert!(!request.request_id.is_empty());
    }

    #[test]
    fn generated_execution_preserves_transport_metadata() {
        let response = v1::ExecuteCommandResponse {
            status: v1::execute_command_response::CompletionStatus::Committed as i32,
            commit_sequence: 9,
            contract_version: 1,
            plan_hash: vec![1; 32],
            outcome_type: "Created".to_owned(),
            outcome: Some(v1::Value {
                kind: Some(v1::value::Kind::RecordValue(v1::ValueRecord {
                    fields: Vec::new(),
                })),
            }),
            provenance_uri: "riffdb://provenance/018f22e2-79b7-7cc3-a85f-250f0f80c78e".to_owned(),
            durability_mode: "sync".to_owned(),
            outcome_uri: None,
            history_incarnation: 1,
        };
        let execution = GeneratedExecution {
            response: response.clone(),
            outcome: 17_u32,
        };
        assert_eq!(execution.outcome(), &17);
        assert_eq!(execution.response(), &response);
        assert_eq!(execution.into_parts(), (17, response));
    }

    #[test]
    fn projection_wait_maps_exact_total_nanoseconds_without_changing_query_identity() {
        let request_id = riffdb_types::RequestId::from_unix_milliseconds_and_random(1, [1; 10])
            .expect("request ID")
            .into_bytes()
            .to_vec();
        let contract = v1::ContractSelection {
            selection: Some(v1::contract_selection::Selection::Active(v1::Unit {})),
        };
        let mut request = v1::QueryProjectionRequest {
            request_id: request_id.clone(),
            contract: Some(contract.clone()),
            projection_id: 1,
            leading_components: Vec::new(),
            required_sequence: None,
            wait_nanos: 0,
            page: Some(v1::PageRequest {
                limit: Some(1),
                cursor: None,
            }),
        };
        let sequence = CommitSequence::new(7).expect("sequence");
        configure_projection_wait(&mut request, sequence, Duration::new(2, 345_678_901))
            .expect("bounded wait");

        assert_eq!(request.request_id, request_id);
        assert_eq!(request.contract, Some(contract));
        assert_eq!(request.projection_id, 1);
        assert_eq!(request.required_sequence, Some(7));
        assert_eq!(request.wait_nanos, 2_345_678_901);
    }

    #[test]
    fn projection_wait_rejects_the_public_thirty_second_overflow() {
        let mut request = v1::QueryProjectionRequest {
            request_id: riffdb_types::RequestId::from_unix_milliseconds_and_random(1, [2; 10])
                .expect("request ID")
                .into_bytes()
                .to_vec(),
            contract: Some(v1::ContractSelection {
                selection: Some(v1::contract_selection::Selection::Active(v1::Unit {})),
            }),
            projection_id: 1,
            leading_components: Vec::new(),
            required_sequence: None,
            wait_nanos: 0,
            page: Some(v1::PageRequest {
                limit: Some(1),
                cursor: None,
            }),
        };
        let error = configure_projection_wait(
            &mut request,
            CommitSequence::new(1).expect("sequence"),
            Duration::from_nanos(30_000_000_001),
        )
        .expect_err("over-limit wait");
        match error {
            ClientError::Protocol(failure) => {
                assert_eq!(failure.kind(), ProtocolFailureKind::InvalidOutboundMessage);
            }
            other => panic!("expected InvalidOutboundMessage, got {other:?}"),
        }
    }

    /// Invalid unary outbound is certain `InvalidOutboundMessage`, never
    /// transport-unavailable uncertainty. Re-dropping `validate_outbound`'s
    /// `validate_public_message` walk fails this test (transcript).
    #[test]
    fn invalid_unary_outbound_is_protocol_not_transport_unavailable() {
        use crate::status::{carries_uncertainty, is_retryable};

        let request = v1::QueryProjectionRequest {
            request_id: riffdb_types::RequestId::from_unix_milliseconds_and_random(1, [3; 10])
                .expect("request ID")
                .into_bytes()
                .to_vec(),
            contract: Some(v1::ContractSelection {
                selection: Some(v1::contract_selection::Selection::Active(v1::Unit {})),
            }),
            projection_id: 1,
            leading_components: Vec::new(),
            required_sequence: Some(1),
            wait_nanos: 30_000_000_001,
            page: Some(v1::PageRequest {
                limit: Some(1),
                cursor: None,
            }),
        };
        let error =
            validate_outbound(&request).expect_err("invalid outbound must fail before any RPC");
        match &error {
            ClientError::Protocol(failure) => {
                assert_eq!(
                    failure.kind(),
                    ProtocolFailureKind::InvalidOutboundMessage,
                    "encoder-only rejection would surface as TransportUnavailable"
                );
            }
            other => panic!("expected Protocol(InvalidOutboundMessage), got {other:?}"),
        }
        assert!(!is_retryable(&error));
        assert!(!carries_uncertainty(&error));
    }

    /// A legal max-sized command input can assemble an ExecuteCommandRequest
    /// over MAX_EXECUTE_REQUEST_BYTES. Local validate_outbound must reject with
    /// zero server submissions (no retry-budget burn / OutcomeUnknown).
    #[test]
    fn max_canonical_input_that_overflows_execute_request_fails_locally() {
        use crate::status::{carries_uncertainty, is_retryable};
        use riffdb_types::MAX_CANONICAL_DOCUMENT_BYTES;

        let payload_len = MAX_CANONICAL_DOCUMENT_BYTES - 32;
        let input = v1::Value {
            kind: Some(v1::value::Kind::RecordValue(v1::ValueRecord {
                fields: vec![v1::ValueField {
                    field_id: Some(1),
                    name: "b".to_owned(),
                    value: Some(v1::Value {
                        kind: Some(v1::value::Kind::BytesValue(vec![0x61; payload_len])),
                    }),
                }],
            })),
        };
        let request_id = riffdb_types::RequestId::from_unix_milliseconds_and_random(1, [4; 10])
            .expect("request ID");
        let execute = v1::ExecuteCommandRequest {
            request_id: request_id.into_bytes().to_vec(),
            command_name: "CreateTicket".to_owned(),
            expected_contract_version: Some(1),
            input: Some(input.clone()),
        };
        let error = validate_outbound(&execute)
            .expect_err("oversize execute request must fail locally before encode/RPC");
        match &error {
            ClientError::Protocol(failure) => {
                assert_eq!(failure.kind(), ProtocolFailureKind::InvalidOutboundMessage);
            }
            other => panic!("expected Protocol(InvalidOutboundMessage), got {other:?}"),
        }
        assert!(!is_retryable(&error));
        assert!(!carries_uncertainty(&error));

        let command = IdempotentCommand::new("CreateTicket", Some(1), input)
            .expect("max-bound input is a legal command document");
        let built = command.request(request_id);
        let error = validate_outbound(&built).expect_err(
            "execute_with_retry would call validate_outbound via execute() before submit",
        );
        match &error {
            ClientError::Protocol(failure) => {
                assert_eq!(
                    failure.kind(),
                    ProtocolFailureKind::InvalidOutboundMessage,
                    "without local outbound validation this becomes OutcomeUnknown"
                );
            }
            other => panic!("expected Protocol(InvalidOutboundMessage), got {other:?}"),
        }
        assert!(!is_retryable(&error));
        assert!(!carries_uncertainty(&error));
        assert!(!matches!(error, ClientError::OutcomeUnknown(_)));
        let _budget = AttemptBudget::new(5).expect("budget");
    }
}

#[cfg(test)]
mod history_incarnation_tests {
    use super::merge_observed_history_incarnation;

    #[test]
    fn merge_prefers_explicit_over_remembered() {
        assert_eq!(merge_observed_history_incarnation(None, Some(7)), Some(7));
        assert_eq!(
            merge_observed_history_incarnation(Some(3), Some(7)),
            Some(3)
        );
        assert_eq!(merge_observed_history_incarnation(None, None), None);
        assert_eq!(merge_observed_history_incarnation(Some(3), None), Some(3));
    }
}
