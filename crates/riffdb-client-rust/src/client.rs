//! Typed Tonic facade over the frozen public RiffDB protocol.

use std::error::Error;
use std::fmt;
use std::time::Duration;

use riffdb_api_grpc::generated::{
    admin_service_client::AdminServiceClient, command_service_client::CommandServiceClient,
    commit_service_client::CommitServiceClient, contract_service_client::ContractServiceClient,
    query_service_client::QueryServiceClient,
};
use riffdb_proto::v1;
use riffdb_proto::{
    PublicMessage, validate_contract_validation_exchange, validate_create_capability_exchange,
    validate_explain_command_exchange, validate_public_message, validate_query_projection_exchange,
    validate_scan_commits_exchange, validate_scan_index_exchange,
};
use riffdb_types::{CommitSequence, RequestId};
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Streaming};

use crate::command::{RetryDecision, RetryState};
use crate::generated::{GeneratedCommand, GeneratedCommandError};
use crate::status::{ClientError, ProtocolFailure, ProtocolFailureKind, checked_status};
use crate::{
    AttemptBudget, BootstrapCallMetadata, CallMetadata, IdempotentCommand, SystemIdSource,
};

trait RetryRequestIdSource {
    fn next_request_id(&mut self) -> Result<RequestId, crate::IdentifierGenerationError>;
}

impl RetryRequestIdSource for SystemIdSource {
    fn next_request_id(&mut self) -> Result<RequestId, crate::IdentifierGenerationError> {
        (*self).request_id()
    }
}

macro_rules! unary {
    ($name:ident, $client:ident, $rpc:ident, $request:ty, $response:ty) => {
        #[doc = concat!("Performs the checked `", stringify!($rpc), "` unary RPC.")]
        pub async fn $name(
            &mut self,
            message: $request,
            metadata: &CallMetadata,
        ) -> Result<$response, ClientError> {
            validate_outbound(&message)?;
            let mut request = Request::new(message);
            metadata.apply(&mut request);
            let response = self
                .$client
                .$rpc(request)
                .await
                .map_err(checked_status)?
                .into_inner();
            validate_inbound(&response)?;
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
            validate_inbound(&response)?;
            $exchange(&exchange_request, &response).map_err(|_| invalid_inbound())?;
            Ok(response)
        }
    };
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
    admin: AdminServiceClient<Channel>,
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
            admin: AdminServiceClient::new(channel),
        }
    }

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
    unary!(
        execute,
        command,
        execute,
        v1::ExecuteCommandRequest,
        v1::ExecuteCommandResponse
    );
    unary!(
        get_outcome,
        command,
        get_outcome,
        v1::GetOutcomeRequest,
        v1::GetOutcomeResponse
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
    unary!(health, admin, health, v1::HealthRequest, v1::HealthResponse);
    unary!(stats, admin, stats, v1::StatsRequest, v1::StatsResponse);
    unary!(
        revoke_capability,
        admin,
        revoke_capability,
        v1::RevokeCapabilityRequest,
        v1::RevokeCapabilityResponse
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
        validate_inbound(&response)?;
        validate_create_capability_exchange(&exchange_request, &response)
            .map_err(|_| invalid_inbound())?;
        Ok(response)
    }

    /// Starts a checked commit stream.
    ///
    /// Each item is structurally validated before it is returned to the caller.
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

    /// Executes one immutable command with an explicit total submission bound.
    ///
    /// Retryable checked failures are resubmitted immediately with a fresh
    /// outer request ID. This method never sleeps, applies jitter, or mutates
    /// the command name, selected version, or opaque input.
    pub async fn execute_with_retry(
        &mut self,
        command: &IdempotentCommand,
        attempt_budget: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<v1::ExecuteCommandResponse, ClientError> {
        let mut retry = RetryState::new(attempt_budget);
        let mut request_ids = SystemIdSource::new();
        loop {
            let Some(request) = next_retry_request(command, &mut retry, &mut request_ids)? else {
                return Err(ClientError::OutcomeUnknown(crate::OutcomeUnknown));
            };
            match self.execute(request, metadata).await {
                Ok(response) => return Ok(response),
                Err(error) => match retry.handle_failure(error) {
                    RetryDecision::Retry => {}
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
        let response = self
            .execute_with_retry(&generic, attempt_budget, metadata)
            .await
            .map_err(GeneratedExecutionError::Client)?;
        let outcome = command
            .decode_outcome(&response)
            .map_err(GeneratedExecutionError::CommandShape)?;
        Ok(GeneratedExecution { response, outcome })
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
        .map(|request_id| Some(command.request(request_id)))
        .map_err(|error| retry.request_id_failure(error))
}

/// A commit-notification stream that validates each peer-provided item.
pub struct CommitNotificationStream {
    inner: Streaming<v1::CommitNotification>,
}

impl CommitNotificationStream {
    /// Receives and validates the next commit notification.
    pub async fn message(&mut self) -> Result<Option<v1::CommitNotification>, ClientError> {
        let item = self.inner.message().await.map_err(checked_status)?;
        if let Some(item) = &item {
            validate_inbound(item)?;
        }
        Ok(item)
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

fn validate_inbound<M: PublicMessage>(message: &M) -> Result<(), ClientError> {
    validate_public_message(message).map_err(|_| invalid_inbound())
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

    use super::*;
    use crate::status::DetailsFreeStatus;
    use tonic_prost::prost::Message;

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
        assert!(matches!(
            error,
            ClientError::Protocol(ProtocolFailure { .. })
        ));
    }
}
