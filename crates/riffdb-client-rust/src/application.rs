//! Name-addressed application requests that hide the kernel wire model.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::OnceLock;
use std::time::Duration;

use futures_util::{StreamExt, stream};
use riffdb_errors::{
    ApplicationError, ApplicationErrorContext, ApplicationOperation, ApplicationRecoveryAction,
};
use riffdb_proto::{app::v1 as app_v1, application_error_from_proto, v1};
use tonic::transport::{Channel, Endpoint};

use crate::{
    AttemptBudget, CallMetadata, ClientError, GeneratedExecutionError, IdempotentCommand,
    RiffDbClient, generate_request_id,
    generated::{GeneratedCommand, GeneratedQuery},
};
use riffdb_config::TlsClientConfig;

const MAX_GENERATED_TRANSPORT_BATCH_ITEMS: usize = 16;
/// Maximum independently in-flight items in one generated command batch.
pub const MAX_GENERATED_BATCH_CONCURRENCY: usize = 384;

const APPLICATION_CATALOG_SCHEMA_V1: &str = "riffdb.application-catalog/v1";

/// Closed application feature vocabulary returned by symbolic catalog preflight.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ApplicationCatalogFeature {
    /// Finite compiler-owned optional predicate families.
    OperationalOptionalPredicates,
    /// Snapshot-bound stable cursor pages.
    StableCursorPages,
    /// Indexed null and existence predicates.
    NullExistencePredicates,
    /// Exact binary UTF-8 prefix indexes.
    BinaryTextPrefix,
    /// Versioned Unicode-fold prefix indexes.
    UnicodeFoldTextPrefixV1,
    /// Bounded exact operational aggregates.
    ExactAggregates,
}

/// Availability of one closed application feature.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationCatalogFeatureState {
    /// The exact selected server surface implements the feature.
    Available,
    /// The feature is explicitly unavailable and must not be emulated.
    Unavailable,
}

/// One checked symbolic application feature view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplicationCatalogFeatureView {
    /// Closed feature identity.
    pub feature: ApplicationCatalogFeature,
    /// Checked availability state.
    pub state: ApplicationCatalogFeatureState,
}

/// Checked feature-preflight result for one exact application contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationCatalogPreflight {
    /// Exact contract lineage returned by the authorized catalog.
    pub contract_lineage: String,
    /// Exact contract version returned by the authorized catalog.
    pub contract_version: u64,
    /// Exact contract bundle hash returned by the authorized catalog.
    pub contract_bundle_hash: [u8; 32],
    /// Active query-module identities in canonical server order.
    pub query_module_hashes: Vec<[u8; 32]>,
    /// Complete closed feature registry.
    pub features: Vec<ApplicationCatalogFeatureView>,
}

impl ApplicationCatalogPreflight {
    /// Returns whether the exact selected server surface exposes `feature`.
    #[must_use]
    pub fn is_available(&self, feature: ApplicationCatalogFeature) -> bool {
        self.features.iter().any(|view| {
            view.feature == feature && view.state == ApplicationCatalogFeatureState::Available
        })
    }
}

/// A local shape failure for one bounded transport batch of ordinary commands.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdempotentTransportBatchError {
    /// The transport batch was empty.
    Empty,
    /// The transport batch exceeded the public protocol bound of 16 items.
    TooManyItems,
}

impl fmt::Display for IdempotentTransportBatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Empty => "command transport batch is empty",
            Self::TooManyItems => "command transport batch exceeds 16 items",
        })
    }
}

impl std::error::Error for IdempotentTransportBatchError {}

/// Application-only client facade.
///
/// This type deliberately has no accessor for its kernel client. Stable
/// application code can execute name-addressed commands and exact named
/// queries, but cannot construct raw entity/index requests through this
/// surface.
#[derive(Clone)]
pub struct StableApplicationClient {
    pub(crate) inner: RiffDbClient,
}

impl StableApplicationClient {
    /// Connects the application-only facade through mandatory explicit TLS
    /// trust and exact peer-name verification.
    pub async fn connect_verified_tls(config: &TlsClientConfig) -> Result<Self, ClientError> {
        Ok(Self {
            inner: RiffDbClient::connect_verified_tls(config).await?,
        })
    }

    /// Connects the application facade from one bounded URI without exposing
    /// the transport package to application code.
    pub async fn connect_uri(endpoint: String) -> Result<Self, ClientError> {
        if endpoint.is_empty() || endpoint.len() > 2_048 {
            return Err(ClientError::ConnectionFailure);
        }
        let endpoint = Endpoint::from_shared(endpoint)
            .map_err(|_| ClientError::ConnectionFailure)?
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30));
        Self::connect(endpoint).await
    }

    /// Connects the application facade over one reusable HTTP/2 channel.
    pub async fn connect(endpoint: Endpoint) -> Result<Self, ClientError> {
        Ok(Self {
            inner: RiffDbClient::connect(endpoint).await?,
        })
    }

    /// Constructs the application facade over an existing channel.
    #[must_use]
    pub fn from_channel(channel: Channel) -> Self {
        Self {
            inner: RiffDbClient::from_channel(channel),
        }
    }

    /// Opens the optional bounded multiplexed application transport against
    /// one exact generated application identity. Unary transport remains the
    /// default until this explicit operation succeeds.
    pub async fn open_bounded_session(
        &mut self,
        identity: crate::ApplicationSessionIdentity,
        metadata: &CallMetadata,
    ) -> Result<(), ApplicationClientError> {
        self.inner
            .enable_bounded_application_session(identity, metadata.clone())
            .await?;
        Ok(())
    }

    /// Reports whether this facade explicitly selected the bounded session.
    #[must_use]
    pub const fn bounded_session_enabled(&self) -> bool {
        self.inner.bounded_application_session_enabled()
    }

    /// Explicitly closes the optional bounded session and returns this facade
    /// to unary transport. In-flight operations are released under the same
    /// uncertainty rules as stream loss.
    pub fn close_bounded_session(&mut self) {
        self.inner.disable_bounded_application_session();
    }

    /// Proves that the authenticated remote database currently exposes the
    /// exact contract identity compiled into the application lock.
    pub async fn verify_active_contract(
        &mut self,
        database: &str,
        lineage: &str,
        version: u64,
        bundle_hash: [u8; 32],
        metadata: &CallMetadata,
    ) -> Result<(), ApplicationClientError> {
        if database.is_empty() || lineage.is_empty() || version == 0 {
            return Err(ApplicationClientError::InvalidInput);
        }
        let request_id = Vec::from(
            generate_request_id()
                .map_err(|_| ApplicationClientError::IdentifierUnavailable)?
                .into_bytes(),
        );
        let response = self
            .inner
            .get_active_contract(v1::GetActiveContractRequest { request_id }, metadata)
            .await?;
        let Some(v1::get_active_contract_response::Result::Present(contract)) = response.result
        else {
            return Err(ApplicationClientError::InvalidResponse);
        };
        if response.database_alias != database
            || contract.contract_lineage != lineage
            || contract.contract_version != version
            || contract.bundle_hash.as_slice() != bundle_hash
        {
            return Err(ApplicationClientError::InvalidResponse);
        }
        Ok(())
    }

    /// Reads and validates the symbolic feature catalog for one exact contract.
    ///
    /// This application-only view deliberately omits catalog symbols, numeric
    /// identities, raw IR, and capability details. An unavailable feature is
    /// explicit and never authorizes a client-side fallback.
    pub async fn preflight_application_features(
        &mut self,
        contract: ApplicationContract,
        metadata: &CallMetadata,
    ) -> Result<ApplicationCatalogPreflight, ApplicationClientError> {
        let expected = contract.clone();
        let request_id = Vec::from(
            generate_request_id()
                .map_err(|_| ApplicationClientError::IdentifierUnavailable)?
                .into_bytes(),
        );
        let response = self
            .inner
            .get_application_catalog(
                app_v1::GetApplicationCatalogRequest {
                    contract: lower_contract(contract),
                    limit: 1,
                    cursor: None,
                    request_id,
                },
                metadata,
            )
            .await?;
        raise_application_catalog_preflight(response, &expected)
    }

    /// Inspects one compiler-declared production vector field through the
    /// symbolic, policy-filtered application operation.
    pub async fn inspect_vector_state(
        &mut self,
        request: VectorStateInspection,
        metadata: &CallMetadata,
    ) -> Result<VectorStateInspectionResult, ApplicationClientError> {
        request.validate()?;
        let request_id = Vec::from(
            generate_request_id()
                .map_err(|_| ApplicationClientError::IdentifierUnavailable)?
                .into_bytes(),
        );
        let response = self
            .inner
            .inspect_vector_state(
                app_v1::InspectVectorStateRequest {
                    contract: lower_contract(request.contract),
                    entity: request.entity,
                    field: request.field,
                    partition: Some(lower_value(request.partition)?),
                    kind: match request.kind {
                        VectorStateInspectionKind::Stale => {
                            app_v1::VectorStateInspectionKind::StaleEntities as i32
                        }
                        VectorStateInspectionKind::OutdatedModel => {
                            app_v1::VectorStateInspectionKind::OutdatedModelEntities as i32
                        }
                    },
                    page: Some(v1::PageRequest {
                        limit: Some(request.limit),
                        cursor: request.cursor,
                    }),
                    request_id,
                },
                metadata,
            )
            .await?;
        VectorStateInspectionResult::try_from(response)
    }

    /// Executes one exact named module query.
    pub async fn execute_named_query(
        &mut self,
        query: NamedQuery,
        metadata: &CallMetadata,
    ) -> Result<NamedQueryResult, ApplicationClientError> {
        let response = self.execute_named_query_wire(query, metadata).await?;
        raise_query_result(response)
    }

    /// Executes one named query while retaining its validated negotiated wire
    /// arm for first-party generated-language adapters.
    #[doc(hidden)]
    pub async fn execute_named_query_wire(
        &mut self,
        query: NamedQuery,
        metadata: &CallMetadata,
    ) -> Result<app_v1::ExecuteQueryResponse, ApplicationClientError> {
        self.inner
            .execute_named_application_query_wire(query, metadata)
            .await
    }

    /// Executes and decodes one generated exact named-query shape.
    pub async fn execute_generated_query<Q: GeneratedQuery>(
        &mut self,
        query: Q,
        options: QueryOptions,
        metadata: &CallMetadata,
    ) -> Result<TypedQueryResult<Q::Output>, ApplicationClientError> {
        let query = query.named_query(options)?;
        let response = self.execute_named_query_wire(query, metadata).await?;
        let application_head = response.application_head;
        let next_cursor = response.next_cursor.clone();
        let identity = query_response_identity(&response)?;
        let value = match app_v1::NamedResultEncoding::try_from(response.selected_result_encoding) {
            Ok(app_v1::NamedResultEncoding::CompactV1) => {
                if !response.fields.is_empty() {
                    return Err(ApplicationClientError::InvalidResponse);
                }
                Q::decode_compact_result(
                    response.outcome,
                    response
                        .compact_result
                        .ok_or(ApplicationClientError::InvalidResponse)?,
                )?
            }
            Ok(app_v1::NamedResultEncoding::LegacyRecords)
            | Ok(app_v1::NamedResultEncoding::Unspecified) => {
                Q::decode_result(raise_query_result(response)?)?
            }
            Err(_) => return Err(ApplicationClientError::InvalidResponse),
        };
        Ok(TypedQueryResult {
            value,
            identity,
            application_head,
            next_cursor,
        })
    }

    /// Executes one exact symbolic command with bounded uncertainty recovery.
    pub async fn execute_command(
        &mut self,
        command: ApplicationCommand,
        attempts: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<ApplicationCommandResult, ApplicationClientError> {
        self.inner
            .execute_application_command(command, attempts, metadata)
            .await
    }

    /// Executes and decodes one generated command, then performs one exact
    /// same-key outcome lookup if transport uncertainty remains.
    pub async fn execute_generated_command<C: GeneratedCommand>(
        &mut self,
        command: &C,
        attempts: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<TypedCommandResult<C::Outcome>, GeneratedExecutionError> {
        let execution = self
            .inner
            .execute_generated_with_recovery(command, attempts, metadata)
            .await?;
        let (outcome, response) = execution.into_parts();
        let workflow_revisions = command
            .workflow_successor_revisions(&response)
            .map_err(GeneratedExecutionError::CommandShape)?;
        Ok(TypedCommandResult {
            outcome,
            commit_sequence: (response.commit_sequence != 0).then_some(response.commit_sequence),
            contract_version: response.contract_version,
            plan_hash: response.plan_hash.try_into().map_err(|_| {
                GeneratedExecutionError::CommandShape(
                    crate::generated::GeneratedCommandError::InvalidOutcomeShape,
                )
            })?,
            replayed: response.status
                == v1::execute_command_response::CompletionStatus::Replayed as i32,
            outcome_uri: response.outcome_uri,
            workflow_revisions,
        })
    }

    /// Executes a bounded collection of ordinary generated commands with
    /// backpressure and independently ordered per-item results.
    pub async fn execute_generated_command_batch<C>(
        &self,
        commands: Vec<C>,
        options: GeneratedBatchOptions,
        attempts: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<GeneratedBatchResult<C::Outcome>, GeneratedBatchError>
    where
        C: GeneratedCommand + Clone,
    {
        self.execute_generated_command_batch_with_progress(
            commands,
            options,
            attempts,
            metadata,
            |_| {},
        )
        .await
    }

    /// Executes a bounded generated-command batch and reports completion plus
    /// the largest contiguous, safely resumable input checkpoint.
    pub async fn execute_generated_command_batch_with_progress<C, F>(
        &self,
        commands: Vec<C>,
        options: GeneratedBatchOptions,
        attempts: AttemptBudget,
        metadata: &CallMetadata,
        mut report_progress: F,
    ) -> Result<GeneratedBatchResult<C::Outcome>, GeneratedBatchError>
    where
        C: GeneratedCommand + Clone,
        F: FnMut(GeneratedBatchProgress),
    {
        options.validate(commands.len())?;
        let total = commands.len();
        let (transport_batch_size, transport_concurrency) =
            generated_transport_batch_policy(options.concurrency);
        let mut chunks = Vec::new();
        let mut chunk = Vec::with_capacity(transport_batch_size);
        for item in commands.into_iter().enumerate().skip(options.checkpoint) {
            chunk.push(item);
            if chunk.len() == transport_batch_size {
                chunks.push(std::mem::replace(
                    &mut chunk,
                    Vec::with_capacity(transport_batch_size),
                ));
            }
        }
        if !chunk.is_empty() {
            chunks.push(chunk);
        }
        let mut pending = stream::iter(chunks.into_iter().map(|chunk| {
            let client = self.clone();
            let metadata = metadata.clone();
            async move {
                client
                    .execute_generated_transport_batch(chunk, attempts, &metadata)
                    .await
            }
        }))
        .buffer_unordered(transport_concurrency);
        let mut items = Vec::with_capacity(total - options.checkpoint);
        let mut progress_state = BatchCheckpointState::new(options.checkpoint);
        while let Some(completed_chunk) = pending.next().await {
            for (index, result) in completed_chunk {
                record_generated_batch_item_completion(
                    index,
                    result,
                    total,
                    &mut progress_state,
                    &mut items,
                    &mut report_progress,
                );
            }
        }
        items.sort_by_key(|item| item.index);
        Ok(GeneratedBatchResult {
            items,
            checkpoint: progress_state.checkpoint,
        })
    }

    async fn execute_generated_transport_batch<C: GeneratedCommand + Clone>(
        &self,
        commands: Vec<(usize, C)>,
        attempts: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Vec<(
        usize,
        Result<TypedCommandResult<C::Outcome>, GeneratedExecutionError>,
    )> {
        let mut requests = Vec::with_capacity(commands.len());
        let mut prepared = Vec::with_capacity(commands.len());
        let mut completed = Vec::new();
        for (index, command) in commands {
            let generic = match command.idempotent_command() {
                Ok(generic) => generic,
                Err(error) => {
                    completed.push((index, Err(GeneratedExecutionError::CommandShape(error))));
                    continue;
                }
            };
            let request_id = match generate_request_id() {
                Ok(request_id) => request_id,
                Err(error) => {
                    completed.push((
                        index,
                        Err(GeneratedExecutionError::Client(
                            ClientError::IdentifierGeneration(error),
                        )),
                    ));
                    continue;
                }
            };
            requests.push(generic.request(request_id));
            prepared.push((index, command));
        }
        if prepared.is_empty() {
            return completed;
        }

        let mut client = self.inner.clone();
        let response = client
            .execute_batch(
                v1::ExecuteCommandBatchRequest { commands: requests },
                metadata,
            )
            .await;
        let Ok(response) = response else {
            // Transport-level or whole-RPC failure: re-enter every item through
            // the normal same-idempotency-key recovery path.
            let recovered: Vec<_> = stream::iter(prepared.into_iter().map(|(index, command)| {
                let mut client = self.clone();
                let metadata = metadata.clone();
                async move {
                    let result = client
                        .execute_generated_command(&command, attempts, &metadata)
                        .await;
                    (index, result)
                }
            }))
            .buffer_unordered(MAX_GENERATED_TRANSPORT_BATCH_ITEMS)
            .collect()
            .await;
            completed.extend(recovered);
            return completed;
        };

        // Prefer per-item carriage when present (ADR-0084). Older servers leave
        // items empty and only populate legacy success rows.
        if !response.items.is_empty() {
            let recovered =
                complete_batch_items_with_reentry(prepared, response.items, |index, command| {
                    let mut client = self.clone();
                    let metadata = metadata.clone();
                    async move {
                        let result = client
                            .execute_generated_command(&command, attempts, &metadata)
                            .await;
                        (index, result)
                    }
                })
                .await;
            completed.extend(recovered);
            return completed;
        }

        // Legacy items-absent path: positional zip on success rows.
        completed.extend(prepared.into_iter().zip(response.responses).map(
            |((index, command), response)| {
                let outcome = command
                    .decode_outcome(&response)
                    .map_err(GeneratedExecutionError::CommandShape);
                (
                    index,
                    outcome.and_then(|outcome| typed_command_result(&command, outcome, response)),
                )
            },
        ));
        completed
    }
}

impl RiffDbClient {
    /// Executes at most 16 ordinary idempotent commands through one transport
    /// batch while preserving independent per-item recovery.
    ///
    /// This is transport coalescing only: every item remains a separately
    /// authorized command with its own idempotency identity, commit decision,
    /// provenance, and typed outcome. Whole-RPC uncertainty and retryable item
    /// failures re-enter through [`RiffDbClient::execute_with_retry`] with the
    /// identical command input and key.
    pub async fn execute_idempotent_transport_batch_with_retry(
        &self,
        commands: Vec<IdempotentCommand>,
        attempts: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<Vec<Result<v1::ExecuteCommandResponse, ClientError>>, IdempotentTransportBatchError>
    {
        validate_idempotent_transport_batch_len(commands.len())?;

        let mut requests = Vec::with_capacity(commands.len());
        for command in &commands {
            let request_id = match generate_request_id() {
                Ok(request_id) => request_id,
                Err(_) => {
                    // Nothing was submitted. Re-enter every immutable command
                    // through the ordinary path so request-id uncertainty is
                    // classified independently and no item is misattributed.
                    return Ok(
                        reenter_idempotent_commands(self, commands, attempts, metadata).await,
                    );
                }
            };
            requests.push(command.request(request_id));
        }

        let mut client = self.clone();
        let response = client
            .execute_batch(
                v1::ExecuteCommandBatchRequest { commands: requests },
                metadata,
            )
            .await;
        let Ok(response) = response else {
            return Ok(reenter_idempotent_commands(self, commands, attempts, metadata).await);
        };

        if response.items.is_empty() {
            return Ok(response.responses.into_iter().map(Ok).collect());
        }

        let mut resolved = Vec::with_capacity(commands.len());
        let mut reenter = Vec::new();
        for (index, (command, item)) in commands.into_iter().zip(response.items).enumerate() {
            match item.result {
                Some(v1::execute_command_batch_item::Result::Response(response)) => {
                    resolved.push((index, Ok(response)));
                }
                Some(v1::execute_command_batch_item::Result::Error(error_wire)) => {
                    match application_error_from_proto(&error_wire) {
                        Ok(error) if application_error_requires_reentry(&error) => {
                            reenter.push((index, command));
                        }
                        Ok(error) => {
                            resolved.push((index, Err(ClientError::Application(Box::new(error)))));
                        }
                        Err(_) => reenter.push((index, command)),
                    }
                }
                None => reenter.push((index, command)),
            }
        }

        if !reenter.is_empty() {
            let recovered: Vec<_> = stream::iter(reenter.into_iter().map(|(index, command)| {
                let mut client = self.clone();
                let metadata = metadata.clone();
                async move {
                    let result = client
                        .execute_with_retry(&command, attempts, &metadata)
                        .await;
                    (index, result)
                }
            }))
            .buffer_unordered(MAX_GENERATED_TRANSPORT_BATCH_ITEMS)
            .collect()
            .await;
            resolved.extend(recovered);
        }
        resolved.sort_by_key(|(index, _)| *index);
        Ok(resolved.into_iter().map(|(_, result)| result).collect())
    }
}

const fn validate_idempotent_transport_batch_len(
    item_count: usize,
) -> Result<(), IdempotentTransportBatchError> {
    if item_count == 0 {
        return Err(IdempotentTransportBatchError::Empty);
    }
    if item_count > MAX_GENERATED_TRANSPORT_BATCH_ITEMS {
        return Err(IdempotentTransportBatchError::TooManyItems);
    }
    Ok(())
}

async fn reenter_idempotent_commands(
    client: &RiffDbClient,
    commands: Vec<IdempotentCommand>,
    attempts: AttemptBudget,
    metadata: &CallMetadata,
) -> Vec<Result<v1::ExecuteCommandResponse, ClientError>> {
    stream::iter(commands.into_iter().map(|command| {
        let mut client = client.clone();
        let metadata = metadata.clone();
        async move {
            client
                .execute_with_retry(&command, attempts, &metadata)
                .await
        }
    }))
    .buffered(MAX_GENERATED_TRANSPORT_BATCH_ITEMS)
    .collect()
    .await
}

type ResolvedBatchItem<T> = (
    usize,
    Result<TypedCommandResult<T>, GeneratedExecutionError>,
);

/// One batch item scheduled for same-key re-entry.
struct ReenterCandidate<C> {
    index: usize,
    command: C,
    /// Defense-in-depth original protocol failure. Production wire validation
    /// rejects unset/undecodable item arms before this path runs; when re-entry
    /// is exhausted, this is surfaced instead of the generic re-entry error.
    protocol_failure: Option<crate::ProtocolFailure>,
}

type BatchItemResolution<C> = (
    Vec<ResolvedBatchItem<<C as GeneratedCommand>::Outcome>>,
    Vec<ReenterCandidate<C>>,
);

/// Contiguous-progress counters for a generated command batch.
struct BatchCheckpointState {
    completed: usize,
    checkpoint: usize,
    completed_after_checkpoint: BTreeSet<usize>,
}

impl BatchCheckpointState {
    /// Seeds completed/checkpoint from a retained resume checkpoint.
    fn new(checkpoint: usize) -> Self {
        Self {
            completed: checkpoint,
            checkpoint,
            completed_after_checkpoint: BTreeSet::new(),
        }
    }
}

/// Records one independent batch item result and advances the contiguous checkpoint.
///
/// Checkpoint advances over every terminal result (Ok or Err). Callers must only
/// invoke this after any same-key re-entry for the item has finished.
fn record_generated_batch_item_completion<T, F>(
    index: usize,
    result: Result<TypedCommandResult<T>, GeneratedExecutionError>,
    total: usize,
    state: &mut BatchCheckpointState,
    items: &mut Vec<GeneratedBatchItem<T>>,
    report_progress: &mut F,
) where
    F: FnMut(GeneratedBatchProgress),
{
    state.completed += 1;
    state.completed_after_checkpoint.insert(index);
    while state.completed_after_checkpoint.remove(&state.checkpoint) {
        state.checkpoint += 1;
    }
    items.push(GeneratedBatchItem { index, result });
    report_progress(GeneratedBatchProgress {
        completed: state.completed,
        total,
        checkpoint: state.checkpoint,
    });
}

/// Classifies per-item batch carriage into terminal results and re-entry candidates.
///
/// Re-entry uses the registry-derived recovery action, not a hand-listed code set:
/// [`ApplicationRecoveryAction::Retry`] and
/// [`ApplicationRecoveryAction::ResolveWithSameIdempotencyKey`] re-enter
/// `execute_generated_command` (same idempotency key; its AttemptBudget and
/// Overloaded backoff then apply). Every other recovery action is terminal and
/// surfaces directly with zero re-entry.
///
/// Undecodable error details and unset oneof arms are **defense-in-depth**:
/// production clients reject both shapes at the kept client inbound decode
/// (`decode_public_message` inside StrictProstDecoder — no separate client
/// re-walk after decode) before an `Ok` batch response exists, so those wire
/// defects arrive as transport-level protocol failures and take the blanket
/// re-entry path. If a decoded response still reaches these arms, same-key
/// re-entry is scheduled; when that re-entry is exhausted, the original
/// protocol failure is surfaced as the item error.
fn resolve_generated_batch_items<C: GeneratedCommand>(
    prepared: Vec<(usize, C)>,
    items: Vec<v1::ExecuteCommandBatchItem>,
) -> BatchItemResolution<C> {
    let mut resolved = Vec::with_capacity(prepared.len());
    let mut reenter = Vec::new();
    for ((index, command), item) in prepared.into_iter().zip(items) {
        match item.result {
            Some(v1::execute_command_batch_item::Result::Response(item_response)) => {
                let outcome = command
                    .decode_outcome(&item_response)
                    .map_err(GeneratedExecutionError::CommandShape);
                resolved.push((
                    index,
                    outcome
                        .and_then(|outcome| typed_command_result(&command, outcome, item_response)),
                ));
            }
            Some(v1::execute_command_batch_item::Result::Error(error_wire)) => {
                match application_error_from_proto(&error_wire) {
                    Ok(error) if application_error_requires_reentry(&error) => {
                        reenter.push(ReenterCandidate {
                            index,
                            command,
                            protocol_failure: None,
                        });
                    }
                    Ok(error) => {
                        resolved.push((
                            index,
                            Err(GeneratedExecutionError::Client(ClientError::Application(
                                Box::new(error),
                            ))),
                        ));
                    }
                    Err(_) => {
                        reenter.push(ReenterCandidate {
                            index,
                            command,
                            protocol_failure: Some(crate::ProtocolFailure::new(
                                crate::ProtocolFailureKind::InvalidApplicationErrorDetails,
                            )),
                        });
                    }
                }
            }
            None => {
                reenter.push(ReenterCandidate {
                    index,
                    command,
                    protocol_failure: Some(crate::ProtocolFailure::new(
                        crate::ProtocolFailureKind::InvalidInboundMessage,
                    )),
                });
            }
        }
    }
    (resolved, reenter)
}

/// Finishes one transport-batch response: keep terminal arms, re-enter the rest.
async fn complete_batch_items_with_reentry<C, F, Fut>(
    prepared: Vec<(usize, C)>,
    items: Vec<v1::ExecuteCommandBatchItem>,
    reenter_one: F,
) -> Vec<ResolvedBatchItem<C::Outcome>>
where
    C: GeneratedCommand + Clone,
    F: Fn(usize, C) -> Fut,
    Fut: std::future::Future<
            Output = (
                usize,
                Result<TypedCommandResult<C::Outcome>, GeneratedExecutionError>,
            ),
        >,
{
    let (mut resolved, reenter) = resolve_generated_batch_items(prepared, items);
    if !reenter.is_empty() {
        let recovered: Vec<_> = stream::iter(reenter.into_iter().map(|candidate| {
            let protocol_failure = candidate.protocol_failure;
            let reenter_one = &reenter_one;
            async move {
                let (index, result) = reenter_one(candidate.index, candidate.command).await;
                let result = match (result, protocol_failure) {
                    (Err(_), Some(failure)) => Err(GeneratedExecutionError::Client(
                        ClientError::Protocol(failure),
                    )),
                    (other, _) => other,
                };
                (index, result)
            }
        }))
        .buffer_unordered(MAX_GENERATED_TRANSPORT_BATCH_ITEMS)
        .collect()
        .await;
        resolved.extend(recovered);
    }
    resolved
}

const fn application_error_requires_reentry(error: &ApplicationError) -> bool {
    matches!(
        error.recovery_action(),
        ApplicationRecoveryAction::Retry | ApplicationRecoveryAction::ResolveWithSameIdempotencyKey
    )
}

fn generated_transport_batch_policy(item_concurrency: usize) -> (usize, usize) {
    let transport_concurrency = item_concurrency.div_ceil(MAX_GENERATED_TRANSPORT_BATCH_ITEMS);
    let transport_batch_size = item_concurrency / transport_concurrency;
    (transport_batch_size, transport_concurrency)
}

pub(crate) fn typed_command_result<C: GeneratedCommand<Outcome = T>, T>(
    command: &C,
    outcome: T,
    response: v1::ExecuteCommandResponse,
) -> Result<TypedCommandResult<T>, GeneratedExecutionError> {
    let workflow_revisions = command
        .workflow_successor_revisions(&response)
        .map_err(GeneratedExecutionError::CommandShape)?;
    let plan_hash = response.plan_hash.try_into().map_err(|_| {
        GeneratedExecutionError::CommandShape(
            crate::generated::GeneratedCommandError::InvalidOutcomeShape,
        )
    })?;
    Ok(TypedCommandResult {
        outcome,
        commit_sequence: (response.commit_sequence != 0).then_some(response.commit_sequence),
        contract_version: response.contract_version,
        plan_hash,
        replayed: response.status
            == v1::execute_command_response::CompletionStatus::Replayed as i32,
        outcome_uri: response.outcome_uri,
        workflow_revisions,
    })
}

/// Bounds for generated command batches.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GeneratedBatchOptions {
    /// Maximum simultaneously in-flight ordinary command calls.
    pub concurrency: usize,
    /// Contiguous input prefix already represented by a retained checkpoint.
    pub checkpoint: usize,
}

impl GeneratedBatchOptions {
    /// Constructs a checked batch policy.
    pub fn new(concurrency: usize) -> Result<Self, GeneratedBatchError> {
        let value = Self {
            concurrency,
            checkpoint: 0,
        };
        value.validate(1)?;
        Ok(value)
    }

    /// Resumes after a previously reported contiguous input checkpoint.
    #[must_use]
    pub const fn with_checkpoint(mut self, checkpoint: usize) -> Self {
        self.checkpoint = checkpoint;
        self
    }

    fn validate(self, item_count: usize) -> Result<(), GeneratedBatchError> {
        if self.concurrency == 0
            || self.concurrency > MAX_GENERATED_BATCH_CONCURRENCY
            || item_count == 0
            || item_count > 4_096
            || self.checkpoint > item_count
        {
            return Err(GeneratedBatchError::InvalidBounds);
        }
        Ok(())
    }
}

/// One independently completed generated command at its stable input offset.
pub struct GeneratedBatchItem<T> {
    /// Zero-based position in the complete input collection.
    pub index: usize,
    /// Typed command result or its independent public execution failure.
    pub result: Result<TypedCommandResult<T>, GeneratedExecutionError>,
}

/// Bounded progress safe to persist without command inputs or outcomes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GeneratedBatchProgress {
    /// Number of inputs covered by the initial checkpoint or completed now.
    pub completed: usize,
    /// Total number of inputs in the bound collection.
    pub total: usize,
    /// Largest contiguous completed input prefix, safe for exact resume.
    pub checkpoint: usize,
}

/// Stable per-item generated batch result and resumable item checkpoint.
pub struct GeneratedBatchResult<T> {
    /// Newly executed, input-ordered independent outcomes and original offsets.
    pub items: Vec<GeneratedBatchItem<T>>,
    /// Largest contiguous completed input prefix.
    pub checkpoint: usize,
}

/// Failure before a generated batch can submit any item.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GeneratedBatchError {
    /// Item count or concurrency exceeded the public bounds.
    InvalidBounds,
}

impl fmt::Display for GeneratedBatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("generated command batch bounds are invalid")
    }
}

impl std::error::Error for GeneratedBatchError {}

/// Canonical UUID value with lazy display text.
///
/// Wire raise paths keep the 16-byte form; the 36-char text is formatted only
/// when a caller asks for display via [`as_str`](Self::as_str) or
/// [`into_string`](Self::into_string). Parameter construction from validated
/// text retains the original string so adapters that already hold display form
/// do not re-allocate.
#[derive(Debug)]
pub struct ApplicationUuid {
    bytes: [u8; 16],
    text: OnceLock<String>,
}

impl Clone for ApplicationUuid {
    fn clone(&self) -> Self {
        let text = OnceLock::new();
        if let Some(existing) = self.text.get() {
            let _ = text.set(existing.clone());
        }
        Self {
            bytes: self.bytes,
            text,
        }
    }
}

impl ApplicationUuid {
    /// Builds from the exact 16-byte public UUID value (raise path).
    #[must_use]
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self {
            bytes,
            text: OnceLock::new(),
        }
    }

    /// Builds from canonical UUID text (parameter path).
    pub fn from_text(text: impl Into<String>) -> Result<Self, ApplicationClientError> {
        let text = text.into();
        let bytes = parse_uuid(&text)?;
        let cell = OnceLock::new();
        let _ = cell.set(text);
        Ok(Self { bytes, text: cell })
    }

    /// Exact 16-byte form without a text round-trip.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.bytes
    }

    /// Canonical 36-char text; formatted on first access when raised from bytes.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.text
            .get_or_init(|| format_uuid_text(&self.bytes))
            .as_str()
    }

    /// Consumes into canonical text (formats once if never requested).
    #[must_use]
    pub fn into_string(self) -> String {
        match self.text.into_inner() {
            Some(text) => text,
            None => format_uuid_text(&self.bytes),
        }
    }

    /// True when display text has already been materialized.
    #[must_use]
    pub fn text_is_materialized(&self) -> bool {
        self.text.get().is_some()
    }
}

impl PartialEq for ApplicationUuid {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl Eq for ApplicationUuid {}

impl From<[u8; 16]> for ApplicationUuid {
    fn from(bytes: [u8; 16]) -> Self {
        Self::from_bytes(bytes)
    }
}

/// A bounded application value addressed only by contract names.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApplicationValue {
    /// Explicit null.
    Null,
    /// Boolean.
    Bool(bool),
    /// Signed integer.
    I64(i64),
    /// Unsigned integer.
    U64(u64),
    /// Exact fixed-scale decimal.
    Decimal {
        /// Minimal big-endian two's-complement coefficient.
        coefficient_twos_complement: Vec<u8>,
        /// Fractional scale.
        scale: u32,
        /// Optional declared precision assertion.
        precision: Option<u32>,
    },
    /// Currency-qualified exact decimal.
    Money {
        /// Three-letter currency code.
        currency: String,
        /// Exact amount.
        amount: Box<Self>,
    },
    /// Exact text.
    String(String),
    /// Canonical UUID (bytes form; display text is lazy).
    Uuid(ApplicationUuid),
    /// Contract enum variant name lowered without caller-visible numeric IDs.
    Enum(String),
    /// Compiler-generated stable enum identity. Application code uses the name.
    EnumIdentity {
        /// Stable enum type identifier pinned by the application lock.
        type_id: u32,
        /// Stable enum variant identifier pinned by the application lock.
        variant_id: u32,
        /// Symbolic variant name retained for checked presentation.
        name: String,
    },
    /// Opaque bytes.
    Bytes(Vec<u8>),
    /// Days since the Unix epoch.
    Date(i32),
    /// UTC timestamp.
    Timestamp {
        /// Whole seconds since the Unix epoch.
        seconds: i64,
        /// Nanosecond fraction.
        nanos: u32,
    },
    /// Canonical finite binary32 vector with a structurally bounded dimension.
    Vector(riffdb_types::CanonicalVector),
    /// Ordered bounded values.
    List(Vec<Self>),
    /// Name-addressed record.
    Record(BTreeMap<String, Self>),
}

/// Closed population selected by an authoritative vector-state inspection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VectorStateInspectionKind {
    /// Source-newer or missing embedding evidence.
    Stale,
    /// Embeddings produced by a non-current declared model version.
    OutdatedModel,
}

/// One symbolic bounded vector-state inspection request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorStateInspection {
    contract: ApplicationContract,
    entity: String,
    field: String,
    partition: ApplicationValue,
    kind: VectorStateInspectionKind,
    limit: u32,
    cursor: Option<Vec<u8>>,
}

impl VectorStateInspection {
    /// Creates one initial bounded inspection page.
    #[must_use]
    pub fn new(
        contract: ApplicationContract,
        entity: impl Into<String>,
        field: impl Into<String>,
        partition: ApplicationValue,
        kind: VectorStateInspectionKind,
        limit: u32,
    ) -> Self {
        Self {
            contract,
            entity: entity.into(),
            field: field.into(),
            partition,
            kind,
            limit,
            cursor: None,
        }
    }

    /// Continues from one opaque server-issued cursor.
    #[must_use]
    pub fn after(mut self, cursor: Vec<u8>) -> Self {
        self.cursor = Some(cursor);
        self
    }

    fn validate(&self) -> Result<(), ApplicationClientError> {
        validate_contract(&self.contract)?;
        if self.entity.is_empty()
            || self.entity.len() > 256
            || self.field.is_empty()
            || self.field.len() > 256
            || self.limit == 0
            || self.limit > 500
            || self
                .cursor
                .as_ref()
                .is_some_and(|cursor| cursor.len() != 16)
        {
            return Err(ApplicationClientError::InvalidInput);
        }
        Ok(())
    }
}

/// Whole-partition staleness summary, returned only when policy permits counts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VectorStalenessSummary {
    /// Total live entities covered by the authoritative observation.
    pub total_entities: u64,
    /// Source-stale live entities.
    pub stale_count: u64,
    /// Strict declared count threshold.
    pub stale_entity_count_threshold: u64,
    /// Whether `stale_count` strictly exceeds the threshold.
    pub slo_breached: bool,
}

/// Whole-partition model-version summary, returned only when policy permits counts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VectorModelVersionSummary {
    /// Embeddings carrying the current declared model identity and version.
    pub current_count: u64,
    /// Embeddings carrying another retained model identity or version.
    pub outdated_count: u64,
}

/// One policy-visible stale entity observation.
#[derive(Clone, Eq, PartialEq)]
pub struct VectorStalenessItem {
    /// Opaque canonical entity key bytes.
    pub entity_key: Vec<u8>,
    /// Newest declared source-field write.
    pub newest_source_write: u64,
    /// Last embedding write, or absence when no embedding exists.
    pub embedding_write: Option<u64>,
}

impl fmt::Debug for VectorStalenessItem {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VectorStalenessItem")
            .field("entity_key", &"[REDACTED]")
            .field("newest_source_write", &self.newest_source_write)
            .field("embedding_write", &self.embedding_write)
            .finish()
    }
}

/// One policy-visible outdated model observation.
#[derive(Clone, Eq, PartialEq)]
pub struct VectorModelVersionItem {
    /// Opaque canonical entity key bytes.
    pub entity_key: Vec<u8>,
    /// Bounded model identity.
    pub model: String,
    /// Bounded model version.
    pub model_version: String,
    /// Authoritative embedding write sequence.
    pub embedding_write: u64,
}

impl fmt::Debug for VectorModelVersionItem {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VectorModelVersionItem")
            .field("entity_key", &"[REDACTED]")
            .field("model", &"[REDACTED]")
            .field("model_version", &"[REDACTED]")
            .field("embedding_write", &self.embedding_write)
            .finish()
    }
}

/// One bounded policy-filtered vector-state page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorInspectionPage<T> {
    /// Visible canonical items.
    pub items: Vec<T>,
    /// Opaque continuation cursor.
    pub next_cursor: Option<Vec<u8>>,
    /// Authoritative observation frontier, when one exists.
    pub observed_frontier: Option<u64>,
}

/// Closed response shape. Count summaries and row-policy-filtered pages cannot
/// be confused or silently substituted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VectorStateInspectionResult {
    /// Whole-partition staleness counts.
    StalenessSummary(VectorStalenessSummary),
    /// Policy-filtered stale entities.
    StaleEntities(VectorInspectionPage<VectorStalenessItem>),
    /// Whole-partition model-version counts.
    ModelVersionSummary(VectorModelVersionSummary),
    /// Policy-filtered outdated embeddings.
    OutdatedModelEntities(VectorInspectionPage<VectorModelVersionItem>),
}

impl TryFrom<app_v1::InspectVectorStateResponse> for VectorStateInspectionResult {
    type Error = ApplicationClientError;

    fn try_from(response: app_v1::InspectVectorStateResponse) -> Result<Self, Self::Error> {
        use app_v1::inspect_vector_state_response::Result;
        Ok(
            match response
                .result
                .ok_or(ApplicationClientError::InvalidResponse)?
            {
                Result::StalenessSummary(report) => {
                    Self::StalenessSummary(VectorStalenessSummary {
                        total_entities: report.total_entities,
                        stale_count: report.stale_count,
                        stale_entity_count_threshold: report.stale_entity_count_threshold,
                        slo_breached: report.slo_breached,
                    })
                }
                Result::StaleEntities(page) => Self::StaleEntities(VectorInspectionPage {
                    items: page
                        .items
                        .into_iter()
                        .map(|item| VectorStalenessItem {
                            entity_key: item.entity_key,
                            newest_source_write: item.newest_source_write,
                            embedding_write: item.embedding_write,
                        })
                        .collect(),
                    next_cursor: page.next_cursor,
                    observed_frontier: page.observed_frontier,
                }),
                Result::ModelVersionSummary(report) => {
                    Self::ModelVersionSummary(VectorModelVersionSummary {
                        current_count: report.current_count,
                        outdated_count: report.outdated_count,
                    })
                }
                Result::OutdatedModelEntities(page) => {
                    Self::OutdatedModelEntities(VectorInspectionPage {
                        items: page
                            .items
                            .into_iter()
                            .map(|item| VectorModelVersionItem {
                                entity_key: item.entity_key,
                                model: item.model,
                                model_version: item.model_version,
                                embedding_write: item.embedding_write,
                            })
                            .collect(),
                        next_cursor: page.next_cursor,
                        observed_frontier: page.observed_frontier,
                    })
                }
            },
        )
    }
}

/// Active or exact symbolic contract selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApplicationContract {
    /// Resolve the current active contract.
    Active,
    /// Resolve one exact retained contract.
    Exact {
        /// Contract lineage.
        lineage: String,
        /// Positive contract version.
        version: u64,
        /// Optional exact bundle hash.
        bundle_hash: Option<[u8; 32]>,
    },
}

/// One named query invocation pinned optionally to an immutable module hash.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamedQuery {
    contract: ApplicationContract,
    name: String,
    module_hash: Option<[u8; 32]>,
    expected_plan_hash: Option<[u8; 32]>,
    parameters: BTreeMap<String, ApplicationValue>,
    cursor: Option<String>,
    minimum_application_head: Option<u64>,
}

impl NamedQuery {
    /// Builds a name-addressed query call.
    pub fn new(
        contract: ApplicationContract,
        name: impl Into<String>,
        module_hash: Option<[u8; 32]>,
        parameters: BTreeMap<String, ApplicationValue>,
        cursor: Option<String>,
    ) -> Result<Self, ApplicationClientError> {
        let name = name.into();
        if name.is_empty() || name.len() > 256 || parameters.len() > 4_096 {
            return Err(ApplicationClientError::InvalidInput);
        }
        validate_contract(&contract)?;
        if parameters
            .iter()
            .any(|(name, _)| name.is_empty() || name.len() > 256)
        {
            return Err(ApplicationClientError::InvalidInput);
        }
        Ok(Self {
            contract,
            name,
            module_hash,
            expected_plan_hash: None,
            parameters,
            cursor,
            minimum_application_head: None,
        })
    }

    /// Requires the server to return this exact compiler-owned query plan.
    #[must_use]
    pub const fn expect_plan_hash(mut self, plan_hash: [u8; 32]) -> Self {
        self.expected_plan_hash = Some(plan_hash);
        self
    }

    /// Applies generated pagination and read-after-commit options.
    pub fn with_options(mut self, options: QueryOptions) -> Result<Self, ApplicationClientError> {
        if options.read_after_commit == Some(0) {
            return Err(ApplicationClientError::InvalidInput);
        }
        self.cursor = options.cursor;
        self.minimum_application_head = options.read_after_commit;
        Ok(self)
    }
}

/// Typed execution options shared by every generated named query.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueryOptions {
    cursor: Option<String>,
    read_after_commit: Option<u64>,
}

impl QueryOptions {
    /// Creates default query options.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            cursor: None,
            read_after_commit: None,
        }
    }

    /// Continues from one opaque application cursor.
    #[must_use]
    pub fn after(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = Some(cursor.into());
        self
    }

    /// Requires a snapshot at or after one observed application commit.
    #[must_use]
    pub const fn read_after_commit(mut self, commit_sequence: u64) -> Self {
        self.read_after_commit = Some(commit_sequence);
        self
    }
}

/// Typed generated result with its snapshot and pagination metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedQueryResult<T> {
    /// Generated declared-result value.
    pub value: T,
    /// Exact returned identity, verified against the generated request.
    pub identity: QueryResponseIdentity,
    /// Authoritative application head observed by the one-snapshot read.
    pub application_head: u64,
    /// Opaque continuation cursor, when another bounded page exists.
    pub next_cursor: Option<String>,
}

/// Typed generated command result without kernel protocol exposure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedCommandResult<T> {
    /// Generated declared business outcome.
    pub outcome: T,
    /// Durable application commit sequence.
    pub commit_sequence: Option<u64>,
    /// Exact contract version used by execution.
    pub contract_version: u64,
    /// Exact compiler-owned command plan used by execution.
    pub plan_hash: [u8; 32],
    /// Whether the invocation replayed an already durable result.
    pub replayed: bool,
    /// Durable opaque outcome locator, when available.
    pub outcome_uri: Option<String>,
    /// Compiler-derived successor revisions for successful checked workflow
    /// mutations, keyed by source binding rather than numeric entity IDs.
    pub workflow_revisions: Vec<crate::generated::WorkflowSuccessorRevision>,
}

/// One symbolic command invocation with only name-addressed input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationCommand {
    name: String,
    expected_contract_version: Option<u64>,
    input: BTreeMap<String, ApplicationValue>,
}

impl ApplicationCommand {
    /// Builds a command invocation.
    pub fn new(
        name: impl Into<String>,
        expected_contract_version: Option<u64>,
        input: BTreeMap<String, ApplicationValue>,
    ) -> Result<Self, ApplicationClientError> {
        let name = name.into();
        if name.is_empty()
            || name.len() > 256
            || expected_contract_version == Some(0)
            || input.is_empty()
            || input.len() > 4_096
            || input
                .keys()
                .any(|field| field.is_empty() || field.len() > 256)
        {
            return Err(ApplicationClientError::InvalidInput);
        }
        Ok(Self {
            name,
            expected_contract_version,
            input,
        })
    }

    pub(crate) fn into_idempotent_command(
        self,
    ) -> Result<IdempotentCommand, ApplicationClientError> {
        let input = lower_value(ApplicationValue::Record(self.input))?;
        IdempotentCommand::new(self.name, self.expected_contract_version, input)
            .map_err(|_| ApplicationClientError::InvalidInput)
    }
}

/// Query result cardinality declared in RiffQL.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationCardinality {
    /// Exactly one record.
    One,
    /// Zero or one record.
    Maybe,
    /// Bounded records.
    Many,
}

/// One name-addressed returned record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationRecord {
    /// Optional entity symbol.
    pub entity: String,
    /// Fields by contract symbol.
    pub fields: BTreeMap<String, ApplicationValue>,
}

/// One top-level query result field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationResultField {
    /// Declared cardinality.
    pub cardinality: ApplicationCardinality,
    /// Returned records.
    pub records: Vec<ApplicationRecord>,
}

/// Complete one-snapshot named query result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamedQueryResult {
    /// Exact query outcome name.
    pub outcome: String,
    /// Authoritative snapshot head.
    pub application_head: u64,
    /// Result fields by declared name.
    pub fields: BTreeMap<String, ApplicationResultField>,
    /// Opaque continuation cursor.
    pub next_cursor: Option<String>,
    /// Exact symbolic identity used by named execution.
    pub identity: QueryResponseIdentity,
}

/// Exact server-returned identity for one named query execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryResponseIdentity {
    /// Exact contract lineage.
    pub contract_lineage: String,
    /// Exact contract version.
    pub contract_version: u64,
    /// Exact contract bundle hash.
    pub contract_bundle_hash: [u8; 32],
    /// Exact deployed query module hash.
    pub module_hash: [u8; 32],
    /// Exact query operation name.
    pub query_name: String,
    /// Exact compiler-owned query plan hash.
    pub plan_hash: [u8; 32],
}

/// Successful command completion without kernel protocol details.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationCommandResult {
    /// Declared business outcome name when present.
    pub outcome: Option<String>,
    /// Complete name-addressed declared outcome payload.
    pub outcome_value: Option<ApplicationValue>,
    /// Application commit sequence; absent for unjournaled read-only commands.
    pub commit_sequence: Option<u64>,
    /// Exact contract version used by the command.
    pub contract_version: u64,
    /// Exact command plan hash used by the command.
    pub plan_hash: [u8; 32],
    /// Whether this invocation replayed an already durable result.
    pub replayed: bool,
    /// Durable outcome locator when present.
    pub outcome_uri: Option<String>,
}

pub(crate) fn raise_application_command_result(
    response: v1::ExecuteCommandResponse,
) -> Result<ApplicationCommandResult, ApplicationClientError> {
    Ok(ApplicationCommandResult {
        outcome: (!response.outcome_type.is_empty()).then_some(response.outcome_type),
        outcome_value: response.outcome.map(raise_value).transpose()?,
        commit_sequence: (response.commit_sequence != 0).then_some(response.commit_sequence),
        contract_version: response.contract_version,
        plan_hash: response
            .plan_hash
            .try_into()
            .map_err(|_| ApplicationClientError::InvalidResponse)?,
        replayed: response.status
            == v1::execute_command_response::CompletionStatus::Replayed as i32,
        outcome_uri: response.outcome_uri,
    })
}

/// Closed application-client failure.
#[derive(Debug)]
pub enum ApplicationClientError {
    /// Submitted name-addressed shape is invalid.
    InvalidInput,
    /// Server returned an invalid or low-level-shaped application response.
    InvalidResponse,
    /// Request identity generation failed.
    IdentifierUnavailable,
    /// Checked public transport failure.
    Client(ClientError),
}

impl fmt::Display for ApplicationClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput => formatter.write_str("application input is invalid"),
            Self::InvalidResponse => formatter.write_str("application response is invalid"),
            Self::IdentifierUnavailable => formatter.write_str("request identity is unavailable"),
            Self::Client(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ApplicationClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Client(error) => Some(error),
            Self::InvalidInput | Self::InvalidResponse | Self::IdentifierUnavailable => None,
        }
    }
}

impl ApplicationClientError {
    /// Returns the checked semantic application failure, when supplied by RiffDB.
    #[must_use]
    pub const fn semantic_error(&self) -> Option<&riffdb_errors::ApplicationError> {
        match self {
            Self::Client(error) => error.application_error(),
            Self::InvalidInput | Self::InvalidResponse | Self::IdentifierUnavailable => None,
        }
    }
}

impl From<ClientError> for ApplicationClientError {
    fn from(error: ClientError) -> Self {
        Self::Client(error)
    }
}

impl From<GeneratedExecutionError> for ApplicationClientError {
    fn from(error: GeneratedExecutionError) -> Self {
        match error {
            GeneratedExecutionError::Client(error) => Self::Client(error),
            GeneratedExecutionError::CommandShape(_) => Self::InvalidResponse,
        }
    }
}

impl RiffDbClient {
    /// Executes one named module query through one public application RPC.
    pub async fn execute_named_application_query(
        &mut self,
        query: NamedQuery,
        metadata: &CallMetadata,
    ) -> Result<NamedQueryResult, ApplicationClientError> {
        let response = self
            .execute_named_application_query_wire(query, metadata)
            .await?;
        raise_query_result(response)
    }

    async fn execute_named_application_query_wire(
        &mut self,
        query: NamedQuery,
        metadata: &CallMetadata,
    ) -> Result<app_v1::ExecuteQueryResponse, ApplicationClientError> {
        let expected_contract = query.contract.clone();
        let expected_name = query.name.clone();
        let expected_module_hash = query.module_hash;
        let expected_plan_hash = query.expected_plan_hash;
        let request_id = Vec::from(
            generate_request_id()
                .map_err(|_| ApplicationClientError::IdentifierUnavailable)?
                .into_bytes(),
        );
        // Move owned parameter names once; avoid intermediate (name, value) copies.
        let mut parameters = Vec::with_capacity(query.parameters.len());
        for (name, value) in query.parameters {
            parameters.push(app_v1::Parameter {
                name,
                value: Some(lower_value(value)?),
            });
        }
        let response = self
            .execute_selected_query_transport(
                app_v1::ExecuteQueryRequest {
                    contract: lower_contract(query.contract),
                    query: Some(app_v1::execute_query_request::Query::QueryName(query.name)),
                    module_hash: query.module_hash.map(|hash| hash.to_vec()),
                    parameters,
                    cursor: query.cursor,
                    minimum_application_head: query.minimum_application_head,
                    accepted_result_encodings: vec![
                        app_v1::NamedResultEncoding::LegacyRecords as i32,
                        app_v1::NamedResultEncoding::CompactV1 as i32,
                    ],
                    request_id,
                },
                metadata,
            )
            .await?;
        validate_query_response_identity(
            &response,
            &expected_contract,
            &expected_name,
            expected_module_hash,
            expected_plan_hash,
        )?;
        Ok(response)
    }

    /// Executes one symbolic command with bounded retry and no kernel-shaped caller input.
    pub async fn execute_application_command(
        &mut self,
        command: ApplicationCommand,
        attempts: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<ApplicationCommandResult, ApplicationClientError> {
        let command_name = command.name.clone();
        let command = command.into_idempotent_command()?;
        let response = self
            .execute_with_retry(&command, attempts, metadata)
            .await
            .map_err(|error| contextualize_command_client_error(error, &command_name))?;
        raise_application_command_result(response)
    }
}

pub(crate) fn contextualize_command_client_error(
    error: ClientError,
    command_name: &str,
) -> ClientError {
    let ClientError::Public(public) = error else {
        return error;
    };
    let context = ApplicationErrorContext::empty()
        .with_operation_symbol(command_name.to_owned())
        .unwrap_or_else(|_| ApplicationErrorContext::empty());
    ClientError::Application(Box::new(ApplicationError::from_public_error(
        &public,
        ApplicationOperation::ExecuteCommand,
        context,
    )))
}

fn validate_query_response_identity(
    response: &app_v1::ExecuteQueryResponse,
    contract: &ApplicationContract,
    query_name: &str,
    module_hash: Option<[u8; 32]>,
    plan_hash: Option<[u8; 32]>,
) -> Result<(), ApplicationClientError> {
    let identity = response
        .identity
        .as_ref()
        .ok_or(ApplicationClientError::InvalidResponse)?;
    if identity.query_name.as_deref() != Some(query_name)
        || module_hash
            .is_some_and(|expected| identity.module_hash.as_deref() != Some(expected.as_slice()))
        || plan_hash.is_some_and(|expected| identity.plan_hash.as_slice() != expected.as_slice())
    {
        return Err(ApplicationClientError::InvalidResponse);
    }
    if let ApplicationContract::Exact {
        lineage,
        version,
        bundle_hash,
    } = contract
        && (identity.contract_lineage != *lineage
            || identity.contract_version != *version
            || bundle_hash.is_some_and(|expected| {
                identity.contract_bundle_hash.as_slice() != expected.as_slice()
            }))
    {
        return Err(ApplicationClientError::InvalidResponse);
    }
    Ok(())
}

fn raise_application_catalog_preflight(
    response: app_v1::GetApplicationCatalogResponse,
    contract: &ApplicationContract,
) -> Result<ApplicationCatalogPreflight, ApplicationClientError> {
    if response.schema != APPLICATION_CATALOG_SCHEMA_V1
        || response.contract_lineage.is_empty()
        || response.contract_version == 0
    {
        return Err(ApplicationClientError::InvalidResponse);
    }
    let contract_bundle_hash: [u8; 32] = response
        .contract_bundle_hash
        .try_into()
        .map_err(|_| ApplicationClientError::InvalidResponse)?;
    if let ApplicationContract::Exact {
        lineage,
        version,
        bundle_hash,
    } = contract
        && (response.contract_lineage != *lineage
            || response.contract_version != *version
            || bundle_hash.is_some_and(|expected| expected != contract_bundle_hash))
    {
        return Err(ApplicationClientError::InvalidResponse);
    }

    let mut query_module_hashes = Vec::with_capacity(response.query_module_hashes.len());
    for hash in response.query_module_hashes {
        query_module_hashes.push(
            hash.try_into()
                .map_err(|_| ApplicationClientError::InvalidResponse)?,
        );
    }
    if query_module_hashes
        .windows(2)
        .any(|pair| pair[0] >= pair[1])
    {
        return Err(ApplicationClientError::InvalidResponse);
    }

    let mut features = Vec::with_capacity(response.features.len());
    for view in response.features {
        let feature = match app_v1::ApplicationCatalogFeature::try_from(view.feature) {
            Ok(app_v1::ApplicationCatalogFeature::OperationalOptionalPredicates) => {
                ApplicationCatalogFeature::OperationalOptionalPredicates
            }
            Ok(app_v1::ApplicationCatalogFeature::StableCursorPages) => {
                ApplicationCatalogFeature::StableCursorPages
            }
            Ok(app_v1::ApplicationCatalogFeature::NullExistencePredicates) => {
                ApplicationCatalogFeature::NullExistencePredicates
            }
            Ok(app_v1::ApplicationCatalogFeature::BinaryTextPrefix) => {
                ApplicationCatalogFeature::BinaryTextPrefix
            }
            Ok(app_v1::ApplicationCatalogFeature::UnicodeFoldTextPrefixV1) => {
                ApplicationCatalogFeature::UnicodeFoldTextPrefixV1
            }
            Ok(app_v1::ApplicationCatalogFeature::ExactAggregates) => {
                ApplicationCatalogFeature::ExactAggregates
            }
            Ok(app_v1::ApplicationCatalogFeature::Unspecified) | Err(_) => {
                return Err(ApplicationClientError::InvalidResponse);
            }
        };
        let state = match app_v1::ApplicationCatalogFeatureState::try_from(view.state) {
            Ok(app_v1::ApplicationCatalogFeatureState::Available) => {
                ApplicationCatalogFeatureState::Available
            }
            Ok(app_v1::ApplicationCatalogFeatureState::Unavailable) => {
                ApplicationCatalogFeatureState::Unavailable
            }
            Ok(app_v1::ApplicationCatalogFeatureState::Unspecified) | Err(_) => {
                return Err(ApplicationClientError::InvalidResponse);
            }
        };
        features.push(ApplicationCatalogFeatureView { feature, state });
    }
    if features.len() != 6
        || features
            .windows(2)
            .any(|pair| pair[0].feature >= pair[1].feature)
    {
        return Err(ApplicationClientError::InvalidResponse);
    }

    Ok(ApplicationCatalogPreflight {
        contract_lineage: response.contract_lineage,
        contract_version: response.contract_version,
        contract_bundle_hash,
        query_module_hashes,
        features,
    })
}

pub(crate) fn validate_contract(
    contract: &ApplicationContract,
) -> Result<(), ApplicationClientError> {
    if let ApplicationContract::Exact {
        lineage, version, ..
    } = contract
        && (lineage.is_empty() || lineage.len() > 256 || *version == 0)
    {
        return Err(ApplicationClientError::InvalidInput);
    }
    Ok(())
}

fn lower_contract(contract: ApplicationContract) -> Option<app_v1::ContractSelector> {
    match contract {
        ApplicationContract::Active => None,
        ApplicationContract::Exact {
            lineage,
            version,
            bundle_hash,
        } => Some(app_v1::ContractSelector {
            lineage,
            version,
            bundle_hash: bundle_hash.map_or_else(Vec::new, |hash| hash.to_vec()),
        }),
    }
}

pub(crate) fn lower_value(value: ApplicationValue) -> Result<v1::Value, ApplicationClientError> {
    use v1::value::Kind;
    let kind = match value {
        ApplicationValue::Null => Kind::NullValue(v1::NullValue::NullValue as i32),
        ApplicationValue::Bool(value) => Kind::BoolValue(value),
        ApplicationValue::I64(value) => Kind::I64Value(value),
        ApplicationValue::U64(value) => Kind::U64Value(value),
        ApplicationValue::Decimal {
            coefficient_twos_complement,
            scale,
            precision,
        } => Kind::DecimalValue(v1::Decimal {
            coefficient_twos_complement,
            scale,
            precision,
        }),
        ApplicationValue::Money { currency, amount } => {
            let Kind::DecimalValue(amount) = lower_value(*amount)?
                .kind
                .ok_or(ApplicationClientError::InvalidInput)?
            else {
                return Err(ApplicationClientError::InvalidInput);
            };
            Kind::MoneyValue(v1::Money {
                currency,
                amount: Some(amount),
            })
        }
        ApplicationValue::String(value) => Kind::StringValue(value),
        ApplicationValue::Uuid(value) => Kind::UuidValue(value.as_bytes().to_vec()),
        ApplicationValue::Enum(name) if !name.is_empty() && name.len() <= 256 => {
            Kind::EnumValue(v1::EnumValue {
                type_id: 0,
                variant_id: 0,
                name,
            })
        }
        ApplicationValue::Enum(_) => return Err(ApplicationClientError::InvalidInput),
        ApplicationValue::EnumIdentity {
            type_id,
            variant_id,
            name,
        } if type_id != 0 && variant_id != 0 && !name.is_empty() && name.len() <= 256 => {
            Kind::EnumValue(v1::EnumValue {
                type_id,
                variant_id,
                name,
            })
        }
        ApplicationValue::EnumIdentity { .. } => {
            return Err(ApplicationClientError::InvalidInput);
        }
        ApplicationValue::Bytes(value) => Kind::BytesValue(value),
        ApplicationValue::Date(days_since_unix_epoch) => Kind::DateValue(v1::Date {
            days_since_unix_epoch,
        }),
        ApplicationValue::Timestamp { seconds, nanos } if nanos < 1_000_000_000 => {
            Kind::TimestampValue(v1::Timestamp { seconds, nanos })
        }
        ApplicationValue::Timestamp { .. } => {
            return Err(ApplicationClientError::InvalidInput);
        }
        ApplicationValue::Vector(value) => Kind::VectorValue(v1::VectorValue {
            components: value.into_components(),
        }),
        ApplicationValue::List(values) => Kind::ListValue(v1::ValueList {
            values: values
                .into_iter()
                .map(lower_value)
                .collect::<Result<Vec<_>, _>>()?,
        }),
        ApplicationValue::Record(fields) => Kind::RecordValue(v1::ValueRecord {
            fields: fields
                .into_iter()
                .map(|(name, value)| {
                    Ok(v1::ValueField {
                        field_id: None,
                        name,
                        value: Some(lower_value(value)?),
                    })
                })
                .collect::<Result<Vec<_>, ApplicationClientError>>()?,
        }),
    };
    Ok(v1::Value { kind: Some(kind) })
}

#[doc(hidden)]
pub fn raise_query_result(
    response: app_v1::ExecuteQueryResponse,
) -> Result<NamedQueryResult, ApplicationClientError> {
    let identity = query_response_identity(&response)?;
    let selected_encoding =
        app_v1::NamedResultEncoding::try_from(response.selected_result_encoding)
            .map_err(|_| ApplicationClientError::InvalidResponse)?;
    let fields = match selected_encoding {
        app_v1::NamedResultEncoding::Unspecified | app_v1::NamedResultEncoding::LegacyRecords => {
            if response.compact_result.is_some() {
                return Err(ApplicationClientError::InvalidResponse);
            }
            raise_legacy_query_fields(response.fields)?
        }
        app_v1::NamedResultEncoding::CompactV1 => {
            if !response.fields.is_empty() {
                return Err(ApplicationClientError::InvalidResponse);
            }
            raise_compact_query_field(
                response
                    .compact_result
                    .ok_or(ApplicationClientError::InvalidResponse)?,
            )?
        }
    };
    Ok(NamedQueryResult {
        outcome: response.outcome,
        application_head: response.application_head,
        fields,
        next_cursor: response.next_cursor,
        identity,
    })
}

fn query_response_identity(
    response: &app_v1::ExecuteQueryResponse,
) -> Result<QueryResponseIdentity, ApplicationClientError> {
    let identity = response
        .identity
        .as_ref()
        .ok_or(ApplicationClientError::InvalidResponse)?;
    Ok(QueryResponseIdentity {
        contract_lineage: identity.contract_lineage.clone(),
        contract_version: identity.contract_version,
        contract_bundle_hash: identity
            .contract_bundle_hash
            .as_slice()
            .try_into()
            .map_err(|_| ApplicationClientError::InvalidResponse)?,
        module_hash: identity
            .module_hash
            .as_deref()
            .ok_or(ApplicationClientError::InvalidResponse)?
            .try_into()
            .map_err(|_| ApplicationClientError::InvalidResponse)?,
        query_name: identity
            .query_name
            .clone()
            .ok_or(ApplicationClientError::InvalidResponse)?,
        plan_hash: identity
            .plan_hash
            .as_slice()
            .try_into()
            .map_err(|_| ApplicationClientError::InvalidResponse)?,
    })
}

fn raise_legacy_query_fields(
    wire_fields: Vec<app_v1::ResultField>,
) -> Result<BTreeMap<String, ApplicationResultField>, ApplicationClientError> {
    let mut fields = BTreeMap::new();
    for field in wire_fields {
        let cardinality = match app_v1::ResultCardinality::try_from(field.cardinality) {
            Ok(app_v1::ResultCardinality::One) => ApplicationCardinality::One,
            Ok(app_v1::ResultCardinality::Maybe) => ApplicationCardinality::Maybe,
            Ok(app_v1::ResultCardinality::Many) => ApplicationCardinality::Many,
            Ok(app_v1::ResultCardinality::Unspecified) | Err(_) => {
                return Err(ApplicationClientError::InvalidResponse);
            }
        };
        let records = field
            .records
            .into_iter()
            .map(|record| {
                let mut fields = BTreeMap::new();
                for field in record.fields {
                    if fields
                        .insert(
                            field.name,
                            raise_value(
                                field.value.ok_or(ApplicationClientError::InvalidResponse)?,
                            )?,
                        )
                        .is_some()
                    {
                        return Err(ApplicationClientError::InvalidResponse);
                    }
                }
                Ok(ApplicationRecord {
                    entity: record.entity,
                    fields,
                })
            })
            .collect::<Result<Vec<_>, ApplicationClientError>>()?;
        if fields
            .insert(
                field.name,
                ApplicationResultField {
                    cardinality,
                    records,
                },
            )
            .is_some()
        {
            return Err(ApplicationClientError::InvalidResponse);
        }
    }
    Ok(fields)
}

fn raise_compact_query_field(
    field: app_v1::CompactResultField,
) -> Result<BTreeMap<String, ApplicationResultField>, ApplicationClientError> {
    if field.name.is_empty() || field.entity.is_empty() || field.fields.is_empty() {
        return Err(ApplicationClientError::InvalidResponse);
    }
    let cardinality = match app_v1::ResultCardinality::try_from(field.cardinality) {
        Ok(app_v1::ResultCardinality::Many) => ApplicationCardinality::Many,
        _ => return Err(ApplicationClientError::InvalidResponse),
    };
    let mut names = BTreeSet::new();
    for name in &field.fields {
        if name.is_empty() || !names.insert(name.as_str()) {
            return Err(ApplicationClientError::InvalidResponse);
        }
    }
    let width = field.fields.len();
    let records = field
        .rows
        .into_iter()
        .map(|row| {
            if row.values.len() != width {
                return Err(ApplicationClientError::InvalidResponse);
            }
            let fields = field
                .fields
                .iter()
                .cloned()
                .zip(row.values)
                .map(|(name, value)| Ok((name, raise_value(value)?)))
                .collect::<Result<BTreeMap<_, _>, ApplicationClientError>>()?;
            Ok(ApplicationRecord {
                entity: field.entity.clone(),
                fields,
            })
        })
        .collect::<Result<Vec<_>, ApplicationClientError>>()?;
    Ok(BTreeMap::from([(
        field.name,
        ApplicationResultField {
            cardinality,
            records,
        },
    )]))
}

#[doc(hidden)]
pub fn raise_value(value: v1::Value) -> Result<ApplicationValue, ApplicationClientError> {
    use v1::value::Kind;
    match value.kind.ok_or(ApplicationClientError::InvalidResponse)? {
        Kind::NullValue(_) => Ok(ApplicationValue::Null),
        Kind::BoolValue(value) => Ok(ApplicationValue::Bool(value)),
        Kind::I64Value(value) => Ok(ApplicationValue::I64(value)),
        Kind::U64Value(value) => Ok(ApplicationValue::U64(value)),
        Kind::DecimalValue(value) => Ok(ApplicationValue::Decimal {
            coefficient_twos_complement: value.coefficient_twos_complement,
            scale: value.scale,
            precision: value.precision,
        }),
        Kind::MoneyValue(value) => {
            let amount = value
                .amount
                .ok_or(ApplicationClientError::InvalidResponse)?;
            Ok(ApplicationValue::Money {
                currency: value.currency,
                amount: Box::new(ApplicationValue::Decimal {
                    coefficient_twos_complement: amount.coefficient_twos_complement,
                    scale: amount.scale,
                    precision: amount.precision,
                }),
            })
        }
        Kind::StringValue(value) => Ok(ApplicationValue::String(value)),
        Kind::BytesValue(value) => Ok(ApplicationValue::Bytes(value)),
        Kind::UuidValue(value) => {
            let bytes: [u8; 16] = value
                .as_slice()
                .try_into()
                .map_err(|_| ApplicationClientError::InvalidResponse)?;
            Ok(ApplicationValue::Uuid(ApplicationUuid::from_bytes(bytes)))
        }
        Kind::EnumValue(value) if !value.name.is_empty() => Ok(ApplicationValue::Enum(value.name)),
        Kind::ListValue(values) => Ok(ApplicationValue::List(
            values
                .values
                .into_iter()
                .map(raise_value)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Kind::RecordValue(record) => {
            let mut fields = BTreeMap::new();
            for field in record.fields {
                if field.name.is_empty()
                    || fields
                        .insert(
                            field.name,
                            raise_value(
                                field.value.ok_or(ApplicationClientError::InvalidResponse)?,
                            )?,
                        )
                        .is_some()
                {
                    return Err(ApplicationClientError::InvalidResponse);
                }
            }
            Ok(ApplicationValue::Record(fields))
        }
        Kind::DateValue(value) => Ok(ApplicationValue::Date(value.days_since_unix_epoch)),
        Kind::TimestampValue(value) if value.nanos < 1_000_000_000 => {
            Ok(ApplicationValue::Timestamp {
                seconds: value.seconds,
                nanos: value.nanos,
            })
        }
        Kind::VectorValue(value) => riffdb_types::CanonicalVector::new(value.components)
            .map(ApplicationValue::Vector)
            .map_err(|_| ApplicationClientError::InvalidResponse),
        Kind::TimestampValue(_) | Kind::EnumValue(_) => {
            Err(ApplicationClientError::InvalidResponse)
        }
    }
}

fn format_uuid_text(bytes: &[u8; 16]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(36);
    for (index, byte) in bytes.iter().copied().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            output.push('-');
        }
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn parse_uuid(value: &str) -> Result<[u8; 16], ApplicationClientError> {
    if value.len() != 36 {
        return Err(ApplicationClientError::InvalidInput);
    }
    let bytes = value.as_bytes();
    if [8, 13, 18, 23]
        .into_iter()
        .any(|index| bytes[index] != b'-')
    {
        return Err(ApplicationClientError::InvalidInput);
    }
    let mut output = [0_u8; 16];
    let mut encoded = bytes.iter().copied().filter(|byte| *byte != b'-');
    for byte in &mut output {
        let high = encoded
            .next()
            .and_then(hex_nibble)
            .ok_or(ApplicationClientError::InvalidInput)?;
        let low = encoded
            .next()
            .and_then(hex_nibble)
            .ok_or(ApplicationClientError::InvalidInput)?;
        *byte = (high << 4) | low;
    }
    encoded
        .next()
        .is_none()
        .then_some(output)
        .ok_or(ApplicationClientError::InvalidInput)
}

const fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_errors::ApplicationErrorCode;

    #[test]
    fn exact_tonic_endpoint_defaults_tcp_nodelay_on() {
        // tonic is exact-pinned at 0.14.6 by the workspace. This executable
        // assertion accompanies WP-632's source receipt so a dependency
        // change cannot silently reintroduce Nagle delay into the diagnosis.
        let endpoint = Endpoint::from_static("http://127.0.0.1:7443");
        assert!(endpoint.get_tcp_nodelay());
    }

    #[test]
    fn named_query_builder_is_name_addressed_and_module_pinned() {
        let mut parameters = BTreeMap::new();
        parameters.insert(
            "organization_id".to_owned(),
            ApplicationValue::String("01900000-0000-7000-8000-000000000001".to_owned()),
        );
        let query = NamedQuery::new(
            ApplicationContract::Exact {
                lineage: "TicketDesk".to_owned(),
                version: 1,
                bundle_hash: None,
            },
            "TicketPage",
            Some([7; 32]),
            parameters,
            None,
        )
        .expect("query");
        assert_eq!(query.name, "TicketPage");
        assert_eq!(query.module_hash, Some([7; 32]));
    }

    #[test]
    fn generated_query_options_preserve_cursor_and_read_fence() {
        let query = NamedQuery::new(
            ApplicationContract::Exact {
                lineage: "TicketDesk".to_owned(),
                version: 1,
                bundle_hash: Some([9; 32]),
            },
            "TicketPage",
            Some([7; 32]),
            BTreeMap::new(),
            None,
        )
        .expect("query")
        .with_options(
            QueryOptions::new()
                .after("opaque-cursor")
                .read_after_commit(41),
        )
        .expect("options");
        assert_eq!(query.cursor.as_deref(), Some("opaque-cursor"));
        assert_eq!(query.minimum_application_head, Some(41));
        assert!(
            NamedQuery::new(
                ApplicationContract::Active,
                "TicketPage",
                Some([7; 32]),
                BTreeMap::new(),
                None,
            )
            .expect("query")
            .with_options(QueryOptions::new().read_after_commit(0))
            .is_err()
        );
    }

    #[test]
    fn query_response_raises_only_names_and_typed_values() {
        let response = app_v1::ExecuteQueryResponse {
            identity: Some(app_v1::QueryIdentity {
                contract_lineage: "TicketDesk".to_owned(),
                contract_version: 1,
                contract_bundle_hash: vec![1; 32],
                query_name: Some("TicketPage".to_owned()),
                plan_hash: vec![2; 32],
                module_hash: Some(vec![3; 32]),
            }),
            outcome: "Found".to_owned(),
            application_head: 4,
            fields: vec![app_v1::ResultField {
                name: "ticket".to_owned(),
                cardinality: app_v1::ResultCardinality::One as i32,
                records: vec![app_v1::ResultRecord {
                    fields: vec![app_v1::Parameter {
                        name: "title".to_owned(),
                        value: Some(v1::Value {
                            kind: Some(v1::value::Kind::StringValue("Hello".to_owned())),
                        }),
                    }],
                    entity: "Ticket".to_owned(),
                }],
            }],
            next_cursor: None,
            selected_result_encoding: app_v1::NamedResultEncoding::LegacyRecords as i32,
            compact_result: None,
        };
        let result = raise_query_result(response).expect("result");
        assert_eq!(result.outcome, "Found");
        assert_eq!(result.identity.contract_lineage, "TicketDesk");
        assert_eq!(result.identity.contract_version, 1);
        assert_eq!(result.identity.contract_bundle_hash, [1; 32]);
        assert_eq!(result.identity.module_hash, [3; 32]);
        assert_eq!(result.identity.query_name, "TicketPage");
        assert_eq!(result.identity.plan_hash, [2; 32]);
        assert_eq!(
            result.fields["ticket"].records[0].fields["title"],
            ApplicationValue::String("Hello".to_owned())
        );
    }

    #[test]
    fn generated_identity_expectations_reject_contract_module_and_name_drift() {
        let response = app_v1::ExecuteQueryResponse {
            identity: Some(app_v1::QueryIdentity {
                contract_lineage: "TicketDesk".to_owned(),
                contract_version: 1,
                contract_bundle_hash: vec![1; 32],
                query_name: Some("TicketPage".to_owned()),
                plan_hash: vec![2; 32],
                module_hash: Some(vec![3; 32]),
            }),
            ..Default::default()
        };
        let contract = ApplicationContract::Exact {
            lineage: "TicketDesk".to_owned(),
            version: 1,
            bundle_hash: Some([1; 32]),
        };
        validate_query_response_identity(
            &response,
            &contract,
            "TicketPage",
            Some([3; 32]),
            Some([2; 32]),
        )
        .expect("exact identity");
        assert!(matches!(
            validate_query_response_identity(
                &response,
                &contract,
                "Other",
                Some([3; 32]),
                Some([2; 32])
            ),
            Err(ApplicationClientError::InvalidResponse)
        ));
        assert!(matches!(
            validate_query_response_identity(
                &response,
                &contract,
                "TicketPage",
                Some([4; 32]),
                Some([2; 32])
            ),
            Err(ApplicationClientError::InvalidResponse)
        ));
        assert!(matches!(
            validate_query_response_identity(
                &response,
                &contract,
                "TicketPage",
                Some([3; 32]),
                Some([4; 32])
            ),
            Err(ApplicationClientError::InvalidResponse)
        ));
        let changed_contract = ApplicationContract::Exact {
            lineage: "TicketDesk".to_owned(),
            version: 1,
            bundle_hash: Some([9; 32]),
        };
        assert!(matches!(
            validate_query_response_identity(
                &response,
                &changed_contract,
                "TicketPage",
                Some([3; 32]),
                Some([2; 32])
            ),
            Err(ApplicationClientError::InvalidResponse)
        ));
    }

    #[test]
    fn application_catalog_preflight_is_exact_closed_and_name_only() {
        let contract = ApplicationContract::Exact {
            lineage: "TicketDesk".to_owned(),
            version: 3,
            bundle_hash: Some([7; 32]),
        };
        let response = app_v1::GetApplicationCatalogResponse {
            schema: APPLICATION_CATALOG_SCHEMA_V1.to_owned(),
            contract_lineage: "TicketDesk".to_owned(),
            contract_version: 3,
            contract_bundle_hash: vec![7; 32],
            query_module_hashes: vec![vec![3; 32], vec![5; 32]],
            features: [
                app_v1::ApplicationCatalogFeature::OperationalOptionalPredicates,
                app_v1::ApplicationCatalogFeature::StableCursorPages,
                app_v1::ApplicationCatalogFeature::NullExistencePredicates,
                app_v1::ApplicationCatalogFeature::BinaryTextPrefix,
                app_v1::ApplicationCatalogFeature::UnicodeFoldTextPrefixV1,
                app_v1::ApplicationCatalogFeature::ExactAggregates,
            ]
            .into_iter()
            .map(|feature| app_v1::ApplicationCatalogFeatureView {
                feature: feature as i32,
                state: if feature == app_v1::ApplicationCatalogFeature::UnicodeFoldTextPrefixV1 {
                    app_v1::ApplicationCatalogFeatureState::Unavailable as i32
                } else {
                    app_v1::ApplicationCatalogFeatureState::Available as i32
                },
            })
            .collect(),
            ..Default::default()
        };

        let preflight =
            raise_application_catalog_preflight(response, &contract).expect("preflight");
        assert_eq!(preflight.contract_lineage, "TicketDesk");
        assert_eq!(preflight.contract_version, 3);
        assert_eq!(preflight.contract_bundle_hash, [7; 32]);
        assert_eq!(preflight.query_module_hashes, vec![[3; 32], [5; 32]]);
        assert!(preflight.is_available(ApplicationCatalogFeature::BinaryTextPrefix));
        assert!(!preflight.is_available(ApplicationCatalogFeature::UnicodeFoldTextPrefixV1));
    }

    #[test]
    fn application_catalog_preflight_rejects_identity_and_registry_drift() {
        let contract = ApplicationContract::Exact {
            lineage: "TicketDesk".to_owned(),
            version: 3,
            bundle_hash: Some([7; 32]),
        };
        let response = app_v1::GetApplicationCatalogResponse {
            schema: APPLICATION_CATALOG_SCHEMA_V1.to_owned(),
            contract_lineage: "TicketDesk".to_owned(),
            contract_version: 4,
            contract_bundle_hash: vec![7; 32],
            ..Default::default()
        };
        assert!(matches!(
            raise_application_catalog_preflight(response, &contract),
            Err(ApplicationClientError::InvalidResponse)
        ));

        let response = app_v1::GetApplicationCatalogResponse {
            schema: APPLICATION_CATALOG_SCHEMA_V1.to_owned(),
            contract_lineage: "TicketDesk".to_owned(),
            contract_version: 3,
            contract_bundle_hash: vec![7; 32],
            query_module_hashes: vec![vec![5; 32], vec![3; 32]],
            ..Default::default()
        };
        assert!(matches!(
            raise_application_catalog_preflight(response, &contract),
            Err(ApplicationClientError::InvalidResponse)
        ));
    }

    #[test]
    fn generated_batch_bounds_and_resume_checkpoint_are_closed() {
        let options = GeneratedBatchOptions::new(MAX_GENERATED_BATCH_CONCURRENCY)
            .expect("maximum concurrency")
            .with_checkpoint(4_096);
        options.validate(4_096).expect("complete checkpoint");
        assert_eq!(options.checkpoint, 4_096);

        assert_eq!(
            GeneratedBatchOptions::new(0),
            Err(GeneratedBatchError::InvalidBounds)
        );
        assert_eq!(
            GeneratedBatchOptions::new(MAX_GENERATED_BATCH_CONCURRENCY + 1),
            Err(GeneratedBatchError::InvalidBounds)
        );
        assert_eq!(
            GeneratedBatchOptions::new(1)
                .expect("options")
                .with_checkpoint(2)
                .validate(1),
            Err(GeneratedBatchError::InvalidBounds)
        );
        assert_eq!(
            GeneratedBatchOptions::new(1)
                .expect("options")
                .validate(4_097),
            Err(GeneratedBatchError::InvalidBounds)
        );
    }

    #[test]
    fn generated_transport_batches_never_exceed_the_item_concurrency_bound() {
        for item_concurrency in 1..=MAX_GENERATED_BATCH_CONCURRENCY {
            let (batch_size, transport_concurrency) =
                generated_transport_batch_policy(item_concurrency);
            assert!((1..=MAX_GENERATED_TRANSPORT_BATCH_ITEMS).contains(&batch_size));
            assert!(transport_concurrency > 0);
            assert!(batch_size * transport_concurrency <= item_concurrency);
        }
        assert_eq!(generated_transport_batch_policy(64), (16, 4));
        assert_eq!(generated_transport_batch_policy(128), (16, 8));
        assert_eq!(generated_transport_batch_policy(384), (16, 24));
        assert_eq!(generated_transport_batch_policy(17), (8, 2));
    }

    #[test]
    fn ordinary_transport_batch_bound_is_closed_before_submission() {
        assert_eq!(
            validate_idempotent_transport_batch_len(0),
            Err(IdempotentTransportBatchError::Empty)
        );
        assert_eq!(validate_idempotent_transport_batch_len(1), Ok(()));
        assert_eq!(
            validate_idempotent_transport_batch_len(MAX_GENERATED_TRANSPORT_BATCH_ITEMS),
            Ok(())
        );
        assert_eq!(
            validate_idempotent_transport_batch_len(MAX_GENERATED_TRANSPORT_BATCH_ITEMS + 1),
            Err(IdempotentTransportBatchError::TooManyItems)
        );
    }

    #[derive(Clone)]
    struct StubBatchCommand {
        outcome: &'static str,
        key: &'static str,
    }

    impl GeneratedCommand for StubBatchCommand {
        type Outcome = String;

        fn idempotent_command(
            &self,
        ) -> Result<crate::IdempotentCommand, crate::generated::GeneratedCommandError> {
            crate::IdempotentCommand::new(
                "OkCommand",
                Some(1),
                v1::Value {
                    kind: Some(v1::value::Kind::RecordValue(v1::ValueRecord {
                        fields: vec![v1::ValueField {
                            field_id: Some(1),
                            name: String::new(),
                            value: Some(v1::Value {
                                kind: Some(v1::value::Kind::StringValue(self.key.to_owned())),
                            }),
                        }],
                    })),
                },
            )
            .map_err(|_| crate::generated::GeneratedCommandError::InvalidInputShape)
        }

        fn outcome_request(
            &self,
            request_id: riffdb_types::RequestId,
        ) -> Result<v1::GetOutcomeRequest, crate::generated::GeneratedCommandError> {
            Ok(v1::GetOutcomeRequest {
                request_id: request_id.into_bytes().to_vec(),
                contract_lineage: "lineage".to_owned(),
                command_name: "OkCommand".to_owned(),
                idempotency_key: self.key.to_owned(),
                outcome_uri: None,
            })
        }

        fn decode_outcome(
            &self,
            response: &v1::ExecuteCommandResponse,
        ) -> Result<Self::Outcome, crate::generated::GeneratedCommandError> {
            if response.outcome_type == self.outcome {
                Ok(self.outcome.to_owned())
            } else {
                Err(crate::generated::GeneratedCommandError::InvalidOutcomeShape)
            }
        }

        fn workflow_successor_revisions(
            &self,
            response: &v1::ExecuteCommandResponse,
        ) -> Result<
            Vec<crate::generated::WorkflowSuccessorRevision>,
            crate::generated::GeneratedCommandError,
        > {
            if response.outcome_type == "Workflow" {
                Ok(vec![
                    crate::generated::WorkflowSuccessorRevision::generated("work", 8),
                ])
            } else {
                Ok(Vec::new())
            }
        }
    }

    fn stub_command(outcome: &'static str, key: &'static str) -> StubBatchCommand {
        StubBatchCommand { outcome, key }
    }

    fn stub_response(outcome: &str) -> v1::ExecuteCommandResponse {
        v1::ExecuteCommandResponse {
            status: v1::execute_command_response::CompletionStatus::ExecutedReadOnly as i32,
            commit_sequence: 0,
            contract_version: 1,
            plan_hash: vec![0x44; 32],
            outcome_type: outcome.to_owned(),
            outcome: Some(v1::Value {
                kind: Some(v1::value::Kind::NullValue(v1::NullValue::NullValue as i32)),
            }),
            provenance_uri: String::new(),
            durability_mode: String::new(),
            outcome_uri: None,
            history_incarnation: 1,
        }
    }

    #[test]
    fn typed_command_results_preserve_generated_workflow_revision_evidence() {
        let command = stub_command("Workflow", "workflow-key");
        let response = stub_response("Workflow");
        let outcome = command.decode_outcome(&response).expect("outcome");
        let result = typed_command_result(&command, outcome, response).expect("typed result");

        assert_eq!(result.workflow_revisions.len(), 1);
        assert_eq!(result.workflow_revisions[0].binding(), "work");
        assert_eq!(result.workflow_revisions[0].revision(), 8);
    }

    fn stub_error_item(code: ApplicationErrorCode) -> v1::ExecuteCommandBatchItem {
        v1::ExecuteCommandBatchItem {
            result: Some(v1::execute_command_batch_item::Result::Error(
                riffdb_proto::application_error_to_proto(&ApplicationError::new(
                    code,
                    ApplicationOperation::BatchCommand,
                    ApplicationErrorContext::empty(),
                    None,
                )),
            )),
        }
    }

    fn application_overloaded() -> ClientError {
        ClientError::Application(Box::new(ApplicationError::new(
            ApplicationErrorCode::Overloaded,
            ApplicationOperation::BatchCommand,
            ApplicationErrorContext::empty(),
            None,
        )))
    }

    fn block_on_with_time<T>(future: impl std::future::Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("test runtime")
            .block_on(future)
    }

    /// Scripted ExecuteRetryAttempt mirror of client.rs tests::ExecuteAttempts.
    struct ScriptedExecuteAttempts {
        results: std::collections::VecDeque<Result<v1::ExecuteCommandResponse, ClientError>>,
        requests: Vec<v1::ExecuteCommandRequest>,
    }

    impl ScriptedExecuteAttempts {
        fn new(
            results: impl IntoIterator<Item = Result<v1::ExecuteCommandResponse, ClientError>>,
        ) -> Self {
            Self {
                results: results.into_iter().collect(),
                requests: Vec::new(),
            }
        }
    }

    impl crate::client::ExecuteRetryAttempt for ScriptedExecuteAttempts {
        async fn submit_execute_attempt(
            &mut self,
            request: v1::ExecuteCommandRequest,
            _metadata: &CallMetadata,
        ) -> Result<v1::ExecuteCommandResponse, ClientError> {
            self.requests.push(request);
            self.results.pop_front().expect("scripted execute result")
        }
    }

    struct FixedRequestIds {
        values: std::collections::VecDeque<riffdb_types::RequestId>,
    }

    impl crate::client::RetryRequestIdSource for FixedRequestIds {
        fn next_request_id(
            &mut self,
        ) -> Result<riffdb_types::RequestId, crate::IdentifierGenerationError> {
            self.values
                .pop_front()
                .ok_or(crate::IdentifierGenerationError::EntropyUnavailable)
        }
    }

    fn request_id(ordinal: u8) -> riffdb_types::RequestId {
        riffdb_types::RequestId::from_unix_milliseconds_and_random(
            u64::from(ordinal),
            [ordinal; 10],
        )
        .expect("request id")
    }

    #[test]
    fn batch_items_non_retryable_errors_surface_without_reentry_candidates() {
        let prepared = vec![
            (0, stub_command("Ok", "k0")),
            (1, stub_command("Ok", "k1")),
            (2, stub_command("Ok", "k2")),
        ];
        let items = vec![
            v1::ExecuteCommandBatchItem {
                result: Some(v1::execute_command_batch_item::Result::Response(
                    stub_response("Ok"),
                )),
            },
            stub_error_item(ApplicationErrorCode::InputInvalid),
            stub_error_item(ApplicationErrorCode::AuthorizationDenied),
        ];
        let (resolved, reenter) = resolve_generated_batch_items(prepared, items);
        assert!(reenter.is_empty(), "non-retryable errors must not re-enter");
        assert_eq!(resolved.len(), 3);
        assert!(resolved[0].1.is_ok());
        assert!(matches!(
            &resolved[1].1,
            Err(GeneratedExecutionError::Client(ClientError::Application(error)))
                if error.code() == ApplicationErrorCode::InputInvalid
        ));
        assert!(matches!(
            &resolved[2].1,
            Err(GeneratedExecutionError::Client(ClientError::Application(error)))
                if error.code() == ApplicationErrorCode::AuthorizationDenied
        ));
        assert_eq!(
            resolved.iter().map(|(index, _)| *index).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
    }

    #[test]
    fn batch_items_registry_retryable_errors_are_reentry_candidates() {
        let prepared = vec![
            (0, stub_command("A", "k0")),
            (1, stub_command("B", "k1")),
            (2, stub_command("C", "k2")),
            (3, stub_command("D", "k3")),
        ];
        let items = vec![
            v1::ExecuteCommandBatchItem {
                result: Some(v1::execute_command_batch_item::Result::Response(
                    stub_response("A"),
                )),
            },
            stub_error_item(ApplicationErrorCode::OutcomeUnknown),
            stub_error_item(ApplicationErrorCode::Overloaded),
            stub_error_item(ApplicationErrorCode::StorageUnavailable),
        ];
        let (resolved, reenter) = resolve_generated_batch_items(prepared, items);
        assert_eq!(resolved.len(), 1);
        assert!(resolved[0].1.is_ok());
        assert_eq!(
            reenter
                .iter()
                .map(|candidate| candidate.index)
                .collect::<Vec<_>>(),
            vec![1, 2, 3],
            "Retry and ResolveWithSameIdempotencyKey must re-enter"
        );
        assert!(reenter.iter().all(|c| c.protocol_failure.is_none()));
        assert_eq!(
            ApplicationErrorCode::Overloaded.recovery_action(),
            ApplicationRecoveryAction::Retry
        );
        assert_eq!(
            ApplicationErrorCode::OutcomeUnknown.recovery_action(),
            ApplicationRecoveryAction::ResolveWithSameIdempotencyKey
        );
    }

    #[test]
    fn batch_items_corrupt_or_unset_error_arms_reenter() {
        let prepared = vec![
            (0, stub_command("A", "k0")),
            (1, stub_command("B", "k1")),
            (2, stub_command("C", "k2")),
        ];
        let corrupt = v1::ExecuteCommandBatchItem {
            result: Some(v1::execute_command_batch_item::Result::Error(
                app_v1::ApplicationError {
                    envelope_version: 0,
                    code: 0,
                    category: 0,
                    recovery_action: 0,
                    operation: 0,
                    ..Default::default()
                },
            )),
        };
        let items = vec![
            v1::ExecuteCommandBatchItem {
                result: Some(v1::execute_command_batch_item::Result::Response(
                    stub_response("A"),
                )),
            },
            corrupt,
            v1::ExecuteCommandBatchItem { result: None },
        ];
        let (resolved, reenter) = resolve_generated_batch_items(prepared, items);
        assert_eq!(resolved.len(), 1);
        assert_eq!(
            reenter
                .iter()
                .map(|candidate| candidate.index)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            reenter[0].protocol_failure.map(|f| f.kind()),
            Some(crate::ProtocolFailureKind::InvalidApplicationErrorDetails)
        );
        assert_eq!(
            reenter[1].protocol_failure.map(|f| f.kind()),
            Some(crate::ProtocolFailureKind::InvalidInboundMessage)
        );
    }

    #[test]
    fn complete_batch_items_retries_all_overloaded_through_real_budget_and_succeeds() {
        use std::collections::HashMap;
        use std::sync::{Arc, Mutex};

        let prepared = vec![
            (0, stub_command("Ok", "k0")),
            (1, stub_command("Ok", "k1")),
            (2, stub_command("Ok", "k2")),
        ];
        let items = vec![
            stub_error_item(ApplicationErrorCode::Overloaded),
            stub_error_item(ApplicationErrorCode::Overloaded),
            stub_error_item(ApplicationErrorCode::Overloaded),
        ];
        let attempt_counts: Arc<Mutex<HashMap<usize, usize>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let recovered = block_on_with_time(complete_batch_items_with_reentry(
            prepared,
            items,
            |index, command| {
                let attempt_counts = Arc::clone(&attempt_counts);
                async move {
                    let generic = command.idempotent_command().expect("stub command shape");
                    let success = stub_response(command.outcome);
                    let mut attempts = ScriptedExecuteAttempts::new([
                        Err(application_overloaded()),
                        Ok(success.clone()),
                    ]);
                    let mut ids = FixedRequestIds {
                        values: [request_id(10 + index as u8), request_id(40 + index as u8)]
                            .into_iter()
                            .collect(),
                    };
                    let response = crate::client::execute_retry_attempts(
                        &mut attempts,
                        &generic,
                        AttemptBudget::new(3).expect("budget"),
                        &CallMetadata::default(),
                        &mut ids,
                    )
                    .await
                    .expect("capacity frees on second attempt");
                    attempt_counts
                        .lock()
                        .expect("counts")
                        .insert(index, attempts.requests.len());
                    let outcome = command
                        .decode_outcome(&response)
                        .map_err(GeneratedExecutionError::CommandShape)
                        .and_then(|outcome| typed_command_result(&command, outcome, response));
                    (index, outcome)
                }
            },
        ));
        assert_eq!(recovered.len(), 3);
        assert!(recovered.iter().all(|(_, result)| result.is_ok()));
        let counts = attempt_counts.lock().expect("counts");
        assert_eq!(counts.len(), 3);
        assert!(
            counts.values().all(|count| *count > 1),
            "mock must record multi-attempt real RetryState path: {counts:?}"
        );
    }

    #[test]
    fn complete_batch_items_real_budget_exhaustion_surfaces_err_while_siblings_succeed() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let prepared = vec![
            (0, stub_command("Ok", "k0")),
            (1, stub_command("Fail", "k1")),
            (2, stub_command("Ok", "k2")),
        ];
        let items = vec![
            v1::ExecuteCommandBatchItem {
                result: Some(v1::execute_command_batch_item::Result::Response(
                    stub_response("Ok"),
                )),
            },
            stub_error_item(ApplicationErrorCode::Overloaded),
            v1::ExecuteCommandBatchItem {
                result: Some(v1::execute_command_batch_item::Result::Response(
                    stub_response("Ok"),
                )),
            },
        ];
        let reenter_attempts = Arc::new(AtomicUsize::new(0));
        let mut recovered = block_on_with_time(complete_batch_items_with_reentry(
            prepared,
            items,
            |index, command| {
                let reenter_attempts = Arc::clone(&reenter_attempts);
                async move {
                    let generic = command.idempotent_command().expect("stub command shape");
                    // Budget of 2: both submissions fail Overloaded → Return after
                    // exactly two real execute_retry_attempts submissions.
                    let mut attempts = ScriptedExecuteAttempts::new([
                        Err(application_overloaded()),
                        Err(application_overloaded()),
                    ]);
                    let mut ids = FixedRequestIds {
                        values: [request_id(20), request_id(21)].into_iter().collect(),
                    };
                    let budget = AttemptBudget::new(2).expect("budget");
                    let error = crate::client::execute_retry_attempts(
                        &mut attempts,
                        &generic,
                        budget,
                        &CallMetadata::default(),
                        &mut ids,
                    )
                    .await
                    .expect_err("budget exhausted");
                    reenter_attempts.store(attempts.requests.len(), Ordering::SeqCst);
                    assert_eq!(
                        attempts.requests.len(),
                        budget.maximum_submissions() as usize
                    );
                    (index, Err(GeneratedExecutionError::Client(error)))
                }
            },
        ));
        recovered.sort_by_key(|(index, _)| *index);
        assert_eq!(reenter_attempts.load(Ordering::SeqCst), 2);
        assert!(recovered[0].1.is_ok());
        assert!(matches!(
            &recovered[1].1,
            Err(GeneratedExecutionError::Client(ClientError::Application(error)))
                if error.code() == ApplicationErrorCode::Overloaded
        ));
        assert!(recovered[2].1.is_ok());
    }

    #[test]
    fn complete_batch_items_corrupt_error_arm_reenters_and_can_succeed() {
        let prepared = vec![
            (0, stub_command("Ok", "k0")),
            (1, stub_command("Replay", "k1")),
        ];
        let items = vec![
            v1::ExecuteCommandBatchItem {
                result: Some(v1::execute_command_batch_item::Result::Response(
                    stub_response("Ok"),
                )),
            },
            v1::ExecuteCommandBatchItem {
                result: Some(v1::execute_command_batch_item::Result::Error(
                    app_v1::ApplicationError {
                        envelope_version: 0,
                        code: 0,
                        category: 0,
                        recovery_action: 0,
                        operation: 0,
                        ..Default::default()
                    },
                )),
            },
        ];
        let mut recovered = block_on_with_time(complete_batch_items_with_reentry(
            prepared,
            items,
            |index, command| async move {
                (
                    index,
                    Ok(TypedCommandResult {
                        outcome: command.outcome.to_owned(),
                        commit_sequence: None,
                        contract_version: 1,
                        plan_hash: [0x55; 32],
                        replayed: true,
                        outcome_uri: None,
                        workflow_revisions: Vec::new(),
                    }),
                )
            },
        ));
        recovered.sort_by_key(|(index, _)| *index);
        assert_eq!(recovered.len(), 2);
        assert!(recovered[0].1.is_ok());
        assert_eq!(recovered[1].1.as_ref().expect("replayed").outcome, "Replay");
    }

    #[test]
    fn complete_batch_items_corrupt_error_arm_surfaces_protocol_failure_on_reentry_exhaustion() {
        let prepared = vec![(0, stub_command("Replay", "k0"))];
        let items = vec![v1::ExecuteCommandBatchItem {
            result: Some(v1::execute_command_batch_item::Result::Error(
                app_v1::ApplicationError {
                    envelope_version: 0,
                    code: 0,
                    category: 0,
                    recovery_action: 0,
                    operation: 0,
                    ..Default::default()
                },
            )),
        }];
        let recovered = block_on_with_time(complete_batch_items_with_reentry(
            prepared,
            items,
            |index, _command| async move {
                // Re-entry itself fails (budget exhausted / still unknown).
                (
                    index,
                    Err(GeneratedExecutionError::Client(ClientError::Application(
                        Box::new(ApplicationError::new(
                            ApplicationErrorCode::Overloaded,
                            ApplicationOperation::BatchCommand,
                            ApplicationErrorContext::empty(),
                            None,
                        )),
                    ))),
                )
            },
        ));
        assert_eq!(recovered.len(), 1);
        assert!(matches!(
            &recovered[0].1,
            Err(GeneratedExecutionError::Client(ClientError::Protocol(failure)))
                if failure.kind() == crate::ProtocolFailureKind::InvalidApplicationErrorDetails
        ));
    }

    #[test]
    fn record_generated_batch_item_completion_advances_checkpoint_in_production_order() {
        // Drive the production completion recorder (used by
        // execute_generated_command_batch_with_progress) with mixed Ok/Err and
        // out-of-order arrivals.
        let mut items = Vec::new();
        let mut state = BatchCheckpointState::new(0);
        let mut progress = Vec::new();
        let total = 3usize;

        let ok = |label: &str| {
            Ok(TypedCommandResult {
                outcome: label.to_owned(),
                commit_sequence: None,
                contract_version: 1,
                plan_hash: [0x11; 32],
                replayed: false,
                outcome_uri: None,
                workflow_revisions: Vec::new(),
            })
        };
        let err = Err(GeneratedExecutionError::Client(ClientError::Application(
            Box::new(ApplicationError::new(
                ApplicationErrorCode::InputInvalid,
                ApplicationOperation::BatchCommand,
                ApplicationErrorContext::empty(),
                None,
            )),
        )));

        // Out of order: 2, then 0, then 1(Err). Checkpoint stays 0 until 0 lands,
        // then 1, then 3 once the Err at 1 closes the prefix.
        record_generated_batch_item_completion(
            2,
            ok("two"),
            total,
            &mut state,
            &mut items,
            &mut |p| progress.push(p.checkpoint),
        );
        assert_eq!(state.checkpoint, 0);
        record_generated_batch_item_completion(
            0,
            ok("zero"),
            total,
            &mut state,
            &mut items,
            &mut |p| progress.push(p.checkpoint),
        );
        assert_eq!(state.checkpoint, 1);
        record_generated_batch_item_completion(1, err, total, &mut state, &mut items, &mut |p| {
            progress.push(p.checkpoint)
        });
        assert_eq!(state.checkpoint, 3);
        assert_eq!(progress, vec![0, 1, 3]);
        assert_eq!(items.len(), 3);
        assert!(
            items
                .iter()
                .any(|item| item.index == 1 && item.result.is_err())
        );
    }

    #[test]
    fn record_generated_batch_item_completion_respects_resumed_checkpoint_seed() {
        let mut items = Vec::new();
        // Production seeds both completed and checkpoint from options.checkpoint.
        let mut state = BatchCheckpointState::new(2);
        let mut progress = Vec::new();
        let total = 4usize;
        let ok = |label: &str| {
            Ok(TypedCommandResult {
                outcome: label.to_owned(),
                commit_sequence: None,
                contract_version: 1,
                plan_hash: [0x22; 32],
                replayed: false,
                outcome_uri: None,
                workflow_revisions: Vec::new(),
            })
        };
        record_generated_batch_item_completion(
            2,
            ok("two"),
            total,
            &mut state,
            &mut items,
            &mut |p| progress.push((p.completed, p.checkpoint)),
        );
        record_generated_batch_item_completion(
            3,
            ok("three"),
            total,
            &mut state,
            &mut items,
            &mut |p| progress.push((p.completed, p.checkpoint)),
        );
        assert_eq!(state.checkpoint, 4);
        assert_eq!(state.completed, 4);
        assert_eq!(progress, vec![(3, 3), (4, 4)]);
    }

    /// R3d: raised UUID bytes equal the text path after parse, without forcing
    /// text materialization on raise.
    #[test]
    fn raised_uuid_bytes_match_parsed_text_and_text_is_lazy() {
        let bytes = [
            0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab,
            0xcd, 0xef,
        ];
        let raised = raise_value(v1::Value {
            kind: Some(v1::value::Kind::UuidValue(bytes.to_vec())),
        })
        .expect("raise uuid");
        let ApplicationValue::Uuid(uuid) = raised else {
            panic!("expected uuid value");
        };
        assert!(!uuid.text_is_materialized(), "raise must not allocate text");
        assert_eq!(uuid.as_bytes(), &bytes);
        let text = uuid.as_str().to_owned();
        assert!(uuid.text_is_materialized());
        let from_text = ApplicationUuid::from_text(text.clone()).expect("parse text");
        assert_eq!(from_text.as_bytes(), &bytes);
        assert_eq!(from_text.into_string(), text);
        // Adapter-equivalence: bytes path and re-parsed text path agree.
        let re_lowered = lower_value(ApplicationValue::Uuid(ApplicationUuid::from_bytes(bytes)))
            .expect("lower bytes");
        let text_lowered = lower_value(ApplicationValue::Uuid(
            ApplicationUuid::from_text(text).expect("text"),
        ))
        .expect("lower text");
        assert_eq!(re_lowered, text_lowered);
    }

    #[test]
    fn canonical_vector_values_round_trip_without_punning() {
        let vector =
            riffdb_types::CanonicalVector::new(vec![-0.0, 1.5, -2.25]).expect("canonical vector");
        let lowered = lower_value(ApplicationValue::Vector(vector.clone())).expect("lower");
        let Some(v1::value::Kind::VectorValue(wire)) = lowered.kind.as_ref() else {
            panic!("expected the dedicated vector wire arm");
        };
        assert_eq!(wire.components, vector.components());
        assert_eq!(
            raise_value(lowered).expect("raise"),
            ApplicationValue::Vector(vector)
        );
    }

    #[test]
    fn non_finite_vector_response_is_rejected() {
        let result = raise_value(v1::Value {
            kind: Some(v1::value::Kind::VectorValue(v1::VectorValue {
                components: vec![f32::NAN],
            })),
        });
        assert!(matches!(
            result,
            Err(ApplicationClientError::InvalidResponse)
        ));
    }
}
