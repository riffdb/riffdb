//! Name-addressed application requests that hide the kernel wire model.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
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

const MAX_GENERATED_TRANSPORT_BATCH_ITEMS: usize = 16;
const MAX_GENERATED_BATCH_CONCURRENCY: usize = 128;

/// Application-only client facade.
///
/// This type deliberately has no accessor for its kernel client. Stable
/// application code can execute name-addressed commands and exact named
/// queries, but cannot construct raw entity/index requests through this
/// surface.
#[derive(Clone)]
pub struct StableApplicationClient {
    inner: RiffDbClient,
}

impl StableApplicationClient {
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

    /// Executes one exact named module query.
    pub async fn execute_named_query(
        &mut self,
        query: NamedQuery,
        metadata: &CallMetadata,
    ) -> Result<NamedQueryResult, ApplicationClientError> {
        self.inner
            .execute_named_application_query(query, metadata)
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
        let result = self.execute_named_query(query, metadata).await?;
        let application_head = result.application_head;
        let next_cursor = result.next_cursor.clone();
        let identity = result.identity.clone();
        let value = Q::decode_result(result)?;
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
        let mut completed = options.checkpoint;
        let mut checkpoint = options.checkpoint;
        let mut completed_after_checkpoint = BTreeSet::new();
        while let Some(completed_chunk) = pending.next().await {
            for (index, result) in completed_chunk {
                completed += 1;
                completed_after_checkpoint.insert(index);
                while completed_after_checkpoint.remove(&checkpoint) {
                    checkpoint += 1;
                }
                items.push(GeneratedBatchItem { index, result });
                report_progress(GeneratedBatchProgress {
                    completed,
                    total,
                    checkpoint,
                });
            }
        }
        items.sort_by_key(|item| item.index);
        Ok(GeneratedBatchResult { items, checkpoint })
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

        // Prefer per-item carriage when present (ADR-0077). Older servers leave
        // items empty and only populate legacy success rows.
        if !response.items.is_empty() {
            let (resolved, reenter) = resolve_generated_batch_items(prepared, response.items);
            completed.extend(resolved);
            if !reenter.is_empty() {
                let recovered: Vec<_> =
                    stream::iter(reenter.into_iter().map(|(index, command)| {
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
            }
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
                    outcome.and_then(|outcome| typed_command_result(outcome, response)),
                )
            },
        ));
        completed
    }
}

type ResolvedBatchItem<T> = (
    usize,
    Result<TypedCommandResult<T>, GeneratedExecutionError>,
);
type BatchItemResolution<C> = (
    Vec<ResolvedBatchItem<<C as GeneratedCommand>::Outcome>>,
    Vec<(usize, C)>,
);

/// Classifies per-item batch carriage into terminal results and re-entry candidates.
///
/// Re-entry uses the registry-derived recovery action, not a hand-listed code set:
/// [`ApplicationRecoveryAction::Retry`] and
/// [`ApplicationRecoveryAction::ResolveWithSameIdempotencyKey`] re-enter
/// `execute_generated_command` (same idempotency key; its AttemptBudget and
/// Overloaded backoff then apply). Every other recovery action is terminal and
/// surfaces directly with zero re-entry.
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
                    outcome.and_then(|outcome| typed_command_result(outcome, item_response)),
                ));
            }
            Some(v1::execute_command_batch_item::Result::Error(error_wire)) => {
                match application_error_from_proto(&error_wire) {
                    Ok(error) if application_error_requires_reentry(&error) => {
                        reenter.push((index, command));
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
                        resolved.push((
                            index,
                            Err(GeneratedExecutionError::Client(ClientError::Protocol(
                                crate::ProtocolFailure::new(
                                    crate::ProtocolFailureKind::InvalidApplicationErrorDetails,
                                ),
                            ))),
                        ));
                    }
                }
            }
            None => {
                resolved.push((
                    index,
                    Err(GeneratedExecutionError::Client(ClientError::Protocol(
                        crate::ProtocolFailure::new(
                            crate::ProtocolFailureKind::InvalidInboundMessage,
                        ),
                    ))),
                ));
            }
        }
    }
    (resolved, reenter)
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

fn typed_command_result<T>(
    outcome: T,
    response: v1::ExecuteCommandResponse,
) -> Result<TypedCommandResult<T>, GeneratedExecutionError> {
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
    /// Canonical UUID text lowered to the typed public value.
    Uuid(String),
    /// Contract enum variant name lowered without caller-visible numeric IDs.
    Enum(String),
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
    /// Ordered bounded values.
    List(Vec<Self>),
    /// Name-addressed record.
    Record(BTreeMap<String, Self>),
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
        let expected_contract = query.contract.clone();
        let expected_name = query.name.clone();
        let expected_module_hash = query.module_hash;
        let expected_plan_hash = query.expected_plan_hash;
        let request_id = generate_request_id()
            .map_err(|_| ApplicationClientError::IdentifierUnavailable)?
            .into_bytes()
            .to_vec();
        let parameters = query
            .parameters
            .into_iter()
            .map(|(name, value)| {
                Ok(app_v1::Parameter {
                    name,
                    value: Some(lower_value(value)?),
                })
            })
            .collect::<Result<Vec<_>, ApplicationClientError>>()?;
        let response = self
            .execute_query(
                app_v1::ExecuteQueryRequest {
                    contract: lower_contract(query.contract),
                    query: Some(app_v1::execute_query_request::Query::QueryName(query.name)),
                    module_hash: query.module_hash.map(|hash| hash.to_vec()),
                    parameters,
                    cursor: query.cursor,
                    minimum_application_head: query.minimum_application_head,
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
        raise_query_result(response)
    }

    /// Executes one symbolic command with bounded retry and no kernel-shaped caller input.
    pub async fn execute_application_command(
        &mut self,
        command: ApplicationCommand,
        attempts: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<ApplicationCommandResult, ApplicationClientError> {
        let command_name = command.name.clone();
        let input = lower_value(ApplicationValue::Record(command.input))?;
        let command =
            IdempotentCommand::new(command.name, command.expected_contract_version, input)
                .map_err(|_| ApplicationClientError::InvalidInput)?;
        let response = self
            .execute_with_retry(&command, attempts, metadata)
            .await
            .map_err(|error| contextualize_command_client_error(error, &command_name))?;
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

fn validate_contract(contract: &ApplicationContract) -> Result<(), ApplicationClientError> {
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

fn lower_value(value: ApplicationValue) -> Result<v1::Value, ApplicationClientError> {
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
        ApplicationValue::Uuid(value) => Kind::UuidValue(parse_uuid(&value)?.to_vec()),
        ApplicationValue::Enum(name) if !name.is_empty() && name.len() <= 256 => {
            Kind::EnumValue(v1::EnumValue {
                type_id: 0,
                variant_id: 0,
                name,
            })
        }
        ApplicationValue::Enum(_) => return Err(ApplicationClientError::InvalidInput),
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

fn raise_query_result(
    response: app_v1::ExecuteQueryResponse,
) -> Result<NamedQueryResult, ApplicationClientError> {
    let identity = response
        .identity
        .ok_or(ApplicationClientError::InvalidResponse)?;
    let module_hash: [u8; 32] = identity
        .module_hash
        .ok_or(ApplicationClientError::InvalidResponse)?
        .try_into()
        .map_err(|_| ApplicationClientError::InvalidResponse)?;
    let contract_bundle_hash = identity
        .contract_bundle_hash
        .try_into()
        .map_err(|_| ApplicationClientError::InvalidResponse)?;
    let plan_hash = identity
        .plan_hash
        .try_into()
        .map_err(|_| ApplicationClientError::InvalidResponse)?;
    let query_name = identity
        .query_name
        .ok_or(ApplicationClientError::InvalidResponse)?;
    let mut fields = BTreeMap::new();
    for field in response.fields {
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
    Ok(NamedQueryResult {
        outcome: response.outcome,
        application_head: response.application_head,
        fields,
        next_cursor: response.next_cursor,
        identity: QueryResponseIdentity {
            contract_lineage: identity.contract_lineage,
            contract_version: identity.contract_version,
            contract_bundle_hash,
            module_hash,
            query_name,
            plan_hash,
        },
    })
}

fn raise_value(value: v1::Value) -> Result<ApplicationValue, ApplicationClientError> {
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
        Kind::UuidValue(value) => Ok(ApplicationValue::Uuid(uuid_text(&value)?)),
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
        Kind::TimestampValue(_) | Kind::EnumValue(_) => {
            Err(ApplicationClientError::InvalidResponse)
        }
    }
}

fn uuid_text(bytes: &[u8]) -> Result<String, ApplicationClientError> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| ApplicationClientError::InvalidResponse)?;
    Ok(format!(
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
    ))
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
    fn generated_batch_bounds_and_resume_checkpoint_are_closed() {
        let options = GeneratedBatchOptions::new(128)
            .expect("maximum concurrency")
            .with_checkpoint(4_096);
        options.validate(4_096).expect("complete checkpoint");
        assert_eq!(options.checkpoint, 4_096);

        assert_eq!(
            GeneratedBatchOptions::new(0),
            Err(GeneratedBatchError::InvalidBounds)
        );
        assert_eq!(
            GeneratedBatchOptions::new(129),
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
        assert_eq!(generated_transport_batch_policy(17), (8, 2));
    }

    #[derive(Clone)]
    struct StubBatchCommand {
        outcome: &'static str,
    }

    impl GeneratedCommand for StubBatchCommand {
        type Outcome = String;

        fn idempotent_command(
            &self,
        ) -> Result<crate::IdempotentCommand, crate::generated::GeneratedCommandError> {
            Err(crate::generated::GeneratedCommandError::InvalidInputShape)
        }

        fn outcome_request(
            &self,
            _request_id: riffdb_types::RequestId,
        ) -> Result<v1::GetOutcomeRequest, crate::generated::GeneratedCommandError> {
            Err(crate::generated::GeneratedCommandError::InvalidInputShape)
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

    #[test]
    fn batch_items_non_retryable_errors_surface_without_reentry_candidates() {
        let prepared = vec![
            (0, StubBatchCommand { outcome: "Ok" }),
            (1, StubBatchCommand { outcome: "Ok" }),
            (2, StubBatchCommand { outcome: "Ok" }),
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
            (0, StubBatchCommand { outcome: "A" }),
            (1, StubBatchCommand { outcome: "B" }),
            (2, StubBatchCommand { outcome: "C" }),
            (3, StubBatchCommand { outcome: "D" }),
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
            reenter.iter().map(|(index, _)| *index).collect::<Vec<_>>(),
            vec![1, 2, 3],
            "Retry and ResolveWithSameIdempotencyKey must re-enter"
        );
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
    fn batch_checkpoint_advances_over_terminal_results_including_errors() {
        // Checkpoint is the largest contiguous prefix with any independent
        // terminal result (Ok or Err). Retryable items re-enter before a
        // terminal result is recorded, so they do not advance the checkpoint
        // until the attempt budget finishes. Terminal InputInvalid results
        // are independent results and correctly advance the checkpoint —
        // resume must not resubmit them.
        let mut completed_after_checkpoint = BTreeSet::new();
        let mut checkpoint = 0usize;
        // indices 0 Ok, 1 terminal InputInvalid, 2 Ok
        for index in [0usize, 1, 2] {
            completed_after_checkpoint.insert(index);
            while completed_after_checkpoint.remove(&checkpoint) {
                checkpoint += 1;
            }
        }
        assert_eq!(checkpoint, 3);
        // Gap at 1 leaves checkpoint at 1 (items 0 done, 1 missing).
        let mut completed_after_checkpoint = BTreeSet::new();
        let mut checkpoint = 0usize;
        for index in [0usize, 2] {
            completed_after_checkpoint.insert(index);
            while completed_after_checkpoint.remove(&checkpoint) {
                checkpoint += 1;
            }
        }
        assert_eq!(checkpoint, 1);
    }
}
