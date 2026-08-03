//! Application-safe durable consumer and live-query transport shapes.

use std::collections::BTreeMap;
use std::marker::PhantomData;

use crate::application::{
    lower_value, raise_application_command_result, raise_value, typed_command_result,
};
use crate::generated::{GeneratedCommand, GeneratedEventConsumer, GeneratedLiveQuery};
use crate::{
    ApplicationCardinality, ApplicationClientError, ApplicationCommand, ApplicationCommandResult,
    ApplicationRecord, ApplicationResultField, ApplicationValue, CallMetadata,
    EventConsumerResponseStream, GeneratedExecutionError, LiveQueryUpdateStream, NamedQueryResult,
    QueryResponseIdentity, StableApplicationClient, TypedCommandResult, generate_request_id, v1,
};

/// Exact immutable reactive operation selected by generated code.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationReactiveOperation {
    module_hash: [u8; 32],
    operation_name: String,
    parameters: BTreeMap<String, ApplicationValue>,
}

impl ApplicationReactiveOperation {
    /// Constructs one checked application-only operation selection.
    pub fn new(
        module_hash: [u8; 32],
        operation_name: impl Into<String>,
        parameters: BTreeMap<String, ApplicationValue>,
    ) -> Result<Self, ApplicationClientError> {
        let operation_name = operation_name.into();
        if operation_name.is_empty()
            || operation_name.len() > 256
            || parameters.len() > 256
            || parameters
                .keys()
                .any(|name| name.is_empty() || name.len() > 256)
        {
            return Err(ApplicationClientError::InvalidInput);
        }
        Ok(Self {
            module_hash,
            operation_name,
            parameters,
        })
    }

    fn lowered_parameters(
        &self,
    ) -> Result<Vec<v1::EventConsumerParameter>, ApplicationClientError> {
        self.parameters
            .iter()
            .map(|(name, value)| {
                Ok(v1::EventConsumerParameter {
                    name: name.clone(),
                    value: Some(lower_value(value.clone())?),
                })
            })
            .collect()
    }

    fn live_parameters(&self) -> Result<Vec<v1::LiveQueryParameter>, ApplicationClientError> {
        self.parameters
            .iter()
            .map(|(name, value)| {
                Ok(v1::LiveQueryParameter {
                    name: name.clone(),
                    value: Some(lower_value(value.clone())?),
                })
            })
            .collect()
    }
}

/// Checked durable consumer identity including the bounded caller-owned name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationEventConsumer {
    operation: ApplicationReactiveOperation,
    consumer_name: String,
}

impl ApplicationEventConsumer {
    /// Binds one generated stream operation to one durable consumer name.
    pub fn new(
        operation: ApplicationReactiveOperation,
        consumer_name: impl Into<String>,
    ) -> Result<Self, ApplicationClientError> {
        let consumer_name = consumer_name.into();
        let valid = consumer_name.len() <= 64
            && consumer_name
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphabetic)
            && consumer_name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'));
        if !valid {
            return Err(ApplicationClientError::InvalidInput);
        }
        Ok(Self {
            operation,
            consumer_name,
        })
    }

    fn selection(&self) -> Result<v1::EventConsumerSelection, ApplicationClientError> {
        Ok(v1::EventConsumerSelection {
            reactive_module_hash: self.operation.module_hash.to_vec(),
            operation_name: self.operation.operation_name.clone(),
            parameters: self.operation.lowered_parameters()?,
            consumer_name: self.consumer_name.clone(),
        })
    }
}

/// Bounded pull/stream policy accepted by the public consumer service.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventConsumerOptions {
    /// Maximum events in one response.
    pub batch_limit: u32,
    /// Maximum live leases for this consumer.
    pub in_flight_limit: u32,
    /// Lease lifetime in seconds.
    pub lease_seconds: u64,
    /// Long-poll bound in nanoseconds.
    pub maximum_wait_nanos: u64,
}

impl Default for EventConsumerOptions {
    fn default() -> Self {
        Self {
            batch_limit: 1,
            in_flight_limit: 16,
            lease_seconds: 60,
            maximum_wait_nanos: 0,
        }
    }
}

impl EventConsumerOptions {
    fn validate(self) -> Result<Self, ApplicationClientError> {
        if !(1..=64).contains(&self.batch_limit)
            || !(1..=64).contains(&self.in_flight_limit)
            || !(5..=900).contains(&self.lease_seconds)
            || self.maximum_wait_nanos > 30_000_000_000
        {
            return Err(ApplicationClientError::InvalidInput);
        }
        Ok(self)
    }
}

/// Stable symbolic event identity.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ApplicationEventId {
    /// Authoritative application commit sequence.
    pub commit_sequence: u64,
    /// Zero-based event ordinal in that commit.
    pub event_ordinal: u32,
}

/// Opaque consistency cursor safe to persist and return on reconnect.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveQueryCursor(Vec<u8>);

impl LiveQueryCursor {
    /// Constructs a bounded opaque cursor returned previously by RiffDB.
    pub fn new(bytes: Vec<u8>) -> Result<Self, ApplicationClientError> {
        if bytes.is_empty() || bytes.len() > 16_384 {
            return Err(ApplicationClientError::InvalidInput);
        }
        Ok(Self(bytes))
    }

    /// Opaque cursor bytes for durable application persistence.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Decodes the canonical standard padded Base64 presentation used by
    /// text-only application transports.
    pub fn from_standard_base64(value: &str) -> Result<Self, ApplicationClientError> {
        decode_standard_base64(value).and_then(Self::new)
    }

    /// Returns the canonical standard padded Base64 presentation.
    #[must_use]
    pub fn to_standard_base64(&self) -> String {
        encode_standard_base64(&self.0)
    }
}

const BASE64_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn encode_standard_base64(value: &[u8]) -> String {
    let mut output = String::with_capacity(value.len().div_ceil(3) * 4);
    for chunk in value.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        output.push(char::from(BASE64_ALPHABET[usize::from(first >> 2)]));
        output.push(char::from(
            BASE64_ALPHABET[usize::from(((first & 0x03) << 4) | (second >> 4))],
        ));
        if chunk.len() >= 2 {
            output.push(char::from(
                BASE64_ALPHABET[usize::from(((second & 0x0f) << 2) | (third >> 6))],
            ));
        } else {
            output.push('=');
        }
        if chunk.len() == 3 {
            output.push(char::from(BASE64_ALPHABET[usize::from(third & 0x3f)]));
        } else {
            output.push('=');
        }
    }
    output
}

fn decode_standard_base64(value: &str) -> Result<Vec<u8>, ApplicationClientError> {
    let bytes = value.as_bytes();
    if bytes.is_empty() || !bytes.len().is_multiple_of(4) || bytes.len() > 21_848 {
        return Err(ApplicationClientError::InvalidInput);
    }
    let mut output = Vec::with_capacity(bytes.len() / 4 * 3);
    let chunks = bytes.chunks_exact(4);
    let chunk_count = chunks.len();
    for (index, chunk) in chunks.enumerate() {
        let last = index + 1 == chunk_count;
        let first = base64_value(chunk[0]).ok_or(ApplicationClientError::InvalidInput)?;
        let second = base64_value(chunk[1]).ok_or(ApplicationClientError::InvalidInput)?;
        output.push((first << 2) | (second >> 4));
        if chunk[2] == b'=' {
            if !last || chunk[3] != b'=' || second & 0x0f != 0 {
                return Err(ApplicationClientError::InvalidInput);
            }
            continue;
        }
        let third = base64_value(chunk[2]).ok_or(ApplicationClientError::InvalidInput)?;
        output.push((second << 4) | (third >> 2));
        if chunk[3] == b'=' {
            if !last || third & 0x03 != 0 {
                return Err(ApplicationClientError::InvalidInput);
            }
            continue;
        }
        let fourth = base64_value(chunk[3]).ok_or(ApplicationClientError::InvalidInput)?;
        output.push((third << 6) | fourth);
    }
    Ok(output)
}

const fn base64_value(value: u8) -> Option<u8> {
    match value {
        b'A'..=b'Z' => Some(value - b'A'),
        b'a'..=b'z' => Some(value - b'a' + 26),
        b'0'..=b'9' => Some(value - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

/// Safe symbolic event selected by the compiled stream definition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationEvent {
    /// Stable event identity.
    pub id: ApplicationEventId,
    /// Symbolic event name.
    pub name: String,
    /// Writer contract version.
    pub writer_contract_version: u64,
    /// Writer command name.
    pub command_name: String,
    /// Safe actor kind name.
    pub actor_kind: String,
    /// Canonical provenance resource locator.
    pub provenance_uri: String,
    /// Restore-incarnation fence.
    pub history_incarnation: u64,
    /// Explicitly selected payload fields only.
    pub fields: BTreeMap<String, ApplicationValue>,
}

/// One attempt-specific leased event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationEventDelivery {
    /// Authorized selected event.
    pub event: ApplicationEvent,
    /// One-based delivery attempt.
    pub attempt: u32,
    /// Opaque attempt token. Possession is not authority.
    pub lease_token: Vec<u8>,
    /// Lease expiration timestamp.
    pub expires_at: (i64, u32),
}

/// Minimal exact evidence required to mutate one live event lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationEventLeaseEvidence {
    event_id: ApplicationEventId,
    lease_token: Vec<u8>,
    history_incarnation: u64,
}

impl ApplicationEventLeaseEvidence {
    /// Checks the public lease identity without treating possession as authority.
    pub fn new(
        event_id: ApplicationEventId,
        lease_token: Vec<u8>,
        history_incarnation: u64,
    ) -> Result<Self, ApplicationClientError> {
        if event_id.commit_sequence == 0 || lease_token.len() != 32 || history_incarnation == 0 {
            return Err(ApplicationClientError::InvalidInput);
        }
        Ok(Self {
            event_id,
            lease_token,
            history_incarnation,
        })
    }
}

impl ApplicationEventDelivery {
    /// Retains only the exact attempt identity needed by ack or nack.
    #[must_use]
    pub fn lease_evidence(&self) -> ApplicationEventLeaseEvidence {
        ApplicationEventLeaseEvidence {
            event_id: self.event.id,
            lease_token: self.lease_token.clone(),
            history_incarnation: self.event.history_incarnation,
        }
    }
}

/// Durable consumer checkpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationEventCheckpoint {
    /// No event has been contiguously resolved.
    BeforeFirst,
    /// Every selected event through this identity is resolved.
    After(ApplicationEventId),
}

/// Bounded durable consumer status.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationEventConsumerStatus {
    /// Compare-and-transition revision.
    pub revision: u64,
    /// Contiguous checkpoint.
    pub checkpoint: ApplicationEventCheckpoint,
    /// Restore-incarnation fence.
    pub history_incarnation: u64,
    /// Current live leases.
    pub live_leases: u32,
    /// Current retry records.
    pub retries: u32,
    /// Current dead letters.
    pub dead_letters: u32,
}

/// One bounded consumer response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationEventBatch {
    /// Leased events in service order.
    pub events: Vec<ApplicationEventDelivery>,
    /// Status after the transition.
    pub status: ApplicationEventConsumerStatus,
    /// Whether a nonzero long poll expired without work.
    pub wait_timed_out: bool,
}

/// One freshly executed hydration in a contextual work item.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationContextualHydration {
    /// Subscription-local declared hydration name.
    pub name: String,
    /// Named-query result branch.
    pub outcome: String,
    /// Name-addressed typed result fields.
    pub fields: BTreeMap<String, ApplicationResultField>,
}

/// One currently authorized declared reaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationContextualReaction {
    /// Subscription-local reaction name.
    pub name: String,
    /// Target contract command name.
    pub command_name: String,
    /// Stable target command identity.
    pub command_id: u32,
    causation_token: Vec<u8>,
}

impl ApplicationContextualReaction {
    /// Checks one transport-raised reaction without treating its token as authority.
    pub fn checked(
        name: impl Into<String>,
        command_name: impl Into<String>,
        command_id: u32,
        causation_token: Vec<u8>,
    ) -> Result<Self, ApplicationClientError> {
        let name = name.into();
        let command_name = command_name.into();
        if name.is_empty()
            || name.len() > 256
            || command_name.is_empty()
            || command_name.len() > 256
            || command_id == 0
            || causation_token.len() <= 32
            || causation_token.len() > 1_024
        {
            return Err(ApplicationClientError::InvalidInput);
        }
        Ok(Self {
            name,
            command_name,
            command_id,
            causation_token,
        })
    }

    /// Borrows the opaque proof for transport forwarding only.
    #[must_use]
    pub fn causation_token(&self) -> &[u8] {
        &self.causation_token
    }
}

/// One leased event with fresh same-snapshot context and authorized reactions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationContextualWorkItem {
    /// Attempt-specific event delivery.
    pub delivery: ApplicationEventDelivery,
    /// Authoritative application head shared by every hydration.
    pub context_head: u64,
    /// Fresh hydration results in declaration order.
    pub hydrations: Vec<ApplicationContextualHydration>,
    /// Reactions authorized when this item was produced.
    pub available_reactions: Vec<ApplicationContextualReaction>,
}

/// One bounded contextual pull.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationContextualBatch {
    /// Zero or one leased contextual work item.
    pub items: Vec<ApplicationContextualWorkItem>,
    /// Durable consumer status after leasing.
    pub status: ApplicationEventConsumerStatus,
    /// Whether the bounded wait elapsed without work.
    pub wait_timed_out: bool,
}

/// One generated event plus its contextual work and lease evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedContextualWorkItem<E> {
    /// Generated closed event union.
    pub event: E,
    evidence: ApplicationContextualWorkItem,
}

impl<E> TypedContextualWorkItem<E> {
    /// Context, reactions, and exact lease evidence used by checked operations.
    #[must_use]
    pub const fn evidence(&self) -> &ApplicationContextualWorkItem {
        &self.evidence
    }
}

/// One generated contextual pull.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedContextualBatch<E> {
    /// Zero or one typed contextual work item.
    pub items: Vec<TypedContextualWorkItem<E>>,
    /// Durable consumer status after leasing.
    pub status: ApplicationEventConsumerStatus,
    /// Whether the bounded wait elapsed without work.
    pub wait_timed_out: bool,
}

/// One generated typed event together with its exact lease evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedEventDelivery<E> {
    /// Generated closed event union.
    pub event: E,
    evidence: ApplicationEventDelivery,
}

impl<E> TypedEventDelivery<E> {
    /// Attempt-specific evidence used by checked acknowledge operations.
    #[must_use]
    pub const fn evidence(&self) -> &ApplicationEventDelivery {
        &self.evidence
    }
}

/// One bounded generated consumer batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedEventBatch<E> {
    /// Typed leased events.
    pub events: Vec<TypedEventDelivery<E>>,
    /// Durable status after leasing.
    pub status: ApplicationEventConsumerStatus,
    /// Whether the bounded wait elapsed without work.
    pub wait_timed_out: bool,
}

/// Closed result for consumer metadata mutations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationEventMutationResult {
    /// Transition applied.
    Applied,
    /// Consumer changed since the supplied evidence.
    StateChanged,
    /// Consumer was not found.
    NotFound,
    /// A seek or retire was blocked by a live lease.
    OutstandingLease,
    /// Lease evidence is stale.
    StaleLease,
    /// Lease evidence is expired.
    LeaseExpired,
}

/// Stream of checked, application-safe durable consumer batches.
pub struct ApplicationEventResponseStream {
    inner: EventConsumerResponseStream,
}

impl ApplicationEventResponseStream {
    /// Receives the next authorized bounded batch.
    pub async fn message(
        &mut self,
    ) -> Result<Option<ApplicationEventBatch>, ApplicationClientError> {
        self.inner
            .message()
            .await?
            .map(raise_event_batch)
            .transpose()
    }
}

/// Closed application live-query update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApplicationLiveQueryUpdate {
    /// Initial complete snapshot.
    Snapshot {
        /// Typed name-addressed result.
        result: NamedQueryResult,
        /// Opaque reconnect evidence.
        cursor: LiveQueryCursor,
        /// Restore-incarnation frontier.
        history_incarnation: u64,
        /// Application-head frontier.
        application_head: u64,
    },
    /// Bounded keyed patch. Generated clients apply its closed operations.
    Patch(LiveQueryPatch),
    /// Complete bounded replacement after convergence reset.
    Reset {
        /// Stable reset reason name.
        reason: String,
        /// Complete replacement result.
        result: NamedQueryResult,
        /// Opaque reconnect evidence.
        cursor: LiveQueryCursor,
        /// Restore-incarnation frontier.
        history_incarnation: u64,
        /// Application-head frontier.
        application_head: u64,
    },
    /// Frontier-only convergence marker.
    Checkpoint(LiveQueryCheckpoint),
    /// Terminal update; clients must clear retained protected state.
    Terminal(LiveQueryTerminal),
}

/// One generated complete snapshot with its exact reconnect frontier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedLiveQuerySnapshot<T> {
    /// Generated declared-result union.
    pub result: T,
    /// Frontier and opaque reconnect cursor for this exact view.
    pub checkpoint: LiveQueryCheckpoint,
}

/// One generated complete reset with its reason and reconnect frontier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedLiveQueryReset<T> {
    /// Stable closed reset reason name.
    pub reason: String,
    /// Generated complete replacement result.
    pub result: T,
    /// Frontier and opaque reconnect cursor for the replacement view.
    pub checkpoint: LiveQueryCheckpoint,
}

/// One bounded keyed patch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveQueryPatch {
    /// Top-level query result field receiving the operations.
    pub result_field: String,
    /// Closed ordered patch operations.
    pub operations: Vec<LiveQueryPatchOperation>,
    /// Convergence frontier.
    pub checkpoint: LiveQueryCheckpoint,
}

/// Closed keyed list mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LiveQueryPatchOperation {
    /// Inserts one complete authorized record.
    Insert {
        /// Destination index.
        index: u32,
        /// Complete inserted record.
        record: ApplicationRecord,
    },
    /// Removes one record by complete authorized key.
    Remove {
        /// Existing index.
        index: u32,
        /// Complete authorized key.
        key: BTreeMap<String, ApplicationValue>,
    },
    /// Replaces one complete authorized record.
    Replace {
        /// Existing index.
        index: u32,
        /// Complete replacement record.
        record: ApplicationRecord,
    },
    /// Moves one keyed record.
    Move {
        /// Existing index.
        from: u32,
        /// Destination index.
        to: u32,
        /// Complete authorized key.
        key: BTreeMap<String, ApplicationValue>,
    },
}

/// Live-query convergence frontier and reconnect cursor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveQueryCheckpoint {
    /// Restore-incarnation frontier.
    pub history_incarnation: u64,
    /// Application-head frontier.
    pub application_head: u64,
    /// Opaque reconnect cursor.
    pub cursor: LiveQueryCursor,
}

/// Terminal live-query reason and last non-secret frontier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveQueryTerminal {
    /// Stable terminal reason name.
    pub reason: String,
    /// Last frontier, when one was established.
    pub last_frontier: Option<(u64, u64)>,
}

/// Stream of closed application-safe live-query updates.
pub struct ApplicationLiveQueryStream {
    inner: LiveQueryUpdateStream,
    terminal: bool,
}

/// Generated closed live-query stream.
pub struct TypedLiveQueryStream<G: GeneratedLiveQuery> {
    inner: ApplicationLiveQueryStream,
    marker: PhantomData<fn() -> G>,
}

impl<G: GeneratedLiveQuery> TypedLiveQueryStream<G> {
    /// Receives and decodes the next generated update.
    pub async fn message(&mut self) -> Result<Option<G::Update>, ApplicationClientError> {
        self.inner
            .message()
            .await?
            .map(G::decode_update)
            .transpose()
    }
}

impl ApplicationLiveQueryStream {
    /// Receives the next update. A terminal update permanently closes this wrapper.
    pub async fn message(
        &mut self,
    ) -> Result<Option<ApplicationLiveQueryUpdate>, ApplicationClientError> {
        if self.terminal {
            return Ok(None);
        }
        let update = self
            .inner
            .message()
            .await?
            .map(raise_live_update)
            .transpose()?;
        if matches!(update, Some(ApplicationLiveQueryUpdate::Terminal(_))) {
            self.terminal = true;
        }
        Ok(update)
    }
}

impl StableApplicationClient {
    /// Pulls and decodes one generated typed event batch.
    pub async fn consume_generated_events<G: GeneratedEventConsumer>(
        &mut self,
        generated: G,
        options: EventConsumerOptions,
        metadata: &CallMetadata,
    ) -> Result<TypedEventBatch<G::Event>, ApplicationClientError> {
        let consumer = generated.event_consumer()?;
        let batch = self
            .consume_event_stream(&consumer, options, metadata)
            .await?;
        let events = batch
            .events
            .into_iter()
            .map(|evidence| {
                Ok(TypedEventDelivery {
                    event: G::decode_event(evidence.event.clone())?,
                    evidence,
                })
            })
            .collect::<Result<Vec<_>, ApplicationClientError>>()?;
        Ok(TypedEventBatch {
            events,
            status: batch.status,
            wait_timed_out: batch.wait_timed_out,
        })
    }

    /// Pulls and decodes one generated contextual work item.
    pub async fn consume_generated_contextual<G: GeneratedEventConsumer>(
        &mut self,
        generated: G,
        maximum_wait_nanos: u64,
        metadata: &CallMetadata,
    ) -> Result<TypedContextualBatch<G::Event>, ApplicationClientError> {
        let consumer = generated.event_consumer()?;
        let batch = self
            .consume_contextual_subscription(&consumer, maximum_wait_nanos, metadata)
            .await?;
        let items = batch
            .items
            .into_iter()
            .map(|evidence| {
                Ok(TypedContextualWorkItem {
                    event: G::decode_event(evidence.delivery.event.clone())?,
                    evidence,
                })
            })
            .collect::<Result<Vec<_>, ApplicationClientError>>()?;
        Ok(TypedContextualBatch {
            items,
            status: batch.status,
            wait_timed_out: batch.wait_timed_out,
        })
    }

    /// Executes one generated command through an available contextual reaction.
    pub async fn execute_generated_contextual_reaction<C: GeneratedCommand>(
        &mut self,
        consumer: &ApplicationEventConsumer,
        reaction: &ApplicationContextualReaction,
        command: &C,
        metadata: &CallMetadata,
    ) -> Result<TypedCommandResult<C::Outcome>, GeneratedExecutionError> {
        let request_id = generate_request_id().map_err(|error| {
            GeneratedExecutionError::Client(crate::ClientError::IdentifierGeneration(error))
        })?;
        let command_request = command
            .idempotent_command()
            .map_err(GeneratedExecutionError::CommandShape)?;
        if command_request.command_name() != reaction.command_name {
            return Err(GeneratedExecutionError::CommandShape(
                crate::generated::GeneratedCommandError::InvalidInputShape,
            ));
        }
        let selection = consumer.selection().map_err(|_| {
            GeneratedExecutionError::CommandShape(
                crate::generated::GeneratedCommandError::InvalidInputShape,
            )
        })?;
        let response = self
            .inner
            .execute_contextual_reaction(
                v1::ExecuteContextualReactionRequest {
                    request_id: request_id.as_bytes().to_vec(),
                    selection: Some(selection),
                    causation_token: reaction.causation_token.clone(),
                    reaction_name: reaction.name.clone(),
                    command: Some(command_request.request(request_id)),
                },
                metadata,
            )
            .await
            .map_err(GeneratedExecutionError::Client)?;
        let outcome = command
            .decode_outcome(&response)
            .map_err(GeneratedExecutionError::CommandShape)?;
        typed_command_result(outcome, response)
    }

    /// Opens and decodes one generated exact live query.
    pub async fn watch_generated_query<G: GeneratedLiveQuery>(
        &mut self,
        generated: G,
        cursor: Option<LiveQueryCursor>,
        metadata: &CallMetadata,
    ) -> Result<TypedLiveQueryStream<G>, ApplicationClientError> {
        let operation = generated.live_operation()?;
        let inner = self.watch_named_query(&operation, cursor, metadata).await?;
        Ok(TypedLiveQueryStream {
            inner,
            marker: PhantomData,
        })
    }

    /// Pulls one bounded authorized batch from an exact generated stream.
    pub async fn consume_event_stream(
        &mut self,
        consumer: &ApplicationEventConsumer,
        options: EventConsumerOptions,
        metadata: &CallMetadata,
    ) -> Result<ApplicationEventBatch, ApplicationClientError> {
        let response = self
            .inner
            .consume_event_stream(consumer_request(consumer, options)?, metadata)
            .await?;
        raise_event_batch(response)
    }

    /// Pulls one contextual event with fresh same-snapshot hydrations.
    pub async fn consume_contextual_subscription(
        &mut self,
        consumer: &ApplicationEventConsumer,
        maximum_wait_nanos: u64,
        metadata: &CallMetadata,
    ) -> Result<ApplicationContextualBatch, ApplicationClientError> {
        if maximum_wait_nanos > 30_000_000_000 {
            return Err(ApplicationClientError::InvalidInput);
        }
        let response = self
            .inner
            .consume_contextual_subscription(
                v1::ConsumeContextualSubscriptionRequest {
                    request_id: request_id()?,
                    selection: Some(consumer.selection()?),
                    maximum_wait_nanos,
                },
                metadata,
            )
            .await?;
        raise_contextual_batch(response)
    }

    /// Acknowledges one exact contextual lease after fresh authorization.
    pub async fn acknowledge_contextual_item(
        &mut self,
        consumer: &ApplicationEventConsumer,
        item: &ApplicationContextualWorkItem,
        metadata: &CallMetadata,
    ) -> Result<ApplicationEventMutationResult, ApplicationClientError> {
        self.acknowledge_contextual_lease(consumer, &item.delivery.lease_evidence(), metadata)
            .await
    }

    /// Acknowledges exact contextual lease evidence after fresh authorization.
    pub async fn acknowledge_contextual_lease(
        &mut self,
        consumer: &ApplicationEventConsumer,
        evidence: &ApplicationEventLeaseEvidence,
        metadata: &CallMetadata,
    ) -> Result<ApplicationEventMutationResult, ApplicationClientError> {
        let response = self
            .inner
            .acknowledge_contextual_subscription(
                v1::AcknowledgeContextualSubscriptionRequest {
                    request_id: request_id()?,
                    selection: Some(consumer.selection()?),
                    event_id: Some(lower_event_id(evidence.event_id)),
                    lease_token: evidence.lease_token.clone(),
                    history_incarnation: evidence.history_incarnation,
                },
                metadata,
            )
            .await?;
        raise_mutation_result(response.result)
    }

    /// Negatively acknowledges one exact contextual lease with bounded delay.
    pub async fn negative_acknowledge_contextual_item(
        &mut self,
        consumer: &ApplicationEventConsumer,
        item: &ApplicationContextualWorkItem,
        retry_delay_nanos: u64,
        metadata: &CallMetadata,
    ) -> Result<ApplicationEventMutationResult, ApplicationClientError> {
        self.negative_acknowledge_contextual_lease(
            consumer,
            &item.delivery.lease_evidence(),
            retry_delay_nanos,
            metadata,
        )
        .await
    }

    /// Releases exact contextual lease evidence with a bounded retry delay.
    pub async fn negative_acknowledge_contextual_lease(
        &mut self,
        consumer: &ApplicationEventConsumer,
        evidence: &ApplicationEventLeaseEvidence,
        retry_delay_nanos: u64,
        metadata: &CallMetadata,
    ) -> Result<ApplicationEventMutationResult, ApplicationClientError> {
        if retry_delay_nanos > 3_600_000_000_000 {
            return Err(ApplicationClientError::InvalidInput);
        }
        let response = self
            .inner
            .negative_acknowledge_contextual_subscription(
                v1::NegativeAcknowledgeContextualSubscriptionRequest {
                    request_id: request_id()?,
                    selection: Some(consumer.selection()?),
                    event_id: Some(lower_event_id(evidence.event_id)),
                    lease_token: evidence.lease_token.clone(),
                    history_incarnation: evidence.history_incarnation,
                    retry_delay_nanos,
                },
                metadata,
            )
            .await?;
        raise_mutation_result(response.result)
    }

    /// Reads one contextual consumer status under contextual consume authority.
    pub async fn contextual_subscription_status(
        &mut self,
        consumer: &ApplicationEventConsumer,
        metadata: &CallMetadata,
    ) -> Result<Option<ApplicationEventConsumerStatus>, ApplicationClientError> {
        let response = self
            .inner
            .get_contextual_subscription_status(
                v1::GetContextualSubscriptionStatusRequest {
                    request_id: request_id()?,
                    selection: Some(consumer.selection()?),
                },
                metadata,
            )
            .await?;
        match response.result {
            Some(v1::get_event_stream_consumer_status_response::Result::NotFound(_)) => Ok(None),
            Some(v1::get_event_stream_consumer_status_response::Result::Found(status)) => {
                raise_consumer_status(status).map(Some)
            }
            None => Err(ApplicationClientError::InvalidResponse),
        }
    }

    /// Executes one declared reaction through the ordinary typed command envelope.
    pub async fn execute_contextual_reaction(
        &mut self,
        consumer: &ApplicationEventConsumer,
        reaction: &ApplicationContextualReaction,
        command: ApplicationCommand,
        metadata: &CallMetadata,
    ) -> Result<ApplicationCommandResult, ApplicationClientError> {
        let request_id =
            generate_request_id().map_err(|_| ApplicationClientError::IdentifierUnavailable)?;
        let command = command.into_idempotent_command()?;
        if command.command_name() != reaction.command_name {
            return Err(ApplicationClientError::InvalidInput);
        }
        let response = self
            .inner
            .execute_contextual_reaction(
                v1::ExecuteContextualReactionRequest {
                    request_id: request_id.into_bytes().to_vec(),
                    selection: Some(consumer.selection()?),
                    causation_token: reaction.causation_token.clone(),
                    reaction_name: reaction.name.clone(),
                    command: Some(command.request(request_id)),
                },
                metadata,
            )
            .await?;
        raise_application_command_result(response)
    }

    /// Opens the checked server stream for the same consumer operation.
    pub async fn stream_event_consumer(
        &mut self,
        consumer: &ApplicationEventConsumer,
        options: EventConsumerOptions,
        metadata: &CallMetadata,
    ) -> Result<ApplicationEventResponseStream, ApplicationClientError> {
        let inner = self
            .inner
            .stream_event_consumer(consumer_request(consumer, options)?, metadata)
            .await?;
        Ok(ApplicationEventResponseStream { inner })
    }

    /// Acknowledges one exact live lease under the same reauthorized identity.
    pub async fn acknowledge_event(
        &mut self,
        consumer: &ApplicationEventConsumer,
        delivery: &ApplicationEventDelivery,
        metadata: &CallMetadata,
    ) -> Result<ApplicationEventMutationResult, ApplicationClientError> {
        self.acknowledge_event_lease(consumer, &delivery.lease_evidence(), metadata)
            .await
    }

    /// Acknowledges one exact checked lease identity.
    pub async fn acknowledge_event_lease(
        &mut self,
        consumer: &ApplicationEventConsumer,
        evidence: &ApplicationEventLeaseEvidence,
        metadata: &CallMetadata,
    ) -> Result<ApplicationEventMutationResult, ApplicationClientError> {
        let response = self
            .inner
            .acknowledge_event_stream(
                v1::AcknowledgeEventStreamRequest {
                    request_id: request_id()?,
                    selection: Some(consumer.selection()?),
                    event_id: Some(lower_event_id(evidence.event_id)),
                    lease_token: evidence.lease_token.clone(),
                    history_incarnation: evidence.history_incarnation,
                },
                metadata,
            )
            .await?;
        raise_mutation_result(response.result)
    }

    /// Negatively acknowledges one exact live lease with a bounded delay.
    pub async fn negative_acknowledge_event(
        &mut self,
        consumer: &ApplicationEventConsumer,
        delivery: &ApplicationEventDelivery,
        retry_delay_nanos: u64,
        metadata: &CallMetadata,
    ) -> Result<ApplicationEventMutationResult, ApplicationClientError> {
        self.negative_acknowledge_event_lease(
            consumer,
            &delivery.lease_evidence(),
            retry_delay_nanos,
            metadata,
        )
        .await
    }

    /// Negatively acknowledges one exact checked lease identity.
    pub async fn negative_acknowledge_event_lease(
        &mut self,
        consumer: &ApplicationEventConsumer,
        evidence: &ApplicationEventLeaseEvidence,
        retry_delay_nanos: u64,
        metadata: &CallMetadata,
    ) -> Result<ApplicationEventMutationResult, ApplicationClientError> {
        if retry_delay_nanos > 3_600_000_000_000 {
            return Err(ApplicationClientError::InvalidInput);
        }
        let response = self
            .inner
            .negative_acknowledge_event_stream(
                v1::NegativeAcknowledgeEventStreamRequest {
                    request_id: request_id()?,
                    selection: Some(consumer.selection()?),
                    event_id: Some(lower_event_id(evidence.event_id)),
                    lease_token: evidence.lease_token.clone(),
                    history_incarnation: evidence.history_incarnation,
                    retry_delay_nanos,
                },
                metadata,
            )
            .await?;
        raise_mutation_result(response.result)
    }

    /// Moves one exact consumer checkpoint after server-side membership proof.
    pub async fn seek_event_consumer(
        &mut self,
        consumer: &ApplicationEventConsumer,
        checkpoint: ApplicationEventCheckpoint,
        metadata: &CallMetadata,
    ) -> Result<ApplicationEventMutationResult, ApplicationClientError> {
        let position = match checkpoint {
            ApplicationEventCheckpoint::BeforeFirst => {
                v1::event_consumer_checkpoint::Position::BeforeFirst(v1::Unit {})
            }
            ApplicationEventCheckpoint::After(event_id) if event_id.commit_sequence != 0 => {
                v1::event_consumer_checkpoint::Position::AfterEventId(lower_event_id(event_id))
            }
            ApplicationEventCheckpoint::After(_) => {
                return Err(ApplicationClientError::InvalidInput);
            }
        };
        let response = self
            .inner
            .seek_event_stream_consumer(
                v1::SeekEventStreamConsumerRequest {
                    request_id: request_id()?,
                    selection: Some(consumer.selection()?),
                    checkpoint: Some(v1::EventConsumerCheckpoint {
                        position: Some(position),
                    }),
                },
                metadata,
            )
            .await?;
        raise_mutation_result(response.result)
    }

    /// Reads bounded status under consume authority.
    pub async fn event_consumer_status(
        &mut self,
        consumer: &ApplicationEventConsumer,
        metadata: &CallMetadata,
    ) -> Result<Option<ApplicationEventConsumerStatus>, ApplicationClientError> {
        let response = self
            .inner
            .get_event_stream_consumer_status(
                v1::GetEventStreamConsumerStatusRequest {
                    request_id: request_id()?,
                    selection: Some(consumer.selection()?),
                },
                metadata,
            )
            .await?;
        match response.result {
            Some(v1::get_event_stream_consumer_status_response::Result::NotFound(_)) => Ok(None),
            Some(v1::get_event_stream_consumer_status_response::Result::Found(status)) => {
                raise_consumer_status(status).map(Some)
            }
            None => Err(ApplicationClientError::InvalidResponse),
        }
    }

    /// Opens one exact generated named-query watch with optional cursor evidence.
    pub async fn watch_named_query(
        &mut self,
        operation: &ApplicationReactiveOperation,
        cursor: Option<LiveQueryCursor>,
        metadata: &CallMetadata,
    ) -> Result<ApplicationLiveQueryStream, ApplicationClientError> {
        let inner = self
            .inner
            .watch_named_query(
                v1::WatchNamedQueryRequest {
                    request_id: request_id()?,
                    reactive_module_hash: operation.module_hash.to_vec(),
                    operation_name: operation.operation_name.clone(),
                    parameters: operation.live_parameters()?,
                    cursor: cursor.map(|value| value.0),
                },
                metadata,
            )
            .await?;
        Ok(ApplicationLiveQueryStream {
            inner,
            terminal: false,
        })
    }
}

fn consumer_request(
    consumer: &ApplicationEventConsumer,
    options: EventConsumerOptions,
) -> Result<v1::ConsumeEventStreamRequest, ApplicationClientError> {
    let options = options.validate()?;
    Ok(v1::ConsumeEventStreamRequest {
        request_id: request_id()?,
        selection: Some(consumer.selection()?),
        batch_limit: options.batch_limit,
        in_flight_limit: options.in_flight_limit,
        lease_seconds: options.lease_seconds,
        maximum_wait_nanos: options.maximum_wait_nanos,
    })
}

fn request_id() -> Result<Vec<u8>, ApplicationClientError> {
    generate_request_id()
        .map(|value| value.into_bytes().to_vec())
        .map_err(|_| ApplicationClientError::IdentifierUnavailable)
}

const fn lower_event_id(value: ApplicationEventId) -> v1::EventId {
    v1::EventId {
        commit_sequence: value.commit_sequence,
        event_ordinal: value.event_ordinal,
    }
}

fn raise_event_id(
    value: Option<v1::EventId>,
) -> Result<ApplicationEventId, ApplicationClientError> {
    let value = value.ok_or(ApplicationClientError::InvalidResponse)?;
    if value.commit_sequence == 0 {
        return Err(ApplicationClientError::InvalidResponse);
    }
    Ok(ApplicationEventId {
        commit_sequence: value.commit_sequence,
        event_ordinal: value.event_ordinal,
    })
}

fn raise_event_batch(
    response: v1::ConsumeEventStreamResponse,
) -> Result<ApplicationEventBatch, ApplicationClientError> {
    let events = response
        .events
        .into_iter()
        .map(raise_event_delivery)
        .collect::<Result<Vec<_>, _>>()?;
    let status = raise_consumer_status(
        response
            .status
            .ok_or(ApplicationClientError::InvalidResponse)?,
    )?;
    Ok(ApplicationEventBatch {
        events,
        status,
        wait_timed_out: response.wait_timed_out,
    })
}

fn raise_contextual_batch(
    response: v1::ConsumeContextualSubscriptionResponse,
) -> Result<ApplicationContextualBatch, ApplicationClientError> {
    let items = response
        .items
        .into_iter()
        .map(raise_contextual_work_item)
        .collect::<Result<Vec<_>, _>>()?;
    let status = raise_consumer_status(
        response
            .status
            .ok_or(ApplicationClientError::InvalidResponse)?,
    )?;
    Ok(ApplicationContextualBatch {
        items,
        status,
        wait_timed_out: response.wait_timed_out,
    })
}

fn raise_contextual_work_item(
    item: v1::ContextualWorkItem,
) -> Result<ApplicationContextualWorkItem, ApplicationClientError> {
    let delivery = raise_event_delivery(
        item.delivery
            .ok_or(ApplicationClientError::InvalidResponse)?,
    )?;
    let hydrations = item
        .hydrations
        .into_iter()
        .map(raise_contextual_hydration)
        .collect::<Result<Vec<_>, _>>()?;
    let available_reactions = item
        .available_reactions
        .into_iter()
        .map(|reaction| {
            if reaction.name.is_empty()
                || reaction.command_name.is_empty()
                || reaction.command_id == 0
                || reaction.causation_token.len() <= 32
                || reaction.causation_token.len() > 1_024
            {
                return Err(ApplicationClientError::InvalidResponse);
            }
            Ok(ApplicationContextualReaction {
                name: reaction.name,
                command_name: reaction.command_name,
                command_id: reaction.command_id,
                causation_token: reaction.causation_token,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if item.context_head == 0 {
        return Err(ApplicationClientError::InvalidResponse);
    }
    Ok(ApplicationContextualWorkItem {
        delivery,
        context_head: item.context_head,
        hydrations,
        available_reactions,
    })
}

fn raise_contextual_hydration(
    hydration: v1::ContextualHydration,
) -> Result<ApplicationContextualHydration, ApplicationClientError> {
    let mut fields = BTreeMap::new();
    for field in hydration.fields {
        let cardinality = match v1::ContextualQueryCardinality::try_from(field.cardinality).ok() {
            Some(v1::ContextualQueryCardinality::One) => ApplicationCardinality::One,
            Some(v1::ContextualQueryCardinality::Maybe) => ApplicationCardinality::Maybe,
            Some(v1::ContextualQueryCardinality::Many) => ApplicationCardinality::Many,
            Some(v1::ContextualQueryCardinality::Unspecified) | None => {
                return Err(ApplicationClientError::InvalidResponse);
            }
        };
        let records = field
            .rows
            .into_iter()
            .map(|row| {
                let mut values = BTreeMap::new();
                for value in row.fields {
                    if value.name.is_empty()
                        || values
                            .insert(
                                value.name,
                                raise_value(
                                    value.value.ok_or(ApplicationClientError::InvalidResponse)?,
                                )?,
                            )
                            .is_some()
                    {
                        return Err(ApplicationClientError::InvalidResponse);
                    }
                }
                Ok(ApplicationRecord {
                    entity: row.entity,
                    fields: values,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if field.name.is_empty()
            || fields
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
    if hydration.name.is_empty() || hydration.outcome.is_empty() {
        return Err(ApplicationClientError::InvalidResponse);
    }
    Ok(ApplicationContextualHydration {
        name: hydration.name,
        outcome: hydration.outcome,
        fields,
    })
}

fn raise_event_delivery(
    delivery: v1::ConsumedEvent,
) -> Result<ApplicationEventDelivery, ApplicationClientError> {
    let event = delivery
        .event
        .ok_or(ApplicationClientError::InvalidResponse)?;
    let id = raise_event_id(event.event_id)?;
    let expires = delivery
        .expires_at
        .ok_or(ApplicationClientError::InvalidResponse)?;
    let mut fields = BTreeMap::new();
    for field in event.fields {
        if field.name.is_empty()
            || fields
                .insert(
                    field.name,
                    raise_value(field.value.ok_or(ApplicationClientError::InvalidResponse)?)?,
                )
                .is_some()
        {
            return Err(ApplicationClientError::InvalidResponse);
        }
    }
    if event.event_name.is_empty()
        || event.command_name.is_empty()
        || event.provenance_uri.is_empty()
        || event.history_incarnation == 0
        || delivery.attempt == 0
        || delivery.lease_token.is_empty()
        || expires.nanos >= 1_000_000_000
    {
        return Err(ApplicationClientError::InvalidResponse);
    }
    let actor_kind = v1::ActorKind::try_from(event.actor_kind)
        .ok()
        .filter(|kind| *kind != v1::ActorKind::Unspecified)
        .map(|kind| kind.as_str_name().to_owned())
        .ok_or(ApplicationClientError::InvalidResponse)?;
    Ok(ApplicationEventDelivery {
        event: ApplicationEvent {
            id,
            name: event.event_name,
            writer_contract_version: event.writer_contract_version,
            command_name: event.command_name,
            actor_kind,
            provenance_uri: event.provenance_uri,
            history_incarnation: event.history_incarnation,
            fields,
        },
        attempt: delivery.attempt,
        lease_token: delivery.lease_token,
        expires_at: (expires.seconds, expires.nanos),
    })
}

fn raise_consumer_status(
    status: v1::EventConsumerStatus,
) -> Result<ApplicationEventConsumerStatus, ApplicationClientError> {
    let checkpoint = match status
        .checkpoint
        .and_then(|value| value.position)
        .ok_or(ApplicationClientError::InvalidResponse)?
    {
        v1::event_consumer_checkpoint::Position::BeforeFirst(_) => {
            ApplicationEventCheckpoint::BeforeFirst
        }
        v1::event_consumer_checkpoint::Position::AfterEventId(value) => {
            ApplicationEventCheckpoint::After(raise_event_id(Some(value))?)
        }
    };
    if status.revision == 0 || status.history_incarnation == 0 {
        return Err(ApplicationClientError::InvalidResponse);
    }
    Ok(ApplicationEventConsumerStatus {
        revision: status.revision,
        checkpoint,
        history_incarnation: status.history_incarnation,
        live_leases: status.live_leases,
        retries: status.retries,
        dead_letters: status.dead_letters,
    })
}

fn raise_mutation_result(
    value: i32,
) -> Result<ApplicationEventMutationResult, ApplicationClientError> {
    match v1::EventConsumerMutationResult::try_from(value).ok() {
        Some(v1::EventConsumerMutationResult::Applied) => {
            Ok(ApplicationEventMutationResult::Applied)
        }
        Some(v1::EventConsumerMutationResult::StateChanged) => {
            Ok(ApplicationEventMutationResult::StateChanged)
        }
        Some(v1::EventConsumerMutationResult::NotFound) => {
            Ok(ApplicationEventMutationResult::NotFound)
        }
        Some(v1::EventConsumerMutationResult::OutstandingLease) => {
            Ok(ApplicationEventMutationResult::OutstandingLease)
        }
        Some(v1::EventConsumerMutationResult::StaleLease) => {
            Ok(ApplicationEventMutationResult::StaleLease)
        }
        Some(v1::EventConsumerMutationResult::LeaseExpired) => {
            Ok(ApplicationEventMutationResult::LeaseExpired)
        }
        Some(v1::EventConsumerMutationResult::Unspecified) | None => {
            Err(ApplicationClientError::InvalidResponse)
        }
    }
}

fn raise_live_update(
    update: v1::LiveQueryUpdate,
) -> Result<ApplicationLiveQueryUpdate, ApplicationClientError> {
    match update
        .update
        .ok_or(ApplicationClientError::InvalidResponse)?
    {
        v1::live_query_update::Update::Snapshot(value) => {
            let checkpoint = live_checkpoint(value.frontier, value.cursor)?;
            Ok(ApplicationLiveQueryUpdate::Snapshot {
                result: raise_live_result(value.result)?,
                cursor: checkpoint.cursor,
                history_incarnation: checkpoint.history_incarnation,
                application_head: checkpoint.application_head,
            })
        }
        v1::live_query_update::Update::Patch(value) => {
            let checkpoint = live_checkpoint(value.frontier, value.cursor)?;
            let operations = value
                .operations
                .into_iter()
                .map(raise_patch_operation)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ApplicationLiveQueryUpdate::Patch(LiveQueryPatch {
                result_field: value.result_field,
                operations,
                checkpoint,
            }))
        }
        v1::live_query_update::Update::Reset(value) => {
            let checkpoint = live_checkpoint(value.frontier, value.cursor)?;
            let reason = v1::LiveQueryResetReason::try_from(value.reason)
                .ok()
                .filter(|reason| *reason != v1::LiveQueryResetReason::Unspecified)
                .map(|reason| reason.as_str_name().to_owned())
                .ok_or(ApplicationClientError::InvalidResponse)?;
            Ok(ApplicationLiveQueryUpdate::Reset {
                reason,
                result: raise_live_result(value.result)?,
                cursor: checkpoint.cursor,
                history_incarnation: checkpoint.history_incarnation,
                application_head: checkpoint.application_head,
            })
        }
        v1::live_query_update::Update::Checkpoint(value) => Ok(
            ApplicationLiveQueryUpdate::Checkpoint(live_checkpoint(value.frontier, value.cursor)?),
        ),
        v1::live_query_update::Update::Terminal(value) => {
            let reason = v1::LiveQueryTerminalReason::try_from(value.reason)
                .ok()
                .filter(|reason| *reason != v1::LiveQueryTerminalReason::Unspecified)
                .map(|reason| reason.as_str_name().to_owned())
                .ok_or(ApplicationClientError::InvalidResponse)?;
            let last_frontier = value
                .last_frontier
                .map(|frontier| (frontier.history_incarnation, frontier.application_head));
            Ok(ApplicationLiveQueryUpdate::Terminal(LiveQueryTerminal {
                reason,
                last_frontier,
            }))
        }
    }
}

fn live_checkpoint(
    frontier: Option<v1::LiveQueryFrontier>,
    cursor: Vec<u8>,
) -> Result<LiveQueryCheckpoint, ApplicationClientError> {
    let frontier = frontier.ok_or(ApplicationClientError::InvalidResponse)?;
    if frontier.history_incarnation == 0 {
        return Err(ApplicationClientError::InvalidResponse);
    }
    Ok(LiveQueryCheckpoint {
        history_incarnation: frontier.history_incarnation,
        application_head: frontier.application_head,
        cursor: LiveQueryCursor::new(cursor)
            .map_err(|_| ApplicationClientError::InvalidResponse)?,
    })
}

fn raise_live_result(
    result: Option<v1::LiveQueryResult>,
) -> Result<NamedQueryResult, ApplicationClientError> {
    let result = result.ok_or(ApplicationClientError::InvalidResponse)?;
    let identity = result
        .identity
        .ok_or(ApplicationClientError::InvalidResponse)?;
    if result.outcome.is_empty() {
        return Err(ApplicationClientError::InvalidResponse);
    }
    let mut fields = BTreeMap::new();
    for field in result.fields {
        let cardinality = match v1::LiveQueryResultCardinality::try_from(field.cardinality).ok() {
            Some(v1::LiveQueryResultCardinality::One) => ApplicationCardinality::One,
            Some(v1::LiveQueryResultCardinality::Maybe) => ApplicationCardinality::Maybe,
            Some(v1::LiveQueryResultCardinality::Many) => ApplicationCardinality::Many,
            Some(v1::LiveQueryResultCardinality::Unspecified) | None => {
                return Err(ApplicationClientError::InvalidResponse);
            }
        };
        let records = field
            .records
            .into_iter()
            .map(raise_live_record)
            .collect::<Result<Vec<_>, _>>()?;
        if field.name.is_empty()
            || fields
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
    let contract_bundle_hash = identity
        .contract_bundle_hash
        .try_into()
        .map_err(|_| ApplicationClientError::InvalidResponse)?;
    let module_hash = identity
        .query_module_hash
        .try_into()
        .map_err(|_| ApplicationClientError::InvalidResponse)?;
    let plan_hash = identity
        .query_plan_hash
        .try_into()
        .map_err(|_| ApplicationClientError::InvalidResponse)?;
    Ok(NamedQueryResult {
        outcome: result.outcome,
        application_head: 0,
        fields,
        next_cursor: None,
        identity: QueryResponseIdentity {
            contract_lineage: identity.contract_lineage,
            contract_version: identity.contract_version,
            contract_bundle_hash,
            module_hash,
            query_name: identity.query_name,
            plan_hash,
        },
    })
}

fn raise_live_record(
    record: v1::LiveQueryResultRecord,
) -> Result<ApplicationRecord, ApplicationClientError> {
    Ok(ApplicationRecord {
        entity: record.entity,
        fields: raise_value_record(record.fields)?,
    })
}

fn raise_value_record(
    record: Option<v1::ValueRecord>,
) -> Result<BTreeMap<String, ApplicationValue>, ApplicationClientError> {
    let mut output = BTreeMap::new();
    for field in record
        .ok_or(ApplicationClientError::InvalidResponse)?
        .fields
    {
        if field.name.is_empty()
            || output
                .insert(
                    field.name,
                    raise_value(field.value.ok_or(ApplicationClientError::InvalidResponse)?)?,
                )
                .is_some()
        {
            return Err(ApplicationClientError::InvalidResponse);
        }
    }
    Ok(output)
}

fn raise_patch_operation(
    operation: v1::LiveQueryPatchOperation,
) -> Result<LiveQueryPatchOperation, ApplicationClientError> {
    match operation
        .operation
        .ok_or(ApplicationClientError::InvalidResponse)?
    {
        v1::live_query_patch_operation::Operation::Insert(value) => {
            Ok(LiveQueryPatchOperation::Insert {
                index: value.index,
                record: raise_live_record(
                    value
                        .record
                        .ok_or(ApplicationClientError::InvalidResponse)?,
                )?,
            })
        }
        v1::live_query_patch_operation::Operation::Remove(value) => {
            Ok(LiveQueryPatchOperation::Remove {
                index: value.index,
                key: raise_value_record(value.key)?,
            })
        }
        v1::live_query_patch_operation::Operation::Replace(value) => {
            Ok(LiveQueryPatchOperation::Replace {
                index: value.index,
                record: raise_live_record(
                    value
                        .record
                        .ok_or(ApplicationClientError::InvalidResponse)?,
                )?,
            })
        }
        v1::live_query_patch_operation::Operation::Move(value) => {
            Ok(LiveQueryPatchOperation::Move {
                from: value.from,
                to: value.to,
                key: raise_value_record(value.key)?,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consumer_names_and_limits_fail_closed() {
        let operation = ApplicationReactiveOperation::new([1; 32], "Events", BTreeMap::new())
            .expect("operation");
        assert!(ApplicationEventConsumer::new(operation.clone(), "worker_1").is_ok());
        assert!(ApplicationEventConsumer::new(operation, "not valid").is_err());
        assert!(
            EventConsumerOptions {
                batch_limit: 65,
                ..EventConsumerOptions::default()
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn cursor_is_opaque_and_bounded() {
        assert!(LiveQueryCursor::new(Vec::new()).is_err());
        assert!(LiveQueryCursor::new(vec![7; 16_385]).is_err());
        assert_eq!(
            LiveQueryCursor::new(vec![7; 32])
                .expect("cursor")
                .as_bytes(),
            &[7; 32]
        );
        let cursor = LiveQueryCursor::new(vec![1, 2, 3, 4]).expect("cursor");
        assert_eq!(cursor.to_standard_base64(), "AQIDBA==");
        assert_eq!(
            LiveQueryCursor::from_standard_base64("AQIDBA==")
                .expect("canonical Base64")
                .as_bytes(),
            &[1, 2, 3, 4]
        );
        for invalid in ["AQIDBA", "AQIDBA=", "AQIDBA__", "AZ==", "A==="] {
            assert!(LiveQueryCursor::from_standard_base64(invalid).is_err());
        }
    }

    #[test]
    fn selected_event_fields_reject_duplicates() {
        let event = v1::ConsumedEvent {
            event: Some(v1::SymbolicEvent {
                event_id: Some(v1::EventId {
                    commit_sequence: 1,
                    event_ordinal: 0,
                }),
                event_name: "Changed".to_owned(),
                writer_contract_version: 1,
                command_name: "Change".to_owned(),
                actor_kind: v1::ActorKind::Service as i32,
                provenance_uri: "riffdb://provenance/x".to_owned(),
                history_incarnation: 1,
                fields: vec![
                    v1::SymbolicEventField {
                        name: "value".to_owned(),
                        value: Some(v1::Value {
                            kind: Some(v1::value::Kind::I64Value(1)),
                        }),
                    },
                    v1::SymbolicEventField {
                        name: "value".to_owned(),
                        value: Some(v1::Value {
                            kind: Some(v1::value::Kind::I64Value(2)),
                        }),
                    },
                ],
                ..v1::SymbolicEvent::default()
            }),
            attempt: 1,
            lease_token: vec![1; 32],
            expires_at: Some(v1::Timestamp {
                seconds: 1,
                nanos: 0,
            }),
        };
        assert!(matches!(
            raise_event_delivery(event),
            Err(ApplicationClientError::InvalidResponse)
        ));
    }
}
