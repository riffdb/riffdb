//! API-neutral durable event-consumer requests and orchestration.

use std::collections::BTreeMap;
use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::{Duration, Instant};

use riffdb_catalog::{ResolvedReactiveEventStream, SymbolicEventEnvelope};
use riffdb_contract_ir::ExecutionClass;
use riffdb_errors::PublicError;
use riffdb_policy::{
    AuditClass, AuthorizedOperation, CommandExecutionClass, Decision, EventConsumerOperationTarget,
    OperationRequest, OperationTenantScope, OutputClassification, PartitionConstraint,
    resolve_authorized_contextual_row_policy_context,
};
use riffdb_query_executor::{QueryExecutionRequest, QueryOwnedSnapshot};
use riffdb_types::{
    CommitSequence, EventConsumerName, EventConsumerRevision, EventDeliveryAttempt, EventId,
    EventLeaseToken, PartitionKey, PartitionKeyHash, QueryParameterHash, ReactiveModuleHash,
    ReactiveOperationName, ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceOperationV1,
    TenantScope, Timestamp, event_consumer_identity_hash,
};

use crate::event_operations::{TailWaitError, wait_for_tail_notification};
use crate::orchestration::{AuditScope, BegunInvocation};
use crate::symbolic_query::query_parameter_hash;
use crate::wait::{ControlledWaitError, wait_with_control};
use crate::{
    AuthoritativeCommitNotification, AuthoritativeCommitSubscriptionRequest,
    AvailableContextualReaction, CommitNotificationSource, ConsumeContextualSubscriptionResult,
    ContextualCausationClaimsV1, ContextualHydration, ContextualWorkItem, ExecuteCommandResult,
    ExecuteContextualReactionRequest, InternalDefect, PageLimit, PortAdmissionError,
    PortDriverStopped, QueryParameters, ReactionIdempotencyValue, RequestContext, RiffDbService,
    RiffDbServiceInner, ServiceAuditTargetMap, ServiceDtoError, ServiceFailure, ServiceFuture,
    ServiceResult, SubmittedFieldIdentity,
};

/// Maximum selected events retained while coordinating one consumer transition.
///
/// One item beyond the maximum batch is retained so checkpoint advancement and
/// continuation decisions never infer stream adjacency from physical routes.
pub const MAX_CONSUMER_EVENT_WINDOW_ITEMS: u16 = 65;
/// Maximum one-call pull batch.
pub const MAX_CONSUMER_BATCH_ITEMS: u8 = 64;
/// Maximum live leases selected by one consumer.
pub const MAX_CONSUMER_IN_FLIGHT_ITEMS: u8 = 64;
/// Default live-lease bound.
pub const DEFAULT_CONSUMER_IN_FLIGHT_ITEMS: u8 = 16;
/// Minimum attempt lease.
pub const MIN_CONSUMER_LEASE: Duration = Duration::from_secs(5);
/// Maximum attempt lease.
pub const MAX_CONSUMER_LEASE: Duration = Duration::from_secs(900);
/// Default attempt lease.
pub const DEFAULT_CONSUMER_LEASE: Duration = Duration::from_secs(60);
/// Maximum long-poll wait.
pub const MAX_CONSUMER_WAIT: Duration = Duration::from_secs(30);
/// Maximum caller-selected retry delay.
pub const MAX_CONSUMER_RETRY_DELAY: Duration = Duration::from_secs(3_600);

/// Exact symbolic durable-consumer selection shared by every operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventConsumerSelection {
    module_hash: ReactiveModuleHash,
    operation_name: ReactiveOperationName,
    parameters: QueryParameters,
    consumer_name: EventConsumerName,
}

impl EventConsumerSelection {
    /// Joins one immutable module operation, canonical parameters, and consumer name.
    #[must_use]
    pub const fn new(
        module_hash: ReactiveModuleHash,
        operation_name: ReactiveOperationName,
        parameters: QueryParameters,
        consumer_name: EventConsumerName,
    ) -> Self {
        Self {
            module_hash,
            operation_name,
            parameters,
            consumer_name,
        }
    }
    /// Immutable reactive module identity.
    #[must_use]
    pub const fn module_hash(&self) -> ReactiveModuleHash {
        self.module_hash
    }
    /// Exact stream operation name.
    #[must_use]
    pub const fn operation_name(&self) -> &ReactiveOperationName {
        &self.operation_name
    }
    /// Canonically name-ordered parameters.
    #[must_use]
    pub const fn parameters(&self) -> &QueryParameters {
        &self.parameters
    }
    /// Application-selected durable consumer name.
    #[must_use]
    pub const fn consumer_name(&self) -> &EventConsumerName {
        &self.consumer_name
    }
}

/// One bounded pull/long-poll request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumeEventStreamRequest {
    selection: EventConsumerSelection,
    batch_limit: u8,
    in_flight_limit: u8,
    lease: Duration,
    maximum_wait: Duration,
}

impl ConsumeEventStreamRequest {
    /// Checks all fixed delivery and wait bounds.
    pub fn new(
        selection: EventConsumerSelection,
        batch_limit: u8,
        in_flight_limit: u8,
        lease: Duration,
        maximum_wait: Duration,
    ) -> Result<Self, ServiceDtoError> {
        if batch_limit == 0
            || batch_limit > MAX_CONSUMER_BATCH_ITEMS
            || in_flight_limit == 0
            || in_flight_limit > MAX_CONSUMER_IN_FLIGHT_ITEMS
            || !(MIN_CONSUMER_LEASE..=MAX_CONSUMER_LEASE).contains(&lease)
            || maximum_wait > MAX_CONSUMER_WAIT
        {
            return Err(ServiceDtoError::OutOfRange);
        }
        Ok(Self {
            selection,
            batch_limit,
            in_flight_limit,
            lease,
            maximum_wait,
        })
    }
    /// Exact durable consumer selection.
    #[must_use]
    pub const fn selection(&self) -> &EventConsumerSelection {
        &self.selection
    }
    /// Requested batch bound.
    #[must_use]
    pub const fn batch_limit(&self) -> u8 {
        self.batch_limit
    }
    /// Requested live-lease bound.
    #[must_use]
    pub const fn in_flight_limit(&self) -> u8 {
        self.in_flight_limit
    }
    /// Requested attempt lease.
    #[must_use]
    pub const fn lease(&self) -> Duration {
        self.lease
    }
    /// Requested long-poll wait.
    #[must_use]
    pub const fn maximum_wait(&self) -> Duration {
        self.maximum_wait
    }
}

/// One leased selected event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumedEvent {
    event: crate::SymbolicEvent,
    attempt: EventDeliveryAttempt,
    token: EventLeaseToken,
    expires_at: Timestamp,
}

impl ConsumedEvent {
    pub(crate) const fn new(
        event: crate::SymbolicEvent,
        attempt: EventDeliveryAttempt,
        token: EventLeaseToken,
        expires_at: Timestamp,
    ) -> Self {
        Self {
            event,
            attempt,
            token,
            expires_at,
        }
    }
    /// Selected symbolic event payload.
    #[must_use]
    pub const fn event(&self) -> &crate::SymbolicEvent {
        &self.event
    }
    /// Durable attempt number.
    #[must_use]
    pub const fn attempt(&self) -> EventDeliveryAttempt {
        self.attempt
    }
    /// Opaque attempt token; possession is not authority.
    #[must_use]
    pub const fn token(&self) -> EventLeaseToken {
        self.token
    }
    /// Exclusive acknowledgement deadline.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
}

/// Consumer checkpoint released through the shared service.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventConsumerCheckpoint {
    /// No selected event is terminally resolved.
    BeforeFirst,
    /// All selected events through this identity are terminally resolved.
    After(EventId),
}

/// Bounded consumer status safe for public release.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventConsumerStatus {
    revision: EventConsumerRevision,
    checkpoint: EventConsumerCheckpoint,
    history_incarnation: u64,
    live_leases: u8,
    retries: u16,
    dead_letters: u16,
}

impl EventConsumerStatus {
    /// Constructs a lower-port checked status.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub const fn new(
        revision: EventConsumerRevision,
        checkpoint: EventConsumerCheckpoint,
        history_incarnation: u64,
        live_leases: u8,
        retries: u16,
        dead_letters: u16,
    ) -> Self {
        Self {
            revision,
            checkpoint,
            history_incarnation,
            live_leases,
            retries,
            dead_letters,
        }
    }
    /// Current durable revision.
    #[must_use]
    pub const fn revision(&self) -> EventConsumerRevision {
        self.revision
    }
    /// Contiguous selected-event checkpoint.
    #[must_use]
    pub const fn checkpoint(&self) -> EventConsumerCheckpoint {
        self.checkpoint
    }
    /// Restore incarnation.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }
    /// Number of live leases.
    #[must_use]
    pub const fn live_leases(&self) -> u8 {
        self.live_leases
    }
    /// Number of retry rows.
    #[must_use]
    pub const fn retries(&self) -> u16 {
        self.retries
    }
    /// Number of dead letters.
    #[must_use]
    pub const fn dead_letters(&self) -> u16 {
        self.dead_letters
    }
}

/// Successful bounded pull result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumeEventStreamResult {
    events: Vec<ConsumedEvent>,
    status: EventConsumerStatus,
    wait_timed_out: bool,
}

impl ConsumeEventStreamResult {
    pub(crate) fn new(
        events: Vec<ConsumedEvent>,
        status: EventConsumerStatus,
        wait_timed_out: bool,
    ) -> Self {
        Self {
            events,
            status,
            wait_timed_out,
        }
    }
    /// Leased events in selected stream order.
    #[must_use]
    pub fn events(&self) -> &[ConsumedEvent] {
        &self.events
    }
    /// Post-operation durable status.
    #[must_use]
    pub const fn status(&self) -> &EventConsumerStatus {
        &self.status
    }
    /// Whether the bounded long poll elapsed without a lease.
    #[must_use]
    pub const fn wait_timed_out(&self) -> bool {
        self.wait_timed_out
    }
}

/// Exact lease identity supplied to acknowledge or negatively acknowledge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventConsumerLeaseSelection {
    selection: EventConsumerSelection,
    event_id: EventId,
    token: EventLeaseToken,
    history_incarnation: u64,
}

impl EventConsumerLeaseSelection {
    /// Joins the exact authorized consumer and attempt identity.
    pub fn new(
        selection: EventConsumerSelection,
        event_id: EventId,
        token: EventLeaseToken,
        history_incarnation: u64,
    ) -> Result<Self, ServiceDtoError> {
        if history_incarnation == 0 {
            return Err(ServiceDtoError::OutOfRange);
        }
        Ok(Self {
            selection,
            event_id,
            token,
            history_incarnation,
        })
    }
    /// Exact consumer selection.
    #[must_use]
    pub const fn selection(&self) -> &EventConsumerSelection {
        &self.selection
    }
    /// Leased event.
    #[must_use]
    pub const fn event_id(&self) -> EventId {
        self.event_id
    }
    /// Attempt token.
    #[must_use]
    pub const fn token(&self) -> EventLeaseToken {
        self.token
    }
    /// Restore fence carried by the token response.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }
}

/// One negative acknowledgement with bounded retry delay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NegativeAcknowledgeEventStreamRequest {
    lease: EventConsumerLeaseSelection,
    retry_delay: Duration,
}

impl NegativeAcknowledgeEventStreamRequest {
    /// Checks the retry delay.
    pub fn new(
        lease: EventConsumerLeaseSelection,
        retry_delay: Duration,
    ) -> Result<Self, ServiceDtoError> {
        if retry_delay > MAX_CONSUMER_RETRY_DELAY {
            return Err(ServiceDtoError::OutOfRange);
        }
        Ok(Self { lease, retry_delay })
    }
    /// Exact lease selection.
    #[must_use]
    pub const fn lease(&self) -> &EventConsumerLeaseSelection {
        &self.lease
    }
    /// Retry delay after the observed nack instant.
    #[must_use]
    pub const fn retry_delay(&self) -> Duration {
        self.retry_delay
    }
}

/// Authorized checkpoint movement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SeekEventStreamConsumerRequest {
    selection: EventConsumerSelection,
    checkpoint: EventConsumerCheckpoint,
}

impl SeekEventStreamConsumerRequest {
    /// Joins an exact consumer and selected-event checkpoint.
    #[must_use]
    pub const fn new(
        selection: EventConsumerSelection,
        checkpoint: EventConsumerCheckpoint,
    ) -> Self {
        Self {
            selection,
            checkpoint,
        }
    }
    /// Exact consumer selection.
    #[must_use]
    pub const fn selection(&self) -> &EventConsumerSelection {
        &self.selection
    }
    /// Requested checkpoint.
    #[must_use]
    pub const fn checkpoint(&self) -> EventConsumerCheckpoint {
        self.checkpoint
    }
}

/// Closed mutation result common to acknowledgement, nack, seek, and retirement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventConsumerMutationResult {
    /// The exact transition committed.
    Applied,
    /// The consumer changed concurrently; retry with fresh state.
    StateChanged,
    /// The consumer does not exist.
    NotFound,
    /// A live lease blocks seek or retirement.
    OutstandingLease,
    /// The lease token is stale or already terminal.
    StaleLease,
    /// The attempt expired before this operation.
    LeaseExpired,
}

/// Narrow lower-port exact identity with no storage record types.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventConsumerPortIdentity {
    /// Immutable module identity.
    pub module_hash: ReactiveModuleHash,
    /// Exact operation name.
    pub operation_name: ReactiveOperationName,
    /// Canonical parameter identity.
    pub parameter_hash: QueryParameterHash,
    /// Consumer name.
    pub consumer_name: EventConsumerName,
}

/// One closed request crossing the consumer-owned lower port.
pub enum EventConsumerPortRequest {
    /// Inspect status without mutation.
    Inspect {
        /// Exact immutable consumer identity.
        identity: EventConsumerPortIdentity,
    },
    /// Validate one exact live lease without mutating durable consumer state.
    ValidateLease {
        /// Exact immutable consumer identity.
        identity: EventConsumerPortIdentity,
        /// Catalog-resolved stream partition.
        partition_hash: PartitionKeyHash,
        /// Leased event.
        event_id: EventId,
        /// Exact attempt number.
        attempt: EventDeliveryAttempt,
        /// Attempt-specific opaque token.
        token: EventLeaseToken,
        /// Restore fence.
        history_incarnation: u64,
        /// Canonical current time used for exclusive-expiry validation.
        observed_at: Timestamp,
    },
    /// Lease a catalog-selected window.
    Lease {
        /// Exact immutable consumer identity.
        identity: EventConsumerPortIdentity,
        /// Canonical selected-stream partition.
        partition_hash: PartitionKeyHash,
        /// Authoritative history incarnation observed for the selection.
        history_incarnation: u64,
        /// Canonical lease transition time.
        observed_at: Timestamp,
        /// Exclusive lease expiration.
        expires_at: Timestamp,
        /// Catalog-selected events in exact delivery order.
        selected_events: Vec<EventId>,
        /// Fresh opaque tokens corresponding one-to-one with selected events.
        tokens: Vec<EventLeaseToken>,
        /// Maximum events that may be returned by this lease operation.
        batch_limit: u8,
        /// Maximum concurrent live leases for this consumer.
        in_flight_limit: u8,
    },
    /// Acknowledge with selected-prefix evidence.
    Acknowledge {
        /// Exact immutable consumer identity.
        identity: EventConsumerPortIdentity,
        /// Event whose lease is acknowledged.
        event_id: EventId,
        /// Opaque token proving ownership of the live lease.
        token: EventLeaseToken,
        /// Authoritative history incarnation used for prefix validation.
        history_incarnation: u64,
        /// Canonical acknowledgement transition time.
        observed_at: Timestamp,
        /// Catalog-proven selected prefix ending at the acknowledged event.
        selected_prefix: Vec<EventId>,
    },
    /// Negative acknowledgement with selected-prefix evidence.
    NegativeAcknowledge {
        /// Exact immutable consumer identity.
        identity: EventConsumerPortIdentity,
        /// Event whose lease is negatively acknowledged.
        event_id: EventId,
        /// Opaque token proving ownership of the live lease.
        token: EventLeaseToken,
        /// Canonical negative-acknowledgement transition time.
        observed_at: Timestamp,
        /// Earliest canonical instant at which redelivery is eligible.
        eligible_at: Timestamp,
        /// Catalog-proven selected prefix ending at the rejected event.
        selected_prefix: Vec<EventId>,
    },
    /// Seek after service-side stream membership proof.
    Seek {
        /// Exact immutable consumer identity.
        identity: EventConsumerPortIdentity,
        /// Proven selected-stream checkpoint target.
        checkpoint: EventConsumerCheckpoint,
    },
    /// Retire one exact consumer.
    Retire {
        /// Exact immutable consumer identity.
        identity: EventConsumerPortIdentity,
    },
}

impl std::fmt::Debug for EventConsumerPortRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("EventConsumerPortRequest([REDACTED])")
    }
}

/// One payload-free lower-port lease.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventConsumerPortLease {
    /// Selected event.
    pub event_id: EventId,
    /// Durable attempt.
    pub attempt: EventDeliveryAttempt,
    /// Opaque token.
    pub token: EventLeaseToken,
    /// Exclusive acknowledgement deadline.
    pub expires_at: Timestamp,
}

/// Closed response from the consumer-owned lower port.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EventConsumerPortResponse {
    /// Optional consumer status.
    Status(Option<EventConsumerStatus>),
    /// Exact read-only lease validation result.
    LeaseValidation(EventConsumerLeaseValidation),
    /// Lease result and required post-operation status.
    Leased {
        /// Closed transition result.
        result: EventConsumerMutationResult,
        /// Newly durable leases, without event payloads.
        leases: Vec<EventConsumerPortLease>,
        /// Post-operation consumer status when the consumer exists.
        status: Option<EventConsumerStatus>,
    },
    /// Mutation result.
    Mutated(EventConsumerMutationResult),
}

/// Closed public-service classification of exact lease validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventConsumerLeaseValidation {
    /// Every requested fence matches a currently live attempt.
    Live,
    /// The consumer does not exist.
    NotFound,
    /// The attempt identity is stale or no longer leased.
    Stale,
    /// The exact lease expired.
    Expired,
}

/// Closed lower-port failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventConsumerPortError {
    /// Bounded durable operation was unavailable.
    Unavailable,
    /// Exact identity or durable semantic evidence disagreed.
    Integrity,
}

/// Closed injected clock failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventConsumerClockError;

/// Service-owned time source boundary for consumer transitions.
pub trait EventConsumerClock: Send + Sync {
    /// Samples one canonical transition instant.
    fn now(&self) -> Result<Timestamp, EventConsumerClockError>;
}

/// Closed injected token-source failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventLeaseTokenSourceError;

/// Service-owned entropy boundary for attempt tokens.
pub trait EventLeaseTokenSource: Send + Sync {
    /// Generates one fresh opaque 32-byte token.
    fn generate(&self) -> Result<EventLeaseToken, EventLeaseTokenSourceError>;
}

/// API-neutral durable event-consumer application surface.
pub trait EventConsumerServiceApplication: Send + Sync {
    /// Pulls one bounded leased batch, optionally waiting for a commit notification.
    fn consume_event_stream(
        &self,
        context: RequestContext,
        request: ConsumeEventStreamRequest,
    ) -> ServiceFuture<'_, ConsumeEventStreamResult>;

    /// Acknowledges one exact live attempt.
    fn acknowledge_event_stream(
        &self,
        context: RequestContext,
        request: EventConsumerLeaseSelection,
    ) -> ServiceFuture<'_, EventConsumerMutationResult>;

    /// Releases or dead-letters one exact live attempt.
    fn negative_acknowledge_event_stream(
        &self,
        context: RequestContext,
        request: NegativeAcknowledgeEventStreamRequest,
    ) -> ServiceFuture<'_, EventConsumerMutationResult>;

    /// Moves one consumer checkpoint after exact seek authorization.
    fn seek_event_stream_consumer(
        &self,
        context: RequestContext,
        request: SeekEventStreamConsumerRequest,
    ) -> ServiceFuture<'_, EventConsumerMutationResult>;

    /// Removes one consumer and all of its delivery metadata.
    fn retire_event_stream_consumer(
        &self,
        context: RequestContext,
        selection: EventConsumerSelection,
    ) -> ServiceFuture<'_, EventConsumerMutationResult>;

    /// Reads bounded status for one exact consumer.
    fn get_event_stream_consumer_status(
        &self,
        context: RequestContext,
        selection: EventConsumerSelection,
    ) -> ServiceFuture<'_, Option<EventConsumerStatus>>;
}

impl EventConsumerServiceApplication for RiffDbService {
    fn consume_event_stream(
        &self,
        context: RequestContext,
        request: ConsumeEventStreamRequest,
    ) -> ServiceFuture<'_, ConsumeEventStreamResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::ConsumeEventStream,
            ingress,
            async move {
                consume_stream(
                    service,
                    context,
                    request,
                    ServiceOperationV1::ConsumeEventStream,
                )
                .await
            },
        )
    }

    fn acknowledge_event_stream(
        &self,
        context: RequestContext,
        request: EventConsumerLeaseSelection,
    ) -> ServiceFuture<'_, EventConsumerMutationResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::AcknowledgeEventStream,
            ingress,
            async move {
                acknowledge(
                    service,
                    context,
                    request,
                    None,
                    ServiceOperationV1::AcknowledgeEventStream,
                )
                .await
            },
        )
    }

    fn negative_acknowledge_event_stream(
        &self,
        context: RequestContext,
        request: NegativeAcknowledgeEventStreamRequest,
    ) -> ServiceFuture<'_, EventConsumerMutationResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::NegativeAcknowledgeEventStream,
            ingress,
            async move {
                let delay = request.retry_delay();
                acknowledge(
                    service,
                    context,
                    request.lease,
                    Some(delay),
                    ServiceOperationV1::NegativeAcknowledgeEventStream,
                )
                .await
            },
        )
    }

    fn seek_event_stream_consumer(
        &self,
        context: RequestContext,
        request: SeekEventStreamConsumerRequest,
    ) -> ServiceFuture<'_, EventConsumerMutationResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::SeekEventStreamConsumer,
            ingress,
            async move { seek(service, context, request).await },
        )
    }

    fn retire_event_stream_consumer(
        &self,
        context: RequestContext,
        selection: EventConsumerSelection,
    ) -> ServiceFuture<'_, EventConsumerMutationResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::RetireEventStreamConsumer,
            ingress,
            async move { retire(service, context, selection).await },
        )
    }

    fn get_event_stream_consumer_status(
        &self,
        context: RequestContext,
        selection: EventConsumerSelection,
    ) -> ServiceFuture<'_, Option<EventConsumerStatus>> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::GetEventStreamConsumerStatus,
            ingress,
            async move {
                status(
                    service,
                    context,
                    selection,
                    ServiceOperationV1::GetEventStreamConsumerStatus,
                )
                .await
            },
        )
    }
}

struct PreparedConsumer {
    catalog: riffdb_catalog::ActiveCatalogSnapshot,
    operation: riffdb_query_module::CompiledReactiveOperationV1,
    stream_operation: riffdb_query_module::CompiledReactiveOperationV1,
    stream_parameters: BTreeMap<String, riffdb_types::CanonicalValue>,
    partition: PartitionKey,
    partition_hash: PartitionKeyHash,
    parameter_hash: QueryParameterHash,
    selection: EventConsumerSelection,
}

impl PreparedConsumer {
    fn port_identity(&self) -> EventConsumerPortIdentity {
        EventConsumerPortIdentity {
            module_hash: self.selection.module_hash(),
            operation_name: self.selection.operation_name().clone(),
            parameter_hash: self.parameter_hash,
            consumer_name: self.selection.consumer_name().clone(),
        }
    }

    fn resolve_stream(&self) -> ServiceResult<ResolvedReactiveEventStream> {
        self.catalog
            .resolve_reactive_event_stream(&self.stream_operation, self.stream_parameters.clone())
            .map_err(|_| invalid_consumer_request())
    }

    fn checkpoint_after(&self, status: Option<&EventConsumerStatus>) -> Option<EventId> {
        status.and_then(|value| match value.checkpoint() {
            EventConsumerCheckpoint::BeforeFirst => None,
            EventConsumerCheckpoint::After(event_id) => Some(event_id),
        })
    }
}

async fn prepare_consumer(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    selection: EventConsumerSelection,
    operation: ServiceOperationV1,
) -> ServiceResult<PreparedConsumer> {
    let catalog = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .catalog
            .prepare_active_catalog(context.control()),
    )
    .await
    {
        Ok(Ok(Some(catalog))) => catalog,
        Ok(Ok(None)) | Ok(Err(_)) => return Err(PublicError::storage_unavailable().into()),
        Err(error) => return Err(controlled_failure(error)),
    };
    let contract = catalog.bundle().clone();
    let modules = service
        .providers
        .reactive_modules
        .as_ref()
        .ok_or_else(|| service.internal_failure(operation, InternalDefect::ProofMismatch))?;
    let module = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        modules.prepare_reactive_module(context.control(), contract, selection.module_hash()),
    )
    .await
    {
        Ok(Ok(Some(module))) => module,
        Ok(Ok(None)) => return Err(invalid_consumer_request()),
        Ok(Err(crate::ReactiveModuleReadError::Unavailable)) => {
            return Err(PublicError::storage_unavailable().into());
        }
        Ok(Err(crate::ReactiveModuleReadError::Integrity)) => {
            return Err(service.internal_failure(operation, InternalDefect::ProofMismatch));
        }
        Err(error) => return Err(controlled_failure(error)),
    };
    let compiled = module
        .plan()
        .operation(selection.operation_name().as_str())
        .cloned()
        .ok_or_else(invalid_consumer_request)?;
    let parameters = selection
        .parameters()
        .iter()
        .map(|(name, value)| (name.to_owned(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    let parameter_hash =
        query_parameter_hash(selection.parameters()).ok_or_else(invalid_consumer_request)?;
    let contextual = matches!(
        operation,
        ServiceOperationV1::ConsumeContextualSubscription
            | ServiceOperationV1::AcknowledgeContextualSubscription
            | ServiceOperationV1::NegativeAcknowledgeContextualSubscription
            | ServiceOperationV1::GetContextualSubscriptionStatus
            | ServiceOperationV1::ExecuteContextualReaction
    );
    let (stream_operation, stream_parameters) = match compiled.plan() {
        riffdb_query_module::ReactiveOperationPlanV1::Stream { .. } if !contextual => {
            (compiled.clone(), parameters.clone())
        }
        riffdb_query_module::ReactiveOperationPlanV1::Subscription {
            stream_name,
            stream_hash,
            stream_arguments,
            ..
        } if contextual => {
            let stream_operation = module
                .plan()
                .operation(stream_name.as_str())
                .filter(|stream| stream.identity() == *stream_hash)
                .cloned()
                .ok_or_else(invalid_consumer_request)?;
            let stream_parameters = riffdb_query_module::bind_reactive_arguments(
                stream_arguments,
                &parameters,
                &BTreeMap::new(),
            )
            .map_err(|_| invalid_consumer_request())?;
            (stream_operation, stream_parameters)
        }
        _ => return Err(invalid_consumer_request()),
    };
    let stream = catalog
        .resolve_reactive_event_stream(&stream_operation, stream_parameters.clone())
        .map_err(|_| invalid_consumer_request())?;
    let partition = stream.partition_key().clone();
    let partition_hash = stream.partition_hash();
    Ok(PreparedConsumer {
        catalog,
        operation: compiled,
        stream_operation,
        stream_parameters,
        partition,
        partition_hash,
        parameter_hash,
        selection,
    })
}

fn policy_request(
    prepared: &PreparedConsumer,
    operation: ServiceOperationV1,
    requested_rows: u8,
) -> ServiceResult<OperationRequest> {
    let pointer = prepared.catalog.pointer();
    let target = EventConsumerOperationTarget::new(
        pointer.lineage().clone(),
        pointer.contract_version(),
        pointer.bundle_hash(),
        prepared.selection.module_hash(),
        prepared.selection.operation_name().clone(),
        prepared.parameter_hash,
        prepared.selection.consumer_name().clone(),
        OperationTenantScope::global_only(),
        prepared.partition.clone(),
    );
    Ok(match operation {
        ServiceOperationV1::ConsumeEventStream => OperationRequest::consume_event_stream(
            target,
            NonZeroU16::new(u16::from(requested_rows)).ok_or_else(invalid_consumer_request)?,
        ),
        ServiceOperationV1::AcknowledgeEventStream => {
            OperationRequest::acknowledge_event_stream(target)
        }
        ServiceOperationV1::NegativeAcknowledgeEventStream => {
            OperationRequest::negative_acknowledge_event_stream(target)
        }
        ServiceOperationV1::SeekEventStreamConsumer => {
            OperationRequest::seek_event_stream_consumer(target)
        }
        ServiceOperationV1::RetireEventStreamConsumer => {
            OperationRequest::retire_event_stream_consumer(target)
        }
        ServiceOperationV1::GetEventStreamConsumerStatus => {
            OperationRequest::get_event_stream_consumer_status(target)
        }
        ServiceOperationV1::ConsumeContextualSubscription => {
            OperationRequest::consume_contextual_subscription(
                target,
                NonZeroU16::new(u16::from(requested_rows)).ok_or_else(invalid_consumer_request)?,
            )
        }
        ServiceOperationV1::AcknowledgeContextualSubscription => {
            OperationRequest::acknowledge_contextual_subscription(target)
        }
        ServiceOperationV1::NegativeAcknowledgeContextualSubscription => {
            OperationRequest::negative_acknowledge_contextual_subscription(target)
        }
        ServiceOperationV1::GetContextualSubscriptionStatus => {
            OperationRequest::get_contextual_subscription_status(target)
        }
        ServiceOperationV1::ExecuteContextualReaction => {
            OperationRequest::execute_contextual_reaction(target)
        }
        _ => return Err(invalid_consumer_request()),
    })
}

async fn begin_consumer(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    prepared: &PreparedConsumer,
    request: OperationRequest,
) -> ServiceResult<BegunInvocation> {
    let operation = request.operation();
    let identity_hash = event_consumer_identity_hash(
        service.identity.database_id(),
        prepared.selection.module_hash(),
        prepared.selection.operation_name(),
        prepared.parameter_hash,
        prepared.selection.consumer_name(),
    );
    service
        .begin_invocation(
            context,
            request,
            ServiceAuditTargetMap::event_consumer(
                prepared.catalog.pointer().lineage().clone(),
                prepared.selection.module_hash(),
                prepared.selection.operation_name().clone(),
                identity_hash,
            )
            .map_err(|_| service.internal_failure(operation, InternalDefect::ProofMismatch))?,
            AuditScope::Intrinsic,
        )
        .await
}

async fn inspect_consumer(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    prepared: &PreparedConsumer,
) -> ServiceResult<Option<EventConsumerStatus>> {
    let port = service.providers.event_consumers.as_ref().ok_or_else(|| {
        service.internal_failure(
            begun.initial_authorization().operation(),
            InternalDefect::ProofMismatch,
        )
    })?;
    let permit = wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        port.reserve_event_consumer(context.control()),
    )
    .await
    .map_err(controlled_failure)?
    .map_err(map_admission)?;
    let authorization = begun.reauthorize(service, context).await?;
    ensure_consumer_authorization(
        service,
        &authorization,
        begun.initial_authorization().operation(),
        prepared,
    )?;
    let receipt = permit
        .submit(EventConsumerPortRequest::Inspect {
            identity: prepared.port_identity(),
        })
        .map_err(map_admission)?;
    match receipt.completion().await {
        Ok(Ok(EventConsumerPortResponse::Status(status))) => Ok(status),
        Ok(Ok(_)) | Ok(Err(EventConsumerPortError::Integrity)) => Err(service.internal_failure(
            begun.initial_authorization().operation(),
            InternalDefect::ProofMismatch,
        )),
        Ok(Err(EventConsumerPortError::Unavailable)) | Err(PortDriverStopped) => {
            Err(PublicError::storage_unavailable().into())
        }
    }
}

async fn read_window(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    prepared: &PreparedConsumer,
    after: Option<EventId>,
) -> ServiceResult<AuthoritativeReactiveEventWindow> {
    let permit = wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .authoritative
            .reserve_reactive_event_window(context.control()),
    )
    .await
    .map_err(controlled_failure)?
    .map_err(map_admission)?;
    let authorization = begun.reauthorize(service, context).await?;
    ensure_consumer_authorization(
        service,
        &authorization,
        begun.initial_authorization().operation(),
        prepared,
    )?;
    let request = AuthoritativeReactiveEventWindowRequest::new(
        prepared.resolve_stream()?,
        after,
        MAX_CONSUMER_EVENT_WINDOW_ITEMS,
        service.identity.history_incarnation(),
    )
    .ok_or_else(invalid_consumer_request)?;
    let receipt = permit.submit(request).map_err(map_admission)?;
    match receipt.completion().await {
        Ok(Ok(window)) => Ok(window),
        Ok(Err(crate::AuthoritativeReadError::Cancelled)) => Err(ServiceFailure::Cancelled),
        Ok(Err(crate::AuthoritativeReadError::DeadlineExceeded)) => {
            Err(ServiceFailure::DeadlineExceeded)
        }
        Ok(Err(crate::AuthoritativeReadError::Unavailable)) | Err(PortDriverStopped) => {
            Err(PublicError::storage_unavailable().into())
        }
        // Retired history is a correct-request client outcome (RDB-HISTORY-0102),
        // never an internal defect. Registered consumers fence prune through
        // consumer_low_water, but the window position is client-chosen: a
        // consumer registered after a prune (or seeking an explicit stale
        // checkpoint) can always resolve below the retention watermark.
        Ok(Err(crate::AuthoritativeReadError::HistoryPruned)) => {
            Err(PublicError::history_pruned().into())
        }
        Ok(Err(_)) => Err(service.internal_failure(
            begun.initial_authorization().operation(),
            InternalDefect::ProofMismatch,
        )),
    }
}

async fn submit_consumer_mutation(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    prepared: &PreparedConsumer,
    request: EventConsumerPortRequest,
) -> ServiceResult<EventConsumerPortResponse> {
    let port = service.providers.event_consumers.as_ref().ok_or_else(|| {
        service.internal_failure(
            begun.initial_authorization().operation(),
            InternalDefect::ProofMismatch,
        )
    })?;
    let permit = wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        port.reserve_event_consumer(context.control()),
    )
    .await
    .map_err(controlled_failure)?
    .map_err(map_admission)?;
    let authorization = begun.reauthorize(service, context).await?;
    ensure_consumer_authorization(
        service,
        &authorization,
        begun.initial_authorization().operation(),
        prepared,
    )?;
    let receipt = permit.submit(request).map_err(map_admission)?;
    match receipt.completion().await {
        Ok(Ok(response)) => Ok(response),
        Ok(Err(EventConsumerPortError::Unavailable)) | Err(PortDriverStopped) => {
            Err(PublicError::storage_unavailable().into())
        }
        Ok(Err(EventConsumerPortError::Integrity)) => Err(service.internal_failure(
            begun.initial_authorization().operation(),
            InternalDefect::ProofMismatch,
        )),
    }
}

enum ConsumerDispatchResult {
    Stream(ConsumeEventStreamResult),
    Contextual(ConsumeContextualSubscriptionResult),
}

async fn consume_stream(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: ConsumeEventStreamRequest,
    operation: ServiceOperationV1,
) -> ServiceResult<ConsumeEventStreamResult> {
    match consume_dispatch(service, context, request, operation).await? {
        ConsumerDispatchResult::Stream(result) => Ok(result),
        ConsumerDispatchResult::Contextual(_) => Err(invalid_consumer_request()),
    }
}

async fn consume_dispatch(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: ConsumeEventStreamRequest,
    operation: ServiceOperationV1,
) -> ServiceResult<ConsumerDispatchResult> {
    const MAX_STATE_CHANGE_RETRIES: u8 = 8;
    const MAX_NOTIFICATIONS: u16 = 256;
    let prepared = prepare_consumer(&service, &context, request.selection, operation).await?;
    let (batch_limit, in_flight_limit, lease) = match prepared.operation.plan() {
        riffdb_query_module::ReactiveOperationPlanV1::Subscription { limits, .. } => (
            limits.batch(),
            limits.in_flight(),
            Duration::from_secs(u64::from(limits.lease_seconds())),
        ),
        riffdb_query_module::ReactiveOperationPlanV1::Stream { .. } => {
            (request.batch_limit, request.in_flight_limit, request.lease)
        }
        riffdb_query_module::ReactiveOperationPlanV1::Watch { .. } => {
            return Err(invalid_consumer_request());
        }
    };
    let begun = begin_consumer(
        &service,
        &context,
        &prepared,
        policy_request(&prepared, operation, batch_limit)?,
    )
    .await?;
    let initial_status = match inspect_consumer(&service, &context, &begun, &prepared).await {
        Ok(status) => status,
        Err(failure) => return Err(finish_failure(&service, &context, &begun, failure).await),
    };
    let _subscriber_lease = if request.maximum_wait.is_zero() {
        None
    } else {
        match service.reserve_commit_subscriber() {
            Ok(lease) => Some(lease),
            Err(_) => {
                return Err(finish_failure(
                    &service,
                    &context,
                    &begun,
                    PublicError::storage_unavailable().into(),
                )
                .await);
            }
        }
    };
    let mut notification_source = if request.maximum_wait.is_zero() {
        None
    } else {
        let after = prepared
            .checkpoint_after(initial_status.as_ref())
            .map(EventId::commit_sequence);
        match establish_consumer_notification_source(
            &service, &context, &begun, &prepared, after, operation,
        )
        .await
        {
            Ok(source) => Some(source),
            Err(failure) => {
                return Err(finish_failure(&service, &context, &begun, failure).await);
            }
        }
    };
    let wait_deadline = if request.maximum_wait.is_zero() {
        None
    } else {
        Some(
            Instant::now()
                .checked_add(request.maximum_wait)
                .ok_or_else(|| {
                    service.internal_failure(operation, InternalDefect::ProofMismatch)
                })?,
        )
    };
    let mut state_changes = 0_u8;
    let mut notifications = 0_u16;
    loop {
        let status = match inspect_consumer(&service, &context, &begun, &prepared).await {
            Ok(status) => status,
            Err(failure) => {
                return Err(finish_failure(&service, &context, &begun, failure).await);
            }
        };
        let window = match read_window(
            &service,
            &context,
            &begun,
            &prepared,
            prepared.checkpoint_after(status.as_ref()),
        )
        .await
        {
            Ok(window) => window,
            Err(failure) => {
                return Err(finish_failure(&service, &context, &begun, failure).await);
            }
        };
        let window_had_events = !window.events().is_empty();
        let observed_at = service
            .providers
            .consumer_clock
            .as_ref()
            .ok_or_else(|| service.internal_failure(operation, InternalDefect::ProofMismatch))?
            .now()
            .map_err(|_| PublicError::storage_unavailable())?;
        let expires_at = add_duration(observed_at, lease)
            .ok_or_else(|| service.internal_failure(operation, InternalDefect::ProofMismatch))?;
        let token_source = service
            .providers
            .event_lease_tokens
            .as_ref()
            .ok_or_else(|| service.internal_failure(operation, InternalDefect::ProofMismatch))?;
        let tokens = (0..batch_limit)
            .map(|_| token_source.generate())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| PublicError::storage_unavailable())?;
        let event_ids = window
            .events()
            .iter()
            .map(SymbolicEventEnvelope::event_id)
            .collect();
        let response = submit_consumer_mutation(
            &service,
            &context,
            &begun,
            &prepared,
            EventConsumerPortRequest::Lease {
                identity: prepared.port_identity(),
                partition_hash: prepared.partition_hash,
                history_incarnation: service.identity.history_incarnation(),
                observed_at,
                expires_at,
                selected_events: event_ids,
                tokens,
                batch_limit,
                in_flight_limit,
            },
        )
        .await;
        let response = match response {
            Ok(response) => response,
            Err(failure) => {
                return Err(finish_failure(&service, &context, &begun, failure).await);
            }
        };
        let EventConsumerPortResponse::Leased {
            result,
            leases,
            status,
        } = response
        else {
            let failure = service.internal_failure(operation, InternalDefect::ProofMismatch);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        };
        if result == EventConsumerMutationResult::StateChanged {
            state_changes = state_changes.saturating_add(1);
            if state_changes <= MAX_STATE_CHANGE_RETRIES {
                continue;
            }
            return Err(finish_failure(
                &service,
                &context,
                &begun,
                PublicError::storage_unavailable().into(),
            )
            .await);
        }
        if result != EventConsumerMutationResult::Applied {
            let failure = service.internal_failure(operation, InternalDefect::ProofMismatch);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
        let status = status
            .ok_or_else(|| service.internal_failure(operation, InternalDefect::ProofMismatch))?;
        let mut events = window.into_events();
        let mut consumed = Vec::with_capacity(leases.len());
        for lease in leases {
            let index = events
                .iter()
                .position(|event| event.event_id() == lease.event_id)
                .ok_or_else(|| {
                    service.internal_failure(operation, InternalDefect::ProofMismatch)
                })?;
            let event = events.remove(index);
            consumed.push(ConsumedEvent::new(
                crate::SymbolicEvent::from_catalog(&event),
                lease.attempt,
                lease.token,
                lease.expires_at,
            ));
        }
        if !consumed.is_empty() || window_had_events || notification_source.is_none() {
            return finalize_consumer_delivery(
                &service, &context, &begun, &prepared, consumed, status, false, operation,
            )
            .await;
        }
        let source = notification_source
            .as_mut()
            .ok_or_else(|| service.internal_failure(operation, InternalDefect::ProofMismatch))?;
        let deadline = wait_deadline
            .ok_or_else(|| service.internal_failure(operation, InternalDefect::ProofMismatch))?;
        match wait_for_tail_notification(
            context.control(),
            service.providers.deadline_scheduler.as_ref(),
            deadline,
            source.next(),
        )
        .await
        {
            Ok(Ok(AuthoritativeCommitNotification::Advanced(_))) => {
                notifications = notifications.saturating_add(1);
                if notifications > MAX_NOTIFICATIONS {
                    return Err(finish_failure(
                        &service,
                        &context,
                        &begun,
                        ServiceFailure::ResponseTooLarge,
                    )
                    .await);
                }
            }
            Err(TailWaitError::MaximumWait) => {
                let authorization = begun.reauthorize(&service, &context).await?;
                ensure_consumer_authorization(&service, &authorization, operation, &prepared)?;
                return finalize_consumer_delivery(
                    &service,
                    &context,
                    &begun,
                    &prepared,
                    Vec::new(),
                    status,
                    true,
                    operation,
                )
                .await;
            }
            Err(TailWaitError::Cancelled) => {
                return Err(finish_controlled(
                    &service,
                    &context,
                    &begun,
                    ControlledWaitError::Cancelled,
                )
                .await);
            }
            Err(TailWaitError::RequestDeadline) => {
                return Err(finish_controlled(
                    &service,
                    &context,
                    &begun,
                    ControlledWaitError::DeadlineExceeded,
                )
                .await);
            }
            Ok(Ok(
                AuthoritativeCommitNotification::Gap { .. }
                | AuthoritativeCommitNotification::Lagged { .. }
                | AuthoritativeCommitNotification::Closed,
            ))
            | Ok(Err(_)) => {
                return Err(finish_failure(
                    &service,
                    &context,
                    &begun,
                    PublicError::storage_unavailable().into(),
                )
                .await);
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn finalize_consumer_delivery(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    prepared: &PreparedConsumer,
    consumed: Vec<ConsumedEvent>,
    status: EventConsumerStatus,
    wait_timed_out: bool,
    operation: ServiceOperationV1,
) -> ServiceResult<ConsumerDispatchResult> {
    if operation == ServiceOperationV1::ConsumeContextualSubscription {
        let authorization = begun.reauthorize(service, context).await?;
        ensure_consumer_authorization(service, &authorization, operation, prepared)?;
        let result = hydrate_contextual_delivery(
            service,
            context,
            prepared,
            &authorization,
            consumed,
            status,
            wait_timed_out,
        )
        .await;
        let result = match result {
            Ok(result) => result,
            Err(failure) => return Err(finish_failure(service, context, begun, failure).await),
        };
        let authorization = begun.reauthorize(service, context).await?;
        ensure_consumer_authorization(service, &authorization, operation, prepared)?;
        finish_success(service, context, begun).await?;
        Ok(ConsumerDispatchResult::Contextual(result))
    } else {
        finish_success(service, context, begun).await?;
        Ok(ConsumerDispatchResult::Stream(
            ConsumeEventStreamResult::new(consumed, status, wait_timed_out),
        ))
    }
}

async fn hydrate_contextual_delivery(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    prepared: &PreparedConsumer,
    authorization: &AuthorizedOperation,
    consumed: Vec<ConsumedEvent>,
    status: EventConsumerStatus,
    wait_timed_out: bool,
) -> ServiceResult<ConsumeContextualSubscriptionResult> {
    let riffdb_query_module::ReactiveOperationPlanV1::Subscription {
        hydrations,
        reactions,
        ..
    } = prepared.operation.plan()
    else {
        return Err(service.internal_failure(
            ServiceOperationV1::ConsumeContextualSubscription,
            InternalDefect::ProofMismatch,
        ));
    };
    if consumed.is_empty() {
        return Ok(ConsumeContextualSubscriptionResult::new(
            Vec::new(),
            status,
            wait_timed_out,
            Arc::clone(prepared.catalog.bundle().enum_variant_names()),
        ));
    }
    let query_modules = service.providers.query_modules.as_ref().ok_or_else(|| {
        service.internal_failure(
            ServiceOperationV1::ConsumeContextualSubscription,
            InternalDefect::ProofMismatch,
        )
    })?;
    let mut compiled_hydrations = Vec::with_capacity(hydrations.len());
    for hydration in hydrations {
        let module = wait_with_control(
            context.control(),
            service.providers.deadline_scheduler.as_ref(),
            query_modules.prepare_query_module(
                context.control(),
                prepared.catalog.bundle().clone(),
                hydration.module_hash(),
            ),
        )
        .await
        .map_err(controlled_failure)?
        .map_err(|error| match error {
            crate::QueryModuleReadError::Unavailable => PublicError::storage_unavailable().into(),
            crate::QueryModuleReadError::Integrity => service.internal_failure(
                ServiceOperationV1::ConsumeContextualSubscription,
                InternalDefect::ProofMismatch,
            ),
        })?
        .ok_or_else(|| {
            service.internal_failure(
                ServiceOperationV1::ConsumeContextualSubscription,
                InternalDefect::ProofMismatch,
            )
        })?;
        let query = module
            .module()
            .query(hydration.query_name())
            .filter(|query| query.plan().identity() == hydration.plan_hash())
            .ok_or_else(|| {
                service.internal_failure(
                    ServiceOperationV1::ConsumeContextualSubscription,
                    InternalDefect::ProofMismatch,
                )
            })?;
        let program = query.shared_ordinary_program().ok_or_else(|| {
            service.internal_failure(
                ServiceOperationV1::ConsumeContextualSubscription,
                InternalDefect::ProofMismatch,
            )
        })?;
        compiled_hydrations.push((hydration, program));
    }
    let mut hydration_entities = compiled_hydrations
        .iter()
        .flat_map(|(_, program)| program.steps())
        .map(riffdb_query_ir::QueryAccessStep::internal_entity_id)
        .collect::<Vec<_>>();
    hydration_entities.sort_unstable();
    hydration_entities.dedup();
    let row_policy = resolve_authorized_contextual_row_policy_context(
        authorization,
        prepared.catalog.bundle().bundle(),
        &hydration_entities,
    )
    .map_err(|_| {
        service.internal_failure(
            ServiceOperationV1::ConsumeContextualSubscription,
            InternalDefect::ProofMismatch,
        )
    })?;
    let executor = service.providers.query_executor.as_ref().ok_or_else(|| {
        service.internal_failure(
            ServiceOperationV1::ConsumeContextualSubscription,
            InternalDefect::ProofMismatch,
        )
    })?;
    let selection_parameters = prepared
        .selection
        .parameters()
        .iter()
        .map(|(name, value)| (name.to_owned(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut items = Vec::with_capacity(consumed.len());
    for delivery in consumed {
        let event_fields = delivery
            .event()
            .fields()
            .iter()
            .map(|field| (field.name().to_owned(), field.value().clone()))
            .collect::<BTreeMap<_, _>>();
        let parameters = compiled_hydrations
            .iter()
            .map(|(hydration, _)| {
                riffdb_query_module::bind_reactive_arguments(
                    hydration.arguments(),
                    &selection_parameters,
                    &event_fields,
                )
                .ok()
                .and_then(QueryParameters::checked)
                .ok_or_else(|| {
                    service.internal_failure(
                        ServiceOperationV1::ConsumeContextualSubscription,
                        InternalDefect::ProofMismatch,
                    )
                })
            })
            .collect::<ServiceResult<Vec<_>>>()?;
        let requests = compiled_hydrations
            .iter()
            .zip(&parameters)
            .map(|((_, program), parameters)| {
                QueryExecutionRequest::new(program.as_ref(), parameters)
            })
            .collect::<Vec<_>>();
        let snapshots = match row_policy.as_ref() {
            Some(policy) => executor.execute_policy_query_group(&requests, policy),
            None => executor.execute_query_group(&requests),
        }
        .map_err(|_| {
            service.internal_failure(
                ServiceOperationV1::ConsumeContextualSubscription,
                InternalDefect::ProofMismatch,
            )
        })?;
        let context_head =
            validate_contextual_snapshot_head(&snapshots, delivery.event().event_id()).ok_or_else(
                || {
                    service.internal_failure(
                        ServiceOperationV1::ConsumeContextualSubscription,
                        InternalDefect::ProofMismatch,
                    )
                },
            )?;
        let hydrated = compiled_hydrations
            .iter()
            .zip(snapshots)
            .map(|((dependency, _), snapshot)| {
                ContextualHydration::new(
                    dependency.name().to_owned(),
                    snapshot.outcome().to_owned(),
                    snapshot.into_fields(),
                )
            })
            .collect();
        let available_reactions =
            filter_contextual_reactions(service, context, prepared, &delivery, reactions)?;
        items.push(ContextualWorkItem::new(
            delivery,
            context_head,
            hydrated,
            available_reactions,
        ));
    }
    Ok(ConsumeContextualSubscriptionResult::new(
        items,
        status,
        wait_timed_out,
        Arc::clone(prepared.catalog.bundle().enum_variant_names()),
    ))
}

fn validate_contextual_snapshot_head(
    snapshots: &[QueryOwnedSnapshot],
    event_id: EventId,
) -> Option<CommitSequence> {
    let first = snapshots.first()?.application_head();
    (snapshots
        .iter()
        .all(|snapshot| snapshot.application_head() == first)
        && first >= event_id.commit_sequence().get())
    .then(|| CommitSequence::new(first))
    .flatten()
}

fn filter_contextual_reactions(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    prepared: &PreparedConsumer,
    delivery: &ConsumedEvent,
    reactions: &[riffdb_query_ir::ReactiveCommandDependencyV1],
) -> ServiceResult<Vec<AvailableContextualReaction>> {
    let codec = service
        .providers
        .contextual_causation
        .as_ref()
        .ok_or_else(|| {
            service.internal_failure(
                ServiceOperationV1::ConsumeContextualSubscription,
                InternalDefect::ProofMismatch,
            )
        })?;
    let pointer = prepared.catalog.pointer();
    let mut available = Vec::with_capacity(reactions.len());
    for reaction in reactions {
        let command = prepared
            .catalog
            .bundle()
            .bundle()
            .command(reaction.internal_command_id())
            .ok_or_else(|| {
                service.internal_failure(
                    ServiceOperationV1::ConsumeContextualSubscription,
                    InternalDefect::ProofMismatch,
                )
            })?;
        let class = match command.execution_class() {
            ExecutionClass::ReadOnly => CommandExecutionClass::ReadOnly,
            ExecutionClass::IdempotentMutation => CommandExecutionClass::Mutation,
        };
        let request = OperationRequest::execute_command(
            pointer.lineage().clone(),
            pointer.contract_version(),
            reaction.internal_command_id(),
            class,
            prepared.partition.clone(),
        );
        match service
            .providers
            .policy
            .authorize(context.principal(), request)
        {
            Ok(Decision::Deny(_)) => continue,
            Ok(Decision::Allow(authorization))
                if authorization.database_id() == service.identity.database_id()
                    && authorization.environment() == service.identity.environment()
                    && authorization.operation() == ServiceOperationV1::ExecuteCommand => {}
            Ok(Decision::Allow(_) | Decision::PrepareCapabilityMutation(_)) => {
                return Err(service.internal_failure(
                    ServiceOperationV1::ConsumeContextualSubscription,
                    InternalDefect::ProofMismatch,
                ));
            }
            Err(_) => return Err(PublicError::storage_unavailable().into()),
        }
        let claims = ContextualCausationClaimsV1::new(
            service.identity.database_id(),
            service.identity.history_incarnation(),
            prepared.selection.module_hash(),
            prepared.operation.identity(),
            prepared.selection.operation_name().clone(),
            prepared.parameter_hash,
            prepared.selection.consumer_name().clone(),
            delivery.event().event_id(),
            delivery.attempt(),
            delivery.token(),
            context.principal().principal_id().clone(),
            context.principal().capability_revision(),
            reaction.internal_command_id(),
            delivery.expires_at(),
            delivery.event().root_request_id(),
        )
        .map_err(|_| {
            service.internal_failure(
                ServiceOperationV1::ConsumeContextualSubscription,
                InternalDefect::ProofMismatch,
            )
        })?;
        let token = codec.seal(&claims).map_err(|_| {
            service.internal_failure(
                ServiceOperationV1::ConsumeContextualSubscription,
                InternalDefect::ProofMismatch,
            )
        })?;
        available.push(AvailableContextualReaction::new(
            reaction.reaction_name().to_owned(),
            reaction.command_name().to_owned(),
            reaction.internal_command_id(),
            token,
        ));
    }
    Ok(available)
}

async fn establish_consumer_notification_source(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    prepared: &PreparedConsumer,
    after: Option<riffdb_types::CommitSequence>,
    operation: ServiceOperationV1,
) -> ServiceResult<Box<dyn CommitNotificationSource>> {
    let permit = wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .authoritative
            .reserve_subscribe_to_commits(context.control()),
    )
    .await
    .map_err(controlled_failure)?
    .map_err(map_admission)?;
    let authorization = begun.reauthorize(service, context).await?;
    ensure_consumer_authorization(service, &authorization, operation, prepared)?;
    let request = AuthoritativeCommitSubscriptionRequest::new(after, PageLimit::default())
        .map_err(|_| service.internal_failure(operation, InternalDefect::ProofMismatch))?;
    let receipt = permit.submit(request).map_err(map_admission)?;
    match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        receipt,
    )
    .await
    {
        Ok(Ok(Ok(source))) => Ok(source),
        Ok(Ok(Err(_))) | Ok(Err(PortDriverStopped)) => {
            Err(PublicError::storage_unavailable().into())
        }
        Err(error) => Err(controlled_failure(error)),
    }
}

pub(crate) async fn consume_contextual_events(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    selection: EventConsumerSelection,
    maximum_wait: Duration,
) -> ServiceResult<ConsumeContextualSubscriptionResult> {
    let request = ConsumeEventStreamRequest::new(selection, 1, 1, MIN_CONSUMER_LEASE, maximum_wait)
        .map_err(|_| invalid_consumer_request())?;
    match consume_dispatch(
        service,
        context,
        request,
        ServiceOperationV1::ConsumeContextualSubscription,
    )
    .await?
    {
        ConsumerDispatchResult::Contextual(result) => Ok(result),
        ConsumerDispatchResult::Stream(_) => Err(invalid_consumer_request()),
    }
}

pub(crate) async fn acknowledge_contextual_event(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    lease: EventConsumerLeaseSelection,
    retry_delay: Option<Duration>,
) -> ServiceResult<EventConsumerMutationResult> {
    let operation = if retry_delay.is_some() {
        ServiceOperationV1::NegativeAcknowledgeContextualSubscription
    } else {
        ServiceOperationV1::AcknowledgeContextualSubscription
    };
    acknowledge(service, context, lease, retry_delay, operation).await
}

pub(crate) async fn contextual_consumer_status(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    selection: EventConsumerSelection,
) -> ServiceResult<Option<EventConsumerStatus>> {
    status(
        service,
        context,
        selection,
        ServiceOperationV1::GetContextualSubscriptionStatus,
    )
    .await
}

pub(crate) async fn execute_contextual_reaction_operation(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: ExecuteContextualReactionRequest,
) -> ServiceResult<ExecuteCommandResult> {
    let (selection, token, reaction_name, command_request) = request.into_parts();
    let codec = service
        .providers
        .contextual_causation
        .as_ref()
        .ok_or_else(|| {
            service.internal_failure(
                ServiceOperationV1::ExecuteContextualReaction,
                InternalDefect::ProofMismatch,
            )
        })?;
    let claims = codec
        .open(&token)
        .map_err(|_| PublicError::authorization_denied())?;
    let prepared = prepare_consumer(
        &service,
        &context,
        selection,
        ServiceOperationV1::ExecuteContextualReaction,
    )
    .await?;
    if claims.database_id() != service.identity.database_id()
        || claims.history_incarnation() != service.identity.history_incarnation()
        || claims.module_hash() != prepared.selection.module_hash()
        || claims.operation_hash() != prepared.operation.identity()
        || claims.operation_name() != prepared.selection.operation_name()
        || claims.parameter_hash() != prepared.parameter_hash
        || claims.consumer_name() != prepared.selection.consumer_name()
        || claims.principal_id() != context.principal().principal_id()
        || claims.capability_revision() != context.principal().capability_revision()
    {
        return Err(PublicError::authorization_denied().into());
    }
    let riffdb_query_module::ReactiveOperationPlanV1::Subscription { reactions, .. } =
        prepared.operation.plan()
    else {
        return Err(service.internal_failure(
            ServiceOperationV1::ExecuteContextualReaction,
            InternalDefect::ProofMismatch,
        ));
    };
    let reaction = reactions
        .iter()
        .find(|reaction| reaction.reaction_name() == reaction_name)
        .filter(|reaction| {
            reaction.internal_command_id() == claims.command_id()
                && reaction.command_name() == command_request.command().as_str()
        })
        .ok_or_else(PublicError::authorization_denied)?;
    let command = prepared
        .catalog
        .bundle()
        .bundle()
        .command(reaction.internal_command_id())
        .ok_or_else(|| {
            service.internal_failure(
                ServiceOperationV1::ExecuteContextualReaction,
                InternalDefect::ProofMismatch,
            )
        })?;
    let command_request =
        bind_reaction_idempotency(&claims, &reaction_name, command, command_request)?;
    let begun = begin_consumer(
        &service,
        &context,
        &prepared,
        policy_request(&prepared, ServiceOperationV1::ExecuteContextualReaction, 1)?,
    )
    .await?;
    let observed_at = service
        .providers
        .consumer_clock
        .as_ref()
        .ok_or_else(|| {
            service.internal_failure(
                ServiceOperationV1::ExecuteContextualReaction,
                InternalDefect::ProofMismatch,
            )
        })?
        .now()
        .map_err(|_| PublicError::storage_unavailable())?;
    let validation = submit_consumer_mutation(
        &service,
        &context,
        &begun,
        &prepared,
        EventConsumerPortRequest::ValidateLease {
            identity: prepared.port_identity(),
            partition_hash: prepared.partition_hash,
            event_id: claims.event_id(),
            attempt: claims.attempt(),
            token: claims.lease_token(),
            history_incarnation: claims.history_incarnation(),
            observed_at,
        },
    )
    .await;
    let validation = match validation {
        Ok(EventConsumerPortResponse::LeaseValidation(validation)) => validation,
        Ok(_) => {
            let failure = service.internal_failure(
                ServiceOperationV1::ExecuteContextualReaction,
                InternalDefect::ProofMismatch,
            );
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
        Err(failure) => return Err(finish_failure(&service, &context, &begun, failure).await),
    };
    let admit_new = match validation {
        EventConsumerLeaseValidation::Live if observed_at < claims.expires_at() => true,
        EventConsumerLeaseValidation::Expired if observed_at >= claims.expires_at() => false,
        EventConsumerLeaseValidation::Live
        | EventConsumerLeaseValidation::Expired
        | EventConsumerLeaseValidation::NotFound
        | EventConsumerLeaseValidation::Stale => {
            return Err(finish_failure(
                &service,
                &context,
                &begun,
                PublicError::authorization_denied().into(),
            )
            .await);
        }
    };
    let mode = crate::command_operations::CommandInvocationMode::Contextual {
        causation: crate::command_operations::TrustedCommandCausation {
            causing_event_id: claims.event_id(),
            root_request_id: claims.root_request_id(),
        },
        admit_new,
    };
    let command_context = context.child(
        crate::derive_reaction_request_id(context.request_id(), &claims, &reaction_name)
            .map_err(|_| invalid_consumer_request())?,
    );
    let command = crate::service::observe_inline_operation(
        service.as_ref(),
        ServiceOperationV1::ExecuteCommand,
        command_context.ingress(),
        crate::command_operations::execute_command(
            service.as_ref(),
            &command_context,
            &command_request,
            mode,
        ),
    )
    .await;
    match command {
        Ok(result) => {
            finish_success(&service, &context, &begun).await?;
            Ok(result)
        }
        Err(failure) => Err(finish_failure(&service, &context, &begun, failure).await),
    }
}

pub(crate) fn bind_reaction_idempotency(
    claims: &ContextualCausationClaimsV1,
    reaction_name: &str,
    command: &riffdb_contract_ir::CommandPlan,
    request: crate::ExecuteCommandRequest,
) -> ServiceResult<crate::ExecuteCommandRequest> {
    let target = command
        .idempotency_input()
        .ok_or_else(invalid_consumer_request)?;
    let field = command
        .input()
        .record()
        .field(target)
        .ok_or_else(invalid_consumer_request)?;
    let uuid_input = field.value_type().tag() == riffdb_contract_ir::ValueTypeTag::Uuid;
    let expected = crate::derive_reaction_idempotency(claims, reaction_name, uuid_input)
        .map_err(|_| invalid_consumer_request())?;
    let expected = match expected {
        ReactionIdempotencyValue::String(value) => {
            crate::SubmittedValue::string(value).map_err(|_| invalid_consumer_request())?
        }
        ReactionIdempotencyValue::Uuid(value) => crate::SubmittedValue::Uuid(value),
    };
    let mut matches = 0_u8;
    let mut fields = Vec::with_capacity(request.input().len().saturating_add(1));
    for submitted in request.input().fields() {
        let identity_matches = match submitted.identity() {
            SubmittedFieldIdentity::Id(id) => *id == target,
            SubmittedFieldIdentity::Name(name) => name.as_str() == field.name(),
            SubmittedFieldIdentity::IdAndName { id, name } => {
                *id == target && name.as_str() == field.name()
            }
        };
        if identity_matches {
            matches = matches.saturating_add(1);
            fields.push(crate::SubmittedField::new(
                submitted.identity().clone(),
                expected.clone(),
            ));
        } else {
            fields.push(submitted.clone());
        }
    }
    match matches {
        0 => fields.push(crate::SubmittedField::new(
            SubmittedFieldIdentity::Id(target),
            expected,
        )),
        1 => {}
        _ => return Err(PublicError::authorization_denied().into()),
    }
    let input = crate::SubmittedRecord::new(fields).map_err(|_| invalid_consumer_request())?;
    crate::ExecuteCommandRequest::new(
        request.command().clone(),
        request.expected_contract_version(),
        input,
    )
    .map_err(|_| invalid_consumer_request())
}

async fn acknowledge(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    lease: EventConsumerLeaseSelection,
    retry_delay: Option<Duration>,
    operation: ServiceOperationV1,
) -> ServiceResult<EventConsumerMutationResult> {
    if lease.history_incarnation() != service.identity.history_incarnation() {
        return Err(PublicError::history_incarnation_mismatch().into());
    }
    let event_id = lease.event_id();
    let token = lease.token();
    let history_incarnation = lease.history_incarnation();
    let prepared = prepare_consumer(&service, &context, lease.selection, operation).await?;
    let begun = begin_consumer(
        &service,
        &context,
        &prepared,
        policy_request(&prepared, operation, 1)?,
    )
    .await?;
    let status = match inspect_consumer(&service, &context, &begun, &prepared).await {
        Ok(Some(status)) => status,
        Ok(None) => {
            finish_success(&service, &context, &begun).await?;
            return Ok(EventConsumerMutationResult::NotFound);
        }
        Err(failure) => return Err(finish_failure(&service, &context, &begun, failure).await),
    };
    let window = match read_window(
        &service,
        &context,
        &begun,
        &prepared,
        prepared.checkpoint_after(Some(&status)),
    )
    .await
    {
        Ok(window) => window,
        Err(failure) => return Err(finish_failure(&service, &context, &begun, failure).await),
    };
    let selected_prefix = window
        .events()
        .iter()
        .map(SymbolicEventEnvelope::event_id)
        .collect::<Vec<_>>();
    if !selected_prefix.contains(&event_id) {
        let failure = invalid_consumer_request();
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }
    let observed_at = service
        .providers
        .consumer_clock
        .as_ref()
        .ok_or_else(|| service.internal_failure(operation, InternalDefect::ProofMismatch))?
        .now()
        .map_err(|_| PublicError::storage_unavailable())?;
    let port_request = match retry_delay {
        None => EventConsumerPortRequest::Acknowledge {
            identity: prepared.port_identity(),
            event_id,
            token,
            history_incarnation,
            observed_at,
            selected_prefix,
        },
        Some(delay) => EventConsumerPortRequest::NegativeAcknowledge {
            identity: prepared.port_identity(),
            event_id,
            token,
            observed_at,
            eligible_at: add_duration(observed_at, delay).ok_or_else(|| {
                service.internal_failure(operation, InternalDefect::ProofMismatch)
            })?,
            selected_prefix,
        },
    };
    let response =
        submit_consumer_mutation(&service, &context, &begun, &prepared, port_request).await;
    finish_mutation_response(&service, &context, &begun, response).await
}

async fn seek(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: SeekEventStreamConsumerRequest,
) -> ServiceResult<EventConsumerMutationResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::SeekEventStreamConsumer;
    let prepared = prepare_consumer(&service, &context, request.selection, OPERATION).await?;
    let begun = begin_consumer(
        &service,
        &context,
        &prepared,
        policy_request(&prepared, OPERATION, 1)?,
    )
    .await?;
    if let EventConsumerCheckpoint::After(event_id) = request.checkpoint {
        let after = event_predecessor(event_id);
        let window = match read_window(&service, &context, &begun, &prepared, after).await {
            Ok(window) => window,
            Err(failure) => return Err(finish_failure(&service, &context, &begun, failure).await),
        };
        if window.events().first().map(SymbolicEventEnvelope::event_id) != Some(event_id) {
            let failure = invalid_consumer_request();
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    }
    let response = submit_consumer_mutation(
        &service,
        &context,
        &begun,
        &prepared,
        EventConsumerPortRequest::Seek {
            identity: prepared.port_identity(),
            checkpoint: request.checkpoint,
        },
    )
    .await;
    finish_mutation_response(&service, &context, &begun, response).await
}

async fn retire(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    selection: EventConsumerSelection,
) -> ServiceResult<EventConsumerMutationResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::RetireEventStreamConsumer;
    let prepared = prepare_consumer(&service, &context, selection, OPERATION).await?;
    let begun = begin_consumer(
        &service,
        &context,
        &prepared,
        policy_request(&prepared, OPERATION, 1)?,
    )
    .await?;
    let response = submit_consumer_mutation(
        &service,
        &context,
        &begun,
        &prepared,
        EventConsumerPortRequest::Retire {
            identity: prepared.port_identity(),
        },
    )
    .await;
    finish_mutation_response(&service, &context, &begun, response).await
}

async fn status(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    selection: EventConsumerSelection,
    operation: ServiceOperationV1,
) -> ServiceResult<Option<EventConsumerStatus>> {
    let prepared = prepare_consumer(&service, &context, selection, operation).await?;
    let begun = begin_consumer(
        &service,
        &context,
        &prepared,
        policy_request(&prepared, operation, 1)?,
    )
    .await?;
    let result = inspect_consumer(&service, &context, &begun, &prepared).await;
    match result {
        Ok(status) => {
            finish_success(&service, &context, &begun).await?;
            Ok(status)
        }
        Err(failure) => Err(finish_failure(&service, &context, &begun, failure).await),
    }
}

async fn finish_mutation_response(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    response: ServiceResult<EventConsumerPortResponse>,
) -> ServiceResult<EventConsumerMutationResult> {
    match response {
        Ok(EventConsumerPortResponse::Mutated(result)) => {
            finish_success(service, context, begun).await?;
            Ok(result)
        }
        Ok(_) => {
            let failure = service.internal_failure(
                begun.initial_authorization().operation(),
                InternalDefect::ProofMismatch,
            );
            Err(finish_failure(service, context, begun, failure).await)
        }
        Err(failure) => Err(finish_failure(service, context, begun, failure).await),
    }
}

fn ensure_consumer_authorization(
    service: &RiffDbServiceInner,
    authorization: &AuthorizedOperation,
    operation: ServiceOperationV1,
    prepared: &PreparedConsumer,
) -> ServiceResult<()> {
    let obligations = authorization.obligations();
    let expected_audit = match operation {
        ServiceOperationV1::ConsumeEventStream
        | ServiceOperationV1::GetEventStreamConsumerStatus
        | ServiceOperationV1::ConsumeContextualSubscription
        | ServiceOperationV1::GetContextualSubscriptionStatus => AuditClass::AdministrativeRead,
        ServiceOperationV1::AcknowledgeEventStream
        | ServiceOperationV1::NegativeAcknowledgeEventStream
        | ServiceOperationV1::SeekEventStreamConsumer
        | ServiceOperationV1::RetireEventStreamConsumer
        | ServiceOperationV1::AcknowledgeContextualSubscription
        | ServiceOperationV1::NegativeAcknowledgeContextualSubscription
        | ServiceOperationV1::ExecuteContextualReaction => AuditClass::ControlPlaneMutation,
        _ => return Err(service.internal_failure(operation, InternalDefect::ProofMismatch)),
    };
    let expected_output = if matches!(
        operation,
        ServiceOperationV1::ConsumeEventStream
            | ServiceOperationV1::ConsumeContextualSubscription
            | ServiceOperationV1::ExecuteContextualReaction
    ) {
        OutputClassification::PolicyFilteredApplicationData
    } else {
        OutputClassification::AdministrativeRedactedData
    };
    let exact_partition = matches!(
        obligations.partition_constraint(),
        Some(PartitionConstraint::Exact(partition))
            if partition.lineage() == prepared.catalog.pointer().lineage()
                && partition.partition_key() == &prepared.partition
    );
    if authorization.database_id() != service.identity.database_id()
        || authorization.environment() != service.identity.environment()
        || authorization.operation() != operation
        || obligations.effective_tenant_scope() != &TenantScope::Global
        || !exact_partition
        || obligations.audit_class() != Some(expected_audit)
        || obligations.output_classification() != expected_output
        || obligations.field_mask().is_some()
    {
        return Err(service.internal_failure(operation, InternalDefect::ProofMismatch));
    }
    Ok(())
}

async fn finish_success(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
) -> ServiceResult<()> {
    if begun
        .finish(
            service,
            context,
            ServiceAuditPhaseV1::Succeeded,
            ServiceAuditLinkV1::None,
        )
        .await
        .is_err()
    {
        service.note_audit_failure(begun.initial_authorization().operation());
        return Err(PublicError::storage_unavailable().into());
    }
    Ok(())
}

async fn finish_failure(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    failure: ServiceFailure,
) -> ServiceFailure {
    if begun
        .finish(
            service,
            context,
            ServiceAuditPhaseV1::Failed,
            ServiceAuditLinkV1::None,
        )
        .await
        .is_err()
    {
        service.note_audit_failure(begun.initial_authorization().operation());
        return PublicError::storage_unavailable().into();
    }
    failure
}

async fn finish_controlled(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    error: ControlledWaitError,
) -> ServiceFailure {
    let failure = controlled_failure(error);
    if begun
        .finish(
            service,
            context,
            ServiceAuditPhaseV1::Cancelled,
            ServiceAuditLinkV1::None,
        )
        .await
        .is_err()
    {
        service.note_audit_failure(begun.initial_authorization().operation());
        return PublicError::storage_unavailable().into();
    }
    failure
}

fn add_duration(timestamp: Timestamp, duration: Duration) -> Option<Timestamp> {
    let seconds = i64::try_from(duration.as_secs()).ok()?;
    let nanos = timestamp
        .nanoseconds()
        .checked_add(duration.subsec_nanos())?;
    let carry = i64::from(nanos >= 1_000_000_000);
    Timestamp::new(
        timestamp
            .seconds()
            .checked_add(seconds)?
            .checked_add(carry)?,
        nanos % 1_000_000_000,
    )
    .ok()
}

fn event_predecessor(event_id: EventId) -> Option<EventId> {
    let bytes = event_id.to_be_bytes();
    let mut sequence = [0_u8; 8];
    sequence.copy_from_slice(&bytes[..8]);
    let mut ordinal = [0_u8; 4];
    ordinal.copy_from_slice(&bytes[8..]);
    let sequence = u64::from_be_bytes(sequence);
    let ordinal = u32::from_be_bytes(ordinal);
    if ordinal > 0 {
        EventId::new(riffdb_types::CommitSequence::new(sequence)?, ordinal - 1).into()
    } else if sequence > 1 {
        EventId::new(riffdb_types::CommitSequence::new(sequence - 1)?, u32::MAX).into()
    } else {
        None
    }
}

pub(crate) fn invalid_consumer_request() -> ServiceFailure {
    PublicError::validation(
        riffdb_errors::ValidationIssues::new(vec![riffdb_errors::ValidationIssue::new(
            riffdb_errors::ValidationCode::InvalidValue,
            riffdb_errors::ValidationPath::root(),
        )])
        .expect("one fixed validation issue is valid"),
    )
    .into()
}

fn controlled_failure(error: ControlledWaitError) -> ServiceFailure {
    match error {
        ControlledWaitError::Cancelled => ServiceFailure::Cancelled,
        ControlledWaitError::DeadlineExceeded => ServiceFailure::DeadlineExceeded,
    }
}

fn map_admission(error: PortAdmissionError) -> ServiceFailure {
    match error {
        PortAdmissionError::Cancelled => ServiceFailure::Cancelled,
        PortAdmissionError::DeadlineExceeded => ServiceFailure::DeadlineExceeded,
        PortAdmissionError::Unavailable | PortAdmissionError::Stopped => {
            PublicError::storage_unavailable().into()
        }
    }
}

/// Lower checked request for one frozen catalog-resolved event-stream window.
pub struct AuthoritativeReactiveEventWindowRequest {
    stream: ResolvedReactiveEventStream,
    after: Option<EventId>,
    limit: u16,
    history_incarnation: u64,
}

impl AuthoritativeReactiveEventWindowRequest {
    /// Constructs one bounded request after the selected consumer checkpoint.
    pub fn new(
        stream: ResolvedReactiveEventStream,
        after: Option<EventId>,
        limit: u16,
        history_incarnation: u64,
    ) -> Option<Self> {
        (limit > 0 && limit <= MAX_CONSUMER_EVENT_WINDOW_ITEMS && history_incarnation > 0)
            .then_some(Self {
                stream,
                after,
                limit,
                history_incarnation,
            })
    }

    /// Decomposes this move-only request for a blocking authoritative adapter.
    #[must_use]
    pub fn into_parts(self) -> (ResolvedReactiveEventStream, Option<EventId>, u16, u64) {
        (
            self.stream,
            self.after,
            self.limit,
            self.history_incarnation,
        )
    }
}

impl std::fmt::Debug for AuthoritativeReactiveEventWindowRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AuthoritativeReactiveEventWindowRequest([CHECKED])")
    }
}

/// One bounded selected-event window from a single frozen route scan.
pub struct AuthoritativeReactiveEventWindow {
    events: Vec<SymbolicEventEnvelope>,
    observed_upper: Option<EventId>,
}

impl AuthoritativeReactiveEventWindow {
    /// Retains a checked selected-event window and its frozen physical frontier.
    pub fn new(
        events: Vec<SymbolicEventEnvelope>,
        observed_upper: Option<EventId>,
    ) -> Option<Self> {
        (events.len() <= usize::from(MAX_CONSUMER_EVENT_WINDOW_ITEMS)
            && events
                .windows(2)
                .all(|pair| pair[0].event_id() < pair[1].event_id()))
        .then_some(Self {
            events,
            observed_upper,
        })
    }

    /// Borrows selected events in authoritative stream order.
    #[must_use]
    pub fn events(&self) -> &[SymbolicEventEnvelope] {
        &self.events
    }

    /// Returns the frozen physical partition-route frontier.
    #[must_use]
    pub const fn observed_upper(&self) -> Option<EventId> {
        self.observed_upper
    }

    /// Consumes the window into selected events.
    #[must_use]
    pub fn into_events(self) -> Vec<SymbolicEventEnvelope> {
        self.events
    }
}

impl std::fmt::Debug for AuthoritativeReactiveEventWindow {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthoritativeReactiveEventWindow")
            .field("items", &self.events.len())
            .field("payload", &"[REDACTED]")
            .finish()
    }
}
