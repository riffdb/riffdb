//! Contextual-subscription causation and retry-stable reaction identities.

use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use riffdb_query_executor::QueryResultValue;

use riffdb_types::{
    ActorId, CommandId, CommitSequence, DatabaseId, DigestKeyId, EventConsumerName,
    EventDeliveryAttempt, EventId, EventLeaseToken, HashDomain, KeyedHashDomain,
    QueryParameterHash, ReactiveModuleHash, ReactiveOperationHash, ReactiveOperationName,
    RequestId, Timestamp, hash, keyed_hash_secret,
};

use crate::{
    ConsumedEvent, EventConsumerLeaseSelection, EventConsumerMutationResult,
    EventConsumerProgressCursor, EventConsumerPublicStatus, EventConsumerPullDisposition,
    EventConsumerSelection, ExecuteCommandRequest, ExecuteCommandResult, RequestContext,
    RiffDbService, ServiceDtoError, ServiceFuture,
};

/// Maximum contextual long-poll wait.
pub const MAX_CONTEXTUAL_SUBSCRIPTION_WAIT: Duration = Duration::from_secs(30);

/// One checked contextual pull request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumeContextualSubscriptionRequest {
    selection: EventConsumerSelection,
    maximum_wait: Duration,
    progress_cursor: Option<EventConsumerProgressCursor>,
}

impl ConsumeContextualSubscriptionRequest {
    /// Checks the fixed wait bound.
    pub fn new(
        selection: EventConsumerSelection,
        maximum_wait: Duration,
    ) -> Result<Self, ServiceDtoError> {
        if maximum_wait > MAX_CONTEXTUAL_SUBSCRIPTION_WAIT {
            return Err(ServiceDtoError::OutOfRange);
        }
        Ok(Self {
            selection,
            maximum_wait,
            progress_cursor: None,
        })
    }

    /// Supplies the last protected-consumer continuation returned by RiffDB.
    #[must_use]
    pub const fn with_progress_cursor(mut self, cursor: EventConsumerProgressCursor) -> Self {
        self.progress_cursor = Some(cursor);
        self
    }
    /// Exact immutable subscription and consumer identity.
    #[must_use]
    pub const fn selection(&self) -> &EventConsumerSelection {
        &self.selection
    }
    /// Bounded long-poll duration.
    #[must_use]
    pub const fn maximum_wait(&self) -> Duration {
        self.maximum_wait
    }

    /// Optional protected-consumer continuation.
    #[must_use]
    pub const fn progress_cursor(&self) -> Option<EventConsumerProgressCursor> {
        self.progress_cursor
    }
    pub(crate) fn into_parts(
        self,
    ) -> (
        EventConsumerSelection,
        Duration,
        Option<EventConsumerProgressCursor>,
    ) {
        (self.selection, self.maximum_wait, self.progress_cursor)
    }
}

/// One freshly executed named hydration result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContextualHydration {
    name: String,
    outcome: String,
    fields: BTreeMap<String, QueryResultValue>,
}

impl ContextualHydration {
    pub(crate) fn new(
        name: String,
        outcome: String,
        fields: BTreeMap<String, QueryResultValue>,
    ) -> Self {
        Self {
            name,
            outcome,
            fields,
        }
    }
    /// Subscription-local hydration name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Declared named-query outcome.
    #[must_use]
    pub fn outcome(&self) -> &str {
        &self.outcome
    }
    /// Bounded name-addressed result fields.
    #[must_use]
    pub const fn fields(&self) -> &BTreeMap<String, QueryResultValue> {
        &self.fields
    }
}

/// One currently authorized declared reaction and its lease-bound proof.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AvailableContextualReaction {
    name: String,
    command_name: String,
    command_id: CommandId,
    causation_token: ContextualCausationToken,
}

impl AvailableContextualReaction {
    pub(crate) const fn new(
        name: String,
        command_name: String,
        command_id: CommandId,
        causation_token: ContextualCausationToken,
    ) -> Self {
        Self {
            name,
            command_name,
            command_id,
            causation_token,
        }
    }
    /// Retry-stable declaration name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Exact source command name.
    #[must_use]
    pub fn command_name(&self) -> &str {
        &self.command_name
    }
    /// Stable target command identity.
    #[must_use]
    pub const fn command_id(&self) -> CommandId {
        self.command_id
    }
    /// Opaque lease-bound causation token.
    #[must_use]
    pub const fn causation_token(&self) -> &ContextualCausationToken {
        &self.causation_token
    }
}

/// One leased event plus freshly authorized same-snapshot domain context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContextualWorkItem {
    delivery: ConsumedEvent,
    context_head: CommitSequence,
    hydrations: Vec<ContextualHydration>,
    available_reactions: Vec<AvailableContextualReaction>,
}

impl ContextualWorkItem {
    pub(crate) const fn new(
        delivery: ConsumedEvent,
        context_head: CommitSequence,
        hydrations: Vec<ContextualHydration>,
        available_reactions: Vec<AvailableContextualReaction>,
    ) -> Self {
        Self {
            delivery,
            context_head,
            hydrations,
            available_reactions,
        }
    }
    /// Stable event and exact live attempt.
    #[must_use]
    pub const fn delivery(&self) -> &ConsumedEvent {
        &self.delivery
    }
    /// Shared hydration snapshot application head.
    #[must_use]
    pub const fn context_head(&self) -> CommitSequence {
        self.context_head
    }
    /// Fresh bounded hydration results in declaration order.
    #[must_use]
    pub fn hydrations(&self) -> &[ContextualHydration] {
        &self.hydrations
    }
    /// Currently authorized reactions in declaration order.
    #[must_use]
    pub fn available_reactions(&self) -> &[AvailableContextualReaction] {
        &self.available_reactions
    }
}

/// Successful contextual pull or long-poll timeout.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumeContextualSubscriptionResult {
    items: Vec<ContextualWorkItem>,
    status: EventConsumerPublicStatus,
    disposition: EventConsumerPullDisposition,
    enum_variant_names: crate::SharedEnumVariantNames,
}

impl ConsumeContextualSubscriptionResult {
    pub(crate) const fn new(
        items: Vec<ContextualWorkItem>,
        status: EventConsumerPublicStatus,
        disposition: EventConsumerPullDisposition,
        enum_variant_names: crate::SharedEnumVariantNames,
    ) -> Self {
        Self {
            items,
            status,
            disposition,
            enum_variant_names,
        }
    }
    /// Leased contextual work in stream order.
    #[must_use]
    pub fn items(&self) -> &[ContextualWorkItem] {
        &self.items
    }
    /// Post-operation durable consumer status.
    #[must_use]
    pub const fn status(&self) -> &EventConsumerPublicStatus {
        &self.status
    }
    /// Closed completion class.
    #[must_use]
    pub const fn disposition(&self) -> EventConsumerPullDisposition {
        self.disposition
    }
    /// Whether the bounded wait elapsed without work.
    #[must_use]
    pub const fn wait_timed_out(&self) -> bool {
        matches!(self.disposition, EventConsumerPullDisposition::WaitTimedOut)
    }

    /// Resolves one canonical enum identity through the active contract schema.
    #[must_use]
    pub fn enum_variant_name(&self, type_id: u32, variant_id: u32) -> Option<&str> {
        self.enum_variant_names
            .get(&(type_id, variant_id))
            .map(String::as_str)
    }
}

/// One validated contextual reaction command request.
pub struct ExecuteContextualReactionRequest {
    selection: EventConsumerSelection,
    causation_token: ContextualCausationToken,
    reaction_name: String,
    command: ExecuteCommandRequest,
}

impl ExecuteContextualReactionRequest {
    /// Joins an opaque causal proof, declared reaction, and ordinary command input.
    pub fn new(
        selection: EventConsumerSelection,
        causation_token: ContextualCausationToken,
        reaction_name: String,
        command: ExecuteCommandRequest,
    ) -> Result<Self, ServiceDtoError> {
        if reaction_name.is_empty() || reaction_name.len() > 256 {
            return Err(ServiceDtoError::OutOfRange);
        }
        Ok(Self {
            selection,
            causation_token,
            reaction_name,
            command,
        })
    }
    /// Exact subscription and durable-consumer identity.
    #[must_use]
    pub const fn selection(&self) -> &EventConsumerSelection {
        &self.selection
    }
    /// Opaque server-issued proof.
    #[must_use]
    pub const fn causation_token(&self) -> &ContextualCausationToken {
        &self.causation_token
    }
    /// Exact declared reaction name.
    #[must_use]
    pub fn reaction_name(&self) -> &str {
        &self.reaction_name
    }
    /// Ordinary typed command request.
    #[must_use]
    pub const fn command(&self) -> &ExecuteCommandRequest {
        &self.command
    }
    pub(crate) fn into_parts(
        self,
    ) -> (
        EventConsumerSelection,
        ContextualCausationToken,
        String,
        ExecuteCommandRequest,
    ) {
        (
            self.selection,
            self.causation_token,
            self.reaction_name,
            self.command,
        )
    }
}

/// API-neutral contextual durable-work application surface.
pub trait ContextualSubscriptionApplication: Send + Sync {
    /// Pulls one bounded batch with fresh same-snapshot hydration.
    fn consume_contextual_subscription(
        &self,
        context: RequestContext,
        request: ConsumeContextualSubscriptionRequest,
    ) -> ServiceFuture<'_, ConsumeContextualSubscriptionResult>;
    /// Acknowledges one exact contextual lease.
    fn acknowledge_contextual_subscription(
        &self,
        context: RequestContext,
        lease: EventConsumerLeaseSelection,
    ) -> ServiceFuture<'_, EventConsumerMutationResult>;
    /// Releases one exact contextual lease with optional retry delay.
    fn negative_acknowledge_contextual_subscription(
        &self,
        context: RequestContext,
        lease: EventConsumerLeaseSelection,
        retry_delay: Duration,
    ) -> ServiceFuture<'_, EventConsumerMutationResult>;
    /// Reads one exact contextual consumer status.
    fn get_contextual_subscription_status(
        &self,
        context: RequestContext,
        selection: EventConsumerSelection,
    ) -> ServiceFuture<'_, Option<crate::EventConsumerPublicStatus>>;
    /// Executes a causally fenced reaction through the ordinary command path.
    fn execute_contextual_reaction(
        &self,
        context: RequestContext,
        request: ExecuteContextualReactionRequest,
    ) -> ServiceFuture<'_, ExecuteCommandResult>;
}

impl ContextualSubscriptionApplication for RiffDbService {
    fn consume_contextual_subscription(
        &self,
        context: RequestContext,
        request: ConsumeContextualSubscriptionRequest,
    ) -> ServiceFuture<'_, ConsumeContextualSubscriptionResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            riffdb_types::ServiceOperationV1::ConsumeContextualSubscription,
            ingress,
            async move {
                let (selection, maximum_wait, progress_cursor) = request.into_parts();
                crate::consumer_operations::consume_contextual_events(
                    service,
                    context,
                    selection,
                    maximum_wait,
                    progress_cursor,
                )
                .await
            },
        )
    }

    fn acknowledge_contextual_subscription(
        &self,
        context: RequestContext,
        lease: EventConsumerLeaseSelection,
    ) -> ServiceFuture<'_, EventConsumerMutationResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            riffdb_types::ServiceOperationV1::AcknowledgeContextualSubscription,
            ingress,
            async move {
                crate::consumer_operations::acknowledge_contextual_event(
                    service, context, lease, None,
                )
                .await
            },
        )
    }

    fn negative_acknowledge_contextual_subscription(
        &self,
        context: RequestContext,
        lease: EventConsumerLeaseSelection,
        retry_delay: Duration,
    ) -> ServiceFuture<'_, EventConsumerMutationResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            riffdb_types::ServiceOperationV1::NegativeAcknowledgeContextualSubscription,
            ingress,
            async move {
                if retry_delay > crate::MAX_CONSUMER_RETRY_DELAY {
                    return Err(crate::consumer_operations::invalid_consumer_request());
                }
                crate::consumer_operations::acknowledge_contextual_event(
                    service,
                    context,
                    lease,
                    Some(retry_delay),
                )
                .await
            },
        )
    }

    fn get_contextual_subscription_status(
        &self,
        context: RequestContext,
        selection: EventConsumerSelection,
    ) -> ServiceFuture<'_, Option<crate::EventConsumerPublicStatus>> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            riffdb_types::ServiceOperationV1::GetContextualSubscriptionStatus,
            ingress,
            async move {
                crate::consumer_operations::contextual_consumer_status(service, context, selection)
                    .await
            },
        )
    }

    fn execute_contextual_reaction(
        &self,
        context: RequestContext,
        request: ExecuteContextualReactionRequest,
    ) -> ServiceFuture<'_, ExecuteCommandResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            riffdb_types::ServiceOperationV1::ExecuteContextualReaction,
            ingress,
            async move {
                crate::consumer_operations::execute_contextual_reaction_operation(
                    service, context, request,
                )
                .await
            },
        )
    }
}

/// Maximum encoded causation-token bytes.
pub const MAX_CONTEXTUAL_CAUSATION_TOKEN_BYTES: usize = 1_024;
const CAUSATION_VERSION_V1: u8 = 1;
const CAUSATION_KEY_ID: u32 = 1;
const CAUSATION_MAC_BYTES: usize = 32;

/// Complete server-validated causal binding for one leased contextual event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContextualCausationClaimsV1 {
    database_id: DatabaseId,
    history_incarnation: u64,
    module_hash: ReactiveModuleHash,
    operation_hash: ReactiveOperationHash,
    operation_name: ReactiveOperationName,
    parameter_hash: QueryParameterHash,
    consumer_name: EventConsumerName,
    event_id: EventId,
    attempt: EventDeliveryAttempt,
    lease_token: EventLeaseToken,
    principal_id: ActorId,
    capability_revision: NonZeroU64,
    command_id: CommandId,
    expires_at: Timestamp,
    root_request_id: RequestId,
}

impl ContextualCausationClaimsV1 {
    /// Constructs one complete nonempty causal binding.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        database_id: DatabaseId,
        history_incarnation: u64,
        module_hash: ReactiveModuleHash,
        operation_hash: ReactiveOperationHash,
        operation_name: ReactiveOperationName,
        parameter_hash: QueryParameterHash,
        consumer_name: EventConsumerName,
        event_id: EventId,
        attempt: EventDeliveryAttempt,
        lease_token: EventLeaseToken,
        principal_id: ActorId,
        capability_revision: NonZeroU64,
        command_id: CommandId,
        expires_at: Timestamp,
        root_request_id: RequestId,
    ) -> Result<Self, ContextualCausationError> {
        if history_incarnation == 0 {
            return Err(ContextualCausationError);
        }
        Ok(Self {
            database_id,
            history_incarnation,
            module_hash,
            operation_hash,
            operation_name,
            parameter_hash,
            consumer_name,
            event_id,
            attempt,
            lease_token,
            principal_id,
            capability_revision,
            command_id,
            expires_at,
            root_request_id,
        })
    }

    /// Exact database identity.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }
    /// Restore fence.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }
    /// Immutable reactive module identity.
    #[must_use]
    pub const fn module_hash(&self) -> ReactiveModuleHash {
        self.module_hash
    }
    /// Exact contextual-operation plan identity.
    #[must_use]
    pub const fn operation_hash(&self) -> ReactiveOperationHash {
        self.operation_hash
    }
    /// Contextual operation name.
    #[must_use]
    pub const fn operation_name(&self) -> &ReactiveOperationName {
        &self.operation_name
    }
    /// Canonical parameter hash.
    #[must_use]
    pub const fn parameter_hash(&self) -> QueryParameterHash {
        self.parameter_hash
    }
    /// Durable consumer name.
    #[must_use]
    pub const fn consumer_name(&self) -> &EventConsumerName {
        &self.consumer_name
    }
    /// Causing event.
    #[must_use]
    pub const fn event_id(&self) -> EventId {
        self.event_id
    }
    /// Lease attempt.
    #[must_use]
    pub const fn attempt(&self) -> EventDeliveryAttempt {
        self.attempt
    }
    /// Exact lease token.
    #[must_use]
    pub const fn lease_token(&self) -> EventLeaseToken {
        self.lease_token
    }
    /// Authenticated principal.
    #[must_use]
    pub const fn principal_id(&self) -> &ActorId {
        &self.principal_id
    }
    /// Capability revision observed for delivery.
    #[must_use]
    pub const fn capability_revision(&self) -> NonZeroU64 {
        self.capability_revision
    }
    /// Exact target command.
    #[must_use]
    pub const fn command_id(&self) -> CommandId {
        self.command_id
    }
    /// Exclusive causal admission deadline.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
    /// Inherited root request correlation.
    #[must_use]
    pub const fn root_request_id(&self) -> RequestId {
        self.root_request_id
    }
}

/// Process-secret key material for contextual causation tokens.
pub struct ContextualCausationKey([u8; 32]);

impl ContextualCausationKey {
    /// Retains exactly one 256-bit configured key.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl fmt::Debug for ContextualCausationKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContextualCausationKey([REDACTED])")
    }
}

/// Least-authority server-key operation used to seal and verify causal claims.
pub trait ContextualCausationMacProvider: Send + Sync {
    /// Computes the current write-key MAC for one canonical bounded payload.
    fn seal_mac(&self, canonical_payload: &[u8]) -> [u8; 32];
    /// Checks one supplied MAC against every readable key.
    fn verifies_mac(&self, canonical_payload: &[u8], supplied: &[u8; 32]) -> bool;
}

impl ContextualCausationMacProvider for ContextualCausationKey {
    fn seal_mac(&self, canonical_payload: &[u8]) -> [u8; 32] {
        causation_mac(self, canonical_payload)
    }

    fn verifies_mac(&self, canonical_payload: &[u8], supplied: &[u8; 32]) -> bool {
        constant_time_equal(supplied, &causation_mac(self, canonical_payload))
    }
}

impl ContextualCausationMacProvider for riffdb_auth::CapabilityDigestKeyProvider {
    fn seal_mac(&self, canonical_payload: &[u8]) -> [u8; 32] {
        *self
            .current_contextual_causation_mac(canonical_payload)
            .as_bytes()
    }

    fn verifies_mac(&self, canonical_payload: &[u8], supplied: &[u8; 32]) -> bool {
        self.matches_contextual_causation_mac(canonical_payload, supplied)
    }
}

/// Opaque bounded sealed causation token. Possession is not authority.
#[derive(Clone, Eq, PartialEq)]
pub struct ContextualCausationToken(Vec<u8>);

impl ContextualCausationToken {
    /// Retains one structurally bounded opaque token.
    pub fn checked(bytes: Vec<u8>) -> Result<Self, ContextualCausationError> {
        (bytes.len() > CAUSATION_MAC_BYTES && bytes.len() <= MAX_CONTEXTUAL_CAUSATION_TOKEN_BYTES)
            .then_some(Self(bytes))
            .ok_or(ContextualCausationError)
    }
    /// Borrows opaque token bytes for transport encoding.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for ContextualCausationToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContextualCausationToken([REDACTED])")
    }
}

/// Seals and validates contextual causation with the configured server key.
#[derive(Clone)]
pub struct ContextualCausationTokenCodec(Arc<dyn ContextualCausationMacProvider>);

impl ContextualCausationTokenCodec {
    /// Installs one key for this codec instance.
    #[must_use]
    pub fn new(key: ContextualCausationKey) -> Self {
        Self(Arc::new(key))
    }

    /// Installs one server-owned rotating MAC provider.
    #[must_use]
    pub fn from_provider(provider: Arc<dyn ContextualCausationMacProvider>) -> Self {
        Self(provider)
    }

    /// Authenticates one canonical claims transcript.
    pub fn seal(
        &self,
        claims: &ContextualCausationClaimsV1,
    ) -> Result<ContextualCausationToken, ContextualCausationError> {
        let mut bytes = encode_claims(claims)?;
        let mac = self.0.seal_mac(&bytes);
        bytes.extend_from_slice(&mac);
        ContextualCausationToken::checked(bytes)
    }

    /// Verifies authentication before decoding any claims for use.
    pub fn open(
        &self,
        token: &ContextualCausationToken,
    ) -> Result<ContextualCausationClaimsV1, ContextualCausationError> {
        let split = token
            .0
            .len()
            .checked_sub(CAUSATION_MAC_BYTES)
            .ok_or(ContextualCausationError)?;
        let (payload, supplied) = token.0.split_at(split);
        let supplied: &[u8; 32] = supplied.try_into().map_err(|_| ContextualCausationError)?;
        if !self.0.verifies_mac(payload, supplied) {
            return Err(ContextualCausationError);
        }
        decode_claims(payload)
    }
}

/// Generated idempotency value for the command's direct supported input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReactionIdempotencyValue {
    /// RFC 9562 UUIDv8 bytes.
    Uuid([u8; 16]),
    /// Exact 64-character lowercase digest hexadecimal.
    String(String),
}

/// Derives the retry-stable reaction identity frozen by ADR-0080.
pub fn derive_reaction_idempotency(
    claims: &ContextualCausationClaimsV1,
    reaction_name: &str,
    uuid_input: bool,
) -> Result<ReactionIdempotencyValue, ContextualCausationError> {
    let digest = reaction_identity_digest(claims, reaction_name)?;
    if uuid_input {
        let mut uuid = [0_u8; 16];
        uuid.copy_from_slice(&digest[..16]);
        uuid[6] = (uuid[6] & 0x0f) | 0x80;
        uuid[8] = (uuid[8] & 0x3f) | 0x80;
        Ok(ReactionIdempotencyValue::Uuid(uuid))
    } else {
        let mut encoded = String::with_capacity(64);
        for byte in digest {
            use std::fmt::Write as _;
            write!(&mut encoded, "{byte:02x}").map_err(|_| ContextualCausationError)?;
        }
        Ok(ReactionIdempotencyValue::String(encoded))
    }
}

pub(crate) fn derive_reaction_request_id(
    parent: RequestId,
    claims: &ContextualCausationClaimsV1,
    reaction_name: &str,
) -> Result<RequestId, ContextualCausationError> {
    let digest = reaction_identity_digest(claims, reaction_name)?;
    let mut bytes = parent.into_bytes();
    let mut changed = false;
    for (target, mask) in [
        (6_usize, digest[0] & 0x0f),
        (7, digest[1]),
        (8, digest[2] & 0x3f),
        (9, digest[3]),
        (10, digest[4]),
        (11, digest[5]),
        (12, digest[6]),
        (13, digest[7]),
        (14, digest[8]),
        (15, digest[9]),
    ] {
        bytes[target] ^= mask;
        changed |= mask != 0;
    }
    if !changed {
        bytes[15] ^= 1;
    }
    RequestId::from_bytes(bytes).map_err(|_| ContextualCausationError)
}

fn reaction_identity_digest(
    claims: &ContextualCausationClaimsV1,
    reaction_name: &str,
) -> Result<[u8; 32], ContextualCausationError> {
    if reaction_name.is_empty() || reaction_name.len() > 256 {
        return Err(ContextualCausationError);
    }
    let mut transcript = Vec::with_capacity(32 + 32 + 12 + 4 + 4 + reaction_name.len());
    transcript.extend_from_slice(claims.module_hash.as_bytes());
    transcript.extend_from_slice(claims.operation_hash.as_bytes());
    transcript.extend_from_slice(&claims.event_id.commit_sequence().get().to_be_bytes());
    transcript.extend_from_slice(&claims.event_id.event_ordinal().to_be_bytes());
    transcript.extend_from_slice(&claims.command_id.to_be_bytes());
    push_bytes(&mut transcript, reaction_name.as_bytes())?;
    Ok(*hash(HashDomain::ContextualReaction, &transcript).as_bytes())
}

/// Applies the contextual reaction identity through the production binder.
#[cfg(feature = "test-fixtures")]
pub fn bind_reaction_idempotency_fixture(
    claims: &ContextualCausationClaimsV1,
    reaction_name: &str,
    command: &riffdb_contract_ir::CommandPlan,
    request: ExecuteCommandRequest,
) -> crate::ServiceResult<ExecuteCommandRequest> {
    crate::consumer_operations::bind_reaction_idempotency(claims, reaction_name, command, request)
}

/// Closed, value-free token or reaction-identity rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContextualCausationError;

impl fmt::Display for ContextualCausationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid contextual causation")
    }
}

impl std::error::Error for ContextualCausationError {}

fn causation_mac(key: &ContextualCausationKey, payload: &[u8]) -> [u8; 32] {
    let key_id = DigestKeyId::try_from(CAUSATION_KEY_ID).expect("nonzero fixed key ID");
    *keyed_hash_secret(
        KeyedHashDomain::ContextualCausation,
        key_id,
        &key.0,
        payload,
    )
    .as_bytes()
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn encode_claims(
    claims: &ContextualCausationClaimsV1,
) -> Result<Vec<u8>, ContextualCausationError> {
    let mut bytes = Vec::new();
    bytes.push(CAUSATION_VERSION_V1);
    bytes.extend_from_slice(claims.database_id.as_bytes());
    bytes.extend_from_slice(&claims.history_incarnation.to_be_bytes());
    bytes.extend_from_slice(claims.module_hash.as_bytes());
    bytes.extend_from_slice(claims.operation_hash.as_bytes());
    push_bytes(&mut bytes, claims.operation_name.as_str().as_bytes())?;
    bytes.extend_from_slice(claims.parameter_hash.as_bytes());
    push_bytes(&mut bytes, claims.consumer_name.as_bytes())?;
    bytes.extend_from_slice(&claims.event_id.commit_sequence().get().to_be_bytes());
    bytes.extend_from_slice(&claims.event_id.event_ordinal().to_be_bytes());
    bytes.push(claims.attempt.get());
    bytes.extend_from_slice(claims.lease_token.as_bytes());
    push_bytes(&mut bytes, claims.principal_id.as_str().as_bytes())?;
    bytes.extend_from_slice(&claims.capability_revision.get().to_be_bytes());
    bytes.extend_from_slice(&claims.command_id.to_be_bytes());
    bytes.extend_from_slice(&claims.expires_at.seconds().to_be_bytes());
    bytes.extend_from_slice(&claims.expires_at.nanoseconds().to_be_bytes());
    bytes.extend_from_slice(claims.root_request_id.as_bytes());
    (bytes.len() + CAUSATION_MAC_BYTES <= MAX_CONTEXTUAL_CAUSATION_TOKEN_BYTES)
        .then_some(bytes)
        .ok_or(ContextualCausationError)
}

fn decode_claims(payload: &[u8]) -> Result<ContextualCausationClaimsV1, ContextualCausationError> {
    let mut cursor = Decoder::new(payload);
    if cursor.u8()? != CAUSATION_VERSION_V1 {
        return Err(ContextualCausationError);
    }
    let database_id =
        DatabaseId::from_bytes(cursor.array()?).map_err(|_| ContextualCausationError)?;
    let history_incarnation = cursor.u64()?;
    let module_hash = ReactiveModuleHash::from_bytes(cursor.array()?);
    let operation_hash = ReactiveOperationHash::from_bytes(cursor.array()?);
    let operation_name =
        ReactiveOperationName::new(cursor.string()?).map_err(|_| ContextualCausationError)?;
    let parameter_hash = QueryParameterHash::from_bytes(cursor.array()?);
    let consumer_name =
        EventConsumerName::new(cursor.string()?).map_err(|_| ContextualCausationError)?;
    let sequence = CommitSequence::new(cursor.u64()?).ok_or(ContextualCausationError)?;
    let event_id = EventId::new(sequence, cursor.u32()?);
    let attempt = EventDeliveryAttempt::new(cursor.u8()?).ok_or(ContextualCausationError)?;
    let lease_token = EventLeaseToken::from_bytes(cursor.array()?);
    let principal_id = ActorId::new(cursor.string()?).map_err(|_| ContextualCausationError)?;
    let capability_revision = NonZeroU64::new(cursor.u64()?).ok_or(ContextualCausationError)?;
    let command_id = CommandId::new(cursor.u32()?).ok_or(ContextualCausationError)?;
    let expires_at =
        Timestamp::new(cursor.i64()?, cursor.u32()?).map_err(|_| ContextualCausationError)?;
    let root_request_id =
        RequestId::from_bytes(cursor.array()?).map_err(|_| ContextualCausationError)?;
    if !cursor.is_empty() {
        return Err(ContextualCausationError);
    }
    ContextualCausationClaimsV1::new(
        database_id,
        history_incarnation,
        module_hash,
        operation_hash,
        operation_name,
        parameter_hash,
        consumer_name,
        event_id,
        attempt,
        lease_token,
        principal_id,
        capability_revision,
        command_id,
        expires_at,
        root_request_id,
    )
}

fn push_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<(), ContextualCausationError> {
    let length = u16::try_from(value.len()).map_err(|_| ContextualCausationError)?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value);
    Ok(())
}

struct Decoder<'a> {
    remaining: &'a [u8],
}
impl<'a> Decoder<'a> {
    const fn new(remaining: &'a [u8]) -> Self {
        Self { remaining }
    }
    const fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }
    fn take(&mut self, length: usize) -> Result<&'a [u8], ContextualCausationError> {
        if self.remaining.len() < length {
            return Err(ContextualCausationError);
        }
        let (value, remaining) = self.remaining.split_at(length);
        self.remaining = remaining;
        Ok(value)
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N], ContextualCausationError> {
        self.take(N)?
            .try_into()
            .map_err(|_| ContextualCausationError)
    }
    fn u8(&mut self) -> Result<u8, ContextualCausationError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, ContextualCausationError> {
        Ok(u32::from_be_bytes(self.array()?))
    }
    fn u64(&mut self) -> Result<u64, ContextualCausationError> {
        Ok(u64::from_be_bytes(self.array()?))
    }
    fn i64(&mut self) -> Result<i64, ContextualCausationError> {
        Ok(i64::from_be_bytes(self.array()?))
    }
    fn string(&mut self) -> Result<String, ContextualCausationError> {
        let length = usize::from(u16::from_be_bytes(self.array()?));
        String::from_utf8(self.take(length)?.to_vec()).map_err(|_| ContextualCausationError)
    }
}
