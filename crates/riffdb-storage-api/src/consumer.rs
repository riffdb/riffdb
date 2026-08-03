//! Durable event-consumer identity, state, and atomic transition protocol.

use riffdb_types::{
    DatabaseId, EventConsumerIdentityHash, EventConsumerName, EventConsumerRevision,
    EventDeliveryAttempt, EventId, EventLeaseToken, MAX_EVENT_DELIVERY_ATTEMPTS, PartitionKeyHash,
    QueryParameterHash, ReactiveModuleHash, ReactiveOperationName, Timestamp,
    event_consumer_identity_hash,
};

use crate::{StorageError, StorageErrorKind};

/// Maximum sparse terminal resolutions retained beyond the contiguous checkpoint.
pub const MAX_CONSUMER_SPARSE_RESOLUTIONS: usize = 64;
/// Maximum durable delivery rows retained by one consumer, including dead letters.
pub const MAX_CONSUMER_DELIVERY_RECORDS: usize = 4_096;
/// Maximum durable consumers admitted to one database inventory.
pub const MAX_EVENT_CONSUMERS: usize = 4_096;
/// Maximum events leased by one generic pull operation.
pub const MAX_CONSUMER_BATCH_ITEMS: usize = 64;
/// Maximum simultaneously live leased delivery rows.
pub const MAX_CONSUMER_IN_FLIGHT: usize = 64;

/// Complete canonical identity of one durable event consumer.
#[derive(Clone, Eq, PartialEq)]
pub struct EventConsumerIdentityV1 {
    database_id: DatabaseId,
    reactive_module_hash: ReactiveModuleHash,
    operation_name: ReactiveOperationName,
    parameter_hash: QueryParameterHash,
    consumer_name: EventConsumerName,
    identity_hash: EventConsumerIdentityHash,
}

impl EventConsumerIdentityV1 {
    /// Constructs and hashes the exact approved identity tuple.
    #[must_use]
    pub fn new(
        database_id: DatabaseId,
        reactive_module_hash: ReactiveModuleHash,
        operation_name: ReactiveOperationName,
        parameter_hash: QueryParameterHash,
        consumer_name: EventConsumerName,
    ) -> Self {
        let identity_hash = event_consumer_identity_hash(
            database_id,
            reactive_module_hash,
            &operation_name,
            parameter_hash,
            &consumer_name,
        );
        Self {
            database_id,
            reactive_module_hash,
            operation_name,
            parameter_hash,
            consumer_name,
            identity_hash,
        }
    }

    /// Returns the selected durable database identity.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Returns the exact immutable reactive module identity.
    #[must_use]
    pub const fn reactive_module_hash(&self) -> ReactiveModuleHash {
        self.reactive_module_hash
    }

    /// Borrows the exact operation name.
    #[must_use]
    pub const fn operation_name(&self) -> &ReactiveOperationName {
        &self.operation_name
    }

    /// Returns the canonical name-addressed parameter hash.
    #[must_use]
    pub const fn parameter_hash(&self) -> QueryParameterHash {
        self.parameter_hash
    }

    /// Borrows the application-selected consumer name.
    #[must_use]
    pub const fn consumer_name(&self) -> &EventConsumerName {
        &self.consumer_name
    }

    /// Returns the domain-separated physical identity hash.
    #[must_use]
    pub const fn identity_hash(&self) -> EventConsumerIdentityHash {
        self.identity_hash
    }
}

impl std::fmt::Debug for EventConsumerIdentityV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EventConsumerIdentityV1")
            .field("identity_hash", &self.identity_hash)
            .field("identity", &"[REDACTED]")
            .finish()
    }
}

/// Contiguous terminal prefix of one exact selected event stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsumerCheckpointV1 {
    /// No selected event has been terminally resolved.
    BeforeFirst,
    /// Every selected event through this exact event is terminally resolved.
    After(EventId),
}

impl ConsumerCheckpointV1 {
    /// Returns whether `event_id` is strictly after this checkpoint.
    #[must_use]
    pub fn precedes(self, event_id: EventId) -> bool {
        match self {
            Self::BeforeFirst => true,
            Self::After(checkpoint) => event_id.to_be_bytes() > checkpoint.to_be_bytes(),
        }
    }

    /// Conservative inclusive retention watermark permitted by this checkpoint.
    #[must_use]
    pub const fn retention_frontier(self) -> u64 {
        match self {
            Self::BeforeFirst => 0,
            Self::After(event_id) => event_id.commit_sequence().get().saturating_sub(1),
        }
    }
}

/// Terminal disposition retained beyond the contiguous checkpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SparseResolutionKindV1 {
    /// The current lease was explicitly acknowledged.
    Acknowledged,
    /// Ten failed attempts durably dead-lettered the event.
    DeadLettered,
}

/// One canonical sparse terminal resolution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SparseConsumerResolutionV1 {
    event_id: EventId,
    kind: SparseResolutionKindV1,
}

impl SparseConsumerResolutionV1 {
    /// Constructs one terminal resolution.
    #[must_use]
    pub const fn new(event_id: EventId, kind: SparseResolutionKindV1) -> Self {
        Self { event_id, kind }
    }

    /// Returns the event identity.
    #[must_use]
    pub const fn event_id(self) -> EventId {
        self.event_id
    }

    /// Returns the terminal disposition.
    #[must_use]
    pub const fn kind(self) -> SparseResolutionKindV1 {
        self.kind
    }
}

/// Durable bounded state for one consumer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredEventConsumerV1 {
    identity: EventConsumerIdentityV1,
    partition_hash: PartitionKeyHash,
    history_incarnation: u64,
    revision: EventConsumerRevision,
    checkpoint: ConsumerCheckpointV1,
    sparse_resolutions: Vec<SparseConsumerResolutionV1>,
}

impl StoredEventConsumerV1 {
    /// Creates a before-first consumer at its first revision.
    pub fn initial(
        identity: EventConsumerIdentityV1,
        partition_hash: PartitionKeyHash,
        history_incarnation: u64,
    ) -> Result<Self, ConsumerStateError> {
        Self::checked(
            identity,
            partition_hash,
            history_incarnation,
            EventConsumerRevision::first(),
            ConsumerCheckpointV1::BeforeFirst,
            Vec::new(),
        )
    }

    /// Validates a decoded or coordinator-produced complete state.
    pub fn checked(
        identity: EventConsumerIdentityV1,
        partition_hash: PartitionKeyHash,
        history_incarnation: u64,
        revision: EventConsumerRevision,
        checkpoint: ConsumerCheckpointV1,
        sparse_resolutions: Vec<SparseConsumerResolutionV1>,
    ) -> Result<Self, ConsumerStateError> {
        if history_incarnation == 0 || sparse_resolutions.len() > MAX_CONSUMER_SPARSE_RESOLUTIONS {
            return Err(ConsumerStateError::InvalidShape);
        }
        let mut previous = None;
        for resolution in &sparse_resolutions {
            if !checkpoint.precedes(resolution.event_id)
                || previous.is_some_and(|value: EventId| value >= resolution.event_id)
            {
                return Err(ConsumerStateError::InvalidShape);
            }
            previous = Some(resolution.event_id);
        }
        Ok(Self {
            identity,
            partition_hash,
            history_incarnation,
            revision,
            checkpoint,
            sparse_resolutions,
        })
    }

    /// Borrows the complete identity.
    #[must_use]
    pub const fn identity(&self) -> &EventConsumerIdentityV1 {
        &self.identity
    }

    /// Returns the checked stream partition hash.
    #[must_use]
    pub const fn partition_hash(&self) -> PartitionKeyHash {
        self.partition_hash
    }

    /// Returns the restore fence.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }

    /// Returns the compare-and-transition revision.
    #[must_use]
    pub const fn revision(&self) -> EventConsumerRevision {
        self.revision
    }

    /// Returns the contiguous terminal checkpoint.
    #[must_use]
    pub const fn checkpoint(&self) -> ConsumerCheckpointV1 {
        self.checkpoint
    }

    /// Borrows canonical sparse terminal resolutions.
    #[must_use]
    pub fn sparse_resolutions(&self) -> &[SparseConsumerResolutionV1] {
        &self.sparse_resolutions
    }

    /// Produces the exact next revision with coordinator-computed terminal state.
    pub fn advance(
        &self,
        checkpoint: ConsumerCheckpointV1,
        sparse_resolutions: Vec<SparseConsumerResolutionV1>,
    ) -> Result<Self, ConsumerStateError> {
        let revision = self
            .revision
            .checked_next()
            .ok_or(ConsumerStateError::RevisionExhausted)?;
        if checkpoint_order(checkpoint) < checkpoint_order(self.checkpoint) {
            return Err(ConsumerStateError::InvalidShape);
        }
        Self::checked(
            self.identity.clone(),
            self.partition_hash,
            self.history_incarnation,
            revision,
            checkpoint,
            sparse_resolutions,
        )
    }

    /// Produces an authorized seek state, including backward movement.
    pub fn seek(&self, checkpoint: ConsumerCheckpointV1) -> Result<Self, ConsumerStateError> {
        Self::checked(
            self.identity.clone(),
            self.partition_hash,
            self.history_incarnation,
            self.revision
                .checked_next()
                .ok_or(ConsumerStateError::RevisionExhausted)?,
            checkpoint,
            Vec::new(),
        )
    }

    /// Rebinds restored state to a new history incarnation and revision.
    pub fn restored(&self, history_incarnation: u64) -> Result<Self, ConsumerStateError> {
        Self::checked(
            self.identity.clone(),
            self.partition_hash,
            history_incarnation,
            self.revision
                .checked_next()
                .ok_or(ConsumerStateError::RevisionExhausted)?,
            self.checkpoint,
            self.sparse_resolutions.clone(),
        )
    }
}

const fn checkpoint_order(checkpoint: ConsumerCheckpointV1) -> Option<[u8; 12]> {
    match checkpoint {
        ConsumerCheckpointV1::BeforeFirst => None,
        ConsumerCheckpointV1::After(event_id) => Some(event_id.to_be_bytes()),
    }
}

/// Why the tenth failed attempt entered durable dead-letter state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsumerDeadLetterReasonV1 {
    /// The lease expired before acknowledgement.
    LeaseExpired,
    /// The caller explicitly negatively acknowledged the attempt.
    NegativeAcknowledgement,
}

/// Closed durable state of one selected event for one consumer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsumerDeliveryStateV1 {
    /// One exact attempt is currently leased.
    Leased {
        /// Attempt number in `1..=10`.
        attempt: EventDeliveryAttempt,
        /// Attempt-specific opaque token.
        token: EventLeaseToken,
        /// Wall-clock expiry supplied by the consumer coordinator.
        expires_at: Timestamp,
    },
    /// Prior attempt failed and the event is eligible at this instant.
    Retry {
        /// Number of failed attempts in `1..=9`.
        failed_attempts: EventDeliveryAttempt,
        /// Earliest next lease time; immediate retry uses the transition time.
        eligible_at: Timestamp,
    },
    /// Ten failed attempts terminally resolved delivery.
    DeadLettered {
        /// Fixed terminal attempt number ten.
        failed_attempts: EventDeliveryAttempt,
        /// Coordinator-observed terminal instant.
        dead_lettered_at: Timestamp,
        /// Closed final failure class.
        reason: ConsumerDeadLetterReasonV1,
    },
}

/// One durable consumer/event delivery row; it contains no event payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredEventConsumerDeliveryV1 {
    consumer_identity_hash: EventConsumerIdentityHash,
    event_id: EventId,
    history_incarnation: u64,
    state: ConsumerDeliveryStateV1,
}

impl StoredEventConsumerDeliveryV1 {
    /// Checks the closed state and restore fence.
    pub fn new(
        consumer_identity_hash: EventConsumerIdentityHash,
        event_id: EventId,
        history_incarnation: u64,
        state: ConsumerDeliveryStateV1,
    ) -> Result<Self, ConsumerStateError> {
        if history_incarnation == 0 || !valid_delivery_state(state) {
            return Err(ConsumerStateError::InvalidShape);
        }
        Ok(Self {
            consumer_identity_hash,
            event_id,
            history_incarnation,
            state,
        })
    }

    /// Returns the owning consumer hash.
    #[must_use]
    pub const fn consumer_identity_hash(&self) -> EventConsumerIdentityHash {
        self.consumer_identity_hash
    }

    /// Returns the authoritative event identity.
    #[must_use]
    pub const fn event_id(&self) -> EventId {
        self.event_id
    }

    /// Returns the restore fence.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }

    /// Returns the closed delivery state.
    #[must_use]
    pub const fn state(&self) -> ConsumerDeliveryStateV1 {
        self.state
    }
}

const fn valid_delivery_state(state: ConsumerDeliveryStateV1) -> bool {
    match state {
        ConsumerDeliveryStateV1::Leased { .. } => true,
        ConsumerDeliveryStateV1::Retry {
            failed_attempts, ..
        } => failed_attempts.get() < 10,
        ConsumerDeliveryStateV1::DeadLettered {
            failed_attempts, ..
        } => failed_attempts.get() == 10,
    }
}

/// Bounded one-snapshot consumer state returned to its coordinator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventConsumerSnapshotV1 {
    consumer: StoredEventConsumerV1,
    deliveries: Vec<StoredEventConsumerDeliveryV1>,
}

impl EventConsumerSnapshotV1 {
    /// Validates identity, incarnation, canonical order, bounds, and in-flight count.
    pub fn new(
        consumer: StoredEventConsumerV1,
        deliveries: Vec<StoredEventConsumerDeliveryV1>,
    ) -> Result<Self, ConsumerStateError> {
        if deliveries.len() > MAX_CONSUMER_DELIVERY_RECORDS {
            return Err(ConsumerStateError::LimitExceeded);
        }
        let mut previous = None;
        let mut in_flight = 0_usize;
        for delivery in &deliveries {
            if delivery.consumer_identity_hash != consumer.identity.identity_hash
                || delivery.history_incarnation != consumer.history_incarnation
                || previous.is_some_and(|event_id: EventId| event_id >= delivery.event_id)
            {
                return Err(ConsumerStateError::InvalidShape);
            }
            if matches!(delivery.state, ConsumerDeliveryStateV1::Leased { .. }) {
                in_flight += 1;
            }
            previous = Some(delivery.event_id);
        }
        if in_flight > MAX_CONSUMER_IN_FLIGHT {
            return Err(ConsumerStateError::LimitExceeded);
        }
        Ok(Self {
            consumer,
            deliveries,
        })
    }

    /// Borrows consumer state.
    #[must_use]
    pub const fn consumer(&self) -> &StoredEventConsumerV1 {
        &self.consumer
    }

    /// Borrows canonical delivery rows.
    #[must_use]
    pub fn deliveries(&self) -> &[StoredEventConsumerDeliveryV1] {
        &self.deliveries
    }
}

/// One consumer state was structurally invalid or exhausted a fixed bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsumerStateError {
    /// A field, ordering rule, or cross-reference was invalid.
    InvalidShape,
    /// A fixed collection bound was exceeded.
    LimitExceeded,
    /// The per-consumer transition revision cannot advance.
    RevisionExhausted,
}

/// Exact expected state of one delivery row in an atomic transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpectedConsumerDeliveryV1 {
    /// No row may exist; this is a first attempt.
    Absent,
    /// The exact retry row must exist.
    Retry {
        /// Failed attempt count.
        failed_attempts: EventDeliveryAttempt,
        /// Exact retained eligibility instant.
        eligible_at: Timestamp,
    },
}

/// One candidate first/retry lease in canonical event order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumerLeaseCandidateV1 {
    expected: ExpectedConsumerDeliveryV1,
    replacement: StoredEventConsumerDeliveryV1,
}

impl ConsumerLeaseCandidateV1 {
    /// Checks that the replacement is leased and matches expected attempt progression.
    pub fn new(
        expected: ExpectedConsumerDeliveryV1,
        replacement: StoredEventConsumerDeliveryV1,
    ) -> Result<Self, ConsumerStateError> {
        let ConsumerDeliveryStateV1::Leased { attempt, .. } = replacement.state else {
            return Err(ConsumerStateError::InvalidShape);
        };
        let valid = match expected {
            ExpectedConsumerDeliveryV1::Absent => attempt.get() == 1,
            ExpectedConsumerDeliveryV1::Retry {
                failed_attempts, ..
            } => failed_attempts.checked_next() == Some(attempt),
        };
        if !valid {
            return Err(ConsumerStateError::InvalidShape);
        }
        Ok(Self {
            expected,
            replacement,
        })
    }

    /// Returns expected prior state.
    #[must_use]
    pub const fn expected(&self) -> ExpectedConsumerDeliveryV1 {
        self.expected
    }

    /// Borrows the exact leased replacement.
    #[must_use]
    pub const fn replacement(&self) -> &StoredEventConsumerDeliveryV1 {
        &self.replacement
    }
}

/// Closed atomic consumer transition. Constructors live with coordinator logic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EventConsumerTransitionV1 {
    /// Create a missing consumer and lease its first bounded batch atomically.
    CreateAndLease {
        /// Initial consumer state.
        consumer: StoredEventConsumerV1,
        /// Canonical leased candidates.
        candidates: Vec<ConsumerLeaseCandidateV1>,
    },
    /// Lease a bounded batch under an exact current revision.
    Lease {
        /// Exact consumer identity hash.
        consumer_identity_hash: EventConsumerIdentityHash,
        /// Required current revision.
        expected_revision: EventConsumerRevision,
        /// Consumer state at the next revision with unchanged terminal state.
        replacement_consumer: StoredEventConsumerV1,
        /// Canonical leased candidates.
        candidates: Vec<ConsumerLeaseCandidateV1>,
    },
    /// Acknowledge one live lease and replace terminal state.
    Acknowledge {
        /// Required current consumer revision.
        expected_revision: EventConsumerRevision,
        /// Exact next consumer state.
        replacement_consumer: StoredEventConsumerV1,
        /// Acknowledged event.
        event_id: EventId,
        /// Exact live attempt token.
        token: EventLeaseToken,
        /// Current history incarnation.
        history_incarnation: u64,
        /// Coordinator-observed transition time.
        observed_at: Timestamp,
    },
    /// Release or dead-letter one live lease.
    NegativeAcknowledge {
        /// Required current consumer revision.
        expected_revision: EventConsumerRevision,
        /// Exact next consumer state.
        replacement_consumer: StoredEventConsumerV1,
        /// Exact replacement retry/dead-letter row.
        replacement_delivery: StoredEventConsumerDeliveryV1,
        /// Exact live attempt token.
        token: EventLeaseToken,
        /// Coordinator-observed transition time.
        observed_at: Timestamp,
    },
    /// Set an authorized selected-event checkpoint and remove later delivery state.
    Seek {
        /// Required current consumer revision.
        expected_revision: EventConsumerRevision,
        /// Exact next consumer state.
        replacement_consumer: StoredEventConsumerV1,
    },
    /// Remove one consumer and all delivery state.
    Retire {
        /// Exact identity hash.
        consumer_identity_hash: EventConsumerIdentityHash,
        /// Required current revision.
        expected_revision: EventConsumerRevision,
    },
    /// Atomically publish startup/restore-normalized state.
    Recover {
        /// Required current revision.
        expected_revision: EventConsumerRevision,
        /// Canonical recovery instant used for expiry and failure timestamps.
        observed_at: Timestamp,
        /// Exact next consumer state.
        replacement_consumer: StoredEventConsumerV1,
        /// Exact complete canonical replacement delivery rows.
        replacement_deliveries: Vec<StoredEventConsumerDeliveryV1>,
    },
}

impl EventConsumerTransitionV1 {
    /// Returns the exact consumer selected by this transition.
    #[must_use]
    pub fn consumer_identity_hash(&self) -> EventConsumerIdentityHash {
        match self {
            Self::CreateAndLease { consumer, .. } => consumer.identity().identity_hash(),
            Self::Lease {
                consumer_identity_hash,
                ..
            }
            | Self::Retire {
                consumer_identity_hash,
                ..
            } => *consumer_identity_hash,
            Self::Acknowledge {
                replacement_consumer,
                ..
            }
            | Self::NegativeAcknowledge {
                replacement_consumer,
                ..
            }
            | Self::Seek {
                replacement_consumer,
                ..
            }
            | Self::Recover {
                replacement_consumer,
                ..
            } => replacement_consumer.identity().identity_hash(),
        }
    }
}

/// Closed result of an atomic consumer transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventConsumerTransitionResultV1 {
    /// The exact transition committed.
    Applied,
    /// A consumer or delivery row changed since coordinator inspection.
    StateChanged,
    /// The consumer did not exist.
    NotFound,
    /// A live lease prevents seek or retirement.
    OutstandingLease,
    /// The supplied lease was absent, stale, or already resolved.
    StaleLease,
    /// The supplied lease expired before acknowledgement.
    LeaseExpired,
}

/// Complete storage mutation produced by the backend-neutral transition evaluator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EvaluatedEventConsumerTransitionV1 {
    /// No row changes; return this typed semantic outcome.
    NoChange(EventConsumerTransitionResultV1),
    /// Atomically replace the complete bounded consumer working set.
    Replace(Box<EventConsumerSnapshotV1>),
    /// Atomically remove the consumer and every delivery row.
    Retire(EventConsumerIdentityHash),
}

/// Evaluates one transition against one exact storage snapshot.
pub fn evaluate_event_consumer_transition(
    current: Option<EventConsumerSnapshotV1>,
    transition: EventConsumerTransitionV1,
) -> Result<EvaluatedEventConsumerTransitionV1, ConsumerStateError> {
    use EvaluatedEventConsumerTransitionV1::{NoChange, Replace, Retire};
    let selected = transition.consumer_identity_hash();
    if current
        .as_ref()
        .is_some_and(|snapshot| snapshot.consumer().identity().identity_hash() != selected)
    {
        return Err(ConsumerStateError::InvalidShape);
    }
    match transition {
        EventConsumerTransitionV1::CreateAndLease {
            consumer,
            candidates,
        } => {
            if current.is_some() {
                return Ok(NoChange(EventConsumerTransitionResultV1::StateChanged));
            }
            if consumer.revision() != EventConsumerRevision::first()
                || consumer.checkpoint() != ConsumerCheckpointV1::BeforeFirst
                || !consumer.sparse_resolutions().is_empty()
            {
                return Err(ConsumerStateError::InvalidShape);
            }
            let deliveries = evaluate_candidates(&consumer, &[], candidates)?;
            Ok(Replace(Box::new(EventConsumerSnapshotV1::new(
                consumer, deliveries,
            )?)))
        }
        EventConsumerTransitionV1::Lease {
            expected_revision,
            replacement_consumer,
            candidates,
            ..
        } => {
            let Some(current) = current else {
                return Ok(NoChange(EventConsumerTransitionResultV1::NotFound));
            };
            if current.consumer().revision() != expected_revision {
                return Ok(NoChange(EventConsumerTransitionResultV1::StateChanged));
            }
            validate_consumer_replacement(
                current.consumer(),
                &replacement_consumer,
                false,
                false,
                false,
            )?;
            let mut deliveries = current.deliveries().to_vec();
            for row in evaluate_candidates(current.consumer(), &deliveries, candidates)? {
                match delivery_index(&deliveries, row.event_id()) {
                    Ok(index) => deliveries[index] = row,
                    Err(index) => deliveries.insert(index, row),
                }
            }
            Ok(Replace(Box::new(EventConsumerSnapshotV1::new(
                replacement_consumer,
                deliveries,
            )?)))
        }
        EventConsumerTransitionV1::Acknowledge {
            expected_revision,
            replacement_consumer,
            event_id,
            token,
            history_incarnation,
            observed_at,
        } => {
            let Some(current) = current else {
                return Ok(NoChange(EventConsumerTransitionResultV1::NotFound));
            };
            if current.consumer().revision() != expected_revision {
                return Ok(NoChange(EventConsumerTransitionResultV1::StateChanged));
            }
            validate_consumer_replacement(
                current.consumer(),
                &replacement_consumer,
                true,
                false,
                false,
            )?;
            let Ok(index) = delivery_index(current.deliveries(), event_id) else {
                return Ok(NoChange(EventConsumerTransitionResultV1::StaleLease));
            };
            match current.deliveries()[index].state() {
                ConsumerDeliveryStateV1::Leased {
                    token: live,
                    expires_at,
                    ..
                } if live == token
                    && history_incarnation == current.consumer().history_incarnation() =>
                {
                    if observed_at >= expires_at {
                        return Ok(NoChange(EventConsumerTransitionResultV1::LeaseExpired));
                    }
                }
                _ => return Ok(NoChange(EventConsumerTransitionResultV1::StaleLease)),
            }
            if !terminally_resolves(&replacement_consumer, event_id) {
                return Err(ConsumerStateError::InvalidShape);
            }
            let mut deliveries = current.deliveries().to_vec();
            deliveries.remove(index);
            Ok(Replace(Box::new(EventConsumerSnapshotV1::new(
                replacement_consumer,
                deliveries,
            )?)))
        }
        EventConsumerTransitionV1::NegativeAcknowledge {
            expected_revision,
            replacement_consumer,
            replacement_delivery,
            token,
            observed_at,
        } => {
            let Some(current) = current else {
                return Ok(NoChange(EventConsumerTransitionResultV1::NotFound));
            };
            if current.consumer().revision() != expected_revision {
                return Ok(NoChange(EventConsumerTransitionResultV1::StateChanged));
            }
            validate_consumer_replacement(
                current.consumer(),
                &replacement_consumer,
                true,
                false,
                false,
            )?;
            let event_id = replacement_delivery.event_id();
            let Ok(index) = delivery_index(current.deliveries(), event_id) else {
                return Ok(NoChange(EventConsumerTransitionResultV1::StaleLease));
            };
            let old = &current.deliveries()[index];
            let ConsumerDeliveryStateV1::Leased {
                attempt,
                token: live,
                expires_at,
            } = old.state()
            else {
                return Ok(NoChange(EventConsumerTransitionResultV1::StaleLease));
            };
            if live != token
                || old.history_incarnation() != current.consumer().history_incarnation()
            {
                return Ok(NoChange(EventConsumerTransitionResultV1::StaleLease));
            }
            if observed_at >= expires_at {
                return Ok(NoChange(EventConsumerTransitionResultV1::LeaseExpired));
            }
            if replacement_delivery.consumer_identity_hash() != selected
                || replacement_delivery.history_incarnation()
                    != current.consumer().history_incarnation()
                || !valid_failure_replacement(attempt, replacement_delivery.state())
                || (matches!(
                    replacement_delivery.state(),
                    ConsumerDeliveryStateV1::DeadLettered { .. }
                ) != terminally_resolves(&replacement_consumer, event_id))
            {
                return Err(ConsumerStateError::InvalidShape);
            }
            let mut deliveries = current.deliveries().to_vec();
            deliveries[index] = replacement_delivery;
            Ok(Replace(Box::new(EventConsumerSnapshotV1::new(
                replacement_consumer,
                deliveries,
            )?)))
        }
        EventConsumerTransitionV1::Seek {
            expected_revision,
            replacement_consumer,
        } => {
            let Some(current) = current else {
                return Ok(NoChange(EventConsumerTransitionResultV1::NotFound));
            };
            if current.consumer().revision() != expected_revision {
                return Ok(NoChange(EventConsumerTransitionResultV1::StateChanged));
            }
            if current
                .deliveries()
                .iter()
                .any(|row| matches!(row.state(), ConsumerDeliveryStateV1::Leased { .. }))
            {
                return Ok(NoChange(EventConsumerTransitionResultV1::OutstandingLease));
            }
            validate_consumer_replacement(
                current.consumer(),
                &replacement_consumer,
                false,
                true,
                false,
            )?;
            if !replacement_consumer.sparse_resolutions().is_empty() {
                return Err(ConsumerStateError::InvalidShape);
            }
            Ok(Replace(Box::new(EventConsumerSnapshotV1::new(
                replacement_consumer,
                Vec::new(),
            )?)))
        }
        EventConsumerTransitionV1::Retire {
            expected_revision, ..
        } => {
            let Some(current) = current else {
                return Ok(NoChange(EventConsumerTransitionResultV1::NotFound));
            };
            if current.consumer().revision() != expected_revision {
                return Ok(NoChange(EventConsumerTransitionResultV1::StateChanged));
            }
            if current
                .deliveries()
                .iter()
                .any(|row| matches!(row.state(), ConsumerDeliveryStateV1::Leased { .. }))
            {
                return Ok(NoChange(EventConsumerTransitionResultV1::OutstandingLease));
            }
            Ok(Retire(selected))
        }
        EventConsumerTransitionV1::Recover {
            expected_revision,
            observed_at,
            replacement_consumer,
            replacement_deliveries,
        } => {
            let Some(current) = current else {
                return Ok(NoChange(EventConsumerTransitionResultV1::NotFound));
            };
            if current.consumer().revision() != expected_revision {
                return Ok(NoChange(EventConsumerTransitionResultV1::StateChanged));
            }
            if current.consumer().checkpoint() != replacement_consumer.checkpoint() {
                return Err(ConsumerStateError::InvalidShape);
            }
            validate_consumer_replacement(
                current.consumer(),
                &replacement_consumer,
                true,
                false,
                true,
            )?;
            validate_recovery_replacement(
                &current,
                observed_at,
                &replacement_consumer,
                &replacement_deliveries,
            )?;
            if current.consumer().history_incarnation()
                == replacement_consumer.history_incarnation()
                && current.deliveries() == replacement_deliveries
            {
                return Err(ConsumerStateError::InvalidShape);
            }
            Ok(Replace(Box::new(EventConsumerSnapshotV1::new(
                replacement_consumer,
                replacement_deliveries,
            )?)))
        }
    }
}

fn validate_consumer_replacement(
    current: &StoredEventConsumerV1,
    replacement: &StoredEventConsumerV1,
    terminal_may_change: bool,
    checkpoint_may_change: bool,
    incarnation_may_change: bool,
) -> Result<(), ConsumerStateError> {
    if current.identity() != replacement.identity()
        || current.partition_hash() != replacement.partition_hash()
        || (!incarnation_may_change
            && current.history_incarnation() != replacement.history_incarnation())
        || current.revision().checked_next() != Some(replacement.revision())
        || (!terminal_may_change
            && !checkpoint_may_change
            && (current.checkpoint() != replacement.checkpoint()
                || current.sparse_resolutions() != replacement.sparse_resolutions()))
    {
        return Err(ConsumerStateError::InvalidShape);
    }
    Ok(())
}

fn evaluate_candidates(
    consumer: &StoredEventConsumerV1,
    existing: &[StoredEventConsumerDeliveryV1],
    candidates: Vec<ConsumerLeaseCandidateV1>,
) -> Result<Vec<StoredEventConsumerDeliveryV1>, ConsumerStateError> {
    if candidates.len() > MAX_CONSUMER_BATCH_ITEMS {
        return Err(ConsumerStateError::LimitExceeded);
    }
    let mut prior = None;
    let mut rows = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let row = candidate.replacement().clone();
        if row.consumer_identity_hash() != consumer.identity().identity_hash()
            || row.history_incarnation() != consumer.history_incarnation()
            || prior.is_some_and(|event: EventId| event >= row.event_id())
        {
            return Err(ConsumerStateError::InvalidShape);
        }
        match (
            candidate.expected(),
            delivery_index(existing, row.event_id()),
        ) {
            (ExpectedConsumerDeliveryV1::Absent, Err(_)) => {}
            (
                ExpectedConsumerDeliveryV1::Retry {
                    failed_attempts,
                    eligible_at,
                },
                Ok(index),
            ) if existing[index].state()
                == ConsumerDeliveryStateV1::Retry {
                    failed_attempts,
                    eligible_at,
                } => {}
            _ => return Err(ConsumerStateError::InvalidShape),
        }
        prior = Some(row.event_id());
        rows.push(row);
    }
    Ok(rows)
}

fn valid_failure_replacement(
    attempt: EventDeliveryAttempt,
    replacement: ConsumerDeliveryStateV1,
) -> bool {
    match replacement {
        ConsumerDeliveryStateV1::Retry {
            failed_attempts, ..
        } => failed_attempts == attempt && attempt.get() < MAX_EVENT_DELIVERY_ATTEMPTS,
        ConsumerDeliveryStateV1::DeadLettered {
            failed_attempts, ..
        } => failed_attempts == attempt && attempt.get() == MAX_EVENT_DELIVERY_ATTEMPTS,
        ConsumerDeliveryStateV1::Leased { .. } => false,
    }
}

fn terminally_resolves(consumer: &StoredEventConsumerV1, event_id: EventId) -> bool {
    !consumer.checkpoint().precedes(event_id)
        || consumer
            .sparse_resolutions()
            .iter()
            .any(|resolution| resolution.event_id() == event_id)
}

fn delivery_index(
    rows: &[StoredEventConsumerDeliveryV1],
    event_id: EventId,
) -> Result<usize, usize> {
    rows.binary_search_by_key(&event_id, |row| row.event_id())
}

/// Specialized storage port for consumer inspection and atomic transitions.
pub trait EventConsumerRepository {
    /// Reads one exact bounded consumer snapshot in one storage view.
    fn inspect_event_consumer(
        &self,
        consumer_identity_hash: EventConsumerIdentityHash,
    ) -> Result<Option<EventConsumerSnapshotV1>, StorageError>;

    /// Applies one closed transition atomically without an application sequence.
    fn transition_event_consumer(
        &mut self,
        transition: EventConsumerTransitionV1,
    ) -> Result<EventConsumerTransitionResultV1, StorageError>;

    /// Reads and validates the complete bounded consumer inventory.
    fn inspect_event_consumer_inventory(
        &self,
    ) -> Result<Vec<EventConsumerSnapshotV1>, StorageError>;

    /// Returns the exact minimum conservative consumer retention frontier.
    fn event_consumer_retention_low_water(&self) -> Result<Option<u64>, StorageError>;
}

/// Normalizes every consumer lease before public operations become available.
///
/// Ordinary restart retains unexpired leases. Expired leases become immediate
/// retries or terminal dead letters. A restore-incarnation change invalidates
/// every live lease and rebinds every retained delivery row to the new fence.
pub fn recover_event_consumers<R: EventConsumerRepository>(
    repository: &mut R,
    observed_at: Timestamp,
    history_incarnation: u64,
) -> Result<(), StorageError> {
    if history_incarnation == 0 {
        return Err(invariant_error("invalid recovery incarnation"));
    }
    let inventory = repository.inspect_event_consumer_inventory()?;
    if inventory.len() > MAX_EVENT_CONSUMERS {
        return Err(invariant_error("consumer inventory exceeded bound"));
    }
    for snapshot in inventory {
        let incarnation_changed = snapshot.consumer().history_incarnation() != history_incarnation;
        let has_expired = snapshot.deliveries().iter().any(|delivery| {
            matches!(
                delivery.state(),
                ConsumerDeliveryStateV1::Leased { expires_at, .. }
                    if observed_at >= expires_at
            )
        });
        if !incarnation_changed && !has_expired {
            continue;
        }
        let (replacement_consumer, replacement_deliveries) = normalize_consumer_recovery(
            &snapshot,
            observed_at,
            history_incarnation,
            incarnation_changed,
        )?;
        let result = repository.transition_event_consumer(EventConsumerTransitionV1::Recover {
            expected_revision: snapshot.consumer().revision(),
            observed_at,
            replacement_consumer,
            replacement_deliveries,
        })?;
        if result != EventConsumerTransitionResultV1::Applied {
            return Err(invariant_error(
                "consumer changed during exclusive recovery",
            ));
        }
    }
    Ok(())
}

fn validate_recovery_replacement(
    current: &EventConsumerSnapshotV1,
    observed_at: Timestamp,
    replacement_consumer: &StoredEventConsumerV1,
    replacement_deliveries: &[StoredEventConsumerDeliveryV1],
) -> Result<(), ConsumerStateError> {
    if current.deliveries().len() != replacement_deliveries.len() {
        return Err(ConsumerStateError::InvalidShape);
    }
    let incarnation_changed =
        current.consumer().history_incarnation() != replacement_consumer.history_incarnation();
    let mut expected_sparse = current.consumer().sparse_resolutions().to_vec();
    for (before, after) in current.deliveries().iter().zip(replacement_deliveries) {
        if before.consumer_identity_hash() != after.consumer_identity_hash()
            || before.event_id() != after.event_id()
            || after.history_incarnation() != replacement_consumer.history_incarnation()
        {
            return Err(ConsumerStateError::InvalidShape);
        }
        match before.state() {
            ConsumerDeliveryStateV1::Leased {
                attempt,
                token,
                expires_at,
            } if !incarnation_changed && observed_at < expires_at => {
                if after.state()
                    != (ConsumerDeliveryStateV1::Leased {
                        attempt,
                        token,
                        expires_at,
                    })
                {
                    return Err(ConsumerStateError::InvalidShape);
                }
            }
            ConsumerDeliveryStateV1::Leased { attempt, .. }
                if attempt.get() < MAX_EVENT_DELIVERY_ATTEMPTS =>
            {
                if after.state()
                    != (ConsumerDeliveryStateV1::Retry {
                        failed_attempts: attempt,
                        eligible_at: observed_at,
                    })
                {
                    return Err(ConsumerStateError::InvalidShape);
                }
            }
            ConsumerDeliveryStateV1::Leased { attempt, .. } => {
                if after.state()
                    != (ConsumerDeliveryStateV1::DeadLettered {
                        failed_attempts: attempt,
                        dead_lettered_at: observed_at,
                        reason: ConsumerDeadLetterReasonV1::LeaseExpired,
                    })
                {
                    return Err(ConsumerStateError::InvalidShape);
                }
                match expected_sparse
                    .binary_search_by_key(&before.event_id(), |item| item.event_id())
                {
                    Ok(_) => return Err(ConsumerStateError::InvalidShape),
                    Err(index) => expected_sparse.insert(
                        index,
                        SparseConsumerResolutionV1::new(
                            before.event_id(),
                            SparseResolutionKindV1::DeadLettered,
                        ),
                    ),
                }
            }
            state if after.state() == state => {}
            _ => return Err(ConsumerStateError::InvalidShape),
        }
    }
    if replacement_consumer.sparse_resolutions() != expected_sparse {
        return Err(ConsumerStateError::InvalidShape);
    }
    Ok(())
}

fn normalize_consumer_recovery(
    snapshot: &EventConsumerSnapshotV1,
    observed_at: Timestamp,
    history_incarnation: u64,
    invalidate_all_leases: bool,
) -> Result<(StoredEventConsumerV1, Vec<StoredEventConsumerDeliveryV1>), StorageError> {
    let mut sparse = snapshot.consumer().sparse_resolutions().to_vec();
    let mut deliveries = Vec::with_capacity(snapshot.deliveries().len());
    for delivery in snapshot.deliveries() {
        let replacement_state = match delivery.state() {
            ConsumerDeliveryStateV1::Leased {
                attempt,
                expires_at,
                ..
            } if invalidate_all_leases || observed_at >= expires_at => {
                if attempt.get() == MAX_EVENT_DELIVERY_ATTEMPTS {
                    let resolution = SparseConsumerResolutionV1::new(
                        delivery.event_id(),
                        SparseResolutionKindV1::DeadLettered,
                    );
                    match sparse.binary_search_by_key(&delivery.event_id(), |item| item.event_id())
                    {
                        Ok(_) => return Err(invariant_error("duplicate recovery resolution")),
                        Err(index) => sparse.insert(index, resolution),
                    }
                    ConsumerDeliveryStateV1::DeadLettered {
                        failed_attempts: attempt,
                        dead_lettered_at: observed_at,
                        reason: ConsumerDeadLetterReasonV1::LeaseExpired,
                    }
                } else {
                    ConsumerDeliveryStateV1::Retry {
                        failed_attempts: attempt,
                        eligible_at: observed_at,
                    }
                }
            }
            state => state,
        };
        deliveries.push(
            StoredEventConsumerDeliveryV1::new(
                delivery.consumer_identity_hash(),
                delivery.event_id(),
                history_incarnation,
                replacement_state,
            )
            .map_err(|_| invariant_error("invalid recovered delivery"))?,
        );
    }
    let replacement = if invalidate_all_leases {
        StoredEventConsumerV1::checked(
            snapshot.consumer().identity().clone(),
            snapshot.consumer().partition_hash(),
            history_incarnation,
            snapshot
                .consumer()
                .revision()
                .checked_next()
                .ok_or_else(|| invariant_error("consumer recovery revision exhausted"))?,
            snapshot.consumer().checkpoint(),
            sparse,
        )
    } else {
        snapshot
            .consumer()
            .advance(snapshot.consumer().checkpoint(), sparse)
    }
    .map_err(|_| invariant_error("invalid recovered consumer"))?;
    Ok((replacement, deliveries))
}

/// Checked high-level lease intent supplied by the API-neutral coordinator.
pub struct CoordinateConsumerLeaseV1 {
    /// Complete exact durable consumer identity.
    pub identity: EventConsumerIdentityV1,
    /// Catalog-proven selected partition.
    pub partition_hash: PartitionKeyHash,
    /// Current durable history incarnation.
    pub history_incarnation: u64,
    /// Coordinator-observed current instant.
    pub observed_at: Timestamp,
    /// Common expiry for newly leased attempts.
    pub expires_at: Timestamp,
    /// Selected stream events after the current checkpoint, in order.
    pub selected_events: Vec<EventId>,
    /// Fresh opaque tokens, one per possible returned lease.
    pub tokens: Vec<EventLeaseToken>,
    /// Requested batch bound in `1..=64`.
    pub batch_limit: u8,
    /// Requested live-lease bound in `1..=64`.
    pub in_flight_limit: u8,
}

/// One newly committed lease returned without an event payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CoordinatedConsumerLeaseV1 {
    /// Authoritative selected event.
    pub event_id: EventId,
    /// Durable delivery attempt.
    pub attempt: EventDeliveryAttempt,
    /// Attempt-specific opaque token.
    pub token: EventLeaseToken,
    /// Exclusive acknowledgement deadline.
    pub expires_at: Timestamp,
}

/// Checked high-level acknowledgement intent.
pub struct CoordinateConsumerAcknowledgementV1 {
    /// Exact durable consumer identity.
    pub identity: EventConsumerIdentityV1,
    /// Selected leased event.
    pub event_id: EventId,
    /// Attempt-specific token.
    pub token: EventLeaseToken,
    /// Token restore fence.
    pub history_incarnation: u64,
    /// Coordinator-observed current instant.
    pub observed_at: Timestamp,
    /// Complete selected prefix strictly after the current checkpoint.
    pub selected_prefix: Vec<EventId>,
}

/// Checked high-level negative-acknowledgement intent.
pub struct CoordinateConsumerNegativeAcknowledgementV1 {
    /// Exact durable consumer identity.
    pub identity: EventConsumerIdentityV1,
    /// Selected leased event.
    pub event_id: EventId,
    /// Attempt-specific token.
    pub token: EventLeaseToken,
    /// Coordinator-observed current instant.
    pub observed_at: Timestamp,
    /// Retry eligibility, equal to or later than `observed_at`.
    pub eligible_at: Timestamp,
    /// Complete selected prefix strictly after the current checkpoint.
    pub selected_prefix: Vec<EventId>,
}

/// Public-safe consumer status derived from one durable snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoordinatedConsumerStatusV1 {
    /// Current compare-and-transition revision.
    pub revision: EventConsumerRevision,
    /// Current contiguous selected-event checkpoint.
    pub checkpoint: ConsumerCheckpointV1,
    /// Durable history incarnation.
    pub history_incarnation: u64,
    /// Number of live leases.
    pub live_leases: u8,
    /// Number of retry-ready or delayed rows.
    pub retries: u16,
    /// Number of retained dead-letter rows.
    pub dead_letters: u16,
}

/// Result of one high-level lease coordination attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoordinateConsumerLeaseResultV1 {
    /// Atomic transition outcome.
    pub transition: EventConsumerTransitionResultV1,
    /// Leases present only when `transition` is applied.
    pub leases: Vec<CoordinatedConsumerLeaseV1>,
    /// Post-transition status when available.
    pub status: Option<CoordinatedConsumerStatusV1>,
}

/// Inspects and projects one exact consumer without exposing durable rows.
pub fn coordinate_consumer_status<R: EventConsumerRepository>(
    repository: &R,
    identity: &EventConsumerIdentityV1,
) -> Result<Option<CoordinatedConsumerStatusV1>, StorageError> {
    repository
        .inspect_event_consumer(identity.identity_hash())
        .map(|snapshot| snapshot.as_ref().map(status_from_snapshot))
}

/// Atomically leases selected events under the inspected revision.
pub fn coordinate_consumer_lease<R: EventConsumerRepository>(
    repository: &mut R,
    request: CoordinateConsumerLeaseV1,
) -> Result<CoordinateConsumerLeaseResultV1, StorageError> {
    let identity_hash = request.identity.identity_hash();
    if request.history_incarnation == 0
        || request.observed_at >= request.expires_at
        || request.batch_limit == 0
        || usize::from(request.batch_limit) > MAX_CONSUMER_BATCH_ITEMS
        || request.in_flight_limit == 0
        || usize::from(request.in_flight_limit) > MAX_CONSUMER_IN_FLIGHT
        || request.selected_events.len() > MAX_CONSUMER_SPARSE_RESOLUTIONS + 1
        || request.tokens.len() < usize::from(request.batch_limit)
        || !strict_event_order(&request.selected_events)
    {
        return Err(invariant_error("invalid coordinated consumer lease"));
    }
    let current = repository.inspect_event_consumer(identity_hash)?;
    if let Some(snapshot) = current.as_ref()
        && (snapshot.consumer().identity() != &request.identity
            || snapshot.consumer().partition_hash() != request.partition_hash
            || snapshot.consumer().history_incarnation() != request.history_incarnation)
    {
        return Err(invariant_error("consumer identity fence mismatch"));
    }

    let live = current.as_ref().map_or(0, |snapshot| {
        snapshot
            .deliveries()
            .iter()
            .filter(|row| matches!(row.state(), ConsumerDeliveryStateV1::Leased { .. }))
            .count()
    });
    let available = usize::from(request.batch_limit)
        .min(usize::from(request.in_flight_limit).saturating_sub(live));
    let mut candidates = Vec::with_capacity(available);
    let mut leases = Vec::with_capacity(available);
    for event_id in request.selected_events {
        if candidates.len() == available {
            break;
        }
        let expected = current.as_ref().and_then(|snapshot| {
            snapshot
                .deliveries()
                .iter()
                .find(|row| row.event_id() == event_id)
                .map(|row| row.state())
        });
        let attempt = match expected {
            None => EventDeliveryAttempt::first(),
            Some(ConsumerDeliveryStateV1::Retry {
                failed_attempts,
                eligible_at,
            }) if eligible_at <= request.observed_at => failed_attempts
                .checked_next()
                .ok_or_else(|| invariant_error("retry attempt exhausted"))?,
            Some(_) => continue,
        };
        if current.as_ref().is_some_and(|snapshot| {
            !snapshot.consumer().checkpoint().precedes(event_id)
                || snapshot
                    .consumer()
                    .sparse_resolutions()
                    .iter()
                    .any(|resolution| resolution.event_id() == event_id)
        }) {
            continue;
        }
        let token = request.tokens[candidates.len()];
        let replacement = StoredEventConsumerDeliveryV1::new(
            request.identity.identity_hash(),
            event_id,
            request.history_incarnation,
            ConsumerDeliveryStateV1::Leased {
                attempt,
                token,
                expires_at: request.expires_at,
            },
        )
        .map_err(|_| invariant_error("invalid coordinated lease row"))?;
        let expected = match expected {
            None => ExpectedConsumerDeliveryV1::Absent,
            Some(ConsumerDeliveryStateV1::Retry {
                failed_attempts,
                eligible_at,
            }) => ExpectedConsumerDeliveryV1::Retry {
                failed_attempts,
                eligible_at,
            },
            Some(_) => continue,
        };
        candidates.push(
            ConsumerLeaseCandidateV1::new(expected, replacement)
                .map_err(|_| invariant_error("invalid coordinated lease candidate"))?,
        );
        leases.push(CoordinatedConsumerLeaseV1 {
            event_id,
            attempt,
            token,
            expires_at: request.expires_at,
        });
    }

    if candidates.is_empty() && current.is_some() {
        return Ok(CoordinateConsumerLeaseResultV1 {
            transition: EventConsumerTransitionResultV1::Applied,
            leases,
            status: current.as_ref().map(status_from_snapshot),
        });
    }
    let transition = match current.as_ref() {
        None => EventConsumerTransitionV1::CreateAndLease {
            consumer: StoredEventConsumerV1::initial(
                request.identity,
                request.partition_hash,
                request.history_incarnation,
            )
            .map_err(|_| invariant_error("invalid initial consumer"))?,
            candidates,
        },
        Some(snapshot) => EventConsumerTransitionV1::Lease {
            consumer_identity_hash: request.identity.identity_hash(),
            expected_revision: snapshot.consumer().revision(),
            replacement_consumer: snapshot
                .consumer()
                .advance(
                    snapshot.consumer().checkpoint(),
                    snapshot.consumer().sparse_resolutions().to_vec(),
                )
                .map_err(|_| invariant_error("consumer revision exhausted"))?,
            candidates,
        },
    };
    let outcome = repository.transition_event_consumer(transition)?;
    if outcome != EventConsumerTransitionResultV1::Applied {
        leases.clear();
    }
    let status = repository
        .inspect_event_consumer(identity_hash)?
        .as_ref()
        .map(status_from_snapshot);
    Ok(CoordinateConsumerLeaseResultV1 {
        transition: outcome,
        leases,
        status,
    })
}

/// Atomically acknowledges one lease and advances only a proven selected prefix.
pub fn coordinate_consumer_acknowledgement<R: EventConsumerRepository>(
    repository: &mut R,
    request: CoordinateConsumerAcknowledgementV1,
) -> Result<EventConsumerTransitionResultV1, StorageError> {
    coordinate_consumer_resolution(repository, request, None)
}

/// Atomically releases or dead-letters one lease.
pub fn coordinate_consumer_negative_acknowledgement<R: EventConsumerRepository>(
    repository: &mut R,
    request: CoordinateConsumerNegativeAcknowledgementV1,
) -> Result<EventConsumerTransitionResultV1, StorageError> {
    if request.eligible_at < request.observed_at {
        return Err(invariant_error("retry instant precedes observation"));
    }
    let acknowledgement = CoordinateConsumerAcknowledgementV1 {
        identity: request.identity,
        event_id: request.event_id,
        token: request.token,
        history_incarnation: 0,
        observed_at: request.observed_at,
        selected_prefix: request.selected_prefix,
    };
    coordinate_consumer_resolution(repository, acknowledgement, Some(request.eligible_at))
}

fn coordinate_consumer_resolution<R: EventConsumerRepository>(
    repository: &mut R,
    request: CoordinateConsumerAcknowledgementV1,
    retry_at: Option<Timestamp>,
) -> Result<EventConsumerTransitionResultV1, StorageError> {
    if request.selected_prefix.len() > MAX_CONSUMER_SPARSE_RESOLUTIONS + 1
        || !strict_event_order(&request.selected_prefix)
        || !request.selected_prefix.contains(&request.event_id)
    {
        return Err(invariant_error("invalid selected acknowledgement prefix"));
    }
    let Some(snapshot) = repository.inspect_event_consumer(request.identity.identity_hash())?
    else {
        return Ok(EventConsumerTransitionResultV1::NotFound);
    };
    if snapshot.consumer().identity() != &request.identity {
        return Err(invariant_error(
            "consumer acknowledgement identity mismatch",
        ));
    }
    let Some(delivery) = snapshot
        .deliveries()
        .iter()
        .find(|row| row.event_id() == request.event_id)
    else {
        return Ok(EventConsumerTransitionResultV1::StaleLease);
    };
    let ConsumerDeliveryStateV1::Leased { attempt, .. } = delivery.state() else {
        return Ok(EventConsumerTransitionResultV1::StaleLease);
    };
    let terminal_kind = match retry_at {
        None => Some(SparseResolutionKindV1::Acknowledged),
        Some(_) if attempt.get() == MAX_EVENT_DELIVERY_ATTEMPTS => {
            Some(SparseResolutionKindV1::DeadLettered)
        }
        Some(_) => None,
    };
    let (checkpoint, sparse) = resolved_terminal_state(
        snapshot.consumer(),
        &request.selected_prefix,
        request.event_id,
        terminal_kind,
    )?;
    let replacement_consumer = snapshot
        .consumer()
        .advance(checkpoint, sparse)
        .map_err(|_| invariant_error("consumer terminal state exhausted"))?;
    let transition = match retry_at {
        None => EventConsumerTransitionV1::Acknowledge {
            expected_revision: snapshot.consumer().revision(),
            replacement_consumer,
            event_id: request.event_id,
            token: request.token,
            history_incarnation: request.history_incarnation,
            observed_at: request.observed_at,
        },
        Some(eligible_at) => {
            let state = if attempt.get() == MAX_EVENT_DELIVERY_ATTEMPTS {
                ConsumerDeliveryStateV1::DeadLettered {
                    failed_attempts: attempt,
                    dead_lettered_at: request.observed_at,
                    reason: ConsumerDeadLetterReasonV1::NegativeAcknowledgement,
                }
            } else {
                ConsumerDeliveryStateV1::Retry {
                    failed_attempts: attempt,
                    eligible_at,
                }
            };
            EventConsumerTransitionV1::NegativeAcknowledge {
                expected_revision: snapshot.consumer().revision(),
                replacement_consumer,
                replacement_delivery: StoredEventConsumerDeliveryV1::new(
                    request.identity.identity_hash(),
                    request.event_id,
                    snapshot.consumer().history_incarnation(),
                    state,
                )
                .map_err(|_| invariant_error("invalid nack delivery"))?,
                token: request.token,
                observed_at: request.observed_at,
            }
        }
    };
    repository.transition_event_consumer(transition)
}

/// Atomically seeks one consumer after service-side stream membership proof.
pub fn coordinate_consumer_seek<R: EventConsumerRepository>(
    repository: &mut R,
    identity: &EventConsumerIdentityV1,
    checkpoint: ConsumerCheckpointV1,
) -> Result<EventConsumerTransitionResultV1, StorageError> {
    let Some(snapshot) = repository.inspect_event_consumer(identity.identity_hash())? else {
        return Ok(EventConsumerTransitionResultV1::NotFound);
    };
    if snapshot.consumer().identity() != identity {
        return Err(invariant_error("consumer seek identity mismatch"));
    }
    let replacement = snapshot
        .consumer()
        .seek(checkpoint)
        .map_err(|_| invariant_error("consumer seek revision exhausted"))?;
    repository.transition_event_consumer(EventConsumerTransitionV1::Seek {
        expected_revision: snapshot.consumer().revision(),
        replacement_consumer: replacement,
    })
}

/// Atomically retires one exact consumer and all delivery metadata.
pub fn coordinate_consumer_retire<R: EventConsumerRepository>(
    repository: &mut R,
    identity: &EventConsumerIdentityV1,
) -> Result<EventConsumerTransitionResultV1, StorageError> {
    let Some(snapshot) = repository.inspect_event_consumer(identity.identity_hash())? else {
        return Ok(EventConsumerTransitionResultV1::NotFound);
    };
    if snapshot.consumer().identity() != identity {
        return Err(invariant_error("consumer retire identity mismatch"));
    }
    repository.transition_event_consumer(EventConsumerTransitionV1::Retire {
        consumer_identity_hash: identity.identity_hash(),
        expected_revision: snapshot.consumer().revision(),
    })
}

fn status_from_snapshot(snapshot: &EventConsumerSnapshotV1) -> CoordinatedConsumerStatusV1 {
    let mut live_leases = 0_u8;
    let mut retries = 0_u16;
    let mut dead_letters = 0_u16;
    for delivery in snapshot.deliveries() {
        match delivery.state() {
            ConsumerDeliveryStateV1::Leased { .. } => live_leases += 1,
            ConsumerDeliveryStateV1::Retry { .. } => retries += 1,
            ConsumerDeliveryStateV1::DeadLettered { .. } => dead_letters += 1,
        }
    }
    CoordinatedConsumerStatusV1 {
        revision: snapshot.consumer().revision(),
        checkpoint: snapshot.consumer().checkpoint(),
        history_incarnation: snapshot.consumer().history_incarnation(),
        live_leases,
        retries,
        dead_letters,
    }
}

fn resolved_terminal_state(
    consumer: &StoredEventConsumerV1,
    selected_prefix: &[EventId],
    resolved_event: EventId,
    resolved_kind: Option<SparseResolutionKindV1>,
) -> Result<(ConsumerCheckpointV1, Vec<SparseConsumerResolutionV1>), StorageError> {
    let mut terminal = consumer.sparse_resolutions().to_vec();
    if let Some(kind) = resolved_kind {
        match terminal.binary_search_by_key(&resolved_event, |resolution| resolution.event_id()) {
            Ok(_) => return Err(invariant_error("duplicate terminal event")),
            Err(index) => {
                terminal.insert(index, SparseConsumerResolutionV1::new(resolved_event, kind))
            }
        }
    }
    let mut checkpoint = consumer.checkpoint();
    for event_id in selected_prefix {
        if !checkpoint.precedes(*event_id) {
            continue;
        }
        let Ok(index) = terminal.binary_search_by_key(event_id, |resolution| resolution.event_id())
        else {
            break;
        };
        checkpoint = ConsumerCheckpointV1::After(*event_id);
        terminal.remove(index);
    }
    if terminal.len() > MAX_CONSUMER_SPARSE_RESOLUTIONS {
        return Err(invariant_error("consumer sparse terminal bound exceeded"));
    }
    Ok((checkpoint, terminal))
}

fn strict_event_order(events: &[EventId]) -> bool {
    events.windows(2).all(|pair| pair[0] < pair[1])
}

fn invariant_error(_safe_context: &'static str) -> StorageError {
    StorageError::new(StorageErrorKind::InvariantViolation, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::{CommitSequence, EventDeliveryAttempt};

    struct TestRepository {
        snapshots: Vec<EventConsumerSnapshotV1>,
        transitions: usize,
    }

    impl EventConsumerRepository for TestRepository {
        fn inspect_event_consumer(
            &self,
            identity: EventConsumerIdentityHash,
        ) -> Result<Option<EventConsumerSnapshotV1>, StorageError> {
            Ok(self
                .snapshots
                .iter()
                .find(|snapshot| snapshot.consumer().identity().identity_hash() == identity)
                .cloned())
        }

        fn transition_event_consumer(
            &mut self,
            transition: EventConsumerTransitionV1,
        ) -> Result<EventConsumerTransitionResultV1, StorageError> {
            self.transitions += 1;
            let identity = transition.consumer_identity_hash();
            let current = self.inspect_event_consumer(identity)?;
            match evaluate_event_consumer_transition(current, transition)
                .map_err(|_| invariant_error("test transition invalid"))?
            {
                EvaluatedEventConsumerTransitionV1::NoChange(result) => Ok(result),
                EvaluatedEventConsumerTransitionV1::Replace(snapshot) => {
                    let index = self
                        .snapshots
                        .iter()
                        .position(|item| item.consumer().identity().identity_hash() == identity)
                        .ok_or_else(|| invariant_error("test consumer missing"))?;
                    self.snapshots[index] = *snapshot;
                    Ok(EventConsumerTransitionResultV1::Applied)
                }
                EvaluatedEventConsumerTransitionV1::Retire(_) => {
                    self.snapshots
                        .retain(|item| item.consumer().identity().identity_hash() != identity);
                    Ok(EventConsumerTransitionResultV1::Applied)
                }
            }
        }

        fn inspect_event_consumer_inventory(
            &self,
        ) -> Result<Vec<EventConsumerSnapshotV1>, StorageError> {
            Ok(self.snapshots.clone())
        }

        fn event_consumer_retention_low_water(&self) -> Result<Option<u64>, StorageError> {
            Ok(self
                .snapshots
                .iter()
                .map(|snapshot| snapshot.consumer().checkpoint().retention_frontier())
                .min())
        }
    }

    fn database_id() -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(1, [7; 10]).expect("database")
    }

    fn event(sequence: u64, ordinal: u16) -> EventId {
        EventId::new(
            CommitSequence::new(sequence).expect("sequence"),
            u32::from(ordinal),
        )
    }

    fn identity() -> EventConsumerIdentityV1 {
        EventConsumerIdentityV1::new(
            database_id(),
            ReactiveModuleHash::from_bytes([1; 32]),
            ReactiveOperationName::new("WorkspaceEvents").expect("operation"),
            QueryParameterHash::from_bytes([2; 32]),
            EventConsumerName::new("worker-1").expect("consumer"),
        )
    }

    fn initial_consumer() -> StoredEventConsumerV1 {
        StoredEventConsumerV1::initial(identity(), PartitionKeyHash::from_bytes([3; 32]), 1)
            .expect("initial")
    }

    fn leased_delivery(event_id: EventId) -> StoredEventConsumerDeliveryV1 {
        StoredEventConsumerDeliveryV1::new(
            identity().identity_hash(),
            event_id,
            1,
            ConsumerDeliveryStateV1::Leased {
                attempt: EventDeliveryAttempt::first(),
                token: EventLeaseToken::from_bytes([4; 32]),
                expires_at: Timestamp::new(10, 0).expect("timestamp"),
            },
        )
        .expect("delivery")
    }

    #[test]
    fn identity_hash_covers_every_identity_component() {
        let baseline = identity();
        let changed = EventConsumerIdentityV1::new(
            database_id(),
            ReactiveModuleHash::from_bytes([1; 32]),
            ReactiveOperationName::new("WorkspaceEvents").expect("operation"),
            QueryParameterHash::from_bytes([2; 32]),
            EventConsumerName::new("worker-2").expect("consumer"),
        );
        assert_ne!(baseline.identity_hash(), changed.identity_hash());
    }

    #[test]
    fn sparse_resolutions_are_bounded_ordered_and_after_checkpoint() {
        let initial =
            StoredEventConsumerV1::initial(identity(), PartitionKeyHash::from_bytes([3; 32]), 1)
                .expect("initial");
        let advanced = initial
            .advance(
                ConsumerCheckpointV1::After(event(1, 0)),
                vec![SparseConsumerResolutionV1::new(
                    event(2, 0),
                    SparseResolutionKindV1::Acknowledged,
                )],
            )
            .expect("advance");
        assert_eq!(advanced.revision().get(), 2);
        assert!(
            StoredEventConsumerV1::checked(
                identity(),
                PartitionKeyHash::from_bytes([3; 32]),
                1,
                EventConsumerRevision::first(),
                ConsumerCheckpointV1::After(event(2, 0)),
                vec![SparseConsumerResolutionV1::new(
                    event(1, 0),
                    SparseResolutionKindV1::Acknowledged,
                )],
            )
            .is_err()
        );
    }

    #[test]
    fn retention_frontier_keeps_the_complete_checkpoint_commit() {
        assert_eq!(ConsumerCheckpointV1::BeforeFirst.retention_frontier(), 0);
        assert_eq!(
            ConsumerCheckpointV1::After(event(7, 3)).retention_frontier(),
            6
        );
        assert_eq!(
            ConsumerCheckpointV1::After(event(1, 0)).retention_frontier(),
            0
        );
    }

    #[test]
    fn create_and_lease_is_one_checked_replacement() {
        let consumer = initial_consumer();
        let delivery = leased_delivery(event(1, 0));
        let candidate =
            ConsumerLeaseCandidateV1::new(ExpectedConsumerDeliveryV1::Absent, delivery.clone())
                .expect("candidate");
        let evaluated = evaluate_event_consumer_transition(
            None,
            EventConsumerTransitionV1::CreateAndLease {
                consumer: consumer.clone(),
                candidates: vec![candidate],
            },
        )
        .expect("evaluate");
        assert_eq!(
            evaluated,
            EvaluatedEventConsumerTransitionV1::Replace(Box::new(
                EventConsumerSnapshotV1::new(consumer, vec![delivery]).expect("snapshot")
            ))
        );
    }

    #[test]
    fn acknowledgement_at_the_expiry_deadline_is_expired() {
        let consumer = initial_consumer();
        let event_id = event(1, 0);
        let current =
            EventConsumerSnapshotV1::new(consumer.clone(), vec![leased_delivery(event_id)])
                .expect("snapshot");
        let replacement = consumer
            .advance(
                ConsumerCheckpointV1::BeforeFirst,
                vec![SparseConsumerResolutionV1::new(
                    event_id,
                    SparseResolutionKindV1::Acknowledged,
                )],
            )
            .expect("replacement");
        let evaluated = evaluate_event_consumer_transition(
            Some(current),
            EventConsumerTransitionV1::Acknowledge {
                expected_revision: EventConsumerRevision::first(),
                replacement_consumer: replacement,
                event_id,
                token: EventLeaseToken::from_bytes([4; 32]),
                history_incarnation: 1,
                observed_at: Timestamp::new(10, 0).expect("timestamp"),
            },
        )
        .expect("evaluate");
        assert_eq!(
            evaluated,
            EvaluatedEventConsumerTransitionV1::NoChange(
                EventConsumerTransitionResultV1::LeaseExpired
            )
        );
    }

    #[test]
    fn only_recovery_may_change_the_history_incarnation() {
        let consumer = initial_consumer();
        let current = EventConsumerSnapshotV1::new(consumer.clone(), Vec::new()).expect("snapshot");
        let restored = consumer.restored(2).expect("restored");
        assert_eq!(
            evaluate_event_consumer_transition(
                Some(current.clone()),
                EventConsumerTransitionV1::Seek {
                    expected_revision: EventConsumerRevision::first(),
                    replacement_consumer: restored.clone(),
                },
            ),
            Err(ConsumerStateError::InvalidShape)
        );
        assert!(matches!(
            evaluate_event_consumer_transition(
                Some(current),
                EventConsumerTransitionV1::Recover {
                    expected_revision: EventConsumerRevision::first(),
                    observed_at: Timestamp::new(1, 0).expect("time"),
                    replacement_consumer: restored,
                    replacement_deliveries: Vec::new(),
                },
            ),
            Ok(EvaluatedEventConsumerTransitionV1::Replace(_))
        ));
    }

    #[test]
    fn recovery_may_normalize_delivery_state_without_a_restore() {
        let consumer = initial_consumer();
        let event_id = event(1, 0);
        let current =
            EventConsumerSnapshotV1::new(consumer.clone(), vec![leased_delivery(event_id)])
                .expect("snapshot");
        let replacement_consumer = consumer
            .advance(
                consumer.checkpoint(),
                consumer.sparse_resolutions().to_vec(),
            )
            .expect("next revision");
        let replacement_delivery = StoredEventConsumerDeliveryV1::new(
            identity().identity_hash(),
            event_id,
            1,
            ConsumerDeliveryStateV1::Retry {
                failed_attempts: EventDeliveryAttempt::first(),
                eligible_at: Timestamp::new(10, 0).expect("timestamp"),
            },
        )
        .expect("retry");

        assert!(matches!(
            evaluate_event_consumer_transition(
                Some(current),
                EventConsumerTransitionV1::Recover {
                    expected_revision: EventConsumerRevision::first(),
                    observed_at: Timestamp::new(10, 0).expect("time"),
                    replacement_consumer,
                    replacement_deliveries: vec![replacement_delivery],
                },
            ),
            Ok(EvaluatedEventConsumerTransitionV1::Replace(_))
        ));
    }

    #[test]
    fn restart_retains_unexpired_leases_without_a_transition() {
        let snapshot =
            EventConsumerSnapshotV1::new(initial_consumer(), vec![leased_delivery(event(1, 0))])
                .expect("snapshot");
        let mut repository = TestRepository {
            snapshots: vec![snapshot.clone()],
            transitions: 0,
        };

        recover_event_consumers(
            &mut repository,
            Timestamp::new(9, 999_999_999).expect("time"),
            1,
        )
        .expect("recover");

        assert_eq!(repository.transitions, 0);
        assert_eq!(repository.snapshots, vec![snapshot]);
    }

    #[test]
    fn restart_releases_an_expired_lease_as_an_immediate_retry() {
        let snapshot =
            EventConsumerSnapshotV1::new(initial_consumer(), vec![leased_delivery(event(1, 0))])
                .expect("snapshot");
        let mut repository = TestRepository {
            snapshots: vec![snapshot],
            transitions: 0,
        };
        let observed_at = Timestamp::new(10, 0).expect("time");

        recover_event_consumers(&mut repository, observed_at, 1).expect("recover");

        assert_eq!(repository.transitions, 1);
        assert_eq!(repository.snapshots[0].consumer().revision().get(), 2);
        assert_eq!(
            repository.snapshots[0].deliveries()[0].state(),
            ConsumerDeliveryStateV1::Retry {
                failed_attempts: EventDeliveryAttempt::first(),
                eligible_at: observed_at,
            }
        );
    }

    #[test]
    fn tenth_expired_attempt_is_dead_lettered_and_terminally_sparse() {
        let event_id = event(1, 0);
        let attempt =
            EventDeliveryAttempt::new(MAX_EVENT_DELIVERY_ATTEMPTS).expect("tenth attempt");
        let delivery = StoredEventConsumerDeliveryV1::new(
            identity().identity_hash(),
            event_id,
            1,
            ConsumerDeliveryStateV1::Leased {
                attempt,
                token: EventLeaseToken::from_bytes([4; 32]),
                expires_at: Timestamp::new(10, 0).expect("expiry"),
            },
        )
        .expect("delivery");
        let snapshot =
            EventConsumerSnapshotV1::new(initial_consumer(), vec![delivery]).expect("snapshot");
        let mut repository = TestRepository {
            snapshots: vec![snapshot],
            transitions: 0,
        };
        let observed_at = Timestamp::new(10, 0).expect("time");

        recover_event_consumers(&mut repository, observed_at, 1).expect("recover");

        assert_eq!(
            repository.snapshots[0].consumer().sparse_resolutions(),
            &[SparseConsumerResolutionV1::new(
                event_id,
                SparseResolutionKindV1::DeadLettered,
            )]
        );
        assert_eq!(
            repository.snapshots[0].deliveries()[0].state(),
            ConsumerDeliveryStateV1::DeadLettered {
                failed_attempts: attempt,
                dead_lettered_at: observed_at,
                reason: ConsumerDeadLetterReasonV1::LeaseExpired,
            }
        );
    }

    #[test]
    fn restore_rebinds_rows_and_invalidates_even_unexpired_leases() {
        let snapshot =
            EventConsumerSnapshotV1::new(initial_consumer(), vec![leased_delivery(event(1, 0))])
                .expect("snapshot");
        let mut repository = TestRepository {
            snapshots: vec![snapshot],
            transitions: 0,
        };
        let observed_at = Timestamp::new(5, 0).expect("time");

        recover_event_consumers(&mut repository, observed_at, 2).expect("recover");

        let recovered = &repository.snapshots[0];
        assert_eq!(recovered.consumer().history_incarnation(), 2);
        assert_eq!(recovered.deliveries()[0].history_incarnation(), 2);
        assert_eq!(
            recovered.deliveries()[0].state(),
            ConsumerDeliveryStateV1::Retry {
                failed_attempts: EventDeliveryAttempt::first(),
                eligible_at: observed_at,
            }
        );
    }

    #[test]
    fn retry_history_does_not_consume_the_live_lease_budget() {
        let consumer = initial_consumer();
        let retries = (1_u64..=65)
            .map(|sequence| {
                StoredEventConsumerDeliveryV1::new(
                    identity().identity_hash(),
                    event(sequence, 0),
                    1,
                    ConsumerDeliveryStateV1::Retry {
                        failed_attempts: EventDeliveryAttempt::first(),
                        eligible_at: Timestamp::new(10, 0).expect("timestamp"),
                    },
                )
                .expect("retry")
            })
            .collect();
        assert!(EventConsumerSnapshotV1::new(consumer.clone(), retries).is_ok());

        let leases = (1_u64..=65)
            .map(|sequence| leased_delivery(event(sequence, 0)))
            .collect();
        assert_eq!(
            EventConsumerSnapshotV1::new(consumer, leases),
            Err(ConsumerStateError::LimitExceeded)
        );
    }
}
