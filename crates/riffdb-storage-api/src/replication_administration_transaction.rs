//! Closed coordinator transaction states; no generic mutation or release port.
use crate::{
    AuditPrincipalV1, ReplicationAdministrationRequestV1, StorageError,
    StoredReplicationAdministrationV1, TransactionCurrentCapabilityObservationV1,
};
use riffdb_types::Timestamp;

/// Exact request and initially authenticated actor, with no assigned sequence.
#[derive(Clone, Eq, PartialEq)]
pub struct ReplicationAdministrationCandidateV1 {
    request: ReplicationAdministrationRequestV1,
    principal: AuditPrincipalV1,
}
impl ReplicationAdministrationCandidateV1 {
    /// Construction grants no authority; policy rechecks the transaction observation.
    #[must_use]
    pub const fn new(
        request: ReplicationAdministrationRequestV1,
        principal: AuditPrincipalV1,
    ) -> Self {
        Self { request, principal }
    }
    /// Immutable target, action, policy or generation.
    #[must_use]
    pub const fn request(&self) -> ReplicationAdministrationRequestV1 {
        self.request
    }
    /// Digest-free initiating actor to resolve under the writer barrier.
    #[must_use]
    pub const fn principal(&self) -> &AuditPrincipalV1 {
        &self.principal
    }
}
impl std::fmt::Debug for ReplicationAdministrationCandidateV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicationAdministrationCandidateV1([REDACTED])")
    }
}

/// Coordinator lowering of the pure final authorization, bound to its candidate.
#[derive(Debug)]
pub struct ReplicationAdministrationIntentV1 {
    candidate: ReplicationAdministrationCandidateV1,
    timestamp: Timestamp,
}
impl ReplicationAdministrationIntentV1 {
    /// Reuses the fresh final authorization sample; never accepts a result sequence.
    #[must_use]
    pub const fn new(
        candidate: ReplicationAdministrationCandidateV1,
        timestamp: Timestamp,
    ) -> Self {
        Self {
            candidate,
            timestamp,
        }
    }
    /// Exact opened candidate, checked again by storage before any mutation.
    #[must_use]
    pub const fn candidate(&self) -> &ReplicationAdministrationCandidateV1 {
        &self.candidate
    }
    /// The one transaction-current authorization/audit sample.
    #[must_use]
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
}

/// Closed semantic refusal, containing no user-selected identity or diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicationAdministrationRefusalV1 {
    /// Selected database incarnation or leadership epoch is no longer current.
    LineageMismatch,
    /// An existing identity has another immutable policy or release action.
    RegistrationConflict,
    /// No audited registration matches the selected original generation.
    RegistrationMissingOrStale,
    /// A newly requested expiry has already been reached by the application head.
    ExpiryReached,
    /// The bounded source hold set is full, including retained tombstones.
    CapacityExhausted,
}

/// Applied or replayed evidence always names the original authoritative sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicationAdministrationResultV1 {
    /// One atomic hold/audit/physical receipt transition committed.
    Applied(Box<StoredReplicationAdministrationV1>),
    /// No write or allocation; the exact existing operation's receipt is returned.
    Replayed(Box<StoredReplicationAdministrationV1>),
    /// No hold change, sequence allocation or control-plane success link.
    Refused(ReplicationAdministrationRefusalV1),
}

/// Sole coordinator's consuming lifecycle transaction port.
pub trait ReplicationAdministrationTransactionPort {
    /// Backend-owned state holding the drained writer barrier.
    type Candidate: ReplicationAdministrationCandidateTransaction;
    /// Opens the exact candidate before sampling final authorization time.
    fn begin_replication_administration(
        &self,
        candidate: ReplicationAdministrationCandidateV1,
    ) -> Result<Self::Candidate, StorageError>;
}

/// Consuming state that can only read current authority or abandon.
pub trait ReplicationAdministrationCandidateTransaction: Sized {
    /// Backend-owned state retained across the pure policy decision.
    type Awaiting: ReplicationAdministrationAwaitingDecision;
    /// Resolves digest-free authority from the same transaction as the hold state.
    fn read_transaction_current(
        self,
    ) -> Result<
        (
            Self::Awaiting,
            Option<TransactionCurrentCapabilityObservationV1>,
        ),
        StorageError,
    >;
    /// Drops the uncommitted transaction and releases its writer barrier.
    fn abandon(self);
}

/// Only the matching coordinator intent may complete this transaction.
pub trait ReplicationAdministrationAwaitingDecision: Sized {
    /// Rechecks candidate/current-state binding, then applies or resolves exact replay.
    fn commit(
        self,
        intent: ReplicationAdministrationIntentV1,
    ) -> Result<ReplicationAdministrationResultV1, StorageError>;
    /// No authorization means no write, even if the selected operation exists.
    fn abandon(self);
}
