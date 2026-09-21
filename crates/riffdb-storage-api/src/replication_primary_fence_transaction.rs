//! Closed coordinator fence transaction. Values are not authorization or proof.
use crate::{
    AuditPrincipalV1, PrimaryFenceRequestV1, StorageError, StoredPrimaryFenceAdministrationV1,
    TransactionCurrentCapabilityObservationV1,
};
use riffdb_types::Timestamp;

/// Exact request and initially authenticated actor, without assigned sequences.
#[derive(Clone, Eq, PartialEq)]
pub struct PrimaryFenceCandidateV1 {
    request: PrimaryFenceRequestV1,
    principal: AuditPrincipalV1,
}
impl PrimaryFenceCandidateV1 {
    /// Construction grants no authority. Policy must recheck transaction-current facts.
    #[must_use]
    pub const fn new(request: PrimaryFenceRequestV1, principal: AuditPrincipalV1) -> Self {
        Self { request, principal }
    }
    /// Immutable operation, selected follower generation and invocation.
    #[must_use]
    pub const fn request(&self) -> PrimaryFenceRequestV1 {
        self.request
    }
    /// Digest-free initiating principal, including the exact capability revision.
    #[must_use]
    pub const fn principal(&self) -> &AuditPrincipalV1 {
        &self.principal
    }
}
impl std::fmt::Debug for PrimaryFenceCandidateV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PrimaryFenceCandidateV1([REDACTED])")
    }
}

/// Coordinator lowering of final policy authorization, bound to the opened candidate.
#[derive(Debug)]
pub struct PrimaryFenceIntentV1 {
    candidate: PrimaryFenceCandidateV1,
    timestamp: Timestamp,
}
impl PrimaryFenceIntentV1 {
    /// Uses the fresh final authorization sample. No caller chooses a result frontier.
    #[must_use]
    pub const fn new(candidate: PrimaryFenceCandidateV1, timestamp: Timestamp) -> Self {
        Self {
            candidate,
            timestamp,
        }
    }
    /// Candidate checked against the retained physical transaction before staging.
    #[must_use]
    pub const fn candidate(&self) -> &PrimaryFenceCandidateV1 {
        &self.candidate
    }
    /// One final authorization and audit sample taken after opening the transaction.
    #[must_use]
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
}

/// Evidence returned only after checked commit/publication or exact replay.
/// Neither variant authenticates a remote source or permits follower promotion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PrimaryFenceResultV1 {
    /// Admission, administration audit and the complete V3 receipt committed atomically.
    Applied(Box<StoredPrimaryFenceAdministrationV1>),
    /// Fresh authorization selected the immutable original receipt without allocating.
    Replayed(Box<StoredPrimaryFenceAdministrationV1>),
    /// Valid source evidence did not admit this selection; no mutation or allocation.
    Refused(PrimaryFenceRefusalV1),
}

/// Closed request refusals, distinct from missing or contradictory stored evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrimaryFenceRefusalV1 {
    /// The requested database, incarnation or leadership epoch is not this source.
    LineageMismatch,
    /// The selected live registration or original generation does not exist.
    RegistrationMissingOrStale,
    /// An existing fence belongs to another operation, selection or original principal.
    FenceConflict,
}

/// Internal sole-coordinator port; no generic mutation callback or unfence exists.
/// The coordinator retains its application-admission pause until the outcome is known.
pub trait PrimaryFenceTransactionPort {
    /// Backend state retaining the drained storage barrier and physical writer.
    type Candidate: PrimaryFenceCandidateTransaction;
    /// Drains prior publication and opens the exact candidate before sampling final time.
    fn begin_primary_fence_transaction(
        &self,
        candidate: PrimaryFenceCandidateV1,
    ) -> Result<Self::Candidate, StorageError>;
}

/// Consuming state that can only resolve current authority or abandon the transaction.
pub trait PrimaryFenceCandidateTransaction: Sized {
    /// State retaining the writer and barrier across pure policy reauthorization.
    type Awaiting: PrimaryFenceAwaitingDecision;
    /// Supplies digest-free capability facts from the same transaction as source state.
    fn read_transaction_current(
        self,
    ) -> Result<
        (
            Self::Awaiting,
            Option<TransactionCurrentCapabilityObservationV1>,
        ),
        StorageError,
    >;
    /// Aborts and releases the non-durable writer barrier.
    fn abandon(self);
}

/// Only the exact coordinator intent may stage and complete the retained writer.
pub trait PrimaryFenceAwaitingDecision: Sized {
    /// Checks candidate/current-state binding and completes the atomic fence or exact replay.
    /// An uncertain outcome must fence subsequent writes until validated reopen.
    fn commit(self, intent: PrimaryFenceIntentV1) -> Result<PrimaryFenceResultV1, StorageError>;
    /// No final authorization means no mutation, including on an existing fence's retry.
    fn abandon(self);
}
