//! Typed idempotency admission and deterministic-failure transitions.

use crate::{
    IdempotencyIdentity, MAX_READABLE_DIGEST_KEYS, PreEvaluationCommitContext, ReadDependencies,
    ReadSnapshot, ServiceAuditAppendIntentV1, StorageError, StorageValueError,
    StoredExecutionFailedV1, StoredOutcomeV1, StoredPendingAdmissionV1, StoredServiceAuditRecordV1,
    TransactionCurrentState, ValidationReadRequest,
};
use riffdb_types::ExecutionFailureCode;

/// A bounded newest-first set of digest-key candidate identities for one caller key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdempotencyLookupCandidatesV1(Vec<IdempotencyIdentity>);

impl IdempotencyLookupCandidatesV1 {
    /// Validates one through eight identities with one common non-digest scope.
    pub fn new(candidates: Vec<IdempotencyIdentity>) -> Result<Self, StorageValueError> {
        if candidates.is_empty() {
            return Err(StorageValueError::Empty);
        }
        if candidates.len() > MAX_READABLE_DIGEST_KEYS {
            return Err(StorageValueError::LimitExceeded);
        }
        let first = &candidates[0];
        for candidate in &candidates[1..] {
            if candidate.database_id() != first.database_id()
                || candidate.environment() != first.environment()
                || candidate.tenant_scope() != first.tenant_scope()
                || candidate.principal_id() != first.principal_id()
                || candidate.contract_lineage() != first.contract_lineage()
                || candidate.command_id() != first.command_id()
            {
                return Err(StorageValueError::IdentityMismatch);
            }
        }
        if candidates.iter().enumerate().any(|(index, candidate)| {
            candidates[..index]
                .iter()
                .any(|prior| prior.caller_key_digest() == candidate.caller_key_digest())
        }) {
            return Err(StorageValueError::Duplicate);
        }
        Ok(Self(candidates))
    }

    /// Borrows candidate identities in provider-defined newest-first order.
    #[must_use]
    pub fn as_slice(&self) -> &[IdempotencyIdentity] {
        &self.0
    }

    /// Returns whether the exact identity appears in the bounded set.
    #[must_use]
    pub fn contains(&self, identity: &IdempotencyIdentity) -> bool {
        self.0.contains(identity)
    }
}

/// The complete request for atomic admission creation or terminal resolution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmissionRequestV1 {
    lookup_candidates: IdempotencyLookupCandidatesV1,
    proposed_pending: StoredPendingAdmissionV1,
}

impl AdmissionRequestV1 {
    /// Requires the proposed write-key identity to be one lookup candidate.
    pub fn new(
        lookup_candidates: IdempotencyLookupCandidatesV1,
        checked_context: &PreEvaluationCommitContext,
    ) -> Result<Self, StorageValueError> {
        let proposed_pending = checked_context.pending();
        if lookup_candidates.as_slice().first() != Some(proposed_pending.identity()) {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self {
            lookup_candidates,
            proposed_pending: proposed_pending.clone(),
        })
    }

    /// Borrows bounded newest-first lookup candidates.
    #[must_use]
    pub const fn lookup_candidates(&self) -> &IdempotencyLookupCandidatesV1 {
        &self.lookup_candidates
    }

    /// Borrows the complete proposed durable pending state.
    #[must_use]
    pub const fn proposed_pending(&self) -> &StoredPendingAdmissionV1 {
        &self.proposed_pending
    }
}

/// One durable state returned by idempotency lookup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StoredAdmissionStateV1 {
    /// Evaluation has not yet reached a durable terminal state.
    Pending(StoredPendingAdmissionV1),
    /// A declared business outcome committed.
    StoredOutcome(StoredOutcomeV1),
    /// A deterministic execution failure became terminal without a command commit.
    ExecutionFailed(StoredExecutionFailedV1),
}

impl StoredAdmissionStateV1 {
    /// Borrows the state-consuming idempotency identity.
    #[must_use]
    pub const fn identity(&self) -> &IdempotencyIdentity {
        match self {
            Self::Pending(value) => value.identity(),
            Self::StoredOutcome(value) => value.identity(),
            Self::ExecutionFailed(value) => value.pending().identity(),
        }
    }
}

/// Closed result of atomic pending admission creation or resolution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdmissionResultV1 {
    /// The proposed pending admission was durably created.
    Created(StoredPendingAdmissionV1),
    /// An equal-input existing pending admission must be resumed unchanged.
    Resumed(StoredPendingAdmissionV1),
    /// An equal-input declared outcome is replayed unchanged.
    StoredOutcome(StoredOutcomeV1),
    /// An equal-input execution failure is replayed unchanged.
    ExecutionFailed(StoredExecutionFailedV1),
    /// The identity existed with a different canonical command input.
    InputMismatch,
    /// More than one digest candidate resolved to durable state.
    MultipleMatches,
}

/// One checked command admission paired with its mandatory `Started` audit.
///
/// Storage accepts this closed value rather than a transaction callback so the
/// only compound transition available to callers is the reviewed
/// audit-plus-idempotency admission operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuditedAdmissionRequestV1 {
    admission: AdmissionRequestV1,
    started: ServiceAuditAppendIntentV1,
}

impl AuditedAdmissionRequestV1 {
    /// Joins an exact admission with one principal-authenticated start record.
    pub fn new(
        admission: AdmissionRequestV1,
        started: ServiceAuditAppendIntentV1,
    ) -> Result<Self, StorageValueError> {
        if started.phase() != riffdb_types::ServiceAuditPhaseV1::Started
            || started.link() != riffdb_types::ServiceAuditLinkV1::None
            || started.principal().is_none()
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self { admission, started })
    }

    /// Borrows the exact idempotency admission.
    #[must_use]
    pub const fn admission(&self) -> &AdmissionRequestV1 {
        &self.admission
    }

    /// Borrows the mandatory start audit.
    #[must_use]
    pub const fn started(&self) -> &ServiceAuditAppendIntentV1 {
        &self.started
    }

    /// Consumes this compound request.
    #[must_use]
    pub fn into_parts(self) -> (AdmissionRequestV1, ServiceAuditAppendIntentV1) {
        (self.admission, self.started)
    }
}

/// Independent semantic result of one item in an audited admission group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuditedAdmissionResultV1 {
    admission: AdmissionResultV1,
    started: StoredServiceAuditRecordV1,
}

impl AuditedAdmissionResultV1 {
    /// Constructs a result only after both records committed atomically.
    pub fn new(
        admission: AdmissionResultV1,
        started: StoredServiceAuditRecordV1,
    ) -> Result<Self, StorageValueError> {
        if started.phase() != riffdb_types::ServiceAuditPhaseV1::Started {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self { admission, started })
    }

    /// Borrows the independently resolved admission state.
    #[must_use]
    pub const fn admission(&self) -> &AdmissionResultV1 {
        &self.admission
    }

    /// Borrows the durably appended start record.
    #[must_use]
    pub const fn started(&self) -> &StoredServiceAuditRecordV1 {
        &self.started
    }

    /// Consumes the result.
    #[must_use]
    pub fn into_parts(self) -> (AdmissionResultV1, StoredServiceAuditRecordV1) {
        (self.admission, self.started)
    }
}

/// Closed compound repository for audited command admission.
pub trait AuditedAdmissionRepository {
    /// Atomically appends every `Started` row and resolves every corresponding
    /// admission in FIFO order.
    fn admit_or_resolve_audited_group(
        &self,
        requests: Vec<AuditedAdmissionRequestV1>,
    ) -> Result<Vec<AuditedAdmissionResultV1>, StorageError>;
}

/// Narrow synchronous pending-admission repository.
pub trait AdmissionRepository {
    /// Atomically looks up all candidates and creates only when none exists.
    fn admit_or_resolve(
        &self,
        request: AdmissionRequestV1,
    ) -> Result<AdmissionResultV1, StorageError>;

    /// Resolves a bounded FIFO group in one physical durable transition.
    ///
    /// Each result is independent and retains input order. Production
    /// repositories override this atomically; the default is a small
    /// conformance adapter for test repositories.
    fn admit_or_resolve_group(
        &self,
        requests: Vec<AdmissionRequestV1>,
    ) -> Result<Vec<AdmissionResultV1>, StorageError> {
        if requests.is_empty() || requests.len() > crate::MAX_GROUPED_WRITE_TRANSITIONS {
            return Err(StorageError::new(
                crate::StorageErrorKind::LimitExceeded,
                None,
            ));
        }
        requests
            .into_iter()
            .map(|request| self.admit_or_resolve(request))
            .collect()
    }

    /// Performs a bounded read without creating or changing admission state.
    fn lookup_admission(
        &self,
        candidates: IdempotencyLookupCandidatesV1,
    ) -> Result<AdmissionLookupResultV1, StorageError>;

    /// Performs a bounded FIFO group of read-only lookups.
    ///
    /// Production stores may override this to share one MVCC read transaction.
    /// The default preserves semantics for small conformance repositories.
    fn lookup_admission_group(
        &self,
        candidates: Vec<IdempotencyLookupCandidatesV1>,
    ) -> Result<Vec<AdmissionLookupResultV1>, StorageError> {
        if candidates.is_empty() || candidates.len() > crate::MAX_GROUPED_WRITE_TRANSITIONS {
            return Err(StorageError::new(
                crate::StorageErrorKind::LimitExceeded,
                None,
            ));
        }
        candidates
            .into_iter()
            .map(|candidate| self.lookup_admission(candidate))
            .collect()
    }
}

/// Least-authority read-only view of durable command admission identities.
pub trait AdmissionLookupRepository {
    /// Performs a bounded read without creating or changing admission state.
    fn lookup_admission(
        &self,
        candidates: IdempotencyLookupCandidatesV1,
    ) -> Result<AdmissionLookupResultV1, StorageError>;

    /// Performs a bounded FIFO group of read-only lookups.
    fn lookup_admission_group(
        &self,
        candidates: Vec<IdempotencyLookupCandidatesV1>,
    ) -> Result<Vec<AdmissionLookupResultV1>, StorageError>;
}

impl<T> AdmissionLookupRepository for T
where
    T: AdmissionRepository + ?Sized,
{
    fn lookup_admission(
        &self,
        candidates: IdempotencyLookupCandidatesV1,
    ) -> Result<AdmissionLookupResultV1, StorageError> {
        AdmissionRepository::lookup_admission(self, candidates)
    }

    fn lookup_admission_group(
        &self,
        candidates: Vec<IdempotencyLookupCandidatesV1>,
    ) -> Result<Vec<AdmissionLookupResultV1>, StorageError> {
        AdmissionRepository::lookup_admission_group(self, candidates)
    }
}

/// Closed read-only lookup result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdmissionLookupResultV1 {
    /// No candidate identity has durable state.
    NotFound,
    /// Exactly one candidate identity has durable state.
    Found(Box<StoredAdmissionStateV1>),
    /// Multiple digest-key candidates resolved and readiness must fail closed.
    MultipleMatches,
}

/// A complete request to recheck a pending deterministic execution failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionFailureTransitionRequestV1 {
    expected_pending: StoredPendingAdmissionV1,
    validation_request: ValidationReadRequest,
    read_dependencies: ReadDependencies,
    code: ExecutionFailureCode,
}

impl ExecutionFailureTransitionRequestV1 {
    /// Copies the exact target/dependency evidence from the failed evaluation snapshot.
    pub fn new(
        expected_pending: StoredPendingAdmissionV1,
        snapshot: &ReadSnapshot,
        code: ExecutionFailureCode,
    ) -> Result<Self, StorageValueError> {
        if expected_pending.plan() != snapshot.plan() {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self {
            expected_pending,
            validation_request: snapshot.validation_request(),
            read_dependencies: snapshot.read_dependencies().clone(),
            code,
        })
    }

    /// Borrows the exact pending state that must still exist.
    #[must_use]
    pub const fn expected_pending(&self) -> &StoredPendingAdmissionV1 {
        &self.expected_pending
    }

    /// Borrows the complete transaction-current read request.
    #[must_use]
    pub const fn validation_request(&self) -> &ValidationReadRequest {
        &self.validation_request
    }

    /// Borrows the complete expected dependency evidence.
    #[must_use]
    pub const fn read_dependencies(&self) -> &ReadDependencies {
        &self.read_dependencies
    }

    /// Returns the closed deterministic failure code.
    #[must_use]
    pub const fn code(&self) -> ExecutionFailureCode {
        self.code
    }

    /// Constructs the only terminal record this request may write.
    #[must_use]
    pub fn terminal_record(&self) -> StoredExecutionFailedV1 {
        StoredExecutionFailedV1::new(self.expected_pending.clone(), self.code)
    }
}

/// Begins a short consuming pending-to-failure transition.
pub trait ExecutionFailureTransitionPort {
    /// Backend-private state after the exact pending admission is rechecked.
    type Rechecked: ExecutionFailureAdmissionRechecked;

    /// Opens the short transition and rechecks the exact admission state.
    fn begin_execution_failure(
        &self,
        request: ExecutionFailureTransitionRequestV1,
    ) -> Result<ExecutionFailureAdmissionResult<Self::Rechecked>, StorageError>;
}

/// Closed result before transaction-current dependency reads.
pub enum ExecutionFailureAdmissionResult<R> {
    /// The exact pending state remains and the transition may continue.
    Rechecked(R),
    /// The pending identity no longer exists.
    Missing,
    /// Durable admission state no longer equals the expected pending value.
    PendingMismatch,
    /// A terminal declared outcome already exists.
    StoredOutcome(StoredOutcomeV1),
    /// The exact deterministic failure is already terminal.
    ExecutionFailed(StoredExecutionFailedV1),
}

/// Consuming state that may only read the request's complete current values.
pub trait ExecutionFailureAdmissionRechecked: Sized {
    /// Backend-private state awaiting the coordinator's dependency decision.
    type AwaitingDecision: ExecutionFailureAwaitingDecision;

    /// Reads all requested entity observations and range epochs in this transaction.
    fn read_transaction_current(
        self,
    ) -> Result<(Self::AwaitingDecision, TransactionCurrentState), StorageError>;
}

/// Consuming decision state after current values are available to the coordinator.
pub trait ExecutionFailureAwaitingDecision: Sized {
    /// Atomically stores the exact request-derived failure after equal validation.
    fn terminalize(self) -> Result<StoredExecutionFailedV1, StorageError>;

    /// Proves no terminal write was requested and rolls back the short transaction.
    fn abandon(self);
}
