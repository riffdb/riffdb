//! API-neutral multi-step durable saga contracts (`ADR-0334`).

use std::time::Duration;

use riffdb_types::{
    ActorId, CommittedSagaStepRecordV1, ContractBundleHash, ContractLineage, ContractVersion,
    IdempotencyKey, PlanHash, SagaAdmissionStorageOutcomeV1, SagaCasOutcomeV1,
    SagaCompareExpectedV1, SagaFencingToken, SagaId, SagaLifecycleStatusV1, SagaRevision,
    SagaTerminalOutcomeRecordV1, StoredSagaInstanceV1, Timestamp,
};

use crate::{
    PortFuture, RequestContext, RequestControl, ServiceDtoError, ServiceFuture, SourceName,
    SubmittedRecord,
};

/// Maximum bounded client wait window for `start_saga` or `await_saga` (`ADR-0334`).
pub const MAX_SAGA_AWAIT_WINDOW: Duration = Duration::from_secs(30);

/// Checked request to start or idempotently replay a compiled saga.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartSagaRequest {
    saga_id: SagaId,
    lineage: ContractLineage,
    saga_name: SourceName,
    input: SubmittedRecord,
    await_window: Option<Duration>,
}

impl StartSagaRequest {
    /// Constructs a checked `StartSagaRequest`.
    pub fn new(
        saga_id: SagaId,
        lineage: ContractLineage,
        saga_name: SourceName,
        input: SubmittedRecord,
    ) -> Self {
        Self {
            saga_id,
            lineage,
            saga_name,
            input,
            await_window: None,
        }
    }

    /// Attaches a bounded wait window (`<= 30s`) to drive or await saga progress before returning.
    pub fn with_await_window(mut self, window: Duration) -> Result<Self, ServiceDtoError> {
        if window > MAX_SAGA_AWAIT_WINDOW {
            return Err(ServiceDtoError::OutOfRange);
        }
        self.await_window = Some(window);
        Ok(self)
    }

    /// Candidate UUIDv7 saga identifier (used when admitting a new instance).
    #[must_use]
    pub const fn saga_id(&self) -> SagaId {
        self.saga_id
    }

    /// Target contract lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Compiled saga symbolic name.
    #[must_use]
    pub const fn saga_name(&self) -> &SourceName {
        &self.saga_name
    }

    /// Submitted saga input record.
    #[must_use]
    pub const fn input(&self) -> &SubmittedRecord {
        &self.input
    }

    /// Optional bounded await window.
    #[must_use]
    pub const fn await_window(&self) -> Option<Duration> {
        self.await_window
    }
}

/// Whether `start_saga` admitted a new saga instance or replayed an existing one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartSagaDisposition {
    /// A new saga instance was durably admitted.
    Admitted,
    /// An existing saga instance matched the caller's idempotency key and input.
    Replayed,
}

/// Public view of one durable saga instance (`ADR-0334`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SagaInstanceView {
    saga_id: SagaId,
    contract_lineage: ContractLineage,
    contract_version: ContractVersion,
    contract_bundle_hash: ContractBundleHash,
    saga_plan_hash: PlanHash,
    saga_name: String,
    execution_role: String,
    delegated_actor_id: ActorId,
    caller_idempotency_key: IdempotencyKey,
    revision: SagaRevision,
    fencing_token: SagaFencingToken,
    lifecycle_status: SagaLifecycleStatusV1,
    current_step_index: u16,
    cancel_requested: bool,
    retry_count: u8,
    next_wakeup_at: Timestamp,
    step_ledger: Vec<CommittedSagaStepRecordV1>,
    terminal_outcome: Option<SagaTerminalOutcomeRecordV1>,
    diagnostic_reason: Option<String>,
    created_at: Timestamp,
    updated_at: Timestamp,
}

impl SagaInstanceView {
    /// Projects a public view from a durable [`StoredSagaInstanceV1`].
    #[must_use]
    pub fn from_stored(instance: &StoredSagaInstanceV1) -> Self {
        Self {
            saga_id: instance.saga_id(),
            contract_lineage: instance.contract_lineage().clone(),
            contract_version: instance.contract_version(),
            contract_bundle_hash: instance.contract_bundle_hash(),
            saga_plan_hash: instance.saga_plan_hash(),
            saga_name: instance.saga_name().to_owned(),
            execution_role: instance.execution_role().to_owned(),
            delegated_actor_id: instance.delegated_actor_id().clone(),
            caller_idempotency_key: instance.caller_idempotency_key().clone(),
            revision: instance.revision(),
            fencing_token: instance.fencing_token(),
            lifecycle_status: instance.lifecycle_status(),
            current_step_index: instance.current_step_index(),
            cancel_requested: instance.cancel_requested(),
            retry_count: instance.retry_count(),
            next_wakeup_at: instance.next_wakeup_at(),
            step_ledger: instance.step_ledger().to_vec(),
            terminal_outcome: instance.terminal_outcome().cloned(),
            diagnostic_reason: instance.diagnostic_reason().map(str::to_owned),
            created_at: instance.created_at(),
            updated_at: instance.updated_at(),
        }
    }

    /// Durable UUIDv7 saga identifier.
    #[must_use]
    pub const fn saga_id(&self) -> SagaId {
        self.saga_id
    }

    /// Pinned contract lineage.
    #[must_use]
    pub const fn contract_lineage(&self) -> &ContractLineage {
        &self.contract_lineage
    }

    /// Pinned contract version.
    #[must_use]
    pub const fn contract_version(&self) -> ContractVersion {
        self.contract_version
    }

    /// Pinned contract bundle hash.
    #[must_use]
    pub const fn contract_bundle_hash(&self) -> ContractBundleHash {
        self.contract_bundle_hash
    }

    /// Pinned saga plan hash.
    #[must_use]
    pub const fn saga_plan_hash(&self) -> PlanHash {
        self.saga_plan_hash
    }

    /// Saga symbolic name.
    #[must_use]
    pub fn saga_name(&self) -> &str {
        &self.saga_name
    }

    /// Saga execution role name.
    #[must_use]
    pub fn execution_role(&self) -> &str {
        &self.execution_role
    }

    /// Deterministic delegated actor identity.
    #[must_use]
    pub const fn delegated_actor_id(&self) -> &ActorId {
        &self.delegated_actor_id
    }

    /// Caller idempotency key.
    #[must_use]
    pub const fn caller_idempotency_key(&self) -> &IdempotencyKey {
        &self.caller_idempotency_key
    }

    /// Monotonic saga revision.
    #[must_use]
    pub const fn revision(&self) -> SagaRevision {
        self.revision
    }

    /// Monotonic worker fencing token.
    #[must_use]
    pub const fn fencing_token(&self) -> SagaFencingToken {
        self.fencing_token
    }

    /// Current durable lifecycle status.
    #[must_use]
    pub const fn lifecycle_status(&self) -> SagaLifecycleStatusV1 {
        self.lifecycle_status
    }

    /// Current step index in the compiled saga plan.
    #[must_use]
    pub const fn current_step_index(&self) -> u16 {
        self.current_step_index
    }

    /// Whether explicit cancellation has been requested.
    #[must_use]
    pub const fn cancel_requested(&self) -> bool {
        self.cancel_requested
    }

    /// Retry count on the current step.
    #[must_use]
    pub const fn retry_count(&self) -> u8 {
        self.retry_count
    }

    /// Next scheduled wakeup timestamp.
    #[must_use]
    pub const fn next_wakeup_at(&self) -> Timestamp {
        self.next_wakeup_at
    }

    /// Committed step ledger entries in execution order.
    #[must_use]
    pub fn step_ledger(&self) -> &[CommittedSagaStepRecordV1] {
        &self.step_ledger
    }

    /// Terminal outcome record when the saga has completed, aborted, or compensated.
    #[must_use]
    pub const fn terminal_outcome(&self) -> Option<&SagaTerminalOutcomeRecordV1> {
        self.terminal_outcome.as_ref()
    }

    /// Diagnostic reason when blocked or failed.
    #[must_use]
    pub fn diagnostic_reason(&self) -> Option<&str> {
        self.diagnostic_reason.as_deref()
    }

    /// Creation timestamp.
    #[must_use]
    pub const fn created_at(&self) -> Timestamp {
        self.created_at
    }

    /// Last update timestamp.
    #[must_use]
    pub const fn updated_at(&self) -> Timestamp {
        self.updated_at
    }
}

/// Result of `start_saga`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartSagaResult {
    disposition: StartSagaDisposition,
    instance: SagaInstanceView,
}

impl StartSagaResult {
    /// Creates a `StartSagaResult`.
    #[must_use]
    pub const fn new(disposition: StartSagaDisposition, instance: SagaInstanceView) -> Self {
        Self {
            disposition,
            instance,
        }
    }

    /// Whether the saga was newly admitted or replayed.
    #[must_use]
    pub const fn disposition(&self) -> StartSagaDisposition {
        self.disposition
    }

    /// Current durable saga instance view.
    #[must_use]
    pub const fn instance(&self) -> &SagaInstanceView {
        &self.instance
    }
}

/// Request to read a saga instance by ID.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GetSagaRequest {
    saga_id: SagaId,
}

impl GetSagaRequest {
    /// Constructs a `GetSagaRequest`.
    #[must_use]
    pub const fn new(saga_id: SagaId) -> Self {
        Self { saga_id }
    }

    /// Target saga ID.
    #[must_use]
    pub const fn saga_id(&self) -> SagaId {
        self.saga_id
    }
}

/// Result of `get_saga`, `await_saga`, `cancel_saga`, or `resume_saga`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GetSagaResult {
    /// The saga instance exists and is visible to the caller.
    Found(Box<SagaInstanceView>),
    /// No visible saga instance exists with that ID.
    NotFound,
}

/// Request to await a saga instance for a bounded duration (`<= 30s`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AwaitSagaRequest {
    saga_id: SagaId,
    maximum_wait: Duration,
}

impl AwaitSagaRequest {
    /// Constructs a checked `AwaitSagaRequest`.
    pub fn new(saga_id: SagaId, maximum_wait: Duration) -> Result<Self, ServiceDtoError> {
        if maximum_wait > MAX_SAGA_AWAIT_WINDOW {
            return Err(ServiceDtoError::OutOfRange);
        }
        Ok(Self {
            saga_id,
            maximum_wait,
        })
    }

    /// Target saga ID.
    #[must_use]
    pub const fn saga_id(&self) -> SagaId {
        self.saga_id
    }

    /// Maximum bounded wait duration.
    #[must_use]
    pub const fn maximum_wait(&self) -> Duration {
        self.maximum_wait
    }
}

/// Request to cancel an active saga instance and drive reverse compensation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CancelSagaRequest {
    saga_id: SagaId,
}

impl CancelSagaRequest {
    /// Constructs a `CancelSagaRequest`.
    #[must_use]
    pub const fn new(saga_id: SagaId) -> Self {
        Self { saga_id }
    }

    /// Target saga ID.
    #[must_use]
    pub const fn saga_id(&self) -> SagaId {
        self.saga_id
    }
}

/// Request to resume a blocked or failed saga instance with fresh capability authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResumeSagaRequest {
    saga_id: SagaId,
}

impl ResumeSagaRequest {
    /// Constructs a `ResumeSagaRequest`.
    #[must_use]
    pub const fn new(saga_id: SagaId) -> Self {
        Self { saga_id }
    }

    /// Target saga ID.
    #[must_use]
    pub const fn saga_id(&self) -> SagaId {
        self.saga_id
    }
}

/// Closed storage port failure for saga coordination.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SagaPortError {
    /// Storage or bounded port capacity is temporarily unavailable.
    Unavailable,
    /// Monotonic revision or fencing token sequence is exhausted.
    SequenceExhausted,
    /// Stored saga state violated a checked invariant.
    Integrity,
}

/// Consumer port for authoritative saga coordination (`ADR-0334`).
pub trait SagaCoordinationPort: Send + Sync {
    /// Atomically admits a new saga instance or returns an existing instance for the same caller idempotency key.
    fn admit_or_get_saga<'a>(
        &'a self,
        instance: StoredSagaInstanceV1,
        control: &'a RequestControl,
    ) -> PortFuture<'a, SagaAdmissionStorageOutcomeV1, SagaPortError>;

    /// Reads one durable saga instance by its UUIDv7 identifier.
    fn read_saga<'a>(
        &'a self,
        saga_id: SagaId,
        control: &'a RequestControl,
    ) -> PortFuture<'a, Option<StoredSagaInstanceV1>, SagaPortError>;

    /// Atomically transitions a saga instance under a revision and optional fencing-token check.
    fn compare_and_swap_saga<'a>(
        &'a self,
        expected: SagaCompareExpectedV1,
        next: StoredSagaInstanceV1,
        control: &'a RequestControl,
    ) -> PortFuture<'a, SagaCasOutcomeV1, SagaPortError>;

    /// Signals the background saga worker that claimable saga work is ready.
    fn notify_work(&self) {}
}

/// Durable multi-step saga orchestration (`ADR-0334`).
pub trait SagaApplication: Send + Sync {
    /// Admits or replays a compiled saga instance and optionally awaits bounded progress.
    fn start_saga(
        &self,
        _context: RequestContext,
        _request: StartSagaRequest,
    ) -> ServiceFuture<'_, StartSagaResult> {
        Box::pin(async { Err(riffdb_errors::PublicError::storage_unavailable().into()) })
    }

    /// Reads the current durable status and ledger of one saga instance.
    fn get_saga(
        &self,
        _context: RequestContext,
        _request: GetSagaRequest,
    ) -> ServiceFuture<'_, GetSagaResult> {
        Box::pin(async { Err(riffdb_errors::PublicError::storage_unavailable().into()) })
    }

    /// Waits up to a bounded window (`<= 30s`) for a saga instance to reach a terminal or blocked status.
    fn await_saga(
        &self,
        _context: RequestContext,
        _request: AwaitSagaRequest,
    ) -> ServiceFuture<'_, GetSagaResult> {
        Box::pin(async { Err(riffdb_errors::PublicError::storage_unavailable().into()) })
    }

    /// Requests explicit cancellation and reverse compensation of a non-terminal saga instance.
    fn cancel_saga(
        &self,
        _context: RequestContext,
        _request: CancelSagaRequest,
    ) -> ServiceFuture<'_, GetSagaResult> {
        Box::pin(async { Err(riffdb_errors::PublicError::storage_unavailable().into()) })
    }

    /// Resumes a blocked or failed saga instance with fresh capability authority.
    fn resume_saga(
        &self,
        _context: RequestContext,
        _request: ResumeSagaRequest,
    ) -> ServiceFuture<'_, GetSagaResult> {
        Box::pin(async { Err(riffdb_errors::PublicError::storage_unavailable().into()) })
    }
}
