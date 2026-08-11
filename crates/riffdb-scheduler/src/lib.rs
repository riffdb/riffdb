#![forbid(unsafe_code)]

//! Bounded orchestration for generated, fenced application workflows.
//!
//! This crate owns no storage, authority, clock, transport, or application
//! callback. It sequences a closed application adapter whose methods must be
//! implemented with generated named queries/events and compiled commands.

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::num::{NonZeroU8, NonZeroU64};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use riffdb_types::{
    QueryModuleHash, QueryOperationName, ReactiveModuleHash, ReactiveOperationName,
    ScheduledAttemptHash, Timestamp, hash_scheduled_attempt,
};

/// Maximum components in one partition-local scheduled target.
pub const MAX_SCHEDULE_TARGET_COMPONENTS: usize = 8;
/// Maximum UTF-8 bytes in one string target component.
pub const MAX_SCHEDULE_TARGET_TEXT_BYTES: usize = 256;
/// Maximum canonical target bytes accepted for attempt derivation.
pub const MAX_SCHEDULE_TARGET_BYTES: usize = 1_024;
/// Maximum work items admitted concurrently by a scheduler worker.
pub const MAX_SCHEDULER_IN_FLIGHT: u8 = 8;
/// Maximum attempts one scheduler policy may request.
pub const MAX_SCHEDULER_ATTEMPTS: u8 = 10;
/// Maximum durable retry delay in seconds.
pub const MAX_SCHEDULER_RETRY_SECONDS: u64 = 3_600;
/// Minimum fenced lease duration in seconds.
pub const MIN_SCHEDULER_LEASE_SECONDS: u64 = 5;
/// Maximum fenced lease duration in seconds.
pub const MAX_SCHEDULER_LEASE_SECONDS: u64 = 900;

/// One exact compiler-owned source from which eligible work is selected.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ScheduleSource {
    /// One immutable named RiffQL operation.
    NamedQuery {
        /// Exact deployed module identity.
        module_hash: QueryModuleHash,
        /// Exact query source symbol.
        operation: QueryOperationName,
    },
    /// One immutable durable reactive operation.
    DurableEvents {
        /// Exact deployed reactive-module identity.
        module_hash: ReactiveModuleHash,
        /// Exact stream or contextual-subscription symbol.
        operation: ReactiveOperationName,
    },
}

/// One immutable schedule definition used in every attempt identity.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ScheduleDefinition {
    name: QueryOperationName,
    source: ScheduleSource,
}

impl ScheduleDefinition {
    /// Binds a stable schedule name to one exact generated source operation.
    #[must_use]
    pub const fn new(name: QueryOperationName, source: ScheduleSource) -> Self {
        Self { name, source }
    }

    /// Exact human-reviewable schedule symbol.
    #[must_use]
    pub const fn name(&self) -> &QueryOperationName {
        &self.name
    }

    /// Exact immutable named selection source.
    #[must_use]
    pub const fn source(&self) -> &ScheduleSource {
        &self.source
    }
}

/// One bounded symbolic component of a partition-local workflow key.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ScheduleTargetComponent {
    /// Canonical UUID bytes.
    Uuid([u8; 16]),
    /// Exact bounded UTF-8 key text.
    Text(String),
    /// Unsigned integer key component.
    U64(u64),
}

impl ScheduleTargetComponent {
    /// Checks and retains one exact string key component.
    pub fn text(value: impl Into<String>) -> Result<Self, ScheduleInputError> {
        let value = value.into();
        if value.is_empty() || value.len() > MAX_SCHEDULE_TARGET_TEXT_BYTES {
            return Err(ScheduleInputError::InvalidTarget);
        }
        Ok(Self::Text(value))
    }

    fn encoded_len(&self) -> usize {
        1 + match self {
            Self::Uuid(_) => 16,
            Self::Text(value) => 4 + value.len(),
            Self::U64(_) => 8,
        }
    }

    fn encode(&self, output: &mut Vec<u8>) {
        match self {
            Self::Uuid(value) => {
                output.push(0x01);
                output.extend_from_slice(value);
            }
            Self::Text(value) => {
                output.push(0x02);
                append_bytes(output, value.as_bytes());
            }
            Self::U64(value) => {
                output.push(0x03);
                output.extend_from_slice(&value.to_be_bytes());
            }
        }
    }
}

impl fmt::Debug for ScheduleTargetComponent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ScheduleTargetComponent([REDACTED])")
    }
}

/// One bounded target whose first component is the explicit partition route.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ScheduleTarget {
    components: Vec<ScheduleTargetComponent>,
}

impl ScheduleTarget {
    /// Creates one nonempty partition-local target.
    pub fn new(
        partition: ScheduleTargetComponent,
        key_suffix: Vec<ScheduleTargetComponent>,
    ) -> Result<Self, ScheduleInputError> {
        let count = key_suffix
            .len()
            .checked_add(1)
            .ok_or(ScheduleInputError::InvalidTarget)?;
        if count > MAX_SCHEDULE_TARGET_COMPONENTS {
            return Err(ScheduleInputError::InvalidTarget);
        }
        let encoded_len = key_suffix
            .iter()
            .try_fold(partition.encoded_len(), |sum, value| {
                sum.checked_add(value.encoded_len())
            });
        if encoded_len.is_none_or(|length| length > MAX_SCHEDULE_TARGET_BYTES) {
            return Err(ScheduleInputError::InvalidTarget);
        }
        let mut components = Vec::with_capacity(count);
        components.push(partition);
        components.extend(key_suffix);
        Ok(Self { components })
    }

    /// Explicit partition-route component.
    #[must_use]
    pub fn partition(&self) -> &ScheduleTargetComponent {
        &self.components[0]
    }

    /// Complete canonical target components.
    #[must_use]
    pub fn components(&self) -> &[ScheduleTargetComponent] {
        &self.components
    }

    fn encode(&self, output: &mut Vec<u8>) {
        output.push(u8::try_from(self.components.len()).expect("bounded target count"));
        for component in &self.components {
            component.encode(output);
        }
    }
}

impl fmt::Debug for ScheduleTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScheduleTarget")
            .field("components", &self.components.len())
            .finish_non_exhaustive()
    }
}

/// Closed attempt purpose, preventing claim/business/release key collisions.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ScheduleAttemptKind {
    /// Acquire the exact fenced lease for one durable delivery attempt.
    Claim {
        /// Nonzero delivery attempt, changed only after nack or lease expiry.
        delivery_attempt: NonZeroU8,
    },
    /// Execute one exact generated business command.
    Business(QueryOperationName),
    /// Release the fence acquired by one durable delivery attempt.
    Release {
        /// Same attempt ordinal used by the corresponding claim.
        delivery_attempt: NonZeroU8,
    },
}

impl ScheduleAttemptKind {
    fn encode(&self, output: &mut Vec<u8>) {
        match self {
            Self::Claim { delivery_attempt } => {
                output.push(0x01);
                output.push(delivery_attempt.get());
            }
            Self::Business(operation) => {
                output.push(0x02);
                append_bytes(output, operation.as_str().as_bytes());
            }
            Self::Release { delivery_attempt } => {
                output.push(0x03);
                output.push(delivery_attempt.get());
            }
        }
    }
}

/// Stable compiler-facing identity for one scheduled attempt.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ScheduledAttempt {
    hash: ScheduledAttemptHash,
    idempotency_key: String,
    due: Timestamp,
    kind: ScheduleAttemptKind,
}

impl ScheduledAttempt {
    /// Derives the exact attempt identity without consulting a clock or random source.
    #[must_use]
    pub fn derive(
        schedule: &ScheduleDefinition,
        due: Timestamp,
        target: &ScheduleTarget,
        kind: ScheduleAttemptKind,
    ) -> Self {
        let mut bytes = Vec::with_capacity(MAX_SCHEDULE_TARGET_BYTES + 512);
        bytes.push(0x01);
        append_bytes(&mut bytes, schedule.name.as_str().as_bytes());
        match &schedule.source {
            ScheduleSource::NamedQuery {
                module_hash,
                operation,
            } => {
                bytes.push(0x01);
                bytes.extend_from_slice(module_hash.as_bytes());
                append_bytes(&mut bytes, operation.as_str().as_bytes());
            }
            ScheduleSource::DurableEvents {
                module_hash,
                operation,
            } => {
                bytes.push(0x02);
                bytes.extend_from_slice(module_hash.as_bytes());
                append_bytes(&mut bytes, operation.as_str().as_bytes());
            }
        }
        bytes.extend_from_slice(&due.seconds().to_be_bytes());
        bytes.extend_from_slice(&due.nanoseconds().to_be_bytes());
        target.encode(&mut bytes);
        kind.encode(&mut bytes);
        let hash = hash_scheduled_attempt(&bytes);
        let idempotency_key = format!("sched_{}", hex(hash.as_bytes()));
        Self {
            hash,
            idempotency_key,
            due,
            kind,
        }
    }

    /// Exact domain-separated attempt hash.
    #[must_use]
    pub const fn hash(&self) -> ScheduledAttemptHash {
        self.hash
    }

    /// Stable bounded command idempotency key.
    #[must_use]
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    /// Logical due instant from the selected work item.
    #[must_use]
    pub const fn due(&self) -> Timestamp {
        self.due
    }

    /// Closed attempt purpose.
    #[must_use]
    pub const fn kind(&self) -> &ScheduleAttemptKind {
        &self.kind
    }
}

impl fmt::Debug for ScheduledAttempt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScheduledAttempt")
            .field("hash", &self.hash)
            .field("due", &self.due)
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

/// One payload-free hint that authorized work may now be available.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SchedulerWakeupHint {
    generation: NonZeroU64,
}

impl SchedulerWakeupHint {
    /// Creates a monotonically observed, payload-free wakeup generation.
    #[must_use]
    pub const fn new(generation: NonZeroU64) -> Self {
        Self { generation }
    }

    /// Opaque wakeup generation; it is not an event cursor or authority.
    #[must_use]
    pub const fn generation(self) -> NonZeroU64 {
        self.generation
    }
}

/// Fixed bounds for one scheduler worker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchedulerPolicy {
    maximum_in_flight: NonZeroU8,
    maximum_attempts: NonZeroU8,
    lease_seconds: NonZeroU64,
    base_retry_seconds: NonZeroU64,
    maximum_retry_seconds: NonZeroU64,
    maximum_wait: Duration,
}

impl SchedulerPolicy {
    /// Checks every concurrency, lease, wait, and retry bound.
    pub fn new(
        maximum_in_flight: NonZeroU8,
        maximum_attempts: NonZeroU8,
        lease_seconds: NonZeroU64,
        base_retry_seconds: NonZeroU64,
        maximum_retry_seconds: NonZeroU64,
        maximum_wait: Duration,
    ) -> Result<Self, ScheduleInputError> {
        if maximum_in_flight.get() > MAX_SCHEDULER_IN_FLIGHT
            || maximum_attempts.get() > MAX_SCHEDULER_ATTEMPTS
            || !(MIN_SCHEDULER_LEASE_SECONDS..=MAX_SCHEDULER_LEASE_SECONDS)
                .contains(&lease_seconds.get())
            || maximum_retry_seconds.get() > MAX_SCHEDULER_RETRY_SECONDS
            || base_retry_seconds > maximum_retry_seconds
            || maximum_wait > Duration::from_secs(30)
        {
            return Err(ScheduleInputError::InvalidPolicy);
        }
        Ok(Self {
            maximum_in_flight,
            maximum_attempts,
            lease_seconds,
            base_retry_seconds,
            maximum_retry_seconds,
            maximum_wait,
        })
    }

    /// Maximum concurrent work retained by the host loop.
    #[must_use]
    pub const fn maximum_in_flight(self) -> NonZeroU8 {
        self.maximum_in_flight
    }

    /// Maximum delivery attempts accepted by this worker.
    #[must_use]
    pub const fn maximum_attempts(self) -> NonZeroU8 {
        self.maximum_attempts
    }

    /// Bounded lease duration supplied to the generated claim command.
    #[must_use]
    pub const fn lease_seconds(self) -> NonZeroU64 {
        self.lease_seconds
    }

    /// Maximum long-poll wait for one named event/query selection.
    #[must_use]
    pub const fn maximum_wait(self) -> Duration {
        self.maximum_wait
    }

    /// Deterministic bounded durable nack delay for one nonzero attempt.
    #[must_use]
    pub fn retry_delay(self, attempt: NonZeroU8) -> Duration {
        let exponent = u32::from(attempt.get().saturating_sub(1).min(62));
        let multiplier = 1_u64.checked_shl(exponent).unwrap_or(u64::MAX);
        let seconds = self
            .base_retry_seconds
            .get()
            .saturating_mul(multiplier)
            .min(self.maximum_retry_seconds.get());
        Duration::from_secs(seconds)
    }
}

/// Exact fenced evidence returned by the generated claim command.
#[derive(Clone, Eq, PartialEq)]
pub struct FencedClaim {
    owner_id: [u8; 16],
    fencing_token: NonZeroU64,
    successor_revision: NonZeroU64,
}

impl FencedClaim {
    /// Retains the exact owner, nonzero fence, and claim successor revision.
    #[must_use]
    pub const fn new(
        owner_id: [u8; 16],
        fencing_token: NonZeroU64,
        successor_revision: NonZeroU64,
    ) -> Self {
        Self {
            owner_id,
            fencing_token,
            successor_revision,
        }
    }

    /// Exact scheduler owner used by subsequent generated commands.
    #[must_use]
    pub const fn owner_id(&self) -> [u8; 16] {
        self.owner_id
    }

    /// Current aggregate-local fencing token.
    #[must_use]
    pub const fn fencing_token(&self) -> NonZeroU64 {
        self.fencing_token
    }

    /// Entity revision produced by the successful claim.
    #[must_use]
    pub const fn successor_revision(&self) -> NonZeroU64 {
        self.successor_revision
    }
}

impl fmt::Debug for FencedClaim {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FencedClaim")
            .field("owner_id", &"[REDACTED]")
            .field("fencing_token", &"[REDACTED]")
            .field("successor_revision", &self.successor_revision)
            .finish()
    }
}

/// Result of one exact generated claim command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClaimDisposition {
    /// Lease acquired or replayed with exact current evidence.
    Acquired(FencedClaim),
    /// Another valid owner currently holds the lease.
    Unavailable,
}

/// Result of one exact fence-protected business command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BusinessResolution {
    successor_revision: NonZeroU64,
    replayed: bool,
}

impl BusinessResolution {
    /// Retains the command's exact generated successor revision and replay flag.
    #[must_use]
    pub const fn new(successor_revision: NonZeroU64, replayed: bool) -> Self {
        Self {
            successor_revision,
            replayed,
        }
    }

    /// Revision required by the exact release command.
    #[must_use]
    pub const fn successor_revision(self) -> NonZeroU64 {
        self.successor_revision
    }

    /// Whether same-key recovery returned the persisted business outcome.
    #[must_use]
    pub const fn replayed(self) -> bool {
        self.replayed
    }
}

/// Result of the exact release command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReleaseDisposition {
    /// Release committed or replayed.
    Released,
    /// The business command already completed and no lease remains.
    AlreadyReleased,
}

/// Result of a durable event checkpoint operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointDisposition {
    /// This worker advanced or replayed the checkpoint.
    Applied,
    /// Another authorized worker already advanced it.
    StateChanged,
}

/// Safe failure class used by orchestration; payload text stays with the adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulerFailureClass {
    /// Retry through the durable lease/checkpoint path.
    Retryable,
    /// Current application authority was revoked.
    AuthorizationRevoked,
    /// Caller cancellation stopped further application calls.
    Cancelled,
    /// A permanent generated-shape, protocol, or business failure.
    Permanent,
}

/// One adapter error with a closed scheduler disposition.
pub trait SchedulerFailure: Error {
    /// Closed redaction-safe failure class.
    fn class(&self) -> SchedulerFailureClass;
}

/// Exact scheduled work evidence obtained from a generated named operation.
pub trait ScheduledWork {
    /// Immutable schedule definition.
    fn schedule(&self) -> &ScheduleDefinition;
    /// Logical due instant carried by application state or the durable event.
    fn due(&self) -> Timestamp;
    /// Explicit partition-local target key.
    fn target(&self) -> &ScheduleTarget;
    /// Exact generated business-command symbol.
    fn business_command(&self) -> &QueryOperationName;
    /// Nonzero durable delivery or due-attempt count.
    fn attempt(&self) -> NonZeroU8;
}

/// Closed application-only port implemented with generated named operations.
///
/// No method receives storage, transaction, capability, raw key, numeric schema
/// ID, or arbitrary command/query text. Server authorization remains mandatory
/// for every method invocation; a [`FencedClaim`] is never authority.
pub trait SchedulerApplication {
    /// Generated domain work item.
    type Work: ScheduledWork;
    /// Adapter-owned checked public failure.
    type Failure: SchedulerFailure;

    /// Pulls at most one item through an exact named query or event consumer.
    fn next(
        &mut self,
        maximum_wait: Duration,
    ) -> impl Future<Output = Result<Option<Self::Work>, Self::Failure>> + Send;

    /// Resolves the stable business-command idempotency identity before any
    /// redelivery acquires a new fence.
    fn resolve_business(
        &mut self,
        work: &Self::Work,
        attempt: &ScheduledAttempt,
    ) -> impl Future<Output = Result<Option<BusinessResolution>, Self::Failure>> + Send;

    /// Invokes the exact generated fenced claim command.
    fn claim(
        &mut self,
        work: &Self::Work,
        attempt: &ScheduledAttempt,
        lease_seconds: NonZeroU64,
    ) -> impl Future<Output = Result<ClaimDisposition, Self::Failure>> + Send;

    /// Invokes the exact generated fence-protected business command.
    fn execute(
        &mut self,
        work: &Self::Work,
        claim: &FencedClaim,
        attempt: &ScheduledAttempt,
    ) -> impl Future<Output = Result<BusinessResolution, Self::Failure>> + Send;

    /// Invokes the exact generated release command.
    fn release(
        &mut self,
        work: &Self::Work,
        claim: &FencedClaim,
        expected_revision: NonZeroU64,
        attempt: &ScheduledAttempt,
    ) -> impl Future<Output = Result<ReleaseDisposition, Self::Failure>> + Send;

    /// Acknowledges only after business state and release are durably resolved.
    fn acknowledge(
        &mut self,
        work: &Self::Work,
    ) -> impl Future<Output = Result<CheckpointDisposition, Self::Failure>> + Send;

    /// Applies one bounded durable retry delay without sleeping in the worker.
    fn negative_acknowledge(
        &mut self,
        work: &Self::Work,
        retry_delay: Duration,
    ) -> impl Future<Output = Result<CheckpointDisposition, Self::Failure>> + Send;
}

/// Cooperative cancellation observed only between application operations.
#[derive(Clone, Debug, Default)]
pub struct SchedulerCancellation {
    cancelled: Arc<AtomicBool>,
}

impl SchedulerCancellation {
    /// Requests that no new application operation begin.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// Whether cancellation was observed.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

/// Named safe point at which one worker run stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulerStage {
    /// Before selection.
    Selection,
    /// Before same-key business outcome recovery.
    OutcomeRecovery,
    /// Before fenced claim.
    Claim,
    /// Before fence-protected business command.
    BusinessCommand,
    /// Before exact lease release.
    Release,
    /// Before durable event acknowledgement.
    Checkpoint,
}

/// One bounded worker turn result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchedulerRunResult {
    /// No eligible item was returned within the bounded wait.
    Idle,
    /// A lease was unavailable and the event was durably deferred.
    Deferred {
        /// Bounded durable retry delay.
        retry_after: Duration,
    },
    /// Business state, release, and checkpoint are durably resolved.
    Completed {
        /// Stable business-attempt identity.
        attempt: ScheduledAttemptHash,
        /// Whether the business result was recovered from persisted outcome.
        replayed: bool,
    },
    /// Cancellation stopped before another application operation began.
    Cancelled {
        /// First application operation not started.
        stage: SchedulerStage,
    },
}

/// One closed worker failure retaining the adapter error without formatting it.
pub enum SchedulerRunError<E> {
    /// Selected work violated configured attempt bounds.
    InvalidWork(ScheduleInputError),
    /// One exact application operation failed.
    Application {
        /// Operation stage that failed.
        stage: SchedulerStage,
        /// Closed adapter failure.
        source: E,
    },
}

impl<E> fmt::Debug for SchedulerRunError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidWork(error) => formatter.debug_tuple("InvalidWork").field(error).finish(),
            Self::Application { stage, .. } => formatter
                .debug_struct("Application")
                .field("stage", stage)
                .field("source", &"[REDACTED]")
                .finish(),
        }
    }
}

impl<E: SchedulerFailure> fmt::Display for SchedulerRunError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidWork(error) => error.fmt(formatter),
            Self::Application { stage, source } => write!(
                formatter,
                "scheduler application stage {stage:?} failed as {:?}",
                source.class()
            ),
        }
    }
}

impl<E: SchedulerFailure + 'static> Error for SchedulerRunError<E> {}

/// Stateless bounded scheduler policy runner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchedulerWorker {
    policy: SchedulerPolicy,
}

impl SchedulerWorker {
    /// Creates a worker over one checked bounded policy.
    #[must_use]
    pub const fn new(policy: SchedulerPolicy) -> Self {
        Self { policy }
    }

    /// Executes at most one complete application-owned workflow attempt.
    pub async fn run_once<A: SchedulerApplication>(
        &self,
        application: &mut A,
        _wakeup: SchedulerWakeupHint,
        cancellation: &SchedulerCancellation,
    ) -> Result<SchedulerRunResult, SchedulerRunError<A::Failure>> {
        if cancellation.is_cancelled() {
            return Ok(SchedulerRunResult::Cancelled {
                stage: SchedulerStage::Selection,
            });
        }
        let Some(work) = application
            .next(self.policy.maximum_wait())
            .await
            .map_err(|source| SchedulerRunError::Application {
                stage: SchedulerStage::Selection,
                source,
            })?
        else {
            return Ok(SchedulerRunResult::Idle);
        };
        if work.attempt().get() > self.policy.maximum_attempts().get() {
            return Err(SchedulerRunError::InvalidWork(
                ScheduleInputError::AttemptsExhausted,
            ));
        }
        if cancellation.is_cancelled() {
            return Ok(SchedulerRunResult::Cancelled {
                stage: SchedulerStage::OutcomeRecovery,
            });
        }
        let business_attempt = ScheduledAttempt::derive(
            work.schedule(),
            work.due(),
            work.target(),
            ScheduleAttemptKind::Business(work.business_command().clone()),
        );
        let recovered_business = application
            .resolve_business(&work, &business_attempt)
            .await
            .map_err(|source| SchedulerRunError::Application {
                stage: SchedulerStage::OutcomeRecovery,
                source,
            })?;
        if cancellation.is_cancelled() {
            return Ok(SchedulerRunResult::Cancelled {
                stage: SchedulerStage::Claim,
            });
        }
        let claim_attempt = ScheduledAttempt::derive(
            work.schedule(),
            work.due(),
            work.target(),
            ScheduleAttemptKind::Claim {
                delivery_attempt: work.attempt(),
            },
        );
        let claim = application
            .claim(&work, &claim_attempt, self.policy.lease_seconds())
            .await
            .map_err(|source| SchedulerRunError::Application {
                stage: SchedulerStage::Claim,
                source,
            })?;
        let ClaimDisposition::Acquired(claim) = claim else {
            let retry_after = self.policy.retry_delay(work.attempt());
            application
                .negative_acknowledge(&work, retry_after)
                .await
                .map_err(|source| SchedulerRunError::Application {
                    stage: SchedulerStage::Checkpoint,
                    source,
                })?;
            return Ok(SchedulerRunResult::Deferred { retry_after });
        };
        if cancellation.is_cancelled() {
            return Ok(SchedulerRunResult::Cancelled {
                stage: SchedulerStage::BusinessCommand,
            });
        }
        let (business, release_revision) = if let Some(business) = recovered_business {
            // The preceding delivery may have died after its business commit
            // and before release. A fresh fenced claim is therefore acquired
            // above and released below before the durable event is
            // acknowledged. Lease possession never becomes authority and a
            // recovered outcome never skips lease cleanup.
            (business, claim.successor_revision())
        } else {
            let business = application
                .execute(&work, &claim, &business_attempt)
                .await
                .map_err(|source| SchedulerRunError::Application {
                    stage: SchedulerStage::BusinessCommand,
                    source,
                })?;
            let expected_business_revision = claim
                .successor_revision()
                .get()
                .checked_add(1)
                .and_then(NonZeroU64::new)
                .ok_or(SchedulerRunError::InvalidWork(
                    ScheduleInputError::InvalidRevisionEvidence,
                ))?;
            if business.successor_revision() != expected_business_revision {
                return Err(SchedulerRunError::InvalidWork(
                    ScheduleInputError::InvalidRevisionEvidence,
                ));
            }
            (business, business.successor_revision())
        };
        if cancellation.is_cancelled() {
            return Ok(SchedulerRunResult::Cancelled {
                stage: SchedulerStage::Release,
            });
        }
        let release_attempt = ScheduledAttempt::derive(
            work.schedule(),
            work.due(),
            work.target(),
            ScheduleAttemptKind::Release {
                delivery_attempt: work.attempt(),
            },
        );
        application
            .release(&work, &claim, release_revision, &release_attempt)
            .await
            .map_err(|source| SchedulerRunError::Application {
                stage: SchedulerStage::Release,
                source,
            })?;
        if cancellation.is_cancelled() {
            return Ok(SchedulerRunResult::Cancelled {
                stage: SchedulerStage::Checkpoint,
            });
        }
        application
            .acknowledge(&work)
            .await
            .map_err(|source| SchedulerRunError::Application {
                stage: SchedulerStage::Checkpoint,
                source,
            })?;
        Ok(SchedulerRunResult::Completed {
            attempt: business_attempt.hash(),
            replayed: business.replayed(),
        })
    }
}

/// Safe invalid scheduler input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScheduleInputError {
    /// Target is empty, oversized, or has too many components.
    InvalidTarget,
    /// Worker policy exceeds an accepted bound.
    InvalidPolicy,
    /// Selected durable attempt exceeds the worker policy.
    AttemptsExhausted,
    /// Generated claim/business successor revisions were not contiguous.
    InvalidRevisionEvidence,
}

impl fmt::Display for ScheduleInputError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidTarget => "scheduled target is invalid",
            Self::InvalidPolicy => "scheduler policy is invalid",
            Self::AttemptsExhausted => "scheduled work exceeded its attempt bound",
            Self::InvalidRevisionEvidence => "workflow successor revision evidence is invalid",
        })
    }
}

impl Error for ScheduleInputError {}

fn append_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    output.extend_from_slice(
        &u32::try_from(bytes.len())
            .expect("bounded scheduler bytes")
            .to_be_bytes(),
    );
    output.extend_from_slice(bytes);
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct MockWork {
        schedule: ScheduleDefinition,
        due: Timestamp,
        target: ScheduleTarget,
        command: QueryOperationName,
        attempt: NonZeroU8,
    }

    impl ScheduledWork for MockWork {
        fn schedule(&self) -> &ScheduleDefinition {
            &self.schedule
        }

        fn due(&self) -> Timestamp {
            self.due
        }

        fn target(&self) -> &ScheduleTarget {
            &self.target
        }

        fn business_command(&self) -> &QueryOperationName {
            &self.command
        }

        fn attempt(&self) -> NonZeroU8 {
            self.attempt
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct MockFailure(SchedulerFailureClass);

    impl fmt::Display for MockFailure {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("mock scheduler failure")
        }
    }

    impl Error for MockFailure {}

    impl SchedulerFailure for MockFailure {
        fn class(&self) -> SchedulerFailureClass {
            self.0
        }
    }

    #[derive(Clone, Copy)]
    enum CancelAfter {
        Claim,
        Business,
        Release,
    }

    struct MockApplication {
        work: Option<MockWork>,
        durable_business: Option<BusinessResolution>,
        calls: Vec<&'static str>,
        attempts: Vec<ScheduledAttemptHash>,
        cancel_after: Option<CancelAfter>,
        cancellation: SchedulerCancellation,
    }

    impl MockApplication {
        fn new(work: MockWork) -> Self {
            Self {
                work: Some(work),
                durable_business: None,
                calls: Vec::new(),
                attempts: Vec::new(),
                cancel_after: None,
                cancellation: SchedulerCancellation::default(),
            }
        }
    }

    impl SchedulerApplication for MockApplication {
        type Work = MockWork;
        type Failure = MockFailure;

        fn next(
            &mut self,
            _maximum_wait: Duration,
        ) -> impl Future<Output = Result<Option<Self::Work>, Self::Failure>> + Send {
            self.calls.push("next");
            let work = self.work.take();
            async move { Ok(work) }
        }

        fn resolve_business(
            &mut self,
            _work: &Self::Work,
            attempt: &ScheduledAttempt,
        ) -> impl Future<Output = Result<Option<BusinessResolution>, Self::Failure>> + Send
        {
            self.calls.push("resolve");
            self.attempts.push(attempt.hash());
            let resolution = self.durable_business;
            async move { Ok(resolution) }
        }

        fn claim(
            &mut self,
            _work: &Self::Work,
            attempt: &ScheduledAttempt,
            _lease_seconds: NonZeroU64,
        ) -> impl Future<Output = Result<ClaimDisposition, Self::Failure>> + Send {
            self.calls.push("claim");
            self.attempts.push(attempt.hash());
            if matches!(self.cancel_after, Some(CancelAfter::Claim)) {
                self.cancellation.cancel();
            }
            async move {
                Ok(ClaimDisposition::Acquired(FencedClaim::new(
                    [0x44; 16],
                    NonZeroU64::new(3).expect("fence"),
                    NonZeroU64::new(8).expect("revision"),
                )))
            }
        }

        fn execute(
            &mut self,
            _work: &Self::Work,
            _claim: &FencedClaim,
            attempt: &ScheduledAttempt,
        ) -> impl Future<Output = Result<BusinessResolution, Self::Failure>> + Send {
            self.calls.push("execute");
            self.attempts.push(attempt.hash());
            let resolution = BusinessResolution::new(NonZeroU64::new(9).expect("revision"), false);
            self.durable_business = Some(resolution);
            if matches!(self.cancel_after, Some(CancelAfter::Business)) {
                self.cancellation.cancel();
            }
            async move { Ok(resolution) }
        }

        fn release(
            &mut self,
            _work: &Self::Work,
            _claim: &FencedClaim,
            _expected_revision: NonZeroU64,
            attempt: &ScheduledAttempt,
        ) -> impl Future<Output = Result<ReleaseDisposition, Self::Failure>> + Send {
            self.calls.push("release");
            self.attempts.push(attempt.hash());
            if matches!(self.cancel_after, Some(CancelAfter::Release)) {
                self.cancellation.cancel();
            }
            async move { Ok(ReleaseDisposition::Released) }
        }

        fn acknowledge(
            &mut self,
            _work: &Self::Work,
        ) -> impl Future<Output = Result<CheckpointDisposition, Self::Failure>> + Send {
            self.calls.push("ack");
            async move { Ok(CheckpointDisposition::Applied) }
        }

        fn negative_acknowledge(
            &mut self,
            _work: &Self::Work,
            _retry_delay: Duration,
        ) -> impl Future<Output = Result<CheckpointDisposition, Self::Failure>> + Send {
            self.calls.push("nack");
            async move { Ok(CheckpointDisposition::Applied) }
        }
    }

    fn definition() -> ScheduleDefinition {
        ScheduleDefinition::new(
            QueryOperationName::new("RunDueWork").expect("name"),
            ScheduleSource::DurableEvents {
                module_hash: ReactiveModuleHash::from_bytes([0x11; 32]),
                operation: ReactiveOperationName::new("DueWork").expect("operation"),
            },
        )
    }

    fn target() -> ScheduleTarget {
        ScheduleTarget::new(
            ScheduleTargetComponent::Uuid([0x22; 16]),
            vec![ScheduleTargetComponent::Uuid([0x33; 16])],
        )
        .expect("target")
    }

    fn work(attempt: u8) -> MockWork {
        MockWork {
            schedule: definition(),
            due: Timestamp::new(41, 7).expect("time"),
            target: target(),
            command: QueryOperationName::new("ExecuteDueWork").expect("command"),
            attempt: NonZeroU8::new(attempt).expect("attempt"),
        }
    }

    fn policy() -> SchedulerPolicy {
        SchedulerPolicy::new(
            NonZeroU8::new(4).expect("nonzero"),
            NonZeroU8::new(10).expect("nonzero"),
            NonZeroU64::new(60).expect("nonzero"),
            NonZeroU64::new(2).expect("nonzero"),
            NonZeroU64::new(60).expect("nonzero"),
            Duration::from_secs(30),
        )
        .expect("policy")
    }

    #[test]
    fn attempt_identity_is_stable_and_binds_every_semantic_component() {
        let due = Timestamp::new(41, 7).expect("time");
        let base = ScheduledAttempt::derive(
            &definition(),
            due,
            &target(),
            ScheduleAttemptKind::Claim {
                delivery_attempt: NonZeroU8::MIN,
            },
        );
        assert_eq!(base.idempotency_key().len(), 70);
        assert_eq!(
            base,
            ScheduledAttempt::derive(
                &definition(),
                due,
                &target(),
                ScheduleAttemptKind::Claim {
                    delivery_attempt: NonZeroU8::MIN,
                },
            )
        );
        assert_ne!(
            base,
            ScheduledAttempt::derive(
                &definition(),
                Timestamp::new(42, 7).expect("time"),
                &target(),
                ScheduleAttemptKind::Claim {
                    delivery_attempt: NonZeroU8::MIN,
                },
            )
        );
        assert_ne!(
            base,
            ScheduledAttempt::derive(
                &definition(),
                due,
                &target(),
                ScheduleAttemptKind::Release {
                    delivery_attempt: NonZeroU8::MIN,
                },
            )
        );
        let other_target = ScheduleTarget::new(
            ScheduleTargetComponent::Uuid([0x22; 16]),
            vec![ScheduleTargetComponent::Uuid([0x44; 16])],
        )
        .expect("target");
        assert_ne!(
            base,
            ScheduledAttempt::derive(
                &definition(),
                due,
                &other_target,
                ScheduleAttemptKind::Claim {
                    delivery_attempt: NonZeroU8::MIN,
                },
            )
        );
        let second_claim = ScheduledAttempt::derive(
            &definition(),
            due,
            &target(),
            ScheduleAttemptKind::Claim {
                delivery_attempt: NonZeroU8::new(2).expect("attempt"),
            },
        );
        assert_ne!(base, second_claim);
        let business = || {
            ScheduledAttempt::derive(
                &definition(),
                due,
                &target(),
                ScheduleAttemptKind::Business(
                    QueryOperationName::new("ExecuteDueWork").expect("command"),
                ),
            )
        };
        assert_eq!(business(), business());
        let expected = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/workflows/scheduled-attempt-v1.txt"
        ))
        .trim();
        assert_eq!(hex(business().hash().as_bytes()), expected);
    }

    #[test]
    fn application_failure_debug_never_formats_adapter_payload() {
        let failure = SchedulerRunError::Application {
            stage: SchedulerStage::BusinessCommand,
            source: MockFailure(SchedulerFailureClass::Permanent),
        };
        let rendered = format!("{failure:?}");
        assert!(rendered.contains("BusinessCommand"));
        assert!(rendered.contains("[REDACTED]"));
        assert!(!rendered.contains("mock scheduler failure"));
    }

    #[tokio::test]
    async fn worker_resolves_before_claim_and_checkpoints_only_after_release() {
        let mut application = MockApplication::new(work(1));
        let cancellation = application.cancellation.clone();
        let result = SchedulerWorker::new(policy())
            .run_once(
                &mut application,
                SchedulerWakeupHint::new(NonZeroU64::MIN),
                &cancellation,
            )
            .await
            .expect("run");

        assert!(matches!(
            result,
            SchedulerRunResult::Completed {
                replayed: false,
                ..
            }
        ));
        assert_eq!(
            application.calls,
            ["next", "resolve", "claim", "execute", "release", "ack"]
        );
    }

    #[tokio::test]
    async fn restart_after_business_commit_recovers_then_cleans_up_a_new_fence() {
        let item = work(2);
        let mut application = MockApplication::new(item);
        application.durable_business = Some(BusinessResolution::new(
            NonZeroU64::new(9).expect("revision"),
            true,
        ));
        let cancellation = application.cancellation.clone();
        let result = SchedulerWorker::new(policy())
            .run_once(
                &mut application,
                SchedulerWakeupHint::new(NonZeroU64::MIN),
                &cancellation,
            )
            .await
            .expect("run");

        assert!(matches!(
            result,
            SchedulerRunResult::Completed { replayed: true, .. }
        ));
        assert_eq!(
            application.calls,
            ["next", "resolve", "claim", "release", "ack"]
        );
    }

    #[tokio::test]
    async fn cancellation_after_each_durable_stage_starts_no_later_operation() {
        for (cancel_after, expected_stage, expected_calls) in [
            (
                CancelAfter::Claim,
                SchedulerStage::BusinessCommand,
                vec!["next", "resolve", "claim"],
            ),
            (
                CancelAfter::Business,
                SchedulerStage::Release,
                vec!["next", "resolve", "claim", "execute"],
            ),
            (
                CancelAfter::Release,
                SchedulerStage::Checkpoint,
                vec!["next", "resolve", "claim", "execute", "release"],
            ),
        ] {
            let mut application = MockApplication::new(work(1));
            application.cancel_after = Some(cancel_after);
            let cancellation = application.cancellation.clone();
            let result = SchedulerWorker::new(policy())
                .run_once(
                    &mut application,
                    SchedulerWakeupHint::new(NonZeroU64::MIN),
                    &cancellation,
                )
                .await
                .expect("run");
            assert_eq!(
                result,
                SchedulerRunResult::Cancelled {
                    stage: expected_stage
                }
            );
            assert_eq!(application.calls, expected_calls);
        }
    }

    #[test]
    fn target_policy_and_backoff_are_strictly_bounded() {
        assert!(ScheduleTargetComponent::text("x".repeat(256)).is_ok());
        assert!(ScheduleTargetComponent::text("x".repeat(257)).is_err());
        let policy = SchedulerPolicy::new(
            NonZeroU8::new(8).expect("nonzero"),
            NonZeroU8::new(10).expect("nonzero"),
            NonZeroU64::new(60).expect("nonzero"),
            NonZeroU64::new(2).expect("nonzero"),
            NonZeroU64::new(60).expect("nonzero"),
            Duration::from_secs(30),
        )
        .expect("policy");
        assert_eq!(
            policy.retry_delay(NonZeroU8::new(1).expect("attempt")),
            Duration::from_secs(2)
        );
        assert_eq!(
            policy.retry_delay(NonZeroU8::new(10).expect("attempt")),
            Duration::from_secs(60)
        );
        assert!(
            SchedulerPolicy::new(
                NonZeroU8::new(9).expect("nonzero"),
                NonZeroU8::new(10).expect("nonzero"),
                NonZeroU64::new(60).expect("nonzero"),
                NonZeroU64::new(1).expect("nonzero"),
                NonZeroU64::new(60).expect("nonzero"),
                Duration::ZERO,
            )
            .is_err()
        );
    }

    #[test]
    fn debug_output_redacts_target_owner_and_fence() {
        let target = ScheduleTarget::new(
            ScheduleTargetComponent::text("tenant-secret").expect("text"),
            vec![ScheduleTargetComponent::text("row-secret").expect("text")],
        )
        .expect("target");
        let claim = FencedClaim::new(
            [0xaa; 16],
            NonZeroU64::new(91).expect("fence"),
            NonZeroU64::new(8).expect("revision"),
        );
        let debug = format!("{target:?} {claim:?}");
        assert!(!debug.contains("tenant-secret"));
        assert!(!debug.contains("row-secret"));
        assert!(!debug.contains("91"));
    }
}
