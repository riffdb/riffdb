//! Canonical durable saga state-machine value model (`ADR-0334`).

use std::error::Error;
use std::fmt;
use std::num::NonZeroU64;

use sha2::{Digest, Sha256};

use crate::{
    ActorId, ActorKind, Audience, CapabilityId, CommandId, ContractBundleHash, ContractLineage,
    ContractVersion, DatabaseId, Environment, IdempotencyKey, OutcomeId, PlanHash,
    SagaFencingToken, SagaId, SagaRevision, TenantId, TenantScope, TextIdError, Timestamp,
};

/// Domain-separated magic header identifying an encoded [`StoredSagaInstanceV1`] payload.
pub const SAGA_INSTANCE_MAGIC: &[u8] = b"riffdb.saga.instance/v1\0";

/// Maximum forward steps in one compiled saga (`ADR-0334`).
pub const MAX_SAGA_STEPS_V1: usize = 16;

/// Maximum committed step ledger records (`16` forward + `16` compensate).
pub const MAX_SAGA_STEP_LEDGER_ENTRIES_V1: usize = 32;

/// Maximum total retained outcome payload bytes across all ledger entries (`64 KiB`).
pub const MAX_SAGA_RETAINED_OUTCOME_BYTES_V1: usize = 64 * 1024;

/// Maximum encoded saga input bytes (`64 KiB`).
pub const MAX_SAGA_INPUT_BYTES_V1: usize = 64 * 1024;

/// Maximum encoded terminal outcome payload bytes (`64 KiB`).
pub const MAX_SAGA_TERMINAL_PAYLOAD_BYTES_V1: usize = 64 * 1024;

/// Maximum automatic transient-contention retries per saga step (`ADR-0334`).
pub const MAX_SAGA_RETRY_COUNT_V1: u8 = 8;

/// Maximum UTF-8 bytes in a saga name, role name, outcome name, or diagnostic string.
pub const MAX_SAGA_SYMBOL_BYTES_V1: usize = 256;

/// Authoritative lifecycle status of a durable saga instance (`ADR-0334`).
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum SagaLifecycleStatusV1 {
    /// Forward execution is in progress or ready to advance.
    RunningForward,
    /// Reverse compensation is in progress in strict LIFO order.
    Compensating,
    /// Forward execution reached a terminal success branch.
    Completed,
    /// Forward execution aborted before any effectful step committed.
    AbortedClean,
    /// Forward execution explicitly transitioned to `abort_uncompensated`.
    AbortedUncompensated,
    /// All prior effectful forward steps have been compensated in reverse order.
    Compensated,
    /// Step execution encountered transient contention and is waiting for backoff or manual resume.
    BlockedRetryable,
    /// Step execution failed live capability/policy authorization and requires `resume_saga`.
    BlockedAuthority,
    /// A compensating command returned a non-success outcome and requires operator intervention.
    CompensationFailed,
}
crate::inspect_owned_capacity_copy_v1!(SagaLifecycleStatusV1);

impl SagaLifecycleStatusV1 {
    /// Stable wire/storage tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::RunningForward => 1,
            Self::Compensating => 2,
            Self::Completed => 3,
            Self::AbortedClean => 4,
            Self::AbortedUncompensated => 5,
            Self::Compensated => 6,
            Self::BlockedRetryable => 7,
            Self::BlockedAuthority => 8,
            Self::CompensationFailed => 9,
        }
    }

    /// Decodes a stable wire/storage tag.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::RunningForward),
            2 => Some(Self::Compensating),
            3 => Some(Self::Completed),
            4 => Some(Self::AbortedClean),
            5 => Some(Self::AbortedUncompensated),
            6 => Some(Self::Compensated),
            7 => Some(Self::BlockedRetryable),
            8 => Some(Self::BlockedAuthority),
            9 => Some(Self::CompensationFailed),
            _ => None,
        }
    }

    /// Whether the saga is in a final immutable terminal state.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::AbortedClean | Self::AbortedUncompensated | Self::Compensated
        )
    }

    /// Whether the saga is paused in a blocked or failed state.
    #[must_use]
    pub const fn is_blocked(self) -> bool {
        matches!(
            self,
            Self::BlockedRetryable | Self::BlockedAuthority | Self::CompensationFailed
        )
    }

    /// Whether background workers may claim this saga when `next_wakeup_at <= now`.
    #[must_use]
    pub const fn is_claimable(self) -> bool {
        matches!(
            self,
            Self::RunningForward | Self::Compensating | Self::BlockedRetryable
        )
    }

    /// Whether `resume_saga` may transition this saga back to active execution.
    #[must_use]
    pub const fn is_resumable(self) -> bool {
        matches!(
            self,
            Self::BlockedRetryable | Self::BlockedAuthority | Self::CompensationFailed
        )
    }

    /// Stable symbolic status name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RunningForward => "running_forward",
            Self::Compensating => "compensating",
            Self::Completed => "completed",
            Self::AbortedClean => "aborted_clean",
            Self::AbortedUncompensated => "aborted_uncompensated",
            Self::Compensated => "compensated",
            Self::BlockedRetryable => "blocked_retryable",
            Self::BlockedAuthority => "blocked_authority",
            Self::CompensationFailed => "compensation_failed",
        }
    }
}

impl fmt::Display for SagaLifecycleStatusV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Execution direction of one saga step invocation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum SagaStepDirectionV1 {
    /// Forward step execution.
    Forward,
    /// Reverse compensating command execution.
    Compensate,
}
crate::inspect_owned_capacity_copy_v1!(SagaStepDirectionV1);

impl SagaStepDirectionV1 {
    /// Stable wire/storage tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::Forward => 1,
            Self::Compensate => 2,
        }
    }

    /// Decodes a stable wire/storage tag.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::Forward),
            2 => Some(Self::Compensate),
            _ => None,
        }
    }

    /// Stable lowercase direction label used in deterministic idempotency keys.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Forward => "forward",
            Self::Compensate => "compensate",
        }
    }
}

impl fmt::Display for SagaStepDirectionV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Whether a committed forward step requires compensation on a later `compensate` transition.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum SagaStepEffectDispositionV1 {
    /// The step committed authoritative state mutations and must be compensated on rollback.
    Effectful,
    /// The step committed a pure rejection or no-op outcome and is skipped during compensation.
    NoEffect,
}
crate::inspect_owned_capacity_copy_v1!(SagaStepEffectDispositionV1);

impl SagaStepEffectDispositionV1 {
    /// Stable wire/storage tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::Effectful => 1,
            Self::NoEffect => 2,
        }
    }

    /// Decodes a stable wire/storage tag.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::Effectful),
            2 => Some(Self::NoEffect),
            _ => None,
        }
    }
}

/// Retained capability identity used to reload live authority at every saga step safe point.
#[derive(Clone, Eq, PartialEq)]
pub struct SagaRetainedAuthorityV1 {
    capability_id: CapabilityId,
    capability_revision: NonZeroU64,
    principal_id: ActorId,
    actor_kind: ActorKind,
    audience: Audience,
    tenant_scope: TenantScope,
    authenticated_at: Timestamp,
}

impl SagaRetainedAuthorityV1 {
    /// Constructs a retained capability authority reference.
    #[must_use]
    pub const fn new(
        capability_id: CapabilityId,
        capability_revision: NonZeroU64,
        principal_id: ActorId,
        actor_kind: ActorKind,
        audience: Audience,
        tenant_scope: TenantScope,
        authenticated_at: Timestamp,
    ) -> Self {
        Self {
            capability_id,
            capability_revision,
            principal_id,
            actor_kind,
            audience,
            tenant_scope,
            authenticated_at,
        }
    }

    /// Stable capability identifier reloaded from `CAPABILITIES` on every step.
    #[must_use]
    pub const fn capability_id(&self) -> CapabilityId {
        self.capability_id
    }

    /// Capability revision observed when authority was bound or updated via `resume_saga`.
    #[must_use]
    pub const fn capability_revision(&self) -> NonZeroU64 {
        self.capability_revision
    }

    /// Underlying capability principal identity.
    #[must_use]
    pub const fn principal_id(&self) -> &ActorId {
        &self.principal_id
    }

    /// Trusted actor classification.
    #[must_use]
    pub const fn actor_kind(&self) -> ActorKind {
        self.actor_kind
    }

    /// Trusted transport audience.
    #[must_use]
    pub const fn audience(&self) -> &Audience {
        &self.audience
    }

    /// Effective tenant scope.
    #[must_use]
    pub const fn tenant_scope(&self) -> &TenantScope {
        &self.tenant_scope
    }

    /// Timestamp when the capability was authenticated.
    #[must_use]
    pub const fn authenticated_at(&self) -> Timestamp {
        self.authenticated_at
    }
}

impl fmt::Debug for SagaRetainedAuthorityV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SagaRetainedAuthorityV1")
            .field("capability_id", &self.capability_id)
            .field("capability_revision", &self.capability_revision)
            .field("principal_id", &"[REDACTED]")
            .finish()
    }
}

/// Active worker lease and monotonic fencing token on a claimed saga instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SagaLeaseStateV1 {
    owner_id: [u8; 16],
    fencing_token: SagaFencingToken,
    expires_at: Timestamp,
}

impl SagaLeaseStateV1 {
    /// Creates a worker lease state.
    #[must_use]
    pub const fn new(
        owner_id: [u8; 16],
        fencing_token: SagaFencingToken,
        expires_at: Timestamp,
    ) -> Self {
        Self {
            owner_id,
            fencing_token,
            expires_at,
        }
    }

    /// Worker owner identifier bytes.
    #[must_use]
    pub const fn owner_id(&self) -> &[u8; 16] {
        &self.owner_id
    }

    /// Monotonic fencing token assigned when the lease was claimed.
    #[must_use]
    pub const fn fencing_token(&self) -> SagaFencingToken {
        self.fencing_token
    }

    /// Exclusive lease expiration timestamp.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
}

/// Durable pre-invocation checkpoint persisted by `PrepareSagaStep` before command execution (`ADR-0334`).
#[derive(Clone, Eq, PartialEq)]
pub struct PendingStepInvocationV1 {
    step_index: u16,
    direction: SagaStepDirectionV1,
    command_id: CommandId,
    contract_lineage: ContractLineage,
    contract_version: ContractVersion,
    contract_bundle_hash: ContractBundleHash,
    command_plan_hash: PlanHash,
    input_bytes: Vec<u8>,
    tenant_scope: TenantScope,
    delegated_actor_id: ActorId,
    caller_idempotency_key: IdempotencyKey,
}

impl PendingStepInvocationV1 {
    /// Constructs a pinned step invocation descriptor.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        step_index: u16,
        direction: SagaStepDirectionV1,
        command_id: CommandId,
        contract_lineage: ContractLineage,
        contract_version: ContractVersion,
        contract_bundle_hash: ContractBundleHash,
        command_plan_hash: PlanHash,
        input_bytes: Vec<u8>,
        tenant_scope: TenantScope,
        delegated_actor_id: ActorId,
        caller_idempotency_key: IdempotencyKey,
    ) -> Result<Self, SagaInstanceErrorV1> {
        if usize::from(step_index) >= MAX_SAGA_STEPS_V1 {
            return Err(SagaInstanceErrorV1::StepIndexOutOfBounds { step_index });
        }
        if input_bytes.len() > MAX_SAGA_INPUT_BYTES_V1 {
            return Err(SagaInstanceErrorV1::InputPayloadTooLarge {
                actual: input_bytes.len(),
                maximum: MAX_SAGA_INPUT_BYTES_V1,
            });
        }
        Ok(Self {
            step_index,
            direction,
            command_id,
            contract_lineage,
            contract_version,
            contract_bundle_hash,
            command_plan_hash,
            input_bytes,
            tenant_scope,
            delegated_actor_id,
            caller_idempotency_key,
        })
    }

    /// Zero-based step index within the saga plan.
    #[must_use]
    pub const fn step_index(&self) -> u16 {
        self.step_index
    }

    /// Forward or compensate direction.
    #[must_use]
    pub const fn direction(&self) -> SagaStepDirectionV1 {
        self.direction
    }

    /// Target compiled command identifier.
    #[must_use]
    pub const fn command_id(&self) -> CommandId {
        self.command_id
    }

    /// Target contract lineage.
    #[must_use]
    pub const fn contract_lineage(&self) -> &ContractLineage {
        &self.contract_lineage
    }

    /// Target contract version pinned by the saga instance.
    #[must_use]
    pub const fn contract_version(&self) -> ContractVersion {
        self.contract_version
    }

    /// Target contract bundle hash pinned by the saga instance.
    #[must_use]
    pub const fn contract_bundle_hash(&self) -> ContractBundleHash {
        self.contract_bundle_hash
    }

    /// Target command plan hash.
    #[must_use]
    pub const fn command_plan_hash(&self) -> PlanHash {
        self.command_plan_hash
    }

    /// Canonical encoded command input bytes.
    #[must_use]
    pub fn input_bytes(&self) -> &[u8] {
        &self.input_bytes
    }

    /// Tenant scope bound into the step's idempotency identity.
    #[must_use]
    pub const fn tenant_scope(&self) -> &TenantScope {
        &self.tenant_scope
    }

    /// Delegated saga actor identity (`saga:<lineage>:<saga_name>:<role>`) bound into idempotency.
    #[must_use]
    pub const fn delegated_actor_id(&self) -> &ActorId {
        &self.delegated_actor_id
    }

    /// Deterministic step idempotency key (`saga-step:<saga_id>:<step_index>:<direction>`).
    #[must_use]
    pub const fn caller_idempotency_key(&self) -> &IdempotencyKey {
        &self.caller_idempotency_key
    }
}

impl fmt::Debug for PendingStepInvocationV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingStepInvocationV1")
            .field("step_index", &self.step_index)
            .field("direction", &self.direction)
            .field("command_id", &self.command_id)
            .field("input_len", &self.input_bytes.len())
            .finish()
    }
}

/// Durable ledger record of one committed forward or compensating step (`ADR-0334`).
#[derive(Clone, Eq, PartialEq)]
pub struct CommittedSagaStepRecordV1 {
    step_index: u16,
    direction: SagaStepDirectionV1,
    outcome_id: OutcomeId,
    effect_disposition: SagaStepEffectDispositionV1,
    outcome_payload_bytes: Vec<u8>,
}

impl CommittedSagaStepRecordV1 {
    /// Creates a committed saga step ledger entry.
    pub fn new(
        step_index: u16,
        direction: SagaStepDirectionV1,
        outcome_id: OutcomeId,
        effect_disposition: SagaStepEffectDispositionV1,
        outcome_payload_bytes: Vec<u8>,
    ) -> Result<Self, SagaInstanceErrorV1> {
        if usize::from(step_index) >= MAX_SAGA_STEPS_V1 {
            return Err(SagaInstanceErrorV1::StepIndexOutOfBounds { step_index });
        }
        if outcome_payload_bytes.len() > MAX_SAGA_RETAINED_OUTCOME_BYTES_V1 {
            return Err(SagaInstanceErrorV1::RetainedOutcomeStateTooLarge {
                actual: outcome_payload_bytes.len(),
                maximum: MAX_SAGA_RETAINED_OUTCOME_BYTES_V1,
            });
        }
        Ok(Self {
            step_index,
            direction,
            outcome_id,
            effect_disposition,
            outcome_payload_bytes,
        })
    }

    /// Zero-based step index.
    #[must_use]
    pub const fn step_index(&self) -> u16 {
        self.step_index
    }

    /// Forward or compensate direction.
    #[must_use]
    pub const fn direction(&self) -> SagaStepDirectionV1 {
        self.direction
    }

    /// Committed command outcome identifier.
    #[must_use]
    pub const fn outcome_id(&self) -> OutcomeId {
        self.outcome_id
    }

    /// Effectful vs no-effect disposition of this committed step.
    #[must_use]
    pub const fn effect_disposition(&self) -> SagaStepEffectDispositionV1 {
        self.effect_disposition
    }

    /// Canonical encoded outcome record bytes.
    #[must_use]
    pub fn outcome_payload_bytes(&self) -> &[u8] {
        &self.outcome_payload_bytes
    }
}

impl fmt::Debug for CommittedSagaStepRecordV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommittedSagaStepRecordV1")
            .field("step_index", &self.step_index)
            .field("direction", &self.direction)
            .field("outcome_id", &self.outcome_id)
            .field("effect_disposition", &self.effect_disposition)
            .field("payload_len", &self.outcome_payload_bytes.len())
            .finish()
    }
}

/// Terminal outcome payload recorded when a saga completes, aborts, or compensates.
#[derive(Clone, Eq, PartialEq)]
pub struct SagaTerminalOutcomeRecordV1 {
    outcome_id: OutcomeId,
    outcome_name: String,
    payload_bytes: Vec<u8>,
}

impl SagaTerminalOutcomeRecordV1 {
    /// Creates a terminal saga outcome record.
    pub fn new(
        outcome_id: OutcomeId,
        outcome_name: impl Into<String>,
        payload_bytes: Vec<u8>,
    ) -> Result<Self, SagaInstanceErrorV1> {
        let outcome_name = outcome_name.into();
        if outcome_name.is_empty() || outcome_name.len() > MAX_SAGA_SYMBOL_BYTES_V1 {
            return Err(SagaInstanceErrorV1::InvalidSymbol);
        }
        if payload_bytes.len() > MAX_SAGA_TERMINAL_PAYLOAD_BYTES_V1 {
            return Err(SagaInstanceErrorV1::TerminalPayloadTooLarge {
                actual: payload_bytes.len(),
                maximum: MAX_SAGA_TERMINAL_PAYLOAD_BYTES_V1,
            });
        }
        Ok(Self {
            outcome_id,
            outcome_name,
            payload_bytes,
        })
    }

    /// Terminal outcome identifier.
    #[must_use]
    pub const fn outcome_id(&self) -> OutcomeId {
        self.outcome_id
    }

    /// Terminal outcome symbolic name.
    #[must_use]
    pub fn outcome_name(&self) -> &str {
        &self.outcome_name
    }

    /// Canonical encoded terminal outcome record bytes.
    #[must_use]
    pub fn payload_bytes(&self) -> &[u8] {
        &self.payload_bytes
    }
}

impl fmt::Debug for SagaTerminalOutcomeRecordV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SagaTerminalOutcomeRecordV1")
            .field("outcome_id", &self.outcome_id)
            .field("outcome_name", &self.outcome_name)
            .field("payload_len", &self.payload_bytes.len())
            .finish()
    }
}

/// Validation or decoding failure for a [`StoredSagaInstanceV1`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SagaInstanceErrorV1 {
    /// A symbolic name or diagnostic string is empty or exceeds `MAX_SAGA_SYMBOL_BYTES_V1`.
    InvalidSymbol,
    /// A step index exceeds `MAX_SAGA_STEPS_V1 - 1`.
    StepIndexOutOfBounds {
        /// Supplied step index.
        step_index: u16,
    },
    /// Saga input payload exceeds `MAX_SAGA_INPUT_BYTES_V1`.
    InputPayloadTooLarge {
        /// Supplied byte length.
        actual: usize,
        /// Maximum permitted byte length.
        maximum: usize,
    },
    /// Total retained outcome bytes across `step_ledger` exceed `MAX_SAGA_RETAINED_OUTCOME_BYTES_V1`.
    RetainedOutcomeStateTooLarge {
        /// Supplied total byte length.
        actual: usize,
        /// Maximum permitted byte length.
        maximum: usize,
    },
    /// Terminal outcome payload exceeds `MAX_SAGA_TERMINAL_PAYLOAD_BYTES_V1`.
    TerminalPayloadTooLarge {
        /// Supplied byte length.
        actual: usize,
        /// Maximum permitted byte length.
        maximum: usize,
    },
    /// Step ledger entries exceed `MAX_SAGA_STEP_LEDGER_ENTRIES_V1` or contain duplicates.
    InvalidStepLedger,
    /// Retry count exceeds `MAX_SAGA_RETRY_COUNT_V1`.
    RetryCountExceeded {
        /// Supplied retry count.
        retry_count: u8,
    },
    /// A terminal saga still carries a lease or pending step invocation.
    TerminalStateHasActiveLeaseOrPendingInvocation,
    /// Binary payload is malformed or uses an unsupported version.
    CorruptEncoding,
}

impl fmt::Display for SagaInstanceErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSymbol => formatter.write_str("saga symbol is empty or too long"),
            Self::StepIndexOutOfBounds { step_index } => {
                write!(formatter, "saga step index {step_index} is out of bounds")
            }
            Self::InputPayloadTooLarge { actual, maximum } => write!(
                formatter,
                "saga input payload is {actual} bytes (max {maximum} bytes)"
            ),
            Self::RetainedOutcomeStateTooLarge { actual, maximum } => write!(
                formatter,
                "retained saga step outcome state is {actual} bytes (max {maximum} bytes)"
            ),
            Self::TerminalPayloadTooLarge { actual, maximum } => write!(
                formatter,
                "saga terminal payload is {actual} bytes (max {maximum} bytes)"
            ),
            Self::InvalidStepLedger => {
                formatter.write_str("saga step ledger exceeds bounds or contains duplicate steps")
            }
            Self::RetryCountExceeded { retry_count } => {
                write!(formatter, "saga retry count {retry_count} exceeds maximum")
            }
            Self::TerminalStateHasActiveLeaseOrPendingInvocation => formatter.write_str(
                "terminal saga instance must not retain an active lease or pending invocation",
            ),
            Self::CorruptEncoding => {
                formatter.write_str("stored saga instance encoding is corrupt")
            }
        }
    }
}

impl Error for SagaInstanceErrorV1 {}

/// Complete authoritative persisted state of one multi-step saga instance (`ADR-0334`).
#[derive(Clone, Eq, PartialEq)]
pub struct StoredSagaInstanceV1 {
    saga_id: SagaId,
    database_id: DatabaseId,
    environment: Environment,
    tenant_scope: TenantScope,
    contract_lineage: ContractLineage,
    contract_version: ContractVersion,
    contract_bundle_hash: ContractBundleHash,
    saga_plan_hash: PlanHash,
    saga_name: String,
    execution_role: String,
    delegated_actor_id: ActorId,
    partition_hash: u64,
    caller_idempotency_key: IdempotencyKey,
    retained_authority: SagaRetainedAuthorityV1,
    revision: SagaRevision,
    fencing_token: SagaFencingToken,
    lifecycle_status: SagaLifecycleStatusV1,
    current_step_index: u16,
    cancel_requested: bool,
    retry_count: u8,
    next_wakeup_at: Timestamp,
    lease: Option<SagaLeaseStateV1>,
    saga_input_bytes: Vec<u8>,
    pending_invocation: Option<PendingStepInvocationV1>,
    step_ledger: Vec<CommittedSagaStepRecordV1>,
    terminal_outcome: Option<SagaTerminalOutcomeRecordV1>,
    diagnostic_reason: Option<String>,
    created_at: Timestamp,
    updated_at: Timestamp,
}

impl StoredSagaInstanceV1 {
    /// Constructs and validates an authoritative saga instance record.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        saga_id: SagaId,
        database_id: DatabaseId,
        environment: Environment,
        tenant_scope: TenantScope,
        contract_lineage: ContractLineage,
        contract_version: ContractVersion,
        contract_bundle_hash: ContractBundleHash,
        saga_plan_hash: PlanHash,
        saga_name: impl Into<String>,
        execution_role: impl Into<String>,
        delegated_actor_id: ActorId,
        partition_hash: u64,
        caller_idempotency_key: IdempotencyKey,
        retained_authority: SagaRetainedAuthorityV1,
        revision: SagaRevision,
        fencing_token: SagaFencingToken,
        lifecycle_status: SagaLifecycleStatusV1,
        current_step_index: u16,
        cancel_requested: bool,
        retry_count: u8,
        next_wakeup_at: Timestamp,
        lease: Option<SagaLeaseStateV1>,
        saga_input_bytes: Vec<u8>,
        pending_invocation: Option<PendingStepInvocationV1>,
        step_ledger: Vec<CommittedSagaStepRecordV1>,
        terminal_outcome: Option<SagaTerminalOutcomeRecordV1>,
        diagnostic_reason: Option<String>,
        created_at: Timestamp,
        updated_at: Timestamp,
    ) -> Result<Self, SagaInstanceErrorV1> {
        let saga_name = saga_name.into();
        let execution_role = execution_role.into();
        if saga_name.is_empty()
            || saga_name.len() > MAX_SAGA_SYMBOL_BYTES_V1
            || execution_role.is_empty()
            || execution_role.len() > MAX_SAGA_SYMBOL_BYTES_V1
        {
            return Err(SagaInstanceErrorV1::InvalidSymbol);
        }
        if let Some(reason) = &diagnostic_reason
            && (reason.is_empty() || reason.len() > MAX_SAGA_SYMBOL_BYTES_V1)
        {
            return Err(SagaInstanceErrorV1::InvalidSymbol);
        }
        if usize::from(current_step_index) > MAX_SAGA_STEPS_V1 {
            return Err(SagaInstanceErrorV1::StepIndexOutOfBounds {
                step_index: current_step_index,
            });
        }
        if retry_count > MAX_SAGA_RETRY_COUNT_V1 {
            return Err(SagaInstanceErrorV1::RetryCountExceeded { retry_count });
        }
        if saga_input_bytes.len() > MAX_SAGA_INPUT_BYTES_V1 {
            return Err(SagaInstanceErrorV1::InputPayloadTooLarge {
                actual: saga_input_bytes.len(),
                maximum: MAX_SAGA_INPUT_BYTES_V1,
            });
        }
        if step_ledger.len() > MAX_SAGA_STEP_LEDGER_ENTRIES_V1 {
            return Err(SagaInstanceErrorV1::InvalidStepLedger);
        }
        let mut total_outcome_bytes = 0usize;
        let mut seen_steps = std::collections::BTreeSet::new();
        for entry in &step_ledger {
            if !seen_steps.insert((entry.step_index(), entry.direction())) {
                return Err(SagaInstanceErrorV1::InvalidStepLedger);
            }
            total_outcome_bytes = total_outcome_bytes
                .checked_add(entry.outcome_payload_bytes().len())
                .ok_or(SagaInstanceErrorV1::RetainedOutcomeStateTooLarge {
                    actual: usize::MAX,
                    maximum: MAX_SAGA_RETAINED_OUTCOME_BYTES_V1,
                })?;
        }
        if total_outcome_bytes > MAX_SAGA_RETAINED_OUTCOME_BYTES_V1 {
            return Err(SagaInstanceErrorV1::RetainedOutcomeStateTooLarge {
                actual: total_outcome_bytes,
                maximum: MAX_SAGA_RETAINED_OUTCOME_BYTES_V1,
            });
        }
        if lifecycle_status.is_terminal() && (lease.is_some() || pending_invocation.is_some()) {
            return Err(SagaInstanceErrorV1::TerminalStateHasActiveLeaseOrPendingInvocation);
        }
        Ok(Self {
            saga_id,
            database_id,
            environment,
            tenant_scope,
            contract_lineage,
            contract_version,
            contract_bundle_hash,
            saga_plan_hash,
            saga_name,
            execution_role,
            delegated_actor_id,
            partition_hash,
            caller_idempotency_key,
            retained_authority,
            revision,
            fencing_token,
            lifecycle_status,
            current_step_index,
            cancel_requested,
            retry_count,
            next_wakeup_at,
            lease,
            saga_input_bytes,
            pending_invocation,
            step_ledger,
            terminal_outcome,
            diagnostic_reason,
            created_at,
            updated_at,
        })
    }

    /// Durable UUIDv7 saga identifier.
    #[must_use]
    pub const fn saga_id(&self) -> SagaId {
        self.saga_id
    }

    /// Owning database identifier.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Owning environment.
    #[must_use]
    pub const fn environment(&self) -> &Environment {
        &self.environment
    }

    /// Effective tenant scope.
    #[must_use]
    pub const fn tenant_scope(&self) -> &TenantScope {
        &self.tenant_scope
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

    /// Pinned compiled saga plan hash.
    #[must_use]
    pub const fn saga_plan_hash(&self) -> PlanHash {
        self.saga_plan_hash
    }

    /// Symbolic saga name.
    #[must_use]
    pub fn saga_name(&self) -> &str {
        &self.saga_name
    }

    /// Symbolic execution role declared on the saga.
    #[must_use]
    pub fn execution_role(&self) -> &str {
        &self.execution_role
    }

    /// Delegated actor identity (`saga:<lineage>:<saga_name>:<role>`).
    #[must_use]
    pub const fn delegated_actor_id(&self) -> &ActorId {
        &self.delegated_actor_id
    }

    /// Deterministic partition hash used for fair background scheduling.
    #[must_use]
    pub const fn partition_hash(&self) -> u64 {
        self.partition_hash
    }

    /// Caller-supplied admission idempotency key.
    #[must_use]
    pub const fn caller_idempotency_key(&self) -> &IdempotencyKey {
        &self.caller_idempotency_key
    }

    /// Retained capability authority reference.
    #[must_use]
    pub const fn retained_authority(&self) -> &SagaRetainedAuthorityV1 {
        &self.retained_authority
    }

    /// Monotonic compare-and-swap revision.
    #[must_use]
    pub const fn revision(&self) -> SagaRevision {
        self.revision
    }

    /// Highest monotonic fencing token issued for this saga.
    #[must_use]
    pub const fn fencing_token(&self) -> SagaFencingToken {
        self.fencing_token
    }

    /// Current lifecycle status.
    #[must_use]
    pub const fn lifecycle_status(&self) -> SagaLifecycleStatusV1 {
        self.lifecycle_status
    }

    /// Current zero-based step cursor.
    #[must_use]
    pub const fn current_step_index(&self) -> u16 {
        self.current_step_index
    }

    /// Whether asynchronous cancellation has been requested.
    #[must_use]
    pub const fn cancel_requested(&self) -> bool {
        self.cancel_requested
    }

    /// Current consecutive retry count for the active step.
    #[must_use]
    pub const fn retry_count(&self) -> u8 {
        self.retry_count
    }

    /// Earliest timestamp at which background workers should wake up this saga.
    #[must_use]
    pub const fn next_wakeup_at(&self) -> Timestamp {
        self.next_wakeup_at
    }

    /// Active worker lease if claimed.
    #[must_use]
    pub const fn lease(&self) -> Option<&SagaLeaseStateV1> {
        self.lease.as_ref()
    }

    /// Canonical encoded saga input bytes.
    #[must_use]
    pub fn saga_input_bytes(&self) -> &[u8] {
        &self.saga_input_bytes
    }

    /// Prepared in-flight step invocation checkpoint, if a step is currently prepared.
    #[must_use]
    pub const fn pending_invocation(&self) -> Option<&PendingStepInvocationV1> {
        self.pending_invocation.as_ref()
    }

    /// Ordered ledger of committed forward and compensating steps.
    #[must_use]
    pub fn step_ledger(&self) -> &[CommittedSagaStepRecordV1] {
        &self.step_ledger
    }

    /// Terminal outcome record once the saga reaches a terminal state.
    #[must_use]
    pub const fn terminal_outcome(&self) -> Option<&SagaTerminalOutcomeRecordV1> {
        self.terminal_outcome.as_ref()
    }

    /// Safe diagnostic reason when blocked or failed.
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

    /// Finds the committed forward step record for `step_index`, if any.
    #[must_use]
    pub fn committed_forward_step(&self, step_index: u16) -> Option<&CommittedSagaStepRecordV1> {
        self.step_ledger.iter().find(|entry| {
            entry.step_index() == step_index && entry.direction() == SagaStepDirectionV1::Forward
        })
    }

    /// Finds the committed compensating step record for `step_index`, if any.
    #[must_use]
    pub fn committed_compensating_step(
        &self,
        step_index: u16,
    ) -> Option<&CommittedSagaStepRecordV1> {
        self.step_ledger.iter().find(|entry| {
            entry.step_index() == step_index && entry.direction() == SagaStepDirectionV1::Compensate
        })
    }

    /// Returns the highest-indexed committed `Effectful` forward step that has not yet been compensated.
    #[must_use]
    pub fn next_uncompensated_effectful_step_index(&self) -> Option<u16> {
        self.step_ledger
            .iter()
            .filter(|entry| {
                entry.direction() == SagaStepDirectionV1::Forward
                    && entry.effect_disposition() == SagaStepEffectDispositionV1::Effectful
            })
            .map(CommittedSagaStepRecordV1::step_index)
            .filter(|step_index| self.committed_compensating_step(*step_index).is_none())
            .max()
    }

    /// Encodes this saga instance into its canonical versioned binary representation.
    #[must_use]
    pub fn encode_canonical_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(512 + self.saga_input_bytes.len());
        out.extend_from_slice(SAGA_INSTANCE_MAGIC);
        out.extend_from_slice(self.saga_id.as_bytes());
        out.extend_from_slice(self.database_id.as_bytes());
        encode_bytes(&mut out, self.environment.as_bytes());
        encode_tenant_scope(&mut out, &self.tenant_scope);
        encode_bytes(&mut out, self.contract_lineage.as_bytes());
        out.extend_from_slice(&self.contract_version.to_be_bytes());
        out.extend_from_slice(self.contract_bundle_hash.as_bytes());
        out.extend_from_slice(self.saga_plan_hash.as_bytes());
        encode_bytes(&mut out, self.saga_name.as_bytes());
        encode_bytes(&mut out, self.execution_role.as_bytes());
        encode_bytes(&mut out, self.delegated_actor_id.as_str().as_bytes());
        out.extend_from_slice(&self.partition_hash.to_be_bytes());
        encode_bytes(
            &mut out,
            self.caller_idempotency_key.expose_secret().as_bytes(),
        );
        // Retained authority
        out.extend_from_slice(self.retained_authority.capability_id.as_bytes());
        out.extend_from_slice(
            &self
                .retained_authority
                .capability_revision
                .get()
                .to_be_bytes(),
        );
        encode_bytes(
            &mut out,
            self.retained_authority.principal_id.as_str().as_bytes(),
        );
        out.push(encode_actor_kind(self.retained_authority.actor_kind));
        encode_bytes(&mut out, self.retained_authority.audience.as_bytes());
        encode_tenant_scope(&mut out, &self.retained_authority.tenant_scope);
        encode_timestamp(&mut out, self.retained_authority.authenticated_at);
        // State fields
        out.extend_from_slice(&self.revision.to_be_bytes());
        out.extend_from_slice(&self.fencing_token.to_be_bytes());
        out.push(self.lifecycle_status.tag());
        out.extend_from_slice(&self.current_step_index.to_be_bytes());
        out.push(u8::from(self.cancel_requested));
        out.push(self.retry_count);
        encode_timestamp(&mut out, self.next_wakeup_at);
        match &self.lease {
            None => out.push(0),
            Some(lease) => {
                out.push(1);
                out.extend_from_slice(&lease.owner_id);
                out.extend_from_slice(&lease.fencing_token.to_be_bytes());
                encode_timestamp(&mut out, lease.expires_at);
            }
        }
        encode_bytes(&mut out, &self.saga_input_bytes);
        match &self.pending_invocation {
            None => out.push(0),
            Some(pending) => {
                out.push(1);
                out.extend_from_slice(&pending.step_index.to_be_bytes());
                out.push(pending.direction.tag());
                out.extend_from_slice(&pending.command_id.to_be_bytes());
                encode_bytes(&mut out, pending.contract_lineage.as_bytes());
                out.extend_from_slice(&pending.contract_version.to_be_bytes());
                out.extend_from_slice(pending.contract_bundle_hash.as_bytes());
                out.extend_from_slice(pending.command_plan_hash.as_bytes());
                encode_bytes(&mut out, &pending.input_bytes);
                encode_tenant_scope(&mut out, &pending.tenant_scope);
                encode_bytes(&mut out, pending.delegated_actor_id.as_str().as_bytes());
                encode_bytes(
                    &mut out,
                    pending.caller_idempotency_key.expose_secret().as_bytes(),
                );
            }
        }
        out.extend_from_slice(&(self.step_ledger.len() as u32).to_be_bytes());
        for entry in &self.step_ledger {
            out.extend_from_slice(&entry.step_index.to_be_bytes());
            out.push(entry.direction.tag());
            out.extend_from_slice(&entry.outcome_id.to_be_bytes());
            out.push(entry.effect_disposition.tag());
            encode_bytes(&mut out, &entry.outcome_payload_bytes);
        }
        match &self.terminal_outcome {
            None => out.push(0),
            Some(outcome) => {
                out.push(1);
                out.extend_from_slice(&outcome.outcome_id.to_be_bytes());
                encode_bytes(&mut out, outcome.outcome_name.as_bytes());
                encode_bytes(&mut out, &outcome.payload_bytes);
            }
        }
        match &self.diagnostic_reason {
            None => out.push(0),
            Some(reason) => {
                out.push(1);
                encode_bytes(&mut out, reason.as_bytes());
            }
        }
        encode_timestamp(&mut out, self.created_at);
        encode_timestamp(&mut out, self.updated_at);
        out
    }

    /// Decodes a canonical versioned binary representation of [`StoredSagaInstanceV1`].
    pub fn decode_canonical_bytes(bytes: &[u8]) -> Result<Self, SagaInstanceErrorV1> {
        if !is_encoded_saga_instance_v1(bytes) {
            return Err(SagaInstanceErrorV1::CorruptEncoding);
        }
        let mut cursor = &bytes[SAGA_INSTANCE_MAGIC.len()..];
        let saga_id = SagaId::from_bytes(take_array::<16>(&mut cursor)?)
            .map_err(|_| SagaInstanceErrorV1::CorruptEncoding)?;
        let database_id = DatabaseId::from_bytes(take_array::<16>(&mut cursor)?)
            .map_err(|_| SagaInstanceErrorV1::CorruptEncoding)?;
        let environment = Environment::new(take_string(&mut cursor)?)
            .map_err(|_| SagaInstanceErrorV1::CorruptEncoding)?;
        let tenant_scope = decode_tenant_scope(&mut cursor)?;
        let contract_lineage = ContractLineage::new(take_string(&mut cursor)?)
            .map_err(|_| SagaInstanceErrorV1::CorruptEncoding)?;
        let contract_version = ContractVersion::new(take_u64(&mut cursor)?)
            .ok_or(SagaInstanceErrorV1::CorruptEncoding)?;
        let contract_bundle_hash = ContractBundleHash::from_bytes(take_array::<32>(&mut cursor)?);
        let saga_plan_hash = PlanHash::from_bytes(take_array::<32>(&mut cursor)?);
        let saga_name = take_string(&mut cursor)?;
        let execution_role = take_string(&mut cursor)?;
        let delegated_actor_id = ActorId::new(take_string(&mut cursor)?)
            .map_err(|_| SagaInstanceErrorV1::CorruptEncoding)?;
        let partition_hash = take_u64(&mut cursor)?;
        let caller_idempotency_key = IdempotencyKey::new(take_string(&mut cursor)?)
            .map_err(|_| SagaInstanceErrorV1::CorruptEncoding)?;
        // Retained authority
        let capability_id = CapabilityId::from_bytes(take_array::<16>(&mut cursor)?)
            .map_err(|_| SagaInstanceErrorV1::CorruptEncoding)?;
        let capability_revision =
            NonZeroU64::new(take_u64(&mut cursor)?).ok_or(SagaInstanceErrorV1::CorruptEncoding)?;
        let auth_principal_id = ActorId::new(take_string(&mut cursor)?)
            .map_err(|_| SagaInstanceErrorV1::CorruptEncoding)?;
        let actor_kind = decode_actor_kind(take_u8(&mut cursor)?)?;
        let audience = Audience::new(take_string(&mut cursor)?)
            .map_err(|_| SagaInstanceErrorV1::CorruptEncoding)?;
        let auth_tenant_scope = decode_tenant_scope(&mut cursor)?;
        let authenticated_at = decode_timestamp(&mut cursor)?;
        let retained_authority = SagaRetainedAuthorityV1::new(
            capability_id,
            capability_revision,
            auth_principal_id,
            actor_kind,
            audience,
            auth_tenant_scope,
            authenticated_at,
        );
        // State fields
        let revision = SagaRevision::new(take_u64(&mut cursor)?)
            .ok_or(SagaInstanceErrorV1::CorruptEncoding)?;
        let fencing_token = SagaFencingToken::new(take_u64(&mut cursor)?)
            .ok_or(SagaInstanceErrorV1::CorruptEncoding)?;
        let lifecycle_status = SagaLifecycleStatusV1::from_tag(take_u8(&mut cursor)?)
            .ok_or(SagaInstanceErrorV1::CorruptEncoding)?;
        let current_step_index = take_u16(&mut cursor)?;
        let cancel_requested = match take_u8(&mut cursor)? {
            0 => false,
            1 => true,
            _ => return Err(SagaInstanceErrorV1::CorruptEncoding),
        };
        let retry_count = take_u8(&mut cursor)?;
        let next_wakeup_at = decode_timestamp(&mut cursor)?;
        let lease = match take_u8(&mut cursor)? {
            0 => None,
            1 => {
                let owner_id = take_array::<16>(&mut cursor)?;
                let lease_token = SagaFencingToken::new(take_u64(&mut cursor)?)
                    .ok_or(SagaInstanceErrorV1::CorruptEncoding)?;
                let expires_at = decode_timestamp(&mut cursor)?;
                Some(SagaLeaseStateV1::new(owner_id, lease_token, expires_at))
            }
            _ => return Err(SagaInstanceErrorV1::CorruptEncoding),
        };
        let saga_input_bytes = take_vec(&mut cursor, MAX_SAGA_INPUT_BYTES_V1)?;
        let pending_invocation = match take_u8(&mut cursor)? {
            0 => None,
            1 => {
                let step_index = take_u16(&mut cursor)?;
                let direction = SagaStepDirectionV1::from_tag(take_u8(&mut cursor)?)
                    .ok_or(SagaInstanceErrorV1::CorruptEncoding)?;
                let command_id = CommandId::new(take_u32(&mut cursor)?)
                    .ok_or(SagaInstanceErrorV1::CorruptEncoding)?;
                let cmd_lineage = ContractLineage::new(take_string(&mut cursor)?)
                    .map_err(|_| SagaInstanceErrorV1::CorruptEncoding)?;
                let cmd_version = ContractVersion::new(take_u64(&mut cursor)?)
                    .ok_or(SagaInstanceErrorV1::CorruptEncoding)?;
                let cmd_bundle_hash =
                    ContractBundleHash::from_bytes(take_array::<32>(&mut cursor)?);
                let command_plan_hash = PlanHash::from_bytes(take_array::<32>(&mut cursor)?);
                let input_bytes = take_vec(&mut cursor, MAX_SAGA_INPUT_BYTES_V1)?;
                let step_tenant_scope = decode_tenant_scope(&mut cursor)?;
                let step_actor_id = ActorId::new(take_string(&mut cursor)?)
                    .map_err(|_| SagaInstanceErrorV1::CorruptEncoding)?;
                let step_idempotency_key = IdempotencyKey::new(take_string(&mut cursor)?)
                    .map_err(|_| SagaInstanceErrorV1::CorruptEncoding)?;
                Some(PendingStepInvocationV1::new(
                    step_index,
                    direction,
                    command_id,
                    cmd_lineage,
                    cmd_version,
                    cmd_bundle_hash,
                    command_plan_hash,
                    input_bytes,
                    step_tenant_scope,
                    step_actor_id,
                    step_idempotency_key,
                )?)
            }
            _ => return Err(SagaInstanceErrorV1::CorruptEncoding),
        };
        let ledger_len = usize::try_from(take_u32(&mut cursor)?)
            .map_err(|_| SagaInstanceErrorV1::CorruptEncoding)?;
        if ledger_len > MAX_SAGA_STEP_LEDGER_ENTRIES_V1 {
            return Err(SagaInstanceErrorV1::InvalidStepLedger);
        }
        let mut step_ledger = Vec::with_capacity(ledger_len);
        for _ in 0..ledger_len {
            let step_index = take_u16(&mut cursor)?;
            let direction = SagaStepDirectionV1::from_tag(take_u8(&mut cursor)?)
                .ok_or(SagaInstanceErrorV1::CorruptEncoding)?;
            let outcome_id = OutcomeId::new(take_u32(&mut cursor)?)
                .ok_or(SagaInstanceErrorV1::CorruptEncoding)?;
            let effect_disposition = SagaStepEffectDispositionV1::from_tag(take_u8(&mut cursor)?)
                .ok_or(SagaInstanceErrorV1::CorruptEncoding)?;
            let outcome_payload_bytes = take_vec(&mut cursor, MAX_SAGA_RETAINED_OUTCOME_BYTES_V1)?;
            step_ledger.push(CommittedSagaStepRecordV1::new(
                step_index,
                direction,
                outcome_id,
                effect_disposition,
                outcome_payload_bytes,
            )?);
        }
        let terminal_outcome = match take_u8(&mut cursor)? {
            0 => None,
            1 => {
                let outcome_id = OutcomeId::new(take_u32(&mut cursor)?)
                    .ok_or(SagaInstanceErrorV1::CorruptEncoding)?;
                let outcome_name = take_string(&mut cursor)?;
                let payload_bytes = take_vec(&mut cursor, MAX_SAGA_TERMINAL_PAYLOAD_BYTES_V1)?;
                Some(SagaTerminalOutcomeRecordV1::new(
                    outcome_id,
                    outcome_name,
                    payload_bytes,
                )?)
            }
            _ => return Err(SagaInstanceErrorV1::CorruptEncoding),
        };
        let diagnostic_reason = match take_u8(&mut cursor)? {
            0 => None,
            1 => Some(take_string(&mut cursor)?),
            _ => return Err(SagaInstanceErrorV1::CorruptEncoding),
        };
        let created_at = decode_timestamp(&mut cursor)?;
        let updated_at = decode_timestamp(&mut cursor)?;
        if !cursor.is_empty() {
            return Err(SagaInstanceErrorV1::CorruptEncoding);
        }
        Self::new(
            saga_id,
            database_id,
            environment,
            tenant_scope,
            contract_lineage,
            contract_version,
            contract_bundle_hash,
            saga_plan_hash,
            saga_name,
            execution_role,
            delegated_actor_id,
            partition_hash,
            caller_idempotency_key,
            retained_authority,
            revision,
            fencing_token,
            lifecycle_status,
            current_step_index,
            cancel_requested,
            retry_count,
            next_wakeup_at,
            lease,
            saga_input_bytes,
            pending_invocation,
            step_ledger,
            terminal_outcome,
            diagnostic_reason,
            created_at,
            updated_at,
        )
    }
}

impl fmt::Debug for StoredSagaInstanceV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredSagaInstanceV1")
            .field("saga_id", &self.saga_id)
            .field("contract_lineage", &self.contract_lineage)
            .field("contract_version", &self.contract_version)
            .field("saga_name", &self.saga_name)
            .field("revision", &self.revision)
            .field("fencing_token", &self.fencing_token)
            .field("lifecycle_status", &self.lifecycle_status)
            .field("current_step_index", &self.current_step_index)
            .field("step_ledger_len", &self.step_ledger.len())
            .finish()
    }
}

/// Checks whether a byte slice begins with [`SAGA_INSTANCE_MAGIC`].
#[must_use]
pub fn is_encoded_saga_instance_v1(bytes: &[u8]) -> bool {
    bytes.starts_with(SAGA_INSTANCE_MAGIC)
}

/// Formats the deterministic delegated saga actor identity (`saga:<lineage>:<saga_name>:<role>`).
pub fn format_saga_delegated_actor_id(
    lineage: &ContractLineage,
    saga_name: &str,
    execution_role: &str,
) -> Result<ActorId, TextIdError> {
    ActorId::new(format!(
        "saga:{}:{saga_name}:{execution_role}",
        lineage.as_str()
    ))
}

/// Derives the deterministic 16-byte RFC 9562 v8 UUID for a saga step's idempotency input.
#[must_use]
pub fn derive_saga_step_idempotency_uuid(
    saga_id: SagaId,
    step_index: u16,
    direction: SagaStepDirectionV1,
) -> [u8; 16] {
    let mut hasher = Sha256::new();
    hasher.update(b"riffdb.saga.step_idempotency/v1\0");
    hasher.update(saga_id.as_bytes());
    hasher.update(step_index.to_be_bytes());
    hasher.update([direction.tag()]);
    let digest = hasher.finalize();
    let mut uuid = [0_u8; 16];
    uuid.copy_from_slice(&digest[..16]);
    uuid[6] = (uuid[6] & 0x0f) | 0x80;
    uuid[8] = (uuid[8] & 0x3f) | 0x80;
    uuid
}

/// Derives the deterministic 36-byte canonical step idempotency key from `(saga_id, step_index, direction)`.
pub fn derive_saga_step_idempotency_key(
    saga_id: SagaId,
    step_index: u16,
    direction: SagaStepDirectionV1,
) -> Result<IdempotencyKey, TextIdError> {
    let bytes = derive_saga_step_idempotency_uuid(saga_id, step_index, direction);
    IdempotencyKey::new(format!(
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

/// Outcome of atomic saga admission against the caller idempotency index.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SagaAdmissionStorageOutcomeV1 {
    /// A new saga instance was durably admitted at revision 1.
    Admitted(Box<StoredSagaInstanceV1>),
    /// An existing saga instance matched the exact admission idempotency key, plan hash, and input bytes.
    Replayed(Box<StoredSagaInstanceV1>),
    /// An existing saga instance used the same admission idempotency key with different plan or input bytes.
    ConflictingIdempotencyKey(Box<StoredSagaInstanceV1>),
}

/// Expected revision and optional fencing token for fenced compare-and-swap transitions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SagaCompareExpectedV1 {
    revision: SagaRevision,
    fencing_token: Option<SagaFencingToken>,
}

impl SagaCompareExpectedV1 {
    /// Requires both `revision` and `fencing_token` to match the current durable record.
    #[must_use]
    pub const fn fenced(revision: SagaRevision, fencing_token: SagaFencingToken) -> Self {
        Self {
            revision,
            fencing_token: Some(fencing_token),
        }
    }

    /// Requires `revision` to match (used by control operations that bump `fencing_token`).
    #[must_use]
    pub const fn revision_only(revision: SagaRevision) -> Self {
        Self {
            revision,
            fencing_token: None,
        }
    }

    /// Expected revision.
    #[must_use]
    pub const fn revision(self) -> SagaRevision {
        self.revision
    }

    /// Expected fencing token, if fenced.
    #[must_use]
    pub const fn fencing_token(self) -> Option<SagaFencingToken> {
        self.fencing_token
    }
}

/// Outcome of a fenced compare-and-swap transition on a durable saga instance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SagaCasOutcomeV1 {
    /// The transition was durably committed.
    Applied(Box<StoredSagaInstanceV1>),
    /// The current record did not exist or did not match the expected revision/fencing token.
    Conflict(Option<Box<StoredSagaInstanceV1>>),
}

/// Computes a deterministic 64-bit partition hash for fair round-robin background scheduling.
#[must_use]
pub fn compute_saga_partition_hash(
    database_id: DatabaseId,
    tenant_scope: &TenantScope,
    lineage: &ContractLineage,
    saga_name: &str,
    saga_id: SagaId,
) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(b"riffdb.saga.partition_hash/v1\0");
    hasher.update(database_id.as_bytes());
    hasher.update(tenant_scope.to_canonical_bytes());
    hasher.update((lineage.as_bytes().len() as u32).to_be_bytes());
    hasher.update(lineage.as_bytes());
    hasher.update((saga_name.len() as u32).to_be_bytes());
    hasher.update(saga_name.as_bytes());
    hasher.update(saga_id.as_bytes());
    let digest = hasher.finalize();
    u64::from_be_bytes([
        digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6], digest[7],
    ])
}

fn encode_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}

fn encode_tenant_scope(out: &mut Vec<u8>, scope: &TenantScope) {
    match scope {
        TenantScope::Global => out.push(0),
        TenantScope::Tenant(tenant) => {
            out.push(1);
            encode_bytes(out, tenant.as_bytes());
        }
    }
}

fn decode_tenant_scope(cursor: &mut &[u8]) -> Result<TenantScope, SagaInstanceErrorV1> {
    match take_u8(cursor)? {
        0 => Ok(TenantScope::Global),
        1 => {
            let tenant = TenantId::new(take_string(cursor)?)
                .map_err(|_| SagaInstanceErrorV1::CorruptEncoding)?;
            Ok(TenantScope::Tenant(tenant))
        }
        _ => Err(SagaInstanceErrorV1::CorruptEncoding),
    }
}

const fn encode_actor_kind(kind: ActorKind) -> u8 {
    kind.tag()
}

const fn decode_actor_kind(tag: u8) -> Result<ActorKind, SagaInstanceErrorV1> {
    match ActorKind::from_tag(tag) {
        Some(kind) => Ok(kind),
        None => Err(SagaInstanceErrorV1::CorruptEncoding),
    }
}

fn encode_timestamp(out: &mut Vec<u8>, ts: Timestamp) {
    out.extend_from_slice(&ts.seconds().to_be_bytes());
    out.extend_from_slice(&ts.nanoseconds().to_be_bytes());
}

fn decode_timestamp(cursor: &mut &[u8]) -> Result<Timestamp, SagaInstanceErrorV1> {
    let seconds = i64::from_be_bytes(take_array::<8>(cursor)?);
    let nanos = take_u32(cursor)?;
    Timestamp::new(seconds, nanos).map_err(|_| SagaInstanceErrorV1::CorruptEncoding)
}

fn take_u8(cursor: &mut &[u8]) -> Result<u8, SagaInstanceErrorV1> {
    let Some((first, rest)) = cursor.split_first() else {
        return Err(SagaInstanceErrorV1::CorruptEncoding);
    };
    *cursor = rest;
    Ok(*first)
}

fn take_u16(cursor: &mut &[u8]) -> Result<u16, SagaInstanceErrorV1> {
    Ok(u16::from_be_bytes(take_array::<2>(cursor)?))
}

fn take_u32(cursor: &mut &[u8]) -> Result<u32, SagaInstanceErrorV1> {
    Ok(u32::from_be_bytes(take_array::<4>(cursor)?))
}

fn take_u64(cursor: &mut &[u8]) -> Result<u64, SagaInstanceErrorV1> {
    Ok(u64::from_be_bytes(take_array::<8>(cursor)?))
}

fn take_array<const N: usize>(cursor: &mut &[u8]) -> Result<[u8; N], SagaInstanceErrorV1> {
    if cursor.len() < N {
        return Err(SagaInstanceErrorV1::CorruptEncoding);
    }
    let (head, tail) = cursor.split_at(N);
    *cursor = tail;
    head.try_into()
        .map_err(|_| SagaInstanceErrorV1::CorruptEncoding)
}

fn take_vec(cursor: &mut &[u8], max_len: usize) -> Result<Vec<u8>, SagaInstanceErrorV1> {
    let len =
        usize::try_from(take_u32(cursor)?).map_err(|_| SagaInstanceErrorV1::CorruptEncoding)?;
    if len > max_len || cursor.len() < len {
        return Err(SagaInstanceErrorV1::CorruptEncoding);
    }
    let (head, tail) = cursor.split_at(len);
    *cursor = tail;
    Ok(head.to_vec())
}

fn take_string(cursor: &mut &[u8]) -> Result<String, SagaInstanceErrorV1> {
    let bytes = take_vec(cursor, 4096)?;
    String::from_utf8(bytes).map_err(|_| SagaInstanceErrorV1::CorruptEncoding)
}
