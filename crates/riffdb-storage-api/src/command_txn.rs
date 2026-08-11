//! Consuming synchronous type-state protocol for authoritative command batches.

use std::num::NonZeroU16;

use riffdb_types::{
    CapabilityId, CommitSequence, EntityTypeId, IndexId, MAX_KEY_BYTES, PartitionKey,
};

use crate::{
    AffectedEpochCurrentState, AffectedIndexEpochTargets, ApplicationSequenceAllocator,
    AtomicCommandRecordSet, CommandWriteSetChargeV1, CommandWriteSetPlanV1, CommitIntent,
    DurabilityMode, MAX_STAGED_COMMANDS, MAX_STAGED_WRITE_BYTES, ReadSnapshot, SnapshotRequest,
    StorageError, StorageErrorKind, StorageValueError, StoredExecutionFailedV1, StoredOutcomeV1,
    TransactionCurrentState,
};

/// One compiler-derived exact index-existence observation needed by row policy.
#[derive(Clone, Eq, PartialEq)]
pub struct TransactionCurrentPolicyLookupV1 {
    target_entity: EntityTypeId,
    index_id: IndexId,
    partition: PartitionKey,
    index_prefix: Vec<u8>,
}

impl TransactionCurrentPolicyLookupV1 {
    /// Constructs one bounded lookup derived by the trusted policy layer.
    pub fn new(
        target_entity: EntityTypeId,
        index_id: IndexId,
        partition: PartitionKey,
        index_prefix: Vec<u8>,
    ) -> Result<Self, StorageValueError> {
        if index_prefix.is_empty() || index_prefix.len() > MAX_KEY_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self {
            target_entity,
            index_id,
            partition,
            index_prefix,
        })
    }

    /// Compiler-selected target entity.
    #[must_use]
    pub const fn target_entity(&self) -> EntityTypeId {
        self.target_entity
    }

    /// Compiler-selected index.
    #[must_use]
    pub const fn index_id(&self) -> IndexId {
        self.index_id
    }

    /// Exact partition derived from trusted policy operands.
    #[must_use]
    pub const fn partition(&self) -> &PartitionKey {
        &self.partition
    }

    /// Exact complete index prefix.
    #[must_use]
    pub fn index_prefix(&self) -> &[u8] {
        &self.index_prefix
    }
}

impl std::fmt::Debug for TransactionCurrentPolicyLookupV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransactionCurrentPolicyLookupV1")
            .field("target_entity", &self.target_entity)
            .field("index_id", &self.index_id)
            .field("key", &"[REDACTED]")
            .finish()
    }
}

/// Closed bounded request for the command write transaction's policy safe point.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionCurrentPolicyRequestV1 {
    capability_id: CapabilityId,
    lookups: Vec<TransactionCurrentPolicyLookupV1>,
}

impl TransactionCurrentPolicyRequestV1 {
    /// Constructs one request within the existing validation-target ceiling.
    pub fn new(
        capability_id: CapabilityId,
        lookups: Vec<TransactionCurrentPolicyLookupV1>,
    ) -> Result<Self, StorageValueError> {
        if lookups.len() > crate::MAX_VALIDATION_TARGETS {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self {
            capability_id,
            lookups,
        })
    }

    /// Exact current capability to reload.
    #[must_use]
    pub const fn capability_id(&self) -> CapabilityId {
        self.capability_id
    }

    /// Ordered compiler-derived relationship lookups.
    #[must_use]
    pub fn lookups(&self) -> &[TransactionCurrentPolicyLookupV1] {
        &self.lookups
    }
}

/// Complete transaction-current observations for one row-policy request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionCurrentPolicyStateV1 {
    capability: Option<crate::StoredCapabilityRecordV1>,
    relationship_exists: Vec<bool>,
}

impl TransactionCurrentPolicyStateV1 {
    /// Joins one capability observation and exact positional lookup results.
    pub fn new(
        request: &TransactionCurrentPolicyRequestV1,
        capability: Option<crate::StoredCapabilityRecordV1>,
        relationship_exists: Vec<bool>,
    ) -> Result<Self, StorageValueError> {
        if relationship_exists.len() != request.lookups.len()
            || capability
                .as_ref()
                .is_some_and(|record| record.capability_id() != request.capability_id)
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self {
            capability,
            relationship_exists,
        })
    }

    /// Transaction-current capability, when it still exists.
    #[must_use]
    pub const fn capability(&self) -> Option<&crate::StoredCapabilityRecordV1> {
        self.capability.as_ref()
    }

    /// Positional exact relationship-existence observations.
    #[must_use]
    pub fn relationship_exists(&self) -> &[bool] {
        &self.relationship_exists
    }
}

/// Checked runtime accounting for a nonempty uncommitted command batch.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct StagedBatchMetrics {
    command_count: NonZeroU16,
    semantic_bytes: usize,
    reserved_encoded_bytes: usize,
}

impl StagedBatchMetrics {
    /// Validates the accepted command-count and aggregate write-set ceilings.
    pub fn new(
        command_count: NonZeroU16,
        semantic_bytes: usize,
        reserved_encoded_bytes: usize,
    ) -> Result<Self, StorageValueError> {
        if usize::from(command_count.get()) > MAX_STAGED_COMMANDS
            || semantic_bytes > MAX_STAGED_WRITE_BYTES
            || reserved_encoded_bytes > MAX_STAGED_WRITE_BYTES
        {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self {
            command_count,
            semantic_bytes,
            reserved_encoded_bytes,
        })
    }

    /// Returns the nonzero number of completely staged commands.
    #[must_use]
    pub const fn command_count(self) -> NonZeroU16 {
        self.command_count
    }

    /// Returns checked aggregate semantic write-set bytes.
    #[must_use]
    pub const fn semantic_bytes(self) -> usize {
        self.semantic_bytes
    }

    /// Returns the aggregate conservative encoded reservation.
    #[must_use]
    pub const fn reserved_encoded_bytes(self) -> usize {
        self.reserved_encoded_bytes
    }

    /// Returns whether another command could fit by count and supplied bytes.
    #[must_use]
    pub fn can_add(self, charge: CommandWriteSetChargeV1) -> bool {
        usize::from(self.command_count.get()) < MAX_STAGED_COMMANDS
            && self
                .semantic_bytes
                .checked_add(charge.semantic_bytes())
                .is_some_and(|total| total <= MAX_STAGED_WRITE_BYTES)
            && self
                .reserved_encoded_bytes
                .checked_add(charge.encoded_upper_bound().total())
                .is_some_and(|total| total <= MAX_STAGED_WRITE_BYTES)
    }
}

/// Opens the narrow authoritative command transaction protocol.
pub trait ApplicationCommandTransactionPort {
    /// Backend-private empty transaction state. It has no commit operation.
    type EmptyBatch: EmptyCommandBatch;

    /// Opens one synchronous empty batch without staging a write.
    fn begin_empty_batch(&self) -> Result<Self::EmptyBatch, StorageError>;
}

/// Opens the closed unpublished-write protocol used by one bounded durability epoch.
///
/// This is deliberately separate from [`ApplicationCommandTransactionPort`]: an
/// ordinary batch can produce a committed result directly, while an epoch batch
/// can only return its owning epoch token. The token withholds every result until
/// its durable tail fence succeeds.
pub trait DeferredCommandEpochPort {
    /// Backend-owned epoch state. It owns the writer exclusion boundary.
    type Epoch: DeferredCommandEpoch;

    /// Begins one standard-profile durability epoch at the current durable root.
    fn begin_deferred_command_epoch(&self) -> Result<Self::Epoch, StorageError>;
}

/// Consuming state for one bounded unpublished durability epoch.
pub trait DeferredCommandEpoch: Sized {
    /// Empty command batch whose write transaction belongs to this epoch.
    type EmptyBatch: EmptyCommandBatch;

    /// Backend-owned durability fence submitted after the complete private
    /// epoch has been sealed. It owns every unpublished result until wait
    /// proves the covering durability boundary and publishes the successor.
    type Fence: DeferredCommandFence;

    /// Opens the next writer-private subgroup from the newest epoch root.
    fn begin_empty_batch(self) -> Result<Self::EmptyBatch, StorageError>;

    /// Seals the epoch and submits its immutable durability work without
    /// releasing any result. Backends may complete the physical fence on a
    /// dedicated lane while the sole ordered apply coordinator prepares a
    /// later bounded epoch.
    fn seal(self) -> Result<Self::Fence, StorageError>;

    /// Performs the complete fence synchronously.
    fn fence(self) -> Result<Vec<AuditedCommittedBatchV1>, StorageError> {
        self.seal()?.wait()
    }
}

/// Submitted durability work whose results remain unpublished and unobservable.
pub trait DeferredCommandFence: Sized {
    /// Whether this fence used the bounded synchronous tail fallback and the
    /// coordinator must publish every earlier fence before opening another
    /// private epoch. This is storage scheduling evidence only; it never
    /// changes command visibility or durability semantics.
    fn requires_pipeline_drain(&self) -> bool;

    /// Polls the fence without blocking. `None` means the durability lane has
    /// not yet resolved the submitted epoch.
    fn try_wait(&mut self) -> Result<Option<Vec<AuditedCommittedBatchV1>>, StorageError>;

    /// Waits for durability, publishes the exact successor frontier, and only
    /// then converts every retained subgroup into committed results.
    fn wait(self) -> Result<Vec<AuditedCommittedBatchV1>, StorageError>;
}

/// Nonempty batch state whose graph may be applied without publication.
pub trait DeferredNonEmptyCommandBatch: Sized {
    /// Epoch token recovered only after the unpublished engine commit succeeds.
    type Epoch: DeferredCommandEpoch;

    /// Applies a complete audited command subgroup through backend-private
    /// non-durable mechanics. No result escapes this call; the returned epoch
    /// owns it until [`DeferredCommandEpoch::fence`] succeeds.
    fn apply_unpublished_with_service_audit_transitions(
        self,
        durability: DurabilityMode,
        transitions: Vec<CommandServiceAuditTransitionV1>,
    ) -> Result<Self::Epoch, StorageError>;
}

/// Empty batch state; the trait deliberately exposes no `commit` method.
pub trait TransactionLocalCommandBatch {
    /// Reads one compiler-declared bounded snapshot from this batch's private
    /// transaction state, including every command already staged in FIFO order.
    ///
    /// This is deliberately not a general transaction reader. Implementations
    /// accept only the existing closed [`SnapshotRequest`] shape and return the
    /// same bounded [`ReadSnapshot`] used by ordinary command evaluation.
    fn read_transaction_local_snapshot(
        &self,
        request: SnapshotRequest,
    ) -> Result<ReadSnapshot, StorageError>;
}

/// Empty batch state; the trait deliberately exposes no `commit` method.
pub trait EmptyCommandBatch: Sized {
    /// Candidate state parameterized by this exact prior empty state.
    type Candidate: CommandCandidateAdmission<Prior = Self>;

    /// Starts one candidate; an empty batch always passes the count-only gate.
    fn begin_candidate(self, intent: Box<CommitIntent>) -> Result<Self::Candidate, StorageError>;

    /// Closes the empty transaction without a commit attempt.
    fn rollback(self);
}

/// Nonempty batch state; only this state may attempt a durable commit.
pub trait NonEmptyCommandBatch: Sized {
    /// Candidate state parameterized by this exact prior nonempty state.
    type Candidate: CommandCandidateAdmission<Prior = Self>;

    /// Returns checked nonempty staging metrics.
    fn metrics(&self) -> StagedBatchMetrics;

    /// Applies only the command-count gate and starts another candidate.
    fn begin_candidate(
        self,
        intent: Box<CommitIntent>,
    ) -> Result<CandidateStartResult<Self, Self::Candidate>, StorageError>;

    /// Durably commits every staged command atomically.
    ///
    /// Before entering the engine commit, a conforming backend must require every
    /// staged graph to satisfy
    /// [`AtomicCommandRecordSet::matches_durability_mode`] for `durability`.
    /// A mismatch aborts the complete batch without committing any record.
    fn commit(self, durability: DurabilityMode) -> Result<CommittedBatchV1, StorageError>;

    /// Durably commits the command graph and its exact linked terminal audit in
    /// one authoritative transition.
    ///
    /// This closed operation is available only to the audited application
    /// command path. The default keeps reference test doubles source
    /// compatible while failing closed; production backends must override it.
    fn commit_with_service_audit(
        self,
        durability: DurabilityMode,
        terminal: crate::ServiceAuditAppendIntentV1,
    ) -> Result<AuditedCommittedBatchV1, StorageError> {
        self.commit_with_service_audit_transitions(
            durability,
            vec![CommandServiceAuditTransitionV1::terminal_only(terminal)?],
        )
    }

    /// Durably commits every command graph and its corresponding terminal
    /// audit in one authoritative transition.
    ///
    /// Inputs and results are in command-sequence order. The operation is
    /// deliberately all-or-nothing at the storage boundary while callers retain
    /// independent application identities and acknowledgements.
    fn commit_with_service_audits(
        self,
        durability: DurabilityMode,
        terminals: Vec<crate::ServiceAuditAppendIntentV1>,
    ) -> Result<AuditedCommittedBatchV1, StorageError> {
        let transitions = terminals
            .into_iter()
            .map(CommandServiceAuditTransitionV1::terminal_only)
            .collect::<Result<Vec<_>, _>>()?;
        self.commit_with_service_audit_transitions(durability, transitions)
    }

    /// Durably commits each command graph with either its already-started
    /// terminal row or a fused `Started` and terminal lifecycle.
    fn commit_with_service_audit_transitions(
        self,
        _durability: DurabilityMode,
        _transitions: Vec<CommandServiceAuditTransitionV1>,
    ) -> Result<AuditedCommittedBatchV1, StorageError> {
        Err(StorageError::new(
            crate::StorageErrorKind::InvariantViolation,
            None,
        ))
    }

    /// Rolls back every staged command without a commit attempt.
    fn rollback(self);
}

/// Audit rows that must accompany one command in the authoritative commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommandServiceAuditTransitionV1 {
    /// Recovery of a legacy `Pending` command whose start is already durable.
    TerminalOnly(crate::ServiceAuditAppendIntentV1),
    /// A fresh synchronous command whose complete lifecycle is fused.
    StartedAndTerminal {
        /// The previously non-durable invocation start.
        started: crate::ServiceAuditAppendIntentV1,
        /// The terminal row linked to the command commit.
        terminal: crate::ServiceAuditAppendIntentV1,
    },
}

impl CommandServiceAuditTransitionV1 {
    /// Validates a terminal belonging to an already durable start.
    pub fn terminal_only(
        terminal: crate::ServiceAuditAppendIntentV1,
    ) -> Result<Self, StorageError> {
        if terminal.phase() != riffdb_types::ServiceAuditPhaseV1::Succeeded
            || !matches!(
                terminal.link(),
                riffdb_types::ServiceAuditLinkV1::Command { .. }
            )
        {
            return Err(StorageError::new(
                crate::StorageErrorKind::InvariantViolation,
                None,
            ));
        }
        Ok(Self::TerminalOnly(terminal))
    }

    /// Validates a complete same-invocation command audit lifecycle.
    pub fn started_and_terminal(
        started: crate::ServiceAuditAppendIntentV1,
        terminal: crate::ServiceAuditAppendIntentV1,
    ) -> Result<Self, StorageError> {
        let same_invocation = started.request_id() == terminal.request_id()
            && started.operation() == terminal.operation()
            && started.principal() == terminal.principal()
            && started.ingress() == terminal.ingress()
            && started.targets() == terminal.targets()
            && started.approval_id() == terminal.approval_id();
        if !same_invocation
            || started.phase() != riffdb_types::ServiceAuditPhaseV1::Started
            || started.link() != riffdb_types::ServiceAuditLinkV1::None
            || terminal.phase() != riffdb_types::ServiceAuditPhaseV1::Succeeded
            || !matches!(
                terminal.link(),
                riffdb_types::ServiceAuditLinkV1::Command { .. }
            )
        {
            return Err(StorageError::new(
                crate::StorageErrorKind::InvariantViolation,
                None,
            ));
        }
        Ok(Self::StartedAndTerminal { started, terminal })
    }

    /// Validates a failed terminal belonging to an already durable start.
    pub fn failure_terminal_only(
        terminal: crate::ServiceAuditAppendIntentV1,
    ) -> Result<Self, StorageError> {
        if terminal.phase() != riffdb_types::ServiceAuditPhaseV1::Failed
            || terminal.link() != riffdb_types::ServiceAuditLinkV1::None
        {
            return Err(StorageError::new(
                crate::StorageErrorKind::InvariantViolation,
                None,
            ));
        }
        Ok(Self::TerminalOnly(terminal))
    }

    /// Validates a complete same-invocation failed command lifecycle.
    pub fn started_and_failure(
        started: crate::ServiceAuditAppendIntentV1,
        terminal: crate::ServiceAuditAppendIntentV1,
    ) -> Result<Self, StorageError> {
        let same_invocation = started.request_id() == terminal.request_id()
            && started.operation() == terminal.operation()
            && started.principal() == terminal.principal()
            && started.ingress() == terminal.ingress()
            && started.targets() == terminal.targets()
            && started.approval_id() == terminal.approval_id();
        if !same_invocation
            || started.phase() != riffdb_types::ServiceAuditPhaseV1::Started
            || started.link() != riffdb_types::ServiceAuditLinkV1::None
            || terminal.phase() != riffdb_types::ServiceAuditPhaseV1::Failed
            || terminal.link() != riffdb_types::ServiceAuditLinkV1::None
        {
            return Err(StorageError::new(
                crate::StorageErrorKind::InvariantViolation,
                None,
            ));
        }
        Ok(Self::StartedAndTerminal { started, terminal })
    }

    /// Returns the terminal row for link validation.
    #[must_use]
    pub const fn terminal(&self) -> &crate::ServiceAuditAppendIntentV1 {
        match self {
            Self::TerminalOnly(terminal) | Self::StartedAndTerminal { terminal, .. } => terminal,
        }
    }

    /// Consumes the transition into append order.
    #[must_use]
    pub fn into_intents(self) -> Vec<crate::ServiceAuditAppendIntentV1> {
        match self {
            Self::TerminalOnly(terminal) => vec![terminal],
            Self::StartedAndTerminal { started, terminal } => vec![started, terminal],
        }
    }
}

/// Result proving one deterministic execution failure and its audit terminal
/// became durable in the same authoritative transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuditedExecutionFailureV1 {
    failure: StoredExecutionFailedV1,
    terminal: crate::StoredServiceAuditRecordV1,
}

impl AuditedExecutionFailureV1 {
    /// Joins an execution failure to its same-invocation failed audit row.
    pub fn new(
        failure: StoredExecutionFailedV1,
        terminal: crate::StoredServiceAuditRecordV1,
    ) -> Result<Self, StorageValueError> {
        if terminal.phase() != riffdb_types::ServiceAuditPhaseV1::Failed
            || terminal.link() != riffdb_types::ServiceAuditLinkV1::None
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self { failure, terminal })
    }

    /// Consumes the compound result.
    #[must_use]
    pub fn into_parts(self) -> (StoredExecutionFailedV1, crate::StoredServiceAuditRecordV1) {
        (self.failure, self.terminal)
    }
}

/// Result proving one command batch and linked terminal audit committed
/// atomically.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuditedCommittedBatchV1 {
    batch: CommittedBatchV1,
    terminals: Vec<crate::StoredServiceAuditRecordV1>,
}

/// Complete command and audit material applied to an unpublished engine root.
///
/// This type intentionally has no conversion to [`AuditedCommittedBatchV1`].
/// A backend retains it inside its epoch token and constructs committed results
/// only after the tail durability fence is known successful.
#[derive(Debug)]
pub struct UnpublishedAuditedBatchV1 {
    outcomes: Vec<StoredOutcomeV1>,
    terminals: Vec<crate::StoredServiceAuditRecordV1>,
}

impl UnpublishedAuditedBatchV1 {
    /// Validates the same exact command/audit links as a committed batch,
    /// without asserting that an engine durability fence has occurred.
    pub fn new(
        outcomes: Vec<StoredOutcomeV1>,
        terminals: Vec<crate::StoredServiceAuditRecordV1>,
    ) -> Result<Self, crate::StorageValueError> {
        if outcomes.len() != terminals.len()
            || outcomes.is_empty()
            || outcomes.len() > MAX_STAGED_COMMANDS
            || terminals
                .iter()
                .any(|terminal| terminal.phase() != riffdb_types::ServiceAuditPhaseV1::Succeeded)
        {
            return Err(crate::StorageValueError::IdentityMismatch);
        }
        let durability = outcomes[0].durability_mode();
        if outcomes
            .iter()
            .any(|outcome| outcome.durability_mode() != durability)
            || outcomes.windows(2).any(|pair| {
                pair[0].commit_sequence().checked_next() != Some(pair[1].commit_sequence())
            })
        {
            return Err(crate::StorageValueError::NonCanonicalOrder);
        }
        for (outcome, terminal) in outcomes.iter().zip(&terminals) {
            if terminal.link()
                != (riffdb_types::ServiceAuditLinkV1::Command {
                    commit_sequence: outcome.commit_sequence(),
                    provenance_id: outcome.provenance_id(),
                })
            {
                return Err(crate::StorageValueError::IdentityMismatch);
            }
        }
        Ok(Self {
            outcomes,
            terminals,
        })
    }

    /// Returns the number of independently identified commands retained by
    /// this unpublished subgroup.
    #[must_use]
    pub fn command_count(&self) -> usize {
        self.outcomes.len()
    }

    /// Returns the first independently assigned command sequence.
    #[must_use]
    pub fn first_commit_sequence(&self) -> CommitSequence {
        self.outcomes[0].commit_sequence()
    }

    /// Returns the final independently assigned command sequence.
    #[must_use]
    pub fn last_commit_sequence(&self) -> CommitSequence {
        self.outcomes[self.outcomes.len() - 1].commit_sequence()
    }

    /// Returns the final linked administration sequence in this subgroup.
    #[must_use]
    pub fn last_administration_sequence(&self) -> riffdb_types::AdministrationSequence {
        self.terminals[self.terminals.len() - 1].administration_sequence()
    }

    /// Consumes the unpublished material for a backend-owned, post-fence seal.
    #[doc(hidden)]
    #[must_use]
    pub fn into_parts(self) -> (Vec<StoredOutcomeV1>, Vec<crate::StoredServiceAuditRecordV1>) {
        (self.outcomes, self.terminals)
    }
}

impl AuditedCommittedBatchV1 {
    /// Joins the two storage-returned results after exact link validation.
    pub fn new(
        batch: CommittedBatchV1,
        terminals: Vec<crate::StoredServiceAuditRecordV1>,
    ) -> Result<Self, crate::StorageValueError> {
        if batch.outcomes().len() != terminals.len()
            || terminals.is_empty()
            || terminals
                .iter()
                .any(|terminal| terminal.phase() != riffdb_types::ServiceAuditPhaseV1::Succeeded)
        {
            return Err(crate::StorageValueError::IdentityMismatch);
        }
        for (outcome, terminal) in batch.outcomes().iter().zip(&terminals) {
            if terminal.link()
                != (riffdb_types::ServiceAuditLinkV1::Command {
                    commit_sequence: outcome.commit_sequence(),
                    provenance_id: outcome.provenance_id(),
                })
            {
                return Err(crate::StorageValueError::IdentityMismatch);
            }
        }
        Ok(Self { batch, terminals })
    }

    /// Borrows the committed command result.
    #[must_use]
    pub const fn batch(&self) -> &CommittedBatchV1 {
        &self.batch
    }

    /// Borrows the linked terminal audit.
    #[must_use]
    pub fn terminals(&self) -> &[crate::StoredServiceAuditRecordV1] {
        &self.terminals
    }

    /// Consumes the compound result.
    #[must_use]
    pub fn into_parts(self) -> (CommittedBatchV1, Vec<crate::StoredServiceAuditRecordV1>) {
        (self.batch, self.terminals)
    }
}

/// Closed result when attempting to add a candidate to one prior batch state.
pub enum CandidateStartResult<P, C> {
    /// The candidate owns the transaction and must progress or be dropped.
    Started(C),
    /// The command-count ceiling is full; prior and intent are unchanged.
    BatchFull {
        /// Exact prior batch state.
        prior: P,
        /// Unchanged candidate intent for a later transaction.
        intent: Box<CommitIntent>,
    },
}

/// Candidate state before exact durable admission recheck.
pub trait CommandCandidateAdmission: Sized {
    /// Exact prior empty or nonempty batch class.
    type Prior;
    /// State after the exact pending admission remains current.
    type StateRead: CommandCandidateStateRead<Prior = Self::Prior>;

    /// Rechecks the exact pending admission before any current-state validation.
    fn recheck_admission(
        self,
    ) -> Result<CandidateAdmissionResult<Self::Prior, Self::StateRead>, StorageError>;
}

/// Closed candidate resolution before transaction-current reads.
pub enum CandidateAdmissionResult<P, N> {
    /// The exact pending state remains and the candidate may continue.
    Proceed(N),
    /// No pending state exists for the candidate identity.
    MissingPending(AbandonedCandidate<P>),
    /// Durable pending fields no longer equal the candidate's exact admission.
    PendingMismatch(AbandonedCandidate<P>),
    /// A different canonical input already consumes the identity.
    InputMismatch(AbandonedCandidate<P>),
    /// The equal-input declared outcome already committed.
    StoredOutcome {
        /// Exact unchanged prior batch state.
        prior: P,
        /// Existing immutable stored outcome.
        outcome: StoredOutcomeV1,
    },
    /// The equal-input deterministic failure already became terminal.
    ExecutionFailed {
        /// Exact unchanged prior batch state.
        prior: P,
        /// Existing immutable failure state.
        failure: StoredExecutionFailedV1,
    },
}

/// Exact prior state and unchanged intent for a candidate that staged nothing.
pub struct AbandonedCandidate<P> {
    prior: P,
    intent: Box<CommitIntent>,
}

impl<P> AbandonedCandidate<P> {
    /// Constructs a typed no-write candidate return.
    #[must_use]
    pub const fn new(prior: P, intent: Box<CommitIntent>) -> Self {
        Self { prior, intent }
    }

    /// Returns the exact prior batch and unchanged intent.
    #[must_use]
    pub fn into_parts(self) -> (P, Box<CommitIntent>) {
        (self.prior, self.intent)
    }
}

/// Candidate state permitted only to materialize complete current values.
pub trait CommandCandidateStateRead: Sized {
    /// Exact prior empty or nonempty batch class.
    type Prior;
    /// State that awaits the coordinator's private semantic decision.
    type AwaitingValidation: CommandCandidateAwaitingValidation<Prior = Self::Prior>;

    /// Reads every exact validation target from transaction-current state.
    fn read_transaction_current(
        self,
    ) -> Result<(Self::AwaitingValidation, TransactionCurrentState), StorageError>;
}

/// Candidate state after influential current values exist but before validation.
pub trait CommandCandidateAwaitingValidation: Sized {
    /// Exact prior empty or nonempty batch class.
    type Prior;
    /// State that reads mutation-derived epoch positions in the same transaction.
    type AffectedEpochRead: CommandCandidateAffectedEpochRead<Prior = Self::Prior>;

    /// Reads capability revision and compiler-derived relationship evidence
    /// from this exact authoritative write transaction.
    ///
    /// The default is deliberately fail-closed so an adapter cannot enable a
    /// protected command merely by implementing the older command protocol.
    fn read_transaction_current_policy(
        &self,
        _request: &TransactionCurrentPolicyRequestV1,
    ) -> Result<TransactionCurrentPolicyStateV1, StorageError> {
        Err(StorageError::new(
            StorageErrorKind::InvariantViolation,
            None,
        ))
    }

    /// Advances after private validation supplies the complete affected bucket set.
    fn plan_validated(self, affected_targets: AffectedIndexEpochTargets)
    -> Self::AffectedEpochRead;

    /// Abandons the candidate with no sequence and returns the exact prior state.
    fn reject(self, reason: CandidateValidationRejection) -> AbandonedCandidate<Self::Prior>;
}

/// Candidate state permitted only to read exact affected epoch positions.
pub trait CommandCandidateAffectedEpochRead: Sized {
    /// Exact prior empty or nonempty batch class.
    type Prior;
    /// State awaiting final staged-capacity reservation.
    type AwaitingCapacity: CommandCandidateAwaitingCapacity<Prior = Self::Prior>;

    /// Reads and retains every affected target from transaction-current state.
    fn read_affected_epoch_current(self) -> Result<Self::AwaitingCapacity, StorageError>;
}

/// Candidate state after all validation reads and before any sequence assignment.
pub trait CommandCandidateAwaitingCapacity: Sized {
    /// Exact prior empty or nonempty batch class.
    type Prior;
    /// State that alone may assign the next sequence.
    type CapacityReserved: CommandCandidateCapacityReserved<Prior = Self::Prior>;

    /// Borrows the exact candidate intent retained since admission recheck.
    fn intent(&self) -> &CommitIntent;

    /// Borrows the exact affected target set retained after private validation.
    fn affected_targets(&self) -> &AffectedIndexEpochTargets;

    /// Borrows the exact transaction-current epoch positions retained by this state.
    fn affected_current(&self) -> &AffectedEpochCurrentState;

    /// Abandons the post-index-read candidate before sequence assignment.
    fn reject(self, reason: CandidateValidationRejection) -> AbandonedCandidate<Self::Prior>;

    /// Reserves exact semantic and conservative encoded batch capacity.
    ///
    /// A conforming implementation must reject and abort if the supplied plan's
    /// intent-derived shape, affected targets, or current epoch positions do not
    /// exactly equal the values retained by this consuming state. Equivalence is
    /// defined by [`CommandWriteSetPlanV1::matches_retained_candidate`]. Before
    /// returning `Reserved`, it must also prove that the intent's immutable
    /// provenance ID exists in neither committed state nor this batch's staged
    /// prefix. A collision consumes and aborts the transaction before assignment.
    fn reserve_capacity(
        self,
        write_plan: CommandWriteSetPlanV1,
    ) -> Result<CandidateCapacityResult<Self::Prior, Self::CapacityReserved>, StorageError>;
}

/// Closed pre-sequence result of reserving capacity for one validated candidate.
pub enum CandidateCapacityResult<P, R> {
    /// Both semantic and conservative encoded capacity are retained.
    Reserved(R),
    /// The transaction-derived plan is discarded; only prior and intent survive.
    BatchFull(AbandonedCandidate<P>),
    /// The immutable provenance identity already exists in committed state or
    /// the current staged prefix. The entire transaction is aborted before any
    /// application sequence is assigned.
    ProvenanceIdCollision(ProvenanceIdCollision),
}

/// Normal, non-storage reasons a transaction-current candidate stages nothing.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CandidateValidationRejection {
    /// An entity absence/version or range epoch changed.
    DependencyChanged,
    /// The exact historical commit-check plan evaluated false.
    CommitCheckRejected,
    /// The exact historical commit-check plan encountered checked arithmetic failure.
    ///
    /// This is a non-durable coordinator control result. The application candidate
    /// must be abandoned before sequence assignment so the coordinator can use the
    /// separate execution-failure transition.
    CommitCheckArithmeticFault,
    /// A mutation or secondary-index precondition changed.
    MutationPreconditionChanged,
    /// Another entity occupies one exact declared unique key.
    UniqueConflict,
    /// Transaction-current capability, relationship, or row policy denied.
    ///
    /// This remains an opaque no-mutation result and never distinguishes a
    /// hidden row from revoked or stale authority.
    RowPolicyDenied,
}

/// Candidate state available only after exact pre-sequence capacity reservation.
pub trait CommandCandidateCapacityReserved: Sized {
    /// Exact prior empty or nonempty batch class.
    type Prior;
    /// State owning the transaction's newly assigned invisible sequence.
    type SequenceAssigned: CommandCandidateSequenceAssigned<Prior = Self::Prior>;

    /// Borrows the exact candidate intent retained by the transaction chain.
    fn intent(&self) -> &CommitIntent;

    /// Borrows the exact reserved sequence-free plan retained by this state.
    fn write_plan(&self) -> &CommandWriteSetPlanV1;

    /// Assigns the next nonzero application sequence inside the transaction.
    fn assign_sequence(self) -> Result<Self::SequenceAssigned, StorageError>;
}

/// Candidate state after an invisible transaction-local sequence assignment.
pub trait CommandCandidateSequenceAssigned: Sized {
    /// Exact prior batch class retained by the transaction.
    type Prior;
    /// Nonempty state returned only after one complete record set is staged.
    type Staged: NonEmptyCommandBatch;

    /// Returns the transaction-local sequence assignment for record construction.
    fn assignment(&self) -> AssignedCommandSequence;

    /// Borrows the exact candidate intent retained by the transaction chain.
    fn intent(&self) -> &CommitIntent;

    /// Borrows the exact plan whose capacity remains reserved through staging.
    fn write_plan(&self) -> &CommandWriteSetPlanV1;

    /// Stages the complete record graph after every pre-sequence invariant check.
    ///
    /// The record set's intent and exact write plan must equal the retained values,
    /// and its commit sequence must equal [`assignment`](Self::assignment).assigned.
    /// Every final entity/event/outcome/provenance/commit field must be the exact
    /// deterministic derivation of that intent and assignment, not an equal-size
    /// substitute. The backend must require
    /// [`AtomicCommandRecordSet::matches_reserved_candidate`] before writing. Any
    /// mismatch is a fatal invariant violation that aborts the batch.
    /// The WP-060 memory model checks explicit synthetic complete-envelope
    /// charges per record class and makes no durable-encoding claim. Once the
    /// WP-065 codec exists, a durable backend must recompute every actual
    /// canonical `StoredEnvelope` charge and prove each class is within the
    /// retained upper bound before staging any write.
    ///
    /// Provenance uniqueness was already checked while reserving capacity. A
    /// later mismatch is an invariant failure that aborts the whole transaction,
    /// never a post-assignment collision result.
    fn stage(self, records: AtomicCommandRecordSet) -> Result<Self::Staged, StorageError>;
}

/// Fatal pre-sequence proof that an immutable provenance key already exists.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProvenanceIdCollision {
    _private: (),
}

impl ProvenanceIdCollision {
    /// Constructs the redacted collision result after an insert-if-absent check.
    #[must_use]
    pub const fn detected() -> Self {
        Self { _private: () }
    }
}

/// One transaction-local nonzero assignment and the allocator state it must store.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct AssignedCommandSequence {
    assigned: CommitSequence,
    next_allocator: ApplicationSequenceAllocator,
}

impl AssignedCommandSequence {
    /// Constructs the exact allocator transition for one assigned sequence.
    #[must_use]
    pub fn from_assigned(assigned: CommitSequence) -> Self {
        let next_allocator = assigned.checked_next().map_or(
            ApplicationSequenceAllocator::Exhausted,
            ApplicationSequenceAllocator::Next,
        );
        Self {
            assigned,
            next_allocator,
        }
    }

    /// Returns the transaction-local assigned sequence.
    #[must_use]
    pub const fn assigned(self) -> CommitSequence {
        self.assigned
    }

    /// Returns allocator metadata after this assignment.
    #[must_use]
    pub const fn next_allocator(self) -> ApplicationSequenceAllocator {
        self.next_allocator
    }
}

/// Ordered outcomes returned only after one nonempty batch commits.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedBatchV1 {
    outcomes: Vec<StoredOutcomeV1>,
    durability_mode: DurabilityMode,
}

impl CommittedBatchV1 {
    /// Validates nonempty, bounded, contiguous sequence order and durability.
    pub fn new(
        outcomes: Vec<StoredOutcomeV1>,
        durability_mode: DurabilityMode,
    ) -> Result<Self, StorageValueError> {
        if outcomes.is_empty() {
            return Err(StorageValueError::Empty);
        }
        if outcomes.len() > MAX_STAGED_COMMANDS {
            return Err(StorageValueError::LimitExceeded);
        }
        if outcomes
            .iter()
            .any(|outcome| outcome.durability_mode() != durability_mode)
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        for pair in outcomes.windows(2) {
            if pair[0].commit_sequence().checked_next() != Some(pair[1].commit_sequence()) {
                return Err(StorageValueError::NonCanonicalOrder);
            }
        }
        Ok(Self {
            outcomes,
            durability_mode,
        })
    }

    /// Borrows committed outcomes in assigned sequence order.
    #[must_use]
    pub fn outcomes(&self) -> &[StoredOutcomeV1] {
        &self.outcomes
    }

    /// Returns the durability contract used for the whole atomic engine commit.
    #[must_use]
    pub const fn durability_mode(&self) -> DurabilityMode {
        self.durability_mode
    }
}
