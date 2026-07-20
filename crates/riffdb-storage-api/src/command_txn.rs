//! Consuming synchronous type-state protocol for authoritative command batches.

use std::num::NonZeroU8;

use riffdb_types::CommitSequence;

use crate::{
    AffectedEpochCurrentState, AffectedIndexEpochTargets, ApplicationSequenceAllocator,
    AtomicCommandRecordSet, CommandWriteSetChargeV1, CommandWriteSetPlanV1, CommitIntent,
    DurabilityMode, MAX_STAGED_COMMANDS, MAX_STAGED_WRITE_BYTES, StorageError, StorageValueError,
    StoredExecutionFailedV1, StoredOutcomeV1, TransactionCurrentState,
};

/// Checked runtime accounting for a nonempty uncommitted command batch.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct StagedBatchMetrics {
    command_count: NonZeroU8,
    semantic_bytes: usize,
    reserved_encoded_bytes: usize,
}

impl StagedBatchMetrics {
    /// Validates the accepted command-count and aggregate write-set ceilings.
    pub fn new(
        command_count: NonZeroU8,
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
    pub const fn command_count(self) -> NonZeroU8 {
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

    /// Rolls back every staged command without a commit attempt.
    fn rollback(self);
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
