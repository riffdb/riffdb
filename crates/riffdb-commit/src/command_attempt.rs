//! One capability-owning deterministic command-evaluation attempt.

use std::{
    error::Error,
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
    time::Instant,
};

use riffdb_catalog::{
    CommandSnapshotMaterialization, CommandSnapshotResourceLimitEvidence,
    MaterializedCommandSnapshot, ResolvedExecutablePlan, ResourceLimitRecheck,
    TransactionCurrentMaterialization,
};
use riffdb_conflict::{CancellationToken, ConflictError, ConflictManager, MutationLease};
use riffdb_contract_ir::BindingMode;
use riffdb_invariant::InputDerivedCommandFacts;
use riffdb_policy::{
    AuthorizedCommandRowPolicyContextV1, resolve_authorized_command_row_policy_context,
};
use riffdb_runtime::{
    ExecutionFault, ExecutionResult, TransactionContext, execute_command_with_facts,
};
use riffdb_storage_api::{
    AdmissionLookupResultV1, AdmissionRepository, CandidateValidationRejection,
    CommandCandidateAwaitingValidation, CommitIntent, EvaluatedCommand, EvaluationBudget,
    ExecutionFailureTransitionRequestV1, IdempotencyLookupCandidatesV1, PreEvaluationCommitContext,
    ReadSnapshot, SnapshotReader, SnapshotRequest, StorageError, StoredAdmissionStateV1,
    StoredExecutionFailedV1, StoredOutcomeV1, TransactionCurrentState,
};
use riffdb_types::{CanonicalRecord, ConflictKey, ExecutionFailureCode, ProvenanceId, RequestId};

use crate::command_admission::{CommandExecutionCandidate, CommandExecutionCandidateParts};
use crate::command_preparation::{
    AuditedCommandLifecycle, PostEvaluationAuthorizationError, PostEvaluationCommandAuthorizer,
};

/// Maximum snapshot-materialization attempt slots in one outer invocation.
pub(crate) const MAX_COMMAND_EVALUATION_ATTEMPTS_V1: usize = 3;

/// Move-only admitted state between complete command-evaluation attempts.
pub(crate) struct PendingCommandAttempts {
    resolved_plan: ResolvedExecutablePlan,
    normalized_input: CanonicalRecord,
    /// Derived once from the plan and input this state retains, both of which
    /// are immutable for the attempt's lifetime, so every later stage borrows
    /// this one proof instead of re-deriving an identical copy.
    input_facts: InputDerivedCommandFacts,
    commit_context: PreEvaluationCommitContext,
    raw_conflict_keys: Vec<ConflictKey>,
    snapshot_request: SnapshotRequest,
    binding_modes: Vec<BindingMode>,
    lookup_candidates: IdempotencyLookupCandidatesV1,
    #[allow(dead_code)] // Retained for redaction-safe per-invocation coordinator telemetry.
    invocation_request_id: RequestId,
    deadline: Instant,
    cancellation: CancellationToken,
    audited_lifecycle: Option<AuditedCommandLifecycle>,
    terminal_admission: bool,
    post_evaluation_authorizer: Option<Box<dyn PostEvaluationCommandAuthorizer>>,
    row_policy: Option<AuthorizedCommandRowPolicyContextV1>,
    completed_attempts: usize,
}

impl PendingCommandAttempts {
    /// Lowers one exact admission candidate without reconstructing semantic facts.
    pub(crate) fn from_admission(
        candidate: Box<CommandExecutionCandidate>,
    ) -> Result<Self, CommandAttemptError> {
        let lookup_candidates = candidate
            .lookup_candidates_for_resolution()
            .map_err(|_| CommandAttemptError::Integrity)?;
        let CommandExecutionCandidateParts {
            resolved_plan,
            normalized_input,
            input_facts,
            commit_context,
            raw_conflict_keys,
            snapshot_request,
            invocation_request_id,
            deadline,
            cancellation,
            audited_lifecycle,
            terminal_admission,
            lookup_candidates: retained_lookup_candidates,
            post_evaluation_authorizer,
            row_policy,
        } = (*candidate).into_acquisition_parts();
        if lookup_candidates != retained_lookup_candidates {
            return Err(CommandAttemptError::Integrity);
        }
        // The proof travels from preparation rather than being re-derived; the
        // bind to this exact plan and input is what re-derivation used to give
        // by construction, so it is checked explicitly here instead.
        if !input_facts.matches_command(resolved_plan.plan(), &normalized_input) {
            return Err(CommandAttemptError::Integrity);
        }
        if input_facts.binding_plan_indices().len() != snapshot_request.binding_targets().len() {
            return Err(CommandAttemptError::Integrity);
        }
        let binding_modes = input_facts
            .binding_plan_indices()
            .iter()
            .map(|index| {
                resolved_plan
                    .plan()
                    .bindings()
                    .get(*index as usize)
                    .map(|binding| binding.mode())
                    .ok_or(CommandAttemptError::Integrity)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            resolved_plan,
            normalized_input,
            input_facts,
            commit_context,
            raw_conflict_keys,
            snapshot_request,
            binding_modes,
            lookup_candidates,
            invocation_request_id,
            deadline,
            cancellation,
            audited_lifecycle,
            terminal_admission,
            post_evaluation_authorizer,
            row_policy,
            completed_attempts: 0,
        })
    }

    /// Returns the number of snapshot-materialization attempt slots consumed.
    #[allow(dead_code)] // Semantic-test inspection of the bounded retry state.
    pub(crate) const fn completed_attempts(&self) -> usize {
        self.completed_attempts
    }

    pub(crate) fn raw_conflict_keys(&self) -> &[ConflictKey] {
        &self.raw_conflict_keys
    }

    pub(crate) fn binding_accesses(
        &self,
    ) -> impl Iterator<Item = (BindingMode, &riffdb_storage_api::EntityTarget)> {
        self.binding_modes
            .iter()
            .zip(self.snapshot_request.binding_targets())
            .map(|(mode, target)| (*mode, target))
    }

    pub(crate) fn root_validation_targets(&self) -> &[riffdb_storage_api::EntityTarget] {
        self.snapshot_request.root_validation_targets()
    }

    /// Returns the compiler-derived grouping proof, if this command remains a
    /// pure child append under the exact deployed schema.
    pub(crate) fn commutative_child_append_proof(
        &self,
    ) -> Option<riffdb_contract_ir::CommutativeChildAppendProof> {
        self.resolved_plan
            .plan()
            .commutative_child_append_proof(self.resolved_plan.bundle().bundle().schema())
    }

    /// Returns whether this is a fresh synchronous terminal-admission attempt,
    /// the initial closed eligibility class for serial micro-batching.
    pub(crate) fn serial_micro_batch_eligible(&self) -> bool {
        self.terminal_admission
            && !self
                .resolved_plan
                .plan()
                .delete_checks()
                .iter()
                .any(|check| {
                    matches!(
                        check.mode(),
                        riffdb_contract_ir::DeleteCheckModeV1::Cascade { .. }
                    )
                })
    }

    /// Returns whether the complete public service lifecycle is retained so a
    /// fenced subgroup can atomically stage both audit phases before its tail.
    pub(crate) const fn has_audited_lifecycle(&self) -> bool {
        self.audited_lifecycle.is_some()
    }

    pub(crate) fn idempotency_identity(&self) -> &riffdb_storage_api::IdempotencyIdentity {
        self.commit_context.pending().identity()
    }
}

impl fmt::Debug for PendingCommandAttempts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PendingCommandAttempts([REDACTED])")
    }
}

/// Closed result of one exact capability-owning evaluation attempt.
#[allow(clippy::large_enum_variant)] // Keep move-only capability state inline at this private boundary.
pub(crate) enum CommandAttemptResolution {
    /// Evaluation produced a commit-required candidate while retaining its capability.
    Evaluated(EvaluatedCommandAttempt),
    /// Evaluation produced a dependency-sensitive deterministic failure.
    ExecutionFault(ExecutionFaultAttempt),
    /// A matching durable command outcome won before snapshot materialization.
    OutcomeReplay(StoredOutcomeV1),
    /// A matching durable deterministic failure won before snapshot materialization.
    ExecutionFailureReplay(StoredExecutionFailedV1),
}

impl fmt::Debug for CommandAttemptResolution {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Evaluated(_) => "CommandAttemptResolution::Evaluated([REDACTED])",
            Self::ExecutionFault(_) => "CommandAttemptResolution::ExecutionFault([REDACTED])",
            Self::OutcomeReplay(_) => "CommandAttemptResolution::OutcomeReplay([REDACTED])",
            Self::ExecutionFailureReplay(_) => {
                "CommandAttemptResolution::ExecutionFailureReplay([REDACTED])"
            }
        })
    }
}

/// Successful deterministic evaluation bundled with its exclusive capability.
pub(crate) struct EvaluatedCommandAttempt {
    state: PendingCommandAttempts,
    lease: CommandMutationAuthority,
    snapshot: MaterializedCommandSnapshot,
    evaluated: Arc<EvaluatedCommand>,
    prepared_body: Option<crate::command_index::PreparedCommandBody>,
}

impl EvaluatedCommandAttempt {
    /// Performs the mandatory fresh policy safe point after deterministic
    /// evaluation and consumes the returned proof after exact semantic checks.
    pub(super) fn authorize_after_evaluation(
        self,
    ) -> Result<Self, PostEvaluationAuthorizationError> {
        authorize_state_after_evaluation(&self.state)?;
        Ok(self)
    }

    /// Releases a completed, uncommitted evaluation back to its exact pending
    /// retry state. This is used only when a compatible physical group must be
    /// abandoned before any storage commit is attempted.
    pub(super) fn into_pending_without_commit(self) -> PendingCommandAttempts {
        let Self {
            state,
            lease,
            snapshot,
            evaluated,
            prepared_body,
        } = self;
        drop(prepared_body);
        drop(evaluated);
        drop(snapshot);
        drop(lease);
        state
    }

    /// Rechecks advisory request control at the final pre-transaction safe point.
    pub(super) fn recheck_request_control(&self) -> Result<(), CommandAttemptError> {
        check_request_control(self.state.deadline, &self.state.cancellation)
    }

    pub(super) const fn commit_context(&self) -> &PreEvaluationCommitContext {
        &self.state.commit_context
    }

    pub(super) const fn resolved_plan(&self) -> &ResolvedExecutablePlan {
        &self.state.resolved_plan
    }

    pub(super) const fn normalized_input(&self) -> &CanonicalRecord {
        &self.state.normalized_input
    }

    /// Borrows the one input-derived proof this attempt retains.
    pub(super) const fn input_facts(&self) -> &InputDerivedCommandFacts {
        &self.state.input_facts
    }

    pub(super) const fn materialized_snapshot(&self) -> &MaterializedCommandSnapshot {
        &self.snapshot
    }

    pub(super) fn evaluated(&self) -> &EvaluatedCommand {
        &self.evaluated
    }

    /// Replays a V18 sealed decision against its exact normalized snapshot.
    /// Worker preparation caches the resulting mutation proof, so the writer
    /// does not repeat this per-plan/per-attempt semantic check.
    pub(super) fn sealed_decision_evaluation_is_exact(&self) -> bool {
        if !self.resolved_plan().plan().requires_ir_v18() {
            return true;
        }
        matches!(
            execute_command_with_facts(
                self.resolved_plan().bundle().bundle(),
                self.normalized_input(),
                self.input_facts(),
                self.materialized_snapshot().snapshot(),
                &transaction_context(self.commit_context()),
                EvaluationBudget::v1(),
            ),
            Ok(ExecutionResult::CommitRequired(replayed)) if replayed == *self.evaluated()
        )
    }

    /// Installs deterministic worker preparation bound to this exact attempt.
    pub(super) fn prepare_body(mut self) -> Result<Self, CommandAttemptError> {
        if self.prepared_body.is_some() {
            return Err(CommandAttemptError::Integrity);
        }
        let prepared = crate::command_index::prepare_command_body(&self)
            .map_err(|_| CommandAttemptError::Integrity)?;
        if let Some(prepared) = prepared {
            if !prepared.matches_attempt(&self) {
                return Err(CommandAttemptError::Integrity);
            }
            self.prepared_body = Some(prepared);
        }
        Ok(self)
    }

    pub(super) fn take_prepared_mutation_positions(&mut self) -> Option<Box<[Option<usize>]>> {
        self.prepared_body
            .as_mut()
            .and_then(crate::command_index::PreparedCommandBody::take_mutation_positions)
    }

    pub(super) fn take_prepared_indexes(
        &mut self,
    ) -> Option<crate::command_index::DerivedCommandIndexes> {
        self.prepared_body
            .as_mut()
            .and_then(crate::command_index::PreparedCommandBody::take_indexes)
    }

    pub(super) fn take_prepared_capsule(
        &mut self,
    ) -> Option<riffdb_storage_api::PreparedCapsuleCommandFragmentsV1> {
        self.prepared_body
            .as_mut()
            .and_then(crate::command_index::PreparedCommandBody::take_capsule)
    }

    pub(super) fn audited_lifecycle(&self) -> Option<&AuditedCommandLifecycle> {
        self.state.audited_lifecycle.as_ref()
    }

    /// Proves all independently checked values still belong to the one admitted
    /// attempt aggregate that produced them.
    pub(super) fn has_exact_semantic_join(&self) -> bool {
        let pending = self.state.commit_context.pending();
        let resolved = self.snapshot.resolved_plan();
        let snapshot = self.snapshot.snapshot();
        pending.plan() == self.state.resolved_plan.reference()
            && self.state.resolved_plan.reference() == resolved.reference()
            && self.state.snapshot_request.plan() == resolved.reference()
            && snapshot.plan() == resolved.reference()
            && snapshot_matches_request(&self.state.snapshot_request, snapshot)
            && snapshot.validation_request() == *self.evaluated.validation_request()
            && snapshot.read_dependencies() == self.evaluated.read_dependencies()
            && self.evaluated.plan() == resolved.reference()
            && self.evaluated.validation_request().plan() == resolved.reference()
    }

    /// Binds the sole provenance candidate sourced for this successful evaluation.
    pub(super) fn bind_provenance(
        self,
        provenance_id: ProvenanceId,
    ) -> Result<ProvenanceBoundCommandAttempt, CommandAttemptError> {
        if !self.has_exact_semantic_join() {
            return Err(CommandAttemptError::Integrity);
        }
        let intent = if self.state.terminal_admission {
            CommitIntent::new_for_vacant_terminal_admission(
                self.state.commit_context.clone(),
                self.state.lookup_candidates.clone(),
                self.evaluated.clone(),
                provenance_id,
            )
        } else {
            CommitIntent::new(
                self.state.commit_context.clone(),
                self.evaluated.clone(),
                provenance_id,
            )
        }
        .map_err(|_| CommandAttemptError::Integrity)?;
        let bound = ProvenanceBoundCommandAttempt {
            attempt: self,
            intent,
        };
        if !bound.has_exact_semantic_join() {
            return Err(CommandAttemptError::Integrity);
        }
        Ok(bound)
    }
}

impl fmt::Debug for EvaluatedCommandAttempt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EvaluatedCommandAttempt([REDACTED])")
    }
}

/// A successfully evaluated attempt bound to its one sourced provenance identity.
pub(super) struct ProvenanceBoundCommandAttempt {
    attempt: EvaluatedCommandAttempt,
    intent: CommitIntent,
}

/// Same-attempt evidence retained after the sole writer has privately applied
/// the complete command graph.
///
/// This state intentionally contains no [`CommandMutationAuthority`]. The
/// ordered writer and private redb successor now own serialization; retaining
/// the conflict lease until the independent durability fence would prevent a
/// later FIFO command from evaluating against that successor and collapse the
/// journal pipeline on ordinary shared partition/index keys.
pub(super) struct PostApplyCommandEvidence {
    state: PendingCommandAttempts,
    intent: CommitIntent,
}

impl PostApplyCommandEvidence {
    pub(super) const fn exact_intent(&self) -> &CommitIntent {
        &self.intent
    }

    pub(super) const fn lookup_candidates(&self) -> &IdempotencyLookupCandidatesV1 {
        &self.state.lookup_candidates
    }

    pub(super) fn matches_terminal_outcome(&self, outcome: &StoredOutcomeV1) -> bool {
        outcome_matches_state(outcome, &self.state)
            && outcome.plan() == self.intent.evaluated().plan()
            && outcome.provenance_id() == self.intent.provenance_id()
    }

    pub(super) fn matches_terminal_failure(&self, failure: &StoredExecutionFailedV1) -> bool {
        failure_matches_state(failure, &self.state)
    }

    pub(super) fn into_pending_after_proven_noncommit(self) -> PendingCommandAttempts {
        let Self { state, intent } = self;
        drop(intent);
        state
    }
}

impl fmt::Debug for PostApplyCommandEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PostApplyCommandEvidence([REDACTED])")
    }
}

impl ProvenanceBoundCommandAttempt {
    pub(super) const fn commit_intent(&self) -> &CommitIntent {
        &self.intent
    }

    /// Produces the one narrow clone transferred into storage candidate admission.
    pub(super) fn storage_intent(&self) -> Box<CommitIntent> {
        Box::new(self.intent.clone())
    }

    pub(super) const fn commit_context(&self) -> &PreEvaluationCommitContext {
        self.attempt.commit_context()
    }

    pub(super) const fn resolved_plan(&self) -> &ResolvedExecutablePlan {
        self.attempt.resolved_plan()
    }

    pub(super) const fn normalized_input(&self) -> &CanonicalRecord {
        self.attempt.normalized_input()
    }

    pub(super) const fn input_facts(&self) -> &InputDerivedCommandFacts {
        self.attempt.input_facts()
    }

    pub(super) const fn materialized_snapshot(&self) -> &MaterializedCommandSnapshot {
        self.attempt.materialized_snapshot()
    }

    pub(super) fn evaluated(&self) -> &EvaluatedCommand {
        self.attempt.evaluated()
    }

    pub(super) fn sealed_decision_evaluation_is_exact(&self) -> bool {
        self.attempt.sealed_decision_evaluation_is_exact()
    }

    pub(super) fn take_prepared_mutation_positions(&mut self) -> Option<Box<[Option<usize>]>> {
        self.attempt.take_prepared_mutation_positions()
    }

    pub(super) fn take_prepared_indexes(
        &mut self,
    ) -> Option<crate::command_index::DerivedCommandIndexes> {
        self.attempt.take_prepared_indexes()
    }

    pub(super) fn take_prepared_capsule(
        &mut self,
    ) -> Option<riffdb_storage_api::PreparedCapsuleCommandFragmentsV1> {
        self.attempt.take_prepared_capsule()
    }

    pub(super) const fn lookup_candidates(&self) -> &IdempotencyLookupCandidatesV1 {
        &self.attempt.state.lookup_candidates
    }

    pub(super) fn audited_lifecycle(&self) -> Option<&AuditedCommandLifecycle> {
        self.attempt.audited_lifecycle()
    }

    pub(super) const fn row_policy(&self) -> Option<&AuthorizedCommandRowPolicyContextV1> {
        self.attempt.state.row_policy.as_ref()
    }

    pub(super) fn matches_terminal_outcome(&self, outcome: &StoredOutcomeV1) -> bool {
        outcome_matches_state(outcome, &self.attempt.state)
    }

    pub(super) fn matches_terminal_failure(&self, failure: &StoredExecutionFailedV1) -> bool {
        failure_matches_state(failure, &self.attempt.state)
    }

    pub(super) fn has_exact_semantic_join(&self) -> bool {
        let context = self.attempt.commit_context();
        self.attempt.has_exact_semantic_join()
            && self.intent.pending() == context.pending()
            && self.intent.evaluated() == self.attempt.evaluated()
            && self.intent.partition_hash() == context.partition_hash()
            && self.intent.conflict_hashes() == context.conflict_hashes()
    }

    /// Rejects the live storage candidate before releasing the logical capability.
    /// The closed reason determines whether the completed attempt may retry or must
    /// enter dependency-validated arithmetic-fault terminalization.
    pub(super) fn reject_storage_and_rollback<C>(
        self,
        candidate: C,
        reason: CandidateValidationRejection,
    ) -> RolledBackCandidateDisposition
    where
        C: CommandCandidateAwaitingValidation,
    {
        let abandoned = candidate.reject(reason);
        let (prior, storage_intent) = abandoned.into_parts();
        let intent_matches = *storage_intent == self.intent;
        drop(prior);
        drop(storage_intent);

        self.finish_after_candidate_rollback(intent_matches, reason)
    }

    pub(super) fn reject_after_index_read_and_rollback<C>(
        self,
        candidate: C,
        reason: CandidateValidationRejection,
    ) -> RolledBackCandidateDisposition
    where
        C: riffdb_storage_api::CommandCandidateAwaitingCapacity,
    {
        let abandoned = candidate.reject(reason);
        let (prior, storage_intent) = abandoned.into_parts();
        let intent_matches = *storage_intent == self.intent;
        drop(prior);
        drop(storage_intent);
        self.finish_after_candidate_rollback(intent_matches, reason)
    }

    /// Discards one unpersisted provenance attempt only after storage proved
    /// that its uncertain commit did not replace the exact Pending admission.
    pub(super) fn into_pending_after_proven_noncommit(self) -> PendingCommandAttempts {
        let Self { attempt, intent } = self;
        drop(intent);
        let EvaluatedCommandAttempt {
            state,
            lease,
            snapshot,
            evaluated,
            prepared_body,
        } = attempt;
        drop(prepared_body);
        drop(evaluated);
        drop(snapshot);
        drop(lease);
        state
    }

    /// Transfers serialization authority from the conflict manager to the
    /// sole writer after storage has accepted the complete private transition.
    /// No constructor exists before that post-apply call site.
    pub(super) fn into_post_apply_evidence(self) -> Result<PostApplyCommandEvidence, ()> {
        if !self.has_exact_semantic_join() {
            return Err(());
        }
        let Self { attempt, intent } = self;
        let EvaluatedCommandAttempt {
            state,
            lease,
            snapshot,
            evaluated,
            prepared_body,
        } = attempt;
        drop(prepared_body);
        drop(evaluated);
        drop(snapshot);
        drop(lease);
        let evidence = PostApplyCommandEvidence { state, intent };
        if evidence.exact_intent().pending() != evidence.state.commit_context.pending()
            || evidence.exact_intent().partition_hash()
                != evidence.state.commit_context.partition_hash()
            || evidence.exact_intent().conflict_hashes()
                != evidence.state.commit_context.conflict_hashes()
        {
            return Err(());
        }
        Ok(evidence)
    }

    fn finish_after_candidate_rollback(
        self,
        intent_matches: bool,
        reason: CandidateValidationRejection,
    ) -> RolledBackCandidateDisposition {
        let Self { attempt, intent } = self;
        drop(intent);
        let EvaluatedCommandAttempt {
            state,
            lease,
            snapshot,
            evaluated,
            prepared_body,
        } = attempt;
        drop(prepared_body);
        drop(evaluated);

        if !intent_matches {
            drop(snapshot);
            drop(lease);
            return RolledBackCandidateDisposition::Integrity;
        }

        match reason {
            CandidateValidationRejection::CommitCheckArithmeticFault => {
                RolledBackCandidateDisposition::ExecutionFault(Box::new(
                    ExecutionFaultAttempt::Arithmetic {
                        state,
                        lease,
                        snapshot,
                    },
                ))
            }
            CandidateValidationRejection::UniqueConflict => {
                RolledBackCandidateDisposition::ExecutionFault(Box::new(
                    ExecutionFaultAttempt::UniqueConflict {
                        state,
                        lease,
                        snapshot,
                    },
                ))
            }
            CandidateValidationRejection::DependencyChanged
            | CandidateValidationRejection::CommitCheckRejected
            | CandidateValidationRejection::MutationPreconditionChanged => {
                drop(snapshot);
                drop(lease);
                RolledBackCandidateDisposition::Retry {
                    state: Box::new(state),
                    reason,
                }
            }
            CandidateValidationRejection::RowPolicyDenied => {
                drop(snapshot);
                drop(lease);
                drop(state);
                RolledBackCandidateDisposition::PolicyDenied
            }
        }
    }
}

impl fmt::Debug for ProvenanceBoundCommandAttempt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProvenanceBoundCommandAttempt([REDACTED])")
    }
}

/// Closed result after the authoritative candidate has been proven rolled back.
pub(super) enum RolledBackCandidateDisposition {
    /// The completed attempt may be reevaluated within the invocation ceiling.
    Retry {
        state: Box<PendingCommandAttempts>,
        #[allow(dead_code)] // Preserved for trusted retry telemetry and semantic tests.
        reason: CandidateValidationRejection,
    },
    /// Late commit-check arithmetic reuses the same attempt evidence without provenance.
    ExecutionFault(Box<ExecutionFaultAttempt>),
    /// Transaction-current row policy denied without revealing its cause.
    PolicyDenied,
    /// The storage candidate did not retain the exact provenance-bound intent.
    Integrity,
}

impl fmt::Debug for RolledBackCandidateDisposition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Retry { .. } => "RolledBackCandidateDisposition::Retry([REDACTED])",
            Self::ExecutionFault(_) => "RolledBackCandidateDisposition::ExecutionFault([REDACTED])",
            Self::PolicyDenied => "RolledBackCandidateDisposition::PolicyDenied",
            Self::Integrity => "RolledBackCandidateDisposition::Integrity",
        })
    }
}

/// Intrinsically resource-limit evidence; it cannot be paired with another code.
pub(crate) enum ResourceLimitFaultEvidence {
    /// Runtime resource failure over a normalized snapshot.
    Runtime(MaterializedCommandSnapshot),
    /// Valid lineage expansion exceeded a materialization limit before runtime.
    Materialization(CommandSnapshotResourceLimitEvidence),
}

impl fmt::Debug for ResourceLimitFaultEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Runtime(_) => "ResourceLimitFaultEvidence::Runtime([REDACTED])",
            Self::Materialization(_) => "ResourceLimitFaultEvidence::Materialization([REDACTED])",
        })
    }
}

/// Dependency-sensitive deterministic failure as a closed, move-only aggregate.
pub(crate) enum ExecutionFaultAttempt {
    Arithmetic {
        state: PendingCommandAttempts,
        lease: CommandMutationAuthority,
        snapshot: MaterializedCommandSnapshot,
    },
    ResourceLimit {
        state: PendingCommandAttempts,
        lease: CommandMutationAuthority,
        evidence: ResourceLimitFaultEvidence,
    },
    UniqueConflict {
        state: PendingCommandAttempts,
        lease: CommandMutationAuthority,
        snapshot: MaterializedCommandSnapshot,
    },
}

/// Closed result of catalog-owned current evidence revalidation for one fault.
pub(super) enum ExecutionFaultCurrentRecheck {
    /// Every dependency and raw physical observation remained exact.
    Stable,
    /// Catalog observed changed dependency evidence.
    DependencyChanged,
    /// Catalog evidence contradicted an independently checked attempt.
    Integrity,
}

impl ExecutionFaultAttempt {
    /// Performs the same mandatory fresh policy safe point as successful
    /// evaluation before making a deterministic failure durable.
    pub(super) fn authorize_after_evaluation(
        self,
    ) -> Result<Self, PostEvaluationAuthorizationError> {
        authorize_state_after_evaluation(self.state())?;
        Ok(self)
    }

    /// Rechecks advisory request control at the final pre-transition safe point.
    pub(super) fn recheck_request_control(&self) -> Result<(), CommandAttemptError> {
        check_request_control(self.state().deadline, &self.state().cancellation)
    }

    /// Builds the only storage transition request permitted by this exact attempt.
    pub(super) fn transition_request(&self) -> Result<ExecutionFailureTransitionRequestV1, ()> {
        let (snapshot, code) = match self {
            Self::Arithmetic { snapshot, .. } => {
                (snapshot.snapshot(), ExecutionFailureCode::ArithmeticFault)
            }
            Self::ResourceLimit {
                evidence: ResourceLimitFaultEvidence::Runtime(snapshot),
                ..
            } => (snapshot.snapshot(), ExecutionFailureCode::ResourceLimit),
            Self::ResourceLimit {
                evidence: ResourceLimitFaultEvidence::Materialization(evidence),
                ..
            } => (evidence.raw_snapshot(), ExecutionFailureCode::ResourceLimit),
            Self::UniqueConflict { snapshot, .. } => {
                (snapshot.snapshot(), ExecutionFailureCode::UniqueConflict)
            }
        };
        let state = self.state();
        let pending = state.commit_context.pending().clone();
        if state.terminal_admission {
            ExecutionFailureTransitionRequestV1::new_for_vacant_terminal_admission(
                pending,
                state.lookup_candidates.clone(),
                snapshot,
                code,
            )
        } else {
            ExecutionFailureTransitionRequestV1::new(pending, snapshot, code)
        }
        .map_err(|_| ())
    }

    /// Borrows the exact lookup identities retained by the admitted attempt.
    pub(super) const fn lookup_candidates(&self) -> &IdempotencyLookupCandidatesV1 {
        &self.state().lookup_candidates
    }

    /// Borrows the exact Pending admission retained by the attempt.
    pub(super) fn pending(&self) -> &riffdb_storage_api::StoredPendingAdmissionV1 {
        self.state().commit_context.pending()
    }

    /// Borrows the public service lifecycle retained by this attempt.
    pub(super) fn audited_lifecycle(&self) -> Option<&AuditedCommandLifecycle> {
        self.state().audited_lifecycle.as_ref()
    }

    /// Returns whether this failure must fuse its previously non-durable start.
    pub(super) const fn requires_fused_start(&self) -> bool {
        self.state().terminal_admission
    }

    /// Borrows the exact influential dependencies retained by the failed snapshot.
    pub(super) const fn read_dependencies(&self) -> &riffdb_storage_api::ReadDependencies {
        match self {
            Self::Arithmetic { snapshot, .. } => snapshot.snapshot().read_dependencies(),
            Self::ResourceLimit {
                evidence: ResourceLimitFaultEvidence::Runtime(snapshot),
                ..
            } => snapshot.snapshot().read_dependencies(),
            Self::ResourceLimit {
                evidence: ResourceLimitFaultEvidence::Materialization(evidence),
                ..
            } => evidence.raw_snapshot().read_dependencies(),
            Self::UniqueConflict { snapshot, .. } => snapshot.snapshot().read_dependencies(),
        }
    }

    /// Returns whether a concurrent command outcome belongs to this admission.
    pub(super) fn matches_outcome(&self, outcome: &StoredOutcomeV1) -> bool {
        outcome_matches_state(outcome, self.state())
    }

    /// Returns whether a durable deterministic failure belongs to this attempt.
    pub(super) fn matches_failure(&self, failure: &StoredExecutionFailedV1) -> bool {
        failure_matches_state(failure, self.state())
    }

    /// Revalidates the opaque lineage evidence only after dependencies compare equal.
    pub(super) fn recheck_equal_transaction_current(
        &self,
        current: TransactionCurrentState,
    ) -> ExecutionFaultCurrentRecheck {
        match self {
            Self::Arithmetic { snapshot, .. }
            | Self::UniqueConflict { snapshot, .. }
            | Self::ResourceLimit {
                evidence: ResourceLimitFaultEvidence::Runtime(snapshot),
                ..
            } => match snapshot.materialize_transaction_current(current) {
                Ok(TransactionCurrentMaterialization::Ready(_)) => {
                    ExecutionFaultCurrentRecheck::Stable
                }
                Ok(TransactionCurrentMaterialization::DependencyChanged) => {
                    ExecutionFaultCurrentRecheck::DependencyChanged
                }
                Err(_) => ExecutionFaultCurrentRecheck::Integrity,
            },
            Self::ResourceLimit {
                evidence: ResourceLimitFaultEvidence::Materialization(evidence),
                ..
            } => match evidence.recheck_transaction_current(current) {
                Ok(ResourceLimitRecheck::Confirmed) => ExecutionFaultCurrentRecheck::Stable,
                Ok(ResourceLimitRecheck::DependencyChanged) => {
                    ExecutionFaultCurrentRecheck::DependencyChanged
                }
                Err(_) => ExecutionFaultCurrentRecheck::Integrity,
            },
        }
    }

    /// Releases fault evidence and capability after storage proved rollback.
    pub(super) fn recover_pending_after_proven_rollback(self) -> PendingCommandAttempts {
        let (state, lease) = match self {
            Self::Arithmetic {
                state,
                lease,
                snapshot,
            } => {
                drop(snapshot);
                (state, lease)
            }
            Self::ResourceLimit {
                state,
                lease,
                evidence,
            } => {
                drop(evidence);
                (state, lease)
            }
            Self::UniqueConflict {
                state,
                lease,
                snapshot,
            } => {
                drop(snapshot);
                (state, lease)
            }
        };
        drop(lease);
        state
    }

    const fn state(&self) -> &PendingCommandAttempts {
        match self {
            Self::Arithmetic { state, .. }
            | Self::ResourceLimit { state, .. }
            | Self::UniqueConflict { state, .. } => state,
        }
    }
}

fn authorize_state_after_evaluation(
    state: &PendingCommandAttempts,
) -> Result<(), PostEvaluationAuthorizationError> {
    let Some(authorizer) = state.post_evaluation_authorizer.as_ref() else {
        // Un-audited unit fixtures and internal legacy harnesses do not
        // represent the public application path.
        if state.audited_lifecycle.is_none() {
            return Ok(());
        }
        return Err(PostEvaluationAuthorizationError::Integrity);
    };
    let authorization = authorizer.authorize()?;
    let row_policy = resolve_authorized_command_row_policy_context(
        &authorization,
        state.resolved_plan.bundle().bundle(),
    )
    .map_err(|_| PostEvaluationAuthorizationError::Integrity)?;
    if row_policy != state.row_policy {
        return Err(PostEvaluationAuthorizationError::Integrity);
    }
    let pending = state.commit_context.pending();
    let claims = authorization.provenance();
    let stored_claims = riffdb_storage_api::StoredAdmittedProvenanceClaimsV1::new(
        claims.source_repository().cloned(),
        claims.source_commit().cloned(),
        claims.reason().cloned(),
        claims.approval_id().cloned(),
    )
    .map_err(|_| PostEvaluationAuthorizationError::Integrity)?;
    let exact = authorization.lineage() == pending.plan().contract_lineage()
        && authorization.version() == pending.plan().contract_version()
        && authorization.command_id() == pending.plan().command_id()
        && authorization.class() == riffdb_policy::CommandExecutionClass::Mutation
        && authorization.partition().lineage() == pending.plan().contract_lineage()
        && authorization.partition().partition_key() == pending.partition_key()
        && authorization.actor() == pending.actor()
        && stored_claims == *pending.provenance_claims();
    if !exact {
        return Err(PostEvaluationAuthorizationError::Integrity);
    }
    Ok(())
}

impl fmt::Debug for ExecutionFaultAttempt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ExecutionFaultAttempt([REDACTED])")
    }
}

/// Failure before a valid evaluated or terminal-replay result can be released.
pub(crate) enum CommandAttemptError {
    /// Cancellation was observed at an attempt safe point.
    Cancelled,
    /// The request deadline was observed at an attempt safe point.
    DeadlineExceeded,
    /// Logical capability acquisition failed for another bounded reason.
    Conflict(ConflictError),
    /// The exact pending-admission recheck could not be read.
    PendingRecheck(StorageError),
    /// A complete owned snapshot could not be materialized.
    SnapshotRead(StorageError),
    /// Three snapshot-materialization attempt slots were already consumed.
    RetryBudgetExhausted,
    /// A containable panic escaped deterministic runtime evaluation.
    EvaluationPanicked,
    /// Independently checked semantic state was inconsistent.
    Integrity,
}

impl fmt::Debug for CommandAttemptError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CommandAttemptError([REDACTED])")
    }
}

impl fmt::Display for CommandAttemptError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("command evaluation attempt could not be completed")
    }
}

impl Error for CommandAttemptError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Conflict(error) => Some(error),
            Self::PendingRecheck(error) | Self::SnapshotRead(error) => Some(error),
            Self::Cancelled
            | Self::DeadlineExceeded
            | Self::RetryBudgetExhausted
            | Self::EvaluationPanicked
            | Self::Integrity => None,
        }
    }
}

/// Acquires, rechecks, snapshots, and synchronously evaluates one complete attempt.
///
/// The acquisition is deliberately the only await point. Once granted, the
/// move-only lease remains bundled with every value that may proceed toward a
/// durable transition and is released by drop on every replay or error path.
pub(crate) async fn evaluate_next_command_attempt(
    state: PendingCommandAttempts,
    admission: &dyn AdmissionRepository,
    snapshots: &dyn SnapshotReader,
    conflicts: &dyn ConflictManager,
) -> Result<CommandAttemptResolution, CommandAttemptError> {
    let acquire_started = crate::writer_census::stage_start();
    let acquired = acquire_command_attempt(state, conflicts).await?;
    crate::writer_census::charge(crate::writer_census::SERIAL_ACQUIRE, acquire_started);
    evaluate_acquired_command_attempt(acquired, admission, snapshots)
}

/// One uniquely owned conflict grant acquired in stable admission order.
pub(crate) struct AcquiredCommandAttempt {
    state: PendingCommandAttempts,
    lease: CommandMutationAuthority,
}

/// Sealed ownership of the conflict capability retained by one attempt.
///
/// The shared form is constructed only after every member has independently
/// supplied a compiler proof and the coordinator has acquired the canonical
/// union of their keys as one lease.  It is intentionally private and has no
/// general-purpose `Clone` implementation.
pub(crate) enum CommandMutationAuthority {
    Exclusive {
        _lease: MutationLease,
    },
    ProvenCommutativeGroup {
        _lease: Arc<ProvenCommutativeGroupLease>,
    },
    TransactionLocalSerialGroup {
        _lease: Arc<TransactionLocalSerialGroupLease>,
    },
}

pub(crate) struct ProvenCommutativeGroupLease {
    _lease: MutationLease,
}

/// Sealed union lease for FIFO transaction-local serial evaluation.
pub(crate) struct TransactionLocalSerialGroupLease {
    _lease: MutationLease,
}

impl AcquiredCommandAttempt {
    pub(crate) const fn lookup_candidates(&self) -> &IdempotencyLookupCandidatesV1 {
        &self.state.lookup_candidates
    }

    pub(crate) fn into_pending_without_evaluation(self) -> PendingCommandAttempts {
        let Self { state, lease } = self;
        drop(lease);
        state
    }

    /// Returns the exact compiler-derived snapshot request that may be read
    /// through the closed transaction-local batch protocol.
    pub(crate) fn transaction_local_snapshot_request(&self) -> SnapshotRequest {
        self.state.snapshot_request.clone()
    }

    /// Completes compiler-owned cascade discovery through the same private
    /// transaction that produced the discovery snapshot.
    pub(crate) fn complete_transaction_local_snapshot<F>(
        &mut self,
        discovery: ReadSnapshot,
        read: F,
    ) -> Result<ReadSnapshot, CommandAttemptError>
    where
        F: FnOnce(SnapshotRequest) -> Result<ReadSnapshot, StorageError>,
    {
        if !snapshot_matches_request(&self.state.snapshot_request, &discovery) {
            return Err(CommandAttemptError::Integrity);
        }
        match crate::command_index::lower_cascade_discovery(
            &self.state.resolved_plan,
            &self.state.input_facts,
            &discovery,
        )
        .map_err(|_| CommandAttemptError::Integrity)?
        {
            crate::command_index::CascadeDiscoveryDecision::Complete => Ok(discovery),
            crate::command_index::CascadeDiscoveryDecision::ReadPredecessors(request) => {
                let request = *request;
                let read_started = crate::writer_census::stage_start();
                let snapshot = read(request.clone()).map_err(CommandAttemptError::SnapshotRead)?;
                crate::writer_census::charge(
                    crate::writer_census::SERIAL_DEPENDENCY_READ,
                    read_started,
                );
                self.state.snapshot_request = request;
                Ok(snapshot)
            }
        }
    }
}

/// Acquires the compiler-derived conflict capability without reading state.
pub(crate) async fn acquire_command_attempt(
    state: PendingCommandAttempts,
    conflicts: &dyn ConflictManager,
) -> Result<AcquiredCommandAttempt, CommandAttemptError> {
    if state.completed_attempts >= MAX_COMMAND_EVALUATION_ATTEMPTS_V1 {
        return Err(CommandAttemptError::RetryBudgetExhausted);
    }
    check_request_control(state.deadline, &state.cancellation)?;

    let lease_started = crate::writer_census::stage_start();
    let lease = conflicts
        .acquire_mut(
            state.raw_conflict_keys.clone(),
            state.deadline,
            state.cancellation.clone(),
        )
        .await
        .map_err(map_conflict_error)?;
    crate::writer_census::charge(crate::writer_census::SERIAL_LEASE, lease_started);

    check_request_control(state.deadline, &state.cancellation)?;
    Ok(AcquiredCommandAttempt {
        state,
        lease: CommandMutationAuthority::Exclusive { _lease: lease },
    })
}

/// Rechecks, snapshots, and deterministically evaluates an acquired attempt.
///
/// This synchronous phase may run on a bounded preparation worker. It performs
/// no authoritative write and returns the unique lease with its checked result.
pub(crate) fn evaluate_acquired_command_attempt(
    acquired: AcquiredCommandAttempt,
    admission: &dyn AdmissionRepository,
    snapshots: &dyn SnapshotReader,
) -> Result<CommandAttemptResolution, CommandAttemptError> {
    let admission_started = crate::writer_census::stage_start();
    let durable = admission
        .lookup_admission(acquired.lookup_candidates().clone())
        .map_err(CommandAttemptError::PendingRecheck)?;
    crate::writer_census::charge(crate::writer_census::SERIAL_ADMISSION, admission_started);
    evaluate_acquired_command_attempt_after_lookup(acquired, durable, snapshots)
}

/// Finishes one acquired attempt after a bounded group lookup established the
/// exact durable admission state for every member under its retained conflict
/// capability.
pub(crate) fn evaluate_acquired_command_attempt_after_lookup(
    acquired: AcquiredCommandAttempt,
    durable: AdmissionLookupResultV1,
    snapshots: &dyn SnapshotReader,
) -> Result<CommandAttemptResolution, CommandAttemptError> {
    let (state, lease) = match lower_acquired_admission(acquired, durable)? {
        AcquiredAdmissionDecision::Continue { state, lease } => (state, lease),
        AcquiredAdmissionDecision::Complete(resolution) => return Ok(resolution),
    };
    check_request_control(state.deadline, &state.cancellation)?;

    let snapshot_started = crate::writer_census::stage_start();
    let raw_snapshot = snapshots
        .read_snapshot(state.snapshot_request.clone())
        .map_err(CommandAttemptError::SnapshotRead)?;
    crate::writer_census::charge(crate::writer_census::SERIAL_SNAPSHOT, snapshot_started);
    finish_acquired_discovery(state, lease, raw_snapshot, snapshots)
}

/// Finishes one acquired attempt from a snapshot materialized in the same
/// bounded backend snapshot group as its FIFO peers.
pub(crate) fn evaluate_acquired_command_attempt_after_lookup_and_snapshot(
    acquired: AcquiredCommandAttempt,
    durable: AdmissionLookupResultV1,
    raw_snapshot: ReadSnapshot,
    snapshots: &dyn SnapshotReader,
) -> Result<CommandAttemptResolution, CommandAttemptError> {
    let (state, lease) = match lower_acquired_admission(acquired, durable)? {
        AcquiredAdmissionDecision::Continue { state, lease } => (state, lease),
        AcquiredAdmissionDecision::Complete(resolution) => return Ok(resolution),
    };
    check_request_control(state.deadline, &state.cancellation)?;
    finish_acquired_discovery(state, lease, raw_snapshot, snapshots)
}

fn finish_acquired_discovery(
    mut state: PendingCommandAttempts,
    lease: CommandMutationAuthority,
    discovery: ReadSnapshot,
    snapshots: &dyn SnapshotReader,
) -> Result<CommandAttemptResolution, CommandAttemptError> {
    if !snapshot_matches_request(&state.snapshot_request, &discovery) {
        return Err(CommandAttemptError::Integrity);
    }
    match crate::command_index::lower_cascade_discovery(
        &state.resolved_plan,
        &state.input_facts,
        &discovery,
    )
        .map_err(|_| CommandAttemptError::Integrity)?
    {
        crate::command_index::CascadeDiscoveryDecision::Complete => {
            finish_acquired_evaluation(state, lease, discovery)
        }
        crate::command_index::CascadeDiscoveryDecision::ReadPredecessors(request) => {
            let request = *request;
            check_request_control(state.deadline, &state.cancellation)?;
            let snapshot = snapshots
                .read_snapshot(request.clone())
                .map_err(CommandAttemptError::SnapshotRead)?;
            state.snapshot_request = request;
            finish_acquired_evaluation(state, lease, snapshot)
        }
    }
}

#[allow(clippy::large_enum_variant)] // Move-only capability state stays inline on the hot path.
enum AcquiredAdmissionDecision {
    Continue {
        state: PendingCommandAttempts,
        lease: CommandMutationAuthority,
    },
    Complete(CommandAttemptResolution),
}

fn lower_acquired_admission(
    acquired: AcquiredCommandAttempt,
    durable: AdmissionLookupResultV1,
) -> Result<AcquiredAdmissionDecision, CommandAttemptError> {
    let AcquiredCommandAttempt { state, lease } = acquired;
    match durable {
        AdmissionLookupResultV1::Found(found) => match *found {
            StoredAdmissionStateV1::Pending(pending)
                if !state.terminal_admission && pending == *state.commit_context.pending() => {}
            StoredAdmissionStateV1::StoredOutcome(outcome)
                if outcome_matches_state(&outcome, &state) =>
            {
                return Ok(AcquiredAdmissionDecision::Complete(
                    CommandAttemptResolution::OutcomeReplay(outcome),
                ));
            }
            StoredAdmissionStateV1::ExecutionFailed(failure)
                if failure_matches_state(&failure, &state) =>
            {
                return Ok(AcquiredAdmissionDecision::Complete(
                    CommandAttemptResolution::ExecutionFailureReplay(failure),
                ));
            }
            _ => return Err(CommandAttemptError::Integrity),
        },
        AdmissionLookupResultV1::NotFound if state.terminal_admission => {}
        AdmissionLookupResultV1::NotFound | AdmissionLookupResultV1::MultipleMatches => {
            return Err(CommandAttemptError::Integrity);
        }
    }
    Ok(AcquiredAdmissionDecision::Continue { state, lease })
}

/// Deterministically evaluates a fresh synchronous attempt from the exact
/// snapshot read through its private serial-batch transaction.
pub(crate) fn evaluate_transaction_local_acquired_command_attempt(
    acquired: AcquiredCommandAttempt,
    raw_snapshot: ReadSnapshot,
) -> Result<CommandAttemptResolution, CommandAttemptError> {
    let AcquiredCommandAttempt { state, lease } = acquired;
    if !state.terminal_admission {
        return Err(CommandAttemptError::Integrity);
    }
    finish_acquired_evaluation(state, lease, raw_snapshot)
}

fn finish_acquired_evaluation(
    mut state: PendingCommandAttempts,
    lease: CommandMutationAuthority,
    raw_snapshot: ReadSnapshot,
) -> Result<CommandAttemptResolution, CommandAttemptError> {
    check_request_control(state.deadline, &state.cancellation)?;
    if !snapshot_matches_request(&state.snapshot_request, &raw_snapshot) {
        return Err(CommandAttemptError::Integrity);
    }

    state.completed_attempts = state
        .completed_attempts
        .checked_add(1)
        .ok_or(CommandAttemptError::Integrity)?;
    let materialization = state
        .resolved_plan
        .clone()
        .materialize_command_snapshot_for_input(
            &state.normalized_input,
            &state.input_facts,
            raw_snapshot,
        )
        .map_err(|_| CommandAttemptError::Integrity)?;
    let snapshot = match materialization {
        CommandSnapshotMaterialization::Ready(snapshot) => snapshot,
        CommandSnapshotMaterialization::ResourceLimit(evidence) => {
            check_request_control(state.deadline, &state.cancellation)?;
            return Ok(CommandAttemptResolution::ExecutionFault(
                ExecutionFaultAttempt::ResourceLimit {
                    state,
                    lease,
                    evidence: ResourceLimitFaultEvidence::Materialization(evidence),
                },
            ));
        }
    };

    let context = transaction_context(&state.commit_context);
    let evaluate_started = crate::writer_census::stage_start();
    let execution = catch_unwind(AssertUnwindSafe(|| {
        execute_command_with_facts(
            snapshot.resolved_plan().bundle().bundle(),
            &state.normalized_input,
            &state.input_facts,
            snapshot.snapshot(),
            &context,
            state.commit_context.evaluation_budget(),
        )
    }))
    .map_err(|_| CommandAttemptError::EvaluationPanicked)?;
    crate::writer_census::charge(crate::writer_census::SERIAL_EVALUATE, evaluate_started);

    let resolution = match execution {
        Ok(ExecutionResult::CommitRequired(evaluated)) => {
            check_request_control(state.deadline, &state.cancellation)?;
            CommandAttemptResolution::Evaluated(EvaluatedCommandAttempt {
                state,
                lease,
                snapshot,
                evaluated: Arc::new(evaluated),
                prepared_body: None,
            })
        }
        Err(ExecutionFault::Arithmetic) => {
            check_request_control(state.deadline, &state.cancellation)?;
            CommandAttemptResolution::ExecutionFault(ExecutionFaultAttempt::Arithmetic {
                state,
                lease,
                snapshot,
            })
        }
        Err(ExecutionFault::ResourceLimit) => {
            check_request_control(state.deadline, &state.cancellation)?;
            CommandAttemptResolution::ExecutionFault(ExecutionFaultAttempt::ResourceLimit {
                state,
                lease,
                evidence: ResourceLimitFaultEvidence::Runtime(snapshot),
            })
        }
        Ok(ExecutionResult::ReadOnly(_)) | Err(ExecutionFault::Integrity) => {
            return Err(CommandAttemptError::Integrity);
        }
    };
    Ok(resolution)
}

/// Acquires one canonical capability for a compiler-proved commutative group.
///
/// Callers must already have rejected exact read/write and write/write overlap.
/// This function repeats the semantic proof before creating the only shared
/// ownership form accepted by command evaluation.
pub(crate) async fn acquire_commutative_command_group(
    states: Vec<PendingCommandAttempts>,
    conflicts: &dyn ConflictManager,
) -> Result<Vec<AcquiredCommandAttempt>, (Vec<PendingCommandAttempts>, CommandAttemptError)> {
    if states.len() < 2
        || states
            .iter()
            .any(|state| state.commutative_child_append_proof().is_none())
    {
        return Err((states, CommandAttemptError::Integrity));
    }
    if let Some(error) = states.iter().find_map(|state| {
        if state.completed_attempts >= MAX_COMMAND_EVALUATION_ATTEMPTS_V1 {
            Some(CommandAttemptError::RetryBudgetExhausted)
        } else {
            check_request_control(state.deadline, &state.cancellation).err()
        }
    }) {
        return Err((states, error));
    }

    let mut keys = states
        .iter()
        .flat_map(|state| state.raw_conflict_keys.iter().cloned())
        .collect::<Vec<_>>();
    keys.sort_unstable();
    keys.dedup();
    let Some(first) = states.first() else {
        return Err((states, CommandAttemptError::Integrity));
    };
    let deadline = states
        .iter()
        .map(|state| state.deadline)
        .min()
        .unwrap_or(first.deadline);
    let cancellation = first.cancellation.clone();
    let lease = match conflicts.acquire_mut(keys, deadline, cancellation).await {
        Ok(lease) => lease,
        Err(error) => return Err((states, map_conflict_error(error))),
    };
    if let Some(error) = states
        .iter()
        .find_map(|state| check_request_control(state.deadline, &state.cancellation).err())
    {
        drop(lease);
        return Err((states, error));
    }

    let authority = Arc::new(ProvenCommutativeGroupLease { _lease: lease });
    Ok(states
        .into_iter()
        .map(|state| AcquiredCommandAttempt {
            state,
            lease: CommandMutationAuthority::ProvenCommutativeGroup {
                _lease: Arc::clone(&authority),
            },
        })
        .collect())
}

/// Acquires one canonical union capability for FIFO serial micro-batching.
///
/// Unlike the commutative path, overlapping keys are allowed because every
/// item is evaluated against the transaction state staged by its predecessors.
pub(crate) async fn acquire_transaction_local_serial_group(
    states: Vec<PendingCommandAttempts>,
    conflicts: &dyn ConflictManager,
) -> Result<Vec<AcquiredCommandAttempt>, (Vec<PendingCommandAttempts>, CommandAttemptError)> {
    if states.len() < 2
        || states
            .iter()
            .any(|state| !state.serial_micro_batch_eligible())
    {
        return Err((states, CommandAttemptError::Integrity));
    }
    acquire_transaction_local_fifo_authority(states, conflicts).await
}

/// Acquires the FIFO writer authority for successors of an unpublished root.
///
/// Unlike a fresh serial micro-batch, this path also admits one command and a
/// resumable command whose `Started` audit is already durable. The sole writer
/// has already selected a private successor frontier, so every member must use
/// transaction-local evaluation; falling back to the public snapshot would be
/// stale by construction.
pub(crate) async fn acquire_writer_private_fifo_group(
    states: Vec<PendingCommandAttempts>,
    conflicts: &dyn ConflictManager,
) -> Result<Vec<AcquiredCommandAttempt>, (Vec<PendingCommandAttempts>, CommandAttemptError)> {
    if states.is_empty() || states.iter().any(|state| !state.has_audited_lifecycle()) {
        return Err((states, CommandAttemptError::Integrity));
    }
    acquire_transaction_local_fifo_authority(states, conflicts).await
}

async fn acquire_transaction_local_fifo_authority(
    states: Vec<PendingCommandAttempts>,
    conflicts: &dyn ConflictManager,
) -> Result<Vec<AcquiredCommandAttempt>, (Vec<PendingCommandAttempts>, CommandAttemptError)> {
    if let Some(error) = states.iter().find_map(|state| {
        if state.completed_attempts >= MAX_COMMAND_EVALUATION_ATTEMPTS_V1 {
            Some(CommandAttemptError::RetryBudgetExhausted)
        } else {
            check_request_control(state.deadline, &state.cancellation).err()
        }
    }) {
        return Err((states, error));
    }

    let mut keys = states
        .iter()
        .flat_map(|state| state.raw_conflict_keys.iter().cloned())
        .collect::<Vec<_>>();
    keys.sort_unstable();
    keys.dedup();
    let Some(first) = states.first() else {
        return Err((states, CommandAttemptError::Integrity));
    };
    let deadline = states
        .iter()
        .map(|state| state.deadline)
        .min()
        .unwrap_or(first.deadline);
    let cancellation = first.cancellation.clone();
    let lease = match conflicts.acquire_mut(keys, deadline, cancellation).await {
        Ok(lease) => lease,
        Err(error) => return Err((states, map_conflict_error(error))),
    };
    if let Some(error) = states
        .iter()
        .find_map(|state| check_request_control(state.deadline, &state.cancellation).err())
    {
        drop(lease);
        return Err((states, error));
    }

    let authority = Arc::new(TransactionLocalSerialGroupLease { _lease: lease });
    Ok(states
        .into_iter()
        .map(|state| AcquiredCommandAttempt {
            state,
            lease: CommandMutationAuthority::TransactionLocalSerialGroup {
                _lease: Arc::clone(&authority),
            },
        })
        .collect())
}

fn transaction_context(context: &PreEvaluationCommitContext) -> TransactionContext {
    let pending = context.pending();
    TransactionContext::new_with_service_values(
        pending.admission_request_id(),
        pending.actor().clone(),
        pending.plan().clone(),
        pending.logical_time(),
        pending.partition_key().clone(),
        pending.service_values().clone(),
    )
}

fn snapshot_matches_request(request: &SnapshotRequest, snapshot: &ReadSnapshot) -> bool {
    snapshot.plan() == request.plan()
        && snapshot.bindings().len() == request.binding_targets().len()
        && snapshot
            .bindings()
            .iter()
            .zip(request.binding_targets())
            .all(|(observation, target)| observation.target() == target)
        && snapshot.root_validations().len() == request.root_validation_targets().len()
        && snapshot
            .root_validations()
            .iter()
            .zip(request.root_validation_targets())
            .all(|(observation, target)| observation.target() == target)
        && snapshot.cascade_predecessors().len() == request.cascade_targets().len()
        && snapshot
            .cascade_predecessors()
            .iter()
            .zip(request.cascade_targets())
            .all(|(observation, target)| observation.target() == target)
        && snapshot.ranges().len() == request.range_targets().len()
        && snapshot
            .ranges()
            .iter()
            .zip(request.range_targets())
            .all(|(observation, target)| observation.target() == target)
}

fn check_request_control(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), CommandAttemptError> {
    if cancellation.is_cancelled() {
        return Err(CommandAttemptError::Cancelled);
    }
    if Instant::now() >= deadline {
        return Err(CommandAttemptError::DeadlineExceeded);
    }
    Ok(())
}

fn map_conflict_error(error: ConflictError) -> CommandAttemptError {
    match error {
        ConflictError::Cancelled => CommandAttemptError::Cancelled,
        ConflictError::DeadlineExceeded => CommandAttemptError::DeadlineExceeded,
        other => CommandAttemptError::Conflict(other),
    }
}

fn outcome_matches_context(
    outcome: &StoredOutcomeV1,
    context: &PreEvaluationCommitContext,
) -> bool {
    let pending = context.pending();
    outcome.identity() == pending.identity()
        && outcome.admission_request_id() == pending.admission_request_id()
        && outcome.plan() == pending.plan()
        && outcome.canonical_input_hash() == pending.canonical_input_hash()
        && outcome.actor() == pending.actor()
        && outcome.logical_time() == pending.logical_time()
        && outcome.partition_key() == pending.partition_key()
        && outcome.partition_hash() == context.partition_hash()
        && outcome.conflict_hashes() == context.conflict_hashes()
        && outcome.admitted_claims() == pending.provenance_claims()
}

fn outcome_matches_state(outcome: &StoredOutcomeV1, state: &PendingCommandAttempts) -> bool {
    if !state.terminal_admission {
        return outcome_matches_context(outcome, &state.commit_context);
    }
    let pending = state.commit_context.pending();
    state.lookup_candidates.contains(outcome.identity())
        && outcome.plan() == pending.plan()
        && outcome.canonical_input_hash() == pending.canonical_input_hash()
        && outcome.actor() == pending.actor()
        && outcome.partition_key() == pending.partition_key()
        && outcome.partition_hash() == state.commit_context.partition_hash()
        && outcome.conflict_hashes() == state.commit_context.conflict_hashes()
        && outcome.admitted_claims() == pending.provenance_claims()
}

fn failure_matches_state(
    failure: &StoredExecutionFailedV1,
    state: &PendingCommandAttempts,
) -> bool {
    if !state.terminal_admission {
        return failure.pending() == state.commit_context.pending();
    }
    let pending = state.commit_context.pending();
    state
        .lookup_candidates
        .contains(failure.pending().identity())
        && failure.pending().plan() == pending.plan()
        && failure.pending().canonical_input_hash() == pending.canonical_input_hash()
        && failure.pending().actor() == pending.actor()
        && failure.pending().partition_key() == pending.partition_key()
        && failure.pending().provenance_claims() == pending.provenance_claims()
}

#[cfg(test)]
impl EvaluatedCommandAttempt {
    /// Releases an attempt without storage only for fixed-ceiling unit tests.
    fn into_retry_state_for_test(self) -> PendingCommandAttempts {
        self.state
    }
}

#[cfg(test)]
impl ProvenanceBoundCommandAttempt {
    /// Enters the post-rollback disposition only for focused ownership tests.
    fn after_proven_rollback_for_test(
        self,
        reason: CandidateValidationRejection,
    ) -> RolledBackCandidateDisposition {
        self.finish_after_candidate_rollback(true, reason)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::{Cell, RefCell},
        collections::BTreeMap,
        marker::PhantomData,
        rc::Rc,
        task::{Context, Poll, Waker},
        time::Duration,
    };

    use riffdb_catalog::{ValidatedContractBundle, resolve_executable_plan};
    use riffdb_conflict::{ConflictManagerConfig, ShardedConflictManager};
    use riffdb_contract_compiler::{compile_contract_source, compile_contract_successor};
    use riffdb_contract_ir::{CommandPlan, RecordSchema};
    use riffdb_invariant::derive_input_command_facts;

    use riffdb_storage_api::{
        AbandonedCandidate, ActiveCatalogPointerV1, AdmissionRequestV1, AdmissionResultV1,
        AffectedEpochCurrentState, AffectedIndexEpochTargets, ApplicationCommandTransactionPort,
        AtomicCommandRecordSet, CandidateAdmissionResult, CandidateCapacityResult,
        CandidateStartResult, CatalogRepository, CommandCandidateAdmission,
        CommandCandidateAffectedEpochRead, CommandCandidateAwaitingCapacity,
        CommandCandidateAwaitingValidation, CommandCandidateCapacityReserved,
        CommandCandidateSequenceAssigned, CommandCandidateStateRead, CommandWriteSetPlanV1,
        CommitIntent, CommittedBatchV1, DeclaredOutcome, DurabilityMode, DurableKeySchemaBindingV1,
        EmptyCommandBatch, EntityObservation, EntityTarget, ExecutablePlanRef,
        ExecutionFailureAdmissionRechecked, ExecutionFailureAdmissionResult,
        ExecutionFailureAwaitingDecision, ExecutionFailureTransitionPort,
        ExecutionFailureTransitionRequestV1, IdempotencyIdentity, IdempotencyKeyDigest,
        NonEmptyCommandBatch, StagedBatchMetrics, StorageErrorKind,
        StoredAdmittedProvenanceClaimsV1, StoredContractBundleV1, StoredEntityRecordV1,
        StoredPendingAdmissionV1, TransactionCurrentState,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdmittedActorContext, AggregateTypeId, CanonicalInputHash,
        CanonicalRecord, CanonicalValue, CommandId, CommitSequence, ConflictKeyHash,
        ContractBundleHash, ContractLineage, ContractVersion, DatabaseId, Decimal, DecimalSpec,
        DigestKeyId, EntityVersion, Environment, ExecutionFailureCode, FieldId, LogicalTime,
        MAX_CANONICAL_DOCUMENT_BYTES, OutcomeId, PartitionKeyBuilder, PlanHash, ProvenanceId,
        TenantScope, Timestamp, encode_canonical_record, hash_conflict_key, hash_partition_key,
    };

    use super::*;

    const PRINCIPAL: &str = "attempt-test-principal";
    const SENSITIVE_MARKER: &str = "attempt-test-sensitive";

    struct ScriptedRepository {
        expected: IdempotencyLookupCandidatesV1,
        result: AdmissionLookupResultV1,
        calls: Cell<usize>,
        order: Rc<RefCell<Vec<&'static str>>>,
        cancel_during_lookup: Option<CancellationToken>,
    }

    impl ScriptedRepository {
        fn new(
            expected: IdempotencyLookupCandidatesV1,
            result: AdmissionLookupResultV1,
            order: Rc<RefCell<Vec<&'static str>>>,
        ) -> Self {
            Self {
                expected,
                result,
                calls: Cell::new(0),
                order,
                cancel_during_lookup: None,
            }
        }

        fn cancelling_during_lookup(mut self, cancellation: CancellationToken) -> Self {
            self.cancel_during_lookup = Some(cancellation);
            self
        }
    }

    impl AdmissionRepository for ScriptedRepository {
        fn admit_or_resolve(
            &self,
            _request: AdmissionRequestV1,
        ) -> Result<AdmissionResultV1, StorageError> {
            panic!("attempt recheck must never create admission")
        }

        fn lookup_admission(
            &self,
            candidates: IdempotencyLookupCandidatesV1,
        ) -> Result<AdmissionLookupResultV1, StorageError> {
            assert_eq!(candidates, self.expected);
            self.calls.set(self.calls.get() + 1);
            self.order.borrow_mut().push("pending-recheck");
            if let Some(cancellation) = &self.cancel_during_lookup {
                cancellation.cancel();
            }
            Ok(self.result.clone())
        }
    }

    struct ScriptedSnapshotReader {
        expected: SnapshotRequest,
        snapshot: ReadSnapshot,
        cancel_during_read: Option<CancellationToken>,
        calls: Cell<usize>,
        order: Rc<RefCell<Vec<&'static str>>>,
    }

    impl ScriptedSnapshotReader {
        fn new(
            expected: SnapshotRequest,
            snapshot: ReadSnapshot,
            order: Rc<RefCell<Vec<&'static str>>>,
        ) -> Self {
            Self {
                expected,
                snapshot,
                cancel_during_read: None,
                calls: Cell::new(0),
                order,
            }
        }

        fn cancelling(mut self, cancellation: CancellationToken) -> Self {
            self.cancel_during_read = Some(cancellation);
            self
        }
    }

    impl SnapshotReader for ScriptedSnapshotReader {
        fn read_snapshot(&self, request: SnapshotRequest) -> Result<ReadSnapshot, StorageError> {
            assert!(request == self.expected, "attempt changed snapshot request");
            self.calls.set(self.calls.get() + 1);
            self.order.borrow_mut().push("snapshot");
            if let Some(cancellation) = &self.cancel_during_read {
                cancellation.cancel();
            }
            Ok(self.snapshot.clone())
        }
    }

    struct LineageRepository {
        active: ActiveCatalogPointerV1,
        bundles: Vec<StoredContractBundleV1>,
    }

    impl CatalogRepository for LineageRepository {
        fn read_active_catalog(&self) -> Result<Option<ActiveCatalogPointerV1>, StorageError> {
            Ok(Some(self.active.clone()))
        }

        fn read_contract_bundle(
            &self,
            lineage: &ContractLineage,
            contract_version: ContractVersion,
        ) -> Result<Option<StoredContractBundleV1>, StorageError> {
            Ok(self
                .bundles
                .iter()
                .find(|bundle| {
                    bundle.lineage() == lineage && bundle.contract_version() == contract_version
                })
                .cloned())
        }
    }

    fn manager() -> ShardedConflictManager {
        ShardedConflictManager::new(ConflictManagerConfig::default()).expect("conflict manager")
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("current-thread runtime")
    }

    fn future_deadline() -> Instant {
        Instant::now()
            .checked_add(Duration::from_secs(30))
            .expect("future deadline")
    }

    fn decimal(coefficient: i128) -> CanonicalValue {
        CanonicalValue::Decimal(
            Decimal::new(DecimalSpec::new(28, 2).expect("decimal spec"), coefficient)
                .expect("bounded decimal"),
        )
    }

    fn input_record<const N: usize>(
        schema: &RecordSchema,
        fields: [(&str, CanonicalValue); N],
    ) -> CanonicalRecord {
        let by_name = fields.into_iter().collect::<BTreeMap<_, _>>();
        CanonicalRecord::new(
            schema
                .fields()
                .iter()
                .map(|field| {
                    (
                        field.id(),
                        by_name
                            .get(field.name())
                            .unwrap_or_else(|| panic!("missing field {}", field.name()))
                            .clone(),
                    )
                })
                .collect(),
        )
        .expect("canonical input")
    }

    fn create_budget_input(plan: &CommandPlan) -> CanonicalRecord {
        input_record(
            plan.input().record(),
            [
                (
                    "idempotency_key",
                    CanonicalValue::string(SENSITIVE_MARKER).expect("bounded caller key"),
                ),
                ("organization_id", CanonicalValue::Uuid([0x41; 16])),
                ("fiscal_year", CanonicalValue::I64(2028)),
                ("approved_amount", decimal(25_000)),
            ],
        )
    }

    fn materialization_input(plan: &CommandPlan) -> CanonicalRecord {
        input_record(
            plan.input().record(),
            [
                (
                    "idempotency_key",
                    CanonicalValue::string(SENSITIVE_MARKER).expect("bounded caller key"),
                ),
                ("tenant", CanonicalValue::Uuid([0x41; 16])),
                ("id", CanonicalValue::Uuid([0x42; 16])),
            ],
        )
    }

    fn materialization_source(version: u64, optional_note: bool) -> String {
        let note = if optional_note {
            "    field note: optional<string<8>>\n"
        } else {
            ""
        };
        format!(
            r#"
contract AttemptMaterialization version {version} {{
  entity Row {{
    key (tenant: uuid, id: uuid)
    field value: i64
{note}  }}

  aggregate Rows {{
    root Row
    partition_by tenant
    conflict_key (tenant)
  }}

  command Increment {{
    input idempotency_key: string<128>
    input tenant: uuid
    input id: uuid

    idempotency_key idempotency_key
    mutate Row(tenant, id) as row
      else Missing {{ id: id }}

    set row.value = row.value + 1
    return Updated {{ value: row.value }}
  }}
}}
"#
        )
    }

    fn pending_attempt(
        resolved_plan: ResolvedExecutablePlan,
        normalized_input: CanonicalRecord,
        snapshot_request: SnapshotRequest,
    ) -> PendingCommandAttempts {
        let facts = derive_input_command_facts(resolved_plan.plan(), normalized_input.clone())
            .expect("input-derived command facts");
        let reference = resolved_plan.reference().clone();
        let actor = AdmittedActorContext::new(
            ActorId::new(PRINCIPAL).expect("principal"),
            ActorKind::Service,
            TenantScope::Global,
            None,
        );
        let identity = IdempotencyIdentity::new(
            database_id(),
            Environment::new("development").expect("environment"),
            TenantScope::Global,
            ActorId::new(PRINCIPAL).expect("principal"),
            reference.contract_lineage().clone(),
            reference.command_id(),
            IdempotencyKeyDigest::from_hmac_bytes(
                DigestKeyId::new(1).expect("digest key ID"),
                [0x51; 32],
            ),
        );
        let pending = StoredPendingAdmissionV1::new(
            identity,
            CanonicalInputHash::from_bytes([0x61; 32]),
            request_id(1),
            reference,
            LogicalTime::new(Timestamp::new(100, 17).expect("timestamp")),
            actor,
            facts.partition_key().clone(),
            StoredAdmittedProvenanceClaimsV1::default(),
        )
        .expect("pending admission");
        let mut raw_conflict_keys = facts.declared_conflict_keys().to_vec();
        raw_conflict_keys.sort_unstable();
        raw_conflict_keys.dedup();
        let mut conflict_hashes = raw_conflict_keys
            .iter()
            .map(|key| hash_conflict_key(key.as_bytes()))
            .collect::<Vec<_>>();
        conflict_hashes.sort_unstable();
        let commit_context = PreEvaluationCommitContext::new(
            pending,
            hash_partition_key(facts.partition_key().as_bytes()),
            conflict_hashes,
        )
        .expect("pre-evaluation context");
        let lookup_candidates =
            IdempotencyLookupCandidatesV1::new(vec![commit_context.pending().identity().clone()])
                .expect("singleton lookup");
        let binding_modes = facts
            .binding_plan_indices()
            .iter()
            .map(|index| resolved_plan.plan().bindings()[*index as usize].mode())
            .collect();

        PendingCommandAttempts {
            resolved_plan,
            normalized_input,
            input_facts: facts,
            commit_context,
            raw_conflict_keys,
            snapshot_request,
            binding_modes,
            lookup_candidates,
            invocation_request_id: request_id(2),
            deadline: future_deadline(),
            cancellation: CancellationToken::new(),
            audited_lifecycle: None,
            terminal_admission: false,
            post_evaluation_authorizer: None,
            row_policy: None,
            completed_attempts: 0,
        }
    }

    fn execution_fixture() -> (PendingCommandAttempts, ReadSnapshot) {
        let bundle = ValidatedContractBundle::decode(include_bytes!(
            "../../../fixtures/compiler/bundle.bin"
        ))
        .expect("checked compiler fixture");
        let plan = bundle
            .bundle()
            .commands()
            .iter()
            .find(|plan| plan.name() == "CreateBudget")
            .expect("CreateBudget plan");
        let reference = ExecutablePlanRef::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            plan.command_id(),
            plan.plan_hash(),
        );
        let normalized_input = create_budget_input(plan);
        let facts = derive_input_command_facts(plan, normalized_input.clone())
            .expect("input-derived command facts");

        let binding_targets = plan
            .bindings()
            .iter()
            .map(|binding| binding.entity_type())
            .zip(facts.binding_entity_keys().iter().cloned())
            .map(|(entity_type, key)| EntityTarget::new(entity_type, key))
            .collect::<Result<Vec<_>, _>>()
            .expect("binding targets");
        let root_targets = plan
            .root_validation_reads()
            .iter()
            .map(|read| read.entity_type())
            .zip(facts.root_validation_entity_keys().iter().cloned())
            .map(|(entity_type, key)| EntityTarget::new(entity_type, key))
            .collect::<Result<Vec<_>, _>>()
            .expect("root targets");
        let snapshot_request =
            SnapshotRequest::new(reference.clone(), binding_targets, root_targets, Vec::new())
                .expect("snapshot request");
        let snapshot = ReadSnapshot::new(
            &snapshot_request,
            None,
            snapshot_request
                .binding_targets()
                .iter()
                .cloned()
                .map(EntityObservation::Absent)
                .collect(),
            snapshot_request
                .root_validation_targets()
                .iter()
                .cloned()
                .map(EntityObservation::Absent)
                .collect(),
            Vec::new(),
        )
        .expect("complete owned snapshot");

        let actor = AdmittedActorContext::new(
            ActorId::new(PRINCIPAL).expect("principal"),
            ActorKind::Service,
            TenantScope::Global,
            None,
        );
        let identity = IdempotencyIdentity::new(
            database_id(),
            Environment::new("development").expect("environment"),
            TenantScope::Global,
            ActorId::new(PRINCIPAL).expect("principal"),
            reference.contract_lineage().clone(),
            reference.command_id(),
            IdempotencyKeyDigest::from_hmac_bytes(
                DigestKeyId::new(1).expect("digest key ID"),
                [0x51; 32],
            ),
        );
        let pending = StoredPendingAdmissionV1::new(
            identity,
            CanonicalInputHash::from_bytes([0x61; 32]),
            request_id(1),
            reference,
            LogicalTime::new(Timestamp::new(100, 17).expect("timestamp")),
            actor,
            facts.partition_key().clone(),
            StoredAdmittedProvenanceClaimsV1::default(),
        )
        .expect("pending admission");
        let mut raw_conflict_keys = facts.declared_conflict_keys().to_vec();
        raw_conflict_keys.sort_unstable();
        raw_conflict_keys.dedup();
        let mut conflict_hashes = raw_conflict_keys
            .iter()
            .map(|key| hash_conflict_key(key.as_bytes()))
            .collect::<Vec<_>>();
        conflict_hashes.sort_unstable();
        let commit_context = PreEvaluationCommitContext::new(
            pending,
            hash_partition_key(facts.partition_key().as_bytes()),
            conflict_hashes,
        )
        .expect("pre-evaluation context");
        let lookup_candidates =
            IdempotencyLookupCandidatesV1::new(vec![commit_context.pending().identity().clone()])
                .expect("singleton lookup");
        let resolved_plan =
            crate::test_support::resolve_genesis_plan(&bundle, commit_context.pending().plan())
                .expect("exact checked plan");
        let binding_modes = facts
            .binding_plan_indices()
            .iter()
            .map(|index| resolved_plan.plan().bindings()[*index as usize].mode())
            .collect();

        (
            PendingCommandAttempts {
                resolved_plan,
                normalized_input,
                input_facts: facts,
                commit_context,
                raw_conflict_keys,
                snapshot_request,
                binding_modes,
                lookup_candidates,
                invocation_request_id: request_id(2),
                deadline: future_deadline(),
                cancellation: CancellationToken::new(),
                audited_lifecycle: None,
                terminal_admission: false,
                post_evaluation_authorizer: None,
                row_policy: None,
                completed_attempts: 0,
            },
            snapshot,
        )
    }

    fn evolved_row_fixture(
        desired_raw_record_bytes: Option<usize>,
    ) -> (PendingCommandAttempts, ReadSnapshot, FieldId) {
        let genesis_compiled =
            compile_contract_source(&materialization_source(1, false)).expect("genesis compiles");
        let successor_source = materialization_source(2, true);
        let successor_compiled = compile_contract_successor(&successor_source, &genesis_compiled)
            .expect("optional-field successor compiles");
        let genesis = ValidatedContractBundle::from_compiler_bundle(genesis_compiled)
            .expect("checked genesis");
        let successor = ValidatedContractBundle::from_compiler_bundle(successor_compiled)
            .expect("checked successor");
        let command = successor
            .bundle()
            .commands()
            .iter()
            .find(|plan| plan.name() == "Increment")
            .expect("Increment plan");
        let reference = ExecutablePlanRef::new(
            successor.lineage().clone(),
            successor.contract_version(),
            successor.bundle_hash(),
            command.command_id(),
            command.plan_hash(),
        );
        let tip = successor.to_stored().expect("stored successor");
        let repository = LineageRepository {
            active: ActiveCatalogPointerV1::from_bundle(&tip),
            bundles: vec![genesis.to_stored().expect("stored genesis"), tip],
        };
        let resolved_plan =
            resolve_executable_plan(&repository, &reference).expect("resolved successor plan");
        let input = materialization_input(resolved_plan.plan());
        let facts = derive_input_command_facts(resolved_plan.plan(), input.clone())
            .expect("allocation facts");
        let binding_targets = resolved_plan
            .plan()
            .bindings()
            .iter()
            .map(|binding| binding.entity_type())
            .zip(facts.binding_entity_keys().iter().cloned())
            .map(|(entity_type, key)| EntityTarget::new(entity_type, key))
            .collect::<Result<Vec<_>, _>>()
            .expect("binding targets");
        let root_targets = resolved_plan
            .plan()
            .root_validation_reads()
            .iter()
            .map(|read| read.entity_type())
            .zip(facts.root_validation_entity_keys().iter().cloned())
            .map(|(entity_type, key)| EntityTarget::new(entity_type, key))
            .collect::<Result<Vec<_>, _>>()
            .expect("root targets");
        let request = SnapshotRequest::new(
            reference,
            binding_targets.clone(),
            root_targets.clone(),
            Vec::new(),
        )
        .expect("snapshot request");
        let target = binding_targets.first().expect("one binding").clone();
        let entity = genesis
            .bundle()
            .schema()
            .entity(target.entity_type_id())
            .expect("Row schema");
        let key_values = entity
            .primary_key()
            .decode_entity(target.key())
            .expect("Row key");
        let base_fields = entity
            .record()
            .fields()
            .iter()
            .map(|field| {
                let value = match field.name() {
                    "tenant" => key_values[0].clone(),
                    "id" => key_values[1].clone(),
                    "value" => CanonicalValue::I64(41),
                    unexpected => panic!("unexpected genesis Row field {unexpected}"),
                };
                (field.id(), value)
            })
            .collect::<Vec<_>>();
        let unknown_field = FieldId::new(u32::MAX).expect("unknown field ID");
        assert!(
            entity
                .record()
                .fields()
                .iter()
                .all(|field| field.id() != unknown_field)
        );
        let fields = if let Some(desired_bytes) = desired_raw_record_bytes {
            let mut empty = base_fields.clone();
            empty.push((
                unknown_field,
                CanonicalValue::bytes(Vec::new()).expect("empty unknown payload"),
            ));
            let empty = CanonicalRecord::new(empty).expect("empty-payload record");
            let payload_bytes = desired_bytes
                .checked_sub(
                    encode_canonical_record(&empty)
                        .expect("empty-payload encoding")
                        .len(),
                )
                .expect("record target leaves payload room");
            let mut fields = base_fields;
            fields.push((
                unknown_field,
                CanonicalValue::bytes(vec![0; payload_bytes]).expect("bounded unknown payload"),
            ));
            let fields = CanonicalRecord::new(fields).expect("large raw record");
            assert_eq!(
                encode_canonical_record(&fields)
                    .expect("large raw encoding")
                    .len(),
                desired_bytes
            );
            fields
        } else {
            CanonicalRecord::new(base_fields).expect("raw genesis record")
        };
        let record = StoredEntityRecordV1::new(
            target,
            EntityVersion::first(),
            genesis.contract_version(),
            DurableKeySchemaBindingV1::new(
                genesis.lineage().clone(),
                genesis.contract_version(),
                genesis.bundle_hash(),
            ),
            fields,
        )
        .expect("stored genesis record");
        let snapshot = ReadSnapshot::new(
            &request,
            None,
            vec![EntityObservation::Present(record)],
            root_targets
                .into_iter()
                .map(EntityObservation::Absent)
                .collect(),
            Vec::new(),
        )
        .expect("raw successor snapshot");
        let note = successor
            .bundle()
            .schema()
            .entity(binding_targets[0].entity_type_id())
            .expect("successor Row")
            .record()
            .fields()
            .iter()
            .find(|field| field.name() == "note")
            .expect("successor note")
            .id();
        (
            pending_attempt(resolved_plan, input, request),
            snapshot,
            note,
        )
    }

    fn acquire_and_release(
        runtime: &tokio::runtime::Runtime,
        manager: &ShardedConflictManager,
        keys: Vec<ConflictKey>,
    ) {
        runtime
            .block_on(manager.acquire_mut(keys, future_deadline(), CancellationToken::new()))
            .expect("logical capability is available")
            .release();
    }

    struct CandidateChainTrace {
        order: Rc<RefCell<Vec<&'static str>>>,
        expected_intent: CommitIntent,
        intent_address: Cell<Option<usize>>,
        lease_held_at_rollback: Cell<Option<bool>>,
        current: RefCell<Option<TransactionCurrentState>>,
        manager: ShardedConflictManager,
        keys: Vec<ConflictKey>,
    }

    impl CandidateChainTrace {
        fn observe_intent(&self, event: &'static str, intent: &CommitIntent) {
            self.order.borrow_mut().push(event);
            assert_eq!(intent, &self.expected_intent, "candidate intent changed");
            let address = std::ptr::from_ref(intent) as usize;
            match self.intent_address.get() {
                Some(expected) => assert_eq!(address, expected, "candidate intent was replaced"),
                None => self.intent_address.set(Some(address)),
            }
        }

        fn record_whether_lease_is_held(&self) {
            let mut probe = self.manager.acquire_mut(
                self.keys.clone(),
                future_deadline(),
                CancellationToken::new(),
            );
            let mut context = Context::from_waker(Waker::noop());
            self.lease_held_at_rollback.set(Some(matches!(
                probe.as_mut().poll(&mut context),
                Poll::Pending
            )));
        }
    }

    struct InstrumentedEmptyBatch {
        trace: Rc<CandidateChainTrace>,
    }

    impl Drop for InstrumentedEmptyBatch {
        fn drop(&mut self) {
            self.trace.record_whether_lease_is_held();
            self.trace.order.borrow_mut().push("storage-rollback");
        }
    }

    struct InstrumentedCandidateAdmission {
        prior: InstrumentedEmptyBatch,
        intent: Box<CommitIntent>,
    }

    struct InstrumentedCandidateStateRead {
        prior: InstrumentedEmptyBatch,
        intent: Box<CommitIntent>,
    }

    struct InstrumentedAwaitingValidation {
        prior: InstrumentedEmptyBatch,
        intent: Box<CommitIntent>,
    }

    struct InstrumentedTransactionPort {
        trace: Rc<CandidateChainTrace>,
    }

    impl ApplicationCommandTransactionPort for InstrumentedTransactionPort {
        type EmptyBatch = InstrumentedEmptyBatch;

        fn begin_empty_batch(&self) -> Result<Self::EmptyBatch, StorageError> {
            self.trace.order.borrow_mut().push("begin-empty");
            Ok(InstrumentedEmptyBatch {
                trace: Rc::clone(&self.trace),
            })
        }
    }

    impl EmptyCommandBatch for InstrumentedEmptyBatch {
        type Candidate = InstrumentedCandidateAdmission;

        fn begin_candidate(
            self,
            intent: Box<CommitIntent>,
        ) -> Result<Self::Candidate, StorageError> {
            self.trace.observe_intent("begin-candidate", &intent);
            Ok(InstrumentedCandidateAdmission {
                prior: self,
                intent,
            })
        }

        fn rollback(self) {
            drop(self);
        }
    }

    impl CommandCandidateAdmission for InstrumentedCandidateAdmission {
        type Prior = InstrumentedEmptyBatch;
        type StateRead = InstrumentedCandidateStateRead;

        fn recheck_admission(
            self,
        ) -> Result<CandidateAdmissionResult<Self::Prior, Self::StateRead>, StorageError> {
            self.prior
                .trace
                .observe_intent("candidate-recheck", &self.intent);
            Ok(CandidateAdmissionResult::Proceed(
                InstrumentedCandidateStateRead {
                    prior: self.prior,
                    intent: self.intent,
                },
            ))
        }
    }

    impl CommandCandidateStateRead for InstrumentedCandidateStateRead {
        type Prior = InstrumentedEmptyBatch;
        type AwaitingValidation = InstrumentedAwaitingValidation;

        fn read_transaction_current(
            self,
        ) -> Result<(Self::AwaitingValidation, TransactionCurrentState), StorageError> {
            self.prior
                .trace
                .observe_intent("current-read", &self.intent);
            let current = self
                .prior
                .trace
                .current
                .borrow_mut()
                .take()
                .expect("one transaction-current read");
            Ok((
                InstrumentedAwaitingValidation {
                    prior: self.prior,
                    intent: self.intent,
                },
                current,
            ))
        }
    }

    impl CommandCandidateAwaitingValidation for InstrumentedAwaitingValidation {
        type Prior = InstrumentedEmptyBatch;
        type AffectedEpochRead = NeverCandidate<InstrumentedEmptyBatch>;

        fn plan_validated(
            self,
            _affected_targets: AffectedIndexEpochTargets,
        ) -> Self::AffectedEpochRead {
            panic!("dependency-changed test must reject before index planning")
        }

        fn reject(self, reason: CandidateValidationRejection) -> AbandonedCandidate<Self::Prior> {
            assert_eq!(reason, CandidateValidationRejection::DependencyChanged);
            self.prior
                .trace
                .observe_intent("candidate-reject", &self.intent);
            AbandonedCandidate::new(self.prior, self.intent)
        }
    }

    struct NeverCandidate<P>(PhantomData<P>);

    impl<P> CommandCandidateAdmission for NeverCandidate<P> {
        type Prior = P;
        type StateRead = Self;

        fn recheck_admission(
            self,
        ) -> Result<CandidateAdmissionResult<Self::Prior, Self::StateRead>, StorageError> {
            panic!("unreachable candidate admission")
        }
    }

    impl<P> CommandCandidateStateRead for NeverCandidate<P> {
        type Prior = P;
        type AwaitingValidation = Self;

        fn read_transaction_current(
            self,
        ) -> Result<(Self::AwaitingValidation, TransactionCurrentState), StorageError> {
            panic!("unreachable transaction-current read")
        }
    }

    impl<P> CommandCandidateAwaitingValidation for NeverCandidate<P> {
        type Prior = P;
        type AffectedEpochRead = Self;

        fn plan_validated(
            self,
            _affected_targets: AffectedIndexEpochTargets,
        ) -> Self::AffectedEpochRead {
            panic!("unreachable validated plan")
        }

        fn reject(self, _reason: CandidateValidationRejection) -> AbandonedCandidate<Self::Prior> {
            panic!("unreachable rejection")
        }
    }

    impl<P> CommandCandidateAffectedEpochRead for NeverCandidate<P> {
        type Prior = P;
        type AwaitingCapacity = Self;

        fn read_affected_epoch_current(self) -> Result<Self::AwaitingCapacity, StorageError> {
            panic!("unreachable affected-epoch read")
        }
    }

    impl<P> CommandCandidateAwaitingCapacity for NeverCandidate<P> {
        type Prior = P;
        type CapacityReserved = Self;

        fn intent(&self) -> &CommitIntent {
            panic!("unreachable candidate intent")
        }

        fn affected_targets(&self) -> &AffectedIndexEpochTargets {
            panic!("unreachable affected targets")
        }

        fn affected_current(&self) -> &AffectedEpochCurrentState {
            panic!("unreachable affected current state")
        }

        fn reject(self, _reason: CandidateValidationRejection) -> AbandonedCandidate<Self::Prior> {
            panic!("unreachable affected-index rejection")
        }

        fn reserve_capacity(
            self,
            _write_plan: CommandWriteSetPlanV1,
        ) -> Result<CandidateCapacityResult<Self::Prior, Self::CapacityReserved>, StorageError>
        {
            panic!("unreachable capacity reservation")
        }
    }

    impl<P> CommandCandidateCapacityReserved for NeverCandidate<P> {
        type Prior = P;
        type SequenceAssigned = Self;

        fn intent(&self) -> &CommitIntent {
            panic!("unreachable reserved intent")
        }

        fn write_plan(&self) -> &CommandWriteSetPlanV1 {
            panic!("unreachable reserved write plan")
        }

        fn assign_sequence(self) -> Result<Self::SequenceAssigned, StorageError> {
            panic!("unreachable sequence assignment")
        }
    }

    impl<P> CommandCandidateSequenceAssigned for NeverCandidate<P> {
        type Prior = P;
        type Staged = NeverStagedBatch;

        fn assignment(&self) -> riffdb_storage_api::AssignedCommandSequence {
            panic!("unreachable sequence assignment evidence")
        }

        fn intent(&self) -> &CommitIntent {
            panic!("unreachable assigned intent")
        }

        fn write_plan(&self) -> &CommandWriteSetPlanV1 {
            panic!("unreachable assigned write plan")
        }

        fn detach(
            self,
        ) -> Result<
            (
                Self::Prior,
                riffdb_storage_api::DetachedCommandReservationV1,
            ),
            StorageError,
        > {
            panic!("unreachable assigned detachment")
        }

        fn stage(self, _records: AtomicCommandRecordSet) -> Result<Self::Staged, StorageError> {
            panic!("unreachable record staging")
        }
    }

    struct NeverStagedBatch;

    impl NonEmptyCommandBatch for NeverStagedBatch {
        type Candidate = NeverCandidate<Self>;

        fn metrics(&self) -> StagedBatchMetrics {
            panic!("unreachable staged metrics")
        }

        fn begin_candidate(
            self,
            _intent: Box<CommitIntent>,
        ) -> Result<CandidateStartResult<Self, Self::Candidate>, StorageError> {
            panic!("unreachable staged candidate")
        }

        fn commit(self, _durability: DurabilityMode) -> Result<CommittedBatchV1, StorageError> {
            panic!("unreachable staged commit")
        }

        fn rollback(self) {}
    }

    struct CandidateChainFixture {
        bound: ProvenanceBoundCommandAttempt,
        exact_current: TransactionCurrentState,
        changed_current: TransactionCurrentState,
        manager: ShardedConflictManager,
        runtime: tokio::runtime::Runtime,
        keys: Vec<ConflictKey>,
        order: Rc<RefCell<Vec<&'static str>>>,
    }

    fn candidate_chain_fixture() -> CandidateChainFixture {
        let (state, snapshot, _) = evolved_row_fixture(None);
        let keys = state.raw_conflict_keys.clone();
        let EntityObservation::Present(snapshot_record) = &snapshot.bindings()[0] else {
            panic!("Increment fixture must read one present row")
        };
        let changed_record = StoredEntityRecordV1::new(
            snapshot_record.target().clone(),
            snapshot_record
                .entity_version()
                .checked_next()
                .expect("fixture entity version advances"),
            snapshot_record.written_by_contract(),
            snapshot_record.schema_binding().clone(),
            snapshot_record.fields().clone(),
        )
        .expect("transaction-current changed record");
        let changed_current = TransactionCurrentState::new(
            &snapshot.validation_request(),
            vec![EntityObservation::Present(changed_record)],
            snapshot.root_validations().to_vec(),
            Vec::new(),
        )
        .expect("complete changed transaction-current state");
        let exact_current = TransactionCurrentState::new(
            &snapshot.validation_request(),
            snapshot.bindings().to_vec(),
            snapshot.root_validations().to_vec(),
            Vec::new(),
        )
        .expect("complete exact transaction-current state");

        let order = Rc::new(RefCell::new(Vec::new()));
        let repository = ScriptedRepository::new(
            state.lookup_candidates.clone(),
            AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::Pending(
                state.commit_context.pending().clone(),
            ))),
            Rc::clone(&order),
        );
        let snapshots = ScriptedSnapshotReader::new(
            state.snapshot_request.clone(),
            snapshot,
            Rc::clone(&order),
        );
        let manager = manager();
        let runtime = runtime();
        let resolution = runtime
            .block_on(evaluate_next_command_attempt(
                state,
                &repository,
                &snapshots,
                &manager,
            ))
            .expect("evaluation attempt");
        let CommandAttemptResolution::Evaluated(attempt) = resolution else {
            panic!("Increment must require a commit")
        };
        let bound = attempt
            .bind_provenance(provenance_id())
            .expect("bind exact provenance");
        CandidateChainFixture {
            bound,
            exact_current,
            changed_current,
            manager,
            runtime,
            keys,
            order,
        }
    }

    fn instrumented_transaction_port(
        bound: &ProvenanceBoundCommandAttempt,
        current: TransactionCurrentState,
        manager: ShardedConflictManager,
        keys: Vec<ConflictKey>,
        order: Rc<RefCell<Vec<&'static str>>>,
    ) -> (InstrumentedTransactionPort, Rc<CandidateChainTrace>) {
        let trace = Rc::new(CandidateChainTrace {
            order,
            expected_intent: bound.commit_intent().clone(),
            intent_address: Cell::new(None),
            lease_held_at_rollback: Cell::new(None),
            current: RefCell::new(Some(current)),
            manager,
            keys,
        });
        let port = InstrumentedTransactionPort {
            trace: Rc::clone(&trace),
        };
        (port, trace)
    }

    #[derive(Clone, Copy)]
    enum FailureTerminalizeBehavior {
        Commit,
        Error(StorageErrorKind),
    }

    struct ExecutionFailureTrace {
        order: Rc<RefCell<Vec<&'static str>>>,
        current: RefCell<Option<TransactionCurrentState>>,
        behavior: FailureTerminalizeBehavior,
        manager: ShardedConflictManager,
        keys: Vec<ConflictKey>,
        lease_held_at_storage_action: Cell<Option<bool>>,
    }

    impl ExecutionFailureTrace {
        fn record_whether_lease_is_held(&self) {
            let mut probe = self.manager.acquire_mut(
                self.keys.clone(),
                future_deadline(),
                CancellationToken::new(),
            );
            let mut context = Context::from_waker(Waker::noop());
            self.lease_held_at_storage_action.set(Some(matches!(
                probe.as_mut().poll(&mut context),
                Poll::Pending
            )));
        }
    }

    struct InstrumentedExecutionFailurePort {
        trace: Rc<ExecutionFailureTrace>,
    }

    struct InstrumentedExecutionFailureRechecked {
        trace: Rc<ExecutionFailureTrace>,
        request: ExecutionFailureTransitionRequestV1,
    }

    struct InstrumentedExecutionFailureAwaiting {
        trace: Rc<ExecutionFailureTrace>,
        expected: StoredExecutionFailedV1,
    }

    impl ExecutionFailureTransitionPort for InstrumentedExecutionFailurePort {
        type Rechecked = InstrumentedExecutionFailureRechecked;

        fn begin_execution_failure(
            &self,
            request: ExecutionFailureTransitionRequestV1,
        ) -> Result<ExecutionFailureAdmissionResult<Self::Rechecked>, StorageError> {
            self.trace.order.borrow_mut().push("failure-begin");
            Ok(ExecutionFailureAdmissionResult::Rechecked(
                InstrumentedExecutionFailureRechecked {
                    trace: Rc::clone(&self.trace),
                    request,
                },
            ))
        }
    }

    impl ExecutionFailureAdmissionRechecked for InstrumentedExecutionFailureRechecked {
        type AwaitingDecision = InstrumentedExecutionFailureAwaiting;

        fn read_transaction_current(
            self,
        ) -> Result<(Self::AwaitingDecision, TransactionCurrentState), StorageError> {
            self.trace.order.borrow_mut().push("failure-current-read");
            let current = self
                .trace
                .current
                .borrow_mut()
                .take()
                .expect("one execution-failure current read");
            Ok((
                InstrumentedExecutionFailureAwaiting {
                    trace: self.trace,
                    expected: self.request.terminal_record(),
                },
                current,
            ))
        }
    }

    impl ExecutionFailureAwaitingDecision for InstrumentedExecutionFailureAwaiting {
        fn terminalize(self) -> Result<StoredExecutionFailedV1, StorageError> {
            self.trace.record_whether_lease_is_held();
            self.trace.order.borrow_mut().push("failure-terminalize");
            let result = match self.trace.behavior {
                FailureTerminalizeBehavior::Commit => Ok(self.expected.clone()),
                FailureTerminalizeBehavior::Error(kind) => Err(StorageError::new(kind, None)),
            };
            drop(self);
            result
        }

        fn abandon(self) {
            self.trace.record_whether_lease_is_held();
            self.trace.order.borrow_mut().push("failure-abandon");
            drop(self);
        }
    }

    impl Drop for InstrumentedExecutionFailureAwaiting {
        fn drop(&mut self) {
            self.trace.record_whether_lease_is_held();
            self.trace.order.borrow_mut().push("failure-storage-drop");
        }
    }

    struct ExecutionFailureFixture {
        fault: ExecutionFaultAttempt,
        exact_current: TransactionCurrentState,
        changed_current: TransactionCurrentState,
        manager: ShardedConflictManager,
        runtime: tokio::runtime::Runtime,
        keys: Vec<ConflictKey>,
        order: Rc<RefCell<Vec<&'static str>>>,
    }

    fn execution_failure_fixture() -> ExecutionFailureFixture {
        let CandidateChainFixture {
            bound,
            exact_current,
            changed_current,
            manager,
            runtime,
            keys,
            order,
        } = candidate_chain_fixture();
        let RolledBackCandidateDisposition::ExecutionFault(fault) = bound
            .after_proven_rollback_for_test(
                CandidateValidationRejection::CommitCheckArithmeticFault,
            )
        else {
            panic!("late arithmetic must retain execution-failure evidence")
        };
        order.borrow_mut().clear();
        ExecutionFailureFixture {
            fault: *fault,
            exact_current,
            changed_current,
            manager,
            runtime,
            keys,
            order,
        }
    }

    fn instrumented_execution_failure_port(
        current: TransactionCurrentState,
        behavior: FailureTerminalizeBehavior,
        manager: ShardedConflictManager,
        keys: Vec<ConflictKey>,
        order: Rc<RefCell<Vec<&'static str>>>,
    ) -> (InstrumentedExecutionFailurePort, Rc<ExecutionFailureTrace>) {
        let trace = Rc::new(ExecutionFailureTrace {
            order,
            current: RefCell::new(Some(current)),
            behavior,
            manager,
            keys,
            lease_held_at_storage_action: Cell::new(None),
        });
        (
            InstrumentedExecutionFailurePort {
                trace: Rc::clone(&trace),
            },
            trace,
        )
    }

    fn request_id(seed: u8) -> RequestId {
        RequestId::from_unix_milliseconds_and_random(u64::from(seed) + 1, [seed; 10])
            .expect("UUIDv7 request ID")
    }

    fn database_id() -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(1, [0x21; 10]).expect("UUIDv7 database ID")
    }

    fn provenance_id() -> ProvenanceId {
        ProvenanceId::from_unix_milliseconds_and_random(1, [0x31; 10])
            .expect("UUIDv7 provenance ID")
    }

    fn commit_context() -> PreEvaluationCommitContext {
        let lineage = ContractLineage::new("attempt-test").expect("lineage");
        let command_id = CommandId::first();
        let plan = ExecutablePlanRef::new(
            lineage.clone(),
            ContractVersion::new(1).expect("contract version"),
            ContractBundleHash::from_bytes([0x41; 32]),
            command_id,
            PlanHash::from_bytes([0x42; 32]),
        );
        let actor = AdmittedActorContext::new(
            ActorId::new(PRINCIPAL).expect("principal"),
            ActorKind::Service,
            TenantScope::Global,
            None,
        );
        let identity = IdempotencyIdentity::new(
            database_id(),
            Environment::new("development").expect("environment"),
            TenantScope::Global,
            ActorId::new(PRINCIPAL).expect("principal"),
            lineage,
            command_id,
            IdempotencyKeyDigest::from_hmac_bytes(
                DigestKeyId::new(1).expect("digest key ID"),
                [0x51; 32],
            ),
        );
        let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
        partition.push_u64(7).expect("partition component");
        let partition = partition.finish().expect("partition key");
        let pending = StoredPendingAdmissionV1::new(
            identity,
            CanonicalInputHash::from_bytes([0x61; 32]),
            request_id(1),
            plan,
            LogicalTime::new(Timestamp::new(100, 17).expect("timestamp")),
            actor,
            partition.clone(),
            StoredAdmittedProvenanceClaimsV1::default(),
        )
        .expect("pending admission");
        PreEvaluationCommitContext::new(
            pending,
            hash_partition_key(partition.as_bytes()),
            vec![ConflictKeyHash::from_bytes([0x71; 32])],
        )
        .expect("pre-evaluation context")
    }

    fn outcome_from(
        context: &PreEvaluationCommitContext,
        admission_request_id: RequestId,
        conflict_hashes: Vec<ConflictKeyHash>,
    ) -> StoredOutcomeV1 {
        let pending = context.pending();
        StoredOutcomeV1::new(
            pending.identity().clone(),
            CommitSequence::new(1).expect("commit sequence"),
            admission_request_id,
            pending.plan().clone(),
            pending.canonical_input_hash(),
            pending.actor().clone(),
            pending.logical_time(),
            pending.partition_key().clone(),
            context.partition_hash(),
            conflict_hashes,
            DeclaredOutcome::new(
                OutcomeId::first(),
                CanonicalRecord::new(Vec::new()).expect("empty outcome"),
            )
            .expect("declared outcome"),
            pending.provenance_claims().clone(),
            provenance_id(),
            DurabilityMode::Memory,
        )
        .expect("stored outcome")
    }

    #[test]
    fn real_evaluation_orders_ports_and_retains_lease_until_drop() {
        let (state, snapshot) = execution_fixture();
        let keys = state.raw_conflict_keys.clone();
        let order = Rc::new(RefCell::new(Vec::new()));
        let repository = ScriptedRepository::new(
            state.lookup_candidates.clone(),
            AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::Pending(
                state.commit_context.pending().clone(),
            ))),
            Rc::clone(&order),
        );
        let snapshots = ScriptedSnapshotReader::new(
            state.snapshot_request.clone(),
            snapshot,
            Rc::clone(&order),
        );
        let manager = manager();
        let runtime = runtime();

        let resolution = runtime
            .block_on(evaluate_next_command_attempt(
                state,
                &repository,
                &snapshots,
                &manager,
            ))
            .expect("evaluation attempt");
        let CommandAttemptResolution::Evaluated(attempt) = resolution else {
            panic!("CreateBudget must require a commit")
        };
        assert!(attempt.has_exact_semantic_join());
        assert_eq!(attempt.state.completed_attempts(), 1);
        assert_eq!(attempt.evaluated().mutations().len(), 1);
        assert_eq!(
            attempt.evaluated().read_dependencies(),
            attempt
                .materialized_snapshot()
                .snapshot()
                .read_dependencies()
        );
        assert_eq!(&*order.borrow(), &["pending-recheck", "snapshot"]);

        let mut competing = manager.acquire_mut(keys, future_deadline(), CancellationToken::new());
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            competing.as_mut().poll(&mut context),
            Poll::Pending
        ));
        let bound = attempt
            .bind_provenance(provenance_id())
            .expect("successful evaluation binds one provenance candidate");
        assert_eq!(bound.commit_intent().provenance_id(), provenance_id());
        assert_eq!(bound.storage_intent().as_ref(), bound.commit_intent());
        drop(bound);
        runtime
            .block_on(competing)
            .expect("dropping retained lease grants competitor")
            .release();
    }

    #[test]
    fn post_apply_evidence_releases_the_conflict_lease_but_retains_exact_recovery_identity() {
        let (state, snapshot) = execution_fixture();
        let keys = state.raw_conflict_keys.clone();
        let expected_lookup = state.lookup_candidates.clone();
        let order = Rc::new(RefCell::new(Vec::new()));
        let repository = ScriptedRepository::new(
            state.lookup_candidates.clone(),
            AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::Pending(
                state.commit_context.pending().clone(),
            ))),
            Rc::clone(&order),
        );
        let snapshots = ScriptedSnapshotReader::new(
            state.snapshot_request.clone(),
            snapshot,
            Rc::clone(&order),
        );
        let manager = manager();
        let runtime = runtime();
        let CommandAttemptResolution::Evaluated(attempt) = runtime
            .block_on(evaluate_next_command_attempt(
                state,
                &repository,
                &snapshots,
                &manager,
            ))
            .expect("evaluation attempt")
        else {
            panic!("fixture must require a commit")
        };
        let bound = attempt
            .bind_provenance(provenance_id())
            .expect("bind exact provenance");
        let expected_intent = bound.commit_intent().clone();

        let mut competing = manager.acquire_mut(keys, future_deadline(), CancellationToken::new());
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            competing.as_mut().poll(&mut context),
            Poll::Pending
        ));

        // Production can construct this evidence only after storage has
        // accepted the complete private command graph. The conversion consumes
        // the live attempt and therefore its mutation authority.
        let evidence = bound
            .into_post_apply_evidence()
            .expect("exact attempt becomes post-apply evidence");
        assert_eq!(evidence.exact_intent(), &expected_intent);
        assert_eq!(evidence.lookup_candidates(), &expected_lookup);
        runtime
            .block_on(competing)
            .expect("post-apply handoff releases the conflict lease")
            .release();
    }

    #[test]
    fn shared_group_authority_releases_only_after_every_member_drops() {
        let (state, _) = execution_fixture();
        let keys = state.raw_conflict_keys.clone();
        let manager = manager();
        let runtime = runtime();
        let lease = runtime
            .block_on(manager.acquire_mut(
                keys.clone(),
                future_deadline(),
                CancellationToken::new(),
            ))
            .expect("group union lease");
        let group = Arc::new(ProvenCommutativeGroupLease { _lease: lease });
        let first = CommandMutationAuthority::ProvenCommutativeGroup {
            _lease: Arc::clone(&group),
        };
        let second = CommandMutationAuthority::ProvenCommutativeGroup { _lease: group };

        let mut competing = manager.acquire_mut(keys, future_deadline(), CancellationToken::new());
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            competing.as_mut().poll(&mut context),
            Poll::Pending
        ));
        drop(first);
        assert!(matches!(
            competing.as_mut().poll(&mut context),
            Poll::Pending
        ));
        drop(second);
        runtime
            .block_on(competing)
            .expect("last group member releases union lease")
            .release();
    }

    #[test]
    fn proven_noncommit_discards_the_old_attempt_before_retry() {
        let (state, snapshot) = execution_fixture();
        let keys = state.raw_conflict_keys.clone();
        let order = Rc::new(RefCell::new(Vec::new()));
        let repository = ScriptedRepository::new(
            state.lookup_candidates.clone(),
            AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::Pending(
                state.commit_context.pending().clone(),
            ))),
            Rc::clone(&order),
        );
        let snapshots = ScriptedSnapshotReader::new(
            state.snapshot_request.clone(),
            snapshot,
            Rc::clone(&order),
        );
        let manager = manager();
        let runtime = runtime();
        let resolution = runtime
            .block_on(evaluate_next_command_attempt(
                state,
                &repository,
                &snapshots,
                &manager,
            ))
            .expect("evaluation attempt");
        let CommandAttemptResolution::Evaluated(attempt) = resolution else {
            panic!("CreateBudget must require a commit")
        };
        let bound = attempt
            .bind_provenance(provenance_id())
            .expect("bind old provenance attempt");

        let mut competing = manager.acquire_mut(keys, future_deadline(), CancellationToken::new());
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            competing.as_mut().poll(&mut context),
            Poll::Pending
        ));

        let retry = bound.into_pending_after_proven_noncommit();
        assert_eq!(retry.completed_attempts(), 1);
        runtime
            .block_on(competing)
            .expect("proven noncommit releases the old attempt lease")
            .release();
    }

    #[test]
    fn storage_candidate_chain_preserves_one_intent_and_rolls_back_before_lease_release() {
        let CandidateChainFixture {
            bound,
            exact_current: _,
            changed_current,
            manager,
            runtime,
            keys,
            order,
        } = candidate_chain_fixture();
        let (port, trace) = instrumented_transaction_port(
            &bound,
            changed_current,
            manager.clone(),
            keys.clone(),
            Rc::clone(&order),
        );

        let crate::command_validation::CommandCandidateChainStart::Ready(candidate) =
            crate::command_validation::begin_bound_command_candidate(&port, bound)
        else {
            panic!("exact Pending candidate must proceed")
        };
        let crate::command_validation::TransactionCurrentAttemptDecision::DependencyChanged(
            changed,
        ) = candidate.read_transaction_current()
        else {
            panic!("changed entity version must reject the exact candidate")
        };
        let RolledBackCandidateDisposition::Retry { state, reason } =
            changed.reject_storage_and_rollback()
        else {
            panic!("dependency change must become a post-rollback retry")
        };

        assert_eq!(reason, CandidateValidationRejection::DependencyChanged);
        assert_eq!(state.completed_attempts(), 1);
        assert!(trace.current.borrow().is_none());
        assert!(trace.intent_address.get().is_some());
        assert_eq!(trace.lease_held_at_rollback.get(), Some(true));
        assert_eq!(
            &*order.borrow(),
            &[
                "pending-recheck",
                "snapshot",
                "begin-empty",
                "begin-candidate",
                "candidate-recheck",
                "current-read",
                "candidate-reject",
                "storage-rollback",
            ]
        );
        acquire_and_release(&runtime, &manager, keys);
    }

    #[test]
    fn dropping_bound_state_read_rolls_back_storage_before_releasing_the_lease() {
        let CandidateChainFixture {
            bound,
            exact_current: _,
            changed_current,
            manager,
            runtime,
            keys,
            order,
        } = candidate_chain_fixture();
        let (port, trace) = instrumented_transaction_port(
            &bound,
            changed_current,
            manager.clone(),
            keys.clone(),
            Rc::clone(&order),
        );

        let crate::command_validation::CommandCandidateChainStart::Ready(candidate) =
            crate::command_validation::begin_bound_command_candidate(&port, bound)
        else {
            panic!("exact Pending candidate must proceed")
        };
        drop(candidate);

        assert_eq!(trace.lease_held_at_rollback.get(), Some(true));
        assert_eq!(
            &*order.borrow(),
            &[
                "pending-recheck",
                "snapshot",
                "begin-empty",
                "begin-candidate",
                "candidate-recheck",
                "storage-rollback",
            ]
        );
        acquire_and_release(&runtime, &manager, keys);
    }

    #[test]
    fn late_commit_check_arithmetic_retains_snapshot_and_lease_after_proven_rollback() {
        let (state, snapshot) = execution_fixture();
        let keys = state.raw_conflict_keys.clone();
        let order = Rc::new(RefCell::new(Vec::new()));
        let repository = ScriptedRepository::new(
            state.lookup_candidates.clone(),
            AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::Pending(
                state.commit_context.pending().clone(),
            ))),
            Rc::clone(&order),
        );
        let snapshots = ScriptedSnapshotReader::new(
            state.snapshot_request.clone(),
            snapshot,
            Rc::clone(&order),
        );
        let manager = manager();
        let runtime = runtime();
        let resolution = runtime
            .block_on(evaluate_next_command_attempt(
                state,
                &repository,
                &snapshots,
                &manager,
            ))
            .expect("evaluation attempt");
        let CommandAttemptResolution::Evaluated(attempt) = resolution else {
            panic!("CreateBudget must require a commit")
        };
        let bound = attempt
            .bind_provenance(provenance_id())
            .expect("bind one provenance candidate");

        let RolledBackCandidateDisposition::ExecutionFault(fault) = bound
            .after_proven_rollback_for_test(
                CandidateValidationRejection::CommitCheckArithmeticFault,
            )
        else {
            panic!("late arithmetic must enter terminalization evidence")
        };
        let ExecutionFaultAttempt::Arithmetic {
            state,
            lease,
            snapshot,
        } = *fault
        else {
            panic!("late commit-check arithmetic must retain arithmetic evidence")
        };
        assert_eq!(state.completed_attempts(), 1);
        assert_eq!(
            snapshot.snapshot().plan(),
            state.commit_context.pending().plan()
        );
        assert!(
            !snapshot
                .snapshot()
                .read_dependencies()
                .as_slice()
                .is_empty()
        );

        let mut competing = manager.acquire_mut(keys, future_deadline(), CancellationToken::new());
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            competing.as_mut().poll(&mut context),
            Poll::Pending
        ));
        drop(snapshot);
        drop(lease);
        runtime
            .block_on(competing)
            .expect("terminalization evidence releases capability only when dropped")
            .release();
    }

    #[test]
    fn execution_failure_terminalizes_only_exact_current_evidence() {
        let ExecutionFailureFixture {
            fault,
            exact_current,
            changed_current: _,
            manager,
            runtime,
            keys,
            order,
        } = execution_failure_fixture();
        let (port, trace) = instrumented_execution_failure_port(
            exact_current,
            FailureTerminalizeBehavior::Commit,
            manager.clone(),
            keys.clone(),
            Rc::clone(&order),
        );

        let crate::command_execution_failure::ExecutionFailureTransitionStart::Ready(current) =
            crate::command_execution_failure::begin_execution_failure_transition(&port, fault)
        else {
            panic!("exact Pending must enter execution-failure current read")
        };
        let crate::command_execution_failure::ExecutionFailureCurrentDecision::Ready(checked) =
            current.read_transaction_current()
        else {
            panic!("exact dependency evidence must authorize terminalization")
        };
        let crate::command_execution_failure::ExecutionFailureTerminalizeResult::Terminalized(
            failure,
        ) = checked.terminalize()
        else {
            panic!("exact terminal record must become durable")
        };

        assert_eq!(failure.code(), ExecutionFailureCode::ArithmeticFault);
        assert_eq!(trace.lease_held_at_storage_action.get(), Some(true));
        assert_eq!(
            &*order.borrow(),
            &[
                "failure-begin",
                "failure-current-read",
                "failure-terminalize",
                "failure-storage-drop",
            ]
        );
        acquire_and_release(&runtime, &manager, keys);
    }

    #[test]
    fn changed_execution_failure_dependencies_abandon_before_bounded_retry() {
        let ExecutionFailureFixture {
            fault,
            exact_current: _,
            changed_current,
            manager,
            runtime,
            keys,
            order,
        } = execution_failure_fixture();
        let (port, trace) = instrumented_execution_failure_port(
            changed_current,
            FailureTerminalizeBehavior::Commit,
            manager.clone(),
            keys.clone(),
            Rc::clone(&order),
        );

        let crate::command_execution_failure::ExecutionFailureTransitionStart::Ready(current) =
            crate::command_execution_failure::begin_execution_failure_transition(&port, fault)
        else {
            panic!("exact Pending must enter execution-failure current read")
        };
        let crate::command_execution_failure::ExecutionFailureCurrentDecision::Retry(retry) =
            current.read_transaction_current()
        else {
            panic!("changed dependencies must abandon and retry")
        };

        assert_eq!(retry.completed_attempts(), 1);
        assert_eq!(trace.lease_held_at_storage_action.get(), Some(true));
        assert_eq!(
            &*order.borrow(),
            &[
                "failure-begin",
                "failure-current-read",
                "failure-abandon",
                "failure-storage-drop",
            ]
        );
        drop(retry);
        acquire_and_release(&runtime, &manager, keys);
    }

    #[test]
    fn dropping_checked_execution_failure_rolls_back_before_releasing_lease() {
        let ExecutionFailureFixture {
            fault,
            exact_current,
            changed_current: _,
            manager,
            runtime,
            keys,
            order,
        } = execution_failure_fixture();
        let (port, trace) = instrumented_execution_failure_port(
            exact_current,
            FailureTerminalizeBehavior::Commit,
            manager.clone(),
            keys.clone(),
            Rc::clone(&order),
        );

        let crate::command_execution_failure::ExecutionFailureTransitionStart::Ready(current) =
            crate::command_execution_failure::begin_execution_failure_transition(&port, fault)
        else {
            panic!("exact Pending must enter execution-failure current read")
        };
        let crate::command_execution_failure::ExecutionFailureCurrentDecision::Ready(checked) =
            current.read_transaction_current()
        else {
            panic!("exact dependencies must reach checked terminalization")
        };
        drop(checked);

        assert_eq!(trace.lease_held_at_storage_action.get(), Some(true));
        assert_eq!(
            &*order.borrow(),
            &[
                "failure-begin",
                "failure-current-read",
                "failure-storage-drop",
            ]
        );
        acquire_and_release(&runtime, &manager, keys);
    }

    #[test]
    fn execution_failure_proven_abort_never_enters_uncertain_recovery() {
        let ExecutionFailureFixture {
            fault,
            exact_current,
            changed_current: _,
            manager,
            runtime,
            keys,
            order,
        } = execution_failure_fixture();
        let (port, trace) = instrumented_execution_failure_port(
            exact_current,
            FailureTerminalizeBehavior::Error(StorageErrorKind::Unavailable),
            manager.clone(),
            keys.clone(),
            Rc::clone(&order),
        );

        let crate::command_execution_failure::ExecutionFailureTransitionStart::Ready(current) =
            crate::command_execution_failure::begin_execution_failure_transition(&port, fault)
        else {
            panic!("exact Pending must enter execution-failure current read")
        };
        let crate::command_execution_failure::ExecutionFailureCurrentDecision::Ready(checked) =
            current.read_transaction_current()
        else {
            panic!("exact dependencies must reach terminalization")
        };
        let crate::command_execution_failure::ExecutionFailureTerminalizeResult::ProvenAbort(error) =
            checked.terminalize()
        else {
            panic!("proved storage abort must not become uncertain")
        };

        assert_eq!(error.kind(), StorageErrorKind::Unavailable);
        assert_eq!(trace.lease_held_at_storage_action.get(), Some(true));
        acquire_and_release(&runtime, &manager, keys);
    }

    #[test]
    fn uncertain_execution_failure_retains_attempt_until_same_key_proves_noncommit() {
        let ExecutionFailureFixture {
            fault,
            exact_current,
            changed_current: _,
            manager,
            runtime,
            keys,
            order,
        } = execution_failure_fixture();
        let lookup_candidates = fault.lookup_candidates().clone();
        let pending = fault.pending().clone();
        let (port, trace) = instrumented_execution_failure_port(
            exact_current,
            FailureTerminalizeBehavior::Error(StorageErrorKind::CommitStatusUnknown),
            manager.clone(),
            keys.clone(),
            Rc::clone(&order),
        );

        let crate::command_execution_failure::ExecutionFailureTransitionStart::Ready(current) =
            crate::command_execution_failure::begin_execution_failure_transition(&port, fault)
        else {
            panic!("exact Pending must enter execution-failure current read")
        };
        let crate::command_execution_failure::ExecutionFailureCurrentDecision::Ready(checked) =
            current.read_transaction_current()
        else {
            panic!("exact dependencies must reach terminalization")
        };
        let crate::command_execution_failure::ExecutionFailureTerminalizeResult::StatusUnknown(
            uncertain,
        ) = checked.terminalize()
        else {
            panic!("unknown commit status must retain same-attempt evidence")
        };
        assert_eq!(
            uncertain.cause().kind(),
            StorageErrorKind::CommitStatusUnknown
        );
        assert_eq!(uncertain.expected_failure().pending(), &pending);
        assert_eq!(trace.lease_held_at_storage_action.get(), Some(true));

        let mut competing =
            manager.acquire_mut(keys.clone(), future_deadline(), CancellationToken::new());
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            competing.as_mut().poll(&mut context),
            Poll::Pending
        ));
        let repository = ScriptedRepository::new(
            lookup_candidates,
            AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::Pending(pending))),
            Rc::clone(&order),
        );
        let crate::command_execution_failure::UncertainExecutionFailureResolution::Retry(retry) =
            crate::command_execution_failure::resolve_uncertain_execution_failure(
                &repository,
                uncertain,
            )
        else {
            panic!("exact Pending lookup must prove noncommit and permit retry")
        };
        assert_eq!(retry.completed_attempts(), 1);
        drop(retry);
        runtime
            .block_on(competing)
            .expect("same-key recovery releases the retained attempt lease")
            .release();
    }

    #[test]
    fn successor_snapshot_is_normalized_before_runtime_and_retained_opaquely() {
        let (state, raw_snapshot, note) = evolved_row_fixture(None);
        let order = Rc::new(RefCell::new(Vec::new()));
        let repository = ScriptedRepository::new(
            state.lookup_candidates.clone(),
            AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::Pending(
                state.commit_context.pending().clone(),
            ))),
            Rc::clone(&order),
        );
        let snapshots = ScriptedSnapshotReader::new(
            state.snapshot_request.clone(),
            raw_snapshot,
            Rc::clone(&order),
        );
        let manager = manager();
        let runtime = runtime();

        let resolution = runtime
            .block_on(evaluate_next_command_attempt(
                state,
                &repository,
                &snapshots,
                &manager,
            ))
            .expect("normalized evaluation");
        let CommandAttemptResolution::Evaluated(attempt) = resolution else {
            panic!("Increment must require a commit")
        };
        assert_eq!(attempt.state.completed_attempts(), 1);
        let EntityObservation::Present(record) =
            &attempt.materialized_snapshot().snapshot().bindings()[0]
        else {
            panic!("present Row binding")
        };
        let note_position = record
            .fields()
            .fields()
            .binary_search_by_key(&note, |(field, _)| *field)
            .expect("catalog inserted optional note");
        assert_eq!(
            record.fields().fields()[note_position].1,
            CanonicalValue::Null
        );
        let mutated = attempt.evaluated().mutations()[0].post_image().fields();
        let note_position = mutated
            .fields()
            .binary_search_by_key(&note, |(field, _)| *field)
            .expect("runtime retained normalized optional note");
        assert_eq!(mutated.fields()[note_position].1, CanonicalValue::Null);
        assert_eq!(&*order.borrow(), &["pending-recheck", "snapshot"]);
        drop(attempt);
    }

    #[test]
    fn transaction_current_must_pass_through_the_exact_attempt_raw_proof() {
        let (state, raw_snapshot, note) = evolved_row_fixture(None);
        let EntityObservation::Present(raw_record) = &raw_snapshot.bindings()[0] else {
            panic!("present ancestor Row")
        };
        let mut explicit_fields = raw_record.fields().fields().to_vec();
        explicit_fields.push((note, CanonicalValue::Null));
        let explicit_record = StoredEntityRecordV1::new(
            raw_record.target().clone(),
            raw_record.entity_version(),
            raw_record.written_by_contract(),
            raw_record.schema_binding().clone(),
            CanonicalRecord::new(explicit_fields).expect("explicit-null fields"),
        )
        .expect("same-version explicit-null record");
        let explicit_snapshot = ReadSnapshot::new(
            &state.snapshot_request,
            None,
            vec![EntityObservation::Present(explicit_record)],
            raw_snapshot.root_validations().to_vec(),
            raw_snapshot.ranges().to_vec(),
        )
        .expect("explicit-null raw snapshot");

        let CommandSnapshotMaterialization::Ready(omitted_normalized) = state
            .resolved_plan
            .clone()
            .materialize_command_snapshot(raw_snapshot.clone())
            .expect("ancestor omission materializes")
        else {
            panic!("bounded ancestor omission")
        };
        let CommandSnapshotMaterialization::Ready(explicit_normalized) = state
            .resolved_plan
            .clone()
            .materialize_command_snapshot(explicit_snapshot.clone())
            .expect("explicit null materializes")
        else {
            panic!("bounded explicit null")
        };
        assert_eq!(
            omitted_normalized.snapshot().bindings(),
            explicit_normalized.snapshot().bindings(),
            "distinct raw states intentionally normalize to the same runtime value"
        );

        let explicit_current = TransactionCurrentState::new(
            &explicit_snapshot.validation_request(),
            explicit_snapshot.bindings().to_vec(),
            explicit_snapshot.root_validations().to_vec(),
            Vec::new(),
        )
        .expect("transaction-current explicit-null state");
        let order = Rc::new(RefCell::new(Vec::new()));
        let repository = ScriptedRepository::new(
            state.lookup_candidates.clone(),
            AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::Pending(
                state.commit_context.pending().clone(),
            ))),
            Rc::clone(&order),
        );
        let snapshots = ScriptedSnapshotReader::new(
            state.snapshot_request.clone(),
            raw_snapshot,
            Rc::clone(&order),
        );
        let manager = manager();
        let runtime = runtime();
        let resolution = runtime
            .block_on(evaluate_next_command_attempt(
                state,
                &repository,
                &snapshots,
                &manager,
            ))
            .expect("ancestor evaluation");
        let CommandAttemptResolution::Evaluated(attempt) = resolution else {
            panic!("Increment must require commit")
        };
        let bound = attempt
            .bind_provenance(provenance_id())
            .expect("bind exact provenance");
        assert!(
            bound
                .materialized_snapshot()
                .materialize_transaction_current(explicit_current)
                .is_err(),
            "same-version physical drift is an integrity failure, not a retryable dependency change"
        );
    }

    #[test]
    // req: BLK-020, TXN-010, TXN-011, TXN-012, TXN-013, TXN-040, TXN-041,
    // req: TXN-042, TXN-043, TXN-044
    fn expanded_graph_resource_limit_is_terminal_without_application_effects() {
        let desired_raw_bytes = MAX_CANONICAL_DOCUMENT_BYTES - 5;
        let (state, raw_snapshot, note) = evolved_row_fixture(Some(desired_raw_bytes));
        let exact_current = TransactionCurrentState::new(
            &raw_snapshot.validation_request(),
            raw_snapshot.bindings().to_vec(),
            raw_snapshot.root_validations().to_vec(),
            Vec::new(),
        )
        .expect("complete raw current state");
        let keys = state.raw_conflict_keys.clone();
        let order = Rc::new(RefCell::new(Vec::new()));
        let repository = ScriptedRepository::new(
            state.lookup_candidates.clone(),
            AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::Pending(
                state.commit_context.pending().clone(),
            ))),
            Rc::clone(&order),
        );
        let snapshots = ScriptedSnapshotReader::new(
            state.snapshot_request.clone(),
            raw_snapshot,
            Rc::clone(&order),
        );
        let manager = manager();
        let runtime = runtime();

        let resolution = runtime
            .block_on(evaluate_next_command_attempt(
                state,
                &repository,
                &snapshots,
                &manager,
            ))
            .expect("valid expansion overflow");
        let CommandAttemptResolution::ExecutionFault(attempt) = resolution else {
            panic!("overflow must be a dependency-sensitive execution fault")
        };
        let ExecutionFaultAttempt::ResourceLimit {
            state, evidence, ..
        } = &attempt
        else {
            panic!("catalog overflow cannot be paired with arithmetic")
        };
        assert_eq!(state.completed_attempts(), 1);
        let ResourceLimitFaultEvidence::Materialization(evidence) = evidence else {
            panic!("runtime must not replace catalog overflow evidence")
        };
        let EntityObservation::Present(raw) = &evidence.raw_snapshot().bindings()[0] else {
            panic!("present raw Row binding")
        };
        assert_eq!(
            encode_canonical_record(raw.fields())
                .expect("raw record encoding")
                .len(),
            desired_raw_bytes
        );
        assert!(
            raw.fields()
                .fields()
                .binary_search_by_key(&note, |(field, _)| *field)
                .is_err(),
            "overflow evidence must retain raw data, not a normalized value"
        );
        assert_eq!(&*order.borrow(), &["pending-recheck", "snapshot"]);
        let expected_pending = state.commit_context.pending().clone();
        order.borrow_mut().clear();
        let (port, trace) = instrumented_execution_failure_port(
            exact_current,
            FailureTerminalizeBehavior::Commit,
            manager.clone(),
            keys.clone(),
            Rc::clone(&order),
        );
        let crate::command_execution_failure::ExecutionFailureTransitionStart::Ready(current) =
            crate::command_execution_failure::begin_execution_failure_transition(&port, attempt)
        else {
            panic!("lineage resource evidence must enter exact current recheck")
        };
        let crate::command_execution_failure::ExecutionFailureCurrentDecision::Ready(checked) =
            current.read_transaction_current()
        else {
            panic!("exact raw lineage evidence must authorize terminalization")
        };
        let crate::command_execution_failure::ExecutionFailureTerminalizeResult::Terminalized(
            failure,
        ) = checked.terminalize()
        else {
            panic!("reproduced lineage overflow must become terminal")
        };
        assert_eq!(failure.code(), ExecutionFailureCode::ResourceLimit);
        assert_eq!(failure.pending(), &expected_pending);
        assert_eq!(trace.lease_held_at_storage_action.get(), Some(true));
        assert_eq!(
            &*order.borrow(),
            &[
                "failure-begin",
                "failure-current-read",
                "failure-terminalize",
                "failure-storage-drop",
            ]
        );
        acquire_and_release(&runtime, &manager, keys);
    }

    #[test]
    fn snapshot_adapter_target_drift_fails_before_materialization() {
        let (state, _, _) = evolved_row_fixture(None);
        let wrong_input = input_record(
            state.resolved_plan.plan().input().record(),
            [
                (
                    "idempotency_key",
                    CanonicalValue::string(SENSITIVE_MARKER).expect("bounded caller key"),
                ),
                ("tenant", CanonicalValue::Uuid([0x41; 16])),
                ("id", CanonicalValue::Uuid([0x43; 16])),
            ],
        );
        let facts = derive_input_command_facts(state.resolved_plan.plan(), wrong_input)
            .expect("wrong-target facts");
        let wrong_target = EntityTarget::new(
            state.resolved_plan.plan().bindings()[0].entity_type(),
            facts.binding_entity_keys()[0].clone(),
        )
        .expect("wrong target");
        assert_ne!(&wrong_target, &state.snapshot_request.binding_targets()[0]);
        let wrong_request = SnapshotRequest::new(
            state.snapshot_request.plan().clone(),
            vec![wrong_target.clone()],
            Vec::new(),
            Vec::new(),
        )
        .expect("wrong snapshot request");
        let wrong_snapshot = ReadSnapshot::new(
            &wrong_request,
            None,
            vec![EntityObservation::Absent(wrong_target)],
            Vec::new(),
            Vec::new(),
        )
        .expect("internally coherent wrong snapshot");
        assert!(!snapshot_matches_request(
            &state.snapshot_request,
            &wrong_snapshot
        ));

        let keys = state.raw_conflict_keys.clone();
        let order = Rc::new(RefCell::new(Vec::new()));
        let repository = ScriptedRepository::new(
            state.lookup_candidates.clone(),
            AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::Pending(
                state.commit_context.pending().clone(),
            ))),
            Rc::clone(&order),
        );
        let snapshots = ScriptedSnapshotReader::new(
            state.snapshot_request.clone(),
            wrong_snapshot,
            Rc::clone(&order),
        );
        let manager = manager();
        let runtime = runtime();

        assert!(matches!(
            runtime.block_on(evaluate_next_command_attempt(
                state,
                &repository,
                &snapshots,
                &manager,
            )),
            Err(CommandAttemptError::Integrity)
        ));
        assert_eq!(&*order.borrow(), &["pending-recheck", "snapshot"]);
        acquire_and_release(&runtime, &manager, keys);
    }

    #[test]
    fn pending_recheck_control_point_skips_snapshot_but_terminal_replay_wins() {
        let runtime = runtime();

        let (state, snapshot) = execution_fixture();
        let keys = state.raw_conflict_keys.clone();
        let cancellation = state.cancellation.clone();
        let order = Rc::new(RefCell::new(Vec::new()));
        let repository = ScriptedRepository::new(
            state.lookup_candidates.clone(),
            AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::Pending(
                state.commit_context.pending().clone(),
            ))),
            Rc::clone(&order),
        )
        .cancelling_during_lookup(cancellation);
        let snapshots = ScriptedSnapshotReader::new(
            state.snapshot_request.clone(),
            snapshot,
            Rc::clone(&order),
        );
        let pending_manager = manager();
        assert!(matches!(
            runtime.block_on(evaluate_next_command_attempt(
                state,
                &repository,
                &snapshots,
                &pending_manager,
            )),
            Err(CommandAttemptError::Cancelled)
        ));
        assert_eq!(repository.calls.get(), 1);
        assert_eq!(snapshots.calls.get(), 0);
        assert_eq!(&*order.borrow(), &["pending-recheck"]);
        acquire_and_release(&runtime, &pending_manager, keys);

        let (state, snapshot) = execution_fixture();
        let keys = state.raw_conflict_keys.clone();
        let cancellation = state.cancellation.clone();
        let outcome = outcome_from(
            &state.commit_context,
            state.commit_context.pending().admission_request_id(),
            state.commit_context.conflict_hashes().to_vec(),
        );
        let order = Rc::new(RefCell::new(Vec::new()));
        let repository = ScriptedRepository::new(
            state.lookup_candidates.clone(),
            AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::StoredOutcome(
                outcome,
            ))),
            Rc::clone(&order),
        )
        .cancelling_during_lookup(cancellation);
        let snapshots = ScriptedSnapshotReader::new(
            state.snapshot_request.clone(),
            snapshot,
            Rc::clone(&order),
        );
        let replay_manager = manager();
        assert!(matches!(
            runtime.block_on(evaluate_next_command_attempt(
                state,
                &repository,
                &snapshots,
                &replay_manager,
            )),
            Ok(CommandAttemptResolution::OutcomeReplay(_))
        ));
        assert_eq!(repository.calls.get(), 1);
        assert_eq!(snapshots.calls.get(), 0);
        assert_eq!(&*order.borrow(), &["pending-recheck"]);
        acquire_and_release(&runtime, &replay_manager, keys);
    }

    #[test]
    fn terminal_replay_skips_snapshot_and_all_other_states_fail_closed() {
        let runtime = runtime();
        for replay_failure in [false, true] {
            let (state, snapshot) = execution_fixture();
            let keys = state.raw_conflict_keys.clone();
            let terminal = if replay_failure {
                StoredAdmissionStateV1::ExecutionFailed(StoredExecutionFailedV1::new(
                    state.commit_context.pending().clone(),
                    ExecutionFailureCode::ArithmeticFault,
                ))
            } else {
                StoredAdmissionStateV1::StoredOutcome(outcome_from(
                    &state.commit_context,
                    state.commit_context.pending().admission_request_id(),
                    state.commit_context.conflict_hashes().to_vec(),
                ))
            };
            let order = Rc::new(RefCell::new(Vec::new()));
            let repository = ScriptedRepository::new(
                state.lookup_candidates.clone(),
                AdmissionLookupResultV1::Found(Box::new(terminal)),
                Rc::clone(&order),
            );
            let snapshots = ScriptedSnapshotReader::new(
                state.snapshot_request.clone(),
                snapshot,
                Rc::clone(&order),
            );
            let manager = manager();

            let replay = runtime
                .block_on(evaluate_next_command_attempt(
                    state,
                    &repository,
                    &snapshots,
                    &manager,
                ))
                .expect("matching terminal replay");
            if replay_failure {
                assert!(matches!(
                    replay,
                    CommandAttemptResolution::ExecutionFailureReplay(_)
                ));
            } else {
                assert!(matches!(replay, CommandAttemptResolution::OutcomeReplay(_)));
            }
            assert_eq!(snapshots.calls.get(), 0);
            assert_eq!(&*order.borrow(), &["pending-recheck"]);
            acquire_and_release(&runtime, &manager, keys);
        }

        for terminal in [Some(AdmissionLookupResultV1::MultipleMatches), None] {
            let (state, snapshot) = execution_fixture();
            let keys = state.raw_conflict_keys.clone();
            let result = terminal.unwrap_or_else(|| {
                let mismatched = outcome_from(
                    &state.commit_context,
                    state.commit_context.pending().admission_request_id(),
                    Vec::new(),
                );
                AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::StoredOutcome(
                    mismatched,
                )))
            });
            let order = Rc::new(RefCell::new(Vec::new()));
            let repository =
                ScriptedRepository::new(state.lookup_candidates.clone(), result, Rc::clone(&order));
            let snapshots = ScriptedSnapshotReader::new(
                state.snapshot_request.clone(),
                snapshot,
                Rc::clone(&order),
            );
            let manager = manager();

            assert!(matches!(
                runtime.block_on(evaluate_next_command_attempt(
                    state,
                    &repository,
                    &snapshots,
                    &manager,
                )),
                Err(CommandAttemptError::Integrity)
            ));
            assert_eq!(snapshots.calls.get(), 0);
            assert_eq!(&*order.borrow(), &["pending-recheck"]);
            acquire_and_release(&runtime, &manager, keys);
        }
    }

    #[test]
    fn cancellation_before_acquire_or_after_snapshot_releases_all_capabilities() {
        let runtime = runtime();

        let (state, snapshot) = execution_fixture();
        let keys = state.raw_conflict_keys.clone();
        state.cancellation.cancel();
        let order = Rc::new(RefCell::new(Vec::new()));
        let repository = ScriptedRepository::new(
            state.lookup_candidates.clone(),
            AdmissionLookupResultV1::NotFound,
            Rc::clone(&order),
        );
        let snapshots = ScriptedSnapshotReader::new(
            state.snapshot_request.clone(),
            snapshot,
            Rc::clone(&order),
        );
        let first_manager = manager();
        assert!(matches!(
            runtime.block_on(evaluate_next_command_attempt(
                state,
                &repository,
                &snapshots,
                &first_manager,
            )),
            Err(CommandAttemptError::Cancelled)
        ));
        assert_eq!(repository.calls.get(), 0);
        assert_eq!(snapshots.calls.get(), 0);
        assert!(order.borrow().is_empty());
        acquire_and_release(&runtime, &first_manager, keys);

        let (state, snapshot) = execution_fixture();
        let keys = state.raw_conflict_keys.clone();
        let cancellation = state.cancellation.clone();
        let order = Rc::new(RefCell::new(Vec::new()));
        let repository = ScriptedRepository::new(
            state.lookup_candidates.clone(),
            AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::Pending(
                state.commit_context.pending().clone(),
            ))),
            Rc::clone(&order),
        );
        let snapshots = ScriptedSnapshotReader::new(
            state.snapshot_request.clone(),
            snapshot,
            Rc::clone(&order),
        )
        .cancelling(cancellation);
        let second_manager = manager();
        assert!(matches!(
            runtime.block_on(evaluate_next_command_attempt(
                state,
                &repository,
                &snapshots,
                &second_manager,
            )),
            Err(CommandAttemptError::Cancelled)
        ));
        assert_eq!(repository.calls.get(), 1);
        assert_eq!(snapshots.calls.get(), 1);
        assert_eq!(&*order.borrow(), &["pending-recheck", "snapshot"]);
        acquire_and_release(&runtime, &second_manager, keys);
    }

    #[test]
    fn three_real_evaluations_are_allowed_and_the_fourth_touches_no_port() {
        let (mut state, snapshot) = execution_fixture();
        let order = Rc::new(RefCell::new(Vec::new()));
        let repository = ScriptedRepository::new(
            state.lookup_candidates.clone(),
            AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::Pending(
                state.commit_context.pending().clone(),
            ))),
            Rc::clone(&order),
        );
        let snapshots = ScriptedSnapshotReader::new(
            state.snapshot_request.clone(),
            snapshot,
            Rc::clone(&order),
        );
        let manager = manager();
        let runtime = runtime();

        for expected in 1..=MAX_COMMAND_EVALUATION_ATTEMPTS_V1 {
            let resolution = runtime
                .block_on(evaluate_next_command_attempt(
                    state,
                    &repository,
                    &snapshots,
                    &manager,
                ))
                .expect("permitted evaluation");
            let CommandAttemptResolution::Evaluated(attempt) = resolution else {
                panic!("CreateBudget must evaluate")
            };
            assert_eq!(attempt.state.completed_attempts(), expected);
            assert_eq!(attempt.evaluated().mutations().len(), 1);
            state = attempt.into_retry_state_for_test();
        }

        let calls_before_fourth = (repository.calls.get(), snapshots.calls.get());
        assert!(matches!(
            runtime.block_on(evaluate_next_command_attempt(
                state,
                &repository,
                &snapshots,
                &manager,
            )),
            Err(CommandAttemptError::RetryBudgetExhausted)
        ));
        assert_eq!(
            calls_before_fourth,
            (
                MAX_COMMAND_EVALUATION_ATTEMPTS_V1,
                MAX_COMMAND_EVALUATION_ATTEMPTS_V1
            )
        );
        assert_eq!(
            (repository.calls.get(), snapshots.calls.get()),
            calls_before_fourth
        );
        assert_eq!(order.borrow().len(), MAX_COMMAND_EVALUATION_ATTEMPTS_V1 * 2);
    }

    #[test]
    fn cancellation_precedes_deadline_at_request_control_safe_points() {
        let expired = Instant::now()
            .checked_sub(Duration::from_secs(1))
            .expect("expired deadline");
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(matches!(
            check_request_control(expired, &cancelled),
            Err(CommandAttemptError::Cancelled)
        ));

        assert!(matches!(
            check_request_control(expired, &CancellationToken::new()),
            Err(CommandAttemptError::DeadlineExceeded)
        ));
        let future = Instant::now()
            .checked_add(Duration::from_secs(30))
            .expect("future deadline");
        assert!(check_request_control(future, &CancellationToken::new()).is_ok());
    }

    #[test]
    fn runtime_context_uses_the_original_admission_identity_and_values() {
        let context = commit_context();
        let pending = context.pending();
        let later_invocation = request_id(2);
        assert_ne!(later_invocation, pending.admission_request_id());

        let runtime = transaction_context(&context);
        assert_eq!(runtime.request_id(), pending.admission_request_id());
        assert_eq!(runtime.actor(), pending.actor());
        assert_eq!(runtime.plan(), pending.plan());
        assert_eq!(runtime.tx_time(), pending.logical_time());
        assert_eq!(runtime.partition_key(), pending.partition_key());
    }

    #[test]
    fn terminal_outcome_match_requires_the_exact_admitted_commit_context() {
        let context = commit_context();
        let exact = outcome_from(
            &context,
            context.pending().admission_request_id(),
            context.conflict_hashes().to_vec(),
        );
        assert!(outcome_matches_context(&exact, &context));

        let wrong_request =
            outcome_from(&context, request_id(9), context.conflict_hashes().to_vec());
        assert!(!outcome_matches_context(&wrong_request, &context));
        let wrong_conflicts = outcome_from(
            &context,
            context.pending().admission_request_id(),
            Vec::new(),
        );
        assert!(!outcome_matches_context(&wrong_conflicts, &context));
    }

    #[test]
    fn attempt_diagnostics_are_static_and_redacted() {
        for error in [
            CommandAttemptError::Cancelled,
            CommandAttemptError::DeadlineExceeded,
            CommandAttemptError::RetryBudgetExhausted,
            CommandAttemptError::Integrity,
        ] {
            let debug = format!("{error:?}");
            assert_eq!(debug, "CommandAttemptError([REDACTED])");
            assert_eq!(
                error.to_string(),
                "command evaluation attempt could not be completed"
            );
            assert!(!debug.contains(PRINCIPAL));
            assert!(!debug.contains(SENSITIVE_MARKER));
        }

        let context = commit_context();
        let outcome = outcome_from(
            &context,
            context.pending().admission_request_id(),
            context.conflict_hashes().to_vec(),
        );
        let resolution = CommandAttemptResolution::OutcomeReplay(outcome);
        let debug = format!("{resolution:?}");
        assert_eq!(debug, "CommandAttemptResolution::OutcomeReplay([REDACTED])");
        assert!(!debug.contains(PRINCIPAL));
        assert!(!debug.contains(SENSITIVE_MARKER));
    }
}
