//! Sealed dependency-validated deterministic execution-failure transition.

use std::fmt;

use riffdb_storage_api::{
    AdmissionLookupResultV1, AdmissionRepository, ExecutionFailureAdmissionRechecked,
    ExecutionFailureAdmissionResult, ExecutionFailureAwaitingDecision,
    ExecutionFailureTransitionPort, IdempotencyLookupCandidatesV1, StorageError, StorageErrorKind,
    StoredAdmissionStateV1, StoredExecutionFailedV1, StoredOutcomeV1,
};

use crate::{
    command_attempt::{
        ExecutionFaultAttempt, ExecutionFaultCurrentRecheck, PendingCommandAttempts,
    },
    command_validation::dependencies_from_current,
};

/// Closed result of opening the exact Pending-to-failure storage transition.
pub(super) enum ExecutionFailureTransitionStart<R> {
    /// The exact Pending admission remains and current dependencies may be read.
    Ready(Box<BoundExecutionFailureCurrentRead<R>>),
    /// A concurrent equal-input command commit became authoritative first.
    OutcomeReplay(StoredOutcomeV1),
    /// The exact deterministic failure is already terminal.
    ExecutionFailureReplay(StoredExecutionFailedV1),
    /// No terminal write was requested and storage could not open or recheck.
    StorageFailure(StorageError),
    /// Durable admission state contradicted the retained attempt.
    Integrity,
}

/// The sole current-read carrier for an exact execution-fault attempt.
pub(super) struct BoundExecutionFailureCurrentRead<R> {
    // Storage state must roll back before the attempt releases its logical lease.
    rechecked: R,
    expected_failure: StoredExecutionFailedV1,
    attempt: ExecutionFaultAttempt,
}

/// Opens and rechecks one short transition derived only from the retained attempt.
pub(super) fn begin_execution_failure_transition<P>(
    port: &P,
    attempt: ExecutionFaultAttempt,
) -> ExecutionFailureTransitionStart<P::Rechecked>
where
    P: ExecutionFailureTransitionPort,
{
    let request = match attempt.transition_request() {
        Ok(request) => request,
        Err(()) => return ExecutionFailureTransitionStart::Integrity,
    };
    let expected_failure = request.terminal_record();
    let result = match port.begin_execution_failure(request) {
        Ok(result) => result,
        Err(error) => return ExecutionFailureTransitionStart::StorageFailure(error),
    };
    match result {
        ExecutionFailureAdmissionResult::Rechecked(rechecked) => {
            ExecutionFailureTransitionStart::Ready(Box::new(BoundExecutionFailureCurrentRead {
                rechecked,
                expected_failure,
                attempt,
            }))
        }
        ExecutionFailureAdmissionResult::StoredOutcome(outcome) => {
            if attempt.matches_outcome(&outcome) {
                ExecutionFailureTransitionStart::OutcomeReplay(outcome)
            } else {
                ExecutionFailureTransitionStart::Integrity
            }
        }
        ExecutionFailureAdmissionResult::ExecutionFailed(failure) => {
            if attempt.matches_failure(&failure) {
                ExecutionFailureTransitionStart::ExecutionFailureReplay(failure)
            } else {
                ExecutionFailureTransitionStart::Integrity
            }
        }
        ExecutionFailureAdmissionResult::Missing
        | ExecutionFailureAdmissionResult::PendingMismatch => {
            ExecutionFailureTransitionStart::Integrity
        }
    }
}

/// Closed result after the exact current state is read and checked.
pub(super) enum ExecutionFailureCurrentDecision<A> {
    /// Complete dependency and opaque physical evidence remained exact.
    Ready(Box<CheckedExecutionFailureTerminalization<A>>),
    /// Changed evidence was abandoned and may consume another bounded attempt slot.
    Retry(Box<PendingCommandAttempts>),
    /// No terminal write was requested and the current read failed.
    StorageFailure(StorageError),
    /// Current evidence was malformed or contradicted the retained attempt.
    Integrity,
}

/// Unforgeable terminalization authority retaining its exact storage state.
pub(super) struct CheckedExecutionFailureTerminalization<A> {
    // Storage state must roll back before the attempt releases its logical lease.
    awaiting: A,
    expected_failure: StoredExecutionFailedV1,
    attempt: ExecutionFaultAttempt,
}

impl<R> BoundExecutionFailureCurrentRead<R>
where
    R: ExecutionFailureAdmissionRechecked,
{
    /// Reads and compares every dependency before invoking opaque catalog recheck.
    pub(super) fn read_transaction_current(
        self,
    ) -> ExecutionFailureCurrentDecision<R::AwaitingDecision> {
        let Self {
            rechecked,
            expected_failure,
            attempt,
        } = self;
        let (awaiting, current) = match rechecked.read_transaction_current() {
            Ok(result) => result,
            Err(error) => {
                drop(attempt);
                return ExecutionFailureCurrentDecision::StorageFailure(error);
            }
        };
        let current_dependencies = match dependencies_from_current(&current) {
            Ok(dependencies) => dependencies,
            Err(_) => {
                awaiting.abandon();
                drop(attempt);
                return ExecutionFailureCurrentDecision::Integrity;
            }
        };
        if current_dependencies != *attempt.read_dependencies() {
            awaiting.abandon();
            return ExecutionFailureCurrentDecision::Retry(Box::new(
                attempt.recover_pending_after_proven_rollback(),
            ));
        }
        match attempt.recheck_equal_transaction_current(current) {
            ExecutionFaultCurrentRecheck::Stable => ExecutionFailureCurrentDecision::Ready(
                Box::new(CheckedExecutionFailureTerminalization {
                    awaiting,
                    expected_failure,
                    attempt,
                }),
            ),
            ExecutionFaultCurrentRecheck::DependencyChanged
            | ExecutionFaultCurrentRecheck::Integrity => {
                awaiting.abandon();
                drop(attempt);
                ExecutionFailureCurrentDecision::Integrity
            }
        }
    }
}

/// Closed result after the only call that may make ExecutionFailed durable.
pub(super) enum ExecutionFailureTerminalizeResult {
    /// The exact expected terminal record is durable.
    Terminalized(Box<StoredExecutionFailedV1>),
    /// Storage proved the transition did not commit.
    ProvenAbort(StorageError),
    /// Storage cannot distinguish commit from rollback; writes must now be fenced.
    StatusUnknown(Box<UncertainExecutionFailureTransition>),
    /// Storage returned a successful record that contradicted checked authority.
    Integrity,
}

impl<A> CheckedExecutionFailureTerminalization<A>
where
    A: ExecutionFailureAwaitingDecision,
{
    /// Attempts the exact terminal transition while retaining the logical lease.
    #[cfg(test)]
    pub(super) fn terminalize(self) -> ExecutionFailureTerminalizeResult {
        self.terminalize_with_audit(None)
    }

    /// Attempts the exact terminal transition with an optional atomic service
    /// lifecycle append.
    pub(super) fn terminalize_with_audit(
        self,
        audit: Option<riffdb_storage_api::CommandServiceAuditTransitionV1>,
    ) -> ExecutionFailureTerminalizeResult {
        let Self {
            awaiting,
            expected_failure,
            attempt,
        } = self;
        let result = match audit {
            Some(audit) => awaiting
                .terminalize_with_service_audit(audit)
                .map(riffdb_storage_api::AuditedExecutionFailureV1::into_parts)
                .map(|(failure, _)| failure),
            None => awaiting.terminalize(),
        };
        match result {
            Ok(actual) if actual == expected_failure => {
                drop(attempt);
                ExecutionFailureTerminalizeResult::Terminalized(Box::new(actual))
            }
            Ok(_) => {
                drop(attempt);
                ExecutionFailureTerminalizeResult::Integrity
            }
            Err(cause) if cause.kind() == StorageErrorKind::CommitStatusUnknown => {
                let lookup_candidates = attempt.lookup_candidates().clone();
                ExecutionFailureTerminalizeResult::StatusUnknown(Box::new(
                    UncertainExecutionFailureTransition {
                        cause,
                        lookup_candidates,
                        expected_failure,
                        attempt,
                    },
                ))
            }
            Err(error) => {
                drop(attempt);
                ExecutionFailureTerminalizeResult::ProvenAbort(error)
            }
        }
    }
}

/// Same-attempt evidence retained after an uncertain terminalization commit.
pub(super) struct UncertainExecutionFailureTransition {
    cause: StorageError,
    lookup_candidates: IdempotencyLookupCandidatesV1,
    expected_failure: StoredExecutionFailedV1,
    attempt: ExecutionFaultAttempt,
}

impl UncertainExecutionFailureTransition {
    /// Borrows the original uncertain commit failure for trusted telemetry.
    pub(super) const fn cause(&self) -> &StorageError {
        &self.cause
    }

    /// Borrows the exact failure record that may have become durable.
    #[allow(dead_code)] // Semantic-test inspection of retained uncertainty evidence.
    pub(super) const fn expected_failure(&self) -> &StoredExecutionFailedV1 {
        &self.expected_failure
    }
}

/// Closed result of one same-key lookup while command writes remain fenced.
pub(super) enum UncertainExecutionFailureResolution {
    /// The exact expected failure became terminal.
    ExecutionFailureReplay(StoredExecutionFailedV1),
    /// A concurrent equal-input command commit became authoritative.
    OutcomeReplay(StoredOutcomeV1),
    /// Exact Pending proves noncommit and permits another bounded attempt.
    Retry(Box<PendingCommandAttempts>),
    /// The read failed and retains both causes plus the exact live attempt.
    OutcomeUnknown {
        uncertain: Box<UncertainExecutionFailureTransition>,
        lookup_error: StorageError,
    },
    /// Durable state contradicted the retained same-attempt evidence.
    Integrity,
}

/// Performs one exact same-key lookup and never begins another transition.
pub(super) fn resolve_uncertain_execution_failure(
    repository: &dyn AdmissionRepository,
    uncertain: Box<UncertainExecutionFailureTransition>,
) -> UncertainExecutionFailureResolution {
    let valid = uncertain.cause.kind() == StorageErrorKind::CommitStatusUnknown
        && uncertain
            .lookup_candidates
            .contains(uncertain.attempt.pending().identity())
        && uncertain
            .attempt
            .transition_request()
            .is_ok_and(|request| request.terminal_record() == uncertain.expected_failure);
    if !valid {
        return UncertainExecutionFailureResolution::Integrity;
    }
    let durable = match repository.lookup_admission(uncertain.lookup_candidates.clone()) {
        Ok(durable) => durable,
        Err(lookup_error) => {
            return UncertainExecutionFailureResolution::OutcomeUnknown {
                uncertain,
                lookup_error,
            };
        }
    };
    match durable {
        AdmissionLookupResultV1::Found(state) => match *state {
            StoredAdmissionStateV1::ExecutionFailed(failure)
                if uncertain.attempt.matches_failure(&failure) =>
            {
                UncertainExecutionFailureResolution::ExecutionFailureReplay(failure)
            }
            StoredAdmissionStateV1::StoredOutcome(outcome)
                if uncertain.attempt.matches_outcome(&outcome) =>
            {
                UncertainExecutionFailureResolution::OutcomeReplay(outcome)
            }
            StoredAdmissionStateV1::Pending(pending) if &pending == uncertain.attempt.pending() => {
                let UncertainExecutionFailureTransition { attempt, .. } = *uncertain;
                UncertainExecutionFailureResolution::Retry(Box::new(
                    attempt.recover_pending_after_proven_rollback(),
                ))
            }
            StoredAdmissionStateV1::Pending(_)
            | StoredAdmissionStateV1::StoredOutcome(_)
            | StoredAdmissionStateV1::ExecutionFailed(_) => {
                UncertainExecutionFailureResolution::Integrity
            }
        },
        AdmissionLookupResultV1::NotFound
            if matches!(
                uncertain.attempt.transition_request().as_ref().map(
                    riffdb_storage_api::ExecutionFailureTransitionRequestV1::admission_expectation
                ),
                Ok(riffdb_storage_api::CommandAdmissionExpectationV1::Vacant(_))
            ) =>
        {
            let UncertainExecutionFailureTransition { attempt, .. } = *uncertain;
            UncertainExecutionFailureResolution::Retry(Box::new(
                attempt.recover_pending_after_proven_rollback(),
            ))
        }
        AdmissionLookupResultV1::NotFound | AdmissionLookupResultV1::MultipleMatches => {
            UncertainExecutionFailureResolution::Integrity
        }
    }
}

impl<R> fmt::Debug for ExecutionFailureTransitionStart<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Ready(_) => "ExecutionFailureTransitionStart::Ready([REDACTED])",
            Self::OutcomeReplay(_) => "ExecutionFailureTransitionStart::OutcomeReplay([REDACTED])",
            Self::ExecutionFailureReplay(_) => {
                "ExecutionFailureTransitionStart::ExecutionFailureReplay([REDACTED])"
            }
            Self::StorageFailure(_) => {
                "ExecutionFailureTransitionStart::StorageFailure([REDACTED])"
            }
            Self::Integrity => "ExecutionFailureTransitionStart::Integrity",
        })
    }
}

impl<A> fmt::Debug for ExecutionFailureCurrentDecision<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Ready(_) => "ExecutionFailureCurrentDecision::Ready([REDACTED])",
            Self::Retry(_) => "ExecutionFailureCurrentDecision::Retry([REDACTED])",
            Self::StorageFailure(_) => {
                "ExecutionFailureCurrentDecision::StorageFailure([REDACTED])"
            }
            Self::Integrity => "ExecutionFailureCurrentDecision::Integrity",
        })
    }
}

impl fmt::Debug for ExecutionFailureTerminalizeResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Terminalized(_) => "ExecutionFailureTerminalizeResult::Terminalized([REDACTED])",
            Self::ProvenAbort(_) => "ExecutionFailureTerminalizeResult::ProvenAbort([REDACTED])",
            Self::StatusUnknown(_) => {
                "ExecutionFailureTerminalizeResult::StatusUnknown([REDACTED])"
            }
            Self::Integrity => "ExecutionFailureTerminalizeResult::Integrity",
        })
    }
}

impl fmt::Debug for UncertainExecutionFailureTransition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("UncertainExecutionFailureTransition([REDACTED])")
    }
}

impl fmt::Debug for UncertainExecutionFailureResolution {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ExecutionFailureReplay(_) => {
                "UncertainExecutionFailureResolution::ExecutionFailureReplay([REDACTED])"
            }
            Self::OutcomeReplay(_) => {
                "UncertainExecutionFailureResolution::OutcomeReplay([REDACTED])"
            }
            Self::Retry(_) => "UncertainExecutionFailureResolution::Retry([REDACTED])",
            Self::OutcomeUnknown { .. } => {
                "UncertainExecutionFailureResolution::OutcomeUnknown([REDACTED])"
            }
            Self::Integrity => "UncertainExecutionFailureResolution::Integrity",
        })
    }
}
