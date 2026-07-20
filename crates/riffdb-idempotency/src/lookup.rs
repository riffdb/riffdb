//! Pure classification of durable command-idempotency lookup state.

use std::fmt;

use riffdb_storage_api::{
    AdmissionLookupResultV1, StoredAdmissionStateV1, StoredExecutionFailedV1, StoredOutcomeV1,
    StoredPendingAdmissionV1,
};
use riffdb_types::CanonicalInputHash;

/// The closed coordinator disposition for one rotation-aware durable lookup.
pub enum IdempotencyLookupClassificationV1 {
    /// No candidate has durable state; a new admission may be proposed.
    Absent,
    /// Equal input found an unchanged pending admission to resume or await.
    Pending(StoredPendingAdmissionV1),
    /// Equal input found the original declared terminal outcome.
    Outcome(StoredOutcomeV1),
    /// Equal input found a deterministic terminal execution failure.
    ExecutionFailed(StoredExecutionFailedV1),
    /// An identity existed for different canonical input; evaluation is forbidden.
    InputMismatch,
    /// More than one rotation candidate matched; storage integrity failed closed.
    MultipleMatches,
}

impl fmt::Debug for IdempotencyLookupClassificationV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = match self {
            Self::Absent => "Absent",
            Self::Pending(_) => "Pending([REDACTED])",
            Self::Outcome(_) => "Outcome([REDACTED])",
            Self::ExecutionFailed(_) => "ExecutionFailed([REDACTED])",
            Self::InputMismatch => "InputMismatch",
            Self::MultipleMatches => "MultipleMatches",
        };
        formatter.write_str(state)
    }
}

/// Classifies a bounded durable lookup against the newly prepared input hash.
///
/// Matching state is returned unchanged for coordinator-owned resume or replay.
/// A mismatch and multiple match never expose prior input, identities, or result
/// content and must not proceed to command evaluation.
#[must_use]
pub fn classify_idempotency_lookup(
    lookup: AdmissionLookupResultV1,
    canonical_input_hash: CanonicalInputHash,
) -> IdempotencyLookupClassificationV1 {
    let state = match lookup {
        AdmissionLookupResultV1::NotFound => {
            return IdempotencyLookupClassificationV1::Absent;
        }
        AdmissionLookupResultV1::MultipleMatches => {
            return IdempotencyLookupClassificationV1::MultipleMatches;
        }
        AdmissionLookupResultV1::Found(state) => state,
    };

    let stored_hash = match state.as_ref() {
        StoredAdmissionStateV1::Pending(value) => value.canonical_input_hash(),
        StoredAdmissionStateV1::StoredOutcome(value) => value.canonical_input_hash(),
        StoredAdmissionStateV1::ExecutionFailed(value) => value.pending().canonical_input_hash(),
    };
    if stored_hash != canonical_input_hash {
        return IdempotencyLookupClassificationV1::InputMismatch;
    }

    match *state {
        StoredAdmissionStateV1::Pending(value) => IdempotencyLookupClassificationV1::Pending(value),
        StoredAdmissionStateV1::StoredOutcome(value) => {
            IdempotencyLookupClassificationV1::Outcome(value)
        }
        StoredAdmissionStateV1::ExecutionFailed(value) => {
            IdempotencyLookupClassificationV1::ExecutionFailed(value)
        }
    }
}
