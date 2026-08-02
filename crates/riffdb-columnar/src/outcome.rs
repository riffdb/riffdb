//! Typed projection lifecycle outcomes (D7).

use std::sync::Arc;

use riffdb_types::{CommitSequence, FrontierPosition};

use crate::definition::DefinitionFingerprint;
use crate::query::QueryResult;
use crate::store::ColumnarSnapshot;

/// Successful ready outcome with a queryable snapshot.
#[derive(Clone, Debug)]
pub struct ProjectionReady {
    /// Published snapshot.
    pub snapshot: Arc<ColumnarSnapshot>,
    /// Visible frontier of the snapshot.
    pub frontier: FrontierPosition,
    /// Application head known at query time (may equal frontier when caught up).
    pub head: FrontierPosition,
    /// Optional embedded query result when a query was executed.
    pub result: Option<QueryResult>,
}

/// Initial catch-up has not yet produced a published snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionBuilding {
    /// Processed position during catch-up.
    pub applied_through: FrontierPosition,
    /// Known application head.
    pub head: FrontierPosition,
}

/// Definition fingerprint mismatch at open.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionInvalid {
    /// Fingerprint expected by the registered definition.
    pub expected_fingerprint: DefinitionFingerprint,
    /// Fingerprint found in the durable manifest.
    pub found_fingerprint: DefinitionFingerprint,
}

/// Causal/bounded freshness lag (shape only in CP1; wiring is CP2).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionLagging {
    /// Required frontier token / sequence.
    pub required: FrontierPosition,
    /// Current projection frontier.
    pub current: FrontierPosition,
    /// Application head.
    pub head: FrontierPosition,
    /// Sequence distance backlog (head - current), when both are sequenced.
    pub lag_sequences: Option<u64>,
    /// Optional retry hint in milliseconds (data shape only).
    pub retry_after_ms: Option<u64>,
}

/// Rebuild in progress after detach/rebuild policy (shape only).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionRebuilding {
    /// Human-safe reason code.
    pub reason: &'static str,
    /// Progress numerator (applied commits during rebuild).
    pub progress_applied: u64,
    /// Progress denominator hint (0 = unknown).
    pub progress_total: u64,
}

/// Degraded health (shape only).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionDegraded {
    /// Human-safe reason code.
    pub reason: &'static str,
    /// Current frontier while degraded.
    pub current_frontier: FrontierPosition,
}

/// Engine-level lifecycle outcome enum.
#[derive(Clone, Debug)]
pub enum ColumnarOutcome {
    /// Snapshot is queryable.
    Ready(ProjectionReady),
    /// Catch-up before first publication.
    Building(ProjectionBuilding),
    /// Manifest fingerprint mismatch.
    Invalid(ProjectionInvalid),
    /// Freshness policy not yet satisfied (CP2 wiring).
    Lagging(ProjectionLagging),
    /// Rebuild required/in progress (CP2/CP3 wiring).
    Rebuilding(ProjectionRebuilding),
    /// Degraded but serving (CP2/CP3 wiring).
    Degraded(ProjectionDegraded),
}

impl ColumnarOutcome {
    /// Builds a Ready outcome.
    #[must_use]
    pub fn ready(
        snapshot: Arc<ColumnarSnapshot>,
        head: FrontierPosition,
        result: Option<QueryResult>,
    ) -> Self {
        let frontier = snapshot.visible_frontier;
        Self::Ready(ProjectionReady {
            snapshot,
            frontier,
            head,
            result,
        })
    }

    /// Builds a Building outcome.
    #[must_use]
    pub const fn building(applied_through: FrontierPosition, head: FrontierPosition) -> Self {
        Self::Building(ProjectionBuilding {
            applied_through,
            head,
        })
    }

    /// Builds an Invalid outcome.
    #[must_use]
    pub const fn invalid(
        expected_fingerprint: DefinitionFingerprint,
        found_fingerprint: DefinitionFingerprint,
    ) -> Self {
        Self::Invalid(ProjectionInvalid {
            expected_fingerprint,
            found_fingerprint,
        })
    }
}

/// Sequence distance between two frontiers when both are sequenced.
#[must_use]
pub fn frontier_lag_sequences(current: FrontierPosition, head: FrontierPosition) -> Option<u64> {
    match (current, head) {
        (FrontierPosition::BeforeFirst, FrontierPosition::AppliedThrough(head_seq)) => {
            Some(head_seq.get())
        }
        (
            FrontierPosition::AppliedThrough(current_seq),
            FrontierPosition::AppliedThrough(head_seq),
        ) => head_seq.get().checked_sub(current_seq.get()),
        _ => None,
    }
}

/// Helper for shape tests constructing a lagging outcome.
#[must_use]
pub fn lagging_for(
    required: CommitSequence,
    current: FrontierPosition,
    head: FrontierPosition,
) -> ColumnarOutcome {
    ColumnarOutcome::Lagging(ProjectionLagging {
        required: FrontierPosition::AppliedThrough(required),
        current,
        head,
        lag_sequences: frontier_lag_sequences(current, head),
        retry_after_ms: Some(50),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::definition::DefinitionFingerprint;

    #[test]
    fn lagging_shape_carries_required_current_head() {
        let required = CommitSequence::new(10).expect("seq");
        let current = FrontierPosition::AppliedThrough(CommitSequence::new(7).expect("seq"));
        let head = FrontierPosition::AppliedThrough(CommitSequence::new(12).expect("seq"));
        let outcome = lagging_for(required, current, head);
        match outcome {
            ColumnarOutcome::Lagging(lag) => {
                assert_eq!(lag.required, FrontierPosition::AppliedThrough(required));
                assert_eq!(lag.current, current);
                assert_eq!(lag.head, head);
                assert_eq!(lag.lag_sequences, Some(5));
            }
            _ => panic!("expected lagging"),
        }
    }

    #[test]
    fn rebuilding_and_degraded_shapes() {
        let rebuilding = ColumnarOutcome::Rebuilding(ProjectionRebuilding {
            reason: "replay_budget_exceeded",
            progress_applied: 3,
            progress_total: 10,
        });
        assert!(matches!(rebuilding, ColumnarOutcome::Rebuilding(_)));
        let degraded = ColumnarOutcome::Degraded(ProjectionDegraded {
            reason: "apply_lag_slo",
            current_frontier: FrontierPosition::BeforeFirst,
        });
        assert!(matches!(degraded, ColumnarOutcome::Degraded(_)));
        let invalid = ColumnarOutcome::invalid(
            DefinitionFingerprint::from_bytes([1; 32]),
            DefinitionFingerprint::from_bytes([2; 32]),
        );
        assert!(matches!(invalid, ColumnarOutcome::Invalid(_)));
    }
}
