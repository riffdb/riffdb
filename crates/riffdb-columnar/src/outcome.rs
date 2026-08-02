//! Typed projection lifecycle outcomes (D7).

use std::sync::Arc;

use riffdb_types::{CommitSequence, FrontierPosition, ProjectionFrontier};

use crate::definition::DefinitionFingerprint;
use crate::query::QueryResult;
use crate::store::ColumnarSnapshot;

/// Closed reason a projection is rebuilding (ADR-0086 §7 / §8).
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RebuildingReason {
    /// Replay budget (age/bytes/backlog) was breached; detach and rebuild.
    ReplayBudgetExceeded,
    /// Operator or policy requested an explicit rebuild.
    ExplicitRebuild,
    /// Durable projection state failed integrity checks and must be rebuilt.
    StateIntegrityFailure,
}

impl RebuildingReason {
    /// Stable semantic tag for exhaustiveness and wire mapping.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::ReplayBudgetExceeded => 0x01,
            Self::ExplicitRebuild => 0x02,
            Self::StateIntegrityFailure => 0x03,
        }
    }

    /// All known variants (exhaustiveness pin).
    #[must_use]
    pub const fn all() -> [Self; 3] {
        [
            Self::ReplayBudgetExceeded,
            Self::ExplicitRebuild,
            Self::StateIntegrityFailure,
        ]
    }
}

/// Closed reason a projection is degraded but still serving.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum DegradedReason {
    /// Lag SLO is violated while the projection remains queryable.
    ApplyLagSlo,
    /// Compaction or maintenance backlog is elevated.
    MaintenanceBacklog,
    /// Partial segment inventory; serving with reduced capacity.
    PartialInventory,
}

impl DegradedReason {
    /// Stable semantic tag for exhaustiveness and wire mapping.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::ApplyLagSlo => 0x01,
            Self::MaintenanceBacklog => 0x02,
            Self::PartialInventory => 0x03,
        }
    }

    /// All known variants (exhaustiveness pin).
    #[must_use]
    pub const fn all() -> [Self; 3] {
        [
            Self::ApplyLagSlo,
            Self::MaintenanceBacklog,
            Self::PartialInventory,
        ]
    }
}

/// Successful ready outcome with a queryable snapshot.
#[derive(Clone, Debug)]
pub struct ProjectionReady {
    /// Published snapshot.
    pub snapshot: Arc<ColumnarSnapshot>,
    /// Visible frontier of the snapshot (incarnation-bound).
    pub frontier: ProjectionFrontier,
    /// Application head known at query time (same incarnation as the engine).
    pub head: ProjectionFrontier,
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

/// Rebuild in progress after detach/rebuild policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionRebuilding {
    /// Closed reason code.
    pub reason: RebuildingReason,
    /// Progress numerator (applied commits during rebuild).
    pub progress_applied: u64,
    /// Progress denominator hint (0 = unknown).
    pub progress_total: u64,
}

/// Degraded health.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionDegraded {
    /// Closed reason code.
    pub reason: DegradedReason,
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
    use std::collections::BTreeSet;

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
    fn rebuilding_and_degraded_use_closed_reason_enums() {
        let rebuilding = ColumnarOutcome::Rebuilding(ProjectionRebuilding {
            reason: RebuildingReason::ReplayBudgetExceeded,
            progress_applied: 3,
            progress_total: 10,
        });
        assert!(matches!(
            rebuilding,
            ColumnarOutcome::Rebuilding(ProjectionRebuilding {
                reason: RebuildingReason::ReplayBudgetExceeded,
                ..
            })
        ));
        let degraded = ColumnarOutcome::Degraded(ProjectionDegraded {
            reason: DegradedReason::ApplyLagSlo,
            current_frontier: FrontierPosition::BeforeFirst,
        });
        assert!(matches!(
            degraded,
            ColumnarOutcome::Degraded(ProjectionDegraded {
                reason: DegradedReason::ApplyLagSlo,
                ..
            })
        ));
        let invalid = ColumnarOutcome::invalid(
            DefinitionFingerprint::from_bytes([1; 32]),
            DefinitionFingerprint::from_bytes([2; 32]),
        );
        assert!(matches!(invalid, ColumnarOutcome::Invalid(_)));
    }

    #[test]
    fn reason_enums_are_exhaustive_and_tags_unique() {
        let rebuild_tags: BTreeSet<u8> = RebuildingReason::all().iter().map(|r| r.tag()).collect();
        assert_eq!(rebuild_tags.len(), RebuildingReason::all().len());
        for reason in RebuildingReason::all() {
            // Match forces a compile break when a variant is added without
            // updating `all()` / `tag()`.
            match reason {
                RebuildingReason::ReplayBudgetExceeded
                | RebuildingReason::ExplicitRebuild
                | RebuildingReason::StateIntegrityFailure => {
                    assert_ne!(reason.tag(), 0);
                }
            }
        }

        let degraded_tags: BTreeSet<u8> = DegradedReason::all().iter().map(|r| r.tag()).collect();
        assert_eq!(degraded_tags.len(), DegradedReason::all().len());
        for reason in DegradedReason::all() {
            match reason {
                DegradedReason::ApplyLagSlo
                | DegradedReason::MaintenanceBacklog
                | DegradedReason::PartialInventory => {
                    assert_ne!(reason.tag(), 0);
                }
            }
        }
    }
}
