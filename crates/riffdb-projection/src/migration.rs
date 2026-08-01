//! Bounded fresh-generation requirements for contract migration.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

use riffdb_types::{CommitSequence, ProjectionGeneration, ProjectionId};

const MAX_MIGRATION_PROJECTIONS: usize = 4_096;

/// Exact duplicate-free projection generations required before cutover.
#[derive(Debug, Eq, PartialEq)]
pub struct MigrationProjectionCandidates {
    projections: Vec<ProjectionId>,
}

impl MigrationProjectionCandidates {
    /// Seals the catalog-selected projection set in canonical identity order.
    pub fn from_catalog_set(
        projections: &BTreeSet<ProjectionId>,
    ) -> Result<Self, MigrationProjectionError> {
        if projections.len() > MAX_MIGRATION_PROJECTIONS {
            return Err(MigrationProjectionError::LimitExceeded);
        }
        Ok(Self {
            projections: projections.iter().copied().collect(),
        })
    }

    /// Builds every required projection and seals exact readiness observations.
    pub fn rebuild<P: MigrationProjectionBuildPort>(
        self,
        frontier: Option<CommitSequence>,
        port: &mut P,
    ) -> Result<MigrationProjectionReadiness, MigrationProjectionError> {
        let mut ready = Vec::with_capacity(self.projections.len());
        for projection in self.projections {
            let observation = port.rebuild_projection(projection, frontier)?;
            if observation.projection != projection || observation.frontier != frontier {
                return Err(MigrationProjectionError::EvidenceMismatch);
            }
            ready.push(observation);
        }
        Ok(MigrationProjectionReadiness { frontier, ready })
    }
}

/// Storage-neutral observation returned after one fresh generation is ready.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MigrationProjectionBuildObservation {
    projection: ProjectionId,
    generation: ProjectionGeneration,
    frontier: Option<CommitSequence>,
}

impl MigrationProjectionBuildObservation {
    /// Reports an actual fresh generation after the projection worker validated it.
    #[must_use]
    pub const fn ready(
        projection: ProjectionId,
        generation: ProjectionGeneration,
        frontier: Option<CommitSequence>,
    ) -> Self {
        Self {
            projection,
            generation,
            frontier,
        }
    }

    /// Returns the ready projection identity.
    #[must_use]
    pub const fn projection(self) -> ProjectionId {
        self.projection
    }

    /// Returns its fresh generation.
    #[must_use]
    pub const fn generation(self) -> ProjectionGeneration {
        self.generation
    }
}

/// Move-only proof that all catalog-required generations reached one frozen frontier.
pub struct MigrationProjectionReadiness {
    frontier: Option<CommitSequence>,
    ready: Vec<MigrationProjectionBuildObservation>,
}

impl MigrationProjectionReadiness {
    /// Returns the exact frozen application frontier.
    #[must_use]
    pub const fn frontier(&self) -> Option<CommitSequence> {
        self.frontier
    }

    /// Borrows ready generations in canonical projection order.
    #[must_use]
    pub fn observations(&self) -> &[MigrationProjectionBuildObservation] {
        &self.ready
    }

    /// Returns the canonical projection identities bound by this proof.
    #[must_use]
    pub fn projection_ids(&self) -> Vec<ProjectionId> {
        self.ready.iter().map(|item| item.projection).collect()
    }
}

impl fmt::Debug for MigrationProjectionReadiness {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MigrationProjectionReadiness")
            .field("frontier", &self.frontier)
            .field("ready_count", &self.ready.len())
            .finish()
    }
}

/// Narrow execution port; projection orchestration, never storage, implements it.
pub trait MigrationProjectionBuildPort {
    /// Builds and validates one fresh generation through the frozen frontier.
    fn rebuild_projection(
        &mut self,
        projection: ProjectionId,
        frontier: Option<CommitSequence>,
    ) -> Result<MigrationProjectionBuildObservation, MigrationProjectionError>;
}

/// Closed preparation or readiness failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrationProjectionError {
    /// The compiled projection set exceeded its global contract bound.
    LimitExceeded,
    /// Projection generation could not be rebuilt and validated.
    BuildFailed,
    /// Returned projection identity or frontier did not match the request.
    EvidenceMismatch,
}

impl fmt::Display for MigrationProjectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("migration projection preparation failed")
    }
}

impl Error for MigrationProjectionError {}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use riffdb_types::{CommitSequence, ProjectionGeneration, ProjectionId};

    use super::{
        MigrationProjectionBuildObservation, MigrationProjectionBuildPort,
        MigrationProjectionCandidates, MigrationProjectionError,
    };

    struct RecordingPort {
        requested: Vec<ProjectionId>,
    }

    impl MigrationProjectionBuildPort for RecordingPort {
        fn rebuild_projection(
            &mut self,
            projection: ProjectionId,
            frontier: Option<CommitSequence>,
        ) -> Result<MigrationProjectionBuildObservation, MigrationProjectionError> {
            self.requested.push(projection);
            Ok(MigrationProjectionBuildObservation::ready(
                projection,
                ProjectionGeneration::first(),
                frontier,
            ))
        }
    }

    #[test]
    fn readiness_is_move_only_and_retains_canonical_identity_order() {
        let candidates = MigrationProjectionCandidates::from_catalog_set(&BTreeSet::from([
            ProjectionId::new(9).unwrap(),
            ProjectionId::new(2).unwrap(),
        ]))
        .expect("bounded candidates");
        let mut port = RecordingPort {
            requested: Vec::new(),
        };
        let frontier = Some(CommitSequence::first());
        let ready = candidates.rebuild(frontier, &mut port).expect("ready");
        assert_eq!(
            ready.projection_ids(),
            [ProjectionId::new(2).unwrap(), ProjectionId::new(9).unwrap()]
        );
        assert_eq!(ready.frontier(), frontier);
        assert_eq!(ready.projection_ids(), port.requested);
    }
}
