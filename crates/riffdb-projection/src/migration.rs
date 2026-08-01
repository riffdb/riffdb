//! Bounded fresh-generation requirements for contract migration.

use std::collections::BTreeSet;

use riffdb_types::ProjectionId;

const MAX_MIGRATION_PROJECTIONS: usize = 4_096;

/// Exact duplicate-free projection generations required before cutover.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationProjectionCandidates {
    projections: Vec<ProjectionId>,
}

impl MigrationProjectionCandidates {
    /// Seals the catalog-selected projection set in canonical identity order.
    pub fn from_catalog_set(
        projections: &BTreeSet<ProjectionId>,
    ) -> Result<Self, MigrationProjectionCandidateError> {
        if projections.len() > MAX_MIGRATION_PROJECTIONS {
            return Err(MigrationProjectionCandidateError::LimitExceeded);
        }
        Ok(Self {
            projections: projections.iter().copied().collect(),
        })
    }

    /// Borrows canonical projection identities to build in fresh generations.
    #[must_use]
    pub fn projections(&self) -> &[ProjectionId] {
        &self.projections
    }
}

/// Closed preparation failure for fresh migration projection generations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrationProjectionCandidateError {
    /// The compiled projection set exceeded its global contract bound.
    LimitExceeded,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use riffdb_types::ProjectionId;

    use super::MigrationProjectionCandidates;

    #[test]
    fn catalog_set_is_retained_in_stable_id_order() {
        let candidates = MigrationProjectionCandidates::from_catalog_set(&BTreeSet::from([
            ProjectionId::new(9).unwrap(),
            ProjectionId::new(2).unwrap(),
        ]))
        .expect("bounded candidates");
        assert_eq!(
            candidates.projections(),
            [ProjectionId::new(2).unwrap(), ProjectionId::new(9).unwrap()]
        );
    }
}
