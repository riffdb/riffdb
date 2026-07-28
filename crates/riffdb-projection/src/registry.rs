//! Exact bounded registry of catalog-checked projection schemas.

use std::collections::BTreeMap;

use riffdb_storage_api::CheckedProjectionSchema;
use riffdb_types::ProjectionIdentity;

use crate::{ProjectionCoreError, ProjectionCoreErrorKind};

// Grammar/IR v1 permits at most 4,096 projections in one checked bundle. This
// process-local registry cannot be constructed from more exact active schemas.
const MAX_REGISTERED_PROJECTIONS_V1: usize = 4_096;

/// Immutable exact-identity projection schema registry.
#[derive(Clone, Debug)]
pub struct ProjectionSchemaRegistry {
    schemas: BTreeMap<ProjectionIdentity, CheckedProjectionSchema>,
}

impl ProjectionSchemaRegistry {
    /// Builds a duplicate-free bounded registry from catalog-checked schemas.
    pub fn new(
        schemas: impl IntoIterator<Item = CheckedProjectionSchema>,
    ) -> Result<Self, ProjectionCoreError> {
        let mut by_identity = BTreeMap::new();
        for schema in schemas {
            if by_identity.contains_key(schema.identity()) {
                return Err(ProjectionCoreError::new(ProjectionCoreErrorKind::Integrity));
            }
            if by_identity.len() == MAX_REGISTERED_PROJECTIONS_V1 {
                return Err(ProjectionCoreError::new(
                    ProjectionCoreErrorKind::LimitExceeded,
                ));
            }
            by_identity.insert(schema.identity().clone(), schema);
        }
        Ok(Self {
            schemas: by_identity,
        })
    }

    /// Resolves one exact lineage, stable ID, and projection-plan hash.
    #[must_use]
    pub fn get(&self, identity: &ProjectionIdentity) -> Option<&CheckedProjectionSchema> {
        self.schemas.get(identity)
    }

    /// Returns schemas in canonical identity order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &CheckedProjectionSchema> {
        self.schemas.values()
    }

    /// Returns the bounded number of exact identities.
    #[must_use]
    pub fn len(&self) -> usize {
        self.schemas.len()
    }

    /// Returns whether no identities are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.schemas.is_empty()
    }
}
