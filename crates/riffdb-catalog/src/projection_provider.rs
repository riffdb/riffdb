//! Pay-once projection-provider binding validation (ADR-0130).

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use riffdb_query_module::ProjectionResultSetBindingV1;
use riffdb_types::QueryOperationName;

/// Maximum named provider bindings in one deployment generation.
pub const MAX_PROJECTION_PROVIDER_BINDINGS_V1: usize = 4_096;

/// Immutable deployment-generation catalog of validated result-set bindings.
///
/// Validation happens only in [`Self::open`]. Lookups return shared checked
/// plans without decoding or re-proving descriptor bytes per request, row,
/// candidate, measure, page, or provider call.
#[derive(Clone, Debug)]
pub struct ProjectionProviderCatalogV1 {
    deployment_generation: u64,
    bindings: BTreeMap<QueryOperationName, Arc<ProjectionResultSetBindingV1>>,
    validation_count: usize,
}

impl ProjectionProviderCatalogV1 {
    /// Validates every canonical binding exactly once for one deployment generation.
    pub fn open(
        deployment_generation: u64,
        canonical_bindings: &[Vec<u8>],
    ) -> Result<Self, ProjectionProviderCatalogError> {
        if deployment_generation == 0 {
            return Err(ProjectionProviderCatalogError::InvalidGeneration);
        }
        if canonical_bindings.len() > MAX_PROJECTION_PROVIDER_BINDINGS_V1 {
            return Err(ProjectionProviderCatalogError::TooManyBindings);
        }
        let mut bindings = BTreeMap::new();
        for bytes in canonical_bindings {
            let binding = ProjectionResultSetBindingV1::from_canonical_bytes(bytes)
                .map_err(|_| ProjectionProviderCatalogError::InvalidBinding)?;
            let name = binding.query_name().clone();
            if bindings.insert(name, Arc::new(binding)).is_some() {
                return Err(ProjectionProviderCatalogError::DuplicateQuery);
            }
        }
        Ok(Self {
            deployment_generation,
            validation_count: canonical_bindings.len(),
            bindings,
        })
    }

    /// Exact catalog/deployment generation that scoped validation.
    #[must_use]
    pub const fn deployment_generation(&self) -> u64 {
        self.deployment_generation
    }
    /// Number of descriptor-plan bindings validated while opening this catalog.
    #[must_use]
    pub const fn validation_count(&self) -> usize {
        self.validation_count
    }
    /// Returns one already validated shared binding without re-decoding it.
    #[must_use]
    pub fn get(&self, name: &QueryOperationName) -> Option<Arc<ProjectionResultSetBindingV1>> {
        self.bindings.get(name).cloned()
    }
}

/// Closed pay-once catalog validation failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectionProviderCatalogError {
    /// Deployment generation is the forbidden zero sentinel.
    InvalidGeneration,
    /// Binding count exceeds the fixed deployment bound.
    TooManyBindings,
    /// One canonical binding or its descriptor/plan is invalid.
    InvalidBinding,
    /// Two bindings claim the same exact named operation.
    DuplicateQuery,
}

impl fmt::Display for ProjectionProviderCatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid projection-provider catalog: {self:?}")
    }
}

impl Error for ProjectionProviderCatalogError {}
