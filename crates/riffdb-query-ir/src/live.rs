//! Derived, non-persisted execution facts for exact live named queries.

use std::collections::BTreeSet;

use riffdb_riffql_syntax::Cardinality;
use riffdb_types::{EntityTypeId, QueryPlanHash};

use crate::{
    QueryAccessKind, QueryAccessProgramV1, QueryPredicateOperator, QueryPredicateValue,
    ReactiveUpdateModeV1,
};

/// Why one query cannot be used as a bounded live watch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveQueryPlanError {
    /// Live execution cannot represent a paginated partial result.
    PaginatedQuery,
    /// Patch mode does not name a complete compiler-proved public key.
    UnkeyedPatch,
    /// Patch mode is only defined for one top-level bounded collection.
    InvalidPatchShape,
}

/// Conservative invalidation precision for one query access step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveInvalidationPrecisionV1 {
    /// The complete entity key can be bound from submitted parameters and constants.
    ExactEntity,
    /// Any same-partition mutation of this entity type may affect the result.
    PartitionEntity,
}

/// One compiler-derived entity dependency retained by a live execution plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveEntityDependencyV1 {
    binding: String,
    entity: String,
    entity_type_id: EntityTypeId,
    precision: LiveInvalidationPrecisionV1,
}

impl LiveEntityDependencyV1 {
    /// Query-local binding that owns the dependency.
    #[must_use]
    pub fn binding(&self) -> &str {
        &self.binding
    }

    /// Safe contract entity name.
    #[must_use]
    pub fn entity(&self) -> &str {
        &self.entity
    }

    /// Stable compiler identity used only below the public boundary.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_entity_type_id(&self) -> EntityTypeId {
        self.entity_type_id
    }

    /// Conservative invalidation precision.
    #[must_use]
    pub const fn precision(&self) -> LiveInvalidationPrecisionV1 {
        self.precision
    }
}

/// Public update strategy proven for one exact query result shape.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LiveUpdateStrategyV1 {
    /// Every changed view is returned as one complete bounded reset.
    Reset,
    /// One top-level collection may use complete-key patches.
    Patch {
        /// Result field populated by the collection binding.
        result_field: String,
        /// Complete explicitly selected primary-key names in canonical order.
        key_fields: Vec<String>,
    },
}

/// Exact non-persisted live execution plan derived from frozen query/reactive IR.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveQueryPlanV1 {
    query_plan_hash: QueryPlanHash,
    dependencies: Vec<LiveEntityDependencyV1>,
    update_strategy: LiveUpdateStrategyV1,
}

impl LiveQueryPlanV1 {
    /// Derives bounded invalidation and update facts without changing canonical IR bytes.
    pub fn derive(
        program: &QueryAccessProgramV1,
        update_mode: ReactiveUpdateModeV1,
        patch_key: &[String],
    ) -> Result<Self, LiveQueryPlanError> {
        if program
            .steps()
            .iter()
            .any(|step| step.cursor_parameter().is_some())
        {
            return Err(LiveQueryPlanError::PaginatedQuery);
        }

        let dependencies = program
            .steps()
            .iter()
            .map(|step| LiveEntityDependencyV1 {
                binding: step.binding().to_owned(),
                entity: step.entity().to_owned(),
                entity_type_id: step.internal_entity_id(),
                precision: if statically_bindable_point(step) {
                    LiveInvalidationPrecisionV1::ExactEntity
                } else {
                    LiveInvalidationPrecisionV1::PartitionEntity
                },
            })
            .collect();

        let update_strategy = match update_mode {
            ReactiveUpdateModeV1::Reset => LiveUpdateStrategyV1::Reset,
            ReactiveUpdateModeV1::Patch => {
                if patch_key.is_empty() || patch_key.windows(2).any(|pair| pair[0] >= pair[1]) {
                    return Err(LiveQueryPlanError::UnkeyedPatch);
                }
                let mut result_steps = program
                    .steps()
                    .iter()
                    .filter(|step| !step.result_names().is_empty());
                let step = result_steps
                    .next()
                    .ok_or(LiveQueryPlanError::InvalidPatchShape)?;
                if result_steps.next().is_some()
                    || step.cardinality() != Cardinality::Many
                    || step.result_names().len() != 1
                    || patch_key
                        .iter()
                        .any(|field| !step.selected_fields().contains(field))
                {
                    return Err(LiveQueryPlanError::InvalidPatchShape);
                }
                LiveUpdateStrategyV1::Patch {
                    result_field: step.result_names()[0].clone(),
                    key_fields: patch_key.to_vec(),
                }
            }
        };

        Ok(Self {
            query_plan_hash: program.identity().hash(),
            dependencies,
            update_strategy,
        })
    }

    /// Exact query plan covered by these derived facts.
    #[must_use]
    pub const fn query_plan_hash(&self) -> QueryPlanHash {
        self.query_plan_hash
    }

    /// Ordered access-step dependencies.
    #[must_use]
    pub fn dependencies(&self) -> &[LiveEntityDependencyV1] {
        &self.dependencies
    }

    /// Proven public update behavior.
    #[must_use]
    pub const fn update_strategy(&self) -> &LiveUpdateStrategyV1 {
        &self.update_strategy
    }
}

fn statically_bindable_point(step: &crate::QueryAccessStep) -> bool {
    let QueryAccessKind::Point { key_fields } = step.access() else {
        return false;
    };
    let key_fields = key_fields
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let bound = step
        .predicates()
        .iter()
        .filter(|predicate| {
            predicate.operator() == QueryPredicateOperator::Equal
                && key_fields.contains(predicate.field())
                && matches!(
                    predicate.value(),
                    QueryPredicateValue::Parameter(_)
                        | QueryPredicateValue::Literal(_)
                        | QueryPredicateValue::EnumVariant { .. }
                )
        })
        .map(|predicate| predicate.field())
        .collect::<BTreeSet<_>>();
    bound == key_fields
}
