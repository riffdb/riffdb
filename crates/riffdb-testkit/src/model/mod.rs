//! Small independent models used to compare semantic backend histories.

mod authoritative;
mod comparison;
pub mod correlated_index;
mod fixtures;
mod namespaces;

pub use authoritative::{AuthoritativeCommandModel, ModelApplyError};
pub use comparison::{
    ModelDivergence, ModelDivergenceCause, ModelFamily, StoreAgreement, StoreDivergence,
    StoreDivergenceCause, verify_model_against_inspection,
};
pub use correlated_index::{
    CorrelatedCommandWorkMetrics, CorrelatedIndexWorkModel, CorrelatedIndexedMutationRow,
    CorrelatedModelDivergence, CorrelatedStoreAgreement, CorrelatedWorkBudgetError,
    compare_correlated_target_pages, indexed_mutation_bulk_bundle,
    verify_correlated_index_work_against_redb, verify_work_accounting_boundary_witnesses,
};
pub use fixtures::budget_projection_schema;
pub use namespaces::{AuthoritativeNamespaceModel, NamespaceModelError};
