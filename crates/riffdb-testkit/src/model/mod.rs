//! Small independent models used to compare semantic backend histories.

mod authoritative;
mod comparison;
mod fixtures;

pub use authoritative::{AuthoritativeCommandModel, ModelApplyError};
pub use comparison::{
    ModelDivergence, ModelDivergenceCause, ModelFamily, StoreAgreement, StoreDivergence,
    StoreDivergenceCause, verify_model_against_inspection,
};
pub use fixtures::budget_projection_schema;
