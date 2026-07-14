//! Small independent models used to compare semantic backend histories.

mod authoritative;
mod fixtures;

pub use authoritative::{AuthoritativeCommandModel, ModelApplyError};
pub use fixtures::budget_projection_schema;
