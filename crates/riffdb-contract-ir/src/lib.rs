#![forbid(unsafe_code)]

//! Checked, versioned contract schemas and executable plans.
//!
//! This crate owns RiffDB's span-free semantic contract boundary. Syntax and
//! source diagnostics stay in the compiler; storage and transports consume
//! only the checked values exported here.

mod bundle;
mod codec;
mod compatibility;
mod error;
mod explain;
mod expression;
mod format_registry;
mod json_schema;
mod key_schema;
mod mcp;
mod migration;
mod operational_index;
mod plan;
mod projection;
mod row_policy;
mod schema;
mod value_type;
mod workflow;

pub use bundle::*;
pub use compatibility::*;
pub use error::*;
pub use explain::*;
pub use expression::*;
#[doc(hidden)]
pub use format_registry::{render_format_markdown, render_json_schema_format_markdown};
pub use json_schema::*;
pub use key_schema::*;
pub use mcp::*;
pub use migration::*;
pub use operational_index::*;
pub use plan::*;
pub use projection::*;
pub use row_policy::*;
pub use schema::*;
pub use value_type::*;
pub use workflow::*;
