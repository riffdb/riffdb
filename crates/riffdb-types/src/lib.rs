#![forbid(unsafe_code)]

//! Canonical domain types shared across RiffDB semantic boundaries.

mod actor;
mod capability;
mod capability_grant;
mod codec;
mod consumer;
mod database;
mod decimal;
mod dual_frontier;
mod execution;
mod freshness;
mod hash;
mod ids;
mod key;
mod limits;
mod maintenance;
mod projection;
mod query;
mod service;
mod time;
mod value;
mod vector;

pub use actor::*;
pub use capability::*;
pub use capability_grant::*;
pub use codec::*;
pub use consumer::*;
pub use database::*;
pub use decimal::*;
pub use dual_frontier::*;
pub use execution::*;
pub use freshness::*;
pub use hash::*;
pub use ids::*;
pub use key::*;
pub use limits::*;
pub use maintenance::*;
pub use projection::*;
pub use query::*;
pub use service::*;
pub use time::*;
pub use value::*;
pub use vector::*;
