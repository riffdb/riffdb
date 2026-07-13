#![forbid(unsafe_code)]

//! Canonical domain types shared across RiffDB semantic boundaries.

mod actor;
mod capability;
mod codec;
mod decimal;
mod execution;
mod hash;
mod ids;
mod key;
mod limits;
mod projection;
mod service;
mod time;
mod value;

pub use actor::*;
pub use capability::*;
pub use codec::*;
pub use decimal::*;
pub use execution::*;
pub use hash::*;
pub use ids::*;
pub use key::*;
pub use limits::*;
pub use projection::*;
pub use service::*;
pub use time::*;
pub use value::*;
