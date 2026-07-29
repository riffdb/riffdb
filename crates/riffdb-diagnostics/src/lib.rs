#![forbid(unsafe_code)]

//! Bounded, value-free operator diagnostics.

mod application_error;
mod render;

pub use application_error::*;
pub use render::*;
