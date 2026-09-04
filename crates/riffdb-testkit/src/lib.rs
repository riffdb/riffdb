#![forbid(unsafe_code)]

//! Deterministic fixtures and reference models shared by RiffDB tests.

pub mod authorization;
pub mod failpoint;
pub mod histories;
pub mod inspection;
pub mod model;
pub mod scratch;

#[cfg(test)]
mod external_page_coalescing;
