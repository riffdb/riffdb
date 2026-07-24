#![forbid(unsafe_code)]

//! Public-only command-line workflows for a standalone RiffDB server.

mod app;
mod cli;
mod config;
mod credential;
mod input;
mod output;
mod runner;
mod value;

pub use app::run;
