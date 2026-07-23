#![forbid(unsafe_code)]

//! Production composition and process providers for `riffdbd`.

mod auth_adapters;
mod clocks;
mod config;
mod cursor;
mod daemon;
mod identifiers;
mod lifecycle;
mod notifications;
mod operational_status;
mod port_driver;
mod process_graph;
mod projection_adapter;
mod read_adapters;
mod runtime_support;
mod server_generation;
mod startup;
mod storage;

pub use daemon::riffdbd_main;
