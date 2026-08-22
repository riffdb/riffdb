#![forbid(unsafe_code)]

//! API-neutral adaptation for generated RiffDB application operations.
//!
//! Transport crates depend on this crate. It never depends on a transport,
//! socket, TLS implementation, or wire runtime.
