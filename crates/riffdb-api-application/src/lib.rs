#![forbid(unsafe_code)]

//! API-neutral adaptation for generated RiffDB application operations.
//!
//! Transport crates depend on this crate. It never depends on a transport,
//! socket, TLS implementation, or wire runtime.

mod conversion;
mod operation;

pub use conversion::{
    ConversionError, ExecuteSymbolicQueryInvocation,
    application_session_catalog_request_from_proto, execute_command_request_from_proto,
    execute_command_result_to_proto, execute_symbolic_query_request_from_proto,
    execute_symbolic_query_result_to_proto,
};
pub use operation::{
    APPLICATION_SESSION_PROTOCOL_V1, ApplicationOperationFailure, ApplicationOperationPresentation,
    ApplicationOperationRoute, ApplicationOperationSecurity, ApplicationPresentationError,
    execute_generated_command, execute_generated_query, open_application_session,
};
