#![forbid(unsafe_code)]

//! Transport-neutral MCP protocol and presentation primitives for RiffDB.
//!
//! Transport adapters are responsible for authentication and service calls.
//! This crate's ungated surface contains only bounded presentation logic and
//! checked foundational values.

mod admission;
mod application_guidance;
mod bounded_json;
#[cfg(feature = "stdio")]
mod builder;
mod catalog_parity;
mod conversion;
mod cursor;
mod handler;
#[cfg(feature = "streamable-http")]
mod hosted_http;
#[cfg(feature = "streamable-http")]
mod hosted_observer;
#[cfg(feature = "streamable-http")]
mod hosted_session;
mod locator;
mod observer;
mod observer_budget;
mod presentation;
mod progress;
mod protocol;
mod rate_limit;
mod registry;
mod request_id;
mod resource_presentation;
mod schema;
mod schema_bound;
#[cfg(feature = "streamable-http")]
mod service_backend;
#[cfg(feature = "stdio")]
mod stdio_transport;
mod telemetry;

pub use admission::*;
pub use application_guidance::*;
#[cfg(feature = "stdio")]
pub use builder::{BuilderMcpConfiguration, BuilderMcpServer};
pub use catalog_parity::*;
pub use conversion::*;
pub use cursor::{
    APPLICATION_QUERY_CURSOR_TEXT_BYTES, MCP_CURSOR_BYTES, MCP_CURSOR_TEXT_BYTES, McpCursorError,
    decode_application_query_cursor, decode_mcp_cursor, encode_application_query_cursor,
    encode_mcp_cursor,
};
pub use handler::*;
#[cfg(feature = "streamable-http")]
pub use hosted_http::{
    HostedMcpHttpConfiguration, HostedMcpHttpConfigurationError, HostedMcpHttpConnection,
    HostedMcpHttpPeerError, HostedMcpHttpRegistration, HostedMcpHttpResponseBody,
    HostedMcpHttpResponseBodyError, HostedMcpInvocationError, HostedMcpSessionMaintenanceError,
    MAX_ALLOWED_ORIGIN_AGGREGATE_BYTES, MAX_ALLOWED_ORIGINS, MAX_ORIGIN_BYTES,
    hosted_mcp_request_context, register_hosted_mcp_http, register_hosted_service_mcp_http,
};
#[cfg(feature = "streamable-http")]
pub use hosted_session::{McpMonotonicClock, McpMonotonicClockError, SystemMcpMonotonicClock};
pub use locator::{
    MAX_MCP_RESOURCE_LOCATOR_BYTES, MAX_OUTCOME_RESOURCE_LOCATOR_BYTES, McpResourceLocator,
    OutcomeKeyHash, ResourceLocatorError, format_active_contract_locator,
    format_application_guidance_locator, format_command_documentation_locator,
    format_command_documentation_locator_from_public, format_command_plan_locator,
    format_command_plan_locator_from_public, format_commit_locator,
    format_commit_locator_from_public, format_commit_template_locator,
    format_contract_version_locator, format_contract_version_locator_from_public,
    format_entity_schema_locator, format_entity_schema_locator_from_public, format_outcome_locator,
    format_outcome_locator_from_public, format_outcome_template_locator_from_public,
    format_projection_status_locator, format_projection_status_locator_from_public,
    format_provenance_locator, format_provenance_locator_from_public,
    format_provenance_template_locator, format_reactive_wakeup_locator,
    format_server_health_locator, parse_resource_locator, validate_command_tool_name,
};
pub use observer::*;
pub use observer_budget::*;
pub use presentation::McpPresentationError;
pub use progress::*;
pub use protocol::{
    MCP_INBOUND_MESSAGE_MAX_BYTES, MCP_OUTBOUND_MESSAGE_MAX_BYTES, MCP_PROTOCOL_VERSION, MCP_ROUTE,
    MCP_SERVER_NAME, MCP_SERVER_VERSION, initialization_result,
};
pub use rate_limit::*;
pub use registry::{
    FixedToolDefinition, FixedToolRegistry, McpGeneratedSchemaKind, RegistryError,
    ResourceDefinition, ResourceRegistry, ResourceSurface, SchemaDocument, fixed_tool_registry,
    resource_registry,
};
pub use request_id::{McpRequestId, RequestIdSource, RequestIdSourceError};
pub use resource_presentation::*;
pub use schema::{GeneratedNamedQueryPagination, generated_named_query_pagination};
pub use schema::{SchemaCompositionError, SchemaValidationError, compose_command_result_schema};
pub use schema_bound::*;
#[cfg(feature = "streamable-http")]
pub use service_backend::{HostedServiceInvocation, HostedServiceMcpBackend};
#[cfg(feature = "stdio")]
pub use stdio_transport::{
    McpStdioClientActivity, McpStdioServeError, McpStdioTransportError, serve_builder_mcp_stdio,
    serve_mcp_stdio,
};
pub use telemetry::{
    MCP_SAFE_TRANSPORT_TRACE_TARGET, McpListChangeKind, McpRiskClass, McpSafeTransportEvent,
    McpSchemaFailurePhase, McpTelemetry, McpTelemetryEvent, McpTransportKind,
    McpTransportRejection, NoopMcpTelemetry, is_safe_transport_trace_target,
    record_safe_transport_event,
};

/// Returns whether a tracing target may pass the mandatory MCP SDK suppression boundary.
///
/// This predicate must remain the final outer filter in each process that hosts
/// an MCP transport. User-controlled logging directives cannot override it.
#[must_use]
pub fn allows_transport_trace_target(target: &str) -> bool {
    target != "rmcp" && !target.starts_with("rmcp::")
}

#[cfg(test)]
mod tests {
    use super::allows_transport_trace_target;

    #[test]
    fn tracing_target_boundary_is_exact() {
        assert!(!allows_transport_trace_target("rmcp"));
        assert!(!allows_transport_trace_target("rmcp::transport"));
        assert!(!allows_transport_trace_target("rmcp::transport::stdio"));
        assert!(allows_transport_trace_target("rmcp2"));
        assert!(allows_transport_trace_target("riffdb_api_mcp"));
        assert!(allows_transport_trace_target(""));
    }
}
