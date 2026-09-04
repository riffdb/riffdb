use tracing::{Level, event};

pub use riffdb_observability::{
    MCP_SAFE_TRANSPORT_TRACE_TARGET, McpListChangeKind, McpRiskClass, McpSafeTransportEvent,
    McpSchemaFailurePhase, McpTelemetry, McpTelemetryEvent, McpTransportKind,
    McpTransportRejection, NoopMcpTelemetry, is_safe_transport_trace_target,
};
/// Emits one bounded first-party event without accepting arbitrary fields.
pub fn record_safe_transport_event(safe_event: McpSafeTransportEvent) {
    match safe_event {
        McpSafeTransportEvent::Started(transport) => {
            event!(
                target: MCP_SAFE_TRANSPORT_TRACE_TARGET,
                Level::INFO,
                event = "mcp_transport_started",
                transport = transport.as_str()
            );
        }
        McpSafeTransportEvent::Stopped(transport) => {
            event!(
                target: MCP_SAFE_TRANSPORT_TRACE_TARGET,
                Level::INFO,
                event = "mcp_transport_stopped",
                transport = transport.as_str()
            );
        }
        McpSafeTransportEvent::Rejected { transport, reason } => {
            event!(
                target: MCP_SAFE_TRANSPORT_TRACE_TARGET,
                Level::WARN,
                event = "mcp_transport_rejected",
                transport = transport.as_str(),
                reason = reason.as_str()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_event_vocabulary_has_only_static_fields() {
        let events = [
            McpSafeTransportEvent::Started(McpTransportKind::Stdio),
            McpSafeTransportEvent::Stopped(McpTransportKind::StreamableHttp),
            McpSafeTransportEvent::Rejected {
                transport: McpTransportKind::StreamableHttp,
                reason: McpTransportRejection::Protocol,
            },
        ];

        assert_eq!(events.len(), 3);
        assert_eq!(McpTransportKind::Stdio.as_str(), "stdio");
        assert_eq!(McpTransportRejection::Limit.as_str(), "limit");
        assert_eq!(McpRiskClass::ALL.len(), 12);
        assert_eq!(
            McpRiskClass::from_fixed("administrative_mutation"),
            Some(McpRiskClass::AdministrativeMutation)
        );
        assert_eq!(
            McpRiskClass::from_fixed("symbolic_read"),
            Some(McpRiskClass::SymbolicRead)
        );
        assert_eq!(
            McpRiskClass::from_fixed("reactive_application"),
            Some(McpRiskClass::ReactiveApplication)
        );
        assert_eq!(
            McpRiskClass::from_fixed("consumer_control"),
            Some(McpRiskClass::ConsumerControl)
        );
        assert_eq!(
            McpRiskClass::from_fixed("application_mutation"),
            Some(McpRiskClass::ApplicationMutation)
        );
        assert_eq!(McpRiskClass::from_fixed("caller-controlled"), None);
    }

    #[test]
    fn safe_transport_trace_target_is_exact() {
        assert!(is_safe_transport_trace_target(
            MCP_SAFE_TRANSPORT_TRACE_TARGET
        ));
        assert!(!is_safe_transport_trace_target("riffdb_api_mcp"));
        assert!(!is_safe_transport_trace_target(
            "riffdb_api_mcp::transport::child"
        ));
        assert!(!is_safe_transport_trace_target("tonic"));
        assert!(!is_safe_transport_trace_target("rmcp"));
    }
}
