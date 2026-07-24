use tracing::{Level, event};

/// The sole tracing target whose events may be formatted by MCP processes.
pub const MCP_SAFE_TRANSPORT_TRACE_TARGET: &str = "riffdb_api_mcp::transport";

/// Returns whether a tracing target belongs to the closed safe-event vocabulary.
#[must_use]
pub fn is_safe_transport_trace_target(target: &str) -> bool {
    target == MCP_SAFE_TRANSPORT_TRACE_TARGET
}

/// The closed MCP transport vocabulary used in first-party telemetry.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum McpTransportKind {
    /// The public-gRPC stdio bridge.
    Stdio,
    /// Hosted loopback Streamable HTTP.
    StreamableHttp,
}

impl McpTransportKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Stdio => "stdio",
            Self::StreamableHttp => "streamable_http",
        }
    }
}

/// Closed, non-sensitive rejection classes for MCP transport telemetry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpTransportRejection {
    /// Protocol framing or shape was invalid.
    Protocol,
    /// A fixed byte or item bound was exceeded.
    Limit,
    /// Authentication did not produce a principal.
    Unauthenticated,
    /// A bounded admission limiter rejected work.
    Admission,
    /// Required process-local state was unavailable.
    Unavailable,
}

impl McpTransportRejection {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Protocol => "protocol",
            Self::Limit => "limit",
            Self::Unauthenticated => "unauthenticated",
            Self::Admission => "admission",
            Self::Unavailable => "unavailable",
        }
    }
}

/// Closed first-party MCP events that carry no request, session, or credential data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpSafeTransportEvent {
    /// A transport completed its process-local startup.
    Started(McpTransportKind),
    /// A transport stopped.
    Stopped(McpTransportKind),
    /// A request was rejected without logging peer-controlled content.
    Rejected {
        /// The transport that rejected the request.
        transport: McpTransportKind,
        /// The closed rejection class.
        reason: McpTransportRejection,
    },
}

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
