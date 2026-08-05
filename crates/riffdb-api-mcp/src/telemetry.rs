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

/// Closed risk classes for MCP tool-call accounting.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum McpRiskClass {
    /// A fixed tool that mutates administrative state.
    AdministrativeMutation,
    /// A fixed administrative read.
    AdministrativeRead,
    /// A bounded fixed administrative read.
    BoundedAdministrativeRead,
    /// A bounded data read.
    BoundedRead,
    /// A read-only operation.
    ReadOnly,
    /// A read-only compute operation.
    ReadOnlyCompute,
    /// A read-only data operation.
    ReadOnlyData,
    /// A symbolic read compiled at the request boundary.
    SymbolicRead,
    /// A bounded durable-consumer or contextual operation.
    ReactiveApplication,
    /// An explicit durable-consumer control mutation.
    ConsumerControl,
    /// A fixed application command mutation.
    ApplicationMutation,
    /// A compiler-owned dynamic command tool.
    DynamicCommand,
}

impl McpRiskClass {
    /// Every closed risk class in stable metric order.
    pub const ALL: [Self; 12] = [
        Self::AdministrativeMutation,
        Self::AdministrativeRead,
        Self::BoundedAdministrativeRead,
        Self::BoundedRead,
        Self::ReadOnly,
        Self::ReadOnlyCompute,
        Self::ReadOnlyData,
        Self::SymbolicRead,
        Self::ReactiveApplication,
        Self::ConsumerControl,
        Self::ApplicationMutation,
        Self::DynamicCommand,
    ];

    pub(crate) fn from_fixed(value: &str) -> Option<Self> {
        match value {
            "administrative_mutation" => Some(Self::AdministrativeMutation),
            "administrative_read" => Some(Self::AdministrativeRead),
            "bounded_administrative_read" => Some(Self::BoundedAdministrativeRead),
            "bounded_read" => Some(Self::BoundedRead),
            "read_only" => Some(Self::ReadOnly),
            "read_only_compute" => Some(Self::ReadOnlyCompute),
            "read_only_data" => Some(Self::ReadOnlyData),
            "symbolic_read" => Some(Self::SymbolicRead),
            "reactive_application" => Some(Self::ReactiveApplication),
            "consumer_control" => Some(Self::ConsumerControl),
            "application_mutation" => Some(Self::ApplicationMutation),
            _ => None,
        }
    }
}

/// Closed schema-validation phase.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum McpSchemaFailurePhase {
    /// Caller-supplied structured tool input failed its checked schema.
    Input,
    /// Backend output failed its checked result schema.
    Output,
}

/// Closed list-change notification kind.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum McpListChangeKind {
    /// The policy-filtered tool inventory changed.
    Tools,
    /// The policy-filtered resource inventory changed.
    Resources,
}

/// Closed semantic MCP observations emitted before any telemetry subscriber.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpTelemetryEvent {
    /// One transport-neutral MCP server session opened.
    SessionOpened {
        /// Transport owning the session.
        transport: McpTransportKind,
    },
    /// One transport-neutral MCP server session closed.
    SessionClosed {
        /// Transport that owned the session.
        transport: McpTransportKind,
    },
    /// One resolved fixed or dynamic tool call reached schema validation.
    ToolCall {
        /// Closed risk class; tool names are deliberately absent.
        risk: McpRiskClass,
    },
    /// Checked tool input or output schema validation failed.
    SchemaFailure {
        /// Input or output phase only.
        phase: McpSchemaFailurePhase,
    },
    /// Current authorization denied one MCP operation.
    AuthorizationDenied,
    /// One coalesced list-change notification entered the bounded transport sink.
    ListChangeNotification {
        /// Tool or resource list only.
        kind: McpListChangeKind,
    },
}

/// Least-authority sink for closed MCP semantic events.
pub trait McpTelemetry: Send + Sync {
    /// Records one event without protocol content, identities, credentials, or text.
    fn record(&self, event: McpTelemetryEvent);
}

/// No-op sink for isolated adapters and tests.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopMcpTelemetry;

impl McpTelemetry for NoopMcpTelemetry {
    fn record(&self, _event: McpTelemetryEvent) {}
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
