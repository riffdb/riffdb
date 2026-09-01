use rmcp::model::{
    Implementation, InitializeResult, ProtocolVersion, ResourcesCapability, ServerCapabilities,
    ToolsCapability,
};

/// The only MCP protocol version advertised by the POC.
pub const MCP_PROTOCOL_VERSION: &str = "2025-11-25";

/// The implementation name reported during MCP initialization.
pub const MCP_SERVER_NAME: &str = "riffdb";

/// The implementation version reported during MCP initialization.
pub const MCP_SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// The sole Streamable HTTP route.
pub const MCP_ROUTE: &str = "/mcp";

/// Maximum bytes in one complete inbound MCP message.
///
/// This includes bounded JSON/base64 overhead for a 4 MiB decoded atomic
/// command. Non-command operations retain their independent service limits.
pub const MCP_INBOUND_MESSAGE_MAX_BYTES: usize = riffdb_types::MAX_ATOMIC_COMMAND_FRAME_BYTES_V2;

/// Maximum bytes in one complete outbound MCP message, including framing.
pub const MCP_OUTBOUND_MESSAGE_MAX_BYTES: usize = 4_194_304;

/// Builds the exact transport-independent initialization result.
#[must_use]
pub fn initialization_result() -> InitializeResult {
    let mut resources = ResourcesCapability::default();
    resources.subscribe = Some(true);
    resources.list_changed = Some(true);
    let mut tools = ToolsCapability::default();
    tools.list_changed = Some(true);
    let mut capabilities = ServerCapabilities::default();
    capabilities.resources = Some(resources);
    capabilities.tools = Some(tools);

    InitializeResult::new(capabilities)
        .with_protocol_version(ProtocolVersion::V_2025_11_25)
        .with_server_info(Implementation::new(MCP_SERVER_NAME, MCP_SERVER_VERSION))
}

/// Reports whether request progress was negotiated on the sole accepted baseline.
#[must_use]
pub(crate) fn progress_is_negotiated(version: Option<&ProtocolVersion>) -> bool {
    version.is_some_and(|version| version.as_str() == MCP_PROTOCOL_VERSION)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn initialization_metadata_is_exact_and_has_no_deferred_capabilities() {
        let value =
            serde_json::to_value(initialization_result()).expect("initialization is serializable");

        assert_eq!(
            value,
            json!({
                "protocolVersion": "2025-11-25",
                "capabilities": {
                    "resources": {
                        "subscribe": true,
                        "listChanged": true
                    },
                    "tools": {
                        "listChanged": true
                    }
                },
                "serverInfo": {
                    "name": "riffdb",
                    "version": env!("CARGO_PKG_VERSION")
                }
            })
        );
        assert_eq!(ProtocolVersion::KNOWN_VERSIONS.len(), 5);
        assert!(
            ProtocolVersion::KNOWN_VERSIONS
                .iter()
                .any(|version| version.as_str() == MCP_PROTOCOL_VERSION)
        );
        assert!(
            ProtocolVersion::KNOWN_VERSIONS
                .iter()
                .all(|version| version.as_str() != "riffdb-unsupported")
        );
    }

    #[test]
    fn progress_requires_the_exact_negotiated_protocol_version() {
        assert!(progress_is_negotiated(Some(&ProtocolVersion::V_2025_11_25)));
        assert!(!progress_is_negotiated(Some(
            &ProtocolVersion::V_2025_06_18
        )));
        assert!(!progress_is_negotiated(None));
    }
}
