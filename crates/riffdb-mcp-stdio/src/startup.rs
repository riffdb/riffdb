//! Ordered process bootstrap for the public gRPC bridge.

use riffdb_api_mcp::McpStdioClientActivity;
use riffdb_client_rust::{CallMetadata, RiffDbClient};

use crate::config::{StdioConfigError, load_process_config};
use crate::telemetry::{self, TelemetryInstallError};
use crate::{PublicGrpcMcpBackend, PublicGrpcMcpObserverBackend};

/// A constructed public-client boundary ready for MCP stdio adaptation.
///
/// This value grants no server, service, policy, authentication, or storage
/// authority. It exists as the narrow handoff to `riffdb-api-mcp`.
pub struct StdioBootstrap {
    client: RiffDbClient,
    metadata: CallMetadata,
}

impl StdioBootstrap {
    /// Moves the public client and its checked outbound metadata into the MCP
    /// adapter.
    #[must_use]
    pub fn into_parts(self) -> (RiffDbClient, CallMetadata) {
        (self.client, self.metadata)
    }
}

impl std::fmt::Debug for StdioBootstrap {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("StdioBootstrap { client: PublicGrpcClient, metadata: [REDACTED] }")
    }
}

/// A bounded process-startup failure that contains no argv, path, endpoint,
/// credential, peer text, or internal source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StdioStartupError {
    /// Process configuration was rejected.
    Configuration,
    /// The mandatory global telemetry boundary could not be installed.
    Telemetry,
    /// The configured public gRPC endpoint could not be reached.
    Connection,
}

impl std::fmt::Display for StdioStartupError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Configuration => "riffdb-mcp configuration was rejected",
            Self::Telemetry => "riffdb-mcp telemetry initialization failed",
            Self::Connection => "riffdb-mcp could not connect to RiffDB",
        })
    }
}

impl std::error::Error for StdioStartupError {}

impl From<StdioConfigError> for StdioStartupError {
    fn from(_: StdioConfigError) -> Self {
        Self::Configuration
    }
}

impl From<TelemetryInstallError> for StdioStartupError {
    fn from(_: TelemetryInstallError) -> Self {
        Self::Telemetry
    }
}

/// A closed process-lifetime failure that contains no protocol, credential,
/// endpoint, path, peer, or internal-source data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StdioRunError {
    /// Ordered process bootstrap failed.
    Startup(StdioStartupError),
    /// The bounded common stdio transport stopped with an error.
    Serving,
}

impl std::fmt::Display for StdioRunError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Startup(error) => error.fmt(formatter),
            Self::Serving => formatter.write_str("riffdb-mcp protocol service failed"),
        }
    }
}

impl std::error::Error for StdioRunError {}

/// Installs the hard telemetry boundary, resolves one credential, and connects
/// one ordinary public gRPC client.
///
/// The returned credential is never reloaded or switched. The caller must move
/// this value directly into the common MCP stdio adapter.
pub async fn bootstrap() -> Result<StdioBootstrap, StdioStartupError> {
    telemetry::install()?;
    let config = load_process_config()?;
    let (endpoint, database, credential) = config.into_parts();
    let endpoint = endpoint
        .parse()
        .map_err(|_| StdioStartupError::Configuration)?;
    let client = RiffDbClient::connect(endpoint)
        .await
        .map_err(|_| StdioStartupError::Connection)?;
    Ok(StdioBootstrap {
        client,
        metadata: CallMetadata::authenticated(credential).with_database(database),
    })
}

/// Runs one public-client MCP stdio process to transport completion.
pub async fn run() -> Result<(), StdioRunError> {
    let (client, metadata) = bootstrap()
        .await
        .map_err(StdioRunError::Startup)?
        .into_parts();
    let client_activity = McpStdioClientActivity::new();
    riffdb_api_mcp::serve_mcp_stdio(
        PublicGrpcMcpBackend::with_client_activity(
            client.clone(),
            metadata.clone(),
            client_activity.clone(),
        ),
        PublicGrpcMcpObserverBackend::new(client, metadata),
        client_activity,
    )
    .await
    .map_err(|_| StdioRunError::Serving)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_failures_are_closed_and_safe() {
        for error in [
            StdioStartupError::Configuration,
            StdioStartupError::Telemetry,
            StdioStartupError::Connection,
        ] {
            let display = error.to_string();
            assert!(display.len() <= 64);
            assert!(!display.contains("token"));
            assert!(!display.contains("http"));
            assert!(!display.contains('/'));
        }
    }

    #[test]
    fn run_failure_is_closed_and_safe() {
        let serving = StdioRunError::Serving;
        assert_eq!(serving.to_string(), "riffdb-mcp protocol service failed");
        assert!(!serving.to_string().contains("http"));
        assert!(!serving.to_string().contains('/'));
    }
}
