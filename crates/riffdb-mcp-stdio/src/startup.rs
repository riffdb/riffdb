//! Ordered process bootstrap for the public gRPC bridge.

use riffdb_api_mcp::McpStdioClientActivity;
use riffdb_client_rust::{CallMetadata, RiffDbClient, generate_request_id, v1};

use crate::config::{
    StdioConfig, StdioConfigError, load_process_config, load_process_config_after_command,
};
use crate::telemetry::{self, TelemetryInstallError};
use crate::{PublicGrpcMcpBackend, PublicGrpcMcpObserverBackend};

/// A constructed public-client boundary ready for MCP stdio adaptation.
///
/// This value grants no server, service, policy, authentication, or storage
/// authority. It exists as the narrow handoff to `riffdb-api-mcp`.
pub struct StdioBootstrap {
    client: RiffDbClient,
    metadata: CallMetadata,
    selected_database: String,
    expected_audience: Option<String>,
}

impl StdioBootstrap {
    /// Moves the public client and its checked outbound metadata into the MCP
    /// adapter.
    #[must_use]
    pub fn into_parts(self) -> (RiffDbClient, CallMetadata) {
        (self.client, self.metadata)
    }

    fn into_doctor_parts(self) -> (RiffDbClient, CallMetadata, String, Option<String>) {
        (
            self.client,
            self.metadata,
            self.selected_database,
            self.expected_audience,
        )
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
    /// The connected server did not match the retained target identity.
    TargetIdentity,
}

impl std::fmt::Display for StdioStartupError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Configuration => "riffdb-mcp configuration was rejected",
            Self::Telemetry => "riffdb-mcp telemetry initialization failed",
            Self::Connection => "riffdb-mcp could not connect to RiffDB",
            Self::TargetIdentity => "riffdb-mcp could not verify the RiffDB target identity",
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
    /// Stdin ended or the host stopped before successful initialization.
    BeforeInitialization,
    /// The bounded common stdio transport stopped after initialization.
    Serving,
}

impl std::fmt::Display for StdioRunError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Startup(error) => error.fmt(formatter),
            Self::BeforeInitialization => formatter.write_str(
                "riffdb-mcp received EOF before MCP initialize; use an MCP host or run `riffdb-mcp doctor`",
            ),
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
    bootstrap_with_config(config).await
}

async fn bootstrap_with_config(config: StdioConfig) -> Result<StdioBootstrap, StdioStartupError> {
    let (endpoint, database, credential, expected_audience) = config.into_parts();
    let selected_database = database.as_str().to_owned();
    let endpoint = endpoint
        .parse()
        .map_err(|_| StdioStartupError::Configuration)?;
    let client = RiffDbClient::connect(endpoint)
        .await
        .map_err(|_| StdioStartupError::Connection)?;
    Ok(StdioBootstrap {
        client,
        metadata: CallMetadata::authenticated(credential).with_database(database),
        selected_database,
        expected_audience,
    })
}

/// Verifies configuration, credential loading, connection, routing, and one
/// operation authorized for the selected credential without starting MCP.
pub async fn doctor() -> Result<String, StdioStartupError> {
    telemetry::install()?;
    let config = load_process_config_after_command()?;
    let (mut client, metadata, selected_database, expected_audience) =
        bootstrap_with_config(config).await?.into_doctor_parts();
    let request_id = generate_request_id().map_err(|_| StdioStartupError::Configuration)?;
    if let Ok(response) = client
        .health(
            v1::HealthRequest {
                request_id: Some(request_id.into_bytes().to_vec()),
            },
            &metadata,
        )
        .await
    {
        if expected_audience
            .as_deref()
            .is_some_and(|expected| expected != response.authentication_audience)
        {
            return Err(StdioStartupError::TargetIdentity);
        }
        return doctor_report(&response.database_alias, &response.authentication_audience);
    }

    let request_id = generate_request_id().map_err(|_| StdioStartupError::Configuration)?;
    client
        .discover_command_tools(
            v1::DiscoverCommandToolsRequest {
                request_id: request_id.into_bytes().to_vec(),
                page: Some(v1::PageRequest {
                    limit: Some(1),
                    cursor: None,
                }),
                prior_fence: None,
                representation: v1::DiscoveryRepresentation::Full as i32,
            },
            &metadata,
        )
        .await
        .map_err(|_| StdioStartupError::Connection)?;
    let expected_audience = expected_audience.ok_or(StdioStartupError::TargetIdentity)?;
    doctor_report(&selected_database, &expected_audience)
}

fn doctor_report(database: &str, audience: &str) -> Result<String, StdioStartupError> {
    serde_json::to_string(&serde_json::json!({
        "ok": true,
        "database": database,
        "audience": audience,
        "status": "connected"
    }))
    .map_err(|_| StdioStartupError::Configuration)
}

/// Runs one public-client MCP stdio process to transport completion.
pub async fn run() -> Result<(), StdioRunError> {
    let (client, metadata) = bootstrap()
        .await
        .map_err(StdioRunError::Startup)?
        .into_parts();
    let client_activity = McpStdioClientActivity::new();
    let lifecycle_observation = client_activity.clone();
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
    .map_err(|_| {
        if lifecycle_observation.was_initialized() {
            StdioRunError::Serving
        } else {
            StdioRunError::BeforeInitialization
        }
    })
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
            StdioStartupError::TargetIdentity,
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
        let before = StdioRunError::BeforeInitialization;
        assert!(before.to_string().contains("EOF"));
        assert!(before.to_string().contains("initialize"));
        assert!(before.to_string().contains("doctor"));
        assert!(!before.to_string().contains("http"));
        assert!(!before.to_string().contains('/'));
        assert_eq!(
            StdioRunError::Serving.to_string(),
            "riffdb-mcp protocol service failed"
        );
    }
}
