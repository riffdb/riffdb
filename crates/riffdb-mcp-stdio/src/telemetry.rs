//! Non-reloadable process telemetry boundary.

use std::io;

use tracing_subscriber::Layer as _;
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// A closed failure to install the sole process-global subscriber.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TelemetryInstallError;

impl std::fmt::Display for TelemetryInstallError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("the MCP telemetry boundary could not be installed")
    }
}

impl std::error::Error for TelemetryInstallError {}

/// Installs formatting to stderr under the common hard SDK-target filter.
///
/// The predicate is the final outer layer and no reload handle or
/// user-controlled directive exists. Failure is returned before any client or
/// MCP service construction.
pub(crate) fn install() -> Result<(), TelemetryInstallError> {
    let safe_target_filter = filter_fn(|metadata| allows_stderr_format_target(metadata.target()));
    let formatting = tracing_subscriber::fmt::layer()
        .with_writer(io::stderr)
        .with_ansi(false)
        .with_filter(safe_target_filter);
    let hard_filter =
        filter_fn(|metadata| riffdb_api_mcp::allows_transport_trace_target(metadata.target()));
    tracing_subscriber::registry()
        .with(formatting)
        .with(hard_filter)
        .try_init()
        .map_err(|_| TelemetryInstallError)
}

fn allows_stderr_format_target(target: &str) -> bool {
    riffdb_api_mcp::is_safe_transport_trace_target(target)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn install_error_is_closed_and_redaction_safe() {
        assert_eq!(
            format!("{:?}", TelemetryInstallError),
            "TelemetryInstallError"
        );
        assert_eq!(
            TelemetryInstallError.to_string(),
            "the MCP telemetry boundary could not be installed"
        );
    }

    #[test]
    fn a_second_global_install_is_rejected() {
        assert_eq!(install(), Ok(()));
        assert_eq!(install(), Err(TelemetryInstallError));
    }

    #[test]
    fn only_the_closed_safe_target_can_reach_stderr_formatting() {
        assert!(allows_stderr_format_target(
            riffdb_api_mcp::MCP_SAFE_TRANSPORT_TRACE_TARGET
        ));
        for unrelated_target in [
            "riffdb_api_mcp",
            "riffdb_api_mcp::transport::child",
            "riffdb_client_rust",
            "tonic",
            "hyper",
            "h2",
        ] {
            assert!(!allows_stderr_format_target(unrelated_target));
        }
    }
}
