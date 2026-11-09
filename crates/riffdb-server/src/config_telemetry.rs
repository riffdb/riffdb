//! File-only, independently bounded diagnostic signal exporters (ADR-0342).
use super::{ServerConfigError, TelemetryPushConfig, parse_telemetry_push_endpoint};
use riffdb_config::ProtectedFilePath;
use serde::Deserialize;
use std::fmt;
use std::time::Duration;

pub(crate) use riffdb_observability::TelemetrySignal as Signal;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Setting {
    Endpoint,
    BearerTokenFile,
    CaCertificatePath,
    QueueCapacity,
    BatchSize,
    ExportInterval,
    Timeout,
    SamplePerMillion,
}
impl Setting {
    fn name(self) -> &'static str {
        match self {
            Self::Endpoint => "endpoint",
            Self::BearerTokenFile => "bearer_token_file",
            Self::CaCertificatePath => "ca_certificate_path",
            Self::QueueCapacity => "queue_capacity",
            Self::BatchSize => "batch_size",
            Self::ExportInterval => "export_interval_ms",
            Self::Timeout => "timeout_ms",
            Self::SamplePerMillion => "sample_per_million",
        }
    }
    fn rule(self) -> &'static str {
        match self {
            Self::Endpoint => {
                "an explicit full verified HTTPS or literal-loopback HTTP endpoint without userinfo, query or fragment"
            }
            Self::BearerTokenFile | Self::CaCertificatePath => {
                "an absolute normalized protected path, disjoint from data and log paths"
            }
            Self::QueueCapacity => "64..=4096 queued records",
            Self::BatchSize => "1..=512 records and no greater than queue_capacity",
            Self::ExportInterval => "100..=60000 milliseconds",
            Self::Timeout => "100..=10000 milliseconds for the complete export",
            Self::SamplePerMillion => "0..=1000000, on the traces table only",
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Cause {
    InvalidValue,
    MissingValue,
    PathRequired,
    BatchExceedsQueue,
    TracesOnly,
    ProtectedPathOverlap,
}
impl Cause {
    fn name(self) -> &'static str {
        match self {
            Self::InvalidValue => "invalid_value",
            Self::MissingValue => "missing_value",
            Self::PathRequired => "endpoint_path_required",
            Self::BatchExceedsQueue => "batch_exceeds_queue",
            Self::TracesOnly => "traces_only_setting",
            Self::ProtectedPathOverlap => "protected_path_overlap",
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SignalConfigError {
    signal: Signal,
    setting: Setting,
    cause: Cause,
}
impl fmt::Display for SignalConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "telemetry_configuration_invalid: signal={} setting={} cause={} expected={}; fix the selected server.telemetry table and restart",
            self.signal.name(),
            self.setting.name(),
            self.cause.name(),
            self.setting.rule()
        )
    }
}
fn error(signal: Signal, setting: Setting, cause: Cause) -> ServerConfigError {
    ServerConfigError::Telemetry(SignalConfigError {
        signal,
        setting,
        cause,
    })
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TelemetryDocument {
    logs: Option<SignalDocument>,
    traces: Option<SignalDocument>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SignalDocument {
    endpoint: Option<String>,
    bearer_token_file: Option<String>,
    ca_certificate_path: Option<String>,
    queue_capacity: Option<usize>,
    batch_size: Option<usize>,
    export_interval_ms: Option<u64>,
    timeout_ms: Option<u64>,
    sample_per_million: Option<u32>,
}
#[derive(Clone, Default, Eq, PartialEq)]
pub(crate) struct TelemetryConfig {
    pub(crate) logs: Option<SignalConfig>,
    pub(crate) traces: Option<SignalConfig>,
}
impl fmt::Debug for TelemetryConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TelemetryConfig")
            .field("logs", &self.logs.is_some())
            .field("traces", &self.traces.is_some())
            .finish()
    }
}
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct SignalConfig {
    pub(crate) signal: Signal,
    pub(crate) push: TelemetryPushConfig,
    pub(crate) queue_capacity: usize,
    pub(crate) batch_size: usize,
    pub(crate) timeout: Duration,
    pub(crate) sample_per_million: u32,
}
impl fmt::Debug for SignalConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SignalConfig")
            .field("signal", &self.signal)
            .field("endpoint", &"[CONFIGURED]")
            .field("queue_capacity", &self.queue_capacity)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}
impl TelemetryConfig {
    pub(super) fn resolve(document: Option<TelemetryDocument>) -> Result<Self, ServerConfigError> {
        let document = document.unwrap_or_default();
        Ok(Self {
            logs: document.logs.map(|d| d.resolve(Signal::Logs)).transpose()?,
            traces: document
                .traces
                .map(|d| d.resolve(Signal::Traces))
                .transpose()?,
        })
    }
    pub(super) fn validate_paths(
        &self,
        owned: &[(super::ConfiguredPathRole, std::path::PathBuf)],
    ) -> Result<(), ServerConfigError> {
        for config in self.logs.iter().chain(self.traces.iter()) {
            let ca = match config.push.endpoint() {
                super::TelemetryPushEndpoint::Https {
                    ca_certificate_path,
                    ..
                } => ca_certificate_path.as_ref(),
                _ => None,
            };
            for (setting, path) in [
                (Setting::BearerTokenFile, config.push.bearer_token_path()),
                (Setting::CaCertificatePath, ca),
            ] {
                if let Some(path) = path
                    && owned
                        .iter()
                        .any(|(_, owned)| super::paths_overlap(path.as_path(), owned))
                {
                    return Err(error(config.signal, setting, Cause::ProtectedPathOverlap));
                }
            }
            if ca.is_some_and(|ca| config.push.bearer_token_path() == Some(ca)) {
                return Err(error(
                    config.signal,
                    Setting::BearerTokenFile,
                    Cause::ProtectedPathOverlap,
                ));
            }
        }
        Ok(())
    }
}
impl SignalDocument {
    fn resolve(self, signal: Signal) -> Result<SignalConfig, ServerConfigError> {
        let checked = |setting: Setting, value: u64, min: u64, max: u64| {
            if (min..=max).contains(&value) {
                Ok(value)
            } else {
                Err(error(signal, setting, Cause::InvalidValue))
            }
        };
        let queue_capacity = checked(
            Setting::QueueCapacity,
            self.queue_capacity.unwrap_or(2048) as u64,
            64,
            4096,
        )? as usize;
        let batch_size = checked(
            Setting::BatchSize,
            self.batch_size.unwrap_or(128) as u64,
            1,
            512,
        )? as usize;
        if batch_size > queue_capacity {
            return Err(error(signal, Setting::BatchSize, Cause::BatchExceedsQueue));
        }
        let interval = Duration::from_millis(checked(
            Setting::ExportInterval,
            self.export_interval_ms.unwrap_or(1000),
            100,
            60000,
        )?);
        let timeout = Duration::from_millis(checked(
            Setting::Timeout,
            self.timeout_ms.unwrap_or(5000),
            100,
            10000,
        )?);
        if signal == Signal::Logs && self.sample_per_million.is_some() {
            return Err(error(signal, Setting::SamplePerMillion, Cause::TracesOnly));
        }
        let sample_per_million = checked(
            Setting::SamplePerMillion,
            u64::from(self.sample_per_million.unwrap_or(100000)),
            0,
            1000000,
        )? as u32;
        let path = |setting, value: Option<String>| {
            value
                .map(|value| {
                    ProtectedFilePath::new(value)
                        .map_err(|_| error(signal, setting, Cause::InvalidValue))
                })
                .transpose()
        };
        let bearer = path(Setting::BearerTokenFile, self.bearer_token_file)?;
        let ca = path(Setting::CaCertificatePath, self.ca_certificate_path)?;
        // The existing metrics parser retains its historical default path. New
        // signals must supply their full endpoint and never inherit that default.
        let endpoint = self
            .endpoint
            .ok_or_else(|| error(signal, Setting::Endpoint, Cause::MissingValue))?;
        let rest = endpoint
            .split_once("://")
            .map(|(_, rest)| rest)
            .ok_or_else(|| error(signal, Setting::Endpoint, Cause::InvalidValue))?;
        if !rest.contains('/') {
            return Err(error(signal, Setting::Endpoint, Cause::PathRequired));
        }
        let endpoint = parse_telemetry_push_endpoint(&endpoint, ca)
            .map_err(|_| error(signal, Setting::Endpoint, Cause::InvalidValue))?;
        Ok(SignalConfig {
            signal,
            push: TelemetryPushConfig::new(endpoint, interval, bearer),
            queue_capacity,
            batch_size,
            timeout,
            sample_per_million,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn parse(table: &str) -> Result<TelemetryConfig, ServerConfigError> {
        TelemetryConfig::resolve(Some(toml::from_str(table).unwrap()))
    }
    // req: ID-005, NET-002, NET-003, END-007, END-008
    #[test]
    fn signal_configuration_preserves_named_causes_without_echoing_values() {
        for (setting, value) in [
            ("queue_capacity", "63"),
            ("queue_capacity", "4097"),
            ("batch_size", "0"),
            ("batch_size", "513"),
            ("export_interval_ms", "99"),
            ("export_interval_ms", "60001"),
            ("timeout_ms", "99"),
            ("timeout_ms", "10001"),
            ("sample_per_million", "1000001"),
        ] {
            let error = parse(&format!(
                "[traces]\nendpoint='http://127.0.0.1:4318/v1/traces'\n{setting}={value}"
            ))
            .unwrap_err()
            .to_string();
            assert!(error.contains("signal=traces"));
            assert!(error.contains(&format!("setting={setting}")));
            assert!(error.contains("cause=invalid_value"));
        }
        for endpoint in [
            "http://example.com/secret-canary",
            "https://user:secret-canary@example.com/v1/logs",
            "https://example.com/v1/logs?secret-canary",
            "https://example.com/v1/logs#secret-canary",
        ] {
            let error = parse(&format!("[logs]\nendpoint='{endpoint}'"))
                .unwrap_err()
                .to_string();
            assert!(error.contains("setting=endpoint"));
            assert!(!error.contains("secret-canary"));
            assert!(!error.contains("example.com"));
        }
        assert!(
            parse("[logs]")
                .unwrap_err()
                .to_string()
                .contains("cause=missing_value")
        );
        assert!(
            parse("[logs]\nendpoint='http://127.0.0.1:4318' ")
                .unwrap_err()
                .to_string()
                .contains("cause=endpoint_path_required")
        );
        assert!(
            parse("[logs]\nendpoint='http://127.0.0.1:4318/v1/logs'\nsample_per_million=0")
                .unwrap_err()
                .to_string()
                .contains("cause=traces_only_setting")
        );
        assert!(parse("[traces]\nendpoint='http://127.0.0.1:4318/v1/traces'\nqueue_capacity=64\nbatch_size=65").unwrap_err().to_string().contains("cause=batch_exceeds_queue"));
        assert!(
            toml::from_str::<TelemetryDocument>(
                "[logs]\nendpoint='http://127.0.0.1:4318/v1/logs'\ninsecure=true"
            )
            .is_err()
        );
        let config = parse("[traces]\nendpoint='http://127.0.0.1:4318/v1/traces'").unwrap();
        let traces = config.traces.unwrap();
        assert_eq!(
            (
                traces.queue_capacity,
                traces.batch_size,
                traces.sample_per_million
            ),
            (2048, 128, 100000)
        );
        assert_eq!(
            (traces.push.interval(), traces.timeout),
            (Duration::from_secs(1), Duration::from_secs(5))
        );
        assert!(!format!("{traces:?}").contains("127.0.0.1"));
    }
}
