//! Bounded process-only logging selection. Environment filters cannot widen safety.

use serde::Deserialize;
use std::collections::BTreeMap;
use tracing::Level;

use super::{ArgumentValues, EnvironmentSource, ServerConfigError};
use crate::operational_logging::Component;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum LogFormat {
    #[default]
    Text,
    Json,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum LogLevel {
    Error,
    Warn,
    #[default]
    Info,
    Debug,
    Trace,
}

impl LogLevel {
    fn parse(value: &str) -> Result<Self, ServerConfigError> {
        match value {
            "error" => Ok(Self::Error),
            "warn" => Ok(Self::Warn),
            "info" => Ok(Self::Info),
            "debug" => Ok(Self::Debug),
            "trace" => Ok(Self::Trace),
            _ => Err(ServerConfigError::InvalidLoggingConfiguration),
        }
    }

    pub(crate) const fn allows(self, level: Level) -> bool {
        let maximum = match self {
            Self::Error => 1,
            Self::Warn => 2,
            Self::Info => 3,
            Self::Debug => 4,
            Self::Trace => 5,
        };
        let observed = match level {
            Level::ERROR => 1,
            Level::WARN => 2,
            Level::INFO => 3,
            Level::DEBUG => 4,
            Level::TRACE => 5,
        };
        observed <= maximum
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LoggingDocument {
    format: Option<LogFormat>,
    level: Option<LogLevel>,
    stderr: Option<bool>,
    #[serde(default)]
    components: BTreeMap<String, LogLevel>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LoggingConfig {
    pub(crate) format: LogFormat,
    pub(crate) level: LogLevel,
    pub(crate) stderr: bool,
    pub(crate) components: BTreeMap<Component, LogLevel>,
}

impl LoggingConfig {
    pub(super) fn resolve(
        arguments: &ArgumentValues,
        environment: &dyn EnvironmentSource,
        document: Option<LoggingDocument>,
    ) -> Result<Self, ServerConfigError> {
        let document = document.unwrap_or_default();
        let selected = |cli: &Option<std::ffi::OsString>,
                        key: &str|
         -> Result<Option<String>, ServerConfigError> {
            cli.clone()
                .or_else(|| environment.value(key))
                .map(|s| {
                    s.into_string()
                        .map_err(|_| ServerConfigError::InvalidLoggingConfiguration)
                })
                .transpose()
        };
        let format = match selected(&arguments.log_format, "RIFFDB_LOG_FORMAT")? {
            Some(v) if v == "text" => LogFormat::Text,
            Some(v) if v == "json" => LogFormat::Json,
            Some(_) => return Err(ServerConfigError::InvalidLoggingConfiguration),
            None => document.format.unwrap_or_default(),
        };
        let level = selected(&arguments.log_level, "RIFFDB_LOG_LEVEL")?
            .as_deref()
            .map(LogLevel::parse)
            .transpose()?
            .or(document.level)
            .unwrap_or_default();
        let stderr = match selected(&arguments.log_stderr, "RIFFDB_LOG_STDERR")? {
            Some(v) if v == "true" => true,
            Some(v) if v == "false" => false,
            Some(_) => return Err(ServerConfigError::InvalidLoggingConfiguration),
            None => document.stderr.unwrap_or(true),
        };
        // File/OTLP sinks extend this invariant in their owning packages.
        if !stderr {
            return Err(ServerConfigError::InvalidLoggingConfiguration);
        }
        let mut components = BTreeMap::new();
        let component_entries = selected(&arguments.log_components, "RIFFDB_LOG_COMPONENTS")?;
        if let Some(entries) = component_entries {
            if entries.len() > 1024 {
                return Err(ServerConfigError::InvalidLoggingConfiguration);
            }
            for entry in entries.split(',') {
                let (name, value) = entry
                    .split_once('=')
                    .ok_or(ServerConfigError::InvalidLoggingConfiguration)?;
                let component =
                    Component::parse(name).ok_or(ServerConfigError::InvalidLoggingConfiguration)?;
                if components
                    .insert(component, LogLevel::parse(value)?)
                    .is_some()
                {
                    return Err(ServerConfigError::InvalidLoggingConfiguration);
                }
            }
        } else {
            if document.components.len() > 32 {
                return Err(ServerConfigError::InvalidLoggingConfiguration);
            }
            for (name, level) in document.components {
                components.insert(
                    Component::parse(&name)
                        .ok_or(ServerConfigError::InvalidLoggingConfiguration)?,
                    level,
                );
            }
        }
        Ok(Self {
            format,
            level,
            stderr,
            components,
        })
    }

    pub(crate) fn allows(&self, component: Component, level: Level) -> bool {
        self.components
            .get(&component)
            .copied()
            .unwrap_or(self.level)
            .allows(level)
    }
}
