//! Bounded, nonsecret process configuration for `riffdbd`.

#![allow(dead_code)]

use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use riffdb_types::{Audience, Environment};

const MAX_CONFIGURED_PATH_BYTES: usize = 4_096;

/// Complete P1 process configuration.
///
/// Command-line values are limited to nonsecret identifiers, socket addresses,
/// and paths. Digest-key contents remain in auth-owned protected-file custody.
pub(crate) struct ServerConfig {
    database_path: PathBuf,
    listen_address: SocketAddr,
    environment: Environment,
    audience: Audience,
    capability_key_path: PathBuf,
    idempotency_key_path: PathBuf,
}

impl ServerConfig {
    pub(crate) fn from_process_args() -> Result<Self, ServerConfigError> {
        Self::parse(std::env::args_os().skip(1))
    }

    fn parse(arguments: impl IntoIterator<Item = OsString>) -> Result<Self, ServerConfigError> {
        let mut arguments = arguments.into_iter();
        let mut database_path = None;
        let mut listen_address = None;
        let mut environment = None;
        let mut audience = None;
        let mut capability_key_path = None;
        let mut idempotency_key_path = None;

        while let Some(flag) = arguments.next() {
            let value = arguments.next().ok_or(ServerConfigError::MissingValue)?;
            match flag.to_str() {
                Some("--database") => set_once(
                    &mut database_path,
                    bounded_path(value)?,
                    ServerConfigError::DuplicateOption,
                )?,
                Some("--listen") => set_once(
                    &mut listen_address,
                    parse_loopback_address(&value)?,
                    ServerConfigError::DuplicateOption,
                )?,
                Some("--environment") => set_once(
                    &mut environment,
                    parse_environment(value)?,
                    ServerConfigError::DuplicateOption,
                )?,
                Some("--audience") => set_once(
                    &mut audience,
                    parse_audience(value)?,
                    ServerConfigError::DuplicateOption,
                )?,
                Some("--capability-keys") => set_once(
                    &mut capability_key_path,
                    bounded_path(value)?,
                    ServerConfigError::DuplicateOption,
                )?,
                Some("--idempotency-keys") => set_once(
                    &mut idempotency_key_path,
                    bounded_path(value)?,
                    ServerConfigError::DuplicateOption,
                )?,
                _ => return Err(ServerConfigError::UnknownOption),
            }
        }

        let config = Self {
            database_path: database_path.ok_or(ServerConfigError::MissingOption)?,
            listen_address: listen_address.ok_or(ServerConfigError::MissingOption)?,
            environment: environment.ok_or(ServerConfigError::MissingOption)?,
            audience: audience.ok_or(ServerConfigError::MissingOption)?,
            capability_key_path: capability_key_path.ok_or(ServerConfigError::MissingOption)?,
            idempotency_key_path: idempotency_key_path.ok_or(ServerConfigError::MissingOption)?,
        };
        config.validate_disjoint_paths()?;
        Ok(config)
    }

    fn validate_disjoint_paths(&self) -> Result<(), ServerConfigError> {
        if self.database_path == self.capability_key_path
            || self.database_path == self.idempotency_key_path
            || self.capability_key_path == self.idempotency_key_path
        {
            return Err(ServerConfigError::OverlappingPaths);
        }
        Ok(())
    }

    pub(crate) fn database_path(&self) -> &Path {
        &self.database_path
    }

    pub(crate) const fn listen_address(&self) -> SocketAddr {
        self.listen_address
    }

    pub(crate) const fn environment(&self) -> &Environment {
        &self.environment
    }

    pub(crate) const fn audience(&self) -> &Audience {
        &self.audience
    }

    pub(crate) fn capability_key_path(&self) -> &Path {
        &self.capability_key_path
    }

    pub(crate) fn idempotency_key_path(&self) -> &Path {
        &self.idempotency_key_path
    }
}

impl fmt::Debug for ServerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServerConfig")
            .field("database_path", &"[CONFIGURED]")
            .field("listen_address", &self.listen_address)
            .field("environment", &self.environment)
            .field("audience", &"[CONFIGURED]")
            .field("capability_key_path", &"[CONFIGURED]")
            .field("idempotency_key_path", &"[CONFIGURED]")
            .finish()
    }
}

fn set_once<T>(
    destination: &mut Option<T>,
    value: T,
    duplicate: ServerConfigError,
) -> Result<(), ServerConfigError> {
    if destination.replace(value).is_some() {
        Err(duplicate)
    } else {
        Ok(())
    }
}

fn bounded_path(value: OsString) -> Result<PathBuf, ServerConfigError> {
    if value.is_empty() || value.as_os_str().as_encoded_bytes().len() > MAX_CONFIGURED_PATH_BYTES {
        return Err(ServerConfigError::InvalidPath);
    }
    Ok(PathBuf::from(value))
}

fn parse_loopback_address(value: &OsStr) -> Result<SocketAddr, ServerConfigError> {
    let value = value
        .to_str()
        .ok_or(ServerConfigError::InvalidListenAddress)?;
    let address = value
        .parse::<SocketAddr>()
        .map_err(|_| ServerConfigError::InvalidListenAddress)?;
    if !address.ip().is_loopback() {
        return Err(ServerConfigError::NonLoopbackListenAddress);
    }
    Ok(address)
}

fn parse_environment(value: OsString) -> Result<Environment, ServerConfigError> {
    value
        .into_string()
        .map_err(|_| ServerConfigError::InvalidEnvironment)
        .and_then(|value| {
            Environment::new(value).map_err(|_| ServerConfigError::InvalidEnvironment)
        })
}

fn parse_audience(value: OsString) -> Result<Audience, ServerConfigError> {
    value
        .into_string()
        .map_err(|_| ServerConfigError::InvalidAudience)
        .and_then(|value| Audience::new(value).map_err(|_| ServerConfigError::InvalidAudience))
}

/// Closed process-configuration failures that never echo a supplied value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ServerConfigError {
    UnknownOption,
    MissingValue,
    DuplicateOption,
    MissingOption,
    InvalidPath,
    OverlappingPaths,
    InvalidListenAddress,
    NonLoopbackListenAddress,
    InvalidEnvironment,
    InvalidAudience,
}

impl fmt::Display for ServerConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::UnknownOption => "unknown riffdbd option",
            Self::MissingValue => "riffdbd option is missing its value",
            Self::DuplicateOption => "riffdbd option was supplied more than once",
            Self::MissingOption => "required riffdbd option is missing",
            Self::InvalidPath => "configured path is invalid",
            Self::OverlappingPaths => "database and key paths must be distinct",
            Self::InvalidListenAddress => "configured listen address is invalid",
            Self::NonLoopbackListenAddress => "POC listen address must be loopback",
            Self::InvalidEnvironment => "configured environment is invalid",
            Self::InvalidAudience => "configured audience is invalid",
        })
    }
}

impl Error for ServerConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_arguments() -> Vec<OsString> {
        [
            "--database",
            "database.redb",
            "--listen",
            "127.0.0.1:0",
            "--environment",
            "development",
            "--audience",
            "riffdb-grpc-loopback",
            "--capability-keys",
            "capability.keys",
            "--idempotency-keys",
            "idempotency.keys",
        ]
        .into_iter()
        .map(OsString::from)
        .collect()
    }

    #[test]
    fn exact_bounded_configuration_is_accepted_without_defaults() {
        let config = ServerConfig::parse(valid_arguments()).expect("valid server configuration");
        assert_eq!(config.database_path(), Path::new("database.redb"));
        assert_eq!(config.listen_address(), "127.0.0.1:0".parse().unwrap());
        assert_eq!(config.environment().as_str(), "development");
        assert_eq!(config.audience().as_str(), "riffdb-grpc-loopback");
    }

    #[test]
    fn every_option_is_required_and_duplicates_or_unknowns_fail_closed() {
        for index in (0..valid_arguments().len()).step_by(2) {
            let mut arguments = valid_arguments();
            arguments.drain(index..=index + 1);
            assert_eq!(
                ServerConfig::parse(arguments).unwrap_err(),
                ServerConfigError::MissingOption
            );
        }

        let mut duplicate = valid_arguments();
        duplicate.extend([OsString::from("--database"), OsString::from("other.redb")]);
        assert_eq!(
            ServerConfig::parse(duplicate).unwrap_err(),
            ServerConfigError::DuplicateOption
        );

        assert_eq!(
            ServerConfig::parse([OsString::from("--unknown"), OsString::from("value")])
                .unwrap_err(),
            ServerConfigError::UnknownOption
        );
    }

    #[test]
    fn only_loopback_socket_addresses_are_allowed() {
        let mut arguments = valid_arguments();
        arguments[3] = OsString::from("0.0.0.0:7337");
        assert_eq!(
            ServerConfig::parse(arguments).unwrap_err(),
            ServerConfigError::NonLoopbackListenAddress
        );
    }

    #[test]
    fn paths_are_bounded_nonempty_and_lexically_disjoint() {
        let mut empty = valid_arguments();
        empty[1] = OsString::new();
        assert_eq!(
            ServerConfig::parse(empty).unwrap_err(),
            ServerConfigError::InvalidPath
        );

        let mut long = valid_arguments();
        long[1] = OsString::from("x".repeat(MAX_CONFIGURED_PATH_BYTES + 1));
        assert_eq!(
            ServerConfig::parse(long).unwrap_err(),
            ServerConfigError::InvalidPath
        );

        let mut overlapping = valid_arguments();
        overlapping[11] = OsString::from("database.redb");
        assert_eq!(
            ServerConfig::parse(overlapping).unwrap_err(),
            ServerConfigError::OverlappingPaths
        );
    }

    #[test]
    fn debug_and_errors_do_not_echo_paths_audience_or_invalid_values() {
        let config = ServerConfig::parse(valid_arguments()).expect("valid configuration");
        let debug = format!("{config:?}");
        assert!(!debug.contains("database.redb"));
        assert!(!debug.contains("riffdb-grpc-loopback"));
        assert!(!debug.contains("capability.keys"));

        let mut arguments = valid_arguments();
        arguments[3] = OsString::from("secret-invalid-address");
        let error = ServerConfig::parse(arguments).unwrap_err().to_string();
        assert!(!error.contains("secret-invalid-address"));
    }
}
