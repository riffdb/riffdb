//! Bounded, nonsecret process configuration for `riffdbd`.

#![allow(dead_code)]

use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;
use std::path::{Component, Path, PathBuf};

use riffdb_api_mcp::{
    MAX_ALLOWED_ORIGIN_AGGREGATE_BYTES, MAX_ALLOWED_ORIGINS, MAX_ORIGIN_BYTES, MCP_ROUTE,
};
use riffdb_types::{Audience, Environment};
use serde::Deserialize;

const MAX_CONFIGURED_PATH_BYTES: usize = 4_096;
const MAX_CONFIG_DOCUMENT_BYTES: u64 = 65_536;

const DEFAULT_DATABASE_PATH: &str = "data/riffdb.redb";
const DEFAULT_LISTEN_ADDRESS: &str = "127.0.0.1:7443";
const DEFAULT_ENVIRONMENT: &str = "local";
const DEFAULT_AUDIENCE: &str = "riffdb-grpc-loopback";
const DEFAULT_CAPABILITY_KEY_PATH: &str = "config/capability.keys";
const DEFAULT_IDEMPOTENCY_KEY_PATH: &str = "config/idempotency.keys";

const CONFIG_ENVIRONMENT: &str = "RIFFDB_CONFIG";
const DATABASE_ENVIRONMENT: &str = "RIFFDB_DATABASE";
const LISTEN_ENVIRONMENT: &str = "RIFFDB_LISTEN";
const ENVIRONMENT_ENVIRONMENT: &str = "RIFFDB_ENVIRONMENT";
const AUDIENCE_ENVIRONMENT: &str = "RIFFDB_AUDIENCE";
const MCP_LISTEN_ENVIRONMENT: &str = "RIFFDB_MCP_LISTEN";
const MCP_ORIGINS_ENVIRONMENT: &str = "RIFFDB_MCP_ORIGINS";
const BACKUP_ROOT_ENVIRONMENT: &str = "RIFFDB_BACKUP_ROOT";
const CAPABILITY_KEYS_ENVIRONMENT: &str = "RIFFDB_CAPABILITY_KEYS";
const IDEMPOTENCY_KEYS_ENVIRONMENT: &str = "RIFFDB_IDEMPOTENCY_KEYS";

/// Complete POC process configuration.
///
/// Every field resolves independently as CLI, environment, explicitly selected
/// TOML, then a safe local default. Digest-key contents remain in auth-owned
/// protected-file custody; this configuration carries paths only.
pub(crate) struct ServerConfig {
    database_path: PathBuf,
    listen_address: SocketAddr,
    environment: Environment,
    audience: Audience,
    mcp_listen_address: Option<SocketAddr>,
    mcp_origins: Vec<String>,
    mcp_audience: Option<Audience>,
    backup_root: PathBuf,
    capability_key_path: PathBuf,
    idempotency_key_path: PathBuf,
}

impl ServerConfig {
    pub(crate) fn from_process_args() -> Result<Self, ServerConfigError> {
        let current_directory =
            std::env::current_dir().map_err(|_| ServerConfigError::InvalidPath)?;
        Self::resolve(
            std::env::args_os().skip(1),
            &ProcessEnvironment,
            &current_directory,
        )
    }

    fn parse(arguments: impl IntoIterator<Item = OsString>) -> Result<Self, ServerConfigError> {
        Self::resolve(
            arguments,
            &EmptyEnvironment,
            Path::new("/tmp/riffdb-config"),
        )
    }

    fn resolve(
        arguments: impl IntoIterator<Item = OsString>,
        environment: &dyn EnvironmentSource,
        current_directory: &Path,
    ) -> Result<Self, ServerConfigError> {
        let arguments = ArgumentValues::parse(arguments)?;
        let config_path = select_optional_os(
            arguments.config.as_ref(),
            environment.value(CONFIG_ENVIRONMENT),
            None,
        )?
        .map(bounded_path)
        .transpose()?;
        let document = config_path
            .as_deref()
            .map(read_document)
            .transpose()?
            .unwrap_or_default();
        let server = document.server.unwrap_or_default();
        let maintenance = document.maintenance.unwrap_or_default();

        let database_path = bounded_path(select_os(
            arguments.database.as_ref(),
            environment.value(DATABASE_ENVIRONMENT),
            server.database.map(OsString::from),
            OsString::from(DEFAULT_DATABASE_PATH),
        )?)?;
        let listen_address = parse_loopback_address(&select_os(
            arguments.listen.as_ref(),
            environment.value(LISTEN_ENVIRONMENT),
            server.grpc_listen.map(OsString::from),
            OsString::from(DEFAULT_LISTEN_ADDRESS),
        )?)?;
        let configured_environment = parse_environment(select_os(
            arguments.environment.as_ref(),
            environment.value(ENVIRONMENT_ENVIRONMENT),
            server.environment.map(OsString::from),
            OsString::from(DEFAULT_ENVIRONMENT),
        )?)?;
        let audience = parse_audience(select_os(
            arguments.audience.as_ref(),
            environment.value(AUDIENCE_ENVIRONMENT),
            server.audience.map(OsString::from),
            OsString::from(DEFAULT_AUDIENCE),
        )?)?;
        let mcp_listen_address = select_optional_os(
            arguments.mcp_listen.as_ref(),
            environment.value(MCP_LISTEN_ENVIRONMENT),
            server.mcp_listen.map(OsString::from),
        )?
        .map(|value| parse_mcp_loopback_address(&value))
        .transpose()?;
        let mcp_origins = resolve_mcp_origins(
            &arguments.mcp_origins,
            environment.value(MCP_ORIGINS_ENVIRONMENT),
            server.mcp_origins,
        )?;
        let backup_root = bounded_absolute_directory(select_os(
            arguments.backup_root.as_ref(),
            environment.value(BACKUP_ROOT_ENVIRONMENT),
            maintenance.backup_root.map(OsString::from),
            current_directory.join("backups").into_os_string(),
        )?)?;
        let capability_key_path = bounded_path(select_os(
            arguments.capability_keys.as_ref(),
            environment.value(CAPABILITY_KEYS_ENVIRONMENT),
            server.capability_keys.map(OsString::from),
            OsString::from(DEFAULT_CAPABILITY_KEY_PATH),
        )?)?;
        let idempotency_key_path = bounded_path(select_os(
            arguments.idempotency_keys.as_ref(),
            environment.value(IDEMPOTENCY_KEYS_ENVIRONMENT),
            server.idempotency_keys.map(OsString::from),
            OsString::from(DEFAULT_IDEMPOTENCY_KEY_PATH),
        )?)?;

        if mcp_listen_address.is_none() && !mcp_origins.is_empty() {
            return Err(ServerConfigError::InvalidMcpConfiguration);
        }
        let mcp_audience = mcp_listen_address
            .map(|address| Audience::new(format!("http://{address}{MCP_ROUTE}")))
            .transpose()
            .map_err(|_| ServerConfigError::InvalidMcpConfiguration)?;
        if mcp_audience.as_ref() == Some(&audience) {
            return Err(ServerConfigError::InvalidMcpConfiguration);
        }

        let config = Self {
            database_path,
            listen_address,
            environment: configured_environment,
            audience,
            mcp_listen_address,
            mcp_origins,
            mcp_audience,
            backup_root,
            capability_key_path,
            idempotency_key_path,
        };
        config.validate_disjoint_paths(current_directory)?;
        Ok(config)
    }

    fn validate_disjoint_paths(&self, current_directory: &Path) -> Result<(), ServerConfigError> {
        let paths = [
            lexical_absolute(&self.database_path, current_directory)?,
            lexical_absolute(&self.backup_root, current_directory)?,
            lexical_absolute(&self.capability_key_path, current_directory)?,
            lexical_absolute(&self.idempotency_key_path, current_directory)?,
        ];
        if paths.iter().enumerate().any(|(left_index, left)| {
            paths
                .iter()
                .skip(left_index + 1)
                .any(|right| paths_overlap(left, right))
        }) {
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

    pub(crate) const fn mcp_listen_address(&self) -> Option<SocketAddr> {
        self.mcp_listen_address
    }

    pub(crate) fn mcp_origins(&self) -> &[String] {
        &self.mcp_origins
    }

    pub(crate) const fn mcp_audience(&self) -> Option<&Audience> {
        self.mcp_audience.as_ref()
    }

    pub(crate) fn backup_root(&self) -> &Path {
        &self.backup_root
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
            .field("mcp_listen_address", &self.mcp_listen_address)
            .field("mcp_origins", &"[CONFIGURED]")
            .field("mcp_audience", &"[DERIVED]")
            .field("backup_root", &"[CONFIGURED]")
            .field("capability_key_path", &"[CONFIGURED]")
            .field("idempotency_key_path", &"[CONFIGURED]")
            .finish()
    }
}

#[derive(Default)]
struct ArgumentValues {
    config: Option<OsString>,
    database: Option<OsString>,
    listen: Option<OsString>,
    environment: Option<OsString>,
    audience: Option<OsString>,
    mcp_listen: Option<OsString>,
    mcp_origins: Vec<OsString>,
    backup_root: Option<OsString>,
    capability_keys: Option<OsString>,
    idempotency_keys: Option<OsString>,
}

impl ArgumentValues {
    fn parse(arguments: impl IntoIterator<Item = OsString>) -> Result<Self, ServerConfigError> {
        let mut values = Self::default();
        let mut arguments = arguments.into_iter();
        while let Some(flag) = arguments.next() {
            let value = arguments.next().ok_or(ServerConfigError::MissingValue)?;
            match flag.to_str() {
                Some("--config") => set_once(&mut values.config, value)?,
                Some("--database") => set_once(&mut values.database, value)?,
                Some("--listen") => set_once(&mut values.listen, value)?,
                Some("--environment") => set_once(&mut values.environment, value)?,
                Some("--audience") => set_once(&mut values.audience, value)?,
                Some("--mcp-listen") => set_once(&mut values.mcp_listen, value)?,
                Some("--mcp-origin") => values.mcp_origins.push(value),
                Some("--backup-root") => set_once(&mut values.backup_root, value)?,
                Some("--capability-keys") => set_once(&mut values.capability_keys, value)?,
                Some("--idempotency-keys") => set_once(&mut values.idempotency_keys, value)?,
                _ => return Err(ServerConfigError::UnknownOption),
            }
        }
        Ok(values)
    }
}

fn set_once(destination: &mut Option<OsString>, value: OsString) -> Result<(), ServerConfigError> {
    if destination.replace(value).is_some() {
        Err(ServerConfigError::DuplicateOption)
    } else {
        Ok(())
    }
}

trait EnvironmentSource {
    fn value(&self, name: &str) -> Option<OsString>;
}

struct ProcessEnvironment;

impl EnvironmentSource for ProcessEnvironment {
    fn value(&self, name: &str) -> Option<OsString> {
        std::env::var_os(name)
    }
}

struct EmptyEnvironment;

impl EnvironmentSource for EmptyEnvironment {
    fn value(&self, _name: &str) -> Option<OsString> {
        None
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigDocument {
    server: Option<ServerDocument>,
    maintenance: Option<MaintenanceDocument>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerDocument {
    database: Option<String>,
    grpc_listen: Option<String>,
    environment: Option<String>,
    audience: Option<String>,
    mcp_listen: Option<String>,
    mcp_origins: Option<Vec<String>>,
    capability_keys: Option<String>,
    idempotency_keys: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct MaintenanceDocument {
    backup_root: Option<String>,
}

fn read_document(path: &Path) -> Result<ConfigDocument, ServerConfigError> {
    let file = File::open(path).map_err(|_| ServerConfigError::InvalidConfigDocument)?;
    let mut bytes = Vec::new();
    file.take(MAX_CONFIG_DOCUMENT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| ServerConfigError::InvalidConfigDocument)?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_CONFIG_DOCUMENT_BYTES {
        return Err(ServerConfigError::InvalidConfigDocument);
    }
    let text = std::str::from_utf8(&bytes).map_err(|_| ServerConfigError::InvalidConfigDocument)?;
    toml::from_str(text).map_err(|_| ServerConfigError::InvalidConfigDocument)
}

fn select_os(
    flag: Option<&OsString>,
    environment: Option<OsString>,
    document: Option<OsString>,
    default: OsString,
) -> Result<OsString, ServerConfigError> {
    let value = flag
        .cloned()
        .or(environment)
        .or(document)
        .unwrap_or(default);
    if value.is_empty() {
        return Err(ServerConfigError::InvalidConfiguredValue);
    }
    Ok(value)
}

fn select_optional_os(
    flag: Option<&OsString>,
    environment: Option<OsString>,
    document: Option<OsString>,
) -> Result<Option<OsString>, ServerConfigError> {
    let selected = flag.cloned().or(environment).or(document);
    if selected.as_ref().is_some_and(|value| value.is_empty()) {
        return Err(ServerConfigError::InvalidConfiguredValue);
    }
    Ok(selected)
}

fn resolve_mcp_origins(
    arguments: &[OsString],
    environment: Option<OsString>,
    document: Option<Vec<String>>,
) -> Result<Vec<String>, ServerConfigError> {
    let raw = if !arguments.is_empty() {
        arguments.to_vec()
    } else if let Some(environment) = environment {
        let environment = environment
            .into_string()
            .map_err(|_| ServerConfigError::InvalidMcpConfiguration)?;
        if environment.is_empty() {
            return Err(ServerConfigError::InvalidMcpConfiguration);
        }
        environment.split(',').map(OsString::from).collect()
    } else {
        document
            .unwrap_or_default()
            .into_iter()
            .map(OsString::from)
            .collect()
    };

    let mut origins = Vec::with_capacity(raw.len());
    let mut aggregate_bytes = 0_usize;
    for value in raw {
        let origin = parse_mcp_origin(value)?;
        aggregate_bytes = aggregate_bytes
            .checked_add(origin.len())
            .ok_or(ServerConfigError::InvalidMcpConfiguration)?;
        if origins.len() >= MAX_ALLOWED_ORIGINS
            || aggregate_bytes > MAX_ALLOWED_ORIGIN_AGGREGATE_BYTES
            || origins.iter().any(|existing| existing == &origin)
        {
            return Err(ServerConfigError::InvalidMcpConfiguration);
        }
        origins.push(origin);
    }
    Ok(origins)
}

fn bounded_path(value: OsString) -> Result<PathBuf, ServerConfigError> {
    if value.is_empty() || value.as_os_str().as_encoded_bytes().len() > MAX_CONFIGURED_PATH_BYTES {
        return Err(ServerConfigError::InvalidPath);
    }
    Ok(PathBuf::from(value))
}

fn bounded_absolute_directory(value: OsString) -> Result<PathBuf, ServerConfigError> {
    let path = bounded_path(value)?;
    if !path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::RootDir | Component::Normal(_)))
        || path.parent().is_none()
    {
        return Err(ServerConfigError::InvalidPath);
    }
    Ok(path)
}

fn lexical_absolute(path: &Path, current_directory: &Path) -> Result<PathBuf, ServerConfigError> {
    if !current_directory.is_absolute() {
        return Err(ServerConfigError::InvalidPath);
    }
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        current_directory.join(path)
    };
    if absolute
        .components()
        .any(|component| !matches!(component, Component::RootDir | Component::Normal(_)))
    {
        return Err(ServerConfigError::InvalidPath);
    }
    Ok(absolute)
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left == right || left.starts_with(right) || right.starts_with(left)
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

fn parse_mcp_loopback_address(value: &OsStr) -> Result<SocketAddr, ServerConfigError> {
    let address = parse_loopback_address(value)?;
    if address.port() == 0 {
        return Err(ServerConfigError::InvalidMcpConfiguration);
    }
    Ok(address)
}

fn parse_mcp_origin(value: OsString) -> Result<String, ServerConfigError> {
    let value = value
        .into_string()
        .map_err(|_| ServerConfigError::InvalidMcpConfiguration)?;
    if value.is_empty()
        || value.len() > MAX_ORIGIN_BYTES
        || !value.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
    {
        return Err(ServerConfigError::InvalidMcpConfiguration);
    }
    Ok(value)
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
    InvalidConfiguredValue,
    InvalidConfigDocument,
    InvalidPath,
    OverlappingPaths,
    InvalidListenAddress,
    NonLoopbackListenAddress,
    InvalidEnvironment,
    InvalidAudience,
    InvalidMcpConfiguration,
}

impl fmt::Display for ServerConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::UnknownOption => "unknown riffdbd option",
            Self::MissingValue => "riffdbd option is missing its value",
            Self::DuplicateOption => "riffdbd option was supplied more than once",
            Self::InvalidConfiguredValue => "configured riffdbd value is invalid",
            Self::InvalidConfigDocument => "riffdbd configuration document is invalid",
            Self::InvalidPath => "configured path is invalid",
            Self::OverlappingPaths => "database, backup, and key paths must be disjoint",
            Self::InvalidListenAddress => "configured listen address is invalid",
            Self::NonLoopbackListenAddress => "POC listen address must be loopback",
            Self::InvalidEnvironment => "configured environment is invalid",
            Self::InvalidAudience => "configured audience is invalid",
            Self::InvalidMcpConfiguration => "configured MCP endpoint is invalid",
        })
    }
}

impl Error for ServerConfigError {}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static NEXT_ROOT: AtomicU64 = AtomicU64::new(1);

    #[derive(Default)]
    struct TestEnvironment(BTreeMap<String, OsString>);

    impl TestEnvironment {
        fn with(mut self, name: &str, value: &str) -> Self {
            self.0.insert(name.to_owned(), OsString::from(value));
            self
        }
    }

    impl EnvironmentSource for TestEnvironment {
        fn value(&self, name: &str) -> Option<OsString> {
            self.0.get(name).cloned()
        }
    }

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new() -> Self {
            let id = NEXT_ROOT.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("riffdb-server-config-{}-{id}", std::process::id()));
            fs::create_dir_all(&path).expect("create config test root");
            Self(path)
        }

        fn write(&self, contents: &[u8]) -> PathBuf {
            let path = self.0.join("riffdb.toml");
            fs::write(&path, contents).expect("write config");
            path
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

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
            "--backup-root",
            "/var/lib/riffdb/backups",
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
    fn exact_bounded_cli_configuration_is_accepted() {
        let config = ServerConfig::parse(valid_arguments()).expect("valid server configuration");
        assert_eq!(config.database_path(), Path::new("database.redb"));
        assert_eq!(config.listen_address(), "127.0.0.1:0".parse().unwrap());
        assert_eq!(config.environment().as_str(), "development");
        assert_eq!(config.audience().as_str(), "riffdb-grpc-loopback");
        assert_eq!(config.mcp_listen_address(), None);
        assert!(config.mcp_origins().is_empty());
        assert_eq!(config.mcp_audience(), None);
        assert_eq!(config.backup_root(), Path::new("/var/lib/riffdb/backups"));
    }

    #[test]
    fn safe_local_defaults_are_complete() {
        let config = ServerConfig::parse(Vec::new()).expect("defaults");
        assert_eq!(config.database_path(), Path::new(DEFAULT_DATABASE_PATH));
        assert_eq!(
            config.listen_address(),
            DEFAULT_LISTEN_ADDRESS.parse().unwrap()
        );
        assert_eq!(config.environment().as_str(), DEFAULT_ENVIRONMENT);
        assert_eq!(config.audience().as_str(), DEFAULT_AUDIENCE);
        assert_eq!(
            config.backup_root(),
            Path::new("/tmp/riffdb-config/backups")
        );
        assert_eq!(
            config.capability_key_path(),
            Path::new(DEFAULT_CAPABILITY_KEY_PATH)
        );
        assert_eq!(
            config.idempotency_key_path(),
            Path::new(DEFAULT_IDEMPOTENCY_KEY_PATH)
        );
    }

    #[test]
    fn cli_environment_toml_and_default_precedence_is_per_field() {
        let root = TestRoot::new();
        let document = root.write(
            br#"
[server]
database = "toml.redb"
grpc_listen = "127.0.0.1:7001"
environment = "toml"
audience = "toml-audience"
capability_keys = "toml-capability.keys"
idempotency_keys = "toml-idempotency.keys"

[maintenance]
backup_root = "/tmp/riffdb-toml-backups"
"#,
        );
        let environment = TestEnvironment::default()
            .with(CONFIG_ENVIRONMENT, document.to_str().unwrap())
            .with(DATABASE_ENVIRONMENT, "environment.redb")
            .with(ENVIRONMENT_ENVIRONMENT, "environment")
            .with(CAPABILITY_KEYS_ENVIRONMENT, "environment-capability.keys");
        let arguments =
            ["--listen", "127.0.0.1:7002", "--audience", "cli-audience"].map(OsString::from);
        let config =
            ServerConfig::resolve(arguments, &environment, &root.0).expect("resolved config");

        assert_eq!(config.database_path(), Path::new("environment.redb"));
        assert_eq!(config.listen_address(), "127.0.0.1:7002".parse().unwrap());
        assert_eq!(config.environment().as_str(), "environment");
        assert_eq!(config.audience().as_str(), "cli-audience");
        assert_eq!(
            config.capability_key_path(),
            Path::new("environment-capability.keys")
        );
        assert_eq!(
            config.idempotency_key_path(),
            Path::new("toml-idempotency.keys")
        );
        assert_eq!(config.backup_root(), Path::new("/tmp/riffdb-toml-backups"));
    }

    #[test]
    fn invalid_higher_precedence_value_does_not_fall_through() {
        let root = TestRoot::new();
        let document = root.write(
            br#"
[server]
grpc_listen = "127.0.0.1:7001"
"#,
        );
        let environment = TestEnvironment::default()
            .with(CONFIG_ENVIRONMENT, document.to_str().unwrap())
            .with(LISTEN_ENVIRONMENT, "");
        assert_eq!(
            ServerConfig::resolve(Vec::new(), &environment, &root.0).unwrap_err(),
            ServerConfigError::InvalidConfiguredValue
        );
    }

    #[test]
    fn unknown_duplicate_wrong_type_and_oversize_toml_reject() {
        let root = TestRoot::new();
        for document in [
            b"[server]\nunknown = true\n".as_slice(),
            b"[server]\ndatabase = \"a\"\ndatabase = \"b\"\n".as_slice(),
            b"[server]\ngrpc_listen = 7443\n".as_slice(),
            b"[unknown]\nvalue = true\n".as_slice(),
        ] {
            let path = root.write(document);
            let arguments = [OsString::from("--config"), path.into_os_string()];
            assert_eq!(
                ServerConfig::resolve(arguments, &EmptyEnvironment, &root.0).unwrap_err(),
                ServerConfigError::InvalidConfigDocument
            );
        }

        let path = root.write(&vec![b'x'; MAX_CONFIG_DOCUMENT_BYTES as usize + 1]);
        let arguments = [OsString::from("--config"), path.into_os_string()];
        assert_eq!(
            ServerConfig::resolve(arguments, &EmptyEnvironment, &root.0).unwrap_err(),
            ServerConfigError::InvalidConfigDocument
        );
    }

    #[test]
    fn duplicate_or_unknown_cli_options_fail_closed() {
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
    fn paths_are_bounded_nonempty_normalized_and_lexically_disjoint() {
        let mut empty = valid_arguments();
        empty[1] = OsString::new();
        assert_eq!(
            ServerConfig::parse(empty).unwrap_err(),
            ServerConfigError::InvalidConfiguredValue
        );

        let mut long = valid_arguments();
        long[1] = OsString::from("x".repeat(MAX_CONFIGURED_PATH_BYTES + 1));
        assert_eq!(
            ServerConfig::parse(long).unwrap_err(),
            ServerConfigError::InvalidPath
        );

        let mut overlapping = valid_arguments();
        overlapping[13] = OsString::from("database.redb");
        assert_eq!(
            ServerConfig::parse(overlapping).unwrap_err(),
            ServerConfigError::OverlappingPaths
        );

        let mut relative_backup_root = valid_arguments();
        relative_backup_root[9] = OsString::from("backups");
        assert_eq!(
            ServerConfig::parse(relative_backup_root).unwrap_err(),
            ServerConfigError::InvalidPath
        );

        let mut lexical_escape = valid_arguments();
        lexical_escape[1] = OsString::from("data/../database.redb");
        assert_eq!(
            ServerConfig::parse(lexical_escape).unwrap_err(),
            ServerConfigError::InvalidPath
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

    #[test]
    fn optional_mcp_endpoint_derives_the_only_allowed_audience() {
        let root = TestRoot::new();
        let document = root.write(
            br#"
[server]
mcp_listen = "127.0.0.1:7444"
mcp_origins = ["http://127.0.0.1:3000", "http://[::1]:3001"]
"#,
        );
        let arguments = [OsString::from("--config"), document.into_os_string()];
        let config =
            ServerConfig::resolve(arguments, &EmptyEnvironment, &root.0).expect("valid MCP");
        assert_eq!(
            config.mcp_listen_address(),
            Some("127.0.0.1:7444".parse().expect("fixture address"))
        );
        assert_eq!(
            config.mcp_audience().map(Audience::as_str),
            Some("http://127.0.0.1:7444/mcp")
        );
        assert_eq!(
            config.mcp_origins(),
            ["http://127.0.0.1:3000", "http://[::1]:3001"]
        );
    }

    #[test]
    fn mcp_origin_precedence_and_bounds_fail_closed() {
        let root = TestRoot::new();
        let document = root.write(
            br#"
[server]
mcp_listen = "127.0.0.1:7444"
mcp_origins = ["http://127.0.0.1:3000"]
"#,
        );
        let environment = TestEnvironment::default()
            .with(CONFIG_ENVIRONMENT, document.to_str().unwrap())
            .with(MCP_ORIGINS_ENVIRONMENT, "http://127.0.0.1:3001");
        let arguments = [
            "--mcp-origin",
            "http://127.0.0.1:3002",
            "--mcp-origin",
            "http://127.0.0.1:3003",
        ]
        .map(OsString::from);
        let config = ServerConfig::resolve(arguments, &environment, &root.0).expect("CLI origins");
        assert_eq!(
            config.mcp_origins(),
            ["http://127.0.0.1:3002", "http://127.0.0.1:3003"]
        );

        let environment = TestEnvironment::default()
            .with(MCP_LISTEN_ENVIRONMENT, "127.0.0.1:7444")
            .with(MCP_ORIGINS_ENVIRONMENT, "");
        assert_eq!(
            ServerConfig::resolve(Vec::new(), &environment, &root.0).unwrap_err(),
            ServerConfigError::InvalidMcpConfiguration
        );

        let duplicate = [
            "--mcp-listen",
            "127.0.0.1:7444",
            "--mcp-origin",
            "http://127.0.0.1:3000",
            "--mcp-origin",
            "http://127.0.0.1:3000",
        ]
        .map(OsString::from);
        assert_eq!(
            ServerConfig::resolve(duplicate, &EmptyEnvironment, &root.0).unwrap_err(),
            ServerConfigError::InvalidMcpConfiguration
        );
    }
}
