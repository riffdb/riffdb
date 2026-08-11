//! Bounded, nonsecret process configuration for `riffdbd`.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use riffdb_api_mcp::{
    MAX_ALLOWED_ORIGIN_AGGREGATE_BYTES, MAX_ALLOWED_ORIGINS, MAX_ORIGIN_BYTES, MCP_ROUTE,
};
use riffdb_config::{
    ApplicationListenerConfig, CanonicalHttpsEndpoint, DirectTlsListenerConfig, ListenerBounds,
    LocalSocketAccess, LocalSocketListenerConfig, LoopbackCleartextListener, ProtectedFilePath,
    RemoteConfigError, ServerTlsFiles,
};
use riffdb_storage_redb::RedbCommitProfile;
use riffdb_types::{Audience, DatabaseAlias, Environment, MAX_DATABASES_PER_PROCESS};
use serde::Deserialize;

const MAX_CONFIGURED_PATH_BYTES: usize = 4_096;
const MAX_CONFIG_DOCUMENT_BYTES: u64 = 65_536;

const DEFAULT_DATABASE_PATH: &str = "data/riffdb.redb";
const DEFAULT_LISTEN_ADDRESS: &str = "127.0.0.1:7443";
const DEFAULT_ENVIRONMENT: &str = "local";
const DEFAULT_AUDIENCE: &str = "riffdb-grpc-loopback";
const DEFAULT_CAPABILITY_KEY_PATH: &str = "config/capability.keys";
const DEFAULT_IDEMPOTENCY_KEY_PATH: &str = "config/idempotency.keys";
const DEFAULT_REDB_COMMIT_PROFILE: &str = "standard";

const CONFIG_ENVIRONMENT: &str = "RIFFDB_CONFIG";
const DATABASE_ENVIRONMENT: &str = "RIFFDB_DATABASE";
const LISTEN_ENVIRONMENT: &str = "RIFFDB_LISTEN";
const ENVIRONMENT_ENVIRONMENT: &str = "RIFFDB_ENVIRONMENT";
const AUDIENCE_ENVIRONMENT: &str = "RIFFDB_AUDIENCE";
const MCP_LISTEN_ENVIRONMENT: &str = "RIFFDB_MCP_LISTEN";
const MCP_ORIGINS_ENVIRONMENT: &str = "RIFFDB_MCP_ORIGINS";
const BACKUP_ROOT_ENVIRONMENT: &str = "RIFFDB_BACKUP_ROOT";
const PROJECTIONS_ROOT_ENVIRONMENT: &str = "RIFFDB_PROJECTIONS_ROOT";
const CAPABILITY_KEYS_ENVIRONMENT: &str = "RIFFDB_CAPABILITY_KEYS";
const IDEMPOTENCY_KEYS_ENVIRONMENT: &str = "RIFFDB_IDEMPOTENCY_KEYS";
const REDB_COMMIT_PROFILE_ENVIRONMENT: &str = "RIFFDB_REDB_COMMIT_PROFILE";

/// Complete POC process configuration.
///
/// Every field resolves independently as CLI, environment, explicitly selected
/// TOML, then a safe local default. Digest-key contents remain in auth-owned
/// protected-file custody; this configuration carries paths only.
pub(crate) struct ServerConfig {
    databases: Vec<DatabaseConfig>,
    application_listener: ApplicationListenerConfig,
    audience: Audience,
    mcp_listen_address: Option<SocketAddr>,
    mcp_origins: Vec<String>,
    mcp_audience: Option<Audience>,
    capability_key_path: PathBuf,
    idempotency_key_path: PathBuf,
    redb_commit_profile: RedbCommitProfile,
}

/// One independently hosted database's non-secret process configuration.
pub(crate) struct DatabaseConfig {
    alias: DatabaseAlias,
    database_path: PathBuf,
    environment: Environment,
    backup_root: PathBuf,
    projections_root: PathBuf,
    /// Configured columnar projection definitions (field names unresolved until startup).
    projections: Vec<ConfiguredProjection>,
}

/// One configured columnar projection before contract-bundle resolution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConfiguredProjection {
    name: String,
    entity: String,
    projected_fields: Vec<String>,
    org_scope_field: String,
}

impl ConfiguredProjection {
    /// Test-only constructor bypassing document parsing (adapter unit tests).
    #[cfg(test)]
    pub(crate) fn for_test(
        name: &str,
        entity: &str,
        projected_fields: &[&str],
        org_scope_field: &str,
    ) -> Self {
        Self {
            name: name.to_owned(),
            entity: entity.to_owned(),
            projected_fields: projected_fields
                .iter()
                .map(|field| (*field).to_owned())
                .collect(),
            org_scope_field: org_scope_field.to_owned(),
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn entity(&self) -> &str {
        &self.entity
    }

    pub(crate) fn projected_fields(&self) -> &[String] {
        &self.projected_fields
    }

    pub(crate) fn org_scope_field(&self) -> &str {
        &self.org_scope_field
    }
}

impl DatabaseConfig {
    pub(crate) const fn alias(&self) -> &DatabaseAlias {
        &self.alias
    }

    pub(crate) fn database_path(&self) -> &Path {
        &self.database_path
    }

    pub(crate) const fn environment(&self) -> &Environment {
        &self.environment
    }

    pub(crate) fn backup_root(&self) -> &Path {
        &self.backup_root
    }

    pub(crate) fn projections_root(&self) -> &Path {
        &self.projections_root
    }

    pub(crate) fn projections(&self) -> &[ConfiguredProjection] {
        &self.projections
    }
}

impl fmt::Debug for DatabaseConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DatabaseConfig")
            .field("alias", &self.alias)
            .field("database_path", &"[CONFIGURED]")
            .field("environment", &self.environment)
            .field("backup_root", &"[CONFIGURED]")
            .field("projections_root", &"[CONFIGURED]")
            .field("projections", &self.projections.len())
            .finish()
    }
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
        Self::resolve(arguments, &EmptyEnvironment, Path::new("/riffdb-config"))
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
        let ConfigDocument {
            server,
            maintenance,
            databases: named_databases,
            projections: top_level_projections,
        } = document;
        let server = server.unwrap_or_default();
        let maintenance = maintenance.unwrap_or_default();

        let database_environment = environment.value(DATABASE_ENVIRONMENT);
        let configured_environment_environment = environment.value(ENVIRONMENT_ENVIRONMENT);
        let backup_root_environment = environment.value(BACKUP_ROOT_ENVIRONMENT);
        let projections_root_environment = environment.value(PROJECTIONS_ROOT_ENVIRONMENT);
        let uses_legacy_database_configuration = arguments.database.is_some()
            || arguments.environment.is_some()
            || arguments.backup_root.is_some()
            || arguments.projections_root.is_some()
            || database_environment.is_some()
            || configured_environment_environment.is_some()
            || backup_root_environment.is_some()
            || projections_root_environment.is_some()
            || server.database.is_some()
            || server.environment.is_some()
            || maintenance.backup_root.is_some()
            || maintenance.projections_root.is_some()
            || !top_level_projections.is_empty();
        if !named_databases.is_empty() && uses_legacy_database_configuration {
            return Err(ServerConfigError::MixedDatabaseConfiguration);
        }
        let databases = if named_databases.is_empty() {
            vec![DatabaseConfig {
                alias: DatabaseAlias::default_alias(),
                database_path: bounded_path(select_os(
                    arguments.database.as_ref(),
                    database_environment,
                    server.database.map(OsString::from),
                    OsString::from(DEFAULT_DATABASE_PATH),
                )?)?,
                environment: parse_environment(select_os(
                    arguments.environment.as_ref(),
                    configured_environment_environment,
                    server.environment.map(OsString::from),
                    OsString::from(DEFAULT_ENVIRONMENT),
                )?)?,
                backup_root: bounded_absolute_directory(select_os(
                    arguments.backup_root.as_ref(),
                    backup_root_environment,
                    maintenance.backup_root.map(OsString::from),
                    current_directory.join("backups").into_os_string(),
                )?)?,
                projections_root: bounded_absolute_directory(select_os(
                    arguments.projections_root.as_ref(),
                    projections_root_environment,
                    maintenance.projections_root.map(OsString::from),
                    current_directory.join("projections").into_os_string(),
                )?)?,
                projections: parse_projection_documents(top_level_projections)?,
            }]
        } else {
            parse_named_databases(named_databases)?
        };
        let listen_environment = environment.value(LISTEN_ENVIRONMENT);
        let uses_legacy_listener_configuration = arguments.listen.is_some()
            || listen_environment.is_some()
            || server.grpc_listen.is_some();
        if server.application_listener.is_some() && uses_legacy_listener_configuration {
            return Err(ServerConfigError::MixedApplicationListenerConfiguration);
        }
        let application_listener = match server.application_listener {
            Some(document) => parse_application_listener(document)?,
            None => ApplicationListenerConfig::LoopbackCleartext(
                LoopbackCleartextListener::new(parse_loopback_address(&select_os(
                    arguments.listen.as_ref(),
                    listen_environment,
                    server.grpc_listen.map(OsString::from),
                    OsString::from(DEFAULT_LISTEN_ADDRESS),
                )?)?)
                .map_err(ServerConfigError::ApplicationListener)?,
            ),
        };
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
        let redb_commit_profile = parse_redb_commit_profile(select_os(
            arguments.redb_commit_profile.as_ref(),
            environment.value(REDB_COMMIT_PROFILE_ENVIRONMENT),
            server.redb_commit_profile.map(OsString::from),
            OsString::from(DEFAULT_REDB_COMMIT_PROFILE),
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
            databases,
            application_listener,
            audience,
            mcp_listen_address,
            mcp_origins,
            mcp_audience,
            capability_key_path,
            idempotency_key_path,
            redb_commit_profile,
        };
        config.validate_disjoint_paths(current_directory)?;
        Ok(config)
    }

    fn validate_disjoint_paths(&self, current_directory: &Path) -> Result<(), ServerConfigError> {
        let mut paths = Vec::with_capacity(self.databases.len() * 3 + 2);
        for database in &self.databases {
            paths.push((
                ConfiguredPathRole::Database(database.alias.clone()),
                lexical_absolute(&database.database_path, current_directory)?,
            ));
            paths.push((
                ConfiguredPathRole::BackupRoot(database.alias.clone()),
                lexical_absolute(&database.backup_root, current_directory)?,
            ));
            paths.push((
                ConfiguredPathRole::ProjectionsRoot(database.alias.clone()),
                lexical_absolute(&database.projections_root, current_directory)?,
            ));
        }
        paths.push((
            ConfiguredPathRole::CapabilityKeys,
            lexical_absolute(&self.capability_key_path, current_directory)?,
        ));
        paths.push((
            ConfiguredPathRole::IdempotencyKeys,
            lexical_absolute(&self.idempotency_key_path, current_directory)?,
        ));
        for (left_index, (left_role, left)) in paths.iter().enumerate() {
            for (right_role, right) in paths.iter().skip(left_index + 1) {
                if paths_overlap(left, right) {
                    return Err(ServerConfigError::OverlappingPaths {
                        left: left_role.clone(),
                        right: right_role.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    pub(crate) fn database_path(&self) -> &Path {
        self.databases[0].database_path()
    }

    pub(crate) fn databases(&self) -> &[DatabaseConfig] {
        &self.databases
    }

    pub(crate) const fn application_listener(&self) -> &ApplicationListenerConfig {
        &self.application_listener
    }

    #[cfg(test)]
    pub(crate) fn loopback_listen_address(&self) -> Option<SocketAddr> {
        match self.application_listener() {
            ApplicationListenerConfig::LoopbackCleartext(listener) => {
                Some(listener.listen_address())
            }
            ApplicationListenerConfig::DirectTls(_) | ApplicationListenerConfig::LocalSocket(_) => {
                None
            }
        }
    }

    pub(crate) fn environment(&self) -> &Environment {
        self.databases[0].environment()
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
        self.databases[0].backup_root()
    }

    pub(crate) fn projections_root(&self) -> &Path {
        self.databases[0].projections_root()
    }

    pub(crate) fn projections(&self) -> &[ConfiguredProjection] {
        self.databases[0].projections()
    }

    pub(crate) fn capability_key_path(&self) -> &Path {
        &self.capability_key_path
    }

    pub(crate) fn idempotency_key_path(&self) -> &Path {
        &self.idempotency_key_path
    }

    pub(crate) const fn redb_commit_profile(&self) -> RedbCommitProfile {
        self.redb_commit_profile
    }
}

impl fmt::Debug for ServerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServerConfig")
            .field("databases", &self.databases)
            .field("application_listener", &self.application_listener)
            .field("audience", &"[CONFIGURED]")
            .field("mcp_listen_address", &self.mcp_listen_address)
            .field("mcp_origins", &"[CONFIGURED]")
            .field("mcp_audience", &"[DERIVED]")
            .field("capability_key_path", &"[CONFIGURED]")
            .field("idempotency_key_path", &"[CONFIGURED]")
            .field("redb_commit_profile", &self.redb_commit_profile)
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
    projections_root: Option<OsString>,
    capability_keys: Option<OsString>,
    idempotency_keys: Option<OsString>,
    redb_commit_profile: Option<OsString>,
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
                Some("--projections-root") => set_once(&mut values.projections_root, value)?,
                Some("--capability-keys") => set_once(&mut values.capability_keys, value)?,
                Some("--idempotency-keys") => set_once(&mut values.idempotency_keys, value)?,
                Some("--redb-commit-profile") => set_once(&mut values.redb_commit_profile, value)?,
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
    #[serde(default)]
    databases: BTreeMap<String, DatabaseDocument>,
    /// Top-level projection list for the legacy single-database configuration form.
    #[serde(default)]
    projections: Vec<ProjectionDocument>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerDocument {
    database: Option<String>,
    grpc_listen: Option<String>,
    application_listener: Option<ApplicationListenerDocument>,
    environment: Option<String>,
    audience: Option<String>,
    mcp_listen: Option<String>,
    mcp_origins: Option<Vec<String>>,
    capability_keys: Option<String>,
    idempotency_keys: Option<String>,
    redb_commit_profile: Option<String>,
}

/// The versioned file-only application ingress selection.
///
/// Legacy `grpc_listen`, `--listen`, and `RIFFDB_LISTEN` remain the exact
/// loopback-development form. Remote and local-socket profiles are deliberately
/// complete documents so an individual environment override cannot detach a
/// bind address from its peer identity or protected material.
#[derive(Debug, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum ApplicationListenerDocument {
    LoopbackCleartext {
        listen: String,
    },
    DirectTls {
        listen: String,
        public_endpoint: String,
        certificate_chain: String,
        private_key: String,
        bounds: Option<ListenerBoundsDocument>,
    },
    LocalSocket {
        path: String,
        access: LocalSocketAccessDocument,
        bounds: Option<ListenerBoundsDocument>,
    },
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LocalSocketAccessDocument {
    OwnerOnly,
    OwnerAndGroup,
}

impl From<LocalSocketAccessDocument> for LocalSocketAccess {
    fn from(value: LocalSocketAccessDocument) -> Self {
        match value {
            LocalSocketAccessDocument::OwnerOnly => Self::OwnerOnly,
            LocalSocketAccessDocument::OwnerAndGroup => Self::OwnerAndGroup,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ListenerBoundsDocument {
    max_connections: Option<u32>,
    max_streams_per_connection: Option<u32>,
    handshake_timeout_seconds: Option<u64>,
    idle_timeout_seconds: Option<u64>,
    keepalive_interval_seconds: Option<u64>,
    drain_timeout_seconds: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct MaintenanceDocument {
    backup_root: Option<String>,
    projections_root: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DatabaseDocument {
    path: String,
    backup_root: String,
    environment: String,
    /// Absolute directory for columnar projection storage; defaults are not
    /// applied for named databases (must be explicit).
    projections_root: Option<String>,
    #[serde(default)]
    projections: Vec<ProjectionDocument>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectionDocument {
    name: String,
    entity: String,
    projected_fields: Vec<String>,
    org_scope_field: String,
}

fn parse_projection_documents(
    documents: Vec<ProjectionDocument>,
) -> Result<Vec<ConfiguredProjection>, ServerConfigError> {
    let mut seen = BTreeMap::new();
    let mut out = Vec::with_capacity(documents.len());
    for document in documents {
        if document.name.is_empty()
            || document.name.len() > 256
            || document.entity.is_empty()
            || document.projected_fields.is_empty()
            || document.org_scope_field.is_empty()
            || document
                .projected_fields
                .iter()
                .any(|field| field.is_empty() || field.len() > 256)
        {
            return Err(ServerConfigError::InvalidProjectionConfiguration);
        }
        if seen.insert(document.name.clone(), ()).is_some() {
            return Err(ServerConfigError::InvalidProjectionConfiguration);
        }
        out.push(ConfiguredProjection {
            name: document.name,
            entity: document.entity,
            projected_fields: document.projected_fields,
            org_scope_field: document.org_scope_field,
        });
    }
    Ok(out)
}

fn parse_named_databases(
    documents: BTreeMap<String, DatabaseDocument>,
) -> Result<Vec<DatabaseConfig>, ServerConfigError> {
    if documents.is_empty() || documents.len() > MAX_DATABASES_PER_PROCESS {
        return Err(ServerConfigError::InvalidDatabaseConfiguration);
    }
    documents
        .into_iter()
        .map(|(alias, document)| {
            let alias = DatabaseAlias::new(alias)
                .map_err(|_| ServerConfigError::InvalidDatabaseConfiguration)?;
            let backup_root = bounded_absolute_directory(OsString::from(document.backup_root))?;
            let projections_root = match document.projections_root {
                Some(path) => bounded_absolute_directory(OsString::from(path))?,
                None => {
                    // Distinct absolute default so validate_disjoint_paths stays
                    // meaningful when no projections are configured.
                    let mut path = backup_root
                        .parent()
                        .ok_or(ServerConfigError::InvalidPath)?
                        .to_path_buf();
                    path.push(format!("{alias}-projections"));
                    if !path.is_absolute() {
                        return Err(ServerConfigError::InvalidPath);
                    }
                    path
                }
            };
            Ok(DatabaseConfig {
                alias,
                database_path: bounded_path(OsString::from(document.path))?,
                environment: parse_environment(OsString::from(document.environment))?,
                backup_root,
                projections_root,
                projections: parse_projection_documents(document.projections)?,
            })
        })
        .collect()
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

fn parse_application_listener(
    document: ApplicationListenerDocument,
) -> Result<ApplicationListenerConfig, ServerConfigError> {
    let listener = match document {
        ApplicationListenerDocument::LoopbackCleartext { listen } => {
            ApplicationListenerConfig::LoopbackCleartext(
                LoopbackCleartextListener::new(parse_loopback_address(OsStr::new(&listen))?)
                    .map_err(ServerConfigError::ApplicationListener)?,
            )
        }
        ApplicationListenerDocument::DirectTls {
            listen,
            public_endpoint,
            certificate_chain,
            private_key,
            bounds,
        } => {
            let listen = listen
                .parse::<SocketAddr>()
                .map_err(|_| ServerConfigError::InvalidListenAddress)?;
            let endpoint = CanonicalHttpsEndpoint::parse(&public_endpoint)
                .map_err(ServerConfigError::ApplicationListener)?;
            let files = ServerTlsFiles::new(
                ProtectedFilePath::new(certificate_chain)
                    .map_err(ServerConfigError::ApplicationListener)?,
                ProtectedFilePath::new(private_key)
                    .map_err(ServerConfigError::ApplicationListener)?,
            )
            .map_err(ServerConfigError::ApplicationListener)?;
            ApplicationListenerConfig::DirectTls(
                DirectTlsListenerConfig::new(
                    listen,
                    endpoint,
                    files,
                    parse_listener_bounds(bounds)?,
                )
                .map_err(ServerConfigError::ApplicationListener)?,
            )
        }
        ApplicationListenerDocument::LocalSocket {
            path,
            access,
            bounds,
        } => ApplicationListenerConfig::LocalSocket(
            LocalSocketListenerConfig::new(path, access.into(), parse_listener_bounds(bounds)?)
                .map_err(ServerConfigError::ApplicationListener)?,
        ),
    };
    Ok(listener)
}

fn parse_listener_bounds(
    document: Option<ListenerBoundsDocument>,
) -> Result<ListenerBounds, ServerConfigError> {
    let Some(document) = document else {
        return Ok(ListenerBounds::alpha_default());
    };
    let defaults = ListenerBounds::alpha_default();
    ListenerBounds::new(
        parse_nonzero_bound(document.max_connections, defaults.max_connections())?,
        parse_nonzero_bound(
            document.max_streams_per_connection,
            defaults.max_streams_per_connection(),
        )?,
        Duration::from_secs(
            document
                .handshake_timeout_seconds
                .unwrap_or(defaults.handshake_timeout().as_secs()),
        ),
        Duration::from_secs(
            document
                .idle_timeout_seconds
                .unwrap_or(defaults.idle_timeout().as_secs()),
        ),
        Duration::from_secs(
            document
                .keepalive_interval_seconds
                .unwrap_or(defaults.keepalive_interval().as_secs()),
        ),
        Duration::from_secs(
            document
                .drain_timeout_seconds
                .unwrap_or(defaults.drain_timeout().as_secs()),
        ),
    )
    .map_err(ServerConfigError::ApplicationListener)
}

fn parse_nonzero_bound(
    configured: Option<u32>,
    default: NonZeroU32,
) -> Result<NonZeroU32, ServerConfigError> {
    configured.map_or(Ok(default), |value| {
        NonZeroU32::new(value).ok_or(ServerConfigError::ApplicationListener(
            RemoteConfigError::InvalidBound,
        ))
    })
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

fn parse_redb_commit_profile(value: OsString) -> Result<RedbCommitProfile, ServerConfigError> {
    match value.to_str() {
        Some("standard") => Ok(RedbCommitProfile::Standard),
        Some("hardened") => Ok(RedbCommitProfile::Hardened),
        _ => Err(ServerConfigError::InvalidRedbCommitProfile),
    }
}

/// Closed process-configuration failures that never echo a supplied value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ConfiguredPathRole {
    Database(DatabaseAlias),
    BackupRoot(DatabaseAlias),
    ProjectionsRoot(DatabaseAlias),
    CapabilityKeys,
    IdempotencyKeys,
}

impl fmt::Display for ConfiguredPathRole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(alias) => write!(formatter, "database path for '{alias}'"),
            Self::BackupRoot(alias) => write!(formatter, "backup_root for '{alias}'"),
            Self::ProjectionsRoot(alias) => write!(formatter, "projections_root for '{alias}'"),
            Self::CapabilityKeys => formatter.write_str("capability key path"),
            Self::IdempotencyKeys => formatter.write_str("idempotency key path"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ServerConfigError {
    UnknownOption,
    MissingValue,
    DuplicateOption,
    InvalidConfiguredValue,
    InvalidConfigDocument,
    MixedDatabaseConfiguration,
    InvalidDatabaseConfiguration,
    InvalidProjectionConfiguration,
    InvalidPath,
    OverlappingPaths {
        left: ConfiguredPathRole,
        right: ConfiguredPathRole,
    },
    InvalidListenAddress,
    NonLoopbackListenAddress,
    MixedApplicationListenerConfiguration,
    ApplicationListener(RemoteConfigError),
    InvalidEnvironment,
    InvalidAudience,
    InvalidMcpConfiguration,
    InvalidRedbCommitProfile,
}

impl fmt::Display for ServerConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::UnknownOption => "unknown riffdbd option",
            Self::MissingValue => "riffdbd option is missing its value",
            Self::DuplicateOption => "riffdbd option was supplied more than once",
            Self::InvalidConfiguredValue => "configured riffdbd value is invalid",
            Self::InvalidConfigDocument => "riffdbd configuration document is invalid",
            Self::MixedDatabaseConfiguration => {
                "legacy and named database configuration cannot be combined"
            }
            Self::InvalidDatabaseConfiguration => "configured database registry is invalid",
            Self::InvalidProjectionConfiguration => {
                "configured columnar projection registry is invalid"
            }
            Self::InvalidPath => "configured path is invalid",
            Self::OverlappingPaths { left, right } => {
                return write!(
                    formatter,
                    "{left} overlaps {right}; use sibling database and backup paths"
                );
            }
            Self::InvalidListenAddress => "configured listen address is invalid",
            Self::NonLoopbackListenAddress => "POC listen address must be loopback",
            Self::MixedApplicationListenerConfiguration => {
                "legacy loopback and application listener configuration cannot be combined"
            }
            Self::ApplicationListener(source) => return source.fmt(formatter),
            Self::InvalidEnvironment => "configured environment is invalid",
            Self::InvalidAudience => "configured audience is invalid",
            Self::InvalidMcpConfiguration => "configured MCP endpoint is invalid",
            Self::InvalidRedbCommitProfile => "configured redb commit profile is invalid",
        })
    }
}

impl Error for ServerConfigError {}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;

    use super::*;

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

    /// Whole-directory scope: `.0` is the root path inside a
    /// [`tempfile::TempDir`] removed on drop — pass, fail, or panic.
    struct TestRoot(PathBuf, tempfile::TempDir);

    impl TestRoot {
        fn new() -> Self {
            let scope = tempfile::TempDir::with_prefix("riffdb-server-config-")
                .expect("create config test root");
            Self(scope.path().to_path_buf(), scope)
        }

        fn write(&self, contents: &[u8]) -> PathBuf {
            let path = self.0.join("riffdb.toml");
            fs::write(&path, contents).expect("write config");
            path
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
        assert_eq!(
            config.loopback_listen_address(),
            Some("127.0.0.1:0".parse().unwrap())
        );
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
            config.loopback_listen_address(),
            Some(DEFAULT_LISTEN_ADDRESS.parse().unwrap())
        );
        assert_eq!(config.environment().as_str(), DEFAULT_ENVIRONMENT);
        assert_eq!(config.audience().as_str(), DEFAULT_AUDIENCE);
        assert_eq!(config.backup_root(), Path::new("/riffdb-config/backups"));
        assert_eq!(
            config.projections_root(),
            Path::new("/riffdb-config/projections")
        );
        assert!(config.projections().is_empty());
        assert_eq!(
            config.capability_key_path(),
            Path::new(DEFAULT_CAPABILITY_KEY_PATH)
        );
        assert_eq!(
            config.idempotency_key_path(),
            Path::new(DEFAULT_IDEMPOTENCY_KEY_PATH)
        );
        assert_eq!(config.redb_commit_profile(), RedbCommitProfile::Standard);
    }

    #[test]
    fn file_listener_profiles_are_closed_complete_and_pre_bind_checked() {
        let root = TestRoot::new();
        let direct = root.write(
            br#"
[server]
audience = "riffdb-grpc-tls"

[server.application_listener]
mode = "direct_tls"
listen = "0.0.0.0:7443"
public_endpoint = "https://riffdb.example.test:7443"
certificate_chain = "/var/lib/riffdb/tls/server.pem"
private_key = "/var/lib/riffdb/tls/server.key"

[server.application_listener.bounds]
max_connections = 512
max_streams_per_connection = 64
handshake_timeout_seconds = 5
idle_timeout_seconds = 120
keepalive_interval_seconds = 20
drain_timeout_seconds = 15
"#,
        );
        let config = ServerConfig::resolve(
            [OsString::from("--config"), direct.into_os_string()],
            &EmptyEnvironment,
            &root.0,
        )
        .expect("complete direct TLS listener");
        let ApplicationListenerConfig::DirectTls(listener) = config.application_listener() else {
            panic!("direct TLS profile expected");
        };
        assert_eq!(listener.listen_address(), "0.0.0.0:7443".parse().unwrap());
        assert_eq!(
            listener.public_endpoint().as_str(),
            "https://riffdb.example.test:7443"
        );
        assert_eq!(listener.bounds().max_connections().get(), 512);

        let local = root.write(
            br#"
[server.application_listener]
mode = "local_socket"
path = "/run/riffdb/application.sock"
access = "owner_only"
"#,
        );
        let config = ServerConfig::resolve(
            [OsString::from("--config"), local.into_os_string()],
            &EmptyEnvironment,
            &root.0,
        )
        .expect("protected local listener");
        let ApplicationListenerConfig::LocalSocket(listener) = config.application_listener() else {
            panic!("local-socket profile expected");
        };
        assert_eq!(listener.access().mode(), 0o600);
        assert_eq!(listener.path(), Path::new("/run/riffdb/application.sock"));

        let insecure = root.write(
            br#"
[server.application_listener]
mode = "loopback_cleartext"
listen = "0.0.0.0:7443"
"#,
        );
        assert_eq!(
            ServerConfig::resolve(
                [OsString::from("--config"), insecure.into_os_string()],
                &EmptyEnvironment,
                &root.0,
            )
            .unwrap_err(),
            ServerConfigError::NonLoopbackListenAddress
        );
    }

    #[test]
    fn legacy_listener_and_closed_profile_cannot_be_mixed() {
        let root = TestRoot::new();
        let document = root.write(
            br#"
[server]
grpc_listen = "127.0.0.1:7443"

[server.application_listener]
mode = "local_socket"
path = "/run/riffdb/application.sock"
access = "owner_only"
"#,
        );
        assert_eq!(
            ServerConfig::resolve(
                [OsString::from("--config"), document.into_os_string()],
                &EmptyEnvironment,
                &root.0,
            )
            .unwrap_err(),
            ServerConfigError::MixedApplicationListenerConfiguration
        );
    }

    #[test]
    fn redb_commit_profile_is_closed_and_uses_normal_precedence() {
        let root = TestRoot::new();
        let document = root.write(
            br#"
[server]
redb_commit_profile = "hardened"
"#,
        );
        let environment = TestEnvironment::default()
            .with(CONFIG_ENVIRONMENT, document.to_str().unwrap())
            .with(REDB_COMMIT_PROFILE_ENVIRONMENT, "standard");
        let config = ServerConfig::resolve(
            ["--redb-commit-profile", "hardened"].map(OsString::from),
            &environment,
            &root.0,
        )
        .expect("closed profile");
        assert_eq!(config.redb_commit_profile(), RedbCommitProfile::Hardened);

        assert_eq!(
            ServerConfig::parse(["--redb-commit-profile", "eventual"].map(OsString::from))
                .unwrap_err(),
            ServerConfigError::InvalidRedbCommitProfile
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
        assert_eq!(
            config.loopback_listen_address(),
            Some("127.0.0.1:7002".parse().unwrap())
        );
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
    fn named_databases_are_canonical_sorted_bounded_and_disjoint() {
        let root = TestRoot::new();
        let document = root.write(
            br#"
[server]
grpc_listen = "127.0.0.1:7001"

[databases.zeta]
path = "data/zeta.redb"
backup_root = "/tmp/riffdb-zeta-backups"
environment = "production"

[databases.alpha]
path = "data/alpha.redb"
backup_root = "/tmp/riffdb-alpha-backups"
environment = "development"
"#,
        );
        let config = ServerConfig::resolve(
            [OsString::from("--config"), document.into_os_string()],
            &EmptyEnvironment,
            &root.0,
        )
        .expect("named database registry");
        assert_eq!(
            config
                .databases()
                .iter()
                .map(|database| database.alias().as_str())
                .collect::<Vec<_>>(),
            ["alpha", "zeta"]
        );
        assert_eq!(
            config.databases()[0].database_path(),
            Path::new("data/alpha.redb")
        );
        assert_eq!(config.databases()[1].environment().as_str(), "production");

        let mixed = root.write(
            br#"
[server]
database = "legacy.redb"

[databases.alpha]
path = "data/alpha.redb"
backup_root = "/tmp/riffdb-alpha-backups"
environment = "development"
"#,
        );
        assert_eq!(
            ServerConfig::resolve(
                [OsString::from("--config"), mixed.into_os_string()],
                &EmptyEnvironment,
                &root.0,
            )
            .unwrap_err(),
            ServerConfigError::MixedDatabaseConfiguration
        );

        let overlap = root.write(
            br#"
[databases.alpha]
path = "data/shared.redb"
backup_root = "/tmp/riffdb-alpha-backups"
environment = "development"

[databases.beta]
path = "data/shared.redb"
backup_root = "/tmp/riffdb-beta-backups"
environment = "development"
"#,
        );
        let error = ServerConfig::resolve(
            [OsString::from("--config"), overlap.into_os_string()],
            &EmptyEnvironment,
            &root.0,
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "database path for 'alpha' overlaps database path for 'beta'; use sibling database and backup paths"
        );
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
        assert!(matches!(
            ServerConfig::parse(overlapping).unwrap_err(),
            ServerConfigError::OverlappingPaths { .. }
        ));

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
