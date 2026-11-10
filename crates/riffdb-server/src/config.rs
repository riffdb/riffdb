//! Bounded, nonsecret process configuration for `riffdbd`.

#![allow(dead_code)]

#[path = "config_logging.rs"]
pub(crate) mod logging;
use logging::{LoggingConfig, LoggingDocument};
#[path = "config_telemetry.rs"]
pub(crate) mod telemetry;
use telemetry::{TelemetryConfig, TelemetryDocument};

#[path = "config_archive.rs"]
mod archive;
pub(crate) use archive::ConfiguredArchive;
use archive::{ArchiveDocument, parse_archives};
#[path = "config_outbox.rs"]
mod outbox;
pub(crate) use outbox::{ConfiguredOutbox, ConfiguredWebhookOutbox};
use outbox::{OutboxDocument, parse_outbox};
#[path = "config_retention.rs"]
mod retention;
pub(crate) use retention::{RetentionConfig, RetentionTarget};
use retention::{RetentionDocument, parse_retention};
#[path = "config_limits.rs"]
mod limits;
pub(crate) use limits::{DatabaseLimitsConfig, ServerDatabaseResourceGovernor};
use limits::{LimitsDocument, parse_limits};
#[path = "config_follower.rs"]
mod follower;
pub(crate) use follower::{
    CommitAcknowledgementMode, FollowerSourceConfig, ReplicationQuorumConfig, ServerMode,
};
use follower::{FollowerSourceDocument, ReplicationQuorumDocument};
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
    ApplicationListenerConfig, AutoIssueTlsConfig, CanonicalHttpsEndpoint, DirectTlsListenerConfig,
    ListenerBounds, LocalSocketAccess, LocalSocketListenerConfig, LoopbackCleartextListener,
    ProtectedFilePath, RemoteConfigError, ServerTlsFiles,
};
use riffdb_storage_redb::RedbCommitProfile;
use riffdb_types::{Audience, DatabaseAlias, Environment, MAX_DATABASES_PER_PROCESS};
use serde::Deserialize;

const MAX_CONFIGURED_PATH_BYTES: usize = 4_096;
const MAX_CONFIG_DOCUMENT_BYTES: u64 = 524_288;
/// Default encoded-byte quota (256 MiB) for the automatic durable-local outbox
/// spool provisioned when online retention (`RetainSequences`) is configured
/// without an explicit outbox destination (`ADR-0338`).
pub(crate) const DEFAULT_AUTO_OUTBOX_ARCHIVE_QUOTA_BYTES: u64 = 256 * 1024 * 1024;

const DEFAULT_DATABASE_PATH: &str = "data/riffdb.redb";
const DEFAULT_LISTEN_ADDRESS: &str = "127.0.0.1:7443";
const DEFAULT_ENVIRONMENT: &str = "local";
const DEFAULT_AUDIENCE: &str = "riffdb-grpc-loopback";
const DEFAULT_CAPABILITY_KEY_PATH: &str = "config/capability.keys";
const DEFAULT_IDEMPOTENCY_KEY_PATH: &str = "config/idempotency.keys";
const DEFAULT_REDB_COMMIT_PROFILE: &str = "standard";
const DEFAULT_TELEMETRY_FLIGHT_INTERVAL_MS: u64 = 10_000;
const MIN_TELEMETRY_FLIGHT_INTERVAL_MS: u64 = 100;
const MAX_TELEMETRY_FLIGHT_INTERVAL_MS: u64 = 3_600_000;
const DEFAULT_TELEMETRY_PUSH_INTERVAL_MS: u64 = 10_000;
const MIN_TELEMETRY_PUSH_INTERVAL_MS: u64 = 100;
const MAX_TELEMETRY_PUSH_INTERVAL_MS: u64 = 3_600_000;
const MAX_TELEMETRY_OTLP_ENDPOINT_BYTES: usize = 1_024;

const CONFIG_ENVIRONMENT: &str = "RIFFDB_CONFIG";
const DATABASE_ENVIRONMENT: &str = "RIFFDB_DATABASE";
const LISTEN_ENVIRONMENT: &str = "RIFFDB_LISTEN";
const ENVIRONMENT_ENVIRONMENT: &str = "RIFFDB_ENVIRONMENT";
const AUDIENCE_ENVIRONMENT: &str = "RIFFDB_AUDIENCE";
const MCP_LISTEN_ENVIRONMENT: &str = "RIFFDB_MCP_LISTEN";
const MCP_ORIGINS_ENVIRONMENT: &str = "RIFFDB_MCP_ORIGINS";
const HTTP_LISTEN_ENVIRONMENT: &str = "RIFFDB_HTTP_LISTEN";
const METRICS_LISTEN_ENVIRONMENT: &str = "RIFFDB_METRICS_LISTEN";
const TELEMETRY_FLIGHT_RECORDER_ENVIRONMENT: &str = "RIFFDB_TELEMETRY_FLIGHT_RECORDER";
const TELEMETRY_FLIGHT_INTERVAL_MS_ENVIRONMENT: &str = "RIFFDB_TELEMETRY_FLIGHT_INTERVAL_MS";
const TELEMETRY_OTLP_ENDPOINT_ENVIRONMENT: &str = "RIFFDB_TELEMETRY_OTLP_ENDPOINT";
const TELEMETRY_PUSH_INTERVAL_MS_ENVIRONMENT: &str = "RIFFDB_TELEMETRY_PUSH_INTERVAL_MS";
const TELEMETRY_BEARER_TOKEN_FILE_ENVIRONMENT: &str = "RIFFDB_TELEMETRY_BEARER_TOKEN_FILE";
const TELEMETRY_CA_CERTIFICATE_PATH_ENVIRONMENT: &str = "RIFFDB_TELEMETRY_CA_CERTIFICATE_PATH";
const BACKUP_ROOT_ENVIRONMENT: &str = "RIFFDB_BACKUP_ROOT";
const PROJECTIONS_ROOT_ENVIRONMENT: &str = "RIFFDB_PROJECTIONS_ROOT";
const CAPABILITY_KEYS_ENVIRONMENT: &str = "RIFFDB_CAPABILITY_KEYS";
const IDEMPOTENCY_KEYS_ENVIRONMENT: &str = "RIFFDB_IDEMPOTENCY_KEYS";
const REDB_COMMIT_PROFILE_ENVIRONMENT: &str = "RIFFDB_REDB_COMMIT_PROFILE";

#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum McpListenerConfig {
    LoopbackCleartext(SocketAddr),
    DirectTls(DirectTlsListenerConfig),
}

impl McpListenerConfig {
    pub(crate) const fn listen_address(&self) -> SocketAddr {
        match self {
            Self::LoopbackCleartext(address) => *address,
            Self::DirectTls(listener) => listener.listen_address(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum HttpListenerConfig {
    LoopbackCleartext(SocketAddr),
    DirectTls(DirectTlsListenerConfig),
}

impl HttpListenerConfig {
    pub(crate) const fn listen_address(&self) -> SocketAddr {
        match self {
            Self::LoopbackCleartext(address) => *address,
            Self::DirectTls(listener) => listener.listen_address(),
        }
    }
}

/// Validated OTLP/HTTP JSON collector endpoint (`ADR-0332`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TelemetryPushEndpoint {
    /// Loopback HTTP collector agent endpoint (`http://127.0.0.1:<port>/...` or `http://[::1]:<port>/...`).
    LoopbackHttp {
        address: SocketAddr,
        authority: String,
        path: String,
    },
    /// Verified HTTPS collector endpoint (`https://<host>[:<port>]/...`).
    Https {
        host: String,
        port: u16,
        authority: String,
        path: String,
        ca_certificate_path: Option<ProtectedFilePath>,
    },
}

/// Periodic and graceful-drain OTLP/HTTP JSON metrics push configuration (`ADR-0332`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TelemetryPushConfig {
    endpoint: TelemetryPushEndpoint,
    interval: Duration,
    bearer_token_path: Option<ProtectedFilePath>,
}

impl TelemetryPushConfig {
    pub(crate) fn new(
        endpoint: TelemetryPushEndpoint,
        interval: Duration,
        bearer_token_path: Option<ProtectedFilePath>,
    ) -> Self {
        Self {
            endpoint,
            interval,
            bearer_token_path,
        }
    }

    pub(crate) const fn endpoint(&self) -> &TelemetryPushEndpoint {
        &self.endpoint
    }

    pub(crate) const fn interval(&self) -> Duration {
        self.interval
    }

    pub(crate) const fn bearer_token_path(&self) -> Option<&ProtectedFilePath> {
        self.bearer_token_path.as_ref()
    }
}

/// Complete POC process configuration.
///
/// Every field resolves independently as CLI, environment, explicitly selected
/// TOML, then a safe local default. Digest-key contents remain in auth-owned
/// protected-file custody; this configuration carries paths only.
#[derive(Clone)]
pub(crate) struct ServerConfig {
    config_file_path: Option<PathBuf>,
    uses_named_databases: bool,
    mode: ServerMode,
    databases: Vec<DatabaseConfig>,
    application_listener: ApplicationListenerConfig,
    audience: Audience,
    mcp_listener: Option<McpListenerConfig>,
    mcp_origins: Vec<String>,
    mcp_audience: Option<Audience>,
    http_listener: Option<HttpListenerConfig>,
    metrics_listen_address: Option<SocketAddr>,
    telemetry_flight_recorder_path: Option<PathBuf>,
    telemetry_flight_interval: Duration,
    telemetry_push: Option<TelemetryPushConfig>,
    logging: LoggingConfig,
    telemetry_signals: TelemetryConfig,
    capability_key_path: PathBuf,
    idempotency_key_path: PathBuf,
    redb_commit_profile: RedbCommitProfile,
}

/// One independently hosted database's non-secret process configuration.
#[derive(Clone)]
pub(crate) struct DatabaseConfig {
    outbox: Option<ConfiguredOutbox>,
    archives: Vec<ConfiguredArchive>,
    retention: RetentionConfig,
    limits: DatabaseLimitsConfig,
    follower: Option<FollowerSourceConfig>,
    replication: ReplicationQuorumConfig,
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
    pub(crate) fn outbox(&self) -> Option<&ConfiguredOutbox> {
        self.outbox.as_ref()
    }

    /// Reserved sibling area for automatic durable-local outbox spooling when
    /// online retention is configured without an explicit outbox destination (`ADR-0338`).
    pub(crate) fn default_outbox_root(&self) -> PathBuf {
        let mut path = self.database_path.as_os_str().to_os_string();
        path.push(".riffoutbox");
        PathBuf::from(path)
    }

    /// Effective outbox destination for this database (`ADR-0338`).
    ///
    /// Returns the explicit `outbox` when configured; otherwise, on a primary
    /// database (`follower.is_none()`) with online sequence retention
    /// (`RetentionTarget::RetainSequences(_)`), returns an automatic
    /// `ConfiguredOutbox::DurableLocal` at `<database_path>.riffoutbox` with
    /// [`DEFAULT_AUTO_OUTBOX_ARCHIVE_QUOTA_BYTES`] (256 MiB).
    pub(crate) fn effective_outbox(&self) -> Option<ConfiguredOutbox> {
        if let Some(outbox) = &self.outbox {
            return Some(outbox.clone());
        }
        if self.follower.is_none()
            && matches!(self.retention.target(), RetentionTarget::RetainSequences(_))
        {
            return Some(ConfiguredOutbox::DurableLocal {
                directory: self.default_outbox_root(),
                quota: std::num::NonZeroU64::new(DEFAULT_AUTO_OUTBOX_ARCHIVE_QUOTA_BYTES)
                    .unwrap_or(std::num::NonZeroU64::MIN),
            });
        }
        None
    }

    pub(crate) fn archives(&self) -> &[ConfiguredArchive] {
        &self.archives
    }

    /// The ADR-0182 online retention lane's target and budget.
    pub(crate) const fn retention(&self) -> RetentionConfig {
        self.retention
    }

    pub(crate) const fn limits(&self) -> DatabaseLimitsConfig {
        self.limits
    }

    pub(crate) fn follower(&self) -> Option<&FollowerSourceConfig> {
        self.follower.as_ref()
    }
    pub(crate) fn replication(&self) -> &ReplicationQuorumConfig {
        &self.replication
    }
    pub(crate) const fn alias(&self) -> &DatabaseAlias {
        &self.alias
    }

    pub(crate) fn database_path(&self) -> &Path {
        &self.database_path
    }

    /// Reserved sibling area for bounded private source artifacts.
    pub(crate) fn replication_root(&self) -> PathBuf {
        let mut path = self.database_path.as_os_str().to_os_string();
        path.push(".riffreplication");
        PathBuf::from(path)
    }

    /// Reserved receiver scratch, separate from the source inventory on promotion.
    pub(crate) fn replication_receiver_root(&self) -> PathBuf {
        let mut path = self.database_path.as_os_str().to_os_string();
        path.push(".riffreceiver");
        PathBuf::from(path)
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
    pub(crate) const fn mode(&self) -> ServerMode {
        self.mode
    }
    pub(crate) fn from_process_args() -> Result<Self, ServerConfigError> {
        let current_directory =
            std::env::current_dir().map_err(|_| ServerConfigError::InvalidPath)?;
        Self::resolve(
            std::env::args_os().skip(1),
            &ProcessEnvironment,
            &current_directory,
        )
    }

    pub(crate) fn parse(
        arguments: impl IntoIterator<Item = OsString>,
    ) -> Result<Self, ServerConfigError> {
        Self::resolve(arguments, &EmptyEnvironment, Path::new("/riffdb-config"))
    }

    pub(crate) fn reload_from_config_file(path: &Path) -> Result<Self, ServerConfigError> {
        let current_directory =
            std::env::current_dir().map_err(|_| ServerConfigError::InvalidPath)?;
        Self::resolve(
            [OsString::from("--config"), path.as_os_str().to_os_string()],
            &ProcessEnvironment,
            &current_directory,
        )
    }

    #[cfg(test)]
    pub(crate) fn parse_config_file(
        path: &Path,
        current_directory: &Path,
    ) -> Result<Self, ServerConfigError> {
        Self::resolve(
            [OsString::from("--config"), path.as_os_str().to_os_string()],
            &EmptyEnvironment,
            current_directory,
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
        let ConfigDocument {
            server,
            maintenance,
            replication: top_level_replication,
            databases: named_databases,
            projections: top_level_projections,
        } = document;
        let uses_named_databases = !named_databases.is_empty();
        let server = server.unwrap_or_default();
        let mode = ServerMode::parse(select_os(
            arguments.mode.as_ref(),
            environment.value("RIFFDB_MODE"),
            server.mode.clone().map(OsString::from),
            OsString::from("primary"),
        )?)?;
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
            || server.replication_source.is_some()
            || server.replication.is_some()
            || top_level_replication.is_some()
            || server.database.is_some()
            || server.environment.is_some()
            || maintenance.backup_root.is_some()
            || maintenance.projections_root.is_some()
            || !maintenance.archives.is_empty()
            || maintenance.retention.is_some()
            || maintenance.limits.is_some()
            || maintenance.outbox.is_some()
            || !top_level_projections.is_empty();
        if !named_databases.is_empty() && uses_legacy_database_configuration {
            return Err(ServerConfigError::MixedDatabaseConfiguration);
        }
        let databases = if named_databases.is_empty() {
            if server.replication.is_some() && top_level_replication.is_some() {
                return Err(ServerConfigError::InvalidFollowerConfiguration);
            }
            let replication = server
                .replication
                .or(top_level_replication)
                .map(ReplicationQuorumDocument::resolve)
                .transpose()?
                .unwrap_or_default();
            let database_path = bounded_path(select_os(
                arguments.database.as_ref(),
                database_environment,
                server.database.map(OsString::from),
                OsString::from(DEFAULT_DATABASE_PATH),
            )?)?;
            let default_outbox_directory = {
                let mut raw = lexical_absolute(&database_path, current_directory)?.into_os_string();
                raw.push(".riffoutbox");
                PathBuf::from(raw)
            };
            vec![DatabaseConfig {
                outbox: parse_outbox(maintenance.outbox, default_outbox_directory)?,
                archives: parse_archives(maintenance.archives)?,
                retention: parse_retention(maintenance.retention)?,
                limits: parse_limits(maintenance.limits)?,
                follower: server
                    .replication_source
                    .map(FollowerSourceDocument::resolve)
                    .transpose()?,
                replication,
                alias: DatabaseAlias::default_alias(),
                database_path,
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
            parse_named_databases(named_databases, current_directory)?
        };
        if databases
            .iter()
            .any(|database| database.follower.is_some() != (mode == ServerMode::Follower))
        {
            return Err(ServerConfigError::InvalidFollowerConfiguration);
        }
        if mode == ServerMode::Follower
            && databases.iter().any(|database| database.outbox.is_some())
        {
            return Err(ServerConfigError::InvalidOutboxConfiguration);
        }
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
        let mcp_listen_environment = environment.value(MCP_LISTEN_ENVIRONMENT);
        let uses_legacy_mcp_listener = arguments.mcp_listen.is_some()
            || mcp_listen_environment.is_some()
            || server.mcp_listen.is_some();
        if server.mcp_listener.is_some() && uses_legacy_mcp_listener {
            return Err(ServerConfigError::InvalidMcpConfiguration);
        }
        let mcp_listener = match server.mcp_listener {
            Some(document) => Some(parse_mcp_listener(document)?),
            None => select_optional_os(
                arguments.mcp_listen.as_ref(),
                mcp_listen_environment,
                server.mcp_listen.map(OsString::from),
            )?
            .map(|value| {
                parse_mcp_loopback_address(&value).map(McpListenerConfig::LoopbackCleartext)
            })
            .transpose()?,
        };
        let mcp_listen_address = mcp_listener.as_ref().map(McpListenerConfig::listen_address);
        let mcp_origins = resolve_mcp_origins(
            &arguments.mcp_origins,
            environment.value(MCP_ORIGINS_ENVIRONMENT),
            server.mcp_origins,
        )?;
        let http_listen_environment = environment.value(HTTP_LISTEN_ENVIRONMENT);
        let uses_legacy_http_listener = arguments.http_listen.is_some()
            || http_listen_environment.is_some()
            || server.http_listen.is_some();
        if server.http_listener.is_some() && uses_legacy_http_listener {
            return Err(ServerConfigError::InvalidHttpConfiguration);
        }
        let http_listener = match server.http_listener {
            Some(document) => Some(parse_http_listener(document)?),
            None => select_optional_os(
                arguments.http_listen.as_ref(),
                http_listen_environment,
                server.http_listen.map(OsString::from),
            )?
            .map(|value| parse_loopback_address(&value).map(HttpListenerConfig::LoopbackCleartext))
            .transpose()?,
        };
        let http_listen_address = http_listener
            .as_ref()
            .map(HttpListenerConfig::listen_address);
        let metrics_listen_address = select_optional_os(
            arguments.metrics_listen.as_ref(),
            environment.value(METRICS_LISTEN_ENVIRONMENT),
            server.metrics_listen.map(OsString::from),
        )?
        .map(|value| parse_metrics_loopback_address(&value))
        .transpose()?;
        let telemetry_flight_recorder_path = select_optional_os(
            arguments.telemetry_flight_recorder.as_ref(),
            environment.value(TELEMETRY_FLIGHT_RECORDER_ENVIRONMENT),
            server.telemetry_flight_recorder.map(OsString::from),
        )?
        .map(bounded_path)
        .transpose()?;
        let telemetry_flight_interval = parse_telemetry_flight_interval(select_os(
            arguments.telemetry_flight_interval_ms.as_ref(),
            environment.value(TELEMETRY_FLIGHT_INTERVAL_MS_ENVIRONMENT),
            server
                .telemetry_flight_interval_ms
                .map(|ms| OsString::from(ms.to_string())),
            OsString::from(DEFAULT_TELEMETRY_FLIGHT_INTERVAL_MS.to_string()),
        )?)?;
        let telemetry_otlp_endpoint = select_optional_os(
            arguments.telemetry_otlp_endpoint.as_ref(),
            environment.value(TELEMETRY_OTLP_ENDPOINT_ENVIRONMENT),
            server.telemetry_otlp_endpoint.map(OsString::from),
        )?;
        let telemetry_push_interval = parse_telemetry_push_interval(select_os(
            arguments.telemetry_push_interval_ms.as_ref(),
            environment.value(TELEMETRY_PUSH_INTERVAL_MS_ENVIRONMENT),
            server
                .telemetry_push_interval_ms
                .map(|ms| OsString::from(ms.to_string())),
            OsString::from(DEFAULT_TELEMETRY_PUSH_INTERVAL_MS.to_string()),
        )?)?;
        let telemetry_bearer_token_file = select_optional_os(
            arguments.telemetry_bearer_token_file.as_ref(),
            environment.value(TELEMETRY_BEARER_TOKEN_FILE_ENVIRONMENT),
            server.telemetry_bearer_token_file.map(OsString::from),
        )?
        .map(|raw| {
            ProtectedFilePath::new(PathBuf::from(raw)).map_err(|_| ServerConfigError::InvalidPath)
        })
        .transpose()?;
        let telemetry_ca_certificate_path = select_optional_os(
            arguments.telemetry_ca_certificate_path.as_ref(),
            environment.value(TELEMETRY_CA_CERTIFICATE_PATH_ENVIRONMENT),
            server.telemetry_ca_certificate_path.map(OsString::from),
        )?
        .map(|raw| {
            ProtectedFilePath::new(PathBuf::from(raw)).map_err(|_| ServerConfigError::InvalidPath)
        })
        .transpose()?;
        let telemetry_push = match telemetry_otlp_endpoint {
            Some(raw_endpoint) => {
                let raw_str = raw_endpoint
                    .to_str()
                    .ok_or(ServerConfigError::InvalidConfiguredValue)?;
                let endpoint =
                    parse_telemetry_push_endpoint(raw_str, telemetry_ca_certificate_path)?;
                Some(TelemetryPushConfig::new(
                    endpoint,
                    telemetry_push_interval,
                    telemetry_bearer_token_file,
                ))
            }
            None => {
                if telemetry_bearer_token_file.is_some() || telemetry_ca_certificate_path.is_some()
                {
                    return Err(ServerConfigError::InvalidConfiguredValue);
                }
                None
            }
        };
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

        if mcp_listener.is_none() && !mcp_origins.is_empty() {
            return Err(ServerConfigError::InvalidMcpConfiguration);
        }
        let mcp_audience = mcp_listener
            .as_ref()
            .map(|listener| match listener {
                McpListenerConfig::LoopbackCleartext(address) => {
                    Audience::new(format!("http://{address}{MCP_ROUTE}"))
                }
                McpListenerConfig::DirectTls(listener) => Audience::new(format!(
                    "{}{MCP_ROUTE}",
                    listener.public_endpoint().as_str()
                )),
            })
            .transpose()
            .map_err(|_| ServerConfigError::InvalidMcpConfiguration)?;
        if mcp_audience.as_ref() == Some(&audience) {
            return Err(ServerConfigError::InvalidMcpConfiguration);
        }
        if let Some(http_addr) = http_listen_address
            && http_addr.port() != 0
        {
            if mcp_listen_address == Some(http_addr) || metrics_listen_address == Some(http_addr) {
                return Err(ServerConfigError::InvalidListenAddress);
            }
            if let ApplicationListenerConfig::LoopbackCleartext(listener) = &application_listener
                && listener.listen_address().port() != 0
                && listener.listen_address() == http_addr
            {
                return Err(ServerConfigError::InvalidListenAddress);
            }
            if let ApplicationListenerConfig::DirectTls(listener) = &application_listener
                && listener.listen_address().port() != 0
                && listener.listen_address() == http_addr
            {
                return Err(ServerConfigError::InvalidListenAddress);
            }
        }
        if let Some(metrics_addr) = metrics_listen_address {
            if mcp_listen_address == Some(metrics_addr) {
                return Err(ServerConfigError::InvalidListenAddress);
            }
            if let ApplicationListenerConfig::LoopbackCleartext(listener) = &application_listener
                && listener.listen_address().port() != 0
                && listener.listen_address() == metrics_addr
            {
                return Err(ServerConfigError::InvalidListenAddress);
            }
        }

        let telemetry_signals = TelemetryConfig::resolve(server.telemetry)?;
        let logging = LoggingConfig::resolve(
            &arguments,
            environment,
            server.logging,
            telemetry_signals.logs.is_some(),
        )?;
        let config = Self {
            logging,
            telemetry_signals,
            config_file_path: config_path,
            uses_named_databases,
            mode,
            databases,
            application_listener,
            audience,
            mcp_listener,
            mcp_origins,
            mcp_audience,
            http_listener,
            metrics_listen_address,
            telemetry_flight_recorder_path,
            telemetry_flight_interval,
            telemetry_push,
            capability_key_path,
            idempotency_key_path,
            redb_commit_profile,
        };
        config.validate_disjoint_paths(current_directory)?;
        Ok(config)
    }

    fn validate_disjoint_paths(&self, current_directory: &Path) -> Result<(), ServerConfigError> {
        let mut paths = Vec::with_capacity(self.databases.len() * 6 + 3);
        for database in &self.databases {
            if let Some(outbox) = database.effective_outbox()
                && let Some(directory) = outbox.directory()
            {
                let outbox_root = bounded_path(directory.as_os_str().to_os_string())?;
                paths.push((
                    ConfiguredPathRole::OutboxRoot(database.alias.clone()),
                    lexical_absolute(&outbox_root, current_directory)?,
                ));
            }
            for archive in database.archives() {
                paths.push((
                    ConfiguredPathRole::ArchiveRoot(database.alias.clone(), archive.name().clone()),
                    archive.path().to_path_buf(),
                ));
            }
            paths.push((
                ConfiguredPathRole::Database(database.alias.clone()),
                lexical_absolute(&database.database_path, current_directory)?,
            ));
            let replication_root = bounded_path(database.replication_root().into_os_string())?;
            paths.push((
                ConfiguredPathRole::ReplicationRoot(database.alias.clone()),
                lexical_absolute(&replication_root, current_directory)?,
            ));
            let receiver_root =
                bounded_path(database.replication_receiver_root().into_os_string())?;
            paths.push((
                ConfiguredPathRole::ReceiverRoot(database.alias.clone()),
                lexical_absolute(&receiver_root, current_directory)?,
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
        if let Some(file) = &self.logging.file {
            paths.push((
                ConfiguredPathRole::LoggingDirectory,
                file.directory.as_path().to_path_buf(),
            ));
        }
        if let Some(flight_recorder_path) = &self.telemetry_flight_recorder_path {
            paths.push((
                ConfiguredPathRole::TelemetryFlightRecorder,
                lexical_absolute(flight_recorder_path, current_directory)?,
            ));
        }
        for database in &self.databases {
            if let Some(source) = &database.follower {
                for file in [
                    source.credential.as_path(),
                    source.tls.trust_root().as_path(),
                ] {
                    let file = lexical_absolute(file, current_directory)?;
                    if paths.iter().any(|(_, owned)| paths_overlap(&file, owned)) {
                        return Err(ServerConfigError::InvalidFollowerConfiguration);
                    }
                }
                if source.credential.as_path() == source.tls.trust_root().as_path() {
                    return Err(ServerConfigError::InvalidFollowerConfiguration);
                }
            }
        }
        self.telemetry_signals.validate_paths(&paths)?;
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

    pub(crate) fn config_file_path(&self) -> Option<&Path> {
        self.config_file_path.as_deref()
    }

    pub(crate) const fn uses_named_databases(&self) -> bool {
        self.uses_named_databases
    }

    #[cfg(test)]
    pub(crate) fn replication_root(&self) -> PathBuf {
        self.databases[0].replication_root()
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

    pub(crate) const fn mcp_listener(&self) -> Option<&McpListenerConfig> {
        self.mcp_listener.as_ref()
    }

    pub(crate) fn mcp_listen_address(&self) -> Option<SocketAddr> {
        self.mcp_listener
            .as_ref()
            .map(McpListenerConfig::listen_address)
    }

    pub(crate) fn mcp_origins(&self) -> &[String] {
        &self.mcp_origins
    }

    pub(crate) const fn mcp_audience(&self) -> Option<&Audience> {
        self.mcp_audience.as_ref()
    }

    pub(crate) const fn http_listener(&self) -> Option<&HttpListenerConfig> {
        self.http_listener.as_ref()
    }

    pub(crate) fn http_listen_address(&self) -> Option<SocketAddr> {
        self.http_listener
            .as_ref()
            .map(HttpListenerConfig::listen_address)
    }

    pub(crate) const fn metrics_listen_address(&self) -> Option<SocketAddr> {
        self.metrics_listen_address
    }

    pub(crate) fn telemetry_flight_recorder_path(&self) -> Option<&Path> {
        self.telemetry_flight_recorder_path.as_deref()
    }

    pub(crate) fn telemetry_signals(&self) -> &TelemetryConfig {
        &self.telemetry_signals
    }

    pub(crate) fn logging(&self) -> &LoggingConfig {
        &self.logging
    }

    pub(crate) const fn telemetry_flight_interval(&self) -> Duration {
        self.telemetry_flight_interval
    }

    pub(crate) const fn telemetry_push(&self) -> Option<&TelemetryPushConfig> {
        self.telemetry_push.as_ref()
    }

    pub(crate) fn backup_root(&self) -> &Path {
        self.databases[0].backup_root()
    }

    #[cfg(test)]
    pub(crate) fn projections_root(&self) -> &Path {
        self.databases[0].projections_root()
    }

    #[cfg(test)]
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

    pub(crate) fn limits(&self) -> DatabaseLimitsConfig {
        self.databases[0].limits()
    }
}

impl fmt::Debug for ServerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServerConfig")
            .field("mode", &self.mode)
            .field("databases", &self.databases)
            .field("application_listener", &self.application_listener)
            .field("audience", &"[CONFIGURED]")
            .field("mcp_listen_address", &self.mcp_listen_address())
            .field("mcp_origins", &"[CONFIGURED]")
            .field("mcp_audience", &"[DERIVED]")
            .field("http_listen_address", &self.http_listen_address())
            .field("metrics_listen_address", &self.metrics_listen_address)
            .field(
                "telemetry_flight_recorder_path",
                &self
                    .telemetry_flight_recorder_path
                    .as_ref()
                    .map(|_| "[CONFIGURED]"),
            )
            .field("telemetry_flight_interval", &self.telemetry_flight_interval)
            .field(
                "telemetry_push",
                &self.telemetry_push.as_ref().map(|_| "[CONFIGURED]"),
            )
            .field("capability_key_path", &"[CONFIGURED]")
            .field("idempotency_key_path", &"[CONFIGURED]")
            .field("redb_commit_profile", &self.redb_commit_profile)
            .finish()
    }
}

#[derive(Default)]
struct ArgumentValues {
    mode: Option<OsString>,
    config: Option<OsString>,
    database: Option<OsString>,
    listen: Option<OsString>,
    environment: Option<OsString>,
    audience: Option<OsString>,
    mcp_listen: Option<OsString>,
    mcp_origins: Vec<OsString>,
    http_listen: Option<OsString>,
    metrics_listen: Option<OsString>,
    telemetry_flight_recorder: Option<OsString>,
    telemetry_flight_interval_ms: Option<OsString>,
    log_directory: Option<OsString>,
    log_max_file_bytes: Option<OsString>,
    log_max_files: Option<OsString>,
    log_format: Option<OsString>,
    log_level: Option<OsString>,
    log_stderr: Option<OsString>,
    log_components: Option<OsString>,
    telemetry_otlp_endpoint: Option<OsString>,
    telemetry_push_interval_ms: Option<OsString>,
    telemetry_bearer_token_file: Option<OsString>,
    telemetry_ca_certificate_path: Option<OsString>,
    backup_root: Option<OsString>,
    projections_root: Option<OsString>,
    capability_keys: Option<OsString>,
    idempotency_keys: Option<OsString>,
    redb_commit_profile: Option<OsString>,
}

impl ArgumentValues {
    pub(crate) fn parse(
        arguments: impl IntoIterator<Item = OsString>,
    ) -> Result<Self, ServerConfigError> {
        let mut values = Self::default();
        let mut arguments = arguments.into_iter();
        while let Some(flag) = arguments.next() {
            let mut next_value = || arguments.next().ok_or(ServerConfigError::MissingValue);
            match flag.to_str() {
                Some("--log-directory") => set_once(&mut values.log_directory, next_value()?)?,
                Some("--log-max-file-bytes") => {
                    set_once(&mut values.log_max_file_bytes, next_value()?)?
                }
                Some("--log-max-files") => set_once(&mut values.log_max_files, next_value()?)?,
                Some("--log-format") => set_once(&mut values.log_format, next_value()?)?,
                Some("--log-level") => set_once(&mut values.log_level, next_value()?)?,
                Some("--log-stderr") => set_once(&mut values.log_stderr, next_value()?)?,
                Some("--log-components") => set_once(&mut values.log_components, next_value()?)?,
                Some("--mode") => set_once(&mut values.mode, next_value()?)?,
                Some("--config") => set_once(&mut values.config, next_value()?)?,
                Some("--database") => set_once(&mut values.database, next_value()?)?,
                Some("--listen") => set_once(&mut values.listen, next_value()?)?,
                Some("--environment") => set_once(&mut values.environment, next_value()?)?,
                Some("--audience") => set_once(&mut values.audience, next_value()?)?,
                Some("--mcp-listen") => set_once(&mut values.mcp_listen, next_value()?)?,
                Some("--http-listen") => set_once(&mut values.http_listen, next_value()?)?,
                Some("--metrics-listen") => set_once(&mut values.metrics_listen, next_value()?)?,
                Some("--telemetry-flight-recorder") => {
                    set_once(&mut values.telemetry_flight_recorder, next_value()?)?
                }
                Some("--telemetry-flight-interval-ms") => {
                    set_once(&mut values.telemetry_flight_interval_ms, next_value()?)?
                }
                Some("--telemetry-otlp-endpoint") => {
                    set_once(&mut values.telemetry_otlp_endpoint, next_value()?)?
                }
                Some("--telemetry-push-interval-ms") => {
                    set_once(&mut values.telemetry_push_interval_ms, next_value()?)?
                }
                Some("--telemetry-bearer-token-file") => {
                    set_once(&mut values.telemetry_bearer_token_file, next_value()?)?
                }
                Some("--telemetry-ca-certificate-path") => {
                    set_once(&mut values.telemetry_ca_certificate_path, next_value()?)?
                }
                Some("--backup-root") => set_once(&mut values.backup_root, next_value()?)?,
                Some("--projections-root") => {
                    set_once(&mut values.projections_root, next_value()?)?
                }
                Some("--capability-keys") => set_once(&mut values.capability_keys, next_value()?)?,
                Some("--idempotency-keys") => {
                    set_once(&mut values.idempotency_keys, next_value()?)?
                }
                Some("--redb-commit-profile") => {
                    set_once(&mut values.redb_commit_profile, next_value()?)?
                }
                Some("--mcp-origin") => values.mcp_origins.push(next_value()?),
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
    replication: Option<ReplicationQuorumDocument>,
    #[serde(default)]
    databases: BTreeMap<String, DatabaseDocument>,
    /// Top-level projection list for the legacy single-database configuration form.
    #[serde(default)]
    projections: Vec<ProjectionDocument>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerDocument {
    logging: Option<LoggingDocument>,
    telemetry: Option<TelemetryDocument>,
    mode: Option<String>,
    replication_source: Option<FollowerSourceDocument>,
    replication: Option<ReplicationQuorumDocument>,
    database: Option<String>,
    grpc_listen: Option<String>,
    application_listener: Option<ApplicationListenerDocument>,
    environment: Option<String>,
    audience: Option<String>,
    mcp_listen: Option<String>,
    mcp_listener: Option<McpListenerDocument>,
    mcp_origins: Option<Vec<String>>,
    http_listen: Option<String>,
    http_listener: Option<HttpListenerDocument>,
    metrics_listen: Option<String>,
    telemetry_flight_recorder: Option<String>,
    telemetry_flight_interval_ms: Option<u64>,
    telemetry_otlp_endpoint: Option<String>,
    telemetry_push_interval_ms: Option<u64>,
    telemetry_bearer_token_file: Option<String>,
    telemetry_ca_certificate_path: Option<String>,
    capability_keys: Option<String>,
    idempotency_keys: Option<String>,
    redb_commit_profile: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
#[allow(clippy::large_enum_variant)]
enum McpListenerDocument {
    LoopbackCleartext {
        listen: String,
    },
    DirectTls {
        listen: String,
        public_endpoint: String,
        certificate_chain: String,
        private_key: String,
        client_ca_certificate: Option<String>,
        auto_issue: Option<bool>,
        ca_certificate: Option<String>,
        ca_private_key: Option<String>,
        validity_days: Option<u16>,
        bounds: Option<ListenerBoundsDocument>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
#[allow(clippy::large_enum_variant)]
enum HttpListenerDocument {
    LoopbackCleartext {
        listen: String,
    },
    DirectTls {
        listen: String,
        public_endpoint: String,
        certificate_chain: String,
        private_key: String,
        client_ca_certificate: Option<String>,
        auto_issue: Option<bool>,
        ca_certificate: Option<String>,
        ca_private_key: Option<String>,
        validity_days: Option<u16>,
        bounds: Option<ListenerBoundsDocument>,
    },
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
        client_ca_certificate: Option<String>,
        auto_issue: Option<bool>,
        ca_certificate: Option<String>,
        ca_private_key: Option<String>,
        validity_days: Option<u16>,
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
    outbox: Option<OutboxDocument>,
    #[serde(default)]
    archives: Vec<ArchiveDocument>,
    backup_root: Option<String>,
    projections_root: Option<String>,
    retention: Option<RetentionDocument>,
    limits: Option<LimitsDocument>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DatabaseDocument {
    outbox: Option<OutboxDocument>,
    #[serde(default)]
    archives: Vec<ArchiveDocument>,
    replication_source: Option<FollowerSourceDocument>,
    replication: Option<ReplicationQuorumDocument>,
    path: String,
    backup_root: String,
    environment: String,
    /// Absolute directory for columnar projection storage; defaults are not
    /// applied for named databases (must be explicit).
    projections_root: Option<String>,
    #[serde(default)]
    projections: Vec<ProjectionDocument>,
    retention: Option<RetentionDocument>,
    limits: Option<LimitsDocument>,
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
            || document.name == "."
            || document.name == ".."
            || document.name.contains('/')
            || document.name.contains('\\')
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
    current_directory: &Path,
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
            let database_path = bounded_path(OsString::from(document.path))?;
            let default_outbox_directory = {
                let mut raw = lexical_absolute(&database_path, current_directory)?.into_os_string();
                raw.push(".riffoutbox");
                PathBuf::from(raw)
            };
            Ok(DatabaseConfig {
                outbox: parse_outbox(document.outbox, default_outbox_directory)?,
                archives: parse_archives(document.archives)?,
                retention: parse_retention(document.retention)?,
                limits: parse_limits(document.limits)?,
                follower: document
                    .replication_source
                    .map(FollowerSourceDocument::resolve)
                    .transpose()?,
                replication: document
                    .replication
                    .map(ReplicationQuorumDocument::resolve)
                    .transpose()?
                    .unwrap_or_default(),
                alias,
                database_path,
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

fn build_server_tls_files(
    certificate_chain: String,
    private_key: String,
    client_ca_certificate: Option<String>,
    auto_issue: Option<bool>,
    ca_certificate: Option<String>,
    ca_private_key: Option<String>,
    validity_days: Option<u16>,
) -> Result<ServerTlsFiles, RemoteConfigError> {
    let cert_path = ProtectedFilePath::new(certificate_chain)?;
    let key_path = ProtectedFilePath::new(private_key)?;
    let mut files = ServerTlsFiles::new(cert_path.clone(), key_path)?;
    if let Some(client_ca) = client_ca_certificate {
        files = files.with_client_ca(ProtectedFilePath::new(client_ca)?)?;
    }
    if auto_issue == Some(true) {
        let parent = cert_path
            .as_path()
            .parent()
            .ok_or(RemoteConfigError::InvalidProtectedPath)?;
        let ca_cert = match ca_certificate {
            Some(path) => ProtectedFilePath::new(path)?,
            None => ProtectedFilePath::new(parent.join("ca.pem"))?,
        };
        let ca_key = match ca_private_key {
            Some(path) => ProtectedFilePath::new(path)?,
            None => ProtectedFilePath::new(parent.join("ca.key"))?,
        };
        let days = std::num::NonZeroU16::new(validity_days.unwrap_or(365))
            .ok_or(RemoteConfigError::InvalidBound)?;
        files = files.with_auto_issue(AutoIssueTlsConfig::new(ca_cert, ca_key, days)?)?;
    } else if ca_certificate.is_some() || ca_private_key.is_some() || validity_days.is_some() {
        return Err(RemoteConfigError::InvalidBound);
    }
    Ok(files)
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
            client_ca_certificate,
            auto_issue,
            ca_certificate,
            ca_private_key,
            validity_days,
            bounds,
        } => {
            let listen = listen
                .parse::<SocketAddr>()
                .map_err(|_| ServerConfigError::InvalidListenAddress)?;
            let endpoint = CanonicalHttpsEndpoint::parse(&public_endpoint)
                .map_err(ServerConfigError::ApplicationListener)?;
            let files = build_server_tls_files(
                certificate_chain,
                private_key,
                client_ca_certificate,
                auto_issue,
                ca_certificate,
                ca_private_key,
                validity_days,
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

fn parse_mcp_listener(
    document: McpListenerDocument,
) -> Result<McpListenerConfig, ServerConfigError> {
    match document {
        McpListenerDocument::LoopbackCleartext { listen } => Ok(
            McpListenerConfig::LoopbackCleartext(parse_mcp_loopback_address(OsStr::new(&listen))?),
        ),
        McpListenerDocument::DirectTls {
            listen,
            public_endpoint,
            certificate_chain,
            private_key,
            client_ca_certificate,
            auto_issue,
            ca_certificate,
            ca_private_key,
            validity_days,
            bounds,
        } => {
            let listen = listen
                .parse::<SocketAddr>()
                .map_err(|_| ServerConfigError::InvalidMcpConfiguration)?;
            let endpoint = CanonicalHttpsEndpoint::parse(&public_endpoint)
                .map_err(|_| ServerConfigError::InvalidMcpConfiguration)?;
            let files = build_server_tls_files(
                certificate_chain,
                private_key,
                client_ca_certificate,
                auto_issue,
                ca_certificate,
                ca_private_key,
                validity_days,
            )
            .map_err(|_| ServerConfigError::InvalidMcpConfiguration)?;
            let bounds = parse_listener_bounds(bounds)
                .map_err(|_| ServerConfigError::InvalidMcpConfiguration)?;
            let config = DirectTlsListenerConfig::new(listen, endpoint, files, bounds)
                .map_err(|_| ServerConfigError::InvalidMcpConfiguration)?;
            Ok(McpListenerConfig::DirectTls(config))
        }
    }
}

fn parse_http_listener(
    document: HttpListenerDocument,
) -> Result<HttpListenerConfig, ServerConfigError> {
    match document {
        HttpListenerDocument::LoopbackCleartext { listen } => Ok(
            HttpListenerConfig::LoopbackCleartext(parse_loopback_address(OsStr::new(&listen))?),
        ),
        HttpListenerDocument::DirectTls {
            listen,
            public_endpoint,
            certificate_chain,
            private_key,
            client_ca_certificate,
            auto_issue,
            ca_certificate,
            ca_private_key,
            validity_days,
            bounds,
        } => {
            let listen = listen
                .parse::<SocketAddr>()
                .map_err(|_| ServerConfigError::InvalidHttpConfiguration)?;
            let endpoint = CanonicalHttpsEndpoint::parse(&public_endpoint)
                .map_err(|_| ServerConfigError::InvalidHttpConfiguration)?;
            let files = build_server_tls_files(
                certificate_chain,
                private_key,
                client_ca_certificate,
                auto_issue,
                ca_certificate,
                ca_private_key,
                validity_days,
            )
            .map_err(|_| ServerConfigError::InvalidHttpConfiguration)?;
            let bounds = parse_listener_bounds(bounds)
                .map_err(|_| ServerConfigError::InvalidHttpConfiguration)?;
            let config = DirectTlsListenerConfig::new(listen, endpoint, files, bounds)
                .map_err(|_| ServerConfigError::InvalidHttpConfiguration)?;
            Ok(HttpListenerConfig::DirectTls(config))
        }
    }
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

fn parse_metrics_loopback_address(value: &OsStr) -> Result<SocketAddr, ServerConfigError> {
    let address = parse_loopback_address(value)?;
    if address.port() == 0 {
        return Err(ServerConfigError::InvalidListenAddress);
    }
    Ok(address)
}

fn parse_telemetry_flight_interval(value: OsString) -> Result<Duration, ServerConfigError> {
    let raw = value
        .to_str()
        .ok_or(ServerConfigError::InvalidConfiguredValue)?;
    let ms = raw
        .parse::<u64>()
        .map_err(|_| ServerConfigError::InvalidConfiguredValue)?;
    if !(MIN_TELEMETRY_FLIGHT_INTERVAL_MS..=MAX_TELEMETRY_FLIGHT_INTERVAL_MS).contains(&ms) {
        return Err(ServerConfigError::InvalidConfiguredValue);
    }
    Ok(Duration::from_millis(ms))
}

fn parse_telemetry_push_interval(value: OsString) -> Result<Duration, ServerConfigError> {
    let raw = value
        .to_str()
        .ok_or(ServerConfigError::InvalidConfiguredValue)?;
    let ms = raw
        .parse::<u64>()
        .map_err(|_| ServerConfigError::InvalidConfiguredValue)?;
    if !(MIN_TELEMETRY_PUSH_INTERVAL_MS..=MAX_TELEMETRY_PUSH_INTERVAL_MS).contains(&ms) {
        return Err(ServerConfigError::InvalidConfiguredValue);
    }
    Ok(Duration::from_millis(ms))
}

pub(crate) fn parse_telemetry_push_endpoint(
    raw_endpoint: &str,
    ca_certificate_path: Option<ProtectedFilePath>,
) -> Result<TelemetryPushEndpoint, ServerConfigError> {
    if raw_endpoint.is_empty()
        || raw_endpoint.len() > MAX_TELEMETRY_OTLP_ENDPOINT_BYTES
        || !raw_endpoint
            .bytes()
            .all(|byte| (0x21..=0x7e).contains(&byte))
        || raw_endpoint.contains('@')
        || raw_endpoint.contains('?')
        || raw_endpoint.contains('#')
    {
        return Err(ServerConfigError::InvalidConfiguredValue);
    }
    if let Some(rest) = raw_endpoint.strip_prefix("http://") {
        if ca_certificate_path.is_some() {
            return Err(ServerConfigError::InvalidConfiguredValue);
        }
        let (authority, path) = match rest.find('/') {
            Some(idx) => (&rest[..idx], &rest[idx..]),
            None => (rest, "/v1/metrics"),
        };
        if authority.is_empty() {
            return Err(ServerConfigError::InvalidConfiguredValue);
        }
        let address = authority
            .parse::<SocketAddr>()
            .map_err(|_| ServerConfigError::NonLoopbackListenAddress)?;
        if !address.ip().is_loopback() {
            return Err(ServerConfigError::NonLoopbackListenAddress);
        }
        if address.port() == 0 {
            return Err(ServerConfigError::InvalidConfiguredValue);
        }
        Ok(TelemetryPushEndpoint::LoopbackHttp {
            address,
            authority: authority.to_owned(),
            path: path.to_owned(),
        })
    } else if let Some(rest) = raw_endpoint.strip_prefix("https://") {
        let (authority, path) = match rest.find('/') {
            Some(idx) => (&rest[..idx], &rest[idx..]),
            None => (rest, "/v1/metrics"),
        };
        if authority.is_empty() {
            return Err(ServerConfigError::InvalidConfiguredValue);
        }
        let (host, port) = if authority.starts_with('[') {
            let close_idx = authority
                .find(']')
                .ok_or(ServerConfigError::InvalidConfiguredValue)?;
            let ipv6_host = &authority[1..close_idx];
            if ipv6_host.parse::<std::net::Ipv6Addr>().is_err() {
                return Err(ServerConfigError::InvalidConfiguredValue);
            }
            let after_bracket = &authority[close_idx + 1..];
            let port = if after_bracket.is_empty() {
                443_u16
            } else if let Some(port_str) = after_bracket.strip_prefix(':') {
                port_str
                    .parse::<u16>()
                    .map_err(|_| ServerConfigError::InvalidConfiguredValue)?
            } else {
                return Err(ServerConfigError::InvalidConfiguredValue);
            };
            (ipv6_host.to_owned(), port)
        } else if let Some((h, p)) = authority.rsplit_once(':') {
            let port = p
                .parse::<u16>()
                .map_err(|_| ServerConfigError::InvalidConfiguredValue)?;
            (h.to_owned(), port)
        } else {
            (authority.to_owned(), 443_u16)
        };
        if host.is_empty() || port == 0 {
            return Err(ServerConfigError::InvalidConfiguredValue);
        }
        rustls_pki_types::ServerName::try_from(host.clone())
            .map_err(|_| ServerConfigError::InvalidConfiguredValue)?;
        Ok(TelemetryPushEndpoint::Https {
            host,
            port,
            authority: authority.to_owned(),
            path: path.to_owned(),
            ca_certificate_path,
        })
    } else {
        Err(ServerConfigError::InvalidConfiguredValue)
    }
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
    OutboxRoot(DatabaseAlias),
    ArchiveRoot(DatabaseAlias, riffdb_types::ArchiveNameV1),
    Database(DatabaseAlias),
    BackupRoot(DatabaseAlias),
    ProjectionsRoot(DatabaseAlias),
    ReplicationRoot(DatabaseAlias),
    ReceiverRoot(DatabaseAlias),
    CapabilityKeys,
    IdempotencyKeys,
    TelemetryFlightRecorder,
    LoggingDirectory,
}

impl fmt::Display for ConfiguredPathRole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutboxRoot(alias) => write!(formatter, "outbox root for '{alias}'"),
            Self::ArchiveRoot(alias, name) => write!(formatter, "archive '{name}' for '{alias}'"),
            Self::Database(alias) => write!(formatter, "database path for '{alias}'"),
            Self::BackupRoot(alias) => write!(formatter, "backup_root for '{alias}'"),
            Self::ProjectionsRoot(alias) => write!(formatter, "projections_root for '{alias}'"),
            Self::ReplicationRoot(alias) => {
                write!(formatter, "replication artifact root for '{alias}'")
            }
            Self::ReceiverRoot(alias) => {
                write!(formatter, "replication receiver root for '{alias}'")
            }
            Self::CapabilityKeys => formatter.write_str("capability key path"),
            Self::IdempotencyKeys => formatter.write_str("idempotency key path"),
            Self::TelemetryFlightRecorder => formatter.write_str("telemetry flight recorder path"),
            Self::LoggingDirectory => formatter.write_str("logging directory"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ServerConfigError {
    Telemetry(telemetry::SignalConfigError),
    InvalidLoggingConfiguration,
    InvalidLogDirectory,
    InvalidLogRetention,
    InvalidOutboxConfiguration,
    InvalidArchiveConfiguration,
    InvalidRetentionConfiguration,
    InvalidLimitsConfiguration,
    InvalidFollowerConfiguration,
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
    InvalidHttpConfiguration,
    InvalidRedbCommitProfile,
}

impl fmt::Display for ServerConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Telemetry(error) => return error.fmt(formatter),
            Self::InvalidLogDirectory => "logging directory must be an absolute normalized protected path, disjoint from database and secret paths",
            Self::InvalidLogRetention => "logging retention requires a directory, max_file_bytes in 1048576..=268435456 and max_files in 2..=32",
            Self::InvalidLoggingConfiguration => "logging configuration is invalid; choose text/json, a closed severity/component, and at least one enabled sink",
            Self::InvalidOutboxConfiguration => {
                "outbox destination configuration is invalid: durable_local requires positive quota_encoded_bytes and an optional absolute directory; http_webhook requires an http/https endpoint, timeout_seconds in 1..=300, and max_attempts in 1..=64"
            }
            Self::InvalidArchiveConfiguration => "archive configuration is invalid or incomplete",
            Self::InvalidRetentionConfiguration => {
                "retention configuration must name retained_sequences or hold, with sequences_per_epoch in 1..=256"
            }
            Self::InvalidLimitsConfiguration => {
                "database resource limits configuration is out of bounds"
            }
            Self::InvalidFollowerConfiguration => {
                "follower source configuration is invalid or incomplete"
            }
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
            Self::InvalidHttpConfiguration => "configured HTTP listener endpoint is invalid",
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
    struct TestRoot(
        PathBuf,
        // Held only so `Drop` removes the whole scope (field-scoped allow so
        // the protection survives if this file's `#![allow(dead_code)]` is
        // ever narrowed).
        #[allow(dead_code)] tempfile::TempDir,
    );

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

    // req: DX-003, ID-005
    #[test]
    fn structured_logging_configuration_accepts_safe_format_and_component_levels() {
        let root = TestRoot::new();
        let document = root.write(
            br#"
[server.logging]
format = "json"
level = "info"
stderr = true
[server.logging.components]
projection = "debug"
"#,
        );
        let arguments = vec![OsString::from("--config"), document.into_os_string()];
        assert!(
            ServerConfig::resolve(arguments, &EmptyEnvironment, &root.0).is_ok(),
            "documented JSON logging configuration must be accepted"
        );
    }

    // req: DX-003, ID-005
    #[test]
    fn logging_precedence_and_hard_component_vocabulary_fail_closed() {
        let root = TestRoot::new();
        let document = root.write(
            br#"
[server.logging]
format = "json"
level = "warn"
[server.logging.components]
projection = "debug"
"#,
        );
        let base = || {
            vec![
                OsString::from("--config"),
                document.clone().into_os_string(),
            ]
        };
        let environment = TestEnvironment::default()
            .with("RIFFDB_LOG_FORMAT", "text")
            .with("RIFFDB_LOG_LEVEL", "error");
        let mut arguments = base();
        arguments.extend(["--log-level", "info"].map(OsString::from));
        let effective = ServerConfig::resolve(arguments, &environment, &root.0).unwrap();
        assert_eq!(effective.logging().format, logging::LogFormat::Text);
        assert_eq!(effective.logging().level, logging::LogLevel::Info);
        assert!(effective.logging().allows(
            crate::operational_logging::Component::Projection,
            tracing::Level::DEBUG
        ));
        for value in [
            "hyper=trace",
            "server=secret-canary",
            "server=info,server=debug",
        ] {
            let environment = TestEnvironment::default().with("RIFFDB_LOG_COMPONENTS", value);
            let error = ServerConfig::resolve(base(), &environment, &root.0).unwrap_err();
            assert_eq!(error, ServerConfigError::InvalidLoggingConfiguration);
            assert!(!error.to_string().contains("secret-canary"));
        }
        let environment = TestEnvironment::default().with("RIFFDB_LOG_FORMAT", "secret-canary");
        assert_eq!(
            ServerConfig::resolve(base(), &environment, &root.0).unwrap_err(),
            ServerConfigError::InvalidLoggingConfiguration
        );
        let environment = TestEnvironment::default().with("RUST_LOG", "hyper=trace");
        let effective = ServerConfig::resolve(base(), &environment, &root.0).unwrap();
        assert_eq!(effective.logging().level, logging::LogLevel::Warn);
    }

    // req: DX-003, ID-005
    #[test]
    fn file_logging_configuration_is_bounded_and_disjoint() {
        let root = TestRoot::new();
        let directory = root.0.join("logs");
        let document = root.write(
            format!(
                r#"
[server.logging]
format = "json"
stderr = false
directory = "{}"
max_file_bytes = 1048576
max_files = 2
"#,
                directory.display()
            )
            .as_bytes(),
        );
        let arguments = vec![OsString::from("--config"), document.into_os_string()];
        assert!(
            ServerConfig::resolve(arguments, &EmptyEnvironment, &root.0).is_ok(),
            "a private bounded file sink permits disabling stderr"
        );

        for arguments in [
            vec!["--log-stderr", "false"],
            vec!["--log-max-files", "2"],
            vec!["--log-directory", "logs", "--log-max-files", "1"],
            vec!["--log-directory", "logs", "--log-max-files", "33"],
            vec!["--log-directory", "logs", "--log-max-file-bytes", "1048575"],
            vec![
                "--log-directory",
                "logs",
                "--log-max-file-bytes",
                "268435457",
            ],
            vec![
                "--log-directory",
                "database.redb",
                "--database",
                "database.redb",
            ],
        ] {
            assert!(
                ServerConfig::resolve(
                    arguments.into_iter().map(|value| if value == "logs" {
                        directory.clone().into_os_string()
                    } else if value == "database.redb" {
                        root.0.join(value).into_os_string()
                    } else {
                        OsString::from(value)
                    }),
                    &EmptyEnvironment,
                    &root.0
                )
                .is_err()
            );
        }
    }

    // req: DX-003, ID-005, VER-001
    #[test]
    fn daemon_unknown_option_and_missing_value_are_distinct() {
        assert!(matches!(
            ArgumentValues::parse([OsString::from("--unknown-canary")]),
            Err(ServerConfigError::UnknownOption)
        ));
        for flag in ["--listen", "--config", "--log-level"] {
            assert!(matches!(
                ArgumentValues::parse([OsString::from(flag)]),
                Err(ServerConfigError::MissingValue)
            ));
        }
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

    // req: NET-005, REC-001, REC-002, PERF-007, PERF-019
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
        assert!(config.uses_named_databases());
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

        let mut max_doc = String::from("[server]\ngrpc_listen = \"127.0.0.1:7001\"\n");
        for index in 0..MAX_DATABASES_PER_PROCESS {
            max_doc.push_str(&format!(
                "[databases.db_{index:03}]\npath = \"data/db_{index:03}.redb\"\nbackup_root = \"/tmp/riffdb-backups-{index:03}\"\nprojections_root = \"/tmp/riffdb-projections-{index:03}\"\nenvironment = \"development\"\n"
            ));
        }
        let max_path = root.write(max_doc.as_bytes());
        let max_config = ServerConfig::resolve(
            [OsString::from("--config"), max_path.into_os_string()],
            &EmptyEnvironment,
            &root.0,
        )
        .expect("512-database registry must parse");
        assert_eq!(max_config.databases().len(), MAX_DATABASES_PER_PROCESS);

        max_doc.push_str(
            "[databases.db_overflow]\npath = \"data/db_overflow.redb\"\nbackup_root = \"/tmp/riffdb-backups-overflow\"\nprojections_root = \"/tmp/riffdb-projections-overflow\"\nenvironment = \"development\"\n",
        );
        let overflow_path = root.write(max_doc.as_bytes());
        assert_eq!(
            ServerConfig::resolve(
                [OsString::from("--config"), overflow_path.into_os_string()],
                &EmptyEnvironment,
                &root.0,
            )
            .unwrap_err(),
            ServerConfigError::InvalidDatabaseConfiguration
        );

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
    // req: REP-003, REC-001
    fn replication_artifact_roots_are_reserved_and_disjoint_from_other_configured_paths() {
        let root = TestRoot::new();
        for receiver in [false, true] {
            for other in ["database", "backup", "keys"] {
                let mut config =
                    ServerConfig::resolve(Vec::new(), &EmptyEnvironment, &root.0).unwrap();
                let reserved = if receiver {
                    config.databases[0].replication_receiver_root()
                } else {
                    config.replication_root()
                };
                assert!(
                    reserved
                        .as_os_str()
                        .as_encoded_bytes()
                        .ends_with(if receiver {
                            b".riffreceiver"
                        } else {
                            b".riffreplication"
                        })
                );
                match other {
                    "database" => {
                        let mut second =
                            ServerConfig::resolve(Vec::new(), &EmptyEnvironment, &root.0)
                                .unwrap()
                                .databases
                                .remove(0);
                        second.alias = DatabaseAlias::new("second").unwrap();
                        second.database_path = reserved.join("second.redb");
                        config.databases.push(second);
                    }
                    "backup" => config.databases[0].backup_root = reserved,
                    _ => config.capability_key_path = reserved.join("secret.keys"),
                }
                assert!(matches!(
                    config.validate_disjoint_paths(&root.0),
                    Err(ServerConfigError::OverlappingPaths { .. })
                ));
            }
        }
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

    #[test]
    // req: ID-005, PERF-001, END-007, END-008
    fn metrics_listener_and_telemetry_flight_recorder_validate_loopback_interval_and_disjoint_paths()
     {
        let root = TestRoot::new();
        let document = root.write(
            br#"
[server]
metrics_listen = "127.0.0.1:9464"
telemetry_flight_recorder = "telemetry/flight.ndjson"
telemetry_flight_interval_ms = 5000
"#,
        );
        let config = ServerConfig::resolve(
            [OsString::from("--config"), document.into_os_string()],
            &EmptyEnvironment,
            &root.0,
        )
        .expect("valid metrics and flight-recorder config");
        assert_eq!(
            config.metrics_listen_address(),
            Some("127.0.0.1:9464".parse().expect("loopback socket"))
        );
        assert_eq!(
            config.telemetry_flight_recorder_path(),
            Some(Path::new("telemetry/flight.ndjson"))
        );
        assert_eq!(
            config.telemetry_flight_interval(),
            Duration::from_millis(5_000)
        );
        let debug = format!("{config:?}");
        assert!(
            !debug.contains("telemetry/flight.ndjson"),
            "Debug formatting must redact telemetry flight recorder paths: {debug}"
        );

        // Non-loopback metrics listener fails closed.
        assert_eq!(
            ServerConfig::parse(["--metrics-listen", "0.0.0.0:9464"].map(OsString::from))
                .unwrap_err(),
            ServerConfigError::NonLoopbackListenAddress
        );
        // Port 0 fails closed.
        assert_eq!(
            ServerConfig::parse(["--metrics-listen", "127.0.0.1:0"].map(OsString::from))
                .unwrap_err(),
            ServerConfigError::InvalidListenAddress
        );
        // Interval below 100ms or above 3_600_000ms fails closed.
        assert_eq!(
            ServerConfig::parse(["--telemetry-flight-interval-ms", "50"].map(OsString::from))
                .unwrap_err(),
            ServerConfigError::InvalidConfiguredValue
        );
        assert_eq!(
            ServerConfig::parse(["--telemetry-flight-interval-ms", "3600001"].map(OsString::from))
                .unwrap_err(),
            ServerConfigError::InvalidConfiguredValue
        );
        // Overlapping flight recorder path with database path fails closed.
        assert!(matches!(
            ServerConfig::parse(
                [
                    "--database",
                    "shared.redb",
                    "--telemetry-flight-recorder",
                    "shared.redb",
                ]
                .map(OsString::from)
            )
            .unwrap_err(),
            ServerConfigError::OverlappingPaths { .. }
        ));
    }

    #[test]
    fn database_limits_parse_in_single_and_named_database_documents_and_reject_mixed_form() {
        let root = TestRoot::new();
        let single_doc = root.write(
            br#"
[maintenance.limits]
max_storage_bytes = 5368709120
max_write_rps = 50
write_burst = 100
max_read_rps = 250
read_burst = 500
max_blocking_operations = 8
blocking_worker_threads = 2
max_coordinator_capacity = 64
"#,
        );
        let single = ServerConfig::resolve(
            [OsString::from("--config"), single_doc.into_os_string()],
            &EmptyEnvironment,
            &root.0,
        )
        .expect("valid single-database limits config");
        let limits = single.databases()[0].limits();
        assert_eq!(limits.max_storage_bytes().unwrap().get(), 5_368_709_120);
        assert_eq!(limits.write_rate().unwrap().rate_per_second().get(), 50);
        assert_eq!(limits.write_rate().unwrap().burst().get(), 100);
        assert_eq!(limits.read_rate().unwrap().rate_per_second().get(), 250);
        assert_eq!(limits.read_rate().unwrap().burst().get(), 500);
        assert_eq!(limits.max_blocking_operations().get(), 8);
        assert_eq!(limits.blocking_worker_threads().get(), 2);
        assert_eq!(limits.max_coordinator_capacity().unwrap().get(), 64);

        let named_doc = root.write(
            br#"
[databases.tenant_a]
path = "data/tenant_a.redb"
backup_root = "/var/lib/riffdb/backups/tenant_a"
projections_root = "/var/lib/riffdb/projections/tenant_a"
environment = "staging"

[databases.tenant_a.limits]
max_storage_bytes = 1073741824
max_write_rps = 25
max_read_rps = 100
max_blocking_operations = 4
"#,
        );
        let named = ServerConfig::resolve(
            [OsString::from("--config"), named_doc.into_os_string()],
            &EmptyEnvironment,
            &root.0,
        )
        .expect("valid named-database limits config");
        let tenant_limits = named.databases()[0].limits();
        assert_eq!(
            tenant_limits.max_storage_bytes().unwrap().get(),
            1_073_741_824
        );
        assert_eq!(
            tenant_limits.write_rate().unwrap().rate_per_second().get(),
            25
        );
        assert_eq!(tenant_limits.write_rate().unwrap().burst().get(), 25);
        assert_eq!(tenant_limits.max_blocking_operations().get(), 4);
        assert_eq!(tenant_limits.blocking_worker_threads().get(), 4);

        // Mixing [maintenance.limits] with [databases.<alias>] fails closed.
        let mixed_doc = root.write(
            br#"
[maintenance.limits]
max_storage_bytes = 1073741824

[databases.tenant_a]
path = "data/tenant_a.redb"
backup_root = "/var/lib/riffdb/backups/tenant_a"
environment = "staging"
"#,
        );
        assert_eq!(
            ServerConfig::resolve(
                [OsString::from("--config"), mixed_doc.into_os_string()],
                &EmptyEnvironment,
                &root.0,
            )
            .unwrap_err(),
            ServerConfigError::MixedDatabaseConfiguration
        );
    }

    // req: MCP-021
    #[test]
    fn mcp_listener_supports_direct_tls_and_rejects_mixed_legacy_configuration() {
        let root = TestRoot::new();
        let tls_doc = root.write(
            br#"
[server]
mcp_origins = ["https://app.example.com"]

[server.mcp_listener]
mode = "direct_tls"
listen = "0.0.0.0:8444"
public_endpoint = "https://db.example.com:8444"
certificate_chain = "/etc/riffdb/mcp-cert.pem"
private_key = "/etc/riffdb/mcp-key.pem"
"#,
        );
        let resolved = ServerConfig::resolve(
            [OsString::from("--config"), tls_doc.into_os_string()],
            &EmptyEnvironment,
            &root.0,
        )
        .expect("valid direct_tls mcp_listener config");
        assert_eq!(
            resolved.mcp_listen_address(),
            Some("0.0.0.0:8444".parse().expect("socket"))
        );
        assert_eq!(
            resolved.mcp_audience().map(Audience::as_str),
            Some("https://db.example.com:8444/mcp")
        );
        assert_eq!(resolved.mcp_origins(), &["https://app.example.com"]);

        let mixed_doc = root.write(
            br#"
[server]
mcp_listen = "127.0.0.1:7444"

[server.mcp_listener]
mode = "loopback_cleartext"
listen = "127.0.0.1:7444"
"#,
        );
        assert_eq!(
            ServerConfig::resolve(
                [OsString::from("--config"), mixed_doc.into_os_string()],
                &EmptyEnvironment,
                &root.0,
            )
            .unwrap_err(),
            ServerConfigError::InvalidMcpConfiguration
        );
    }
    // req: ID-005, NET-002, NET-003, END-007, END-008
    #[test]
    fn telemetry_signal_tables_are_explicit_independent_and_allow_a_log_only_sink() {
        let root = TestRoot::new();
        let document = root.write(
            br#"
[server.logging]
stderr=false
[server.telemetry.logs]
endpoint="http://127.0.0.1:4318/v1/logs"
[server.telemetry.traces]
endpoint="http://127.0.0.1:4318/v1/traces"
sample_per_million=0
"#,
        );
        let environment = TestEnvironment::default()
            .with("OTEL_EXPORTER_OTLP_ENDPOINT", "secret-endpoint-canary")
            .with("OTEL_RESOURCE_ATTRIBUTES", "secret-resource-canary");
        let config = ServerConfig::resolve(
            [OsString::from("--config"), document.into_os_string()],
            &environment,
            &root.0,
        )
        .expect("explicit per-signal file tables and an OTLP-only log sink must resolve");
        assert!(!config.logging().stderr);
    }
}
