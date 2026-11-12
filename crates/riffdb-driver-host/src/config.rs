use std::fmt;
use std::fs;
use std::io::Read as _;
use std::num::NonZeroU32;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use riffdb_client_rust::{CallMetadata, load_protected_bearer_credential};
use riffdb_config::{
    CanonicalHttpsEndpoint, ClientTrustRootConfig, ProtectedFilePath, TlsClientConfig,
    TlsServerIdentity,
};
use riffdb_types::DatabaseAlias;
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::{ApplicationCatalog, DriverHost, DriverPool, DriverSocket};

const MAX_CONFIG_BYTES: u64 = 64 * 1024;
const MAX_ARTIFACT_BYTES: u64 = 8 * 1024 * 1024;

/// Fully built driver runtime retaining no caller-visible credential material.
pub struct DriverRuntime {
    host: DriverHost,
    socket: DriverSocket,
}

impl DriverRuntime {
    /// Loads one protected exact configuration and establishes the complete
    /// verified remote connection pool before binding the local socket.
    pub async fn from_config_file(path: &Path) -> Result<Self, DriverRuntimeError> {
        let bytes = read_checked(path, MAX_CONFIG_BYTES, true)?;
        let text =
            std::str::from_utf8(&bytes).map_err(|_| DriverRuntimeError::InvalidConfiguration)?;
        let document: DriverDocument =
            toml::from_str(text).map_err(|_| DriverRuntimeError::InvalidConfiguration)?;
        let socket_path = checked_absolute(&document.driver.socket)?;
        crate::socket::checked_socket_path(&socket_path)
            .map_err(|_| DriverRuntimeError::InvalidConfiguration)?;
        crate::socket::validate_allowed_peer_uids(&document.driver.allowed_peer_uids)
            .map_err(|_| DriverRuntimeError::InvalidConfiguration)?;
        let mut app_sections = Vec::new();
        if let Some(app) = document.application.clone() {
            app_sections.push(app);
        }
        app_sections.extend(document.applications.iter().cloned());
        if app_sections.is_empty() || app_sections.len() > 64 {
            return Err(DriverRuntimeError::InvalidConfiguration);
        }
        let pool_connections = NonZeroU32::new(document.remote.pool_connections)
            .filter(|value| value.get() <= 16)
            .ok_or(DriverRuntimeError::InvalidConfiguration)?;
        let streams = NonZeroU32::new(document.remote.streams_per_connection)
            .filter(|value| value.get() <= 256)
            .ok_or(DriverRuntimeError::InvalidConfiguration)?;
        let tls = build_remote_tls(
            &document.remote.endpoint,
            &document.remote,
            pool_connections,
            streams,
        )?;
        if document.remote.standby_endpoints.len() > 4
            || document.remote.read_replica_endpoints.len() > 4
        {
            return Err(DriverRuntimeError::InvalidConfiguration);
        }
        let standby_tls = document
            .remote
            .standby_endpoints
            .iter()
            .map(|endpoint| build_remote_tls(endpoint, &document.remote, pool_connections, streams))
            .collect::<Result<Vec<_>, _>>()?;
        let read_replica_tls = document
            .remote
            .read_replica_endpoints
            .iter()
            .map(|endpoint| build_remote_tls(endpoint, &document.remote, pool_connections, streams))
            .collect::<Result<Vec<_>, _>>()?;

        let mut prepared_targets = Vec::with_capacity(app_sections.len());
        for section in &app_sections {
            let database = DatabaseAlias::new(section.database.clone())
                .map_err(|_| DriverRuntimeError::InvalidConfiguration)?;
            let lock = read_checked(
                &checked_absolute(&section.lock_file)?,
                MAX_ARTIFACT_BYTES,
                false,
            )?;
            let manifest = read_checked(
                &checked_absolute(&section.manifest_file)?,
                MAX_ARTIFACT_BYTES,
                false,
            )?;
            let catalog_bytes = read_checked(
                &checked_absolute(&section.operation_catalog_file)?,
                MAX_ARTIFACT_BYTES,
                false,
            )?;
            let catalog = ApplicationCatalog::from_exact_artifacts(
                &lock,
                &manifest,
                &catalog_bytes,
                database.as_str(),
                &section.role,
            )
            .map_err(|_| DriverRuntimeError::IdentityMismatch)?;
            let raw_cred = section
                .credential_file
                .as_deref()
                .or(document.remote.credential_file.as_deref())
                .ok_or(DriverRuntimeError::InvalidConfiguration)?;
            let credential_path = checked_absolute(raw_cred)?;
            let credential = load_protected_bearer_credential(&credential_path)
                .map_err(|_| DriverRuntimeError::CredentialUnavailable)?;
            let metadata = CallMetadata::authenticated(credential).with_database(database.clone());
            prepared_targets.push((catalog, metadata, credential_path, database));
        }

        let pool =
            DriverPool::connect_verified_tls_with_topology(&tls, &standby_tls, &read_replica_tls)
                .await
                .map_err(|_| DriverRuntimeError::RemoteUnavailable)?;
        for (catalog, metadata, _, _) in &prepared_targets {
            pool.verify_remote_identity(catalog, metadata)
                .await
                .map_err(|_| DriverRuntimeError::IdentityMismatch)?;
        }
        let socket = DriverSocket::bind_with_allowed_peer_uids(
            socket_path,
            document.driver.allowed_peer_uids,
        )
        .map_err(|_| DriverRuntimeError::SocketUnavailable)?;
        let remote_identity_hash = hex(&Sha256::digest(format!(
            "{}|{}",
            document.remote.endpoint,
            tls.expected_server_identity().as_str()
        ))
        .into());
        let mut targets_iter = prepared_targets.into_iter();
        let (first_catalog, first_metadata, first_cred_path, first_db) = targets_iter
            .next()
            .ok_or(DriverRuntimeError::InvalidConfiguration)?;
        let mut host = DriverHost::new(
            first_catalog,
            pool.clone(),
            first_metadata,
            remote_identity_hash.clone(),
        )
        .and_then(|host| host.with_credential_file(first_cred_path, first_db))
        .map_err(|_| DriverRuntimeError::InvalidConfiguration)?;
        for (catalog, metadata, cred_path, db) in targets_iter {
            let target_host = DriverHost::new(
                catalog,
                pool.clone(),
                metadata,
                remote_identity_hash.clone(),
            )
            .and_then(|target| target.with_credential_file(cred_path, db))
            .map_err(|_| DriverRuntimeError::InvalidConfiguration)?;
            host = host
                .with_additional_target(target_host)
                .map_err(|_| DriverRuntimeError::InvalidConfiguration)?;
        }
        Ok(Self { host, socket })
    }

    /// Serves until the supplied shutdown future resolves, then drains local
    /// connections and removes only the owned socket.
    pub async fn serve_until<F>(self, shutdown: F) -> Result<(), DriverRuntimeError>
    where
        F: std::future::Future<Output = ()>,
    {
        self.socket
            .serve_until(self.host, shutdown)
            .await
            .map_err(|_| DriverRuntimeError::SocketUnavailable)
    }

    /// Produces a value-free diagnostic message naming the offending file path or reason.
    #[must_use]
    pub fn describe_config_error(path: &Path, error: DriverRuntimeError) -> String {
        match error {
            DriverRuntimeError::UnsafeFile => {
                if let Some(reason) = describe_unsafe_file(path, MAX_CONFIG_BYTES, true) {
                    return format!("{error} ({reason})");
                }
                if let Ok(bytes) = read_checked(path, MAX_CONFIG_BYTES, true)
                    && let Ok(text) = std::str::from_utf8(&bytes)
                    && let Ok(document) = toml::from_str::<DriverDocument>(text)
                {
                    let mut sections = Vec::new();
                    if let Some(app) = document.application {
                        sections.push(app);
                    }
                    sections.extend(document.applications);
                    for section in &sections {
                        for candidate in [
                            &section.lock_file,
                            &section.manifest_file,
                            &section.operation_catalog_file,
                        ] {
                            let candidate_path = Path::new(candidate);
                            if let Some(reason) =
                                describe_unsafe_file(candidate_path, MAX_ARTIFACT_BYTES, false)
                            {
                                return format!("{error} ({reason})");
                            }
                        }
                    }
                }
                error.to_string()
            }
            DriverRuntimeError::CredentialUnavailable => {
                if let Ok(bytes) = read_checked(path, MAX_CONFIG_BYTES, true)
                    && let Ok(text) = std::str::from_utf8(&bytes)
                    && let Ok(document) = toml::from_str::<DriverDocument>(text)
                {
                    let mut sections = Vec::new();
                    if let Some(app) = document.application {
                        sections.push(app);
                    }
                    sections.extend(document.applications);
                    for section in &sections {
                        if let Some(cred) = section
                            .credential_file
                            .as_deref()
                            .or(document.remote.credential_file.as_deref())
                        {
                            let cred_path = Path::new(cred);
                            if let Some(reason) = describe_unsafe_file(cred_path, 64 * 1024, true) {
                                return format!("{error} ({reason})");
                            }
                        }
                    }
                }
                format!(
                    "{error} (check credential_file path and ensure mode 0600 in `{}`)",
                    path.display()
                )
            }
            DriverRuntimeError::RemoteUnavailable => format!(
                "{error} (could not connect to remote endpoint or verify TLS server name from `{}`)",
                path.display()
            ),
            DriverRuntimeError::IdentityMismatch => format!(
                "{error} (local lock/manifest/operation-catalog or remote database contract/role does not match `{}`)",
                path.display()
            ),
            DriverRuntimeError::SocketUnavailable => format!(
                "{error} (could not bind Unix socket configured in `{}`; check parent directory permissions and mode 0700)",
                path.display()
            ),
            DriverRuntimeError::InvalidConfiguration => format!(
                "{error} (check TOML syntax and absolute file paths in `{}`)",
                path.display()
            ),
        }
    }
}

fn describe_unsafe_file(path: &Path, maximum: u64, private: bool) -> Option<String> {
    let link = match fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Some(format!("`{}` does not exist", path.display()));
        }
        Err(err) => {
            return Some(format!("cannot stat `{}`: {}", path.display(), err.kind()));
        }
    };
    if link.file_type().is_symlink() {
        return Some(format!(
            "`{}` is a symlink; must be a regular file",
            path.display()
        ));
    }
    if !link.is_file() {
        return Some(format!("`{}` is not a regular file", path.display()));
    }
    if link.len() == 0 {
        return Some(format!("`{}` is empty", path.display()));
    }
    if link.len() > maximum {
        return Some(format!(
            "`{}` exceeds maximum size ({maximum} bytes)",
            path.display()
        ));
    }
    let mode = link.permissions().mode() & 0o777;
    if private && (mode & 0o077 != 0) {
        return Some(format!(
            "`{}` has permissions {:04o}; protected files require mode 0600",
            path.display(),
            mode
        ));
    }
    if mode & 0o022 != 0 {
        return Some(format!(
            "`{}` has permissions {:04o}; group/world-writable files are rejected",
            path.display(),
            mode
        ));
    }
    None
}

fn build_remote_tls(
    raw_endpoint: &str,
    remote: &RemoteSection,
    pool_connections: NonZeroU32,
    streams: NonZeroU32,
) -> Result<TlsClientConfig, DriverRuntimeError> {
    let endpoint = CanonicalHttpsEndpoint::parse(raw_endpoint)
        .map_err(|_| DriverRuntimeError::InvalidConfiguration)?;
    let trust_root = match remote.tls_trust_root.as_deref() {
        Some(path) => ClientTrustRootConfig::ExplicitPemFile(
            ProtectedFilePath::new(checked_absolute(path)?)
                .map_err(|_| DriverRuntimeError::InvalidConfiguration)?,
        ),
        None => ClientTrustRootConfig::SystemCaBundle,
    };
    let identity = match remote.tls_server_name.as_deref() {
        Some(name) if raw_endpoint == remote.endpoint => {
            TlsServerIdentity::parse(name).map_err(|_| DriverRuntimeError::InvalidConfiguration)?
        }
        Some(_) | None => endpoint.identity().clone(),
    };
    TlsClientConfig::with_trust_root_config(
        endpoint,
        trust_root,
        identity,
        Duration::from_secs(5),
        Duration::from_secs(30),
        pool_connections,
        streams,
    )
    .map_err(|_| DriverRuntimeError::InvalidConfiguration)
}

fn hex(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(64);
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0xf)]));
    }
    value
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DriverDocument {
    driver: DriverSection,
    #[serde(default)]
    application: Option<ApplicationSection>,
    #[serde(default)]
    applications: Vec<ApplicationSection>,
    remote: RemoteSection,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DriverSection {
    socket: String,
    #[serde(default)]
    allowed_peer_uids: Vec<u32>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ApplicationSection {
    lock_file: String,
    manifest_file: String,
    operation_catalog_file: String,
    database: String,
    role: String,
    #[serde(default)]
    credential_file: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteSection {
    endpoint: String,
    #[serde(default)]
    standby_endpoints: Vec<String>,
    #[serde(default)]
    read_replica_endpoints: Vec<String>,
    tls_trust_root: Option<String>,
    tls_server_name: Option<String>,
    #[serde(default)]
    credential_file: Option<String>,
    pool_connections: u32,
    streams_per_connection: u32,
}

/// Closed path/configuration setup failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DriverRuntimeError {
    /// Static configuration is absent, unknown, out of bounds, or inconsistent.
    InvalidConfiguration,
    /// A configured local file is mutable by other users, replaced, or otherwise unsafe.
    UnsafeFile,
    /// Exact application artifacts disagree.
    IdentityMismatch,
    /// The protected application credential cannot be loaded.
    CredentialUnavailable,
    /// The verified remote pool cannot be established.
    RemoteUnavailable,
    /// The protected local socket cannot be owned or served.
    SocketUnavailable,
}
impl fmt::Display for DriverRuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidConfiguration => "driver configuration is invalid",
            Self::UnsafeFile => "driver configuration file is unsafe",
            Self::IdentityMismatch => "driver application identities do not match",
            Self::CredentialUnavailable => "driver application credential is unavailable",
            Self::RemoteUnavailable => "driver remote endpoint is unavailable",
            Self::SocketUnavailable => "driver local socket is unavailable",
        })
    }
}
impl std::error::Error for DriverRuntimeError {}

fn checked_absolute(value: &str) -> Result<PathBuf, DriverRuntimeError> {
    let path = PathBuf::from(value);
    if !path.is_absolute()
        || path.as_os_str().as_encoded_bytes().len() > 4_096
        || !path
            .components()
            .all(|component| matches!(component, Component::RootDir | Component::Normal(_)))
    {
        return Err(DriverRuntimeError::InvalidConfiguration);
    }
    Ok(path)
}

fn read_checked(path: &Path, maximum: u64, private: bool) -> Result<Vec<u8>, DriverRuntimeError> {
    let link = fs::symlink_metadata(path).map_err(|_| DriverRuntimeError::UnsafeFile)?;
    if !link.is_file()
        || link.file_type().is_symlink()
        || link.len() == 0
        || link.len() > maximum
        || link.permissions().mode() & 0o022 != 0
        || (private && link.permissions().mode() & 0o077 != 0)
    {
        return Err(DriverRuntimeError::UnsafeFile);
    }
    let file = fs::File::open(path).map_err(|_| DriverRuntimeError::UnsafeFile)?;
    let opened = file
        .metadata()
        .map_err(|_| DriverRuntimeError::UnsafeFile)?;
    if opened.dev() != link.dev() || opened.ino() != link.ino() || opened.len() != link.len() {
        return Err(DriverRuntimeError::UnsafeFile);
    }
    let mut bytes = Vec::with_capacity(
        usize::try_from(opened.len()).map_err(|_| DriverRuntimeError::UnsafeFile)?,
    );
    file.take(maximum + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| DriverRuntimeError::UnsafeFile)?;
    let after = fs::symlink_metadata(path).map_err(|_| DriverRuntimeError::UnsafeFile)?;
    if bytes.is_empty()
        || u64::try_from(bytes.len())
            .ok()
            .is_none_or(|length| length > maximum)
        || after.dev() != opened.dev()
        || after.ino() != opened.ino()
        || after.len() != opened.len()
    {
        return Err(DriverRuntimeError::UnsafeFile);
    }
    Ok(bytes)
}
