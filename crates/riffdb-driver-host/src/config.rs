use std::fmt;
use std::fs;
use std::io::Read as _;
use std::num::NonZeroU32;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use riffdb_client_rust::{CallMetadata, load_protected_bearer_credential};
use riffdb_config::{
    CanonicalHttpsEndpoint, ProtectedFilePath, TlsClientConfig, TlsServerIdentity,
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
        let database = DatabaseAlias::new(document.application.database.clone())
            .map_err(|_| DriverRuntimeError::InvalidConfiguration)?;
        let endpoint = CanonicalHttpsEndpoint::parse(&document.remote.endpoint)
            .map_err(|_| DriverRuntimeError::InvalidConfiguration)?;
        let trust_root = ProtectedFilePath::new(checked_absolute(&document.remote.tls_trust_root)?)
            .map_err(|_| DriverRuntimeError::InvalidConfiguration)?;
        let identity = TlsServerIdentity::parse(&document.remote.tls_server_name)
            .map_err(|_| DriverRuntimeError::InvalidConfiguration)?;
        let pool_connections = NonZeroU32::new(document.remote.pool_connections)
            .filter(|value| value.get() <= 16)
            .ok_or(DriverRuntimeError::InvalidConfiguration)?;
        let streams = NonZeroU32::new(document.remote.streams_per_connection)
            .filter(|value| value.get() <= 256)
            .ok_or(DriverRuntimeError::InvalidConfiguration)?;
        let tls = TlsClientConfig::new(
            endpoint,
            trust_root,
            identity,
            Duration::from_secs(5),
            Duration::from_secs(30),
            pool_connections,
            streams,
        )
        .map_err(|_| DriverRuntimeError::InvalidConfiguration)?;

        let lock = read_checked(
            &checked_absolute(&document.application.lock_file)?,
            MAX_ARTIFACT_BYTES,
            false,
        )?;
        let manifest = read_checked(
            &checked_absolute(&document.application.manifest_file)?,
            MAX_ARTIFACT_BYTES,
            false,
        )?;
        let catalog_bytes = read_checked(
            &checked_absolute(&document.application.operation_catalog_file)?,
            MAX_ARTIFACT_BYTES,
            false,
        )?;
        let catalog = ApplicationCatalog::from_exact_artifacts(
            &lock,
            &manifest,
            &catalog_bytes,
            database.as_str(),
            &document.application.role,
        )
        .map_err(|_| DriverRuntimeError::IdentityMismatch)?;
        let credential_path = checked_absolute(&document.remote.credential_file)?;
        let credential = load_protected_bearer_credential(&credential_path)
            .map_err(|_| DriverRuntimeError::CredentialUnavailable)?;
        let metadata = CallMetadata::authenticated(credential).with_database(database);
        let pool = DriverPool::connect_verified_tls(&tls)
            .await
            .map_err(|_| DriverRuntimeError::RemoteUnavailable)?;
        pool.verify_remote_identity(&catalog, &metadata)
            .await
            .map_err(|_| DriverRuntimeError::IdentityMismatch)?;
        let socket = DriverSocket::bind(checked_absolute(&document.driver.socket)?)
            .map_err(|_| DriverRuntimeError::SocketUnavailable)?;
        let remote_identity_hash = hex(&Sha256::digest(format!(
            "{}|{}",
            document.remote.endpoint, document.remote.tls_server_name
        ))
        .into());
        Ok(Self {
            host: DriverHost::new(catalog, pool, metadata, remote_identity_hash)
                .map_err(|_| DriverRuntimeError::InvalidConfiguration)?,
            socket,
        })
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
    application: ApplicationSection,
    remote: RemoteSection,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DriverSection {
    socket: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ApplicationSection {
    lock_file: String,
    manifest_file: String,
    operation_catalog_file: String,
    database: String,
    role: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteSection {
    endpoint: String,
    tls_trust_root: String,
    tls_server_name: String,
    credential_file: String,
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
