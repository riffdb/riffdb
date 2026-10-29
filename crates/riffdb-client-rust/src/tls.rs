//! Verified direct-TLS channel construction from the closed deployment config.

use std::error::Error;
use std::fmt;
use std::fs;
use std::io::Read;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;

use riffdb_config::{ClientTrustRootConfig, TlsClientConfig};
use rustls_pki_types::{CertificateDer, pem::PemObject};
use tonic::transport::{Certificate, ClientTlsConfig as TonicTlsConfig, Endpoint};

use crate::{ClientError, RiffDbClient};

const MAX_TRUST_ROOT_BYTES: u64 = 1_024 * 1_024;
const MAX_TRUST_ROOT_CERTIFICATES: usize = 512;
const DEFAULT_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const SYSTEM_CA_BUNDLE_CANDIDATES: &[&str] = &[
    "/etc/ssl/certs/ca-certificates.crt",
    "/etc/pki/tls/certs/ca-bundle.crt",
    "/etc/ssl/cert.pem",
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
    "/etc/ssl/ca-bundle.pem",
];

#[derive(Clone, Debug, Eq, PartialEq)]
struct TrustRootFileIdentity {
    bytes: u64,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    modified_seconds: i64,
    #[cfg(unix)]
    modified_nanoseconds: i64,
    #[cfg(not(unix))]
    modified_since_epoch: Option<std::time::Duration>,
}

struct TrustRootSnapshot {
    bytes: Vec<u8>,
    identity: TrustRootFileIdentity,
}

impl fmt::Debug for TrustRootSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TrustRootSnapshot")
            .field("bytes", &"[TRUST ROOT]")
            .field("identity", &self.identity)
            .finish()
    }
}

/// Closed, path-redacted failure while constructing a verified TLS channel.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TlsClientFailure {
    /// The configured trust-root file was absent, replaced, empty, or oversized.
    TrustRootUnavailable,
    /// The trust-root file was writable by a group or other users.
    TrustRootPermissions,
    /// The exact HTTPS endpoint or TLS verifier could not be constructed.
    InvalidConfiguration,
    /// The peer could not be reached or did not pass the mandatory TLS handshake.
    ConnectionOrPeerVerification,
}

impl fmt::Display for TlsClientFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::TrustRootUnavailable => "the configured TLS trust root is unavailable",
            Self::TrustRootPermissions => "the configured TLS trust root has unsafe permissions",
            Self::InvalidConfiguration => "the verified TLS endpoint is invalid",
            Self::ConnectionOrPeerVerification => "the TLS peer could not be reached or verified",
        })
    }
}

impl Error for TlsClientFailure {}

/// Redacted disposition of the latest trust-root observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TlsTrustReloadStatus {
    /// The configured file and active snapshot are the same valid identity.
    Current,
    /// The configured file was unavailable or unsafe, so the prior snapshot remains active.
    RetainedPreviousUnavailable,
    /// A complete replacement was malformed, so the prior snapshot remains active.
    RetainedPreviousInvalid,
}

/// Stateful verified-TLS connector with atomic trust-root replacement.
///
/// A complete valid replacement is used for subsequent connections. An
/// unavailable, malformed, or unsafe replacement retains the last valid
/// snapshot. Existing channels are unaffected and never downgrade.
pub struct VerifiedTlsConnector {
    config: TlsClientConfig,
    current: TrustRootSnapshot,
    rejected: Option<TrustRootFileIdentity>,
    last_reload: TlsTrustReloadStatus,
}

impl VerifiedTlsConnector {
    /// Loads and validates the initial trust-root snapshot.
    pub fn new(config: TlsClientConfig) -> Result<Self, ClientError> {
        let current = read_trust_root_snapshot(&config)?;
        Ok(Self {
            config,
            current,
            rejected: None,
            last_reload: TlsTrustReloadStatus::Current,
        })
    }

    /// Connects one reusable HTTP/2 channel using the current valid snapshot.
    pub async fn connect(&mut self) -> Result<RiffDbClient, ClientError> {
        self.reload_trust_root_for_new_handshake();
        connect_with_trust_root(&self.config, self.current.bytes.clone()).await
    }

    /// Returns a path-free disposition suitable for operator diagnostics.
    #[must_use]
    pub const fn last_reload_status(&self) -> TlsTrustReloadStatus {
        self.last_reload
    }

    fn reload_trust_root_for_new_handshake(&mut self) {
        let observed = match self.config.trust_root_config() {
            ClientTrustRootConfig::ExplicitPemFile(path) => {
                trust_root_file_identity(path.as_path())
            }
            ClientTrustRootConfig::SystemCaBundle => {
                resolve_system_ca_bundle_path().and_then(|path| system_ca_bundle_identity(&path))
            }
        };
        let Ok(observed) = observed else {
            self.last_reload = TlsTrustReloadStatus::RetainedPreviousUnavailable;
            return;
        };
        if observed == self.current.identity || self.rejected.as_ref() == Some(&observed) {
            if observed == self.current.identity {
                self.last_reload = TlsTrustReloadStatus::Current;
            }
            return;
        }
        match read_trust_root_snapshot(&self.config) {
            Ok(snapshot) => {
                self.current = snapshot;
                self.rejected = None;
                self.last_reload = TlsTrustReloadStatus::Current;
            }
            Err(_) => {
                self.rejected = Some(observed);
                self.last_reload = TlsTrustReloadStatus::RetainedPreviousInvalid;
            }
        }
    }
}

impl RiffDbClient {
    /// Connects through mandatory CA and exact server-name verification.
    ///
    /// This path has no trust-all, cleartext fallback, redirect,
    /// client-certificate, cipher-suite, or provider selector.
    pub async fn connect_verified_tls(config: &TlsClientConfig) -> Result<Self, ClientError> {
        VerifiedTlsConnector::new(config.clone())?.connect().await
    }
}

async fn connect_with_trust_root(
    config: &TlsClientConfig,
    trust_root: Vec<u8>,
) -> Result<RiffDbClient, ClientError> {
    let tls = TonicTlsConfig::new()
        .ca_certificate(Certificate::from_pem(trust_root))
        .domain_name(config.expected_server_identity().as_str());
    let endpoint = Endpoint::from_shared(config.endpoint().as_str().to_owned())
        .map_err(|_| ClientError::Tls(TlsClientFailure::InvalidConfiguration))?
        .connect_timeout(config.connect_timeout())
        .timeout(DEFAULT_REQUEST_TIMEOUT)
        .http2_keep_alive_interval(config.keepalive_interval())
        .keep_alive_while_idle(true)
        .tls_config(tls)
        .map_err(|_| ClientError::Tls(TlsClientFailure::InvalidConfiguration))?;
    let channel = endpoint
        .connect()
        .await
        .map_err(|_| ClientError::Tls(TlsClientFailure::ConnectionOrPeerVerification))?;
    Ok(RiffDbClient::from_channel(channel))
}

fn read_trust_root_snapshot(config: &TlsClientConfig) -> Result<TrustRootSnapshot, ClientError> {
    match config.trust_root_config() {
        ClientTrustRootConfig::ExplicitPemFile(path) => {
            read_explicit_trust_root_snapshot(path.as_path())
        }
        ClientTrustRootConfig::SystemCaBundle => read_system_ca_bundle_snapshot(),
    }
}

fn read_explicit_trust_root_snapshot(path: &Path) -> Result<TrustRootSnapshot, ClientError> {
    let identity = trust_root_file_identity(path)?;
    let file = fs::File::open(path)
        .map_err(|_| ClientError::Tls(TlsClientFailure::TrustRootUnavailable))?;
    let opened = file
        .metadata()
        .map_err(|_| ClientError::Tls(TlsClientFailure::TrustRootUnavailable))?;
    if !opened.is_file()
        || opened.len() == 0
        || opened.len() > MAX_TRUST_ROOT_BYTES
        || !opened_file_matches_identity(&identity, &opened)
    {
        return Err(ClientError::Tls(TlsClientFailure::TrustRootUnavailable));
    }
    let mut contents = Vec::with_capacity(opened.len() as usize);
    file.take(MAX_TRUST_ROOT_BYTES + 1)
        .read_to_end(&mut contents)
        .map_err(|_| ClientError::Tls(TlsClientFailure::TrustRootUnavailable))?;
    if contents.is_empty() || contents.len() as u64 > MAX_TRUST_ROOT_BYTES {
        return Err(ClientError::Tls(TlsClientFailure::TrustRootUnavailable));
    }
    let after = trust_root_file_identity(path)?;
    if after != identity {
        return Err(ClientError::Tls(TlsClientFailure::TrustRootUnavailable));
    }
    validate_trust_root(&contents)?;
    Ok(TrustRootSnapshot {
        bytes: contents,
        identity,
    })
}

fn resolve_system_ca_bundle_path() -> Result<std::path::PathBuf, ClientError> {
    for candidate in SYSTEM_CA_BUNDLE_CANDIDATES {
        let path = Path::new(candidate);
        if fs::metadata(path).is_ok_and(|meta| meta.is_file() && meta.len() > 0) {
            return Ok(path.to_path_buf());
        }
    }
    Err(ClientError::Tls(TlsClientFailure::TrustRootUnavailable))
}

fn read_system_ca_bundle_snapshot() -> Result<TrustRootSnapshot, ClientError> {
    let path = resolve_system_ca_bundle_path()?;
    read_system_ca_bundle_from_path(&path)
}

fn read_system_ca_bundle_from_path(path: &Path) -> Result<TrustRootSnapshot, ClientError> {
    let identity = system_ca_bundle_identity(path)?;
    let file = fs::File::open(path)
        .map_err(|_| ClientError::Tls(TlsClientFailure::TrustRootUnavailable))?;
    let opened = file
        .metadata()
        .map_err(|_| ClientError::Tls(TlsClientFailure::TrustRootUnavailable))?;
    if !opened.is_file()
        || opened.len() == 0
        || opened.len() > MAX_TRUST_ROOT_BYTES
        || !opened_file_matches_identity(&identity, &opened)
    {
        return Err(ClientError::Tls(TlsClientFailure::TrustRootUnavailable));
    }
    let mut contents = Vec::with_capacity(opened.len() as usize);
    file.take(MAX_TRUST_ROOT_BYTES + 1)
        .read_to_end(&mut contents)
        .map_err(|_| ClientError::Tls(TlsClientFailure::TrustRootUnavailable))?;
    if contents.is_empty() || contents.len() as u64 > MAX_TRUST_ROOT_BYTES {
        return Err(ClientError::Tls(TlsClientFailure::TrustRootUnavailable));
    }
    let after = system_ca_bundle_identity(path)?;
    if after != identity {
        return Err(ClientError::Tls(TlsClientFailure::TrustRootUnavailable));
    }
    validate_system_ca_bundle(&contents)?;
    Ok(TrustRootSnapshot {
        bytes: contents,
        identity,
    })
}

#[cfg(unix)]
fn system_ca_bundle_identity(path: &Path) -> Result<TrustRootFileIdentity, ClientError> {
    let metadata =
        fs::metadata(path).map_err(|_| ClientError::Tls(TlsClientFailure::TrustRootUnavailable))?;
    if !metadata.is_file() || metadata.len() == 0 {
        return Err(ClientError::Tls(TlsClientFailure::TrustRootUnavailable));
    }
    if metadata.permissions().mode() & 0o022 != 0 {
        return Err(ClientError::Tls(TlsClientFailure::TrustRootPermissions));
    }
    Ok(TrustRootFileIdentity {
        bytes: metadata.len(),
        device: metadata.dev(),
        inode: metadata.ino(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
    })
}

#[cfg(not(unix))]
fn system_ca_bundle_identity(path: &Path) -> Result<TrustRootFileIdentity, ClientError> {
    let metadata =
        fs::metadata(path).map_err(|_| ClientError::Tls(TlsClientFailure::TrustRootUnavailable))?;
    if !metadata.is_file() || metadata.len() == 0 {
        return Err(ClientError::Tls(TlsClientFailure::TrustRootUnavailable));
    }
    Ok(TrustRootFileIdentity {
        bytes: metadata.len(),
        modified_since_epoch: metadata
            .modified()
            .ok()
            .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok()),
    })
}

#[cfg(unix)]
fn trust_root_file_identity(path: &Path) -> Result<TrustRootFileIdentity, ClientError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| ClientError::Tls(TlsClientFailure::TrustRootUnavailable))?;
    if !metadata.file_type().is_file() || metadata.len() == 0 {
        return Err(ClientError::Tls(TlsClientFailure::TrustRootUnavailable));
    }
    if metadata.permissions().mode() & 0o022 != 0 {
        return Err(ClientError::Tls(TlsClientFailure::TrustRootPermissions));
    }
    Ok(TrustRootFileIdentity {
        bytes: metadata.len(),
        device: metadata.dev(),
        inode: metadata.ino(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
    })
}

#[cfg(not(unix))]
fn trust_root_file_identity(path: &Path) -> Result<TrustRootFileIdentity, ClientError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| ClientError::Tls(TlsClientFailure::TrustRootUnavailable))?;
    if !metadata.file_type().is_file() || metadata.len() == 0 {
        return Err(ClientError::Tls(TlsClientFailure::TrustRootUnavailable));
    }
    Ok(TrustRootFileIdentity {
        bytes: metadata.len(),
        modified_since_epoch: metadata
            .modified()
            .ok()
            .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok()),
    })
}

#[cfg(unix)]
fn opened_file_matches_identity(identity: &TrustRootFileIdentity, opened: &fs::Metadata) -> bool {
    identity.device == opened.dev()
        && identity.inode == opened.ino()
        && identity.bytes == opened.len()
        && identity.modified_seconds == opened.mtime()
        && identity.modified_nanoseconds == opened.mtime_nsec()
}

#[cfg(not(unix))]
fn opened_file_matches_identity(identity: &TrustRootFileIdentity, opened: &fs::Metadata) -> bool {
    identity.bytes == opened.len()
        && identity.modified_since_epoch
            == opened
                .modified()
                .ok()
                .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
}

fn validate_trust_root(contents: &[u8]) -> Result<(), ClientError> {
    let mut reader = std::io::Cursor::new(contents);
    let mut count = 0_usize;
    for certificate in CertificateDer::pem_reader_iter(&mut reader) {
        let certificate =
            certificate.map_err(|_| ClientError::Tls(TlsClientFailure::InvalidConfiguration))?;
        count = count
            .checked_add(1)
            .ok_or(ClientError::Tls(TlsClientFailure::InvalidConfiguration))?;
        if count > MAX_TRUST_ROOT_CERTIFICATES
            || webpki::anchor_from_trusted_cert(&certificate).is_err()
        {
            return Err(ClientError::Tls(TlsClientFailure::InvalidConfiguration));
        }
    }
    if count == 0 {
        return Err(ClientError::Tls(TlsClientFailure::InvalidConfiguration));
    }
    Ok(())
}

fn validate_system_ca_bundle(contents: &[u8]) -> Result<(), ClientError> {
    let mut reader = std::io::Cursor::new(contents);
    let mut count = 0_usize;
    let mut valid_anchors = 0_usize;
    for certificate in CertificateDer::pem_reader_iter(&mut reader) {
        let certificate =
            certificate.map_err(|_| ClientError::Tls(TlsClientFailure::InvalidConfiguration))?;
        count = count
            .checked_add(1)
            .ok_or(ClientError::Tls(TlsClientFailure::InvalidConfiguration))?;
        if count > MAX_TRUST_ROOT_CERTIFICATES {
            return Err(ClientError::Tls(TlsClientFailure::InvalidConfiguration));
        }
        if webpki::anchor_from_trusted_cert(&certificate).is_ok() {
            valid_anchors += 1;
        }
    }
    if valid_anchors == 0 {
        return Err(ClientError::Tls(TlsClientFailure::InvalidConfiguration));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    #[cfg(unix)]
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use riffdb_config::{CanonicalHttpsEndpoint, ProtectedFilePath, TlsServerIdentity};

    use super::*;

    static NEXT_TEST_ROOT: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn verified_tls_channels_have_a_bounded_request_lifetime() {
        assert_eq!(DEFAULT_REQUEST_TIMEOUT, Duration::from_secs(30));
        let source = include_str!("tls.rs");
        assert!(source.contains(".timeout(DEFAULT_REQUEST_TIMEOUT)"));
    }

    fn config(path: PathBuf) -> TlsClientConfig {
        TlsClientConfig::new(
            CanonicalHttpsEndpoint::parse("https://127.0.0.1:7443").expect("endpoint"),
            ProtectedFilePath::new(path).expect("trust path"),
            TlsServerIdentity::parse("127.0.0.1").expect("identity"),
            Duration::from_secs(5),
            Duration::from_secs(30),
            NonZeroU32::new(1).expect("pool"),
            NonZeroU32::new(64).expect("streams"),
        )
        .expect("TLS client config")
    }

    #[cfg(unix)]
    #[test]
    fn trust_root_rejects_writable_and_symlinked_files_without_disclosing_paths() {
        let unique = NEXT_TEST_ROOT.fetch_add(1, Ordering::Relaxed);
        let root = std::env::current_dir()
            .expect("test current directory")
            .join("target/client-tls-tests")
            .join(format!("{}-{unique}", std::process::id()));
        fs::create_dir_all(root.parent().expect("test root parent"))
            .expect("create TLS test parent");
        fs::create_dir(&root).expect("create TLS test root");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).expect("protect root");
        let trust = root.join("ca.pem");
        fs::write(&trust, b"not-empty").expect("write trust root");
        fs::set_permissions(&trust, fs::Permissions::from_mode(0o666)).expect("unsafe mode");
        let error =
            read_trust_root_snapshot(&config(trust.clone())).expect_err("writable trust root");
        assert!(matches!(
            error,
            ClientError::Tls(TlsClientFailure::TrustRootPermissions)
        ));
        assert!(
            !error
                .to_string()
                .contains(root.to_str().expect("UTF-8 root"))
        );

        fs::set_permissions(&trust, fs::Permissions::from_mode(0o600)).expect("safe mode");
        let link = root.join("linked.pem");
        symlink(&trust, &link).expect("create test symlink");
        assert!(matches!(
            read_trust_root_snapshot(&config(link)),
            Err(ClientError::Tls(TlsClientFailure::TrustRootUnavailable))
        ));

        fs::remove_file(&trust).expect("remove trust root");
        fs::remove_file(root.join("linked.pem")).expect("remove symlink");
        fs::remove_dir(&root).expect("remove TLS test root");
    }

    #[cfg(unix)]
    #[test]
    fn connector_atomically_adopts_valid_trust_roots_and_retains_last_good_on_failure() {
        let unique = NEXT_TEST_ROOT.fetch_add(1, Ordering::Relaxed);
        let root = std::env::current_dir()
            .expect("test current directory")
            .join("target/client-tls-tests")
            .join(format!("{}-{unique}", std::process::id()));
        fs::create_dir_all(root.parent().expect("test root parent"))
            .expect("create TLS test parent");
        fs::create_dir(&root).expect("create TLS test root");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).expect("protect root");
        let trust = root.join("ca.pem");
        let valid = include_bytes!("../../riffdb-server/tests/fixtures/test-ca.pem");
        fs::write(&trust, valid).expect("write initial trust root");
        fs::set_permissions(&trust, fs::Permissions::from_mode(0o600))
            .expect("protect initial trust root");
        let mut connector =
            VerifiedTlsConnector::new(config(trust.clone())).expect("initial trust root is valid");
        let initial = connector.current.identity.clone();

        let invalid = root.join("invalid.pem");
        fs::write(&invalid, b"not a certificate\n").expect("write invalid replacement");
        fs::set_permissions(&invalid, fs::Permissions::from_mode(0o600))
            .expect("protect invalid replacement");
        fs::rename(&invalid, &trust).expect("atomically publish invalid replacement");
        connector.reload_trust_root_for_new_handshake();
        assert_eq!(connector.current.identity, initial);
        assert!(connector.rejected.is_some());
        assert_eq!(
            connector.last_reload_status(),
            TlsTrustReloadStatus::RetainedPreviousInvalid
        );

        let replacement = root.join("replacement.pem");
        fs::write(&replacement, valid).expect("write valid replacement");
        fs::set_permissions(&replacement, fs::Permissions::from_mode(0o600))
            .expect("protect valid replacement");
        fs::rename(&replacement, &trust).expect("atomically publish valid replacement");
        connector.reload_trust_root_for_new_handshake();
        assert_ne!(connector.current.identity, initial);
        assert!(connector.rejected.is_none());
        assert_eq!(
            connector.last_reload_status(),
            TlsTrustReloadStatus::Current
        );

        fs::remove_file(&trust).expect("remove trust root");
        fs::remove_dir(&root).expect("remove TLS test root");
    }

    #[cfg(unix)]
    #[test]
    // req: NET-001, NET-002, NET-005
    fn remote_https_profile_derives_server_name_and_supports_system_and_explicit_trust_roots() {
        let endpoint =
            CanonicalHttpsEndpoint::parse("https://cloud.example.com:7443").expect("endpoint");
        let system_cfg = TlsClientConfig::system_trust(
            endpoint.clone(),
            Duration::from_secs(5),
            Duration::from_secs(30),
            NonZeroU32::new(4).expect("pool"),
            NonZeroU32::new(64).expect("streams"),
        )
        .expect("system trust config");
        assert_eq!(
            system_cfg.expected_server_identity(),
            &TlsServerIdentity::parse("cloud.example.com").expect("identity")
        );
        assert_eq!(
            system_cfg.trust_root_config(),
            &ClientTrustRootConfig::SystemCaBundle
        );
        assert!(system_cfg.explicit_trust_root().is_none());

        // Verify system CA bundle snapshot loads when standard OS bundle is present on host.
        if resolve_system_ca_bundle_path().is_ok() {
            let connector = VerifiedTlsConnector::new(system_cfg)
                .expect("system CA bundle loads and validates");
            assert_eq!(
                connector.last_reload_status(),
                TlsTrustReloadStatus::Current
            );
        }

        // Verify explicit PEM trust root with derived server identity also works.
        let unique = NEXT_TEST_ROOT.fetch_add(1, Ordering::Relaxed);
        let root = std::env::current_dir()
            .expect("test current directory")
            .join("target/client-tls-tests")
            .join(format!("{}-system-{unique}", std::process::id()));
        fs::create_dir_all(root.parent().expect("test root parent"))
            .expect("create TLS test parent");
        fs::create_dir(&root).expect("create TLS test root");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).expect("protect root");
        let trust = root.join("ca.pem");
        let valid = include_bytes!("../../riffdb-server/tests/fixtures/test-ca.pem");
        fs::write(&trust, valid).expect("write initial trust root");
        fs::set_permissions(&trust, fs::Permissions::from_mode(0o600))
            .expect("protect initial trust root");
        let explicit_cfg = TlsClientConfig::with_trust_root_config(
            endpoint.clone(),
            ClientTrustRootConfig::ExplicitPemFile(
                ProtectedFilePath::new(trust.clone()).expect("protected path"),
            ),
            endpoint.identity().clone(),
            Duration::from_secs(5),
            Duration::from_secs(30),
            NonZeroU32::new(4).expect("pool"),
            NonZeroU32::new(64).expect("streams"),
        )
        .expect("explicit trust config");
        assert!(explicit_cfg.explicit_trust_root().is_some());
        let connector = VerifiedTlsConnector::new(explicit_cfg)
            .expect("explicit CA bundle loads and validates");
        assert_eq!(
            connector.last_reload_status(),
            TlsTrustReloadStatus::Current
        );

        fs::remove_file(&trust).expect("remove trust root");
        fs::remove_dir(&root).expect("remove TLS test root");
    }
}
