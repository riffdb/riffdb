//! Verified direct-TLS channel construction from the closed deployment config.

use std::error::Error;
use std::fmt;
use std::fs;
use std::io::Read;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};

use riffdb_config::TlsClientConfig;
use tonic::transport::{Certificate, ClientTlsConfig as TonicTlsConfig, Endpoint};

use crate::{ClientError, RiffDbClient};

const MAX_TRUST_ROOT_BYTES: u64 = 256 * 1_024;

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

impl RiffDbClient {
    /// Connects through mandatory CA and exact server-name verification.
    ///
    /// This path has no trust-all, native-root, cleartext fallback, redirect,
    /// client-certificate, cipher-suite, or provider selector.
    pub async fn connect_verified_tls(config: &TlsClientConfig) -> Result<Self, ClientError> {
        let trust_root = read_trust_root(config)?;
        let tls = TonicTlsConfig::new()
            .ca_certificate(Certificate::from_pem(trust_root))
            .domain_name(config.expected_server_identity().as_str());
        let endpoint = Endpoint::from_shared(config.endpoint().as_str().to_owned())
            .map_err(|_| ClientError::Tls(TlsClientFailure::InvalidConfiguration))?
            .connect_timeout(config.connect_timeout())
            .http2_keep_alive_interval(config.keepalive_interval())
            .keep_alive_while_idle(true)
            .tls_config(tls)
            .map_err(|_| ClientError::Tls(TlsClientFailure::InvalidConfiguration))?;
        let channel = endpoint
            .connect()
            .await
            .map_err(|_| ClientError::Tls(TlsClientFailure::ConnectionOrPeerVerification))?;
        Ok(Self::from_channel(channel))
    }
}

fn read_trust_root(config: &TlsClientConfig) -> Result<Vec<u8>, ClientError> {
    let path = config.trust_root().as_path();
    let linked = fs::symlink_metadata(path)
        .map_err(|_| ClientError::Tls(TlsClientFailure::TrustRootUnavailable))?;
    if !linked.file_type().is_file() {
        return Err(ClientError::Tls(TlsClientFailure::TrustRootUnavailable));
    }
    #[cfg(unix)]
    if linked.permissions().mode() & 0o022 != 0 {
        return Err(ClientError::Tls(TlsClientFailure::TrustRootPermissions));
    }
    let file = fs::File::open(path)
        .map_err(|_| ClientError::Tls(TlsClientFailure::TrustRootUnavailable))?;
    let opened = file
        .metadata()
        .map_err(|_| ClientError::Tls(TlsClientFailure::TrustRootUnavailable))?;
    if !opened.is_file()
        || opened.len() == 0
        || opened.len() > MAX_TRUST_ROOT_BYTES
        || !same_file(&linked, &opened)
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
    Ok(contents)
}

#[cfg(unix)]
fn same_file(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(not(unix))]
fn same_file(_left: &fs::Metadata, _right: &fs::Metadata) -> bool {
    true
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    #[cfg(unix)]
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::path::PathBuf;
    use std::time::Duration;

    use riffdb_config::{CanonicalHttpsEndpoint, ProtectedFilePath, TlsServerIdentity};

    use super::*;

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
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("test clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "riffdb-client-tls-test-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir(&root).expect("create TLS test root");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).expect("protect root");
        let trust = root.join("ca.pem");
        fs::write(&trust, b"not-empty").expect("write trust root");
        fs::set_permissions(&trust, fs::Permissions::from_mode(0o666)).expect("unsafe mode");
        let error = read_trust_root(&config(trust.clone())).expect_err("writable trust root");
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
            read_trust_root(&config(link)),
            Err(ClientError::Tls(TlsClientFailure::TrustRootUnavailable))
        ));

        fs::remove_file(&trust).expect("remove trust root");
        fs::remove_file(root.join("linked.pem")).expect("remove symlink");
        fs::remove_dir(&root).expect("remove TLS test root");
    }
}
