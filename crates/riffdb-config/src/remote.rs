use std::error::Error;
use std::fmt;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::num::{NonZeroU16, NonZeroU32};
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

/// Maximum bytes in one canonical public HTTPS endpoint.
pub const MAX_HTTPS_ENDPOINT_BYTES: usize = 2_048;
/// Maximum bytes in one configured DNS identity.
pub const MAX_DNS_NAME_BYTES: usize = 253;
/// Maximum bytes in one protected configuration path.
pub const MAX_PROTECTED_PATH_BYTES: usize = 4_096;
/// Conservative Linux `sockaddr_un` pathname ceiling including no terminator.
pub const MAX_LOCAL_SOCKET_PATH_BYTES: usize = 100;
/// Maximum accepted listener connections retained by one process.
pub const MAX_LISTENER_CONNECTIONS: u32 = 4_096;
/// Maximum concurrent HTTP/2 streams admitted on one connection.
pub const MAX_HTTP2_STREAMS: u32 = 1_024;
/// Maximum client-side connections in one endpoint pool.
pub const MAX_CLIENT_POOL_CONNECTIONS: u32 = 64;

/// Closed deployment-configuration rejection without supplied values or paths.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoteConfigError {
    /// A cleartext listener was not literal loopback.
    NonLoopbackCleartext,
    /// A remote endpoint was not one canonical HTTPS authority.
    InvalidHttpsEndpoint,
    /// A TLS server identity was not one bounded DNS name or literal IP.
    InvalidServerIdentity,
    /// A protected path was empty, relative, non-normal, root, or too long.
    InvalidProtectedPath,
    /// Two protected inputs named the same path.
    OverlappingProtectedPaths,
    /// A local-socket path was not one bounded absolute normal path.
    InvalidLocalSocketPath,
    /// One listener or pool bound was zero or outside the closed maximum.
    InvalidBound,
    /// A direct-TLS listener used port zero.
    InvalidTlsListenAddress,
    /// Endpoint identity and explicitly configured TLS identity disagree.
    EndpointIdentityMismatch,
}

impl fmt::Display for RemoteConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NonLoopbackCleartext => "cleartext application ingress must use literal loopback",
            Self::InvalidHttpsEndpoint => {
                "remote application endpoint must be one canonical HTTPS authority"
            }
            Self::InvalidServerIdentity => "TLS server identity is invalid",
            Self::InvalidProtectedPath => "protected transport path is invalid",
            Self::OverlappingProtectedPaths => "protected transport paths must be distinct",
            Self::InvalidLocalSocketPath => "local application socket path is invalid",
            Self::InvalidBound => "remote transport bound is invalid",
            Self::InvalidTlsListenAddress => "direct TLS requires a nonzero TCP listen port",
            Self::EndpointIdentityMismatch => "endpoint and TLS server identities do not match",
        })
    }
}

impl Error for RemoteConfigError {}

/// Exact certificate identity verified by a direct-TLS client.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TlsServerIdentity {
    /// Canonical lower-case ASCII DNS name.
    Dns(String),
    /// Canonical literal IPv4 or IPv6 address.
    Ip(IpAddr),
}

impl TlsServerIdentity {
    /// Parses one canonical identity without wildcard or trailing-dot aliases.
    pub fn parse(value: &str) -> Result<Self, RemoteConfigError> {
        if let Ok(address) = value.parse::<IpAddr>() {
            if address.to_string() == value {
                return Ok(Self::Ip(address));
            }
            return Err(RemoteConfigError::InvalidServerIdentity);
        }
        validate_dns_name(value)?;
        Ok(Self::Dns(value.to_owned()))
    }

    /// Returns the canonical certificate identity text.
    #[must_use]
    pub fn as_str(&self) -> String {
        match self {
            Self::Dns(name) => name.clone(),
            Self::Ip(address) => address.to_string(),
        }
    }
}

/// One canonical HTTPS endpoint with no path, query, fragment, or user info.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CanonicalHttpsEndpoint {
    uri: String,
    identity: TlsServerIdentity,
    port: NonZeroU16,
}

impl CanonicalHttpsEndpoint {
    /// Parses `https://host:port` and rejects every alias or downgrade form.
    pub fn parse(value: &str) -> Result<Self, RemoteConfigError> {
        if value.len() > MAX_HTTPS_ENDPOINT_BYTES || !value.starts_with("https://") {
            return Err(RemoteConfigError::InvalidHttpsEndpoint);
        }
        let authority = &value["https://".len()..];
        if authority.is_empty()
            || authority
                .bytes()
                .any(|byte| matches!(byte, b'/' | b'?' | b'#' | b'@') || !byte.is_ascii_graphic())
        {
            return Err(RemoteConfigError::InvalidHttpsEndpoint);
        }
        let (identity, raw_port) = if let Some(rest) = authority.strip_prefix('[') {
            let (host, suffix) = rest
                .split_once(']')
                .ok_or(RemoteConfigError::InvalidHttpsEndpoint)?;
            let raw_port = suffix
                .strip_prefix(':')
                .ok_or(RemoteConfigError::InvalidHttpsEndpoint)?;
            let address = host
                .parse::<Ipv6Addr>()
                .map_err(|_| RemoteConfigError::InvalidHttpsEndpoint)?;
            if address.to_string() != host {
                return Err(RemoteConfigError::InvalidHttpsEndpoint);
            }
            (TlsServerIdentity::Ip(IpAddr::V6(address)), raw_port)
        } else {
            let (host, raw_port) = authority
                .rsplit_once(':')
                .ok_or(RemoteConfigError::InvalidHttpsEndpoint)?;
            if host.contains(':') {
                return Err(RemoteConfigError::InvalidHttpsEndpoint);
            }
            (
                TlsServerIdentity::parse(host)
                    .map_err(|_| RemoteConfigError::InvalidHttpsEndpoint)?,
                raw_port,
            )
        };
        let port = raw_port
            .parse::<NonZeroU16>()
            .map_err(|_| RemoteConfigError::InvalidHttpsEndpoint)?;
        if port.get().to_string() != raw_port {
            return Err(RemoteConfigError::InvalidHttpsEndpoint);
        }
        Ok(Self {
            uri: value.to_owned(),
            identity,
            port,
        })
    }

    /// Returns the canonical endpoint text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.uri
    }

    /// Returns the certificate identity derived from the endpoint authority.
    #[must_use]
    pub const fn identity(&self) -> &TlsServerIdentity {
        &self.identity
    }

    /// Returns the explicit nonzero endpoint port.
    #[must_use]
    pub const fn port(&self) -> NonZeroU16 {
        self.port
    }
}

/// An absolute, normalized path whose contents are loaded only by a runtime owner.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProtectedFilePath(PathBuf);

impl ProtectedFilePath {
    /// Accepts one bounded absolute path with only root and normal components.
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, RemoteConfigError> {
        let path = path.into();
        if !normal_absolute_path(&path, MAX_PROTECTED_PATH_BYTES) {
            return Err(RemoteConfigError::InvalidProtectedPath);
        }
        Ok(Self(path))
    }

    /// Borrows the protected path for the first-party loader.
    #[must_use]
    pub fn as_path(&self) -> &Path {
        &self.0
    }
}

impl fmt::Debug for ProtectedFilePath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProtectedFilePath([CONFIGURED])")
    }
}

/// Complete file-backed direct-TLS server material.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerTlsFiles {
    certificate_chain: ProtectedFilePath,
    private_key: ProtectedFilePath,
}

impl ServerTlsFiles {
    /// Builds one complete, path-disjoint server material selection.
    pub fn new(
        certificate_chain: ProtectedFilePath,
        private_key: ProtectedFilePath,
    ) -> Result<Self, RemoteConfigError> {
        if certificate_chain == private_key {
            return Err(RemoteConfigError::OverlappingProtectedPaths);
        }
        Ok(Self {
            certificate_chain,
            private_key,
        })
    }

    /// Borrows the certificate-chain path.
    #[must_use]
    pub const fn certificate_chain(&self) -> &ProtectedFilePath {
        &self.certificate_chain
    }

    /// Borrows the private-key path.
    #[must_use]
    pub const fn private_key(&self) -> &ProtectedFilePath {
        &self.private_key
    }
}

/// Independently bounded listener and connection lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListenerBounds {
    max_connections: NonZeroU32,
    max_streams_per_connection: NonZeroU32,
    handshake_timeout: Duration,
    idle_timeout: Duration,
    keepalive_interval: Duration,
    drain_timeout: Duration,
}

impl ListenerBounds {
    /// Checks every server-owned resource and time ceiling.
    pub fn new(
        max_connections: NonZeroU32,
        max_streams_per_connection: NonZeroU32,
        handshake_timeout: Duration,
        idle_timeout: Duration,
        keepalive_interval: Duration,
        drain_timeout: Duration,
    ) -> Result<Self, RemoteConfigError> {
        if max_connections.get() > MAX_LISTENER_CONNECTIONS
            || max_streams_per_connection.get() > MAX_HTTP2_STREAMS
            || !duration_within(handshake_timeout, 1, 30)
            || !duration_within(idle_timeout, 1, 3_600)
            || !duration_within(keepalive_interval, 1, 300)
            || !duration_within(drain_timeout, 1, 300)
        {
            return Err(RemoteConfigError::InvalidBound);
        }
        Ok(Self {
            max_connections,
            max_streams_per_connection,
            handshake_timeout,
            idle_timeout,
            keepalive_interval,
            drain_timeout,
        })
    }

    /// Safe alpha defaults used when no explicit remote bounds are present.
    #[must_use]
    pub fn alpha_default() -> Self {
        Self::new(
            NonZeroU32::new(1_024).expect("constant is nonzero"),
            NonZeroU32::new(128).expect("constant is nonzero"),
            Duration::from_secs(10),
            Duration::from_secs(300),
            Duration::from_secs(30),
            Duration::from_secs(30),
        )
        .expect("alpha defaults satisfy closed bounds")
    }

    /// Returns the process connection ceiling.
    #[must_use]
    pub const fn max_connections(self) -> NonZeroU32 {
        self.max_connections
    }

    /// Returns the per-connection HTTP/2 stream ceiling.
    #[must_use]
    pub const fn max_streams_per_connection(self) -> NonZeroU32 {
        self.max_streams_per_connection
    }

    /// Returns the complete-handshake deadline.
    #[must_use]
    pub const fn handshake_timeout(self) -> Duration {
        self.handshake_timeout
    }

    /// Returns the idle-connection deadline.
    #[must_use]
    pub const fn idle_timeout(self) -> Duration {
        self.idle_timeout
    }

    /// Returns the keepalive interval.
    #[must_use]
    pub const fn keepalive_interval(self) -> Duration {
        self.keepalive_interval
    }

    /// Returns the bounded graceful-drain interval.
    #[must_use]
    pub const fn drain_timeout(self) -> Duration {
        self.drain_timeout
    }
}

/// Existing development-only loopback cleartext listener.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LoopbackCleartextListener {
    listen_address: SocketAddr,
}

impl LoopbackCleartextListener {
    /// Rejects every non-loopback address before a socket is opened.
    pub fn new(listen_address: SocketAddr) -> Result<Self, RemoteConfigError> {
        if !listen_address.ip().is_loopback() {
            return Err(RemoteConfigError::NonLoopbackCleartext);
        }
        Ok(Self { listen_address })
    }

    /// Returns the literal loopback bind address.
    #[must_use]
    pub const fn listen_address(self) -> SocketAddr {
        self.listen_address
    }
}

/// Complete direct-TLS listener selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectTlsListenerConfig {
    listen_address: SocketAddr,
    public_endpoint: CanonicalHttpsEndpoint,
    tls: ServerTlsFiles,
    bounds: ListenerBounds,
}

impl DirectTlsListenerConfig {
    /// Constructs a direct listener only after its complete static identity validates.
    pub fn new(
        listen_address: SocketAddr,
        public_endpoint: CanonicalHttpsEndpoint,
        tls: ServerTlsFiles,
        bounds: ListenerBounds,
    ) -> Result<Self, RemoteConfigError> {
        if listen_address.port() == 0 {
            return Err(RemoteConfigError::InvalidTlsListenAddress);
        }
        Ok(Self {
            listen_address,
            public_endpoint,
            tls,
            bounds,
        })
    }

    /// Returns the configured TCP bind address.
    #[must_use]
    pub const fn listen_address(&self) -> SocketAddr {
        self.listen_address
    }

    /// Returns the externally verified endpoint identity.
    #[must_use]
    pub const fn public_endpoint(&self) -> &CanonicalHttpsEndpoint {
        &self.public_endpoint
    }

    /// Returns the complete protected TLS file selection.
    #[must_use]
    pub const fn tls(&self) -> &ServerTlsFiles {
        &self.tls
    }

    /// Returns the server lifecycle bounds.
    #[must_use]
    pub const fn bounds(&self) -> ListenerBounds {
        self.bounds
    }
}

/// Filesystem access granted to a protected local application socket.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalSocketAccess {
    /// Only the daemon owner may connect (`0600`).
    OwnerOnly,
    /// The daemon owner and configured filesystem group may connect (`0660`).
    OwnerAndGroup,
}

impl LocalSocketAccess {
    /// Returns the exact Unix permission bits applied after bind.
    #[must_use]
    pub const fn mode(self) -> u32 {
        match self {
            Self::OwnerOnly => 0o600,
            Self::OwnerAndGroup => 0o660,
        }
    }
}

/// Same-host or same-pod protected Unix-domain listener configuration.
#[derive(Clone, Eq, PartialEq)]
pub struct LocalSocketListenerConfig {
    path: PathBuf,
    access: LocalSocketAccess,
    bounds: ListenerBounds,
}

impl LocalSocketListenerConfig {
    /// Accepts one bounded absolute normal pathname; the runtime owns safe bind/publication.
    pub fn new(
        path: impl Into<PathBuf>,
        access: LocalSocketAccess,
        bounds: ListenerBounds,
    ) -> Result<Self, RemoteConfigError> {
        let path = path.into();
        if !normal_absolute_path(&path, MAX_LOCAL_SOCKET_PATH_BYTES) {
            return Err(RemoteConfigError::InvalidLocalSocketPath);
        }
        Ok(Self {
            path,
            access,
            bounds,
        })
    }

    /// Borrows the configured socket pathname.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the only permitted filesystem access profile.
    #[must_use]
    pub const fn access(&self) -> LocalSocketAccess {
        self.access
    }

    /// Returns the listener lifecycle bounds.
    #[must_use]
    pub const fn bounds(&self) -> ListenerBounds {
        self.bounds
    }
}

impl fmt::Debug for LocalSocketListenerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalSocketListenerConfig")
            .field("path", &"[CONFIGURED]")
            .field("access", &self.access)
            .field("bounds", &self.bounds)
            .finish()
    }
}

/// Closed application-listener profile. There is intentionally no insecure remote arm.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApplicationListenerConfig {
    /// Development-only cleartext on literal loopback.
    LoopbackCleartext(LoopbackCleartextListener),
    /// TCP with mandatory direct TLS.
    DirectTls(DirectTlsListenerConfig),
    /// Protected same-host/same-pod Unix-domain socket.
    LocalSocket(LocalSocketListenerConfig),
}

/// Complete static client TLS endpoint selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TlsClientConfig {
    endpoint: CanonicalHttpsEndpoint,
    trust_root: ProtectedFilePath,
    expected_server_identity: TlsServerIdentity,
    connect_timeout: Duration,
    keepalive_interval: Duration,
    max_pool_connections: NonZeroU32,
    max_streams_per_connection: NonZeroU32,
}

impl TlsClientConfig {
    /// Builds one verified-peer-only client selection with bounded pooling.
    #[allow(
        clippy::too_many_arguments,
        reason = "all transport bounds are explicit and closed"
    )]
    pub fn new(
        endpoint: CanonicalHttpsEndpoint,
        trust_root: ProtectedFilePath,
        expected_server_identity: TlsServerIdentity,
        connect_timeout: Duration,
        keepalive_interval: Duration,
        max_pool_connections: NonZeroU32,
        max_streams_per_connection: NonZeroU32,
    ) -> Result<Self, RemoteConfigError> {
        if endpoint.identity() != &expected_server_identity {
            return Err(RemoteConfigError::EndpointIdentityMismatch);
        }
        if !duration_within(connect_timeout, 1, 30)
            || !duration_within(keepalive_interval, 1, 300)
            || max_pool_connections.get() > MAX_CLIENT_POOL_CONNECTIONS
            || max_streams_per_connection.get() > MAX_HTTP2_STREAMS
        {
            return Err(RemoteConfigError::InvalidBound);
        }
        Ok(Self {
            endpoint,
            trust_root,
            expected_server_identity,
            connect_timeout,
            keepalive_interval,
            max_pool_connections,
            max_streams_per_connection,
        })
    }

    /// Returns the canonical HTTPS endpoint.
    #[must_use]
    pub const fn endpoint(&self) -> &CanonicalHttpsEndpoint {
        &self.endpoint
    }

    /// Returns the protected CA-root path.
    #[must_use]
    pub const fn trust_root(&self) -> &ProtectedFilePath {
        &self.trust_root
    }

    /// Returns the exact expected peer identity.
    #[must_use]
    pub const fn expected_server_identity(&self) -> &TlsServerIdentity {
        &self.expected_server_identity
    }

    /// Returns the bounded connect timeout.
    #[must_use]
    pub const fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// Returns the bounded keepalive interval.
    #[must_use]
    pub const fn keepalive_interval(&self) -> Duration {
        self.keepalive_interval
    }

    /// Returns the endpoint pool-size ceiling.
    #[must_use]
    pub const fn max_pool_connections(&self) -> NonZeroU32 {
        self.max_pool_connections
    }

    /// Returns the per-connection stream ceiling.
    #[must_use]
    pub const fn max_streams_per_connection(&self) -> NonZeroU32 {
        self.max_streams_per_connection
    }
}

fn validate_dns_name(value: &str) -> Result<(), RemoteConfigError> {
    if value.is_empty()
        || value.len() > MAX_DNS_NAME_BYTES
        || value.ends_with('.')
        || value.bytes().any(|byte| byte.is_ascii_uppercase())
        || value.split('.').any(|label| {
            label.is_empty()
                || label.len() > 63
                || label.starts_with('-')
                || label.ends_with('-')
                || !label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        })
    {
        return Err(RemoteConfigError::InvalidServerIdentity);
    }
    Ok(())
}

fn normal_absolute_path(path: &Path, maximum_bytes: usize) -> bool {
    path.is_absolute()
        && path.parent().is_some()
        && !path.as_os_str().is_empty()
        && path.as_os_str().as_encoded_bytes().len() <= maximum_bytes
        && path
            .components()
            .all(|component| matches!(component, Component::RootDir | Component::Normal(_)))
}

fn duration_within(value: Duration, minimum_seconds: u64, maximum_seconds: u64) -> bool {
    value >= Duration::from_secs(minimum_seconds) && value <= Duration::from_secs(maximum_seconds)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn protected(name: &str) -> ProtectedFilePath {
        ProtectedFilePath::new(PathBuf::from("/var/lib/riffdb").join(name)).expect("path")
    }

    #[test]
    fn listener_profile_is_closed_and_cleartext_is_literal_loopback_only() {
        let loopback = LoopbackCleartextListener::new("127.0.0.1:7443".parse().expect("address"))
            .expect("loopback");
        assert!(matches!(
            ApplicationListenerConfig::LoopbackCleartext(loopback),
            ApplicationListenerConfig::LoopbackCleartext(_)
        ));
        assert_eq!(
            LoopbackCleartextListener::new("0.0.0.0:7443".parse().expect("address")),
            Err(RemoteConfigError::NonLoopbackCleartext)
        );
        assert_eq!(
            LoopbackCleartextListener::new("[::]:7443".parse().expect("address")),
            Err(RemoteConfigError::NonLoopbackCleartext)
        );
    }

    #[test]
    fn endpoint_parser_accepts_only_one_canonical_https_authority() {
        for valid in [
            "https://riffdb.example.test:7443",
            "https://127.0.0.1:7443",
            "https://[2001:db8::1]:7443",
        ] {
            assert_eq!(
                CanonicalHttpsEndpoint::parse(valid)
                    .expect("canonical endpoint")
                    .as_str(),
                valid
            );
        }
        for invalid in [
            "http://riffdb.example.test:7443",
            "https://riffdb.example.test",
            "https://riffdb.example.test:0",
            "https://riffdb.example.test:07443",
            "https://RiffDB.example.test:7443",
            "https://riffdb.example.test.:7443",
            "https://user@riffdb.example.test:7443",
            "https://riffdb.example.test:7443/",
            "https://riffdb.example.test:7443?x=1",
            "https://riffdb.example.test:7443#fragment",
            "https://2001:db8::1:7443",
        ] {
            assert_eq!(
                CanonicalHttpsEndpoint::parse(invalid),
                Err(RemoteConfigError::InvalidHttpsEndpoint),
                "{invalid}"
            );
        }
    }

    #[test]
    fn direct_tls_requires_complete_disjoint_files_and_nonzero_bind_port() {
        assert_eq!(
            ServerTlsFiles::new(protected("server.pem"), protected("server.pem")),
            Err(RemoteConfigError::OverlappingProtectedPaths)
        );
        let tls = ServerTlsFiles::new(protected("server.pem"), protected("server.key"))
            .expect("complete TLS files");
        let endpoint =
            CanonicalHttpsEndpoint::parse("https://riffdb.example.test:7443").expect("endpoint");
        assert_eq!(
            DirectTlsListenerConfig::new(
                "0.0.0.0:0".parse().expect("address"),
                endpoint,
                tls,
                ListenerBounds::alpha_default(),
            ),
            Err(RemoteConfigError::InvalidTlsListenAddress)
        );
    }

    #[test]
    fn local_socket_is_absolute_normal_bounded_and_never_world_accessible() {
        let listener = LocalSocketListenerConfig::new(
            "/run/riffdb/application.sock",
            LocalSocketAccess::OwnerAndGroup,
            ListenerBounds::alpha_default(),
        )
        .expect("protected local socket");
        assert_eq!(listener.access().mode(), 0o660);
        assert_eq!(LocalSocketAccess::OwnerOnly.mode(), 0o600);
        for invalid in [
            PathBuf::from("relative.sock"),
            PathBuf::from("/run/riffdb/../application.sock"),
            PathBuf::from("/"),
        ] {
            assert_eq!(
                LocalSocketListenerConfig::new(
                    invalid,
                    LocalSocketAccess::OwnerOnly,
                    ListenerBounds::alpha_default(),
                ),
                Err(RemoteConfigError::InvalidLocalSocketPath)
            );
        }
    }

    #[test]
    fn client_config_cannot_disable_or_redirect_peer_identity() {
        let endpoint =
            CanonicalHttpsEndpoint::parse("https://riffdb.example.test:7443").expect("endpoint");
        let wrong = TlsServerIdentity::parse("other.example.test").expect("identity");
        assert_eq!(
            TlsClientConfig::new(
                endpoint,
                protected("ca.pem"),
                wrong,
                Duration::from_secs(10),
                Duration::from_secs(30),
                NonZeroU32::new(4).expect("nonzero"),
                NonZeroU32::new(128).expect("nonzero"),
            ),
            Err(RemoteConfigError::EndpointIdentityMismatch)
        );
    }

    #[test]
    fn configured_paths_and_debug_output_never_disclose_secret_locations() {
        assert_eq!(
            ProtectedFilePath::new("relative.pem"),
            Err(RemoteConfigError::InvalidProtectedPath)
        );
        let path = protected("private/server.key");
        let rendered = format!("{path:?}");
        assert!(!rendered.contains("server.key"));
        assert!(!rendered.contains("/var/lib"));
    }

    #[test]
    fn every_listener_and_pool_dimension_has_an_exact_ceiling() {
        let over_connections = NonZeroU32::new(MAX_LISTENER_CONNECTIONS + 1).expect("nonzero");
        assert_eq!(
            ListenerBounds::new(
                over_connections,
                NonZeroU32::new(1).expect("nonzero"),
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_secs(1),
            ),
            Err(RemoteConfigError::InvalidBound)
        );
    }
}
