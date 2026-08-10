#![forbid(unsafe_code)]

//! Closed, non-secret deployment configuration shared by RiffDB transports.
//!
//! This crate owns configuration identity only. TLS parsing, handshakes,
//! protected-file loading, and authorization remain in their first-party
//! runtime owners.

mod remote;

pub use remote::{
    ApplicationListenerConfig, CanonicalHttpsEndpoint, DirectTlsListenerConfig, ListenerBounds,
    LocalSocketAccess, LocalSocketListenerConfig, LoopbackCleartextListener,
    MAX_CLIENT_POOL_CONNECTIONS, MAX_DNS_NAME_BYTES, MAX_HTTP2_STREAMS, MAX_HTTPS_ENDPOINT_BYTES,
    MAX_LISTENER_CONNECTIONS, MAX_LOCAL_SOCKET_PATH_BYTES, MAX_PROTECTED_PATH_BYTES,
    ProtectedFilePath, RemoteConfigError, ServerTlsFiles, TlsClientConfig, TlsServerIdentity,
};
