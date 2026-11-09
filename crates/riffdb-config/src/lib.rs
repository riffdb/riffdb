#![forbid(unsafe_code)]

//! Closed, non-secret deployment configuration shared by RiffDB transports.
//!
//! This crate owns configuration identity only. TLS parsing, handshakes,
//! protected-file loading, and authorization remain in their first-party
//! runtime owners.

mod remote;
mod trace_parent;
pub use trace_parent::{TraceParentError, W3cTraceParent};
mod x509;

pub use remote::{
    ApplicationListenerConfig, AutoIssueTlsConfig, CanonicalHttpsEndpoint, ClientTlsIdentity,
    ClientTrustRootConfig, DirectTlsListenerConfig, ListenerBounds, LocalSocketAccess,
    LocalSocketListenerConfig, LoopbackCleartextListener, MAX_CLIENT_POOL_CONNECTIONS,
    MAX_DNS_NAME_BYTES, MAX_HTTP2_STREAMS, MAX_HTTPS_ENDPOINT_BYTES, MAX_LISTENER_CONNECTIONS,
    MAX_LOCAL_SOCKET_PATH_BYTES, MAX_PROTECTED_PATH_BYTES, ProtectedFilePath, RemoteConfigError,
    ServerTlsFiles, TlsClientConfig, TlsServerIdentity,
};
pub use x509::{
    IssuedTlsBundle, IssuedTlsIdentityPem, issue_tls_bundle, issue_tls_bundle_from_seed,
    write_protected_pem_file,
};
