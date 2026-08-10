# Remote and Local Application Ingress

RiffDB application gRPC has three and only three listener profiles:

- `loopback_cleartext` for local development;
- `direct_tls` for TCP beyond literal loopback; and
- `local_socket` for a same-host or same-pod first-party client.

There is no insecure non-loopback profile. Hosted MCP remains loopback-only.
Every protected operation still requires a RiffDB bearer capability and the
normal database selector; transport identity never becomes application
authority.

## Direct TLS

Configure a canonical public endpoint and one static certificate/key pair:

```toml
[server]
audience = "riffdb-application"
capability_keys = "/etc/riffdb/capability.keys"
idempotency_keys = "/etc/riffdb/idempotency.keys"

[server.application_listener]
mode = "direct_tls"
listen = "0.0.0.0:7443"
public_endpoint = "https://riffdb.internal.example:7443"
certificate_chain = "/etc/riffdb/tls/server-chain.pem"
private_key = "/etc/riffdb/tls/server-key.pem"
```

The leaf certificate must contain the endpoint's DNS name or IP address in its
subject alternative names. RiffDB validates the complete pair and endpoint
identity before opening the listener. The certificate must be a regular,
non-symlink file that is not group/world writable. The private key must be a
regular, non-symlink file with no group or other permission bits.

Replace certificate and key files using atomic same-directory renames. On the
next connection RiffDB reads both files as one snapshot and installs the new
identity only if the complete pair validates. Invalid or partially published
material retains the previous valid snapshot and emits a path-free diagnostic.
Established connections keep their existing identity until bounded drain.

The Rust client uses a protected explicit trust root:

```rust,ignore
use std::num::NonZeroU32;
use std::time::Duration;
use riffdb_client_rust::RiffDbClient;
use riffdb_config::{
    CanonicalHttpsEndpoint, ProtectedFilePath, TlsClientConfig,
    TlsServerIdentity,
};

let tls = TlsClientConfig::new(
    CanonicalHttpsEndpoint::parse("https://riffdb.internal.example:7443")?,
    ProtectedFilePath::new("/etc/my-app/riffdb-ca.pem")?,
    TlsServerIdentity::parse("riffdb.internal.example")?,
    Duration::from_secs(10),
    Duration::from_secs(30),
    NonZeroU32::new(1).unwrap(),
    NonZeroU32::new(128).unwrap(),
)?;
let client = RiffDbClient::connect_verified_tls(&tls).await?;
```

For a long-lived host that creates replacement channels, retain a
`VerifiedTlsConnector` instead. It validates the initial CA bundle, atomically
adopts a complete valid file replacement for later handshakes, and retains its
last valid snapshot when a replacement is missing, unsafe, or malformed.
Existing `RiffDbClient` channels keep the snapshot used for their handshake.
`last_reload_status()` reports only the closed current/retained disposition and
never exposes a path or certificate bytes.

The alpha TLS surface is intentionally small: rustls with the reviewed `ring`
provider, certificate/key files on the server, and an explicit CA root plus
peer identity on the client. There is no mTLS, native-root discovery,
trust-all, redirect, downgrade, cipher-suite, protocol-version, or provider
configuration.

## Protected local socket

For a same-host or same-pod process, configure:

```toml
[server.application_listener]
mode = "local_socket"
path = "/run/riffdb/application.sock"
access = "owner_only"
```

`owner_only` publishes mode `0600`; `owner_and_group` publishes `0660`. The
parent directory must already exist and must not be group/world writable.
RiffDB refuses an existing pathname instead of deleting a potentially
unrelated file. After an unclean stop, the operator must verify and remove a
stale socket before restart. Normal shutdown removes only the socket inode the
process created.

The socket protects the transport boundary; it does not bypass bearer
authentication, database selection, authorization, or shared application
service semantics.

## Bounds and proxies

The optional `[server.application_listener.bounds]` table sets bounded maximum
connections, HTTP/2 streams per connection, handshake, idle, keepalive, and
drain durations. Defaults are documented in the
[configuration reference](../configuration.md). These are resource ceilings,
not application authority or durability controls.

RiffDB ignores `Forwarded`, `X-Forwarded-*`, and proxy-supplied principal,
tenant, database, and authorization assertions. A proxy may carry the byte
stream, but the ordinary RiffDB credential and current authorization checks are
always repeated by the shared service.

Per-principal request-rate limits and tenant storage quotas are explicitly
deferred for alpha. Deploy remote ingress only on a controlled network behind
external connection controls; do not claim untrusted public multi-tenancy.
