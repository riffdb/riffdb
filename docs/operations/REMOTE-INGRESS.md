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

The CLI accepts the same verified target through its ordinary client file:

```toml
[client]
endpoint = "https://riffdb.internal.example:7443"
database = "default"
credential_file = "/run/secrets/riffdb-application.credential"
tls_trust_root = "/run/secrets/riffdb-ca.pem"
tls_server_name = "riffdb.internal.example"
```

`RIFFDB_TLS_TRUST_ROOT` and `RIFFDB_TLS_SERVER_NAME` are the only environment
overrides for those TLS selectors. They must be supplied together. The CLI
does not accept certificate material, bearer material, or a verification
override on the command line.

## Liveness and readiness

`riffdb server health` without a credential is a payload-free liveness probe.
It must also omit a database selector. Its successful response says only that
the application protocol process is alive: lifecycle is unspecified, the
database alias and authentication audience are empty, and readiness is false.
Adding a database selector to an unauthenticated probe rejects rather than
confirming whether that alias exists.

Database readiness is a separate authenticated probe. Supply the selected
database and a protected credential authorized for health; the response may
then include lifecycle, readiness, database alias, audience, and component
status. The canonical component names are `authoritative_storage`, `catalog`,
`commit_coordinator`, `projection`, `outbox`, and `vector_staleness`; each is
`healthy`, `degraded`, or `unavailable`. `vector_staleness` reports embedding
quality state under its own identity and must not be interpreted as projection
or vector-index readiness.

The POC does not yet have an authoritative staleness observer, so production
reports `vector_staleness: unavailable` rather than claiming healthy state.
Once authoritative serving is available, a non-healthy projection, outbox, or
vector-staleness component makes aggregate authenticated health `degraded`;
the current unavailable vector-staleness component therefore prevents an
overall `ready` report. Container routing must use authenticated readiness,
while process restart may use unauthenticated liveness. Neither probe prints
bearer material.

## Application credential rotation

An application deployed with `--provision-role` retains its exact role hash,
capability ID, protected credential path, and resumable deployment state under
`.riffdb/deployments/<database>/`. Rotate it by rerunning the same exact locked
deployment with the same role and explicit replacement:

```bash
riffdb --config /run/secrets/riffdb-operator.toml application deploy \
  riffdb.application.json \
  --lock riffdb.application.lock.json \
  --provision-role TicketDeskApplication \
  --replace-role-credential
```

Replacement is a retained campaign, not revoke-then-create. RiffDB records a
fresh successor identity, creates a new create-only protected credential file,
proves the selected database and audience with authenticated health, proves
the successor's active contract and complete authorized command/query catalog,
switches generated application client configuration, then explicitly revokes
the predecessor. The predecessor file is removed only after revocation is
durably observed. The bearer is never returned in command output or deployment
state.

If the process stops at any phase, rerun the identical command with
`--replace-role-credential`. Omitting the flag while a campaign is active
fails with a resume instruction. If a crash loses a just-created successor
secret before local retention, the retry revokes that exact unusable
successor, preserves the predecessor, and asks for one more retry with a fresh
identity. A capability-ID conflict is never treated as authority owned by the
campaign and is never revoked.

Certificate/trust rotation and capability rotation are independent. Replacing
TLS files does not create, widen, or revoke an application role; replacing an
application credential does not modify TLS trust.

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

## Compose and Kubernetes

The current checked release surfaces are:

- `release/container/compose.yaml`,
  with a direct-TLS database, TCP pass-through proxy, and disjoint one-shot
  application/operator proof containers; and
- `release/helm/riffdb`, with a stateful database pod, retained backup volume,
  authenticated typed-readiness gate, payload-free liveness, pass-through proxy,
  and exactly two opt-in proof Jobs using separate external Secrets.

`release/kubernetes/riffdb.yaml` and
`scripts/remote-kubernetes-render-check` are retained historical fixtures. They
are not the current NET-010 proof, an operator installation path, or
controlled-cluster execution evidence.

The release container normalizes the installed `riffdb` and `riffdbd` modes
inside the image before switching to its fixed non-root identity. A restrictive
host build umask therefore cannot produce an image whose runtime user cannot
execute the installed binaries. The acceptance harness also detects a rootless
Podman-backed `docker` command and applies `keep-id` only to the three RiffDB
containers, preserving owner-only config and credential mounts without running
the service as container root. Docker Engine uses the checked Compose file
without that override.

The application workloads mount no database, backup, digest key, TLS private
key, or operator credential. Chart Secret projections select only the required
keys and are copied by a bounded init container into a size-bounded,
memory-backed owner-only directory because RiffDB deliberately rejects
group-readable bearer and private-key files. Chart-owned probes and proof Jobs
use the exact release Service DNS identity; the values surface has no endpoint
or TLS-name override that can redirect them. Application routing begins only
after the authenticated probe returns the canonical typed `ready` result.
Bootstrap remains local-authority-only and outside chart assembly. The Compose
ceremony executes that one operation inside the database container's loopback
namespace and publishes the result to a protected operator-only handoff
directory. It does not permit bootstrap through the proxy.

Run `./scripts/remote-compose-acceptance` for the real container proof,
`./scripts/check-helm-operator-package` for the parsed chart gate, and
`./scripts/check-helm-upgrade-rehearsal` for the explicitly offline non-release
lifecycle render. The Compose proof builds the release image, starts sibling
containers, bootstraps through the TLS proxy, proves separate credentials,
rotates certificate files and the application credential, rejects the revoked
predecessor, and performs a bounded graceful stop. The pass-through proxy uses
the container runtime's resolver with bounded retries, so replacing or
restarting the database container cannot leave it pinned to a stale backend
address. Neither surface treats proxy headers or network placement as
authority.
