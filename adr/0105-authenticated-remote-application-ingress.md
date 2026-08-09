# ADR-0105: Authenticated Remote Application Ingress

- **Status:** Proposed
- **Direction approved:** 2026-08-09
- **Exact text accepted:** No
- **Decision deadline:** Before WP-553 changes listener, endpoint, certificate,
  health, or credential-rotation interfaces
- **Requires:** ADR-0007, ADR-0009, ADR-0025, ADR-0029, ADR-0040,
  ADR-0055, ADR-0063, and ADR-0071
- **Defines or blocks:** WP-550, WP-553, WP-554, and WP-570

Direction approval records the alpha blocker and intended product shape. This
record is not authoritative until the human maintainer accepts its exact text.

## Context

The public application gRPC service is intentionally loopback-only and
cleartext today. That was the correct POC boundary, but it prevents an
application container, Kubernetes workload, remote operator, or ordinary
reverse proxy from reaching RiffDB without collapsing the transport trust
boundary. Opaque bearer capabilities are not safe on an unencrypted remote
link, and accepting proxy-derived identity would move authentication out of the
shared service.

Alpha needs deployable networking without making the transport a policy engine,
granting a proxy authority over RiffDB principals, or weakening the existing
loopback development path.

## Proposed Decision

RiffDB adds a closed listener profile with exactly these modes:

1. `loopback_cleartext`, preserving the current development-only literal-
   loopback behavior;
2. `direct_tls`, permitting TCP bind on configured interfaces only after a
   complete server certificate, private key, trust configuration, and public
   endpoint identity validate; and
3. `local_socket`, permitting a protected Unix-domain socket for a same-host or
   same-pod first-party driver/proxy process.

There is no generic `insecure_remote` mode. A non-loopback TCP bind without TLS
fails configuration before the socket opens. Hosted MCP remains separately
governed and gains no remote mode merely because application gRPC does.

### TLS and proxy boundary

`direct_tls` uses first-party Rust transport configuration and HTTP/2. Clients
verify a configured CA chain and DNS name or IP subject alternative name.
Hostname verification, certificate validity, ALPN, maximum chain size, key
type, and protocol versions fail closed. Disabling verification, trusting all
certificates, or falling back to cleartext is not an application option.

An L4 or HTTP/2 proxy may forward or re-encrypt the connection, but RiffDB does
not trust `Forwarded`, `X-Forwarded-*`, proxy principal, tenant, database, or
authorization headers. The opaque RiffDB credential and database selector are
still authenticated and authorized by the shared service. Proxy source address
is diagnostic transport metadata only and is redacted before public errors.

TLS client certificates may be configured as an additional ingress admission
condition. They do not become a RiffDB principal, capability, role, tenant, or
authorization decision. Bearer capability authentication remains mandatory for
protected operations in the first alpha.

### Architecture-pin amendment and replication sequencing

This decision deliberately supersedes two current server architecture pins:

- `production_transport_features_are_exact_and_default_disabled`; and
- `lockfile_has_one_base64_and_no_tls_or_compression_stack`.

They are not deleted or weakened opportunistically. WP-550 must first freeze
their replacements. The production-feature pin becomes an exact allowlist for
the one reviewed TLS implementation and its minimum feature closure while
retaining `default = []` and keeping TLS disabled in the
`loopback_cleartext` profile. The lockfile pin continues to require exactly one
accepted `base64`, forbids compression and alternate TLS/cryptographic stacks,
and permits only the reviewed, version-pinned TLS dependency closure. Adding a
second backend, default feature, compression codec, or native cryptographic
dependency remains an architecture-test failure requiring another accepted
ADR.

The replication arc is active: WP-491 is merged and RE2's `ShipChangelog`
streaming RPC will amend the exact public transport inventory. WP-553 may not
change the streaming-RPC exactness pin or allocate overlapping Protobuf fields
against an unrecorded RE2 revision. WP-550 records the exact merged RE2
transport revision (or an explicit pre-RE2 sequencing decision); WP-553 then
rebases on that inventory. TLS wraps the accepted service surface and does not
reinterpret changelog authorization, ordering, framing, resumption, or
backpressure.

### Endpoints, bounds, and lifecycle

Remote endpoints use canonical `https` URIs with bounded DNS names or literal
IPs and a nonzero port. Client configuration also binds the expected database,
public capability audience, trust roots, server name, connect timeout,
keepalive, maximum concurrent streams, and bounded connection-pool size.
Redirects and URI user information are rejected.

Handshake, request-header, body, decompression, idle, keepalive, connection,
stream, and graceful-drain bounds are server-owned. Saturation produces the
existing typed overload behavior, never an unbounded accept queue.

Certificate and trust-root reload is atomic: a complete newly validated
snapshot replaces the prior snapshot for new handshakes. Existing connections
may finish only within a configured bounded drain interval. Invalid reload
retains the prior valid snapshot and emits a safe operator diagnostic.

### Health and credential rotation

The network edge exposes an unauthenticated liveness result containing only
process-alive and protocol-version-compatible facts. Database readiness,
selected alias, history identity, active contract, audience, and degraded state
remain authenticated. Kubernetes and Compose examples use liveness for process
restart and an authenticated, database-selected readiness probe before routing
application traffic.

Application credential rotation is a resumable least-authority campaign:

1. derive a new capability from the same exact symbolic role and scope;
2. publish it to a new protected credential file or secret version;
3. prove one authenticated health and one exact application identity handshake;
4. switch new connections to the successor; and
5. explicitly revoke the predecessor after a bounded overlap.

Rotation never widens a role, prints a credential, overwrites the sole retained
copy before successor proof, or treats certificate rotation as capability
rotation. Revocation is authoritative even for pooled connections because each
operation retains current-policy authorization.

### Deployment proof

The release supplies Compose and Kubernetes acceptance deployments with
separate application and operator credentials, sibling containers/pods,
readiness/liveness probes, certificate rotation, capability rotation, proxy
forwarding, graceful drain, and expired/revoked credential cases. No example
mounts a database file into an application container or exposes a kernel/admin
credential to the application.

## Options Considered

1. **Permit cleartext on private networks:** rejected because network placement
   is not an authentication or confidentiality proof.
2. **Trust a reverse proxy to assert the RiffDB principal:** rejected because it
   creates a second authorization boundary and weakens current-policy checks.
3. **Require mutual TLS as the only application identity:** rejected for alpha;
   certificate lifecycle and RiffDB role/capability lifecycle are different
   concerns, and opaque capabilities already carry exact authority.
4. **TLS transport plus unchanged capability authentication:** proposed because
   it makes the current security model deployable without reinterpreting roles.

## Consequences

- Remote deployment becomes possible without an insecure compatibility mode.
- Operators must provision certificates and capability secrets explicitly.
- A reverse proxy is optional and never authoritative for application policy.
- OAuth, internet-facing hosted MCP, public certificate automation, and
  multi-region routing remain later decisions.

## Compatibility

Loopback configuration remains valid and retains its current meaning. Remote
listener, endpoint, health, and rotation fields are additive versioned public
configuration and protocol surfaces. No durable application record, command
plan, query plan, storage key, or outcome meaning changes.

## Security

Private keys are loaded only from protected files or injected secret handles,
never CLI literals or public diagnostics. Credentials, certificate material,
submitted values, peer prose, and absolute paths remain redacted. TLS parsing
and cryptographic dependencies require explicit dependency and feature review
before ADR acceptance or implementation. That review includes the complete
transitive lockfile closure and the exact replacement assertions for both
architecture pins named above.

## Standing Design Tests

- **Interface safety:** applications cannot select insecure remote transport,
  disable peer verification, inject proxy identity, or convert a certificate
  into application authority. The safe path is the only remote path.
- **Scale:** listeners and pools are bounded per process but do not assume that
  authoritative data is memory-resident or globally co-located. Endpoint
  routing remains compatible with later partition placement.

## Testing

- Configuration property and snapshot tests for every listener/profile pair.
- TLS interoperability, hostile certificate, ALPN, hostname, reload, expiry,
  and downgrade tests against real processes.
- Proxy tests proving forwarded identity and authorization headers are ignored.
- Compose and Kubernetes container-to-container smoke tests.
- Rotation schedules covering successor failure, crash between every phase,
  predecessor revocation, pooled connections, and uncertainty.
- Architecture checks proving remote requests still use the shared application
  service, authorization layer, and commit coordinator.

## Requirements and Work Packages

- **Provisional requirements:** `NET-001` through `NET-012`, to be added to
  `SPEC.md` only after exact acceptance.
- **Defines or blocks:** WP-550, WP-553, WP-554, and WP-570.
- **Final evidence:** WP-570.

## Decision Deadline

Exact acceptance is required before adding a non-loopback listener, TLS/public
endpoint configuration, certificate dependency, or remote deployment claim.
