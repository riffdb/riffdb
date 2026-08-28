# ADR-0105: Authenticated Remote Application Ingress

- **Status:** Accepted
- **Direction approved:** 2026-08-09
- **Exact text accepted:** Yes, 2026-08-09; amended 2026-08-27
  (Amendment 1: h2 0.4.19)
- **Decision deadline:** Before WP-553 changes listener, endpoint, certificate,
  health, or credential-rotation interfaces
- **Requires:** ADR-0007, ADR-0009, ADR-0025, ADR-0029, ADR-0040,
  ADR-0055, ADR-0063, and ADR-0071
- **Defines or blocks:** WP-550, WP-553, WP-554, and WP-579

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

The first alpha exposes no client-certificate, cipher-suite, protocol-version,
or cryptographic-provider knobs. Server configuration is exactly a certificate
chain and private-key path; client configuration is exactly a trust-root path
and expected peer identity. Mutual TLS is deferred. Bearer capability
authentication remains mandatory for protected operations.

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
- Per-principal request-rate limits and per-tenant storage/work quotas on the
  remote application gRPC ingress are explicitly deferred for alpha. Existing
  bounded global admission, per-query/command budgets, connection/stream
  ceilings, and current authorization remain mandatory. This deferral permits
  controlled design-partner networks only; it must be revisited before an
  untrusted internet-facing or shared multi-tenant service claim.

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
before ADR acceptance or implementation. The accepted provider is rustls with
the `ring` backend (`tls-ring` in Tonic), chosen over AWS-LC to avoid a
CMake/NASM installation requirement. This deliberately introduces the
reviewed C/assembly implementation in `ring`; it receives exact version pins,
Cargo-deny review, and lockfile/checksum discipline. The transport architecture
pins confine this stack to ingress/client transport crates and forbid it from
service, command, storage, and deterministic-runtime dependency graphs.

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
- **Defines or blocks:** WP-550, WP-553, WP-554, and WP-579.
- **Final evidence:** WP-579.

## Decision Deadline

Exact acceptance is required before adding a non-loopback listener, TLS/public
endpoint configuration, certificate dependency, or remote deployment claim.

## Amendment 1 — h2 advanced to 0.4.19 (Accepted 2026-08-27)

The maintainer accepted this exact text on 2026-08-27. The transport HTTP/2
framing dependency advances from `h2` 0.4.16 to 0.4.19 as a transitive
lockfile-only change. `h2` remains undeclared by any first-party crate, reached
only through `hyper` 1.11.0 and `tonic` 0.14.6. Every obligation in ADR-0105's
transport dependency review stands: the reviewed TLS closure is unchanged,
`tls-ring` remains the only backend, no compression codec is introduced, and
the exact `tonic = "=0.14.6"` pin does not move.

- **Why the pin advances — not security.** RUSTSEC-2026-0258 is already
  remediated: the advisory records `patched = [">= 0.4.16"]` and the tree
  reached 0.4.16 in commit `94ba1704` on 2026-08-21 as incidental lockfile
  drift. `cargo audit` reports zero vulnerabilities at 0.4.16. The advisory
  item recorded as outstanding in ADR-0004 Amendment 5 is stale and should be
  closed there.
- **Why the pin advances anyway — availability.** The 0.4.16 remediation (h2
  PR #935) added a connection-level DATA-framing budget that closes the
  connection with `GOAWAY ENHANCE_YOUR_CALM` on exhaustion, and charged
  end-of-stream DATA frames against it. h2 issue #939 reports well-behaved
  clients multiplexing many concurrent small-payload requests on one connection
  being terminated with `too_many_data_frames`. That is RiffDB's own client
  profile: a single long-lived `tonic::transport::Channel` issuing many small
  unary RPCs. 0.4.16 is the only released version carrying the regression.
  This repository has not observed the failure: its own c=128 benchmark runs
  completed 212,404 operations with zero errors, which is counter-evidence that
  RiffDB actually triggers #939 in practice. The pin therefore advances as a
  matter of prudence against a known upstream regression in the exact version
  the tree happened to land on, not in response to a live incident.
- **Fix verified upstream, not assumed.** h2 PR #940 (`c12d7820ad`, in 0.4.17)
  sets `is_budgeted = !frame.is_end_stream()` so EOS DATA frames are neither
  charged nor leaked from the budget. 0.4.18 adds `data_frame_budget(n)` to the
  builders; 0.4.19 scales the default budget to the configured connection
  window. The issue reporter confirmed #940 resolves both the reduced
  reproduction and their production system. No RiffDB-side reproduction was
  built and none is required.
- **Dependency graph.** `cargo tree -p h2 --edges normal` resolves to 11
  unchanged normal edges: `atomic-waker`, `bytes`, `fnv`, `futures-core`,
  `futures-sink`, `http`, `indexmap`, `slab`, `tokio`, `tokio-util`, and
  `tracing`. The `Cargo.lock` diff touches only h2's `version` and `checksum`;
  its dependency list is unchanged.
- **Features.** `cargo tree -p h2 -e features` shows no feature enabled. `h2`
  is selected as an optional dependency of `hyper`/`tonic`; RiffDB selects none
  directly.
- **Unsafe surface.** Unchanged and byte-identical between 0.4.16 and 0.4.19:
  exactly one `unsafe` block,
  `unsafe { std::str::from_utf8_unchecked(self.0.as_ref()) }` at
  `src/hpack/header.rs:283`. The two other textual matches are the
  `clippy::undocumented_unsafe_blocks` lint name at `src/lib.rs:85` and a
  comment at `src/proto/streams/streams.rs:33`.
- **Architecture pins.** No architecture test pins `h2`;
  `crates/riffdb-server/tests/architecture.rs` pins `tonic`, `rustls`,
  `tokio-rustls`, `ring`, and `base64`, all unchanged.
  `production_transport_features_are_exact_default_disabled_and_confined` and
  `lockfile_has_one_exact_ring_tls_stack_and_no_alternative_or_compression_stack`
  pass.
- **`cargo deny`/`cargo audit`.** `advisories` reports no RUSTSEC finding at
  either 0.4.16 or 0.4.19. At the time this amendment was drafted
  `cargo deny check advisories` failed at both versions on a single unrelated
  pre-existing item — yanked `chacha20` 0.10.1, reached via `rmcp` 2.2.0 →
  `rand` 0.10.2 — which was not an h2 finding and has since been cleared by
  advancing `chacha20` to 0.10.2 in a separate commit. `advisories` is green.
- **What this amendment does not grant.** It does not adopt
  `data_frame_budget()` as a configured bound, does not change the bounds
  applied to the `loopback_cleartext` profile, and does not extend the trust
  boundary in `docs/security.md`.
