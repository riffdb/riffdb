# ADR-0106: Rust-Owned Multilanguage Driver Platform

- **Status:** Accepted
- **Direction approved:** 2026-08-09
- **Exact text accepted:** Yes, 2026-08-09
- **Decision deadline:** Before WP-555 freezes the driver-host protocol or Go
  and TypeScript runtime contract
- **Requires:** ADR-0037, ADR-0040, ADR-0055, ADR-0056, ADR-0074, and ADR-0105
- **Defines or blocks:** WP-555 through WP-558 and WP-579

## Context

Rust has a native stable application client and Python delegates transport
trust to that Rust client through PyO3. TypeScript currently lacks a long-lived
native transport, Go has no stable binding, and Python supports a narrow wheel
matrix. Reimplementing status trust, retries, uncertainty recovery, identity
negotiation, TLS, cancellation, and read-after-commit separately in Go and
TypeScript would violate the first-party Rust semantics boundary and create
language-dependent safety behavior.

Alpha needs ordinary language ergonomics and pooled long-lived connections
without making generated bindings miniature database clients.

## Proposed Decision

The first-party Rust application client remains the sole owner of:

- endpoint and TLS verification;
- connection pooling and HTTP/2 channel lifecycle;
- credential/database/identity handshakes;
- public error validation and transport-status trust;
- retry, replay, idempotency, and outcome-uncertainty recovery;
- cancellation/deadline propagation; and
- read-after-commit and reactive cursor resumption.

Rust applications link it directly. Python continues to use the accepted PyO3
bridge. Go and server-side TypeScript use a long-lived first-party Rust process,
`riffdb-driverd`, through a protected local socket and a closed versioned
application-operation protocol.

### Driver host boundary

`riffdb-driverd` loads one exact application lock, one generated operation
catalog, one database selector, and one or more protected role credentials. It
connects to RiffDB through ADR-0105 and exposes only the named commands, named
queries, batches, and reactive operations present in that exact catalog. It has
no generic kernel, administration, ad-hoc RiffQL, contract deployment, role
binding, or storage method.

The local protocol carries symbolic operation identity, schema-hashed typed
values, request options, cancellation, bounded progress, and the existing
structured application result/error families. It never carries a bearer
credential, field ID, mask, encoded storage key, raw Protobuf message, arbitrary
RPC method, or caller-selected endpoint. A handshake proves driver protocol,
application lock, contract/module/role identities, generated schema digest,
database alias, and driver version before an operation is accepted.

The socket is a protected Unix-domain socket for the alpha deployment matrix.
The host may be an application child process or same-pod sidecar. It refuses a
world-readable socket, symlink replacement, peer outside the configured UID/GID
policy, stale application lock, or second instance owning the same socket.
Windows named pipes and browser-direct access are deferred.

### Generated Go and TypeScript bindings

Generated Go and TypeScript own only language-idiomatic immutable values,
operation methods, typed outcomes, async/stream presentation, and local request
assembly. They do not interpret gRPC status, retry remote calls, load RiffDB
credentials, choose a remote endpoint, or parse peer prose.

The Go package provides context cancellation, typed errors, bounded iterators,
explicit attempt/read options, deterministic code generation, and a supported
Go toolchain floor. The TypeScript package provides one long-lived asynchronous
transport, abort signals, async iterators, bounded pooling through the host, and
server-runtime shutdown. Neither silently starts a new host per operation.

Browser code connects only to an application-owned authenticated HTTP/SSE
relay. It never receives the local socket path or a RiffDB credential.

### Pooling, cancellation, and errors

The Rust owner maintains bounded endpoint pools with explicit maximum
connections, streams per connection, idle timeout, connect timeout, and queue
capacity. Pool overload returns a typed local capacity error. It does not create
unbounded tasks or connections.

Cancellation propagates from the generated caller to the exact in-flight Rust
operation. It releases non-durable resources and never claims to cancel a
command whose durable status is uncertain. Such a command retains the original
idempotency identity and resolves through the existing recovery path.

All languages receive the same checked semantic error code, retryability,
recovery action, identities, safe symbol path, and trace ID. Language wrappers
may add a local presentation stack but may not substitute message matching for
the closed error registry.

### Python release expansion

The accepted Python semantics remain unchanged while release work expands the
tested abi3 runtime and platform matrix. The exact CPython floor, manylinux,
musllinux, macOS, Windows, x86_64, and aarch64 tiers are frozen by WP-557 after
clean-host build probes. Unsupported combinations fail installation clearly;
they do not fall back to an unreviewed pure-Python transport.

### Conformance manifest

Every released driver publishes a machine-readable conformance manifest naming
its protocol, error registry, value model, operation families, cancellation,
pooling, retry, read-after-commit, reactive support, platform/toolchain matrix,
and exact golden corpus digest. A claimed feature must pass the common corpus on
an installed artifact; absence is explicit, never emulated silently.

## Options Considered

1. **Independent pure-Go and Node gRPC clients:** rejected because transport
   trust and uncertainty semantics would move out of first-party Rust.
2. **A first-party C ABI used by cgo and N-API:** deferred because it requires a
   new unsafe FFI boundary, allocator/ownership contract, and crash containment.
3. **One CLI subprocess per call:** rejected because it has no long-lived pool,
   streaming, or reliable cancellation lifecycle.
4. **A closed local Rust driver host:** proposed because it preserves semantic
   ownership, supplies long-lived connections, and uses no new first-party
   unsafe code.

## Consequences

- Go and TypeScript gain stable application bindings without semantic forks.
- Deployments add a small sidecar/child process and protected local socket.
- Python remains a native in-process exception under ADR-0074.
- Direct browser, pure-Go transport, and an FFI ABI remain deferred.

## Compatibility

The driver-host protocol is a new versioned local public interface and requires
golden compatibility fixtures. Generated signatures are public compatibility
artifacts. Remote gRPC and durable database formats are unchanged.

## Security

The driver host owns credentials and remote trust; target-language processes
receive operation authority only through the protected local catalog. Socket
permissions are defense in depth, while server-side authorization remains
mandatory for every operation. Local and remote error text remains bounded and
redacted.

## Standing Design Tests

- **Interface safety:** Go and TypeScript cannot construct an arbitrary remote
  call, choose a weaker TLS policy, supply raw authority, or bypass generated
  named operations. Python and Rust retain their already reviewed boundaries.
- **Scale:** pools, queues, messages, and streams are bounded. The driver host
  is stateless with respect to authoritative application data and does not
  assume a single database node.

## Testing

- One cross-language golden corpus for Rust, Go, TypeScript, and Python.
- Host protocol fuzzing, hostile length/schema/identity tests, and socket peer
  authorization tests.
- Pool saturation, reconnect, cancellation-at-every-boundary, retry, replay,
  read-after-commit, cursor, and shutdown schedules.
- Installed artifact tests for every claimed platform/toolchain tier.
- Boundary lint proving generated Go/TypeScript contain no gRPC client,
  credential parser, retry classifier, or kernel operation.

## Requirements and Work Packages

- **Provisional requirements:** `DRV-001` through `DRV-014`, to be added to
  `SPEC.md` only after exact acceptance.
- **Defines or blocks:** WP-555, WP-556, WP-557, WP-558, and WP-579.
- **Final evidence:** WP-558 and WP-579.

## Decision Deadline

Exact acceptance is required before the driver-host protocol, Go package,
long-lived TypeScript runtime, or expanded Python support claim is frozen.
