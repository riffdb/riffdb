# ADR-0137: Bounded Framed Application Transport

- **Status:** Proposed
- **Direction approved:** Yes
- **Exact text accepted:** No
- **Decision deadline:** Before WP-662 adds a non-HTTP/2 listener path, changes
  generated-client transport selection, or amends `PERF-018`
- **Requires:** ADR-0040, ADR-0055, ADR-0056, ADR-0105, ADR-0106,
  ADR-0123, ADR-0127, ADR-0132, and ADR-0133
- **Defines or blocks:** WP-662 and any framed-transport `PERF-018` evidence

## Context

The optimized application service is no longer the dominant cost of a small
generated read. A current optimized workstation measurement of 10,000
generated point reads reports a 119-microsecond caller mean and approximately
24 microseconds across every instrumented server stage. The remaining
approximately 95 microseconds is customer-paid client, Protobuf, Tonic,
HTTP/2, socket, and runtime scheduling work. The retained N1 and E2 receipts
measure the same outside-stage family at approximately 430--700 microseconds.

ADR-0127's bidirectional HTTP/2 session proved that amortizing unary stream
setup can help: it materially improved N1 and earlier mixed-load cells. It did
not produce stable gains on E2, and its single-owner variant regressed there.
Both variants still traverse Tonic and HTTP/2 and add channel/router wakeups.
WP-660 and WP-661 therefore closed without activation. Reopening those exact
candidates would ignore falsifying evidence.

The remaining alpha gates are conjunctive. Every representative generated
operation must be at most 1.10 times same-run safe-application PostgreSQL;
mixed c32 throughput must be at least 0.90 times PostgreSQL with p95 at most
1.25 times PostgreSQL; and the seed must remain within its 5.0-times regression
ceiling. More storage, query, coordinator, cache, compression, batching-window,
or result-packing work cannot remove a transport residual paid before and after
an already-small service invocation.

This record authorizes one reject-first transport candidate shaped like the
persistent database session used by the comparator, while preserving RiffDB's
first-party Rust protocol, trust, authorization, command, freshness,
uncertainty, and boundedness rules. It does not declare gRPC obsolete and does
not amend the release comparator merely by being accepted.

## Proposed Decision

### 1. Add one closed generated-operation frame protocol

RiffDB may add version 1 of a first-party Rust framed application protocol.
The protocol carries only the already accepted bounded application-session
open, generated command, exact named-query, cancellation, response, typed
failure, and controlled-close envelopes. It carries no kernel operation,
administration, deployment, contract source, ad-hoc RiffQL, raw field or type
identifier, generic transaction, caller-selected method, or storage option.

One connection selects exactly one database alias, credential presentation,
application lock, contract identity, finite sorted query-module set, protocol
version, and maximum in-flight count no greater than 128. Each operation has a
nonzero monotonically increasing correlation ID. Responses may complete out of
order and must match exactly one live correlation. Connection order grants no
snapshot, transaction, command order, or read-after-write guarantee.

The binary frame header is fixed-width, versioned, canonical, and independently
bounded. It contains one protocol magic, version, closed frame kind, zero-only
reserved flags, correlation ID, and payload length. Multi-byte integers use one
specified byte order. The payload is exactly one existing strict public
Protobuf message. Unknown versions, kinds, flags, duplicate/live correlations,
zero correlations, noncanonical headers, truncated frames, trailing bytes, or
payloads above the message and connection ceilings fail closed before
application execution. There is no compression, content negotiation, dynamic
schema, application-defined extension, or caller-selected buffer size in v1.

The implementation retains no completed-response history. Request bytes,
response bytes, live correlations, cancellation tombstones, output backlog,
total connection work, idle time, and connection lifetime all have fixed hard
ceilings. Slow readers and writers are closed at finite byte and time bounds.

### 2. Share one listener and the existing trust boundary

The framed protocol does not add an insecure remote port.

- Under `direct_tls`, the existing first-party rustls ingress advertises only
  exact allowlisted ALPN values for HTTP/2 and the framed application protocol.
  Certificate, key, trust-root, peer-name, validity, and downgrade rules remain
  ADR-0105 exact. An absent or unknown ALPN fails closed.
- Under `loopback_cleartext`, the existing literal-loopback-only listener may
  distinguish the HTTP/2 client preface from one fixed framed-protocol preface
  within a small byte and time ceiling. An ambiguous, partial, slow, or unknown
  preface is rejected without application admission.
- Under `local_socket`, the existing same-host filesystem-identity and
  permission checks remain exact. A local proxy cannot assert a RiffDB
  principal or rewrite a frame. A framed connection may cross a remote proxy
  only as an opaque layer-4/TCP stream terminating at `direct_tls`; the
  existing HTTP/2 application-proxy path remains gRPC-only. Whether
  `local_socket` accepts the framed protocol is fixed by WP-662's process
  tests, never an application knob.

Ingress performs TLS and protocol selection, then hands an already protected
bounded byte stream to either Tonic or the framed adapter. The TLS provider,
versions, cipher defaults, certificate files, and cryptographic dependency
closure do not change. Rustls and `ring` remain confined to the accepted
ingress/client transport crates; service, command, storage, and deterministic
runtime graphs gain no cryptographic dependency.

This decision deliberately amends ADR-0105's direct-TLS statement that the
application endpoint speaks only HTTP/2. Direct TLS instead advertises one
exact ALPN allowlist containing `h2` and the versioned framed protocol. It does
not amend ADR-0105's three exact ingress profiles, certificate and peer-name
verification, credential boundary, proxy restrictions, downgrade refusal, or
default-disabled posture.

WP-662 must replace, not delete, the architecture pins named
`production_transport_features_are_exact_default_disabled_and_confined` and
`reviewed_transport_and_entropy_graph_remains_exact`. The server pin gains only
the exact framed ingress dependency and retains crypto confinement. The client
pin may allow the already reviewed `tokio-rustls`/`rustls` stack only in the
native framed client transport; it continues to forbid direct `ring`
selection, alternate crypto stacks, and crypto dependencies in generated
facades or semantic crates. The existing
`tonic_runtime_features_remain_narrow_and_default_disabled` pin remains
unchanged: framing lives in a separate first-party crate and does not widen
the gRPC crate's feature or dependency closure. Each pin change and its
accepted ADR citation must land with the implementation that requires it.

### 3. Both transports invoke one API-neutral operation adapter

WP-662 must refactor command and named-query adaptation into one first-party
Rust operation adapter used by both gRPC and framing. Neither transport calls
the other's handler. The shared adapter owns structural-to-domain conversion,
operation-specific error mapping, request control, current lifecycle admission,
authentication, authorization, application-service invocation, response
release, and domain-to-wire conversion.

The framed ingress owns only byte bounds, frame state, correlation, connection
backpressure, and transport cancellation. It receives no storage, catalog,
policy, plan, commit, projection, or audit port. Architecture tests must make
that dependency boundary mechanical.

Opening the connection authenticates the presentation for establishment but
does not cache authority. Every operation reloads and verifies the current
capability, expiry, audience, database, contract/role revision, operation
permission, row and field policy, and response-release decision through the
same safe points as gRPC. A credential, role, policy, module, database, or
contract change affects the next operation and may close the connection with a
typed reason. No positive allow decision survives an operation.

### 4. Durability, freshness, cancellation, and uncertainty do not change

Every command retains its ordinary command identity, idempotency identity,
typed outcome, coordinator admission, conflict ownership, atomic mutation and
event boundary, durable acknowledgement fence, audit/provenance, and
outcome-recovery behavior. Stream or process loss cannot turn an uncertain
command into a safe fresh retry.

Every query retains its exact named plan and module identity, current-policy
checks, row/field redaction, bounded result, cursor, explicit freshness, and
read-after-commit fence. Submission order never implies freshness; callers
continue to supply the committed frontier explicitly.

Cancellation releases non-durable query and command work under the existing
rules. Once command durability is unknown or possible, the result is
outcome-unknown and is resolved only with the original idempotency identity.
Reconnect opens a new protocol generation and cannot infer or replay a prior
result.

### 5. Rust owns remote protocol and trust for every language

The native Rust application client may implement the framed connection
directly. Go, TypeScript, and Python generated clients continue to use the
retained first-party Rust driver host, so target-language packages do not
reimplement TLS, framing, Protobuf validation, authentication, retry,
uncertainty, freshness, or error classification. They retain only the
language-idiomatic value and typed-facade work already authorized by their
driver ADRs.

No target-language-native framed protocol is permitted by this decision. A
future native transport for another language requires a separate accepted ADR
and equivalent hostile-wire and semantic evidence.

### 6. Activation is reject-first and comparator-governed

WP-662 first produces an instrumented mechanics ledger for the same generated
point read and representative command over unary gRPC, ADR-0127 session, and
framed transport. The ledger separates client encode/queue, socket, server
decode/adapt, application service, server encode/queue, socket, and client
decode/wakeup, and closes within five percent of caller time. Benchmark-driver
bridging remains measurement artifact and cannot be credited.

Before a complete release sweep, the framed candidate must, in three
counterbalanced process generations on both N1 and E2:

- reduce c1 generated-operation mean by at least 35 percent and c8 mean by at
  least 20 percent relative to same-run unary RiffDB;
- reduce the measured outside-service-stage component by at least 40 percent;
- show no greater-than-five-percent regression in server service time, memory,
  connection setup, seed, startup, shutdown, or any unary/session control; and
- pass the complete hostile-frame, authorization, cancellation, uncertainty,
  freshness, lifecycle, and bounded-resource corpus.

Missing any mechanics threshold removes production selection and stops before
expensive qualification. A passing mechanics gate still does not amend
`PERF-018`. Activation additionally requires same-run workstation, N1, and E2
evidence proving every representative generated scenario at most 1.10 times
safe-application PostgreSQL, c32 mixed throughput at least 0.90 times with p95
at most 1.25 times PostgreSQL, seed at most 5.0 times PostgreSQL, zero semantic
or correctness mismatch, and no existing metric regression above five percent
where a stricter gate does not apply.

Only after every gate passes may a separate exact human decision amend
`PERF-018` and generated-client defaults. That amendment freezes protocol
version, listener profile, connection count, in-flight limit, retry budget,
dataset, weights, and comparator shapes. Unary gRPC and the control-plane RPCs
remain supported for compatibility and diagnosis.

## Options Considered

1. **Retry ADR-0127's existing multiplexed or single-owner sessions:** rejected.
   WP-660 and WP-661 already falsified cross-profile stability, and both retain
   the Tonic/HTTP/2 scheduling family this work must measure.
2. **Skip strict validation or cache authority:** rejected. The product cannot
   buy performance by accepting ambiguous wire messages or stale permissions.
3. **Expose the frame protocol directly in each target language:** rejected.
   That would duplicate protocol, TLS, retry, and uncertainty semantics outside
   first-party Rust.
4. **Replace all gRPC:** rejected. Administration, streaming control surfaces,
   compatibility, and low-level diagnosis retain gRPC; this candidate is only
   the closed high-frequency generated-operation data plane.
5. **Add bounded first-party framing beside gRPC:** proposed because it attacks
   the measured outside-stage floor while preserving the application service
   and every semantic safe point.

## Consequences

- The listener and Rust client become more complex and must maintain two
  transport adapters with one semantic operation core.
- The framed protocol becomes an additive compatibility surface only if its
  activation gates pass. Failed mechanics leave no production listener or
  generated-client selection.
- Successful activation can reduce scheduler and HTTP/2 cost for small
  operations without weakening command/query semantics or requiring every
  target language to implement a database protocol.
- The design does not improve intrinsically expensive query plans, storage
  work, writer serialization, or seed apply. Those remain separately measured.

## Compatibility

The candidate adds no contract grammar, contract/query IR, bundle/module/plan
hash, durable record, journal, backup, export, changelog, or replication change.
Existing gRPC clients and servers remain interoperable. A framed client uses
the candidate only after exact protocol negotiation; it never silently falls
back after a command becomes uncertain. Old servers reject or do not advertise
the protocol and continue serving gRPC.

The outer frame and its payload fixtures are additive and versioned. A future
frame version requires an accepted compatibility decision; reserved bits and
unknown kinds are not an extension mechanism.

## Security

Remote framing receives exactly ADR-0105 TLS, peer verification, and capability
authentication. It adds no trust-all, cleartext remote, proxy-principal,
application cipher, compression, or mTLS knob. Credentials, values, identities,
absolute paths, frame payloads, and peer prose never enter diagnostics. Metrics
use only fixed frame kinds, bounded counts/bytes, and duration buckets.

Strict public-message validation remains on every untrusted payload. Header
parsing is constant-space and bounded before allocation. Slowloris, oversized,
truncated, duplicate-correlation, response-flood, cancel-flood, reconnect,
revocation, and downgrade tests must fail closed without leaking whether a
hidden row, event, principal, or operation exists.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** Applications still express only
  generated typed commands and exact named reads. Framing exposes no transaction,
  generic query, durability, policy, freshness, retry, storage, or protocol
  choice. Every operation passes the same API-neutral semantic and authority
  safe points as gRPC.
- **Scale:** Header and payload bytes, correlations, tasks, buffers, queued
  responses, cancellations, waits, connection lifetime/work, telemetry, and
  evidence are hard-bounded independently of database size and runtime. The
  protocol assumes neither one tenant nor memory-resident authoritative state.

## Testing

- Golden frame and strict-Protobuf fixtures for every request, response,
  failure, cancellation, and boundary value.
- Parser fuzzing and fragmentation/coalescing tests over every byte boundary,
  malformed length, unknown kind, duplicate correlation, and reserved bit.
- Deterministic schedules for out-of-order completion, cancellation at every
  durability boundary, slow peers, capacity exhaustion, stream loss, reconnect,
  graceful shutdown, and server restart.
- Unary/session/framed semantic equivalence for authorization, revocation,
  expiry, row policy, field visibility, idempotency, outcome recovery,
  read-after-commit, typed errors, and result bytes.
- Real process tests for loopback preface selection, TLS ALPN, hostile
  certificates, local-socket and layer-4 proxy carriage, rotation, no
  downgrade, and bounded shutdown.
- Architecture tests proving first-party Rust ownership and absence of service,
  storage, catalog, plan, policy, and commit authority in the frame adapter.
- Counterbalanced workstation/N1/E2 mechanics followed by the complete
  `PERF-018` matrix only after the reject-first gate passes.

## Requirements and Work Packages

- **Requirements:** existing `API-001`, `PERF-008`, `PERF-018`, `NET-001`
  through `NET-012`, and `DRV-001` through `DRV-014`.
- **Implements:** WP-662.
- **Final evidence:** WP-623 and WP-579 only after a separate accepted
  comparator/default amendment.

## Decision Deadline

Exact human acceptance is required before adding framed ingress, changing the
listener/TLS demultiplexer, changing generated-client transport selection, or
using framed results as release evidence.
