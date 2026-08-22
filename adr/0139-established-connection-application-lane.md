# ADR-0139: Established-Connection Generated-Operation Lane

- **Status:** Accepted
- **Direction approved:** Yes
- **Exact text accepted:** Yes, 2026-08-22
- **Accepted:** 2026-08-22
- **Acceptance reference:** Maintainer exact-text approval in the current
  Codex session for commit `291fd842`
- **Decision deadline:** Before WP-664 restores any direct application
  listener, framing crate, ALPN, client selector, or comparator candidate
- **Requires:** ADR-0040, ADR-0055, ADR-0056, ADR-0070, ADR-0105, ADR-0106,
  ADR-0120, ADR-0123, ADR-0127, ADR-0132, ADR-0133, ADR-0137, and ADR-0138
- **Defines or blocks:** WP-664, WP-623, WP-578, and WP-579

This record is authoritative for WP-664 implementation.

## Context

WP-663 implemented ADR-0138's direct-ownership mechanics probe and then
removed it under the accepted reject-first rule. Direct ownership passed every
loopback threshold in all three counterbalanced N1 and E2 generations. Under
verified TLS it reduced complete established-connection point-read mean by
48.51 percent on N1 and 52.37 percent on E2, reduced the outside-service
component by 64.82 and 63.70 percent, and closed its fine-grained ledger within
0.001 percent.

The candidate nevertheless failed ADR-0138's requirement that the application
protocol reduce connection setup plus the first operation by 25 percent.
Reduction was 12.17 percent on N1 and 22.23 percent on E2. The implementation
was reverted in full and unary gRPC remains the only generated-application
transport. The receipts and decision are recorded in
`docs/performance/wp-663-direct-exclusive-closure.md`.

That result distinguishes two mechanisms that ADR-0138 combined in one gate:

1. TCP and verified TLS establish the peer, trust root, encryption, and ALPN.
   Both application protocols must pay that trust-layer cost.
2. Once the protected connection exists, HTTP/2, Tonic, the client session
   actor, channels, correlation routing, and task wakeups are application-
   protocol costs. Direct ownership removes most of that measured cost.

RiffDB's generated clients are persistent clients. A caller awaits connection
establishment once, then issues bounded operations over retained connections.
Cold connection latency remains customer-visible and must be bounded and
reported, but requiring an inner application protocol to improve a shared TLS
handshake by 25 percent tests the wrong mechanism. Silently deleting the gate
would be equally wrong. The accounting boundary and replacement falsifier need
an explicit accepted decision before any implementation returns.

`PERF-018` and the complete release ratios remain unchanged. This decision does
not excuse connection latency, allow hidden warmup, or activate a protocol.

## Proposed Decision

### 1. Trust establishment and application-operation latency are separate gates

WP-664 measures three non-overlapping customer-paid intervals:

- **cold trust establishment:** socket creation through verified TLS, exact
  peer identity, exact ALPN, and transport readiness;
- **session establishment:** credential presentation through one accepted or
  refused application-session result; and
- **generated operation:** submission after session acceptance through the
  complete typed response and caller wakeup.

All three intervals are reported. No interval may be moved outside caller time,
charged to warmup, subtracted as benchmark artifact, or hidden behind a
constructor that reports readiness early. The sum must close to a separately
measured connect-session-first-operation caller interval within five percent.

Cold trust plus session establishment must be no more than five percent slower
than same-run unary gRPC in every process generation on N1 and E2 and must
remain inside the existing finite connect, setup, and authentication
deadlines. The candidate is not required to reduce the TLS handshake itself.

The first generated operation after an accepted session, with no earlier
application operation on that connection, must be at least 25 percent faster
than the corresponding first unary operation on an already established and
authenticated channel. Established-connection c1 operation mean must retain
ADR-0138's stronger gates: at least 40 percent complete reduction, at least 55
percent outside-service reduction, and at most five percent ledger error.

This paragraph supersedes only ADR-0138 section 1's combined setup-plus-first-
operation 25-percent threshold. Every other ADR-0138 semantic, hostile-input,
resource, no-regression, cross-profile, removal, and release gate remains in
force.

### 2. One verified transport generation is paid once and bounded

The first-party Rust client may compile one immutable verified transport
generation containing only:

- the exact endpoint and peer name;
- the exact trust-root identity and TLS provider;
- the exact allowlisted ALPN;
- finite connect and operation deadlines; and
- rustls's bounded in-process resumption store for that exact verifier and
  client-auth configuration.

The generation may be shared only by connections with byte-identical trust and
peer configuration. A certificate, trust-root, endpoint, peer-name, ALPN, or
client-auth configuration change creates a new generation and drops the old
resumption state. Resumption state is process-local, bounded, never exported,
never persisted, and never crosses database or server identity.

The first connection performs ordinary full certificate verification. TLS
zero-RTT and half-RTT application data remain disabled. Resumption may reduce
later pool-fill or reconnect cost, but no release claim may substitute resumed
setup for the separately reported cold full-handshake cell.

Transport readiness proves only a protected connection to the configured
peer. It conveys no RiffDB principal, capability, database, role, contract,
query, row, field, freshness, durability, or response-release authority.

### 3. The direct lane semantics remain exactly ADR-0138's

If the revised mechanics gate passes, WP-664 may restore version 1 of the
exclusive generated-operation lane specified by ADR-0138 sections 2 through 5:

- one connection binds one database, credential presentation, exact
  application identity, query-module set, protocol version, and ceilings;
- one operation is in flight and the submitting caller directly polls the
  protected stream;
- only exact generated commands and named queries cross the lane;
- concurrency is a finite pool of exclusive connections;
- loopback cleartext, verified direct TLS, and protected local socket reuse the
  existing trust profiles without downgrade; and
- credential presentation may be proven once for session establishment while
  every mutable authority and response-release fact is reloaded and checked
  for every operation.

No client actor, internal request channel, multiplexer, correlation registry,
response router, cached allow decision, dynamic request, raw ID, arbitrary
transaction, compression, extension payload, or target-language protocol
implementation is admitted.

The frame magic, version, kinds, flags, ALPN, fixtures, and compatibility
surface are new candidate identities. Removed WP-663 bytes are not
grandfathered and cannot be copied without recreating their strict fixtures and
evidence under this decision.

### 4. Pool readiness and failure are explicit

A generated client is ready only after its required minimum pool cardinality
has completed trust and session establishment. Optional additional connections
may fill only within the accepted finite pool and deadline. Pool readiness,
partial fill, saturation, reconnect, and terminal failure are typed states; an
application cannot observe a ready client backed by zero authenticated lanes.

The default minimum is one ready connection. Client construction therefore
cannot hide the cold setup cost, while subsequent operations measure the
persistent application shape customers use. A pool connection is returned
only after a complete known response. Any connection with uncertain command
status is destroyed, and retry uses the existing outcome-recovery rules and
original idempotency identity.

Cancellation, credential rotation, revocation, expiry, role or contract drift,
certificate rotation, server restart, graceful shutdown, and pool replacement
retain ADR-0138's finite and fail-closed behavior. TLS resumption never skips
application-session establishment after reconnect.

### 5. Activation remains release-gated

The diagnostic again runs three counterbalanced process generations on N1 and
E2 using one exact artifact per revision. It covers loopback and verified TLS,
the cold/full-handshake interval, accepted-session first operation,
established-operation mean, the exact ledger, and matching feature-disabled
resource controls. Any threshold or order-stability failure removes every
listener, protocol, ALPN, client, and benchmark selector and closes WP-664 with
only a value-free failed-candidate receipt.

A mechanics pass does not amend `PERF-018`. Production activation additionally
requires the complete workstation, N1, and E2 matrix proving every
representative generated scenario at most 1.10 times safe-application
PostgreSQL, c32 mixed throughput at least 0.90 times with p95 at most 1.25
times PostgreSQL, seed at most 5.0 times PostgreSQL, zero correctness mismatch,
and no existing metric regression above five percent where a stricter gate
does not apply.

Only after the full matrix passes may a separate exact human decision amend
`PERF-018` and generated-client defaults. Unary gRPC remains supported for
compatibility, administration, streaming control surfaces, and diagnosis.

## Options Considered

1. **Keep ADR-0138's combined 25-percent setup gate:** rejected as a mechanism
   mismatch. WP-663 proved that verified TLS dominates the interval even while
   direct ownership removes most established-connection protocol cost.
2. **Delete setup testing:** rejected. Cold start, pool fill, reconnect, and
   certificate rotation are customer-paid lifecycle behavior and remain
   bounded, reported, and regression-gated.
3. **Warm the pool invisibly:** rejected. Client readiness waits for the
   required minimum authenticated connection, and the complete construction
   interval remains explicit evidence.
4. **Use TLS zero-RTT application data:** rejected. Replay and early-data
   semantics are unnecessary and would complicate command uncertainty and
   authentication boundaries.
5. **Restore the direct lane with corrected lifecycle accounting:** proposed
   because WP-663 supplied an existence proof for the remaining release-scale
   operation latency while preserving the ordinary application service.

## Consequences

- Cold TLS establishment remains visible and may still be several
  milliseconds on general-purpose cloud CPUs.
- Persistent generated operations can be evaluated against the cost actually
  owned by their protocol without credit for removing shared TLS work.
- The Rust client gains one immutable bounded transport-generation and pool-
  readiness concept; this is additional lifecycle complexity.
- A passed candidate still does not improve storage-heavy queries, writer
  serialization, durability fences, or batch apply.
- Failed mechanics again leave no production or public compatibility surface.

## Compatibility

This decision changes no contract grammar, contract/query IR, bundle, module,
plan, generated operation, durable record, journal, backup, export, changelog,
replication, or target-language value format.

Unary gRPC remains byte- and behavior-compatible. The direct lane is additive
only if all activation gates pass. Its candidate frame and ALPN identities are
new and versioned; old servers continue to serve gRPC and do not advertise the
lane. There is no fallback after possible command submission.

## Security

The existing rustls/ring provider, exact trust root, peer-name validation, and
ALPN allowlist remain authoritative. There is no insecure remote listener, TLS
knob, custom verifier, alternate provider, 0-RTT, compression, proxy principal,
or target-language TLS implementation.

Resumption is confined to one in-process immutable verifier/client-auth
generation and inherits rustls's bounded store. A generation change discards
it. Credential bytes remain redacted and are not retained after session proof.
Every operation reloads current capability, revocation, expiry, database,
audience, principal, role, contract, module, tenant, row, field, secret,
freshness, and response-release facts. Revocation or drift can close the next
operation even on an otherwise healthy resumed TLS connection.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** Applications select no trust,
  resumption, pool, framing, authority-cache, retry, durability, or freshness
  escape. They receive only a ready typed generated client or a bounded typed
  failure. Every write remains a compiled idempotent command and every read a
  bounded exact named query through the shared application service.
- **Scale:** Transport generations, server names, tickets, connections, pool
  fill, frames, buffers, operations, waits, setup, idle time, lifetime,
  cancellation, shutdown, and telemetry are fixed-bounded independently of
  database size. The design retains no result or full-state cache and does not
  assume co-located storage or rewrite durable state.

## Testing

- Exact three-interval and total caller ledgers with a five-percent closure
  bound; cold full-handshake, resumed reconnect, accepted-session first-op, and
  established-operation cells are structurally distinct.
- Three counterbalanced loopback and verified-TLS generations on N1/E2 using
  byte-identical artifacts, plus workstation and complete release matrices
  only after mechanics pass.
- Architecture tests confining transport generation, rustls, ring, sockets,
  framing, and resumption to reviewed ingress/client crates.
- Hostile tests for wrong CA, peer name, ALPN, preface, version, kind, flags,
  lengths, truncation, trailing bytes, slow peers, pool exhaustion, and
  readiness failure.
- Deterministic lifecycle schedules for pool fill, cancellation, connection
  loss before/during/after command admission and durability, uncertain result,
  reconnect, rotation, revocation, expiry, drift, shutdown, and replacement.
- Assertions that cold setup is never labeled warm/resumed, zero-RTT and
  half-RTT application data remain disabled, resumption never crosses a
  transport generation, and every reconnect repeats application session proof.
- Unary/direct byte-semantic equivalence and Rust/Go/TypeScript/Python driver-
  host conformance through the one first-party Rust transport.

## Requirements and Work Packages

- **Requirements:** `API-001`, `PERF-005`, `PERF-008`, `PERF-018`, `NET-001`
  through `NET-012`, and `DRV-001` through `DRV-014`.
- **Defines or blocks:** WP-664 and WP-623.
- **Final evidence:** WP-623, WP-578, and WP-579 after a separately accepted
  comparator/default amendment.

## Decision Deadline

The maintainer accepted this exact decision text on 2026-08-22 for commit
`291fd842`. WP-664 may restore the diagnostic candidate and may proceed beyond
it only under the reject-first conditions above.
