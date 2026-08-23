# ADR-0140: Serial Ownership for the Existing One-In-Flight gRPC Application Session

- **Status:** Accepted
- **Direction approved:** Yes
- **Exact text accepted:** Yes, 2026-08-22
- **Accepted:** 2026-08-22
- **Acceptance reference:** Maintainer exact-text approval in the current
  Codex session for commit `7969108a`
- **Decision deadline:** Before WP-665 changes the implementation selected by
  `ApplicationSessionOpen.requested_max_in_flight == 1`, adds a generated
  serial-session pool, or uses session-shaped evidence for `PERF-018`
- **Requires:** ADR-0040, ADR-0055, ADR-0056, ADR-0105, ADR-0106, ADR-0120,
  ADR-0123, ADR-0127, ADR-0132, ADR-0133, ADR-0137, ADR-0138, and ADR-0139
- **Defines or blocks:** WP-665, WP-623, WP-578, and WP-579

This record is authoritative for WP-665 implementation. It does not reactivate
any removed framed or direct protocol.

## Context

RiffDB's remaining alpha performance failure is representative unary operation
latency. The current unary production path clears the short mixed c32, tail,
and seed gates on N1 and E2, but named reads remain approximately 1.31 to 2.42
times same-run safe-application PostgreSQL. Running the longer WP-623 matrix
cannot convert that known miss into qualification evidence.

Three transport campaigns have now separated the mechanism:

1. ADR-0127's existing bidirectional gRPC application session improved mixed
   throughput, but its maximum-in-flight implementation did not improve c1
   named-read latency. Even when configured for one caller, every operation
   still crosses a client mpsc channel, synchronous map lock, BTreeMap lookup,
   oneshot response, background response task, server BTreeMap,
   `FuturesUnordered`, `Abortable`, output mpsc channel, and the ordinary unary
   handler wrappers.
2. ADR-0137's framed multiplexer removed part of the HTTP/2 residual but added
   similar routing machinery and failed its cross-generation mechanics gate.
3. ADR-0138 and ADR-0139 proved that direct one-in-flight ownership frequently
   removes 44 to 55 percent of complete point-read latency and 59 to 66 percent
   of outside-service latency. Those candidates were correctly removed after
   missing their accepted proxy thresholds. No listener, frame, ALPN, client,
   or selector from those protocols remains.

The accepted gRPC session wire already represents a one-in-flight connection:
`requested_max_in_flight` is a nonzero bounded protocol field, and value one
already promises no concurrent application operation on that stream. Its
current implementation nevertheless pays the general multiplexer cost. A
serial implementation of that existing semantic case can test the direct-
ownership mechanism without another listener, ALPN, framing format, TLS stack,
or target-language protocol.

The prior campaigns' intermediate percentage thresholds were reject-first
mechanics falsifiers, not product requirements. This proposal does not weaken
them retroactively or reinterpret a failed cell as a pass. It defines a new,
smaller candidate and makes the actual `PERF-008` ratios its activation gate.

## Proposed Decision

### 1. Preserve ApplicationSession protocol V1 byte-for-byte

`ApplicationSessionService.Open`, `ApplicationSessionRequest`,
`ApplicationSessionResponse`, their strict Protobuf encoding, and
`APPLICATION_SESSION_PROTOCOL_V1` do not change. No field, enum, service,
method, message limit, or feature advertisement is added.

An exact `requested_max_in_flight == 1` selects a private serial execution
strategy. Values 2 through 128 retain ADR-0127's existing multiplexed
implementation and semantics. The selected strategy is not a latency promise
on the wire and does not alter correlation, cancellation, authorization,
freshness, durability, or uncertainty behavior.

This is not an exception that revives ADR-0137 through ADR-0139's removed
protocols. It uses only the already accepted and shipped gRPC session identity.
The new-identity requirement in the WP-664 closure continues to apply to any
new listener, ALPN, frame protocol, or direct transport. Exact acceptance of
this ADR is required to establish that narrow distinction.

### 2. One serial client lane has one owner and no operation router

For the one-in-flight case, the first-party Rust client owns exactly one
outbound request stream and its matching inbound response stream. One caller
at a time directly submits an existing session request and awaits the exact
matching response. The serial operation path contains no per-operation
BTreeMap entry, oneshot response channel, background response-routing task, or
correlation multiplexer.

Admission to a serial lane is try-only against one finite permit. Contention
returns the existing typed local-capacity outcome or selects another ready
lane in the fixed pool; it never creates an unbounded mutex, semaphore, future,
or waiter queue. Correlation IDs remain nonzero and strictly increasing, and
the one expected response must match the exact live correlation and operation
kind.

Cancellation after submission closes that lane generation. A query is
cancelled; a command remains outcome-unknown unless ordinary idempotency
recovery proves its durable result. The client never drains a possibly stale
response into a later operation, returns an uncertain lane to the pool, or
falls back to unary after possible command submission.

### 3. One serial server lane has one operation state, not a collection

For the one-in-flight case, the gRPC server uses a dedicated bounded serial
state machine. It holds at most one operation, one cancellation state, and one
response. It does not allocate a per-operation BTreeMap, `FuturesUnordered`,
or general correlation registry, and it does not spawn a task per operation.

The state machine concurrently observes the live operation, exact inbound
cancel or stream loss, absolute session lifetime, request deadline, server
shutdown, and bounded response delivery. It cannot stop polling lifecycle
edges merely because an operation is running or a peer is slow. Existing
maximum message, output-stall, session-work, idle, and lifetime ceilings remain
in force.

Command and named-query adaptation is factored only as needed into one private
API-neutral application-operation adapter shared by unary and both session
strategies. The adapter retains structural conversion, request control,
authentication, current authorization, application service invocation,
response release, typed error construction, and response conversion. Neither
the serial state machine nor the client lane receives storage, catalog, plan,
policy, commit, projection, audit, or clock authority.

### 4. Authentication and every mutable safe point remain per operation

Session establishment proves the credential presentation and exact application
scope but grants no reusable allow decision. Every operation re-enters ordinary
authentication and reloads current capability revision, revocation, expiry,
audience, database, role, contract, module, operation permission, tenant, row,
field, secret, freshness, and response-release facts.

Immutable presentation parsing may be retained only in the protected redacting
form already allowed by ADR-0127. Any pay-once proof must be bound to the exact
session generation and may establish only byte validity, never current
authority. A policy or identity change affects the next operation and closes
or refuses through the existing typed outcome.

### 5. Concurrency is a finite pool of independent serial streams

A production candidate may use a fixed first-party Rust pool of serial gRPC
sessions with one shared immutable channel and trust generation. Each stream
independently performs application-session establishment and owns at most one
operation. Pool cardinality, fill, ready minimum, replacement, idle time,
lifetime, and total retained bytes are fixed implementation ceilings; alpha
exposes no application tuning knob.

A client is ready only after its required minimum lanes are authenticated and
scope-exact. Partial fill, saturation, reconnect, certificate or credential
rotation, server restart, and terminal failure are typed and bounded. A lane
is returned only after one complete known response. Stream order and pool
selection grant no transaction, snapshot, command order, or read-after-write
semantics; causal reads still require the explicit commit frontier.

Go, TypeScript, and Python continue to use the first-party Rust driver host.
They may not implement session ownership, TLS, retries, cancellation,
uncertainty, or framing independently.

### 6. Activation is decided by public ratios, not a proxy gain

WP-665 begins with a diagnostic-only serial strategy selected explicitly by
the benchmark. It runs three counterbalanced process generations on N1 and E2
with byte-identical artifacts and separate loopback and verified-TLS cells.
Each cell reports cold trust, session establishment, first operation, steady
operation, every server stage, outside-service time, caller total, CPU, RSS,
and shutdown. Component sums must close to caller total within five percent.

The candidate stops and is removed if any generation:

- makes cold trust plus session establishment more than five percent slower
  than the existing multiplexed session;
- fails to reduce exact `GetTicket` outside-service mean by at least 50 percent
  from unary, because the current public ratio cannot pass with a smaller
  mechanism gain;
- regresses the API-neutral service-stage sum, seed, startup, shutdown, CPU, or
  RSS by more than five percent; or
- violates any semantic, lifecycle, resource, or stability test.

Passing that mechanics gate permits a short same-run public matrix. Retention
then requires every representative generated scenario at most 1.10 times
safe-application PostgreSQL on N1 and E2, c32 mixed throughput at least 0.90
times PostgreSQL with p95 at most 1.25 times PostgreSQL, seed at most 5.0 times
PostgreSQL, zero correctness mismatch, and no otherwise-uncovered regression
above five percent. These are the existing `PERF-008` gates, not substitutes.

Only after the complete workstation, N1, and E2 90-second matrix passes may a
separate exact human decision amend `PERF-018` and generated-client defaults.
Until then unary remains the default and the serial pool is diagnostic only.
Any failed mechanics or public gate removes the serial specialization and pool
without removing ADR-0127's existing optional multiplexed session.

## Options Considered

1. **Run WP-623 now:** rejected. Current production bytes are materially the
   same as the receipted preflight that misses representative unary latency by
   1.31 to 2.42 times; longer windows cannot make a known candidate eligible.
2. **Revive the direct framed lane with a lower threshold:** rejected. That
   would reinterpret accepted failed evidence and reintroduce another protocol
   surface.
3. **Activate the existing multiplexed session:** rejected. It regressed exact
   c1 reads on E2 and still contains the measured routing machinery even at one
   in-flight operation.
4. **Optimize unary HTTP/2 calls incrementally:** retained as a fallback, but
   the direct-lane existence proof is larger than the measured removable unary
   handler stages and points to ownership rather than another small wrapper
   reduction.
5. **Specialize the existing session's exact one-in-flight case:** proposed.
   It tests direct ownership while retaining the accepted gRPC trust and wire
   boundary.

## Consequences

- RiffDB may obtain persistent-session ownership without a new protocol or TLS
  implementation.
- The client and server gain a second private implementation strategy for one
  existing public session method.
- Cancellation destroys one serial lane rather than retaining it; bounded pool
  replacement pays fresh session establishment.
- The generic multiplexed session remains for callers that explicitly request
  more than one in-flight operation.
- Failure removes only the specialization; unary and the existing session wire
  remain unchanged.

## Compatibility

There is no Protobuf, contract grammar, RiffQL, contract/query IR, bundle,
module, plan, generated operation, durable record, journal, backup, export,
changelog, replication, or target-language value-format change.

Existing session V1 clients and servers remain wire compatible. A server is
free to implement maximum-in-flight one with the generic or serial strategy;
observable semantics are identical. Generated serial-pool selection is
additive and cannot become the default without a later accepted `PERF-018`
amendment.

## Security

TLS, peer verification, ALPN `h2`, capability credentials, database selection,
deadlines, message bounds, and compression policy are unchanged. There is no
new listener, cleartext remote path, verifier, trust knob, early data, custom
framing, or target-language transport.

Every mutable authorization and disclosure safe point remains per operation.
The serial implementation retains no positive allow decision, completed
response, application value, or durable outcome. Errors and telemetry remain
fixed-cardinality and redaction-safe.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** Applications still express only
  generated typed commands and exact named queries. They cannot select
  transactions, ordering, cached authority, durability, retries, storage,
  freshness, framing, TLS, or pool dimensions. Cancellation and uncertainty
  remain fail-closed per operation.
- **Scale:** Each lane retains one bounded operation and response. Pool,
  connection, byte, task, deadline, idle, lifetime, and replacement counts are
  fixed independently of database size and process duration. The design
  assumes neither co-located storage nor a full-state rewrite.

## Testing

- Architecture tests proving the max-one path contains no operation BTreeMap,
  `FuturesUnordered`, oneshot response router, per-operation spawn, or target-
  language protocol implementation.
- Unary, generic-session, and serial-session semantic equivalence for every
  generated command, named query, declared outcome, structured error,
  read-after-commit fence, and maximum response.
- Deterministic cancellation schedules before submission and before/during/
  after command admission and durability; uncertain lanes are destroyed and
  exact retry recovers only by idempotency identity.
- Revocation, expiry, role/contract/module drift, row-policy change, secret
  output, database retirement, server shutdown, slow peer, saturation, partial
  pool fill, reconnect, and rotation tests.
- Strict Protobuf, correlation, response-kind, truncation, duplicate-field,
  size, lifetime-work, output-stall, and fuzz tests on the unchanged V1 wire.
- Three-generation N1/E2 mechanics, short public ratio matrix, then complete
  workstation/N1/E2 qualification only in that order.

## Requirements and Work Packages

- **Requirements:** `API-001`, `PERF-005`, `PERF-008`, `PERF-018`, `SEC-001`,
  `SEC-002`, `NET-001` through `NET-012`, and `DRV-001` through `DRV-014`
- **Defines or blocks:** WP-665 and WP-623
- **Final evidence:** WP-623, WP-578, and WP-579 after a separate accepted
  comparator/default amendment

## Decision Deadline

The maintainer accepted this exact decision text on 2026-08-22 for commit
`7969108a`. WP-665 may proceed under the reject-first conditions above.
