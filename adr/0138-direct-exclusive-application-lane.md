# ADR-0138: Direct Exclusive Generated-Operation Lane

- **Status:** Accepted
- **Direction approved:** Yes
- **Exact text accepted:** Yes, 2026-08-22
- **Accepted:** 2026-08-22
- **Acceptance reference:** Maintainer exact-text acceptance in the current
  Codex session for commit `f580be6e`
- **Decision deadline:** Before WP-663 adds a non-HTTP/2 application listener,
  changes credential-proof scope, exposes an exclusive generated-operation
  session, or changes `PERF-018`
- **Requires:** ADR-0040, ADR-0055, ADR-0056, ADR-0070, ADR-0105, ADR-0106,
  ADR-0120, ADR-0123, ADR-0127, ADR-0132, ADR-0133, and ADR-0137
- **Defines or blocks:** WP-663, WP-623, WP-578, and WP-579

This record is authoritative for WP-663 implementation.

## Context

WP-662 implemented ADR-0137's multiplexed bounded frame candidate and removed
it after the reject-first gate failed. The candidate demonstrated that removing
Tonic and HTTP/2 can materially reduce small-read latency, but it retained a
client session actor, an internal request channel, correlation bookkeeping,
out-of-order completion, and multiple task wakeups. Across three generations,
the resulting c1 point-read gain was only 32.97--34.26 percent on the
workstation, 34.61--37.51 percent on N1, and 21.13--30.53 percent on E2. The
production implementation and all public selection were removed. Unary gRPC
remains the default and no framed compatibility surface was activated.

The current-head WP-623 preflight changes the release problem. Unary RiffDB now
passes the real-world mixed c32 throughput and p95 gates and the seed ceiling on
both cloud profiles:

| Host | c32 throughput / safe PG | c32 p95 / safe PG | seed / safe PG |
| --- | ---: | ---: | ---: |
| N1 | 0.944x | 1.241x | 3.76x |
| E2 | 0.935x | 1.120x | 2.47x |

The remaining failure is low-concurrency generated-operation latency. At c1,
point reads are 2.00--2.13x safe-application PostgreSQL on N1 and 1.61x on E2;
bounded lists reach 2.42x and 2.09x. N1 commands remain 1.20--1.40x.

An exact generated `GetTicket` diagnostic attributes only 124.5 microseconds
on N1 and 92.9 microseconds on E2 to all server stages, while the
outside-service residual is 415.3 and 387.7 microseconds. The complete 1.10x
safe-PostgreSQL budgets are 312.7 and 323.5 microseconds. The residual alone
therefore exceeds the complete budget. Eliminating every server query stage
could not make unary gRPC pass.

The measurements and value-free receipt hashes are recorded in
`docs/performance/wp-623-cloud-preflight.md`; WP-662's rejected-candidate
evidence is recorded in `docs/performance/wp-662-framed-transport-closure.md`.

This rules out another storage, entity, query-cache, result-carriage, writer,
or benchmark-driver campaign as the next release package. It also rules out
repeating ADR-0127 or ADR-0137 unchanged. A new candidate must remove the
per-operation HTTP/2 and internal session-actor scheduling floor, and it must
prove that mechanism before rebuilding a production protocol.

## Proposed Decision

### 1. Mechanics first: direct ownership must earn implementation

WP-663 first builds a diagnostic-only loopback and direct-TLS mechanics probe
outside production listener and generated-client selection. The probe uses the
same strict application request and response Protobuf payloads as unary gRPC,
but one caller task directly owns and polls one protected byte stream. There is
no client actor, internal request channel, correlation map, response router,
writer task, or out-of-order completion.

The probe runs three counterbalanced process generations on N1 and E2 with the
same exact release artifact. It must:

- reduce the c1 outside-service mean by at least 55 percent relative to unary
  gRPC on both hosts;
- reduce complete c1 exact generated point-read mean by at least 40 percent on
  both hosts;
- reduce direct connection setup plus first-operation mean by at least 25
  percent relative to a fresh unary HTTP/2 channel;
- show no greater-than-five-percent server-stage, RSS, CPU, startup, shutdown,
  seed, or unary-control regression; and
- close client encode, stream poll/write, server decode/adapt, application
  service, server encode/write, stream poll/read, client decode, and caller
  wakeup to within five percent of caller time.

The probe has fixed request and response bytes, fixed sample counts, finite
timeouts, and no authority, storage, or benchmark-only service bypass. If any
mechanics threshold fails or varies across order on either host, WP-663 stops.
No listener, protocol crate, generated selector, or public documentation may be
added from a failed probe.

### 2. A passing probe permits one exclusive application lane

Only after the mechanics gate passes may WP-663 add version 1 of the direct
exclusive generated-operation lane.

One connection binds exactly one database alias, credential presentation,
application lock, contract identity, finite canonically sorted query-module
set, protocol version, and transport ceiling. Exactly one operation may be in
flight. The client task that submits the operation directly polls the socket;
there is no hidden per-connection actor, multiplexer, correlation registry,
response reorder buffer, or internal request queue.

The lane carries only:

- session open and accepted/refused;
- one generated command or exact named-query request;
- one matching response or typed public failure; and
- controlled close.

It carries no kernel operation, administrative method, contract source,
ad-hoc RiffQL, raw entity/field/index/command-input ID, generic transaction,
caller-selected storage or durability option, compression, dynamic schema, or
extension payload.

The fixed canonical header contains one distinct magic, version, closed frame
kind, zero-only reserved flags, and bounded payload length. The rejected
ADR-0137 candidate's preface and ALPN identifier are not reused. Unknown
versions, kinds, flags, noncanonical headers, oversized lengths, truncation,
trailing bytes, unsolicited responses, a second request, or any byte after a
terminal close fail before application execution. Payloads remain existing
strict public Protobuf messages validated at the ordinary public boundary.

Connection bytes, one live request and response, buffers, setup duration, idle
duration, operation duration, total operations, and total lifetime all have
fixed server-owned ceilings. A client reaching the operation or lifetime bound
receives a controlled reconnect requirement between operations. No completed
result, request, authorization decision, or application value survives
delivery.

### 3. Concurrency is multiple bounded exclusive lanes

Concurrency uses a finite pool of exclusive connections, not multiplexing
inside one connection. Pool size remains a library/server ceiling and is never
an application transaction, consistency, durability, or ordering knob. A
generated operation leases one connection for exactly one call and returns it
only after a complete known response. A connection with an uncertain command
is destroyed rather than returned to the pool.

The native Rust async facade may expose an exclusive session requiring mutable
ownership. The ordinary cloneable facade and the first-party Rust driver host
may own a bounded pool of those sessions. Go, TypeScript, and Python continue
to reach the lane only through the Rust driver host; no target-language package
implements TLS, framing, authentication, retry, uncertainty, freshness, or
error semantics.

Pool saturation returns or waits under the existing finite client deadline and
overload taxonomy. It cannot open an unbounded replacement connection, queue
unbounded work, or silently fall back after a command becomes uncertain.

### 4. Existing ingress trust is reused without downgrade

The lane adds no insecure remote port and no transport knob.

- `direct_tls` uses the existing rustls provider, certificate/key files,
  trust-root, exact peer-name validation, and a new exact allowlisted ALPN.
- `loopback_cleartext` remains literal-loopback only and uses one distinct
  fixed preface under the existing byte and time bounds.
- `local_socket` retains its existing filesystem identity and permission
  checks. A remote proxy may carry only opaque TLS/TCP bytes and cannot assert
  a principal.

An absent, ambiguous, partial, slow, unknown, or downgraded preface/ALPN fails
closed. Rustls, `ring`, socket, and frame dependencies remain confined to the
reviewed ingress and client transport crates. Service, policy, command,
storage, projection, and deterministic-runtime graphs gain no cryptographic or
socket dependency. Existing architecture pins are amended by exact allowlist,
not deleted.

### 5. Credential proof may be pay-once; authority never is

Session open performs the expensive proof that the presented credential bytes
match one retained capability identity. The connection may retain only a
redacted authenticated-session proof containing the database, audience,
principal, capability locator and revision, and a domain-separated digest of
the presentation. It retains neither the credential bytes nor a positive
authorization result.

Every operation still reloads the exact current capability record and checks:

- database and audience;
- principal and actor kind;
- current capability identity and revision;
- revocation and expiry;
- active contract, role, and query-module identity;
- operation permission and command/query bounds;
- tenant/partition scope;
- row policy and relationship evidence;
- field and secret-output visibility;
- freshness/read-after-commit requirements; and
- response release under the current authority.

Any revision, revocation, expiry, identity, role, contract, module, or policy
change closes the session before the next result is released. The server does
not reinterpret a nearby record or refresh the session silently. Authentication
proof is proven per presented credential datum; authorization is proven per
operation. This is the standing pay-once rule, not cached allow.

The pay-once authentication proof is a separately measured stage. It may be
activated only if session rotation, revocation, expiry, database isolation,
credential replacement, and stolen/stale connection tests pass with byte-
identical public denials relative to unary gRPC.

### 6. Both transports share one semantic adapter

Unary gRPC and the exclusive lane invoke one first-party API-neutral generated-
operation adapter. It owns structural-to-domain conversion, current lifecycle
admission, per-operation authorization, application-service invocation,
response release, public failure construction, and domain-to-wire conversion.

The exclusive transport owns only ingress selection, bounded framing,
connection state, cancellation-by-close, and byte I/O. It receives no storage,
catalog, policy, plan, commit, projection, audit, clock, or idempotency port.
Architecture and dependency-direction tests enforce that boundary.

The shared adapter must not make unary slower by more than five percent. No
transport may call the other or implement a second semantic conversion path.

### 7. Ordering, freshness, durability, and uncertainty are unchanged

One in-flight operation does not imply a transaction or cross-operation
ordering guarantee.

Commands retain their exact command and idempotency identities, coordinator
admission, conflict ownership, atomic mutation/event/provenance/outcome record,
durable acknowledgement fence, typed outcome, and uncertainty recovery. If a
connection closes after command admission could have occurred and before a
known response is decoded, the client returns outcome-unknown and resolves
only with the original idempotency identity.

Queries retain exact contract/module/plan identity, current policy checks,
bounded execution, cursor, snapshot, and explicit freshness. A prior command's
response does not grant freshness; generated code continues to pass its
commit token as `read_after_commit`.

Cancellation closes the exclusive connection. Query work and command work
release only under their existing cancellation rules. A canceled or failed
connection is never pooled for reuse. Reconnect begins a new authenticated
session and cannot infer a previous result.

### 8. Activation is conjunctive and separately accepted

After the mechanics probe and full semantic corpus pass, the production
candidate runs three counterbalanced repetitions on workstation, N1, and E2.
Activation requires:

- every representative c1 generated query and command at most 1.10x same-run
  safe-application PostgreSQL;
- c32 mixed throughput at least 0.90x PostgreSQL and p95 at most 1.25x;
- seed at most 5.0x PostgreSQL;
- zero correctness, authorization, idempotency, uncertainty, freshness,
  cancellation, or lifecycle mismatch;
- no existing unary, c8, memory, CPU, startup, shutdown, setup, seed, or tail
  regression above five percent where a stricter gate does not apply; and
- stable results under counterbalanced order on both cloud profiles.

Passing WP-663 does not itself amend `PERF-018` or generated-client defaults.
It produces an exact comparator/default proposal for separate human
acceptance. Unary gRPC remains supported for compatibility and control-plane
diagnosis. Failing any threshold removes every production exclusive-lane path
and leaves only a value-free failed-candidate receipt.

## Options Considered

1. **Continue server/storage micro-optimization:** rejected as the next step.
   The measured outside-service residual alone exceeds the full parity budget.
2. **Retry ADR-0127 multiplexed HTTP/2 sessions:** rejected by WP-660 and
   WP-661's cross-profile evidence.
3. **Retry ADR-0137's actor-owned framed multiplexer:** rejected by WP-662's
   three-generation mechanics evidence.
4. **Relax the 1.10x unary gate:** deferred. The current evidence identifies a
   customer-paid mechanism large enough to investigate before changing the
   product requirement.
5. **Direct exclusive generated-operation lane:** proposed because it removes
   both HTTP/2 stream machinery and the candidate's internal client actor while
   matching PostgreSQL's one-session/one-current-operation comparison shape.

## Consequences

- The candidate is simpler per connection but uses multiple connections for
  concurrency.
- The public implementation cost is a second versioned application transport;
  reject-first mechanics must earn that cost before it exists in production.
- Cancellation destroys a connection rather than multiplexing a cancel frame.
- Authentication proof can be amortized only across the exact presented datum;
  every mutable authority fact remains per operation.
- This work does not reopen seed parity, writer serialization, durable formats,
  entity caching, query-language design, or target-language protocol ownership.

## Compatibility

No durable record, journal, storage key, backup, export, changelog, contract
grammar, contract/query IR, bundle/module/plan hash, or generated application
method changes. Existing unary gRPC clients and servers remain compatible.

ADR-0137's candidate never activated, and WP-662 removed its listener,
protocol, fixtures, client API, driver-host selection, and documentation. This
record therefore creates no migration from or compatibility with that rejected
wire shape. It uses a distinct preface and ALPN so stale experiment binaries
cannot negotiate accidentally.

A server not advertising the exclusive lane continues serving unary gRPC. A
client may select the lane only before submitting an operation and may fall
back only before any command request bytes are written. After possible command
submission, fallback is forbidden and uncertainty rules apply.

## Security

The transport receives exactly the accepted TLS and credential boundary and
adds no cleartext remote, trust-all, proxy-principal, compression, mTLS,
cipher, or application-controlled security knob. Strict frame validation
precedes allocation and application admission. Credentials, payloads,
identities, paths, peer prose, policy inputs, and results never enter telemetry
or public diagnostics.

Slowloris, oversized, truncated, repeated-request, unsolicited-response,
connection-flood, pool-exhaustion, stale-proof, rotation, revocation, expiry,
and downgrade cases fail closed under fixed byte/time/resource limits. Hidden
rows and operations remain indistinguishable under the existing public error
and timing-inference rules.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** Applications continue to invoke
  only generated typed commands and exact named queries. They cannot express a
  transaction, generic operation, durability/freshness downgrade, cached
  authority, transport fallback after uncertainty, raw identifier, or storage
  choice. Connection pooling and authentication proof scope are first-party
  implementation details with conservative typed failure.
- **Scale:** Every connection, pool, frame, payload, buffer, operation, wait,
  setup, idle period, lifetime, cancellation, and telemetry family is bounded
  independently of database size and process duration. The design assumes no
  full-state cache or rewrite. Multiple server nodes may implement the same
  lane later because protocol semantics depend only on published application
  service facts, not local storage bytes.

## Testing

- Diagnostic-only direct-stream mechanics fixtures and closed stage ledgers on
  N1/E2 before production integration.
- Golden and strict-message fixtures for every frame and boundary value;
  fragmentation/coalescing and parser fuzzing over every byte boundary.
- Architecture tests for one in-flight operation, no actor/channel/router,
  dependency confinement, no authority ports, and no target-language protocol.
- Deterministic schedules for cancellation/close before admission, during
  evaluation, before/after durability, before response write, during response
  decode, pool return, reconnect, and graceful shutdown.
- Process tests for loopback preface, direct TLS ALPN and peer identity, local
  socket, layer-4 proxy, slow peer, connection caps, operation/lifetime caps,
  overload, restart, and shutdown drain.
- Unary/exclusive byte-semantic equivalence for commands, idempotent replay,
  uncertainty, named reads, cursors, read-after-commit, typed errors, row/field/
  secret policy, rotation, revocation, expiry, and contract/module/role drift.
- Rust, Go, TypeScript, and Python driver-host conformance using the same Rust
  transport implementation.
- Counterbalanced workstation/N1/E2 mechanics and complete `PERF-018` matrix
  only after every reject-first gate passes.

## Requirements and Work Packages

- **Requirements:** `API-001`, `PERF-005`, `PERF-008`, `PERF-018`, `NET-001`
  through `NET-012`, and `DRV-001` through `DRV-014`.
- **Defines or blocks:** WP-663 and WP-623.
- **Final evidence:** WP-623, WP-578, and WP-579 after a separately accepted
  comparator/default amendment.

## Decision Deadline

The maintainer accepted this exact decision text on 2026-08-22 for commit
`f580be6e`. WP-663 may now implement the diagnostic gate and may proceed beyond
it only under the reject-first conditions above.
