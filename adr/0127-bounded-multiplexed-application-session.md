# ADR-0127: Bounded Multiplexed Application Session

- **Status:** Proposed
- **Direction approved:** 2026-08-15 (maintainer)
- **Exact text accepted:** No
- **Decision deadline:** Before WP-633 adds a session RPC or changes the
  `PERF-018` transport/client freeze
- **Requires:** ADR-0040, ADR-0055, ADR-0056, ADR-0105, ADR-0106, and ADR-0123
- **Defines or blocks:** WP-633 and any session-shaped `PERF-018` evidence

Direction approval authorizes attribution and this draft. It does not authorize
the protocol, comparator amendment, or implementation until the maintainer
accepts this exact text.

## Context

The portable-cloud gate compares RiffDB's generated application client with a
safe-application PostgreSQL client. Both keep connections alive, but their
operation transports differ. PostgreSQL carries many operations over one
long-lived session protocol. RiffDB currently starts one HTTP/2 unary request
and response exchange for every generated command or named query, even though
the underlying channel is pooled and persistent.

WP-627 found the same low-concurrency gap on Intel N1 and AMD E2 hosts: about
0.59 times the safe PostgreSQL comparator at one client and 0.58--0.60 times at
eight clients. Named reads do not wait for derived catch-up, and the c=32 server
uses only about 52--56 percent of host CPU. At c=1, client-observed latency
exceeds all recorded server stages by hundreds of microseconds. The first
follow-up must distinguish the frozen benchmark's synchronous-to-asynchronous
bridge from customer-paid Tonic, Protobuf, HTTP/2, and service-orchestration
cost. Removing a benchmark artifact cannot count as product improvement.

`PERF-018` freezes transport/client shapes. A diagnostic asynchronous shadow is
permitted, but it cannot replace the frozen comparator or emit release
evidence. If WP-632 proves that unary exchange is a material product cost, a
long-lived multiplexed application session is a legitimate product option, not
a benchmark-only shortcut: it gives the RiffDB application protocol the
persistent request stream shape already present in the comparator.

The session must not become a transaction, authorization cache, raw protocol
escape hatch, or source of hidden ordering semantics. Every operation must
continue through the same API-neutral application service, authorization,
runtime, commit coordinator, idempotency, freshness, and structured-error
boundaries as the unary path.

## Proposed Decision

### 1. Add one optional application-operation session

The public application protocol adds one bidirectional streaming RPC dedicated
to generated named application operations. Opening the stream selects exactly:

- one canonical database alias;
- one authenticated application credential presentation;
- one exact application lock identity and supported session protocol version;
- one finite server-advertised in-flight ceiling no greater than 128; and
- the existing request, response, recursion, collection, and diagnostic byte
  ceilings.

The stream accepts only closed envelopes for named commands and exact named
queries already available through the stable application client. It exposes no
kernel read, administration, ad-hoc RiffQL, role binding, deployment, raw
Protobuf method selection, numeric symbol, field mask, transaction callback, or
caller-selected storage behavior.

Every envelope carries a session-unique nonzero correlation ID and exactly one
ordinary operation request. Responses may complete out of submission order and
must carry the matching correlation ID. Duplicate live correlation IDs,
unknown envelope kinds, oversized messages, or work above the negotiated
in-flight ceiling close or reject through a typed bounded protocol error before
application execution.

### 2. A session is transport multiplexing, never a transaction

Each envelope is independently authenticated, authorized, admitted, executed,
committed, audited, and released through the existing application service.
Opening a stream grants no snapshot, lock, transaction, batch atomicity,
ordering, or durable session state. Two commands on one stream may conflict or
complete in either order exactly as two concurrent unary commands do.

Commands retain their existing command identity and idempotency key. Queries
retain explicit read-after-commit and cursor inputs. A caller that needs causal
read-after-write behavior supplies the committed frontier exactly as today;
stream order never implies freshness. No API may request relaxed durability,
skip authorization, or reuse an earlier policy decision.

The server retains the bounded credential presentation only for the life of
the stream in the existing redacting protected type. Before every operation it
re-runs ordinary authentication and authorization safe points against current
capability state. Revocation, expiry, role/contract drift, database retirement,
or policy change therefore affects the next operation and may close the
session with the existing typed cause. The server never persists an allow
decision from stream establishment.

### 3. Cancellation and uncertainty remain per operation

A cancel envelope names one live correlation ID. Cancellation releases all
non-durable work for that operation. It may not report a command as cancelled
once durability is uncertain or known; the client resolves that command using
the existing idempotency identity and outcome-recovery rules.

Stream loss creates no durable response queue and no implicit replay. Queries
may be resubmitted. Commands may be resubmitted only with their original
idempotency identity and input. The stable client performs the same bounded
retry/uncertainty classification as the unary path. Reconnection opens a new
session generation and cannot reuse correlation IDs to infer an outcome.

### 4. Bounds, fairness, and backpressure are protocol facts

The client and server each enforce fixed finite limits for in-flight
operations, queued response bytes, per-operation bytes, and session lifetime
work. The alpha exposes no application knob above those ceilings. When a limit
is reached, the client stops polling new application futures or returns the
existing typed local/server capacity outcome; neither side creates an
unbounded task, channel, response buffer, or retry queue.

One session cannot reserve coordinator capacity ahead of independently
authorized work. Admission remains fair across sessions and unary callers.
Connection-level flow control may delay delivery but cannot change command
ordering, durability, or authorization. A slow response consumer is closed at
the finite byte/time bound without cancelling already durable commands.

### 5. Trust and wire policy are unchanged

Remote sessions use ADR-0105 TLS, exact peer-name verification, capability
credentials, database selection, deadlines, message limits, and compression
policy. The session adds no cleartext remote fallback, mTLS identity system,
cipher knob, trust-all mode, application-selected keepalive, or protocol
compression.

`TCP_NODELAY` remains enabled on both client and server sockets. WP-632 freezes
that fact against the exact Tonic dependency before session implementation so
Nagle behavior is not mistaken for an architectural result.

The RPC and envelopes receive additive Protobuf fields and golden fixtures.
The unary application methods remain supported for compatibility,
administrative diagnosis, and servers that do not advertise the session
feature. A generated client may prefer the session only after exact feature
negotiation. Fallback must preserve semantics and report its selected transport
shape in diagnostics; it may not silently weaken an operation or mix envelopes
between application identities.

### 6. PERF-018 changes only through a receipted comparator amendment

WP-632's asynchronous shadow is permanently non-evidentiary under the current
freeze. If session implementation is accepted and passes semantic conformance,
the `PERF-018` comparator must be amended explicitly before session results can
qualify:

- freeze the exact session protocol/client version, connection count,
  in-flight ceiling, retry budget, dataset, weights, and PostgreSQL shapes;
- rerun all PostgreSQL and RiffDB cells in the same release campaign rather
  than splicing old comparator results into a new RiffDB shape;
- report unary and session results side by side during the transition; and
- book only Tonic/Protobuf/orchestration reductions as product gains.
  Synchronous benchmark-bridge removal remains measurement correction and is
  disclosed separately.

Session activation requires material public improvement on both inventoried
cloud CPU families with correctness unchanged. Before implementation, WP-633
must predeclare per-cell bounds from WP-632. At minimum, the session candidate
must improve c=1 by 15 percent and c=8 by 10 percent, must not regress c=32,
seed, unary commands, or tail latency by more than five percent, and must leave
all authorization, revocation, uncertainty, and read-after-commit tests exact.
Passing those candidate gates does not itself waive the independent
`PERF-008` release ratios.

## Options Considered

1. **Keep unary gRPC and optimize only server CPU:** retained as the default if
   WP-632 finds unary exchange immaterial. Current evidence says spare CPU and
   low-concurrency latency deserve transport attribution first.
2. **Change only the benchmark to native async:** rejected as a product claim.
   It is useful diagnostic evidence, but `PERF-018` freezes the comparator and
   artifact removal is not customer-paid improvement.
3. **Expose a generic transaction/session API:** rejected. It would violate the
   command-only and safe-interface boundaries and introduce implicit ordering,
   authority, and recovery states.
4. **Use a bounded generated-operation stream:** proposed because it amortizes
   per-unary transport work while preserving one independently checked
   application operation per envelope.

## Consequences

- Low-concurrency application calls can amortize HTTP/2 request lifecycle and
  service ingress orchestration over one bounded stream.
- The public protocol gains an additive session version, envelope family,
  conformance corpus, and another transport path that must remain semantically
  identical to unary calls.
- Unary RPCs remain available and cannot be removed during alpha.
- There is no session transaction, ordered-command promise, durable response
  queue, exactly-once delivery claim, or authorization cache.
- If WP-632 does not find enough customer-paid unary overhead to satisfy the
  predeclared candidate gates, WP-633 is not activated.

## Compatibility

The proposal is additive to public Protobuf and generated client runtime APIs.
It changes no contract grammar, RiffQL, command/query IR, bundle/module/plan
hash, durable record, export, backup, changelog, or replication format. Old
clients continue using unary methods; new clients negotiate the session feature
and fall back only to semantically equivalent unary operations.

Accepting this ADR does not by itself amend `PERF-018`. That requires the
receipted comparator update in the implementing work package and fresh evidence
for every backend.

## Security

TLS and credential trust remain owned by first-party Rust. The stream retains
only one bounded redacted credential presentation and revalidates current
authority per operation. Session establishment never grants data authority.
Errors, telemetry, correlation IDs, and close reasons remain bounded and
redaction-safe; request values cannot enter metrics or protocol diagnostics.

Malformed, replayed-live, cross-database, cross-identity, oversized, stale, or
unauthorized envelopes fail closed. Stream closure cannot convert an uncertain
command into a safe retry or expose whether a hidden row/event exists.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** Applications can express only
  generated named commands and exact named queries with their existing typed
  inputs. The stream offers no transaction, ordering, durability, policy,
  freshness, credential, endpoint, or raw-method control. Every envelope
  traverses the same authorization and semantic service as unary execution.
- **Scale:** Per-session tasks, in-flight work, queued bytes, and response
  buffers are finite. The design assumes neither co-located storage nor a
  single database node; a later router may terminate or forward sessions while
  preserving per-operation identity. No full-state rewrite or unbounded
  per-session history is introduced.

## Testing

- Golden unary/session semantic corpus for every command, named query, typed
  outcome, error, and read-after-commit result across Rust, Go, TypeScript, and
  Python generated clients.
- Deterministic schedules for out-of-order completion, duplicate correlation
  IDs, cancellation at every durability boundary, queue saturation, slow
  consumers, stream loss, reconnect, retry, replay, and graceful shutdown.
- Authorization tests for revocation, expiry, role revision, row-policy change,
  contract/module drift, database selection, and cross-identity envelope
  rejection while a stream is live.
- Protobuf fuzzing and byte/collection/in-flight boundary tests.
- Architecture tests proving the session adapter calls only the API-neutral
  application service and exposes no kernel/admin/storage dependency.
- N1 and E2 short candidate cells followed by the complete amended
  `PERF-018` matrix only after semantic gates pass.

## Requirements and Work Packages

- **Requirements:** existing `PERF-008` and `PERF-018`; additive session
  requirements are registered only after exact acceptance.
- **Defines or blocks:** WP-633.
- **Final evidence:** the future amended `PERF-018` package and alpha gate.

## Decision Deadline

Exact acceptance is required before any session RPC, envelope, generated
runtime method, feature advertisement, or comparator-shape amendment merges.
