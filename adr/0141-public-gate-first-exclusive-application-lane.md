# ADR-0141: Diagnostic-Gate-First Exclusive Application Lane

- **Status:** Accepted
- **Direction approved:** Yes
- **Exact text accepted:** Yes
- **Decision deadline:** Before WP-670 restores a direct application listener,
  frame implementation, ALPN, client selector, or candidate wire identity
- **Requires:** ADR-0040, ADR-0055, ADR-0056, ADR-0070, ADR-0105, ADR-0106,
  ADR-0120, ADR-0123, ADR-0127, ADR-0132, ADR-0133, ADR-0137, ADR-0138,
  ADR-0139, and ADR-0140
- **Defines or blocks:** WP-670, WP-671, WP-672, WP-623, WP-578, and WP-579

The maintainer approved the diagnostic-first direction on 2026-08-23 and
accepted the exact text in commit `839b1f2e`. This decision neither revives a
rejected wire identity nor amends `PERF-018`.

## Context

WP-663 through WP-665 tested three implementations of persistent generated
operations and removed all of them under their accepted reject-first rules.
Those failures remain authoritative:

- the direct exclusive lane missed ADR-0138's combined TLS-setup/first-call
  reduction even while established verified-TLS calls improved 48.51 percent
  on N1 and 52.37 percent on E2;
- the corrected established-connection lane missed one E2 steady proxy cell
  and one E2 first-operation proxy cell by narrow margins; and
- the serial gRPC session removed only 31.13 percent of outside-service cost
  against ADR-0140's 50-percent minimum.

The proxy thresholds did their job: they prevented a second public transport
from entering the product without stable evidence. They were not product
requirements and their failure cannot be silently reinterpreted as a pass.

Current-head revision `c8d2e5fb` still fails the actual alpha requirements.
Short same-run c32 cells reach 0.873x safe-application PostgreSQL on N1 and
0.866x on E2, with p95 ratios of 1.290x and 1.250x. Representative unary
operations range from 1.40x to 3.79x, and `board_page_450` remains 1.74x to
1.99x. Correctness is clean and seed remains below its 5.0x ceiling.

An exact generated `GetTicket` ledger attributes 492 microseconds of a
634-microsecond async unary call on N1 to outside-service work. A following E2
process attributes 777 microseconds of a 1,050-microsecond call to the same
family. The synchronous benchmark bridge contributes only 4--13 microseconds.
Storage/query-only work cannot fit the small-operation budget while that
residual remains. Conversely, transport alone cannot close the large-result
cell.

The prior direct-lane evidence is therefore useful as an existence proof but
insufficient as activation evidence. Repeating full production integration
before asking whether the candidate closes the public unary gap would repeat
the build/remove cycle that caused the current churn. This decision instead
permits one private candidate identity whose first purpose is a decisive
customer-shaped measurement. A minimum safe prototype is followed immediately
by the complete unary matrix. Only a successful small-operation verdict permits
production hardening.

This ordering does not lower a product threshold, credit a benchmark artifact,
or claim that one transport fix completes alpha. It explicitly separates the
small-operation transport question from the already measured large-result
question.

## Proposed Decision

### 1. A new candidate starts from no compatibility entitlement

WP-670 may implement version 2 of a direct exclusive generated-operation lane.
It must use a new fixed magic, version, ALPN, fixtures, and feature identity.
No byte, protocol name, selector, or compatibility promise from the removed
WP-662 through WP-665 experiments is reused or grandfathered.

The candidate carries only:

- one exact database/application/credential session establishment;
- one generated command or exact named-query request at a time;
- one matching typed response or public failure; and
- one controlled close.

It carries no kernel/admin operation, source, ad-hoc query, raw numeric ID,
generic transaction, caller-selected field/predicate/index, durability or
freshness option, compression, dynamic schema, or extension payload.

The fixed canonical header is versioned and independently bounded. Unknown
magic, version, kind, flag, size, truncation, trailing bytes, unsolicited
response, second live request, or bytes after terminal close fail before
application execution. Payloads are the existing strict public Protobuf
messages and retain their current validation and response budgets.

### 2. One caller directly owns one protected lane

Exactly one operation may be in flight on a lane. The submitting first-party
Rust task directly polls the protected stream and the matching response. The
steady operation path contains no client actor, internal request channel,
correlation map, response router, writer task, out-of-order completion, or
per-operation task spawn.

Concurrency uses a fixed finite pool of independent exclusive lanes. A lane is
returned only after one complete known response. Saturation is bounded by the
existing client deadline and typed overload taxonomy. A lane with a cancelled,
malformed, disconnected, or uncertain command is destroyed, never reused.
Pool size, ready minimum, buffers, setup, idle time, operation count, lifetime,
replacement rate, and total retained bytes are server/library ceilings and not
application knobs.

The native Rust client owns the protocol. Go, TypeScript, and Python continue
through the first-party Rust driver host and may not implement TLS, framing,
authentication, retry, cancellation, uncertainty, or freshness themselves.

### 3. Trust is reused; authority remains current per operation

The lane adds no insecure remote port or trust choice.

- `direct_tls` uses the existing rustls/ring provider, certificate and key,
  trust root, exact peer name, TLS defaults, and one new exact ALPN.
- `loopback_cleartext` remains literal-loopback only and uses one distinct
  fixed preface under existing byte/time limits.
- `local_socket` retains filesystem identity and permission checks. A remote
  proxy may carry only opaque TLS/TCP bytes and cannot assert a principal.

Unknown, absent, ambiguous, partial, slow, or downgraded protocol selection
fails closed. Crypto and sockets remain confined to the reviewed ingress and
client crates; service, policy, command, storage, projection, and deterministic
runtime graphs gain no such dependency.

Session establishment may retain only a redacted proof that the exact
presented credential bytes matched one capability locator/revision for the
selected database and audience. Credential bytes and positive authorization
decisions are not retained.

Every operation reloads and checks current capability identity/revision,
revocation, expiry, audience, database, principal, role, contract, module,
operation permission, tenant/partition, row and relationship policy, field and
secret visibility, freshness/read-after-commit, and response release. Any
drift may refuse or close the lane before release. Authentication proof is per
credential datum; mutable authority remains per operation.

### 4. One API-neutral service owns semantics

Unary gRPC, the accepted bounded gRPC session, and the exclusive candidate
invoke the same first-party API-neutral generated-operation adapter. That
adapter owns structural conversion, lifecycle admission, authentication,
current authorization, application-service invocation, response release,
public failure construction, and response conversion.

The exclusive transport owns only protected ingress, bounded framing, lane
state, cancellation-by-close, and byte I/O. It receives no storage, catalog,
policy, plan, commit, projection, audit, clock, or idempotency port. Neither
transport calls another. Architecture and dependency tests enforce this
boundary and pin the semantic adapter independent of Tonic, HTTP, TLS, socket,
and frame engines.

### 5. Durability, freshness, cancellation, and uncertainty do not change

Commands retain exact command/idempotency identity, coordinator admission,
conflict ownership, atomic mutation/event/provenance/outcome, durable
acknowledgement, audit, and outcome recovery. If request admission may have
occurred and a known response is unavailable, the result is outcome-unknown;
only the original idempotency identity may resolve it. There is no automatic
fallback after possible submission.

Queries retain exact contract/module/plan identity, compiler bounds, current
row/field policy, cursor, snapshot, explicit freshness, and response release.
Lane order is not a transaction or freshness guarantee. Generated causal reads
continue to pass the returned commit frontier explicitly.

Cancellation closes the lane. It releases query/command work only under the
existing rules and never turns uncertainty into a safe retry. Reconnect creates
a new authenticated generation and infers no prior result.

### 6. Build only the minimum safe diagnostic before the public verdict

Before editing protocol code, WP-670 must produce current-head N1 and E2
ledgers for every representative unary scenario. For each operation it
predeclares:

- the current API-neutral service time;
- the outside-service time the lane can remove;
- the same-run 1.10x PostgreSQL budget; and
- the candidate mean required to fit that budget.

The first implementation is private diagnostic machinery, not a production
selector or documented application feature. It contains only the strict frame
parser, protected connection, shared API-neutral adapter invocation, one
in-flight request, controlled close, and the instrumentation needed to close
caller time. It must not add public configuration, generated defaults, target-
language selection, compatibility promises, automatic fallback, pool tuning,
or handbook claims.

Before full hostile-input, lifecycle, pool, target-language, and release-matrix
hardening, the private candidate runs three counterbalanced loopback and
verified-TLS generations on both hosts. The run includes the complete existing
public unary scenario matrix, not only `GetTicket`. It stops and is removed if:

- any minimum semantic-equivalence, malformed-frame, authorization,
  uncertainty, or bounded-resource test fails;
- client/server stage sums fail to close caller time within five percent;
- cold trust plus session establishment is more than five percent slower than
  same-run unary gRPC;
- established operation mean does not improve at least 25 percent for the
  representative point read on both hosts in every generation;
- API-neutral service time, CPU, RSS, startup, shutdown, seed, or unary control
  regresses more than five percent; or
- any representative small read or command remains above 1.10x same-run safe-
  application PostgreSQL; or
- the measured service-only floor for any such miss cannot arithmetically fit
  its PostgreSQL budget even with the remaining transport residual removed.

For this decision, a small operation is an existing point, bounded list,
bounded detail, or command scenario whose encoded response is at or below the
frozen ordinary-response budget. Board scenarios and any operation above that
budget are large-result scenarios. The classification comes from encoded byte
size, not operation name, observed latency, or whether a result is favorable.

The verdict report must show, for every scenario, same-run PostgreSQL mean and
p50, candidate mean and p50, API-neutral service time, outside-service
residual, encoded request/response bytes, and the remaining microseconds needed
to reach 1.10x. This is the decision ledger: if a small-operation miss is
service-owned, transport work ends. If all small operations pass, the direct
lane has earned production hardening while large results remain separately
owned.

The 25-percent coarse falsifier prevents an immaterial second transport. It
does not replace the unary verdict and does not retroactively weaken the larger
proxy thresholds in ADR-0138 through ADR-0140.

### 7. Small-operation success permits hardening, not activation

Only a complete small-operation pass permits a later WP-672 to build the finite
lane pool, exhaustive hostile-wire and lifecycle matrix, target-language
driver-host integration, production selector, replacement behavior, shutdown
evidence, and full architecture pins described in Decisions 1 through 5. A
miss removes the private protocol implementation and records the closed ledger;
no nearby body, partial frame, or experimental identity is retained in
production.

Board and other large-result scenarios are never waived. WP-671 profiles
`board_page_450` under the same private candidate while it exists and closes a
result-path stage ledger covering result assembly, windowing, hydration,
encoding, response release, and transport. It may propose one separately
reviewed result-path package only when the ledger identifies a dominant
movable stage and the predicted public gain fits the gate. It may not change
transport, cache authority, or introduce an unbounded result surface.

WP-672 is conditional: it may start only after WP-670 records a complete small-
operation pass and WP-671 either closes the large-result gate or has an exact
accepted result-path dependency that can do so. After that hardening and any
separately accepted large-result fix, candidate activation requires, in every
counterbalanced generation on both N1 and E2:

- every representative generated unary scenario at most 1.10x
  safe-application PostgreSQL;
- c32 mixed throughput at least 0.90x PostgreSQL and p95 at most 1.25x;
- seed at most 5.0x PostgreSQL;
- zero correctness, authorization, idempotency, freshness, uncertainty,
  cancellation, or lifecycle mismatch; and
- no otherwise-uncovered regression above five percent.

`board_page_450` and every other large-result scenario remain in the matrix.
Transport savings may not hide or waive a result-set miss. While WP-671 is
open, WP-670 may preserve the implementation only as private diagnostic code
without a selected default, generated exposure, compatibility entitlement, or
claim of availability.

Passing the short matrix still does not amend `PERF-018` or generated defaults.
The complete workstation/N1/E2 90-second evidence must pass, after which a
separate exact human decision freezes the protocol version, trust profiles,
pool shape, retry budget, dataset, weights, and comparator/default selection.
Unary gRPC and control-plane RPCs remain supported.

## Options Considered

1. **Run WP-623 now:** rejected. Current-head short evidence already misses
   known unary, c32, tail, and large-result gates; longer windows cannot make
   the artifact eligible.
2. **Continue small server/storage optimizations:** rejected as the next
   package. The outside-service residual alone exceeds the point-read budget.
3. **Declare earlier proxy misses irrelevant:** rejected. Their exact evidence
   remains valid and motivates a new identity and new accepted decision.
4. **Relax `PERF-008`:** rejected. The measured direct-ownership mechanism is
   still large enough to test before renegotiating product performance.
5. **Activate direct framing on improvement alone:** rejected. A second public
   transport must earn its cost through the actual complete product gates.
6. **Fully harden another proxy-gated transport before measuring it:** rejected.
   This repeats the build/remove cycle without answering the public question.
7. **One diagnostic-gate-first exclusive candidate:** proposed. It attacks the
   dominant measured small-operation residual, obtains the complete unary
   verdict immediately, and permits hardening only after it proves product
   value.

## Consequences

- The candidate repeats only enough implementation work from removed
  experiments to obtain a decisive product-relevant result under a new private
  identity.
- A failed candidate is cheaper: it stops before pool, driver-host, public
  configuration, exhaustive lifecycle, and release integration work.
- RiffDB may still need separate large-result work before any transport can
  activate; that miss stays visible.
- A passing lane adds a second versioned application data-plane transport and
  finite connection-pool lifecycle complexity.
- A failed lane leaves unary gRPC and all durable/public identities unchanged.
- Cold TLS remains customer-visible, bounded, and regression-gated; it is not
  credited as an application-operation improvement.

## Compatibility

This proposal changes no contract/RiffQL grammar, IR, bundle, module, plan,
generated operation, durable record, journal, backup, export, changelog,
replication, or target-language value format.

Unary gRPC and ApplicationSession V1 remain byte- and behavior-compatible. The
exclusive candidate is additive only after every activation decision. Old
servers continue serving gRPC and do not advertise the new ALPN/preface. A
client may fall back only before writing possible command bytes.

The proposed frame identity is new and versioned. Rejected experiment bytes
have no compatibility status and are not accepted as aliases.

## Security

Existing TLS, peer verification, capability, database, deadline, message-size,
and no-compression rules remain exact. There is no trust-all verifier,
cleartext remote listener, alternate crypto provider, 0-RTT, proxy principal,
or target-language transport.

Strict frame and Protobuf validation precede allocation and application
admission. Credentials, payloads, identities, paths, peer prose, policy facts,
and results do not enter diagnostics. Telemetry uses fixed frame/lane states,
bounded counts/bytes, and duration buckets only.

Slowloris, oversized/truncated frames, duplicate operations, response floods,
connection floods, saturation, stale proof, rotation, revocation, expiry,
downgrade, and reconnect fail closed without disclosing hidden rows,
principals, operations, or policy facts.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** Applications continue to invoke
  only generated typed commands and exact named reads. They cannot express a
  transaction, generic query, raw ID, cached authority, durability/freshness
  downgrade, retry after uncertainty, transport fallback after submission,
  pool dimension, trust choice, or storage behavior. All semantic and mutable
  authority safe points remain in the shared application service.
- **Scale:** Connections, lanes, frames, payloads, buffers, pool state,
  operation/lifetime counts, waits, retries, replacements, telemetry, and
  evidence are fixed-bounded independently of data/history size and runtime.
  No full-state/result cache or co-located-storage assumption is introduced.

## Testing

- Golden strict-frame/Protobuf fixtures, parser fuzzing, fragmentation and
  coalescing, every size/flag/version/kind boundary, and hostile peers.
- Architecture tests for one live operation, direct caller ownership, no
  actor/channel/router, Rust-only protocol/trust, dependency confinement, and
  absence of semantic authority in the transport.
- Unary/session/exclusive equivalence for commands, replay, uncertainty,
  queries, cursors, compact results, read-after-commit, typed errors, row/field/
  secret policy, revocation, expiry, and contract/module/role drift.
- Deterministic cancellation/close schedules before admission, during
  evaluation, before/after durability, before response write, during decode,
  pool return, reconnect, rotation, restart, and shutdown.
- Fixed resource tests for slow peers, pool exhaustion, operation/lifetime
  caps, reconnect storms, output stalls, and clean shutdown.
- Counterbalanced N1/E2 complete-unary diagnostic matrix immediately after the
  minimum safe candidate, including per-scenario service/residual/byte ledgers.
- BoardPage450 stage profiling under the private candidate, owned by WP-671.
- Full hostile/lifecycle/driver and workstation/N1/E2 matrices only after the
  small-operation verdict passes; full `PERF-018` only after all short product
  gates pass.

## Requirements and Work Packages

- **Requirements:** `API-001`, `PERF-005`, `PERF-008`, `PERF-018`, `SEC-001`,
  `SEC-002`, `NET-001` through `NET-012`, and `DRV-001` through `DRV-014`
- **Defines or blocks:** WP-670, WP-671, WP-672, and WP-623
- **Final evidence:** WP-623, WP-578, and WP-579 after a separately accepted
  comparator/default amendment

## Decision Deadline

The maintainer accepted this exact decision on 2026-08-23. WP-670 may restore
only the private diagnostic listener, frame implementation, ALPN, and candidate
wire identity authorized above; public selection and activation remain blocked
on WP-671, WP-672, and a separate exact decision.
