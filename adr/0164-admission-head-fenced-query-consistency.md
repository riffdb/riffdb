# ADR-0164: Admission-Head-Fenced Query Consistency

- **Status:** Accepted
- **Direction approved:** 2026-08-27
- **Exact text accepted:** Yes, 2026-08-27
- **Accepted:** 2026-08-27
- **Acceptance reference:** Maintainer exact-text acceptance in the current
  Codex session for upstream commit `60050745`
- **Decision deadline:** Before WP-719 adds a public query-consistency option,
  captures a server-side admission head, changes cursor consistency identity,
  or adds a request-protocol field
- **Requires:** ADR-0004, ADR-0010, ADR-0051, ADR-0052, ADR-0053,
  ADR-0055, ADR-0070, ADR-0071, ADR-0086, ADR-0108, ADR-0111, ADR-0124,
  ADR-0130, ADR-0134, ADR-0141, ADR-0150, ADR-0158, and ADR-0159
- **Amends if accepted:** ADR-0086 by adding the stronger caller-requested
  admission-head floor below without weakening `Available`, explicit causal,
  or compiler-declared bounded-freshness semantics
- **Defines or blocks:** WP-719 and WP-720

This record is authoritative for WP-719 and WP-720.

## Context

RiffDB generated query APIs can require `read_after_commit` when a caller holds
one concrete commit sequence. Projected and maintained exact-result providers
then select a snapshot or common epoch at or beyond that floor, or return a
typed freshness/lifecycle outcome.

Some authoritative application interfaces expose only a consistency
preference. Their caller can ask for a higher-consistency read but receives no
database commit token to submit. Default RiffDB reads may use the currently
servable projection/provider frontier, so an acknowledged write from another
adapter instance can remain temporarily invisible. Caching a process-local
"last commit" fence is incorrect across processes, servers, restarts, and
independent client instances.

The server already owns the missing fact: the authoritative application head
when a query is admitted. A stronger read can capture that head once and require
the selected authoritative snapshot or derived provider epoch to reach it.
That produces a useful and explainable guarantee without making the caller
manufacture a sequence.

The capture must be once per logical query, not once per backend page. If each
cursor continuation captured a newer head, sustained writes could force a page
walk to chase a moving frontier indefinitely and could change the snapshot
between pages. The first page must negotiate one sufficient snapshot; its
opaque cursor must retain that snapshot and consistency identity.

This guarantee is "fresh through admission," not linearizability. A commit
published after the capture is not required to appear. It is also not a reason
to hide unbounded waiting, silently fall back to stale data, or expose provider
selection to applications.

## Proposed Decision

### 1. Add one stronger typed query option

Every generated application query surface gains one closed consistency value:
`AdmissionHead`. In Rust the common conceptual operation is
`QueryOptions::at_least_admission_head()`; Go, TypeScript, Python, CLI, MCP,
local-driver, and remote-gRPC surfaces expose their language-idiomatic spelling
of the same generated enum/value.

`AdmissionHead` means:

> On the first invocation of one logical query, return only from a snapshot or
> provider epoch whose frontier is at least the authoritative application head
> captured by RiffDB after successful admission and initial authorization.

The value strengthens a read only. It cannot disable an explicit
`read_after_commit`, inherited causal session fence, source-declared freshness
rule, row policy, or provider requirement. An omitted value retains the exact
existing query behavior. No `eventual`, `stale`, `skip_wait`, provider, epoch,
head, timeout, fallback, or isolation-level selector is added.

### 2. Capture the floor once at the service boundary

For a first-page or non-cursor request, the API-neutral application service:

1. authenticates the request, resolves the exact application/catalog/plan, and
   completes initial operation authorization;
2. observes the authoritative committed application head once;
3. computes
   `required_floor = max(admission_head, explicit_read_after_commit,
   inherited_session_commit)` over the floors that are present;
4. selects or waits for one authorized snapshot/provider epoch at or beyond
   `required_floor`; and
5. reauthorizes at the existing post-wake and pre-release safe points.

The head observation occurs after denial can be decided without data access and
before provider or snapshot selection. It is an internal service fact, not a
client-submitted sequence or adapter cache. Commits acknowledged before the
capture are included in the floor; commits published afterward may or may not
appear according to the selected snapshot.

An authoritative query executes in one immutable read snapshot whose
application head is at least the floor. A one-provider projected or maintained-
index query serves one epoch whose frontier is at least the floor. A composite
query uses ADR-0130's newest common servable epoch subject to that same minimum.
Page rows, exact count, facets, rank, offset, and every other provider result
come from that one epoch.

Immutable descriptor and policy-shape compatibility are validated once per
plan/provider epoch. Head capture, provider choice, epoch negotiation, and the
request freshness proof are paid once per logical query. They are never
repeated per row, result, operation step, index entry, provider subcall, or
cursor continuation.

### 3. Freeze the first-page floor and epoch across continuations

When the first invocation returns an opaque cursor, its process-local cursor
state binds:

- the `AdmissionHead` consistency class;
- the captured required floor;
- the selected authoritative snapshot or provider epoch/frontier; and
- every existing operation, plan, parameter, principal, policy, history,
  ordering, physical-profile, and continuation identity.

A continuation resumes that exact snapshot/epoch and does not observe a new
application head. The opaque cursor carries the consistency context
automatically, so generated iterators and exact append-only external page
coalescing do not need a process-local commit cache. Supplying
`AdmissionHead` again is idempotent. A caller cannot upgrade an existing
non-fenced cursor in place; that request fails with the existing typed invalid-
continuation behavior and must begin a fresh logical query.

ADR-0159's page-cardinality independence remains unchanged. Page size may vary
within its compiled domain, but consistency class, required floor, and selected
epoch are identity-bearing. Numeric-offset queries have no continuation and
therefore capture a new admission head on every invocation; their page and
exact total still share one selected epoch.

### 4. Wait only within closed service bounds and fail typed

Admission-head waiting is bounded by the earliest of the existing request
deadline, cancellation, and a server-owned maximum freshness wait. The server
maximum is configured beneath the public application boundary and capped by a
finite implementation constant. Applications can request the stronger class
but cannot lengthen its maximum, select a polling interval, reserve a worker,
or force provider activation beyond ordinary bounded lifecycle rules.

Providers use their existing frontier notification/readiness mechanisms. The
request path cannot busy-poll, sleep for correctness, hold an authoritative
writer capability, retain a per-row waiter, or block projection application.
Waiters, retained snapshots, provider handles, and cancellation registrations
are globally and per-database bounded under ADR-0071.

If no valid snapshot can reach the floor before the bound, RiffDB returns the
existing typed freshness-unsatisfied/lifecycle family with bounded safe
required/current/head and retry guidance. A retired snapshot, rebuilding or
diverged provider, authorization change, deadline, cancellation, saturation,
or history-incarnation change remains a typed failure. RiffDB never silently
serves a lower frontier, falls back to another source, walks pages, rereads from
an adapter, or returns partial results.

### 5. Preserve policy, revocation, and nondisclosure

Initial authorization precedes head capture and provider waiting. Current
capability and row/field policy are revalidated after any wait and immediately
before response release under the existing service contract. The served
snapshot and policy evidence must be compatible at the selected provider epoch;
denied rows cannot contribute to result rows, exact counts, facets, scores,
ranks, offsets, cursors, aggregates, or released diagnostics.

Projection/index partitioning remains the primary policy-enforcement mode where
declared. Row predicates remain the secondary exact mechanism. Admission-head
freshness does not authorize a field, row, partition, provider, secret, or
historical version and cannot turn a freshness preference into a storage
existence oracle.

Public failures use the existing bounded redaction rules. Telemetry may report
value-free counts and durations by closed consistency class and lifecycle code;
it cannot label tenant keys, query values, provider contents, hidden row counts,
or captured application sequences.

### 6. Add only additive public protocol and generated-surface successors

The application gRPC request gains one additive closed enum field whose only
new meaningful value is `ADMISSION_HEAD`. The local driver request options gain
the corresponding optional closed value. Older clients omit the field and
retain existing behavior. New clients detect an older server/driver through
the existing capability/version handshake and receive typed refresh/upgrade
guidance rather than emulating the guarantee.

Generated Rust, Go, TypeScript, and Python query options, CLI, and MCP route the
same value through the common application service. No transport captures its
own head, waits independently, retries a stale success, or implements
consistency with connection order. The generated option contains no raw commit
sequence.

This decision changes no contract grammar, query IR, query module, plan hash,
application lock, role hash, provider durable state, entity, index, commit,
cursor token bytes, or storage key. The process-local cursor registry gains the
consistency/floor fields. Public Protobuf and driver request versions use their
least-sufficient additive successors and are recorded in version topology and
compatibility fixtures before release.

### 7. Keep external consistency vocabulary outside RiffDB

An external adapter may map its stronger consistency preference to
`AdmissionHead` and its ordinary preference to the existing default. The
adapter does not retain a sequence, inspect a provider frontier, or emulate the
wait. RiffDB documentation and code use only the generic admission-head term;
no external enum, API route, schema, test package, or framework branch enters
this repository.

Acceptance may include a value-free external receipt proving that two
independent adapter/client instances observe an acknowledged write under the
stronger preference. The external repository remains authoritative for its own
mapping and conformance.

## Options Considered

1. **Require a concrete commit token:** retained for exact causal reads but
   insufficient when the authoritative caller exposes only a preference.
2. **Cache the last commit in the adapter process:** rejected because it fails
   across processes, servers, restarts, and independently acknowledged writes.
3. **Make all reads admission-fenced:** rejected because it changes existing
   available-read latency and availability semantics without caller intent.
4. **Capture a new head on every cursor page:** rejected because it can chase a
   moving frontier and violate one-snapshot continuation.
5. **Add one server-resolved admission-head option:** proposed because it is
   stronger-only, bounded, cross-process, provider-neutral, and composable with
   existing causal floors.

## Consequences

- Applications that possess no commit token can request visibility through the
  server's admission point across independent client and adapter instances.
- The guarantee is precise but weaker than linearizability: later concurrent
  commits are not required.
- Stronger reads may wait or return a typed freshness/lifecycle outcome when a
  provider lags; the default availability behavior remains unchanged.
- Public request options and generated runtimes gain one additive enum value,
  while query plans and durable database formats remain unchanged.

## Compatibility

This Proposed ADR changes no current bytes or behavior. After acceptance,
legacy requests with the new field absent retain exact existing semantics.
New clients use additive gRPC/driver request successors and capability
negotiation. A server that cannot implement admission-head fencing refuses the
request; a client must not substitute process-local caching or stale success.

Opaque public cursor token bytes remain unchanged because exact continuation
state is held in the existing bounded process-local registry. Tokens still do
not survive registry restart. Contract, query-plan, module, application-lock,
role, and durable provider identities remain byte-exact.

## Security

Only an authenticated and initially authorized request can cause a head capture
or freshness wait. The option grants no data authority and cannot weaken a
declared freshness or policy rule. Post-wake and pre-release reauthorization
prevent a wait from freezing revoked capability. Bounded, redacted failures and
telemetry reveal no protected values, hidden population, tenant-specific head,
or provider contents.

The stronger option can consume bounded wait capacity, so existing per-
principal/database/global admission, cancellation, deadline, and fairness
controls apply before a waiter is retained. Saturation fails typed and cannot
starve commits or projection catch-up.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** callers choose only one stronger
  closed consistency class. They cannot supply a head, epoch, provider, wait
  duration, polling rule, fallback, isolation level, or stale-success opt-out;
  every generated/transport surface reaches the same service implementation.
- **Scale:** one head capture, one bounded provider negotiation, and one bounded
  waiter occur per logical query, never per row or cursor page. Waiters and
  retained epochs are bounded, cancellable, and independent of total database
  population and write duration.

## Testing

- Service tests with explicit barriers proving an acknowledged pre-admission
  commit is visible, a post-capture concurrent commit is not required, and the
  returned snapshot/frontier is never below the computed maximum floor.
- Cross-client and cross-process tests proving no process-local commit cache is
  involved.
- Lagging projected, maintained exact-index, authoritative, and composite
  provider tests for wakeup, deadline, cancellation, saturation, rebuild,
  divergence, retired epoch, history reset, and typed refusal with no stale
  success or busy polling.
- Cursor tests under sustained writes proving the first-page floor and epoch
  remain fixed across `1 -> 100`, `100 -> 1`, forward, reverse, and exact
  external append-only coalescing; attempted in-place upgrade of an old cursor
  fails typed.
- Exact count/offset and policy tests proving page and total share one epoch and
  denied rows affect no released result, count, facet, score, rank, offset, or
  cursor. Freshness waiting depends only on the global application head and
  provider frontier, never on a hidden row value or hidden-result cardinality.
- Revocation schedules at pre-wait, wake, provider execution, and pre-release
  safe points.
- Additive Protobuf/driver fixtures and Rust/Go/TypeScript/Python, CLI, MCP,
  local-driver, and remote-gRPC conformance, including old-client omission and
  old-server typed upgrade guidance.
- Pay-once instrumentation proving one head observation and one provider/
  descriptor/freshness negotiation per logical query, with no per-row, per-
  operation-step, or per-continuation recapture.

## Requirements and Work Packages

- **Future requirements after exact acceptance:** `OQ-075` through `OQ-083`
  and `DRV-018`
- **Service, provider, cursor, and public option implementation:** WP-719
- **Generated, compatibility, cross-process, and external acceptance:** WP-720

## Decision Deadline

Exact human acceptance is required before WP-719 adds the public consistency
enum, captures an admission head, changes cursor registry identity, or adds a
request-protocol field. Any default-consistency change, caller-selected wait,
new durable cursor/provider encoding, head exposure, silent fallback, transport-
local implementation, or stronger claim than fresh-through-admission requires
a separately accepted decision.
