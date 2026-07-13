# ADR-0012: Deterministic Transaction Context and Execution-Fault Admission

- **Status:** Accepted
- **Direction approved:** 2026-07-12
- **Exact text accepted:** Yes
- **Accepted:** 2026-07-13
- **Requires:** ADR-0004 and ADR-0007 accepted before or in the same governance
  change
- **Amended by:** ADR-0018 for UUIDv7 source isolation
- **Companion clarification:** 2026-07-13 `AdmissionClock` port/provider ownership
- **Amends:** SPEC `LOG-001` and Sections 5.2, 9.1, 9.2, 9.5,
  13.4, 17, and 22.2; ADR-0004 runtime/intent construction; ADR-0005 terminal
  admission states; ADR-0006 public-error registry; ADR-0007 admitted-
  provenance assembly; and work-package schema ownership
- **Decision deadline:** Before WP-060 freezes admission records or WP-080 implements runtime execution

The human maintainer accepted this exact no-randomness and recorded-time record
on 2026-07-13 as part of the atomic semantic-interface governance batch and
accepted ADR-0018's companion source-isolation amendment the same day.

## Context

Runtime output must be reproducible from declared inputs. `tx.time` is required
by commands but cannot come from an operating-system clock during evaluation.
Normative `TXN-011` also forbids POC command randomness.

Accepted ADR-0005 requires a durable pending admission before a mutating command
is evaluated. Accepted ADR-0013 additionally fixes checked arithmetic faults as
typed deterministic execution failures that discard in-memory work and return no
`CommitIntent`; it deliberately leaves their durable admission and public retry
semantics for a later accepted decision. Leaving that state transition implicit
would allow one implementation to retry a known-failed logical request while
another permanently consumes its idempotency key.

Grammar-v1 read-only commands require a separate return shape because the POC
does not journal them. They create no pending or terminal idempotency record,
`CommitSequence`, application commit, or persisted outcome. Their required
service-security audit is a separate record and never persists the command
outcome. Future durable read-only idempotency or journaling requires an accepted
ADR; it is not a deployment policy switch.

## Decision

### Ownership and assembly boundary

`riffdb-types` owns `LogicalTime`, the bounded value-only
`AdmittedActorContext`, and the closed nonzero `ExecutionFailureCode` used by
runtime, admission persistence, public-safe errors, and checked Proto
conversion. `riffdb-runtime` owns `TransactionContext`, `ExecutionResult`, and
`ExecutionFault`. `riffdb-storage-api` owns `EvaluationBudget`,
`EvaluatedCommand`, `CommitIntent`, the pending and terminal admission DTOs, and
the typed
`Pending -> ExecutionFailed` transition. `riffdb-commit` owns the consumer-side
`AdmissionClock` interface, orchestration, dependency revalidation, and final
intent assembly.
`riffdb-errors` owns the public-safe error wrapper, while `riffdb-proto` alone
owns the public and durable Protobuf encodings.

The runtime never receives ADR-0007 `StoredAdmittedProvenanceClaimsV1` and
therefore cannot construct the complete ADR-0004 `CommitIntent`. On a successful
mutating evaluation it returns only evaluation-owned data. The coordinator
combines that value with the exact stored pending admission, including admitted
actor, logical time, plan reference, identity, input hash, partition, and the
separate stored provenance-claim snapshot, to construct the storage-owned
`CommitIntent`. No caller, adapter, policy component, or runtime value may
replace a field already frozen by admission.

### No command randomness

POC command IR has no randomness instruction, value, seed, or capability. Runtime
accepts no entropy provider and cannot access OS randomness. Randomness used for
capability-token issuance is outside command execution and never enters a command
plan, snapshot, transaction context, outcome, event, or provenance value.

Recorded deterministic command randomness remains deferred to an MVP ADR. Adding
it requires a new IR instruction and value contract, durable recorded input,
compatibility fixtures, and an explicit specification amendment; reserving an
unused field or passing a hidden test seed is forbidden.

### Coordinator-supplied logical time

`tx.time` is a validated `riffdb_types::LogicalTime` wrapping the accepted
`Timestamp { seconds: i64, nanoseconds: u32 }` representation. Seconds are signed
Unix seconds and nanoseconds are in `0..=999_999_999`. No alternate precision,
timezone, leap-second flag, local-calendar representation, or floating-point
conversion is accepted.

For a new invocation, the commit orchestration layer reads an injected
`AdmissionClock` exactly once before evaluation. The production implementation
observes the host UTC wall clock outside the deterministic runtime and converts
it exactly to signed Unix seconds plus nanoseconds. A clock value outside the
accepted timestamp representation or a clock-provider failure prevents admission
and selects `PublicErrorKind::InternalDefect` with an opaque incident ID. That
safe error is returned only after the required terminal `failed` audit append is
known durable. If that append aborts or is uncertain, accepted ADR-0007's audit-
outage rule instead returns `StorageUnavailable` and withholds the original error;
it never returns `OutcomeUnknown` because no pending admission or durable command
terminal identity exists. The time is never clamped, rounded, wrapped, or replaced
with a default, and no pending record is created for the failed observation.

Logical time is not a commit-order clock. It may be equal to or earlier than a
previous command's time after host-clock adjustment and must not drive sequence
ordering, uniqueness, storage-key order, lease expiry, or authorization expiry.
Commit sequence remains the only authoritative application order. Projection
`tx.date` is the UTC calendar date deterministically derived from the originating
event's recorded `tx.time`.

For a mutating command, the coordinator durably creates or resolves the pending
admission before evaluation and stores the one observed `tx.time`. Every resume
of that same admission reuses it. The clock is not read when a pending or terminal
record already exists. A grammar-v1 read-only command receives one
invocation-local logical time and does not promise the same time across
independent invocations or retries because no admission record exists.

Tests use an injected clock whose values are explicit harness inputs. No test
clock, scheduler seed, fuzz seed, or global mutable clock can compile into the
production runtime path.

`AdmissionClock` is a narrow synchronous consumer port declared by
`riffdb-commit`; `riffdb-server` owns and composes its production operating
system clock provider. It is a separate semantic interface from
`riffdb-commit`'s `AdministrationClock`, the auth-owned initial-authentication
clock, and the policy-owned `AuthorizationClock`. Sharing a concrete server
clock implementation does not merge those ports or allow one consumer to call
another consumer's clock. Tests inject each port independently.

ADR-0018 applies the same dependency direction to identifier generation.
`riffdb-commit` owns the consumer-side `ProvenanceIdSource` port and
`riffdb-server` owns its production provider. No UUIDv7 generation clock,
entropy, provider, or source-ordering state for `ProvenanceId`, `DatabaseId`,
`RequestId`, or `CapabilityId` is a runtime dependency or a command-randomness
input. The checked `RequestId` already present in `TransactionContext` remains
an opaque admitted/invocation identity; runtime must not derive `tx.time`,
randomness, uniqueness, retry order, commit order, or authorization facts from
its UUID fields. `ProvenanceId` generation occurs outside evaluation and its
source is never passed through a runtime argument.

### Exact immutable transaction context

Runtime receives an owned, immutable, value-only context with these semantic
fields:

```text
TransactionContext
  request_id: RequestId
  actor: AdmittedActorContext
  plan: ExecutablePlanRef
  tx_time: LogicalTime
  partition_key: PartitionKey
```

For a mutating command, `request_id` is exactly the
`admission_request_id` frozen in the pending record and is reused on every
resume; a retry's new outer request ID never enters runtime. For an unjournaled
read-only command, it is that invocation's checked outer `RequestId`. The field
name remains the SPEC's generic `request_id` because read-only execution has no
admission; the two construction rules are explicit and never interchangeable.

`ExecutablePlanRef` is the exact historical plan identity frozen with ADR-0004:
contract lineage, application contract version, contract bundle hash, stable
command ID, and command plan hash. The pending admission stores this complete
reference. A compatible deployment never substitutes the currently active plan
for a pending plan.

`AdmittedActorContext` contains only authorization-resolved, bounded values that
are required by deterministic execution or policy-approved provenance: stable
principal ID, trusted actor kind, authorization-resolved tenant scope, and the
optional admitted agent-session ID. Raw credentials, token digests, untrusted
claims, approval text, transport metadata, and current retry-session state are
excluded. A retry is reauthenticated and reauthorized against current policy
before resuming. Current authorization is evaluated against the stored stable
principal and tenant scope, exact historical command and plan, and exact stored
partition. A different current capability may resume only when it resolves to
that same stable principal and tenant and authorizes that exact operation. The
original actor and optional agent-session context remain the evaluation and
provenance context so deterministic output and provenance do not drift.

The runtime also receives the owned bounded `ReadSnapshot` defined by ADR-0004
and an immutable `EvaluationBudget`. The coordinator derives that budget solely
from accepted IR/storage hard limits and the checked historical plan. It contains
only fixed registry counts and bytes; it is not command-visible, configurable per
request, selected by an adapter, or derived from provenance/approval fields. V1
reserves 64 KiB of ADR-0004's 15 MiB `CommitIntent` ceiling for all coordinator-
added admission and provenance fields and therefore caps `EvaluatedCommand`
semantic encoded content at 15,663,104 bytes. Admission construction rejects if
its complete non-runtime fields cannot fit that reserve. Equal plan/format
versions derive an equal budget on every retry, so admitted provenance cannot
alter runtime behavior even indirectly through available capacity.
It receives no clock, entropy source, filesystem/network/environment access,
locale, engine handle, storage transaction, async executor, process-global
mutation, request buffer reference, or conflict-manager internal state. A
non-transferable acquired mutation capability may be borrowed as a separate
orchestration proof; it is not a context value and never enters `CommitIntent`.

Evaluation is synchronous and contains no `.await`. All maps and sets that can
affect evaluation are already canonical ordered values or are traversed in an
explicit plan-defined order.

ADR-0007's policy-approved source, reason, and approval claims are persisted in
the storage-owned `StoredAdmittedProvenanceClaimsV1` beside the admitted actor and
transaction context. They never enter `TransactionContext` or another runtime
argument. The coordinator reuses that exact stored snapshot on resume and later
combines it with runtime output as specified below.

### Runtime result boundary

The runtime's semantic return is `Result<ExecutionResult, ExecutionFault>`, where
both sides are closed unions:

```text
ExecutionResult
  ReadOnly(EncodedOutcome)
  CommitRequired(EvaluatedCommand)

ExecutionFault
  Arithmetic
  ResourceLimit
  Integrity
```

A mutating command, including a declared zero-mutation business rejection,
returns `CommitRequired` with ADR-0004's storage-API-owned `EvaluatedCommand`.
The coordinator combines that value with the exact stored admission into the
final self-contained `CommitIntent`; only it may assign a sequence and persist the
terminal business outcome. A command with read bindings only returns `ReadOnly`.
In the POC that result is returned directly after current authorization and
service-audit handling; it is never submitted as an application commit or stored
for idempotent replay.

`EvaluatedCommand` contains only the complete canonical binding/range targets
and influential read dependencies, complete entity mutation post-images and
expected observations, ordered pre-commit event values, and the declared encoded
outcome produced by evaluation. It contains no admission identity, raw
idempotency key, admitted actor, stored provenance claims, sequence, final event
ID, durable record, or `StoredEnvelope`. Its checked constructor enforces the
remaining `EvaluationBudget`; it makes no claim that transaction-current
dependencies still match.

`Arithmetic` covers checked integer overflow/underflow, division by zero,
signed-minimum division by `-1`, decimal precision overflow, money amount
overflow, and unary-negation overflow fixed by ADR-0013. `ResourceLimit` covers a
validated plan whose bounded data reaches a deterministic runtime aggregate
budget that the compiler could not discharge statically. The only such budgets
are the accepted ADR-0011/ADR-0013 value, collection, nesting, and IR bounds and
the ADR-0004 snapshot, mutation, event, outcome, and final-intent count/byte
ceilings represented by `EvaluationBudget`. Process configuration cannot raise,
lower, or reinterpret them. Changing one changes the compatible IR/storage
registry and requires an accepted versioned decision. Both faults discard all
working mutations, captured events, and provisional outcomes and return no
`EvaluatedCommand` or `CommitIntent`.

`Integrity` is reserved for an impossible or malformed validated-plan/snapshot
condition. It is an internal incident, not a normal deterministic terminal
failure, and does not use the terminal execution-failure transition below.
Panics, allocation failure, storage failure, lock timeout, cancellation, and
process failure are not converted into `ExecutionFault`.

The accepted aggregate-root companion boundary classifies a missing internal
`RootValidationReadPlan` observation as `Integrity`. Runtime first resolves all
source-declared binding failures in ascending `BindingId`; only if they all
succeed does it require each internal root observation. The fault discards all
working mutations, events, and provisional outcomes. For a mutating command it
leaves the durable admission pending, receives no `CommitSequence`, and is not
terminalized as `ExecutionFailed`. This is distinct from an absent
source-declared root binding, which uses that binding's declared business
outcome.

### Durable execution-failure transition

For a mutating command with a pending admission, `Arithmetic` or `ResourceLimit`
may become a terminal, durable, non-commit result for that idempotency identity
only after the evaluation snapshot is still proven current. Before returning it
to the caller, the coordinator uses a short typed storage transaction to:

1. recheck the exact pending identity, canonical input hash, complete historical
   plan reference, admitted actor/time/partition, and stored provenance snapshot;
2. reread and compare every influential `EntityObservation` and
   `IndexRangeEpoch` dependency from the complete ADR-0004 `ReadSnapshot`,
   including observed absence; and
3. only if every dependency is equal, atomically transition:

```text
Pending -> ExecutionFailed { code }
```

A missing dependency, changed version/absence/epoch, conflicting duplicate, or
plan/evidence mismatch cannot terminalize the fault. A normal dependency change
writes nothing, releases every capability from the abandoned attempt, and
causes a bounded full reevaluation under the exact stored plan, actor, partition,
and `tx.time`, with fresh canonical capability acquisition and a new complete
snapshot according to the accepted retry policy. If its retry or
deadline budget is exhausted, the admission remains pending and the coordinator
returns the existing nonterminal retry/deadline result; it never returns the
stale execution fault as terminal. Missing or malformed evidence is an integrity
incident and also leaves the admission pending. Thus state outside the acquired
conflict domains cannot make a stale arithmetic/resource observation permanent.

The closed durable codes are `ArithmeticFault = 1` and `ResourceLimit = 2`.
The terminal record retains the pending record's identity, canonical input hash,
admission request ID, exact plan reference, admitted actor context, and `tx.time`,
plus the code. It stores no source expression, operand, business value, raw key,
credential, diagnostic string, engine error, or stack trace.

The record also retains the exact `StoredAdmittedProvenanceClaimsV1` captured with
the pending admission. It does not turn those claims into command provenance,
expose them to runtime, or replace them with claims from the retry that observed
the failure.

`ExecutionFailed` receives no `CommitSequence` and creates no entity mutation,
index change, `StoredOutcome`, durable event, outbox entry, application commit
record, or command provenance record. It is terminal idempotency state, not a
declared business outcome and not proof of an application commit. This explicitly
extends ADR-0005's admission state machine without changing its rule that every
declared terminal business rejection receives one sequence.

Acceptance therefore requires this explicit specification amendment:

> `ExecutionFailed` is a terminal idempotency-admission resolution, but it is not
> a terminal command outcome or application commit. `LOG-001`, command provenance
> requirements, and terminal-record sequence properties apply to committed
> declared outcomes, including zero-mutation business rejections, and do not apply
> to this closed pre-commit execution-failure state.

This exclusion does not remove the authorization/security audit record required
by `MCP-046` for a mutating tool invocation. That audit record is not command
provenance and carries no `CommitSequence`.

The coordinator holds acquired mutation capabilities until the failure transition
commits or is abandoned. Cancellation becomes advisory after failure
terminalization is submitted. A storage failure that proves the transition
aborted returns `StorageUnavailable` and leaves the admission pending/resumable;
it must not claim that the execution failure is terminal. ADR-0004
`CommitStatusUnknown` instead fences writes and returns `OutcomeUnknown`, because
the record may be pending or terminal; idempotency lookup after reopen resolves
it. A crash after the transition but before response is recovered by the same
lookup.

Same identity and equal canonical input returns the stored execution failure
without evaluation. Different canonical input returns `IdempotencyKeyReuse`.
After state or contract correction, the caller must submit a new logical command
with a new idempotency key. After `ExecutionFailed` durably commits, the old
identity can never later mutate. If terminalization proves abort and the caller
receives `StorageUnavailable`, the pending admission remains resumable. If commit
status is unknown, the caller resolves rather than assuming either state. This
prevents an automated retry of a known-failed request from unexpectedly succeeding
after state changes without falsely claiming terminality before durable storage.

For an unjournaled read-only command, `Arithmetic` or `ResourceLimit` is returned
without a durable record because there is no pending admission. A service audit
may record the bounded invocation classification required by ADR-0007, but it
does not contain or persist the read-only outcome or execution-fault details.
Any future durable read-only result or fault requires an accepted ADR and cannot
reuse a `CommitSequence` merely by configuration.

An `Integrity` fault leaves a mutating admission pending for operator intervention;
the POC provides no online repair path. It maps to a redacted internal incident.
The coordinator must not terminalize it as
an expected execution failure because doing so would hide corrupt plan or
snapshot evidence.

### Public error amendment

Upon acceptance, ADR-0006's closed public-error registry is additively extended
before external release:

- `PublicErrorKind::CommandExecutionFailed` is wire enum value `9`.
- Stable code: `command_execution_failed`.
- Static safe message: `command execution failed`.
- Error class: `FailedPrecondition`.
- Recovery action: existing `ContactOperator`.
- Required detail is one closed `ExecutionFailureCode`: unspecified wire value
  `0`, arithmetic fault `1`, resource limit `2`. Zero is never a valid Rust or
  durable value and rejects when decoding this required detail.
- Protobuf adds
  `CommandExecutionFailureDetails { ExecutionFailureCode code = 1; }` and uses it
  as `PublicError.execution_failure = 8` in the detail oneof. Existing field
  numbers and enum values do not change.
- gRPC maps it to `FAILED_PRECONDITION`; MCP maps it to a safe actionable tool
  execution error after the required invocation-terminal audit is known durable.
  The execution result itself is never uncertain because the database durably
  knows that no application command committed. Accepted ADR-0007's narrower
  audit-outage rule still maps failure or uncertainty while appending that
  terminal audit to `OutcomeUnknown` so the same idempotency identity can resolve
  the durable `ExecutionFailed` result without releasing unaudited output.

The safe detail contains only the closed code. Internal tracing may attach an
incident ID for unexpected internal faults, but expected arithmetic/resource
failures do not expose operands or automatically create incidents.

The exact additive Protobuf symbols are:

```protobuf
PUBLIC_ERROR_KIND_COMMAND_EXECUTION_FAILED = 9;

enum ExecutionFailureCode {
  EXECUTION_FAILURE_CODE_UNSPECIFIED = 0;
  EXECUTION_FAILURE_CODE_ARITHMETIC_FAULT = 1;
  EXECUTION_FAILURE_CODE_RESOURCE_LIMIT = 2;
}

message CommandExecutionFailureDetails {
  ExecutionFailureCode code = 1;
}
```

This amendment requires a focused WP-010 type/error follow-up that atomically
adds the exact Protobuf symbols above, the exhaustive safe domain mapping,
bounded preflight, generated descriptor/source, and golden fixtures. This is a
narrow sequencing exception needed to keep the closed domain and wire registries
compilable; it adds no request, result, service, RPC, capability, projection, or
durable-record field. WP-127 preserves this slice and completes every remaining
public schema before WP-130 exposes the error. WP-020 retains the accepted
phase-zero protocol/envelope baseline; it is not reopened to absorb other
post-semantic public or durable records. This addition does not renumber or
reinterpret an existing public kind.

### Bounds and validation

Transaction context has a fixed field count and uses the bounds of its owned
types. Snapshot, intent, expression, collection, and aggregate budgets come from
accepted compiler/storage registries and are checked with overflow-safe arithmetic
before allocation. The compiler rejects statically excessive plans. Runtime
rechecks dynamic counts and encoded sizes deterministically. The v1 non-runtime
intent reserve is exactly 65,536 bytes and the v1 `EvaluatedCommand` ceiling is
exactly 15,663,104 semantic encoded bytes; neither is configurable. Exceeding the
runtime-owned ceiling produces `ResourceLimit`, while an admission whose fixed
fields exceed the reserve is rejected before evaluation as bounded validation or
an internal construction defect according to whether caller input caused it.

No error path may include source snippets, input values, entity values, raw
partition/conflict keys, idempotency keys, credentials, or arbitrary strings.
Stable plan/type IDs may be retained only in internal redacted diagnostics.

## Options Considered

1. **Recorded wall time plus terminal non-commit execution failure:** Selected.
   It preserves deterministic replay and prevents a known-failed identity from
   mutating later.
2. **Seeded deterministic command randomness:** Rejected for the POC because it
   violates `TXN-011` and expands IR/durable semantics.
3. **Derive time from request ID or commit sequence:** Rejected. Request IDs are
   not clocks, and sequence does not exist before evaluation.
4. **Monotonically clamp wall time:** Rejected. It makes time depend on mutable
   global history and falsely gives `tx.time` commit-order meaning.
5. **Leave arithmetic failure pending and freely retry:** Rejected. The same
   known-failed logical request could later mutate after state drift.
6. **Delete and recreate admission after failure:** Rejected. It loses durable
   uncertainty evidence and permits retry-time plan/time substitution.
7. **Persist execution failure as a sequenced business outcome:** Rejected. It is
   absent from the contract outcome algebra and ADR-0013 returns no intent.
8. **Map a durably known failure to `InternalDefect` or `OutcomeUnknown`:**
   Rejected. Checked arithmetic/resource faults are expected typed execution
   failures. `OutcomeUnknown` remains mandatory only when the terminalization
   transaction itself has unknown commit status.

## Consequences

- WP-060 needs a terminal non-commit admission record and transition in its
  storage state machine even though evaluation arrives in WP-080.
- WP-080 has one pure synchronous interface and no ambient time/entropy.
- Runtime returns the storage-owned evaluation payload only; WP-100 combines it
  with the exact stored admission/provenance snapshot into `CommitIntent`.
- WP-100 can recover a lost execution-failure response without reevaluation.
- A terminal failed identity is intentionally consumed; correction uses a new
  logical request and idempotency key.
- WP-065 owns the durable terminal record schema. The focused WP-010 follow-up
  owns the exact additive public error symbols/mapping/preflight/goldens; WP-127
  preserves that slice and owns every remaining public schema completion. WP-020
  remains the accepted baseline.
- The specification must narrow `LOG-001`, Section 13.4 provenance, and Section 17
  terminal-sequence properties exactly as stated above; this is not an implied
  exception created only by storage code.
- Grammar-v1 read-only results and faults remain unjournaled; service audit is
  separate and outcome-free. Durable read-only idempotency/journaling is an
  explicit post-POC ADR boundary.

## Compatibility

Transaction-context fields, timestamp representation, exact plan reference,
admitted actor values, `EvaluatedCommand` membership, budget/reserve values,
execution-result variants, terminal dependency-validation rules, terminal
admission-state variants and codes, and public error numbers are semantic,
durable, or public boundaries. Any new
context field, command randomness, time interpretation, execution-failure code,
or retry transition requires an accepted compatible versioning decision.

Existing accepted hash, key, plan, outcome, and commit-sequence formats do not
change. The accepted specification wording changes before implementation to make
the non-commit admission state explicit. `ExecutionFailed` is not encoded as
`StoredOutcome` and cannot be silently migrated into one. WP-100 may map a stored
command outcome into the current-call `CommittedOutcome` response wrapper; an
`ExecutionFailed` admission has no such stored command outcome.

## Security

Runtime receives no ambient authority. Original admitted actor values are reused
for determinism, while every retry still passes current authentication,
authorization, revocation, audience, tenant, partition, and obligation checks
before execution. Raw credentials and token digests never enter context.

Terminal failure records and public errors contain no operand or business data.
Test hooks cannot mutate production global state. Clock and entropy providers are
kept outside runtime dependency edges.

## Testing

- Same plan/input/snapshot/context produces byte-identical result or identical
  typed fault across repeated runs and collection insertion orders.
- Injected clock tests prove one read for new admission, zero reads on resume or
  replay, one invocation-local read for each unjournaled read-only execution,
  exact negative/positive second and nanosecond boundaries, and no monotonic
  clamping.
- Architecture tests forbid clock, randomness, filesystem, network, environment,
  async, and process-global dependencies from runtime/invariant crates.
- Arithmetic tables cover every ADR-0013 fault on boundary values without wrap,
  saturation, panic, or business-outcome conversion.
- Resource-budget tests accept the exact limit and reject the next unit before
  excessive allocation; varying stored provenance under equal plan/runtime input
  does not change the derived budget or runtime result.
- Architecture and constructor tests prove runtime cannot receive admitted
  provenance, cannot construct `CommitIntent`, and that only the coordinator can
  combine `EvaluatedCommand` with the exact stored admission snapshot.
- Mutation-between-evaluation-and-terminalization tests cover present, absent,
  and range-epoch dependencies; every change causes full reevaluation or leaves
  the admission pending, never a stale `ExecutionFailed` record.
- Failpoints before and after failure terminalization prove pending-or-complete
  atomicity, proven-abort versus unknown-commit mapping, write fencing, no
  sequence/commit/event/command-provenance, retained admitted-provenance claims,
  and same-key replay.
- Different-input reuse, compatible deployment, current-policy reauthorization,
  cancellation, storage-unavailable, and internal-integrity cases exercise the
  distinct state transitions.
- Grammar-v1 read-only tests prove success and every execution fault create no
  admission, persisted outcome, application commit, or sequence; separate
  service-audit fixtures prove that record contains no outcome or fault detail.
- Public error/Protobuf/MCP/gRPC fixtures freeze enum value `9`, detail field `8`,
  stable code/message/class/recovery, and secret-canary absence.
- Property-generated command histories compare runtime output and failure state
  with a single-threaded reference model; harness seeds never enter semantics.

### Exact specification and manifest reconciliation

Acceptance updates SPEC Section 5.2 so `riffdb-runtime` produces
`EvaluatedCommand`, not `CommitIntent`, and updates Sections 9.1, 9.2, and 9.5 so
the coordinator combines that runtime value with the exact stored admission and
provenance snapshot before ADR-0004 validation/commit. Section 9.2 uses the
exact context above, including the distinct mutating-admission and unjournaled-
read-only construction rules for `request_id`. The same change applies
the `LOG-001`, Section 13.4, and Section 17 non-commit exception quoted above and
reconciles Section 22.2 to make grammar-v1 read-only execution unjournaled with
separate outcome-free service audit.
ADR-0004 must name `EvaluatedCommand` as the storage-API-owned evaluation DTO and
the coordinator as the only `CommitIntent` assembler. ADR-0007 must retain
`StoredAdmittedProvenanceClaimsV1` beside the runtime context and through
`ExecutionFailed`.

The manifest delta is exact:

- WP-010 adds ADR-0006 and ADR-0012 and delivers `AdmittedActorContext`, nonzero
  `ExecutionFailureCode`, `CommandExecutionFailed`, the fixed budget constants,
  and mapping/redaction tests. Its exact additional paths are
  `proto/riffdb/v1/error.proto`, generated `riffdb.v1.rs`, the command/value/public-
  error decoders and mappers, wire preflight, the Proto generator example,
  schema/wire tests, and the production descriptor, schema-inventory, and wire-
  vector fixtures. Command/value edits only enforce the approved nonzero semantic
  types against existing phase-zero fields. Its amended
  acceptance runs types/errors/Proto tests and Clippy plus
  `./scripts/generate-proto --check`; no other public or durable symbol is in
  scope.
- WP-060 adds ADR-0012 and delivers checked `EvaluatedCommand`, terminal
  admission DTO/transition, complete dependency-validation request, and memory
  conformance tests. It does not define Protobuf.
- WP-065 depends on WP-020 and WP-060; requires ADR-0004, ADR-0005, ADR-0006,
  ADR-0007, ADR-0009, ADR-0010, ADR-0011, ADR-0012, ADR-0013, ADR-0014,
  ADR-0016, and ADR-0017; and may edit only `Cargo.lock`, `proto/**`,
  `crates/riffdb-proto/**`, `fixtures/proto/**`, `scripts/generate-proto*`,
  `crates/riffdb-storage-api/Cargo.toml`,
  `crates/riffdb-storage-api/src/lib.rs`, and
  `crates/riffdb-storage-api/src/proto_codec/**`. It owns the durable
  pending/terminal admission messages, descriptors, schema hashes, golden bytes,
  bounds, historical registrations, and checked durable DTO mappings. Its
  acceptance commands are `cargo test -p riffdb-proto -p riffdb-storage-api`
  and `./scripts/generate-proto --check` with a clean generated diff.
- WP-070 retains every existing dependency and additionally depends on WP-065;
  it requires ADR-0012 and persists only the WP-065-reviewed records.
- WP-080's objective and SPEC-facing deliverable change from constructing
  `CommitIntent` to constructing `EvaluatedCommand`; it adds the immutable fixed
  budget and deterministic fault tests and never receives stored provenance.
- WP-100 delivers coordinator-only intent assembly, dependency-validated failure
  terminalization, changed-dependency reevaluation, and proven-abort versus
  unknown-commit recovery.
- WP-127 is the public API schema-completion package. It depends on WP-020 and
  WP-120, requires ADR-0006, ADR-0007, ADR-0009, ADR-0010, ADR-0011,
  ADR-0012, ADR-0013, and ADR-0017, and may edit only `Cargo.lock`, `proto/**`,
  `crates/riffdb-proto/**`, `fixtures/proto/**`, and
  `scripts/generate-proto*`. It preserves the WP-010 execution-failure slice and
  adds every remaining reviewed public service field, descriptor, schema hash,
  wire-structural validation rule, and golden fixture. It does not depend
  on `riffdb-service` or own service-to-wire conversion. Its requirements
  include `API-001`, `VAL-003`, and projection-fixture `POC-006`; existing
  assignments are retained. Its acceptance commands are
  `cargo test -p riffdb-proto` and `./scripts/generate-proto --check` with a clean
  generated diff.
- WP-130 retains every existing dependency and additionally depends on WP-127;
  it adds `Cargo.lock` to its allowed paths, consumes but never defines the
  public schema, and owns total service-to-wire conversion.
- WP-065 and WP-127 are explicit P1 gate members; their hard downstream edges do
  not replace any existing P1 package or dependency.
- ADR-0012 remains required for WP-120, WP-127, WP-130, WP-140, WP-185, WP-190,
  and WP-200. WP-020 remains unchanged and is not retroactively made dependent on a
  later semantic owner.

## Requirements and Work Packages

- **Requirements:** `SYS-003`, `VAL-001`, `DSL-006`, `DSL-007`, `TXN-001`,
  `TXN-010` through `TXN-013`, `TXN-030`, `TXN-043`, `TXN-044`, `REC-001`
  through `REC-003`, `API-001`, `MCP-023`, `MCP-046`
- **Defines or blocks:** foundational follow-up in `WP-010`; semantic admission
  and evaluation DTOs in `WP-060`; durable schema in `WP-065`; runtime in
  `WP-080`; coordinator in `WP-100`; public schema completion in `WP-127`;
  required ADR for `WP-010`, `WP-060`, `WP-065`, `WP-070`, `WP-080`, `WP-100`,
  `WP-120`, `WP-127`, `WP-130`, `WP-140`, `WP-185`, `WP-190`, and `WP-200`
- **Final evidence:** `WP-190`, `WP-200`

## Decision Deadline

Exact acceptance is required before WP-060 freezes admission state and before
WP-080 implements transaction context or runtime fault handling. Upon acceptance,
the same governance commit applies the exact specification, ADR, and manifest
reconciliation above, including WP-065, WP-127, and the additional WP-130
dependency; updates WP-010/WP-060/WP-080/WP-100 deliverables; and adds amendment
cross-references to accepted ADR-0005 and ADR-0006. ADR-0004 and ADR-0007 must be
accepted first or in that same commit. No implementation or fixture may choose a
different runtime-result, terminal, retry, schema-owner, or public-error mapping
first.
