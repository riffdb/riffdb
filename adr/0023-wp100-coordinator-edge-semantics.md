# ADR-0023: WP-100 Coordinator Edge Semantics

- **Status:** Accepted
- **Direction approved:** 2026-07-20
- **Exact text accepted:** 2026-07-20, amended 2026-07-20, 2026-07-21, and
  2026-07-22
- **Accepted:** 2026-07-20
- **Maintainer-accepted clarification:** 2026-07-20, the audit-input view has no
  field or method for the new audit record's assigned sequence, while its
  checked result link may carry a prior authoritative transition sequence
- **Maintainer-accepted zero-mutation clarification:** 2026-07-20, every
  influential dependency is revalidated; zero-mutation declared outcomes skip
  commit-check evaluation and index derivation but still commit their terminal
  outcome graph; nonzero mutation sets exactly cover all mutable plan bindings
- **Maintainer-accepted derived-index bound and exhaustion clarification:**
  2026-07-20, checked v1 plans conservatively fit all pre-sequence index bounds;
  an exhausted mutation-affected epoch aborts before sequence assignment and
  stops, rather than fences, the coordinator
- **Maintainer-accepted lineage-materialization clarification:** 2026-07-20,
  catalog proves bounded exact ancestry; commit applies its opaque ancestor-only
  null-fill authority before snapshot and transaction-current evaluation; and
  runtime rejects every remaining omission
- **Maintainer-accepted lineage-overflow clarification:** 2026-07-21, valid
  over-budget returned evidence retains only the raw snapshot and exact resolved
  plan/proof; catalog owns opaque readiness/resource evidence and the pure
  current-recheck API, while commit orders dependency comparison and
  terminalization
- **Maintainer-accepted durability-construction clarification:** 2026-07-21,
  production coordinator construction explicitly receives `Sync` or `Group`,
  `Memory` is test-only, P1 `riffdbd` passes `Sync`, and no implicit default is
  permitted
- **Maintainer-accepted attempt-scope clarification:** 2026-07-21, an attempt
  slot is consumed immediately before catalog snapshot materialization; a valid
  lineage-expansion `ResourceLimit` consumes the slot without entering runtime,
  while `Ready` continues into one runtime evaluation in the same slot
- **Requires:** ADR-0002, ADR-0003, ADR-0004, ADR-0005, ADR-0006, ADR-0007,
  ADR-0009, ADR-0011, ADR-0012, ADR-0013, ADR-0016, ADR-0018, ADR-0021, and
  ADR-0022
- **Amends:** ADR-0002's grammar-v1 index semantics; ADR-0004's
  coordinator/storage validation boundary; ADR-0007's commit dependency and
  service-audit ownership declarations; ADR-0009's capability-revoke
  authorization; ADR-0012's retry wording; ADR-0018's provenance replay wording;
  ADR-0006's `ConcurrencyDeadlineExceeded` cause; ADR-0013's checked-plan bounds
  and lineage materialization; SPEC Sections 5.2, 9.5, 9.6, 10.5, and 16.2; and narrow
  WP-050/WP-060/WP-100/WP-110/WP-120/WP-130/WP-200 metadata
- **Amended by:** ADR-0039 for the exact aggregate-capacity admission result and
  its closed coordinator disposition before sequence assignment
- **Decision deadline:** Before WP-100 publishes the affected executor, retry,
  index, lineage-normalization, execution-failure, or capability-revoke paths

The human maintainer accepted this exact record on 2026-07-20. The audit-input
inversion, provenance-attempt boundary, grammar-v1 covered-value rule,
commit-check arithmetic classification, absent-revoke rule, no-transition audit
classification, exact commit-evaluator dependency, and bounded-reevaluation
ceiling are authoritative for the affected work packages.

On 2026-07-20 the human maintainer accepted the audit-sequence clarification
below. It distinguishes the new service-audit record's sequence, which remains
coordinator/storage assigned, from a prior authoritative control-plane sequence
carried as checked result-link data.

On 2026-07-20 the human maintainer also accepted the zero-mutation clarification
below. It distinguishes a durable declared business rejection from a read-only
command and closes the post-image coverage rule without permitting pre-image
fallback.

On 2026-07-20 the human maintainer accepted the derived-index admission and
epoch-exhaustion clarification below. It closes a checked-plan cross-product gap
without changing IR or durable bytes and distinguishes proven exhaustion from
uncertain commit status.

On 2026-07-20 the human maintainer accepted the bounded lineage-materialization
clarification below. It closes blind optional-field fill versus blanket missing-
field rejection while preserving exact parent/hash ancestry, fail-closed durable
integrity, and every public/durable format.

On 2026-07-21 the human maintainer accepted the lineage-overflow retention and
recheck clarification plus explicit process-local coordinator durability
selection below. They add no durable/protocol field, operator-facing durability
setting, or POC group-commit policy.

On 2026-07-21 the human maintainer also accepted the attempt-scope clarification
below. It closes whether a deterministic pre-runtime lineage overflow consumes
the bounded invocation budget without changing any durable or public format.

## Context

WP-100 must join already accepted compiler, runtime, storage, authorization,
idempotency, and audit interfaces without introducing a dependency cycle or
silently choosing unspecified durable behavior. Implementation tracing exposed
twelve narrow gaps:

1. `riffdb-service` owns the concrete checked `ServiceAuditInput`, while
   `riffdb-commit` owns `AdministrationAuditExecutor`; a direct executor
   dependency on the service type would create the wrong dependency direction.
2. ADR-0018 permits a new provenance candidate after a proven pre-commit abort,
   while the WP-100 deliverable can be read as forbidding generation on every
   durable-pending resume, including after process loss.
3. Storage supports generic index covered values, but grammar/IR version 1 has no
   declaration for covering fields.
4. Transaction-current commit-check evaluation can produce an arithmetic fault,
   but `CandidateValidationRejection` currently distinguishes only a false
   predicate from dependency and mutation-precondition changes.
5. A revoke request can name a capability that does not exist, so complete
   target grant facts needed for ordinary subset authorization cannot be built.
6. Authorized control-plane requests can complete with a typed no-transition
   result and no authoritative transition sequence, while the accepted audit
   matrix does not permit such a result to claim a sequence-linked success.
7. ADR-0004 assigns exact historical-plan matching and transaction-current
   commit-check orchestration to `riffdb-commit`, but ADR-0007 and SPEC Section
   5.2 omit `riffdb-contract-ir` and `riffdb-invariant` from that crate's allowed
   dependencies. No accepted runtime port exposes the exact plan or pure
   transaction-current evaluator, and adding one would move coordinator
   semantics into the snapshot interpreter.
8. `RetryPolicy::BoundedFullReevaluation` delegates to a bounded coordinator
   policy, and ADR-0012 fixes the durable behavior when that budget is exhausted,
   but no accepted source sets a numeric attempt ceiling or identifies the exact
   existing public result for count exhaustion.
9. Runtime correctly emits a zero-mutation `EvaluatedCommand` for a declared
   business rejection, but the transaction-current wording assumes every
   evaluated command supplies mutable post-images. Applying that wording
   literally would either reject the required `OUT-003` result or invite an
   undeclared pre-image fallback for commit checks and index derivation.
10. Runtime currently cannot distinguish a compatible ancestor omission from a
    corrupt same-version omission, while commit-time record validation can
    reject both. Treating numeric versions or a stored compatibility report as
    ancestry evidence would make these paths disagree and authorize unproved
    state materialization.
11. A valid lineage null-fill expansion can cross the shared 16 MiB ceiling
    before runtime, but returned resource evidence must not retain over-budget
    normalized data or masks and terminalization still needs an exact bounded
    source of truth.
12. The semantic durability enum includes `Sync`, `Group`, and test-only
    `Memory`, but coordinator construction and P1 composition do not yet state
    an explicit production selection boundary; an inferred/default mode could
    silently change correctness and the durable outcome's recorded mode.

These are interface and fail-closed decisions. None permits a new POC language
feature, transport path, storage bypass, or durable record kind.

## Proposed Decision

### Commit-check evaluator dependency and ownership

`riffdb-commit` may depend directly on `riffdb-contract-ir` and
`riffdb-invariant` for three coordinator-owned tasks only: exact historical
`CommandPlan` matching, grammar-v1 index derivation from that checked plan, and
pure transaction-current commit-check evaluation. ADR-0007 and SPEC Section 5.2
are amended to include those dependencies in the `riffdb-commit` row. This does
not permit the coordinator to compile contracts, parse source, reinterpret an
unchecked plan, or duplicate evaluator logic.

The coordinator owns a private transaction-current value source assembled
mechanically from the exact normalized input, fixed admitted `tx.time`, current
binding/root observations returned by the short authoritative transaction, and
proposed complete post-images. It passes that owned value input and the exact
checked historical commit-check plan to `riffdb-invariant`. No storage
transaction, handle, callback, reader, engine object, clock, entropy source, or
asynchronous operation crosses into the evaluator. The evaluator performs no
I/O and remains the sole implementation of checked expression and commit-check
semantics.

Value resolution is closed and positional. Input expressions resolve only from
the frozen normalized input, and transaction time resolves only from the frozen
admitted logical time. Each mutate/create binding resolves only to its proposed
complete post-image. Each read-only binding and each internal aggregate-root
validation read resolves only to its transaction-current complete record.
Missing, duplicate, out-of-order, or unmatched semantic positions are
`EvaluationError::Integrity`; the coordinator never falls back from a missing
post-image to a current pre-image or vice versa.

`riffdb-runtime` continues to depend on the same invariant evaluator for
snapshot execution, but does not gain a coordinator callback or a wrapper around
transaction-current validation. `riffdb-storage-api` remains structurally aware
and IR-blind outside its already accepted projection-schema value dependency.
The coordinator retains its private commit-candidate semantic proof, distinct
from catalog-owned lineage evidence, and storage alone retains the transaction
and progression type state.

### Lineage-proved record normalization

ADR-0013's catalog-owned `ResolvedExecutablePlan` carries a shared opaque
`LineageMaterializationProofV1` over the exact bounded active chain. The
catalog owns the opaque command-materialization evidence API that applies it;
the coordinator is the only command-path orchestrator and invokes that API at
two fixed points:

1. after the synchronous storage read has copied a complete owned
   `ReadSnapshot` and closed its engine view, but before runtime evaluation; and
2. after transaction-current binding/root records have been read and their
   dependencies and raw physical content have been compared, but before they
   enter the private commit-check value source or any index/post-image validation
   that consumes their declared fields.

At both points, the catalog-owned pure operation identifies the exact stored
writer through the proof, applies only the opaque writer/executing-schema mask,
inserts null only for fields satisfying
`writer_ordinal < introduction_ordinal <= executing_ordinal`, and returns a
canonical `FieldId`-ordered record. It preserves every present unknown field and
value. A numeric `ContractVersion` comparison, field optionality alone, or the
stored compatibility report never authorizes materialization. WP-100 consumes
opaque evidence and normalized results but does not inspect masks or implement
proof application. Storage remains IR-blind and never receives the proof or mask.

On a successful first application, the catalog returns opaque `Ready` evidence
containing the bounded normalized snapshot and current-recheck evidence. Its
retained normalized semantic bytes plus all retained nonempty mask bitset payload
bytes fit together inside the existing 16 MiB command-snapshot ceiling; the 2
MiB mask structural maximum is not an extra budget. Checked accounting accepts
the combined charge at exactly 16 MiB.

If valid insertion exceeds the shared cap, the catalog instead returns opaque
resource evidence retaining only the original bounded raw `ReadSnapshot` plus
the exact `ResolvedExecutablePlan` and shared proof. Returned resource evidence
retains no over-budget normalized data or masks, and runtime/invariant evaluation
receives no partial record. The exact charge and result are deterministic;
internal allocation strategy, transient masks, and evaluation order are not
semantic interfaces.

Inside the write transaction, the coordinator compares influential
absence/version/epoch dependencies first. `DependencyChanged` aborts and
reevaluates before current recheck. For each equal present dependency, the
coordinator passes the raw current observation to the opaque evidence's catalog-
owned pure current-recheck operation. That operation requires equality with the
retained raw physical observation in target, entity version, writer identity,
schema binding, canonical field order, and every known or unknown field/value,
then returns the normalized current value. Same-version or binding drift is
`InternalDefect`, not `DependencyChanged`. It performs no catalog lookup,
storage callback, or additional read inside the write transaction.

Each inserted null adds exactly six canonical bytes: four for `FieldId` and two
for the canonical value-version/null-tag pair. The coordinator rechecks the
existing 1 MiB record, 16 MiB owned-snapshot, and immutable evaluation-budget
limits using checked arithmetic. A proof-authorized expansion that exceeds an
existing execution limit is the closed dependency-sensitive
`ExecutionFault::ResourceLimit`. It may become durable `ExecutionFailed` only
through ADR-0012's separate terminalization. That recheck compares every
absence/version/epoch dependency first, then invokes the resource evidence's
catalog-owned pure current-recheck operation with each transaction-current raw
binding/root observation. That operation requires exact equality with the
retained raw physical observation across absence/presence, target, entity
version, writer identity, schema binding, canonical field order, and every known
or unknown field/value before repeating the same deterministic materialization
and exact charge. The coordinator may terminalize only if
that pass reproduces the same valid over-limit `ResourceLimit` result. A changed
dependency reevaluates; raw drift, successful normalization, a different fault,
or an impossible proof/cap result is integrity. The pure recheck performs no
catalog lookup, storage callback/additional read, runtime evaluation, clock, or
entropy.

A missing exact-writer, descendant-writer, genesis, or required field; a foreign
lineage/version/hash; malformed proof or mask; or any other unproved omission is
integrity. Pending remains byte-identical, and no sequence, mutation, outcome,
event, outbox intent, commit, durable provenance, or `ExecutionFailed` record is
written. Durable-record integrity or a proof/cap invariant reached after catalog
activation returns opaque `InternalDefect`, stops the coordinator, and keeps
authoritative readiness false. It is never `ResourceLimit`, validation, or a
declared business outcome.

`riffdb-runtime` consequently removes blind optional-field fill for stored
records. It accepts already-normalized complete declared fields, retains
unknowns in mutation post-images while hiding them from historical expressions,
and returns integrity for any remaining missing declared field. New-record
construction continues to insert compiler-required nulls under its exact
construction plan; that is not historical materialization.

WP-120 maps a candidate activation that would exceed ADR-0013's 4,096 active-
bundle ceiling to one root `ValidationCode::TooManyItems` issue. It maps a
candidate that would exceed the 64 MiB exact canonical-byte sum or 2 MiB
process-local proof charge to one root `ValidationCode::TooLong` issue. Each is
the existing public `Validation` kind with `CorrectRequest`, is returned before
coordinator submission, and retains the ordinary authenticated contract-
deployment audit path. Catalog storage failure remains storage failure. Existing
over-limit or malformed history is startup `InvalidHistoricalEvidence` and a
redacted `InternalDefect`, not caller-correctable validation.

### Zero-mutation evaluated outcomes

For an admitted mutating command, a successful `EvaluatedCommand` with zero
mutations is a commit-required declared business outcome under `OUT-003`; it is
not reclassified as an unjournaled read-only execution. The coordinator always
reads and compares every influential absence, entity-version, and range-epoch
dependency before choosing either mutation branch. A changed dependency writes
nothing and follows the accepted bounded full-reevaluation policy.

When the evaluated mutation set is empty, the coordinator skips the historical
commit-check plan and skips mutation-affected index-entry, prefix-target, epoch,
and index-delta derivation. There is no mutable post-image value source in this
branch, and no current pre-image may be substituted. The coordinator still
freezes and reserves the zero-mutation sequence-free write plan, assigns one
nonzero `CommitSequence` only after validation and reservation, and atomically
replaces Pending with the declared terminal outcome together with its provenance
and commit record. The result retains ordinary committed-outcome replay and
uncertain-response recovery semantics.

When the evaluated mutation set is nonempty, it must contain exactly one complete
mutation for every mutable create/mutate binding in the exact checked historical
plan, with no missing, duplicate, or extra binding. Failure of that coverage
proof is `EvaluationError::Integrity`. Only after coverage succeeds does the
coordinator expose those proposed complete post-images to the sole invariant
evaluator, evaluate the complete historical commit-check plan, and derive index
entry and mutation-affected epoch changes. A mutable binding never resolves to a
transaction-current pre-image in either branch.

### Bounded full-reevaluation ceiling

`riffdb-commit` privately owns the fixed v1 ceiling
`MAX_COMMAND_EVALUATION_ATTEMPTS_V1: usize = 3`. The initial materialization
consumes attempt slot one, so one outer invocation may perform at most two
automatic full reevaluations. The ceiling is invocation-local, resets only for a
fresh authenticated and authorized submission, and is unrelated to the
coincidentally three-observation inspect/confirm protocol. It is nonconfigurable,
non-durable, not command-visible, not an executable-IR field, and absent from
every plan hash, public message, and durable encoding.

An attempt slot is consumed immediately before the catalog materializes one
complete owned snapshot. If materialization returns `Ready`, that slot continues
into exactly one `riffdb-runtime` evaluation. If valid lineage expansion returns
the dependency-sensitive `ResourceLimit`, that slot is consumed even though
runtime is not entered. Integrity, storage, cancellation, or deadline failures
are not reclassified as resource faults merely to consume retry budget.
`DependencyChanged`, `CommitCheckRejected`,
`MutationPreconditionChanged`, and changed evidence while terminalizing an
execution fault consume that completed attempt and require full reevaluation.
Before a next attempt, the coordinator proves the prior transaction aborted,
releases the logical capability, checks cancellation and the request deadline,
reacquires all canonical conflict keys, and materializes a new complete snapshot.
Deadline or cancellation may stop progress earlier. `CommitStatusUnknown` never
spends remaining budget on another attempt: writes are fenced and the same
idempotency identity is resolved before any reevaluation.

If the third attempt requires another reevaluation, no fourth attempt begins.
The coordinator discards any never-persisted provenance candidate from that
attempt, releases every non-durable capability, and returns its internal typed
`RetryBudgetExhausted`. The durable Pending admission remains byte-for-byte
unchanged. No application sequence, mutation, outcome, event, outbox intent,
commit, execution-failure record, or durable provenance is written.

`riffdb-service` maps `RetryBudgetExhausted` to the existing public
`ConcurrencyDeadlineExceeded`. This explicitly broadens that existing error from
an actual logical-conflict/request deadline to also cover exhaustion of the fixed
v1 concurrency reevaluation budget. Its numeric code, safe message, error class,
retry recovery action, and gRPC `DEADLINE_EXCEEDED` mapping remain unchanged. A
later caller retry is a new bounded invocation, must use the same idempotency key,
and still resumes the exact stored plan, actor, partition, logical time, and
admitted claims after current authentication and authorization.

### Explicit coordinator durability construction

Every production `riffdb-commit` coordinator constructor requires one explicit
checked process-local durability value. Its closed production variants are
`Sync` and `Group`; it has no `Default`, zero-argument constructor, backend-
inferred mode, or other fallback. `Memory` remains in the storage semantic and
durable enum for memory-adapter/model fixtures, but only a test-only coordinator
constructor may select it. The production constructor input cannot represent
`Memory`.

The selected mode is immutable for that coordinator instance and is carried
unchanged into the storage commit operation, stored outcome, and commit record.
P1 `riffdbd` explicitly passes the code-level `Sync` value from its component
graph and exposes no operator durability selector. `Group` remains a valid
production coordinator value but is not an enabled POC server selection until
the already required scheduling, fairness, latency, crash evidence, and human
review exist. This decision chooses no implicit POC or MVP default and does not
alter the durable `DurabilityMode` encoding.

### Audit input dependency inversion

`riffdb-commit` owns an object-safe, `Send + Sync`
`AdministrationAuditInputView` consumer interface. It exposes by reference only
the complete already checked, pre-sequence semantic fields needed to construct a
service-audit record. It exposes no field or method for the new service-audit
record's coordinator-assigned `AdministrationSequence`, timestamp, storage
handle, append operation, raw credential, free-form error, or constructor for a
policy decision. It does expose the checked `ServiceAuditLinkV1` by reference.
The link's `ControlPlane { administration_sequence }` member identifies the
already-authoritative control-plane transition linked from this audit attempt;
it is not the sequence assigned to the new service-audit record.

`riffdb-service` continues to own the concrete `ServiceAuditInput` and implements
the view. `riffdb-commit` never depends on `riffdb-service` and never duplicates
that concrete type. `AdministrationAuditExecutor` consumes the view, samples its
injected `AdministrationClock` exactly once at the accepted lifecycle point,
copies the checked result link unchanged, allocates the new audit record's
administration sequence only inside the storage transition, and copies the
remaining checked fields into the storage-owned record. Storage does not accept a
caller-selected timestamp or sequence for the new record.

Principal-less bootstrap does not use the general view as a way to forge a
service audit. `riffdb-commit` owns a separate opaque, nonserializable
`BootstrapCompoundAuditProof`, constructible only inside the checked bootstrap
coordinator path from the accepted bootstrap preparation. The proof is consumed
by the atomic bootstrap transition that creates or replays the principal-less
`started` record under ADR-0007 and ADR-0009. It is neither a general append port
nor a service-constructible authorization token.

### Provenance attempts and pending recovery

ADR-0018 remains controlling. A `ProvenanceId` belongs to one successfully
evaluated commit attempt, not to the durable Pending admission itself. A proven
pre-commit abort internally discards that attempt's never-persisted, never-exposed
candidate. A later safe reevaluation of the same durable Pending admission,
including after process loss, is a new attempt and may request one new provenance
candidate only after successful evaluation and before opening its authoritative
write transaction.

No regeneration is allowed while resolving an uncertain result from the same
attempt. After `CommitStatusUnknown`, the coordinator fences writes and resolves
the same-key durable state before any reevaluation or source call. A committed-
outcome replay returns the originally stored provenance identity and never
invokes the source. An `ExecutionFailed` replay has no provenance identity and
also never invokes the source. A proven noncommit may return to Pending and begin
a new attempt. Pending continues to store no provenance identity.

The WP-100 phrase "no regeneration on pending resume" is therefore replaced by
"no regeneration during uncertain same-attempt resolution; a proven-noncommit
reevaluation of durable Pending is a new attempt under ADR-0018." Terminal replay
continues to prohibit generation.

### Grammar-v1 index covered values

Grammar and executable IR version 1 define index key components only. They define
no covering-field declaration. Every WP-100 index entry produced for a
grammar/IR-v1 plan therefore uses the one canonical empty `CanonicalRecord` as
`covered_values`. The coordinator may derive index keys only from the exact
declared index fields and may not copy the entity, indexed fields, or an ad hoc
projection into covered values.

The storage semantic API and durable codec remain generic and retain their
bounded `covered_values` field and covered-value epoch behavior. This preserves
backend conformance and an explicit future extension point; it does not enable a
v1 producer to write nonempty covered values. Adding covering fields requires an
accepted language/IR version decision plus durable compatibility and migration
semantics. It cannot be inferred from the existing storage field.

For v1 coordinator writes, entry presence or key changes determine index entry
mutations. Backend conformance tests continue to prove that a generic
covered-value replacement advances every affected whole-index and complete
leading-prefix epoch even though the v1 compiler cannot currently produce one.

### Checked derived-index bounds and epoch exhaustion

Checked grammar/IR-v1 plan construction uses a conservative, checked-arithmetic
upper bound over mutable bindings, assigned fields, declared index components,
and component byte maxima. Before plan hashing it rejects any successful shape
that can exceed 4,096 index-entry mutations, 4,096 mutation-affected prefix
targets, 4,096 combined binding/root/affected-prefix validation positions, or
16 MiB for affected targets and their current epoch observations. The estimator
deduplicates a guaranteed whole-index bucket once per stable `IndexId` and the
unchanged leading prefixes before the earliest possibly changed component for
one replacement; it need not prove runtime equality or cross-binding
deduplication. This conservative compiler-specific lower acceptance limit is
authoritative for v1.

The coordinator still constructs index entries and affected targets
incrementally and storage still checks exact count and byte bounds. If one of
those guards is reached from a canonical checked plan, the coordinator treats it
as `InternalDefect`; it never persists or publicly returns
`ExecutionFailureCode::ResourceLimit` for that impossible shape.

If an affected current epoch is `Value(u64::MAX)`, advancing it returns internal
`StorageErrorKind::SequenceExhausted`. The complete transaction aborts before
application-sequence assignment, Pending remains unchanged, and no command
record is durable. The current executor call returns opaque `InternalDefect`,
the actor transitions to `Stopped` and readiness fails, and queued or future
work returns `CoordinatorStopped`. Because rollback is proven, this path is
never `StorageUnavailable`, `OutcomeUnknown`, or `CoordinatorFenced`.

### Commit-check arithmetic classification

A false transaction-current commit-check predicate remains
`CandidateValidationRejection::CommitCheckRejected` and causes no application
commit or terminal execution-failure record. `EvaluationError::Integrity` remains
an internal integrity defect and is never converted into caller data.

An `EvaluationError::Arithmetic` after exact transaction-current dependency
revalidation is a deterministic `ExecutionFailureCode::ArithmeticFault` under
ADR-0012. The non-durable storage-control enum gains the additive variant
`CandidateValidationRejection::CommitCheckArithmeticFault`. The single-purpose
variant cannot represent `ResourceLimit` or an integrity defect. The coordinator
maps it to `ExecutionFailureCode::ArithmeticFault`, abandons the application
candidate before sequence assignment, and rolls back that candidate transaction.

The coordinator then uses ADR-0012's existing execution-failure transition with
the exact Pending admission, evaluated dependencies, and arithmetic code. That
transition revalidates dependencies in its own short transaction. Unchanged
dependencies atomically replace Pending with `StoredExecutionFailedV1`; changed
dependencies leave Pending and require reevaluation. No application sequence,
entity mutation, event, outbox intent, provenance record, outcome, or commit
record is written for the execution failure. A storage failure retains the
accepted proven-abort versus unknown-status recovery behavior.

Because successful runtime evaluation precedes the authoritative transaction,
this late commit-check fault occurs after one provenance candidate has already
been sourced under ADR-0018. The coordinator discards that never-persisted,
never-exposed candidate when it abandons the application transaction. It does not
source a second candidate while terminalizing the failure, and no provenance
record or provenance link is written. ADR-0018's no-source rule for
`ExecutionFailed` continues to apply to faults known before successful runtime
evaluation; this late commit-check case is the narrow exception.

The new rejection variant is not a durable tag, public error value, runtime
outcome, or permission for a backend to evaluate the commit check.

### Absent capability-revoke targets

Authorization facts distinguish a present revoke target from an absent target.
Present targets retain the complete ADR-0009 record facts and the accepted rule:
ordinary `RevokeCapability` authority must prove its complete subset relation,
while `AdministerCapabilities` may authorize the exact database/environment
administration operation.

An absent target carries only the checked request ID, requested `CapabilityId`,
and trusted server/request `DatabaseId` and `Environment`. No revision,
principal, lifecycle, audience, grant, issue time, or expiry fact is invented.
Because subset authorization cannot be proven, ordinary `RevokeCapability`
authority fails closed. Only a current authorizing capability with
`AdministerCapabilities` for that exact database and environment may receive an
authorized absent-target preparation.

After any queue wait, the coordinator reloads both the authorizing capability
and target in its transaction, samples the accepted authorization clock, and
passes mechanically lowered transaction-current facts to the policy verifier.
An absent-target preparation is valid only if the target is still absent and the
current authorizer still has exact `AdministerCapabilities` authority. It may
then return storage's typed `CapabilityNotFound` result without allocating a
capability-administration transition sequence. The invocation's already required
`started` and terminal service-audit records retain their own separately assigned
administration sequences.

If the absent target has appeared, the coordinator must not promote the
absence-authorized preparation into a present-target transition. It abandons the
short transaction without a sequence or write and returns the internal typed
`CapabilityPreparationChanged`. The service reloads the now-present complete
target record, rebuilds the full present-target facts, reruns current policy,
obtains a fresh bounded executor permit, and resubmits; it does not append another
`started` record for the same invocation. Capability records are retained rather
than deleted, so absence-to-presence is monotone and this branch cannot create an
unbounded existence-change loop. If any authorizer fact has changed, the
transaction-current verifier instead denies the transition under ADR-0009; it is
not reinterpreted by the coordinator. Both paths assign no capability-
administration transition sequence.

This rule prevents ordinary delegated revokers from using the operation as a
capability-existence oracle. It does not hide `CapabilityNotFound` from an exact
database/environment administrator and does not turn absence into a synthetic
capability record.

### No-transition control-plane audit classification

The existing storage phase/link matrix and durable encoding are preserved; this
decision amends ADR-0007's service-side terminal selection within that closed
matrix. A control-plane terminal audit uses `Succeeded` with
`ServiceAuditLinkV1::ControlPlane(sequence)` only when the executor knows the
exact authoritative administration sequence for a new or replayed transition.
It uses `OutcomeUncertain` under the already accepted unknown-status rules when
the transition status cannot be established.

An authorized operation that completes with a bounded typed no-transition result
uses terminal `Failed` with `ServiceAuditLinkV1::None`. In the POC this class is
exactly catalog expected-version mismatch, catalog bundle conflict, capability-
ID conflict, and `CapabilityNotFound`. The audit phase describes absence of an
authoritative transition; it does not replace the safe API-neutral result with a
generic public error. The service appends the required terminal record before
releasing that typed result. The already accepted target list is unchanged.

Already-active, already-created, and already-revoked recovery states identify
their original authoritative transition sequence and therefore use `Succeeded`
with that exact control-plane link. Storage/coordinator recovery metadata supplies
the original sequence for this audit link. In particular, ADR-0009's API-neutral
`AlreadyCreatedTokenUnavailable` result continues to omit that sequence; this
decision does not add it to the service or public result. Internal token-digest
collision remains an internal failure with no caller detail. Bootstrap conflict
retains ADR-0007 and ADR-0009's pre-bootstrap telemetry behavior and does not gain
a fabricated sequence or general audit append.

## Options Considered

1. **Consumer-owned audit view plus a separate bootstrap proof:** Proposed. It
   preserves concrete service ownership, coordinator authority, and acyclic
   crate dependencies.
2. **Move `ServiceAuditInput` into `riffdb-commit`:** Rejected because service
   owns operation classification, target construction, and checked pre-sequence
   input semantics.
3. **Make `riffdb-commit` depend on `riffdb-service`:** Rejected because service
   already consumes commit executors and this creates a circular ownership graph.
4. **Persist provenance in Pending:** Rejected because it changes ADR-0018's
   durable format and generates identities before a successful evaluation.
5. **Populate v1 covered values from the whole entity or indexed fields:**
   Rejected because neither choice is declared by the grammar or typed IR.
6. **Treat commit-check arithmetic as predicate false or integrity:** Rejected;
   either loses ADR-0012's deterministic terminal classification or misstates a
   false invariant.
7. **Return `CapabilityNotFound` to an ordinary revoker without subset facts:**
   Rejected because it authorizes from incomplete target facts and creates an
   existence oracle.
8. **Encode a no-transition result as `Succeeded + None`:** Rejected because it
   widens the accepted control-plane success matrix and loses the distinction
   between a known authoritative transition and an ordinary closed conflict.
9. **Put transaction-current commit-check evaluation behind a runtime port:**
   Rejected because runtime neither owns the authoritative transaction-current
   value assembly nor historical-plan matching, and the wrapper would obscure
   rather than remove the same IR/evaluator dependency.
10. **Duplicate commit-check evaluation in the coordinator or storage backend:**
    Rejected because it creates divergent semantics and makes backend behavior
    depend on executable IR interpretation.
11. **Use only a wall/request deadline as the reevaluation bound:** Rejected
    because it provides no fixed evaluation ceiling, makes deterministic tests
    depend on timing, and can leave an invocation unbounded when no earlier
    deadline interrupts repeated invalidation.
12. **Make the attempt ceiling configurable, put it in IR, or persist a retry
    counter:** Rejected for v1 because equal commands could change behavior by
    deployment configuration, while IR or durable state would expand hashed and
    compatibility-sensitive formats. Three total attempts conservatively allow
    two recovery opportunities while retaining a small hard bound.
13. **Add a new public retry-budget error:** Rejected because the existing
    transient concurrency error already carries the required retry recovery and
    protocol mapping; its broadened trigger is made explicit here instead.
14. **Evaluate zero-mutation commit checks against current pre-images:** Rejected
    because it changes the meaning of mutable binding references, violates exact
    post-image semantics, and can turn a declared business rejection into an
    undeclared commit-check result.
15. **Skip dependency validation for a zero-mutation outcome:** Rejected because
    snapshot observations can influence the declared outcome even when the
    command proposes no entity mutation.
16. **Let runtime fill every missing optional field:** Rejected because type
    optionality does not prove the stored writer predates field introduction;
    same-version corruption would become valid state.
17. **Reject every missing stored field:** Rejected because it makes the accepted
    compatible optional-null evolution class non-executable for ancestor data.
18. **Infer ancestry from numeric versions or compatibility reports:** Rejected
    because versions are identifiers rather than chain positions and reports are
    reproducible display evidence, not parent/hash authority.
19. **Return over-budget normalized data or masks as resource evidence:**
    Rejected because retained evidence would violate the same command ceiling
    whose failure is being reported and create two possible replay inputs.
20. **Terminalize lineage overflow from dependency equality alone:** Rejected
    because equal versions do not prove equal writer/schema/field content and
    cannot establish that the same deterministic expansion still overflows.
21. **Default coordinator durability from the backend or `Default`:** Rejected
    because deployment composition could silently change acknowledgement and
    durable outcome semantics. Production construction must make the choice.

## Consequences

- Commit, service, policy, and storage retain one clear owner for each shared
  semantic type and can compile without circular dependencies.
- Commit can satisfy ADR-0004's exact-plan and transaction-current validation
  obligations using the one invariant evaluator, while runtime and storage keep
  their accepted ownership boundaries.
- Every outer invocation has a deterministic evaluation ceiling; exhaustion is
  nonterminal and cannot consume or rewrite its durable admission.
- Pending recovery has an explicit attempt boundary without adding durable
  provenance reservation state.
- Grammar-v1 index bytes are deterministic and do not invent a covering-index
  language feature.
- Commit-time arithmetic uses the existing dependency-validated terminal failure
  path and consumes no application sequence.
- A zero-mutation business rejection retains `OUT-003` durability without
  fabricating mutable post-images, while every influential snapshot dependency
  is still revalidated and every nonzero mutation set has exact plan coverage.
- Compatible optional-null evolution has one executable path: catalog proves
  ancestry and owns opaque first-pass/current-recheck proof application, commit
  orchestrates both calls, and runtime rejects unproved omissions. Unknown
  descendant fields survive both normalization and older-plan mutations.
- Transaction-current dependency comparison precedes normalization, and equal
  versions additionally prove exact raw physical equality against the retained
  observation. This preserves short-transaction closure without trusting a
  version alone or introducing catalog/storage callbacks.
- Valid over-budget lineage expansion returns one catalog-owned bounded source
  of truth: opaque resource evidence containing the raw snapshot plus exact
  resolved plan/proof and no over-budget normalized value or mask.
  Terminalization reproduces the fault only after dependency equality and the
  evidence's exact raw-observation recheck.
- Production coordinator construction makes durability an explicit immutable
  input. `Memory` remains test-only, and P1 composition visibly passes code-
  level `Sync`; it exposes no operator selector, does not enable `Group`, and
  introduces no fallback.
- Missing capability targets fail closed for delegated revokers while exact
  administrators retain the typed not-found result.
- Typed no-transition control-plane results have one audit classification and
  never fabricate or omit a control-plane sequence on a success link.
- WP-100 must add focused integration tests before claiming completion; these
  clarifications do not broaden the POC.

## Compatibility

This decision adds no public protocol field, Protobuf field, durable record type,
durable tag, storage key, contract grammar production, or executable IR node.
The empty covered-value record uses the already accepted durable field and
encoding. The candidate rejection, audit view, bootstrap proof,
transaction-current value source, `RetryBudgetExhausted`,
`CapabilityPreparationChanged`, and absent-target facts are non-durable Rust
interfaces and may be added without a data migration. The added crate
dependencies do not alter a public or durable encoding.

The zero-mutation clarification adds no public field, durable field, record kind,
or IR node. It uses the already accepted terminal outcome, provenance, and commit
records with an empty mutation/index/epoch portion of the command graph. Exact
nonzero mutation coverage is a coordinator-private semantic proof.

The lineage clarification adds no public or durable field, Protobuf message,
storage key, bundle byte, IR node/tag, or hash input. The proof, masks,
introduction ordinals, and normalized transaction-current value source are
process-local Rust values. Existing stored records and bundles require no data
migration; startup revalidates them against the exact active chain and fails
closed if that evidence is not valid.

The overflow amendment adds opaque process-local catalog evidence and operations
but no durable DTO, public error, terminal code, or encoded state. It narrows
returned resource evidence and the order used to establish the already accepted
`ResourceLimit` terminal state. The durability amendment adds no
`DurabilityMode` tag, durable field, or operator configuration; it constrains
only process-local construction and P1 component-graph composition.

The retry decision adds no public error kind or wire value, but it intentionally
broadens the documented cause of existing `ConcurrencyDeadlineExceeded`. The
private attempt counter is not recoverable across process loss because it is an
invocation budget, not durable command state; a recovered submission begins its
own bounded invocation against the unchanged admission.

A future nonempty covered-value producer is an incompatible language/IR semantic
extension even though the storage codec already represents the field. It requires
its own accepted compatibility and migration decision.

## Security

Audit timestamps and sequences remain coordinator/storage assigned. The audit
view and bootstrap proof contain no raw credential or policy-construction escape.
Provenance candidates never become a retry oracle. The pure evaluator receives
only bounded owned semantic values, never an authoritative handle or ambient
capability. Arithmetic faults retain safe closed public classification and no
internal evaluator text. Absent revoke facts cannot fabricate a target grant,
and ordinary revokers receive no existence signal. All diagnostics remain
redacted. A fixed attempt ceiling limits adversarial invalidation work without
turning a stale observation into a terminal failure or bypassing reauthorization.
Only exact parent/hash ancestry can authorize null insertion. Untrusted writer
versions, stored reports, masks, and malformed omissions never become runtime
values or caller-visible diagnostics. The fixed proof/mask and existing
record/snapshot budgets bound adversarial lineage and expansion work.
Returned overflow evidence retains no over-budget normalized data or masks, and
terminalization cannot use a version-only comparison to convert changed raw
state into a durable failure. Explicit durability construction prevents a
backend or constructor fallback from silently weakening acknowledgement.

## Testing

- Architecture tests prove `riffdb-commit` does not depend on `riffdb-service`
  and that only service implements the production audit-input view.
- Architecture tests permit the two narrow direct commit dependencies while
  continuing to reject compiler, syntax, service, transport, and concrete-engine
  dependencies. Source/import tests prove runtime and commit both call the sole
  `riffdb-invariant` implementation and contain no duplicate evaluator.
- Differential fixtures pass identical checked commit-check plans and canonical
  value inputs through the coordinator's private value-source adapter and a
  direct `riffdb-invariant` reference call and require the same result or typed
  evaluation error.
- Compile-time/source-shape tests prove the private value source owns `'static`
  semantic values and imports no storage transaction, reader, callback, engine,
  clock, entropy, or async type.
- Retry tests prove two invalidations can precede a third-attempt commit; a third
  invalidation starts no fourth attempt, leaves Pending byte-identical, and
  writes no sequence or terminal graph. Deadline/cancellation tests stop before
  reacquisition, unknown status never retries, and property histories bound
  evaluation, acquisition, and provenance-source calls to three per invocation.
- Public-error and gRPC fixtures prove retry-budget exhaustion has the unchanged
  `ConcurrencyDeadlineExceeded` code, safe text, class, recovery action, and
  status mapping. A no-index entity-dependency case exercises the branch.
- Audit tests prove one clock sample, coordinator assignment of the new record's
  sequence, absence of a view accessor for that sequence, unchanged copying of a
  prior control-plane transition sequence inside the checked result link, exact
  field copying, and bootstrap proof non-constructibility.
- Provenance source call-count tests cover first attempt, proven abort and new
  attempt, uncertain same-attempt resolution, process-loss Pending recovery, and
  committed-outcome and execution-failure replay. The commit-check arithmetic
  case proves one discarded, never-exposed source candidate and no durable
  provenance record.
- Coordinator index goldens prove every v1 put has canonical empty covered
  values; storage conformance retains a generic nonempty covered-value epoch test.
- Command tests force commit-check overflow with unchanged and changed
  dependencies and assert terminalization, reevaluation, rollback, and no
  sequence gap.
- Zero-mutation outcome tests prove every influential dependency is compared,
  changed evidence reevaluates without a write, no commit-check or index
  derivation call occurs, and the declared outcome, sequence, provenance, and
  commit record persist atomically and replay unchanged. Nonzero fixtures prove
  exact one-per-mutable-binding coverage and reject missing, duplicate, and extra
  mutations as integrity failures without pre-image fallback.
- Snapshot and transaction-current normalization tests cover ancestor, equal,
  and descendant writers; same-version/genesis/required omissions; exact
  six-byte null expansion; canonical ordering; unknown-field preservation
  through older-plan mutation; malformed/foreign proof failure; and no runtime
  blind fill. Combined snapshot accounting accepts normalized semantic bytes
  plus retained nonempty mask bitset payload bytes at exactly 16 MiB and rejects
  one byte more, with aligned vector position adding no charge. One-byte-over
  WP-050 tests prove returned resource evidence contains only the raw snapshot
  and exact resolved plan/proof and no over-budget normalized data/masks. WP-100
  terminalization fixtures instrument dependency comparison followed by the
  opaque current recheck and deterministic re-derivation; changed evidence
  cannot persist, raw drift is integrity, and only the same reproduced overflow
  produces the accepted `ResourceLimit` execution failure.
- Transaction-current tests compare dependencies before invoking the opaque
  current recheck, abort a change without invoking it, accept its exact raw
  equality and normalized-current result, and classify equal-version changes to
  target, writer, schema binding, known fields, or unknown fields as stopped-
  readiness `InternalDefect`. Architecture/call-count tests prove no catalog
  lookup or additional storage I/O occurs while the write transaction is open.
- Catalog/service tests cover 4,096 versus 4,097 active bundles, exactly 64 MiB
  versus one byte more canonical lineage bytes, a synthetic checked proof-charge
  calculator at exactly 2 MiB versus one byte more, and the separate valid
  1,810,703-byte maximum/headroom proof. They assert root
  `TooManyItems`/`TooLong` with `CorrectRequest` before
  coordinator submission, ordinary audit behavior, parent gaps/cycles/hash
  substitution, and startup `InvalidHistoricalEvidence` with readiness false.
- Policy/coordinator tests cover present and absent revoke targets, ordinary and
  administrator permission, database/environment mismatch, target appearance,
  exact no-write `CapabilityPreparationChanged`, fresh service authorization
  without a second `started` record, authorizer revision/revocation/expiry
  change, and redacted diagnostics.
- Service-audit tests prove exact `Succeeded + ControlPlane`, `Failed + None`,
  and uncertain classifications for every closed control-plane result.
- Compile/source-shape tests prove every production coordinator constructor
  requires an explicit `Sync` or `Group`, has no `Default`, zero-argument, or
  backend-inferred path, and cannot receive `Memory`. WP-130 component-graph
  tests prove the P1 child process explicitly passes code-level `Sync`, exposes
  no POC operator durability selector, and does not enable `Group`.

## Requirements and Work Packages

- **Requirements:** `SYS-004`, `STO-001`, `DSL-005`, `TXN-001`, `TXN-002`, `TXN-012`, `TXN-013`,
  `TXN-030`, `TXN-040`, `TXN-041`, `TXN-042`, `TXN-043`, `TXN-044`, `ENT-002`,
  `ENT-003`, `ID-005`, `SEC-001`, `SEC-002`, `SEC-003`, `SEC-004`, `OUT-003`,
  `MCP-046`, `POC-003`, `POC-009`, `REC-001`, and `REC-002`
- **Defines or corrects:** the narrow WP-060 storage-control variant, WP-050
  command-materialization evidence API, `WP-100`,
  the WP-110 capability-revoke interface, WP-120 audit input/orchestration,
  retry and catalog-limit mapping, WP-130 gRPC/startup mapping evidence, and
  WP-200 final evidence metadata
- **Final evidence:** `WP-100`, `WP-120`, `WP-130`, and `WP-200`

WP-200's `required_adrs` reconciliation adds both already-accepted ADR-0022,
which its final generated/durable-artifact evidence already consumes, and this
ADR-0023. No declared work-package dependency or gate changes.

## 2026-07-22 aggregate-capacity amendment

ADR-0039 adds one closed pre-sequence result owned by `riffdb-storage-api` and
produced through the WP-065 durable codec:
`Fits(EncodedWriteSetUpperBound) | ExceedsAcceptedAggregateCap`. The aggregate-
cap branch is available only after every complete per-record upper bound and the
checked aggregate sum have succeeded, at the final comparison with the accepted
16 MiB staged-write cap.

WP-100 is only a consumer. Its candidate maps exactly
`ExceedsAcceptedAggregateCap` to the private origin-specific
`CapacityUnavailable` decision and then to the existing public
`StorageUnavailable`. That path assigns no sequence, attempts no authoritative
write, leaves the Pending row unchanged, does not fence or stop the coordinator,
and keeps readiness true. It does not construct the storage result, inspect an
error string, or classify a general codec failure.

Every per-record limit, checked arithmetic overflow, malformed or noncanonical
encoding, unknown tuple, key/envelope mismatch, reservation undercharge,
retained-plan substitution, or other codec/integrity failure remains fatal under
the existing coordinator rules. Post-sequence or attempted-write disposition is
unchanged.

## Decision Deadline

Exact acceptance is required before WP-100 merges the command executor, retry
loop, audit executor, index write planner, commit-check arithmetic path, or
capability-revoke coordinator. The accepted zero-mutation clarification is also
required before WP-100 merges commit validation or graph construction. The
accepted lineage clarification is required before WP-100 merges snapshot or
transaction-current record validation and before WP-120 publishes deployment-
limit error mapping. The
foundation clock, initialization, immutable outcome, and idempotency
preparation/inspection interfaces do not depend on this decision.

The accepted 2026-07-21 amendments are required before WP-100 publishes lineage
overflow terminalization or a production coordinator constructor and before
WP-130 publishes P1 component-graph composition. The manifest adds WP-050
ADR-0023 ownership, evidence/current-recheck deliverables and fixtures, plus
WP-100/WP-130 consumption evidence under existing paths and commands; it adds no
package, hard WP dependency, allowed path, acceptance command, protocol, or
durable-format change. Atomic commit requirement `TXN-042` remains outside the
catalog package.
