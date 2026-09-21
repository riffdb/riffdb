# RiffDB transaction / concurrency / idempotency design review

Scope: SPEC.md §9 (3011–3608), §6 (2205–2360), §7.6 (2588–2665), §21 risk
register (7628–7649), ADR-0003, ADR-0005, ADR-0012, ADR-0058, ADR-0059, plus
ADR-0061/0094/0095/0098/0101/0072/0170 and the compiler/IR code that implements
the conflict and commit-check derivation.

Verdict up front: the *core* of the model is sound. Pessimistic acquisition of
statically derived conflict keys, plus exact transaction-current revalidation of
every influential observation, plus a single sequence-assigning coordinator, is
a coherent and defensible design, and the fail-closed posture around plan
identity, dependency evidence, and integrity faults is unusually disciplined.
The real flaws are not in that core. They are (a) one **critical** hole where
the compiler's own invariant-coverage proof is anchored on the wrong aggregate,
(b) a set of **high**-severity gaps where the design's stated guarantees are
weaker than the prose implies, and (c) several **medium** internal
contradictions between accepted records.

---

## F-01 — CRITICAL: aggregate invariants are silently dropped when a command's first mutating binding belongs to another aggregate

**Evidence.**
- `crates/riffdb-contract-compiler/src/command_lowering.rs:1416-1436` —
  `lower_locality` picks the *first* mutating binding as the "anchor" and
  returns that binding's aggregate as the command's single `aggregate_id`.
- `crates/riffdb-contract-compiler/src/command_lowering.rs:1576-1585` —
  `lower_commit_checks` takes that one `aggregate_id`, and returns early
  (`if aggregate.invariants.is_empty()`) for **that** aggregate only. No other
  written aggregate's invariants are ever lowered into commit checks.
- `crates/riffdb-contract-ir/src/plan.rs:7175-7242` — the IR validator's
  "commit checks do not exactly cover applicable invariants" proof is computed
  against `schema.aggregate(locality.aggregate_id)` — the same single anchor.
  So the validator agrees with the compiler and cannot catch the omission.
- ADR-0170 (`adr/0170-partition-local-cross-aggregate-writes.md:101-118`) and
  SPEC `PERF-005` (SPEC.md:8254-8262) now permit a command to write entities in
  **multiple** aggregates in one partition, with conflict ownership being the
  union of the bindings' keys.

**Reproduced.** A contract with aggregate `Family { root Root, child Child,
invariant non_negative: total >= 0 }` and aggregate `Others { root Other }`,
and a command whose bindings are ordered `mutate Other(...)` then
`mutate Child(...)`, compiles with:

```text
COMPILED: commit_checks=0 root_validation_reads=0 conflict_keys=2 spans_aggregates=true
```

Reordering the two bindings so the `Family` child comes first yields:

```text
CONTROL:  commit_checks=2 root_validation_reads=2 conflict_keys=2
```

Same semantics, same writes, same conflict keys — the only difference is
binding order, and it decides whether `Family`'s aggregate invariant is
enforced at all. The command acquires `Family`'s conflict key (so it is
serialized correctly) but never evaluates `Family`'s invariant, and never emits
the `RootValidationReadPlan` that ADR-0003's 2026-07-13 amendment
(`adr/0003-...md:40-64`) requires for a child mutation under an aggregate
invariant.

**Why it matters technically.** This is a direct violation of `POC-003`
("Every committed state satisfies all supported invariants", SPEC.md:242) and
of the invariant-preservation property (SPEC.md:6805). It is exactly risk
`R-03` (SPEC.md:7634) — "declared conflict keys are too narrow and miss
invariant dependencies", rated *Medium probability / Critical impact* — except
the miss is not in the contract author's `conflict_key` declaration at all; it
is in the compiler's own coverage proof, which is the mechanism the risk
register names as the mitigation. The failure is silent: no diagnostic, no
runtime fault, no integrity incident. A committed state can violate a declared
aggregate invariant and every layer will report success.

**What fails closed and what does not.** Everything downstream of the plan is
fail-closed: the IR validator proves commit checks exactly cover the invariants
*of the anchor aggregate*, the coordinator refuses to substitute pre-images for
missing post-images (SPEC.md:3125), and drift behind an equal version is
`InternalDefect` (SPEC.md:3379-3382). But the anchor selection happens *before*
that proof, and nothing re-derives the applicable aggregate set from the
complete binding set. The proof is sound relative to a premise that ADR-0170
invalidated.

**Severity: critical.** Fix direction: derive the applicable aggregate set from
every mutating binding (not the first), lower commit checks and root-validation
reads for each, and make the IR validator's coverage proof range over that set.
A regression test should assert that binding order cannot change
`commit_checks`/`root_validation_reads`.

---

## F-02 — HIGH: the compiler never proves that a declared `conflict_key` actually protects the invariants it is supposed to protect

**Evidence.** SPEC.md:2744-2751 shows the intended diagnostic verbatim:

```text
error[ADB-C042]: annual budget invariant is not protected by the declared conflict key
help: use conflict_key (organization_id, fiscal_year)
```

No such diagnostic exists. `CompilerDiagnosticCode::ALL`
(`crates/riffdb-contract-compiler/src/diagnostic.rs:367-417`) is a closed
49-entry registry; `RDB-C042` is `CrossPartitionRowPolicy`
(`diagnostic.rs:347,464`), and there is no code whose summary is "invariant not
protected by conflict key". Searching the compiler for any check relating
invariant dependencies to conflict-key components returns nothing:
`command_analysis.rs` validates uniqueness (`:1109`), relationships (`:1162`),
ownership (`:1366`), idempotency (`:1398`), and secret flows — but no
invariant/conflict coverage. `locality.rs` validates only that conflict
expressions are input-computable and derive from the root key field set
(`locality.rs:45-52,163`).

**Why it matters technically.** The design's stated answer to `R-03` is
"compiler dependency checks, runtime read revalidation, adversarial corpus, fail
closed on undeclared mutable dependency" (SPEC.md:7634). Runtime revalidation
covers *reads the plan declares*. It cannot cover an invariant whose subject
fields the plan never reads. So the only thing standing between a misdeclared
`conflict_key` and an invariant violation is the author's care — precisely the
"assertion is not a proof" failure mode ADR-0094 explicitly rejects for
commutativity (`adr/0094-...md:92-95`) but does not close here.

Concretely: `aggregate AnnualBudget { root Budget partition_by organization_id
conflict_key (organization_id) }` with `invariant within_approval:
allocated_amount <= approved_amount` is safe only because the conflict key is
coarse enough to serialize every writer of that root. A finer key
(`conflict_key matter_id`, the exact example in SPEC.md:2747) is rejected today
only if it fails the input-computable/root-key-prefix checks — not because the
invariant is unprotected. The documented diagnostic is aspirational.

**Severity: high.** The gap is real but requires author error to trigger, and
the runtime revalidation layer does catch the *read-dependency* half of the
problem. It is not critical because it is not a silent compiler bug; it is a
missing proof the spec claims exists.

---

## F-03 — HIGH: `ExecutionFailed` is terminal and consumes the idempotency key, but the design has no bounded retention or operator path for the `Pending` states it can leave behind

**Evidence.**
- ADR-0012 (`adr/0012-...md:360-368`): after `ExecutionFailed` durably commits,
  "the old identity can never later mutate"; correction requires a new logical
  command and a new idempotency key.
- ADR-0012 (`:377-381`): an `Integrity` fault "leaves a mutating admission
  pending for operator intervention; the POC provides no online repair path."
- ADR-0005 (`adr/0005-...md:204-205`) lists as a *consequence*: "Admission
  records need explicit abandoned/resumable recovery and bounded retention
  policy." No accepted ADR supplies one. ADR-0085
  (`adr/0085-history-retention-and-startup-scaling.md:158`) says idempotency
  terminal records are retained independently and defers their policy "later,
  per ADR-0082".
- SPEC.md:2656-2659 restates the no-repair-path rule.

**Why it matters technically.** Three distinct hazards compound:
1. A `Pending` row left by an `Integrity` fault is *permanently* unresolvable
   in-process. Every retry with the same identity re-enters the same
   deterministic integrity fault (the plan and snapshot evidence are frozen),
   so the caller's idempotency key is burned forever with no way to distinguish
   "operator will fix this" from "this key is dead". The caller cannot even
   learn which, because the fault is a redacted internal incident.
2. `ExecutionFailed` is terminal *by design* to prevent a known-failed request
   from later succeeding (ADR-0012 option 5, `:467-468`). That is a defensible
   choice, but it means a transient-looking `ResourceLimit` — which the spec
   itself describes as arising from "a validated plan whose bounded data
   reaches a deterministic runtime aggregate budget" (`:263-267`) — permanently
   consumes the key. A caller that retries with the same key after fixing the
   *data* (not the plan) gets the stored failure forever. The spec's answer is
   "submit a new logical command with a new idempotency key", which pushes
   retry-identity management onto every application.
3. There is no expiry, no tombstone, and no compaction for either state, while
   ADR-0085 establishes that history never prunes and startup validates all of
   it. Unbounded `Pending`/`ExecutionFailed` accumulation is therefore a
   startup-cost and storage-growth input with no accepted bound.

**Severity: high.** Not a correctness violation, but an unbounded operational
hazard with no accepted mitigation, and the spec's own risk register does not
carry it.

---

## F-04 — HIGH: crash between reservation and commit is handled, but the *retry storm* case is not bounded by anything durable

**Evidence.**
- SPEC.md:3551-3571: `MAX_COMMAND_EVALUATION_ATTEMPTS_V1 = 3`, "nonconfigurable,
  invocation-local, non-durable, absent from IR and hashes, and resets only for
  a fresh authenticated and authorized submission."
- SPEC.md:2650-2654: exhausting the budget leaves `Pending` byte-identical and
  maps to `ConcurrencyDeadlineExceeded`; "a later caller retry is a new bounded
  invocation under the same idempotency identity and frozen admission."
- SPEC.md:3234-3239: lock timeout and reevaluation-budget exhaustion both map to
  the same public `ConcurrencyDeadlineExceeded`.

**Why it matters technically.** The attempt budget is deliberately
invocation-local and non-durable, which is correct for determinism and for
keeping it out of the plan hash. But it means the *only* thing bounding total
work on one idempotency identity is the caller's willingness to retry. A
contended hot key plus an aggressive client produces unbounded
acquire → snapshot → evaluate → revalidate → abort cycles, each of which is
individually bounded but collectively not. The design has no durable attempt
counter, no backoff obligation, and no admission-side rate limit tied to the
identity. The spec is explicit that this is intentional (no durable counter,
SPEC.md:7757), but it never states what *does* bound the aggregate, and
`R-02` (hot-key serialization, SPEC.md:7633) is rated High/High precisely for
this workload.

The crash-between-reservation-and-commit case itself is handled correctly:
`Pending` is durable, resume reuses the frozen plan/time/actor
(SPEC.md:3056-3059), and `CommitStatusUnknown` fences writes and forces same-key
resolution (SPEC.md:3562-3563). That part is sound.

**Severity: high** for the unbounded-retry aggregate; the crash case is sound.

---

## F-05 — MEDIUM: ADR-0061's "terminal admission" (no durable `Pending` for new synchronous commands) directly contradicts SPEC §9.1 and ADR-0005/0058, and the SPEC never reconciled it

**Evidence.**
- ADR-0061 (`adr/0061-...md:85-125`): "For a newly executed synchronous
  mutation, bounded snapshot materialization and deterministic evaluation MAY
  occur before a durable Pending row or durable `Started` audit exists"; "New
  synchronous v1 commands do not create Pending merely to cross runtime
  evaluation." SPEC `PERF-010` (SPEC.md:8384-8389) restates this as normative:
  "new synchronous commands MUST NOT create Pending merely to cross
  deterministic evaluation."
- SPEC §9.1 step 5 (SPEC.md:3041-3043) still says: "For an allowed mutation,
  the service durably appends `started`, then waits for a bounded
  executor-capacity permit." Step 7 (`:3052-3055`) says the coordinator
  "atomically creates a pending admission".
- ADR-0058 (`adr/0058-...md:75-95`) says the first authoritative transition
  atomically appends `Started` **and** creates the durable `Pending` admission,
  and "The deterministic runtime MUST NOT start until the combined transition is
  durably known committed."
- ADR-0005 (`adr/0005-...md:106-110`) says the coordinator "durably creates or
  resumes a bounded pending admission" *before* runtime evaluation.

**Why it matters technically.** These are not compatible. ADR-0061 permits
evaluation *before* any durable record; ADR-0058 forbids runtime from starting
before the combined `Started`+`Pending` transition is durable; SPEC §9.1
describes the ADR-0058 ordering. ADR-0061 is later and is reflected in
`PERF-010`, but §9.1 — the normative request-lifecycle section a reviewer would
read first — was never updated, and ADR-0058's "Prohibited shortcuts" list
(`:160-173`) still forbids "omitting `Started`, terminal audit, `Pending`,
provenance, outcome, event, outbox, or commit records". The code follows
ADR-0061 (`crates/riffdb-commit/src/command_admission.rs:574-575` handles a
`Vacant` identity and `:379` builds a `CommitIntent::new_for_vacant_terminal_admission`),
so the SPEC text is stale rather than the code being wrong — but a design review
cannot tell which of two accepted records governs without reading the changelog.

**Severity: medium.** No runtime defect; a genuine normative contradiction
between an accepted ADR and the SPEC section that describes it, in the exact
area under review.

---

## F-06 — MEDIUM: ADR-0059's "exactly one mutation aggregate" is superseded by ADR-0170, but ADR-0059's own text and the SPEC's ADR table still assert it

**Evidence.**
- ADR-0059 (`adr/0059-...md:33-47`): "Every grammar-v1 command containing
  `create` or `mutate` bindings MUST have exactly one mutation aggregate. All
  such bindings MUST be owned by that same aggregate. The compiler MUST reject a
  command that creates or mutates entities owned by different aggregates."
- ADR-0170 (`adr/0170-...md:101-118`) reverses exactly this: bindings "MAY
  belong to different aggregates in that partition".
- SPEC.md:7722 still lists ADR-0059 as "exactly one mutation aggregate,
  mutation-only conflict keys". SPEC.md:8254-8262 (`PERF-005`) carries the
  ADR-0170 rule. ADR-0059's header lists `Amended by: ADR-0096, ADR-0097` —
  **not** ADR-0170, even though ADR-0170's header says `Amends: SPEC PERF-005
  and the RDB-C017 diagnostic` and its body says it amends the cross-partition
  mutation rule.

**Why it matters technically.** The amendment graph is the mechanism by which a
reader resolves which record governs. ADR-0170 does not record that it amends
ADR-0059, and ADR-0059 does not record that it was amended. The code implements
ADR-0170 (`crates/riffdb-contract-compiler/tests/cross_aggregate_writes.rs`
asserts two conflict keys and `spans_aggregates()`), so this is a governance
defect rather than an implementation one — but it is the same class of defect
that produced F-01: a rule changed in one place and not propagated to the
proofs that depended on it.

**Severity: medium.**

---

## F-07 — MEDIUM: ADR-0170 ships two explicitly outstanding safety proofs, and the SPEC changelog records them as accepted

**Evidence.** ADR-0170 (`adr/0170-...md:120-125`): "Two proofs are still
outstanding, both tracked by WP-721: deadlock-freedom of the lease order under
an arbitrary acquisition sequence, and crash atomicity of a command writing two
aggregates." SPEC.md:53 records the same: "Two proofs remain outstanding and are
tracked by WP-721: a deadlock arm that fails under an arbitrary acquisition
order, and a crash arm proving a command writing two aggregates is atomic across
a restart." ADR-0170's own Testing section (`:193-203`) states: "No test in this
repository currently covers a cross-aggregate write, because none can be
expressed."

**Why it matters technically.** SPEC §9.3 requires all keys to be sorted and
deduplicated before waiting and a multi-key waiter to be granted only when all
keys can be granted (SPEC.md:3228-3232), and `TXN-023` requires Loom coverage of
multi-key ordering (SPEC.md:3280). The conflict manager does sort and dedup
(`crates/riffdb-conflict/src/manager.rs:530-531`) and has an all-or-nothing
multi-shard grant test (`:1844`), so the *mechanism* is present. What is missing
is the proof that the union lease set spanning two aggregates is acquired in a
globally consistent order — ADR-0170 says the canonical order over
`(aggregate, conflict key)` "was already in place" (SPEC.md:53) but also that
the deadlock arm is outstanding. A design that admits a new multi-aggregate
lease shape while its deadlock-freedom proof is outstanding is shipping an
unproven concurrency property on the critical path.

**Severity: medium** (the ordering is plausibly correct; it is simply unproven
and the record says so).

---

## F-08 — MEDIUM: sequencing zero-mutation business rejections into the same log is defensible, but `R-05`'s stated mitigation is not achievable under the frozen IR

**Evidence.** `LOG-001` (SPEC.md:293) requires every committed declared outcome,
"including persisted no-op business outcomes", to receive a `CommitSequence`.
`OUT-003` (SPEC.md:2600-2609) makes a zero-mutation rejection a full
commit-required command with provenance, commit record, and sequence. `R-05`
(SPEC.md:7636) rates this Medium/Medium and proposes: "consider a separately
ordered outcome journal only through ADR."

**Why it matters technically.** The sequencing itself is *sound* and I would not
call it a flaw: it is what makes replay, provenance, and projection frontiers
uniform, and the alternative (a second ordering domain) would create exactly the
kind of cross-domain ordering problem the design otherwise avoids. The problem
is the mitigation. A separately ordered outcome journal would need its own
ordering relation against `CommitSequence`, which is the sole authoritative
application order (SPEC.md:3194-3195, `ID-002` at :2300-2302). Any such journal
either (a) shares the sequence space, in which case it is not separate, or
(b) introduces a second order, which contradicts `LOG-001`'s stated rationale
("Unifies committed outcomes, provenance, change consumption, and projection
frontiers"). So `R-05`'s escape hatch is not actually available, and the risk
should be recorded as accepted-with-no-mitigation rather than open. The
throughput half of `R-05` is real: a rejection-heavy workload writes a full
commit record, provenance row, and sequence per rejection.

**Severity: medium** (risk-register accuracy, not a runtime defect).

---

## F-09 — MEDIUM: `R-16` (sequence reuse after restore) is narrowed but not closed, and the fence is opt-in

**Evidence.** ADR-0072 (`adr/0072-database-history-incarnation.md:26-65`) adds a
durable monotonic `history_incarnation` bumped only on destructive restore, and
exposes it as an *optional* `observed_history_incarnation` on sequence-anchored
requests. ADR-0072 (`:62-65`): "Idempotency identity, per-record formats,
provenance locator bytes, and cursor token formats are unchanged. The fence
makes staleness detectable at the APIs that resolve observations; it does not
stamp history into identities." ADR-0072 (`:69-78`): "Sequence reuse after
restore remains possible, but clients that carry the incarnation can no longer
silently bind stale observations to new data... the residual risk
(non-participating clients) stays documented." ADR-0050 (`:316-343`) still
carries the underlying limitation, including: "An idempotency key whose row
existed only in the destroyed suffix may be accepted again."

**Why it matters technically.** The idempotency identity is
`database/environment + tenant + principal + lineage + command ID + keyed
digest(caller key)` (ADR-0005:31-38) — it contains no incarnation component, by
explicit decision (ADR-0072 rejected putting it in the identity, `:84-86`). So
after a destructive restore, a caller retrying a pre-restore command with the
same idempotency key finds *no* terminal record (it was in the destroyed
suffix) and the command **executes again**, producing a second, different
outcome under a sequence value that was already used. The incarnation fence does
not help here: the retry is a *new* submission, and the fence only rejects
requests that explicitly carry a stale `observed_history_incarnation`. A client
that does not participate — including every pre-fence client, which ADR-0072
deliberately keeps working unvalidated (`:74-75`) — gets silent double
execution. `R-16` is rated Medium/Critical (SPEC.md:7647) and the mitigation
listed is "require an incarnation ADR before stronger claims"; ADR-0072 is that
ADR, but it explicitly declines to make the guarantee stronger for
non-participating clients. The risk register entry should be updated to say the
residual is permanent for non-participating clients, not merely "documented".

**Severity: medium** (bounded by the fact that restore is an explicit,
confirmed, offline administrative operation).

---

## F-10 — MEDIUM: the "escrow / optimistic deferred" plan for `R-02` is not credible against the frozen IR, and the measurements in ADR-0170 already refute its premise

**Evidence.**
- `R-02` (SPEC.md:7633): "Declared conflict keys are too coarse and cause
  hot-key serialization", High/High, mitigation "evaluate optimistic or escrow
  plans only after semantics are proven."
- SPEC §22.3 open research (SPEC.md:7783): "When can a pessimistic plan be
  safely replaced by optimistic validation or escrow without changing declared
  outcomes?"
- ADR-0170's measurement (`adr/0170-...md:50-64`): a coarse
  `conflict_key (tenant_id)` scaled **10.9×** at c=32 while fine-grained
  disjoint keys scaled **10.5×** — "The fine-grained conflict key bought no
  measured scalability." The same record notes a second refutation from the
  OpenFGA suite (`:62-64`).
- ADR-0094 (`adr/0094-...md:11-24`) and ADR-0095 (`adr/0095-...md:13-35`) both
  attribute the real bottleneck to *physical durable commit count*, not to
  conflict-key granularity, and solve it with grouping rather than optimism.

**Why it matters technically.** The escrow/optimistic direction is aimed at a
premise the project's own measurements have now twice refuted: coarse conflict
keys do not serialize writers, because group commit amortizes the flush across
concurrent writers regardless of lease contention. Meanwhile the IR is frozen
around pessimistic, statically derived, pre-acquisition keys — `ConflictKey`
bytes and evidence variants are declared semantic compatibility boundaries
(ADR-0003:86-88), the conflict set is derived before evaluation and cannot be
discovered at runtime (SPEC.md:3060-3062), and dynamic acquisition is forbidden
(SPEC.md:3245-3246). An escrow design would need to *release* a lease before
commit and re-validate, which is precisely the "release the aggregate lease
after evaluation" alternative ADR-0094 rejected as unsound
(`adr/0094-...md:98-99`). So the open research question is not merely deferred;
it is in tension with two accepted records, and the honest disposition is to
close it rather than carry it as a live plan.

**Severity: medium** (roadmap/risk-register accuracy; no runtime defect).

---

## Areas I examined and consider sound

- **Commit-time revalidation.** The ordering is right: dependency comparison
  *precedes* transaction-current normalization (SPEC.md:3371-3382), equal
  version with unequal content is `InternalDefect` rather than
  `DependencyChanged`, and no catalog lookup or second read is permitted inside
  the write transaction to explain drift. This is a genuinely careful design.
- **Sequence assignment ordering.** `TXN-041` (SPEC.md:3575-3579) forbids
  assigning even an invisible transaction-local sequence until validation,
  epoch reads, the sequence-free write plan, and capacity reservation all
  succeed. Combined with `TXN-042`'s atomicity requirement, this closes the
  "sequence assigned but graph not written" hole.
- **Idempotency identity and the single-terminal-row shape.** ADR-0005's
  2026-07-14 clarification (one full `StoredOutcomeV1` envelope that is
  simultaneously terminal idempotency state and persisted outcome, with atomic
  pending deletion and no tombstone) is a clean design, and the startup
  reciprocity checks (`:166-173`) are the right fail-closed posture.
- **Crash between reservation and commit.** Durable `Pending` with frozen
  plan/time/actor, resume that never substitutes the active plan, and
  `CommitStatusUnknown` fencing writes until same-key resolution
  (SPEC.md:3056-3059, 3562-3563) is correct.
- **`ExecutionFailed` as terminal admission.** ADR-0012's reasoning (a
  known-failed identity must not later mutate after state drift) is sound; my
  objection in F-03 is about retention and operator path, not about the
  terminality decision itself.
- **Conflict manager mechanics.** Sort/dedup before waiting, all-or-nothing
  multi-key grant, FIFO per key, no lock upgrade, and idempotent release on
  cancellation/panic are all present in
  `crates/riffdb-conflict/src/manager.rs` (`:530-531`, `:1844`, `:847-891`) and
  match SPEC §9.3.

---

## Summary table

| ID | Severity | Finding | Primary evidence |
|---|---|---|---|
| F-01 | **critical** | Aggregate invariants silently dropped when the first mutating binding belongs to another aggregate; reproduced | `command_lowering.rs:1416-1436,1576-1585`; `plan.rs:7175-7242`; ADR-0170:101-118 |
| F-02 | high | No compiler proof that a declared `conflict_key` protects the invariants it must; `ADB-C042` does not exist | SPEC.md:2744-2751; `diagnostic.rs:367-417`; `command_analysis.rs:1109-1160` |
| F-03 | high | `Pending`/`ExecutionFailed` have no bounded retention or operator path; `Integrity` burns the key permanently | ADR-0012:360-381; ADR-0005:204-205; ADR-0085:158 |
| F-04 | high | Retry storms on one identity are unbounded; attempt budget is deliberately non-durable | SPEC.md:3551-3571, 2650-2654, 7757 |
| F-05 | medium | ADR-0061 terminal admission contradicts SPEC §9.1 and ADR-0058; SPEC never reconciled | ADR-0061:85-125; SPEC.md:3041-3055, 8384-8389; ADR-0058:75-95 |
| F-06 | medium | ADR-0059's "exactly one mutation aggregate" superseded by ADR-0170 but not recorded as amended | ADR-0059:33-47; ADR-0170:101-118; SPEC.md:7722 |
| F-07 | medium | ADR-0170 ships with deadlock-freedom and crash-atomicity proofs explicitly outstanding | ADR-0170:120-125, 193-203; SPEC.md:53 |
| F-08 | medium | `R-05`'s "separately ordered outcome journal" mitigation is unavailable under `LOG-001` | SPEC.md:293, 2600-2609, 7636 |
| F-09 | medium | `R-16` narrowed but not closed; incarnation fence is opt-in and idempotency identity carries no incarnation | ADR-0072:26-78; ADR-0005:31-38; ADR-0050:316-343 |
| F-10 | medium | Escrow/optimistic plan for `R-02` is not credible against the frozen IR and is refuted by ADR-0170's own measurements | SPEC.md:7633, 7783; ADR-0170:50-64; ADR-0094:98-99 |

**Bottom line.** The transaction/concurrency/idempotency core is well designed
and the fail-closed discipline is real. F-01 is the one finding that should
block: it is a silent, order-dependent hole in the compiler's own
invariant-coverage proof, introduced by ADR-0170's relaxation and not
propagated to the proof that depended on the old single-aggregate premise. F-02
is the same class of gap at the contract-author boundary. F-03 and F-04 are
unbounded operational hazards with no accepted mitigation. F-05 through F-10
are governance and risk-register accuracy defects that make the accepted-record
set harder to reason about than it should be.
