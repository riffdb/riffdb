---
adr: 0183
title: Performance Package Freeze and the Durable-Group Lever
status: accepted
tier: guarantee
date: "2026-09-01"
accepted: "2026-09-01"
requires: [ADR-0059, ADR-0098, ADR-0101, ADR-0104, ADR-0123, ADR-0129, ADR-0142,
  ADR-0143, ADR-0146, ADR-0171]
amends:
  - PERF-008 only by adding the freeze and the durable-group activation arithmetic
  - SPEC 19.6 human review triggers by one entry
requirements: [PERF-020, PERF-021, PERF-022]
packages: [WP-762, WP-763]
# The record also blocks every later PERF-* package until the freeze lifts.
obligations:
  - id: OBL-0183-1
    package: WP-762
    proof: check-performance-freeze
    says: A work package that lists a PERF-* requirement while the freeze is in
      force fails repository checks unless it is WP-762, WP-763, or a package named
      by the accepted lifting record.
  - id: OBL-0183-2
    package: WP-762
    proof: assemble-wp674-unary-manifest --require-both-profiles
    says: The banked baseline carries interactive c32, write-only, and unary
      receipts for both N1 and E2, each recording the stability_rule_binds method
      field set to gated_backend, before the freeze is recorded as started.
  - id: OBL-0183-3
    package: WP-763
    proof: idle_singleton_seals_without_group_formation_wait
    says: An idle singleton command seals its epoch on the existing immediate path
      with no group-formation wait.
  - id: OBL-0183-4
    package: WP-763
    proof: group_formation_drains_only_prepared_prefix_within_ceilings
    says: Group formation consumes only already-admitted, already-prepared commands
      and stops at the 256-transition and 16 MiB ceilings.
  - id: OBL-0183-5
    package: WP-763
    proof: check-durable-group-activation --self-test
    says: The durable-group candidate activates only when both profiles satisfy the
      c32 throughput, c32 p95, and unary low-water gates in one receipt set; a miss
      on either profile removes the candidate.
  - id: OBL-0183-6
    package: WP-763
    proof: shutdown_evidence_reports_commands_per_durable_flush
    says: Graceful shutdown evidence reports commands per durable flush as a
      fixed-cardinality histogram.
review_triggers:
  - A performance package other than WP-762 or WP-763 would be registered or
    activated before WP-750 and WP-760 close, or the freeze would be lifted by
    anything other than an accepted ADR.
  - A banked baseline value would be moved upward, a failed generation retried or
    replaced, or a receipt banked without both inventoried profiles.
  - PERF-009 acknowledgement, fence, or durability semantics would change, or any
    fsync, fence, or acknowledgement would be elided or reordered.
  - Group formation would wait for work that is not already admitted and prepared,
    or any ceiling of DeferredCommandEpoch would widen.
---
# ADR-0183: Performance Package Freeze and the Durable-Group Lever

## Context

The write path is at the storage device's synchronous-flush floor. The
release-build decomposition in `docs/performance/write-path-decomposition.md`
attributes 1,213 µs of a 2,162 µs single-tuple write (56 percent) to fsync,
385 µs to writer CPU, 265 µs to coordinator queueing, and 232 µs to transport,
authentication, and the driver host. The interpreter is 118 µs. Under load the
flush census records 7.12 commands per durable flush, so group commit already
amortizes the floor; it is the only mechanism that moves it.

The latest banked-quality comparison, `docs/performance/wp-644-session-baseline.md`,
puts interactive c32 throughput at 0.88 times safe-application PostgreSQL on
both N1 and E2 against the 0.90 gate, and p95 at 1.38 times on N1 and 1.23
times on E2 against the 1.25 gate. Seed is inside the amended 5.0 ceiling on
both profiles. The remaining gap is a few percent inside the fsync floor.

Forty performance notes cover WP-620 through WP-674. Of the packages in that
range, WP-640, WP-644, WP-649, WP-650, WP-659, WP-660, WP-661, WP-662, WP-663,
WP-664, WP-665, and WP-670 closed without production or release activation;
WP-671, WP-672, and WP-673 closed without a candidate. WP-624 tested ADR-0098's
two-millisecond completion-edge window on the standard profile and rejected it:
commands per frame rose from 7.31 to 12.91 while public throughput fell 3.9
percent, because waiting adds latency before every contended dispatch.

Two governance facts bound what remains. ADR-0142 made unary PostgreSQL ratios
disclosure rather than gates. ADR-0171, accepted 2026-08-30, bound the
five-generation stability rule to the gated backend only, which removed the
comparator-spread blocker that had held `release/evidence/wp-674/` to failed
attempts since 2026-08-24. Since then `release/evidence/wp-674/n1/` carries an
accepted N1 unary profile (commit `f4f03bae`); E2 is not yet banked, and
WP-674 remains `unblocked_pending_qualification_run`. The gate-design repair is
therefore done. What is missing is a banked baseline on both profiles and a
decision about what performance work is still worth doing.

The review that produced this record (2026-09-01) ranked availability
(ADR-0178) and bounded dirty recovery (ADR-0182) above further performance
work, because a single node with offline backups and an unbounded dirty
restart changes what "fast enough" means more than another three percent
does. This record turns that ranking into a rule.

## Decision

### 1. Performance packages are frozen behind availability and recovery

No work package other than WP-762 and WP-763 may register or activate a
`PERF-*` requirement until WP-750 (ADR-0178 replication acceptance) and WP-760
(ADR-0182 bounded dirty recovery) both close. The freeze is enforced by
`scripts/check-performance-freeze`, which reads `work_packages.yaml`, the
freeze record it carries, and the closure status of WP-750 and WP-760, and
fails on any other package listing a `PERF-*` requirement without a `closure`
that predates the freeze. Registering a performance package during the freeze
is a SPEC 19.6 human review trigger.

The freeze does not stop measurement. The performance sentinel, the endurance
harness, host-validity preflight, and every existing acceptance command keep
running; a regression they detect is a defect, not a performance package.

### 2. The freeze starts from a banked baseline, not from failed attempts

WP-762 delivers one qualified baseline receipt set under ADR-0171's rule on
both inventoried profiles: interactive c32 and write-only concurrency sweeps
per `PERF-018`, and the complete unary matrix per ADR-0142 and ADR-0146. Each
receipt records `method.stability_rule_binds: gated_backend`, host validity,
correctness reconciliation, and the exact source, lock, harness, runner, and
daemon identities. The N1 unary column already retained under commit
`f4f03bae` is reused only if its identities match the banked revision;
otherwise it is re-run. The freeze is recorded as started in the same change
that lands the receipts. A freeze recorded without both profiles banked is
invalid.

### 3. One structural lever remains: durable-group formation from ready work

The coordinator already forms one private epoch per writer unit and seals it
behind one durable fence (ADR-0101, ADR-0104). The lever is to make every seal
carry the complete prefix of commands that are already admitted and already
prepared at that edge, up to the existing 256-transition and 16 MiB ceilings
of `DeferredCommandEpoch`, instead of the subset that happened to be staged by
the current writer unit. Preparation is the pay-once, admission-ordered
material ADR-0129 defines, produced per disjoint ADR-0059 conflict domain by
WP-640's mechanics so that more commands are ready at each edge.

Group formation never waits for work that is not yet ready. WP-624 measured a
two-millisecond wait and rejected it; that result stands. The only permitted
age bound is the existing `OLDEST_GROUPABLE_TRANSITION_MAX_AGE` of 200 µs,
which governs whether a transition may still join a forming group and does
not delay an idle singleton, which keeps the immediate one-phase path.

Every guarantee in `PERF-008` and ADR-0129 sections 3 through 6 is retained:
conflict ownership and final sequencing stay admission ordered, transaction-
current revalidation stays inside the sole writer transaction, nothing
observes the private frontier, and acknowledgement still means the covering
fence succeeded.

### 4. Activation arithmetic is fixed before the candidate is built

The candidate activates in production only when, in one receipt set on both
N1 and E2: interactive c32 throughput is at least 0.90 times same-run safe-
application PostgreSQL; c32 p95 is at most 1.25 times PostgreSQL; write-only
p95 does not regress more than five percent against the WP-762 bank; and no
unary p50 or p95 exceeds 1.10 times its frozen low-water baseline. A miss on
either profile removes the candidate before pool, driver-host, selector, or
release hardening, exactly as WP-670 removed its lane. A candidate that
passes one profile is not reported as partial success.

### 5. Evidence names the mechanism, not only the mean

Graceful shutdown evidence adds a fixed-cardinality histogram of commands per
durable flush and a counter of seals that reached a ceiling. Their buckets are
first-party constants independent of command count, tenant count, or database
size. WP-763's receipt retains group-size, frame-byte, physical-fence, and
ordered-fence-latency evidence so a tail change cannot hide behind throughput.

### 6. What this record refuses

The lever excludes: any change to `PERF-009` acknowledgement semantics; fsync
or fence elision; acknowledging before the covering durability boundary;
`O_DIRECT`, `io_uring`, `O_DSYNC`, or other device-level experiments;
interpreter, encoder, or allocator micro-optimization packages; and any new
private transport or session lane. Each of these is either unsafe or inside
the noise of the fsync floor.

### 7. The freeze lifts only by an accepted record

A later ADR lifts the freeze after WP-750 and WP-760 close. It carries the
WP-762 baseline as its starting receipt and names the packages it admits.
`check-performance-freeze` reads that record; no package, closure note, or
manifest edit lifts the freeze on its own.

## Options Considered

1. **Keep registering performance packages as they are found:** rejected. Since
   WP-620 that practice produced fifteen packages closed without activation or
   candidate and no banked baseline, while availability and bounded recovery
   stayed unbuilt.
2. **Freeze without banking a baseline:** rejected. The freeze would start from
   `adr0143-failed-attempts-v1.json`, and the lifting record would have no
   receipt to compare against. ADR-0171 removed the reason the bank failed.
3. **Reopen the completion-edge wait with a smaller budget:** rejected. WP-624
   showed the cost is latency before contended dispatch, not the size of the
   budget, and the interactive workload is closed-loop.
4. **Relax durability for the standard profile to reach the gate:** rejected
   without discussion; AGENTS.md boundary 11 and `PERF-009` forbid it.
5. **Durable-group formation from already-ready work, gated as above:**
   chosen. It is the only remaining mechanism that changes commands per flush
   without waiting, it reuses WP-640's mechanics, and its arithmetic is fixed
   before any code is written.

## Consequences

- Positive: the team's next two quarters of engine work go to ADR-0178 and
  ADR-0182, which change the product's adoptability; the comparator gate is
  banked rather than perpetually attempted.
- Positive: one candidate with fixed arithmetic replaces open-ended tuning.
- Cost: a real regression found during the freeze is fixed as a defect
  against the banked baseline, without a performance package to schedule it.
- Cost: the c32 gap may remain at 0.88 if the lever fails; the release then
  proceeds on ADR-0142's absolute service levels with the ratio disclosed.
- Deferred: seed parity, ADR-0127's transport remainder, and any synchronous
  replication cost analysis remain owned by their existing records.

## Compatibility

No public API, protocol, durable record, journal frame, storage key, contract
IR, RiffQL, or generated-client change. Group formation changes which
already-ordered commands share one durable frame; frame encoding, sequence
assignment, publication order, replay, and recovery are unchanged. Evidence
schemas gain one additive `riffdb.app-baseline-qualified-candidate/v1` field
for the freeze marker and one histogram in the shutdown census.

## Security

No trust boundary moves. Group formation observes only coordinator-private
prepared material; it never reads application values, and the shutdown
histogram carries counts only. `check-performance-freeze` is read-only over
repository files.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** no application surface
  changes. Group size, wait, durability, and activation are first-party
  constants and receipts; no caller can select a larger group, a wait, or a
  weaker fence.
- **Scale:** the lever assumes one sole writer and one fence lane per process,
  which is the existing single-node POC constraint. Because preparation is
  named per conflict domain (ADR-0129 section 5), the same formation rule
  applies unchanged to a future per-domain leader. Ceilings are constants, so
  memory does not grow with load.

## Testing

- `check-performance-freeze` self-test: a synthetic manifest with a frozen
  `PERF-*` package fails; the same manifest with WP-762 and WP-763 passes;
  closing WP-750 and WP-760 plus a lifting record passes.
- `assemble-wp674-unary-manifest --require-both-profiles` refuses an N1-only
  bank and a receipt whose `method.stability_rule_binds` is not
  `gated_backend`.
- `idle_singleton_seals_without_group_formation_wait` and
  `group_formation_drains_only_prepared_prefix_within_ceilings` run under the
  deterministic coordinator schedules in `riffdb-commit`, including the
  ADR-0129 frontier-equivalence assertion at every epoch boundary.
- `check-durable-group-activation --self-test` covers pass-both, miss-N1,
  miss-E2, unary-regression, and write-only-regression receipts.
- `shutdown_evidence_reports_commands_per_durable_flush` pins bucket
  cardinality and that the histogram is present on both durability profiles.
- Existing crash, recovery, replay, uncertainty, and storage-recovery matrices
  run unchanged against the candidate before any activation receipt.

## Requirements and Work Packages

- **Requirements:** `PERF-020` through `PERF-022`
- **Defines or blocks:** WP-762 through WP-763
- **Final evidence:** WP-763

## Decision Deadline

Exact acceptance is required before WP-762 records the freeze or banks a
baseline, and before any package registered after WP-745 lists a `PERF-*`
requirement. Implementation of WP-763 may not begin until acceptance, because
it changes group formation on the authoritative write path.

## Acceptance

Direction approved 2026-09-01; exact text accepted 2026-09-01. The maintainer
accepted the exact text of this record in the Claude Code session of
2026-09-01, all seven consolidation records together.
