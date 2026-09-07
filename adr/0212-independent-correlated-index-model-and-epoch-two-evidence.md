---
adr: "0212"
title: Independent Correlated-Index Model and Epoch-Two Evidence
status: proposed
tier: guarantee
date: 2026-09-07
accepted: null
requires: [ADR-0003, ADR-0004, ADR-0113, ADR-0124, ADR-0149, ADR-0180, ADR-0204]
amends:
  - ADR-0149 Decisions 4, 5, and 7, its compatibility, testing, consequences, and WP-684 description only as stated here
  - SPEC BLK-021, BLK-025, and BLK-027 with the exact replacement text in Decision 8 after acceptance
supersedes: []
requirements: [BLK-021, BLK-024, BLK-025, BLK-027]
packages: [WP-684]
obligations:
  - id: OBL-0212-1
    package: WP-684
    proof: correlated_index_work_matches_independent_authoritative_model
    says: Every admitted representative and exact-bound correlated-index command has the same canonical targets and post-state in the independent model, the coordinator's sealed set, and inspected redb state.
  - id: OBL-0212-2
    package: WP-684
    proof: correlated_index_work_model_rejects_target_divergence
    says: The comparison independently rejects every missing, extra, duplicate, reordered, or wrong target and every wrong modeled redb post-state without accepting a shared production defect.
  - id: OBL-0212-3
    package: WP-684
    proof: correlated_index_work_redb_crash_reopen_is_complete_or_absent
    says: Real redb transaction, sequence, atomicity, crash, reopen, and durable inspection prove each correlated-index command complete or absent at every required boundary.
  - id: OBL-0212-4
    package: WP-684
    proof: correlated_index_work_remote_redb_loopback_matches_model
    says: A real riffdbd process using redb executes the neutral remote corpus and its bounded durable result equals the independent model.
  - id: OBL-0212-5
    package: WP-684
    proof: scripts/correlated-index-work-qualification
    says: One qualification gate proves the exact corpus matrix, bounded canonical inspection pages, epoch-two identity posture, model independence, durable evidence, and stage-attributed cost bounds.
review_triggers:
  - The oracle would consume a coordinator target/result, production layout or codec, or another production-derived expected post-state.
  - A memory backend, fake transaction, in-process service, or fixture-only result would stand in for required redb, crash/reopen, or real riffdbd evidence.
  - Any 1, 9, 19, 100, D, A, V, W, semantic-byte, inspection-page, or divergence-negative case would be omitted or its bound raised.
  - Changelog, audit, durability, recovery, remote, or performance evidence would be inferred from model equality instead of observed on its required production path.
  - Epoch-two could write or read a retired capsule or segment identity, change V6/V5 bytes or semantics, or make a durable identity caller-selectable.
  - WP-684 would retain the removed memory-backend path or omit the completed WP-756 dependency, testkit-server harness path, or required governance edits in Decision 9.
---
# ADR-0212: Independent Correlated-Index Model and Epoch-Two Evidence

## Context

ADR-0149 requires memory/redb parity for WP-684. ADR-0180 and completed WP-756 removed
`riffdb-storage-memory`: redb is the sole production backend, while the independent
`riffdb-testkit` authoritative model and durable inspector now provide the stronger semantic
oracle. Recreating memory would reverse the accepted dependency repair; testing redb only against
its own coordinator output would let a shared target-derivation defect false-green.

ADR-0149 also selected additive V6/V5 successors while ADR-0204's accepted epoch-two reset retires
every earlier command-capsule and segment identity. The historical least-sufficient proof remains
valid before epoch two, but the epoch-two writer and reader topology must have one current retained
identity without reinterpreting any durable bytes.

## Decision

1. This record amends only ADR-0149 Decision 4's post-epoch identity selection, Decision 5's
   memory/redb comparison, Decision 7's corpus, and their corresponding compatibility,
   consequences, testing, and WP-684 language. ADR-0149's `D`, `A`, `V`, `W`, byte, pay-once,
   transaction, diagnostic, and performance semantics remain exact. ADR-0180 and WP-756 remain
   exact: `riffdb-storage-redb` is the sole production backend and
   `riffdb-storage-memory` is not restored.

2. `riffdb-testkit` owns an independent authoritative correlated-index model. From only the neutral
   corpus declaration, declared indexes and bounds, initial semantic state, and submitted ordered
   input, it independently derives expected candidate deltas, the complete canonical affected-
   prefix target sequence, validation and generation observations, refusal or outcome, and final
   semantic state. It does not consume a coordinator-produced target, sealed set, result, production
   storage layout, durable envelope, production encoder/decoder, or production derivation helper.
   Shared stable semantic newtypes are allowed; shared derivation or normalization logic is not.

3. The corpus contains admitted 1, 9, 19, and 100 element commands and independent exact/plus-one
   witnesses for `D` 4,096/4,097, `A` 65,535/65,536, `V` 65,535/65,536, `W` 65,535/65,536, and the
   16 MiB affected-target/current-observation semantic-byte ceiling. For each admitted witness, the
   model's canonical sequence equals the coordinator's sealed sequence and the model post-state
   equals redb semantic inspection. For each plus-one witness, the model predicts refusal, the
   coordinator refuses before effects, and redb inspection proves no sequence, target advance, row,
   outcome, event, provenance, audit, changelog, or durable command appears.

4. Comparison is bidirectional and order-sensitive. Finite negatives independently inject and
   reject every class: one missing target, one extra target, one duplicate, one adjacent reorder,
   one wrong target identity or prefix byte, and one wrong generation or post-state value. A shared
   count, digest, set conversion, sorting repair, or production codec is not equality. Target and
   inspection inventories use strictly ordered canonical pages of at most
   `MAX_INSPECTION_TARGETS = 256`; continuation proves strict cross-page order, no duplicate or gap,
   exact terminal exhaustion, checked aggregate count, and checked semantic bytes. The constant is
   not raised and no page or diagnostic retains an unbounded population.

5. The model is expected-state evidence, not storage authority. Real redb remains responsible for
   transaction-current validation, capacity reservation, sequence ownership, atomic commit,
   rollback, process crash/reopen, recovery, persistent bytes, and durable inspection. Required
   changelog order and bytes, audit lifecycle/linkage, durability and recovery, backup/manifest
   compatibility, and stage-cost/performance evidence are observed on their existing production
   paths; model equality never substitutes for any of them. ADR-0113 simulation remains additional
   redb fault-schedule evidence and does not replace the process-level crash/reopen cases.

6. Remote loopback starts a real `riffdbd` process configured with a real redb database, invokes the
   ordinary generated neutral command surface, shuts down or crashes at the named boundaries,
   reopens as required, and compares bounded inspected state with the independent model. An
   in-process service, storage fake, hand-built coordinator intent, imported durable bytes, or
   identity-only fixture is not remote evidence. The daemon and caller gain no backend, target,
   budget, transaction, split, or durable-identity selector.

7. Before epoch two, ADR-0149's least-sufficient selection remains exact: commands with at most
   4,096 transitions use the then-current sufficient capsule/segment identity, while 4,097 through
   65,535 use `StoredCommandCapsuleV6` and `StoredCommandSegmentV5`; each older identity keeps its
   frozen bound and decoder. At epoch two, ADR-0204 applies: only capsule V6 and segment V5 are
   readable, writable, and current for every 0..=65,535 transition count. Retired numeric and
   symbolic identities and hashes remain reserved and refused before dispatch. V6/V5 fields,
   ordering, payload bytes, hashes, structural/semantic validation, and meanings do not change;
   only the epoch-two selection/topology narrows. Unknown readers still refuse before mutation.

8. After exact acceptance, the separate governance commit makes these exact SPEC replacements:

   - `BLK-021`: "A framework-neutral bounded-context corpus MUST prove one through 100 elements,
     individual and aggregate exact-bound/plus-one cases, nine- and nineteen-element atomic sets,
     business failure, authorization, cancellation, idempotency, concurrency, crash recovery,
     provenance, events, and complete-or-absent visibility against an independent riffdb-testkit
     authoritative model and the sole production redb backend across every generated surface. The
     model MUST derive expected targets and post-state without coordinator output, production layout,
     or production codecs; required durable and remote evidence MUST execute against real redb."
   - `BLK-025`: "Before epoch two, more than 4,096 durable index-generation transitions MUST use
     least-sufficient successor command-capsule and segment identities with a 65,535-entry maximum;
     every older identity retains its exact bound and byte-exact decoder. At epoch two, only
     StoredCommandCapsuleV6 and StoredCommandSegmentV5 are readable, writable, and current for
     0..=65,535 transitions; retired identities remain reserved and refused. Existing bytes,
     semantics, and semantic/envelope byte ceilings remain unchanged, and unsupported readers MUST
     refuse before mutation."
   - `BLK-027`: "A framework-neutral index-rich corpus MUST prove 1, 9, 19, and 100 element atomic
     commands; exact and plus-one `D`, `A`, `V`, `W`, and byte boundaries; epoch-appropriate durable
     selection; deterministic schedules; idempotency; rollback; crash recovery; provenance; events;
     audit and changelog order; real riffdbd-plus-redb remote loopback; and bounded stage-cost
     evidence. Every admitted target/post-state case MUST compare the coordinator's sealed sequence,
     an independently derived authoritative model, and redb inspection through canonical pages of
     at most 256 targets, with missing, extra, duplicate, reorder, wrong-target, and wrong-state
     negatives. The model MUST NOT substitute for changelog, audit, durability, recovery, remote, or
     performance evidence."

9. That governance commit also changes WP-684 only: set `tier: guarantee`; add completed WP-756 to
   `depends_on`; add ADR-0180, ADR-0204, and ADR-0212 to `required_adrs`; remove
   `crates/riffdb-storage-memory/**` and add `crates/riffdb-testkit-server/**` in `allowed_paths`;
   replace every memory/redb deliverable and exit-gate claim with the model/sealed-set/redb proof in
   Decisions 2 through 6; remove the storage-memory crate from acceptance; add
   `riffdb-testkit-server` to the focused test command; and add
   `./scripts/correlated-index-work-qualification`. The same commit applies the three SPEC
   replacements and no implementation. Implementation and closure commits are guarantee tier.

10. No production data behavior changes. The coordinator still derives and seals once; redb still
    commits the identical bounded graph; V6/V5 still encode the identical messages. This decision
    changes the independent evidence source and reconciles epoch-specific identity selection. It
    adds no public field, method, schema, transport, configuration, backend, model, inspection,
    target, budget, fallback, or transaction control.

## Options considered

1. **Restore memory parity:** rejected because ADR-0180 deliberately removed a non-production
   backend and replaced its oracle role with the independent model.
2. **Use coordinator output as expected state:** rejected because target omission, sorting, and
   codec defects could agree with themselves.
3. **Use only model or simulated redb:** rejected because neither proves production transaction,
   persistence, process recovery, or remote composition.
4. **Keep least-sufficient old writers in epoch two:** rejected because it violates ADR-0204's
   closed retirement and one-current topology; narrowing to unchanged V6/V5 bytes is chosen.

## Consequences

- WP-684 gains a stronger three-way semantic proof without resurrecting a second backend.
- Exact-bound inspection requires bounded pagination and exhaustive divergence negatives.
- Epoch-two small commands use V6/V5 even when a historical pre-epoch writer used an older identity;
  this changes identity selection only at the accepted reset, not payload meaning.
- SPEC and WP governance remain unchanged until this proposal is accepted exactly.

## Standing design tests

- **Interface safety:** application callers retain one generated compiled command and cannot select
  the oracle, backend, targets, pages, budgets, transaction, split, fallback, or durable identity.
- **Scale:** `D <= 4,096`; `A`, `V`, and `W <= 65,535`; semantic/envelope ceilings remain 16 MiB;
  derivation is once per attempt; comparison and redb inspection are linear, page-bounded to 256,
  checked-arithmetic operations with no database/history scan.

## Checks

- `correlated_index_work_matches_independent_authoritative_model` covers the exact positive matrix.
- `correlated_index_work_model_rejects_target_divergence` covers every finite divergence class.
- `correlated_index_work_redb_crash_reopen_is_complete_or_absent` proves real durable boundaries.
- `correlated_index_work_remote_redb_loopback_matches_model` proves real process composition.
- `scripts/correlated-index-work-qualification` composes those proofs with manifest/topology,
  changelog/audit, requirement coverage, model-independence architecture, and stage-cost checks.
