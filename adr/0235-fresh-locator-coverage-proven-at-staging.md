---
adr: "0235"
title: Fresh Locator Coverage Proven At Staging
status: proposed
tier: guarantee
date: 2026-09-17
accepted: null
acceptance: null
requires: [ADR-0183, ADR-0197, ADR-0234]
amends:
  - ADR-0183 only to admit the single enumerated repair below.
  - ADR-0197 Decision 6 only to widen what a queued command witness may be
    constructed from, without changing what it must establish.
supersedes: []
requirements: []
packages: []
obligations:
  - id: OBL-0235-1
    package: null
    proof: staged_coverage_evidence_is_minted_only_by_the_locator_writer
    says: Staged coverage evidence is constructible only inside the walk that
      writes a segment's locator rows, is fixed size, and carries no key,
      locator, segment or history collection. It cannot be cloned, copied,
      defaulted, serialized, reused across segments, or built by any other
      caller.
  - id: OBL-0235-2
    package: null
    proof: staged_coverage_rejects_every_case_the_rederived_witness_rejected
    says: Every malformed span, count, ordinal, manifest entry, locator,
      capsule identity and audit pairing that disabled coverage under the
      re-derived witness still disables coverage, and none reaches publication.
  - id: OBL-0235-3
    package: null
    proof: grouped_fresh_locator_seal_is_constant_work_per_segment
    says: Sealing an n-command group performs one durable segment resolution
      and work proportional to the number of segments, not to n, while the
      per-command facts remain established exactly once by the staging walk.
review_triggers:
  - Coverage would advance on evidence the staging walk did not mint, or a
    mismatch would do anything other than disable coverage.
  - The per-command facts ADR-0197 Decision 6 enumerates would stop being
    established for any command.
  - Proof would move after the journal submit, or publication would consume a
    witness whose evidence was incomplete when the frame was encoded.
  - A durable byte, key, encoding, transaction ordering, acknowledgement or
    public surface would change.
---
# ADR-0235: Fresh Locator Coverage Proven At Staging

## Context

A bisect on the E2 bench host attributes a 2026-09-05 write-path regression to
`89f2e78bcf`, which moved ADR-0197's queued coverage witness inside the journal
runtime guard and ahead of `lane.submit`. That commit added no work; it moved
existing work onto the critical path, so the proof no longer overlaps the
journal I/O it precedes. An ablation that skips the witness entirely measures
its cost at 29 to 33 percent of mean group-commit time at 1, 8 and 32 clients.

Resolving the witness's locator reads from the staged composite mutations,
which ADR-0197 Decision 6 already names as an allowed input, recovers about a
fifth of that. The remainder is re-derivation: for every command the seal
recomputes an identity storage key, looks up a manifest entry, decodes a
locator and compares a capsule identity, all of which the staging walk that
wrote those very rows computed moments earlier. Decision 6 fixes what the
witness may be constructed from, so relocating that establishment is a decision
change rather than an implementation choice, and ADR-0183 freezes performance
work, so admitting the repair is a second decision change.

## Decision

1. Admit this one repair as a narrow amendment to ADR-0183, on the ADR-0234
   pattern. No sealed package, banked baseline, threshold or permitted closure
   changes. Register the implementing non-PERF package after acceptance, with
   exact allowed paths and the proofs below, before implementation begins.
2. Amend ADR-0197 Decision 6 so a queued command witness is constructible from
   the canonical sealed command segment, the same ADR-0104 composite mutation
   stage, and fixed-size staged coverage evidence minted by the walk that wrote
   that segment's locator rows. What the witness must establish is unchanged;
   only where each fact is established moves.
3. Staged coverage evidence is minted only inside the locator-writing walk,
   which already visits every command and already holds its sequence, identity
   and encoded locator. As it writes each row it establishes, for that command,
   the exact facts Decision 6 enumerates: the canonical identity-key locator
   decoding to its own sequence, the capsule agreement on sequence, database,
   environment, tenant scope, principal, contract lineage, command id and keyed
   caller-key digest, the single matching manifest entry at that ordinal, and
   the started and terminal audit pairing.
4. The evidence is fixed size. It records the segment's first and last commit
   sequence, its command count, its first and last administration sequence, and
   nothing else. It stores no key, locator, segment or history collection, so
   ADR-0197 Decision 3 is preserved. It implements no `Clone`, `Copy`,
   `Default`, serialization, public constructor or cross-segment conversion,
   and a dropped, duplicated or mismatched instance disables coverage exactly
   as a dropped witness does today.
5. At the seal the queued witness consumes the evidence for each staged segment
   and establishes only what spans segments: the database binding, the
   arithmetic `first = successor(predecessor)` and `count = last - first + 1`,
   agreement between segment span, retained count and stage count, and
   contiguity across segments. That is work proportional to the number of
   segments, not to the number of commands.
6. Failure semantics are unchanged. Any mismatch disables coverage, and every
   existing disable-and-fence path keeps its current behaviour and placement.
7. The ordering `89f2e78bcf` established is retained. The evidence is complete
   before the frame is encoded, so all fallible proof work still precedes
   `lane.submit` and no submitted frame can escape through an unfenced proof
   error. This decision buys that property back at a cost near zero rather than
   trading it away.
8. The direct-path witness is out of scope and unchanged.
9. No durable byte, storage key, encoding, transaction ordering, conflict
   ownership, acknowledgement, outcome sequencing or public surface changes.

## Options considered

1. **Remove only the redundant reads.** Measured at roughly 5 percent of mean
   group-commit time, needs no record change, and is available immediately. It
   leaves four fifths of the witness cost in place, so it is a complement to
   this decision rather than an alternative.
2. **Move the proof after the journal submit.** Recovers most of the cost,
   because a proof failure disables coverage rather than invalidating the
   frame, and the earlier code constructed the witness there. Rejected: it
   reverses a deliberate guarantee-tier safety decision to buy what Decision 7
   obtains for free.
3. **Prove a sample of seals.** Rejected. Coverage answers absence, and a
   sampled proof would make absence answerable from an unproven prefix.
4. **Retire fresh-locator coverage.** Coverage exists to avoid bounded history
   scans, and it is not established that the scans it avoids cost more than the
   proof that maintains them. Out of scope here and worth measuring separately.

## Consequences

- Per-command establishment happens exactly once, in the walk that writes the
  rows, instead of once there and once again at the seal.
- Trust moves from the seal to the staging walk. The walk becomes the single
  place segment exactness is established, and the seal becomes a chain check.
- The evidence is stricter in one respect: it cannot be satisfied by a
  pre-existing row of the right shape, which the current merged-view read can.
- No speedup is claimed until measured. The measured facts today are the
  witness's 29 to 33 percent share and the 5 percent recovered by Option 1.
- The remaining performance program stays frozen.

## Standing design tests

- **Interface safety:** no public, operator, agent, SDK, transport or
  configuration surface changes. Nothing can arm, observe, extend, weaken or
  bypass coverage, or select where a fact is established.
- **Scale:** the evidence is fixed size per segment and retains nothing after
  the seal consumes it. Seal work is proportional to segments, per-command work
  is proportional to commands and happens once, and no history-sized or
  key-sized collection is introduced.

## Checks

- `staged_coverage_evidence_is_minted_only_by_the_locator_writer`
- `staged_coverage_rejects_every_case_the_rederived_witness_rejected`
- `grouped_fresh_locator_seal_is_constant_work_per_segment`
- The existing `cold_fresh_database_publications_complete_without_history_scans`
  and `grouped_fresh_locator_seal_resolves_the_durable_segment_once` proofs, and
  every existing `fresh_locator_*` unit test, unchanged.
- `./scripts/check-adr-obligations` and `./scripts/check-performance-freeze`.
