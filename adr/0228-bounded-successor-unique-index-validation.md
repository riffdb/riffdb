---
adr: "0228"
title: Bounded Successor Unique Index Validation
status: accepted
tier: guarantee
date: 2026-09-16
accepted: 2026-09-16
acceptance: 'maintainer, in session, 2026-09-16: "Accept both ADRs as written" (ADR-0228 and ADR-0229)'
requires: [ADR-0076, ADR-0078]
amends: []
supersedes: []
requirements: [MIG-013]
packages: []
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations: []
review_triggers: []
---
# ADR-0228: Bounded Successor Unique Index Validation

## Context

Successor validation scans the entire private stage for every row participating
in a newly added unique constraint. This repeats row decoding, invariants and
unique-prefix derivation quadratically. Existing constraints alone do not take
this path. Large migrations that add uniqueness can therefore spend excessive
time with the selected database drained. This is a high-severity availability
repair within D-001, not a new migration feature.

The current semantic validator independently derives facts from every row.
Replacing that proof with an unchecked index lookup would weaken MIG-013.
This record specifies the reciprocal proof needed to change read validation;
AGENTS.md requires human review for that change. It is proposed, not accepted.

## Decision

1. Final successor validation retains its complete, strictly ordered row scan,
   schema/key/invariant checks, relationship checks, exact row count, catalog
   history proof and full stage structural validation. No work is skipped merely
   because a stage reports that an index exists or a prior batch succeeded.
2. After all successor index rebuilding is complete, the storage stage may
   expose a closed, read-only unique-prefix observation. Its identity binds the
   exact stage, frozen frontier, migration artifact, successor bundle, entity,
   index and complete canonical unique prefix. The caller cannot select a
   different database, historical snapshot, index schema or partial prefix.
3. One observation returns at most two matching index entries from the same
   immutable private stage. Storage checks canonical physical key/value identity,
   exact entity/index prefix, complete schema binding, covered values and owner
   identity before returning an entry. Unknown, missing or inconsistent evidence
   is a typed integrity failure. Two owners prove a unique conflict; the reader
   need not scan the rest of that prefix.
4. For every independently derived row fact, the catalog requires exactly one
   index owner and requires that owner to equal the row's complete EntityTarget.
   This is checked for every staged row, including unchanged rows. Missing an
   index entry cannot conceal a duplicate: the omitted row's reciprocal check
   fails. Duplicate observations, wrong owners and prefix/schema substitutions
   cannot yield a successful validation proof.
5. The observation is usable only while the validated private stage is frozen.
   Any stage mutation invalidates its structural witness and all observations.
   No independently mutable cache, process-global proof or persisted flag can
   stand in for the final scan. Preflight before index rebuilding retains its
   independent validation path.
6. The existing sealed ValidatedMigrationStage remains the only catalog proof
   consumed by cutover. Its digest framing, checked-row meaning, retained-history
   checks and publication conditions remain unchanged. A supported adapter that
   cannot produce the bounded observation uses the existing complete scan; it
   cannot report unsupported as absence or uniqueness success.
7. This introduces no durable table, key, envelope, application API, transform,
   backup policy or transaction-order change. Crash/restart discards observations
   and reruns final validation against the exact recovered stage. Existing
   complete validation after publication and before readiness remains required.

## Options and consequences

The existing scan is independent but costs O(N squared) row visits for a newly
added unique constraint. An unbounded in-memory set violates collection bounds.
A bounded external sort would work but adds scratch storage and recovery
ownership. Reciprocal bounded index reads reuse indexes that migration already
builds, costing one ordered row scan plus O(U log I) index seeks for U derived
unique facts and I index entries. Only a bounded row page and two index entries
need be retained. Performance claims must count actual stage/index reads.

## Standing design tests

- **Interface safety:** application callers cannot supply uniqueness proofs,
  skip validation or elect to trust an index. Only the existing migration
  coordinator can publish a completely validated private stage.
- **Scale:** row pages retain MAX_MIGRATION_SCAN_ROWS; prefix observations retain
  at most two bounded entries. No collection grows with the database size.
- **Recovery:** no cached proof survives a stage change or process restart;
  cutover and its atomic evidence remain unchanged.

## Checks

- Compare successful proofs and duplicate rejection with the existing full-scan
  oracle across multiple entities, indexes, nullable values and page boundaries.
- Omit either duplicate's index row, substitute owner/schema/prefix/covered data,
  introduce malformed cold bytes, or mutate the stage after structural validation:
  all must refuse cutover. Legitimately empty stages remain valid.
- Count row visits and index observations for 1,000 and 10,000 distinct rows;
  final semantic validation must scan each row once rather than once per fact.
- Exercise restart after transformation, after structural validation and before
  cutover; revalidation must detect injected duplicates and missing index rows.
- Retain existing migration crash, provenance, idempotency, immutable-history and
  post-publication validation suites. Update migration operations documentation
  when the accepted implementation lands.
