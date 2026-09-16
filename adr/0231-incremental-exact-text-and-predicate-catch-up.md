---
adr: "0231"
title: Incremental Exact Text And Predicate Catch Up
status: accepted
tier: guarantee
date: 2026-09-16
accepted: 2026-09-16
acceptance: 'maintainer, in session, 2026-09-16: "Accept all four as written" (ADR-0227, ADR-0230, ADR-0231, ADR-0232)'
requires: [ADR-0131, ADR-0134, ADR-0207]
amends: []
supersedes: []
requirements: [PRJ-001, PRJ-002, PRJ-004]
packages: []
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations: []
review_triggers: []
---
# ADR-0231: Incremental Exact Text And Predicate Catch Up

## Context

Binary exact-text and exact-predicate slots currently reread an entire partition
and write a replacement checkpoint whenever the application head changes. The
slot is unavailable during construction. Under sustained writes, full rebuild
cost can dominate serving time. Retaining the prior Ready value alone does not
fix this: query execution independently requires the selected provider frontier
to equal the captured authoritative head. This high-severity availability repair
is within D-001; it does not add stale-query functionality.

Incremental application needs a complete source-prefix proof and correct row
replacement/removal, not a guessed frontier advance. Changing that read/recovery
validation requires review under AGENTS.md. This record is proposed, not accepted;
it does not authorize serving a predecessor as though it reflected later writes.

## Decision

1. A same-specification slot keeps its last completely validated published
   provider while one private successor is prepared. Published means the exact
   frontier in that provider, never the current head by inference. Query epoch,
   policy, schema, generation and continuation validation remain unchanged.
   Latest execution still requires exact equality with its captured head.
2. A healthy same-specification provider at F may prepare a successor from an
   immutable validated V3 source prefix covering every transition after F through
   a captured H. Bind source/database identity, history incarnation, specification,
   generation and exact V3 chain position together. A sequence gap, missing proof,
   identity substitution or unsupported transition cannot be treated as an
   irrelevant commit or empty tail.
3. Derive candidate changes from complete checked entity post-images/deletions
   in that prefix, using the exact writer schema and existing catalog/lineage
   materialization rules. Do not point-read a newer current entity as the image
   of an earlier commit. Apply every relevant replacement/removal in source order;
   remove old search/filter/order entries and install their complete successors.
   Commits with no relevant changes still advance the validated logical prefix.
4. Build against a private clone or persistent fork; no published provider is
   mutated. Each worker pass processes at most 64 application commits and one
   existing bounded source byte page. Existing row/value/posting/state ceilings
   remain authoritative. A slot owns at most one published and one candidate
   provider; captured query views remain under the existing bounded lifetime and
   retention rules. No unbounded tail or pending-generation queue is introduced.
5. Persist the successor with the existing complete checkpoint encoding, checksum,
   sync and reopen validation. Install it only if the expected slot specification,
   source identity and prior selection still match. The generation/frontier pair
   must advance consistently; concurrent replacement, retirement or incarnation
   change invalidates the candidate. No partial multi-index state is visible.
6. Initial population, incompatible specification changes and unavailable source
   history use the existing full rebuild path with its existing typed unavailable
   behavior. An invalid selected provider remains failed closed. A healthy prior
   provider is retained only during preparation; this is not corruption fallback
   and does not let queries above its frontier succeed.
7. Retention pins the exact source tail required by the selected frontier until
   a complete successor is durable and selected. Process restart trusts only a
   fully validated checkpoint and its exact source binding, then validates the
   remaining prefix again. No persisted progress flag may claim changes not
   present in the checkpoint.
8. Scope is the affected binary-text and exact-predicate slots. Tokenized/long-
   pattern providers, query IR, arbitrary substring semantics, public freshness
   classes, authorization rules and cursor formats remain unchanged. The existing
   checkpoint format must represent the full resulting provider state without
   weakening validation; any incompatible format need requires separate review.

## Options and consequences

Double buffering alone leaves fresh queries unavailable when head equality fails.
Serving stale state while reporting the current head is rejected. Rebuilding every
partition on every commit remains a correctness reference but is expensive.
Applying validated bounded deltas makes steady-state work proportional to changed
rows, with full rebuild retained for bootstrap and source-history loss. If writes
outpace catch-up, existing typed freshness refusals remain correct; this proposal
does not promise arbitrary-write-rate availability or pause authoritative writers.

## Standing design tests

- **Interface safety:** callers cannot choose an unchecked provider, ignore lag,
  alter the reported frontier or bypass current policy and continuation checks.
- **Scale:** one private successor, fixed transition/page bounds and existing
  provider-state ceilings; processing never retains an entire source backlog.
- **Recovery:** publish only a complete validated checkpoint bound to a proven
  source prefix; retention and restart use its actual frontier.

## Checks

- Compare every incremental result with a full rebuild at the same frontier:
  substring matches, predicate membership, exact counts, ordering and windows.
- Exercise replacements, removals, partition moves, nulls, duplicate text, changes
  to filter/order fields, unrelated commits and compatible catalog transitions.
- Advance the head during preparation; a completed H may be retained as H but
  must not satisfy Latest at H+1. Queries at a valid matched epoch remain exact.
- Reject malformed/missing/reordered source members, wrong schema/incarnation,
  stale candidate control and corrupted checkpoints without partial publication.
- Crash before checkpoint sync, after sync, before selection and after selection;
  reopen the exact complete selected state and replay without duplicate postings.
- Count full-partition reads under a steady update workload: after initial build,
  fitting same-specification updates must use bounded source deltas. Measure
  availability and lag without weakening the Latest condition.
- Retain existing policy-isolation, epoch, count/window, checkpoint and migration
  tests; update exact-result operations documentation after accepted implementation.
