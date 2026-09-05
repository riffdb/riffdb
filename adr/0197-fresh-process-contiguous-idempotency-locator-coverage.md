---
adr: 0197
title: Fresh-Process Contiguous Idempotency Locator Coverage
status: proposed
tier: guarantee
date: 2026-09-05
accepted: null
requires: [ADR-0004, ADR-0006, ADR-0058, ADR-0101, ADR-0104, ADR-0156, ADR-0157, ADR-0165]
amends: [ADR-0165]
supersedes: []
requirements: [OUT-001, OUT-002, TXN-042, REC-004, PERF-019]
packages: [WP-778]
obligations:
  - id: OBL-0197-1
    package: WP-778
    proof: fresh_locator_coverage_arms_only_from_an_empty_authoritative_frontier
    says: Process-local locator coverage arms only from an exact command-empty activation and never from a checkpoint, nonempty history, or restart.
  - id: OBL-0197-2
    package: WP-778
    proof: fresh_locator_coverage_advances_only_with_contiguous_published_locators
    says: Each advance is contiguous and is backed by every exact idempotency locator in the same atomically published command successor.
  - id: OBL-0197-3
    package: WP-778
    proof: fresh_locator_coverage_fails_closed_across_gaps_uncertainty_and_restart
    says: Gaps, unsupported lanes, uncertainty, publication failure, invalidation, process death, and nonempty reopen disable the proof without changing authority.
  - id: OBL-0197-4
    package: WP-778
    proof: cold_fresh_database_publications_complete_without_history_scans
    says: Bounded concurrent fresh-database command publication completes with dormant transient indexes and no history fallback scan.
review_triggers:
  - Coverage would arm from a validated-prefix checkpoint, locator-table cardinality, a nonempty application frontier, retained history, or evidence from another process generation.
  - A missing exact locator could mean absence without coverage of the read's captured frontier, or a malformed or mismatched locator could avoid CorruptData.
  - Transaction ordering, conflict ownership, transaction-adjacent idempotency revalidation, command batching, acknowledgement, or outcome sequencing would change.
  - Coverage would advance before public successor publication, survive an unsupported lane or uncertainty, retain command-derived population state, or become durable.
  - Public audit or SubscribeCommits first-demand reconstruction, any public or operator surface, or any durable, journal, Protobuf, storage-key, or registry byte would change.
---
# ADR-0197: Fresh-Process Contiguous Idempotency Locator Coverage

## Context

On a clean bounded start, ADR-0165 keeps command-derived population indexes
dormant and resolves exact committed identities through durable locator rows.
For a genuinely new idempotency identity, however, the current admission read
falls back to scanning every preceding command segment. The direct inspection
batcher and the coordinator's transaction-adjacent revalidation both perform
that read. At 213 committed rows of a fixed 65,536-row qualification this
became repeated CPU-bound history work, not a lock wait, and command progress
stopped within the external diagnostic bound.

ADR-0165 does not authorize an absence inference from a validated-prefix
checkpoint. Its additive locator tables are empty for pre-ADR histories and
old clean certificates remain valid, so neither checkpoint frontier nor table
cardinality proves that every historical command owns a locator. The narrower
case in which activation proves there has never been an application command
has an exact inductive proof. Adopting that proof changes admission read
validation and therefore cannot be selected by an implementation package.

## Decision

1. Only a bounded-start operational activation that still owns the exclusive
   pre-writer boundary and derives an exact application frontier of `None` from
   one immutable authoritative view may arm fresh locator coverage. The
   derivation includes the existing allocator and command-authority-presence
   checks. Any numeric or exhausted frontier, retained watermark, read failure,
   contradiction, or already-active writer leaves coverage disabled.

2. Coverage is one fields-private process-local state:
   `Disabled | ContiguousThrough(Option<CommitSequence>)`. It starts as
   `ContiguousThrough(None)` only under Decision 1, retains no locator, key,
   segment, command, or population collection, and is never serialized,
   checkpointed, restored, copied, or carried across process generations.

3. A current-process command group may extend coverage only after a bounded
   proof over its sealed composite successor establishes all of the following:
   its first sequence is the checked successor of the covered frontier; its
   sequence delta equals its independently retained command count; and every
   command has one exact `idempotency_locators` row in that same successor
   whose decoded locator and command identity name the command's sequence.
   The existing grouped-write ceiling bounds this proof at 256 commands.

4. The composite successor becomes public before coverage advances. The
   advance occurs while the existing transient-index publication lock still
   excludes readers. A predecessor snapshot remains a safe prefix; a reader of
   the successor cannot observe permissive coverage until the complete
   locator-bearing successor is visible. No transaction, journal, durability,
   conflict, acknowledgement, or changelog ordering changes.

5. Admission still performs the exact locator point read first and validates a
   found locator against command authority. Only when that point read is absent
   may fresh coverage establish absence, and only when its contiguous frontier
   covers the read access's captured application frontier. The coordinator's
   existing transaction-adjacent lookup remains mandatory, so a pre-inspection
   against an older snapshot cannot authorize a duplicate execution.

6. A known unsupported or direct application-commit lane disables coverage
   before a later group can bridge it. A gap, count or locator-proof mismatch,
   failed or uncertain publication, transient-index invalidation, or loss of
   publication ownership disables coverage and otherwise follows the existing
   write fence and typed failure. Coverage is monotonic and cannot be re-armed
   in that process. Async checkpoint rebase may preserve it only because the
   logical published frontier and bytes are unchanged.

7. A crash loses coverage. Reopen after even one command observes a nonempty
   authoritative frontier and cannot re-arm it. Existing exact durable locator
   reads, accepted transient reconstruction, dirty recovery, retention, backup,
   restore, and historical compatibility remain authoritative; no checkpoint
   or locator-table count substitutes for them.

8. The proof is private to redb admission reads. Public audit and
   `SubscribeCommits` retain their accepted first-demand reconstruction.
   Applications, SDKs, transports, MCP, CLI, configuration, and operators gain
   no switch, status, receipt, warmup operation, fallback selector, storage
   handle, or authority.

9. WP-778 implements and proves this decision only. WP-705 separately owns the
   fixed-scale lifecycle qualification after WP-778 closes. WP-578 separately
   owns its uninterrupted 72-hour evidence; neither evidence package can be
   satisfied, relabelled, or closed by WP-778 tests.

## Options considered

1. **Fresh empty-process induction:** selected. It proves a complete base
   without scanning or durable evidence and extends only across bounded atomic
   publications produced by the same process.
2. **Seed from validated-prefix checkpoint or locator cardinality:** rejected.
   Pre-locator databases can carry valid checkpoints, and a count is not an
   exact key-set proof.
3. **Rebuild or retain the command-derived index:** rejected for this path. It
   restores population-linear time and memory and does not address repeated
   fallback work while the index is deliberately dormant.
4. **Persist a coverage marker or backfill history:** deferred. Either changes
   durable compatibility and requires a separate accepted decision.

## Consequences

- Fresh databases can publish large bounded command populations without an
  O(history) absence scan per candidate while retaining dormant indexes.
- The optimization deliberately does not apply after restart or to upgraded,
  restored, retained, or otherwise nonempty histories.
- Any inability to prove the induction loses only the optimization and remains
  fail closed; it never manufactures absence or repairs authority.
- WP-705 must rerun its exact qualification commands on the accepted
  implementation revision. Prior failed diagnostic runs remain non-evidence.

## Standing design tests

- **Interface safety:** The proof is storage-private, automatic, and usable only
  for exact absence validation. No caller can arm, preserve, reset, inspect, or
  bypass it, select startup or reconstruction behavior, or skip the mandatory
  transaction-adjacent idempotency check.
- **Scale:** State is one frontier and one closed tag. Publication proof is
  bounded by the existing 256-command group, point reads remain bounded, and
  no history, locator, command, key, segment, or duration-proportional
  collection is retained or scanned.

## Checks

- `fresh_locator_coverage_arms_only_from_an_empty_authoritative_frontier`.
- `fresh_locator_coverage_advances_only_with_contiguous_published_locators`.
- `fresh_locator_coverage_fails_closed_across_gaps_uncertainty_and_restart`.
- `cold_fresh_database_publications_complete_without_history_scans`.
- The storage recovery matrix and full recovery suite preserve crash,
  uncertainty, atomicity, and idempotent replay semantics.
- Architecture checks freeze the absence of durable/public coverage state and
  any call from public audit or `SubscribeCommits`.
