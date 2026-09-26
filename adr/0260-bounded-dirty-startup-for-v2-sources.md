---
adr: "0260"
title: Bounded Dirty Startup For V2 Sources
status: accepted
tier: guarantee
date: 2026-09-25
accepted: 2026-09-25
acceptance: 'maintainer, in session, 2026-09-25: exact text selected ("Accept exact
  text"); Amendment 1 accepted 2026-09-25, maintainer, in session: exact text selected
  ("Accept exact text")'
requires: [ADR-0156, ADR-0157, ADR-0182, ADR-0203]
amends:
  - ADR-0156 Amendment 6 for V2 source startup that meets the ADR-0182 bounded dirty preconditions
  - ADR-0182 to include AuthoritativeStateCatalogV2 sources in the bounded dirty path
  - ADR-0203 Decision 5 for retained-history scrub failures on V2 sources
  - PERF-019 for the V2 startup cost statement
supersedes: []
requirements: [REC-004, REC-005, PERF-019]
packages: [WP-760]
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations:
  - id: OBL-0260-1
    package: WP-760
    proof: v2_dirty_startup_is_bounded_and_point_validates_its_fence
    says: A V2 source meeting the bounded dirty preconditions reaches readiness on the bounded path after point-validating its source admission and, when fenced, the referenced fence record and its exact receipt; a missing or contradictory admission or fence refuses before readiness and never synthesizes Active or clears a fence.
  - id: OBL-0260-2
    package: WP-760
    proof: stray_fence_row_withdraws_v2_source_authority_after_bounded_readiness
    says: A stray fence row planted outside every transaction does not stop bounded V2 readiness; the background scrub's retained-history validation refuses it, records an incident, and withdraws authoritative readiness, and the offline scrub refuses the same database.
review_triggers:
  - A V2 source would gain source authority without point-validating its admission and referenced fence.
  - A retained-history or fence failure found after readiness would degrade instead of withdrawing V2 source authority, or would be repaired, hidden, or retried past.
  - A fence could be written other than in one physical transaction with the admission update, or more than one fence could exist for one lineage.
  - A durable format, catalog namespace, or public surface would change under this record.
---
# ADR-0260: Bounded Dirty Startup For V2 Sources

Amendment 1 accepted 2026-09-25 by the maintainer in session as exact text:
Decision 2's refusal scope, recorded in Decision 2. No other decision changes.

## Context

ADR-0182 (accepted 2026-09-01) makes a startup without a valid clean-close
certificate journal-suffix recovery plus bounded-root validation, with no
population walk before readiness. ADR-0156 Amendment 6 (accepted 2026-09-18)
then required an AuthoritativeStateCatalogV2 source to perform complete
ordinary validation, including retained fence evidence, before it is granted
source authority, and deferred any bounded V2 startup to a successor design.
Database initialization creates every new source on catalog V2, so under
Amendment 6 no new database can use the bounded dirty path, and a
production-scale V2 dirty restart refuses at the 512 MiB complete-path
ceiling (the WP-705 receipt handed to WP-760).

The retained fence evidence Amendment 6 names is `validate_fences`, which
streams all of AUDIT. A fence is written only while the admission is unfenced,
in one physical transaction whose receipt carries both the fence record and
the fenced admission, and journal recovery applies whole frames. After any
crash, therefore, the admission record already names the only fence of its
lineage. Every open already point-validates that fence and its receipt. The
full scan adds exactly one power: detecting a fence row that no transaction
wrote, which is corruption, not a crash state. No bounded structure can detect
such a row either, since it would lack any index entry.

## Decision

1. A V2 source startup that meets the ADR-0182 bounded dirty preconditions
   (V3 active, current record registry, a validated-prefix checkpoint that
   loads, not a follower, no valid clean certificate) takes the ADR-0182
   bounded dirty path. The first open under the current registry still takes
   the complete path and writes the checkpoint.
2. Before readiness, and therefore before source authority, that path
   validates by bounded point lookups: the fixed V3 roots, the history tail
   and its allocator frontier, the source admission record, and, when the
   admission is fenced, the referenced fence administration record and the
   exact reconstructed fence receipt, byte for byte; and the admission's
   source evidence against that fence. A missing, contradictory, or corrupt
   admission or fence refuses before readiness. Validation never synthesizes
   Active and never clears a fence.
   Amendment 1: Decision 2's "contradictory" means contradictory with what
   point lookups read. Before readiness the bounded path refuses a missing
   admission, an admission of another lineage, a corrupt admission, and a
   referenced fence record or receipt that is missing or differs from its
   exact reconstruction. An admission that reads Active while an unreferenced
   fence row of its lineage exists cannot be seen by any point lookup; like
   every stray fence row it is corruption, never a crash state, and Decision
   3's scrub refuses it with Decision 4's authority withdrawal.
3. The exhaustive retained-fence validation (no fence of the current lineage
   other than the referenced one, none from the same or a later incarnation
   under another lineage) runs in the background scrub and in the offline
   scrub, as part of their complete retained-history validation.
4. On a V2 source, a background-scrub failure of that retained-history
   validation records its incident and withdraws authoritative readiness
   (runtime routing stops, health NOT_READY), because it puts source
   authority itself in doubt. Every other scrub finding keeps ADR-0203
   Decision 5: storage DEGRADED with the incident while serving continues.
   Nothing is repaired, hidden, or retried past.
5. A V2 clean-close certificate remains declined for bounded clean startup
   and is consumed exactly as REC-004 requires; the startup that follows is
   the Decision 1 path when its preconditions hold, and otherwise the
   complete path.
6. PERF-019's V2 statement becomes: V2 startup that meets Decision 1's
   preconditions has bounded cost and reports the closed mode
   `complete_validation` with `population_table_walk=none`; V2 evidence still
   never claims a bounded clean-certificate startup. This record changes no
   durable format, catalog namespace, receipt, or public surface.

## Options considered

1. **Keep complete validation for every V2 startup:** honours Amendment 6
   as written, but no new database gets bounded recovery, and production-scale
   V2 dirty restart cannot reach readiness. Rejected.
2. **A durable per-lineage fence index:** adds a durable-format change and
   crash proofs yet detects nothing the admission record does not already
   prove after a crash; a stray AUDIT row would have no index entry. Rejected.
3. **Withhold source authority until a post-readiness AUDIT fence scan, or
   until the whole scrub, completes:** reads serve at once, but write
   availability after every crash scales with the audit or full population.
   Rejected.
4. **Bounded V2 startup with the exhaustive scan in the scrubs, and fence
   failures withdrawing authority:** keeps crash-state source authority fully
   proven before readiness and moves only corruption detection after it, as
   ADR-0182 does for every other population. Chosen.

## Consequences

- New databases get bounded dirty recovery; the WP-760 production-scale exit
  gate becomes reachable for V2.
- Between readiness and scrub completion, a stray fence row produced by
  corruption is undetected; source authority is withdrawn when the scrub
  finds it. The window exists only for corruption, never for a crash.
- A retained-history failure on a V2 source stops writes and reads on that
  process until an operator runs the offline scrub and repairs from backup.
- No migration: existing V2 databases reach the bounded path after their
  first complete open under the current registry writes a checkpoint.

## Standing design tests

- **Interface safety:** no request, flag, or transport option selects the
  path, skips the point validation, suppresses the scrub, or keeps authority
  after a fence failure; the only observable is the closed startup mode and
  the existing health.
- **Scale:** readiness performs a fixed number of point reads for admission
  and fence regardless of audit or population size; the exhaustive scan
  streams in the rate-bounded scrub.

## Checks

- `v2_dirty_startup_is_bounded_and_point_validates_its_fence` (OBL-0260-1):
  fenced and unfenced V2 sources, plus corrupt referenced fence and receipt
  arms refusing before readiness.
- `stray_fence_row_withdraws_v2_source_authority_after_bounded_readiness`
  (OBL-0260-2): planted stray fence row, bounded readiness, scrub withdrawal
  with incident, offline scrub refusal.
- The existing V2 clean-certificate decline tests, re-baselined to route to
  the Decision 1 path.
