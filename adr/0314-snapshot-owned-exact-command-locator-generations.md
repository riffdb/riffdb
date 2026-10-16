---
adr: "0314"
title: Snapshot-Owned Exact Command Locator Generations
status: accepted
tier: guarantee
date: 2026-10-15
accepted: "2026-10-15"
acceptance: 'maintainer, in session, 2026-10-15: "Accept ADR-0311 and its registration as written"'
requires: [ADR-0102, ADR-0104, ADR-0182, ADR-0244, ADR-0263, ADR-0266]
amends:
  - ADR-0244 Decision 1's before-readiness rebuild in existing bounded startup modes
supersedes: []
requirements: [TXN-042, STO-032, REC-001]
packages: [WP-825]
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations:
  - id: OBL-0314-1
    package: WP-825
    proof: owned_snapshot_outcome_survives_retention_of_original_command
    says: A captured read retains its original outcome after later retention.
  - id: OBL-0314-2
    package: WP-825
    proof: owned_snapshot_provenance_survives_retention_of_original_command
    says: A captured read retains its original provenance after later retention.
  - id: OBL-0314-3
    package: WP-825
    proof: locator_generation_publication_never_splices_two_source_views
    says: Command, pruning and checkpoint publication keep source and directory together.
  - id: OBL-0314-4
    package: WP-825
    proof: locator_generation_budget_refuses_before_durable_submission
    says: Exhaustion refuses before durability and charges protected old generations.
  - id: OBL-0314-5
    package: WP-825
    proof: locator_generation_last_owner_releases_all_charges
    says: Cancellation and final owner release reclaim all charged derived ownership.
  - id: OBL-0314-6
    package: WP-825
    proof: locator_generation_pending_never_proves_absence
    says: All snapshot constructors distinguish pending, invalid and complete directories.
review_triggers:
  - A compiled ceiling, its scope or its attribution would change.
  - A lookup would consult a different authoritative source or an incomplete directory.
  - Recovery scratch, overlay, provider or payload-cache guarantees would change.
  - A durable format, public bypass, acknowledgement or ordering rule would change.
---
# ADR-0314: Snapshot-Owned Exact Command Locator Generations

## Context

The eight-hour TicketDesk rehearsal failed at additive deployment, before its
scheduled restore. A native test reproduces a cold module lookup combining an
old checkpoint with newer command-audit locators. Routing that request through
existing bounded metadata reads fixes that path. Independent tests then show
old owned snapshots returning absent outcome and provenance after ordinary
retention, despite preserving their original command authority.

ADR-0104 already requires exact locators from the captured source generation.
The implementation instead owns mutable process-current maps. Repairing their
lifetime requires an explicit aggregate memory allowance and pressure policy;
neither disposable payload-cache capacity nor provider capacity grants one.

## Decision

1. Each published authoritative checkpoint-plus-suffix owner carries a locator
   capability bound to that snapshot. Locator-dependent answers require its
   complete immutable generation; pending/invalid capabilities follow 7 and 8.
   Capture and clone that owner once; lookups use its generation and original source
   custody. Bind completeness to database, incarnation, registry, exact retained
   command set, segment frontier and source-root identity. Application head alone
   is insufficient. Partial, pending and invalid directories never prove absence.
2. Derive the existing six locator kinds from canonical segment manifests. Use
   first-party ordered trees with immutable shared nodes and keys; capture is
   O(1), and mutations copy bounded search paths. Successor deletion removes
   entries rather than accumulating lifetime tombstones. Payload residency is
   optional. Preserve all exact key, member, ordinal and reciprocal checks.
3. Use one compiled **512-MiB aggregate native locator-memory ceiling per
   process**, shared across all databases and live, follower, archive, restored
   and static readers in that process. It covers the union of current, pinned,
   retired and private staged generations; nodes, key capacity, manifests,
   segment metadata, allocation/control overhead, root descriptors, construction
   scratch, reservation headroom and queued destruction all count. Count each
   physical allocation once within this pool while any owner retains it.
   Within the 512 MiB, ordinary construction/ownership may consume at most
   448 MiB; reserve 64 MiB for maintenance progress and reclamation. At most
   1,024 native generation owners, including private and queued owners, may
   exist; ordinary publication must leave one owner slot for maintenance.
4. This pool is separate from the existing 64-MiB recovery scratch, 64-MiB
   disposable payload cache, 256-MiB overlay and 1-GiB provider ceilings. None
   increases or becomes exempt. Provider admission must conservatively account
   for the native ownership it can retain under ADR-0266; overlapping accounting
   between independent ceilings grants no extra native capacity. No application
   setting may raise a ceiling, evict a protected root or skip validation.
5. Admit the complete incremental allocation and bounded destruction work
   before allocation or durable submission. Prepare successor source and locator
   ownership together, then publish one owner after the successful existing
   fence and before releasing results. Apply this to journaled and direct
   publication, pruning and checkpoint rebases. There is no interval with
   retired locators and an older source or a newer source and older locators.
   Preserve FIFO sequencing, sole-writer authority, uncertainty and acknowledgements.
6. Pressure produces existing typed resource/unavailable refusal before
   durability. Pending private stages and protected snapshots continue to own
   their charges until cancellation, definitive disposition or final release.
   Never substitute a later root or discard unresolved durable ownership.
   Protected pins or a legal large layout may prevent another operation from
   fitting; refusal is accepted, rather than reducing retention fences, stealing
   another budget or acknowledging an incomplete publication. Reserve and test
   actual progress headroom; if the frozen workload cannot progress within this
   ceiling, qualification fails and changing the ceiling needs separate acceptance.
7. Startup may construct a directory during its existing complete validation.
   Amend ADR-0244 Decision 1's before-readiness rebuild for existing bounded
   startup modes: verified clean, dirty and applicable restored startup keep
   their existing lifecycle, bounded-root, readiness and recovery-scratch rules.
   A pending locator generation has an admitted cancellable construction owner
   bound to the captured source. Locator-dependent operations use their existing
   bounded waits or return typed unavailable until that complete generation
   exists. Metadata reads retain their bounded first-access checks. No population
   walk is moved into readiness, and no incomplete read becomes absence.
   Locator-dependent mutations, including idempotency, uniqueness, audit and
   retention decisions, also refuse before durability while capability is pending
   or invalid. A builder completes only its original source owner, or transfers
   evidence to a successor after proving the exact command membership unchanged;
   it cannot attach a stale completion to an unrelated later root.
8. Every constructor, including readers without a live shared-port handle,
   carries complete locator capability or explicit pending/invalid state.
   Original-source command resolution uses the existing history-stub-aware
   decoder. Do not revive retired durable locator tables, rescan all history per
   point lookup, copy the entire index per snapshot, or hold a global writer or
   index lock for an arbitrary snapshot lifetime.
9. Old readers retain original source pages and extent custody until release.
   Logical pruning can advance; physical recycling observes those owners.
   Share unchanged nodes across generations and release charges at final
   reference destruction, including bounded queued retirement. Reference cycles
   and an unbounded predecessor chain are forbidden. Crash reconstructs derived
   directories from unchanged durable authority; no new persistent format,
   registry identity, compatibility fixture or application interface is introduced.
   Reserve a retirement slot and its descriptor when admitting each owner.
   Final-owner release transfers ownership into that reserved slot in bounded
   work without allocation or refusal. Destruction processes at most 256 charged
   ownership items per scheduling step, then yields; this includes nodes and
   manifest entries, so a large recursive drop cannot bypass the quota. Queued
   items retain charges, and cleanup uses reserved progress capacity. New
   ordinary admission refuses before exhausting these reserved resources.
10. The 512-MiB choice is a new explicit capacity policy, not a measured claim.
    Before merge, measure actual node/manifest ownership, retained-generation
    peaks and interference on the qualification host, including the frozen
    workload and large legal layouts. The existing 5-second administrative,
    45-second release, 60-second recovery and retention-interference gates remain.

## Options considered

1. **Current-record hints:** rejected as the directory owner. Identity reuse,
   retirement/publication gaps and constructors without a live source prevent
   current negative membership from proving historical absence.
2. **Lazy whole-history index per snapshot:** rejected for the regular path.
   Its first access scans history, repeats ownership and needs its own population
   budget. A partial index cannot justify a negative answer.
3. **Keep pruned locators indefinitely or copy every map per read:** rejected
   because retention or capture cost grows with lifetime work.

## Consequences

- Same-view lookup becomes an owned capability rather than a timing assumption.
- Persistent-map staging and lifetime accounting add implementation work and
  write-path metadata costs that must be measured before merge.
- Large workloads and long-lived pins can receive typed capacity refusal.
  Increasing capacity, adding durable index snapshots and changing public
  controls are deferred decisions, not implementation discretion.

## Standing design tests

- **Interface safety:** applications still express only safe compiled commands
  and existing typed reads. No knob opts out of snapshot consistency, scoping,
  idempotency, authorization, validation or acknowledged durability.
- **Scale:** aggregate retained nodes and metadata, staging, scratch, work,
  destruction queues and protected source owners are admitted and bounded.

## Checks

- The two genuine pin/prune regressions above; future-key exclusion and identity
  reuse across pins; malformed/missing locator and history-stub refusals; all
  source constructors; command/prune/checkpoint publication schedules; exhaustion
  before durability; cancellation and final-reference release; process crashes;
  original public deployment and restore; scoped acceptance and complete CI.
