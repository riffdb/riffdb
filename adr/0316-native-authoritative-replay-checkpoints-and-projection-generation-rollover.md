---
adr: "0316"
title: Native Authoritative Replay Checkpoints And Projection Generation Rollover
status: accepted
tier: guarantee
date: 2026-10-16
accepted: "2026-10-16"
acceptance: 'maintainer, in session, 2026-10-16: "i approve" (exact ADR-0316, SPEC amendments and WP-825 registration)'
requires: [ADR-0019, ADR-0017, ADR-0086, ADR-0186, ADR-0240, ADR-0246, ADR-0248, ADR-0263, ADR-0265, ADR-0266]
amends: [ADR-0019, ADR-0017, ADR-0086, ADR-0186, ADR-0240, ADR-0246, ADR-0248, ADR-0263, ADR-0265]
supersedes: []
requirements: [STO-012, PRJ-001, PRJ-002, PRJ-003, PRJ-004, REC-001, REC-003, STO-020]
packages: [WP-825]
obligations: []
review_triggers:
  - A checkpoint would derive authority from a disposable projection.
  - A derived apply would acquire the primary mutation gate.
  - Source closure, exact duplicate equality or restore identity would weaken.
  - An incomplete or foreign checkpoint would become selectable.
---
# ADR-0316: Native Authoritative Replay Checkpoints And Projection Generation Rollover

## Context
A native three-command fixture was independently Clean and queried exactly; permitted pruning of command 1 left exact derived rows but broke recovery at HistoryPruned/ProjectionStateIntegrity.
Empty-target restore also lacks this prefix. ADR-0086 requires complete source closure; this record proposes an authoritative compact input and keeps operational projections caches.

## Decision
1. Store a versioned, plan-specific commit-log compaction checkpoint in native
   primary tables. It is authoritative input for exactly one ProjectionIdentity,
   not general historical export/audit authority and never live projection state.
   Independently reconstruct it from complete authoritative inputs; never certify
   a cache by copying its rows, frontier, marker hashes or a checksum over them.
2. Preserve ProjectionIdentity=(lineage,ProjectionId,ProjectionPlanHash). Full source locator
   is (DatabaseId,allocation-history-incarnation,nonzero CheckpointOrdinal). Only the
   coordinator allocates checked ordinals,never reused within that allocation incarnation.
   Each checked new-incarnation transition,including promotion,installs Next(1) before
   allocation. Inherited source IDs remain unchanged;verified restore/promotion/bootstrap
   transition evidence establishes validity,never bare incarnation equality or ID rewriting.
   Every key/reference binds full locator;every base/root/capture additionally binds immutable
   digest/origin commitment and verified transition evidence. The tuple alone is not authority
   or universal cross-fork uniqueness;conflicting retained-key bytes refuse. Source rows stay
   generation-neutral;new keys/messages/hash domains never reinterpret V1 projection bytes.
3. Native SourceRowV1 binds projection identity,full source identity,canonical group
   components,measures,last_changed. SourceObjectV1 binds both identities,AppliedThrough(F),
   captured authoritative source fence/commitment,optional prior-source digest,row count,
   encoded charge,canonical row root,lifecycle. The digest hashes immutable descriptor
   fields only; lifecycle/progress cannot alter source identity. Row-root preimages exclude
   storage ordinal/generation and bind projection identity,F,components,measures,last_changed.
   SourceRootV1 selects the full source locator,digest/origin commitment and publication receipt.
   SourceMaintenanceV1 binds full candidate identity,immutable expected prior root,
   source fence,final root/count/charge and copied progress/last key/rolling hash.
   Row/object/maintenance keys and every root/base reference bind full source identity.
4. Object lifecycle is Copying -> Published -> Superseded -> Collecting -> Removed,
   or Copying -> Aborting -> Removed. Rows are immutable after insertion; only the
   object/control record changes. Only the SourceRootV1-selected Published object
   authorizes new source acquisition or generation seeding. A valid existing persisted
   GenerationSourceBaseV1 or captured pin retains exact authoritative access to its
   Superseded base+contiguous tail through generation retirement/final pin release,
   including restart validation. Never silently substitute the latest source; missing
   or corrupt closure invokes Decision20. Superseded alone authorizes neither new
   generation seeding nor pruning. Partial candidate rows are hidden maintenance
   records; they cannot initialize a generation,justify pruning or close backup sources.
5. Root/object/row/maintenance namespaces are ReplicatedAuthoritative in the native
   authoritative catalog. Normal V3 mutations carry exact bytes; bootstrap includes
   them at its captured fence. No derived rows/generation controls cross the wire.
   Native physical backup captures source tables in its same primary snapshot,
   avoiding an external authoritative file or a second publication protocol.
6. Evaluate independently from original commits1..F, or a previously published
   exact-identity checkpoint plus every retained commit after its frontier throughF.
   Use existing catalog-owned event materialization, exact writer plans and checked
   aggregate arithmetic; account for irrelevant commits and exact last_changed.
   Capture one immutable prefix fence and real source pin. Appends beyondF are safe;
   changed incarnation,plan,source binding or lost pinned prefix invalidates work.
   A reused parent source requires exact locator,digest/origin and verified closure;parent mismatch refuses.
7. One background owner recomputes in a private bounded redb staging map. It streams
   canonical rows and derives root/count/charge without a population-sized RAM map
   or per-group prefix rescans. Registered domain-separated SHA-256 hashes canonical
   length-framed rows in order and final count/charge; page boundaries are irrelevant.
   Hash verification protects authoritative bytes, never substitutes for evaluation.
8. A fields-private candidate owns verified plan,source pin,independently derived
   expected root and immutable staging custody. Specialized coordinator maintenance
   operations allocate candidate,copy ordered pages,publish,abort and collect. They
   expose neither arbitrary callbacks nor public writes. Exact copied progress,
   new keys and checked per-page transitions commit atomically with canonical V3.
9. Only bounded prepared authoritative page/control writes hold the primary gate.
   Evaluation,hashing,bulk iteration,plan resolution and staging filesystem I/O run
   outside it. Ordinary derived apply never calls this writer. Publication atomically
   requires complete root/count/charge,expected prior root and unchanged source
   authority. It does not advance the retention watermark or wait for latest head.
10. Candidate/unknown retries reconcile exact persisted page/control bytes before
    advancing. Unequal bytes under one ordinal refuse. A crash leaves the previous
    complete root or the new complete root. A copied candidate may resume only its
    recorded immutable source/progress; otherwise coordinator aborts it safely.
11. Before pruning, each active history projection must have complete authoritative
    source closure through the proposed watermark. Checkpoint coverage joins all
    existing consumer,outbox,follower,bootstrap,reader,scrub and operator fences.
    Add an independently read minimum history-projection source frontier to the
    retention-fencing inputs; missing/unreadable required source prevents pruning.
    A cache frontier alone never proves closure. Unreadable/capacity-exhausted source
    refuses unsafe pruning; no dropped hold, synthetic acknowledgement or history skip.
12. Seed a disjoint new derived generation from a selected authoritative checkpointF.
    Immutable GenerationSourceBaseV1 binds projection identity,generation,full source
    locator,digest/origin commitment,F. Seed rows remain unpublished until exact canonical seed completion
    and control selection commit. Only then is F the starting frontier; no individual
    apply or manufactured markers for commits<=F. BeforeFirst remains the unseeded base.
13. The seeded generation individually applies every commitF+1..H, with unchanged
    exact ProjectionApplyHash and atomic rows/marker/frontier. After catching up,
    normal publication cannot lower the identity's old published frontier. A new
    checkpoint triggers source-based rollover, never a mutable base change within
    an active generation. Keep at most published+candidate generations as today.
14. Validate each retained generation from its exact source base plus complete
    contiguous tail to its captured frontier. Check every individually applied
    marker and exact final group rows,absence,last_changed. An absent/corrupt cache
    rebuilds; a corrupt source is authoritative corruption and cannot use the cache
    as repair evidence. Latest-head appends alone do not invalidate the captured fence.
15. An apply at/below a seeded generation's base returns typed NotAppliedInGeneration:
    no mutation,no equal-success assertion. Every sequence actually applied to a
    live generation retains exact current/historical duplicate equality and markers
    for that generation's entire lifetime. Retired-generation requests return typed
    GenerationRetired; outstanding unknown applies reconcile before retirement.
16. After new-generation publication, the old generation becomes inert and never
    resumes or serves new queries. Collect its whole namespace in bounded crash-safe
    chunks only after all captured query,recovery,worker and unknown-outcome pins
    release. An incomplete collector cannot reactivate it. Do not delete markers
    from a live generation or make source publication depend on its cache contents.
17. Keep at most one uncollected retired generation per identity; another rollover
    waits for its collection. Start rollover by32768 individually applied tail commits;
    a generation refuses further derived apply at131072 such commits. This proposed
    limit allows65536 population commands plus headroom; it asserts no rollover ETA
    or measured keep-up. Loaded seed+write qualification must prove timely rollover.
    It bounds marker storage; it never refuses an authoritative command. Failures remain
    typed Building/Lagging/Degraded as appropriate and unsafe history stays protected.
18. Every live/captured generation retains its exact authoritative base AND the
    contiguous tail after that base through its captured frontier. A source-object
    pin alone is insufficient. Primary prune/GC uses the minimum of these complete
    generation/unknown-outcome/source pins as well as selected checkpoint coverage.
    Superseded objects retire only after rollover and final reference release.
19. Followers receive source checkpoints through normal V3/bootstrap and keep local
    generations. Before applying a prune/source-GC frame that breaks an old local
    generation's closure, pause follower replay and complete rollover/retirement
    while its old base+tail still exist. Existing bounded replication queues/source
    holds handle lag; never buffer an unbounded tail or hold a whole-redb snapshot
    while unrelated writes advance. No primary command waits on follower rollover.
20. Whenever a retained generation lacks complete base+tail, suspend it and rebuild
    from the latest complete authoritative source; never attempt partial validation
    or serve it as independently recovered. This applies during runtime and restart,
    amending ADR-0248's absent/torn-only restriction. Captured operations finish only
    under their complete pinned source; otherwise return typed invalidation/refusal.
21. Backup validates selected checkpoints and closed source for all backed active plans;
    candidates never justify closure. Restore preserves source tables and checked origin
    identities through RestoreAnchor,never rewrites them or derives authority from caches;
    remove the sidecar and rebuild locally. Before new allocation,restore normalization
    establishes the new incarnation's versioned allocator at Next(1),with checked exhaustion.
    New sources bind that incarnation; copied unfinished candidates cannot resume across
    changed incarnation and abort under Decision10. Missing,foreign,corrupt or incomplete
    source refuses before replacement. No external high-water authority is introduced.
22. Unrelated compatible deployments with unchanged plan hash reuse exact sources.
    A new/changed identity still promises BeforeFirst semantics and requires complete
    closure for that identity before activation. If old input is gone and no exact
    authoritative source exists, return typed SourceUnavailable before catalog change.
    An old plan's aggregate never seeds a changed plan; future-only semantics are excluded.
23. NEW compiled admission limits:<=64 published identity roots;<=65536rows and64MiB
    canonical key/value charge per source object;<=256MiB selected-source charge and
    <=512MiB total source-table charge including candidate,superseded/pinned/retiring
    rows and metadata;one candidate/database. Reserve full new object before copying.
    FourKiB bounds each complete descriptor/root/maintenance record. These deliberate
    pre-alpha source limits are independent of provider/locator budgets and may refuse.
24. NEW worker limits:one worker/database;64MiB aggregate incremental working memory,
    including every staging-cache,decoded-page,evaluator,prepared-write,codec,root
    and scratch allocation/capacity; reserve before allocation,not after measuring it;
    private staging<=64MiB encoded/128MiB allocated disk. Reserve capacities before
    allocation and refuse on overflow/excess. Pins release only after final references.
    Authoritative native sources are never evicted by provider LRU or worker drop.
25. A copy/collection step has<=128 row mutations and4MiB complete row bytes, reduced
    further by actual existing mutation/write/V3 frame ceilings including all control,
    preimage and audit costs. Schedule through existing drained maintenance/pacing
    owner,including nineteen-times gate-cost rest; no competing timer or forced drain.
26. Preserve existing500rows/4MiB scan pages,1MiB state semantics,4096updates,15MiB apply
    semantics,16MiB apply writes/snapshots and key/group limits. Exact providers retain
    their256slots,family-specific ceilings and1GiB aggregate owned/reserved/scratch
    budget. Shared allocations have one charged owner; no source allocation borrows
    an uncharged provider capacity. Track physical redb overhead/snapshot holds separately.
27. Abort/drop/cancellation releases private working resources; published source
    authority survives worker cancellation. Object drop needs exact root/generation/
    backup/captured-reference closure. Bounded cleanup cannot stimulate endless
    eligible V3-history backlog. Expose bounded byte/count/pressure diagnostics with
    ordinary redaction; no raw keys,payloads,paths or caller-selected source/budget.

## Consequences
- PRJ-004 becomes: "Projection state MUST be rebuildable from complete authoritative commit-log inputs,
  or a coordinator-published same-identity authoritative replay checkpoint plus a contiguous retained tail.
  Derived state MUST NOT establish or repair that source's authority." AGENTS8 stays literal:
  checkpoints are authoritative commit-log compaction;operational projections remain disposable.
- SPEC15.2/ADR-0017 add source-seeded generations and immutable source bases;individually applied suffixes
  retain every marker and exact equality for their generation lifetime;seeded prefixes have no fabricated marker.
  PRJ-003 adds typed seed-prefix/retired refusal and source-seeded rebuilds;PRJ-001/2 stay unchanged. Collect whole retired generations boundedly.
- ADR-0086§1/8 requires selected backup-authoritative compact source+tail before retiring input; capacity failure cannot erase source obligations.
- ADR-0240 distinguishes coordinator-owned authoritative compaction page writes from derived apply:
  only the former uses primary maintenance transactions;the latter stays outsideprimary and cannot gate commands. Measure maintenance cost.
- ADR-0246 default removal applies only to caches,not authoritative native source;ADR-0248 permits replicated source compaction,local source-seeded rollover and Decision20 recovery. Derived rows/control stay local.
- ADR-0186/0263 add native authoritative domains/registry/catalog identities and exact V3/bootstrap/backup/restore handling.
  Preserve V3 bytes;use normal catalog upgrade or controlled refusal. ADR-0265 adds source accounting/scope,not weaker fault gates.
- SPEC10.2/STO-012 and ADR-0019 permit four versioned native source namespaces and
  permanent `projection_checkpoint_ordinal/v1` state in `meta`,binding allocation-history-incarnation
  and Next(nonzero)|Exhausted. Only the coordinator advances it;maximum means exhaustion,no reuse/wrap
  within an incarnation,including abort/removal. Restore follows Decision21;normal authority-catalog/record-registry upgrade and fixture review are required.

## Standing design tests
Interface safety: no public checkpoint/source/unsafe operation; application commands,
authorization,idempotency and durability remain unchanged. Scale: finite new source,
worker,staging and marker budgets with explicit refusal; current45/60-second recovery,
maintenance interference,full Ready/progress and24-hour gates remain required.
Native co-location and finite source caps are deliberate,reversible pre-alpha limits.
## Checks
Proofs: genuine three-command/prune-one recovery; exact seed+tail and changed-plan
refusal; corrupt cache versus corrupt source; randomized count/sum/schema/irrelevant
commits; V3/follower/bootstrap/native empty restore; captured append/source races;
unknown duplicate/retired pin closure; budget exact/one-over and release; process kills
at every candidate/page/root/rollover/prune/collection/RestoreAnchor boundary; loaded
checkpoint keep-up,physical bytes,writer interference and final authorized Ready query.
