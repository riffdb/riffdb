---
adr: "0281"
title: Bounded Native Bootstrap Source Custody Validation
status: accepted
tier: guarantee
date: 2026-10-07
accepted: '2026-10-07'
acceptance: 'maintainer, in session, 2026-10-07: Accepted, in response to the exact-text acceptance and WP-825 scope request for ADR-0281'
requires: [ADR-0240, ADR-0248, ADR-0268, ADR-0271, ADR-0279, ADR-0280]
amends: [ADR-0268, ADR-0280]
supersedes: []
requirements: [REP-003, REP-006, REC-001, END-008, LOG-001]
packages: [WP-825]
obligations:
  - id: OBL-0281-1
    package: WP-825
    proof: bootstrap_custody_continuation_matches_complete_source_oracle_without_population_walk_per_page
    says: One complete bounded initial proof and checked successors preserve exact current bootstrap custody without repeating the retained population walk for every page.
  - id: OBL-0281-2
    package: WP-825
    proof: bootstrap_custody_refuses_retired_changed_corrupt_and_unclassified_evidence
    says: Missing or changed holds, retirement, corrupt source evidence, incompatible lineage and unknown publication cannot release bootstrap bytes.
  - id: OBL-0281-3
    package: WP-825
    proof: bootstrap_custody_cancellation_preserves_shared_candidate_capacity_and_pin_bound
    says: Cancellation releases nondurable custody, preserves a prepared acknowledgement owner and keeps the shared compiled capacity and two-pin bound.
review_triggers:
  - Bootstrap registration, attachment, receiver startup, publication or validation semantics change.
  - A root counter, elapsed time, old artifact or capability assertion substitutes for source proof.
  - Foreground reads bypass maintenance pacing for mutation or introduce another proof owner, writer, queue or retained snapshot chain.
  - Public surfaces, formats, authorization, ordering, durability, workload or deadlines change.
---
# ADR-0281: Bounded Native Bootstrap Source Custody Validation

## Context

After ADR-0280's acknowledgement and shutdown repairs, the unchanged required-host
replication suite passes 25 cases and fails initial bootstrap held attachment at
its original 60-second deadline. A separate diagnostic build on the 8-vCPU,
32-GiB Linux SSD host preserves the same workload and deadlines. Bootstrap source
custody performs 60 complete retained-history validations, spending 23.525 seconds
on them. Each observes source sequence 299. Initial source construction and hold
registration finish in 3.980 seconds; the receiver finishes transfer at 27.230
seconds and candidate construction at 44.355 seconds. Subsequent complete startup
validation has not finished when the attachment watchdog expires. These failed
contended debug diagnostics establish repeated population-dependent source-read
work, not its exclusive responsibility for the timeout or release throughput.

Every manifest/page custody check calls the complete retained-history validator.
An exact held artifact still requires fresh source authority before releasing
bytes; retaining old authorization is unsafe. ADR-0280 explicitly excludes
bootstrap validation. Replacing repeated source walks with a complete continuing
proof therefore needs exact human acceptance. Receiver construction, startup,
projection rebuild and publication validators remain outside this proposal.

## Decision

1. Extend native source-proof reuse only to read-only current-custody checks for
   an already durably held bootstrap artifact before its manifest or page is
   released. Preserve complete canonical source, receipt-chain, audit,
   registration, fence, promotion and hold validation. Grant no bootstrap
   registration, attachment, retirement, archive acknowledgement, source fence,
   promotion, restore or receiver publication operation.
2. Establish the complete initial source proof outside the writer in bounded
   native pages, using existing validators, byte/allocation ceilings, at most
   256 receipt rows per step and exact-end checks. Incomplete, failed or cancelled
   work releases no manifest or page. Preparation owns no writer lease or
   coordinator execution turn and cannot pause offered foreground load.
3. Use the existing graph-owned source-control certificate shared with source
   reclamation and follower acknowledgement. Retain at most one certificate and
   one candidate, at most two validation pins, and the existing combined 64-KiB
   new-bookkeeping capacity. Add no per-transfer source history, second proof
   cache, snapshot chain or population-sized queue. A bootstrap reader cannot
   replace or cancel another request's prepared acknowledgement candidate.
4. Reuse a complete proof only against its actually qualified immutable source
   pin, or through the existing closed, predecessor-checked native publication
   machinery against an actual successor pin. Validate every new receipt, audit
   entry and affected source evidence within the existing bounded page limits.
   Counters and publication identities may invalidate reuse; they cannot alone
   establish ancestry or source authority. Unknown or lost qualification requires
   fresh complete off-gate validation before releasing more bytes.
5. At every manifest/page release, check exact current lineage, write/source
   fencing, canonical hold bytes, hold identity, registered generation and phase,
   registration/audit authority, and the artifact's fixed bootstrap point against
   the qualified current source snapshot. A retired, missing, replaced, foreign,
   incompatible or unexplained hold refuses. Preserve every other current hold.
   Do not turn a bootstrap fence into an acknowledgement or infer receiver
   durability from an artifact, page count, certificate or completed transfer.
6. Preserve the existing exact artifact manifest, private directory/file identity,
   page framing, checksum and canonicality checks. Preserve fresh service
   authorization and current source checks before/after outbound work. A native
   prepared-read operation must recheck qualification and custody at its actual
   read; a stale preparation alone cannot release a page. Existing direct native
   reads remain fully validated unless they use this closed safe protocol.
7. Keep original request deadlines, cancellation custody, construction capacity,
   stream backpressure, retry rules, page/frame limits and the original workload.
   Bootstrap source reads remain foreground read work under those existing
   limits. Account and observe their actual proof work; cache reuse does not make
   work free. Preserve shared background/acknowledgement maintenance pacing and
   its remaining rest. Bootstrap reads create no extra source mutation budget,
   maintenance turn, writer priority or coordinator queue position.
8. Issue no source mutation, forced checkpoint, journal drain or reclamation
   license to prepare or release a bootstrap page. A source-read proof cannot
   renew an independently durable checkpoint ceiling or manufacture independent
   progress. Existing reclamation still rechecks its exact current floor, every
   hold, finite checkpoint license and bounded ordered admission.
9. Release nondurable request custody and unneeded pins on cancellation, failure
   and source-owner retirement. Preserve actual submitted-operation custody and
   uncertainty for existing control writes. A source proof never survives restart.
   Granted atomic turns retain their current completion protocol; retirement is
   irreversible and grants no later source turn.
10. Keep complete bootstrap hold registration and attachment validators, receiver
    construction and projection rebuild, startup/offline/engine integrity checks,
    candidate publication and recovery, archive and promotion protocols unchanged.
    Keep the 60-second process watchdog and every crash/frontier assertion. The
    final-source original suite and full CI must pass; a serialized diagnostic,
    smaller fixture or isolated pass does not close the failed suite.
11. Preserve all application, CLI, MCP, SDK and replication interfaces, durable
    encodings and compatibility fixtures. Native preparation/read operations are
    closed and cannot select weaker validation, release holds or mint proof.
    Diagnostics contain bounded numeric work counts, durations and closed refusal
    classes only. No new throughput or real-time guarantee is claimed.

## Options considered

1. **Raise the watchdog or shrink/serialize the campaign:** retains the scaling
   defect and cannot qualify the unchanged process campaign. Rejected.
2. **Trust the retained artifact, capability or root counter:** does not prove
   current source custody after retirement or policy change. Rejected.
3. **Remove receiver startup or publication checks:** changes a separate complete
   validation boundary without establishing a safe replacement. Excluded.
4. **Complete shared source proof with bounded checked reuse:** selected; preserve
   custody while eliminating the retained-population walk on each outbound page.

## Consequences

- Transfer cost need not multiply retained source population by page count.
- Cold or invalidated source authority still requires complete validation and
  can refuse or exceed an existing deadline. No source availability promise.
- Sharing a certificate requires explicit candidate custody and accounting;
  progress, memory, writer interference and cancellation need actual proofs.
- This decision alone closes no failed test and qualifies no rehearsal.

## Standing design tests

- **Interface safety:** applications and agents cannot bypass validation, select
  weaker durability, mutate source holds, invent acknowledgement or publish a
  partial follower. Only compiled writes and existing authorized surfaces remain.
- **Scale:** one existing shared proof owner, one candidate, two pins, compiled
  bookkeeping and native page/allocation limits. No retained-population scan per
  page after qualification and no new unbounded request or snapshot collection.

## Checks

- Count actual initial/continued rows over increasing populations and multiple
  pages; compare qualified history and exact custody with the independent full
  validator at each checked publication. Prove incomplete work releases no bytes.
- Reject stale, changed, missing, retired, corrupt, incompatible and unclassified
  source/hold evidence, qualification overflow and lost source custody.
- Deterministic schedules cover concurrent acknowledgement custody, cancellation,
  proof replacement and retirement without sleeps as correctness synchronization;
  assert the shared capacity and pin bounds and durable hold preservation.
- Preserve actual bootstrap construction/attachment/publication crash tests and
  prove cancellation cannot release unqualified bytes or lose durable fences.
- Run unchanged original process tests, scoped acceptance and clean frozen-source
  full CI on the required host. Then obtain unchanged preparation, independent
  reconciliation and the complete short rehearsal with all seven faults, actual
  retirement/resource accounting and six final quiet hours before 24-hour readiness.
