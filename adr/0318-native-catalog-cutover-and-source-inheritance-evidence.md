---
adr: "0318"
title: Native Catalog Cutover And Source Inheritance Evidence
status: accepted
tier: guarantee
date: 2026-10-16
accepted: "2026-10-16"
acceptance: 'maintainer, in session, 2026-10-16: "Accept ADR-0318 and its SPEC/WP-825 amendments as written"'
requires: [ADR-0186, ADR-0316, ADR-0317]
amends: [ADR-0186, ADR-0316, ADR-0317]
supersedes: []
requirements: [REP-002, REP-003, REP-005, REP-007, REC-001, REC-004, REC-005, STO-012, STO-020, PRJ-004]
packages: [WP-825]
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations: []
review_triggers:
  - Original history or administration bytes would acquire a new meaning.
  - Decoded or inherited evidence would confer unchecked current authority.
  - A required base would be dropped to fit a transition bound.
  - Audit allocation, exact preimages or crash reconciliation would change.
---
# ADR-0318: Native Catalog Cutover And Source Inheritance Evidence

## Context

ADR-0316 adds durable replay sources; ADR-0317 requires explicit catalog
activation and original-lineage preservation. Existing active history has one
catalog, and existing restore resets source-local history with an empty anchor.
Neither mechanism supplies a durable catalog cut or inherited-source proof.

Literal preservation of every source-only HISTORY row through restore or
bootstrap conflicts with ADR-0186's reset/exclusion. This decision resolves
that boundary explicitly and specifies the missing closed transition owners.
It adds no general multi-catalog engine or application-selected migration.

## Decision

1. Permit exactly one ordinary Active catalog-V2 to catalog-V3 cut, keeping
   database ID, history incarnation, leadership epoch, physical sequence/hash
   chain and application frontier. Preserve every predecessor HISTORY value
   and retained administration byte at commit; never relabel an old frame.
   Restore and promotion remain distinct new-incarnation operations.
2. Add two registered Audit records under existing canonical administration
   keys and shared sequence allocation: `StoredAuthoritativeCatalogTransitionV1`,
   family85/revision1, complete envelope <=2KiB; and
   `StoredSourceInheritanceAdministrationV1`, family86/revision1, complete
   envelope <=16KiB. Append roles after ADR-0317. Add no catalog namespace or
   metadata key. Preserve existing catalog digests, source schemas and encodings.
3. The catalog record has closed FreshV3 and V2ToV3 variants. Both bind database,
   assigned administration sequence, exact nonrecursive publication transaction
   binding, target V3 catalog/registry and initial current-incarnation Next(1).
   V2ToV3 also binds complete predecessor V2 history, exact original registry
   envelope <=256B and original Active admission. Unknown variants refuse.
   FreshV3 requires native proven-Empty custody, never missing-record inference.
4. Before upgrade require exclusive offline custody, complete predecessor
   validation, explicit ActiveV1, no unresolved fence/promotion and complete
   authoritative plan input. Require no live source holds, bootstrap/archive
   jobs or captured work. Existing authorized owners retire followers first.
   Missing plan input returns SourceUnavailable; live work returns typed drain
   refusal before mutation. A Fenced predecessor remains unchanged and fenced.
5. One closed offline coordinator owner allocates the next physical receipt and
   exactly one administration sequence. Explicitly qualify
   StorageFormatMigration18 for this sole catalog cut. Its sorted authoritative
   mutations insert the fact, advance administration, replace exact registry
   and initially install the source allocator at Next(1). An existing allocator
   refuses; this cut neither allocates an ordinal nor resets a same-incarnation
   counter. Current catalog, ActiveAdmissionV2, history and physical allocator
   change atomically as dedicated control roots in the same hardened commit.
6. Cut receipt predecessor/hash/frontier equal the original tail. Current V3
   anchor and minimum_resume become that cut point; initial tail equals it.
   A private validated two-case context checks retained predecessor receipts
   with original V2 rules and current receipts with V3 rules. No public digest
   map, caller callback or generic multi-catalog history API is introduced.
   Existing fixed-lineage wire formats and validators retain their meanings.
7. The cut record additionally binds a domain-separated streaming commitment
   over the retained V1 replication/fence/promotion control facts through the
   predecessor administration head: exact canonical keys/values, lengths and
   final count, original database/catalog and head. Use SHA-256 domain
   `riffdb/native-catalog-legacy-controls/v1` plus zero byte, then database16B,
   incarnation/epoch/admin-head as BEu64 (absent head=0), catalog32B; each
   sorted key9B, BEu64 value length and value; final BEu64 count and sum of
   key/value lengths. Full original validation
   precedes this attestation. Original facts must name that database and fixed
   V2 catalog; foreign, malformed or invalid original facts refuse. Exclude reclaimable
   command-linked service audits. This explicitly preserves legacy historical
   interpretation without inventing unavailable old restore links. It grants
   no current admission, fence, promotion or checkpoint acquisition authority.
8. Full scrub validates every retained old HISTORY row under captured V2
   context, including existing stub resolution. Remaining predecessor rows
   form a contiguous suffix through the committed old tail, or are wholly
   reclaimed. Check the first surviving predecessor binding and every link
   through that pinned tail; never trust a caller-supplied suffix boundary.
   Cleanup uses the existing drained/checkpoint retention owner, at most256
   rows/step, only after durable cut/checkpoint and all required fences permit.
   Delete exact rows; preserve survivors, audit facts and extent references.
   Existing command/source retention protections remain independently required.
9. Current V3 source cursors, registrations, acknowledgements and requests bind
   the complete V3 lineage. Pre-cut resume requires bootstrap; fabricated V3
   positions on old rows refuse. Bootstrap fences are cut-or-later and transfer
   authoritative Audit facts with current state. Never emit old V2 rows as V3
   frames or silently convert live V2 holds. Validated retired source-only
   policies may be removed atomically; their original audit facts remain.
10. Fresh V3 initialization inserts its FreshV3 fact and initial allocator in
    the V3Activation24 transaction, attributing every authoritative stamp it
    changes. The current anchor's administration frontier directly locates the
    origin fact. Normal history reclamation/rotation preserves this anchor.
    A later new-incarnation witness directly references the original fact by
    administration sequence and canonical digest; no unbounded discovery walk.
11. Restore and promotion use a fields-private validated inheritance owner.
    Its Audit fact binds closed Restore/Promotion cause, exact predecessor
    history, successor lineage and nonrecursive anchor transaction binding,
    original catalog-origin reference, fixed32B commitment to the authorized
    immutable backup/attempt selection (not its complete external receipt),
    and sorted unique (full source locator, descriptor digest, origin commitment)
    witnesses. Locator bytes are database16B then allocation-incarnation and
    ordinal as BEu64; both digests are32B.
    Verify actual descriptor identity and complete source closure;
    a decoded fact or older incarnation number alone supplies no authority.
12. NEW transition admission limit: <=128 distinct inherited witnesses, each
    fixed-width. Include every selected root and every valid generation base
    that must survive. Release conflicting captured/retired operational engine
    and source custody through its actual owner; retain the offline proof pins
    until commit. Promotion preserves valid published generations/bases;
    restore retains existing sidecar removal and local rebuild. Retire
    or rebuild only under existing complete-closure rules. If required references
    do not fit, refuse before publication with predecessor intact. Never silently
    drop a valid base to fit. Admit all witness/codec capacity to the actual
    engine's source ledger before allocation; payload <=15KiB, envelope <=16KiB.
13. The fact retains exact predecessor scalar envelopes needed to reconstruct
    the anchor's expected-state hashes: source allocator <=160B, incarnation
    <=128B, registry <=256B and optional watermark <=256B. Explicit absence is
    permitted only by the selected recipe. Compare each with the transaction's
    actual preimage; derive canonical successor bytes. The fact contains neither
    its own digest nor successor receipt hash: encode fact, reconstruct exact
    sorted anchor mutations, then derive/check the anchor hash without recursion.
14. V3 RestoreAnchor30 atomically inserts inheritance fact, advances its one
    administration sequence, installs new-incarnation Next(1) and attributes
    every authoritative incarnation/registry/watermark stamp actually changed.
    Existing backup selection, fencing restrictions, custody, publication and
    epoch rules remain. Same-incarnation retry reconciles the exact committed
    fact/postimage and allocates nothing. Legacy V2 empty anchors stay unchanged.
15. V3 promotion adds inheritance immediately before its existing consecutive
    start/control/success records in the same Promotion29 commit. Allocate four
    administration items from the frozen applied frontier; retain exact typed
    result and service Success links to the shifted actual start/control rows.
    Use checked V3 construction/reconstruction, never forge V1 assignments.
    Existing V1/V2-catalog three-item bytes remain unchanged. PromotionV2 keeps
    its 8KiB payload; the separate inheritance record carries the witness.
    Authenticated fencing, immutable attempt, RPO, exhaustion, offline phases,
    external receipt framing/caps and terminal reconciliation remain required.
16. Cold validation reconstructs exact origin/anchor evidence even after its
    original receipt was pruned. A current inherited acquisition requires
    witness membership plus its current root/base/pin and exact object bytes.
    Historical witnesses never pin payloads or resurrect removed objects.
    Unselected superseded or unfinished copied objects grant no acquisition;
    verify they have no required surviving references before bounded abort/GC.
    Candidates cannot resume across incarnation. Preserve source IDs/digests.
    Invalidate copied candidate markers through their checked local generation
    owner, or refuse before publication; sidecar controls stay local.
    Authoritative source-object abort/GC remains coordinator-attributed.
17. Amend ADR-0317 Decision13 explicitly: ordinary upgrade preserves predecessor
    source HISTORY bytes under original catalog until fenced reclamation.
    Destructive restore/promotion and bootstrap retain ADR-0186's SourceOnly
    history reset/exclusion. Preserve original retained administration bindings,
    source identities/origins and the attributed durable origin/inheritance
    facts transferred with authoritative state. No relabeling, cache authority
    or requirement for already-pruned original publication frames is introduced.
    All complete source-closure checks remain; reset is not permission to prune.
18. Both new records use existing bounded canonical schema/preflight checks and
    one storage-API semantic owner. Source7, V1/V2 catalogs, admission/fence V1,
    legacy promotion/restore and external maintenance bytes stay unchanged.
    Only the new V3 promotion construction gets the four-item semantics.
    No new attribution tag, hash-chain format, generic mutation callback,
    dependency, unsafe code, timer or public bypass is permitted.
19. Crash/retry selects exact predecessor or exact committed successor, including
    catalog, registry, admission, audit, history and allocators. It never mixes
    roots, repeats an administration/ordinal allocation or infers Active.
    Keep ADR-0316 budgets/step limits, all existing query/write/scan ceilings,
    maintenance pacing and 45/60-second readiness/recovery gates unchanged.
20. Separate concrete human fixture review, legacy compatibility, source/follower
    startup and scrub, upgrade/restore/promotion/bootstrap and process-crash
    proofs must pass on integrated source before V3 activation or qualification.

## Options considered

- Frozen history segments: unnecessary tables, hydration and retention machinery.
- New incarnation for ordinary upgrade: unnecessary reset and ancestry work.
- One chain cut and separate bounded inheritance facts: chosen; preserves original
  context and keeps upgrade distinct from destructive recovery.

## Consequences

Two new Audit roles and an explicit V3 promotion sequence are required. The
new128-reference transition limit can refuse an over-capacity maintenance
operation safely. Legacy historical attestation confers no present authority.
The SourceOnly preservation ambiguity is resolved explicitly, not by weakening
closure, fencing, exact attribution or the rehearsal gates.

## Standing design tests

- Interface safety: applications gain no source selection, arbitrary mutation,
  unsafe migration or guarantee waiver. Only existing authorized offline owners
  produce checked transition facts; serialization grants no authority.
- Scale: fixed records and <=128 admitted witnesses; streaming old-prefix and
  control-fact validation; constant origin lookup; <=256-row cleanup steps.
  No population-sized memory, generic ancestry graph or permanent history pin.

## Checks

- Real legacy fixture upgrade; unchanged old history/admin bytes and counter retry.
- Foreign/missing/corrupt facts, live holds, incomplete plans and counter exhaustion.
- Cut equality/one-before/one-after; original V2 scrub; V3 pre-cut stream refusal.
- Pruned anchor reconstruction with exact scalar preimages and substituted-byte refusal.
- Witness128/129, ordering, duplicates, budget pressure and no payload resurrection.
- Exact V3 promotion four-item result/audit links and frozen legacy three-item vectors.
- Real bootstrap/restore/promotion inheritance, cancellation and process-kill windows.
- Scoped acceptance, fixture review and full C3D CI before activation/rehearsal.
