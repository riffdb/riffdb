---
adr: "0317"
title: Explicit Catalog Binding For Primary Fencing And Promotion
status: accepted
tier: guarantee
date: 2026-10-16
accepted: "2026-10-16"
acceptance: 'maintainer, in session, 2026-10-16: "Accept ADR-0317 and its SPEC/WP-825 amendments as written"'
requires: [ADR-0019, ADR-0072, ADR-0093, ADR-0178, ADR-0186, ADR-0260, ADR-0310, ADR-0316]
amends: [ADR-0019, ADR-0178, ADR-0186, ADR-0260, ADR-0310, ADR-0316]
supersedes: []
requirements: [REP-002, REP-003, REP-005, REP-007, REC-001, REC-004, REC-005, STO-012]
packages: [WP-825]
obligations: []
review_triggers:
  - An old fence, admission, promotion, catalog or receipt would be reinterpreted.
  - Missing admission or configuration would infer new source authority.
  - Catalog selection would replace authentication, ancestry or current authorization.
  - A fenced source would become writable through upgrade, restore or recovery.
  - Source authority would survive a retained-fence scrub failure.
---
# ADR-0317: Explicit Catalog Binding For Primary Fencing And Promotion

## Context

ADR-0316 adds authoritative replay-source domains and a successor catalog.
The current admission and fence durable V1 messages omit the catalog digest;
their constructors and decoders reconstruct catalog V2. Promotion V1 embeds
that fence and compares it with exact source-history lineage. Merely widening
catalog checks cannot represent V3 faithfully and can skip existing fencing.

SPEC 13.5 preserves old record meaning, forbids inferred admission and requires
reviewed explicit upgrades. Server follower configuration and authenticated
fence requests also independently select V2. This decision binds every new
path to one exact catalog while preserving existing fencing and V1 bytes.

## Decision

1. Preserve catalog V1/V2 declarations and all revision-1 identities, schemas,
   encodings, decoders, constructors and vectors. Admission/fence/promotion V1
   continue to mean catalog V2. Never decode them using a surrounding catalog
   as an omitted field, relabel an old receipt or make a V1 encoder emit V3.
2. Add `ReplicationPrimaryAdmissionV2`, family 76/revision 2;
   `StoredPrimaryFenceAdministrationV2`, family 77/revision 2; and
   `StoredPromotionAdministrationV2`, family 78/revision 2. These are distinct
   registered durable messages for catalog V3, not modifications to V1.
   Admission V2 Active adds required 32-byte `catalog_digest` at field 4.
   Fence V2 adds required 32-byte `catalog_digest` at field 10 and retains
   every existing fence fact. Admission V2 Fenced embeds that exact fence V2.
   Promotion V2 embeds fence V2 at field 8, retains exact source history and
   adds required 32-byte `published_catalog_digest` at field 16.
3. V2 constructors/codecs require the closed catalog V3 digest. Database,
   incarnation, epoch, catalog, target, registration generation, application
   frontier, fence, retained administration and source-history bindings must
   agree under their existing validators. Unknown catalogs, absent/wrong-width
   digests and contradictory nested bindings refuse. Deserialization is
   evidence reconstruction, never source, mutation or promotion authority.
4. Keep one storage-API-owned semantic validation model with version-specific
   checked construction and encoding. Other crates may not copy these
   semantics. Record dispatch selects the exact outer family/revision; a
   checksum, schema, canonicality, shape or semantic error cannot trigger a
   fallback to another version. Preserve strict old-format refusal.
5. Catalog V3 source mode requires explicit V2 admission before a writer or
   readiness is exposed. Missing admission is corruption. Attached followers
   carry no source-only admission. Current authorization, admission stop,
   coordinator drain, atomic fence/admission/V3 receipt, unchanged application
   head, original retry facts and typed `primary_fenced` behavior all remain.
   No unfence, automatic promotion or caller-provided proof is introduced.
6. Extend ADR-0260's bounded dirty source path to V3 under the same validated
   prefix/current-registry preconditions. Before readiness, point-validate
   exact V3 roots, admission, and any referenced fence and reconstructed
   receipt. V3 follows V2's bounded-clean certificate decline/consumption and
   bounded-dirty or complete routing, including complete first-open validation.
   Exhaustive retained-fence checks remain in background/offline scrub. A
   retained-history/fence failure withdraws authoritative readiness and records
   the incident; it is never converted to serving with degraded authority.
7. Add optional closed `catalog_version` to follower-source configuration.
   Absence preserves the historical V2 meaning; explicit values are only 2
   and 3, mapping to their fixed declaration digests. Unknown values refuse.
   Helm's matching optional `catalogVersion` renders that same selection and
   validates the same domain. No arbitrary digest, trust bypass or automatic
   downgrade is available; legacy configuration does not silently select V3.
8. Bind the configured peer to its complete resolved source lineage and hold.
   Before receiver/promotion use, compare them with validated local bootstrap
   or attached state. Every stream/fence request and authenticated response
   must match that pin. Registration generation comes from exact validated
   registration/selection, not a new configuration override. Existing TLS,
   capability checks, retained ancestry and terminal stream boundary remain.
   Configuration and decoded fence bytes alone never create private peer proof.
9. Promotion preserves exact authenticated fence and drained applied position,
   checked application RPO, exclusive offline custody, audited attempt phases,
   checked successor incarnation/epoch and exact committed reconciliation.
   V3 writes promotion V2 and Active admission V2 for the exact new lineage.
   Source allocator reset required by ADR-0316 is an actual attributed atomic
   mutation, never unreceipted normalization. Counter exhaustion refuses.
10. Existing bounded external promotion-receipt framing stays unchanged: its
    independently versioned embedded fence record dispatches to its exact V1
    or V2 codec. Preserve old embedded bytes and all immutable attempt choices.
    The new record registry/catalog markers make incompatible old binaries
    refuse before source open, cutover or target replacement. No outer receipt
    checksum, phase, count, authorization or reconciliation check is removed.
11. Activate catalog V3 only through the reviewed explicit offline predecessor
    upgrade or validated backup/restore/bootstrap path. Require current
    authority, complete predecessor validation and ADR-0316's complete source
    closure for each active plan. Publish the new catalog, record registry,
    source-mode admission when required (attached bootstrap transfers none),
    and required allocator state atomically through their existing
    owner and attributed transition; no open-time inferred Active or repair.
12. Ordinary source upgrade is restricted to validated Active V2 predecessors
    with no unresolved fence/promotion. Fenced predecessors remain fenced and
    cannot upgrade to Active; retain their old catalog/receipts and refuse this
    upgrade. Existing old-format recovery/promotion remains available under its
    own protocol. Missing complete plan input returns SourceUnavailable before
    activation; a derived cache cannot certify source closure. Restore preserves
    existing fencing restrictions and inherited source identities.
13. Preserve original retained V1 administration and history bytes through any
    supported transition. Historical validation preserves each retained fact's
    original frame/history/administration lineage and verified transition
    evidence; it never relabels old bytes as current lineage or requires already
    pruned original publication frames. New authority must prove the explicit
    transition and complete closure. Crash reconciliation selects exact old or new committed state,
    never a mixture or a second allocation/counter increment.
14. Keep payload ceilings at 1,024 bytes for admission, 1,000 for fence and
    8,192 for promotion, with existing bounded principals, approvals, 16 attempt
    steps, 1,536-byte complete fence envelopes and 8,192-byte external receipts.
    New schema/preflight bounds are generated and checked before decoding
    allocation. Preserve all command, scan,
    memory, maintenance pacing, readiness and 45/60-second recovery gates.
    This decision supplies no performance qualification or extra retry.
15. Register the three additive roles after ADR-0316's seven source roles,
    preserving every existing registry row and frozen writer-4 fixture.
    Generated schema hashes, bounds, wire vectors and compatibility inventories
    require separate concrete human fixture review before merge. Catalog V3
    cannot activate until mixed-version, refusal, startup, transport, upgrade,
    restore, promotion and process-crash proofs pass on the integrated source.

## Options considered

- Reconstruct V3 from surrounding state while decoding V1: rejected because
  identical durable bytes would acquire different lineage and authority.
- Skip fence checks for the new catalog: rejected because catalog growth cannot
  make a fenced database writable or supply promotion authority.
- Add explicit successor records and retain closed legacy behavior: chosen.
  This costs three codecs and compatibility proofs while preserving guarantees.

## Consequences

SPEC 13.5's primary-fencing amendment permits these explicit V3 successors and
the narrowly defined offline Active predecessor upgrade. ADR-0019/0178/0186
inherit that amendment; all other fencing and promotion requirements remain.
ADR-0260 includes V3 point checks and source-authority withdrawal. ADR-0310
adds the closed optional configuration selection. ADR-0316's catalog upgrade
and incarnation transitions must carry these exact admission/fence bindings.
Already-fenced predecessor stores are deliberately ineligible for this upgrade.

## Standing design tests

- Interface safety: applications gain no write, promotion, source selection,
  weaker validation or unsafe operation. The operator's closed catalog version
  cannot override authenticated lineage, persisted admission or authorization.
- Scale: readiness uses bounded point reads; exhaustive checks stream in the
  existing scrub. Three fixed registry roles and one closed configuration value
  add no population-sized memory, unbounded queue, timer or external service.

## Checks

- Frozen V1 vectors, constructors and genuine writer-4 database/backup restore.
- V2 canonical round trips, exact bounds/one-over, missing/foreign digest,
  nested mismatch, schema/checksum corruption and no version fallback.
- V3 Active/Fenced startup; corrupt referenced receipt refuses before readiness;
  stray retained fence withdraws authority on scrub and fails offline scrub.
- Legacy/default and explicit configuration, Helm validation/rendering, peer
  lineage/hold/generation mismatch and terminal denial/extra item refusal.
- Exact authenticated promotion, application RPO, exhaustion, uncertain retries,
  inherited source IDs and attributed incarnation allocator reset.
- Process kills at upgrade/cutover/restore publication and reconciliation;
  Fenced or incomplete predecessor refuses with original bytes unchanged.
- Scoped acceptance and full CI on C3D; existing workload gates unchanged.
