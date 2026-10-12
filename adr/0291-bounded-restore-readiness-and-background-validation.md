---
adr: "0291"
title: Bounded Restore Readiness And Background Validation
status: accepted
tier: guarantee
date: 2026-10-12
accepted: '2026-10-12'
acceptance: 'maintainer, in session, 2026-10-12: i approve, in response to the joint exact-text review of ADR-0290, ADR-0291 and ADR-0292 in WP-825-RESTORE-RECOVERY-REVIEW.md; proposed text SHA-256 37f5acc10edc1e4adbd601f6777182ca3ed7a851ba94c70197e733bfcefc680d'
requires: [ADR-0050, ADR-0182, ADR-0203, ADR-0269, ADR-0289, ADR-0290]
amends: [ADR-0050, ADR-0289]
supersedes: []
requirements: [REC-001, REC-004, REC-005, REC-006, REC-007, REC-008, END-001]
packages: [WP-825]
obligations: []
review_triggers:
  - Restore publishes without full artifact verification, bounded roots or staged authorization.
  - Corruption becomes absence, a business outcome, partial output or automatic repair.
  - Offline scrub, archive replay, format upgrade or another complete validator is weakened.
  - A durable receipt, certificate, public selector or acknowledgement meaning changes beyond this decision.
---
# ADR-0291: Bounded Restore Readiness And Background Validation

## Context

The populated ordinary restore takes 162.7 seconds. An independent profile
attributes about 34 seconds each to private staged validation and a second
complete population walk after publication. Repeated full hashing and copying
consume most of the remaining time. Eliminating duplicate work alone therefore
does not establish the required recovery budget or scale with retained data.

ADR-0182/ADR-0203 separate ordinary readiness from exhaustive verification:
bounded roots and journal recovery precede readiness, first access validates
rows, and a background scrub examines the population. ADR-0050 and ADR-0289
instead require complete staged restore validation. Extending the ordinary
trust model to restore changes that guarantee and needs exact acceptance.

## Decision

1. Ordinary self-contained backup restore uses bounded readiness rather than a
   pre-readiness structural/catalog population walk. Before publication, verify
   the complete immutable manifest and every artifact hash; preserve copying,
   native exclusion, format/registry compatibility, complete journal recovery,
   required history hydration and repair refusal. Retain the nonrepairing
   physical-engine integrity check on the private stage. Hash equality is not
   reported as a semantic validation result.
2. Validate the same bounded roots and active catalog required by ordinary dirty
   startup on the private staged state. Validate database/session identity,
   retained metadata, bootstrap lifecycle, allocators and readable digest
   inventories before constructing an immutable authorization reader. Do not
   manufacture a checkpoint or CLEAN certificate to make this path eligible.
   No population-sized count, locator index or exact-end stream is constructed
   for this readiness proof. Unsupported or contradictory roots fail closed.
3. Authenticate the retained bearer freshly against that staged reader with the
   existing authorization layer and current clocks. Preserve current-source and
   staged authorization as independent checks. Release every principal, bearer,
   proof and read pin before controlled mutation and publication.
4. Keep the receipt-authorized incarnation choice, transaction-current stamp,
   retention-watermark rebind, RestoreAnchor and replication reset in their
   existing Immediate two-phase transaction. Revalidate the affected bounded
   roots and the existing journal/format bindings after that transition.
   Preserve ADR-0290's full content seals at mutation/publication boundaries.
5. Preserve immutable backup revalidation, copy verification, target exclusion,
   marker-last publication, directory synchronization, rollback and uncertainty.
   After publication, reopen through the same bounded startup proof, check the
   receipt's database/incarnation/frontier binding, durably consume the dirty
   generation, then complete the receipt and activate ordinary service.
   No transfer of an old authentication decision grants readiness.
6. An ordinary restore success proves verified artifact materialization,
   authorized publication and bounded recoverability; it no longer asserts a
   complete pre-publication semantic scrub. Existing receipt encodings and
   identities remain unchanged. Update operational documentation explicitly;
   an old successful receipt is not evidence of a new background scrub result.
   Full-assurance operators retain the existing authorized offline scrub.
7. Every operational row access retains REC-006's envelope, checksum, canonical
   encoding, physical-key identity, schema, bounds and locally required
   reciprocal checks before the row influences any operation. Corruption fails
   closed with an incident; it is never absence, a business result, partial
   output, skipped work or repaired state. Affected commands cannot acknowledge
   success. This applies to authorization, queries, delivery and derived work.
8. Start exactly the existing bounded-rate semantic background scrub after
   readiness. Preserve REC-007 and ADR-0203's closed health transitions and
   incident behavior. Failure degrades authoritative storage while affected
   rows continue to fail closed; it does not retroactively claim clean data.
   This scrub does not certify every physical page. Explicit offline scrub
   retains both complete physical and complete semantic verification.
9. Restart discards all private process proofs and follows the existing durable
   receipt reconciliation and bounded startup rules. Preserve receipt authority,
   exactly-once incarnation application, original outcomes/provenance and
   idempotent uncertainty resolution at every crash cut.
10. Scope is ordinary self-contained restore only. Archive replay, follower
    promotion/bootstrap, format upgrades and explicit scrub retain their current
    complete validation requirements. Add no public skip/force/readiness option,
    new durable certificate, dependency or resource-ceiling increase. Preserve
    application guarantees and all workload thresholds; clock changes require
    their own accepted record.

## Options considered

1. **Reuse one complete walk across publication:** removes duplication but keeps
   population size on the readiness path; superseded as the selected direction.
2. **Write a restore CLEAN certificate:** adds a durable trust mechanism and
   does not remove the initial full walk; rejected.
3. **Use ordinary bounded readiness and first-access/background validation:**
   selected, with verified artifacts and private physical verification retained.

## Consequences

- Latent semantic corruption can be discovered after ordinary restore success,
  matching ordinary dirty startup instead of the former restore assurance.
- Copying, hashing, physical checking and required journal/history recovery
  remain byte/suffix dependent. No fixed 25-second recovery claim follows.
- Background-scrub interference and corruption behavior must be measured under
  the continuing workload; complete assurance remains an explicit offline action.

## Standing design tests

- **Interface safety:** the engine selects one safe ordinary recovery policy;
  applications cannot bypass authorization, request repair or suppress checks.
- **Scale:** readiness has no population-proportional semantic structure or
  enumeration. Preserve the existing recovery heap, buffers, caches and workers.

## Checks

- Ordinary restore reaches first successful write without a population walk;
  counters prove no hidden table count, exact-end stream or full reopen walk.
- Valid-hash backups with a corrupt non-root row become ready, but every affected
  first access fails closed and background scrub reports the existing incident.
- Corrupt roots, hashes, physical trees, journals, formats and repair requests
  refuse publication; expired/staged-denied credentials retain current refusal.
- Process crashes across stamp, publication, receipt and dirty consumption retain
  exact outcomes, original acknowledgement replay and history reconciliation.
- Explicit scrub and other complete-validation call sites retain their full
  oracles. Run the focused restored-fixture gate first, then scoped acceptance,
  full CI, handbook checks and continuing-load qualification.
