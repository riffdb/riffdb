---
adr: "0285"
title: Owned Private Restore Validation Custody
status: accepted
tier: guarantee
date: 2026-10-10
accepted: '2026-10-11'
acceptance: 'maintainer, in session, 2026-10-11: I accept, in response to the exact-text review request; proposed text SHA-256 03b5dde4c68bca8d53c827bbe285deb2e22f0e30534ce88e3530b3a52193b371'
requires: [ADR-0050, ADR-0203, ADR-0263, ADR-0269, ADR-0276]
amends: [ADR-0050, ADR-0269]
supersedes: []
requirements: [REC-001, REC-006, END-001]
packages: [WP-825]
obligations: []
review_triggers:
  - An engine owner, file-lock protocol, private validation boundary or publication custody changes.
  - A complete validator or existing startup proof join is omitted, sampled or replaced by a certificate.
  - A proof crosses uncertain byte identity, an unclassified mutation, an operation or a process restart.
  - An application surface, durable identity, format, dependency, ordering, durability policy, resource ceiling or workload gate changes.
---
# ADR-0285: Owned Private Restore Validation Custody

## Context

The populated native restore materializes a private stage, hydrates its retained
history, and makes another dense private copy for complete verification. The
corrected component takes 100.1 seconds on the required 8-vCPU, 32-GiB Linux SSD
host; the earlier nested profile assigns 15.4 seconds to verification-copy
capture. Neither measurement qualifies the unchanged 60-second whole public
recovery action. Native regressions also show that final sealing can silently
repair a validated stage, and incarnation stamping can replace a changed seal.
Those repair-refusal corrections are necessary independently of performance.

ADR-0269 protects an authoritative source by checking a private copy. A restore
stage is already an unpublished copy of a verified immutable backup. Treating
it as an ordinary source again introduces writable startup and repeated engine
ownership transitions before publication. A storage-owned private lifecycle can
retain complete validation authority and refuse repair at every such boundary.
This changes ADR-0269's file custody and ADR-0050's staged startup construction;
it therefore requires exact human acceptance rather than an implementation
package deviation.

## Decision

1. Only native restore materialization may construct an unforgeable private
   stage owner, after verifying the complete immutable backup manifest and
   every artifact, retaining exact file and directory identities, and copying
   into the existing unpublished private stage. A pathname, receipt, caller
   assertion or copied CLEAN certificate cannot construct this owner. Ordinary
   public offline scrub retains its authoritative-source/private-copy behavior.
2. The private stage itself is the single owned verification artifact. Do not
   make a second dense verification copy of it. Before any engine open, retain
   native exclusive file custody. Configure every private engine open, including
   hydration, validation, authorization and incarnation stamping, to abort a
   repair request. A callback, failed integrity check, corruption, changed
   identity or uncertain custody refuses success and preserves the immutable
   backup; a repaired stage never gains publication authority.
3. After the existing bounded durable history hydration, run the complete
   physical-engine integrity pass, native journal/extent verification and
   complete structural and catalog-semantic validators on that owned stage.
   Both evidence streams must reach their exact ends with zero findings at the
   same authoritative frontier. Preserve every current row, envelope, schema,
   checksum, canonicality, allocator, linkage and retained-history predicate.
   CLEAN, checkpoint, bounded dirty-startup and sampled shortcuts remain barred.
4. Retain a move-only native validation proof tied to the exact engine owner,
   immutable read pin, frontier and file identities. At a closed-engine handoff,
   bind it to the exact artifact checksums. Retain it only while continuous
   exclusive custody and unchanged validated bytes are proven. Uncertain
   ownership, changed bytes or an unclassified mutation invalidates the proof
   and requires rebuilding and complete validation from the immutable backup.
   Do not persist, cache across operations or reconstruct this proof on restart.
5. Preserve every existing ordinary startup proof join before staged
   authorization: matching storage/catalog database and session identities,
   active catalog, retained metadata, bootstrap lifecycle and allocator state.
   Keep existing required native preparation and compatibility transformations
   under the private owner, and repeat complete validation after such a change.
   Then consume the complete native proof to perform the same joins and obtain
   the authorization reader, without activating operational source writers,
   readiness, checkpoint publication or workers for that read. Add no
   initialization, migration or compatibility override. A previously supported
   input cannot become refused merely because custody changed. The private
   proof grants no source readiness or operational write port.
6. Authenticate the retained bearer freshly against one immutable owned staged
   snapshot, using the existing authorization layer and current readable digest
   inventories. Preserve current and staged authorization as independent checks;
   carry no current-source principal or decision into the staged check. Release
   every bearer and read pin before the controlled publication transition.
7. Before incarnation stamping, verify the existing primary, journal and marker
   seals under native custody. Preserve the existing durable receipt authority,
   incarnation choice, transaction-current history validation, restore-anchor
   construction and replication-state reset. The existing immediate two-phase
   transaction remains unchanged. Only that controlled metadata transition may
   replace the seal; a fresh checksum never launders an earlier changed artifact.
   Complete engine close and the final byte seal before target publication.
8. Keep the existing source-bound and target-bound publication, immutable backup
   revalidation, copy verification, target exclusion, marker-last rename,
   directory synchronization, rollback and receipt uncertainty behavior. Run the
   existing fresh complete post-publication startup before readiness. On crash
   resume, discard incomplete private proof and stage state, rebuild from the
   immutable backup, and reapply the receipt's existing incarnation idempotently.
9. Retain existing bounded copying, disk admission, caches, page sizes, worker
   count, allocations, waits and diagnostics. Keep at most one private
   verification artifact and one validation pin. Cancellation releases all
   nondurable capabilities and publishes no success. Change no command ordering,
   commit profile, transaction grouping, synchronization boundary, durable
   encoding, wire field, dependency or supported target. Add no public selector.
10. Qualify this implementation only after the custody, repair, corruption,
    cancellation, compatibility and process-crash checks pass, followed by final
    combined-source full CI and the unchanged public rehearsal. The original
    whole-action recovery deadline, normal-window latency, workload rates,
    resource limits and all seven fault paths remain mandatory. Reducing a copy
    or passing a component measurement does not qualify any of those gates.

## Options considered

1. **Retain repeated copies and ordinary writable staged startup:** the narrow
   repair guards remain useful, but this keeps an avoidable full artifact copy
   and makes ownership harder to audit across private opens.
2. **Owned private validation and read-only authorization:** selected; complete
   checks remain, and their authority has one explicit native custody lifetime.
3. **Skip physical or logical checks, reuse CLEAN, or relax the recovery gate:**
   rejected; none supplies the accepted proof for the same complete artifact.

## Consequences

- Native private-stage ownership and proof plumbing change; public application
  behavior, durable formats and genuine compatibility fixtures remain intact.
- A complete walk and history hydration still scale with retained data. This is
  a deliberate reversible single-node POC limit, not a billion-row design.
- Copy savings and the final whole-action timing must be measured. The current
  100.1-second failure remains evidence until the complete public action passes.

## Standing design tests

- **Interface safety:** an application developer or agent cannot select private
  validation, supply a proof, request repair, skip a check or publish a stage.
  Only first-party Rust native owners construct and consume these capabilities;
  existing compiled writes and shared authorization remain the public boundary.
- **Scale:** ownership, pins, copying, queues and worker input retain their fixed
  bounds. No decoded population cache, second private verification artifact,
  snapshot chain, additional writer or additional worker pool is introduced.

## Checks

- Native healthy and repaired-stage oracles, including repair before final seal,
  changed primary/journal/marker bytes, repeated stamping and preserved originals.
- Exact complete-validator parity, retained metadata/catalog/bootstrap joins,
  digest inventories, expired and staged-only credentials, and no writer/readiness
  capability from a private proof.
- Cross-process exclusion and deterministic ownership-transfer, cancellation,
  replacement, missing identity, corruption and uncertain-custody schedules.
- Existing genuine backup/upgrade fixtures and receipt/stamp/publication/restart
  crash cuts, with original outcome, idempotency and provenance reconciliation.
- Scoped acceptance, full CI, four-language conformance, frozen ordinary release
  binaries, and actual required-host public whole-action and short-campaign gates.
