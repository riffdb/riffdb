---
adr: "0301"
title: Read Only Source Byte Accounting Independent Of Deletion Proofs
status: accepted
tier: guarantee
date: 2026-10-14
accepted: '2026-10-14'
acceptance: 'maintainer, in session, 2026-10-14: Accepted. Bring in main one more time before starting the rehearsal; proposed text SHA-256 8ac20a237f9a12628cbd1abf6466f51571238c51aed441599f1d77f8b88bfc36'
requires: [ADR-0265, ADR-0268, ADR-0279]
amends: [ADR-0279]
supersedes: []
requirements: [STO-030, STO-031, STO-032, REC-001, END-007, END-008]
packages: [WP-825]
obligations:
  - id: OBL-0301-1
    package: WP-825
    proof: source_accounting_can_observe_a_checkpointed_prefix_before_reclamation_proof_finishes
    says: Actual checkpoint coverage and canonical byte validation can produce read-only accounting while the complete deletion proof is unfinished; no deletion is authorized.
  - id: OBL-0301-2
    package: WP-825
    proof: source_accounting_refuses_changed_or_incomplete_floor_evidence
    says: Current holds, source lineage, checkpoint coverage, qualification and exact canonical bytes remain mandatory; unavailable evidence never becomes zero or a default.
  - id: OBL-0301-3
    package: WP-825
    proof: source_accounting_preserves_source_rest_and_two_validation_pins
    says: Accounting can capture during unchanged mutation-preparation rest without advancing mutation proof, granting a writer turn or exceeding two validation snapshot pins.
review_triggers:
  - Accounting evidence is accepted as a deletion license, source-validation proof or database health certificate.
  - Current hold, canonical, checksum, source-chain or exact-end checks are removed from accounting.
  - Complete source/audit validation, writer admission or mutation pacing is weakened.
  - Capture cadence, freshness, observation gaps, resource bounds or rehearsal gates change.
---
# ADR-0301: Read Only Source Byte Accounting Independent Of Deletion Proofs

## Context

The fixed-schedule diagnostic at `6f35ed7e` stops near second 2,900 on exact
source-byte observation freshness before reaching restore. A copied 275,313-row
interval completes in 23.63 seconds without load but expires at 30 seconds under
public 100-command/400-read load. Removing repeated conversions and generating
catalog lookup from its sole declaration brings that loaded scan to 28.00
seconds. Those component results do not qualify native-worker progress or service
levels; the diagnostic still reports latency overflow.

The corrected native server, on the same closed population and public load,
emits no source-byte observations within the unchanged 95-second limit. At
shutdown, initial source validation has checked 71,168 rows, with no invalidations
or admission refusals. The implementation waits for the complete, paced source
and audit proof before recording its first checkpoint observation or capturing
byte accounting. A read-only census consequently inherits deletion-preparation
latency. ADR-0279's qualification boundary needs an explicit separation; merely
relaxing a deadline or reporting an incomplete interval would hide the failure.

## Decision

1. Amend ADR-0279 decision 6 for read-only source-byte accounting readiness.
   Permit its complete canonical interval census to run before the complete
   reclamation source/audit proof finishes. Accounting establishes exact bytes
   and the native eligibility floor; it establishes neither deletion authority
   nor a claim that the whole database has passed its integrity scrub.
2. Establish accounting checkpoint coverage from actual native immutable
   checkpoints and independently committed progress, using the same earlier
   observed point, later durable coverage, lineage, anchor and closed native
   publication classification as the existing finite-ceiling rule. Keep this
   evidence in a distinct private accounting type. It must not initialize,
   advance, renew or substitute for the deletion owner's observation, license,
   source/audit certificate or independent-progress counter.
3. Capture one actual immutable source pin. Validate bounded native root and
   metadata shape, current complete durable and derived hold inputs and their
   existing floor validators, and exact checkpoint coverage before scanning.
   Validate every counted canonical receipt, binding, predecessor hash/frontier
   and exact interval end before emitting a sample. Missing, corrupt, oversized,
   changed or incomplete evidence stays unknown and refuses emission; it is
   never reported as zero. Count the same canonical receipts and physical keys
   under the existing native floor, with the existing V2 fields and basis.
4. Whole retained source/audit, registration-link, fence and promotion validation
   remains mandatory in the mutation proof before any source deletion. The
   independent integrity scrub retains its complete checks and failure behavior.
   Do not claim those walks were completed merely because byte accounting passed.
   Accounting may finish while those independent walks remain pending; this
   separation changes readiness of the observation, not authoritative mutations.
5. Accounting may capture on its existing 60-second cadence during mutation-proof
   or writer rest, then continue its bounded read-only pages using the existing
   observation path. It cannot advance mutation proof, obtain a writer turn,
   force a checkpoint/drain or shorten, clear or renew a mutation rest deadline.
   Keep the 30-second capture freshness, 95-second observation gap, 256-row pages,
   existing byte ceilings and cancellation checks between pages.
6. Amend ADR-0279 decision 7 only to allow one bounded scalar accounting coverage
   record alongside its existing accounting certificate/candidate. Keep all new
   bookkeeping within the compiled 64-KiB limit, with at most two validation
   snapshot pins total. Share or release existing pins before capture; if a slot
   is unavailable, refuse new capture. Retain no snapshot chain, per-receipt
   index, hold history, mutation queue or durable byte counter.
7. Recheck native qualification before advancing and emitting. Hold changes,
   incompatible lineage/catalog, lost checkpoint coverage or unclassified
   publication revoke accounting evidence under the existing closed rules.
   Rebase only through the existing checked append, acknowledgement and owned
   deletion transitions; preserve exact removed canonical preimages and counters.
   Startup reconstructs accounting evidence from actual media and checkpoints.
8. Preserve commit ordering, atomicity, acknowledgement durability, uncertainty
   recovery, all durable and compatibility formats, provider admission and
   physical reference/reader protection. No application-facing control can
   select a proof, skip validation, change accounting or authorize deletion.
   Add no dependency, cryptographic algorithm or unsafe Rust.
9. Preserve every workload, queue, memory, disk, latency and fault gate, including
   the 60-second primary-loss deadline and fixed 45-second release. Require the
   saved populated native-worker observation check, final-source acceptance and
   CI, the complete eight-hour rehearsal and then uninterrupted 24-hour evidence.
   This decision and component tests alone establish none of those qualifications.

## Options considered

1. **Longer observation gaps or zero during validation:** rejected; conceals an
   unknown interval and weakens the resource evidence contract.
2. **Faster mutation preparation or foreground pauses:** rejected; changes the
   accepted pacing or workload to compensate for an observation dependency.
3. **Reuse accounting as the deletion proof:** rejected; byte counts do not prove
   complete source/audit semantics or current admission authority.
4. **Independent exact census with separate authority:** selected; read-only
   validation can progress while the complete deletion proof remains paced.

## Consequences

- Read-only accounting no longer waits for an unrelated complete audit walk.
- Two evidence owners require explicit type separation and shared pin accounting.
- Initial canonical scan throughput still must fit the unchanged freshness limit.
- This introduces no persistent index or shortcut for authoritative validation.

## Standing design tests

- **Interface safety:** applications retain compiled writes, scoped authorization,
  idempotency and durability. They cannot assert an observation or mint authority.
- **Scale:** scalar bookkeeping, two pins and bounded canonical pages; no extra
  writer priority or retained work proportional to historical receipt count.

## Checks

- Freeze the failing actual native-worker check and the deterministic initial
  checkpoint/append schedule while the reclamation proof remains unfinished.
- Compare completed bytes and floor with an independent pinned full oracle;
  prove that completed accounting alone still cannot authorize deletion.
- Change holds, classification, lineage and coverage between capture and emit;
  refuse incomplete, corrupt, stale, overflowing and cancelled candidates.
- Explore capture during mandatory rest, shared pin replacement and cancellation;
  preserve the existing source deadline and two-pin bound in every schedule.
- Keep original full source/audit validator equivalence and process-crash checks;
  repeat populated observations and all final qualification gates unchanged.
