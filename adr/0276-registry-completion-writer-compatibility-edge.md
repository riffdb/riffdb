---
adr: "0276"
title: Registry Completion Writer Compatibility Edge
status: accepted
tier: guarantee
date: 2026-10-06
accepted: '2026-10-06'
acceptance: 'maintainer, in session, 2026-10-06: Approved, in response to the request for exact acceptance of ADR-0276 and its implementation scope extension'
requires: [ADR-0112, ADR-0263, ADR-0269, ADR-0275]
amends: [ADR-0275 compatibility transition, ADR-0263 Amendment 5 release graph]
supersedes: []
requirements: [AFC-001, AFC-002, REC-005, REC-006, REC-007, STO-032, END-008]
packages: [WP-825]
obligations:
  - id: OBL-0276-1
    package: WP-825
    proof: registry_completion_writer_upgrade_preserves_predecessor_and_refuses_old_binary
    says: Exact predecessor markers require the existing backup-bound offline upgrade; unsupported combinations refuse before mutation and successor markers refuse older binaries.
review_triggers:
  - A predecessor marker is accepted without its exact registry and compatibility corpus, or source-free preflight is bypassed.
  - Ordinary startup silently upgrades an old marker, the format epoch changes, or backup, free-space, receipt or retry requirements weaken.
  - A marker or format-upgrade receipt grants registry completion without ADR-0275's exact complete native proof.
---
# ADR-0276: Registry Completion Writer Compatibility Edge

## Context

ADR-0275 adds the registry completion record. The existing immutable format
marker also binds the readable registry digest. Adding the record changes that
digest, so a genuine predecessor command database refuses before redb opens,
even though its records remain readable. AFC-001/AFC-002 and ADR-0112 prohibit
loosening this check or using ordinary startup as an undeclared format upgrade.

ADR-0263 Amendment 5 established the backup-bound offline writer transition.
Extend that existing transition for the reviewed registry successor. The exact
predecessor registry and compatibility inventory are frozen below; host data,
addresses and raw diagnostic output are not part of these fixtures.

## Decision

1. Keep alpha format epoch 1 and advance the durable-format writer from 2 to 3
   for ADR-0275's record registry. Keep marker, upgrade-receipt, storage,
   journal-frame, extent and backup encodings unchanged. The generated current
   manifest advertises the complete successor registry and compatibility corpus.
   This is a one-way in-epoch compatibility edge, not a destructive epoch reset.
2. Add the exact writer-2 to writer-3 edge using the existing OfflineInPlace
   action: backup required, downtime required, two source-size multiples of
   free space, one-way, and the existing safe upgrade command. Ordinary startup
   refuses before engine open, journal replay or mutation with that typed action.
   New empty databases are initialized with the current writer-3 marker.
3. Freeze writer-2's registry digest as
   `a141b765c2d468a7c270461553aad6a56f9d2af3a0e4f63027151227ed4b40a3`
   and compatibility-inventory SHA-256 as
   `09b389c827da34e5527385ff743bde7a8a8bd6101fc620191c9b6842b77fadc0`.
   Only that exact pair authorizes this predecessor edge. Unknown, corrupt,
   substituted or ambiguous combinations fail before target mutation. Preserve
   the genuine predecessor fixtures and backups byte for byte.
4. Retain the existing writer-0 and writer-1 offline upgrade edges. Freeze each
   retained predecessor's own accepted registry/corpus identity rather than
   comparing it against the successor's changing current digest. Writer-1 keeps
   ADR-0263 Amendment 5's exact registry and frozen compatibility corpus; no
   additional predecessor is inferred from an epoch or writer number alone.
5. Reuse the existing native offline upgrade owner, immutable backup binding,
   exclusive file ownership, exact source comparison, Accepted/Migrated/Complete
   receipt phases, durable marker publication and crash-safe retries. Validate
   the backup under its declared predecessor identity using the successor's
   retained readable codecs; do not rewrite or promote the backup to current.
   A mismatched or absent backup grants no mutation authority. Existing source
   and temporary-artifact resource bounds and refusal behavior remain.
6. Under that closed upgrade owner, reconcile the predecessor's acknowledged
   journal before any successor registry transition. Install ADR-0275's Pending
   metadata as one StorageFormatMigration receipt through the existing ordered
   durable writer, with exact registry before-image and unchanged application
   and administration frontiers. No separate unreceipted registry write occurs.
   Preserve outcomes, events, provenance, archive bindings, retained receipts,
   watermarks, tombstones, source admission and referenced fences.
7. Format-upgrade success means the current physical format can be opened; it
   does not grant readiness or Completed registry eligibility. A migrated
   Pending database still earns completion only through ADR-0275's native
   exact-end structural and catalog-semantic owner. Interrupted upgrade or
   migration grants no shortcut, and no bounded/background path earns completion.
8. Publish and verify the current marker through the existing upgrade protocol
   before ordinary startup or operational writes are allowed. Kill cuts at
   migration, receipt and marker boundaries recover only through existing
   backup-bound retry. Unknown outcomes retain write fencing. An older binary
   must refuse writer 3 before opening the engine or replaying its journal.
9. Backup/restore, changelog reconstruction and follower bootstrap distinguish
   exact predecessor and current physical identities. Preserve their existing
   complete proof and incarnation rules. Supported predecessor input uses its
   declared offline transition; there is no downgrade, public raw import,
   marker override, force, completion selector or validation bypass.
10. Regenerate current manifests, inventory and handbook compatibility tables;
    retain every frozen old vector. Qualification uses the final writer-3
    binaries and artifacts. Existing diagnostic originals remain preserved;
    upgrade/restart evidence uses owned copies and records its actual identity.
    This decision does not authorize the 24-hour run.

## Options considered

1. **Accept the old marker as current:** rejected; violates immutable preflight
   identity and permits an older binary to enter a successor database.
2. **Change the epoch and reimport applications:** rejected; the approved change
   is an additive readable record and does not require destructive conversion.
3. **Existing backup-bound offline writer edge:** selected; preserves the
   established compatibility ceremony and exact predecessor artifacts.

## Consequences

- Existing writer-2 databases need one explicit offline upgrade and a complete
  native validation before they can retain completion across future pruning.
- New writer-3 databases earn completion through their ordinary full first open.
- The cost is one reviewed manifest edge and predecessor/retry compatibility
  evidence. Normal latency, source capacity, physical reclamation, archive
  capacity and full rehearsal qualification remain independently measured.

## Standing design tests

- **Interface safety:** the existing safe offline upgrade remains the only
  transition; applications cannot assert completion, rewrite a marker, skip
  backup/validation, weaken durability or choose raw storage mutations.
- **Scale:** pre-open classification reads fixed-size identity records. Offline
  upgrade retains existing backup, free-space and bounded-copy constraints.
  Normal current-format startup gains no population walk or extra ledger.

## Checks

- Exact old/current/unknown marker and manifest fixtures; genuine predecessor
  open refusal with unchanged database, journal, extent and marker bytes.
- Immutable predecessor backup validation, wrong/absent backup refusal, exact
  source mismatch refusal, retry and process kills across all receipt/marker cuts.
- Old-binary refusal before engine open; current initialization and reopen;
  Pending after upgrade, Completed only after exact full native validation.
- Preserved acknowledged outcomes, events, provenance and archive frontier;
  public backup/restore, changelog and follower compatibility; frozen-source
  acceptance, full CI and required-host populated restart qualification.
