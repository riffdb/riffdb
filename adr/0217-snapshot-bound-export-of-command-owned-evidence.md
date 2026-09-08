---
adr: "0217"
title: Snapshot-Bound Export of Command-Owned Evidence
status: accepted
tier: guarantee
date: 2026-09-07
accepted: "2026-09-08"
requires: [ADR-0099, ADR-0102, ADR-0104, ADR-0111, ADR-0112, ADR-0156, ADR-0165, ADR-0181, ADR-0204]
amends: [ADR-0204]
supersedes: []
requirements: [GOV-001, AFC-006, EXP-002, EXP-003, EXP-004, EXP-007, EXP-010]
packages: [WP-757]
obligations:
  - id: OBL-0217-1
    package: WP-757
    proof: application_export_reads_segment_owned_events_and_provenance_from_one_snapshot
    says: Application export reads retained events and command-owned provenance from one captured authoritative snapshot, orders events by commit sequence and event ordinal, and never treats physically empty derived tables as authoritative absence.
  - id: OBL-0217-2
    package: WP-757
    proof: application_export_canonically_deduplicates_mixed_legacy_and_segment_evidence
    says: Mixed provenance exports emit legacy rows first, skip only exactly equal command-owned duplicates, and refuse every mismatch, malformed owner, or noncanonical identity.
  - id: OBL-0217-3
    package: WP-757
    proof: application_export_sparse_evidence_pages_charge_bounded_progress
    says: Empty and sparse nonterminal evidence pages advance a canonical internal continuation, charge inspected work, and remain within every source, physical-scan, event, chunk, and public-page bound.
review_triggers:
  - Export would read events or command-owned provenance from physical derived tables as authority, use more than one captured read access, or order events by anything other than commit sequence and event ordinal.
  - Provenance phases, exact-equality deduplication, corruption refusal, or canonical continuation fields would change.
  - Any source-page, source-byte, physical-command-scan, segment, command-event, server-chunk, total-work, or continuation bound would change or become configurable.
  - A public protocol, CLI, SDK, authorization, policy, reimport, epoch-two identity, durable record schema, storage key, transaction, acknowledgement, recovery, or redaction behavior would change.
  - Implementation would touch a production path other than the four exact paths authorized here.
---
# ADR-0217: Snapshot-Bound Export of Command-Owned Evidence

## Context

The WP-757 epoch ceremony reached application export with committed events and
provenance, but the exported event and provenance classes were empty. The
export snapshot reader scans the physical `EVENTS` and `PROVENANCE` tables,
while ADR-0099 and ADR-0102 make command capsules and bounded command segments
the authority for successful-command events and provenance. ADR-0165 records
that those derived tables may be physically empty; treating that shape as
semantic absence violates `EXP-003`, `EXP-010`, and the complete ADR-0112
ceremony required by `AFC-006`.

The repair must preserve ADR-0104's frozen operational view, ADR-0111's policy
boundary, and the existing bounded export contract. Segment expansion can also
produce an empty nonterminal output page after duplicate suppression or policy
filtering. Counting only returned records would then permit repeated work
without consuming the public page's total-work ceiling.

## Decision

1. ADR-0204 Decision 2 is amended only to authorize guarantee-tier edits in
   these exact production paths for this repair:
   `crates/riffdb-storage-api/src/application_export.rs` for the private source
   page's inspected-work charge;
   `crates/riffdb-storage-redb/src/command_authority.rs` for bounded
   snapshot-owned command-row expansion;
   `crates/riffdb-storage-redb/src/application_export.rs` for event and
   provenance source paging; and
   `crates/riffdb-server/src/application_export_adapter.rs` for charging that
   work. This is general production application-export correctness, not a
   ceremony-only exception. No other file or purpose is authorized by this
   record.

2. One export operation captures exactly one `RedbReadAccess` representing one
   ADR-0104 checkpoint-plus-suffix publication root. Entity, event, provenance,
   audit, relationship, policy-anchor, command-authority, duplicate-check, and
   continuation reads for that operation use only that captured access and its
   existing database, history, catalog, and frontier binding. No page opens a
   newer transaction, splices a frontier, or consults process-current state.

3. Event export walks authoritative command physical rows in increasing
   physical key order, validates each capsule or segment and its physical key,
   and emits each retained event in the total order
   `(commit_sequence, event_ordinal)`. The continuation may stop inside one
   command and resumes at the exact next ordinal. A missing, malformed,
   noncanonical, substituted, discontinuous, or mismatched command member or
   event fails closed; the physically empty `EVENTS` table is never evidence
   that the command-owned event stream is empty.

4. Provenance export has exactly two ordered phases. It first emits canonical
   standalone legacy provenance rows in physical key order. It then walks
   authoritative commands in commit order and emits each command-owned
   provenance record unless a legacy row with the same complete provenance
   identity exists in the captured snapshot. Such a pair is skipped only when
   the two semantic records are exactly equal after their existing canonical
   decode. Any unequal duplicate, key/record mismatch, duplicate command owner,
   malformed bytes, or lineage disagreement is corruption and refuses the page.
   Deduplication uses a bounded point check, not a retained population set.

5. `ApplicationExportSourcePageV1` gains a private `inspected_work` value that
   is positive for every nonterminal source page and counts physical rows,
   command members, events, provenance candidates, and duplicate probes
   actually inspected under the closed storage algorithm. Returned records
   remain at most the requested `StorageScanLimit`, whose compiled maximum is
   500, and their charged canonical source bytes remain at most 4 MiB. One
   source call inspects at most 16 MiB of physical command-authority bytes. One
   segment remains at most 256 commands and 16 MiB, and one command remains at
   most 4,096 events. Overflow, inability to advance, or exhaustion of any
   bound returns `LimitExceeded` or corruption rather than an empty exact end.

6. The server requests at most 64 source records per storage chunk, releases at
   most 64 JSONL lines from it, and adds `inspected_work`, not only returned
   record count, to the existing total public-page work ceiling of 100,000. An
   empty or sparse nonterminal page is valid only when it advances its
   continuation and consumes positive work. Policy filtering, duplicate
   suppression, or a large command cannot create an uncharged retry loop,
   partial success, or a caller-selected larger bound.

7. Internal continuation remains at most 4 KiB and is a tagged canonical tuple
   containing the class and phase plus the exact legacy key or command physical
   key, logical command position, and event or provenance position required to
   resume after the last inspected item. Unknown tags, extra or nonminimal
   bytes, impossible phases or ordinals, cross-class reuse, non-advancement,
   and a tuple inconsistent with the captured authority refuse. The public
   export cursor remains opaque and unchanged.

8. This repair adds no public selector, operation, field, permission, policy,
   protocol, CLI or SDK behavior, reimport path, epoch-two identity, durable
   record schema or registry identity, storage key, compatibility promise, or
   configuration. Existing transaction, acknowledgement, recovery, receipt,
   redaction, symbolic serialization, and row/field-policy semantics remain
   exact. The inspected-work value is engine-neutral private page metadata, and
   the tagged continuation is carried only inside the existing opaque internal
   continuation slot.

9. OBL-0181-2 and
   `epoch_two_ceremony_refuses_in_place_open_and_reconciles_reimport` remain the
   complete epoch ceremony proof. The three obligations here are focused
   semantic prerequisites and do not replace, weaken, or relabel that proof.
   Implementation and governance changes begin only after exact-text human
   acceptance; this proposal changes only this record and the generated index.

## Options considered

1. **Continue scanning empty physical tables:** rejected because derived-table
   emptiness is not authoritative absence under command-segment storage.
2. **Repopulate event and provenance tables:** rejected because it creates a
   second authority and changes the atomic writer and durable layout.
3. **Build an unbounded export-only evidence index:** rejected because it makes
   memory and startup proportional to retained history.
4. **Page command authority from one snapshot with charged continuation:**
   chosen because it uses existing authority and preserves bounded export.

## Consequences

- Epoch ceremony export observes the events and provenance already owned by
  successful commands without changing their durable representation.
- Mixed historical provenance remains exportable with deterministic ordering,
  bounded exact deduplication, and fail-closed disagreement handling.
- Sparse evidence can require more internal chunks, but every chunk advances
  and consumes a fixed public work budget.

## Standing design tests

- **Interface safety:** applications, agents, and clients gain no table,
  command-authority, continuation, work-budget, policy, or compatibility
  control. Existing explicit export class and scope authority remains the only
  public choice, and corrupt evidence cannot be converted into absence.
- **Scale:** reads remain snapshot-bound and streaming; output is capped at 500
  records and 4 MiB per source page, command scanning at 16 MiB, segments at
  256 commands/16 MiB, commands at 4,096 events, server chunks at 64 records,
  total public-page work at 100,000, and continuation state at 4 KiB. No
  population set or full-history memory is introduced.

## Checks

- `application_export_reads_segment_owned_events_and_provenance_from_one_snapshot`
  proves command-owned event/provenance authority, exact event order, one
  captured access, continuation across commands and ordinals, and corruption
  refusal.
- `application_export_canonically_deduplicates_mixed_legacy_and_segment_evidence`
  proves legacy-first provenance, exact-equality suppression, deterministic
  order, and refusal of every disagreement or duplicate-owner shape.
- `application_export_sparse_evidence_pages_charge_bounded_progress` proves
  positive inspected work and continuation advancement for empty and sparse
  pages and exercises every compiled source, scan, segment, event, chunk,
  public-work, and continuation ceiling.
- Existing application-export, policy, compatibility, command-authority,
  requirement, ADR-obligation, topology, generated-index, and WP-757 ceremony
  checks must pass before closure.
