---
adr: "0204"
title: Epoch-Two Storage Reader Retirement and Package Retiering
status: proposed
tier: guarantee
date: 2026-09-05
accepted: null
requires: [ADR-0112, ADR-0124, ADR-0181, ADR-0190, ADR-0192]
amends: [ADR-0181]
supersedes: []
requirements: [GOV-001, GOV-002, GOV-003, AFC-005, AFC-006, VER-005]
packages: [WP-757]
obligations:
  - id: OBL-0204-1
    package: WP-757
    proof: epoch_two_format_gate_precedes_retired_reader_dispatch
    says: The immutable epoch comparison refuses every epoch-1 database before any retired storage reader, table open, journal replay, repair, allocator action, or mutation can run, while missing, malformed, unknown, and future identities remain fail-closed.
  - id: OBL-0204-2
    package: WP-757
    proof: epoch_two_removes_legacy_vector_control_bridge
    says: Epoch 2 removes the tag-65 vector-control reader, generated message, table, migration bridge, fixtures, and topology identity without translating, adopting, or relabelling its bytes and without changing tag-67 current control semantics.
review_triggers:
  - Epoch-1 bytes could reach a retired decoder, table, migration, journal, repair, or mutation path before the typed epoch refusal.
  - A current merged-main identity, tag-67 control, V2 columnar artifact, fresh-locator proof, transaction order, conflict owner, atomicity boundary, acknowledgement, or outcome sequence would change.
  - A retired tag, field, enum value, hash, filename, or symbolic identity would be reused, translated, parked as readable, or accepted as current.
  - The ceremony would omit validation, verified backup, export receipt, empty epoch-2 target, compiled reimport, reconciliation, final startup validation, or retained epoch-1 artifacts.
  - Implementation would require a guarantee path other than the two storage-redb files named in this record.
---
# ADR-0204: Epoch-Two Storage Reader Retirement and Package Retiering

## Context

ADR-0181 accepted one pre-external breaking reset and assigned WP-757 the
removal of every superseded durable reader. It classified the record and
package as `surface`, but implementing the accepted removal necessarily edits
`crates/riffdb-storage-redb/src/startup.rs` and `store.rs`, which governance
classifies as `guarantee`. A surface-tier package cannot truthfully carry those
bytes, and a commit trailer may raise but not waive the missing guarantee
decision.

The dirty WP-757 prototype also predates WP-711 and WP-778. Blindly applying its
old identity inventory would delete or reinterpret current tag-67 columnar and
fresh-locator guarantees. The reset therefore needs an exact integration rule:
retire only identities superseded in the merged authoritative epoch-2
inventory, behind a format gate that refuses epoch 1 before retired code could
run.

## Decision

1. WP-757 is reclassified from `surface` to `guarantee`. Its objective,
   requirements, dependency on WP-711, accepted ADR-0181 ceremony, existing
   allowed paths, and public behavior do not otherwise change. The governance
   amendment that adds this ADR to `required_adrs` and raises the package tier is
   committed separately from implementation; every WP-757 implementation and
   closure commit carries `Governance-Tier: guarantee`.

2. This record narrowly authorizes guarantee-tier edits only to
   `crates/riffdb-storage-redb/src/startup.rs` and
   `crates/riffdb-storage-redb/src/store.rs`. Those edits may remove epoch-1
   reader selection, table opening, startup validation, and dispatch arms that
   become unreachable after the epoch-2 refusal. They may not change a current
   reader's validation, storage key, mutation, transaction ordering, conflict
   ownership, durability, journal ordering, allocator semantics, acknowledgement,
   recovery result, or outcome sequencing. Any other guarantee path requires a
   separate exact human-accepted amendment.

3. The epoch comparison is the first database-sensitive action. From immutable
   format identity alone, an epoch-2 binary distinguishes the single exact
   epoch-2 writer identity from epoch 1, missing, malformed, unknown, and future
   identities before opening an authoritative table or journal and before
   initialization, migration, repair, replay, allocator inspection, receipt
   reconciliation, or mutation. Epoch 1 returns the existing bounded
   `RDB-FORMAT-0101` public refusal naming `riffdb storage preflight` and the
   export/reimport ceremony. Every other nonmatching form retains its existing
   typed fail-closed classification; it is never treated as an empty database.

4. No epoch-1 storage or durable record is decoded to decide that refusal. The
   epoch-1 binary remains the only reader used to validate, back up, and export
   the old database. The epoch-2 binary has no fallback, compatibility mode,
   in-place migration, restore shortcut, best-effort decode, raw copy, repair,
   relabelling, or application/operator selector that can reach retired bytes.

5. Before retirement is applied, WP-757 merges the then-current main without
   rebasing or dropping its prior patch and recomputes the complete generated
   inventory from authoritative source. An identity is retired only when the
   merged epoch-2 manifest and accepted ADRs name a distinct current successor.
   Current tag-67 columnar-control semantics from ADR-0192, V2 root/publication
   semantics from ADR-0190, and WP-711's selected artifacts remain unchanged.
   WP-778's fresh-locator proof and every current registry/storage identity also
   remain unchanged unless ADR-0181 explicitly names that identity as the
   superseded side of the epoch reset.

6. The legacy tag-65 vector-control bridge is retired completely in epoch 2:
   its Protobuf message and generated Rust type, decoder/encoder, redb table and
   table-open path, migration/translation branch, fixtures, manifest entry, and
   topology identity are removed. Its tag, field and enum numbers, hash/schema
   identities, filenames, and symbolic names remain permanently reserved.
   Epoch-2 startup never scans, translates, copies, adopts, or relabels tag-65
   bytes. Tag-67 state is rebuilt or selected only under its accepted current
   ADR-0192 lifecycle.

7. The same rule applies to every other retired durable family: delete the old
   reader, generated artifact, fixture, registry row, manifest window, topology
   identity, and migration dispatch together; reserve every former numeric and
   symbolic identity; retain exactly one equal readable/writable/current
   identity per domain while `external_databases` is empty. An old identity is
   neither a second readable form nor a `retirement_candidate` placeholder.

8. The one real ceremony remains load-bearing. A retained epoch-1 binary
   completely validates and backs up a seeded TicketDesk database, produces the
   accepted portability export and receipt, and retains the old database and
   backup. The epoch-2 binary first proves typed in-place refusal, initializes a
   distinct empty epoch-2 database, reimports only through the accepted compiled
   portability path, reconciles bounded entity, event, provenance, authority,
   contract, and hash observations, and completes ordinary epoch-2 startup
   validation. Hand-authored JSON, physical copy/restore, or a fixture-only
   refusal cannot satisfy the proof.

9. This decision adds no public operation, protocol field, MCP surface, storage
   selector, epoch selector, decoder selector, force flag, or application
   authority. Public gRPC, CLI, SDK, MCP, contract, command, and generated driver
   semantics remain current and are regenerated only to remove superseded
   identities. Documentation states expected downtime, the refusal and sole
   ceremony, old-binary retention, unsupported data classes, and no downgrade.

10. WP-757 cannot close until generated checks prove source/descriptor/fixture
    equality, the topology and durable manifest each name one exact active
    identity per domain, all retired numbers remain reserved, the two obligations
    above pass, and the complete ceremony receipt is independently validated.
    A clean build or a smaller registry is not evidence of safe retirement.

## Options considered

1. **Keep WP-757 surface-tier and use a guarantee trailer:** rejected because a
   trailer raises ceremony but supplies no accepted guarantee decision.
2. **Leave the two old reader arms unreachable:** rejected because ADR-0181
   requires deletion, and dead decoders preserve attack and maintenance surface.
3. **Remove retired readers before comparing the epoch:** rejected because an
   epoch-1 database could fail through an arbitrary decoder/table error rather
   than the stable pre-mutation refusal.
4. **Retire from the old prototype inventory without merging main:** rejected
   because it predates current columnar and locator identities and could delete
   live guarantees.
5. **Retier narrowly and keep the immutable epoch gate first:** chosen because
   it authorizes exactly the necessary guarantee paths without changing current
   storage semantics.

## Consequences

- WP-757 can truthfully remove old storage readers while retaining a full exact-
  text guarantee review and a bounded path owner.
- Integration must reconcile a large generated diff against current main before
  evidence, and every retired identity needs reservation and absence proofs.
- Old databases require the retained old binary and full export/reimport
  ceremony; direct open, restore, repair, and downgrade remain unavailable.
- Further guarantee-path edits, post-external retirement, online migration, and
  partial-domain resets remain deferred.

## Standing design tests

- **Interface safety:** an application, agent, or operator cannot select an
  epoch, decoder, fallback, target mutation, or weaker ceremony; the only
  epoch-2 behavior for epoch-1 storage is typed pre-mutation refusal.
- **Scale:** startup decides incompatibility from fixed format identity and does
  not enumerate population data; the explicit ceremony retains the accepted
  bounded export/reimport page and receipt ceilings.

## Checks

- `epoch_two_format_gate_precedes_retired_reader_dispatch` uses hooks at every
  table, journal, repair, allocator, receipt, and mutation boundary to prove an
  epoch-1 open reaches none of them before `RDB-FORMAT-0101`.
- `epoch_two_removes_legacy_vector_control_bridge` proves tag-65 source, generated
  code, registry, table, migration, fixtures, and topology are absent and their
  identities reserved while tag-67 behavior is unchanged.
- `epoch_two_ceremony_refuses_in_place_open_and_reconciles_reimport` proves the
  real retained-binary backup/export and empty-target reimport/reconciliation
  sequence with exact artifact hashes and final startup validation.
- `scripts/check-epoch-two-inventory`, `scripts/check-version-topology`, generated
  checks, release compatibility tests, and corruption/source-span fixtures prove
  one current identity, complete retirement, permanent reservation, and bounded
  refusal diagnostics.
