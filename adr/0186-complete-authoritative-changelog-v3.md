---
adr: 0186
title: Complete Authoritative Changelog V3
status: accepted
tier: guarantee
date: "2026-09-02"
accepted: "2026-09-02"
requires: [ADR-0019, ADR-0061, ADR-0072, ADR-0082, ADR-0083, ADR-0085,
  ADR-0093, ADR-0100, ADR-0101, ADR-0104, ADR-0112, ADR-0124, ADR-0156,
  ADR-0157, ADR-0178]
amends: [ADR-0019, ADR-0093, ADR-0100, ADR-0178, REP-003, STO-012]
supersedes: []
requirements: [REP-002, REP-003, REP-007, REC-001, PERF-007, STO-012]
packages: [WP-772, WP-746]
obligations:
  - id: OBL-0186-1
    package: WP-772
    proof: replication_inventory_classifies_every_storage_namespace
    says: One closed catalog classifies every physical table and metadata key exactly once and
      makes every replicated-authoritative namespace available to bootstrap, tail, and follower
      comparison while excluding only explicitly rebuildable or replication-control state.
  - id: OBL-0186-2
    package: WP-772
    proof: successor_changelog_receipts_survive_checkpoint_and_recovery
    says: Journaled and direct or control-plane authoritative transaction receipts remain exact
      through suffix recovery, byte-identical checkpoint materialization, later overwrite or
      deletion, and every crash edge without changing Journal V1 or acknowledgement.
  - id: OBL-0186-3
    package: WP-746
    proof: bootstrap_to_tail_fence_is_gap_free_across_crash
    says: A staged bootstrap and its retained tail survive repeated source and receiver crashes
      without a gap, duplicate apply, partial publication, or foreign-lineage acceptance.
  - id: OBL-0186-4
    package: WP-772
    proof: changelog_history_reclamation_respects_checkpoint_and_fences
    says: Neither a journal receipt source nor materialized V3 history is reclaimed before a
      known-durable subsuming checkpoint and every follower, archive, and bootstrap fence permits it.
review_triggers:
  - Any authoritative namespace would be omitted, reclassified as rebuildable, or represented by
    a lossy transition.
  - Application or administration sequence assignment, command atomicity, acknowledgement, or the
    single-writer property would change.
  - A Journal V1 or changelog V1/V2 byte, identity, tag, checksum, receipt, or interpretation
    would change.
  - Replication history could be pruned past a bootstrap hold or acknowledged consumer frontier.
  - A public error, diagnostic, log, metric, or receipt would expose replicated keys, values,
    payload-derived hashes, capability material, or row cardinalities.
---
# ADR-0186: Complete Authoritative Changelog V3

## Context

ADR-0093 requires a byte-faithful follower prefix across every authoritative table. The immutable `ChangelogFrameV1` and `ChangelogFrameV2` cannot satisfy that guarantee: their
closed entry catalogs omit idempotency state, retained metadata, and most control-plane state, and
a later published snapshot cannot reconstruct a value that an intervening transaction overwrote or
deleted. ADR-0178 nevertheless asks WP-746 to activate those formats as a complete follower stream.
Implementation cannot silently narrow “every authoritative table” to the tables V1/V2 happen to
carry.

The successor must cover journaled command groups and direct, offline, migration, and control-plane
transactions without shipping writer-private roots or the local journal. It must preserve
ADR-0061 atomicity and acknowledgement, ADR-0082 application ordering, and ADR-0100 publication
from durable state while giving resume and bootstrap enough retained evidence to avoid a full-state
rewrite.

## Decision

### 1. One catalog owns the complete storage inventory

`riffdb-storage-api` owns one closed, versioned `AuthoritativeStateCatalogV1` over physical tables or closed key
domains in mixed tables such as `meta`. Every redb table and registered metadata key appears exactly once as:

- `ReplicatedAuthoritative`: loss or divergence changes database identity, results, authorization, contracts,
  retained history, durable operation state, or promotion behavior;
- `RebuildableLocal`: an accepted owner proves it derived and rebuildable from replicated-authoritative rows; or
- `ReplicationControl`: V3 history, lifecycle, bootstrap, and positions, each declaring `LineageShared`,
  `SourceOnly`, `FollowerLocal`, or `Staged` transfer behavior and never masquerading as application authority.

The declaration is shared by startup, backup, bootstrap, emitter, apply, and prefix comparison. A generated reviewed
fixture freezes classes/tags; CI rejects unclassified, duplicate, or copied inventory. Existing `Authoritative`,
`OutboxDelivery`, and `Projection` scopes seed it. `RebuildableLocal` requires an accepted complete rebuild source and
validation; mixed or unproven state defaults authoritative. Weakening that class requires an accepted guarantee ADR;
adding or reclassifying replicated authority requires a new changelog identity, and V3 rejects unknown tags.

Prefix exactness is canonical key/value-byte agreement for every `ReplicatedAuthoritative` namespace at one V3
position. A follower rebuilds and validates required derived state before readiness, but rebuildable or
replication-control byte differences cannot narrow the authoritative comparison.

### 2. V3 records every authoritative storage transaction

The successor identity is `riffdb.changelog-frame/v3`, numeric version `3`, with frame magic
`RDBCLF03` and footer magic `RDBCLE03`. V1 and V2 bytes, tags, chains, and rotation receipts remain
immutable. V3 is a new ADR-0124 durable domain identity, not an additive interpretation of either.

Every same-lineage storage transaction that changes a `ReplicatedAuthoritative` namespace produces one bounded
`AuthoritativeTransactionV3` receipt. A retained checked `next_changelog_transaction/v3` allocator
advances in that transaction and assigns a contiguous nonzero `ChangelogTransactionSequence`;
exhaustion refuses before mutation and never wraps. This orders physical authoritative transactions only.
It neither replaces nor assigns application `CommitSequence` or `AdministrationSequence`, and creates no application-visible ordering.

Each receipt binds database ID, history incarnation, predecessor and covered changelog transaction
sequence, predecessor and covered dual frontier, a closed attribution tag, canonical mutations,
the prior history hash, and its checksum. Attribution is exactly one of journaled application group,
journaled standalone service audit, direct application or service-audit group, `CommandAdmission`,
`CommandExecutionFailure`, or a closed named control-plane/lifecycle operation. `CommandAdmission`
records pending admission without advancing either frontier. `CommandExecutionFailure` records
terminal execution failure without advancing the application frontier; its administration frontier
advances only when the same transaction includes service audit. Both classes carry nonempty exact
authoritative mutations and retain the existing transaction boundaries and acknowledgement semantics.
Application and administration sequences are repeated where
they exist; an unchanged dual frontier is explicit. An unknown source tag or a transaction whose
declared sequences do not match its durable records is corruption.

One canonical mutation is `(namespace tag, key, operation)`. `Put` carries the complete stored value and expected
prior state (`Absent` or exact SHA-256); `Delete` carries the exact expected prior-value SHA-256. Mutations are
strictly ordered by namespace tag and key, contain at most one net transition per key, and describe only
`ReplicatedAuthoritative` state. Receipt/history rows, their allocator, and lifecycle rows are
`ReplicationControl` and never describe themselves; a control-only receipt has an empty mutation list plus its exact
attribution. The receiver checks predecessors before mutation. Retained receipts therefore replay later-overwritten
or deleted values without a historical snapshot. Keys, values, and their hashes never enter public diagnostics.

Receipt durability follows the existing checkpoint-plus-suffix authority. For a standard journaled transaction,
its allocator mutation, attribution, and exact canonical mutations in the validated journal suffix are the receipt
source. The checkpoint transaction materializes the identical V3 row before that suffix may be reclaimed.
A direct, hardened, migration, or control-plane `Immediate` transaction writes the materialized row atomically
with its mutations. Overlapping journal and materialized forms must decode byte-identically. A crash always leaves
at least one complete source; recovery rejects disagreement. Journal V1 identity and bytes remain unchanged; no second
flush or acknowledgement dependency is added, and receipts never derive from latest values. Incomplete paths refuse.

### 3. Frames are bounded durable-publication groups

A V3 frame contains a nonempty contiguous range of complete transaction rows, in changelog sequence
order, and binds database ID, history incarnation, leadership epoch, catalog identity, predecessor
and covered changelog positions, predecessor and covered dual frontiers, prior frame hash, payload
length, per-attribution counts, and checksum. One transaction is never split across frames. The
existing 256-transition and 32-MiB changelog ceilings remain hard maxima; admission proves a single
transaction fits before mutation. A receiver rejects gaps, duplicates, noncanonical mutations,
unknown identities, count or frontier disagreement, and substitutions before apply.

The durable-publication edge gives the emitter only a constant-size notification with a pinned
`PublishedDurableSnapshot`. Storage exposes a bounded receipt cursor over that view, choosing validated journal or
materialized rows without exposing journal bytes. The emitter consumes it after releasing the write gate and may
coalesce wakeups. Overflow scans from its last position through the newest pinned view; it is not data loss.
Network, archive, framing, and backpressure never hold the gate or delay acknowledgement. Unflushed rows cannot frame.

A Journal V1 receipt source cannot be reclaimed before a known-durable checkpoint contains its byte-identical
materialized receipt. Materialized history cannot be reclaimed until a later known-durable checkpoint subsumes its
transitions and its positions are below the minimum registered follower acknowledgement, archive acknowledgement,
and live bootstrap hold. Lower positions return typed `history_pruned`. Reclamation runs only at an existing drained
checkpoint/barrier, adds no command-path flush, and cannot rewrite a surviving receipt or authoritative row.

### 4. Bootstrap and tail share one exact fence

A V3 bootstrap pins one published durable snapshot and records a hold at fence `(catalog identity, database ID,
history incarnation, leadership epoch, changelog position and hash, dual frontier)`. It streams every
`ReplicatedAuthoritative` namespace in namespace/key order with bounded pages and a checksummed manifest. Tail begins
only at the fence's exact successor. The hold is durable before release and remains through durable tail attachment
or audited abort; pruning cannot create a snapshot-to-tail gap.

The receiver writes pages only to an offline staging database, records bounded resume progress, verifies exact ends,
catalog identity, manifest digest, and complete startup validation, then publishes one maintenance replacement. It
never serves or applies tail frames to a partial bootstrap. After replacement each frame is one storage transaction;
node-local applied and acknowledged positions advance only after durability. Repeated bytes are idempotent only when
the durable receipt, position, and hash match exactly.

### 5. Lineage metadata and promotion remain fail-closed

Database ID, history incarnation, V3 catalog identity and history anchor/tail, and `leadership_epoch/v1` are
lineage-shared and bootstrap copies them exactly. History rows and source holds are `SourceOnly`; follower positions
are `FollowerLocal`; a bootstrap receipt is `Staged`. A physical backup may carry these bytes as a complete engine
copy, but destructive restore resets source-, follower-, and staging-local state before publication, preserves
lineage-shared values, advances history incarnation under ADR-0072, and starts a new V3 anchor.

This record amends ADR-0019 and `STO-012` to permit exactly five additional retained metadata domains:
`authoritative_state_catalog/v1`, `leadership_epoch/v1`, `changelog_history_state/v3`,
`next_changelog_transaction/v3`, and `replication_follower_state/v3`, plus the bounded
`replication_source_holds/v1` table. The V3 history table and external `replication_bootstrap_receipt/v1` are
versioned replication-control records, not general operational metadata. Absence is valid only before receipted
V3 activation; afterward missing, unknown, or inconsistent state fails closed. This adds no `NodeId` and does not
reinterpret ADR-0019's other deferred metadata.

The ADR-0157 lifecycle row is `ReplicationControl`. Every `CLEAN` close and every `DIRTY` activation or clean
consumption allocates one V3 sequence and writes its exactly attributed materialized control receipt in the same
existing `Immediate` lifecycle transaction. Its mutation list excludes the lifecycle row, allocator, history state,
and receipt row, preventing recursion. The final close transaction atomically writes `CLEAN`, the advanced allocator,
history tail, and receipt; no V3 or other database write may follow. Before clean eligibility, startup validates the
V3 catalog, allocator, history tail, and terminal receipt as bounded ADR-0156 roots; absence, mismatch, exhaustion, or
malformation selects complete validation. This changes no ADR-0157 V1 lifecycle identity, byte, or binding stream.

Promotion under ADR-0178 retains the applied V3 prefix, advances leadership epoch and history
incarnation through its audited transaction, and starts a new chain anchor at that state. Old
handshakes, frames, cursors, acknowledgements, and bootstrap manifests then fail with the existing
typed stale-epoch or foreign-incarnation outcomes. A follower lacking a complete V3 prefix or the
registered catalog identity is not promotable.

### 6. Migration and negotiation never downgrade completeness

An existing database activates V3 only through an exclusive, crash-safe registry migration after
complete startup validation. One hardened transaction installs the catalog identity, allocator,
history anchor, and V3 bootstrap fence. Recovery observes either the old inactive state or the
complete V3 state. It never synthesizes V3 tail history for pre-activation writes; a new receiver
must bootstrap at or after the activation fence.

Destructive restore is a lineage break, not an unbounded tail transaction: it advances history incarnation,
installs a new V3 anchor in the existing publish transaction, invalidates old streams, and requires bootstrap.
Every bounded same-lineage migration transaction remains an ordinary attributed V3 receipt.

The replication handshake advertises exact readable identities and selects V3 only when both ends
name the same catalog identity and bounds. WP-746 production replication writes and applies only V3.
V1/V2 decoders and fixtures remain for compatibility evidence, but no negotiation falls back to
them while claiming ADR-0093 completeness. Unsupported format, catalog, lineage, epoch, bound, or
pruned position refuses before bootstrap transfer or follower mutation. WP-749 archives these same
V3 bytes; it does not translate V1/V2 into V3.

### 7. Bounds and security are structural

Frame, transaction, mutation, key, value, page, staged-bootstrap, hold-count, follower-count, and diagnostic sizes
have checked hard limits. Bootstrap and history scans are streaming and cursor bounded; no operation materializes
database population in memory. Replication capability is administrative and unavailable to application roles or
bootstrap convenience capabilities. Frames and bootstraps contain unredacted database bytes and require transport
confidentiality plus the ADR-0178 capability check. Public errors, logs, metrics, health, MCP text, CLI output, and
audit receipts expose only closed outcomes and non-sensitive positions; never payloads, physical keys,
payload-derived hashes, capability material, or population cardinality.

### 8. Package boundary

WP-772 owns the inventory, V3 receipts/allocator, suffix sourcing, checkpoint materialization, direct receipts,
retention, migration/rotation, compatibility fixtures, and durability/crash proofs. WP-746 depends on WP-772 and owns
only production emitter, RPC, follower/applier, and bootstrap activation over that sealed storage substrate.

## Options considered

1. **Activate V1/V2 for their eight or ten tables:** rejected because it turns an explicit partial catalog into a promotable database while violating ADR-0093's every-authoritative-table promise.
2. **Derive all transitions from the latest snapshot:** rejected because overwritten values and deletes no longer exist there, and many control-plane rows have no complete sequence attribution.
3. **Ship journal bytes:** rejected by ADR-0101 and ADR-0104; the journal omits direct control-plane paths and is an engine recovery encoding rather than a negotiated replication contract.

## Consequences

- V3 adds bounded atomic history bytes and one internal sequence allocation to every authoritative transaction, but adds no transaction, flush, network wait, or acknowledgement dependency.
- The closed inventory makes future tables fail CI until their authority and replication semantics are explicit. A new authoritative namespace requires a successor changelog identity.
- V3 history retention costs storage proportional to the unacknowledged transition suffix; holds and typed pruning make that cost visible and bounded by operator policy rather than correctness.
- Explicitly deferred are quorum acknowledgement, election, cascading followers, multi-primary, journal-format reuse, and translation of legacy changelog history.

## Standing design tests

- **Interface safety:** Applications gain no replication or storage-mutation surface and cannot select a format, skip a namespace, weaken apply checks, bypass freshness, or request fallback.
- **Scale:** Bootstrap is paged and tail cost follows changed bytes, not database size. Durable receipts replace historical snapshot retention; bounded coalesced notifications keep networking and slow consumers outside the writer gate.

## Checks

- The four obligation proofs plus `follower_applies_exact_prefix_byte_faithfully` and `replication_stream_resumes_gap_free_after_repeated_kills` cover inventory, exact apply, bootstrap, repeated crashes, retention, refusal, and unchanged authority.
- `./scripts/check-version-topology`, `./scripts/check-durable-format-manifest`, generated V3 vectors, storage conformance, and the process recovery matrix freeze identity and crash behavior.

Amendment 1 (accepted 2026-09-14, maintainer, in session): completed the closed attribution set before first durable use.
