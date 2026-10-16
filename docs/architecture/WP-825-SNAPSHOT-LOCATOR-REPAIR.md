# WP-825 snapshot locator repair

Status: implementation design under accepted ADR-0313, 2026-10-15. The
maintainer accepted its exact text and WP-825 registration in session. Native
implementation and complete qualification remain open. The attempted
8-hour rehearsal stopped at additive deployment before the scheduled restore;
no 24-hour campaign has started.

## Confirmed failure and current boundary

A cold query-module request reached the legacy full administration validator,
which combined a pinned checkpoint with current command-audit locators. Genuine
online retention retired those locators after promoting the audit rows into a
successor view. The old checkpoint had neither representation. The original
native regression reproduced typed `CorruptData` while proving that all audits,
outcomes and provenance remained intact. The server request-path correction uses
existing owned metadata reads and preserves exact pointer/audit/body checks;
ADR-0073 already requires the full validator to stay off that request path.

Focused check 1340 also confirmed a separate snapshot defect. Both an old pin's
outcome and its provenance became `None` after ordinary retention, although the
original command remained readable through that pin. The independent regressions
are `owned_snapshot_outcome_survives_retention_of_original_command` and
`owned_snapshot_provenance_survives_retention_of_original_command`. Both failed
before the repair. Focused check 1364 passes six genuine retention, publication
and source-lifetime cases, including both regressions and their controls. The
read-only engine lifetime case, pre-durability budget refusal, seven provider
ownership cases, shared retirement and cold-stub cache charging also pass.
Later focused checks pass both server cold-publication interleavings, full startup,
follower reconnect, graceful-close, legacy-engine lifetime and builder tests.
All nine provider ownership cases pass. Scoped Clippy passes for the four changed
crates. Reclamation/control checks pass 118 of 124 cases. Focused follow-up check 1378
passes all six original failures, including both process-crash boundaries, and
two new direct-receipt membership regressions. The repaired immediate metadata-only
commit uses checked same-membership reuse; command-changing mutations remain pending.
These focused results do not qualify the combined source or the campaign.
Main is integrated through `067a0f7b` at merge checkpoint `f12a2888`.
The preceding full native library run passes 1,367 cases, with one owned catalog
migration failure and one existing ignored case. Focused check 1384 repairs the
internal cached-engine ownership seam and passes the original failure, an escaped
snapshot negative control, nine migration cases and 34 owned-backup cases.
Scoped Clippy, handbook and allowed-path checks pass; final complete CI remains open.
Combined-source native tests and complete CI remain open.

At the failed revision, `RedbOwnedSnapshot` consults the current mutable locator directory.
A current miss cannot prove absence in an earlier generation. Reading a newer
retained record as an advisory hint does not supply a complete as-of directory,
including identity reuse and publication gaps. That alternative is rejected.

Integration also exposed source-lifetime and accounting defects. A retained
redb transaction does not keep its database backend open after the database
owner closes. Snapshots and escaping read-only cursors must retain that engine
owner alongside their original history resolver. Exact providers must account
for retained native generations, source descriptors and engine caches, including
physical allocations awaiting bounded retirement. These are native repairs;
the harness does not mask them by reopening a database or dropping protected pins.

The integrated tests exposed additional publication seams. Full activation now
completes the locator capability of its captured composite source rather
than only the raw checkpoint. Classified metadata-only control commits preserve
a checked complete successor directory when command membership is unchanged;
the generic immediate receipt path now applies the equivalent checked transition.
Unchecked test fixtures that fabricate operational ports also need to use the
existing activation path; a pending capability correctly refuses their writes.

## One source view and its complete locator generation

Implement the same-view requirement in accepted [ADR-0104](https://github.com/riffdb/riffdb/blob/main/adr/0104-journal-authoritative-state-overlay.md)
and [ADR-0244](https://github.com/riffdb/riffdb/blob/main/adr/0244-retire-durable-command-derived-locator-tables.md).
Use one immutable published owner containing the authoritative checkpoint and
suffix, a complete exact command-locator generation, and their source custody.
A snapshot captures that owner once. Neither a positive nor a negative lookup
consults a later process-global locator directory.

The locator generation contains the existing manifest-derived idempotency,
provenance, audit-sequence, audit-request, event-route and pending-outbox locator
kinds, plus segment metadata needed to validate their targets. Independently
mutable delivery status still comes from the captured authoritative view.
Payload-cache residency is optional and never proves directory completeness.

Bind completeness to database identity, history incarnation, registry identity,
the exact retained command set and segment frontier, and its validated root
identity. Application frontier alone is insufficient: pruning changes the set
without advancing it. Only complete validation or a checked successor transition
may construct the capability. A default, partially filled or invalid index must
not impersonate a complete empty generation.

Use a first-party persistent ordered tree with shared immutable nodes and keys.
Capture and clone a root in O(1); insert and delete copy only bounded search paths
and rotations. Reuse the existing persistent-overlay approach where appropriate,
without adding a critical dependency or duplicating semantic locator types.
Deletion must remove keys from the successor tree, not accumulate lifetime
locator tombstones. Old roots retain their original nodes. Retain canonical key
ordering and exact member/ordinal reciprocity; tree shape is not durable authority.

## Stage before durability; publish together

For each command, build the successor locator roots from the exact predecessor
and checked canonical manifest while staging its existing authoritative frame.
For pruning, stage retained rows, command deletion and locator deletion as one
successor. Reserve their complete incremental allocation and retirement costs
before allocating nodes or submitting durable work. A private root cannot be
served and cannot observe another private writer's partial transition.

Prebuild the immutable publication owner before the durability boundary. After a
known-successful fence, one compare-and-publish installs authoritative state and
locator roots together, then releases results and notifications. There must be
no interval with retired locators and an older authoritative root, or a new
engine root and older locators. Apply this to both journal and direct/hardened
publication. Preserve existing sequence assignment, FIFO order and uncertainty
rules. Unexpected publication failure fences writes; it cannot acknowledge an
incomplete generation or discard unresolved durable ownership.

Checkpoint compaction reuses the same logical locator generation when the
retained command set is unchanged, while rebinding physical source custody.
A checkpoint at C must never acquire a directory containing private or published
commands beyond C. The checkpointer's captured covered view supplies that exact
root; do not retain an unbounded chain of all predecessor generations.

## Source lifetime, retirement and release

A locator root alone does not keep its command bytes readable. Its published
owner must retain the checkpoint pages, overlay and sealed-extent read custody
that resolve its original command set. Keep this distinct from a changelog
resume hold: logical pruning can progress while an older read still protects
its physical sources. Extend the existing source-reference/recycling checks
rather than introducing an untracked retention exemption.

Retirement removes a root from discovery, not from live snapshots. A pinned
reader, worker or provider keeps its exact generation and ordinary authorization
checks. Cancellation releases only its references. Never substitute a newer
root, evict a protected pin, or resurrect revoked authority to reclaim space.

Each node/key allocation has one charged owner shared by every referencing root.
Release its charge only after its final reference is destroyed. Budget a bounded
retirement queue and bounded destruction work; moving an owner into that queue
must not release its bytes. Avoid root/index/source reference cycles. Shutdown,
private-stage cancellation and unknown fences need explicit release ownership.
Crash discards these derived process-local generations; recovery reconstructs
only from existing durable authority.

## Startup and readers without a live shared directory

Build the persistent directory directly while an existing complete manifest
validation runs; do not rebuild a mutable whole index and clone it per snapshot.
An existing validated index snapshot may be loaded only when its supported
identity and completeness checks match. This proposal adds no durable index
format, locator table, writer epoch or compatibility fixture.

Preserve ADR-0182's bounded dirty readiness and the recovery scratch ceiling.
Metadata reads that do not need command locators remain available from their
bounded validated roots. A locator-dependent read needs a complete capability
for its own captured generation. If construction is pending or refused, return
typed unavailable/integrity failure, never `None`; a rebuild must keep the exact
source pin and use admitted bounded pages and cancellable workers.

`from_read_access` readers currently have no live shared directory. Follower,
archive, offline-validation and restored readers therefore need an explicit
same-generation locator capability or an explicit pending/invalid state. They
cannot silently fall back to current source indexes or treat missing standalone
rows as absence. Static complete validation can produce the capability from its
existing walk; bounded startup needs the selected admitted construction policy.
History-stub targets must use the existing stub-aware command decoder.

## Accepted budget and pending measurements

Accepted ADR-0313 introduces a 512-MiB aggregate native locator-memory pool per
process across all databases and source constructors. Ordinary ownership and
construction may consume 448 MiB; 64 MiB is reserved for maintenance progress
and reclamation. Current, private, pinned, retired and queued ownership all
count, including metadata, manifests, keys, control allocations and scratch.
At most 1,024 generation owners exist, with one slot reserved for maintenance.
Final-owner release uses its preadmitted retirement descriptor without allocation
or refusal; cleanup yields after at most 256 charged ownership items, including
manifest entries. Moving ownership into a queue does not release its charge.

The 64-MiB payload cache, 64-MiB recovery scratch, 256-MiB overlay and 1-GiB
provider limits retain their separate scope and accounting. They do not grant
extra native locator capacity or exempt provider-retained native ownership.
The record also reconciles ADR-0244's before-readiness index rebuild with the
existing bounded clean, dirty and applicable restore startup modes. A pending
or invalid generation cannot authorize a locator-dependent read or mutation;
construction stays bound to the original captured source.

Saved-population diagnostic 1363 reconstructs 29,958 retained command rows at
frontier 425,735. Native ownership peaks at 310.9 MiB in total and 310.3 MiB
ordinary; the completed generation's conservative provider bound is 303.2 MiB.
The reusable input remains unchanged. This checks one captured source with
temporary numerical instrumentation in a debug build. It does not establish
shipping restore timing, old-pin interference or the full campaign's capacity.

At 32,768 retained commands, 512 MiB allows 16 KiB per command; at
65,536 it allows 8 KiB, before old pinned generations and private work. A
no-event command has six manifest entries; one event makes eight. The maximum
idempotency key is 908 bytes, and a segment can contain 65,535 manifest entries.
Charge duplicated manifests and keys unless ownership is structurally shared.

Event-heavy legal graphs, large keys, protected pins or conservative staging
reservations can exhaust the pool before retention becomes eligible. The
accepted result is typed refusal before durability, preserving protected roots,
not reduced retention fences or another owner's capacity. Measure current-base,
many-root and worst-admitted-frame footprints, progress headroom and interference
before claiming the frozen workload fits. Changing a ceiling needs new acceptance.

Use checked arithmetic and conservative target-specific layout bounds. Derive
a closed tree-height and copied-node/rotation reservation before allocation.
Preserve original source custody, refuse incomplete negative evidence, and
avoid unbounded recursive drops, predecessor chains and whole-index copies.

## Focused proof and integration order

1. Retain the separate failed outcome/provenance regressions and original evidence.
2. Apply the accepted ADR-0313 budget/readiness policy and registered scope.
3. Prove persistent-map insertion/deletion, balanced bounds, shared-root isolation,
   checked charging, cancellation and final-reference release with deterministic
   tests; cover exhaustion before any durable submission.
4. Exercise command and prune publication interleavings, identity reuse across
   historical pins, corrupt/missing locators, all source constructors, history
   stubs, checkpoint rebases, and process crashes at each durable boundary.
5. Recheck actual metadata cost and write/read interference on the qualification
   host, then run the closest public deployment and restore gates before full CI
   and the complete rehearsal. Preserve all original campaign thresholds.

This note describes the accepted implementation boundary. The native repair,
resource measurements and complete campaign qualification remain open.
