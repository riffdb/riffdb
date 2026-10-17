# WP-825 snapshot locator repair

Status: implementation design under accepted ADR-0319, 2026-10-15. The
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
Main is integrated through `b1e5117c5` at merge checkpoint `6635a546`.
The preceding full native library run passes 1,367 cases, with one owned catalog
migration failure and one existing ignored case. Focused check 1384 repairs the
internal cached-engine ownership seam and passes the original failure, an escaped
snapshot negative control, nine migration cases and 34 owned-backup cases.
Scoped Clippy, handbook and allowed-path checks pass; final complete CI remains open.
Combined-source native tests and complete CI remain open.

Fresh preparation 1391 failed during seeding with `AuditUnavailable`, before the
measured load. Deterministic regression 1392 reproduces a native retirement
fairness defect: a dead generation's owner and provider charges wait for the
entire cleanup queue to become empty, so later independent work can indefinitely
prevent their release. The regression includes a child first queued while its
parent is being retired and checks the unchanged 256-item step limit. Retirement
now tracks that generation's physical descendants through fixed causal epochs,
retaining owner and byte charges until physical retirement while later independent
work continues. The existing owner ceiling bounds the tracking ring.
Focused check 1395 passes the original regression, all 18 retirement cases,
five persistent-tree cases and nine provider cases. A separate seed reproduction
1393 reports ordinary byte pressure: 382,903,734 bytes charged and an additional
87,353,040-byte reservation refused, with one generation owner. Thus the fairness
repair alone does not establish recovery from the seeding failure.

Borrowed-data probe 1397 finds a 71,729-byte, 16-command segment with an 83-MiB decode
reservation. The general provider receipt allowance includes a transient 4-MiB
ownership-inspection workspace. The native adapter accidentally sums that fixed
window for every command prefix, although prefix decoding retains mutation graphs
and no inspection workspace per prefix. The corrected composition keeps all
variable graphs, copies and canonical allowances and reserves one conservative
inspection window per segment that contains prefixes. General provider admission,
the original conservative message-size model and all ceilings remain unchanged.
Focused check 1403 passes a genuine 32-command V7 graph at the same retained memory
pressure: its decode reservation falls from 158,686,512 to 28,663,088 bytes, with
full equality, checksum refusal and scratch release asserted. Both existing dense
canonical-value checks also pass, including refusal before decoding when the
unchanged recovery window is insufficient. Focused check 1409 also passes the
corrected structural-vector test, scoped Clippy, handbook and allowed-path checks;
no fixture bytes or production parser checks change.

The ordinary daemon's closest full-seed check 1404 acknowledges 61,887 of 65,536
tickets before another `AuditUnavailable` failure; retention completed 26 steps
covering 6,492 sequences. Private reproduction 1406 confirms ordinary pressure:
451,346,308 bytes charged and an additional 23,211,824-byte decode reservation
refused. Its retirement queue has no pending population destruction. Cold rebuild
1408 of an owned copy also refuses, with one owner and no segment crossing the
unchanged 64-MiB recovery window. This establishes a native footprint problem.

Segment metadata reserves four times its manifest vector's element storage even
though construction preallocates the final count once and never grows it. The
corrected lifetime allowance counts that allocation once; conservative event-map,
key, wrapper, task, descriptor and header allowances remain. Closest cold rebuild
1410 now succeeds against the same failed input, retaining 420,713,796 charged
bytes. The ordinary release daemon in closest check 1411 acknowledges all 65,536
seed tickets and completes 100 business commands and 400 reads without retries;
its one-second diagnostic then correctly fails the required provider telemetry
coverage, whose native period is 60 seconds. This is stage evidence only, not a
passed preparation receipt. Future native-observed diagnostics need a long enough
window to capture the periodic samples; required qualification remains unchanged.
Focused check 1412 confirms the exact-manifest regression fails under the old
reservation and passes with the correction, including every membership field,
alias retention and charge release only after physical retirement. Payload,
shared-scratch, dense-window and structural-vector checks, three-crate Clippy,
allowed paths and handbook checks pass. Public component 1417 then uses the
ordinary main-synced binaries to reconcile all 65,536 tickets and 65,636 archive
records, including payloads and policy anchors, on a byte-verified owned copy.
Original inputs remain unchanged. This closes the omitted semantic stage without
qualifying the failed telemetry receipt or replacing full preparation. Scope
check 1419 proves every branch path after removing redundant locator-record
path globs: required ADR records are already permitted by the checker, and the
existing source-path scope remains unchanged. Final-source qualification is open.

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

Accepted ADR-0319 introduces a 512-MiB aggregate native locator-memory pool per
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

The saved-population restore diagnostic exposed a native commit-path mismatch:
consumer recovery was admitted while derived indexes were dormant, but its
direct commit still required ready indexes. Publication succeeded and runtime
construction then refused before readiness. The native regression reproduces
this with a genuine restored incarnation and pending source, without manually
completing a directory.

The correction uses the existing closed mutation proof for the three metadata
attributions already admitted during bounded startup. A dormant source permits
only a transition without a locator delta or execution-failure census; its net
mutations must preserve command membership and decoder identity. Invalid
indexes still refuse. The durable successor retains explicit pending ownership
and requests asynchronous construction. Tests check consumer incarnation and
revision, exact receipt advancement, old-source refusal and successor progress.
The recovery test fails with typed unavailable before the fix and passes after
it. The negative history-identity test, all three local consumer tests, six
captured-source builder tests, workspace formatting and handbook checks pass.
Public restore timing and the complete rehearsal remain separate gates.

1. Retain the separate failed outcome/provenance regressions and original evidence.
2. Apply the accepted ADR-0319 budget/readiness policy and registered scope.
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
