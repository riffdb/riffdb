# ADR-0104: Journal-Authoritative Published State Overlay

- **Status:** Accepted
- **Date:** 2026-08-08
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `STO-001`, `STO-002`, `REC-001`, `REC-002`,
  `TXN-040`, `TXN-041`, `TXN-042`, `TXN-043`, `TXN-044`, `PERF-004`,
  `PERF-005`, `PERF-007`, `PERF-008`, `PERF-009`, `PERF-010`, `PERF-015`,
  `PERF-016`, `PERF-017`
- **Related work packages:** `WP-486`, `WP-487`, `WP-488`, `WP-489`, `WP-490`
- **Amends:** ADR-0004, ADR-0035, ADR-0053, ADR-0061, ADR-0070,
  ADR-0082, ADR-0098, ADR-0101, ADR-0102, ADR-0103
- **Amends specification requirements:** `PERF-015` and `PERF-017`

## Status boundary

This accepted record changes materialized-state authority, transaction-current
reads, and the operational read root. WP-486 passed its mechanics gate before
exact-text acceptance. Production may select the path only after WP-487 through
WP-490 establish the required read views, writer publication, checkpoint and
recovery behavior, and performance evidence. The hardened profile is unchanged.

## Context

ADR-0101 already defines standard-profile authority as one known-durable redb
checkpoint plus the exact durable journal suffix. RiffDB nevertheless applies
each command graph to a redb `Durability::None` transaction before it constructs
and fences the authoritative journal frame. The journal is write-ahead in
authority but write-behind in neither mechanics nor publication.

A fresh proxy-free 32-client application comparison measured about 23,762
operations per second through RiffDB and 46,204 through PostgreSQL safe-app.
The matching write-only slices measured about 4,732 and 11,255 operations per
second. RiffDB's write-only path retained about 9,128 durable bytes and caused
about 71,407 process-write bytes per successful mutation. One interactive run
caused about 112,830 process-write bytes per successful mutation while
PostgreSQL safe-app emitted about 1,995 WAL bytes per successful mutation.

The preallocated journal fence is no longer the dominant mechanism. Its same-
host median is about 0.9 milliseconds. In the write-only slice the writer spent
about 2 milliseconds per redb group before that fence. Increasing completion-
edge coalescing previously improved application throughput by only 3.8 percent.
WP-485 also rejected storing complete state transitions in a redb segment: it
reduced table work by only 1.08--1.79 times, below its predeclared 2-times gate.

The remaining large lever is to stop synchronously materializing the already-
authoritative suffix into redb before acknowledgement. This is not a second WAL
and does not replace redb. It uses ADR-0101's existing closed, checksummed,
gap-free mutation frames as the authority they already are, publishes an exact
bounded materialized view of that suffix, and checkpoints those mutations into
redb behind the acknowledged frontier.

## Proposed decision

### 1. Standard authority remains checkpoint plus suffix

The authoritative standard-profile database remains exactly:

1. one known-durable redb checkpoint at application and administration
   frontiers `C`; and
2. one complete gap-free durable ADR-0101 journal suffix `(C, P]` ending at the
   published frontier `P`.

The journal frame bytes, ordering, transition validation, hash chain, extent
generation, acknowledgement guarantee, backup unit, and replication boundary
do not change. The optimization changes when redb materialization occurs, not
what makes a command durable.

Redb remains the only checkpoint engine. The overlay is neither an independent
authority nor recoverable without the checkpoint-plus-suffix pair. It is an
immutable validated materialization of that exact suffix and may always be
rebuilt from it.

The hardened profile continues to apply one complete group through a two-phase
redb `Immediate` commit and never selects the overlay path.

Every standard-profile command, including an idle singleton, uses the journal
and overlay path without a batching delay. This amends ADR-0101's direct-redb
idle-singleton rule. The singleton still receives an independent journal fence
and acknowledgement; it is never parked merely to await sibling work.

### 2. One frozen operational read view

One published standard-profile read view owns, as one immutable object:

- the redb read transaction at checkpoint `C`;
- the ordered validated overlay for every suffix transition `(C, P]`;
- the database identity, history incarnation, registry digest, checkpoint
  application and administration frontiers, published application and
  administration frontiers, and terminal journal-frame hash; and
- the exact completeness proof that the overlay begins at the checkpoint's
  successor and ends at `P` without a gap, duplicate, substitution, or omitted
  table mutation.

Every operational point read, scan, named RiffQL query, command preparation,
transaction-current validation, outbox/projection/consumer read, event read,
and subscription hydration captures one such view. One request never combines
a checkpoint from one view with an overlay or frontier from another.

The overlay is keyed by the same canonical table key bytes used by redb. Each
entry contains the newest transition at or below `P`: either a complete
canonical value or an explicit tombstone. A point read consults the overlay
first and falls back to the captured checkpoint only when the key is absent.
A tombstone is authoritative absence and never falls through.

A bounded range or index scan performs a canonical ordered merge of checkpoint
members and overlay entries. Overlay values replace equal checkpoint keys;
tombstones remove equal checkpoint keys; overlay-only values enter at their
canonical position. Bounds, partition filters, row limits, cursors, scan
fences, field authorization, corruption checks, and result ordering remain
unchanged. The merge must stop at the existing bounded result and work limits;
it may not materialize an unbounded union.

The index generation returned as the scan fence is read from that same frozen
view after applying the identical overlay-first/tombstone rule. Checkpoint
compaction does not itself advance a logical index generation. A continuation
therefore observes the same generation semantics as today and still rejects a
real intervening mutation rather than treating physical overlay compaction as
an application change.

Derived exact command-segment locators remain governed by ADR-0102. Their
published generation is bound to the same view and cannot claim a key beyond
`P` or omit one at or below `P`.

### 3. Writer-private staging and durable publication

The sole commit coordinator remains the only sequence allocator and ordered
authoritative writer. For each accepted FIFO group it:

1. evaluates and transaction-current validates against one writer-private view
   consisting of the published view plus every earlier complete unpublished
   transition;
2. constructs and fully validates the same closed command segment, entity/index
   post-images, generation advances, audit, outcome, event, route, outbox,
   provenance, allocator, and other journal mutations required by ADR-0101 and
   ADR-0102;
3. applies those exact mutations to a bounded writer-private overlay builder,
   without opening or committing a redb write transaction;
4. seals and submits the ordinary immutable journal frame; and
5. withholds every result and effect until the journal lane reports the frame's
   durable fence.

Sequence assignment remains invisible until the fence in step 5 succeeds. A frame
construction, validation, capacity, or submission failure before durability
publishes neither sequence nor state. A later command observes each complete
earlier private transition in FIFO order even when their journal fence is still
in flight. It never observes a partial graph or a later transition.

After a known-successful fence, storage verifies the covered frontiers and
frame hash, freezes the covered overlay prefix, and atomically publishes one
new read view before releasing any command outcome, audit result, notification,
transient index, projection work, outbox work, subscription update, or public
response. One successful compare-and-publish changes checkpoint/overlay/frontier
identity together; a stale publisher is an invariant failure that fences
writes.

Sharing one fence or overlay generation does not create a public multi-command
transaction. Every command retains its independent idempotency identity,
outcome, audit, provenance, event, retry, cancellation, and uncertainty result.

### 4. Bounded suffix and backpressure

The published overlay and the complete durable-but-uncheckpointed suffix retain
ADR-0101's maximum 4,096 transitions and 16 MiB of encoded journal frames. The
writer-private unpublished prefix retains its independent 256-transition and
16-MiB caps. Overlay keys, values, tombstones, manifests, and indexing overhead
receive a separate conservative in-memory charge derived before admission; the
closed POC ceiling is 64 MiB.

Checkpoint work begins before any hard bound is reached. If the checkpointer
cannot reclaim sufficient headroom, authoritative writer admission applies
bounded backpressure before accepting a command that could exceed any encoded,
transition, overlay-memory, extent, or retained-read-root limit. It never drops
old overlay entries, acknowledges beyond the bound, performs an unbounded
checkpoint, or converts pressure into a false successful outcome. Reads remain
available from the last published view while write admission is backpressured.

These are compiled safety ceilings, not operator-tunable correctness knobs.

### 5. Ordered asynchronous redb checkpoint

One storage-owned checkpointer is the only component that materializes a
published suffix into redb. It captures one published view ending at `P`, opens
one redb write transaction rooted at that view's checkpoint, and applies every
covered journal mutation exactly once in writer order using the ordinary table
validators. It then writes the checkpoint application and administration
frontiers, terminal frame hash, history incarnation, registry digest, and
format identity in that same transaction and commits with one-phase
`Durability::Immediate`.

Before commit, any mismatch between the redb checkpoint, journal predecessor,
overlay value, canonical mutation, allocator, segment manifest, generation, or
expected prior value aborts the checkpoint and fences authoritative writes.
The checkpointer never reruns command logic and never invents mutations from
the overlay.

After known commit success, storage opens and validates the successor redb read
transaction. It may publish a compacted read view that removes only the exact
overlay prefix now proven present in that checkpoint. Concurrent newer durable
overlay entries remain attached in order. Journal extent reclamation or
generation recycling occurs only after that compacted view is published and
the checkpoint-plus-retained-suffix pair is independently sufficient for
recovery.

A checkpoint error does not revoke already acknowledged commands. It fences
new authoritative writes, preserves the last complete checkpoint plus journal
suffix, keeps reads on the last valid published view when safe, and returns a
typed degraded-storage/operator state. An uncertain redb checkpoint commit is
resolved on restart from its embedded frontiers and terminal frame hash; the
journal is never discarded merely because the redb commit may have succeeded.

### 6. Redb-writing barriers

Catalog, contract, query-module, reactive-module, capability, other
administration, offline maintenance, backup publication, shutdown, and
hardened-profile operations remain non-bypassable barriers. Before any such
operation opens a redb write transaction, the writer drains every unpublished
journal fence and the checkpointer applies the complete published suffix to an
Immediate redb checkpoint. The operation begins only from the validated empty-
overlay successor view.

Failure or uncertainty while draining or checkpointing prevents the barrier
operation from starting and retains its existing public failure class. A
barrier never writes an administrative successor onto a redb root that omits an
acknowledged application or service-audit transition. Ordinary reads do not
force this drain and remain available from the published composite view.

### 7. Crash recovery and corrupt states

On standard-profile startup, storage validates the redb checkpoint and scans
the journal exactly as required by ADR-0101 and ADR-0103. It deterministically
rebuilds the immutable overlay from the complete suffix, publishes that
composite view, and may then become ready without first rewriting the suffix
into redb. The ordinary bounded checkpointer starts after readiness. Startup
does not choose between replay policies based on timing, suffix size, or an
operator knob. Repeated restart is idempotent.

Recovery rejects: a suffix gap or duplicate; a wrong predecessor frontier or
hash; a frame/database/history mismatch; a canonical mutation whose expected
prior value differs from the reconstructed view; an overlay/checkpoint frontier
mismatch; a redb checkpoint frontier without the matching terminal frame hash;
mixed extent generations; a reclaimed frame still required by the checkpoint;
or any incomplete nonterminal frame. Only ADR-0103's permitted torn terminal
frame may be ignored.

Process crash coverage arms at least: before frame submission; during the
journal write and fence; after fence before overlay publication; after overlay
publication before response; during redb apply; during redb `Immediate` commit;
after checkpoint commit before compacted-view publication; during journal
reclamation; and after reclamation before the next frame. Every restart exposes
the predecessor or the complete durable successor, never partial state.

### 8. Backup, maintenance, retention, and replication

Offline backup captures the known-durable redb checkpoint plus every required
journal extent as one verified unit, as ADR-0101 and ADR-0103 already require.
It need not force synchronous checkpointing merely to empty the overlay.
Restore validates or replays that unit before readiness. Online maintenance is
not introduced by this record.

Retention may rewrite or remove journal-backed history only after the accepted
ADR-0102 materialization and watermark rules prove that the new checkpoint and
retained suffix preserve every required authoritative view. No checkpointer
shortcut may bypass retention eligibility.

Replication and changelog frames continue to derive from published durable
frontiers under ADR-0100. Physical overlay entries and journal extent bytes are
not a replication format, CDC surface, or public API.

## Normative specification amendments

The accepted change is also encoded in `SPEC.md`; updating the ADR alone is
insufficient.

`PERF-015` previously required every operational read to use an atomically
published last-durable **redb snapshot**. It now requires one
atomically published last-durable **composite read view** containing the exact
redb checkpoint and gap-free immutable journal overlay. Applied-but-unfenced
overlay state remains writer-private. It also removes the requirement that
an idle standard-profile singleton use the direct redb Immediate path; the
singleton instead uses one immediate journal submission with no batching wait.

`PERF-017` previously required the final covered redb read snapshot to publish
after a journal fence and recovery to replay and checkpoint the suffix before
readiness. It now requires publication of the final covered
composite read view and permits readiness after deterministic complete overlay
rebuild. The exact checkpoint-plus-suffix authority, frame validation,
acknowledgement fence, suffix bounds, FIFO order, barrier drain, backup unit,
and corruption behavior remain unchanged.

## Consequences

- The common standard-profile acknowledgement path performs bounded command
  evaluation, exact mutation/frame construction, one journal positional write,
  and a grouped journal fence; it performs no synchronous redb page apply.
- Redb remains the first storage engine, durable checkpoint, MVCC base, backup
  component, and hardened-profile oracle.
- Current-state reads gain a bounded ordered overlay merge. This is accepted
  only if WP-486 proves its point/range overhead and checkpoint bound.
- Checkpoint failure becomes an explicit degraded-storage state rather than an
  acknowledgement failure for commands already durable in the suffix.
- Memory use, retained redb roots, suffix transitions, journal bytes, and merge
  work remain independently bounded and fail closed.
- No SQL, arbitrary transaction callback, second writer, or external WAL is
  introduced.

## Rejected alternatives

- **Continue scheduler and allocation tuning.** Current evidence requires about
  a two-times application improvement; measured scheduler changes are single-
  digit gains.
- **Make a state-bearing redb segment.** WP-485 measured only 1.08--1.79 times
  less table work and rejected it under the predeclared gate.
- **Acknowledge a non-durable in-memory overlay.** A crash could erase accepted
  state and is prohibited.
- **Treat the overlay as a second authority.** It must be exactly rebuildable
  from checkpoint plus suffix or readiness fails.
- **Let reads consult redb's newest root.** They could observe an unfenced or
  partially checkpointed transition.
- **Apply checkpoints out of FIFO order.** Expected-prior and frontier proofs
  would no longer establish one database history.
- **Replace redb.** The proposal removes redb from the synchronous standard
  acknowledgement path while retaining it where its MVCC and durability are
  valuable.

## Acceptance gate

This record may become Accepted only after WP-486 demonstrates all of:

- at least 2.0-times lower median pre-acknowledgement apply work for 32- and
  128-transition distinct-create, retained-update, and mixed windows;
- merged point and bounded index-page reads no slower than 1.25 times the
  checkpoint-only control at the maximum overlay bound;
- one bounded checkpoint correctly clears a 4,096-transition/16-MiB suffix;
- no production selector exists before exact-text acceptance; and
- the maintainer explicitly accepts this authority, read-merge, ordering,
  backpressure, checkpoint, recovery, and degraded-state contract.

## WP-486 mechanics evidence

The benchmark-only gate passed on 2026-08-08 with five repetitions per case.
Across distinct-create, retained-update, and mixed 1,024-command windows,
physical groups of 32 and 128 measured 2.30--2.97 times lower median pre-
acknowledgement work. The comparison includes exact current journal mutation
and frame construction on both sides and excludes the common positional write
and durability fence.

At the 4,096-transition overlay bound, point reads measured 0.38 times the
checkpoint-only control and bounded 50-row ordered merges measured 1.11 times
the control, with equal semantic checksums. One immediate checkpoint of that
suffix completed in 27.43 milliseconds. The overlay-apply interval initiated
no process writes; the checkpoint initiated about 23.56 MiB. These are
benchmark-only synthetic application-value shapes through the real redb table
definitions and journal frame encoder, not production enablement or a public
performance claim.

The mechanics evidence satisfies the quantitative prerequisite. The maintainer
accepted this exact authority, ordering, recovery, and read-validation decision
on 2026-08-08. Production selection remains gated by WP-487 through WP-490.
