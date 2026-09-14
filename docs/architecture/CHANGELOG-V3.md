# Authoritative changelog V3 substrate

The offline retention-watermark stamp now validates retained V3 history and
source holds on its original hardened write pin before an equal-value retry can
return. A changed watermark prepares its exact expected-state `RetentionPrune`
receipt before mutation, then stages the receipt, allocator and tail in that
same transaction. No commit or flush is added. It preserves the existing
watermark normalization and recorded chain-root digest behavior; a nonzero
watermark cannot fabricate a missing rooting digest. Same-lineage history and
source/follower state are not reset. Crash tests cover preflight, watermark
mutation, receipt staging and completed commit with repeated recovery/retry.
The shared transitional activation routing and higher-level restore owners
remain open integration work, not guarantees proved by this stamp.

Under ADR-0207, V1/V2 emitter construction and derivation now live only in
`tests/storage_recovery/changelog_compatibility.rs`. Their original decoder,
unit and process-recovery evidence remains, using the canonical storage codecs
and frozen bytes. Production retains the published-snapshot adapter and bounded
V3 receipt cursor; there is no production legacy-emitter factory or format
selection. The separate supported application-export implementation and public
export behavior are unchanged. This move alone does not complete production V3
replication: the remaining WP-772 substrate obligations and later emitter/RPC
packages still gate activation.

The existing offline incarnation stamp now reanchors an already-active V3
database in its original hardened transaction. It streams retained history and
source holds and checks follower/lifecycle bindings before mutation, preserves
database identity, leadership epoch and the dual frontier, clears source holds
and old-lineage history, detaches follower progress, removes the old lifecycle
record, and installs one empty `RestoreAnchor` at sequence 1. A present retention
watermark is rebound without changing its original chain-root digest. Equal
incarnation retries validate and do not write; backwards incarnations refuse.
Actual process tests cover preflight, incarnation, local reset, anchor staging
and committed edges, with repeated recovery/retry and unchanged unrelated
authoritative rows. These prove the stamp, not every higher-level restore
publication/staging owner. Fresh activation and unconditional refusal of a
current-registry database with every V3 root erased remain open integration
work; the stamp retains the same transitional presence routing as other owners.

The source-only hold table now has a distinct bounded V1 codec (compact tag 73,
revision 1): a closed follower acknowledgement, archive acknowledgement or
bootstrap kind, a nonzero 16-byte opaque hold ID, and an exact lineage-bound
history position/hash/dual frontier. The canonical 17-byte key repeats kind and
ID. The payload ceiling is 163 bytes; the combined hard count ceiling is 4096,
which also bounds each consumer kind. These IDs are not NodeIds or capabilities.
Complete startup validates every hold against the retained chain from the same
read pin before lifecycle writes, refusing unknown, substituted, foreign,
out-of-range or excessive holds. CLEAN eligibility retains its bounded-root
check rather than scanning the hold population. Construction or decoding alone
grants no acknowledgement, registration or reclamation authority. Production
hold registration and history reclamation remain unimplemented. The maintainer
reviewed and approved the three source-hold codec vectors and these format notes
on 2026-09-14; that fixture review does not close WP-772 or accept another ADR.

Real command-owner tests now cover one direct multi-command group followed by
separately published journal overwrite and delete transactions. Original entity
bytes remain available through pinned receipt cursors after the live row is
gone, checkpointing preserves those exact receipts, and repeated full reopen
retains command outcomes, provenance, events and idempotency identities. Two
logical subgroups in one journal epoch produce one physical net receipt while
both complete command identities survive. Five actual process-crash arms cover
direct pre/post-commit, private journal staging and published journal-tail
completion, with two full recovery passes each. These are isolated-activation
fixtures, not fresh activation or the complete rotation/reclamation crash matrix.

The V3 checkpoint receipt checks the V2 certificate's exact physical `AUDIT`
bound against the same read snapshot. That bound can be below the V3 logical
administration frontier because command-owned audits reside inside command
segments. The latter remains the receipt's full dual frontier; neither existing
certificate bytes nor audit placement changes. A substituted physical bound
still refuses, and no additional read transaction or commit is introduced.

The dormant storage opener recognizes complete, already-activated V3 layouts
using the catalog's exact physical table set and bounded current-format roots
before selecting any legacy migration or additive-table repair. Real reopen
tests preserve the original history and perform no durable write. Nine malformed
layout arms (missing history, holds, entity or locator tables; missing epoch;
old registry or format; unknown table or metadata) refuse on repeated open and
leave physical rows unchanged without creating a journal. This is not fresh
activation or complete startup validation: those owners and the all-roots-erased
current-registry refusal still require integration before WP-772 can close.

For an already-activated database, the actual structural startup session now
uses that same catalog-derived table inventory and the closed V3 metadata
decoders. When CLEAN is unavailable, it streams the entire retained receipt
interval from the session's pinned view before any successful validation can
write a prefix checkpoint or DIRTY transition. Valid CLEAN still takes the
bounded-root branch without that history walk. Tests run full validation and
two bounded clean reopen/consumption cycles with exact retained receipt bytes;
missing or corrupt interior history refuses before writes. Eight process exits
in the real startup/close orchestration prove old-or-complete checkpoint and
lifecycle receipts, repeated dormant reopen, and actual validation retry. These
fixtures start from isolated activation with empty entity and source-hold
populations. Separate full-startup source-hold tests cover all three valid kinds,
substitutions and the exact count boundary. They do not prove fresh activation,
every authoritative population, or the remaining retention/rotation obligations.

Offline operator hold additions, replacements and removals now use the same
sealed receipt capture as audited projection hold changes, inside their existing
single hardened transaction and before/after hooks. Offline preparation validates
retained V3 history before granting the first write; no operational startup proof
is assumed. Tests preserve exact prior values and receipts across replacement,
removal, no-op retry, projection audit/allocator updates, commit uncertainty,
interior corruption refusal and four actual offline process-crash edges. This
does not implement replication source holds and history reclamation.

Authoritative offline pruning also captures receipts in each original transaction:
checkpoint invalidation and each bounded delete/tombstone/watermark subrange.
Retained idempotency, provenance and audit view replacements use captured tables
too. Tombstone digesting measures and hashes the same transaction-current rows
in two streaming passes, preserving v1 framing without buffering the preimage.
Retained delete/rewrite plans and segment-event deduplication have checked limits;
oversized plans refuse rather than split an authoritative transaction. These
scans still follow retained history; this is a memory bound, not constant-time
pruning. A populated semantic fixture proves exact deletion preconditions,
watermark/tombstone atomicity, original receipt retention, independent v1 digest
equality, no-op retry, full-prune startup, uncertainty, and four actual process
crash edges. It is not the production command-group/segment crash matrix or
permission to reclaim V3 receipt history; those proofs remain open.

Catalog-owned index migration batches and current-registry epoch repair now
capture receipts in their original hardened transactions. Generation insertion,
legacy epoch removal and the one-shot marker remain separate existing commits;
unchanged confirmations do not allocate receipts. Tests cover exact original
preimages and postimages, original commit counts, stale-compare whole-batch
rollback, precommit and uncertain outcomes, and four actual catalog-driver
process exits followed by repeated open, retry and startup validation. These
fixtures use isolated activation with legacy index rows to exercise the owners;
they do not replace the required full validation before production activation
or prove every same-lineage contract migration and lineage-reset path.

WP-772 is in progress. The storage API now has a closed namespace catalog,
checked physical transaction allocator, expected-state mutation values, and
V3 receipt/frame codecs. Constructing these internal values does not activate a
production replication stream or a writer. Do not treat codec tests or the
synthetic format vectors as evidence of durable receipt publication.

## Authority inventory

`riffdb-storage-api::AuthoritativeStateCatalogV1` owns the classification.
The generated `fixtures/replication/authoritative-state-catalog-v1.txt` is its
review artifact, not an alternate configuration. A redb layout test compares
the existing inventory with every current table and exact metadata key. Seven
additional namespaces are reserved in that declaration for ADR-0186's receipted
V3 activation; they are not created by the codecs or made eligible at startup.

Unknown table names, namespace tags and metadata keys are refused. Metadata
classification uses complete key equality, not a prefix or table-wide default.
Only projection rows and apply markers are classified as rebuildable under
ADR-0017's exact historical-plan and contiguous-log owner. Missing rebuild
inputs never authorize fabricated rows or readiness. The mixed
`projection_frontier` namespace remains authoritative: it stores lifecycle,
highest-generation and retention-fence state, not merely derived observations.
Outbox delivery, consumer delivery, locators, validated-prefix evidence and
vector/columnar controls likewise remain authoritative; the catalog does not
infer rebuildability from their names.

## Bounded canonical values

`ChangelogTransactionSequence` is nonzero and independent of application and
administration sequences. Its allocator is `Next(nonzero) | Exhausted` and
uses checked arithmetic. Allocation only returns the state that a future
storage transaction must persist atomically; it performs no mutation itself.
Its durable codec is the distinct `StoredChangelogTransactionAllocatorV3`
envelope (compact tag 68, revision 1), with an 11-byte maximum Protobuf payload.
The journal precondition hashes the complete canonical envelope. Bare counters,
application/admin allocator identities, missing states and zero `Next` refuse.
First, maximum and exhausted envelope vectors are frozen separately.

The catalog and leadership codecs have distinct retained identities (tags 69
and 70, revision 1), rather than reusing a registry digest or incarnation record.
The catalog accepts only the single owned inventory's exact digest; a foreign
catalog returns a value-free incompatible-format error. `LeadershipEpochV1` is
nonzero and has checked advancement, with no successor after its maximum.
Frame bindings use that checked type without changing any V3 frame bytes.
Neither codec grants activation or leadership. The history and follower root
codecs use tags 71 and 72, revision 1, with maximum Protobuf payloads of 281 and
215 bytes. History binds lineage, anchor, materialized tail and minimum resume
points, each with an exact receipt hash and dual frontier. Equal positions
cannot substitute another hash or frontier. Follower state is explicitly
detached or attached, with applied and optional acknowledged positions;
acknowledgement cannot outrun applied state. Decoding roots grants no
publication, ancestry, startup-readiness or reclamation permission.

An isolated hardened installation primitive now atomically installs all five
roots, the two empty/new control tables, and the sequence-one activation receipt.
Its only authoritative mutation is the exact registry replacement, when needed;
it preserves application/admin allocators and never synthesizes older receipts.
Process-exit tests cover preflight, uncommitted roots, uncommitted receipt and
completed commit, plus retry/reopen and partial-state refusal. This primitive
is **not called by production startup**: full-validation integration and every
post-activation writer/recovery lane must be completed first. These isolated
transaction tests do not discharge WP-772's end-to-end crash obligations.

The bounded checkpoint-root reader uses one pinned redb read transaction. It
checks all five roots, both control-table presences, the exact terminal receipt,
core lineage and dual frontiers, and allocator agreement without scanning
history or entity rows. Only the exact predecessor registry with every V3
root/table absent is inactive. A V3 registry with erased roots, partial state,
or substituted bindings is corruption, never an initialization or repair hint.
This consistency check is not yet wired into clean eligibility and does not
grant readiness or replace complete startup validation.

The existing delete-aware entity checkpoint fingerprint now uses exact-length
streaming SHA-256 v1 framing. Measurement and hashing traverse the same pinned
head table in canonical key order; they retain one bounded head preimage and
prior target, not the population. The domain, length framing, field order and
digest bytes are unchanged. Checkpoint-head copying preserves byte-identical
rows, replaces changed rows and removes stale rows in the existing transaction.
These current-state scans still scale with the entity population; this is a
bounded-memory change, not a constant-time checkpoint or a V3 receipt proof.

An isolated Immediate receipt owner now opens a fresh write transaction, checks
the current roots and every exact prior value before applying any mutation,
and stages the receipt, allocator and tail together. The transaction cannot be
handed back for additional writes. It preserves the existing Standard/Hardened
commit profile. Missing tables, stale predecessors and mismatched physical
frontiers refuse; the write-root reader never creates absent control tables.
Tests cover pinned readers, complete original put values retained after later
deletion, and process exits after mutations, receipt, roots and commit. These
are physical transaction tests, not operation authorization/source-validation or
production activation evidence. Journal, lifecycle and lineage ceremonies are
explicitly refused by this owner pending their separate integration.

The same private owner also supports mutation-time capture within one owned
Immediate transaction. Its tables retain exact original preconditions and net
post-images through the shared accumulator; they expose no mutable raw-table
escape. Unknown or missing tables, replication-control writes, and whole-receipt
overflow refuse and poison the operation. Finishing seals that same transaction,
checks the unchanged control predecessor and actual allocator post-images, and
hands back only a prepared commit. No-op captures do not allocate a position.
Process tests cover exits after captured mutations, receipt, roots, and commit.
These owners are not yet wired through all operational, journal, migration, and
lifecycle paths; they do not by themselves complete WP-772 or activate V3.
Operational direct-entry call sites now name their closed source attribution
before writer admission and carry it with the existing write access. Journal
epochs retain their separate source path. The operational transaction is now
the captured owner: it takes the caller's existing Immediate transaction after
draining the published journal suffix into that same transaction. Capture starts
at the resulting exact history predecessor, so the drained suffix is not counted
as another direct operation. The existing commit path seals the direct receipt
before its one durable commit and preserves frontier publication, checkpoint
completion, coverage bookkeeping and transient-index effects. Receipt refusal
before commit aborts the candidate and restores the original journal source;
actual commit failures retain their existing uncertainty fencing.

The hardened service-adapter regression proves one durable epoch advance for a
complete audit group, exact stored receipt post-images and no allocation on a
phase conflict. Process exits after mutations, receipt and roots leave neither
the audit group nor its receipt committed; exit after commit retains both on
repeated raw reopen. A mixed journal/direct test proves a poisoned candidate
aborts the drained suffix too, then a retry materializes the byte-identical
journal receipt and the direct successor in one commit. Its deliberately invalid
candidate row tests physical rollback, not application semantics. Audited offline
projection-hold changes also use capture around their original transaction.
Other offline retention, migration, startup, lifecycle and rotation owners still
need integration and their own proofs. All these tests use isolated V3 activation;
they do not establish a deployable startup path or close WP-772.

The existing DIRTY activation/clean-consumption writer and final CLEAN writer
now preflight one checked V3 allocation before changing the lifecycle row, then
stage its empty, exactly attributed control receipt in that same Immediate
transaction. Lifecycle, allocator, tail and receipt remain atomic; no write
follows final CLEAN. The lifecycle V1 bytes and binding stream are unchanged.
With V3 controls present, clean eligibility additionally checks the bounded
catalog/lineage/allocator/tail/terminal roots and room for the next allocation;
malformed roots decline the shortcut without authorizing repair or activation.
Actual-writer tests cover DIRTY, CLEAN, exact clean consumption, stale repeated
consumption refusal, old pinned roots and missing-terminal refusal. Process
tests exit before/after each original commit and verify old or complete control
state through repeated raw reopens. Complete startup fallback and production
activation remain open; no-V3 routing is still transitional, not permission to
treat erased current-registry roots as an inactive database.

The checkpoint receipt planner reads that same pinned source view. It validates
the current entity proof, collects only changed checkpoint-head bytes, preserves
exact delete/replacement preconditions, and includes the chained checkpoint
metadata in the complete bounded receipt. Identical stored proofs are no-ops;
wrong parents and malformed old rows refuse. A delta larger than one receipt
refuses without publishing a partial snapshot. With V3 controls present, the
actual checkpoint writer now selects that plan from its builder's same pin.
The sealed writer opens one hardened transaction, rechecks exact history and
every expected prior value before mutation, and writes the planned checkpoint
rows and receipt together. This replaces the raw legacy writer for that attempt;
it adds no transaction or flush and preserves the existing before/after commit
hooks. Even the exact-checkpoint fixture no-op refuses malformed V3 roots.
Actual-writer tests prove one epoch per checkpoint, original receipt bytes after
a later checkpoint overwrites the singleton, no-op behavior, precommit rollback,
postcommit uncertainty and four process-crash edges with repeated raw reopen.
These fixtures use an empty authoritative entity population and isolated V3
activation; existing physical head-delta tests prove changed/deleted head bytes
and bounds, not full startup validation. The caller still owns complete prefix
validation, the drained gate and activation. WP-772 remains open.

Registering this additive codec changes the registry digest but is **not** a
completed database upgrade. The pre-V3 registry digest remains frozen in a
compatibility test; its validated, atomic activation migration remains a release
blocker within WP-772. Do not deploy this intermediate implementation against an
existing database or treat fresh-store codec registration as V3 activation.

Receipts retain complete put values, expected absent/prior-hash states, exact
delete preconditions, strictly ordered unique namespace/key transitions,
lineage, physical positions, dual frontiers and a closed source attribution.
Control rows cannot enter their own mutation list. Storage integration must
still prove that attribution and declared sequences match actual durable rows;
structural decoding alone does not establish that fact.

The bounded mutation accumulator retains the original precondition and final
value across repeated writes. Exact cancellation drops the redundant post-image
but retains a bounded observed-state marker so a later write cannot invent a
new predecessor. Any precondition or size failure poisons the entire result.
The journal conversion helper uses only the admitted frame and its exact
allocator mutation, never latest-row reads. Its current tests are synthetic
conversion evidence, not checkpoint durability or real command attribution.
Standalone service-audit framing now recognizes only a checked one-step physical
allocator update, alongside its existing audit metadata. It rejects activation
position one, skipped or reversed counters, missing preconditions, deletion and
premature exhaustion. This adds no journal field or table tag. Replay into an
inactive or partial V3 database still refuses before changing the counter; a
valid source shape is not an activation permit.

The recovered-source materialization primitive now applies each original ordered
mutation inside its caller's existing checkpoint transaction and stages the
checked allocator, exact receipt and history tail through the same root writer
as Immediate receipts. It adds no commit. Cancelled net mutations still require
their original before-images. Its overlap checker reads the original receipt
and predecessor from one pinned checkpoint and compares complete source-derived
bytes; later entity overwrite or deletion cannot substitute for that evidence.
The live retained-mutation converter and independently decoded journal converter
share the same bounded fold and produce identical receipt bytes without a live
frame re-decode.

The existing journal recovery entry now selects strict V3 recovery whenever a
V3 source counter or any retained V3 control domain is present. Before replay or
suffix reclamation it validates the complete retained chain and every original
overlap from one pin; it replays only the exact missing successor suffix in its
existing hardened transaction. A later direct overwrite or dual-frontier advance
does not invalidate an exact original receipt. Missing counters, skipped or
duplicate positions, partial roots, missing overlap rows and broken ancestry
refuse without reclaiming the source. An already-empty extent keeps its bounded
root check and exact-header no-op behavior; it does not scan retained history.
Process tests exit after replay staging, commit, and before/after reclamation,
then reopen and retry twice to prove an original source or identical durable
receipt remains. These tests use opaque record payloads and prove physical
recovery, not full command semantics, acknowledgement or the complete package
crash matrix. Production activation remains pending; ordinary
legacy replay checks and journal encodings are unchanged.

The live checkpoint worker now uses the same receipt fold over its retained
validated mutations, with no journal-frame decode. It stages the exact receipt
and checked roots before its existing single durable commit. The drained
direct-write barrier also materializes each original V3 source in the caller's
existing transaction; aborting that transaction leaves its predecessor intact.
The physical tests compare worker output byte-for-byte with independent recovery,
reject duplicate application and malformed final batch totals, retain old pinned
views, and exercise process exits before/after the worker's durable commit.
Neither path permits a missing source counter once V3 controls are present.
The journal admission paths now stage one checked V3 source allocation and prove
the complete bounded receipt before submitting an application or service-audit
frame. They advance only the private history after successful submission and
retain its constant-size binding beside the existing original mutations. The
worker requires that binding to match its materialized receipt; missing or
foreign bindings refuse without committing. No new flush or acknowledgement
dependency is added. The identity probe validates exactly the catalog-declared
V3 metadata envelopes; it does not replace strict cross-root validation.

The real service-adapter regression
`submitted_service_audit_allocates_one_v3_source_before_publication` covers
consecutive source allocations, duplicate-request refusal without allocation,
published audit visibility and byte-identical materialization through repeated
recovery. The metadata-refusal regression covers partial, malformed and unknown
controls before source submission. These tests use isolated fixture activation;
production startup activation and complete command/lifecycle proofs remain open.

`PublishedDurableSnapshot::changelog_receipts_v3` now opens a read-only successor
cursor bound to one exact lineage and resume point. Each call returns at most
one complete bounded receipt; it validates continuity and stops at the pin's
tail. Foreign lineage, stale epoch, substituted positions and pruned history
have closed refusals. A read failure is sticky, never permission to skip a row.
Legacy-only snapshots refuse this method without selecting a legacy format.

Journal admission attaches immutable source references to the same composite
snapshot it later publishes. These share the original retained mutation bytes
with checkpoint batches, not reconstructed latest values. Appending a source
is constant-time; opening a cursor collects only bounded source references,
and receipt decoding follows changed bytes rather than database population.
Checkpoint rebase drops only the exactly covered source prefix while preserving
newer sources and old pins. Source-chain destruction is iterative on the fixed
production stack. The cursor has no writer handle, lease acquisition, journal
I/O, or durability decision. The real service-adapter test covers old pins,
an existing direct-write checkpoint barrier plus a newer suffix, resume
refusals and reading while the writer lease is held. Separate physical tests
prove original overwrite/delete bytes, prefix-source release, materialized
history, pruning-floor pinning and sticky missing-row refusal. Those physical
tests use opaque record payloads; they do not complete the command, startup,
lifecycle, retention-authorization or full crash obligations.

The complete retained-history validator is separate from bounded clean-root
eligibility. It streams every retained receipt from the exact minimum-resume
point through the terminal root using one pinned view and constant-size chain
state. Missing rows and rechecksummed interior substitutions fail even when the
terminal root still validates. This is receipt-chain evidence only; wiring it
into complete startup alongside the existing durable-record validators remains
part of the production activation gate.

Frames carry complete contiguous receipt ranges, exact catalog and leadership
bindings, source counts, checksums and V3-only framing. The 32 MiB frame ceiling
and 256-transition ceiling remain independent; one receipt is never split to
make it fit. Receipt admission reserves the entire frame wrapper, and direct
groups over 256 logical transitions refuse before storage mutation. The
accumulator poisons the whole result when this budget is exceeded. Decoders
reject unknown versions, malformed absence, reordering,
duplicates, wrong counts, truncation, trailing bytes and broken history edges.
Debug and error text omit keys, values and payload-derived hashes.

The V1 and V2 decoders remain byte-unchanged compatibility evidence under
ADR-0207, not a fallback for production V3 replication. Standalone vectors for
all three formats are checked against fixed byte digests. Regenerate the
catalog and format vectors with `./scripts/generate-changelog-fixtures`; its
`--check` mode also runs through the generated-artifact check.

## Remaining gates

WP-772 remains open until journal sourcing, atomic direct/control receipts,
published snapshot cursors, checkpoint materialization, migration/rotation,
clean-close integration, retention fences, and the required process-crash
matrix are implemented and proven. Human review of the current authority catalog
and accompanying V1/V2/V3 substrate codec vectors has been received; subsequent
fixture changes still require review. Topology and durable-format registration,
package acceptance and full CI gates remain required. No receipt, replication,
recovery or performance obligation
is discharged by this initial codec work. WP-746 and later activation packages
remain downstream of the completed substrate.
