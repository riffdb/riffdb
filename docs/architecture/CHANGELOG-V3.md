# Authoritative changelog V3 substrate

WP-772 is in progress. The storage API now has a closed namespace catalog,
checked physical transaction allocator, expected-state mutation values, and
V3 receipt/frame codecs. These internal values do not activate a production
replication stream or alter current writers. Do not treat codec tests or the
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
matrix are implemented and proven. The catalog/format fixture review, topology
and durable-format registration, package acceptance and full CI gates also
remain required. No receipt, replication, recovery or performance obligation
is discharged by this initial codec work. WP-746 and later activation packages
remain downstream of the completed substrate.
