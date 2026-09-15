# Authoritative changelog V3 substrate

WP-772 implements ADR-0186's storage substrate and ADR-0207's frozen
compatibility boundary. Its [verification and fixture review](WP-772-VERIFICATION.md)
are complete as of 2026-09-14.
Production emitter, RPC, follower apply and bootstrap activation remain WP-746;
archives remain WP-749. Internal values grant no application mutation, format
selection, bootstrap installation or pruning authority. Application export is
supported and unchanged.

## Authority inventory and published readers

`riffdb-storage-api::AuthoritativeStateCatalogV1` owns all 62 table/metadata
domains, including 52 replicated-authoritative domains. The generated
`fixtures/replication/authoritative-state-catalog-v1.txt` is a reviewed artifact,
not configuration. Seven V3 control domains are installed by validated activation,
not by constructing codecs. Unknown tables, namespace tags and metadata keys
refuse; metadata classification uses exact keys, never a table-wide default.

Only projection rows and apply markers are rebuildable under ADR-0017's complete
historical-plan/contiguous-log owner. Missing rebuild inputs cannot authorize
readiness. Mixed projection-frontier state, delivery state, locators,
validated-prefix evidence and vector/columnar controls remain authoritative.

`PublishedDurableSnapshot::authoritative_state_v3` returns one bounded row or
exact namespace end per call, including empty namespaces, at one V3 fence.
Callers cannot select partial inventory. Journaled tables and metadata use the
original immutable overlay's overwrite/tombstone semantics; other rows use the
same checkpoint. Borrowed bounds precede copying; errors are redacted and fused.

`PublishedDurableSnapshot::changelog_receipts_v3` reads exact successors through
that pin's tail. Lineage, epoch, position, hash and frontier are checked; pruned
positions return typed `history_pruned`. Missing rows cannot be skipped, and
inactive/legacy snapshots refuse without fallback. Old pins retain original
bytes through overwrite, deletion, checkpoint and reclamation. Neither cursor
owns a writer, gate, journal-I/O lane or durability decision.

## Frozen formats and bounds

The independent nonzero `ChangelogTransactionSequence` orders physical
transactions, not application commands. Its checked `Next(nonzero) | Exhausted`
allocator never wraps. `StoredChangelogTransactionAllocatorV3` is compact tag
68, revision 1, with an 11-byte maximum Protobuf payload. Journal preconditions
hash its complete canonical envelope; bare counters, zero, missing state and
another allocator identity refuse.

Catalog and leadership roots use tags 69/70, revision 1. Catalog accepts only
the owned inventory digest; leadership is nonzero with checked advancement.
History/follower roots use tags 71/72, revision 1, with 281/215-byte maximum
payloads. History binds lineage, anchor, tail and minimum resume with exact
receipt hashes/frontiers. Follower state is detached or attached; acknowledgement
cannot outrun applied state. Decoding grants no ancestry or readiness proof.

Source holds use tag 73, revision 1: follower acknowledgement, archive
acknowledgement or bootstrap; a nonzero opaque 16-byte ID; and exact lineage,
position, hash and dual frontier. The canonical 17-byte key repeats kind/ID.
Payloads are at most 163 bytes and the combined count is at most 4,096.
IDs are neither NodeIds nor capabilities. Full validation checks every hold
against the same retained chain; constructing one grants no authority.

Receipts carry strictly ordered namespace/key mutations, complete put values
and exact absent/prior-hash preconditions. Deletes retain the prior-value hash.
Repeated writes fold to the original precondition/final value; cancellation
retains bounded observed-state evidence. A precondition or bounds failure
poisons the candidate. Control rows never describe themselves recursively;
control-only receipts have empty mutations and exact attribution.

ADR-0186 Amendment 1 adds `CommandAdmission` tag 32 and
`CommandExecutionFailure` tag 33 before first durable use. Both carry nonempty
exact authoritative mutations. Admission advances neither frontier; failure
never advances application and advances administration only for audit in that
same transaction. Existing advancing-group checks remain strict.

V3 is exactly `riffdb.changelog-frame/v3`, numeric 3, `RDBCLF03`/`RDBCLE03`.
Its 33 source-count slots yield a 298-byte header; its footer is 48 bytes.
Admission reserves the whole wrapper and length prefix. The 32 MiB and
256-transition ceilings are independent hard limits; receipts never split.
Unknown tags/versions, wrong counts/frontiers, noncanonical order, broken
ancestry, truncation and trailing bytes refuse. Diagnostics omit keys, values,
payload-derived hashes and population counts.

V1/V2 decoders and original encodings remain compatibility-only. Legacy emitter
construction/derivation lives in `tests/storage_recovery/changelog_compatibility.rs`,
not production exports. Topology has exactly ordered V1/V2/V3 readers, only V3
writable/current/active, V1/V2 read-only, empty candidates/retired, and
`single_current`. Readability never grants production selection, fallback,
translation or partial-authority replication.

## Transaction and checkpoint durability

Same-lineage authoritative transactions retain one exact receipt in their
existing durability boundary. Direct commands, admission/failure, audited
controls, offline holds, pruning and migration use mutation-time capture in the
original Immediate transaction. Sealing checks actual allocator/frontier
postimages and stages receipt, allocator and tail atomically. Stale inputs,
unknown tables and overflow refuse; no-op captures allocate nothing. Commit
uncertainty retains existing writer fencing.

Standard journal admission proves a bounded receipt and stages its checked
allocation before submitting the original Journal V1 frame. Immutable source
bindings retain original mutations, not latest-row reads or another frame
decode. Logical subgroups in one physical epoch produce one net receipt while
preserving each command's outcome, events, provenance and idempotency identity.
Journal V1 bytes/tags and command acknowledgement/flush dependencies are unchanged.
Writer-private changes cannot publish a receipt cursor.

Checkpoint workers materialize identical source receipts in the existing
durable transaction before source reclamation. Direct barriers drain the suffix
into the caller's same transaction before capturing its successor; abort leaves
the source intact. Recovery validates retained history and exact original
overlaps before replaying missing successors. Gaps, duplicates, partial roots
and disagreement refuse without reclaiming the source. Empty extents retain
their bounded-root/header no-op check. Composite rebase drops only the covered
source prefix, preserving newer sources and old pins; destruction is iterative
on the production stack.

Validated-prefix certificates check physical AUDIT bounds from the same pin.
Command-segment audits can make the V3 logical frontier larger without changing
certificate bytes or audit placement. Optional-proof fallback remains: after
full validation, invalid proof replacement requires its exact preimage; an
absent singleton can be inserted with bounded net head changes. Valid-prior
parent/monotonicity checks and authoritative corruption refusal remain strict.
Head hashing streams unchanged SHA-256 v1 framing with bounded memory; it is
not a constant-time population scan.

## Activation, lifecycle, migration and restore

Fresh initialization/legacy normalization retain the exact pre-V3 registry.
Inactive startup completes structural and catalog validation without CLEAN or
prefix shortcuts. The dormant handoff owns the exclusive lease across their
join; cancellation releases it without writing even if readers retain the
database handle. One hardened transaction installs current registry, complete
V3 roots/control tables and the sequence-one activation receipt through the
existing durable publication owner. Application/admin allocators are preserved;
older receipts are never synthesized.

Current-registry claims require complete V3 roots; erasure cannot authorize
legacy repair or make an active database inactive. Dormant reopen validates
the catalogued layout without writing. Full startup streams retained history
and every hold before lifecycle writes. Valid CLEAN takes bounded root checks;
missing, malformed or exhausted evidence selects full validation or refusal.
Each DIRTY activation/clean consumption and final CLEAN close includes its
empty, nonrecursive receipt in the existing Immediate transaction. Lifecycle,
allocator, tail and receipt are atomic; no write follows CLEAN. Lifecycle V1
bytes and binding streams remain unchanged.

Index rewrites, generation repair, marker insertion and same-lineage contract
migration keep their transaction boundaries. Private migration witnesses bind
the original V3 prefix and validate its complete successor chain. Only tail
and allocator may advance; original receipt, lineage, anchor, minimum resume
and all other immutable bytes remain checked. Resume reuses the predecessor
witness rather than adopting a newer stage baseline.

Offline authoritative pruning receipts each original checkpoint-invalidation
and bounded delete/tombstone/watermark transaction. Tombstone hashing uses the
same current rows in streaming passes; oversized plans refuse. Watermark
stamps validate before equal retries and preserve the chain-root digest.
Same-lineage pruning does not reset lineage.

Destructive restore reanchors active V3 in its existing incarnation-stamp
transaction: preserve database identity, leadership and dual frontier; advance
incarnation; clear source history/holds; detach follower state; remove lifecycle;
install one empty sequence-one RestoreAnchor. Watermarks are rebound without
inventing a digest. Equal retries validate without writes; backwards incarnation
refuses. Old-lineage resume fails and receivers must bootstrap after the anchor.

## Source holds and history retention

Controls are crate-private, with no application exports or active scheduler.
Registration checks exact retained fences; equal retries allocate nothing.
Follower/archive acknowledgements advance monotonically; bootstrap fences
cannot move or release here. WP-746 owns authorized remote durability evidence,
durable tail attachment and audited abort.

Reclamation needs two observed known-durable checkpoints, using both publication
identity and successful-commit epoch. The floor cannot pass the earlier tail or
any registered follower/archive/bootstrap fence. An existing drained exclusive
barrier encloses at most 256 receipt deletions, exact minimum resume, allocator
and an empty HistoryReclamation receipt. Surviving receipts and authoritative
rows are unchanged. Post-commit observation prevents self-stimulation; lost
observations delay pruning safely. No command-path hook/flush is added. CLEAN,
writer fencing, invalid holds and exhaustion refuse; cancellation aborts and
releases the lease.

## Proofs and fixture review

- `replication_inventory_classifies_every_storage_namespace`: populated physical
  catalog, exact ends and reviewed classification.
- `successor_changelog_receipts_survive_checkpoint_and_recovery`: real direct
  groups, journal overwrite/delete, complete-authority replay, unchanged command
  graphs, identical checkpoint bytes and repeated recovery; also executes the
  real command allocator/journal process-crash matrix.
- `changelog_history_reclamation_respects_checkpoint_and_fences`: real journaled
  audits, exact materialization, every hold kind, unchanged full authority,
  old/new pins and seven hold/reclamation crash edges with repeated reopen.
- `changelog_v3_is_only_production_replication_identity`: fixed format digests,
  round trips, resealed downgrade refusal and no legacy production consumers.

Additional process tests cover actual activation, lifecycle, checkpoint workers,
journal recovery, captured controls, optional-proof fallback, migration, restore
and watermarks. Refusal tests cover interior corruption, missing roots,
substituted holds, exhaustion, the 4,096-hold ceiling, concurrent registration
and cancellation. Integrated recovery and full CI remain required alongside
scoped acceptance. Isolated codec fixtures do not prove application semantics
or an activated replication protocol.

Simulation keeps all historical coordinates and predicates. Before/after replay
attributes one restored window to `630e1a42` and six moved windows to `a29312ff`.
Appended witnesses `0x51C2C30A`, `0x51C2C404` and `0x51C2C500` each reproduced
identical reports through twelve reruns. No oracle, generator, bound, accepted
outcome or retirement history was weakened.

The maintainer approved the catalog and initial V1/V2/V3 vectors, then the three
source-hold vectors on 2026-09-14. Amendment 1 subsequently added admission,
execution-failure, audited-failure and command-lifecycle vectors and regenerated
`changelog-frame-v3.hex` for 33 source slots. These synthetic vectors freeze
encoding, not application-record validity. The maintainer approved all five final
vectors in session on 2026-09-14; their exact hashes are in the verification report.
V1/V2 and unrelated vectors remain byte-identical. Regenerate with
`./scripts/generate-changelog-fixtures`; its `--check` runs in generated-artifact
acceptance. Fixture review does not accept a new ADR.
